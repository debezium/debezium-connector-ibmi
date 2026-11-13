/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/** Offset-pinned table publication while the legacy reader encounters an unresolved clear. */
final class As400LegacyClearBuffer {
    @FunctionalInterface
    interface Preparation {
        Optional<As400NativeLegacyClear.Prepared> prepare(BigInteger cycle, List<As400NativeLegacyClear.Target> tables,
                                                          JournalPosition through, String journal)
                throws Exception;
    }

    private final As400Partition partition;
    private final As400OffsetContext scan;
    private final As400StreamingChangeEventSource source;
    private final As400LegacySnapshotBootstrap.Delivery delivery;
    private final Preparation preparation;
    private final Map<BigInteger, List<As400NativeLegacyClear.Target>> pending;
    private final Map<String, As400NativeLegacyClear.Target> earliest;
    private final Map<BigInteger, Map<String, JournalPosition>> rows;
    private final Map<BigInteger, RawJournalPage.Entry> terminals;
    private final Map<String, JournalPosition> released;
    private final int maximumCycles;
    private boolean pinned;

    As400LegacyClearBuffer(As400Partition partition, As400OffsetContext scan, As400StreamingChangeEventSource source,
                           As400LegacySnapshotBootstrap.Delivery delivery, Preparation preparation, int maximumCycles, As400SpillStore spill) {
        this.partition = partition;
        this.scan = scan;
        this.source = source;
        this.delivery = delivery;
        this.preparation = preparation;
        this.maximumCycles = maximumCycles;
        var targetCodec = As400SpillCodec.<As400NativeLegacyClear.Target> codec((out, target) -> {
            As400SpillCodec.tableId(out, target.table());
            As400SpillCodec.string(out, target.file());
            As400SpillCodec.position(out, target.firstClear());
        }, in -> new As400NativeLegacyClear.Target(As400SpillCodec.tableId(in), As400SpillCodec.string(in), As400SpillCodec.position(in)));
        pending = spill.map("legacy-clears", As400SpillCodec.INTEGER, As400SpillCodec.codec((out, targets) -> {
            out.writeInt(targets.size());
            for (var target : targets) {
                As400SpillCodec.bytes(out, targetCodec.encode(target));
            }
        }, in -> {
            int count = in.readInt();
            require(count > 0 && count <= As400SpillCodec.MAX_VALUE_BYTES / 20, "Invalid deferred table count");
            var targets = new ArrayList<As400NativeLegacyClear.Target>();
            for (int i = 0; i < count; i++) {
                targets.add(targetCodec.decode(As400SpillCodec.bytes(in)));
            }
            return targets;
        }));
        earliest = spill.map("legacy-held-tables", As400SpillCodec.STRING, targetCodec);
        rows = spill.map("legacy-held-rows", As400SpillCodec.INTEGER, As400SpillCodec.POSITIONS);
        terminals = spill.map("legacy-held-terminals", As400SpillCodec.INTEGER, As400SpillCodec.codec((out, entry) -> {
            As400SpillCodec.position(out, entry.position());
            As400SpillCodec.bytes(out, entry.data());
        }, in -> {
            var position = As400SpillCodec.position(in);
            byte[] raw = As400SpillCodec.bytes(in);
            return new RawJournalPage.Entry(position, new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(raw, 0), raw);
        }));
        released = spill.map("legacy-released-tables", As400SpillCodec.STRING, As400SpillCodec.POSITION);
    }

    void process(RawJournalPage.Entry entry) throws Exception {
        var h = entry.header();
        var cycle = h.getCommitCycle();
        if (h.getJournalCode() == 'C' && List.of("CM", "RB").contains(h.getEntryType())
                && (rows.containsKey(cycle) || pending.containsKey(cycle))) {
            var control = new TransactionControlEntryDecoder().decode(h, entry.data(), 0);
            require(control.controlInfo().nestedCommitLevel() == 0
                    && (control.outcome() == TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT
                            || control.outcome() == TransactionControlEntryDecoder.Outcome.FULL_ROLLBACK),
                    "Ambiguous deferred terminal");
            require(terminals.putIfAbsent(cycle, entry) == null, "Repeated deferred terminal");
            return;
        }
        String key = h.getLibrary() + "/" + h.getFile();
        var held = earliest.get(key);
        if (held != null) {
            if (h.getJournalCode() == 'D' && h.getEntryType().equals("CG")
                    && new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(h, entry.data(), 0).clearsRows()) {
                hold(cycle, new As400NativeLegacyClear.Target(held.table(), held.file(), entry.position()));
            }
            if (h.getJournalCode() == 'R' && cycle.signum() > 0) {
                retain(cycle, key, entry.position());
            }
            return;
        }
        source.processOwnedEntry(partition, scan, entry, (table, header, file) -> {
            require(header.getSequenceNumber().equals(entry.position().offset()), "Deferred clear source coordinate changed");
            hold(header.getCommitCycle(), new As400NativeLegacyClear.Target(table, file, entry.position()));
        });
    }

    private void hold(BigInteger cycle, As400NativeLegacyClear.Target target) throws Exception {
        require(cycle.signum() > 0, "Only transactional D/CG can be deferred");
        if (!pinned) {
            delivery.pin(scan.getOffset());
            pinned = true;
        }
        var targets = pending.get(cycle);
        if (targets == null) {
            require(pending.size() < maximumCycles, "Deferred-clear transaction limit exceeded");
            targets = new ArrayList<>();
        }
        if (targets.stream().noneMatch(t -> t.key().equals(target.key()))) {
            targets.add(target);
            pending.put(cycle, targets);
        }
        earliest.putIfAbsent(target.key(), target);
        retain(cycle, target.key(), target.firstClear());
    }

    private void retain(BigInteger cycle, String key, JournalPosition position) {
        var tables = rows.get(cycle);
        if (tables == null) {
            require(rows.size() < maximumCycles, "Deferred row transaction limit exceeded");
            tables = new LinkedHashMap<>();
        }
        tables.put(key, position);
        rows.put(cycle, tables);
    }

    void resolve(JournalPosition through, String journal) throws Exception {
        var waiting = pending.entrySet().iterator();
        while (waiting.hasNext()) {
            var group = waiting.next();
            if (!terminals.containsKey(group.getKey()) || group.getValue().stream()
                    .anyMatch(t -> !earliest.get(t.key()).firstClear().equals(t.firstClear()))) {
                continue;
            }
            var candidate = preparation.prepare(group.getKey(), group.getValue(), through, journal);
            if (candidate.isEmpty()) {
                continue;
            }
            try (var prepared = candidate.orElseThrow()) {
                require(prepared.resolution.terminal().commitCycle().equals(group.getKey()), "Prepared clear belongs to another cycle");
                source.publishDeferredClear(partition, scan, prepared);
                waiting.remove();
                for (var target : group.getValue()) {
                    earliest.remove(target.key());
                    for (var remaining : pending.values()) {
                        for (var next : remaining) {
                            if (next.key().equals(target.key()) && (!earliest.containsKey(next.key())
                                    || next.firstClear().offset().compareTo(earliest.get(next.key()).firstClear().offset()) < 0)) {
                                earliest.put(next.key(), next);
                            }
                        }
                    }
                    released.put(target.key(), prepared.resolution.consumedThrough());
                }
                // Rows committed after this clear may have been held while its
                // rollback companions/metadata were being verified. Release only
                // the prefix before the next unresolved clear of that same table.
                for (var entry : prepared.entries) {
                    var h = entry.header();
                    String key = h.getLibrary() + "/" + h.getFile();
                    if (group.getValue().stream().noneMatch(t -> t.key().equals(key))) {
                        continue;
                    }
                    var cutoff = earliest.get(key);
                    if (entry.position().offset().compareTo(released.get(key).offset()) > 0
                            && (cutoff == null || entry.position().offset().compareTo(cutoff.firstClear().offset()) < 0)) {
                        source.processOwnedEntry(partition, scan, entry);
                        released.put(key, entry.position());
                    }
                }
            }
            releaseRowsAndTerminals();
        }
        if (pinned && pending.isEmpty() && rows.isEmpty() && terminals.isEmpty() && !source.hasPendingBeforeImage()) {
            delivery.releasePin();
            pinned = false;
            released.clear();
        }
    }

    private void releaseRowsAndTerminals() throws Exception {
        var retained = rows.entrySet().iterator();
        while (retained.hasNext()) {
            var cycle = retained.next();
            var tables = cycle.getValue();
            tables.entrySet().removeIf(table -> released.containsKey(table.getKey())
                    && table.getValue().offset().compareTo(released.get(table.getKey()).offset()) <= 0);
            if (tables.isEmpty()) {
                retained.remove();
            }
            else {
                rows.put(cycle.getKey(), tables);
            }
        }
        var terminal = terminals.entrySet().iterator();
        while (terminal.hasNext()) {
            var entry = terminal.next();
            if (!rows.containsKey(entry.getKey()) && !pending.containsKey(entry.getKey())) {
                source.finishDeferredTransaction(partition, scan, As400SourceMetadata.from(entry.getValue().header(), entry.getValue().position()));
                terminal.remove();
            }
        }
    }

    boolean pending() {
        return pinned;
    }

    Map<String, ?> safeOffset(Map<String, ?> offset) {
        return delivery.safeOffset(offset);
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
