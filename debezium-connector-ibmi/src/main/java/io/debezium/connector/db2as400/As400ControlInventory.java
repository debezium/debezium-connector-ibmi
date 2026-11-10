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

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Kind;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Outcome;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Pure C/J reconstruction over separately verified retained native history.
 * Does not prove row completeness, historical schemas, publication or retention.
 * Maintenance remains explicit evidence; it is never a transaction boundary.
 */
final class As400ControlInventory {
    private static final int MAX_ENTRIES = 20_000;
    private static final long MAX_BYTES = 64 * 1024 * 1024;

    record Producer(String system, String job, String user, String number, BigInteger thread) {
        static Producer from(EntryHeader header) {
            var info = header.getRecordInfo();
            require(info.threadId() != null && info.threadId().signum() > 0
                    && info.jobNumber().matches("[0-9]{6}") && !info.jobNumber().equals("000000"), "Missing native control producer");
            for (String value : List.of(info.systemName(), info.jobName(), info.jobUser())) {
                require(!value.isBlank() && !value.startsWith("*"), "Missing native control producer");
            }
            return new Producer(info.systemName(), info.jobName(), info.jobUser(), info.jobNumber(), info.threadId());
        }
    }

    record Start(As400SourceMetadata source, Producer producer) {
    }

    record Maintenance(JournalPosition position, String type, String contentDigest) {
    }

    record Inventory(Map<BigInteger, Start> openAtLower, Map<BigInteger, Start> openThrough,
            Map<BigInteger, JournalPosition> terminals, Map<BigInteger, JournalPosition> completedBeforeHistory, List<Maintenance> maintenance) {
        Inventory {
            openAtLower = Map.copyOf(openAtLower);
            openThrough = Map.copyOf(openThrough);
            terminals = Map.copyOf(terminals);
            completedBeforeHistory = Map.copyOf(completedBeforeHistory);
            maintenance = List.copyOf(maintenance);
        }
    }

    static Inventory reconstruct(List<RawJournalPage.Entry> history, As400JournalOrder order, String journal,
                                 JournalPosition from, JournalPosition lower, JournalPosition through)
            throws Exception {
        require(history != null && history.size() <= MAX_ENTRIES, "Bounded C/J history required");
        require(order.compare(from, lower) <= 0 && order.compare(lower, through) <= 0, "Observation lies outside native history");
        var starts = new LinkedHashMap<BigInteger, Start>();
        var open = new LinkedHashMap<BigInteger, Start>();
        var terminals = new LinkedHashMap<BigInteger, JournalPosition>();
        var completedBeforeHistory = new LinkedHashMap<BigInteger, JournalPosition>();
        var maintenance = new ArrayList<Maintenance>();
        Map<BigInteger, Start> atLower = null;
        var headers = new EntryHeaderDecoder();
        var controls = new TransactionControlEntryDecoder();
        JournalPosition previous = null;
        long bytes = 0;
        // Reuse the normal transaction control state machine. Empty batches are
        // consumed locally; this class has no sink, acknowledgement or checkpoint.
        try (var buffer = new As400TransactionBuffer(journal,
                new As400TransactionBuffer.Limits(128, MAX_ENTRIES, 2048, MAX_BYTES))) {
            for (RawJournalPage.Entry entry : history) {
                byte[] data = entry.data();
                require((bytes += data.length) <= MAX_BYTES, "C/J history exceeds byte budget");
                var position = entry.position();
                require(order.compare(from, position) <= 0 && order.compare(position, through) <= 0,
                        "Control lies outside captured native history");
                require(previous == null || order.compare(previous, position) < 0 && previous.offset().compareTo(position.offset()) < 0,
                        "Repeated, reset or unordered C/J history");
                previous = position;
                if (atLower == null && order.compare(position, lower) >= 0) {
                    atLower = Map.copyOf(open);
                }
                var header = headers.decode(data, 0);
                // Maintenance can legitimately have flags that ordinary rows do
                // not. Preserve it; do not run it through a row normalizer.
                As400SourceMetadata.from(header, position);
                if (header.getJournalCode() == 'J') {
                    require(order.compare(position, lower) < 0, "Journal maintenance overlaps the observation");
                    maintenance.add(new Maintenance(position, header.getEntryType(), As400JournalRecordContent.digest(data, position)));
                    continue;
                }
                require(header.getJournalCode() == 'C', "Non-control entry in C/J history");
                var source = As400SourceNormalizer.validateSource(header, data, 0, position);
                var control = controls.decode(header, data, 0);
                var producer = Producer.from(header);
                var cycle = source.commitCycle();
                require(!completedBeforeHistory.containsKey(cycle), "Control follows an excluded completed prefix");
                if (cycle.signum() > 0 && (control.kind() == Kind.BEGIN_COMMITMENT_CONTROL || control.kind() == Kind.END_COMMITMENT_CONTROL)) {
                    require(starts.containsKey(cycle), "Commitment lifecycle has no retained original cycle start");
                }
                boolean terminal = control.kind() == Kind.COMMIT || control.kind() == Kind.ROLLBACK;
                if (!starts.containsKey(cycle) && cycle.signum() > 0 && cycle.compareTo(from.offset()) < 0 && terminal
                        && order.compare(position, lower) < 0) {
                    // A receiver may begin in the tail of a transaction that
                    // finished before L. Its original start is unnecessary ONLY
                    // to determine which cycles remain open at L. Do not return
                    // it as reconstructed history or an output checkpoint.
                    require(control.controlInfo().nestedCommitLevel() == 0
                            && (control.outcome() == Outcome.DURABLE_COMMIT || control.outcome() == Outcome.FULL_ROLLBACK),
                            "Unresolved terminal before observation");
                    completedBeforeHistory.put(cycle, position);
                    continue;
                }
                if (control.kind() == Kind.START) {
                    require(cycle.equals(position.offset()) && !starts.containsKey(cycle), "Ambiguous or repeated original cycle start");
                    var start = new Start(source, producer);
                    starts.put(cycle, start);
                    open.put(cycle, start);
                }
                else if (control.kind() != Kind.BEGIN_COMMITMENT_CONTROL && control.kind() != Kind.END_COMMITMENT_CONTROL) {
                    require(starts.containsKey(cycle), "Missing retained original cycle start");
                    require(open.containsKey(cycle), "Control follows a completed cycle");
                    require(starts.get(cycle).producer().equals(producer), "Control cycle changed native producer");
                }
                var batch = buffer.acceptControl(source, control, control.kind() == Kind.ROLLBACK_SAVEPOINT
                        ? As400TransactionBuffer.SavepointRollback.CONSUME_TARGET
                        : As400TransactionBuffer.SavepointRollback.UNRESOLVED);
                if (batch.isPresent()) {
                    require(batch.orElseThrow().changes().isEmpty(), "Control inventory cannot contain row output");
                    buffer.completeBatch(batch.orElseThrow());
                }
                if (terminal) {
                    // acceptControl has already rejected incomplete, nested,
                    // non-durable and unknown outcomes. SU/SQ never close a cycle.
                    terminals.put(cycle, position);
                    open.remove(cycle);
                }
            }
            require(buffer.usage().transactions() == open.size() && open.keySet().stream().allMatch(buffer::hasTransaction),
                    "Control projection disagrees with transaction state");
            return new Inventory(atLower == null ? open : atLower, open, terminals, completedBeforeHistory, maintenance);
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
