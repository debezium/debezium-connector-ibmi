/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Pure reconstruction of an already-published legacy prefix under owned bindings.
 * Caller must qualify native history, schema epoch and exact durable publication;
 * this calculation has no native I/O, history persistence or startup admission.
 */
final class As400HandoffReplay {
    static final int MAX_BYTES = 64 * 1024 * 1024;
    private static final Set<String> COMPLETED_ROWS = Set.of("PT", "PX", "UP", "DL");

    record Evidence(List<RawJournalPage.Entry> entries, List<As400BindingDefinition> definitions, As400JournalOrder order,
            String journal, String fingerprint, JournalPosition lower, JournalPosition upper, Set<BigInteger> inventory,
            Map<String, String> partition, As400HandoffWork work) {
        Evidence(List<RawJournalPage.Entry> entries, List<As400BindingDefinition> definitions, As400JournalOrder order,
                 String journal, String fingerprint, JournalPosition lower, JournalPosition upper, Set<BigInteger> inventory,
                 Map<String, String> partition) {
            this(entries, definitions, order, journal, fingerprint, lower, upper, inventory, partition, null);
        }

        Evidence {
            require(entries != null && (work == null ? entries.size() <= 20_000 : entries instanceof As400JournalCapture.Entries),
                    "Bounded owned retained history required");
            entries = As400JournalCapture.owned(entries); // RawJournalPage.Entry owns its bytes.
            definitions = List.copyOf(definitions);
            inventory = work == null ? Set.copyOf(inventory) : Collections.unmodifiableSet(inventory);
            partition = Map.copyOf(partition);
            long bytes = 0;
            for (var entry : entries) {
                byte[] raw = entry.data();
                require(raw.length <= 1024 * 1024 && (work != null || (bytes += raw.length) <= MAX_BYTES), "Excessive retained history bytes");
            }
        }
    }

    record Candidate(As400Checkpoint checkpoint, JournalPosition scan, Set<BigInteger> openCycles, As400HandoffWork owner) {
        Candidate(As400Checkpoint checkpoint, JournalPosition scan, Set<BigInteger> openCycles) {
            this(checkpoint, scan, openCycles, null);
        }

        Candidate {
            openCycles = owner == null ? Set.copyOf(openCycles) : Collections.unmodifiableSet(openCycles);
        }
    }

    static final class SelectedPrefixPending extends IllegalArgumentException {
        private final Set<BigInteger> cycles;

        SelectedPrefixPending(Set<BigInteger> cycles) {
            this(cycles, "Open cycle has a selected legacy prefix; continue legacy publication before migration");
        }

        SelectedPrefixPending(Set<BigInteger> cycles, String reason) {
            super(reason);
            this.cycles = As400HandoffWork.diagnosticCycles(cycles);
        }

        Set<BigInteger> cycles() {
            return cycles;
        }
    }

    static Candidate prepare(Evidence evidence, JournalPosition cut, As400ConnectorConfig config) throws Exception {
        require(!evidence.entries().isEmpty(), "Retained history required");
        var work = evidence.work();
        var order = evidence.order();
        require(order.compare(evidence.lower(), evidence.upper()) <= 0 && order.compare(evidence.upper(), cut) <= 0,
                "Inventory bracket must precede the exact publication cut");
        for (JournalPosition anchor : List.of(evidence.lower(), evidence.upper(), cut)) {
            require(evidence.entries().stream().anyMatch(e -> e.position().equals(anchor)), "Missing inventory or publication anchor");
        }
        Map<BigInteger, JournalPosition> starts = work == null ? new HashMap<>() : work.map(As400SpillCodec.INTEGER, As400SpillCodec.POSITION);
        Set<BigInteger> open = work == null ? new HashSet<>() : work.set(As400SpillCodec.INTEGER);
        Set<BigInteger> dirty = work == null ? new HashSet<>() : work.set(As400SpillCodec.INTEGER);
        Set<BigInteger> required = work == null ? new HashSet<>() : work.set(As400SpillCodec.INTEGER);
        for (var cycle : evidence.inventory()) {
            if (work != null) {
                work.check();
            }
            required.add(cycle);
        }
        var selectedFiles = new HashSet<String>();
        evidence.definitions().stream().filter(d -> config.getTableFilters().dataCollectionFilter().isIncluded(d.identity().tableId()))
                .forEach(d -> selectedFiles.add(d.identity().tableId().schema() + "/" + d.identity().physicalFile()));
        JournalPosition floor = null, completed = null, previous = null;
        var controls = new TransactionControlEntryDecoder();
        for (RawJournalPage.Entry entry : evidence.entries()) {
            if (work != null) {
                work.check();
            }
            require(previous == null || order.compare(previous, entry.position()) < 0, "Repeated or unordered retained entry");
            previous = entry.position();
            if (order.compare(entry.position(), cut) > 0) {
                continue;
            }
            byte[] raw = entry.data();
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(raw, 0);
            var source = As400SourceMetadata.from(header, entry.position());
            BigInteger cycle = source.commitCycle();
            if (cycle.signum() > 0 && order.compare(entry.position(), evidence.lower()) >= 0) {
                required.add(cycle);
            }
            var control = header.getJournalCode() == 'C' ? controls.decode(header, raw, 0) : null;
            boolean start = control != null && control.kind() == TransactionControlEntryDecoder.Kind.START;
            if (start) {
                require(cycle.equals(entry.position().offset()) && starts.putIfAbsent(cycle, entry.position()) == null,
                        "Missing, repeated or ambiguous cycle start");
                open.add(cycle);
                if (floor == null) {
                    floor = entry.position();
                }
            }
            if (cycle.signum() > 0) {
                require(starts.containsKey(cycle), "Missing retained original transaction start");
                require(open.contains(cycle), "Work follows a completed transaction");
                boolean selected = selectedFiles.contains(header.getLibrary() + "/" + header.getFile());
                if (selected && (header.getJournalCode() == 'R' || header.getJournalCode() == 'D' && header.getEntryType().equals("CG"))) {
                    dirty.add(cycle);
                }
            }
            boolean terminal = control != null && (control.kind() == TransactionControlEntryDecoder.Kind.COMMIT
                    || control.kind() == TransactionControlEntryDecoder.Kind.ROLLBACK);
            if (terminal) {
                require(control.controlInfo().nestedCommitLevel() == 0
                        && (control.outcome() == TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT
                                || control.outcome() == TransactionControlEntryDecoder.Outcome.FULL_ROLLBACK),
                        "Ambiguous terminal");
                open.remove(cycle);
                dirty.remove(cycle);
                completed = entry.position();
            }
            else if (cycle.signum() == 0 && (header.getJournalCode() == 'R' && COMPLETED_ROWS.contains(header.getEntryType())
                    || header.getJournalCode() == 'F' && header.getEntryType().equals("CR"))) {
                completed = entry.position();
            }
        }
        for (var cycle : required) {
            if (work != null) {
                work.check();
            }
            require(starts.containsKey(cycle), "Inventory cycle has no retained original start");
        }
        if (floor == null && required.isEmpty()) {
            // A qualified interval may contain only autocommit work. Replay its
            // actual first record inclusively; never invent a C/SC or F-1 anchor.
            // The normal replay/lifecycle checks below still reject incomplete
            // updates, rollback companions and unavailable earlier context.
            floor = evidence.entries().get(0).position();
        }
        require(floor != null && completed != null && order.compare(floor, completed) <= 0, "No supported completed-output boundary in retained context");
        validateEarlierContext(evidence, floor);
        // W is an actual terminal/completed autocommit entry, independently of cut R.
        // F is an actual retained record; never synthesize a closed F-1 anchor.
        var seed = new As400Checkpoint(evidence.journal(), evidence.fingerprint(), floor, true, completed,
                order.incarnation(floor), order.incarnation(completed));
        try (var scratch = work == null ? null : work.transactionStore();
                var replay = coordinator(evidence, seed.toOffset(), record -> {
                    throw new IllegalStateException("Candidate preparation must never publish");
                }, scratch)) {
            for (RawJournalPage.Entry entry : evidence.entries()) {
                if (order.compare(entry.position(), floor) >= 0 && order.compare(entry.position(), cut) <= 0) {
                    byte[] raw = entry.data();
                    require(replay.accept(new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(raw, 0), raw, 0, entry.position()).isEmpty(),
                            "Undelivered output within the acknowledged prefix");
                }
            }
            replay.verifyOrdinaryCut();
            require(replay.scanPosition().orElseThrow().equals(cut) && replay.checkpoint().completedThrough().equals(completed),
                    "Reconstruction changed the publication/completion boundary");
            // Replay under the existing source/binding/lifecycle checks first.
            // A detected history fault cannot become a harmless busy-cut retry.
            if (!dirty.isEmpty()) {
                throw new SelectedPrefixPending(dirty);
            }
            if (config.shouldProvideTransactionMetadata() && !open.isEmpty()) {
                // Legacy BEGIN metadata can precede any captured row, including
                // a cycle touching only excluded files. Keep publishing its old
                // metadata until the actual terminal; never invent a new BEGIN
                // halfway through that sequence or strand the existing one.
                throw new SelectedPrefixPending(open, "Open legacy transaction metadata; continue legacy publication before migration");
            }
            return new Candidate(replay.checkpoint(), cut, open, work);
        }
    }

    private static void validateEarlierContext(Evidence evidence, JournalPosition floor) throws Exception {
        var work = evidence.work();
        var limits = work == null ? new As400TransactionBuffer.Limits(128, 40_000, 2048, MAX_BYTES) : work.limits().transactions();
        var bindings = new HashMap<String, As400SourceNormalizer.TableBinding>();
        evidence.definitions().forEach(d -> bindings.put(d.identity().tableId().schema() + "/" + d.identity().physicalFile(), d.binding(1024 * 1024)));
        try (var scratch = work == null ? null : work.transactionStore();
                var transactions = new As400TransactionBuffer(evidence.journal(), limits, scratch);
                var autocommit = new As400AutocommitBuffer(evidence.journal(), limits, transactions::usage, scratch)) {
            var normalizer = new As400SourceNormalizer(transactions, (file, library) -> bindings.get(library + "/" + file), 1024 * 1024,
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), autocommit);
            for (RawJournalPage.Entry entry : evidence.entries()) {
                if (evidence.order().compare(entry.position(), floor) >= 0) {
                    break;
                }
                // Earlier lifecycle dependencies cannot be discarded just because
                // F was chosen from a transaction start. Only completely resolved
                // ordinary rows/controls qualify here; unsupported transitions reject.
                byte[] raw = entry.data();
                var batch = normalizer.accept(new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(raw, 0), raw, 0, entry.position());
                if (batch.isPresent()) {
                    require(batch.orElseThrow().transactionId().commitCycle().signum() == 0, "Earlier transaction context is incomplete");
                    autocommit.complete(batch.orElseThrow());
                }
            }
            require(transactions.usage().transactions() == 0 && autocommit.retainedStarts().isEmpty(), "Unresolved context before candidate F");
        }
    }

    static As400ReplayCoordinator coordinator(Evidence evidence, Map<String, ?> offset, As400ReplayCoordinator.RecordSink sink) {
        require(evidence.work() == null, "Owned handoff replay requires its scoped working storage");
        return coordinator(evidence, offset, sink, null);
    }

    private static As400ReplayCoordinator coordinator(Evidence evidence, Map<String, ?> offset, As400ReplayCoordinator.RecordSink sink,
                                                      As400SpillStore scratch) {
        var bindings = new HashMap<String, As400SourceNormalizer.TableBinding>();
        evidence.definitions().forEach(d -> require(bindings.put(d.identity().tableId().schema() + "/" + d.identity().physicalFile(), d.binding(1024 * 1024)) == null,
                "Duplicate historical binding"));
        var limits = evidence.work() == null
                ? new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(128, 40_000, 2048, MAX_BYTES),
                        new As400JournalLifecycle.Limits(20_000, MAX_BYTES, 128, 2048, 1024, 4096), 20_000)
                : evidence.work().limits();
        return new As400ReplayCoordinator(offset, evidence.journal(), evidence.fingerprint(), evidence.order(),
                limits.transactions(), 1024 * 1024, limits.outputRecords(),
                (file, library) -> bindings.get(library + "/" + file), As400SourceNormalizer.SavepointPolicy.consumingTargets(), evidence.partition(), sink,
                limits.lifecycle(), true, scratch);
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
