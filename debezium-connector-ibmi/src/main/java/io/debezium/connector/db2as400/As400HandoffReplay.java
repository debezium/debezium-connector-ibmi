/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
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
            Map<String, String> partition) {
        Evidence {
            require(entries != null && entries.size() <= 20_000, "Bounded retained history required");
            entries = List.copyOf(entries); // RawJournalPage.Entry owns its bytes.
            definitions = List.copyOf(definitions);
            inventory = Set.copyOf(inventory);
            partition = Map.copyOf(partition);
            long bytes = 0;
            for (var entry : entries) {
                byte[] raw = entry.data();
                require(raw.length <= 1024 * 1024 && (bytes += raw.length) <= MAX_BYTES, "Excessive retained history bytes");
            }
        }
    }

    record Candidate(As400Checkpoint checkpoint, JournalPosition scan, Set<BigInteger> openCycles) {
        Candidate {
            openCycles = Set.copyOf(openCycles);
        }
    }

    static final class SelectedPrefixPending extends IllegalArgumentException {
        private final Set<BigInteger> cycles;

        SelectedPrefixPending(Set<BigInteger> cycles) {
            super("Open cycle has a selected legacy prefix; continue legacy publication before migration");
            this.cycles = Set.copyOf(cycles);
        }

        Set<BigInteger> cycles() {
            return cycles;
        }
    }

    static Candidate prepare(Evidence evidence, JournalPosition cut, As400ConnectorConfig config) throws Exception {
        require(!config.shouldProvideTransactionMetadata(), "Legacy transaction metadata transition is outside this proof");
        require(!evidence.entries().isEmpty() && evidence.entries().size() <= 20_000, "Bounded retained history required");
        var order = evidence.order();
        require(order.compare(evidence.lower(), evidence.upper()) <= 0 && order.compare(evidence.upper(), cut) <= 0,
                "Inventory bracket must precede the exact publication cut");
        for (JournalPosition anchor : List.of(evidence.lower(), evidence.upper(), cut)) {
            require(evidence.entries().stream().anyMatch(e -> e.position().equals(anchor)), "Missing inventory or publication anchor");
        }
        var starts = new HashMap<BigInteger, JournalPosition>();
        var open = new HashSet<BigInteger>();
        var dirty = new HashSet<BigInteger>();
        var required = new HashSet<>(evidence.inventory());
        JournalPosition floor = null, completed = null, previous = null;
        var controls = new TransactionControlEntryDecoder();
        for (RawJournalPage.Entry entry : evidence.entries()) {
            require(previous == null || order.compare(previous, entry.position()) < 0, "Repeated or unordered retained entry");
            previous = entry.position();
            if (order.compare(entry.position(), cut) > 0) {
                continue;
            }
            byte[] raw = entry.data();
            var header = new EntryHeaderDecoder().decode(raw, 0);
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
                boolean selected = evidence.definitions().stream().anyMatch(d -> d.identity().physicalFile().equals(header.getFile())
                        && d.identity().tableId().schema().equals(header.getLibrary())
                        && config.getTableFilters().dataCollectionFilter().isIncluded(d.identity().tableId()));
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
        require(required.stream().allMatch(starts::containsKey), "Inventory cycle has no retained original start");
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
        try (var replay = coordinator(evidence, seed.toOffset(), record -> {
            throw new IllegalStateException("Candidate preparation must never publish");
        })) {
            for (RawJournalPage.Entry entry : evidence.entries()) {
                if (order.compare(entry.position(), floor) >= 0 && order.compare(entry.position(), cut) <= 0) {
                    byte[] raw = entry.data();
                    require(replay.accept(new EntryHeaderDecoder().decode(raw, 0), raw, 0, entry.position()).isEmpty(),
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
            return new Candidate(replay.checkpoint(), cut, open);
        }
    }

    private static void validateEarlierContext(Evidence evidence, JournalPosition floor) throws Exception {
        var limits = new As400TransactionBuffer.Limits(128, 40_000, 2048, MAX_BYTES);
        var bindings = new HashMap<String, As400SourceNormalizer.TableBinding>();
        evidence.definitions().forEach(d -> bindings.put(d.identity().tableId().schema() + "/" + d.identity().physicalFile(), d.binding(1024 * 1024)));
        try (var transactions = new As400TransactionBuffer(evidence.journal(), limits);
                var autocommit = new As400AutocommitBuffer(evidence.journal(), limits, transactions::usage)) {
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
                var batch = normalizer.accept(new EntryHeaderDecoder().decode(raw, 0), raw, 0, entry.position());
                if (batch.isPresent()) {
                    require(batch.orElseThrow().transactionId().commitCycle().signum() == 0, "Earlier transaction context is incomplete");
                    autocommit.complete(batch.orElseThrow());
                }
            }
            require(transactions.usage().transactions() == 0 && autocommit.retainedStarts().isEmpty(), "Unresolved context before candidate F");
        }
    }

    static As400ReplayCoordinator coordinator(Evidence evidence, Map<String, ?> offset, As400ReplayCoordinator.RecordSink sink) {
        var bindings = new HashMap<String, As400SourceNormalizer.TableBinding>();
        evidence.definitions().forEach(d -> require(bindings.put(d.identity().tableId().schema() + "/" + d.identity().physicalFile(), d.binding(1024 * 1024)) == null,
                "Duplicate historical binding"));
        return new As400ReplayCoordinator(offset, evidence.journal(), evidence.fingerprint(), evidence.order(),
                new As400TransactionBuffer.Limits(128, 40_000, 2048, MAX_BYTES), 1024 * 1024, 20_000,
                (file, library) -> bindings.get(library + "/" + file), As400SourceNormalizer.SavepointPolicy.consumingTargets(), evidence.partition(), sink,
                new As400JournalLifecycle.Limits(20_000, MAX_BYTES, 128, 2048, 1024, 4096), true);
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
