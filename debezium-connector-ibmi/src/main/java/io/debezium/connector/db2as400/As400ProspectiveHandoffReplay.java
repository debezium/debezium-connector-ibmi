/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.debezium.connector.db2as400.As400HandoffReplay.Candidate;
import io.debezium.connector.db2as400.As400HandoffReplay.Evidence;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Pure schema-epoch handoff reconstruction. The caller must acquire the native
 * boundary and retained C/J/row history; no origin or publication is authorized
 * by this model. Today's schemas are never used to decode older source rows.
 */
final class As400ProspectiveHandoffReplay {
    /** This temporary refusal allows a scheduler to keep ordinary legacy CDC moving. */
    static final class PriorEpochPending extends IllegalArgumentException {
        private final Set<BigInteger> cycles;

        PriorEpochPending(Set<BigInteger> cycles) {
            super("Prior-schema transactions remain open; continue legacy publication and defer migration");
            this.cycles = As400HandoffWork.diagnosticCycles(cycles);
        }

        Set<BigInteger> cycles() {
            return cycles;
        }
    }

    record Prepared(Evidence projection, Candidate candidate, Set<BigInteger> completedPriorCycles) {
        Prepared {
            completedPriorCycles = projection.work() == null ? Set.copyOf(completedPriorCycles) : Collections.unmodifiableSet(completedPriorCycles);
        }
    }

    static Prepared prepare(Evidence future, List<RawJournalPage.Entry> controls, JournalPosition schemaOrigin, JournalPosition cut,
                            As400ConnectorConfig config)
            throws Exception {
        var work = future.work();
        require(!future.entries().isEmpty() && (work == null ? controls.size() <= 20_000 : controls instanceof As400JournalCapture.Entries),
                "Bounded prospective rows and older controls required");
        long controlBytes = 0;
        for (RawJournalPage.Entry entry : controls) {
            require(entry.data().length <= 1024 * 1024 && (work != null || (controlBytes += entry.data().length) <= 64 * 1024 * 1024),
                    "Older controls exceed byte budget");
        }
        var order = future.order();
        require(order.compare(schemaOrigin, cut) <= 0, "Cut precedes qualified schema origin");
        var headers = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID);
        var ownedControls = work == null ? controls.stream().filter(e -> order.compare(e.position(), cut) <= 0).toList()
                : work.select(controls, e -> order.compare(e.position(), cut) <= 0);
        // A complete native C/J selection can be empty for autocommit-only
        // history. Inventory and overlapping C projections must still agree;
        // no empty list is a substitute for a successful native read.
        var controlFrom = ownedControls.isEmpty() || order.compare(ownedControls.get(0).position(), schemaOrigin) > 0
                ? schemaOrigin
                : ownedControls.get(0).position();
        var inventory = As400ControlInventory.reconstruct(ownedControls, order, future.journal(),
                controlFrom, schemaOrigin, cut, work);
        // The older C/J projection, not today's row decoder, establishes old-cycle lifetime.
        Set<BigInteger> prior = inventory.openAtLower().keySet();
        Set<BigInteger> stillOpen;
        if (work == null) {
            stillOpen = inventory.openThrough().keySet().stream().filter(prior::contains).collect(Collectors.toSet());
        }
        else {
            stillOpen = work.set(As400SpillCodec.INTEGER);
            for (var cycle : inventory.openThrough().keySet()) {
                work.check();
                if (prior.contains(cycle)) {
                    stillOpen.add(cycle);
                }
            }
        }
        JournalPosition priorThrough = schemaOrigin;
        for (var cycle : prior) {
            if (work != null) {
                work.check();
            }
            require(stillOpen.contains(cycle) || inventory.terminals().containsKey(cycle), "Old cycle lacks a supported terminal");
            if (stillOpen.contains(cycle)) {
                priorThrough = cut;
            }
            else if (order.compare(inventory.terminals().get(cycle), priorThrough) > 0) {
                priorThrough = inventory.terminals().get(cycle);
            }
        }
        require(future.inventory().equals(prior), "Supplied origin inventory differs from reconstructed controls");
        JournalPosition previous = null;
        for (RawJournalPage.Entry entry : future.entries()) {
            if (work != null) {
                work.check();
            }
            require(order.compare(entry.position(), schemaOrigin) >= 0, "Today's schema must not decode rows before its origin");
            require(previous == null || order.compare(previous, entry.position()) < 0, "Prospective history is unordered or repeated");
            previous = entry.position();
            if (order.compare(entry.position(), cut) > 0) {
                continue;
            }
            var header = headers.decode(entry.data(), 0);
            As400SourceMetadata.from(header, entry.position());
            // No lifecycle projection is invented for an old clear or rollback tail.
            // Even a cycle-zero companion while an old transaction is unresolved rejects.
            if (header.getJournalCode() == 'D' || header.getJournalCode() == 'F') {
                require(prior.isEmpty() || stillOpen.isEmpty() && order.compare(entry.position(), priorThrough) > 0,
                        "Lifecycle overlaps a prior-schema transaction; requalify a later origin");
            }
            if (prior.contains(header.getCommitCycle())) {
                require(stillOpen.contains(header.getCommitCycle())
                        || order.compare(entry.position(), inventory.terminals().get(header.getCommitCycle())) <= 0,
                        "Work follows a completed prior-schema cycle");
                // This entire cycle is already processed by the exact legacy cut. Its
                // old rows are neither decoded with new schemas nor replayed as new output.
                As400SourceNormalizer.validateSource(header, entry.data(), 0, entry.position(), (file, library) -> future.definitions().stream()
                        .filter(d -> d.identity().physicalFile().equals(file) && d.identity().tableId().schema().equals(library))
                        .findFirst().map(d -> d.binding(1024 * 1024)).orElse(null));
                require(header.getJournalCode() == 'C' || header.getJournalCode() == 'R', "Unsupported prior-cycle operation");
            }
        }
        require(As400HandoffWork.sameProjection(future.entries(), ownedControls, schemaOrigin.offset(), cut.offset(), Set.of('C')),
                "Prospective and historical C projections disagree");
        validateProspectivePrefix(future, ownedControls, schemaOrigin, priorThrough);
        if (!stillOpen.isEmpty()) {
            throw new PriorEpochPending(stillOpen);
        }
        java.util.function.Predicate<RawJournalPage.Entry> included = e -> order.compare(e.position(), cut) <= 0
                && !prior.contains(headers.decode(e.data(), 0).getCommitCycle());
        var projected = work == null ? future.entries().stream().filter(included).toList() : work.select(future.entries(), included);
        require(!projected.isEmpty() && projected.stream().anyMatch(e -> e.position().equals(cut)),
                "Actual publication cut must survive the prospective projection");
        var projectedEvidence = new Evidence(projected, future.definitions(), order, future.journal(), future.fingerprint(),
                projected.get(0).position(), projected.get(0).position(), Set.of(), future.partition(), work);
        var candidate = As400HandoffReplay.prepare(projectedEvidence, cut, config);
        require(order.compare(candidate.checkpoint().replayFrom(), schemaOrigin) >= 0, "Prospective checkpoint requires an older schema");
        require(prior.isEmpty() || order.compare(candidate.checkpoint().replayFrom(), priorThrough) > 0,
                "Replay floor still overlaps prior-schema transaction history; requalify a later cut");
        return new Prepared(projectedEvidence, candidate, prior);
    }

    private static void validateProspectivePrefix(Evidence future, List<RawJournalPage.Entry> controls, JournalPosition origin, JournalPosition through) {
        var bindings = new LinkedHashMap<String, As400SourceNormalizer.TableBinding>();
        future.definitions().forEach(d -> require(bindings.put(d.identity().tableId().schema() + "/" + d.identity().physicalFile(),
                d.binding(1024 * 1024)) == null, "Duplicate prospective binding"));
        var work = future.work();
        var limits = work == null ? new As400TransactionBuffer.Limits(128, 40_000, 2048, 64 * 1024 * 1024) : work.limits().transactions();
        var headers = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID);
        try (var scratch = work == null ? null : work.transactionStore();
                var transactions = new As400TransactionBuffer(future.journal(), limits, scratch);
                var autocommit = new As400AutocommitBuffer(future.journal(), limits, transactions::usage, scratch)) {
            var normalizer = new As400SourceNormalizer(transactions, (file, library) -> bindings.get(library + "/" + file),
                    1024 * 1024, As400SourceNormalizer.SavepointPolicy.consumingTargets(), autocommit);
            var prefix = java.util.stream.Stream.concat(
                    controls.stream().filter(e -> future.order().compare(e.position(), origin) < 0 && headers.decode(e.data(), 0).getJournalCode() == 'C'),
                    future.entries().stream().filter(e -> future.order().compare(e.position(), through) <= 0)).iterator();
            while (prefix.hasNext()) {
                if (work != null) {
                    work.check();
                }
                var entry = prefix.next();
                var batch = normalizer.accept(headers.decode(entry.data(), 0), entry.data(), 0, entry.position());
                if (batch.isPresent()) {
                    if (batch.orElseThrow().transactionId().commitCycle().signum() == 0) {
                        autocommit.complete(batch.orElseThrow());
                    }
                    else {
                        transactions.completeBatch(batch.orElseThrow());
                    }
                }
            }
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
