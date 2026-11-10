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
            this.cycles = Set.copyOf(cycles);
        }

        Set<BigInteger> cycles() {
            return cycles;
        }
    }

    record Prepared(Evidence projection, Candidate candidate, Set<BigInteger> completedPriorCycles) {
        Prepared {
            completedPriorCycles = Set.copyOf(completedPriorCycles);
        }
    }

    static Prepared prepare(Evidence future, List<RawJournalPage.Entry> controls, JournalPosition schemaOrigin, JournalPosition cut,
                            As400ConnectorConfig config)
            throws Exception {
        require(!future.entries().isEmpty() && future.entries().size() <= 20_000 && controls.size() <= 20_000,
                "Bounded prospective rows and older controls required");
        long controlBytes = 0;
        for (RawJournalPage.Entry entry : controls) {
            require(entry.data().length <= 1024 * 1024 && (controlBytes += entry.data().length) <= 64 * 1024 * 1024,
                    "Older controls exceed byte budget");
        }
        var order = future.order();
        require(order.compare(schemaOrigin, cut) <= 0, "Cut precedes qualified schema origin");
        var headers = new EntryHeaderDecoder();
        var ownedControls = controls.stream()
                .filter(e -> order.compare(e.position(), cut) <= 0).toList();
        // A complete native C/J selection can be empty for autocommit-only
        // history. Inventory and overlapping C projections must still agree;
        // no empty list is a substitute for a successful native read.
        var controlFrom = ownedControls.isEmpty() || order.compare(ownedControls.get(0).position(), schemaOrigin) > 0
                ? schemaOrigin
                : ownedControls.get(0).position();
        var inventory = As400ControlInventory.reconstruct(ownedControls, order, future.journal(),
                controlFrom, schemaOrigin, cut);
        // The older C/J projection, not today's row decoder, establishes old-cycle lifetime.
        Set<BigInteger> prior = inventory.openAtLower().keySet();
        var stillOpen = inventory.openThrough().keySet().stream().filter(prior::contains).collect(Collectors.toSet());
        require(prior.stream().allMatch(c -> stillOpen.contains(c) || inventory.terminals().containsKey(c)), "Old cycle lacks a supported terminal");
        require(future.inventory().equals(prior), "Supplied origin inventory differs from reconstructed controls");
        var rowControls = new ArrayList<String>();
        var projected = new ArrayList<RawJournalPage.Entry>();
        JournalPosition previous = null;
        for (RawJournalPage.Entry entry : future.entries()) {
            require(order.compare(entry.position(), schemaOrigin) >= 0, "Today's schema must not decode rows before its origin");
            require(previous == null || order.compare(previous, entry.position()) < 0, "Prospective history is unordered or repeated");
            previous = entry.position();
            if (order.compare(entry.position(), cut) > 0) {
                continue;
            }
            var header = headers.decode(entry.data(), 0);
            As400SourceMetadata.from(header, entry.position());
            if (header.getJournalCode() == 'C') {
                rowControls.add(As400JournalRecordContent.digest(entry.data(), entry.position()));
            }
            // No lifecycle projection is invented for an old clear or rollback tail.
            // Even a cycle-zero companion while an old transaction is unresolved rejects.
            if (header.getJournalCode() == 'D' || header.getJournalCode() == 'F') {
                require(prior.stream().noneMatch(c -> stillOpen.contains(c) || order.compare(entry.position(), inventory.terminals().get(c)) <= 0),
                        "Lifecycle overlaps a prior-schema transaction; requalify a later origin");
            }
            if (prior.contains(header.getCommitCycle())) {
                require(stillOpen.contains(header.getCommitCycle())
                        || order.compare(entry.position(), inventory.terminals().get(header.getCommitCycle())) <= 0,
                        "Work follows a completed prior-schema cycle");
                // This entire cycle is already processed by the exact legacy cut. Its
                // old rows are neither decoded with new schemas nor replayed as new output.
                As400SourceNormalizer.validateSource(header, entry.data(), 0, entry.position());
                require(header.getJournalCode() == 'C' || header.getJournalCode() == 'R', "Unsupported prior-cycle operation");
            }
            else {
                projected.add(entry);
            }
        }
        var overlappingControls = new ArrayList<String>();
        for (RawJournalPage.Entry entry : ownedControls) {
            if (order.compare(entry.position(), schemaOrigin) >= 0 && headers.decode(entry.data(), 0).getJournalCode() == 'C') {
                overlappingControls.add(As400JournalRecordContent.digest(entry.data(), entry.position()));
            }
        }
        require(rowControls.equals(overlappingControls), "Prospective and historical C projections disagree");
        JournalPosition priorThrough = stillOpen.isEmpty() ? prior.stream().map(inventory.terminals()::get).max(order::compare).orElse(schemaOrigin) : cut;
        validateProspectivePrefix(future, ownedControls, schemaOrigin, priorThrough);
        if (!stillOpen.isEmpty()) {
            throw new PriorEpochPending(stillOpen);
        }
        require(!projected.isEmpty() && projected.stream().anyMatch(e -> e.position().equals(cut)),
                "Actual publication cut must survive the prospective projection");
        var projectedEvidence = new Evidence(projected, future.definitions(), order, future.journal(), future.fingerprint(),
                projected.get(0).position(), projected.get(0).position(), Set.of(), future.partition());
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
        var limits = new As400TransactionBuffer.Limits(128, 40_000, 2048, 64 * 1024 * 1024);
        var headers = new EntryHeaderDecoder();
        try (var transactions = new As400TransactionBuffer(future.journal(), limits);
                var autocommit = new As400AutocommitBuffer(future.journal(), limits, transactions::usage)) {
            var normalizer = new As400SourceNormalizer(transactions, (file, library) -> bindings.get(library + "/" + file),
                    1024 * 1024, As400SourceNormalizer.SavepointPolicy.consumingTargets(), autocommit);
            var prefix = new ArrayList<RawJournalPage.Entry>();
            controls.stream().filter(e -> future.order().compare(e.position(), origin) < 0 && headers.decode(e.data(), 0).getJournalCode() == 'C')
                    .forEach(prefix::add);
            future.entries().stream().filter(e -> future.order().compare(e.position(), through) <= 0).forEach(prefix::add);
            for (RawJournalPage.Entry entry : prefix) {
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
