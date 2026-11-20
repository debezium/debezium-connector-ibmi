/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/**
 * Reconstruct the affected tables of a clear encountered by the legacy prefix.
 * The caller supplies complete, identity-checked native history and definitions
 * proved valid for that interval. This class neither publishes nor grants a
 * startup boundary. Other tables retain their existing legacy publication path.
 */
final class As400LegacyClearReplay implements AutoCloseable {
    record Resolution(As400SourceMetadata terminal, JournalPosition consumedThrough, Optional<CommittedBatch> committed) {
    }

    private final BigInteger cycle;
    private final Map<String, JournalPosition> firstClears;
    private final Set<String> observed = new HashSet<>();
    private final As400JournalOrder order;
    private final As400TransactionBuffer buffer;
    private final As400AutocommitBuffer autocommit;
    private final As400SourceNormalizer normalizer;
    private final As400JournalLifecycle lifecycle;
    private JournalPosition previous;
    private JournalPosition consumedThrough;
    private CommittedBatch completed;
    private As400SourceMetadata terminal;
    private String producer;

    As400LegacyClearReplay(String journal, BigInteger cycle, JournalPosition start, Map<String, JournalPosition> firstClears,
                           Map<String, TableBinding> bindings, As400JournalOrder order, As400ContinuousRuntime.Limits limits,
                           As400SpillStore spill) {
        require(cycle.signum() > 0 && start.offset().equals(cycle) && !firstClears.isEmpty()
                && bindings.keySet().equals(firstClears.keySet()), "Complete original cycle and exact affected bindings required");
        this.cycle = cycle;
        this.firstClears = Map.copyOf(firstClears);
        this.order = order;
        firstClears.values().forEach(clear -> require(order.compare(start, clear) < 0, "Clear precedes its original cycle"));
        As400SourceNormalizer.TableResolver tables = (file, library) -> Objects.requireNonNull(bindings.get(library + "/" + file),
                "Missing deferred-clear binding");
        buffer = new As400TransactionBuffer(journal, limits.transactions(), spill);
        autocommit = new As400AutocommitBuffer(journal, limits.transactions(), buffer::usage, spill);
        normalizer = new As400SourceNormalizer(buffer, tables, 1024 * 1024,
                As400SourceNormalizer.SavepointPolicy.consumingTargets(), autocommit);
        lifecycle = new As400JournalLifecycle(limits.lifecycle(), tables, order, start, spill);
    }

    void accept(RawJournalPage.Entry entry) throws Exception {
        require(previous == null || order.compare(previous, entry.position()) < 0, "Deferred clear history is unordered or repeated");
        As400JournalRecordContent.digest(entry.data(), entry.position());
        var h = entry.header();
        if (previous == null) {
            require(h.getJournalCode() == 'C' && h.getEntryType().equals("SC")
                    && h.getCommitCycle().equals(cycle) && entry.position().offset().equals(cycle), "Missing original deferred-clear start");
            producer = As400SchemaChange.producer(entry);
        }
        previous = entry.position();
        if (consumedThrough != null) {
            return; // The caller must separately validate later schema stability.
        }
        String key = h.getLibrary() + "/" + h.getFile();
        JournalPosition first = firstClears.get(key);
        boolean control = h.getJournalCode() == 'C' && h.getCommitCycle().equals(cycle);
        if (!control && first == null) {
            return; // Its ordinary legacy dispatcher remains responsible for output.
        }
        if (!producer.equals(As400SchemaChange.producer(entry))) {
            // Each foreign clear is qualified separately by the native collector.
            // Its companions must not borrow this transaction's SC or produce
            // output in this transaction's batch.
            return;
        }
        if (first != null && h.getJournalCode() == 'F' && h.getEntryType().equals("CR")) {
            // The native collector validates its identity; the held legacy
            // prefix publishes it in source order, outside this transaction.
            return;
        }
        if (first != null && h.getJournalCode() == 'R' && order.compare(entry.position(), first) < 0) {
            // An old published prefix on this same table would need explicit
            // compensation after rollback. Never silently reinterpret it as an
            // unexposed transaction or replay a clear over that ambiguity.
            require(!h.getCommitCycle().equals(cycle), "Deferred clear has an already-published row prefix on its table");
            return;
        }
        if (first != null && entry.position().equals(first)) {
            require(h.getJournalCode() == 'D' && h.getEntryType().equals("CG") && h.getCommitCycle().equals(cycle),
                    "Deferred clear position identifies different source work");
            observed.add(key);
        }
        byte[] raw = entry.data();
        lifecycle.accept(h, raw, 0, entry.position());
        Optional<As400JournalLifecycle.Entry> ready;
        while ((ready = lifecycle.poll()).isPresent()) {
            var next = ready.orElseThrow();
            var batch = normalizer.accept(next.header(), next.bytes(), 0, next.position());
            if (batch.isPresent()) {
                require(completed == null && batch.orElseThrow().transactionId().commitCycle().equals(cycle),
                        "Foreign or repeated output during deferred clear recovery");
                completed = batch.orElseThrow();
                terminal = completed.commitSource();
            }
            else if (next.type().equals("C/RB")) {
                require(terminal == null, "Repeated deferred-clear terminal");
                terminal = As400SourceMetadata.from(next.header(), next.position());
            }
        }
        if (terminal != null && lifecycle.drained()) {
            require(observed.equals(firstClears.keySet()), "Deferred cycle ended before all held clears were reconstructed");
            consumedThrough = entry.position();
        }
    }

    Optional<Resolution> resolution() {
        return consumedThrough == null ? Optional.empty() : Optional.of(new Resolution(terminal, consumedThrough, Optional.ofNullable(completed)));
    }

    @Override
    public void close() {
        lifecycle.close();
        autocommit.close();
        buffer.close();
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
