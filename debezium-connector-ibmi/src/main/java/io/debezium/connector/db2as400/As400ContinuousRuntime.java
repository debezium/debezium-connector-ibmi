/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/**
 * Single-owner incremental ordinary-CDC runtime used by admitted task sources.
 * Startup requires recovered history and durable F/W; there is no implicit
 * initialization. The caller can keep completed enqueue progress separate from
 * durable F/W; retention checks always use the latter. R and the live buffer
 * survive idle polls/receiver extension, never restart.
 */
final class As400ContinuousRuntime implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(As400ContinuousRuntime.class);

    record Limits(As400TransactionBuffer.Limits transactions, As400JournalLifecycle.Limits lifecycle, int outputRecords, boolean deferAttachmentRaces,
            As400SpillStore.Settings storage) {
        Limits(As400TransactionBuffer.Limits transactions, As400JournalLifecycle.Limits lifecycle, int outputRecords, boolean deferAttachmentRaces) {
            this(transactions, lifecycle, outputRecords, deferAttachmentRaces, null);
        }

        Limits(As400TransactionBuffer.Limits transactions, As400JournalLifecycle.Limits lifecycle, int outputRecords) {
            this(transactions, lifecycle, outputRecords, false);
        }

        static Limits experimental() {
            return new Limits(new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000),
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), 1000);
        }
    }

    interface Intervals {
        As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception;

        default As400SchemaChange schemaChange(As400BindingDefinition before, RawJournalPage.Entry opening,
                                               As400SourceNormalizer.TableResolver verifiedBindings)
                throws Exception {
            throw new IllegalArgumentException("This source has no qualified current-definition refresh");
        }
    }

    record Progress(Map<String, String> checkpoint, Optional<JournalPosition> scan, int entries, int retries, boolean deferred) {
        Progress {
            checkpoint = Map.copyOf(checkpoint);
        }
    }

    private final Limits limits;
    private final Thread owner = Thread.currentThread();
    private final JournalReadLifecycle admission = new JournalReadLifecycle();
    private final As400BindingHistory.Recovered history;
    private final Intervals intervals;
    private final As400ReplayCoordinator.RecordFactory dispatcher;
    private final As400ReplayCoordinator.RecordSink sink;
    private final Map<String, ?> partition;
    private final Runnable cancel;
    private final java.util.function.Consumer<As400BindingDefinition> schemaChanged;
    private Map<String, String> expected;
    private As400Checkpoint lastDurable;
    private final java.util.List<JournalPosition> startupAnchors;
    private As400JournalOrder order;
    private As400ReplayCoordinator replay;
    private As400SpillStore spill;
    private As400SchemaTimeline schemas;
    private RawJournalPage.Entry scan;
    private boolean closed;

    As400ContinuousRuntime(Map<String, ?> durable, As400BindingHistory.Recovered history, Intervals intervals,
                           As400ReplayCoordinator.RecordFactory dispatcher, As400ReplayCoordinator.RecordSink sink,
                           Map<String, ?> partition, Runnable cancel) {
        this(durable, history, intervals, dispatcher, sink, partition, cancel, Limits.experimental());
    }

    As400ContinuousRuntime(Map<String, ?> durable, As400BindingHistory.Recovered history, Intervals intervals,
                           As400ReplayCoordinator.RecordFactory dispatcher, As400ReplayCoordinator.RecordSink sink,
                           Map<String, ?> partition, Runnable cancel, Limits limits) {
        this(durable, history, intervals, dispatcher, sink, partition, cancel, limits, null);
    }

    As400ContinuousRuntime(Map<String, ?> durable, As400BindingHistory.Recovered history, Intervals intervals,
                           As400ReplayCoordinator.RecordFactory dispatcher, As400ReplayCoordinator.RecordSink sink,
                           Map<String, ?> partition, Runnable cancel, Limits limits, java.util.function.Consumer<As400BindingDefinition> schemaChanged) {
        this.limits = Objects.requireNonNull(limits);
        this.history = Objects.requireNonNull(history);
        this.intervals = Objects.requireNonNull(intervals);
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.sink = Objects.requireNonNull(sink);
        this.partition = Map.copyOf(partition);
        this.cancel = Objects.requireNonNull(cancel);
        this.schemaChanged = schemaChanged;
        var loaded = As400Checkpoint.fromOffset(durable, history.journalIdentity(), history.fingerprint());
        expected = loaded.toOffset();
        lastDurable = loaded;
        startupAnchors = java.util.List.of(loaded.replayFrom(), loaded.completedThrough());
    }

    Progress poll(Map<String, ?> durable) throws Exception {
        return poll(durable, durable);
    }

    /** Continue this live instance from completed enqueue progress, retaining the older durable recovery floor. */
    Progress poll(Map<String, ?> published, Map<String, ?> durable) throws Exception {
        requireOwner();
        try (var lease = admission.enter()) {
            if (!expected.equals(published)) {
                throw new IllegalArgumentException("Poll requires the exact completed enqueue checkpoint; R is not a restart checkpoint");
            }
            var saved = As400Checkpoint.fromOffset(durable, history.journalIdentity(), history.fingerprint());
            var completed = As400Checkpoint.fromOffset(published, history.journalIdentity(), history.fingerprint());
            if (order == null ? !published.equals(durable)
                    : order.compare(saved.completedThrough(), completed.completedThrough()) > 0
                            || order.compare(saved.completedThrough(), lastDurable.completedThrough()) < 0
                            || order.compare(saved.replayFrom(), lastDurable.replayFrom()) < 0) {
                throw new IllegalArgumentException("Durable recovery positions regressed or exceed completed enqueue progress");
            }
            As400OperatorCutReader.Cut cut = null;
            int retries = 0;
            boolean incompleteObservation = false;
            while (cut == null) {
                admission.checkOpen();
                try {
                    cut = intervals.read(durable, Optional.ofNullable(scan));
                }
                catch (As400OperatorJournalEvidence.IncompleteObservationException incomplete) {
                    incompleteObservation = true;
                    if (incomplete.getSuppressed().length > 0 || ++retries > 2) {
                        throw incomplete;
                    }
                    // Reopen and revalidate from the same scan and durable F/W.
                    // Unlike attachment races, repeated partial observations may
                    // indicate a permanent authority problem and cannot defer.
                    LOGGER.warn("Discarded incomplete journal observation (SQLSTATE {}, code {}); recollecting interval, attempt {} of 2",
                            incomplete.getSQLState(), incomplete.getErrorCode(), retries);
                }
                catch (As400OperatorCutReader.UnstableCutException race) {
                    if (race.getSuppressed().length > 0) {
                        throw race; // An uncertain connection/resource close cannot authorize another read.
                    }
                    if (++retries > 2) {
                        if (incompleteObservation || !limits.deferAttachmentRaces()) {
                            throw race;
                        }
                        admission.checkOpen();
                        // All raced cuts were discarded before any entry escaped.
                        // Yield to the coordinator; neither R nor F/W changes.
                        return new Progress(expected, replay == null ? Optional.empty() : replay.scanPosition(), 0, retries, true);
                    }
                }
            }
            admission.checkOpen();
            if (!cut.checkpoint().toOffset().equals(durable) || cut.historicalBindings().orElseThrow() != history || !cut.tables().isEmpty()) {
                throw new IllegalArgumentException("Interval changed the admitted checkpoint or immutable history");
            }
            if (replay == null) {
                order = cut.order();
                if (limits.storage() != null) {
                    spill = new As400SpillStore(limits.storage());
                }
                var tables = history.resolver(65536);
                if (history.schemaHistory() != null && schemaChanged != null) {
                    schemas = new As400SchemaTimeline(history.schemaHistory(), history.definitions(), completed, order,
                            intervals::schemaChange, schemaChanged, spill);
                    tables = schemas;
                }
                else if (!completed.schemaState().equals(As400SchemaState.EMPTY)) {
                    throw new IllegalArgumentException("Schema checkpoint has no admitted history timeline");
                }
                replay = new As400ReplayCoordinator(durable, history.journalIdentity(), history.fingerprint(), order,
                        limits.transactions(), 65536, limits.outputRecords(), selectedTables(tables),
                        As400SourceNormalizer.SavepointPolicy.consumingTargets(), partition, record -> {
                            admission.checkOpen();
                            boolean accepted = sink.enqueue(record);
                            admission.checkOpen();
                            return accepted;
                        }, limits.lifecycle(), true, spill);
            }
            else {
                // The interval already verified every retained receiver. Bound
                // live ordering to that observed chain plus startup anchors,
                // rather than rejecting a long-running transaction at receiver 129.
                order.refreshVerified(cut.order(), cut.checkpoint().replayFrom(), startupAnchors, cut.order().receiverCount());
            }
            // No snapshot work may change state while F/W reconstruction is
            // still replaying an older prefix. All signal publication belongs
            // to this same source thread and conservative checkpoint boundary.
            if (dispatcher.hasSignalWork() && !replay.replaying()) {
                replay.deliverSignals(dispatcher);
            }
            int processed = 0;
            for (var entry : cut.entries()) {
                admission.checkOpen();
                if (schemas != null) {
                    try {
                        schemas.before(entry);
                    }
                    catch (As400SchemaTimeline.PendingChangeException pending) {
                        expected = replay.checkpoint().toOffset();
                        lastDurable = saved;
                        return new Progress(expected, replay.scanPosition(), processed, retries, true);
                    }
                }
                if (replay.accept(entry.header(), entry.data(), 0, entry.position()).isPresent()) {
                    replay.deliver(dispatcher);
                }
                admission.checkOpen();
                scan = entry;
                processed++;
            }
            replay.verifyOrdinaryInterval(cut.complete());
            admission.checkOpen();
            expected = replay.checkpoint().toOffset();
            lastDurable = saved;
            return new Progress(expected, replay.scanPosition(), cut.entries().size(), retries, false);
        }
        catch (Exception failure) {
            admission.requestStop();
            if (replay != null) {
                replay.close();
            }
            if (spill != null) {
                try {
                    spill.close();
                }
                catch (RuntimeException cleanup) {
                    failure.addSuppressed(cleanup);
                }
            }
            throw failure;
        }
    }

    private As400SourceNormalizer.TableResolver selectedTables(As400SourceNormalizer.TableResolver retained) {
        return new As400SourceNormalizer.TableResolver() {
            @Override
            public As400SourceNormalizer.TableBinding resolve(String file, String library) throws Exception {
                var binding = retained.resolve(file, library);
                return dispatcher.needsValues(binding.identity().tableId()) ? binding
                        : new As400SourceNormalizer.TableBinding(binding.identity(), null, false, true, binding.members(), binding.excludedLob());
            }

            @Override
            public boolean allowsSchemaChange(JournalPosition opening) {
                return retained.allowsSchemaChange(opening);
            }

            @Override
            public boolean creationLifecycle(RawJournalPage.Entry entry) throws Exception {
                return retained.creationLifecycle(entry);
            }

            @Override
            public void schemaChange(RawJournalPage.Entry entry) throws Exception {
                retained.schemaChange(entry);
            }

            @Override
            public As400Checkpoint schemaCheckpoint(As400Checkpoint checkpoint) {
                return retained.schemaCheckpoint(checkpoint);
            }
        };
    }

    long[] bufferUsage() {
        requireOwner();
        return replay == null ? new long[]{ 0, 0 } : replay.bufferUsage();
    }

    long oldestTransactionTimestamp() {
        requireOwner();
        return replay == null ? -1 : replay.oldestTransactionTimestamp();
    }

    /** Publish the dispatcher heartbeat with the replay coordinator's conservative F/W. */
    boolean publishCheckpoint(As400BatchDispatcher output, boolean force) throws InterruptedException, java.io.IOException {
        requireOwner();
        try (var lease = admission.enter()) {
            if (replay == null) {
                throw new IllegalStateException("No verified replay interval");
            }
            return output.publishCheckpoint(replay, force);
        }
    }

    /** Nonblocking request; a blocked admitted read is not a completed drain. */
    void requestStop() {
        admission.requestStop();
        cancel.run();
    }

    void awaitDrained() {
        admission.awaitDrained();
    }

    private void requireOwner() {
        if (Thread.currentThread() != owner || closed) {
            throw new IllegalStateException("Continuous runtime is closed or called by another owner");
        }
    }

    @Override
    public void close() {
        requireOwner();
        admission.requestStop();
        admission.awaitDrained();
        if (replay != null) {
            replay.close();
        }
        if (spill != null) {
            spill.close();
        }
        closed = true;
    }
}
