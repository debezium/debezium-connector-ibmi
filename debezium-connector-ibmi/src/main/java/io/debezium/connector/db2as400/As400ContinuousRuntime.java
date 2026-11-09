/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/**
 * Single-owner incremental ordinary-CDC runtime used by admitted task sources.
 * Startup requires recovered history and durable F/W; there is no implicit
 * initialization. Caller acknowledges returned F/W through Connect before polling
 * again. R and the live buffer survive idle polls/receiver extension, never restart.
 */
final class As400ContinuousRuntime implements AutoCloseable {
    record Limits(As400TransactionBuffer.Limits transactions, As400JournalLifecycle.Limits lifecycle, int outputRecords, boolean deferAttachmentRaces) {
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
    private Map<String, String> expected;
    private final java.util.List<JournalPosition> startupAnchors;
    private As400JournalOrder order;
    private As400ReplayCoordinator replay;
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
        this.limits = Objects.requireNonNull(limits);
        this.history = Objects.requireNonNull(history);
        this.intervals = Objects.requireNonNull(intervals);
        this.dispatcher = Objects.requireNonNull(dispatcher);
        this.sink = Objects.requireNonNull(sink);
        this.partition = Map.copyOf(partition);
        this.cancel = Objects.requireNonNull(cancel);
        var loaded = As400Checkpoint.fromOffset(durable, history.journalIdentity(), history.fingerprint());
        expected = loaded.toOffset();
        startupAnchors = java.util.List.of(loaded.replayFrom(), loaded.completedThrough());
    }

    Progress poll(Map<String, ?> durable) throws Exception {
        requireOwner();
        try (var lease = admission.enter()) {
            if (!expected.equals(durable)) {
                throw new IllegalArgumentException("Poll requires the exact acknowledged F/W; R is not a restart checkpoint");
            }
            As400OperatorCutReader.Cut cut = null;
            int retries = 0;
            while (cut == null) {
                admission.checkOpen();
                try {
                    cut = intervals.read(durable, Optional.ofNullable(scan));
                }
                catch (As400OperatorCutReader.UnstableCutException race) {
                    if (++retries > 2) {
                        if (!limits.deferAttachmentRaces()) {
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
                replay = new As400ReplayCoordinator(durable, history.journalIdentity(), history.fingerprint(), order,
                        limits.transactions(), 65536, limits.outputRecords(), history.resolver(65536),
                        As400SourceNormalizer.SavepointPolicy.consumingTargets(), partition, record -> {
                            admission.checkOpen();
                            boolean accepted = sink.enqueue(record);
                            admission.checkOpen();
                            return accepted;
                        }, limits.lifecycle(), true);
            }
            else {
                order.refreshVerified(cut.order(), cut.checkpoint().replayFrom(), startupAnchors, 128);
            }
            for (var entry : cut.entries()) {
                admission.checkOpen();
                if (replay.accept(entry.header(), entry.data(), 0, entry.position()).isPresent()) {
                    replay.deliver(dispatcher);
                }
                admission.checkOpen();
                scan = entry;
            }
            replay.verifyOrdinaryInterval(cut.complete());
            admission.checkOpen();
            expected = replay.checkpoint().toOffset();
            return new Progress(expected, replay.scanPosition(), cut.entries().size(), retries, false);
        }
        catch (Exception failure) {
            admission.requestStop();
            if (replay != null) {
                replay.close();
            }
            throw failure;
        }
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
        closed = true;
    }
}
