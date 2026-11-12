/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400RecoveryTaskTest.await;
import static io.debezium.connector.db2as400.As400RecoveryTaskTest.metadata;
import static io.debezium.connector.db2as400.As400RecoveryTaskTest.properties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;

class As400InitializationTaskTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = { "heartbeat.interval.ms=-1",
            "event.processing.failure.handling.mode=warn", "event.processing.failure.handling.mode=skip" })
    void invalidStreamingConfigurationCannotEnrollPublishOrInstallHistory(String setting) throws Exception {
        try (var run = new Running(directory, null)) {
            var config = new HashMap<>(properties());
            String[] parts = setting.split("=");
            if (parts[1].equals("missing")) {
                config.remove(parts[0]);
            }
            else {
                config.put(parts[0], parts[1]);
            }
            run.started = true;
            assertThrows(Exception.class, () -> run.task.start(config));
            assertThat(run.input.heldChecks).hasValue(0);
            assertThat(run.input.materialized).isFalse();
            assertThat(Files.exists(run.input.enrollment)).isFalse();
            assertThat(Files.exists(run.input.fixture.file)).isFalse();
            assertThat(run.input.reads).hasValue(0);
            assertThat(run.input.handoffs).hasValue(0);
            assertThat(run.durable.get()).isNull();
            org.mockito.Mockito.verifyNoInteractions(run.input.setup.session);
        }
    }

    @Test
    void realTaskSnapshotRequiresBothDurableBarriersBeforeContinuousReading() throws Exception {
        try (var run = new Running(directory, null)) {
            run.start();
            var baseline = run.through("BASELINE");
            assertThat(baseline).hasSize(7);
            for (var record : baseline.subList(0, 6)) {
                var value = (Struct) record.value();
                assertThat(value.getString("op")).isEqualTo("r");
                assertThat(value.getStruct("source").get("snapshot").toString()).isEqualTo(record == baseline.get(5) ? "last" : "true");
                assertThat(((Struct) record.key()).getInt32("ID")).isEqualTo(value.getStruct("after").getInt32("ID"));
                assertThat(record.sourceOffset()).doesNotContainKey("replay_from.sequence");
            }
            assertThat(Files.exists(run.input.fixture.file)).isFalse();
            assertThat(run.input.reads).hasValue(0);
            run.producerAcks(baseline);
            assertThat(Files.exists(run.input.fixture.file)).isFalse();
            run.commit(baseline);
            var initial = run.through("INITIAL");
            Map<String, ?> cp = initial.get(0).sourceOffset();
            assertThat(cp.get("replay_from.sequence")).isEqualTo("29");
            assertThat(cp.get("replay_from.inclusive")).isEqualTo("false");
            assertThat(cp.get("completed_through.sequence")).isEqualTo("29");
            assertThat(run.input.fixture.history().recover(cp, run.input.journalIdentity(), Fixture.POLICY).initialization()).isPresent();
            assertThat(run.input.heldSession).isTrue();
            assertThat(run.input.reads).hasValue(0);
            run.producerAcks(initial);
            assertThat(run.input.handoffs).hasValue(0);
            run.commit(initial);
            var streaming = run.throughCheckpoint("42");
            assertThat(As400RecoveryTestSupport.data(streaming)).containsExactly("c:DCOC1107.A:8");
            assertThat(run.input.handoffs).hasValue(1);
            assertThat(run.input.heldSession).isFalse();
            run.ack(streaming);
            await(() -> run.input.reads.get() > 1);
            assertThat(run.durable.get().get("replay_from.sequence")).isEqualTo("31");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "partial", "baseline", "history", "checkpoint" })
    void freshTaskAfterCrashOnlyResumesFullyDurableCheckpoint(String stage) throws Exception {
        Map<String, ?> saved;
        try (var run = new Running(directory, null)) {
            run.input.hold = "handoff";
            run.start();
            if (stage.equals("partial")) {
                run.ack(run.next());
            }
            else {
                run.ack(run.through("BASELINE"));
                if (!stage.equals("baseline")) {
                    var initial = run.through("INITIAL");
                    if (stage.equals("checkpoint")) {
                        run.ack(initial);
                        assertThat(run.input.held.await(10, TimeUnit.SECONDS)).isTrue();
                    }
                }
            }
            saved = run.durable.get();
            assertThat(run.input.reads).hasValue(0);
        }
        if (stage.equals("checkpoint")) {
            var baseline = saved.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
            try (var replacement = new As400RecoveryTaskTest.Running(directory, baseline, saved)) {
                replacement.input.tail = 64;
                replacement.start();
                var records = replacement.throughCheckpoint("59");
                assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:DCOC1107.A:8", "t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
                replacement.ack(records);
            }
        }
        else {
            try (var replacement = new Running(directory, saved)) {
                assertThrows(Exception.class, replacement::start);
                assertThat(replacement.input.reads).hasValue(0);
                assertThat(replacement.durable.get()).isEqualTo(saved);
            }
        }
        // Even deleting offsets cannot consume the same first-start enrollment again.
        try (var lost = new Running(directory, null)) {
            lost.start();
            assertThrows(Exception.class, lost::next);
            assertThat(lost.input.reads).hasValue(0);
            assertThat(lost.durable.get()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "enroll", "prepare", "checkpoint", "handoff" })
    void stopDuringInitializationDoesNotReleaseSourceOrStartLateWork(String stage) throws Exception {
        try (var run = new Running(directory, null)) {
            run.input.hold = stage;
            run.start();
            if (stage.equals("checkpoint") || stage.equals("handoff")) {
                run.ack(run.through("BASELINE"));
                run.ack(run.through("INITIAL"));
            }
            assertThat(run.input.held.await(10, TimeUnit.SECONDS)).isTrue();
            var saved = run.durable.get();
            var stop = CompletableFuture.runAsync(run.task::stop);
            await(() -> run.input.stopped.get());
            assertThat(run.input.closes).hasValue(0);
            assertThat(stop).isNotDone();
            // Independent replacement can progress while the old native operation is held.
            if (saved != null && saved.containsKey("capture_fingerprint")) {
                var baseline = saved.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
                try (var replacement = new As400RecoveryTaskTest.Running(directory, baseline, saved)) {
                    replacement.start();
                    replacement.ack(replacement.throughCheckpoint("42"));
                }
            }
            run.input.release.countDown();
            stop.get(15, TimeUnit.SECONDS);
            assertThat(run.input.closes).hasValue(1);
            assertThat(run.input.reads).hasValue(0);
            assertThat(run.input.handoffs).hasValue(0);
            assertThat(run.input.heldSession).isTrue();
            assertThat(run.durable.get()).isEqualTo(saved);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "session", "epoch", "readback", "missing-callback", "null-metadata" })
    void acknowledgementOrSessionFailureNeverInstallsCheckpoint(String fault) throws Exception {
        try (var run = new Running(directory, null)) {
            run.start();
            var baseline = run.through("BASELINE");
            if (fault.equals("session")) {
                run.input.heldSession.set(false);
            }
            if (fault.equals("epoch")) {
                run.input.epoch = "changed";
            }
            if (fault.equals("session") || fault.equals("epoch") || fault.equals("null-metadata")) {
                assertThrows(Exception.class, () -> run.task.commitRecord(baseline.get(0), fault.equals("null-metadata") ? null : metadata(baseline.get(0))));
            }
            else if (fault.equals("readback")) {
                run.producerAcks(baseline);
                run.task.commit();
                assertThrows(Exception.class, run::next);
            }
            else {
                run.task.commitRecord(baseline.get(6), metadata(baseline.get(6)));
                run.task.commit();
                assertThrows(Exception.class, run::next);
            }
            assertThat(run.input.reads).hasValue(0);
            assertThat(Files.exists(run.input.fixture.file)).isFalse();
            assertThat(run.durable.get()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "type", "projection", "null-key", "size" })
    void invalidBaselineFailsBeforeAnyPublication(String fault) throws Exception {
        try (var run = new Running(directory, null)) {
            run.input.rowFault = fault;
            run.start();
            assertThrows(Exception.class, run::next);
            assertThat(run.durable.get()).isNull();
            assertThat(Files.exists(run.input.fixture.file)).isFalse();
            assertThat(run.input.reads).hasValue(0);
        }
    }

    @Test
    void concurrentCommitCannotObserveHalfCompletedSessionRelease() throws Exception {
        try (var run = new Running(directory, null)) {
            run.input.hold = "released";
            run.start();
            run.ack(run.through("BASELINE"));
            run.ack(run.through("INITIAL"));
            assertThat(run.input.held.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(run.input.heldSession).isFalse();
            AtomicReference<Thread> callbackThread = new AtomicReference<>();
            var callback = CompletableFuture.runAsync(() -> {
                callbackThread.set(Thread.currentThread());
                try {
                    run.task.commit();
                }
                catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
            await(() -> callback.isDone() || callbackThread.get() != null && callbackThread.get().getState() == Thread.State.BLOCKED);
            assertThat(callback).isNotDone();
            run.input.release.countDown();
            callback.get(10, TimeUnit.SECONDS);
            run.ack(run.throughCheckpoint("42"));
            assertThat(run.input.handoffs).hasValue(1);
            assertThat(run.input.stopped).isFalse();
        }
    }

    @Test
    void emptyBaselineStillRequiresAcknowledgedBarrier() throws Exception {
        try (var run = new Running(directory, null)) {
            run.input.rowFault = "empty";
            run.start();
            var baseline = run.through("BASELINE");
            assertThat(baseline).hasSize(1);
            run.ack(baseline);
            run.ack(run.through("INITIAL"));
            run.ack(run.throughCheckpoint("42"));
        }
    }

    static class Running implements AutoCloseable {
        final As400InitializationTaskFixture input;
        final As400ConnectorTask task;
        final AtomicReference<Map<String, ?>> durable;
        boolean started;

        Running(Path directory, Map<String, ?> saved) throws Exception {
            this(directory, saved, input -> input);
        }

        Running(Path directory, Map<String, ?> saved, java.util.function.Function<As400InitializationTaskFixture, As400TaskInitialization> admission) throws Exception {
            this(new As400InitializationTaskFixture(directory), saved, admission);
        }

        Running(As400InitializationTaskFixture input, Map<String, ?> saved,
                java.util.function.Function<As400InitializationTaskFixture, As400TaskInitialization> admission)
                throws Exception {
            this.input = input;
            var initialization = admission.apply(input);
            task = new As400ConnectorTask() {
                @Override
                As400TaskInitialization initializationForTesting(Configuration config) {
                    return initialization;
                }
            };
            durable = new AtomicReference<>(saved);
            var reader = mock(OffsetStorageReader.class);
            when(reader.offset(any())).thenAnswer(i -> durable.get());
            when(reader.offsets(any())).thenAnswer(i -> {
                Map<Map<String, ?>, Map<String, ?>> offsets = new HashMap<>();
                offsets.put(input.partition().getSourcePartition(), durable.get());
                return offsets;
            });
            var context = mock(SourceTaskContext.class);
            when(context.offsetStorageReader()).thenReturn(reader);
            task.initialize(context);
        }

        void start() {
            started = true;
            task.start(properties());
        }

        List<SourceRecord> next() throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (System.nanoTime() < deadline) {
                var records = task.poll();
                if (!records.isEmpty()) {
                    return records;
                }
            }
            throw new AssertionError("No task output");
        }

        List<SourceRecord> through(String marker) throws Exception {
            List<SourceRecord> result = new ArrayList<>();
            do {
                result.addAll(next());
            } while (!marker.equals(result.get(result.size() - 1).value()));
            return result;
        }

        List<SourceRecord> throughCheckpoint(String sequence) throws Exception {
            List<SourceRecord> result = new ArrayList<>();
            do {
                result.addAll(next());
            } while (!sequence.equals(result.get(result.size() - 1).sourceOffset().get("completed_through.sequence"))
                    || !result.get(result.size() - 1).topic().startsWith("__debezium-heartbeat"));
            return result;
        }

        void producerAcks(List<SourceRecord> records) throws Exception {
            for (var record : records) {
                task.commitRecord(record, metadata(record));
            }
        }

        void commit(List<SourceRecord> records) throws Exception {
            durable.set(records.get(records.size() - 1).sourceOffset());
            task.commit();
        }

        void ack(List<SourceRecord> records) throws Exception {
            producerAcks(records);
            commit(records);
        }

        @Override
        public void close() {
            input.release.countDown();
            if (started) {
                task.stop();
            }
        }
    }
}
