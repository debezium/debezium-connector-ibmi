/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;

class As400RecoveryTaskTest {
    @TempDir
    Path directory;

    @Test
    void actualTaskResumesAndWaitsForDurableCallbacksBeforeReadingAgain() throws Exception {
        Map<String, String> initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        try (var first = new Running(directory, initial, initial)) {
            first.start();
            List<SourceRecord> batch = first.throughCheckpoint("42");
            assertThat(As400RecoveryTestSupport.data(batch)).containsExactly("c:DCOC1107.A:8");
            assertThat(first.input.reads).hasValue(1);
            assertThat(first.durable.get()).isEqualTo(initial);
            SourceRecord barrier = batch.get(batch.size() - 1);
            assertThat(barrier.sourceOffset().get("replay_from.sequence")).isEqualTo("31");
            assertThat(barrier.sourceOffset()).doesNotContainKey("scan.sequence");
            first.producerAcks(batch);
            // A producer callback alone cannot release the reader or the coordinator context.
            assertThat(first.task.recoveredOffset()).isEqualTo(initial);
            assertThat(first.input.reads).hasValue(1);
            first.durable.set(barrier.sourceOffset());
            first.task.commit();
            await(() -> first.input.reads.get() > 1);
            assertThat(first.task.recoveredOffset()).isEqualTo(first.durable.get());
            try (var replacement = new Running(directory, initial, first.durable.get())) {
                replacement.input.tail = 64;
                replacement.start();
                var rest = replacement.throughCheckpoint("59");
                assertThat(As400RecoveryTestSupport.data(rest)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
                replacement.ack(rest);
                await(() -> replacement.input.reads.get() > 1);
                assertThat(replacement.durable.get().get("completed_through.sequence")).isEqualTo("59");
            }
        }
    }

    @Test
    void interruptedClearBatchReplaysWholeBatchFromPreviousDurableWInFreshTask() throws Exception {
        Map<String, String> initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        Map<String, ?> saved;
        try (var first = new Running(directory, initial, initial)) {
            first.input.tail = 44;
            first.start();
            List<SourceRecord> prefix = new ArrayList<>();
            do {
                prefix.addAll(first.next());
            } while (!As400RecoveryTestSupport.data(prefix).contains("t:DCOC1107.B"));
            first.ack(prefix);
            saved = first.durable.get();
            assertThat(saved.get("completed_through.sequence")).isEqualTo("42");
            assertThat(saved.get("replay_from.sequence")).isEqualTo("31");
        }
        try (var replacement = new Running(directory, initial, saved)) {
            replacement.input.tail = 64;
            replacement.start();
            var replay = replacement.throughCheckpoint("59");
            assertThat(As400RecoveryTestSupport.data(replay)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            replacement.ack(replay);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "legacy", "fingerprint", "scan", "journal" })
    void invalidOffsetsFailBeforeSourceWorkAndCannotReinitialize(String fault) throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        Map<String, Object> bad = new HashMap<>(initial);
        switch (fault) {
            case "legacy" -> bad = new HashMap<>(Map.of("sequence", "29"));
            case "fingerprint" -> bad.put("capture_fingerprint", "v1:" + "0".repeat(64));
            case "scan" -> bad.put("scan.sequence", "64");
            case "journal" -> bad.put("journal_identity", "replaced");
        }
        byte[] history = Files.readAllBytes(directory.resolve("history.jsonl"));
        try (var run = new Running(directory, initial, fault.equals("missing") ? null : bad)) {
            assertThrows(Exception.class, run::start);
            assertThat(run.input.recoveries).hasValue(0);
            assertThat(run.input.reads).hasValue(0);
            assertThat(run.input.closes).hasValue(1);
        }
        assertThat(Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "malformed", "uninitialized" })
    void historyFailureReachesActualTaskPollWithZeroOutput(String fault) throws Exception {
        Fixture fixture = new Fixture(directory);
        Map<String, String> initial = fault.equals("uninitialized") ? fixture.initialize() : As400RecoveryTaskFixture.initialize(fixture);
        if (fault.equals("missing")) {
            Files.delete(fixture.file);
        }
        if (fault.equals("malformed")) {
            Files.writeString(fixture.file, "bad history\n");
        }
        try (var run = new Running(directory, initial, initial)) {
            run.start();
            await(() -> run.input.stopped.get());
            assertThrows(Exception.class, run::next);
            assertThat(run.input.reads).hasValue(0);
            assertThat(run.durable.get()).isEqualTo(initial);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-readback", "dropped-record", "wrong-topic", "wrong-partition", "changed-session", "skipped-prefix" })
    void uncertainAcknowledgementFailsClosedAtActualTaskBoundary(String fault) throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        try (var run = new Running(directory, initial, initial)) {
            run.start();
            var batch = run.throughCheckpoint("42");
            if (fault.equals("skipped-prefix")) {
                run.producerAcks(batch.subList(1, batch.size()));
                run.task.commit();
            }
            else if (fault.equals("missing-readback")) {
                run.producerAcks(batch);
                run.task.commit();
            }
            else {
                if (fault.equals("changed-session")) {
                    run.input.qualified.set(false);
                }
                var record = batch.get(0);
                RecordMetadata metadata = fault.equals("dropped-record") ? null
                        : new RecordMetadata(
                                new TopicPartition(fault.equals("wrong-topic") ? "wrong" : record.topic(), fault.equals("wrong-partition") ? 1 : 0), 0, 0, 0, 1, 1);
                assertThrows(Exception.class, () -> run.task.commitRecord(record, metadata));
            }
            await(() -> run.input.stopped.get());
            assertThrows(Exception.class, run::next);
            assertThat(run.input.reads).hasValue(1);
            assertThat(run.durable.get()).isEqualTo(initial);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "history", "read", "close" })
    void stopRequestIsDistinctFromCompletedDrainAndReplacementProgresses(String stage) throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        try (var old = new Running(directory, initial, initial)) {
            old.input.hold = stage;
            old.start();
            List<SourceRecord> records = stage.equals("close") ? old.throughCheckpoint("42") : List.of();
            if (!stage.equals("close")) {
                assertThat(old.input.held.await(5, TimeUnit.SECONDS)).isTrue();
            }
            var stop = CompletableFuture.runAsync(old.task::stop);
            try {
                await(() -> old.input.stopped.get());
                assertThat(old.task.recoveryDrained()).isFalse();
                assertThat(stop.isDone()).isFalse();
                try (var replacement = new Running(directory, initial, initial)) {
                    replacement.input.tail = 64;
                    replacement.start();
                    var output = replacement.throughCheckpoint("59");
                    assertThat(As400RecoveryTestSupport.data(output)).containsExactly("c:DCOC1107.A:8", "t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
                    replacement.ack(output);
                    await(() -> replacement.input.reads.get() > 1);
                    assertThat(replacement.durable.get().get("completed_through.sequence")).isEqualTo("59");
                }
            }
            finally {
                old.input.release.countDown();
                stop.get(10, TimeUnit.SECONDS);
            }
            assertThat(old.task.recoveryDrained()).isTrue();
            assertThat(old.input.closes).hasValue(1);
            assertThat(old.durable.get()).isEqualTo(initial);
            assertThat(old.task.poll()).isEmpty();
            if (!records.isEmpty()) {
                assertThrows(Exception.class, () -> old.task.commitRecord(records.get(0), metadata(records.get(0))));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "read", "close" })
    void shouldRejectPublicationAndRetainFwWhenSourceDrainDeadlineExpires(String stage) throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        try (var old = new Running(directory, initial, initial)) {
            old.input.hold = stage;
            old.start();
            if (stage.equals("close")) {
                old.throughCheckpoint("42");
            }
            else {
                assertThat(old.input.held.await(5, TimeUnit.SECONDS)).isTrue();
            }
            try {
                var source = old.task.source();
                assertThat(assertThrows(IllegalStateException.class, () -> source.closeWithin(Duration.ofMillis(20))))
                        .hasMessageContaining("deadline exceeded").hasMessageContaining("drain unverified");
                assertThat(source.stopped()).isTrue();
                assertThat(source.drained()).isFalse();
                assertThrows(IllegalStateException.class, source::checkForPublication);
                assertThat(old.durable.get()).isEqualTo(initial);
                try (var replacement = new Running(directory, initial, initial)) {
                    replacement.start();
                    replacement.ack(replacement.throughCheckpoint("42"));
                    assertThat(replacement.durable.get().get("completed_through.sequence")).isEqualTo("42");
                }
            }
            finally {
                old.input.release.countDown();
                old.task.source().closeWithin(Duration.ofSeconds(5));
            }
            assertThat(old.task.recoveryDrained()).isTrue();
            assertThat(old.input.closes).hasValue(1);
            assertThat(old.durable.get()).isEqualTo(initial);
        }
    }

    @Test
    void publicTaskHasNoRecoveryAdmissionAndLegacyLoaderStillRejectsFw() throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        var config = Configuration.from(properties());
        assertThat(new As400ConnectorTask().recoveryForTesting(config)).isNull();
        assertThrows(Exception.class, () -> new As400OffsetContext.Loader(new As400ConnectorConfig(config)).load(initial));
    }

    @Test
    void unexpectedReaderInterruptionFailsTaskInsteadOfSilentlyLeavingItRunning() throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        try (var run = new Running(directory, initial, initial)) {
            run.input.failure = "read-interrupted";
            run.start();
            var failure = assertThrows(org.apache.kafka.connect.errors.ConnectException.class, run::next);
            assertThat(failure).isNotInstanceOf(org.apache.kafka.connect.errors.RetriableException.class);
            assertThat(run.input.reads).hasValue(1);
            assertThat(run.durable.get()).isEqualTo(initial);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "cancel", "close" })
    void cancellationOrCleanupFailureCannotClaimSuccessfulDrain(String fault) throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        var run = new Running(directory, initial, initial);
        run.start();
        try {
            run.throughCheckpoint("42");
        }
        finally {
            run.input.failure = fault;
            assertThrows(Exception.class, run.task::stop);
        }
        assertThat(run.task.recoveryDrained()).isFalse();
        assertThat(run.input.closes).hasValue(1);
        assertThat(run.durable.get()).isEqualTo(initial);
    }

    @ParameterizedTest
    @ValueSource(strings = { "exactly.once.support", "transaction.boundary", "errors.tolerance", "transforms", "event.converting.failure.handling.mode",
            "signal.enabled.channels" })
    void unsupportedTaskProfileRejectsBeforeHistoryOrSourceWork(String key) throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        try (var run = new Running(directory, initial, initial)) {
            var properties = new HashMap<>(properties());
            properties.put(key, "unqualified");
            run.started = true;
            assertThrows(Exception.class, () -> run.task.start(properties));
            assertThat(run.input.recoveries).hasValue(0);
            assertThat(run.input.reads).hasValue(0);
            assertThat(run.durable.get()).isEqualTo(initial);
        }
    }

    static Map<String, String> properties() {
        return Configuration.create().with("name", "local-recovery").with("topic.prefix", "test")
                .with("database.hostname", "no-live-ibmi.invalid").with("database.user", "fixture").with("database.password", "fixture")
                .with("database.dbname", "DB").with("database.schema", "DCOC1107").with("skipped.operations", "none")
                .with("max.batch.size", 1).with("max.queue.size", 64).with("poll.interval.ms", 10).with("heartbeat.interval.ms", 3600000)
                .with("event.converting.failure.handling.mode", "fail").with("provide.transaction.metadata", true)
                .with("exactly.once.support", "requested").with("transaction.boundary", "poll").with("errors.tolerance", "none")
                .with("custom.metric.tags", "test=" + UUID.randomUUID()).build().asMap();
    }

    static RecordMetadata metadata(SourceRecord record) {
        return new RecordMetadata(new TopicPartition(record.topic(), 0), 1, 0, 0, 1, 1);
    }

    static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("Condition did not become true");
            }
            Thread.sleep(5);
        }
    }

    static class AdmittedTask extends As400ConnectorTask {
        final As400TaskRecovery input;

        AdmittedTask(As400TaskRecovery input) {
            this.input = input;
        }

        As400RecoverySource source() {
            return (As400RecoverySource) coordinator.getStreamingSource().orElseThrow();
        }

        Map<String, ?> recoveredOffset() {
            return coordinator.getStreamingSource().orElseThrow().getOffsetContext().getOffset();
        }

        @Override
        As400TaskRecovery recoveryForTesting(Configuration config) {
            return input;
        }
    }

    static class Running implements AutoCloseable {
        final As400RecoveryTaskFixture input;
        final AdmittedTask task;
        final AtomicReference<Map<String, ?>> durable;
        final SourceTaskContext context;
        boolean started;

        Running(Path directory, Map<String, String> baseline, Map<String, ?> saved) throws Exception {
            this(new As400RecoveryTaskFixture(directory, baseline.get("capture_fingerprint")), saved);
        }

        Running(As400RecoveryTaskFixture input, Map<String, ?> saved) {
            this.input = input;
            task = new AdmittedTask(input);
            durable = new AtomicReference<>(saved);
            var reader = mock(OffsetStorageReader.class);
            when(reader.offset(any())).thenAnswer(i -> durable.get());
            when(reader.offsets(any())).thenAnswer(i -> {
                Map<Map<String, ?>, Map<String, ?>> offsets = new HashMap<>();
                offsets.put(input.partition().getSourcePartition(), durable.get());
                return offsets;
            });
            context = mock(SourceTaskContext.class);
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

        List<SourceRecord> throughCheckpoint(String completed) throws Exception {
            List<SourceRecord> records = new ArrayList<>();
            while (true) {
                var batch = next();
                records.addAll(batch);
                if (batch.stream().anyMatch(r -> r.topic().startsWith("__debezium-heartbeat")
                        && completed.equals(r.sourceOffset().get("completed_through.sequence")))) {
                    return records;
                }
            }
        }

        void producerAcks(List<SourceRecord> records) throws Exception {
            for (var record : records) {
                task.commitRecord(record, metadata(record));
            }
        }

        void ack(List<SourceRecord> records) throws Exception {
            producerAcks(records);
            durable.set(records.get(records.size() - 1).sourceOffset());
            task.commit();
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
