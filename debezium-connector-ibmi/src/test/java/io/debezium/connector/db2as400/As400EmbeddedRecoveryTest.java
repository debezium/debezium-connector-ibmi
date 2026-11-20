/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400RecoveryTaskTest.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageWriter;
import org.apache.kafka.connect.util.Callback;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.config.EmbeddedWorkerConfig;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

/** Actual engine, task, dispatcher and file offsets. Only native I/O and its admission are fixtures. */
public class As400EmbeddedRecoveryTest {
    private static final Map<String, Run> RUNS = new ConcurrentHashMap<>();
    @TempDir
    Path directory;

    @ParameterizedTest
    @CsvSource({ "legacy-row,false", "legacy-barrier,false", "migration,false", "legacy-row,true", "legacy-barrier,true", "migration,true" })
    void shouldResumeEachAcknowledgedHandoffPhaseWithoutResnapshotOrKafkaTransactions(String phase, boolean routed) throws Exception {
        Map<String, String> transforms = routed ? As400TopicRoutingTest.properties() : Map.of();
        var fixture = new As400RecoveryTestSupport.Fixture(directory);
        var initial = fixture.initialize();
        seedOffset(As400RecoveryTestSupport.Fixture.legacyOffset(29));
        Map<String, ?> saved;
        try (var first = new Run(new As400LegacyActivationTest.TransitionSource(directory, initial), "after-" + phase, transforms)) {
            first.finished.get(15, TimeUnit.SECONDS);
            assertThat(first.failure.get()).isNotNull();
            saved = first.lastFlushed.get();
            assertThat(saved).containsKey(As400DeliveryOffset.SESSION);
            assertThat(saved.containsKey(As400Checkpoint.VERSION)).isEqualTo(phase.equals("migration"));
            assertThat(saved.containsKey(As400LegacyHandoff.VERSION)).isEqualTo(phase.equals("legacy-barrier"));
            assertThat(first.output).noneMatch(As400EmbeddedRecoveryTest::snapshot);
            if (!phase.equals("migration")) {
                assertThat(first.input.reads).hasValue(0);
            }
        }
        var input = new As400LegacyActivationTest.TransitionSource(directory, initial);
        input.tail = 64;
        try (var replacement = new Run(input, "", transforms)) {
            replacement.until("59");
            var observed = replacement.output;
            if (routed) {
                assertThat(observed).allSatisfy(record -> assertThat(record.topic()).startsWith("routed."));
            }
            // Compare operations on logical tables independently of the topic prefix.
            var originalTopics = observed.stream().map(record -> !routed ? record
                    : record.newRecord(record.topic().substring("routed.".length()),
                            record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp()))
                    .toList();
            assertThat(As400RecoveryTestSupport.data(originalTopics))
                    .containsExactly("c:DCOC1107.A:8", "t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            assertThat(replacement.output).noneMatch(As400EmbeddedRecoveryTest::snapshot);
            assertThat(input.handoffs).isEqualTo(phase.equals("migration") ? 0 : 1);
            assertThat(Files.readAllLines(fixture.file)).hasSize(2); // Original bindings and one idempotent activation.
            assertThat(replacement.saved().get(As400DeliveryOffset.SESSION)).isNotEqualTo(saved.get(As400DeliveryOffset.SESSION));
        }
    }

    @Test
    void shouldRetainTheLegacyBarrierWhenDurableHistoryIsUnavailableBeforeActivation() throws Exception {
        var fixture = new As400RecoveryTestSupport.Fixture(directory);
        var initial = fixture.initialize();
        byte[] history = Files.readAllBytes(fixture.file);
        seedOffset(As400RecoveryTestSupport.Fixture.legacyOffset(29));
        var input = new As400LegacyActivationTest.TransitionSource(directory, initial);
        try (var channel = java.nio.channels.FileChannel.open(fixture.file, java.nio.file.StandardOpenOption.READ, java.nio.file.StandardOpenOption.WRITE);
                var held = channel.lock();
                var run = new Run(input, "")) {
            run.finished.get(15, TimeUnit.SECONDS);
            assertThat(run.failure.get()).isNotNull();
            assertThat(run.lastFlushed.get()).containsKey(As400LegacyHandoff.VERSION).doesNotContainKey(As400Checkpoint.VERSION);
            assertThat(run.output).noneMatch(r -> "migration".equals(r.value()));
            assertThat(input.reads).hasValue(0);
            assertThat(Files.readAllBytes(fixture.file)).isEqualTo(history);
        }
        var replacement = new As400LegacyActivationTest.TransitionSource(directory, initial);
        replacement.tail = 64;
        try (var run = new Run(replacement, "")) {
            run.until("59");
            assertThat(As400RecoveryTestSupport.data(run.output))
                    .containsExactly("c:DCOC1107.A:8", "t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            assertThat(Files.readAllLines(fixture.file)).hasSize(2);
        }
    }

    @Test
    void shouldKeepHandoffPendingUntilItsDeliveredBarrierIsActuallySaved() throws Exception {
        var fixture = new As400RecoveryTestSupport.Fixture(directory);
        var initial = fixture.initialize();
        var legacy = As400RecoveryTestSupport.Fixture.legacyOffset(29);
        seedOffset(legacy);
        byte[] savedFile = Files.readAllBytes(directory.resolve("offsets.dat"));
        var input = new As400LegacyActivationTest.TransitionSource(directory, initial);
        input.tail = 64;
        try (var run = new Run(input, "offset-flush")) {
            await(() -> run.failedFlushes.get() > 0 && run.output.stream().anyMatch(r -> "legacy-barrier".equals(r.value())));
            assertThat(run.saved()).isEqualTo(legacy);
            assertThat(Files.readAllBytes(directory.resolve("offsets.dat"))).isEqualTo(savedFile);
            assertThat(Files.readAllLines(fixture.file)).hasSize(1);
            assertThat(input.recoveries).hasValue(0);
            assertThat(input.reads).hasValue(0);
            assertThat(run.output).noneMatch(r -> "migration".equals(r.value()));
            run.failFlush.set(false);
            run.until("59");
            assertThat(input.handoffs).isEqualTo(1);
            assertThat(Files.readAllLines(fixture.file)).hasSize(2);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "legacy-barrier", "migration" })
    void shouldRejectMissingHandoffHistoryOnFreshEngineBeforePublishing(String phase) throws Exception {
        var fixture = new As400RecoveryTestSupport.Fixture(directory);
        var initial = fixture.initialize();
        seedOffset(As400RecoveryTestSupport.Fixture.legacyOffset(29));
        try (var first = new Run(new As400LegacyActivationTest.TransitionSource(directory, initial), "after-" + phase)) {
            first.finished.get(15, TimeUnit.SECONDS);
            assertThat(first.failure.get()).isNotNull();
        }
        Files.delete(fixture.file);
        byte[] savedFile = Files.readAllBytes(directory.resolve("offsets.dat"));
        try (var replacement = new Run(new As400LegacyActivationTest.TransitionSource(directory, initial), "")) {
            replacement.finished.get(15, TimeUnit.SECONDS);
            assertThat(replacement.failure.get()).isNotNull();
            assertThat(replacement.output).isEmpty();
            assertThat(replacement.input.reads).hasValue(0);
        }
        assertThat(Files.exists(fixture.file)).isFalse();
        assertThat(Files.readAllBytes(directory.resolve("offsets.dat"))).isEqualTo(savedFile);
    }

    /** Seed only the pre-experiment legacy offset through Kafka's real serialization and file store. */
    private void seedOffset(Map<String, ?> saved) throws Exception {
        var key = new JsonConverter();
        var value = new JsonConverter();
        key.configure(Map.of("schemas.enable", false), true);
        value.configure(Map.of("schemas.enable", false), false);
        var store = new FileOffsetBackingStore(key);
        store.configure(new EmbeddedWorkerConfig(new java.util.HashMap<>(Map.of("offset.storage.file.filename", directory.resolve("offsets.dat").toString()))));
        store.start();
        try {
            var writer = new OffsetStorageWriter(store, "local-recovery", key, value);
            writer.offset(Map.of("server", "test"), saved);
            assertThat(writer.beginFlush()).isTrue();
            writer.doFlush((error, ignored) -> {
            }).get(10, TimeUnit.SECONDS);
        }
        finally {
            store.stop();
            key.close();
            value.close();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false,false", "true,false,false", "false,true,false", "true,true,false", "false,false,true", "true,false,true" })
    void shouldPreserveConfiguredOutputAcrossSnapshotAndFreshEngineRecovery(boolean disk, boolean routed, boolean converted) throws Exception {
        Map<String, String> transforms = new java.util.HashMap<>(routed ? As400TopicRoutingTest.properties() : Map.of());
        if (converted) {
            transforms.putAll(As400NativeTableSchemasTest.converterProperties());
        }
        Map<String, ?> checkpoint;
        try (var first = initialize(disk, transforms)) {
            first.until("42");
            assertThat(first.input.reads.get()).isGreaterThan(0);
            assertThat(((As400InitializationTaskFixture) first.input).handoffs).hasValue(1);
            var snapshots = first.output.stream().filter(As400EmbeddedRecoveryTest::snapshot).toList();
            assertThat(snapshots).hasSize(6);
            assertThat(((Struct) snapshots.get(5).value()).getStruct("source").get("snapshot").toString()).isEqualTo("last");
            if (converted) {
                assertThat(snapshots).allSatisfy(record -> {
                    assertThat(((Struct) record.key()).getString("ID")).startsWith("custom:");
                    assertThat(((Struct) record.value()).getStruct("after").getString("ID")).startsWith("custom:");
                });
            }
            checkpoint = first.saved();
            assertThat(checkpoint).containsKey(As400DeliveryOffset.SESSION).doesNotContainKey("scan.sequence");
            assertThat(checkpoint.get("replay_from.sequence")).isEqualTo("31");
            if (routed) {
                assertThat(first.output).allSatisfy(record -> assertThat(record.topic()).startsWith("routed."));
            }
        }
        assertThat(Files.size(directory.resolve("offsets.dat"))).isGreaterThan(0);
        try (var replacement = recover(checkpoint, "", disk, transforms)) {
            replacement.until("59");
            String prefix = routed ? "routed.test." : "test.";
            var rows = replacement.output.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
            assertThat(rows).extracting(SourceRecord::topic, record -> ((Struct) record.value()).getString("op"), record -> {
                var after = ((Struct) record.value()).getStruct("after");
                return after == null ? null : after.get("ID");
            }).containsExactly(org.assertj.core.api.Assertions.tuple(prefix + "DCOC1107.B", "t", null),
                    org.assertj.core.api.Assertions.tuple(prefix + "DCOC1107.B", "c", converted ? "custom:100" : 100),
                    org.assertj.core.api.Assertions.tuple(prefix + "DCOC1107.A", "c", converted ? "custom:9" : 9));
            if (converted) {
                assertThat(((Struct) rows.get(1).key()).getString("ID")).isEqualTo("custom:100");
                assertThat(((Struct) rows.get(2).key()).getString("ID")).isEqualTo("custom:9");
            }
            assertThat(replacement.output).noneMatch(As400EmbeddedRecoveryTest::snapshot);
            assertThat(replacement.saved().get(As400DeliveryOffset.SESSION)).isNotEqualTo(checkpoint.get(As400DeliveryOffset.SESSION));
            if (routed) {
                assertThat(replacement.output).allSatisfy(record -> assertThat(record.topic()).startsWith("routed."));
            }
        }
    }

    @Test
    void shouldHonorTheStandardUnwrapTransformAndItsFilteredTruncatesAcrossEngineRestart() throws Exception {
        var transforms = Map.of("transforms", "unwrap", "transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.add.fields", "op");
        Map<String, ?> checkpoint;
        try (var first = initialize(false, transforms)) {
            first.until("42");
            var rows = first.output.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("__op") != null).toList();
            assertThat(rows).filteredOn(r -> "r".equals(((Struct) r.value()).getString("__op"))).hasSize(6);
            assertThat(rows).allSatisfy(r -> {
                assertThat(((Struct) r.value()).schema().field("after")).isNull();
                assertThat(((Struct) r.key()).getInt32("ID")).isEqualTo(((Struct) r.value()).getInt32("ID"));
            });
            checkpoint = first.saved();
        }
        try (var replacement = recover(checkpoint, "", false, transforms)) {
            replacement.until("59");
            var rows = replacement.output.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("__op") != null).toList();
            // Debezium's existing unwrap deliberately drops op=t. This is its
            // configured processing contract, not evidence that a sink got a clear.
            assertThat(rows).extracting(r -> ((Struct) r.value()).getString("__op"), r -> ((Struct) r.value()).getInt32("ID"))
                    .containsExactly(org.assertj.core.api.Assertions.tuple("c", 100), org.assertj.core.api.Assertions.tuple("c", 9));
            assertThat(replacement.saved().get("completed_through.sequence")).isEqualTo("59");
        }
    }

    @Test
    void shouldNotPublishATruncateOrAdvanceRecoveryWhenItsReloadFailsCustomConversion() throws Exception {
        var conversion = new java.util.HashMap<>(As400NativeTableSchemasTest.converterProperties());
        conversion.put("label.reject", "100");
        Map<String, ?> checkpoint;
        try (var first = initialize(false, conversion)) {
            first.until("42");
            checkpoint = first.saved();
        }
        byte[] savedFile = Files.readAllBytes(directory.resolve("offsets.dat"));
        try (var replacement = recover(checkpoint, "", false, conversion)) {
            replacement.finished.get(15, TimeUnit.SECONDS);
            assertThat(replacement.failure.get()).isNotNull();
            assertThat(replacement.output).noneMatch(record -> record.value() instanceof Struct value && value.schema().field("op") != null);
        }
        assertThat(Files.readAllBytes(directory.resolve("offsets.dat"))).isEqualTo(savedFile);
    }

    @Test
    void shouldRetainTheOldCheckpointUntilAFailedOffsetFlushIsRetriedSuccessfully() throws Exception {
        Map<String, ?> checkpoint;
        try (var first = initialize()) {
            first.until("42");
            checkpoint = first.saved();
        }
        byte[] file = Files.readAllBytes(directory.resolve("offsets.dat"));
        try (var next = recover(checkpoint, "offset-flush")) {
            await(() -> next.failedFlushes.get() > 0);
            assertThat(next.saved()).isEqualTo(checkpoint);
            assertThat(Files.readAllBytes(directory.resolve("offsets.dat"))).isEqualTo(file);
            await(() -> next.input.reads.get() > 1);
            assertThat(As400RecoveryTestSupport.data(next.output)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            assertThat(next.saved()).isEqualTo(checkpoint);
            assertThat(next.task.pendingAcknowledgementCount()).isGreaterThan(0);
            next.failFlush.set(false);
            next.until("59");
            assertThat(next.failure.get()).isNull();
            assertThat(next.saved().get("completed_through.sequence")).isEqualTo("59");
        }
    }

    @Test
    void shouldReadMultipleCutsBeforeALongFlushIntervalAndReplayAfterUnflushedDeliveryFails() throws Exception {
        Map<String, ?> checkpoint;
        try (var first = initialize()) {
            first.until("42");
            checkpoint = first.saved();
        }
        var input = new As400RecoveryTaskFixture(directory, (String) checkpoint.get("capture_fingerprint"));
        try (var ongoing = new Run(input, "delayed-crash")) {
            await(() -> input.reads.get() > 1);
            ongoing.failFlush.set(true); // Also exclude the engine's opportunistic idle/shutdown flushes.
            input.tail = 44;
            await(() -> As400RecoveryTestSupport.data(ongoing.output).contains("c:DCOC1107.B:100"));
            input.tail = 56;
            await(() -> As400RecoveryTestSupport.data(ongoing.output).contains("c:DCOC1107.A:9"));
            input.tail = 64;
            ongoing.finished.get(10, TimeUnit.SECONDS); // Well below the configured 60-second offset interval.
            assertThat(ongoing.failure.get()).isNotNull();
            assertThat(ongoing.lastFlushed.get().get("completed_through.sequence")).isEqualTo("42");
            assertThat(As400RecoveryTestSupport.data(ongoing.output)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
        }
        try (var replacement = recover(checkpoint, "")) {
            replacement.until("59");
            assertThat(As400RecoveryTestSupport.data(replacement.output)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "long-flush" })
    void shouldDeliverBeyondTheAcknowledgementWindowWithBoundedState(String flush) throws Exception {
        var initialization = new As400InitializationTaskFixture(directory);
        initialization.tail = 29;
        Map<String, ?> checkpoint;
        try (var first = new Run(initialization, "")) {
            first.until("29");
            checkpoint = first.saved();
        }
        var input = new As400RecoveryTaskFixture(directory, (String) checkpoint.get("capture_fingerprint")) {
            @Override
            As400OperatorCutReader.Limits readerLimits() {
                return new As400OperatorCutReader.Limits(16, 1000, 1_000_000);
            }
        };
        input.repeatedDeletes();
        input.tail = 631;
        try (var streaming = new Run(input, flush)) {
            await(() -> streaming.output.stream().anyMatch(r -> "631".equals(r.sourceOffset().get("completed_through.sequence"))));
            assertThat(streaming.failure.get()).isNull();
            assertThat(streaming.maximumPending.get()).isBetween(1, 256);
            assertThat(streaming.output.stream().filter(r -> r.value() == null).count()).isEqualTo(300);
            assertThat(streaming.output.stream().filter(r -> r.value() instanceof Struct value && value.schema().field("op") != null).count()).isEqualTo(600);
            if (flush.equals("long-flush")) {
                assertThat(streaming.saved().get("completed_through.sequence")).isEqualTo("29");
            }
            else {
                streaming.until("631");
            }
        }
    }

    @Test
    void shouldReplayTheWholeClearBatchAfterAnAcknowledgedPrefixAndConsumerFailure() throws Exception {
        Map<String, ?> checkpoint;
        try (var first = initialize()) {
            first.until("42");
            checkpoint = first.saved();
        }
        try (var interrupted = recover(checkpoint, "after-truncate-flush")) {
            interrupted.finished.get(15, TimeUnit.SECONDS);
            assertThat(interrupted.failure.get()).isNotNull();
            assertThat(As400RecoveryTestSupport.data(interrupted.output)).containsExactly("t:DCOC1107.B");
            checkpoint = interrupted.lastFlushed.get();
            assertThat(checkpoint.get("completed_through.sequence")).isEqualTo("42");
            assertThat(checkpoint.get("replay_from.sequence")).isEqualTo("31");
        }
        try (var replacement = recover(checkpoint, "")) {
            replacement.until("59");
            assertThat(As400RecoveryTestSupport.data(replacement.output))
                    .containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
        }
    }

    @Test
    void shouldRejectMissingOffsetsWithExistingHistoryBeforeNativeReads() throws Exception {
        Map<String, ?> checkpoint;
        try (var first = initialize()) {
            first.until("42");
            checkpoint = first.saved();
        }
        byte[] history = Files.readAllBytes(directory.resolve("history.jsonl"));
        Files.delete(directory.resolve("offsets.dat"));
        try (var replacement = recover(checkpoint, "")) {
            replacement.finished.get(15, TimeUnit.SECONDS);
            assertThat(replacement.failure.get()).isNotNull();
            assertThat(replacement.output).isEmpty();
            assertThat(replacement.input.reads).hasValue(0);
            assertThat(replacement.input.recoveries).hasValue(0);
        }
        assertThat(Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
    }

    @Test
    void shouldRejectPartiallyDeliveredInitializationOnFreshEngineRestart() throws Exception {
        var input = new As400InitializationTaskFixture(directory);
        try (var partial = new Run(input, "after-snapshot-flush")) {
            partial.finished.get(15, TimeUnit.SECONDS);
            assertThat(partial.failure.get()).isNotNull();
            assertThat(partial.lastFlushed.get()).containsKey("initialization.phase").doesNotContainKey(As400Checkpoint.VERSION);
            assertThat(input.reads).hasValue(0);
            assertThat(input.handoffs).hasValue(0);
        }
        assertThat(Files.exists(directory.resolve("history.jsonl"))).isFalse();
        try (var replacement = new Run(new As400RecoveryTaskFixture(directory, "unused"), "")) {
            replacement.finished.get(15, TimeUnit.SECONDS);
            assertThat(replacement.failure.get()).isNotNull();
            assertThat(replacement.output).isEmpty();
            assertThat(replacement.input.recoveries).hasValue(0);
        }
        assertThat(Files.exists(directory.resolve("history.jsonl"))).isFalse();
    }

    private Run initialize() throws Exception {
        return initialize(false);
    }

    private Run initialize(boolean disk) throws Exception {
        return initialize(disk, Map.of());
    }

    private Run initialize(boolean disk, Map<String, String> transforms) throws Exception {
        var input = new As400InitializationTaskFixture(directory);
        input.storage = storage(disk);
        return new Run(input, "", transforms);
    }

    private Run recover(Map<String, ?> checkpoint, String fault) throws Exception {
        return recover(checkpoint, fault, false);
    }

    private Run recover(Map<String, ?> checkpoint, String fault, boolean disk) throws Exception {
        return recover(checkpoint, fault, disk, Map.of());
    }

    private Run recover(Map<String, ?> checkpoint, String fault, boolean disk, Map<String, String> transforms) throws Exception {
        var input = new As400RecoveryTaskFixture(directory, (String) checkpoint.get("capture_fingerprint"));
        input.storage = storage(disk);
        input.tail = 64;
        return new Run(input, fault, transforms);
    }

    private As400SpillStore.Settings storage(boolean disk) {
        return disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null;
    }

    private static boolean snapshot(SourceRecord record) {
        return record.value() instanceof Struct value && value.schema().field("op") != null && "r".equals(value.getString("op"));
    }

    private class Run implements AutoCloseable {
        final String id = UUID.randomUUID().toString();
        final As400RecoveryTaskFixture input;
        final List<SourceRecord> output = new CopyOnWriteArrayList<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<Map<String, ?>> lastFlushed = new AtomicReference<>();
        final CompletableFuture<Void> finished = new CompletableFuture<>();
        final AtomicBoolean failFlush = new AtomicBoolean();
        final AtomicInteger failedFlushes = new AtomicInteger();
        final AtomicInteger maximumPending = new AtomicInteger();
        final DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine;
        final CompletableFuture<Void> runner;
        volatile FixtureTask task;

        Run(As400RecoveryTaskFixture input, String fault) {
            this(input, fault, Map.of());
        }

        Run(As400RecoveryTaskFixture input, String fault, Map<String, String> transforms) {
            this.input = input;
            // Use the shipping history adapter for every actual Engine case.
            // Native journal I/O remains controlled, but history is no longer a fake store.
            input.fixture.config = input.fixture.config.edit()
                    .with(io.debezium.storage.file.history.FileSchemaHistory.FILE_PATH, input.fixture.file.toString()).build();
            input.fixture.factory = As400FileBindingHistory::new;
            input.sourceOffsets = true;
            if (input instanceof As400LegacyActivationTest.TransitionSource handoff) {
                handoff.durable = this::saved;
            }
            failFlush.set(fault.equals("offset-flush") || fault.equals("long-flush"));
            RUNS.put(id, this);
            var properties = new Properties();
            properties.putAll(As400RecoveryTaskTest.properties());
            properties.remove("exactly.once.support");
            properties.remove("transaction.boundary");
            properties.setProperty("connector.class", FixtureConnector.class.getName());
            properties.setProperty("fixture.id", id);
            properties.setProperty("offset.storage", fault.equals("offset-flush") || fault.equals("delayed-crash") || fault.equals("long-flush")
                    ? FaultingOffsetStore.class.getName()
                    : "org.apache.kafka.connect.storage.FileOffsetBackingStore");
            properties.setProperty("offset.storage.file.filename", directory.resolve("offsets.dat").toString());
            properties.setProperty("offset.flush.interval.ms", fault.equals("long-flush") || fault.equals("delayed-crash") ? "60000" : "0");
            properties.setProperty("record.processing.order", "ORDERED");
            properties.setProperty("record.processing.threads", "1");
            properties.putAll(transforms);
            engine = DebeziumEngine.create(Connect.class).using(properties)
                    .using((success, message, error) -> {
                        failure.set(error);
                        finished.complete(null);
                    }).notifying((records, committer) -> {
                        for (var event : records) {
                            var record = event.value();
                            output.add(record);
                            maximumPending.accumulateAndGet(task.pendingAcknowledgementCount(), Math::max);
                            if (fault.equals("delayed-crash") && "59".equals(record.sourceOffset().get("completed_through.sequence"))) {
                                failFlush.set(true); // Model loss before a successful final flush, including shutdown's retry.
                                throw new IllegalStateException("Injected consumer failure before offset flush");
                            }
                            committer.markProcessed(event);
                            committer.markBatchFinished();
                            lastFlushed.set(saved());
                            boolean truncate = record.value() instanceof Struct value && value.schema().field("op") != null
                                    && "t".equals(value.getString("op"));
                            if (fault.equals("after-truncate-flush") && truncate || fault.equals("after-snapshot-flush") && snapshot(record)
                                    || record.value() instanceof String phase && fault.equals("after-" + phase)) {
                                throw new IllegalStateException("Injected consumer failure after a durable partial batch");
                            }
                        }
                        // The framework can pass an empty batch after an SMT
                        // filters all records. Finishing it acknowledges those
                        // intentional drops through the ordinary Engine contract.
                        committer.markBatchFinished();
                    }).build();
            runner = CompletableFuture.runAsync(engine);
        }

        Map<String, ?> saved() {
            return task.saved(input.partition());
        }

        void until(String completed) throws Exception {
            await(() -> failure.get() != null || task != null && saved() != null && completed.equals(saved().get("completed_through.sequence")));
            assertThat(failure.get()).isNull();
            await(() -> input.reads.get() > 1);
        }

        @Override
        public void close() throws Exception {
            try {
                if (!finished.isDone()) {
                    engine.close();
                }
                runner.get(15, TimeUnit.SECONDS);
            }
            finally {
                RUNS.remove(id);
            }
        }
    }

    /** Only failed writes are injected; successful writes/readback use Kafka's real file offset store. */
    public static class FaultingOffsetStore extends FileOffsetBackingStore {
        private Run run;

        public FaultingOffsetStore() {
            super(converter());
        }

        private static JsonConverter converter() {
            var converter = new JsonConverter();
            converter.configure(Map.of("schemas.enable", false), true);
            return converter;
        }

        @Override
        public void configure(WorkerConfig config) {
            super.configure(config);
            run = RUNS.get(config.originalsStrings().get("fixture.id"));
        }

        @Override
        public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
            if (run.failFlush.get()) {
                run.failedFlushes.incrementAndGet();
                var error = new java.io.IOException("Injected failed offset write");
                callback.onCompletion(error, null);
                return CompletableFuture.failedFuture(error);
            }
            return super.set(values, callback);
        }
    }

    public static class FixtureConnector extends As400RpcConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return FixtureTask.class;
        }

        @Override
        protected void validateConnection(Map<String, ConfigValue> values, Configuration config) {
            // No network; task admission and native evidence are the existing controlled fixtures.
        }
    }

    public static class FixtureTask extends As400ConnectorTask {
        Map<String, ?> saved(As400Partition partition) {
            return context.offsetStorageReader().offset(partition.getSourcePartition());
        }

        private Run run(Configuration config) {
            var run = RUNS.get(config.getString("fixture.id"));
            run.task = this;
            assertThat(context.transactionContext()).isNull();
            return run;
        }

        @Override
        As400TaskInitialization initializationForTesting(Configuration config) {
            return run(config).input instanceof As400TaskInitialization initialization ? initialization : null;
        }

        @Override
        As400TaskRecovery recoveryForTesting(Configuration config) {
            return run(config).input;
        }
    }
}
