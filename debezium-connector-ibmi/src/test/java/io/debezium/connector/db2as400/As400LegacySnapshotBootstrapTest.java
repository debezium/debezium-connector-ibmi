/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.file.history.FileSchemaHistory;

class As400LegacySnapshotBootstrapTest {
    @TempDir
    Path directory;

    private static Map<String, String> partial() {
        var partial = new HashMap<>(Fixture.legacyOffset(29));
        partial.put("snapshot_completed", "false");
        return Map.copyOf(partial);
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial", "initial_only", "when_needed", "no_data" })
    @SuppressWarnings("unchecked")
    void shouldUseTheOrdinarySnapshotterWhenRestartingAnAcknowledgedPartialSnapshot(String mode) {
        var config = new As400ConnectorConfig(io.debezium.config.Configuration.create().with("topic.prefix", "test")
                .with("database.dbname", "DB").with("database.schema", "LIB").with("snapshot.mode", mode).build());
        var previous = new As400OffsetContext.Loader(config).load(partial());
        io.debezium.spi.snapshot.Snapshotter snapshotter = switch (mode) {
            case "initial" -> new io.debezium.snapshot.mode.InitialSnapshotter();
            case "initial_only" -> new io.debezium.snapshot.mode.InitialOnlySnapshotter();
            case "when_needed" -> new io.debezium.snapshot.mode.WhenNeededSnapshotter();
            default -> new io.debezium.snapshot.mode.NoDataSnapshotter();
        };
        var connections = (io.debezium.jdbc.MainConnectionProvidingConnectionFactory<As400JdbcConnection>) org.mockito.Mockito.mock(
                io.debezium.jdbc.MainConnectionProvidingConnectionFactory.class);
        var source = new As400SnapshotChangeEventSource(config, () -> {
            throw new AssertionError("Evaluating snapshot policy must not read the native journal");
        }, connections,
                org.mockito.Mockito.mock(As400DatabaseSchema.class), org.mockito.Mockito.mock(io.debezium.pipeline.EventDispatcher.class),
                io.debezium.util.Clock.system(), org.mockito.Mockito.mock(SnapshotProgressListener.class),
                org.mockito.Mockito.mock(io.debezium.pipeline.notification.NotificationService.class),
                new io.debezium.snapshot.SnapshotterService(snapshotter, null, null));
        var task = source.getSnapshottingTask(new As400Partition("test"), previous);
        assertThat(task.snapshotSchema()).isTrue();
        assertThat(task.snapshotData()).isEqualTo(!mode.equals("no_data"));
        assertThat(source.getSnapshottingTask(new As400Partition("test"), new As400OffsetContext.Loader(config).load(Fixture.legacyOffset(29))).snapshotData())
                .isFalse();
    }

    private static SourceRecord record(Map<String, ?> offset, String value) {
        return new SourceRecord(Map.of("server", "test"), offset, "snapshot-control", SchemaBuilder.string().name("test.snapshot.control").build(), value);
    }

    private static List<SourceRecord> through(As400RecoveryTaskTest.Running run, String value) throws Exception {
        var records = new ArrayList<SourceRecord>();
        while (records.stream().noneMatch(record -> value.equals(record.value()))) {
            records.addAll(run.next());
            assertThat(records).hasSizeLessThan(1000);
        }
        return records;
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid", "sequence", "receiver", "unprocessed", "handoff" })
    void shouldAdmitOnlyARecognizableIncompleteLegacySnapshotForRestart(String state) {
        var saved = new HashMap<>(partial());
        switch (state) {
            case "sequence" -> saved.put("offset.event_sequence", "null");
            case "receiver" -> saved.remove("offset.receiver");
            case "unprocessed" -> saved.put("offset.processed", "false");
            case "handoff" -> saved.put(As400LegacyHandoff.CHECKPOINT, "unacknowledged");
        }
        if (state.equals("valid")) {
            assertThat(As400NativeHandoffSession.snapshotRequired(saved)).isTrue();
            assertThat(As400NativeHandoffSession.snapshotRequired(Fixture.legacyOffset(29))).isFalse();
        }
        else {
            assertThatThrownBy(() -> As400NativeHandoffSession.snapshotRequired(saved)).isInstanceOf(RuntimeException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDeliverStandardEarlySnapshotNotificationsOnlyWithARealSnapshotOffset(boolean noData) throws Exception {
        var records = new ArrayList<SourceRecord>();
        var acknowledged = new ArrayList<Map<String, ?>>();
        var delivery = new As400LegacySnapshotBootstrap.Delivery(new As400TaskHandoff.Output() {
            @Override
            public void emit(SourceRecord record) {
                records.add(record);
            }

            @Override
            public void barrier(SourceRecord record) {
                records.add(record);
            }

            @Override
            public String acknowledge(SourceRecord record) {
                acknowledged.add(record.sourceOffset());
                return "acknowledged";
            }
        }, offset -> record(offset, "boundary"));
        var config = new As400ConnectorConfig(io.debezium.config.Configuration.create().with("topic.prefix", "test")
                .with("notification.enabled.channels", "sink").with("notification.sink.topic.name", "notifications").build());
        var notifications = new io.debezium.pipeline.notification.NotificationService<As400Partition, As400OffsetContext>(
                List.of(new io.debezium.pipeline.notification.channels.SinkNotificationChannel()), config,
                io.debezium.schema.SchemaFactory.get(), record -> delivery.enqueue(new DataChangeEvent(record)));
        try {
            notifications.initialSnapshotNotificationService().notifyStarted(new As400Partition("test"), null);
            assertThat(records).isEmpty();
            if (!noData) {
                delivery.enqueue(new DataChangeEvent(record(partial(), "row")));
                assertThat(records.get(0).sourceOffset()).isEqualTo(partial());
            }
            delivery.flush(Fixture.legacyOffset(29));
            assertThat(records.get(0).topic()).isEqualTo("notifications");
            assertThat(records).allSatisfy(record -> assertThat(record.sourceOffset()).isNotEmpty());
            assertThat(acknowledged).containsExactly(Fixture.legacyOffset(29));
        }
        finally {
            notifications.stop();
        }
    }

    @Test
    void shouldPinEveryRecordAndAutomaticBoundaryUntilTheDeferredOutcomeIsComplete() throws Exception {
        var records = new ArrayList<SourceRecord>();
        var acknowledged = new ArrayList<Map<String, ?>>();
        var delivery = new As400LegacySnapshotBootstrap.Delivery(new As400TaskHandoff.Output() {
            @Override
            public void emit(SourceRecord record) {
                records.add(record);
            }

            @Override
            public void barrier(SourceRecord record) {
            }

            @Override
            public String acknowledge(SourceRecord record) {
                acknowledged.add(record.sourceOffset());
                return "acknowledged";
            }
        }, offset -> record(offset, "boundary"));
        var safe = Fixture.legacyOffset(29);
        var scanned = Fixture.legacyOffset(1000);
        delivery.pin(safe);
        for (int i = 0; i < 600; i++) {
            var row = record(scanned, "row-" + i);
            row.headers().addInt("row-number", i);
            delivery.enqueue(new DataChangeEvent(row));
        }
        delivery.flush(scanned);
        assertThat(records).hasSize(600).allSatisfy(row -> assertThat(row.sourceOffset()).isEqualTo(safe));
        assertThat(acknowledged).hasSize(4).allMatch(safe::equals);
        for (int i = 0; i < records.size(); i++) {
            assertThat(records.get(i).value()).isEqualTo("row-" + i);
            assertThat(records.get(i).headers().lastWithName("row-number").value()).isEqualTo(i);
        }
        delivery.releasePin();
        delivery.flush(scanned);
        assertThat(acknowledged.get(acknowledged.size() - 1)).isEqualTo(scanned);
    }

    @Test
    void shouldBoundSnapshotDeliveryBeyondTheFormerCeilingAndPreserveIncompleteOffsetsUntilCompletion() throws Exception {
        var acknowledgements = new ArrayList<Map<String, ?>>();
        var delivered = new AtomicInteger();
        var pending = new AtomicInteger();
        var delivery = new As400LegacySnapshotBootstrap.Delivery(new As400TaskHandoff.Output() {
            @Override
            public void emit(SourceRecord record) {
                delivered.incrementAndGet();
                assertThat(pending.incrementAndGet()).isLessThanOrEqualTo(256);
                assertThat(record.sourceOffset()).isEqualTo(partial());
            }

            @Override
            public void barrier(SourceRecord record) {
            }

            @Override
            public String acknowledge(SourceRecord record) {
                acknowledgements.add(record.sourceOffset());
                pending.set(0);
                return "acknowledged";
            }
        }, offset -> record(offset, "snapshot"));
        for (int i = 0; i < 12000; i++) {
            delivery.enqueue(new DataChangeEvent(record(partial(), "row")));
        }
        assertThat(acknowledgements).allMatch(partial()::equals);
        delivery.flush(Fixture.legacyOffset(29));
        assertThat(delivered).hasValue(12000);
        assertThat(pending).hasValue(0);
        assertThat(acknowledgements.get(acknowledgements.size() - 1)).isEqualTo(Fixture.legacyOffset(29));
    }

    @ParameterizedTest
    @ValueSource(strings = { "emit", "barrier", "acknowledge" })
    void shouldPropagateInterruptedOrFailedSnapshotDeliveryBeforeAnyLaterRows(String failure) throws Exception {
        var calls = new ArrayList<String>();
        var delivery = new As400LegacySnapshotBootstrap.Delivery(new As400TaskHandoff.Output() {
            private void call(String stage) throws InterruptedException {
                calls.add(stage);
                if (failure.equals(stage)) {
                    throw new InterruptedException("stopped during " + stage);
                }
            }

            @Override
            public void emit(SourceRecord record) throws Exception {
                call("emit");
            }

            @Override
            public void barrier(SourceRecord record) throws Exception {
                call("barrier");
            }

            @Override
            public String acknowledge(SourceRecord record) throws Exception {
                call("acknowledge");
                return "acknowledged";
            }
        }, offset -> record(offset, "snapshot"));
        assertThatThrownBy(() -> {
            for (int i = 0; i < 300; i++) {
                delivery.enqueue(new DataChangeEvent(record(partial(), "row")));
            }
        }).isInstanceOf(InterruptedException.class).hasMessageContaining(failure);
        assertThat(calls.stream().filter("emit"::equals).count()).isEqualTo(failure.equals("emit") ? 1 : 256);
    }

    @ParameterizedTest
    @ValueSource(strings = { "manifest", "partial-page", "damaged" })
    void shouldRejectMissingOffsetsWithAnyRetainedHistoryBeforeNativeAdmission(String history) throws Exception {
        var f = new Fixture(directory);
        f.config = io.debezium.config.Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName())
                .with(FileSchemaHistory.FILE_PATH, f.file.toString()).with(SchemaHistory.NAME, "test").build();
        f.factory = As400BindingHistory.storage(f.config);
        f.history().requireEmptyForSnapshot();
        assertThat(Files.exists(f.file)).isFalse();
        var pending = f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), Fixture.legacyOffset(28));
        if (history.equals("partial-page")) {
            Files.write(f.file, Files.readAllLines(f.file).subList(0, 1));
        }
        if (history.equals("damaged")) {
            Files.writeString(f.file, "{broken");
        }
        byte[] before = Files.readAllBytes(f.file);
        assertThatThrownBy(() -> f.history().requireEmptyForSnapshot()).hasMessageContaining("retained binding history");
        var raw = f.config.edit().with("truncate.transactional.enabled", true).with("topic.prefix", "test")
                .with("database.schema", "LIB").with("database.dbname", "DB").with("skipped.operations", "none")
                .with("provide.transaction.metadata", true).with("errors.tolerance", "none").with("event.converting.failure.handling.mode", "fail").build();
        var context = org.mockito.Mockito.mock(org.apache.kafka.connect.source.SourceTaskContext.class);
        var offsets = org.mockito.Mockito.mock(org.apache.kafka.connect.storage.OffsetStorageReader.class);
        org.mockito.Mockito.when(context.offsetStorageReader()).thenReturn(offsets);
        org.mockito.Mockito.when(offsets.offset(org.mockito.ArgumentMatchers.any())).thenReturn(null);
        try (var handles = org.mockito.Mockito.mockConstruction(As400DcgNativeIo.class)) {
            assertThatThrownBy(() -> new As400NativeHandoffSession(new As400ConnectorConfig(raw), context, null, raw,
                    Fixture.JOURNAL, Fixture.POLICY, null, "control", As400ContinuousRuntime.Limits.experimental(), true))
                    .hasMessageContaining("retained binding history");
            assertThat(handles.constructed()).isEmpty();
        }
        assertThat(Files.readAllBytes(f.file)).isEqualTo(before);
        assertThat(pending.fingerprint()).startsWith("v2:");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldFinishInitialOnlyAfterDurableSnapshotWithoutAcquiringAHandoff(boolean sourceOffsets) throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        Files.delete(f.file);
        var snapshots = new AtomicInteger();
        var input = new As400LegacyActivationTest.TransitionSource(directory, initial) {
            @Override
            public boolean snapshotRequired() {
                return true;
            }

            @Override
            public Map<String, ?> snapshot(ChangeEventSourceContext context, As400OffsetContext previous,
                                           SnapshotProgressListener<As400Partition> listener,
                                           List<io.debezium.pipeline.notification.channels.NotificationChannel> channels, Output output)
                    throws Exception {
                snapshots.incrementAndGet();
                var delivery = new As400LegacySnapshotBootstrap.Delivery(output, offset -> record(offset, "snapshot-boundary"));
                delivery.enqueue(new DataChangeEvent(record(partial(), "snapshot-row")));
                delivery.flush(Fixture.legacyOffset(29));
                return Fixture.legacyOffset(29);
            }
        };
        input.sourceOffsets = sourceOffsets;
        try (var run = new As400RecoveryTaskTest.Running(input, null)) {
            startSnapshotOnly(run);
            var records = through(run, "snapshot-boundary");
            assertThat(records.stream().map(SourceRecord::value)).containsExactly("snapshot-row", "snapshot-boundary");
            assertThat(input.closes).hasValue(0);
            assertThat(run.durable.get()).isNull();
            run.ack(records);
            As400RecoveryTaskTest.await(() -> input.closes.get() == 1);
            assertThat(As400DeliveryOffset.strip(run.durable.get())).isEqualTo(Fixture.legacyOffset(29));
            assertThat(snapshots).hasValue(1);
            assertThat(input.handoffs).isZero();
            assertThat(input.reads).hasValue(0);
            assertThat(input.recoveries).hasValue(0);
            assertThat(Files.exists(f.file)).isFalse();
            assertThat(run.task.poll()).isEmpty();
        }
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({ "legacy,false", "legacy,true", "native,false", "native,true" })
    void shouldLeaveCompletedOffsetsUnchangedWhenRestartingInitialOnly(String offsetKind, boolean sourceOffsets) throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        var activation = As400LegacyActivationTest.handoff(f, initial);
        f.history().recordLegacyHandoff(activation, Fixture.POLICY, As400LegacyActivationTest.order(f), activation::barrier);
        var history = Files.readAllBytes(f.file);
        Map<String, ?> saved = offsetKind.equals("native") ? initial : activation.barrier();
        var input = new As400LegacyActivationTest.TransitionSource(directory, initial);
        input.sourceOffsets = sourceOffsets;
        try (var run = new As400RecoveryTaskTest.Running(input, saved)) {
            startSnapshotOnly(run);
            As400RecoveryTaskTest.await(() -> input.closes.get() == 1);
            assertThat(run.task.poll()).isEmpty();
            assertThat(run.durable.get()).isEqualTo(saved);
            assertThat(Files.readAllBytes(f.file)).isEqualTo(history);
            assertThat(input.handoffs).isZero();
            assertThat(input.reads).hasValue(0);
        }
    }

    private static void startSnapshotOnly(As400RecoveryTaskTest.Running run) {
        if (!run.input.sourceOffsets) {
            org.mockito.Mockito.when(run.context.transactionContext())
                    .thenReturn(org.mockito.Mockito.mock(org.apache.kafka.connect.source.TransactionContext.class));
        }
        var properties = As400RecoveryTaskTest.properties();
        properties.put("transaction.boundary", "connector");
        properties.put("snapshot.mode", "initial_only");
        run.started = true;
        run.task.start(properties);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldResumeIncompleteSnapshotAndRequireDurableCompletionBeforeTheActualTaskHandsOff(boolean sourceOffsets) throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        Files.delete(f.file);
        Map<String, ?> saved = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            var previous = saved;
            var input = new As400LegacyActivationTest.TransitionSource(directory, initial) {
                @Override
                public boolean snapshotRequired() {
                    return true;
                }

                @Override
                public Map<String, ?> snapshot(ChangeEventSourceContext context, As400OffsetContext old,
                                               SnapshotProgressListener<As400Partition> listener,
                                               List<io.debezium.pipeline.notification.channels.NotificationChannel> channels, Output output)
                        throws Exception {
                    assertThat(old == null).isEqualTo(previous == null);
                    if (old != null) {
                        assertThat(old.isSnapshotComplete()).isFalse();
                    }
                    var delivery = new As400LegacySnapshotBootstrap.Delivery(output, offset -> record(offset, "snapshot-boundary"));
                    for (int i = 0; i < 600; i++) {
                        delivery.enqueue(new DataChangeEvent(record(partial(), "snapshot-row")));
                    }
                    delivery.flush(Fixture.legacyOffset(29));
                    fixture.initialize(); // Controlled boundary acquisition only after snapshot acknowledgement.
                    return Fixture.legacyOffset(29);
                }
            };
            input.sourceOffsets = sourceOffsets;
            try (var run = new As400RecoveryTaskTest.Running(input, saved)) {
                input.durable = run.durable::get;
                if (!sourceOffsets) {
                    org.mockito.Mockito.when(run.context.transactionContext())
                            .thenReturn(org.mockito.Mockito.mock(org.apache.kafka.connect.source.TransactionContext.class));
                }
                var properties = As400RecoveryTaskTest.properties();
                properties.put("transaction.boundary", "connector");
                run.started = true;
                run.task.start(properties);
                var first = through(run, "snapshot-boundary");
                assertThat(first.stream().filter(r -> "snapshot-row".equals(r.value()))).hasSize(256);
                assertThat(input.handoffs).isZero();
                assertThat(input.reads).hasValue(0);
                assertThat(Files.exists(f.file)).isFalse();
                run.ack(first);
                if (attempt == 0) {
                    saved = As400DeliveryOffset.strip(run.durable.get());
                    assertThat(saved).isEqualTo(partial());
                    continue;
                }
                run.ack(through(run, "snapshot-boundary"));
                var complete = through(run, "snapshot-boundary");
                assertThat(complete.get(complete.size() - 1).sourceOffset().get("snapshot_completed")).isEqualTo("true");
                assertThat(input.handoffs).isZero();
                assertThat(Files.exists(f.file)).isFalse();
                run.ack(complete);
                run.ack(through(run, "legacy-barrier"));
                run.ack(through(run, "migration"));
                var streaming = run.throughCheckpoint("42");
                assertThat(As400RecoveryTestSupport.data(streaming)).containsExactly("c:DCOC1107.A:8");
                run.ack(streaming);
                assertThat(input.handoffs).isEqualTo(1);
            }
        }
    }
}
