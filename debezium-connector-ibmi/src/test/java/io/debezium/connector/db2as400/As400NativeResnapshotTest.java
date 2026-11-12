/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.Tables;

class As400NativeResnapshotTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = { "success", "interrupted", "schema", "history" })
    void shouldRetainOriginalRecoveryUntilTheWholeRequestedSnapshotIsAcknowledged(String fault) throws Exception {
        var f = new Snapshot(directory);
        byte[] history = Files.readAllBytes(f.source.fixture.file);
        if (fault.equals("schema")) {
            f.mismatchedSchema = true;
        }
        if (fault.equals("history")) {
            f.source.fixture.entries.removeIf(e -> e.position().equals(Fixture.position(31)));
        }
        var saved = new AtomicReference<Map<String, ?>>(f.original.toOffset());
        var records = new ArrayList<SourceRecord>();
        var output = output(saved, records, fault.equals("interrupted"));
        if (!fault.equals("success")) {
            assertThatThrownBy(() -> f.snapshot(output, false)).isInstanceOf(Exception.class).hasStackTraceContaining(switch (fault) {
                case "schema" -> "Current snapshot schema differs";
                case "history" -> "Missing cycle or changed producer";
                default -> "Interrupted after an acknowledged snapshot prefix";
            });
            assertThat(As400FullSnapshotState.base(saved.get())).isEqualTo(f.original.toOffset());
            assertThat(records).allSatisfy(r -> assertThat(As400FullSnapshotState.base(r.sourceOffset())).isEqualTo(f.original.toOffset()));
        }
        else {
            var completed = f.snapshot(output, false);
            assertThat(completed.toOffset()).containsEntry("completed_through.sequence", "42").containsEntry("replay_from.sequence", "31");
            assertThat(saved.get()).isEqualTo(completed.toOffset());
            var rows = snapshotRows(records);
            assertThat(rows).hasSize(300);
            assertThat(rows).allSatisfy(r -> {
                assertThat(((Struct) r.value()).getString("op")).isEqualTo("r");
                assertThat(As400FullSnapshotState.pending(r.sourceOffset())).isTrue();
                assertThat(As400FullSnapshotState.base(r.sourceOffset())).isEqualTo(f.original.toOffset());
            });
            assertThat(((Struct) rows.get(299).value()).getStruct("source").getString("snapshot")).isEqualTo("last");
        }
        assertThat(Files.readAllBytes(f.source.fixture.file)).isEqualTo(history);
        if (fault.equals("interrupted")) {
            assertThat(As400FullSnapshotState.pending(saved.get())).isTrue();
            var replacement = new Snapshot(directory);
            records.clear();
            var completed = replacement.snapshot(output(saved, records, false), true);
            assertThat(saved.get()).isEqualTo(completed.toOffset());
            assertThat(snapshotRows(records)).hasSize(300);
        }
        if (fault.equals("schema") || fault.equals("history")) {
            assertThat(records).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "always", "configuration_based", "snapshot_only", "interrupted", "pending_no_data" })
    void shouldHonorSnapshotPolicyAndPendingDeliveryThroughTheActualTask(String mode) throws Exception {
        var snapshot = new Snapshot(Files.createDirectory(directory.resolve("sql")));
        var nativeDirectory = Files.createDirectory(directory.resolve("task"));
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(nativeDirectory));
        Map<String, ?> saved = initial;
        for (int attempt = 0; attempt < (mode.equals("interrupted") ? 2 : 1); attempt++) {
            var input = new As400RecoveryTaskFixture(nativeDirectory, initial.get("capture_fingerprint")) {
                @Override
                public boolean allowLegacyHandoffRecovery() {
                    return true;
                }

                @Override
                public io.debezium.ibmi.db2.journal.retrieve.JournalPosition snapshotPosition() {
                    return Fixture.position(42);
                }

                @Override
                public As400JdbcConnection incrementalSnapshotConnection(As400ConnectorConfig config) throws Exception {
                    return snapshot.input.incrementalSnapshotConnection(config);
                }

                @Override
                public As400Checkpoint fullSnapshot(As400Checkpoint original, As400BindingHistory.Recovered history,
                                                    io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext context,
                                                    SnapshotProgressListener<As400Partition> listener,
                                                    List<io.debezium.pipeline.notification.channels.NotificationChannel> channels,
                                                    As400TaskHandoff.Output output, boolean interrupted)
                        throws Exception {
                    return As400NativeResnapshot.execute(this, snapshot.config, snapshot.context, original, history, context, listener, channels,
                            output, offset -> new SourceRecord(partition().getSourcePartition(), offset, "snapshot-control",
                                    org.apache.kafka.connect.data.SchemaBuilder.string().name("SnapshotBoundary").build(), "BOUNDARY"),
                            interrupted);
                }
            };
            input.sourceOffsets = true;
            if (mode.equals("pending_no_data")) {
                saved = As400FullSnapshotState.pending(As400Checkpoint.fromOffset(initial, input.journalIdentity(), input.fingerprint()));
            }
            try (var task = new As400RecoveryTaskTest.Running(input, saved)) {
                var properties = new java.util.HashMap<>(As400RecoveryTaskTest.properties());
                properties.put("snapshot.mode", mode.equals("pending_no_data") ? "no_data"
                        : mode.equals("snapshot_only") ? "configuration_based"
                                : mode.equals("interrupted") ? (attempt == 0 ? "always" : "initial") : mode);
                properties.put("snapshot.mode.configuration.based.snapshot.data", "true");
                properties.put("snapshot.mode.configuration.based.start.stream", Boolean.toString(!mode.equals("snapshot_only")));
                properties.remove("exactly.once.support");
                properties.remove("transaction.boundary");
                task.started = true;
                task.task.start(properties);
                if (mode.equals("pending_no_data")) {
                    assertThatThrownBy(task::next).hasStackTraceContaining("Cannot skip an interrupted requested snapshot");
                    assertThat(input.reads).hasValue(0);
                    assertThat(task.durable.get()).isEqualTo(saved);
                    continue;
                }
                var delivered = new ArrayList<SourceRecord>();
                while (true) {
                    var records = task.next();
                    delivered.addAll(records);
                    task.ack(records);
                    var current = As400DeliveryOffset.strip(task.durable.get());
                    if (mode.equals("interrupted") && attempt == 0 && snapshotRows(delivered).size() >= 256) {
                        assertThat(As400FullSnapshotState.pending(current)).isTrue();
                        assertThat(As400FullSnapshotState.base(current)).isEqualTo(initial);
                        saved = task.durable.get();
                        break;
                    }
                    if (!As400FullSnapshotState.pending(current)) {
                        assertThat(current.get("completed_through.sequence")).isEqualTo("42");
                        assertThat(current.get("replay_from.sequence")).isEqualTo("31");
                        assertThat(snapshotRows(delivered)).hasSize(300);
                        assertThat(snapshotRows(delivered)).allSatisfy(r -> assertThat(As400FullSnapshotState.base(
                                As400DeliveryOffset.strip(r.sourceOffset()))).isEqualTo(initial));
                        if (mode.equals("snapshot_only")) {
                            As400RecoveryTaskTest.await(() -> input.stopped.get());
                            assertThat(input.reads).hasValue(1); // Boundary replay only; no streaming reader.
                        }
                        else {
                            input.tail = 64;
                            assertThat(As400RecoveryTestSupport.data(task.throughCheckpoint("59")))
                                    .containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
                        }
                        break;
                    }
                }
            }
        }
    }

    private static List<SourceRecord> snapshotRows(List<SourceRecord> records) {
        return records.stream().filter(r -> r.value() instanceof Struct value && value.schema().field("op") != null
                && "r".equals(value.getString("op"))).toList();
    }

    private static As400TaskHandoff.Output output(AtomicReference<Map<String, ?>> saved, List<SourceRecord> records, boolean interrupt) {
        return new As400TaskHandoff.Output() {
            private int rows;

            @Override
            public void emit(SourceRecord record) {
                if (interrupt && ++rows == 270) {
                    throw new IllegalStateException("Interrupted after an acknowledged snapshot prefix");
                }
                records.add(record);
            }

            @Override
            public void barrier(SourceRecord record) {
                records.add(record);
            }

            @Override
            public String acknowledge(SourceRecord record) {
                saved.set(record.sourceOffset());
                return "durable";
            }
        };
    }

    private static final class Snapshot extends As400ConnectorTask {
        final As400ContinuousRuntimeTest.Source source;
        final As400TaskRecovery input;
        final As400ConnectorConfig config;
        final CdcSourceTaskContext<As400ConnectorConfig> context;
        final As400Checkpoint original;
        boolean mismatchedSchema;

        @SuppressWarnings("unchecked")
        Snapshot(Path directory) throws Exception {
            source = new As400ContinuousRuntimeTest.Source(directory);
            var definition = source.history.definitions().get(0);
            var table = definition.schema();
            var raw = Configuration.from(As400RecoveryTaskTest.properties()).edit().with("snapshot.mode", "always")
                    .with("truncate.transactional.enabled", true).with("database.schema", table.id().schema())
                    .with("table.include.list", table.id().schema() + "." + table.id().table()).with("snapshot.max.threads", 1).build();
            context = (CdcSourceTaskContext<As400ConnectorConfig>) preStart(raw);
            config = (As400ConnectorConfig) context.getConfig();
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, context);
            var jdbc = mock(As400JdbcConnection.class);
            var connection = mock(java.sql.Connection.class);
            when(jdbc.connection()).thenReturn(connection);
            when(jdbc.isValid()).thenReturn(true);
            when(jdbc.getCurrentTimestamp()).thenReturn(Optional.of(Instant.EPOCH));
            when(jdbc.readTableNames(any(), any(), any(), any())).thenReturn(Set.of(table.id()));
            when(jdbc.getSystemName(any(), any())).thenReturn(Optional.of(table.id().table()));
            when(jdbc.quoteIdentifier(anyString())).thenAnswer(call -> "\"" + call.getArgument(0) + "\"");
            doAnswer(call -> {
                ((Tables) call.getArgument(0)).overwriteTable(mismatchedSchema
                        ? table.edit().addColumn(table.columns().get(0).edit().optional(true).create()).create()
                        : table);
                return null;
            }).when(jdbc).readSchema(any(), any(), any(), any(), any(), anyBoolean());
            when(jdbc.rowToArray(any(), any(), any())).thenAnswer(call -> new Object[]{ ((ResultSet) call.getArgument(1)).getInt(1) });
            when(jdbc.readTableStatement(any(), any())).thenAnswer(call -> {
                var statement = mock(Statement.class);
                when(statement.executeQuery(anyString())).thenAnswer(query -> {
                    var rows = RowSetProvider.newFactory().createCachedRowSet();
                    var metadata = new RowSetMetaDataImpl();
                    metadata.setColumnCount(1);
                    metadata.setColumnName(1, "ID");
                    metadata.setColumnLabel(1, "ID");
                    metadata.setColumnType(1, Types.INTEGER);
                    rows.setMetaData(metadata);
                    for (int id = 300; id > 0; id--) {
                        rows.moveToInsertRow();
                        rows.updateInt(1, id);
                        rows.insertRow();
                        rows.moveToCurrentRow();
                    }
                    rows.beforeFirst();
                    return rows;
                });
                return statement;
            });
            input = mock(As400TaskRecovery.class);
            when(input.runtimeLimits()).thenReturn(As400ContinuousRuntime.Limits.experimental());
            when(input.partition()).thenReturn(new As400Partition("test"));
            when(input.incrementalSnapshotConnection(any())).thenReturn(jdbc);
            when(input.snapshotPosition()).thenReturn(Fixture.position(42));
            when(input.read(any(), any())).thenAnswer(call -> source.read(call.getArgument(0), call.getArgument(1)));
            original = As400Checkpoint.fromOffset(source.initial, source.fixture.journalIdentity, source.history.fingerprint());
        }

        As400Checkpoint snapshot(As400TaskHandoff.Output output, boolean interrupted) throws Exception {
            var running = mock(io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext.class);
            when(running.isRunning()).thenReturn(true);
            return As400NativeResnapshot.execute(input, config, context, original, source.history, running,
                    mock(SnapshotProgressListener.class), List.of(), output,
                    saved -> new SourceRecord(input.partition().getSourcePartition(), saved, "snapshot-control", Schema.STRING_SCHEMA, "BOUNDARY"), interrupted);
        }
    }
}
