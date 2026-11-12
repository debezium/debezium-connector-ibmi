/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.data.Envelope.Operation;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalRecord;
import io.debezium.pipeline.signal.actions.snapshotting.CloseIncrementalSnapshotWindow;
import io.debezium.pipeline.signal.actions.snapshotting.ExecuteSnapshot;
import io.debezium.pipeline.signal.actions.snapshotting.OpenIncrementalSnapshotWindow;
import io.debezium.pipeline.source.snapshot.incremental.ChunkQueryBuilder;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;

class As400BufferedIncrementalSnapshotTest {
    @TempDir
    Path directory;

    @Test
    void shouldRejectRestoredSnapshotTargetsWithoutReadingTheCurrentCatalog() throws Exception {
        var context = new io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext<TableId>(true);
        context.addDataCollectionNamesToSnapshot("request", List.of("DB.UNSEEN.TABLE"), List.of(), "");
        var saved = As400IncrementalSnapshotState.capture(context);
        try (var f = new Fixture(directory, saved, false)) {
            assertThatThrownBy(() -> f.output.prepareSignals(f.records::add)).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("no recovered capture schema");
            assertThat(f.records).isEmpty();
            assertThat(f.markers).isEmpty();
        }
    }

    @Test
    void shouldRequireARecoveredSignalTableBeforeOpeningAnIncrementalSnapshotConnection() throws Exception {
        try (var f = new Fixture(directory, Map.of())) {
            assertThatThrownBy(() -> As400RecoverySource.requireSignalingBindings(f.config, List.of(f.definition)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no recovered journal binding");
            var raw = f.config.getConfig().edit().with("signal.data.collection", f.table.toString()).build();
            var selected = new As400ConnectorConfig(raw);
            As400RecoverySource.requireSignalingBindings(selected, List.of(f.definition));
            assertThatThrownBy(() -> As400RecoverySource.requireSignalingBindings(selected,
                    List.of(As400BindingDefinition.excluded(f.definition.identity()))))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no recovered journal binding");
        }
    }

    @Test
    void shouldStopBeforeLaterActionsWhenTheStandardProcessorCatchesAnActionFailure() throws Exception {
        try (var f = new Fixture(directory, Map.of())) {
            var calls = new ArrayList<String>();
            f.signals.registerSignalAction("failure", payload -> {
                calls.add("failed");
                throw new IllegalStateException("injected snapshot failure");
            });
            f.signals.registerSignalAction("later", payload -> {
                calls.add("must not run");
                return true;
            });
            f.channel.signals.add(new SignalRecord("one", "failure", "{}", Map.of()));
            f.channel.signals.add(new SignalRecord("two", "later", "{}", Map.of()));
            f.signals.process();
            f.signals.process();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> f.output.prepareSignals(f.records::add))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("before checkpoint");
            assertThat(calls).containsExactly("failed");
            assertThat(f.records).isEmpty();
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> f.output.prepareSignals(f.records::add))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("stopped");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRemoveTheEntireOpenSnapshotWindowOnlyWhenAClearCommits(boolean clear) throws Exception {
        try (var f = new Fixture(directory, Map.of())) {
            f.signal("request", ExecuteSnapshot.NAME, "{\"data-collections\":[\"" + f.table + "\"],\"type\":\"incremental\"}");
            assertThat(f.records).isEmpty(); // Reading a chunk does not publish its unvalidated window.
            f.watermark(0);
            if (clear) {
                f.clear();
            }
            f.watermark(1);
            assertThat(f.records.stream().map(r -> ((Struct) r.value()).getString("op"))).containsExactlyElementsOf(
                    clear ? List.of("t") : List.of("r", "r", "r"));
            assertThat(f.output.snapshotState().orElseThrow()).isEmpty();
        }
    }

    @Test
    void shouldRestartAnUnfinishedChunkWithFreshWatermarksAndRejectOldMarkers() throws Exception {
        Map<String, String> saved;
        SignalRecord stale;
        try (var first = new Fixture(directory, Map.of())) {
            first.signal("request", ExecuteSnapshot.NAME, "{\"data-collections\":[\"" + first.table + "\"],\"type\":\"incremental\"}");
            first.watermark(0);
            saved = first.output.snapshotState().orElseThrow();
            stale = first.markers.get(1);
            assertThat(first.records).isEmpty();
        }
        try (var replacement = new Fixture(directory, saved)) {
            replacement.output.prepareSignals(replacement.records::add);
            assertThat(replacement.markers.get(0).getId()).isNotEqualTo(stale.getId());
            replacement.signal(stale.getId(), stale.getType(), stale.getData());
            assertThat(replacement.records).isEmpty();
            replacement.watermark(0);
            replacement.watermark(1);
            assertThat(replacement.records).hasSize(3).allSatisfy(row -> assertThat(((Struct) row.value()).getString("op")).isEqualTo("r"));
        }
    }

    @Test
    void shouldExecuteQueuedRequestsAgainstTheReplacementDispatcherOnTheSourceThread() throws Exception {
        try (var f = new Fixture(directory, Map.of())) {
            var calls = new ArrayList<String>();
            f.signals.registerSignalAction("probe", payload -> {
                calls.add("old");
                return true;
            });
            f.channel.signals.add(new SignalRecord("queued", "probe", "{}", Map.of()));
            f.signals.process();
            assertThat(calls).isEmpty();
            Thread owner = Thread.currentThread();
            f.signals.registerSignalAction("probe", payload -> {
                assertThat(Thread.currentThread()).isSameAs(owner);
                calls.add("replacement");
                return true;
            });
            f.output.prepareSignals(f.records::add);
            assertThat(calls).containsExactly("replacement");
        }
    }

    private static final class Fixture extends As400ConnectorTask implements AutoCloseable {
        final io.debezium.pipeline.signal.channels.SourceSignalChannel channel = new io.debezium.pipeline.signal.channels.SourceSignalChannel();
        final List<SignalRecord> markers = new ArrayList<>();
        final List<SourceRecord> records = new ArrayList<>();
        final As400RecoveryTestSupport.Fixture capture;
        final TableId table;
        final As400BindingDefinition definition;
        final As400BatchDispatcher output;
        final As400BufferedSignals signals;
        final As400ConnectorConfig config;

        Fixture(Path directory, Map<String, String> snapshotState) throws Exception {
            this(directory, snapshotState, true);
        }

        @SuppressWarnings("unchecked")
        Fixture(Path directory, Map<String, String> snapshotState, boolean start) throws Exception {
            capture = new As400RecoveryTestSupport.Fixture(directory);
            definition = As400NativeHandoffSession.legacyCatalog(capture.definitions, "DB").get(0);
            table = definition.schema().id();
            var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                    .with("database.schema", table.schema()).with("skipped.operations", "none").with("heartbeat.interval.ms", 0)
                    .with("provide.transaction.metadata", false).with("event.converting.failure.handling.mode", "fail")
                    .with("signal.data.collection", "DB." + table.schema() + ".SIGNALS").with("incremental.snapshot.chunk.size", 10).build();
            var taskContext = (io.debezium.connector.common.CdcSourceTaskContext<As400ConnectorConfig>) preStart(raw);
            config = (As400ConnectorConfig) taskContext.getConfig();
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
            var jdbc = mock(As400JdbcConnection.class);
            when(jdbc.getRealDatabaseName()).thenReturn("DB");
            when(jdbc.quotedTableIdString(any())).thenReturn("SIGNALS");
            var queries = (ChunkQueryBuilder<TableId>) mock(ChunkQueryBuilder.class, org.mockito.Answers.CALLS_REAL_METHODS);
            when(jdbc.<TableId> chunkQueryBuilder(any())).thenReturn(queries);
            when(queries.getQueryColumns(any(), any())).thenReturn(definition.schema().primaryKeyColumns());
            when(queries.buildMaxPrimaryKeyQuery(any(), any(), any())).thenReturn("maximum");
            when(queries.buildChunkQuery(any(), any(), any())).thenReturn("chunk");
            when(jdbc.queryAndMap(anyString(), any(JdbcConnection.ResultSetMapper.class))).thenAnswer(call -> {
                try (var rows = rows(List.of(3))) {
                    return ((JdbcConnection.ResultSetMapper<?>) call.getArgument(1)).apply(rows);
                }
            });
            when(jdbc.rowToArray(any(), any(), any())).thenAnswer(call -> new Object[]{ ((ResultSet) call.getArgument(1)).getInt(1) });
            when(queries.readTableChunkStatement(any(), any(), any())).thenAnswer(call -> {
                var context = (IncrementalSnapshotContext<TableId>) call.getArgument(0);
                int from = context.chunkEndPosititon() == null ? 0 : (Integer) context.chunkEndPosititon()[0];
                var statement = mock(PreparedStatement.class);
                when(statement.executeQuery()).thenReturn(rows(List.of(1, 2, 3).stream().filter(id -> id > from).toList()));
                return statement;
            });
            when(jdbc.prepareUpdate(anyString(), any())).thenAnswer(call -> {
                var values = new String[3];
                var statement = mock(PreparedStatement.class);
                doAnswer(set -> {
                    values[(int) set.getArgument(0) - 1] = set.getArgument(1);
                    return null;
                }).when(statement).setString(org.mockito.ArgumentMatchers.anyInt(), any());
                ((JdbcConnection.StatementPreparer) call.getArgument(1)).accept(statement);
                markers.add(new SignalRecord(values[0], values[1], values[2], Map.of()));
                return jdbc;
            });
            var topics = config.<TableId> getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
            var schemas = new As400DatabaseSchema(config, jdbc, topics, config.schemaNameAdjuster(), As400NativeTableSchemas.converterRegistry(config), taskContext);
            schemas.forwardSchema(definition.outputTable());
            signals = new As400BufferedSignals(config, List.of(channel), null);
            output = new As400BatchDispatcher(config, new As400Partition("test"), schemas, topics, new DebeziumHeaderProducer(taskContext),
                    () -> 42000L, 100, true, signals);
            var notification = new NotificationService<As400Partition, As400OffsetContext>(List.of(), config, SchemaFactory.get(),
                    output.eventDispatcher()::enqueueNotification);
            var snapshot = new As400BufferedIncrementalSnapshot(config, jdbc, output.eventDispatcher(), schemas, () -> 42000L,
                    mock(SnapshotProgressListener.class), DataChangeEventListener.NO_OP(), notification);
            var checkpoint = As400Checkpoint.afterVerifiedBoundary(capture.journalIdentity, "v1:" + "a".repeat(64),
                    As400RecoveryTestSupport.Fixture.position(29), capture.receiverIdentity).withSnapshotState(snapshotState);
            output.incrementalSnapshot(snapshot, checkpoint);
            signals.registerSignalAction(ExecuteSnapshot.NAME, new ExecuteSnapshot<>(output.eventDispatcher(), null));
            signals.registerSignalAction(OpenIncrementalSnapshotWindow.NAME, new OpenIncrementalSnapshotWindow<>());
            signals.registerSignalAction(CloseIncrementalSnapshotWindow.NAME, new CloseIncrementalSnapshotWindow<>(output.eventDispatcher()));
            if (start) {
                output.prepareSignals(records::add);
            }
        }

        void signal(String id, String type, String data) throws Exception {
            channel.signals.add(new SignalRecord(id, type, data, Map.of()));
            signals.process();
            output.prepareSignals(records::add);
        }

        void watermark(int index) throws Exception {
            var marker = markers.get(index);
            signal(marker.getId(), marker.getType(), marker.getData());
        }

        void clear() throws Exception {
            var entry = capture.entries.stream().filter(e -> e.header().getJournalCode() == 'D' && e.header().getEntryType().equals("CG")).findFirst().orElseThrow();
            var source = As400SourceMetadata.from(entry.header(), entry.position());
            var change = new As400TransactionBuffer.Change(Operation.TRUNCATE, definition.identity(), null, source, null, null);
            output.prepare(new As400TransactionBuffer.CommittedBatch(new As400TransactionBuffer.TransactionId(capture.journalIdentity,
                    BigInteger.TEN, entry.position()), source, source, List.of(change)), records::add);
        }

        @Override
        public void close() throws Exception {
            output.close();
            signals.stop();
            config.getServiceRegistry().close();
        }
    }

    private static CachedRowSet rows(List<Integer> values) throws Exception {
        var rows = RowSetProvider.newFactory().createCachedRowSet();
        var metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(1);
        metadata.setColumnName(1, "ID");
        metadata.setColumnLabel(1, "ID");
        metadata.setColumnType(1, Types.INTEGER);
        metadata.setColumnTypeName(1, "INTEGER");
        rows.setMetaData(metadata);
        for (int i = values.size() - 1; i >= 0; i--) {
            rows.moveToInsertRow();
            rows.updateInt(1, values.get(i));
            rows.insertRow();
            rows.moveToCurrentRow();
        }
        rows.beforeFirst();
        return rows;
    }
}
