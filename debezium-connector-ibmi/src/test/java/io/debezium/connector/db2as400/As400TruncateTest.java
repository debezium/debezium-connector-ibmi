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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400ZonedDecimal;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.As400StreamingChangeEventSource.TruncateException;
import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

class As400TruncateTest {
    private static final TableId TABLE = new TableId("DB", "LIB", "TEST");
    private static final Instant SOURCE_TIME = Instant.parse("2026-01-01T12:00:00Z");
    private final As400Partition partition = new As400Partition("test");
    private final As400JdbcConnection jdbc = mock(As400JdbcConnection.class);
    private As400RpcConnection rpc = mock(As400RpcConnection.class);
    private final As400DatabaseSchema schema = mock(As400DatabaseSchema.class);
    private final RetrieveJournal journal = mock(RetrieveJournal.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final EventDispatcher<As400Partition, TableId> dispatcher = mock(EventDispatcher.class);
    private final List<Struct> values = new ArrayList<>();
    private final List<Object> keys = new ArrayList<>();
    private TableSchema tableSchema;
    private As400ConnectorConfig config;
    private As400StreamingChangeEventSource source;
    private As400OffsetContext offset;

    @BeforeEach
    void setup() throws Exception {
        when(jdbc.getRealDatabaseName()).thenReturn("DB");
        when(jdbc.getLongName(anyString(), anyString())).thenAnswer(i -> i.getArgument(1));
        when(jdbc.isOnlyMember("LIB", "TEST", "TEST")).thenReturn(true);
        JdbcFileDecoder rowDecoder = mock(JdbcFileDecoder.class);
        when(schema.getFileDecoder()).thenReturn(rowDecoder);
        when(journal.decode(rowDecoder)).thenReturn(new Object[]{ 1 });
        when(schema.getRecordFormat(anyString(), anyString())).thenReturn(Optional.of(mock(TableInfo.class)));
        configure("none");
    }

    private void configure(String skipped) throws Exception {
        configure(skipped, "initial");
    }

    private void configure(String skipped, String snapshotMode) throws Exception {
        configure(skipped, snapshotMode, "fail");
    }

    private void configure(String skipped, String snapshotMode, String failureMode) throws Exception {
        Configuration.Builder builder = Configuration.create()
                .with("topic.prefix", "test").with("database.dbname", "DB")
                .with("database.schema", "LIB").with("table.include.list", "LIB.TEST")
                .with("snapshot.mode", snapshotMode)
                .with("event.processing.failure.handling.mode", failureMode);
        if (skipped != null) {
            builder.with("skipped.operations", skipped);
        }
        config = new As400ConnectorConfig(builder.build());
        offset = new As400OffsetContext(config, new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("RCV", "LIB"), SOURCE_TIME, true));
        Schema row = SchemaBuilder.struct().name("test.Value").field("ID", Schema.INT32_SCHEMA).optional().build();
        Schema key = SchemaBuilder.struct().name("test.Key").field("ID", Schema.INT32_SCHEMA).build();
        Envelope envelope = Envelope.defineSchema().withName("test.Envelope")
                .withRecord(row).withSource(offset.getSourceInfoSchema()).build();
        tableSchema = new TableSchema(TABLE, key, data -> new Struct(key).put("ID", data[0]),
                envelope, row, data -> new Struct(row).put("ID", data[0]));
        doAnswer(i -> {
            As400ChangeRecordEmitter emitter = i.getArgument(2);
            emitter.emitChangeRecords(tableSchema, (p, s, operation, k, value, ctx, headers) -> {
                keys.add(k);
                values.add(value);
            });
            return true;
        }).when(dispatcher).dispatchDataChangeEvent(eq(partition), eq(TABLE), any());
        source = new As400StreamingChangeEventSource(config, rpc, jdbc, dispatcher, errorHandler, Clock.system(), schema);
    }

    @Test
    void nativeClearEmitsKeylessTruncateWithJournalMetadata() throws Exception {
        accept('F', "CR", ' ', 42, 0, "TEST");
        assertTruncate();
        verify(journal, never()).decode(any());
    }

    @Test
    void sqlTruncateEmitsTruncateFromObjectLevelData() throws Exception {
        accept('D', "CG", '8', 42, 0, "TEST");
        assertTruncate();
        verify(schema, never()).clearCache(anyString(), anyString());
    }

    @Test
    void optimizedDeleteEmitsTruncate() throws Exception {
        accept('D', "CG", '3', 42, 0, "TEST");
        assertTruncate();
    }

    @Test
    void schemaChangesOnlyRefreshSchema() throws Exception {
        for (char subtype : "01245679ABCJ?".toCharArray()) {
            accept('D', "CG", subtype, 42, 0, "TEST");
        }
        assertThat(values).isEmpty();
        verify(schema, times(13)).clearCache("TEST", "LIB");
        verify(jdbc, never()).isOnlyMember(anyString(), anyString(), anyString());
    }

    @Test
    void truncatesRemainSkippedByDefault() throws Exception {
        configure(null);
        accept('F', "CR", ' ', 42, 0, "TEST");
        accept('D', "CG", '8', 43, 0, "TEST");
        assertThat(values).isEmpty();
        verify(journal, never()).decode(any());
        verify(jdbc, never()).isOnlyMember(anyString(), anyString(), anyString());
        verify(schema, never()).getRecordFormat(anyString(), anyString());
    }

    @Test
    void schemaChangesKeepExistingBehaviorByDefault() throws Exception {
        configure(null);
        EntryHeader entry = new EntryHeader(0, 0, 240, BigInteger.valueOf(42), BigInteger.valueOf(42),
                SOURCE_TIME, 'D', "CG", String.format("%-10s%-10s%-10s", "TEST", "LIB", "TEST"),
                BigInteger.ZERO, 365, 0, "RCV", "LIB");
        source.processJournalEntries(partition, offset).accept(BigInteger.valueOf(42), journal, entry);
        verify(schema).clearCache("TEST", "LIB");
        verify(schema).getRecordFormat("TEST", "LIB");
        verify(journal, never()).decode(any());
        assertThat(values).isEmpty();
    }

    @Test
    void sqlTruncatePrecedesReloadInsert() throws Exception {
        accept('D', "CG", '8', 42, 0, "TEST");
        accept('R', "PT", ' ', 43, 0, "TEST");
        assertThat(values).extracting(v -> v.getString("op")).containsExactly("t", "c");
    }

    @Test
    void metadataFailureIsNotSwallowed() throws Exception {
        when(jdbc.isOnlyMember("LIB", "TEST", "TEST")).thenThrow(new java.sql.SQLException("catalog unavailable"));
        assertThatThrownBy(() -> accept('F', "CR", ' ', 42, 0, "TEST"))
                .isInstanceOf(TruncateException.class).hasMessageContaining("Failed to capture clear");
        assertThat(values).isEmpty();
    }

    @Test
    void excludedTablesDoNotEmitTruncates() throws Exception {
        accept('F', "CR", ' ', 42, 0, "OTHER");
        accept('D', "CG", '8', 43, 0, "OTHER");
        assertThat(values).isEmpty();
        verify(jdbc, never()).isOnlyMember(anyString(), anyString(), anyString());
    }

    @Test
    void preservesInsertClearReloadOrder() throws Exception {
        accept('R', "PT", ' ', 41, 0, "TEST");
        accept('F', "CR", ' ', 42, 0, "TEST");
        accept('R', "PT", ' ', 43, 0, "TEST");
        assertThat(values).extracting(v -> v.getString("op")).containsExactly("c", "t", "c");
        assertThat(values).extracting(v -> v.getStruct("source").getString("sequence")).containsExactly("41", "42", "43");
        assertThat(keys.get(0)).isNotNull();
        assertThat(keys.get(1)).isNull();
        assertThat(keys.get(2)).isNotNull();
    }

    @Test
    void clearDiscardsCachedBeforeImage() throws Exception {
        accept('R', "UB", ' ', 41, 0, "TEST");
        accept('F', "CR", ' ', 42, 0, "TEST");
        accept('R', "UP", ' ', 43, 0, "TEST");
        assertThat(values).extracting(v -> v.getString("op")).containsExactly("t", "u");
        assertThat(values.get(1).get("before")).isNull();
        assertThat(values.get(1).getStruct("after").getInt32("ID")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void skipsClearWithUnknownTableIdentityAndAdvancesPosition(int missingPart) throws Exception {
        String[] parts = { "TEST", "LIB", "TEST" };
        parts[missingPart] = "";
        useRealRpc(clearWithIdentity(parts[0], parts[1], parts[2]));
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);

        source.execute(context, partition, offset);

        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
        verify(jdbc, never()).getLongName(anyString(), anyString());
        verify(jdbc, never()).isOnlyMember(anyString(), anyString(), anyString());
        verifyNoInteractions(schema, dispatcher, errorHandler);
    }

    @Test
    void rejectsClearWithMissingMemberOnIncludedTable() throws Exception {
        EntryHeader clear = clearWithIdentity("TEST", "LIB", "");
        assertThatThrownBy(() -> source.processJournalEntries(partition, offset).accept(BigInteger.valueOf(42), journal, clear))
                .isInstanceOf(TruncateException.class).hasMessageContaining("Missing member");
        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(jdbc, never()).isOnlyMember(anyString(), anyString(), anyString());
    }

    @Test
    void skipsClearWithMissingMemberOnExcludedTable() throws Exception {
        EntryHeader clear = clearWithIdentity("OTHER", "LIB", "");
        source.processJournalEntries(partition, offset).accept(BigInteger.valueOf(42), journal, clear);
        assertThat(values).isEmpty();
        verify(jdbc, never()).isOnlyMember(anyString(), anyString(), anyString());
        verifyNoInteractions(schema, dispatcher, errorHandler);
    }

    @Test
    void loadsSchemaBeforeDispatchingClear() throws Exception {
        accept('F', "CR", 42, 0, "TEST");
        InOrder order = inOrder(schema, dispatcher);
        order.verify(schema).getRecordFormat("TEST", "LIB");
        order.verify(dispatcher).dispatchDataChangeEvent(eq(partition), eq(TABLE), any());
        assertTruncate();
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial", "no_data", "never" })
    void clearLoadsMissingSchemaThroughRealDispatcher(String snapshotMode) throws Exception {
        configure("none", snapshotMode);
        when(schema.schemaFor(TABLE)).thenReturn(null);
        // The journal carries a system name; dispatch uses the SQL table name.
        when(jdbc.getLongName("LIB", "SYS0001")).thenReturn("TEST");
        when(jdbc.isOnlyMember("LIB", "SYS0001", "SYS0001")).thenReturn(true);
        when(schema.getRecordFormat("SYS0001", "LIB")).thenAnswer(i -> {
            when(schema.schemaFor(TABLE)).thenReturn(tableSchema);
            return Optional.of(mock(TableInfo.class));
        });
        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        useRealDispatcher(queue);
        assertThat(schema.schemaFor(TABLE)).isNull();

        accept('F', "CR", 42, 0, "SYS0001");

        ArgumentCaptor<DataChangeEvent> event = ArgumentCaptor.forClass(DataChangeEvent.class);
        verify(queue).enqueue(event.capture());
        assertThat(event.getValue().getRecord().key()).isNull();
        assertThat(((Struct) event.getValue().getRecord().value()).getString("op")).isEqualTo("t");
        verify(schema).getRecordFormat("SYS0001", "LIB");
        verify(journal, never()).decode(any());
    }

    @Test
    void clearRetriesSchemaLoadingAfterFileChange() throws Exception {
        when(schema.getRecordFormat("TEST", "LIB")).thenReturn(Optional.empty(), Optional.of(mock(TableInfo.class)));
        accept('D', "CG", 41, 0, "TEST");
        accept('F', "CR", 42, 0, "TEST");
        InOrder order = inOrder(schema, dispatcher);
        order.verify(schema).clearCache("TEST", "LIB");
        order.verify(schema, times(2)).getRecordFormat("TEST", "LIB");
        order.verify(dispatcher).dispatchDataChangeEvent(eq(partition), eq(TABLE), any());
        assertTruncate();
    }

    @Test
    void missingSchemaStopsStreamingBeforeJournalPositionAdvances() throws Exception {
        when(schema.getRecordFormat("TEST", "LIB")).thenReturn(Optional.empty());
        useRealRpc(entry('F', "CR", 42, 0, "TEST"));
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);

        assertThatThrownBy(() -> source.execute(context, partition, offset))
                .isInstanceOf(TruncateException.class).hasMessageContaining("Missing table schema");

        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(journal, never()).getPosition();
        verifyNoInteractions(dispatcher, errorHandler);
    }

    @Test
    void schemaLoadingFailureIsNotSwallowed() throws Exception {
        when(schema.getRecordFormat("TEST", "LIB")).thenThrow(new IllegalStateException("schema unavailable"));
        assertThatThrownBy(() -> accept('F', "CR", 42, 0, "TEST"))
                .isInstanceOf(TruncateException.class).hasMessageContaining("Failed to capture clear")
                .hasRootCauseMessage("schema unavailable");
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verifyNoInteractions(dispatcher);
    }

    @Test
    void rejectsTransactionalClearWithoutEmittingOrAdvancingOffset() {
        assertThatThrownBy(() -> accept('F', "CR", 42, 7, "TEST"))
                .isInstanceOf(TruncateException.class).hasMessageContaining("commitment control");
        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
    }

    @ParameterizedTest
    @ValueSource(chars = { '3', '8' })
    void rejectsTransactionalSqlClearWithoutEmittingOrAdvancingOffset(char subtype) {
        assertThatThrownBy(() -> accept('D', "CG", subtype, 42, 7, "TEST"))
                .isInstanceOf(TruncateException.class).hasMessageContaining("commitment control");
        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
    }

    @Test
    void rejectsAmbiguousMemberScope() throws Exception {
        when(jdbc.isOnlyMember("LIB", "TEST", "TEST")).thenReturn(false);
        assertThatThrownBy(() -> accept('F', "CR", ' ', 42, 0, "TEST"))
                .isInstanceOf(TruncateException.class).hasMessageContaining("one matching file member");
        assertThat(values).isEmpty();
    }

    @Test
    void propagatesDecodeFailure() throws Exception {
        EntryHeader entry = entry('D', "CG", 42, 0, "TEST");
        when(journal.decode(any(FileChangeEntryDecoder.class))).thenThrow(new IOException("short payload"));
        assertThatThrownBy(() -> source.processJournalEntries(partition, offset).accept(BigInteger.valueOf(42), journal, entry))
                .isInstanceOf(TruncateException.class).hasMessageContaining("Cannot classify");
        assertThat(values).isEmpty();
    }

    @Test
    void dispatchFailureIsNotSwallowed() throws Exception {
        doThrow(new IllegalStateException("queue failed")).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        assertThatThrownBy(() -> accept('F', "CR", ' ', 42, 0, "TEST"))
                .isInstanceOf(TruncateException.class).hasMessageContaining("Failed to capture clear");
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
    }

    @ParameterizedTest
    @ValueSource(strings = { "F.CR", "D.CG.8", "D.CG.3" })
    void falseDispatchStopsStreamingBeforeJournalPositionAdvances(String operation) throws Exception {
        String[] parts = operation.split("\\.");
        EntryHeader clear = entry(parts[0].charAt(0), parts[1], 42, 0, "TEST");
        if (parts.length == 3) {
            when(journal.decode(any(FileChangeEntryDecoder.class)))
                    .thenReturn(new FileChangeEntryDecoder.FileChange("TEST", "LIB", "TEST", parts[2].charAt(0)));
        }
        doReturn(false).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());

        useRealRpc(clear);
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);

        assertThatThrownBy(() -> source.execute(context, partition, offset))
                .isInstanceOf(TruncateException.class).hasMessageContaining("Failed to dispatch clear").hasMessageContaining("42");
        verifyNoInteractions(errorHandler);
        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(journal, never()).getPosition();
        verify(journal).nextEntry();
    }

    @Test
    void unsupportedClearPropagatesToCoordinator() throws Exception {
        useRealRpc(entry('F', "CR", 42, 7, "TEST"));
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);
        assertThatThrownBy(() -> source.execute(context, partition, offset))
                .isInstanceOf(TruncateException.class).hasMessageContaining("commitment control");
        verifyNoInteractions(errorHandler);
        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(journal, never()).getPosition();
    }

    @Test
    void dispatchInterruptionPropagatesUnwrapped() throws Exception {
        InterruptedException interrupted = new InterruptedException("stopping");
        doThrow(interrupted).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        assertThatThrownBy(() -> accept('F', "CR", 42, 0, "TEST")).isSameAs(interrupted);
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verifyNoInteractions(errorHandler);
    }

    @ParameterizedTest
    @CsvSource({ "false,fail", "true,fail", "true,warn", "true,skip" })
    void interruptDuringShutdownDoesNotReportFailureOrAdvancePosition(boolean realDispatcher, String failureMode) throws Exception {
        configure("none", "initial", failureMode);
        useRealRpc(entry('F', "CR", 42, 0, "TEST"));
        AtomicBoolean running = new AtomicBoolean(true);
        if (realDispatcher) {
            when(schema.schemaFor(TABLE)).thenReturn(tableSchema);
            ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
            doAnswer(i -> {
                running.set(false);
                throw new InterruptedException("stopping");
            }).when(queue).enqueue(any());
            useRealDispatcher(queue);
        }
        else {
            doAnswer(i -> {
                running.set(false);
                throw new InterruptedException("stopping");
            }).when(dispatcher).dispatchDataChangeEvent(any(), any(), any());
        }

        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenAnswer(i -> running.get());
        source.execute(context, partition, offset);

        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(journal, never()).getPosition();
        verify(rpc, never()).close();
        verify(jdbc, never()).close();
        verifyNoInteractions(errorHandler);
    }

    @ParameterizedTest
    @ValueSource(strings = { "fail", "warn", "skip" })
    void heartbeatInterruptionAfterTruncateDuringShutdownDoesNotReportFailureOrAdvancePosition(String failureMode) throws Exception {
        configure("none", "initial", failureMode);
        useRealRpc(entry('F', "CR", 42, 0, "TEST"));
        when(schema.schemaFor(TABLE)).thenReturn(tableSchema);
        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger enqueued = new AtomicInteger();
        doAnswer(i -> {
            if (enqueued.incrementAndGet() == 2) {
                running.set(false);
                throw new InterruptedException("heartbeat interrupted during shutdown");
            }
            return null;
        }).when(queue).enqueue(any());
        // A zero interval makes the real heartbeat enqueue deterministic without a timer or sleep.
        useRealDispatcher(queue, alwaysDueHeartbeat(queue));
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenAnswer(i -> running.get());

        source.execute(context, partition, offset);

        assertTruncateThenHeartbeat(queue);
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(journal, never()).getPosition();
        verify(journal).nextEntry();
        verify(rpc, never()).close();
        verify(jdbc, never()).close();
        verifyNoInteractions(errorHandler);
    }

    @ParameterizedTest
    @ValueSource(strings = { "fail", "warn", "skip" })
    void heartbeatFailureAfterTruncateRemainsFatalInEveryHandlingMode(String failureMode) throws Exception {
        configure("none", "initial", failureMode);
        useRealRpc(entry('F', "CR", 42, 0, "TEST"));
        when(schema.schemaFor(TABLE)).thenReturn(tableSchema);
        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        AtomicInteger enqueued = new AtomicInteger();
        doAnswer(i -> {
            if (enqueued.incrementAndGet() == 2) {
                throw new IllegalStateException("heartbeat queue failed");
            }
            return null;
        }).when(queue).enqueue(any());
        useRealDispatcher(queue, alwaysDueHeartbeat(queue));
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);

        assertThatThrownBy(() -> source.execute(context, partition, offset)).isInstanceOf(TruncateException.class);

        assertTruncateThenHeartbeat(queue);
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(journal, never()).getPosition();
        verifyNoInteractions(errorHandler);
    }

    @ParameterizedTest
    @ValueSource(strings = { "fail", "warn", "skip" })
    void heartbeatInterruptionIsPreservedAndDoesNotMaskLaterFailure(String failureMode) throws Exception {
        configure("none", "initial", failureMode);
        when(schema.schemaFor(TABLE)).thenReturn(tableSchema);
        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        AtomicInteger enqueued = new AtomicInteger();
        InterruptedException interrupted = new InterruptedException("heartbeat interrupted");
        doAnswer(i -> {
            int enqueue = enqueued.incrementAndGet();
            if (enqueue == 2) {
                throw interrupted;
            }
            if (enqueue == 4) {
                throw new IllegalStateException("later heartbeat queue failed");
            }
            return null;
        }).when(queue).enqueue(any());
        useRealDispatcher(queue, alwaysDueHeartbeat(queue));

        assertThatThrownBy(() -> accept('F', "CR", 42, 0, "TEST")).isSameAs(interrupted);
        assertThatThrownBy(() -> accept('F', "CR", 43, 0, "TEST")).isInstanceOf(TruncateException.class);

        verify(queue, times(4)).enqueue(any());
        verifyNoInteractions(errorHandler);
    }

    private void assertTruncateThenHeartbeat(ChangeEventQueue<DataChangeEvent> queue) throws InterruptedException {
        ArgumentCaptor<DataChangeEvent> events = ArgumentCaptor.forClass(DataChangeEvent.class);
        verify(queue, times(2)).enqueue(events.capture());
        assertThat(events.getAllValues().get(0).getRecord().key()).isNull();
        assertThat(((Struct) events.getAllValues().get(0).getRecord().value()).getString("op")).isEqualTo("t");
        assertThat(events.getAllValues().get(1).getRecord().topic()).isEqualTo("__debezium-heartbeat.test");
    }

    @ParameterizedTest
    @ValueSource(strings = { "fail", "warn", "skip" })
    void queueFailureRemainsFatalInEveryHandlingMode(String failureMode) throws Exception {
        configure("none", "initial", failureMode);
        useRealRpc(entry('F', "CR", 42, 0, "TEST"));
        when(schema.schemaFor(TABLE)).thenReturn(tableSchema);
        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        doThrow(new IllegalStateException("queue failed")).when(queue).enqueue(any());
        useRealDispatcher(queue);
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);

        assertThatThrownBy(() -> source.execute(context, partition, offset)).isInstanceOf(TruncateException.class);

        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(journal, never()).getPosition();
        verifyNoInteractions(errorHandler);
    }

    private ScheduledHeartbeat alwaysDueHeartbeat(ChangeEventQueue<DataChangeEvent> queue) {
        return new HeartbeatImpl(Duration.ZERO, "__debezium-heartbeat.test", "test", config.schemaNameAdjuster(), queue) {
            @Override
            public void emitWithDelay(java.util.Map<String, ?> partition, io.debezium.pipeline.spi.OffsetContext offset) throws InterruptedException {
                emit(partition, offset);
            }
        };
    }

    private void useRealDispatcher(ChangeEventQueue<DataChangeEvent> queue) {
        useRealDispatcher(queue, ScheduledHeartbeat.NOOP_HEARTBEAT);
    }

    private void useRealDispatcher(ChangeEventQueue<DataChangeEvent> queue, ScheduledHeartbeat heartbeat) {
        DebeziumHeaderProducer headers = mock(DebeziumHeaderProducer.class);
        when(headers.contextHeaders()).thenReturn(new ConnectHeaders());
        EventDispatcher<As400Partition, TableId> realDispatcher = new As400EventDispatcher(config,
                config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true), schema, queue,
                config.getTableFilters().dataCollectionFilter(), DataChangeEvent::new, new As400EventMetadataProvider(),
                heartbeat, config.schemaNameAdjuster(), null, headers);
        source = new As400StreamingChangeEventSource(config, rpc, jdbc, realDispatcher, errorHandler, Clock.system(), schema);
    }

    private void useRealRpc(EntryHeader entry) throws Exception {
        // Exercise the real RPC consumption loop and its diagnostics without IBM i access.
        rpc = spy(new As400RpcConnection(config, mock(As400StreamingChangeEventSourceMetrics.class),
                mock(JournalInfoRetrieval.class), journal));
        JournalProcessedPosition position = new JournalProcessedPosition(BigInteger.valueOf(42),
                new JournalReceiver("RCV", "LIB"), SOURCE_TIME, true);
        doReturn(position.asJournalPosition()).when(rpc).getCurrentPosition();
        when(journal.retrieveJournal(any())).thenReturn(RetrievalState.Success);
        when(journal.nextEntry()).thenReturn(true, false);
        when(journal.getEntryHeader()).thenReturn(entry);
        when(journal.getPosition()).thenReturn(position);
        source = new As400StreamingChangeEventSource(config, rpc, jdbc, dispatcher, errorHandler, Clock.system(), schema);
    }

    private EntryHeader clearWithIdentity(String file, String library, String member) {
        return new EntryHeader(0, 0, 240, BigInteger.valueOf(42), BigInteger.valueOf(42),
                SOURCE_TIME, 'F', "CR", String.format("%-10s%-10s%-10s", file, library, member),
                BigInteger.ZERO, 365, 0, "RCV", "LIB");
    }

    private void assertTruncate() {
        assertThat(values).hasSize(1);
        assertThat(keys).containsExactly((Object) null);
        Struct value = values.get(0);
        assertThat(value.getString("op")).isEqualTo(Operation.TRUNCATE.code());
        assertThat(value.get("before")).isNull();
        assertThat(value.get("after")).isNull();
        assertThat(value.getStruct("source").getString("sequence")).isEqualTo("42");
        assertThat(value.getStruct("source").getInt64("ts_ms")).isEqualTo(SOURCE_TIME.toEpochMilli());
        assertThat(offset.getTransactionContext().isTransactionInProgress()).isFalse();
        assertThat(offset.getTransactionContext().getTransactionId()).isNull();
    }

    private void accept(char code, String type, int sequence, int transaction, String file) throws Exception {
        accept(code, type, code == 'D' ? '0' : ' ', sequence, transaction, file);
    }

    private void accept(char code, String type, char subtype, int sequence, int transaction, String file) throws Exception {
        EntryHeader entry = entry(code, type, sequence, transaction, file);
        if (code == 'D') {
            byte[] data = new byte[240 + 16 + 109];
            new AS400ZonedDecimal(5, 0).toBytes(BigDecimal.valueOf(109), data, 240);
            new AS400Text(30).toBytes(String.format("%-10s%-10s%-10s", file, "LIB", file), data, 256);
            new AS400Text(1).toBytes(String.valueOf(subtype), data, 256 + 108);
            when(journal.decode(any(FileChangeEntryDecoder.class)))
                    .thenAnswer(i -> ((FileChangeEntryDecoder) i.getArgument(0)).decode(entry, data, 0));
        }
        source.processJournalEntries(partition, offset).accept(BigInteger.valueOf(sequence), journal, entry);
    }

    private EntryHeader entry(char code, String type, int sequence, int transaction, String file) {
        // Object-level entries are routed from the ESD, even when the header is blank.
        String object = code == 'D' ? " ".repeat(30) : String.format("%-10s%-10s%-10s", file, "LIB", file);
        return new EntryHeader(0, 0, 240, BigInteger.valueOf(sequence), BigInteger.valueOf(sequence),
                SOURCE_TIME, code, type, object, BigInteger.valueOf(transaction), 365, 0, "RCV", "LIB");
    }
}
