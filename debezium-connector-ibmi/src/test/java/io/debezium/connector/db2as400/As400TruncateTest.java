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
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400ZonedDecimal;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400StreamingChangeEventSource.TruncateException;
import io.debezium.data.Envelope;
import io.debezium.data.Envelope.Operation;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
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
    private final As400RpcConnection rpc = mock(As400RpcConnection.class);
    private final As400DatabaseSchema schema = mock(As400DatabaseSchema.class);
    private final RetrieveJournal journal = mock(RetrieveJournal.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final EventDispatcher<As400Partition, TableId> dispatcher = mock(EventDispatcher.class);
    private final List<Struct> values = new ArrayList<>();
    private final List<Object> keys = new ArrayList<>();
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
        configure("none");
    }

    private void configure(String skipped) throws Exception {
        Configuration.Builder builder = Configuration.create()
                .with("topic.prefix", "test").with("database.dbname", "DB")
                .with("database.schema", "LIB").with("table.include.list", "LIB.TEST");
        if (skipped != null) {
            builder.with("skipped.operations", skipped);
        }
        As400ConnectorConfig config = new As400ConnectorConfig(builder.build());
        offset = new As400OffsetContext(config, new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("RCV", "LIB"), SOURCE_TIME, true));
        Schema row = SchemaBuilder.struct().name("test.Value").field("ID", Schema.INT32_SCHEMA).optional().build();
        Schema key = SchemaBuilder.struct().name("test.Key").field("ID", Schema.INT32_SCHEMA).build();
        Envelope envelope = Envelope.defineSchema().withName("test.Envelope")
                .withRecord(row).withSource(offset.getSourceInfoSchema()).build();
        TableSchema tableSchema = new TableSchema(TABLE, key, data -> new Struct(key).put("ID", data[0]),
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
    @ValueSource(ints = { 0, 1, 2 })
    void rejectsClearWithMissingObjectIdentity(int missingPart) throws Exception {
        String[] parts = { "TEST", "LIB", "TEST" };
        parts[missingPart] = "";
        EntryHeader clear = new EntryHeader(0, 0, 240, BigInteger.valueOf(42), BigInteger.valueOf(42),
                SOURCE_TIME, 'F', "CR", String.format("%-10s%-10s%-10s", parts[0], parts[1], parts[2]),
                BigInteger.ZERO, 365, 0, "RCV", "LIB");
        assertThatThrownBy(() -> source.processJournalEntries(partition, offset).accept(BigInteger.valueOf(42), journal, clear))
                .isInstanceOf(TruncateException.class).hasMessageContaining("Missing");
        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(jdbc, never()).isOnlyMember(anyString(), anyString(), anyString());
    }

    @ParameterizedTest
    @ValueSource(chars = { '3', '8' })
    void rejectsTransactionalClearWithoutEmittingOrAdvancingOffset(char subtype) {
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

        // Exercise the real RPC consumption loop, including its offset advancement.
        // Inject only the journal and disable diagnostic polling to avoid IBM i access.
        Field retrieval = As400RpcConnection.class.getDeclaredField("retrieveJournal");
        retrieval.setAccessible(true);
        retrieval.set(rpc, journal);
        Field periodic = As400RpcConnection.class.getDeclaredField("periodic");
        periodic.setAccessible(true);
        periodic.set(rpc, mock(periodic.getType()));
        doCallRealMethod().when(rpc).getJournalEntries(any(), any(), any(), any());
        when(journal.retrieveJournal(any())).thenReturn(RetrievalState.Success);
        when(journal.nextEntry()).thenReturn(true, false);
        when(journal.getEntryHeader()).thenReturn(clear);
        when(journal.getPosition()).thenReturn(new JournalProcessedPosition(BigInteger.valueOf(42),
                new JournalReceiver("RCV", "LIB"), SOURCE_TIME, true));
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true, true, false);

        source.execute(context, partition, offset);

        ArgumentCaptor<TruncateException> failure = ArgumentCaptor.forClass(TruncateException.class);
        verify(errorHandler).setProducerThrowable(failure.capture());
        assertThat(failure.getValue()).hasMessageContaining("Failed to dispatch clear").hasMessageContaining("42");
        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(journal, never()).getPosition();
        verify(journal).nextEntry();
    }

    @Test
    void unsupportedClearStopsStreamingThroughErrorHandler() throws Exception {
        doAnswer(i -> {
            As400RpcConnection.BlockingReceiverConsumer consumer = i.getArgument(2);
            consumer.accept(BigInteger.valueOf(42), journal, entry('F', "CR", 42, 7, "TEST"));
            return null;
        }).when(rpc).getJournalEntries(any(), any(), any(), any());
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true);
        source.execute(context, partition, offset);
        verify(errorHandler).setProducerThrowable(any(TruncateException.class));
        assertThat(values).isEmpty();
        assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
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
        assertThat(offset.getTransactionContext()).isNull();
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
