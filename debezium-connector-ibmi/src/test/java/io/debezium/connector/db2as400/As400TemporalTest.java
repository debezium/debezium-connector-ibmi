/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.data.types.AS400Temporal;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.AbstractTopicNamingStrategy;

class As400TemporalTest {
    private static TableSchema outputSchema(As400BindingDefinition definition) {
        var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "TEST")
                .with("time.precision.mode", "adaptive_time_microseconds").build();
        return new As400NativeTableSchemas(List.of(definition), new As400ConnectorConfig(raw), new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
            @Override
            public String dataChangeTopic(TableId id) {
                return "test." + id.table();
            }
        }).schemaFor(definition.schema().id());
    }

    static As400BindingDefinition definition() throws Exception {
        var acquired = new As400NativeBindingAcquisitionTest.Fixture(true).acquire().definitions().get(0);
        var original = acquired.identity();
        // Same explicit identity mapping as the sanitized native header fixture.
        return new As400BindingDefinition(new TableIdentity(original.tableId(), "M", "M", "00000000000000000001", "00000000000000000002",
                original.recordFormat()), acquired.schema(), acquired.codecs());
    }

    static List<Map<String, String>> entries() throws Exception {
        return new ObjectMapper().readValue(As400TemporalTest.class.getResourceAsStream("/native-transactions/temporal-rows.json"), new TypeReference<>() {
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "UTC", "America/Chicago", "Asia/Tokyo" })
    void nativeRowsHistoryNormalizationAndStandardOutputPreserveTimeWithoutTimezone(String zone) throws Exception {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            var definition = definition();
            assertThat(As400BindingDefinition.restore(definition.storedTable())).isEqualTo(definition);
            assertThat(definition.codecs().stream().map(As400BindingDefinition.Codec::kind)).containsExactly(
                    "BIN4", "DATE_ISO", "TIME_ISO", "TIMESTAMP_ISO", "TIMESTAMP_ISO", "TIMESTAMP_ISO", "TIMESTAMP_ISO");
            var schema = outputSchema(definition);
            assertThat(schema.valueSchema().field("TS0").schema().name()).isEqualTo("io.debezium.time.Timestamp");
            assertThat(schema.valueSchema().field("TS3").schema().name()).isEqualTo("io.debezium.time.Timestamp");
            assertThat(schema.valueSchema().field("TS6").schema().name()).isEqualTo("io.debezium.time.MicroTimestamp");
            assertThat(schema.valueSchema().field("TS9").schema().name()).isEqualTo("io.debezium.time.NanoTimestamp");
            var limits = new As400TransactionBuffer.Limits(10, 100, 20, 65536);
            try (var buffer = new As400TransactionBuffer("journal", limits);
                    var ordinary = new As400AutocommitBuffer("journal", limits, buffer::usage)) {
                var normalizer = new As400SourceNormalizer(buffer, (file, library) -> definition.binding(1024), 1024,
                        (source, control) -> SavepointRollback.UNRESOLVED, ordinary);
                int completed = 0;
                for (var entry : entries()) {
                    byte[] bytes = HexFormat.of().parseHex(entry.get("hex"));
                    var header = new EntryHeaderDecoder().decode(bytes, 0);
                    int id = java.nio.ByteBuffer.wrap(bytes, (int) header.getEntrySpecificDataOffset() + 16, 4).getInt();
                    if (id >= 4) {
                        // Native timestamp(9) values outside the signed epoch-nanos
                        // range must reject, not wrap, even in a nullable column.
                        assertThrows(IOException.class, () -> definition.binding(1024).values(header, bytes, 0));
                        continue;
                    }
                    Object[] decoded = definition.binding(1024).values(header, bytes, 0);
                    assertThat(decoded[0]).isEqualTo(id);
                    if (id == 2) {
                        assertThat(Arrays.copyOfRange(decoded, 1, decoded.length)).containsOnlyNulls();
                    }
                    else {
                        boolean update = header.getEntryType().equals("UP");
                        assertThat(decoded[1]).isEqualTo(LocalDate.parse(id == 3 ? "1969-12-31" : update ? "2000-02-29" : "2024-02-29"));
                        assertThat(decoded[2]).isEqualTo(Duration.ofSeconds(id == 3 ? 0 : update ? 3723 : 86399));
                        var value = schema.valueFromColumnData(decoded);
                        assertThat(value.getInt64("TS9")).isEqualTo(id == 3 ? -1L : 1709251199123456789L);
                        assertThat(value.getInt64("TS3")).isEqualTo(id == 3 ? -1L : 1709251199123L);
                        assertThat(value.getInt64("TS6")).isEqualTo(id == 3 ? -1L : update ? 951786123654321L : 1709251199123456L);
                    }
                    var batch = normalizer.accept(header, bytes, 0, new JournalPosition(header.getSequenceNumber(), new JournalReceiver("RCV", "LIB")));
                    Arrays.fill(bytes, (byte) 0);
                    if (batch.isPresent()) {
                        var change = batch.orElseThrow().changes().get(0);
                        Object[] owned = change.after() == null ? change.before().values() : change.after().values();
                        assertThat(owned).isEqualTo(decoded);
                        assertThat(change.source().recordInfo().threadId()).isPositive();
                        ordinary.complete(batch.orElseThrow());
                        completed++;
                    }
                }
                assertThat(completed).isEqualTo(5); // Three inserts, paired update and null delete.
            }
        }
        finally {
            TimeZone.setDefault(previous);
        }
    }

    static Stream<Arguments> invalidMetadata() {
        return Stream.of(Arguments.of(1, "DATETIME_PRECISION", null), Arguments.of(1, "DATETIME_PRECISION", 1),
                Arguments.of(1, "DATE_FORMAT", "USA"), Arguments.of(1, "DATE_SEPARATOR", "/"), Arguments.of(1, "CCSID", 1208),
                Arguments.of(1, "STORAGE", 10), Arguments.of(1, "BUFFER_LENGTH", 4), Arguments.of(2, "TIME_FORMAT", "USA"),
                Arguments.of(2, "TIME_SEPARATOR", ":"), Arguments.of(3, "LENGTH", 19), Arguments.of(3, "DATETIME_PRECISION", -1),
                Arguments.of(6, "DATETIME_PRECISION", 12), Arguments.of(6, "NUMERIC_SCALE", 9));
    }

    @ParameterizedTest
    @MethodSource("invalidMetadata")
    void temporalMetadataCannotGuessOrCoerceMissingLayout(int column, String field, Object value) throws Exception {
        var f = new As400NativeBindingAcquisitionTest.Fixture(true);
        f.columns.get(column).put(field, value);
        assertThrows(SQLException.class, f::acquire);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void snapshotUsesExplicitIsoProjectionAndRejectsPrecisionLossBeforeDelivery(boolean invalid) throws Exception {
        var definition = definition();
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        ResultSet rows = mock(ResultSet.class);
        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(rows);
        when(rows.next()).thenReturn(true, false);
        when(rows.getObject(1)).thenReturn(4);
        when(rows.getString(2)).thenReturn("0001-01-01");
        when(rows.getString(3)).thenReturn("24.00.00");
        when(rows.getString(4)).thenReturn("0001-01-01-00.00.00");
        when(rows.getString(5)).thenReturn("0001-01-01-00.00.00.123");
        when(rows.getString(6)).thenReturn(invalid ? "0001-01-01-00.00.00.1234567" : "0001-01-01-00.00.00.123456");
        when(rows.getString(7)).thenReturn("1969-12-31-23.59.59.999999999");
        List<As400TaskInitialization.Row> output = new ArrayList<>();
        var completed = new AtomicBoolean();
        var scanner = new As400NativeSnapshotScanner(connection, new Object(), () -> {
        }, () -> {
        }, s -> {
        }, new As400SnapshotChunks.Limits(256, 100000, 1000, 1000000));
        var consumer = new As400SnapshotChunks.Consumer() {
            @Override
            public void chunk(TableId table, List<As400TaskInitialization.Row> chunk) {
                output.addAll(chunk);
            }

            @Override
            public void complete(TableId table) {
                completed.set(true);
            }
        };
        if (invalid) {
            assertThrows(IllegalArgumentException.class, () -> scanner.scan(List.of(definition), consumer));
            assertThat(output).isEmpty();
            assertThat(completed).isFalse();
        }
        else {
            scanner.scan(List.of(definition), consumer);
            assertThat(output).hasSize(1);
            assertThat(output.get(0).values().values()[1]).isEqualTo(LocalDate.of(1, 1, 1));
            assertThat(output.get(0).values().values()[2]).isEqualTo(Duration.ofDays(1));
            assertThat(output.get(0).values().values()[5]).isEqualTo(LocalDateTime.of(1, 1, 1, 0, 0, 0, 123456000));
            assertThat(completed).isTrue();
        }
        verify(statement).executeQuery(
                "SELECT \"ID\",CHAR(\"D\", ISO),CHAR(\"T\", ISO),CHAR(\"TS0\"),CHAR(\"TS3\"),CHAR(\"TS6\"),CHAR(\"TS9\") FROM \"LIB\".\"M\" T ORDER BY \"ID\"");
        verify(rows).close();
        verify(statement).close();
    }

    @ParameterizedTest
    @ValueSource(strings = { "2023-02-29", "0000-01-01", "2024/02/29", "2024-02-29 ", "2024-13-01" })
    void malformedDatesNeverBecomeDefaultedValues(String date) {
        assertThrows(RuntimeException.class, () -> new AS400Temporal(AS400Temporal.Kind.DATE, 0, 37).parse(date));
    }

    @ParameterizedTest
    @ValueSource(strings = { "24.00.01", "25.00.00", "23.60.00", "23.59.60", "23:59:59" })
    void invalidOrUnqualifiedTimesCannotWrapToMidnight(String time) {
        assertThrows(IllegalArgumentException.class, () -> new AS400Temporal(AS400Temporal.Kind.TIME, 0, 37).parse(time));
    }

    @Test
    void yearRangeEndOfDayAndPrecisionRemainLossless() {
        var time = new AS400Temporal(AS400Temporal.Kind.TIME, 0, 37);
        assertThat(time.parse("24.00.00")).isEqualTo(Duration.ofDays(1));
        assertThrows(IllegalArgumentException.class, () -> time.validate(Duration.ofNanos(1)));
        var timestamp = new AS400Temporal(AS400Temporal.Kind.TIMESTAMP, 6, 37);
        assertThat(timestamp.parse("9999-12-31-23.59.59.999999")).isEqualTo(LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000));
        assertThrows(IllegalArgumentException.class, () -> timestamp.validate(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 1)));
        assertThrows(IllegalArgumentException.class, () -> new AS400Temporal(AS400Temporal.Kind.TIMESTAMP, 12, 37));
        assertThrows(IllegalArgumentException.class, () -> As400BindingDefinition.Codec.decode("TIMESTAMP_ISO:32:12:37"));
        var precise = new AS400Temporal(AS400Temporal.Kind.TIMESTAMP, 9, 37);
        assertThrows(ArithmeticException.class, () -> precise.parse("9999-12-31-23.59.59.999999999"));
        assertThrows(IllegalArgumentException.class, () -> timestamp.parse("2024-02-29-24.00.00.000000"));
    }

    @Test
    void standardOutputRejectsNullablePrecisionLossAndSharedConverterRangeFailures() throws Exception {
        var schema = outputSchema(definition());
        Object[] row = { 1, null, null, null, null, null, null };
        row[5] = LocalDateTime.of(2024, 1, 1, 0, 0, 0, 1);
        assertThrows(IllegalArgumentException.class, () -> schema.valueFromColumnData(row));
        row[5] = null;
        row[2] = java.sql.Time.valueOf("01:02:03");
        assertThrows(IllegalArgumentException.class, () -> schema.valueFromColumnData(row));
        row[2] = Duration.ofDays(1);
        assertThat(schema.valueFromColumnData(row).getInt64("T")).isEqualTo(86_400_000_000L);
        row[6] = LocalDateTime.ofEpochSecond(Math.floorDiv(Long.MIN_VALUE, 1_000_000_000), (int) Math.floorMod(Long.MIN_VALUE, 1_000_000_000), ZoneOffset.UTC);
        // Current core rejects this partial day despite its representable nanos.
        // Keep that limitation explicit; nullable output must not hide it.
        assertThrows(IllegalArgumentException.class, () -> schema.valueFromColumnData(row));
        row[6] = LocalDateTime.of(1677, 9, 22, 0, 0);
        assertThat(schema.valueFromColumnData(row).getInt64("TS9")).isEqualTo(-9223286400000000000L);
        row[6] = LocalDateTime.ofEpochSecond(Math.floorDiv(Long.MAX_VALUE, 1_000_000_000), (int) Math.floorMod(Long.MAX_VALUE, 1_000_000_000), ZoneOffset.UTC);
        assertThat(schema.valueFromColumnData(row).getInt64("TS9")).isEqualTo(Long.MAX_VALUE);
        row[6] = ((LocalDateTime) row[6]).plusNanos(1);
        assertThrows(ArithmeticException.class, () -> schema.valueFromColumnData(row));
    }
}
