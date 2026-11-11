/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400OperatorTableEvidence.Format;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Limits;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Request;
import io.debezium.relational.TableId;

class As400NativeBindingAcquisitionTest {
    @Test
    void capturedCatalogProvidesOwnedPhysicalCodecsKeysAndNullability() throws Exception {
        Fixture f = new Fixture();
        var acquired = f.acquire();
        var definition = acquired.definitions().get(0);
        assertThat(definition.codecs().stream().map(As400BindingDefinition.Codec::kind)).containsExactly(
                "BIN4", "BIN2", "ZONED", "PACKED", "TEXT", "VARCHAR", "BINARY", "VARBINARY", "BIN8", "FLOAT4", "FLOAT8");
        assertThat(definition.schema().primaryKeyColumnNames()).containsExactly("ID");
        assertThat(definition.schema().columns().stream().map(c -> c.isOptional())).containsExactly(false, true, true, true, true, true, true, true, true, true, true);
        assertThat(As400BindingDefinition.restore(definition.storedTable())).isEqualTo(definition);
        verify(f.tables, times(2)).collect(List.of(f.observation.request()));
        verify(f.connection).close();
        for (PreparedStatement st : f.statements) {
            verify(st).setMaxRows(st == f.statements.get(0) ? 2 : 4097);
            verify(st).setQueryTimeout(5);
            verify(st).setString(1, "LIB");
            verify(st).setString(2, "M");
            verify(st).close();
        }
        verify(f.connection, never()).commit();
    }

    @ParameterizedTest
    @ValueSource(strings = { "R", "C", "B", "?", "null", "missing", "duplicate", "wrong-table" })
    void shouldRejectAccessControlWithoutAcquiringOrPublishingARowLayout(String fault) throws Exception {
        var f = new Fixture();
        switch (fault) {
            case "missing" -> f.controls.clear();
            case "duplicate" -> f.controls.add(f.controls.get(0));
            case "wrong-table" -> f.controls.get(0).put("SYSTEM_TABLE_NAME", "OTHER");
            default -> f.controls.get(0).put("CONTROL", fault.equals("null") ? null : fault);
        }
        assertThrows(SQLException.class, f::acquire);
        verify(f.connection, never()).prepareStatement(As400NativeBindingAcquisition.COLUMNS_SQL);
        verify(f.connection).close();
    }

    @Test
    void missingNativePrimaryKeyRequiresExplicitRrnAndRoundTripsThroughHistory() throws Exception {
        Fixture rejected = new Fixture();
        rejected.keys.clear();
        assertThrows(SQLException.class, rejected::acquire);
        Fixture f = new Fixture();
        f.keys.clear();
        var definition = f.acquire(true).definitions().get(0);
        assertThat(definition.schema().primaryKeyColumnNames()).isEmpty();
        assertThat(definition.schema().columns()).hasSize(11);
        assertThat(definition.outputTable().primaryKeyColumnNames()).containsExactly(As400BindingDefinition.RRN_COLUMN);
        assertThat(definition.outputTable().columns()).hasSize(12);
        assertThat(As400BindingDefinition.restore(definition.storedTable())).isEqualTo(definition);
        assertThat(definition.binding(1024).rrnKey()).isTrue();
    }

    @Test
    void rrnModeStillRejectsCorruptNativeKeyEvidenceAndReservedColumnCollision() throws Exception {
        Fixture f = new Fixture();
        f.keys.get(0).put("COLUMN_NAME", "MISSING");
        assertThrows(SQLException.class, () -> f.acquire(true));
        Fixture collision = new Fixture();
        collision.keys.clear();
        collision.columns.get(0).put("COLUMN_NAME", As400BindingDefinition.RRN_COLUMN);
        assertThat(assertThrows(SQLException.class, () -> collision.acquire(true)).getCause()).hasMessage("Native column collides with relative record key");
    }

    static Stream<Arguments> corrupt() {
        List<Arguments> cases = new ArrayList<>();
        for (String key : List.of("TABLE_SCHEMA", "TABLE_NAME", "SYSTEM_TABLE_SCHEMA", "SYSTEM_TABLE_NAME", "COLUMN_NAME", "SYSTEM_COLUMN_NAME",
                "ORDINAL_POSITION", "DATA_TYPE", "LENGTH", "IS_NULLABLE", "IS_IDENTITY", "HIDDEN", "HAS_FLDPROC", "BUFFER_POSITION", "BUFFER_LENGTH", "STORAGE")) {
            cases.add(Arguments.of(0, key, null));
        }
        cases.addAll(List.of(Arguments.of(1, "BUFFER_POSITION", 4), Arguments.of(1, "ORDINAL_POSITION", 1), Arguments.of(1, "COLUMN_NAME", "ID"),
                Arguments.of(1, "SYSTEM_COLUMN_NAME", "ID"), Arguments.of(0, "IS_NULLABLE", "Y"), Arguments.of(0, "HAS_FLDPROC", "Y"),
                Arguments.of(0, "HIDDEN", "P"), Arguments.of(0, "BUFFER_LENGTH", 3), Arguments.of(0, "STORAGE", 5), Arguments.of(0, "CCSID", 37),
                Arguments.of(3, "NUMERIC_SCALE", null), Arguments.of(3, "NUMERIC_SCALE", 10), Arguments.of(4, "CCSID", null),
                Arguments.of(4, "CCSID", 65535), Arguments.of(4, "CHARACTER_OCTET_LENGTH", 10), Arguments.of(5, "CCSID", 1208),
                Arguments.of(6, "CCSID", 37), Arguments.of(0, "DATA_TYPE", "DATE"), Arguments.of(0, "DATA_TYPE", "DISTINCT"),
                Arguments.of(0, "LENGTH", new BigDecimal("1.5"))));
        return cases.stream();
    }

    @ParameterizedTest
    @MethodSource("corrupt")
    void rejectsMissingAmbiguousOrUnsupportedPhysicalLayout(int index, String key, Object value) throws Exception {
        Fixture f = new Fixture();
        f.columns.get(index).put(key, value);
        assertThrows(SQLException.class, f::acquire);
        verify(f.connection).close();
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-column", "extra-column", "missing-key", "duplicate-key", "wrong-key", "key-order", "warning", "changed", "not-read-only" })
    void rejectsIncompleteOrUnstableEvidence(String fault) throws Exception {
        Fixture f = new Fixture();
        switch (fault) {
            case "missing-column" -> f.columns.remove(1);
            case "extra-column" -> f.columns.add(f.columns.get(0));
            case "missing-key" -> f.keys.clear();
            case "duplicate-key" -> f.keys.add(f.keys.get(0));
            case "wrong-key" -> f.keys.get(0).put("COLUMN_NAME", "MISSING");
            case "key-order" -> f.keys.get(0).put("ORDINAL_POSITION", 2);
            case "warning" -> when(f.connection.getWarnings()).thenReturn(new SQLWarning("partial"));
            case "changed" -> when(f.tables.collect(anyList())).thenReturn(List.of(f.observation), List.of());
            case "not-read-only" -> when(f.connection.isReadOnly()).thenReturn(false);
            default -> throw new AssertionError(fault);
        }
        assertThrows(SQLException.class, f::acquire);
    }

    static ResultSet rows(List<Map<String, Object>> values) throws Exception {
        ResultSet result = mock(ResultSet.class);
        AtomicInteger index = new AtomicInteger(-1);
        when(result.next()).thenAnswer(i -> index.incrementAndGet() < values.size());
        when(result.getString(anyString())).thenAnswer(i -> {
            Object v = values.get(index.get()).get(i.getArgument(0));
            return v == null ? null : v.toString();
        });
        when(result.getBigDecimal(anyString())).thenAnswer(i -> {
            Object v = values.get(index.get()).get(i.getArgument(0));
            return v == null ? null : new BigDecimal(v.toString());
        });
        return result;
    }

    static class Fixture {
        final Connection connection = mock(Connection.class);
        final As400OperatorTableEvidence tables = mock(As400OperatorTableEvidence.class);
        final List<PreparedStatement> statements = new ArrayList<>();
        final List<Map<String, Object>> columns;
        final List<Map<String, Object>> keys;
        // Separate synthetic admission observation: the older column capture
        // did not query SYSTABLES and is not evidence of source RCAC settings.
        final List<Map<String, Object>> controls = new ArrayList<>(List.of(new java.util.HashMap<>(Map.of(
                "TABLE_SCHEMA", "LIB", "SYSTEM_TABLE_SCHEMA", "LIB", "TABLE_NAME", "M", "SYSTEM_TABLE_NAME", "M", "CONTROL", " "))));
        final Observation observation;

        Fixture() throws Exception {
            this(false);
        }

        Fixture(boolean temporal) throws Exception {
            ObjectMapper json = new ObjectMapper();
            columns = json.readValue(
                    getClass().getResourceAsStream(temporal ? "/native-transactions/temporal-columns.json" : "/native-transactions/native-binding-columns-M.json"),
                    new TypeReference<>() {
                    });
            keys = json.readValue(getClass().getResourceAsStream("/native-transactions/native-binding-keys-M.json"), new TypeReference<>() {
            });
            var original = new As400OperatorTableEvidenceTest.Fixture().collect().get(0);
            observation = new Observation(new Request(original.request().journal(), "LIB", "M"), new TableId(null, "LIB", "M"), "M",
                    original.fileObject(), original.memberObject(), temporal ? new Format("M", "40BFA08CB67A6", 119, 7, false)
                            : new Format("M", "29087D5938DDA", 65, 11, true));
            when(tables.collect(anyList())).thenReturn(List.of(observation));
            when(connection.isReadOnly()).thenReturn(true);
            when(connection.prepareStatement(anyString())).thenAnswer(i -> {
                String sql = i.getArgument(0);
                assertThat(sql).isIn(As400NativeBindingAcquisition.COLUMNS_SQL, As400NativeBindingAcquisition.KEYS_SQL, As400NativeBindingAcquisition.CONTROLS_SQL);
                PreparedStatement st = mock(PreparedStatement.class);
                statements.add(st);
                when(st.executeQuery()).thenAnswer(q -> rows(sql.equals(As400NativeBindingAcquisition.COLUMNS_SQL) ? columns
                        : sql.equals(As400NativeBindingAcquisition.KEYS_SQL) ? keys : controls));
                return st;
            });
        }

        As400NativeBindingAcquisition.Acquired acquire() throws Exception {
            return acquire(false);
        }

        As400NativeBindingAcquisition.Acquired acquire(boolean rrn) throws Exception {
            return new As400NativeBindingAcquisition(() -> connection, tables, new Limits(4, 5), rrn).collect(List.of(observation.request()));
        }
    }
}
