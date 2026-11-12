/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400OperatorTableEvidence.Format;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Limits;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Request;
import io.debezium.connector.db2as400.conversion.SchemaInfoConversion;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF;
import io.debezium.relational.TableId;

class As400NativeBindingAcquisitionTest {
    @Test
    void shouldAcquireTheExactRemainingColumnsWhenTheNativeVaryingFlagSurvivesDropColumn() throws Exception {
        var f = new Fixture();
        f.columns.subList(1, f.columns.size()).clear();
        f.outputColumns.subList(1, f.outputColumns.size()).clear();
        var original = f.observation;
        // Isolated native ADD VARCHAR / DROP COLUMN capture: SYSFILES retains
        // YES, while SYSCOLUMNS2 and the actual R/PT payload contain only ID.
        var remaining = new Observation(original.request(), original.table(), original.member(), original.fileObject(), original.memberObject(),
                new Format("M", "29187D5938DDA", 4, 1, true));
        when(f.tables.collect(anyList())).thenReturn(List.of(remaining));
        var acquired = f.acquire().definitions().get(0);
        assertThat(acquired.schema().columns()).extracting(io.debezium.relational.Column::name).containsExactly("ID");
        assertThat(acquired.identity().recordFormat()).isEqualTo("M:29187D5938DDA:4:1:true");
        var restored = As400BindingDefinition.restore(acquired.storedTable());
        assertThat(restored.codecs()).hasSize(1);
        assertThat(restored.codecs().get(0).decoder().toObject(java.util.HexFormat.of().parseHex("00000014"))).isEqualTo(20);
        // Native shape and complete column strides remain mandatory.
        f.columns.get(0).put("BUFFER_LENGTH", 8);
        assertThrows(SQLException.class, f::acquire);
    }

    @Test
    void shouldPreserveLegacyTextSubstitutionAndItsOwnedRecoveryLayout() throws Exception {
        var f = new Fixture();
        var original = f.acquire().definitions().get(0);
        var changed = new As400NativeBindingAcquisition(() -> f.connection, f.tables, new Limits(4, 5), false, table -> true,
                new As400NativeBindingAcquisition.CcsidMapping(37, 500)).collect(List.of(f.observation.request())).definitions().get(0);
        assertThat(changed.schema()).isEqualTo(original.schema());
        assertThat(changed.identity()).isEqualTo(original.identity());
        // These exact EBCDIC 37 bytes are []^! plus space. The configured
        // interpretation must survive recovery even though the catalog stays 37.
        byte[] text = java.util.HexFormat.of().parseHex("babbb05a40");
        assertThat(original.codecs().get(4).decoder().toObject(text)).isEqualTo("[]^! ");
        assertThat(changed.codecs().get(4).decoder().toObject(text)).isEqualTo("¬|¢] ");
        var restored = As400BindingDefinition.restore(changed.storedTable());
        assertThat(restored).isEqualTo(changed);
        assertThat(restored.codecs().get(4).decoder().toObject(text)).isEqualTo("¬|¢] ");
        for (int i = 0; i < original.codecs().size(); i++) {
            assertThat(changed.codecs().get(i).decoder().getByteLength()).isEqualTo(original.codecs().get(i).decoder().getByteLength());
            if (!List.of("TEXT", "VARCHAR", "VARGRAPHIC").contains(original.codecs().get(i).kind())) {
                assertThat(changed.codecs().get(i)).isEqualTo(original.codecs().get(i));
            }
        }
        var varying = new As400NativeBindingAcquisition(() -> f.connection, f.tables, new Limits(4, 5), false, table -> true,
                new As400NativeBindingAcquisition.CcsidMapping(500, 37)).collect(List.of(f.observation.request())).definitions().get(0);
        byte[] prefixed = java.util.HexFormat.of().parseHex("0005babbb05a400000");
        assertThat(original.codecs().get(5).decoder().toObject(prefixed)).isEqualTo("¬|¢] ");
        assertThat(varying.codecs().get(5).decoder().toObject(prefixed)).isEqualTo("[]^! ");
        assertThat(varying.codecs().get(4)).isEqualTo(original.codecs().get(4));
        // An override cannot make a missing physical encoding valid.
        f.columns.get(4).put("CCSID", null);
        assertThrows(SQLException.class, () -> new As400NativeBindingAcquisition(() -> f.connection, f.tables, new Limits(4, 5), false, table -> true,
                new As400NativeBindingAcquisition.CcsidMapping(37, 500)).collect(List.of(f.observation.request())));
    }

    @Test
    void shouldKeepTemporalDecodingIndependentOfTextSubstitution() throws Exception {
        var f = new Fixture(true);
        var original = f.acquire().definitions().get(0);
        var changed = new As400NativeBindingAcquisition(() -> f.connection, f.tables, new Limits(4, 5), false, table -> true,
                new As400NativeBindingAcquisition.CcsidMapping(37, 500)).collect(List.of(f.observation.request())).definitions().get(0);
        assertThat(changed).isEqualTo(original);
    }

    @ParameterizedTest
    @CsvSource({ "4,CHAR,5,65535,5,BINARY", "5,VARCHAR,7,65535,7,VARBINARY",
            "4,GRAPHIC,3,1200,6,TEXT", "5,VARG,4,1200,8,VARGRAPHIC",
            "4,GRAPHIC,3,13488,6,TEXT", "5,VARG,4,13488,8,VARGRAPHIC", "5,VARCHAR,7,1208,7,VARCHAR" })
    void shouldAcquireBinaryAndUnicodeLayoutsWithDurablePhysicalWidths(int index, String type, int length, int ccsid, int octets, String kind) throws Exception {
        var f = new Fixture();
        var column = f.columns.get(index);
        column.putAll(Map.of("DATA_TYPE", type, "LENGTH", length, "CCSID", ccsid, "CHARACTER_OCTET_LENGTH", octets));
        int bytes = octets + (type.equals("VARG") || type.equals("VARCHAR") ? 2 : 0);
        column.put("BUFFER_LENGTH", bytes);
        column.put("STORAGE", bytes);
        f.outputColumns.set(index, jdbcColumn(column));
        int position = 1;
        for (var current : f.columns) {
            current.put("BUFFER_POSITION", position);
            position += ((Number) current.get("BUFFER_LENGTH")).intValue();
        }
        var original = f.observation;
        var observation = new Observation(original.request(), original.table(), original.member(), original.fileObject(), original.memberObject(),
                new Format("M", "29087D5938DDA", position - 1, f.columns.size(), true));
        when(f.tables.collect(anyList())).thenReturn(List.of(observation));
        var definition = f.acquire().definitions().get(0);
        assertThat(definition.codecs().get(index).kind()).isEqualTo(kind);
        assertThat(definition.codecs().get(index).decoder().getByteLength()).isEqualTo(bytes);
        assertThat(definition.schema().columns().get(index).jdbcType()).isEqualTo(switch (kind) {
            case "BINARY" -> Types.BINARY;
            case "VARBINARY" -> Types.VARBINARY;
            case "VARGRAPHIC" -> Types.NVARCHAR;
            case "TEXT" -> Types.NCHAR;
            default -> Types.VARCHAR;
        });
        assertThat(As400BindingDefinition.restore(definition.storedTable())).isEqualTo(definition);
        // Reject an unproven stride even if the resulting whole row could fit.
        column.put("CHARACTER_OCTET_LENGTH", octets + 1);
        assertThrows(SQLException.class, f::acquire);
    }

    @Test
    void shouldPreserveSqlNamesAndPhysicalFileIdentityAcrossAcquisitionAndHistory() throws Exception {
        var f = new Fixture();
        var original = f.observation;
        var named = new Observation(original.request(), new TableId(null, "LIB", "LONG_ORDER_TABLE"), original.member(),
                original.fileObject(), original.memberObject(), original.format());
        when(f.tables.collect(anyList())).thenReturn(List.of(named));
        f.columns.forEach(row -> row.put("TABLE_NAME", "LONG_ORDER_TABLE"));
        f.outputColumns.forEach(row -> row.put("TABLE_NAME", "LONG_ORDER_TABLE"));
        f.controls.get(0).put("TABLE_NAME", "LONG_ORDER_TABLE");
        var definition = f.acquire().definitions().get(0);
        assertThat(definition.identity().physicalFile()).isEqualTo("M");
        assertThat(definition.schema().id().table()).isEqualTo("LONG_ORDER_TABLE");
        assertThat(As400BindingDefinition.restore(definition.storedTable())).isEqualTo(definition);
        for (var statement : f.statements) {
            verify(statement).setString(2, "LONG_ORDER_TABLE");
        }
        verify(f.metadata).getColumns(null, "LIB", "LONG\\_ORDER\\_TABLE", null);
    }

    @Test
    void shouldUseTheLegacyPhysicalFileKeyFallbackOnlyWhenSqlKeysAreAbsent() throws Exception {
        var f = new Fixture();
        f.keys.clear();
        f.ddsKeys.add(Map.of("COLUMN_NAME", "C", "ORDINAL_POSITION", 1));
        var definition = f.acquire().definitions().get(0);
        assertThat(definition.schema().primaryKeyColumnNames()).containsExactly("C");
        assertThat(As400BindingDefinition.restore(definition.storedTable())).isEqualTo(definition);
        var sql = new Fixture();
        sql.ddsKeys.add(Map.of("COLUMN_NAME", "UNKNOWN", "ORDINAL_POSITION", 1));
        assertThat(sql.acquire().definitions().get(0).schema().primaryKeyColumnNames()).containsExactly("ID");
        verify(sql.connection, never()).prepareStatement(As400NativeBindingAcquisition.DDS_KEYS_SQL);
    }

    @ParameterizedTest
    @ValueSource(strings = { "unknown", "duplicate", "missing-position", "warning", "failure" })
    void shouldRejectAmbiguousOrIncompletePhysicalFileKeys(String fault) throws Exception {
        var f = new Fixture();
        f.keys.clear();
        f.ddsKeys.add(Map.of("COLUMN_NAME", fault.equals("unknown") ? "UNKNOWN" : "ID", "ORDINAL_POSITION", fault.equals("missing-position") ? 2 : 1));
        if (fault.equals("duplicate")) {
            f.ddsKeys.add(Map.of("COLUMN_NAME", "ID", "ORDINAL_POSITION", 2));
        }
        if (fault.equals("warning") || fault.equals("failure")) {
            var statement = mock(PreparedStatement.class);
            when(f.connection.prepareStatement(As400NativeBindingAcquisition.DDS_KEYS_SQL)).thenReturn(statement);
            if (fault.equals("warning")) {
                when(statement.getWarnings()).thenReturn(new SQLWarning("Partial keys"));
            }
            else {
                when(statement.executeQuery()).thenThrow(new SQLException("Cannot read keys"));
            }
        }
        assertThrows(SQLException.class, f::acquire);
    }

    @Test
    void shouldRetainExcludedLobIdentityWithoutEnablingLobValueCapture() throws Exception {
        var f = new Fixture();
        var old = f.observation;
        var shape = old.format();
        var lob = new Observation(old.request(), old.table(), old.member(), old.fileObject(), old.memberObject(),
                new Format(shape.name(), shape.level(), shape.recordBytes(), shape.fields(), shape.varying(), 1, true));
        when(f.tables.collect(anyList())).thenReturn(List.of(lob));
        assertThrows(SQLException.class, f::acquire);
        var excluded = new As400NativeBindingAcquisition(() -> f.connection, f.tables, new Limits(4, 5), false, table -> false)
                .collect(List.of(lob.request())).definitions().get(0);
        assertThat(excluded.excluded()).isTrue();
        assertThat(excluded.identity()).isEqualTo(lob.tableIdentity());
        assertThat(excluded.schema().columns()).isEmpty();
        var restored = As400BindingDefinition.restore(excluded.storedTable());
        assertThat(restored).isEqualTo(excluded);
        assertThat(restored.binding(65536).excludedLob()).isTrue();
        verify(f.connection, never()).prepareStatement(anyString());
    }

    @Test
    void shouldRetainNativeIdentitiesWithoutAcquiringExcludedColumnLayouts() throws Exception {
        var f = new Fixture();
        f.columns.get(0).put("DATA_TYPE", "GRAPHIC");
        assertThrows(SQLException.class, f::acquire);
        var excluded = new Fixture();
        excluded.columns.get(0).put("DATA_TYPE", "GRAPHIC");
        var acquired = new As400NativeBindingAcquisition(() -> excluded.connection, excluded.tables, new Limits(4, 5), false, table -> false)
                .collect(List.of(excluded.observation.request()));
        var definition = acquired.definitions().get(0);
        assertThat(definition.excluded()).isTrue();
        assertThat(definition.identity()).isEqualTo(excluded.observation.tableIdentity());
        assertThat(definition.codecs()).isEmpty();
        assertThat(definition.schema().columns()).isEmpty();
        assertThat(As400BindingEncoding.decode(As400BindingEncoding.encode(List.of(definition), false))).containsExactly(definition);
        verify(excluded.connection, never()).prepareStatement(anyString());
        verify(excluded.tables, times(2)).collect(List.of(excluded.observation.request()));
    }

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
            verify(st).setMaxRows(st == f.statements.get(0) ? 2 : f.observation.format().fields() + 1);
            verify(st).setQueryTimeout(5);
            verify(st).setString(1, "LIB");
            verify(st).setString(2, "M");
            verify(st).close();
        }
        verify(f.connection, never()).commit();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRetainAnExcludedSchemaOnlyWhenItsCompletePhysicalLayoutIsAvailable(boolean unsupported) throws Exception {
        var f = new Fixture();
        if (unsupported) {
            f.columns.get(0).put("DATA_TYPE", "GRAPHIC");
        }
        var acquired = new As400NativeBindingAcquisition(() -> f.connection, f.tables, new Limits(4, 5), false, table -> false,
                new As400NativeBindingAcquisition.CcsidMapping(-1, -1), true).collect(List.of(f.observation.request()));
        var definition = acquired.definitions().get(0);
        assertThat(definition.excluded()).isEqualTo(unsupported);
        assertThat(definition.identity()).isEqualTo(f.observation.tableIdentity());
        assertThat(definition.codecs()).hasSize(unsupported ? 0 : 11);
        assertThat(As400BindingEncoding.decode(As400BindingEncoding.encode(List.of(definition), false))).containsExactly(definition);
        verify(f.tables, times(2)).collect(List.of(f.observation.request()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "42501", "08006", "HY008", "HYT00" })
    void shouldDistinguishUnavailableExcludedMetadataFromInterruptedOrFailedAcquisition(String state) throws Exception {
        var f = new Fixture();
        when(f.connection.prepareStatement(As400NativeBindingAcquisition.COLUMNS_SQL)).thenThrow(new SQLException("Unavailable metadata", state));
        var acquisition = new As400NativeBindingAcquisition(() -> f.connection, f.tables, new Limits(4, 5), false, table -> false,
                new As400NativeBindingAcquisition.CcsidMapping(-1, -1), true);
        if (state.equals("42501")) {
            assertThat(acquisition.collect(List.of(f.observation.request())).definitions()).singleElement()
                    .satisfies(definition -> assertThat(definition.excluded()).isTrue());
            verify(f.tables, times(2)).collect(List.of(f.observation.request()));
        }
        else {
            assertThat(assertThrows(SQLException.class, () -> acquisition.collect(List.of(f.observation.request()))).getSQLState()).isEqualTo(state);
            verify(f.tables, times(1)).collect(List.of(f.observation.request()));
        }
        verify(f.connection).close();
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
    void shouldPreserveKeylessTablesAndUseRrnOnlyWhenExplicitlyRequested() throws Exception {
        Fixture keyless = new Fixture();
        keyless.keys.clear();
        var acquired = keyless.acquire().definitions().get(0);
        assertThat(acquired.outputTable().primaryKeyColumnNames()).isEmpty();
        assertThat(acquired.outputTable().columns()).hasSize(11);
        assertThat(acquired.usesRrn()).isFalse();
        assertThat(As400BindingDefinition.restore(acquired.storedTable())).isEqualTo(acquired);
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
        collision.outputColumns.get(0).put("COLUMN_NAME", As400BindingDefinition.RRN_COLUMN);
        assertThat(assertThrows(SQLException.class, () -> collision.acquire(true)).getCause()).hasMessage("Native column collides with relative record key");
    }

    @Test
    void shouldPersistLegacyOutputPrecisionSeparatelyFromPhysicalWidths() throws Exception {
        var f = new Fixture();
        var acquired = f.acquire().definitions().get(0);
        var integer = acquired.schema().columnWithName("ID");
        assertThat(integer.length()).isEqualTo(10);
        assertThat(acquired.codecs().get(0).size()).isEqualTo(4);
        assertThat(acquired.schema().columnWithName("C").scale()).contains(0);
        assertThat(As400BindingEncoding.decode(As400BindingEncoding.encode(List.of(acquired), false))).containsExactly(acquired);

        var columns = new ArrayList<SchemaCacheIF.Structure>();
        try (var rows = rows(f.outputColumns)) {
            while (rows.next()) {
                columns.add(JdbcFileDecoder.readColumnMetadata(rows));
            }
        }
        var ordinary = SchemaInfoConversion.tableInfo2Table(null, "LIB", "M",
                new SchemaCacheIF.TableInfo(columns, List.of("ID"), null));
        assertThat(acquired.schema()).isEqualTo(ordinary);
        verify(f.metadata).getColumns(isNull(), anyString(), anyString(), isNull());

        // Upgrading must not silently reinterpret an already durable generation.
        var old = new As400BindingDefinition(acquired.identity(), acquired.schema().edit()
                .addColumn(integer.edit().length(4).create()).create(), acquired.codecs());
        assertThat(As400BindingDefinition.restore(old.storedTable()).schema().columnWithName("ID").length()).isEqualTo(4);
    }

    @Test
    void shouldAcquireAndRestoreAllEightThousandNativeFields() throws Exception {
        var f = new Fixture();
        var template = new HashMap<>(f.columns.get(1));
        template.put("IS_NULLABLE", "N");
        f.columns.clear();
        f.outputColumns.clear();
        for (int i = 1; i <= 8000; i++) {
            var column = new HashMap<>(template);
            column.putAll(Map.of("COLUMN_NAME", "C" + i, "SYSTEM_COLUMN_NAME", "C" + i, "ORDINAL_POSITION", i, "BUFFER_POSITION", 2 * i - 1));
            f.columns.add(column);
            f.outputColumns.add(jdbcColumn(column));
        }
        f.keys.get(0).put("COLUMN_NAME", "C1");
        var observed = f.observation;
        var wide = new Observation(observed.request(), observed.table(), observed.member(), observed.fileObject(), observed.memberObject(),
                new Format("M", "29087D5938DDA", 16000, 8000, false));
        when(f.tables.collect(anyList())).thenReturn(List.of(wide));
        var acquired = f.acquire().definitions().get(0);
        var restored = As400BindingEncoding.decode(As400BindingEncoding.encode(List.of(acquired), false)).get(0);
        assertThat(restored).isEqualTo(acquired);
        assertThat(restored.schema().columns()).hasSize(8000);
        assertThat(restored.schema().columnWithName("C8000").length()).isEqualTo(5);
        assertThat(restored.codecs().get(7999).size()).isEqualTo(2);
        restored.binding(16000);
        assertThrows(IllegalArgumentException.class, () -> restored.binding(15999));
    }

    @Test
    void shouldPreserveLegacyZeroScaleWhenJdbcDecimalGetterReportsNull() throws Exception {
        var f = new Fixture();
        f.outputColumns.get(4).put("DECIMAL_DIGITS", null);
        f.outputColumns.get(6).put("DECIMAL_DIGITS", null);
        var output = rows(f.outputColumns);
        when(f.metadata.getColumns(null, "LIB", "M", null)).thenReturn(output);
        var definition = f.acquire().definitions().get(0);
        assertThat(definition.schema().columnWithName("C").scale()).contains(0);
        assertThat(definition.schema().columnWithName("B").scale()).contains(0);
        assertThat(definition.schema().columnWithName("D").scale()).contains(3);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "extra", "table", "schema", "column", "order", "type", "nullable", "unknown-nullable", "identity",
            "unknown-generated", "unknown-type", "size", "missing-size", "decimal-scale", "warning", "query-failure", "close-failure", "escape" })
    void shouldRejectIncompleteOrConflictingJdbcOutputEvidence(String fault) throws Exception {
        var f = new Fixture();
        switch (fault) {
            case "missing" -> f.outputColumns.remove(1);
            case "extra" -> f.outputColumns.add(f.outputColumns.get(0));
            case "table" -> f.outputColumns.get(0).put("TABLE_NAME", "OTHER");
            case "schema" -> f.outputColumns.get(0).put("TABLE_SCHEM", "OTHER");
            case "column" -> f.outputColumns.get(0).put("COLUMN_NAME", "OTHER");
            case "order" -> f.outputColumns.get(0).put("ORDINAL_POSITION", 2);
            case "type" -> f.outputColumns.get(0).put("DATA_TYPE", Types.REAL);
            case "nullable" -> f.outputColumns.get(0).put("NULLABLE", 1);
            case "unknown-nullable" -> f.outputColumns.get(0).put("NULLABLE", 2);
            case "identity" -> f.outputColumns.get(0).put("IS_AUTOINCREMENT", "YES");
            case "unknown-generated" -> f.outputColumns.get(0).put("IS_GENERATEDCOLUMN", "");
            case "unknown-type" -> f.outputColumns.get(0).put("TYPE_NAME", "");
            case "size" -> f.outputColumns.get(0).put("COLUMN_SIZE", 0);
            case "missing-size" -> f.outputColumns.get(0).remove("COLUMN_SIZE");
            case "decimal-scale" -> f.outputColumns.get(3).put("DECIMAL_DIGITS", 0);
            case "escape" -> when(f.metadata.getSearchStringEscape()).thenReturn("");
            case "query-failure" -> when(f.metadata.getColumns(null, "LIB", "M", null)).thenThrow(new SQLException("metadata unavailable"));
            default -> {
                var rows = rows(f.outputColumns);
                when(f.metadata.getColumns(null, "LIB", "M", null)).thenReturn(rows);
                if (fault.equals("warning")) {
                    when(rows.getWarnings()).thenReturn(new SQLWarning("partial"));
                }
                else {
                    org.mockito.Mockito.doThrow(new SQLException("close failed")).when(rows).close();
                }
            }
        }
        assertThrows(SQLException.class, f::acquire);
        verify(f.connection).close();
    }

    // Independent JDBC-shaped descriptors. These represent logical precision and
    // driver type names, not the physical layout query returned by SYSCOLUMNS2.
    static Map<String, Object> jdbcColumn(Map<String, Object> physical) {
        var row = new HashMap<String, Object>();
        row.put("TABLE_SCHEM", physical.get("TABLE_SCHEMA"));
        row.put("TABLE_NAME", physical.get("TABLE_NAME"));
        row.put("COLUMN_NAME", physical.get("COLUMN_NAME"));
        row.put("ORDINAL_POSITION", physical.get("ORDINAL_POSITION"));
        row.put("NULLABLE", "Y".equals(physical.get("IS_NULLABLE")) ? 1 : 0);
        row.put("IS_AUTOINCREMENT", physical.get("IS_IDENTITY"));
        row.put("IS_GENERATEDCOLUMN", "NO");
        String type = physical.get("DATA_TYPE").toString().strip();
        int length = ((Number) physical.get("LENGTH")).intValue();
        int jdbc = switch (type) {
            case "INTEGER" -> Types.INTEGER;
            case "SMALLINT" -> Types.SMALLINT;
            case "BIGINT" -> Types.BIGINT;
            case "FLOAT" -> length == 4 ? Types.REAL : Types.DOUBLE;
            case "NUMERIC" -> Types.NUMERIC;
            case "DECIMAL" -> Types.DECIMAL;
            case "CHAR" -> Integer.valueOf(65535).equals(physical.get("CCSID")) ? Types.BINARY : Types.CHAR;
            case "VARCHAR" -> Integer.valueOf(65535).equals(physical.get("CCSID")) ? Types.VARBINARY : Types.VARCHAR;
            case "BINARY" -> Types.BINARY;
            case "VARBIN" -> Types.VARBINARY;
            case "GRAPHIC" -> Types.NCHAR;
            case "VARG" -> Types.NVARCHAR;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "TIMESTMP" -> Types.TIMESTAMP;
            default -> throw new AssertionError(type);
        };
        String name = switch (jdbc) {
            case Types.NCHAR -> "NCHAR";
            case Types.NVARCHAR -> "NVARCHAR";
            case Types.REAL -> "REAL";
            case Types.DOUBLE -> "DOUBLE";
            case Types.TIMESTAMP -> "TIMESTAMP";
            default -> type.equals("CHAR") || type.equals("VARCHAR")
                    ? type + (Integer.valueOf(65535).equals(physical.get("CCSID")) ? " () FOR BIT DATA" : "")
                    : type;
        };
        int precision = switch (jdbc) {
            case Types.INTEGER -> 10;
            case Types.SMALLINT -> 5;
            case Types.BIGINT -> 19;
            case Types.REAL -> 7;
            case Types.DOUBLE -> 15;
            case Types.DATE, Types.TIME, Types.TIMESTAMP -> ((Number) physical.get("BUFFER_LENGTH")).intValue();
            default -> length;
        };
        row.put("DATA_TYPE", jdbc);
        row.put("TYPE_NAME", name);
        row.put("COLUMN_SIZE", precision);
        row.put("DECIMAL_DIGITS", physical.get("NUMERIC_SCALE") != null ? physical.get("NUMERIC_SCALE")
                : physical.get("DATETIME_PRECISION") != null ? physical.get("DATETIME_PRECISION") : 0);
        return row;
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
                Arguments.of(4, "CCSID", 65536), Arguments.of(4, "CHARACTER_OCTET_LENGTH", 10), Arguments.of(5, "CCSID", 0),
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
    @ValueSource(strings = { "missing-column", "extra-column", "duplicate-key", "wrong-key", "key-order", "warning", "changed", "not-read-only" })
    void rejectsIncompleteOrUnstableEvidence(String fault) throws Exception {
        Fixture f = new Fixture();
        switch (fault) {
            case "missing-column" -> f.columns.remove(1);
            case "extra-column" -> f.columns.add(f.columns.get(0));
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
        var jdbcNames = Map.of(4, "COLUMN_NAME", 5, "DATA_TYPE", 6, "TYPE_NAME", 7, "COLUMN_SIZE", 9, "DECIMAL_DIGITS", 11, "NULLABLE",
                17, "ORDINAL_POSITION", 23, "IS_AUTOINCREMENT");
        when(result.getString(anyInt())).thenAnswer(i -> {
            Object value = values.get(index.get()).get(jdbcNames.get(i.getArgument(0)));
            return value == null ? null : value.toString();
        });
        when(result.getObject(anyInt())).thenAnswer(i -> values.get(index.get()).get(jdbcNames.get(i.getArgument(0))));
        when(result.getInt(anyInt())).thenAnswer(i -> {
            Object value = values.get(index.get()).get(jdbcNames.get(i.getArgument(0)));
            return value == null ? 0 : ((Number) value).intValue();
        });
        return result;
    }

    static class Fixture {
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        final As400OperatorTableEvidence tables = mock(As400OperatorTableEvidence.class);
        final List<PreparedStatement> statements = new ArrayList<>();
        final List<Map<String, Object>> columns;
        final List<Map<String, Object>> keys;
        final List<Map<String, Object>> outputColumns;
        final List<Map<String, Object>> ddsKeys = new ArrayList<>();
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
            outputColumns = new ArrayList<>(columns.stream().map(As400NativeBindingAcquisitionTest::jdbcColumn).toList());
            when(connection.getMetaData()).thenReturn(metadata);
            when(metadata.getSearchStringEscape()).thenReturn("\\");
            when(metadata.getColumns(isNull(), anyString(), anyString(), isNull())).thenAnswer(i -> rows(outputColumns));
            var original = new As400OperatorTableEvidenceTest.Fixture().collect().get(0);
            observation = new Observation(new Request(original.request().journal(), "LIB", "M"), new TableId(null, "LIB", "M"), "M",
                    original.fileObject(), original.memberObject(), temporal ? new Format("M", "40BFA08CB67A6", 119, 7, false)
                            : new Format("M", "29087D5938DDA", 65, 11, true));
            when(tables.collect(anyList())).thenReturn(List.of(observation));
            when(connection.isReadOnly()).thenReturn(true);
            when(connection.prepareStatement(anyString())).thenAnswer(i -> {
                String sql = i.getArgument(0);
                assertThat(sql).isIn(As400NativeBindingAcquisition.COLUMNS_SQL, As400NativeBindingAcquisition.KEYS_SQL,
                        As400NativeBindingAcquisition.DDS_KEYS_SQL, As400NativeBindingAcquisition.CONTROLS_SQL);
                PreparedStatement st = mock(PreparedStatement.class);
                statements.add(st);
                when(st.executeQuery()).thenAnswer(q -> rows(sql.equals(As400NativeBindingAcquisition.COLUMNS_SQL) ? columns
                        : sql.equals(As400NativeBindingAcquisition.KEYS_SQL) ? keys : sql.equals(As400NativeBindingAcquisition.DDS_KEYS_SQL) ? ddsKeys : controls));
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
