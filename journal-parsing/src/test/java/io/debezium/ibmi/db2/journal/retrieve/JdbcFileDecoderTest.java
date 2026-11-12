/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Text;

class JdbcFileDecoderTest {
    private final JdbcFileDecoder decoder = new JdbcFileDecoder(null, null, new SchemaCacheHash(), -1, -1);

    @ParameterizedTest
    @ValueSource(strings = { "NATIVE", "LONG_TABLE" })
    void shouldReloadCharacterEncodingWhenEitherTableAliasIsInvalidated(String alias) throws Exception {
        var f = new MetadataFixture();
        var before = f.load();
        byte[] bytes = new AS400Text(2, 37).toBytes("[]");
        assertArrayEquals(new Object[]{ "[]" }, f.decoder.decodeEntry(before.getAs400Structure(), bytes, 0));
        f.ccsid = 500;
        assertSame(before, f.load());
        f.decoder.clearCache(alias, "LIB");
        var after = f.load();
        assertArrayEquals(new Object[]{ "¬|" }, f.decoder.decodeEntry(after.getAs400Structure(), bytes, 0));
        // A completed reload never mutates a previously acquired schema/codec.
        assertArrayEquals(new Object[]{ "[]" }, f.decoder.decodeEntry(before.getAs400Structure(), bytes, 0));
        assertEquals(2, f.encodingReads);
        verify(f.metadata, times(2)).getColumns(null, "LIB", "LONG_TABLE", null);
    }

    @Test
    void shouldReloadColumnWidthWhenConstructingTheRefreshedDecoder() throws Exception {
        var f = new MetadataFixture();
        assertEquals(2, f.load().getAs400Structure().getByteLength());
        f.type = "NCHAR";
        f.jdbcType = Types.NCHAR;
        f.octets = 4;
        f.ccsid = 13488;
        f.decoder.clearCache("NATIVE", "LIB");
        var after = f.load();
        assertEquals(4, after.getAs400Structure().getByteLength());
        assertArrayEquals(new Object[]{ "AB" }, f.decoder.decodeEntry(after.getAs400Structure(), new AS400Text(4, 13488).toBytes("AB"), 0));
        assertEquals(2, f.encodingReads);
        assertEquals(1, f.widthReads);
    }

    @Test
    void shouldKeepNativeNameMappingsSeparateAcrossLibrariesAndRefreshOnlyTheChangedTable() throws Exception {
        var f = new MetadataFixture();
        assertEquals("LONG_TABLE", f.decoder.getLongName("LIB", "NATIVE").orElseThrow());
        assertEquals("OTHER_TABLE", f.decoder.getLongName("OTHER", "NATIVE").orElseThrow());
        f.longName = "RENAMED_TABLE";
        f.decoder.clearCache("LONG_TABLE", "LIB");
        assertEquals("RENAMED_TABLE", f.decoder.getLongName("LIB", "NATIVE").orElseThrow());
        assertEquals("OTHER_TABLE", f.decoder.getLongName("OTHER", "NATIVE").orElseThrow());
        assertEquals(4, f.nameReads); // two initial lookups, SQL alias invalidation, native-name refresh
    }

    @Test
    void shouldNotReturnTheOldSchemaWhenMetadataReloadFails() throws Exception {
        var f = new MetadataFixture();
        f.load();
        f.decoder.clearCache("NATIVE", "LIB");
        when(f.metadata.getColumns(null, "LIB", "LONG_TABLE", null)).thenThrow(new SQLException("metadata unavailable"));
        assertTrue(f.decoder.getRecordFormat("NATIVE", "LIB").isEmpty());
    }

    @ParameterizedTest
    @CsvSource({ "CHAR () FOR BIT DATA,10", "CHAR (20) FOR BIT DATA,20" })
    void shouldDecodeFixedBinaryUsingDeclaredOrCatalogLength(String type, int length) {
        var codec = decoder.toDataType("SCHEMA", "TABLE", "VALUE", type, 10, 0);
        byte[] payload = new byte[length];
        Arrays.fill(payload, (byte) 0x80);
        assertEquals(length, codec.getByteLength());
        assertArrayEquals(payload, (byte[]) codec.toObject(payload));
    }

    @ParameterizedTest
    @CsvSource({ "VARCHAR () FOR BIT DATA,10", "VARCHAR (20) FOR BIT DATA,20" })
    void shouldDecodeVariableBinaryWithoutIncludingLengthPrefixOrPadding(String type, int length) {
        var codec = decoder.toDataType("SCHEMA", "TABLE", "VALUE", type, 10, 0);
        byte[] payload = new byte[length + 2];
        ByteBuffer.wrap(payload).putShort((short) 3).put(new byte[]{ 0, (byte) 0x80, (byte) 0xff });
        assertEquals(length + 2, codec.getByteLength());
        assertArrayEquals(new byte[]{ 0, (byte) 0x80, (byte) 0xff }, (byte[]) codec.toObject(payload));
    }

    private static final class MetadataFixture {
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metadata = mock(DatabaseMetaData.class);
        final JdbcFileDecoder decoder = new JdbcFileDecoder(() -> connection, "DB", new SchemaCacheHash(), -1, -1);
        String longName = "LONG_TABLE";
        String type = "CHAR";
        int jdbcType = Types.CHAR;
        int ccsid = 37;
        int octets = 2;
        int encodingReads;
        int widthReads;
        int nameReads;

        MetadataFixture() throws Exception {
            when(connection.getMetaData()).thenReturn(metadata);
            when(metadata.getColumns(isNull(), anyString(), anyString(), isNull()))
                    .thenAnswer(i -> rows(List.of(Map.of(4, "VALUE", 5, jdbcType, 6, type, 7, 2, 9, 0, 11, DatabaseMetaData.columnNoNulls,
                            16, octets, 17, 1, 23, "NO"))));
            when(metadata.getPrimaryKeys(isNull(), anyString(), anyString())).thenAnswer(i -> rows(List.of(Map.of(4, "VALUE"))));
            when(connection.prepareStatement(anyString())).thenAnswer(i -> {
                String sql = i.getArgument(0);
                var statement = mock(PreparedStatement.class);
                var parameters = new java.util.HashMap<Integer, String>();
                org.mockito.Mockito.doAnswer(call -> {
                    parameters.put(call.getArgument(0), call.getArgument(1));
                    return null;
                }).when(statement).setString(anyInt(), anyString());
                when(statement.executeQuery()).thenAnswer(call -> {
                    if (sql.contains("qsys2.systables")) {
                        nameReads++;
                        return rows(parameters.get(2).equals("NATIVE")
                                ? List.of(Map.of(1, parameters.get(1).equals("LIB") ? longName : "OTHER_TABLE"))
                                : List.of());
                    }
                    if (sql.contains("ccsid FROM qsys2.SYSCOLUMNS")) {
                        encodingReads++;
                        return rows(List.of(Map.of(1, longName, 2, "NATIVE", 3, "VALUE", 4, "VALUE", 5, ccsid)));
                    }
                    if (sql.contains("character_octet_length FROM qsys2.SYSCOLUMNS")) {
                        widthReads++;
                        return rows(List.of(Map.of(1, longName, 2, "NATIVE", 3, "VALUE", 4, "VALUE", 5, 2, 6, octets)));
                    }
                    throw new AssertionError("Unexpected catalog query: " + sql);
                });
                return statement;
            });
        }

        SchemaCacheIF.TableInfo load() {
            return decoder.getRecordFormat("NATIVE", "LIB").orElseThrow();
        }

        private static ResultSet rows(List<Map<Integer, Object>> values) throws Exception {
            var rows = mock(ResultSet.class);
            var index = new AtomicInteger(-1);
            when(rows.next()).thenAnswer(i -> index.incrementAndGet() < values.size());
            when(rows.getString(anyInt())).thenAnswer(i -> {
                var value = values.get(index.get()).get(i.getArgument(0));
                return value == null ? null : value.toString();
            });
            when(rows.getInt(anyInt())).thenAnswer(i -> {
                var value = values.get(index.get()).get(i.getArgument(0));
                return value == null ? 0 : ((Number) value).intValue();
            });
            when(rows.getObject(anyInt())).thenAnswer(i -> values.get(index.get()).get(i.getArgument(0)));
            return rows;
        }
    }
}
