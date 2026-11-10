/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class JdbcFileDecoderTest {
    private final JdbcFileDecoder decoder = new JdbcFileDecoder(null, null, new SchemaCacheHash(), -1, -1);

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
}
