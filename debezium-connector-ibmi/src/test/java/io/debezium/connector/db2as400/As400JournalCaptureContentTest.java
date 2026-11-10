/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400JournalCaptureContentTest {
    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void separateNativeReadsHaveIdenticalContentDespiteTransportPlacement(int index) throws Exception {
        var pair = pair(index);
        byte[] a = bytes(pair, "left"), b = bytes(pair, "right");
        assertFalse(Arrays.equals(a, b));
        assertEquals(As400JournalCaptureContent.digest(a, position(pair)), As400JournalCaptureContent.digest(b, position(pair)));
    }

    @ParameterizedTest
    @ValueSource(ints = { 40, 64, 197, 218, 232, 288 })
    void changingSourceMetadataOrPayloadCannotCompareEqual(int offset) throws Exception {
        var pair = pair(0);
        byte[] a = bytes(pair, "right"), b = a.clone();
        b[offset] ^= 1;
        assertNotEquals(As400JournalCaptureContent.digest(a, position(pair)), As400JournalCaptureContent.digest(b, position(pair)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "padding", "extra-padding", "partial-padding", "truncated", "length", "null-range", "overlap", "XA", "LUW", "pointer", "receiver" })
    void unrecognizedOrDamagedSectionsReject(String fault) throws Exception {
        var pair = pair(0);
        byte[] raw = bytes(pair, "right");
        switch (fault) {
            case "padding" -> raw[raw.length - 1] = 1;
            case "extra-padding" -> raw = Arrays.copyOf(raw, raw.length + 16);
            case "partial-padding" -> raw = Arrays.copyOf(raw, raw.length - 1);
            case "truncated" -> raw = Arrays.copyOf(raw, 290);
            case "length" -> raw[ByteBuffer.wrap(raw).getInt(8)] = 0;
            case "null-range" -> ByteBuffer.wrap(raw).putInt(4, raw.length);
            case "overlap" -> ByteBuffer.wrap(raw).putInt(4, 236);
            case "XA" -> ByteBuffer.wrap(raw).putInt(12, 236);
            case "LUW" -> ByteBuffer.wrap(raw).putInt(16, 236);
            case "pointer" -> ByteBuffer.wrap(raw).putInt(72, 1);
            case "receiver" -> raw[236] ^= 1;
            default -> throw new IllegalArgumentException(fault);
        }
        byte[] damaged = raw;
        assertThrows(IllegalArgumentException.class, () -> As400JournalCaptureContent.digest(damaged, position(pair)));
    }

    private static JsonNode pair(int index) throws Exception {
        try (var input = As400JournalCaptureContentTest.class.getResourceAsStream("/native-transactions/census-comparison.json")) {
            return new ObjectMapper().readTree(input).get("pairs").get(index);
        }
    }

    private static byte[] bytes(JsonNode pair, String field) {
        return Base64.getDecoder().decode(pair.get(field).asText());
    }

    private static JournalPosition position(JsonNode pair) {
        var value = pair.get("position");
        return new JournalPosition(new BigInteger(value.get("sequence").asText()),
                new JournalReceiver(value.get("receiver").asText(), value.get("library").asText()));
    }
}
