/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400ZonedDecimal;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

class FileChangeEntryDecoderTest {
    private static final int OFFSET = 23;
    private static final int ESD_OFFSET = 240;
    private static final int END = ESD_OFFSET + 16 + 109;
    private final FileChangeEntryDecoder decoder = new FileChangeEntryDecoder();

    @ParameterizedTest
    @ValueSource(chars = { '3', '8' })
    void decodesSanitizedLiveRpcPayloadWithBlankMember(char subtype) throws Exception {
        byte[] entrySpecificData;
        try (var stream = getClass().getResourceAsStream("/dcg/dcg-" + subtype + ".hex")) {
            entrySpecificData = HexFormat.of().parseHex(new String(stream.readAllBytes(), StandardCharsets.US_ASCII).replaceAll("\\s", ""));
        }
        // Offsets observed in the real single-entry RPC responses, including unused buffer space.
        byte[] buffer = new byte[OFFSET + 131008];
        System.arraycopy(entrySpecificData, 0, buffer, OFFSET + 272, entrySpecificData.length);
        EntryHeader header = new EntryHeader(0, 0, 272, BigInteger.ONE, BigInteger.ONE,
                Instant.EPOCH, 'D', "CG", " ".repeat(30), BigInteger.valueOf(7), 131008, 0, "RCV", "LIB");
        var change = decoder.decode(header, buffer, OFFSET);
        assertEquals("TESTT", change.file());
        assertEquals("DTEST", change.library());
        assertEquals("", change.member());
        assertEquals(subtype, change.changeType());
        assertTrue(change.clearsRows());
    }

    @Test
    void decodesSqlTruncateAndOptimizedDeleteAtNonzeroBufferOffset() throws Exception {
        for (char subtype : new char[]{ '3', '8' }) {
            var change = decoder.decode(header(END), data(subtype), OFFSET);
            assertEquals("TESTFILE", change.file());
            assertEquals("TESTLIB", change.library());
            assertEquals("MEMBER", change.member());
            assertEquals(subtype, change.changeType());
            assertTrue(change.clearsRows());
        }
    }

    @Test
    void doesNotTreatOtherFileChangesAsTruncates() throws Exception {
        for (char subtype : "01245679ABCJ ?".toCharArray()) {
            assertFalse(decoder.decode(header(END), data(subtype), OFFSET).clearsRows(), "subtype " + subtype);
        }
    }

    @Test
    void rejectsPayloadThatEndsBeforeSubtypeEvenIfBufferHasMoreData() {
        assertThrows(IOException.class, () -> decoder.decode(header(END - 1), data('8'), OFFSET));
    }

    @Test
    void rejectsShortDeclaredLengthEvenIfBufferContainsTruncateMarker() {
        byte[] data = data('8');
        new AS400ZonedDecimal(5, 0).toBytes(BigDecimal.valueOf(108), data, OFFSET + ESD_OFFSET);
        assertThrows(IOException.class, () -> decoder.decode(header(END), data, OFFSET));
    }

    @Test
    void rejectsShortBufferAndMissingPrefix() {
        assertThrows(IOException.class, () -> decoder.decode(header(END), Arrays.copyOf(data('8'), OFFSET + END - 1), OFFSET));
        assertThrows(IOException.class, () -> decoder.decode(header(ESD_OFFSET + 15), data('8'), OFFSET));
    }

    @Test
    void rejectsInvalidLengthAndWrongJournalType() {
        byte[] data = data('8');
        Arrays.fill(data, OFFSET + ESD_OFFSET, OFFSET + ESD_OFFSET + 5, (byte) 0x40);
        assertThrows(IOException.class, () -> decoder.decode(header(END), data, OFFSET));
        EntryHeader wrongType = new EntryHeader(END, 0, ESD_OFFSET, BigInteger.ONE, BigInteger.ONE,
                Instant.EPOCH, 'F', "CR", " ".repeat(30), BigInteger.ZERO, END, 0, "RCV", "LIB");
        assertThrows(IOException.class, () -> decoder.decode(wrongType, data('8'), OFFSET));
    }

    private EntryHeader header(int end) {
        return new EntryHeader(end, 0, ESD_OFFSET, BigInteger.ONE, BigInteger.ONE,
                Instant.EPOCH, 'D', "CG", " ".repeat(30), BigInteger.ZERO, end, 0, "RCV", "LIB");
    }

    private byte[] data(char subtype) {
        byte[] data = new byte[OFFSET + END + 50];
        Arrays.fill(data, (byte) 0x40);
        int prefix = OFFSET + ESD_OFFSET;
        new AS400ZonedDecimal(5, 0).toBytes(BigDecimal.valueOf(109), data, prefix);
        new AS400Text(30).toBytes(String.format("%-10s%-10s%-10s", "TESTFILE", "TESTLIB", "MEMBER"), data, prefix + 16);
        new AS400Text(1).toBytes(String.valueOf(subtype), data, prefix + 16 + 108);
        return data;
    }
}
