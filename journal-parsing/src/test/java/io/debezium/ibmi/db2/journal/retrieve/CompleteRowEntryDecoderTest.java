/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400ByteArray;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Date;
import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.data.types.AS400VarBin;
import io.debezium.ibmi.db2.journal.data.types.AS400VarChar;
import io.debezium.ibmi.db2.journal.data.types.AS400Xml;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class CompleteRowEntryDecoderTest {
    private static final int OFFSET = 19;
    private final byte[] data = new byte[OFFSET + 300];

    static Stream<Arguments> capturedRows() {
        return Stream.of(Arguments.of(1, 16, 0), Arguments.of(1, 16, OFFSET),
                Arguments.of(2, 18, 0), Arguments.of(2, 18, OFFSET),
                Arguments.of(3, 19, 0), Arguments.of(3, 19, OFFSET));
    }

    @ParameterizedTest
    @MethodSource("capturedRows")
    void decodesCapturedRequiredColumnWithEmptyVariableNullMap(int expected, int sequence, int offset) throws Exception {
        byte[] captured;
        try (var input = getClass().getResourceAsStream("/complete-rows/required-int-" + expected + ".hex")) {
            captured = HexFormat.of().parseHex(new String(input.readAllBytes(), StandardCharsets.US_ASCII).replaceAll("\\s", ""));
        }
        byte[] buffer = new byte[offset + captured.length];
        System.arraycopy(captured, 0, buffer, offset, captured.length);
        EntryHeader entry = new EntryHeaderDecoder().decode(buffer, offset);
        assertEquals(BigInteger.valueOf(sequence), entry.getSequenceNumber());
        // The second capture follows a clear: RRN 1 is reused for value 2.
        assertEquals(BigInteger.valueOf(expected == 1 ? 1 : expected - 1), entry.getRelativeRecordNumber());
        assertEquals('R', entry.getJournalCode());
        assertEquals("PT", entry.getEntryType());
        assertEquals(236, entry.getNullValueOffest());
        assertEquals(240, entry.getEntrySpecificDataOffset());
        assertEquals(0, ByteBuffer.wrap(buffer).getInt(offset + entry.getNullValueOffest()));
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 4);
        Object[] row = decoder.decode(entry, buffer, offset);
        Arrays.fill(buffer, (byte) 0);
        assertArrayEquals(new Object[]{ expected }, row);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void emptyMapRejectsAnyNullableColumn(int nullableIndex) {
        prefix(8);
        boolean[] nullable = new boolean[2];
        nullable[nullableIndex] = true;
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4(), new AS400Bin4() }, nullable, 8);
        assertThrows(IOException.class, () -> decoder.decode(header(8, 236, (byte) 0), data, OFFSET));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 236, 280, 296 })
    void emptyMapAcceptsOnlyAnEntirelyRequiredLayout(int start) throws Exception {
        prefix(8);
        ByteBuffer.wrap(data).putInt(OFFSET + 256, 42).putInt(OFFSET + 260, 43);
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4(), new AS400Bin4() }, new boolean[2], 8);
        assertArrayEquals(new Object[]{ 42, 43 }, decoder.decode(header(8, start, (byte) 0), data, OFFSET));
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 235, 238, 240, 245, 256, 259, 297, 300, Integer.MAX_VALUE })
    void emptyMapStillRequiresValidDisplacementAndNonoverlappingLengthWord(int start) {
        prefix(4);
        // Zero a complete length word where possible, including inside the payload.
        if (start >= 236 && start <= 296) {
            ByteBuffer.wrap(data).putInt(OFFSET + start, 0);
        }
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 4);
        assertThrows(IOException.class, () -> decoder.decode(header(4, start, (byte) 0), data, OFFSET));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 4, 8, 16, 32 })
    void emptyMapDoesNotAdmitUnsupportedOrMinimizedRequiredRows(int flag) {
        prefix(4);
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 4);
        assertThrows(IOException.class, () -> decoder.decode(header(4, 236, (byte) flag), data, OFFSET));
    }

    @Test
    void requiredLayoutStillRejectsDefaultedFieldsAndWrongNonemptyMapLength() {
        prefix(4);
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 4);
        nulls(0xf9);
        assertThrows(IOException.class, () -> decoder.decode(header(4, 280, (byte) 0), data, OFFSET));
        nulls(0xf0, 0xf0);
        assertThrows(IOException.class, () -> decoder.decode(header(4, 280, (byte) 0), data, OFFSET));
        ByteBuffer.wrap(data).putInt(OFFSET + 280, -1);
        assertThrows(IOException.class, () -> decoder.decode(header(4, 280, (byte) 0), data, OFFSET));
    }

    @Test
    void decodesOwnedColumnsAtNonzeroOffsetWithNulls() throws Exception {
        var decoder = decoder(new AS400Bin4(), new AS400ByteArray(3), new AS400Date());
        prefix(17);
        ByteBuffer.wrap(data).putInt(OFFSET + 256, 42);
        data[OFFSET + 260] = 1;
        nulls(0xf0, 0xf0, 0xf1);
        Object[] row = decoder.decode(header(17, 280, (byte) 0), data, OFFSET);
        Arrays.fill(data, (byte) 0);
        assertEquals(42, row[0]);
        assertArrayEquals(new byte[]{ 1, 0, 0 }, (byte[]) row[1]);
        assertNull(row[2]);
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 3, 5, 99999 })
    void rejectsWrongPayloadLengthEvenWhenNextEntryHasBytes(int declared) {
        prefix(declared);
        var decoder = decoder(new AS400Bin4());
        assertThrows(IOException.class, () -> decoder.decode(header(4, 0, (byte) 0), data, OFFSET));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0xf9, 0x00, 0x01, 0x40, 0xfa })
    void rejectsDefaultedAndInvalidIndicators(int indicator) {
        prefix(4);
        nulls(indicator);
        var decoder = decoder(new AS400Bin4());
        assertThrows(IOException.class, () -> decoder.decode(header(4, 280, (byte) 0), data, OFFSET));
    }

    @Test
    void rejectsAbsentOrTruncatedNullMapForNullableLayout() {
        prefix(4);
        var decoder = decoder(new AS400Bin4());
        assertThrows(IOException.class, () -> decoder.decode(header(4, 0, (byte) 0), data, OFFSET));
        ByteBuffer.wrap(data).putInt(OFFSET + 280, Integer.MAX_VALUE);
        assertThrows(IOException.class, () -> decoder.decode(header(4, 280, (byte) 0), data, OFFSET));
    }

    @Test
    void rejectsNullMapOverlappingPayloadAndWrongColumnCount() {
        prefix(4);
        var decoder = decoder(new AS400Bin4());
        ByteBuffer.wrap(data).putInt(OFFSET + 256, 1);
        assertThrows(IOException.class, () -> decoder.decode(header(4, 256, (byte) 0), data, OFFSET));
        nulls(0xf0, 0xf0);
        assertThrows(IOException.class, () -> decoder.decode(header(4, 280, (byte) 0), data, OFFSET));
    }

    @Test
    void rejectsNullForRequiredColumnButAcceptsAbsentMapForRequiredLayout() throws Exception {
        prefix(4);
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 100);
        nulls(0xf1);
        assertThrows(IOException.class, () -> decoder.decode(header(4, 280, (byte) 0), data, OFFSET));
        assertArrayEquals(new Object[]{ 0 }, decoder.decode(header(4, 0, (byte) 0), data, OFFSET));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 4, 8, 16, 32 })
    void rejectsUnsupportedFlags(int flag) {
        prefix(4);
        var decoder = decoder(new AS400Bin4());
        assertThrows(IOException.class, () -> decoder.decode(header(4, 0, (byte) flag), data, OFFSET));
    }

    @ParameterizedTest
    @ValueSource(strings = { "binary", "text", "graphic" })
    void varyingFieldCannotReadFollowingColumnOrPadMissingBytes(String type) {
        AS400DataType varying = switch (type) {
            case "binary" -> new AS400VarBin(3);
            case "text" -> new AS400VarChar(3, 1, 37);
            default -> new AS400VarChar(3, 2, 13488);
        };
        int length = varying.getByteLength() + 4;
        prefix(length);
        nulls(0xf0, 0xf0);
        ByteBuffer.wrap(data).putShort(OFFSET + 256, (short) 4);
        var decoder = decoder(varying, new AS400Bin4());
        assertThrows(IOException.class, () -> decoder.decode(header(length, 280, (byte) 0), data, OFFSET));
    }

    @Test
    void supportsCompleteVaryingFields() throws Exception {
        var decoder = decoder(new AS400VarChar(3, 1, 37), new AS400VarBin(3));
        prefix(10);
        ByteBuffer.wrap(data).putShort(OFFSET + 256, (short) 2).putShort(OFFSET + 261, (short) 2);
        new AS400Text(2, 37).toBytes("AB", data, OFFSET + 258);
        data[OFFSET + 263] = 7;
        data[OFFSET + 264] = 8;
        nulls(0xf0, 0xf0);
        Object[] row = decoder.decode(header(10, 280, (byte) 0), data, OFFSET);
        assertEquals("AB", row[0]);
        assertArrayEquals(new byte[]{ 7, 8 }, (byte[]) row[1]);
    }

    @ParameterizedTest
    @ValueSource(ints = { 1200, 13488, 1208 })
    void shouldDecodeUnicodeLengthUnitsWithoutConsumingTheFollowingColumn(int ccsid) throws Exception {
        int unit = ccsid == 1208 ? 1 : 2;
        var varying = new AS400VarChar(6 / unit, unit, ccsid);
        var decoder = decoder(varying, new AS400Bin4());
        prefix(12);
        byte[] text = "Ä界".getBytes(ccsid == 1208 ? StandardCharsets.UTF_8 : StandardCharsets.UTF_16BE);
        ByteBuffer.wrap(data).putShort(OFFSET + 256, (short) (text.length / unit)).putInt(OFFSET + 264, 42);
        System.arraycopy(text, 0, data, OFFSET + 258, text.length);
        nulls(0xf0, 0xf0);
        assertArrayEquals(new Object[]{ "Ä界", 42 }, decoder.decode(header(12, 280, (byte) 0), data, OFFSET));
        ByteBuffer.wrap(data).putShort(OFFSET + 256, (short) (6 / unit + 1));
        assertThrows(IOException.class, () -> decoder.decode(header(12, 280, (byte) 0), data, OFFSET));
    }

    @Test
    void rejectsXmlPlaceholderCodecEvenForNullColumns() {
        assertThrows(IllegalArgumentException.class, () -> decoder(new AS400Xml()));
    }

    @Test
    void rejectsOversizedPhysicalLayoutBeforeMaterialization() {
        assertThrows(IllegalArgumentException.class,
                () -> new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Text(101) }, new boolean[]{ false }, 100));
    }

    @Test
    void shouldDecodeTheNativeFieldLimitAndValidateTheLastNullIndicator() throws Exception {
        int count = 8000;
        int bytes = count * 4;
        int nullOffset = 256 + bytes;
        int end = nullOffset + 4 + count;
        byte[] raw = new byte[OFFSET + end];
        new AS400Text(5, 37).toBytes(String.format("%05d", bytes), raw, OFFSET + 240);
        ByteBuffer buffer = ByteBuffer.wrap(raw);
        buffer.putInt(OFFSET + nullOffset, count);
        var types = new AS400DataType[count];
        var nullable = new boolean[count];
        Object[] expected = new Object[count];
        for (int i = 0; i < count; i++) {
            types[i] = new AS400Bin4();
            nullable[i] = true;
            buffer.putInt(OFFSET + 256 + i * 4, i);
            raw[OFFSET + nullOffset + 4 + i] = (byte) (i % 2 == 0 ? 0xf0 : 0xf1);
            expected[i] = i % 2 == 0 ? i : null;
        }
        var entry = new EntryHeader(end, nullOffset, 240, BigInteger.ONE, BigInteger.ONE, Instant.EPOCH,
                'R', "PT", "TEST      LIB       TEST      ", BigInteger.TEN, end, 0, "RCV", "LIB",
                new EntryHeader.ControlInfo('1', 0, false, false, (byte) 0));
        var decoder = new CompleteRowEntryDecoder(types, nullable, bytes);
        assertArrayEquals(expected, decoder.decode(entry, raw, OFFSET));
        raw[OFFSET + end - 1] = (byte) 0xf9;
        assertThrows(IOException.class, () -> decoder.decode(entry, raw, OFFSET));
        assertThrows(IllegalArgumentException.class, () -> new CompleteRowEntryDecoder(types, nullable, bytes - 1));
        assertThrows(IllegalArgumentException.class, () -> new CompleteRowEntryDecoder(new AS400DataType[8001], new boolean[8001], 65536));
    }

    private CompleteRowEntryDecoder decoder(AS400DataType... columns) {
        boolean[] optional = new boolean[columns.length];
        Arrays.fill(optional, true);
        return new CompleteRowEntryDecoder(columns, optional, 100);
    }

    private void prefix(int length) {
        new AS400Text(5, 37).toBytes(String.format("%05d", length), data, OFFSET + 240);
    }

    private void nulls(int... indicators) {
        ByteBuffer.wrap(data).putInt(OFFSET + 280, indicators.length);
        for (int i = 0; i < indicators.length; i++) {
            data[OFFSET + 284 + i] = (byte) indicators[i];
        }
    }

    private EntryHeader header(int length, int nullOffset, byte flags) {
        return new EntryHeader(300, nullOffset, 240, BigInteger.ONE, BigInteger.ONE, Instant.EPOCH,
                'R', "PT", "TEST      LIB       TEST      ", BigInteger.TEN, 300, 0, "RCV", "LIB",
                new EntryHeader.ControlInfo('1', 0, false, false, flags));
    }
}
