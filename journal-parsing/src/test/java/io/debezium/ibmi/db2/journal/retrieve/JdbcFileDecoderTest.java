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

import java.math.BigDecimal;
import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400PackedDecimal;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400ZonedDecimal;

import io.debezium.ibmi.db2.journal.data.types.AS400VarChar;

public class JdbcFileDecoderTest {

    @Test
    public void testToDataType() throws Exception {
        final JdbcFileDecoder decoder = new JdbcFileDecoder(null, null, new SchemaCacheHash(), -1, -1);
        final AS400DataType passwordNoLength = decoder.toDataType("schem", "table", "password", "CHAR () FOR BIT DATA",
                10, 0);
        assertEquals(AS400DataType.TYPE_BYTE_ARRAY, passwordNoLength.getInstanceType());
        final AS400DataType passwordLength = decoder.toDataType("schem", "table", "password", "CHAR (20) FOR BIT DATA",
                10, 0);
        assertEquals(AS400DataType.TYPE_BYTE_ARRAY, passwordLength.getInstanceType());
        assertEquals(20, passwordLength.getByteLength());
        final AS400DataType varPasswordNoLength = decoder.toDataType("schem", "table", "password",
                "VARCHAR () FOR BIT DATA", 10, 0);
        assertEquals(-1, varPasswordNoLength.getInstanceType());
        final AS400DataType varPasswordLength = decoder.toDataType("schem", "table", "password",
                "VARCHAR (20) FOR BIT DATA", 10, 0);
        assertEquals(-1, varPasswordLength.getInstanceType());
        assertEquals(20, passwordLength.getByteLength());
    }

    private static final JdbcFileDecoder DECODER = new JdbcFileDecoder(null, null, new SchemaCacheHash(), -1, -1);

    // [CHAR(3), DECIMAL(5,0), CHAR(2)] -> 3 + 3 + 2 = 8 bytes
    private static AS400Structure threeFieldStructure() {
        return new AS400Structure(new AS400DataType[]{ new AS400Text(3), new AS400PackedDecimal(5, 0), new AS400Text(2) });
    }

    /** Blank-fills [from, to) with EBCDIC blanks (0x40), the way a null numeric column appears in a journal image. */
    private static byte[] blankFill(byte[] data, int from, int to) {
        Arrays.fill(data, from, to, (byte) 0x40);
        return data;
    }

    @Test
    public void testDecodeEntryDecodesAllFieldsWhenNoneNull() {
        final AS400Structure structure = threeFieldStructure();
        final byte[] data = structure.toBytes(new Object[]{ "ABC", new BigDecimal(123), "ZZ" });

        final Object[] result = DECODER.decodeEntry(structure, data, 0, null);

        assertEquals(3, result.length);
        assertEquals("ABC", result[0]);
        assertEquals(0, new BigDecimal(123).compareTo((BigDecimal) result[1]));
        assertEquals("ZZ", result[2]);
    }

    /**
     * The non-null path must stay byte-for-byte identical to the driver's own
     * {@link AS400Structure#toObject(byte[], int)}: same fields, same offset progression.
     */
    @Test
    public void testDecodeEntryMatchesDriverWhenNoNulls() {
        final AS400Structure structure = threeFieldStructure();
        final byte[] data = structure.toBytes(new Object[]{ "XYZ", new BigDecimal(98765), "qq" });

        final Object[] expected = (Object[]) structure.toObject(data, 0);
        final Object[] actual = DECODER.decodeEntry(structure, data, 0, null);

        assertArrayEquals(expected, actual);
    }

    /**
     * Variable-length types ({@link AS400VarChar}) occupy a fixed slot (max width + 2-byte length
     * prefix) but only decode {@code actualLength} bytes. The driver advances the offset by the
     * fixed {@code getByteLength()}, not the actual data length — decodeEntry must do exactly the
     * same, or every field after a varchar would be misaligned. Proven by equivalence.
     */
    @Test
    public void testDecodeEntryMatchesDriverForVariableLengthTypes() {
        // [VARCHAR(max 5), CHAR(2)] -> (5 + 2) + 2 = 9 bytes
        final AS400Structure structure = new AS400Structure(
                new AS400DataType[]{ new AS400VarChar(5, 1), new AS400Text(2) });
        final byte[] data = new byte[structure.getByteLength()];
        new AS400Bin2().toBytes((short) 3, data, 0); // varchar actual length = 3 (< max 5)

        final Object[] expected = (Object[]) structure.toObject(data, 0);
        final Object[] actual = DECODER.decodeEntry(structure, data, 0, null);

        assertArrayEquals(expected, actual);
    }

    /**
     * Guards against a vacuous regression test: the blank-filled DECIMAL slot must genuinely make
     * the driver throw, so that skipping the null field is what prevents the failure.
     */
    @Test
    public void testRawDriverThrowsOnBlankFilledDecimal() {
        final AS400Structure structure = threeFieldStructure();
        final byte[] data = blankFill(structure.toBytes(new Object[]{ "ABC", new BigDecimal(123), "ZZ" }), 3, 6);

        assertThrows(NumberFormatException.class, () -> structure.toObject(data, 0));
    }

    @Test
    public void testDecodeEntrySkipsNullPackedDecimalField() {
        final AS400Structure structure = threeFieldStructure();
        // Blank-fill the DECIMAL slot (bytes 3..5) as a null numeric column would appear.
        final byte[] data = blankFill(structure.toBytes(new Object[]{ "ABC", new BigDecimal(123), "ZZ" }), 3, 6);

        final Object[] result = DECODER.decodeEntry(structure, data, 0, new boolean[]{ false, true, false });

        assertEquals(3, result.length);
        assertEquals("ABC", result[0]);
        assertNull(result[1]);
        assertEquals("ZZ", result[2]);
    }

    /** Same problem class for zoned decimal (NUMERIC) — the fix is type-agnostic. */
    @Test
    public void testDecodeEntrySkipsNullZonedDecimalField() {
        // [NUMERIC(5,0), CHAR(2)] -> 5 + 2 = 7 bytes
        final AS400Structure structure = new AS400Structure(
                new AS400DataType[]{ new AS400ZonedDecimal(5, 0), new AS400Text(2) });
        final byte[] data = blankFill(structure.toBytes(new Object[]{ new BigDecimal(42), "ok" }), 0, 5);

        final Object[] result = DECODER.decodeEntry(structure, data, 0, new boolean[]{ true, false });

        assertNull(result[0]);
        assertEquals("ok", result[1]);
    }

    /**
     * Offset integrity: a null field at the first and last positions must not desync the offset of
     * the field between them, even when both neighbours hold invalid (blank) bytes.
     */
    @Test
    public void testDecodeEntryNullsAtBoundariesKeepMiddleAligned() {
        // [DECIMAL(5,0), CHAR(3), DECIMAL(5,0)] -> 3 + 3 + 3 = 9 bytes
        final AS400Structure structure = new AS400Structure(
                new AS400DataType[]{ new AS400PackedDecimal(5, 0), new AS400Text(3), new AS400PackedDecimal(5, 0) });
        byte[] data = structure.toBytes(new Object[]{ new BigDecimal(11), "MID", new BigDecimal(22) });
        data = blankFill(data, 0, 3); // first DECIMAL
        data = blankFill(data, 6, 9); // last DECIMAL

        final Object[] result = DECODER.decodeEntry(structure, data, 0, new boolean[]{ true, false, true });

        assertNull(result[0]);
        assertEquals("MID", result[1]);
        assertNull(result[2]);
    }

    /** Records sit at an offset inside the journal buffer — decoding must honour a non-zero start offset. */
    @Test
    public void testDecodeEntryHonoursStartOffset() {
        final AS400Structure structure = threeFieldStructure();
        final byte[] record = blankFill(structure.toBytes(new Object[]{ "ABC", new BigDecimal(123), "ZZ" }), 3, 6);
        final byte[] buffer = new byte[4 + record.length];
        System.arraycopy(record, 0, buffer, 4, record.length); // 4-byte prefix

        final Object[] result = DECODER.decodeEntry(structure, buffer, 4, new boolean[]{ false, true, false });

        assertEquals("ABC", result[0]);
        assertNull(result[1]);
        assertEquals("ZZ", result[2]);
    }

    @Test
    public void testDecodeEntryAllFieldsNull() {
        final AS400Structure structure = threeFieldStructure();
        final byte[] data = blankFill(new byte[structure.getByteLength()], 0, structure.getByteLength());

        final Object[] result = DECODER.decodeEntry(structure, data, 0, new boolean[]{ true, true, true });

        assertArrayEquals(new Object[]{ null, null, null }, result);
    }

    /** A shorter (or absent) indicator array must not break decoding: unflagged fields decode normally. */
    @Test
    public void testDecodeEntryToleratesShortIndicatorArray() {
        final AS400Structure structure = threeFieldStructure();
        final byte[] data = structure.toBytes(new Object[]{ "ABC", new BigDecimal(123), "ZZ" });

        final Object[] result = DECODER.decodeEntry(structure, data, 0, new boolean[]{ false }); // only covers field 0

        assertEquals("ABC", result[0]);
        assertEquals(0, new BigDecimal(123).compareTo((BigDecimal) result[1]));
        assertEquals("ZZ", result[2]);
    }

    @Test
    public void dateAndTimeFallBackToIsoWhenFormatUnknown() throws Exception {
        // null connection -> the format cache lookup fails and is swallowed; DATE/TIME must default to *ISO
        // rather than throwing (preserves prior behaviour for ISO files and never NPEs the decoder).
        final JdbcFileDecoder decoder = new JdbcFileDecoder(null, null, new SchemaCacheHash(), -1, -1);

        final AS400DataType date = decoder.toDataType("schem", "table", "d", "DATE", 10, 0);
        assertEquals(AS400DataType.TYPE_DATE, date.getInstanceType());
        assertEquals(com.ibm.as400.access.AS400Date.FORMAT_ISO, ((com.ibm.as400.access.AS400Date) date).getFormat());

        final AS400DataType time = decoder.toDataType("schem", "table", "t", "TIME", 8, 0);
        assertEquals(AS400DataType.TYPE_TIME, time.getInstanceType());
    }
}
