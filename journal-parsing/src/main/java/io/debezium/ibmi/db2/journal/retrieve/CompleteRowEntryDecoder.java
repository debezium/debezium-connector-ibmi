/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Set;

import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Bin8;
import com.ibm.as400.access.AS400ByteArray;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Date;
import com.ibm.as400.access.AS400Float4;
import com.ibm.as400.access.AS400Float8;
import com.ibm.as400.access.AS400PackedDecimal;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400Time;
import com.ibm.as400.access.AS400Timestamp;
import com.ibm.as400.access.AS400ZonedDecimal;

import io.debezium.ibmi.db2.journal.data.types.AS400Boolean;
import io.debezium.ibmi.db2.journal.data.types.AS400Temporal;
import io.debezium.ibmi.db2.journal.data.types.AS400VarBin;
import io.debezium.ibmi.db2.journal.data.types.AS400VarChar;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/**
 * Strict inline-row decoder for buffered capture, separate from the legacy decoder.
 * Requires a pinned, verified physical record layout and *VARLEN null indicators.
 * No pointers, minimized/defaulted fields, XML placeholders, or unknown codecs.
 */
public final class CompleteRowEntryDecoder implements JournalEntryDeocder<Object[]> {
    private static final Set<Class<?>> SUPPORTED = Set.of(AS400Bin2.class, AS400Bin4.class, AS400Bin8.class,
            AS400ByteArray.class, AS400Date.class, AS400Float4.class, AS400Float8.class, AS400PackedDecimal.class,
            AS400Text.class, AS400Time.class, AS400Timestamp.class, AS400ZonedDecimal.class,
            AS400Boolean.class, AS400Temporal.class, AS400VarBin.class, AS400VarChar.class);
    private final AS400DataType[] columns;
    private final boolean[] nullable;
    private final int rowLength;

    public CompleteRowEntryDecoder(AS400DataType[] columns, boolean[] nullable, int maximumRowBytes) {
        if (columns == null || nullable == null || columns.length == 0 || columns.length > 4096
                || columns.length != nullable.length || maximumRowBytes <= 0) {
            throw new IllegalArgumentException("Invalid complete-row layout");
        }
        this.columns = new AS400DataType[columns.length];
        this.nullable = nullable.clone();
        long total = 0;
        for (int i = 0; i < columns.length; i++) {
            AS400DataType column = columns[i];
            if (column == null || !SUPPORTED.contains(column.getClass()) || column.getByteLength() <= 0) {
                throw new IllegalArgumentException("Unsupported inline column decoder at " + i);
            }
            total += column.getByteLength();
            if (total > maximumRowBytes || total > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Physical row layout exceeds limit");
            }
            this.columns[i] = (AS400DataType) column.clone();
        }
        rowLength = (int) total;
    }

    @Override
    public Object[] decode(EntryHeader header, byte[] data, int offset) throws IOException {
        if (header.getJournalCode() != 'R' || !Set.of("PT", "PX", "UB", "UP", "DL").contains(header.getEntryType())) {
            throw new IOException("Not a supported forward row image");
        }
        if (header.getControlInfo() == null || (header.getControlInfo().flags() & 0x3f) != 0 || header.getPointerHandle() != 0) {
            throw new IOException("Incomplete, minimized, pointer-backed, or unsupported row flags");
        }
        if ((header.getEntryType().equals("UB") || header.getEntryType().equals("DL"))
                && header.getControlInfo().indicatorFlag() != '1') {
            throw new IOException("Required before-image is absent or ambiguous");
        }
        if (offset < 0 || header.getEndOffset() < 236 || (long) offset + header.getEndOffset() > data.length) {
            throw new IOException("Invalid current entry boundary");
        }
        int prefix = header.getEntrySpecificDataOffset();
        int end = header.getEndOffset();
        if (prefix < 236 || (long) prefix + 16 + rowLength > end) {
            throw new IOException("Incomplete row payload");
        }
        int declared = 0;
        for (int i = 0; i < 5; i++) {
            int digit = data[offset + prefix + i] & 0xff;
            if (digit < 0xf0 || digit > 0xf9) {
                throw new IOException("Invalid row payload length");
            }
            declared = declared * 10 + digit - 0xf0;
        }
        if (declared != rowLength) {
            throw new IOException("Row payload does not match pinned record format");
        }
        boolean[] nulls = nulls(header, data, offset, prefix, prefix + 16 + rowLength);
        Object[] result = new Object[columns.length];
        int position = offset + prefix + 16;
        for (int i = 0; i < columns.length; i++) {
            int length = columns[i].getByteLength();
            if (!nulls[i]) {
                // Bound every field independently: a varying field may not read the next column/entry.
                byte[] field = Arrays.copyOfRange(data, position, position + length);
                if (columns[i] instanceof AS400VarBin || columns[i] instanceof AS400VarChar) {
                    int used = Short.toUnsignedInt(ByteBuffer.wrap(field).getShort());
                    if (used > Short.MAX_VALUE || used > length - 2) {
                        throw new IOException("Invalid varying field length");
                    }
                }
                try {
                    result[i] = columns[i].toObject(field, 0);
                }
                catch (RuntimeException e) {
                    throw new IOException("Cannot decode complete column " + i, e);
                }
            }
            position += length;
        }
        return result;
    }

    private boolean[] nulls(EntryHeader header, byte[] data, int offset, int payloadStart, int payloadEnd) throws IOException {
        boolean[] result = new boolean[columns.length];
        int start = header.getNullValueOffest();
        if (start == 0) {
            requireNonNullableLayout();
            return result;
        }
        if (start < 236 || (long) start + 4 > header.getEndOffset()) {
            throw new IOException("Invalid null-indicator displacement");
        }
        long length = Integer.toUnsignedLong(ByteBuffer.wrap(data, offset + start, 4).getInt());
        long end = start + 4L + length;
        if ((length != 0 && length != columns.length) || end > header.getEndOffset() || (start < payloadEnd && end > payloadStart)) {
            throw new IOException("Incomplete, overlapping, or mismatched null indicators");
        }
        if (length == 0) {
            // *VARLEN can return a length word with no indicators for a complete,
            // non-null-capable record. Validate its span before accepting absence.
            requireNonNullableLayout();
            return result;
        }
        for (int i = 0; i < result.length; i++) {
            int value = data[offset + start + 4 + i] & 0xff;
            if (value != 0xf0 && value != 0xf1) {
                throw new IOException("Uncollected/defaulted or invalid column indicator");
            }
            result[i] = value == 0xf1;
            if (result[i] && !nullable[i]) {
                throw new IOException("Null value conflicts with pinned record format");
            }
        }
        return result;
    }

    private void requireNonNullableLayout() throws IOException {
        for (boolean optional : nullable) {
            if (optional) {
                throw new IOException("Missing null indicators for nullable layout");
            }
        }
    }
}
