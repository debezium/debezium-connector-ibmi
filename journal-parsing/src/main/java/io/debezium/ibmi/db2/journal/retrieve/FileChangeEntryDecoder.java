/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.io.IOException;
import java.math.BigDecimal;

import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400ZonedDecimal;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/**
 * Decodes the documented object-level D/CG layout. The change field type is at
 * one-based offset 109 in the entry-specific data, after the API's 16-byte prefix.
 * See https://www.ibm.com/docs/en/ssw_ibm_i_74/rzaki/rzakivarlength.htm.
 */
public class FileChangeEntryDecoder implements JournalEntryDeocder<FileChangeEntryDecoder.FileChange> {
    private static final int CHANGE_TYPE_OFFSET = 108;
    private final AS400Text name = new AS400Text(10);
    private final AS400Text changeType = new AS400Text(1);
    private final AS400ZonedDecimal length = new AS400ZonedDecimal(5, 0);

    @Override
    public FileChange decode(EntryHeader header, byte[] data, int offset) throws IOException {
        if (header.getJournalEntryType() != JournalEntryType.FILE_CHANGE) {
            throw new IOException("Expected a D/CG journal entry");
        }
        long prefix = (long) offset + header.getEntrySpecificDataOffset();
        long end = Math.min(data.length, (long) offset + header.getEndOffset());
        if (offset < 0 || header.getEntrySpecificDataOffset() < 0 || prefix + ENTRY_SPECIFIC_DATA_OFFSET > end) {
            throw new IOException("Incomplete D/CG entry-specific data prefix");
        }
        final int dataLength;
        try {
            dataLength = ((BigDecimal) length.toObject(data, (int) prefix)).intValueExact();
        }
        catch (RuntimeException e) {
            throw new IOException("Invalid D/CG entry-specific data length", e);
        }
        long start = prefix + ENTRY_SPECIFIC_DATA_OFFSET;
        if (dataLength <= CHANGE_TYPE_OFFSET || start + dataLength > end) {
            throw new IOException("Incomplete D/CG entry-specific data: missing change field type or payload");
        }
        int position = (int) start;
        return new FileChange(((String) name.toObject(data, position)).trim(),
                ((String) name.toObject(data, position + 10)).trim(),
                ((String) name.toObject(data, position + 20)).trim(),
                ((String) changeType.toObject(data, position + CHANGE_TYPE_OFFSET)).charAt(0));
    }

    public record FileChange(String file, String library, String member, char changeType) {
        public boolean clearsRows() {
            // 3: optimized DELETE without WHERE; 8: SQL TRUNCATE TABLE.
            return changeType == '3' || changeType == '8';
        }
    }
}
