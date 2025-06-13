/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400UnsignedBin4;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

public abstract class JournalFileEntryDecoder implements JournalEntryDeocder<Object[]> {

    public JournalFileEntryDecoder() {
    }

    public abstract Object[] decodeFile(EntryHeader entryHeader, byte[] data, int offset) throws Exception;

    @Override
    public Object[] decode(EntryHeader entryHeader, byte[] data, int offset) throws Exception {
        Object[] objs = decodeFile(entryHeader, data, offset);
        Object[] nullified = nullify(objs, entryHeader, data, offset);
        return nullified;
    }

    Object[] nullify(Object[] objs, EntryHeader entryHeader, byte[] data, int offset) {
        boolean[] isNull = getNullFieldIndicators(data, entryHeader.getNullValueOffest(), offset);
        if (isNull == null) {
            return objs;
        }
        else {
            int length = Math.min(objs.length, isNull.length);
            for (int i = 0; i < length; i++) {
                if (isNull[i]) {
                    objs[i] = null;
                }
            }
        }
        return objs;
    }

    static final Logger log = LoggerFactory.getLogger(JournalEntryDeocder.class);
    final AS400UnsignedBin4 nullIndicatorLengthDecoder = new AS400UnsignedBin4();

    private boolean[] getNullFieldIndicators(byte[] data, int nullEntryOffset, int offset) {
        boolean[] isNull = null;
        if (nullEntryOffset != 0) {
            long l = nullIndicatorLengthDecoder.toLong(data, offset + nullEntryOffset);
            if (l > Integer.MAX_VALUE) {
                log.error("null indicator length unreasonably large {}", l);
                return null;
            }
            int length = (int) l;
            isNull = new boolean[length];
            for (int i = 0; i < length; i++) {
                // BCD bottom 4 bits
                isNull[i] = (data[offset + nullEntryOffset + 4 + i] & 15) == 1; // 1 = is null, 0 = not null, 9 = default value returned
            }
        }
        return isNull;
    }
}
