/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.util.Arrays;
import java.util.Set;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/**
 * Bounds-check native row metadata without following LOB pointers. The caller
 * must release the returned handle in its retrieving API job. This does not
 * decode row values or authorize publication; only a verified excluded binding
 * may consume such a row without its external data.
 * https://www.ibm.com/docs/en/i/7.5.0?topic=objects-journal-entry-layout-lob-columns
 */
public final class PointerRowEntryDecoder {
    private static final Set<String> ROWS = Set.of("PT", "PX", "UB", "UP", "DL", "BR", "DR", "UR", "R1");

    private PointerRowEntryDecoder() {
    }

    public static byte[] metadata(EntryHeader header, byte[] data, int offset) {
        var info = header.getControlInfo();
        require(header.getJournalCode() == 'R' && ROWS.contains(header.getEntryType()) && header.getPointerHandle() != 0
                && info != null && (info.flags() & 0x3f) == 0x20 && !info.hasTransactionIdentifier() && !info.hasLogicalUnitOfWork()
                && info.nestedCommitLevel() >= 0 && header.getRecordInfo().objectType().equals("*QDDS")
                && header.getRecordInfo().objectNameIndicator() == '1' && header.getRecordInfo().recordNumber().signum() > 0,
                "Unsupported pointer-backed row metadata");
        long start = header.getEntrySpecificDataOffset();
        int end = header.getEndOffset();
        require(offset >= 0 && start >= 236 && start + 16 <= end && (long) offset + end <= data.length, "Incomplete pointer-row bounds");
        int at = Math.toIntExact(offset + start), length = 0;
        for (int i = 0; i < 5; i++) {
            int digit = Byte.toUnsignedInt(data[at + i]) - 0xf0;
            require(digit >= 0 && digit <= 9, "Invalid pointer-row length");
            length = length * 10 + digit;
        }
        require(length > 0 && start + 16 + length <= end, "Incomplete pointer-row descriptor");
        // External values and process-local addresses are deliberately absent.
        // Source identity, operation, length and null metadata are compared by
        // the recovery layer; this result is never a complete row checksum.
        return Arrays.copyOfRange(data, at, at + 5);
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
