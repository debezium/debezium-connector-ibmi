/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/**
 * Documented object-creation metadata, never the opaque system apply image.
 * The first 144 bytes describe identity, creation kind and logging; data after
 * that is internal to IBM. A D/CT apply image may be pointer-backed even for a
 * small ordinary table. Its handle must be released by the retrieving API job.
 * https://www.ibm.com/docs/en/i/7.5.0?topic=information-layouts-variable-length-portion-journal-entries
 */
public final class FileCreationEntryDecoder {
    public static final int METADATA_BYTES = 144;

    private FileCreationEntryDecoder() {
    }

    public static byte[] metadata(EntryHeader header, byte[] data, int offset) {
        boolean file = header.getJournalCode() == 'D' && header.getEntryType().equals("CT");
        require(file || header.getJournalCode() == 'F' && header.getEntryType().equals("MC"), "Expected native file/member creation");
        var control = header.getControlInfo();
        var info = header.getRecordInfo();
        boolean logical = control.logicalFile();
        boolean pointers = header.getPointerHandle() != 0;
        require(!pointers || file, "Pointer-backed member creation is unqualified");
        require(header.getCommitCycle().signum() == 0 && control.nestedCommitLevel() == 0
                && !control.hasTransactionIdentifier() && !control.hasLogicalUnitOfWork()
                && control.flags() == ((pointers ? 0x20 : 0) | (logical ? 0x04 : 0)) && control.indicatorFlag() == '0'
                && info.objectNameIndicator() == '1' && info.objectType().equals(file || logical ? "*FILE" : "*QDDS"),
                "Unsupported creation context or incomplete metadata");
        int at = Math.addExact(offset, header.getEntrySpecificDataOffset());
        int end = Math.addExact(offset, header.getEndOffset());
        require(offset >= 0 && at >= offset + 236 && end <= data.length && (long) at + 16 + METADATA_BYTES <= end,
                "Incomplete creation metadata");
        int length = 0;
        for (int i = 0; i < 5; i++) {
            int digit = Byte.toUnsignedInt(data[at + i]) - 0xf0;
            require(digit >= 0 && digit <= 9, "Invalid creation payload length");
            length = length * 10 + digit;
        }
        require(length >= METADATA_BYTES && (long) at + 16 + length <= end, "Truncated creation payload");
        byte[] metadata = Arrays.copyOfRange(data, at + 16, at + 16 + METADATA_BYTES);
        var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
        require(name.toObject(metadata, 0).toString().trim().equals(header.getFile())
                && name.toObject(metadata, 10).toString().trim().equals(header.getLibrary())
                && (logical && !file ? header.getMember().isEmpty() && !name.toObject(metadata, 20).toString().isBlank()
                        : name.toObject(metadata, 20).toString().trim().equals(header.getMember()))
                && new AS400Text(30, RawJournalPage.METADATA_CCSID).toObject(metadata, 30).toString().isBlank(),
                "Creation payload identity differs from its header");
        require(new AS400Text(1, RawJournalPage.METADATA_CCSID).toObject(metadata, 108).equals("9")
                && name.toObject(metadata, 124).toString().trim().equals(logical ? "LF" : "PF")
                && (!file || new AS400Text(1, RawJournalPage.METADATA_CCSID).toObject(metadata, 142).equals("0")),
                "Creation does not match its native file type or logging profile");
        require(!pointers || ByteBuffer.wrap(metadata).getInt(72) > 0, "Pointer-backed creation has no apply image length");
        return metadata;
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
