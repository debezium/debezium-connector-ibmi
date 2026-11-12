/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.FileCreationEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/**
 * The native file-type flag identifies logical-file metadata at the time of the
 * entry. It does not depend on today's catalog or a name-only exclusion list.
 * Logical files do not own row images; their physical files remain fully checked.
 */
final class As400LogicalFileRecords {
    private As400LogicalFileRecords() {
    }

    static boolean accept(As400JournalLifecycle.Entry entry) {
        var h = entry.header();
        var control = h.getControlInfo();
        if (!control.logicalFile()) {
            return false;
        }
        var object = h.getRecordInfo();
        boolean creation = entry.type().equals("D/CT") || entry.type().equals("F/MC");
        require((h.getJournalCode() == 'D' || h.getJournalCode() == 'F') && object.objectType().equals("*FILE")
                && object.objectNameIndicator() == '1' && As400TransactionBuffer.TableIdentity.validJournalId(object.objectJournalId())
                && h.getFile().matches("[A-Z0-9_#$@]{1,10}") && h.getLibrary().matches("[A-Z0-9_#$@]{1,10}")
                && h.getMember().isEmpty() && !control.hasTransactionIdentifier() && !control.hasLogicalUnitOfWork()
                && control.nestedCommitLevel() >= 0 && h.getCommitCycle().signum() >= 0,
                "Incomplete or inconsistent logical-file metadata identity");
        As400SourceMetadata.from(h, entry.position());
        require(h.getEndOffset() >= 236 && h.getEndOffset() <= entry.bytes().length, "Invalid logical-file metadata bounds");
        if (creation) {
            // This also validates pointer-backed D/CT metadata. The reader has
            // already released its native handle; opaque apply data is not used.
            FileCreationEntryDecoder.metadata(h, entry.bytes(), 0);
        }
        else {
            require(control.flags() == 0x04 && h.getPointerHandle() == 0
                    && (control.indicatorFlag() == '0' || control.indicatorFlag() == '1'),
                    "Unsupported logical-file metadata flags");
            byte[] payload = As400JournalLifecycle.payload(entry);
            if (entry.type().equals("D/CG") || entry.type().equals("D/DT")) {
                // In particular, a damaged physical clear cannot disappear just
                // because its fixed file-type bit disagrees with the payload.
                var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
                require(payload.length >= FileCreationEntryDecoder.METADATA_BYTES
                        && name.toObject(payload, 0).toString().trim().equals(h.getFile())
                        && name.toObject(payload, 10).toString().trim().equals(h.getLibrary())
                        && name.toObject(payload, 124).toString().trim().equals("LF"),
                        "Logical-file metadata contradicts its object payload");
            }
        }
        return true;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
