/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.FileCreationEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/** Complete observed creation establishes a new current binding; never a past schema reconstructed from today's catalog. */
final class As400FileCreation {
    private As400FileCreation() {
    }

    /** A null definition performs only the pre-catalog lifecycle check. */
    static As400SchemaChange qualify(As400BindingDefinition after, List<RawJournalPage.Entry> entries,
                                     As400JournalOrder order, JournalPosition metadataStartedAfter)
            throws Exception {
        require(!entries.isEmpty(), "Creation requires its complete observed journal interval");
        var opening = entries.get(0);
        var first = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(opening.data(), 0);
        require(first.getJournalCode() == 'D' && first.getEntryType().equals("CT") && !first.getControlInfo().logicalFile(),
                "Physical-file creation requires a physical D/CT");
        FileCreationEntryDecoder.metadata(first, opening.data(), 0);
        String fileJid = first.getRecordInfo().objectJournalId();
        require(As400TransactionBuffer.TableIdentity.validJournalId(fileJid), "Creation lacks its physical-file identity");
        String producer = As400SchemaChange.producer(opening);
        String member = null, memberJid = null;
        var marks = new ArrayList<As400SchemaChange.Mark>();
        JournalPosition previous = null;
        for (var entry : entries) {
            require(previous == null || order.compare(previous, entry.position()) < 0, "Creation evidence is out of order");
            previous = entry.position();
            byte[] bytes = entry.data();
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(bytes, 0);
            if (!header.getFile().equals(first.getFile()) || !header.getLibrary().equals(first.getLibrary())) {
                continue; // Other tables and transactions do not establish this file's creation.
            }
            var nativeEntry = new As400JournalLifecycle.Entry(header, bytes, entry.position());
            if (marks.size() == As400SchemaChange.CREATION_TYPES.size()) {
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position());
                if (As400JournalLifecycle.administrativeType(nativeEntry.type())) {
                    if (after != null) {
                        As400JournalLifecycle.administrative(nativeEntry, (file, library) -> after.binding(65536));
                    }
                    continue;
                }
                require(header.getJournalCode() == 'R', "Another table transition precedes creation metadata acquisition");
                require(header.getRecordInfo().objectJournalId().equals(memberJid) && header.getMember().equals(member)
                        && header.getRecordInfo().objectType().equals("*QDDS"), "Rows belong to a different created member");
                continue;
            }
            String type = nativeEntry.type();
            require(type.equals(As400SchemaChange.CREATION_TYPES.get(marks.size())) && producer.equals(As400SchemaChange.producer(entry)),
                    "Incomplete or ambiguous native creation lifecycle");
            As400SourceMetadata.from(header, entry.position());
            if (type.equals("D/CT") || type.equals("F/MC")) {
                FileCreationEntryDecoder.metadata(header, bytes, 0);
            }
            else {
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position());
            }
            require(header.getCommitCycle().signum() == 0 && header.getControlInfo().nestedCommitLevel() == 0
                    && header.getRecordInfo().objectNameIndicator() == '1', "Transactional or unidentified creation is unqualified");
            if (type.equals("F/JM")) {
                member = header.getMember();
                memberJid = header.getRecordInfo().objectJournalId();
                require(!member.isBlank() && As400TransactionBuffer.TableIdentity.validJournalId(memberJid)
                        && !memberJid.equals(fileJid), "Creation requires separate file and member identities");
            }
            boolean file = header.getJournalCode() == 'D';
            require(header.getRecordInfo().objectType().equals(file ? "*FILE" : "*QDDS")
                    && header.getRecordInfo().objectJournalId().equals(file ? fileJid : memberJid)
                    && header.getMember().equals(file ? "" : member), "Creation file/member identity mismatch");
            byte[] payload = As400JournalLifecycle.payload(nativeEntry);
            if (type.equals("D/JF") || type.equals("F/JM")) {
                require(header.getControlInfo().indicatorFlag() == '1' && payload.length == 3
                        && new AS400Text(3, RawJournalPage.METADATA_CCSID).toObject(payload).toString().matches("[01]00"),
                        "Creation has an unsupported journaling profile");
            }
            if (type.equals("D/MA")) {
                var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
                require(payload.length == 40 && header.getControlInfo().indicatorFlag() == '0'
                        && HexFormat.of().formatHex(payload, 0, 10).equals(memberJid)
                        && name.toObject(payload, 10).toString().trim().equals(first.getFile())
                        && name.toObject(payload, 20).toString().trim().equals(first.getLibrary())
                        && name.toObject(payload, 30).toString().trim().equals(member), "Native member association does not match its file");
            }
            marks.add(As400SchemaChange.mark(entry, order));
        }
        if (marks.size() != As400SchemaChange.CREATION_TYPES.size()) {
            throw new As400SchemaTimeline.PendingChangeException();
        }
        require(order.compare(marks.get(marks.size() - 1).position(), metadataStartedAfter) <= 0,
                "Catalog was read before creation completed");
        if (after == null) {
            return null;
        }
        var id = after.identity();
        require(id.tableId().schema().equals(first.getLibrary()) && id.physicalFile().equals(first.getFile()) && id.member().equals(member)
                && id.fileJournalId().equals(fileJid) && id.memberJournalId().equals(memberJid), "Current catalog differs from the observed creation");
        return new As400SchemaChange(null, after, marks);
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
