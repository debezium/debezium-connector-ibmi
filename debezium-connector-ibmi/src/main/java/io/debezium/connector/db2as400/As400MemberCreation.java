/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeMap;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.FileCreationEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/** A newly added member inherits its physical file's already retained record format. */
final class As400MemberCreation {
    static final List<String> TYPES = List.of("F/JM", "D/MA", "F/MC");

    private As400MemberCreation() {
    }

    static boolean opening(As400BindingDefinition before, RawJournalPage.Entry entry) {
        var header = entry.header();
        return before != null && header.getJournalCode() == 'F' && header.getEntryType().equals("JM")
                && !before.members().containsKey(header.getMember());
    }

    static As400SchemaChange qualify(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        require(!entries.isEmpty() && opening(before, entries.get(0)), "Member creation requires an unknown member's F/JM");
        var first = entries.get(0);
        var firstHeader = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(first.data(), 0);
        var id = before.identity();
        String member = firstHeader.getMember(), jid = firstHeader.getRecordInfo().objectJournalId();
        require(member.matches("[A-Z0-9_#$@]{1,10}") && As400TransactionBuffer.TableIdentity.validJournalId(jid)
                && !jid.equals(id.fileJournalId()) && !before.members().containsValue(jid), "New member must have a distinct native identity");
        String producer = As400SchemaChange.producer(first);
        var marks = new ArrayList<As400SchemaChange.Mark>();
        RawJournalPage.Entry previous = null;
        for (var entry : entries) {
            require(previous == null || order.compare(previous.position(), entry.position()) < 0, "Member creation evidence is out of order");
            previous = entry;
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(entry.data(), 0);
            if (!header.getFile().equals(id.physicalFile()) || !header.getLibrary().equals(id.tableId().schema())) {
                continue;
            }
            var nativeEntry = new As400JournalLifecycle.Entry(header, entry.data(), entry.position());
            // Work in existing members does not establish the new member. It
            // still traverses the normal decoder when the interval is replayed.
            if (!header.getMember().isEmpty() && before.members().containsKey(header.getMember())
                    && (header.getJournalCode() == 'R' || As400JournalLifecycle.administrativeType(nativeEntry.type()))) {
                continue;
            }
            As400SourceNormalizer.validateSource(header, entry.data(), 0, entry.position());
            var info = header.getRecordInfo();
            boolean file = header.getJournalCode() == 'D';
            require(nativeEntry.type().equals(TYPES.get(marks.size())) && producer.equals(As400SchemaChange.producer(entry))
                    && header.getCommitCycle().signum() == 0 && header.getControlInfo().nestedCommitLevel() == 0
                    && info.objectNameIndicator() == '1' && info.objectType().equals(file ? "*FILE" : "*QDDS")
                    && info.objectJournalId().equals(file ? id.fileJournalId() : jid) && header.getMember().equals(file ? "" : member),
                    "Incomplete or mismatched native member creation");
            byte[] payload = As400JournalLifecycle.payload(nativeEntry);
            var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
            if (nativeEntry.type().equals("F/JM")) {
                require(header.getControlInfo().indicatorFlag() == '1' && payload.length == 3
                        && new AS400Text(3, RawJournalPage.METADATA_CCSID).toObject(payload).toString().matches("[01]00"),
                        "New member has an unsupported journaling profile");
            }
            else if (nativeEntry.type().equals("D/MA")) {
                require(header.getControlInfo().indicatorFlag() == '0' && payload.length == 40 && HexFormat.of().formatHex(payload, 0, 10).equals(jid)
                        && name.toObject(payload, 10).toString().trim().equals(id.physicalFile())
                        && name.toObject(payload, 20).toString().trim().equals(id.tableId().schema())
                        && name.toObject(payload, 30).toString().trim().equals(member), "Member association differs from the retained physical file");
            }
            else {
                FileCreationEntryDecoder.metadata(header, entry.data(), 0);
            }
            marks.add(As400SchemaChange.mark(entry, order));
            if (marks.size() == TYPES.size()) {
                var members = new TreeMap<>(before.members());
                members.put(member, jid);
                return new As400SchemaChange(before, new As400BindingDefinition(id, before.schema(), before.codecs(), members), marks);
            }
        }
        throw new As400SchemaTimeline.PendingChangeException();
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
