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

/** Member associations retain their physical file's schema, key and native identities. */
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
                        && new AS400Text(3, RawJournalPage.METADATA_CCSID).toObject(payload).toString().matches("[01]0[01]"),
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
                var primary = before.memberless()
                        ? new As400TransactionBuffer.TableIdentity(id.tableId(), id.physicalFile(), member, id.fileJournalId(), jid, id.recordFormat())
                        : id;
                return new As400SchemaChange(before, new As400BindingDefinition(primary, before.schema(), before.codecs(), members), marks);
            }
        }
        throw new As400SchemaTimeline.PendingChangeException();
    }

    static boolean deletionOpening(As400BindingDefinition before, RawJournalPage.Entry entry) {
        return before != null && entry.header().getJournalCode() == 'F' && entry.header().getEntryType().equals("DM");
    }

    static boolean renameOpening(As400BindingDefinition before, RawJournalPage.Entry entry) {
        return before != null && entry.header().getJournalCode() == 'F' && entry.header().getEntryType().equals("MN");
    }

    /** RNMM journals the old member in the header and both names in its payload. */
    static As400SchemaChange rename(As400BindingDefinition before, RawJournalPage.Entry entry, As400JournalOrder order) throws Exception {
        require(renameOpening(before, entry), "Member rename requires its F/MN");
        var h = entry.header();
        var id = before.identity();
        As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
        var control = h.getControlInfo();
        var info = h.getRecordInfo();
        require(h.getFile().equals(id.physicalFile()) && h.getLibrary().equals(id.tableId().schema())
                && h.getCommitCycle().signum() == 0 && control.nestedCommitLevel() == 0 && control.flags() == 0 && control.indicatorFlag() == '0'
                && info.objectNameIndicator() == '1' && info.objectType().equals("*QDDS") && info.recordNumber().signum() == 0
                && info.objectJournalId().equals(before.members().get(h.getMember())), "Member rename identity or context changed");
        byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()));
        require(payload.length >= 60, "Member rename lacks complete names");
        var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
        String renamed = name.toObject(payload, 50).toString().trim();
        require(name.toObject(payload, 0).toString().trim().equals(id.physicalFile())
                && name.toObject(payload, 30).toString().trim().equals(id.physicalFile())
                && name.toObject(payload, 10).toString().trim().equals(id.tableId().schema())
                && name.toObject(payload, 40).toString().trim().equals(id.tableId().schema())
                && name.toObject(payload, 20).toString().trim().equals(h.getMember())
                && renamed.matches("[A-Z0-9_#$@]{1,10}") && !before.members().containsKey(renamed),
                "Member rename changes file/library or lacks distinct member names");
        var members = new TreeMap<>(before.members());
        members.put(renamed, members.remove(h.getMember()));
        var primary = id.member().equals(h.getMember())
                ? new As400TransactionBuffer.TableIdentity(id.tableId(), id.physicalFile(), renamed, id.fileJournalId(), id.memberJournalId(), id.recordFormat())
                : id;
        return new As400SchemaChange(before, new As400BindingDefinition(primary, before.schema(), before.codecs(), members),
                List.of(As400SchemaChange.mark(entry, order)));
    }

    static boolean sameRenamedDefinition(As400BindingDefinition before, As400BindingDefinition after) {
        if (before == null || after == null) {
            return false;
        }
        var a = before.identity();
        var b = after.identity();
        return a.tableId().equals(b.tableId()) && a.physicalFile().equals(b.physicalFile()) && a.fileJournalId().equals(b.fileJournalId())
                && a.memberJournalId().equals(b.memberJournalId()) && a.recordFormat().equals(b.recordFormat())
                && before.schema().equals(after.schema()) && before.codecs().equals(after.codecs())
                && before.members().size() == after.members().size()
                && new java.util.HashSet<>(before.members().values()).equals(new java.util.HashSet<>(after.members().values()))
                && before.members().entrySet().stream().filter(member -> !after.members().entrySet().contains(member)).count() == 1;
    }

    static As400SchemaChange deletion(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        require(!entries.isEmpty() && deletionOpening(before, entries.get(0)), "Member deletion requires its F/DM");
        var opening = entries.get(0);
        String member = opening.header().getMember();
        require(before.members().containsKey(member), "Member deletion requires a retained member");
        var binding = before.binding(65536).forMember(member);
        String producer = As400SchemaChange.producer(opening);
        var marks = new ArrayList<As400SchemaChange.Mark>();
        var previous = opening.position();
        for (var entry : entries) {
            require(marks.isEmpty() || order.compare(previous, entry.position()) < 0, "Member deletion evidence is unordered");
            previous = entry.position();
            As400JournalRecordContent.digest(entry.data(), entry.position());
            var h = entry.header();
            if (!h.getFile().equals(before.identity().physicalFile()) || !h.getLibrary().equals(before.identity().tableId().schema())
                    || !h.getMember().equals(member)) {
                continue;
            }
            As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
            var info = h.getRecordInfo();
            require(producer.equals(As400SchemaChange.producer(entry)) && h.getCommitCycle().signum() == 0
                    && h.getControlInfo().nestedCommitLevel() == 0 && h.getControlInfo().flags() == 0 && h.getControlInfo().indicatorFlag() == '0'
                    && info.objectNameIndicator() == '1' && info.objectType().equals("*QDDS") && info.objectJournalId().equals(binding.identity().memberJournalId())
                    && info.recordNumber().signum() == 0, "Member deletion identity or producer changed");
            var nativeEntry = new As400JournalLifecycle.Entry(h, entry.data(), entry.position());
            byte[] payload = As400JournalLifecycle.payload(nativeEntry);
            if (marks.isEmpty()) {
                var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
                require(nativeEntry.type().equals("F/DM") && payload.length >= 144
                        && name.toObject(payload, 0).toString().trim().equals(h.getFile())
                        && name.toObject(payload, 10).toString().trim().equals(h.getLibrary())
                        && name.toObject(payload, 20).toString().trim().equals(member)
                        && new AS400Text(30, RawJournalPage.METADATA_CCSID).toObject(payload, 30).toString().isBlank(),
                        "Unqualified member deletion metadata");
                marks.add(As400SchemaChange.mark(entry, order));
            }
            else {
                require(nativeEntry.type().equals("F/MD") && payload.length == 0, "Member deletion lacks its exact completion");
                marks.add(As400SchemaChange.mark(entry, order));
                if (before.members().size() == 1) {
                    return new As400SchemaChange(before, before.withoutMembers(), marks);
                }
                var members = new TreeMap<>(before.members());
                members.remove(member);
                var primary = before.memberIdentity(members.containsKey(before.identity().member()) ? before.identity().member() : members.firstKey());
                return new As400SchemaChange(before, new As400BindingDefinition(primary, before.schema(), before.codecs(), members), marks);
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
