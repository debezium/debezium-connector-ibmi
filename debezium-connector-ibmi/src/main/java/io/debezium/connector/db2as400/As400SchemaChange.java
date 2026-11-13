/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * A single committed, observed current-definition refresh. Native collection must
 * bracket metadata acquisition with a complete retained journal interval. This
 * never reconstructs an unknown historical definition from the current catalog.
 */
record As400SchemaChange(As400BindingDefinition before, As400BindingDefinition after, List<As400SchemaChange.Mark> records) {

    private static final List<String> TYPES = List.of("F/CH", "F/EJ", "D/EF", "D/JF", "F/JM", "D/CG", "C/SQ", "C/CM");
    static final List<String> CREATION_TYPES = List.of("D/CT", "D/JF", "F/JM", "D/MA", "F/MC");

    record Mark(String type, JournalPosition position, String receiverIdentity, String digest) {
        Mark {
            As400Checkpoint.validatePosition(position);
            require((TYPES.contains(type) || CREATION_TYPES.contains(type) || List.of("D/DT", "F/MD", "D/DF", "C/RB").contains(type))
                    && receiverIdentity != null && !receiverIdentity.isBlank()
                    && digest != null && digest.matches("[0-9a-f]{64}"), "Incomplete schema transition record");
        }
    }

    As400SchemaChange {
        records = List.copyOf(records);
        boolean memberCreation = !records.isEmpty() && records.get(0).type().equals("F/JM");
        boolean deletion = !records.isEmpty() && records.get(0).type().equals("D/DT");
        require(before != null || after != null, "Schema transition requires a definition");
        require(after != null || deletion, "Only a physical-file deletion can retire a binding");
        require(!deletion || before != null && (after == null || before.equals(after)), "Rolled-back deletion must retain its exact binding");
        require(records.stream().map(Mark::type).toList()
                .equals(deletion ? deletionTypes(before, after == null, records.size() > 1 && records.get(1).type().equals("C/CM"))
                        : before == null ? CREATION_TYPES : memberCreation ? As400MemberCreation.TYPES : TYPES),
                "Incomplete schema transition lifecycle");
        if (before != null && after != null) {
            var a = before.identity();
            var b = after.identity();
            require(a.tableId().equals(b.tableId()) && a.physicalFile().equals(b.physicalFile()) && a.member().equals(b.member())
                    && a.fileJournalId().equals(b.fileJournalId()) && a.memberJournalId().equals(b.memberJournalId())
                    && (memberCreation ? after.members().size() == before.members().size() + 1
                            && after.members().entrySet().containsAll(before.members().entrySet())
                            && before.schema().equals(after.schema()) && before.codecs().equals(after.codecs()) : before.members().equals(after.members()))
                    && before.excluded() == after.excluded() && before.usesRrn() == after.usesRrn(),
                    "Schema refresh cannot replace a file/member or change capture selection");
        }
        require(records.stream().map(Mark::position).distinct().count() == records.size(), "Repeated schema transition position");
    }

    boolean memberCreation() {
        return before != null && records.get(0).type().equals("F/JM");
    }

    boolean creationLifecycle() {
        return before == null || memberCreation() || records.get(0).type().equals("D/DT");
    }

    private static List<String> deletionTypes(As400BindingDefinition before, boolean deleted, boolean transactional) {
        if (!deleted) {
            return List.of("D/DT", "C/RB");
        }
        var types = new ArrayList<String>();
        types.add("D/DT");
        if (transactional) {
            types.add("C/CM");
        }
        before.members().forEach((name, jid) -> types.add("F/MD"));
        types.add("D/DF");
        return types;
    }

    JournalPosition start() {
        return records.get(0).position();
    }

    JournalPosition change() {
        // The complete future creation/deletion was qualified before accepting
        // its opening. Persist it here: unrelated output may complete during
        // that lifecycle, while F still pins its full sequence for replay.
        return records.get(creationLifecycle() ? 0 : 5).position();
    }

    JournalPosition commit() {
        return records.get(records.size() - 1).position();
    }

    void verifyRecord(RawJournalPage.Entry entry, As400JournalOrder order) throws Exception {
        Mark expected = records.stream().filter(mark -> mark.position().equals(entry.position())).findFirst().orElseThrow(
                () -> new IllegalArgumentException("Unexpected schema transition position"));
        require(expected.equals(mark(entry, order)), "Schema transition journal bytes or receiver identity changed");
    }

    /** All entries from F/CH through the metadata observation, including other jobs, must be supplied. */
    static As400SchemaChange qualify(As400BindingDefinition before, As400BindingDefinition after, List<RawJournalPage.Entry> entries,
                                     As400JournalOrder order, JournalPosition metadataStartedAfter)
            throws Exception {
        return qualify(before, after, entries, order, metadataStartedAfter, (file, library) -> {
            require(before != null && before.identity().physicalFile().equals(file) && before.identity().tableId().schema().equals(library),
                    "No verified binding for unrelated pointer metadata");
            return before.binding(65536);
        });
    }

    static As400SchemaChange qualify(As400BindingDefinition before, As400BindingDefinition after, List<RawJournalPage.Entry> entries,
                                     As400JournalOrder order, JournalPosition metadataStartedAfter, As400SourceNormalizer.TableResolver verifiedBindings)
            throws Exception {
        if (before == null) {
            return As400FileCreation.qualify(after, entries, order, metadataStartedAfter);
        }
        require(!entries.isEmpty(), "Schema refresh requires complete native history");
        var opening = entries.get(0);
        require(isOpening(opening), "Schema refresh requires its exact subtype-zero F/CH");
        String producer = producer(opening);
        var marks = new ArrayList<Mark>();
        BigInteger cycle = null;
        long level = -1;
        JournalPosition previous = null;
        for (var entry : entries) {
            if (previous != null) {
                require(order.compare(previous, entry.position()) < 0, "Schema evidence is not in journal order");
            }
            previous = entry.position();
            byte[] bytes = entry.data();
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(bytes, 0);
            String type = header.getJournalCode() + "/" + header.getEntryType();
            boolean target = header.getFile().equals(before.identity().physicalFile()) && header.getLibrary().equals(before.identity().tableId().schema());
            boolean control = header.getJournalCode() == 'C' && producer.equals(producer(entry));
            if (!target && !control) {
                // The interval proves this file's committed definition, not the
                // definition of every other file at the observation tail. Another
                // file can be created, altered or re-journaled here; its binding
                // must be selected when normal replay reaches that operation.
                // Keep transport/position checks without interpreting unrelated
                // lifecycle flags as row flags or borrowing an obsolete layout.
                As400JournalRecordContent.digest(bytes, entry.position());
                continue;
            }
            if (target && List.of("F/CH", "F/EJ", "D/EF", "D/JF", "F/JM").contains(type)) {
                // Journaling companions deliberately carry the apply/remove
                // ignore flag. Validate their exact existing lifecycle profile;
                // do not relax the normal row/control validator.
                As400SourceMetadata.from(header, entry.position());
                As400JournalLifecycle.auxiliary(new As400JournalLifecycle.Entry(header, bytes, entry.position()), before.binding(65536),
                        type.equals("F/CH") ? 0 : 0x10);
            }
            else {
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position(), verifiedBindings);
            }
            if (target && (header.getJournalCode() == 'F' || header.getJournalCode() == 'D')
                    && As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(header, bytes, entry.position()),
                            (file, library) -> after.binding(65536))) {
                continue;
            }
            if (marks.size() == TYPES.size()) {
                // A second DDL or journaling transition makes today's catalog
                // ambiguous, even if its final format ID equals the old one.
                require(!target || header.getJournalCode() == 'R', "Another table transition precedes the metadata observation");
                continue;
            }
            require(type.equals(TYPES.get(marks.size())) && producer.equals(producer(entry)), "Unqualified schema-change lifecycle");
            if (type.equals("D/CG")) {
                var change = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(header, bytes, 0);
                require(change.changeType() == '0' && change.file().equals(header.getFile()) && change.library().equals(header.getLibrary())
                        && (change.member().isEmpty() || change.member().equals(before.identity().member())), "Not a matching definition change");
                cycle = header.getCommitCycle();
                level = header.getControlInfo().nestedCommitLevel();
                require(cycle.signum() > 0 && level > 0, "Definition change lacks transaction context");
            }
            else if (type.equals("C/SQ") || type.equals("C/CM")) {
                var controlRecord = new TransactionControlEntryDecoder().decode(header, bytes, 0);
                require(header.getCommitCycle().equals(cycle), "Schema outcome belongs to another transaction");
                if (type.equals("C/SQ")) {
                    require(header.getControlInfo().nestedCommitLevel() == level, "Schema savepoint completion has wrong scope");
                }
                else {
                    require(controlRecord.outcome() == TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT
                            && header.getControlInfo().nestedCommitLevel() == 0, "Schema change did not durably commit");
                }
            }
            if (target) {
                boolean file = header.getJournalCode() == 'D';
                var id = before.identity();
                var info = header.getRecordInfo();
                require(header.getMember().equals(file ? "" : id.member()) && info.objectNameIndicator() == '1'
                        && info.objectType().equals(file ? "*FILE" : "*QDDS")
                        && info.objectJournalId().equals(file ? id.fileJournalId() : id.memberJournalId()), "Schema object identity changed");
            }
            marks.add(mark(entry, order));
        }
        if (marks.size() != TYPES.size()) {
            throw new As400SchemaTimeline.PendingChangeException();
        }
        var result = new As400SchemaChange(before, after, marks);
        require(order.compare(result.commit(), metadataStartedAfter) <= 0, "Metadata was acquired before the schema committed");
        return result;
    }

    static boolean isOpening(RawJournalPage.Entry entry) {
        var h = entry.header();
        if (h.getControlInfo().logicalFile()) {
            return false; // The logical-file classifier validates these separately.
        }
        if (h.getJournalCode() == 'D' && h.getEntryType().equals("DT")) {
            return true;
        }
        if (h.getJournalCode() == 'D' && h.getEntryType().equals("CT")) {
            io.debezium.ibmi.db2.journal.retrieve.FileCreationEntryDecoder.metadata(h, entry.data(), 0);
            return true;
        }
        if (h.getJournalCode() != 'F' || !h.getEntryType().equals("CH")) {
            return false;
        }
        h = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(entry.data(), 0);
        int at = h.getEntrySpecificDataOffset();
        byte[] bytes = entry.data();
        require(at >= 236 && (long) at + 17 <= bytes.length, "Incomplete schema-change companion");
        return new AS400Text(5, RawJournalPage.METADATA_CCSID).toObject(bytes, at).equals("00001")
                && new AS400Text(1, RawJournalPage.METADATA_CCSID).toObject(bytes, at + 16).equals("0");
    }

    static String producer(RawJournalPage.Entry entry) {
        var info = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(entry.data(), 0).getRecordInfo();
        return String.join("/", info.systemName(), info.jobName(), info.jobUser(), info.jobNumber(), info.threadId().toString());
    }

    static Mark mark(RawJournalPage.Entry entry, As400JournalOrder order) throws Exception {
        byte[] bytes = entry.data();
        var h = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(bytes, 0);
        // Use the same documented content comparison as native handoff. Receiver
        // sections may be inherited within a response and alignment is transport
        // placement; every source field and entry-specific byte remains checked.
        java.nio.ByteBuffer.wrap(bytes).putInt(0, 0);
        return new Mark(h.getJournalCode() + "/" + h.getEntryType(), entry.position(), order.incarnation(entry.position()),
                As400JournalRecordContent.digest(bytes, entry.position()));
    }

    Map<String, String> fields() throws Exception {
        var fields = new TreeMap<String, String>();
        fields.put("before", before == null ? "" : As400BindingEncoding.encode(List.of(before), false));
        fields.put("after", after == null ? "" : As400BindingEncoding.encode(List.of(after), false));
        for (int i = 0; i < records.size(); i++) {
            var mark = records.get(i);
            String key = "record." + i + ".";
            fields.put(key + "type", mark.type());
            fields.put(key + "sequence", mark.position().offset().toString());
            fields.put(key + "library", mark.position().receiver().library());
            fields.put(key + "receiver", mark.position().receiver().name());
            fields.put(key + "receiver.identity", mark.receiverIdentity());
            fields.put(key + "hash", mark.digest());
        }
        return Map.copyOf(fields);
    }

    static As400SchemaChange restore(Map<String, String> fields) throws Exception {
        var before = "".equals(fields.get("before")) ? List.<As400BindingDefinition> of() : As400BindingEncoding.decode(fields.get("before"));
        var after = "".equals(fields.get("after")) ? List.<As400BindingDefinition> of() : As400BindingEncoding.decode(fields.get("after"));
        require(before.size() <= 1 && after.size() <= 1 && !(before.isEmpty() && after.isEmpty()), "Schema transition must describe one table");
        var marks = new ArrayList<Mark>();
        var expectedTypes = "D/DT".equals(fields.get("record.0.type"))
                ? deletionTypes(before.get(0), after.isEmpty(), "C/CM".equals(fields.get("record.1.type")))
                : before.isEmpty() ? CREATION_TYPES : "F/JM".equals(fields.get("record.0.type")) ? As400MemberCreation.TYPES : TYPES;
        for (int i = 0; i < expectedTypes.size(); i++) {
            String key = "record." + i + ".";
            marks.add(new Mark(fields.get(key + "type"),
                    new JournalPosition(new BigInteger(fields.get(key + "sequence")), new JournalReceiver(fields.get(key + "receiver"), fields.get(key + "library"))),
                    fields.get(key + "receiver.identity"), fields.get(key + "hash")));
        }
        var restored = new As400SchemaChange(before.isEmpty() ? null : before.get(0), after.isEmpty() ? null : after.get(0), marks);
        require(restored.fields().equals(fields), "Noncanonical or incomplete schema transition");
        return restored;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
