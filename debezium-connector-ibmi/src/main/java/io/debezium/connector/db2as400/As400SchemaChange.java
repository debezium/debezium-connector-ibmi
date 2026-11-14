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
record As400SchemaChange(As400BindingDefinition before, As400BindingDefinition after, List<As400SchemaChange.Mark> records, boolean deferred,
        As400ExcludedCreation excludedCreation) {

    As400SchemaChange(As400BindingDefinition before, As400BindingDefinition after, List<Mark> records, boolean deferred) {
        this(before, after, records, deferred, null);
    }

    As400SchemaChange(As400BindingDefinition before, As400BindingDefinition after, List<Mark> records) {
        this(before, after, records, false);
    }

    private static final List<String> TYPES = List.of("F/CH", "F/EJ", "D/EF", "D/JF", "F/JM", "D/CG", "C/SQ", "C/CM");
    private static final java.util.Set<String> RESTORED_LAYOUT_SUFFIX = java.util.Set.of(
            "R/PT", "R/PX", "R/UB", "R/UP", "R/DL", "R/DR", "R/UR", "R/BR", "R/R1", "F/OP", "F/CL");
    static final List<String> CREATION_TYPES = List.of("D/CT", "D/JF", "F/JM", "D/MA", "F/MC");

    static final class MetadataMovedException extends IllegalArgumentException {
        MetadataMovedException(String message) {
            super(message);
        }
    }

    static void requireMetadataAfter(JournalPosition completion, JournalPosition observedBeforeAcquisition, As400JournalOrder order, String reason) {
        if (order.compare(completion, observedBeforeAcquisition) > 0) {
            throw new MetadataMovedException(reason);
        }
    }

    record Mark(String type, JournalPosition position, String receiverIdentity, String digest) {
        Mark {
            As400Checkpoint.validatePosition(position);
            require((TYPES.contains(type) || CREATION_TYPES.contains(type) || As400JournalLifecycle.administrativeType(type)
                    || List.of("D/FN", "F/MN", "D/FM", "F/MM", "D/DT", "F/DM", "F/MD", "D/DF", "C/RB",
                            "R/PT", "R/PX", "R/UB", "R/UP", "R/DL", "R/DR", "R/UR").contains(type))
                    && receiverIdentity != null && !receiverIdentity.isBlank()
                    && digest != null && digest.matches("[0-9a-f]{64}"), "Incomplete schema transition record");
        }
    }

    As400SchemaChange {
        records = List.copyOf(records);
        require(excludedCreation == null || before == null && after == null && !records.isEmpty()
                && excludedCreation.from().equals(records.get(0).position())
                && excludedCreation.through().equals(records.get(records.size() - 1).position()), "Excluded creation lacks its complete retained lifetime");
        boolean memberCreation = !records.isEmpty() && records.get(0).type().equals("F/JM");
        boolean memberDeletion = !records.isEmpty() && records.get(0).type().equals("F/DM");
        boolean memberRename = !records.isEmpty() && records.get(0).type().equals("F/MN");
        boolean deletion = !records.isEmpty() && records.get(0).type().equals("D/DT");
        boolean rename = !records.isEmpty() && records.get(0).type().equals("D/FN");
        boolean move = !records.isEmpty() && records.get(0).type().equals("D/FM");
        boolean changedThenDeleted = before != null && after == null && !records.isEmpty() && records.get(0).type().equals("F/CH");
        boolean abortedCreation = before == null && after == null;
        require(after != null || deletion || rename || changedThenDeleted || abortedCreation, "Only a physical-file deletion can retire a binding");
        require(!deletion || before != null && (after == null || before.equals(after)), "Rolled-back deletion must retain its exact binding");
        var types = records.stream().map(Mark::type).toList();
        require(move ? before != null && types.size() == before.members().size() + 1
                && types.subList(1, types.size()).stream().allMatch("F/MM"::equals)
                : rename ? before != null && (after != null ? renameTypes(types) : renamedDeletionTypes(before, types))
                        : changedThenDeleted ? changedDeletionTypes(before, types)
                                : abortedCreation ? abortedCreationTypes(types)
                                        : deletion ? types.equals(deletionTypes(before, after == null, records.size() > 1 && records.get(1).type().equals("C/CM")))
                                                : before == null ? creationWithMetadata(types)
                                                        : memberCreation ? types.equals(As400MemberCreation.TYPES)
                                                                : memberDeletion ? types.equals(List.of("F/DM", "F/MD"))
                                                                        : memberRename ? types.equals(List.of("F/MN"))
                                                                                : metadataTypes(types) || schemaTypes(types) || restoredDeletionTypes(types),
                "Incomplete schema transition lifecycle");
        require(!types.contains("C/RB") || before == null || before.equals(after)
                || before.unobservedLayout() && after != null && !after.unobservedLayout(),
                "Rollback must retain the original binding or observe its previously unknown restored layout");
        if (move) {
            require(As400FileRename.sameMovedDefinition(before, after), "File move changed the retained definition or native identities");
        }
        else if (rename) {
            require(after == null || As400FileRename.sameDefinition(before, after), "Rename changed the retained definition or native identities");
        }
        else if (memberRename) {
            require(As400MemberCreation.sameRenamedDefinition(before, after), "Member rename changed the retained definition or native identities");
        }
        else if (before != null && after != null) {
            var a = before.identity();
            var b = after.identity();
            require((a.tableId().equals(b.tableId()) || sqlNameChange(before, after, records)) && a.physicalFile().equals(b.physicalFile())
                    && a.fileJournalId().equals(b.fileJournalId())
                    && (memberDeletion ? before.members().size() == after.members().size() + 1
                            && before.members().entrySet().containsAll(after.members().entrySet())
                            && before.schema().equals(after.schema()) && before.codecs().equals(after.codecs())
                            : (memberCreation && before.memberless() || a.member().equals(b.member()) && a.memberJournalId().equals(b.memberJournalId()))
                                    && (memberCreation ? after.members().size() == before.members().size() + 1
                                            && after.members().entrySet().containsAll(before.members().entrySet())
                                            && before.schema().equals(after.schema()) && before.codecs().equals(after.codecs())
                                            : before.members().equals(after.members())))
                    && (before.unobservedLayout() || after.unobservedLayout()
                            || before.excluded() == after.excluded() && before.usesRrn() == after.usesRrn()),
                    "Schema refresh cannot replace a file/member or change capture selection");
        }
        require(records.stream().map(Mark::position).distinct().count() == records.size(), "Repeated schema transition position");
        require(!deferred || (before != null && List.of("F/CH", "D/DT").contains(records.get(0).type())
                || before == null && records.get(0).type().equals("D/CT"))
                && records.stream().anyMatch(mark -> mark.type().equals("C/CM") || mark.type().equals("C/RB")),
                "Deferred schema history requires a qualified transactional schema outcome");
    }

    boolean memberCreation() {
        return before != null && records.get(0).type().equals("F/JM");
    }

    boolean abortedCreation() {
        return before == null && after == null;
    }

    boolean rolledBackAlter() {
        return before != null && records.get(0).type().equals("F/CH") && records.stream().anyMatch(mark -> mark.type().equals("C/RB"));
    }

    boolean retiredByRename() {
        return before != null && after == null && records.get(0).type().equals("D/FN");
    }

    boolean creationLifecycle() {
        return before == null || memberCreation() || List.of("D/FN", "D/FM", "D/DT", "F/DM", "F/MN").contains(records.get(0).type());
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

    private static boolean renameTypes(List<String> types) {
        return !types.isEmpty() && types.get(0).equals("D/FN") && types.contains("F/MN")
                && types.stream().allMatch(type -> List.of("D/FN", "F/MN", "F/CH", "D/CG").contains(type)
                        || As400JournalLifecycle.administrativeType(type));
    }

    private static boolean renamedDeletionTypes(As400BindingDefinition before, List<String> types) {
        int drop = types.indexOf("D/DT");
        return drop > 1 && renameTypes(types.subList(0, drop))
                && types.subList(drop, types.size()).equals(deletionTypes(before, true, types.size() > drop + 1 && types.get(drop + 1).equals("C/CM")));
    }

    private static boolean changedDeletionTypes(As400BindingDefinition before, List<String> types) {
        int drop = types.indexOf("D/DT");
        if (drop <= 0) {
            return false;
        }
        var prefix = new ArrayList<>(types.subList(0, drop));
        if (!prefix.contains("C/CM") && types.size() > drop + 1 && types.get(drop + 1).equals("C/CM")) {
            prefix.add("C/CM"); // The same qualified terminal completes ALTER and DROP.
        }
        return (metadataTypes(prefix) || schemaTypes(prefix))
                && types.subList(drop, types.size()).equals(deletionTypes(before, true, types.size() > drop + 1 && types.get(drop + 1).equals("C/CM")));
    }

    private static boolean restoredDeletionTypes(List<String> types) {
        int drop = types.indexOf("D/DT");
        if (drop <= 0 || drop != types.lastIndexOf("D/DT") || drop + 1 >= types.size()
                || !types.get(drop + 1).equals("C/RB") || types.contains("C/CM")) {
            return false;
        }
        var restored = new ArrayList<>(types);
        restored.remove(drop);
        return schemaTypes(restored);
    }

    /** Replay must recheck names and capture selection before using an unavailable intermediate layout. */
    boolean sharedAlterDeletion() {
        int drop = records.stream().map(Mark::type).toList().indexOf("D/DT");
        return before != null && after == null && records.get(0).type().equals("F/CH") && drop > 0
                && records.subList(0, drop).stream().noneMatch(mark -> mark.type().equals("C/CM"));
    }

    static List<String> creationTypes(boolean transactional) {
        var types = new ArrayList<>(CREATION_TYPES);
        if (transactional) {
            types.add("C/CM");
        }
        return types;
    }

    private static boolean creationWithMetadata(List<String> types) {
        if (types.size() < CREATION_TYPES.size() || !types.subList(0, CREATION_TYPES.size()).equals(CREATION_TYPES)) {
            return false;
        }
        var physical = withoutMetadata(types).stream().filter(type -> !As400JournalLifecycle.administrativeType(type)).toList();
        if (physical.equals(CREATION_TYPES) || physical.equals(creationTypes(true))) {
            return true;
        }
        int suffix = CREATION_TYPES.size();
        if (physical.get(suffix).equals("C/CM")) {
            suffix++;
        }
        if (physical.get(suffix).equals("D/FN")) {
            int at = suffix;
            while (at < physical.size()) {
                if (!physical.get(at++).equals("D/FN") || at == physical.size() || !physical.get(at).equals("F/MN")) {
                    return false;
                }
                while (at < physical.size() && physical.get(at).equals("F/MN")) {
                    at++;
                }
            }
            return true;
        }
        return schemaTypes(physical.subList(suffix, physical.size())) && !physical.contains("C/RB");
    }

    private static List<String> withoutMetadata(List<String> types) {
        var result = new ArrayList<String>();
        for (int at = 0; at < types.size(); at++) {
            if (at + 1 < types.size() && types.get(at).equals("F/CH") && types.get(at + 1).equals("D/CG")) {
                // Exact subtype, producer, flags and JIDs are checked before marks are created.
                at++;
            }
            else {
                result.add(types.get(at));
            }
        }
        return result;
    }

    private static boolean schemaTypes(List<String> types) {
        types = withoutMetadata(types);
        int at = 0;
        while (at < types.size()) {
            if (at + 6 >= types.size() || !types.subList(at, at + 6).equals(TYPES.subList(0, 6))) {
                return false;
            }
            at += 6;
            if (!types.get(at).equals("C/SQ")) {
                return false;
            }
            while (at < types.size() && types.get(at).equals("C/SQ")) {
                at++;
            }
            while (at < types.size() && types.get(at).startsWith("R/")) {
                at++;
            }
            if (at < types.size() && types.get(at).equals("C/RB")) {
                return types.subList(at, types.size()).equals(List.of("C/RB", "F/EJ", "D/EF", "D/JF", "F/JM"));
            }
            if (at < types.size() && types.get(at).equals("C/CM")) {
                at++;
            }
        }
        return !types.isEmpty() && types.get(types.size() - 1).equals("C/CM");
    }

    private static boolean metadataTypes(List<String> types) {
        if (types.isEmpty() || types.size() % 2 != 0) {
            return false;
        }
        for (int i = 0; i < types.size(); i++) {
            if (!types.get(i).equals(i % 2 == 0 ? "F/CH" : "D/CG")) {
                return false;
            }
        }
        return true;
    }

    private static boolean abortedCreationTypes(List<String> types) {
        types = withoutMetadata(types);
        if (types.size() < CREATION_TYPES.size() + 3 || !types.subList(0, CREATION_TYPES.size()).equals(CREATION_TYPES)) {
            return false;
        }
        int drop = types.indexOf("D/DT");
        if (drop >= CREATION_TYPES.size()) {
            var suffix = types.subList(drop, types.size());
            boolean transactional = suffix.equals(List.of("D/DT", "C/CM", "F/MD", "D/DF"));
            if (transactional || suffix.equals(List.of("D/DT", "F/MD", "D/DF"))) {
                var prefix = new ArrayList<>(types.subList(0, drop));
                if (transactional && !prefix.get(prefix.size() - 1).equals("C/CM")) {
                    prefix.add("C/CM"); // The qualified creation/ALTER and DROP share this terminal.
                }
                return creationWithMetadata(prefix);
            }
        }
        int at = CREATION_TYPES.size();
        while (at < types.size() && types.get(at).startsWith("R/")) {
            at++;
        }
        if (at == CREATION_TYPES.size()) {
            var suffix = types.subList(at, types.size());
            if (suffix.equals(List.of("D/DT", "F/MD", "D/DF"))
                    || suffix.equals(List.of("D/DT", "C/CM", "F/MD", "D/DF"))
                    || suffix.equals(List.of("C/CM", "D/DT", "F/MD", "D/DF"))
                    || suffix.equals(List.of("C/CM", "D/DT", "C/CM", "F/MD", "D/DF"))) {
                return true;
            }
        }
        return types.subList(at, types.size()).equals(List.of("C/RB", "F/MD", "D/DF"));
    }

    JournalPosition start() {
        return records.get(0).position();
    }

    JournalPosition change() {
        // The complete future creation/deletion was qualified before accepting
        // its opening. Persist it here: unrelated output may complete during
        // that lifecycle, while F still pins its full sequence for replay.
        if (creationLifecycle()) {
            return records.get(0).position();
        }
        return records.stream().filter(mark -> mark.type().equals("D/CG")).findFirst().orElseThrow().position();
    }

    JournalPosition commit() {
        return records.get(records.size() - 1).position();
    }

    /** New deferred changes become visible at their first qualified outcome, not at the earlier physical D/CG. */
    JournalPosition visibility() {
        return deferred ? records.stream().filter(mark -> mark.type().equals("C/CM") || mark.type().equals("C/RB"))
                .findFirst().orElseThrow().position() : change();
    }

    As400SchemaChange withDeferredVisibility() {
        return new As400SchemaChange(before, after, records, true, excludedCreation);
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
            return As400FileCreation.qualify(after, entries, order, metadataStartedAfter, verifiedBindings);
        }
        if (!entries.isEmpty() && entries.get(0).header().getJournalCode() == 'D' && entries.get(0).header().getEntryType().equals("FN")) {
            return As400FileRename.qualify(before, after, entries, order, verifiedBindings);
        }
        var deleted = As400FileDeletion.afterSchemaChange(before, entries, order, verifiedBindings);
        if (deleted != null) {
            return deleted;
        }
        boolean excluded = verifiedBindings.resolve(before.identity().physicalFile(), before.identity().tableId().schema()).excluded();
        var evidence = qualifyEvidence(ObjectIdentity.of(before), entries, order, verifiedBindings, excluded, false);
        requireMetadataAfter(evidence.observedThrough(), metadataStartedAfter, order, "Metadata was acquired before the schema committed or was restored");
        if (!evidence.definitionChange()) {
            after = retainEventKey(before, after);
        }
        boolean sqlRename = sqlNameChange(before, after, evidence.records());
        if (sqlRename && (!excluded || verifiedBindings.needsValues(after.schema().id()))) {
            var finalMetadata = evidence.records().get(evidence.records().size() - 1).position();
            require(entries.stream().noneMatch(entry -> entry.header().getJournalCode() == 'R'
                    && entry.header().getFile().equals(before.identity().physicalFile())
                    && entry.header().getLibrary().equals(before.identity().tableId().schema())
                    && order.compare(entry.position(), finalMetadata) < 0),
                    "Captured rows require an unavailable intermediate SQL table name");
            require(entries.stream().noneMatch(entry -> clearOpening(entry)
                    && entry.header().getFile().equals(before.identity().physicalFile())
                    && entry.header().getLibrary().equals(before.identity().tableId().schema())
                    && order.compare(entry.position(), finalMetadata) < 0),
                    "Captured clears require their historical SQL table name");
        }
        require(evidence.definitionChange() || after != null && (excluded || before.equals(after) || sqlRename),
                "Miscellaneous change altered a captured table's schema");
        boolean restoredLayout = evidence.rollback() && after != null && !after.unobservedLayout()
                && restoredLayoutStable(before, entries, evidence.observedThrough(), order);
        return new As400SchemaChange(before, evidence.rollback() && !restoredLayout ? before : after, evidence.records());
    }

    /**
     * The layout of an excluded committed prefix may have been unobservable
     * while a later ALTER held the catalog. After that ALTER is fully restored,
     * a stable suffix permits acquisition of the surviving definition. Do not
     * use a later DDL/constraint/name change as the rollback's definition.
     */
    static boolean restoredLayoutStable(As400BindingDefinition before, List<RawJournalPage.Entry> entries,
                                        JournalPosition restoredThrough, As400JournalOrder order) {
        if (before == null || !before.unobservedLayout()) {
            return false;
        }
        for (var entry : entries) {
            var h = entry.header();
            if (order.compare(entry.position(), restoredThrough) <= 0
                    || !h.getFile().equals(before.identity().physicalFile()) || !h.getLibrary().equals(before.schema().id().schema())) {
                continue;
            }
            String type = h.getJournalCode() + "/" + h.getEntryType();
            if (!RESTORED_LAYOUT_SUFFIX.contains(type)) {
                return false;
            }
        }
        return true;
    }

    /** SQL RENAME can preserve the system name and JIDs, with only a metadata pair in the journal. */
    private static boolean sqlNameChange(As400BindingDefinition before, As400BindingDefinition after, List<Mark> records) {
        return before != null && after != null && !before.schema().id().equals(after.schema().id())
                && before.identity().physicalFile().equals(after.identity().physicalFile())
                && metadataTypes(records.stream().map(Mark::type).toList()) && (before.unobservedLayout() || As400FileRename.sameDefinition(before, after));
    }

    /** Metadata/rename observations must not apply a later constraint's key to retained rows. */
    static As400BindingDefinition retainEventKey(As400BindingDefinition before, As400BindingDefinition after) {
        if (after == null || before.excluded() || after.excluded()) {
            return after;
        }
        require(before.schema().primaryKeyColumnNames().stream().allMatch(name -> after.schema().columnWithName(name) != null),
                "Metadata observation removed a retained key column");
        return new As400BindingDefinition(after.identity(), after.schema().edit().setPrimaryKeyNames(before.schema().primaryKeyColumnNames()).create(),
                after.codecs(), after.members());
    }

    /** File/member association without inventing a row layout for a newly created file. */
    record ObjectIdentity(String file, String library, String fileJid, String member, Map<String, String> members) {
        ObjectIdentity {
            members = Map.copyOf(members);
        }

        static ObjectIdentity of(As400BindingDefinition definition) {
            var id = definition.identity();
            return new ObjectIdentity(id.physicalFile(), id.tableId().schema(), id.fileJournalId(), id.member(), definition.members());
        }

        void identity(io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader h) {
            boolean fileRecord = h.getJournalCode() == 'D';
            require(h.getFile().equals(file) && h.getLibrary().equals(library) && h.getMember().equals(fileRecord ? "" : member)
                    && h.getRecordInfo().objectNameIndicator() == '1' && h.getRecordInfo().objectType().equals(fileRecord ? "*FILE" : "*QDDS")
                    && h.getRecordInfo().objectJournalId().equals(fileRecord ? fileJid : members.get(member)), "Schema object identity changed");
        }

        void auxiliary(As400JournalLifecycle.Entry entry, int flags) {
            identity(entry.header());
            As400JournalLifecycle.auxiliary(entry, flags);
        }
    }

    record Evidence(List<Mark> records, boolean rollback, boolean definitionChange, JournalPosition observedThrough) {
        Evidence(List<Mark> records, boolean rollback, boolean definitionChange) {
            this(records, rollback, definitionChange, records.get(records.size() - 1).position());
        }

        Evidence {
            records = List.copyOf(records);
        }
    }

    /**
     * A later open ALTER need not hold an already committed excluded table's
     * transaction. Keep identity/name history, explicitly without a row layout.
     * A future transition into capture must acquire a complete definition.
     */
    static As400SchemaChange excludedCommittedPrefix(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order,
                                                     As400SourceNormalizer.TableResolver bindings, As400HistoricalTableNames.Reader names)
            throws Exception {
        if (before == null || names == null || entries.isEmpty() || metadataOpening(entries.get(0))
                || bindings.needsValues(before.schema().id())) {
            return null;
        }
        var id = ObjectIdentity.of(before);
        BigInteger cycle = null;
        for (int at = 0; at < entries.size(); at++) {
            var entry = entries.get(at);
            var h = entry.header();
            if (cycle == null && h.getJournalCode() == 'D' && h.getEntryType().equals("CG")
                    && h.getFile().equals(id.file()) && h.getLibrary().equals(id.library())) {
                var changed = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(h, entry.data(), 0);
                if (changed.changeType() != '0' || h.getCommitCycle().signum() <= 0) {
                    return null;
                }
                cycle = h.getCommitCycle();
            }
            if (cycle == null || h.getJournalCode() != 'C' || !h.getCommitCycle().equals(cycle)
                    || !List.of("CM", "RB").contains(h.getEntryType())) {
                continue;
            }
            if (!h.getEntryType().equals("CM")) {
                return null;
            }
            var prefix = entries.subList(0, at + 1);
            var proof = qualifyEvidence(id, prefix, order, bindings, true, false);
            require(!proof.rollback() && proof.records().get(proof.records().size() - 1).position().equals(entry.position()),
                    "Excluded schema prefix lacks its exact committed outcome");
            var table = before.schema().id();
            for (var item : prefix) {
                var header = item.header();
                if (header.getJournalCode() == 'D' && header.getEntryType().equals("CG")
                        && header.getFile().equals(id.file()) && header.getLibrary().equals(id.library())) {
                    table = names.read(item).changed(item, table);
                    if (bindings.needsValues(table) || !table.equals(before.schema().id())) {
                        return null; // A changed selection/name requires its own qualified transition.
                    }
                }
            }
            return new As400SchemaChange(before, before.withoutObservedLayout(), proof.records());
        }
        return null;
    }

    static Evidence qualifyEvidence(ObjectIdentity identity, List<RawJournalPage.Entry> entries, As400JournalOrder order,
                                    As400SourceNormalizer.TableResolver verifiedBindings, boolean excluded, boolean creation)
            throws Exception {
        require(!entries.isEmpty() && isOpening(entries.get(0)), "Schema refresh requires its exact F/CH opening");
        var marks = new ArrayList<Mark>();
        var miscellaneous = new Miscellaneous(identity.file(), identity.library(), identity.fileJid(), identity.members());
        var clear = new LayoutPreservingClear(identity);
        var cycles = new java.util.HashMap<BigInteger, SchemaCycle>();
        SchemaCycle rollback = null;
        int restoration = 0;
        String producer = null;
        JournalPosition opening = null;
        JournalPosition previous = null;
        JournalPosition restoredThrough = null;
        boolean interveningRows = false;
        boolean definitionChange = false;
        boolean commandChange = false;
        boolean lastMiscellaneous = false;
        BigInteger internalCycle = null;
        BigInteger internalMarker = null;
        long internalLevel = 0;
        long minimumInternalLevel = 0;
        int step = 0;
        for (int index = 0; index < entries.size(); index++) {
            var entry = entries.get(index);
            require(previous == null || order.compare(previous, entry.position()) < 0, "Schema evidence is not in journal order");
            previous = entry.position();
            byte[] bytes = entry.data();
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(bytes, 0);
            String type = header.getJournalCode() + "/" + header.getEntryType();
            boolean target = header.getFile().equals(identity.file()) && header.getLibrary().equals(identity.library());
            var context = cycles.get(header.getCommitCycle());
            if (step != 0 && header.getJournalCode() == 'C' && producer.equals(producer(entry))) {
                // Native ALTER can commit an internal nested operation after
                // D/EF, before D/JF. Keep it separate from schema-history marks:
                // it neither changes the layout nor releases the ALTER scope.
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position(), verifiedBindings);
                var control = new TransactionControlEntryDecoder().decode(header, bytes, 0);
                var cycle = header.getCommitCycle();
                long level = header.getControlInfo().nestedCommitLevel();
                require(step == 3 && cycle.signum() > 0 && (internalCycle == null || internalCycle.equals(cycle)),
                        "Internal schema savepoint has wrong transaction or lifecycle position");
                if (type.equals("C/SB")) {
                    require(internalMarker == null && level > 0 && (context == null || level > context.level),
                            "Internal schema savepoint has wrong nesting");
                    internalCycle = cycle;
                    internalMarker = entry.position().offset();
                    internalLevel = level;
                    minimumInternalLevel = minimumInternalLevel == 0 ? level : Math.min(minimumInternalLevel, level);
                }
                else {
                    require(type.equals("C/SQ") && internalMarker != null && internalMarker.equals(control.savepointSequence())
                            && internalLevel == level, "Internal schema savepoint lacks its exact release");
                    internalMarker = null;
                }
                continue;
            }
            clear.control(entry);
            if (!target && !(header.getJournalCode() == 'C' && context != null)) {
                As400JournalRecordContent.digest(bytes, entry.position());
                continue;
            }
            if (!target) {
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position(), verifiedBindings);
                require(context.producer.equals(producer(entry)), "Schema outcome belongs to another producer");
                var control = new TransactionControlEntryDecoder().decode(header, bytes, 0);
                if (type.equals("C/SQ")) {
                    long level = header.getControlInfo().nestedCommitLevel();
                    var targetMarker = control.savepointSequence();
                    Long observedLevel = context.markers.get(targetMarker);
                    require(step == 0 && level > 0 && level <= context.level && targetMarker.signum() > 0
                            && (observedLevel != null ? observedLevel == level : targetMarker.compareTo(context.beforeMarker) < 0),
                            "Schema savepoint completion has wrong scope");
                    context.level = level - 1;
                    if (observedLevel == null) {
                        context.beforeMarker = targetMarker;
                    }
                    context.markers.keySet().removeIf(marker -> marker.compareTo(targetMarker) >= 0);
                    context.released = true;
                    marks.add(mark(entry, order));
                }
                else if (type.equals("C/SB")) {
                    long level = header.getControlInfo().nestedCommitLevel();
                    require(step == 0 && level > context.level && context.markers.putIfAbsent(entry.position().offset(), level) == null,
                            "Schema savepoint start has wrong scope");
                    context.level = level;
                }
                else if (type.equals("C/SU")) {
                    var targetMarker = control.savepointSequence();
                    long level = header.getControlInfo().nestedCommitLevel();
                    Long observedLevel = context.markers.get(targetMarker);
                    require(step == 0 && observedLevel != null && observedLevel == level && level <= context.level
                            && targetMarker.compareTo(context.schemaThrough) > 0,
                            "Schema savepoint rollback includes DDL or has an unobserved scope");
                    context.markers.keySet().removeIf(marker -> marker.compareTo(targetMarker) >= 0);
                    context.level = level - 1;
                    // Row compensation and SU still reach the transaction buffer.
                    // They do not install or undo a schema-history definition.
                }
                else if (type.equals("C/CM")) {
                    require(step == 0 && context.released && control.outcome() == TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT
                            && header.getControlInfo().nestedCommitLevel() == 0, "Schema change did not durably commit");
                    cycles.remove(header.getCommitCycle());
                    marks.add(mark(entry, order));
                }
                else if (type.equals("C/RB")) {
                    require(step == 0 && context.released && cycles.size() == 1
                            && marks.stream().noneMatch(mark -> mark.type().equals("C/CM"))
                            && control.outcome() == TransactionControlEntryDecoder.Outcome.FULL_ROLLBACK
                            && header.getControlInfo().nestedCommitLevel() == 0, "Schema rollback has ambiguous preceding outcomes");
                    marks.add(mark(entry, order));
                    rollback = context;
                }
                else {
                    throw new IllegalArgumentException("Unqualified schema-change outcome");
                }
                continue;
            }
            var nativeEntry = new As400JournalLifecycle.Entry(header, bytes, entry.position());
            if (step == 0 && clear.accept(entry)) {
                // Clear and restoration records still reach the transaction
                // lifecycle during replay; never consume them as schema marks.
                continue;
            }
            if (step == 0 && rollback == null && cycles.isEmpty() && type.equals("F/CH") && !metadataOpening(entry)
                    && marks.stream().anyMatch(mark -> mark.type().equals("C/CM"))) {
                int restored = restoredLaterAlter(identity, entries, index, order, verifiedBindings, excluded, creation);
                if (restored >= 0) {
                    // Its complete rollback restores the first committed layout.
                    // Keep this later transaction out of the first history event;
                    // ordinary scanning will qualify/discard its own rows again.
                    previous = entries.get(restored).position();
                    restoredThrough = previous;
                    index = restored;
                    continue;
                }
            }
            if (rollback != null) {
                require(type.equals(List.of("F/EJ", "D/EF", "D/JF", "F/JM").get(restoration))
                        && rollback.producer.equals(producer(entry)), "Incomplete schema rollback restoration");
                identity.auxiliary(nativeEntry, 0x10);
                marks.add(mark(entry, order));
                if (++restoration == 4) {
                    return new Evidence(marks, true, true);
                }
                continue;
            }
            if (step == 0 && miscellaneous.accept(entry)) {
                if (type.equals("D/CG") && new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(header, bytes, 0).changeType() == '1') {
                    require(excluded || !interveningRows || !definitionChange && !commandChange,
                            "Captured rows require an unavailable intermediate CL-command definition");
                    commandChange = true;
                }
                marks.add(mark(entry, order));
                lastMiscellaneous = true;
                continue;
            }
            if (creation ? As400JournalLifecycle.creationAdministrative(nativeEntry, identity, excluded || !interveningRows)
                    : As400JournalLifecycle.administrative(nativeEntry, verifiedBindings)) {
                if (creation && List.of("D/AC", "D/DC").contains(type)) {
                    marks.add(mark(entry, order));
                }
                continue;
            }
            if (header.getJournalCode() == 'R') {
                require(step == 0 && !marks.isEmpty(), "Row interrupts the schema-change lifecycle");
                if (creation) {
                    As400SourceNormalizer.validateUnobservedRow(header, bytes, 0, entry.position());
                    identity.identity(header);
                }
                else {
                    As400SourceNormalizer.validateSource(header, bytes, 0, entry.position(), verifiedBindings);
                }
                interveningRows = true;
                if (context != null) {
                    require(context.producer.equals(producer(entry)), "Uncommitted schema rows changed transaction producer");
                }
                continue;
            }
            if (step == 0) {
                require(isOpening(entry), "Another table transition precedes the metadata observation");
                require(excluded || !interveningRows, "Captured rows require an unavailable intermediate table definition");
                require(marks.isEmpty() || lastMiscellaneous || marks.get(marks.size() - 1).type().equals("C/SQ")
                        || marks.get(marks.size() - 1).type().equals("C/CM"), "Previous schema statement has not released its savepoint");
                definitionChange = true;
                lastMiscellaneous = false;
                producer = producer(entry);
                opening = entry.position();
                internalCycle = null;
                internalLevel = 0;
                minimumInternalLevel = 0;
            }
            require(internalMarker == null, "Schema lifecycle resumes before its internal savepoint is released");
            require(type.equals(TYPES.get(step)) && producer.equals(producer(entry)), "Unqualified schema-change lifecycle");
            if (step < 5) {
                As400SourceMetadata.from(header, entry.position());
                identity.auxiliary(nativeEntry, step == 0 ? 0 : 0x10);
            }
            else {
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position(), verifiedBindings);
                var change = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(header, bytes, 0);
                require(change.changeType() == '0' && change.file().equals(header.getFile()) && change.library().equals(header.getLibrary())
                        && (change.member().isEmpty() || change.member().equals(identity.member())), "Not a matching definition change");
                var cycle = header.getCommitCycle();
                long level = header.getControlInfo().nestedCommitLevel();
                require(cycle.signum() > 0 && level > 0 && (!cycles.containsKey(cycle) || cycles.get(cycle).producer.equals(producer)),
                        "Definition change lacks consistent transaction context");
                require(internalCycle == null || internalCycle.equals(cycle) && minimumInternalLevel > level,
                        "Definition change does not retain its outer savepoint scope");
                if (context == null) {
                    context = new SchemaCycle(producer, level, opening.offset());
                    cycles.put(cycle, context);
                }
                else {
                    require(context.level == level, "Definition change differs from its observed savepoint scope");
                }
                context.schemaThrough = entry.position().offset();
            }
            identity.identity(header);
            marks.add(mark(entry, order));
            step = (step + 1) % 6;
        }
        if (step != 0 || !cycles.isEmpty() || marks.isEmpty() || miscellaneous.pending() || clear.pending()) {
            throw new As400SchemaTimeline.PendingChangeException();
        }
        var completed = marks.get(marks.size() - 1).position();
        return new Evidence(marks, false, definitionChange,
                restoredThrough != null && order.compare(restoredThrough, completed) > 0 ? restoredThrough : completed);
    }

    private static int restoredLaterAlter(ObjectIdentity identity, List<RawJournalPage.Entry> entries, int opening,
                                          As400JournalOrder order, As400SourceNormalizer.TableResolver bindings, boolean excluded, boolean creation)
            throws Exception {
        BigInteger cycle = null;
        for (int at = opening; at < entries.size(); at++) {
            var h = entries.get(at).header();
            if (cycle == null && h.getJournalCode() == 'D' && h.getEntryType().equals("CG")
                    && h.getFile().equals(identity.file()) && h.getLibrary().equals(identity.library())) {
                var change = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(h, entries.get(at).data(), 0);
                if (change.changeType() != '0' || h.getCommitCycle().signum() <= 0) {
                    return -1;
                }
                cycle = h.getCommitCycle();
            }
            if (cycle != null && h.getJournalCode() == 'C' && h.getCommitCycle().equals(cycle)
                    && List.of("CM", "RB").contains(h.getEntryType())) {
                if (h.getEntryType().equals("CM")) {
                    return -1;
                }
                var proof = qualifyEvidence(identity, entries.subList(opening, entries.size()), order, bindings, excluded, creation);
                require(proof.rollback(), "Later schema rollback did not restore its exact definition");
                var last = proof.records().get(proof.records().size() - 1).position();
                for (int end = at + 1; end < entries.size(); end++) {
                    if (entries.get(end).position().equals(last)) {
                        return end;
                    }
                }
                throw new IllegalArgumentException("Schema restoration is outside the retained interval");
            }
        }
        return -1;
    }

    /** Only proves that a complete physical clear preserves the observed file format. */
    static final class LayoutPreservingClear {
        private final ObjectIdentity identity;
        private int step;
        private char subtype;
        private String producer;
        private BigInteger opening;
        private final Map<BigInteger, ClearCycle> cycles = new java.util.HashMap<>();
        private final Map<String, Integer> restorations = new java.util.HashMap<>();

        private static final class ClearCycle {
            final String producer;
            BigInteger lastOpening;
            int clears;

            ClearCycle(String producer) {
                this.producer = producer;
            }
        }

        LayoutPreservingClear(ObjectIdentity identity) {
            this.identity = identity;
        }

        boolean pending() {
            return step != 0 || !restorations.isEmpty();
        }

        void control(RawJournalPage.Entry entry) throws Exception {
            var h = entry.header();
            if (step == 1 && h.getJournalCode() == 'C' && h.getEntryType().equals("SU") && producer.equals(producer(entry))) {
                // A failed TRUNCATE or fast-DELETE fallback can abandon F/CH
                // before F/EJ changes journaling. This proves only that the
                // layout is unchanged; the ordinary lifecycle still requires
                // the exact observed cycle/savepoint before consuming C/SU.
                As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
                var control = new TransactionControlEntryDecoder().decode(h, entry.data(), 0);
                require(h.getCommitCycle().signum() > 0 && h.getControlInfo().nestedCommitLevel() > 0
                        && control.savepointSequence().compareTo(opening) < 0,
                        "Abandoned clear lacks its preceding savepoint scope");
                step = 0;
                return;
            }
            var cycle = cycles.get(h.getCommitCycle());
            if (h.getJournalCode() != 'C' || cycle == null) {
                return;
            }
            As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
            require(cycle.producer.equals(producer(entry)), "Clear outcome belongs to another producer");
            var control = new TransactionControlEntryDecoder().decode(h, entry.data(), 0);
            if (h.getEntryType().equals("CM") || h.getEntryType().equals("RB")) {
                boolean rollback = h.getEntryType().equals("RB");
                require(step == 0 && h.getControlInfo().nestedCommitLevel() == 0
                        && control.outcome() == (rollback ? TransactionControlEntryDecoder.Outcome.FULL_ROLLBACK
                                : TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT),
                        "Clear lacks its complete transaction outcome");
                if (rollback) {
                    require(restorations.putIfAbsent(cycle.producer, Math.multiplyExact(cycle.clears, 4)) == null,
                            "Overlapping clear rollback restoration");
                }
                cycles.remove(h.getCommitCycle());
            }
            else if (h.getEntryType().equals("SU")) {
                require(control.savepointSequence().compareTo(cycle.lastOpening) > 0,
                        "Clear savepoint rollback requires its qualified physical restoration");
            }
        }

        boolean accept(RawJournalPage.Entry entry) throws Exception {
            var h = entry.header();
            var nativeEntry = new As400JournalLifecycle.Entry(h, entry.data(), entry.position());
            String owner = producer(entry);
            Integer remaining = restorations.get(owner);
            if (remaining != null) {
                require(nativeEntry.type().equals(List.of("F/EJ", "D/EF", "D/JF", "F/JM").get((4 - remaining % 4) % 4)),
                        "Incomplete clear rollback restoration");
                As400SourceMetadata.from(h, entry.position());
                identity.auxiliary(nativeEntry, 0x10);
                if (remaining == 1) {
                    restorations.remove(owner);
                }
                else {
                    restorations.put(owner, remaining - 1);
                }
                return true;
            }
            if (step == 0) {
                if (!nativeEntry.type().equals("F/CH")) {
                    return false;
                }
                byte[] data = As400JournalLifecycle.payload(nativeEntry);
                if (data.length != 1 || data[0] != (byte) 0xf3 && data[0] != (byte) 0xf8) {
                    return false;
                }
                subtype = data[0] == (byte) 0xf3 ? '3' : '8';
                producer = owner;
                opening = entry.position().offset();
            }
            require(nativeEntry.type().equals(TYPES.get(step)) && producer.equals(producer(entry)),
                    "Incomplete clear interrupts schema observation");
            if (step < 5) {
                As400SourceMetadata.from(h, entry.position());
                identity.auxiliary(nativeEntry, step == 0 ? 0 : 0x10);
            }
            else {
                As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
                identity.identity(h);
                var change = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(h, entry.data(), 0);
                require(change.clearsRows() && change.changeType() == subtype && h.getCommitCycle().signum() > 0
                        && h.getControlInfo().nestedCommitLevel() > 0 && change.file().equals(h.getFile())
                        && change.library().equals(h.getLibrary()) && (change.member().isEmpty() || change.member().equals(identity.member())),
                        "Clear cannot establish an unchanged file definition");
                var cycle = cycles.computeIfAbsent(h.getCommitCycle(), ignored -> new ClearCycle(producer));
                require(cycle.producer.equals(producer), "Clear cycle changed producer");
                cycle.lastOpening = opening;
                cycle.clears++;
            }
            step = (step + 1) % 6;
            return true;
        }
    }

    static boolean clearOpening(RawJournalPage.Entry entry) {
        var h = entry.header();
        if (h.getJournalCode() != 'F' || !h.getEntryType().equals("CH")) {
            return false;
        }
        byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()));
        return payload.length == 1 && (payload[0] == (byte) 0xf3 || payload[0] == (byte) 0xf8);
    }

    /**
     * Keep a clear between two metadata boundaries under the name in force at
     * that clear. The retained interval must contain no row needing a possibly
     * changed layout, and a later boundary must reacquire or retire that layout
     * before any future row. Names alone never authorize decoding row values.
     */
    static As400SchemaChange metadataBeforeClear(As400BindingDefinition before, List<RawJournalPage.Entry> entries,
                                                 As400JournalOrder order, As400SourceNormalizer.TableResolver bindings,
                                                 As400HistoricalTableNames.Reader names)
            throws Exception {
        if (before == null || names == null || entries.isEmpty() || !metadataOpening(entries.get(0))) {
            return null;
        }
        var id = ObjectIdentity.of(before);
        var metadata = new Miscellaneous(id.file(), id.library(), id.fileJid(), id.members());
        var pairs = new ArrayList<RawJournalPage.Entry>();
        int clear = -1;
        for (int at = 0; at < entries.size(); at++) {
            var entry = entries.get(at);
            var h = entry.header();
            if (!h.getFile().equals(id.file()) || !h.getLibrary().equals(id.library())) {
                continue;
            }
            if (clear < 0 && metadata.accept(entry)) {
                if (h.getJournalCode() == 'D') {
                    if (new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(h, entry.data(), 0).changeType() != '2') {
                        return null; // A CL command can require a different physical layout.
                    }
                    pairs.add(entry);
                }
                continue;
            }
            if (clear < 0 && clearOpening(entry)) {
                clear = at;
                continue;
            }
            if (clear >= 0 && isOpening(entry)) {
                var interval = entries.subList(0, at);
                var proof = qualifyEvidence(id, interval, order, bindings, false, false);
                require(!pairs.isEmpty() && !proof.definitionChange() && metadataTypes(proof.records().stream().map(Mark::type).toList()),
                        "Clear name prefix must contain only complete SQL metadata changes");
                for (var pair : pairs) {
                    if (pair.header().getCommitCycle().signum() == 0) {
                        continue;
                    }
                    var terminal = interval.stream().filter(item -> item.header().getJournalCode() == 'C'
                            && item.header().getCommitCycle().equals(pair.header().getCommitCycle())
                            && List.of("CM", "RB").contains(item.header().getEntryType())).findFirst();
                    if (terminal.isEmpty()) {
                        throw new As400SchemaTimeline.PendingChangeException();
                    }
                    var outcome = terminal.orElseThrow();
                    As400SourceNormalizer.validateSource(outcome.header(), outcome.data(), 0, outcome.position());
                    require(producer(pair).equals(producer(outcome)) && order.compare(pair.position(), outcome.position()) < 0,
                            "Metadata name outcome changed producer or order");
                    for (var item : interval) {
                        var controlHeader = item.header();
                        if (controlHeader.getJournalCode() == 'C' && controlHeader.getEntryType().equals("SU")
                                && controlHeader.getCommitCycle().equals(pair.header().getCommitCycle())
                                && order.compare(item.position(), pair.position()) > 0 && order.compare(item.position(), outcome.position()) < 0) {
                            As400SourceNormalizer.validateSource(controlHeader, item.data(), 0, item.position());
                            var control = new TransactionControlEntryDecoder().decode(controlHeader, item.data(), 0);
                            require(producer(pair).equals(producer(item)) && controlHeader.getControlInfo().nestedCommitLevel() > 0
                                    && control.savepointSequence().compareTo(pair.position().offset()) > 0,
                                    "Metadata name may have been undone by savepoint rollback");
                        }
                    }
                    if (new TransactionControlEntryDecoder().decode(outcome.header(), outcome.data(), 0)
                            .outcome() != TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT || outcome.header().getControlInfo().nestedCommitLevel() != 0) {
                        return null; // A name-only prefix cannot stand in for rollback semantics.
                    }
                }
                return new As400SchemaChange(before, metadataName(before, entries.subList(0, clear), names), proof.records());
            }
            if (h.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType("R/" + h.getEntryType())) {
                return null;
            }
            if (clear < 0 && !As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()), bindings)) {
                return null;
            }
        }
        return null;
    }

    /**
     * Separate a metadata-only prefix from a later ALTER or physical rename
     * when no row uses an intermediate definition/name. Retain the old layout
     * until that transition acquires the new one; never query the vanished old name.
     * The complete verified interval proves there is no captured row between
     * these boundaries. All transaction controls still pass through replay.
     */
    static As400SchemaChange metadataBeforeTransition(As400BindingDefinition before, List<RawJournalPage.Entry> entries,
                                                      As400JournalOrder order, As400SourceNormalizer.TableResolver verifiedBindings)
            throws Exception {
        return metadataBeforeTransition(before, entries, order, verifiedBindings, null);
    }

    static As400SchemaChange metadataBeforeTransition(As400BindingDefinition before, List<RawJournalPage.Entry> entries,
                                                      As400JournalOrder order, As400SourceNormalizer.TableResolver verifiedBindings,
                                                      As400HistoricalTableNames.Reader names)
            throws Exception {
        if (before == null || entries.isEmpty() || !metadataOpening(entries.get(0))) {
            return null;
        }
        var clearPrefix = metadataBeforeClear(before, entries, order, verifiedBindings, names);
        if (clearPrefix != null) {
            return clearPrefix;
        }
        var identity = before.identity();
        var metadata = new Miscellaneous(identity.physicalFile(), identity.tableId().schema(), identity.fileJournalId(), before.members());
        var marks = new ArrayList<Mark>();
        JournalPosition previous = null;
        for (int index = 0; index < entries.size(); index++) {
            var entry = entries.get(index);
            require(previous == null || order.compare(previous, entry.position()) < 0, "Metadata prefix is not in journal order");
            previous = entry.position();
            As400JournalRecordContent.digest(entry.data(), entry.position());
            var h = entry.header();
            if (!h.getFile().equals(identity.physicalFile()) || !h.getLibrary().equals(identity.tableId().schema())) {
                continue;
            }
            if (metadata.accept(entry)) {
                marks.add(mark(entry, order));
            }
            else if (h.getJournalCode() == 'F' && isOpening(entry)) {
                As400SourceMetadata.from(h, entry.position());
                As400JournalLifecycle.auxiliary(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()), before.binding(65536), 0);
                return new As400SchemaChange(before, metadataName(before, entries.subList(0, index), names), marks);
            }
            else if (h.getJournalCode() == 'D' && h.getEntryType().equals("FN")) {
                // Distinct SQL/system names produce the metadata pair BEFORE
                // D/FN. Prove the rename association now, then acquire its new
                // catalog name when replay reaches the separate D/FN boundary.
                As400FileRename.opening(before, entries.subList(index, entries.size()), order,
                        verifiedBindings.resolve(identity.physicalFile(), identity.tableId().schema()));
                return new As400SchemaChange(before, metadataName(before, entries.subList(0, index), names), marks);
            }
            else if (!As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()), verifiedBindings)) {
                // Even an excluded row keeps this optimization conservative.
                // The ordinary acquisition path decides whether the observed
                // intermediate layout is required and available.
                return null;
            }
        }
        return null;
    }

    private static As400BindingDefinition metadataName(As400BindingDefinition before, List<RawJournalPage.Entry> prefix,
                                                       As400HistoricalTableNames.Reader names)
            throws Exception {
        if (names == null) {
            return before;
        }
        // Query names only after finding a prefix that can be separated. A
        // standalone LABEL/COMMENT still uses its ordinary catalog observation.
        var table = before.schema().id();
        for (var entry : prefix) {
            var h = entry.header();
            if (h.getJournalCode() == 'D' && h.getEntryType().equals("CG")
                    && h.getFile().equals(before.identity().physicalFile()) && h.getLibrary().equals(table.schema())) {
                table = names.read(entry).changed(entry, table);
            }
        }
        return As400FileRename.sqlName(before, table);
    }

    private static final class SchemaCycle {
        final String producer;
        long level;
        BigInteger beforeMarker;
        BigInteger schemaThrough;
        final Map<BigInteger, Long> markers = new TreeMap<>();
        boolean released;

        SchemaCycle(String producer, long level, BigInteger beforeMarker) {
            this.producer = producer;
            this.level = level;
            this.beforeMarker = beforeMarker;
        }
    }

    /**
     * Validate blank F/CH plus miscellaneous/CL-command D/CG. The caller must
     * still compare the acquired captured definition: CHGPF can change layout.
     */
    static final class Miscellaneous {
        private final String file;
        private final String library;
        private final String fileJid;
        private final Map<String, String> members;
        private RawJournalPage.Entry opening;

        Miscellaneous(String file, String library, String fileJid, Map<String, String> members) {
            this.file = file;
            this.library = library;
            this.fileJid = fileJid;
            this.members = Map.copyOf(members);
        }

        boolean pending() {
            return opening != null;
        }

        boolean accept(RawJournalPage.Entry entry) throws java.io.IOException {
            var h = entry.header();
            if (opening == null && !metadataOpening(entry)) {
                return false;
            }
            As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
            require(h.getFile().equals(file) && h.getLibrary().equals(library) && h.getControlInfo().flags() == 0
                    && h.getControlInfo().indicatorFlag() == '0' && h.getRecordInfo().objectNameIndicator() == '1',
                    "Miscellaneous change has unsupported source metadata");
            boolean physical = h.getRecordInfo().objectType().equals("*FILE");
            require(physical ? h.getMember().isEmpty() && h.getRecordInfo().objectJournalId().equals(fileJid)
                    : h.getRecordInfo().objectType().equals("*QDDS") && h.getRecordInfo().objectJournalId().equals(members.get(h.getMember())),
                    "Miscellaneous change file or member identity mismatch");
            if (opening == null) {
                require(h.getCommitCycle().signum() == 0 && h.getControlInfo().nestedCommitLevel() == 0,
                        "Unsupported miscellaneous change opening");
                opening = entry;
            }
            else {
                require(h.getJournalCode() == 'D' && h.getEntryType().equals("CG") && physical
                        && (h.getCommitCycle().signum() == 0 ? h.getControlInfo().nestedCommitLevel() == 0 : h.getControlInfo().nestedCommitLevel() > 0)
                        && producer(opening).equals(producer(entry)), "Miscellaneous metadata change lacks its matching D/CG");
                var change = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(h, entry.data(), 0);
                require((change.changeType() == '1' || change.changeType() == '2') && change.file().equals(file) && change.library().equals(library)
                        && (change.member().isEmpty() || members.containsKey(change.member())), "Metadata change identity or subtype differs");
                opening = null;
            }
            return true;
        }
    }

    static boolean metadataOpening(RawJournalPage.Entry entry) {
        var h = entry.header();
        return h.getJournalCode() == 'F' && h.getEntryType().equals("CH")
                && java.util.Arrays.equals(As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position())), new byte[]{ 0x40 });
    }

    static boolean isOpening(RawJournalPage.Entry entry) {
        var h = entry.header();
        if (h.getControlInfo().logicalFile()) {
            return false; // The logical-file classifier validates these separately.
        }
        if (h.getJournalCode() == 'D' && (h.getEntryType().equals("DT") || h.getEntryType().equals("FN") || h.getEntryType().equals("FM"))) {
            return true;
        }
        if (h.getJournalCode() == 'D' && h.getEntryType().equals("CT")) {
            io.debezium.ibmi.db2.journal.retrieve.FileCreationEntryDecoder.metadata(h, entry.data(), 0);
            return true;
        }
        if (h.getJournalCode() != 'F' || !h.getEntryType().equals("CH")) {
            return false;
        }
        if (metadataOpening(entry)) {
            return true;
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
        if (deferred) {
            fields.put("visibility", "outcome");
        }
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
        boolean deferred = "outcome".equals(fields.get("visibility"));
        int fixed = deferred ? 3 : 2;
        require((fields.size() - fixed) % 6 == 0, "Incomplete schema transition fields");
        int count = (fields.size() - fixed) / 6;
        for (int i = 0; i < count; i++) {
            String key = "record." + i + ".";
            marks.add(new Mark(fields.get(key + "type"),
                    new JournalPosition(new BigInteger(fields.get(key + "sequence")), new JournalReceiver(fields.get(key + "receiver"), fields.get(key + "library"))),
                    fields.get(key + "receiver.identity"), fields.get(key + "hash")));
        }
        var restored = new As400SchemaChange(before.isEmpty() ? null : before.get(0), after.isEmpty() ? null : after.get(0), marks, deferred);
        require(restored.fields().equals(fields), "Noncanonical or incomplete schema transition");
        return restored;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
