/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/** Native file name/library changes preserve JIDs; names alone never identify a replacement object. */
final class As400FileRename {
    record Observed(String file, List<As400SchemaChange.Mark> records) {
    }

    record Following(String file, String library, boolean preservesSqlName, List<RawJournalPage.Entry> prefix,
            io.debezium.ibmi.db2.journal.retrieve.JournalPosition observedThrough) {
    }

    record Move(String library, List<As400SchemaChange.Mark> records) {
    }

    private As400FileRename() {
    }

    static boolean moveOpening(RawJournalPage.Entry entry) {
        return entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("FM");
    }

    /** MOVOBJ changes the library, not the file's SQL name, format, members or identities. */
    static As400SchemaChange move(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        require(before != null && !entries.isEmpty() && moveOpening(entries.get(0)), "File move requires its retained identity and exact opening");
        var id = before.identity();
        var observed = observeMove(As400SchemaChange.ObjectIdentity.of(before), entries, order);
        var table = new io.debezium.relational.TableId(id.tableId().catalog(), observed.library(), id.tableId().table());
        var after = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(table, id.physicalFile(), id.member(), id.fileJournalId(),
                id.memberJournalId(), id.recordFormat()), before.schema().edit().tableId(table).create(), before.codecs(), before.members());
        return new As400SchemaChange(before, after, observed.records());
    }

    /** A complete native association is sufficient to locate a newly created file before its schema is acquired. */
    static Move observeMove(As400SchemaChange.ObjectIdentity id, List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        require(id != null && !entries.isEmpty() && moveOpening(entries.get(0)), "File move requires its retained identity and exact opening");
        String source = id.library();
        String target = null;
        String producer = As400SchemaChange.producer(entries.get(0));
        var members = new HashSet<>(id.members().keySet());
        var marks = new ArrayList<As400SchemaChange.Mark>();
        io.debezium.ibmi.db2.journal.retrieve.JournalPosition previous = null;
        var text = new AS400Text(10, RawJournalPage.METADATA_CCSID);
        for (var entry : entries) {
            require(previous == null || order.compare(previous, entry.position()) < 0, "File move evidence is not ordered");
            previous = entry.position();
            var h = entry.header();
            if (!h.getFile().equals(id.file()) || !(h.getLibrary().equals(source) || h.getLibrary().equals(target))) {
                require(!marks.isEmpty(), "File move opening changed native identity");
                continue;
            }
            As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
            byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()));
            require(payload.length >= 60 && h.getCommitCycle().signum() == 0 && h.getControlInfo().nestedCommitLevel() == 0
                    && h.getControlInfo().flags() == 0 && (h.getControlInfo().indicatorFlag() == '0' || h.getControlInfo().indicatorFlag() == '1')
                    && h.getRecordInfo().objectNameIndicator() == '1' && h.getRecordInfo().recordNumber().signum() == 0
                    && producer.equals(As400SchemaChange.producer(entry)), "File move has unsupported source metadata");
            String destination = text.toObject(payload, 40).toString().trim();
            String member = text.toObject(payload, 20).toString().trim();
            require(h.getLibrary().equals(source) && text.toObject(payload, 0).toString().trim().equals(id.file())
                    && text.toObject(payload, 30).toString().trim().equals(id.file())
                    && text.toObject(payload, 10).toString().trim().equals(source)
                    && member.equals(h.getMember()) && text.toObject(payload, 50).toString().trim().equals(member)
                    && destination.matches("[A-Z#$@][A-Z0-9_$#@]{0,9}") && !destination.equals(source)
                    && (target == null || target.equals(destination)), "File move changed file/member names or has inconsistent libraries");
            if (marks.isEmpty()) {
                require(moveOpening(entry) && h.getRecordInfo().objectType().equals("*FILE") && member.isEmpty()
                        && h.getRecordInfo().objectJournalId().equals(id.fileJid()), "File move opening changed native identity");
                target = destination;
            }
            else {
                require(h.getJournalCode() == 'F' && h.getEntryType().equals("MM") && h.getRecordInfo().objectType().equals("*QDDS")
                        && h.getRecordInfo().objectJournalId().equals(id.members().get(member)) && members.remove(member),
                        "File move lacks an exact file/member association");
            }
            marks.add(As400SchemaChange.mark(entry, order));
            if (members.isEmpty()) {
                return new Move(target, List.copyOf(marks));
            }
        }
        throw new As400SchemaTimeline.PendingChangeException();
    }

    static boolean sameMovedDefinition(As400BindingDefinition before, As400BindingDefinition after) {
        if (before == null || after == null) {
            return false;
        }
        var a = before.identity();
        var b = after.identity();
        return java.util.Objects.equals(a.tableId().catalog(), b.tableId().catalog()) && a.tableId().table().equals(b.tableId().table())
                && !a.tableId().schema().equals(b.tableId().schema()) && a.physicalFile().equals(b.physicalFile())
                && a.fileJournalId().equals(b.fileJournalId()) && a.member().equals(b.member()) && a.memberJournalId().equals(b.memberJournalId())
                && a.recordFormat().equals(b.recordFormat()) && before.members().equals(after.members()) && before.codecs().equals(after.codecs())
                && before.schema().edit().tableId(after.schema().id()).create().equals(after.schema());
    }

    /** Keep rows before a later SQL-name change under the physical rename's own identity. */
    static As400SchemaChange beforeMetadata(As400BindingDefinition before, List<RawJournalPage.Entry> entries,
                                            As400JournalOrder order, As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        if (before == null || entries.isEmpty() || entries.get(0).header().getJournalCode() != 'D'
                || !entries.get(0).header().getEntryType().equals("FN")) {
            return null;
        }
        var id = before.identity();
        var selected = bindings.resolve(id.physicalFile(), id.tableId().schema());
        var association = observe(As400SchemaChange.ObjectIdentity.of(before), before, entries, order, selected, true);
        var through = association.records().get(association.records().size() - 1).position();
        boolean rows = false;
        for (int at = 0; at < entries.size(); at++) {
            var entry = entries.get(at);
            var h = entry.header();
            if (order.compare(entry.position(), through) <= 0 || !h.getLibrary().equals(id.tableId().schema())
                    || !h.getFile().equals(association.file())) {
                continue;
            }
            if (As400SchemaChange.metadataOpening(entry)) {
                if (!rows) {
                    return null;
                }
                // Validate every preceding row/member and administrative entry;
                // never install the later metadata pair at the earlier D/FN.
                var prefix = observe(before, entries.subList(0, at), order, selected);
                var table = id.tableId();
                if (table.table().equals(id.physicalFile())) {
                    table = new io.debezium.relational.TableId(table.catalog(), table.schema(), prefix.file());
                }
                return new As400SchemaChange(before, sqlName(alias(before, prefix.file()), table), prefix.records());
            }
            if (As400SchemaChange.isOpening(entry)) {
                return null;
            }
            rows |= h.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType("R/" + h.getEntryType());
        }
        return null;
    }

    /** A complete row-free rename/drop needs no invented SQL name for the vanished alias. */
    static As400SchemaChange deleted(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order,
                                     As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        return deleted(before, entries, order, bindings, null);
    }

    static As400SchemaChange deleted(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order,
                                     As400SourceNormalizer.TableResolver bindings, As400HistoricalTableNames.Reader names)
            throws Exception {
        if (before == null || entries.isEmpty() || entries.get(0).header().getJournalCode() != 'D'
                || !entries.get(0).header().getEntryType().equals("FN")) {
            return null;
        }
        var id = before.identity();
        for (int at = 1; at < entries.size(); at++) {
            var h = entries.get(at).header();
            if (h.getJournalCode() != 'D' || !h.getEntryType().equals("DT") || !h.getLibrary().equals(id.tableId().schema())
                    || !h.getRecordInfo().objectJournalId().equals(id.fileJournalId())) {
                continue;
            }
            var prefix = entries.subList(0, at);
            var renamed = observe(before, prefix, order, bindings.resolve(id.physicalFile(), id.tableId().schema()));
            require(renamed.file().equals(h.getFile()), "Deletion does not follow the observed rename");
            boolean rows = prefix.stream().anyMatch(entry -> entry.header().getJournalCode() == 'R'
                    && !As400JournalLifecycle.administrativeType("R/" + entry.header().getEntryType())
                    && entry.header().getLibrary().equals(id.tableId().schema())
                    && before.members().containsValue(entry.header().getRecordInfo().objectJournalId()));
            if (rows && names != null) {
                var table = excludedNames(before, prefix, names, bindings);
                var identity = new As400TransactionBuffer.TableIdentity(table, renamed.file(), id.member(), id.fileJournalId(),
                        id.memberJournalId(), id.recordFormat());
                var alias = new As400BindingDefinition(identity, before.schema().edit().tableId(table).create(), before.codecs(), before.members());
                // Keep the rename and DROP as ordinary separate history events.
                // No current catalog or invented vanished row layout is needed.
                return new As400SchemaChange(before, alias, renamed.records());
            }
            require(!rows, "Renamed rows require the vanished SQL name and definition");
            var deletion = As400FileDeletion.qualifyEvidence(new As400SchemaChange.ObjectIdentity(renamed.file(), id.tableId().schema(),
                    id.fileJournalId(), id.member(), before.members()), entries.subList(at, entries.size()), order, false);
            require(!deletion.rollback(), "Rolled-back deletion requires the renamed catalog definition");
            var marks = new ArrayList<>(renamed.records());
            marks.addAll(deletion.records());
            return new As400SchemaChange(before, null, marks);
        }
        return null;
    }

    private static io.debezium.relational.TableId excludedNames(As400BindingDefinition before, List<RawJournalPage.Entry> entries,
                                                                As400HistoricalTableNames.Reader names, As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        var table = before.schema().id();
        String file = before.identity().physicalFile();
        require(!bindings.needsValues(table), "Selected renamed rows require their unavailable historical SQL name");
        for (var entry : entries) {
            var h = entry.header();
            if (!h.getLibrary().equals(table.schema()) || !h.getRecordInfo().objectJournalId().equals(before.identity().fileJournalId())) {
                continue;
            }
            if (h.getJournalCode() == 'D' && h.getEntryType().equals("FN")) {
                // With no alternative SQL name, IBM's object rename changes
                // both names. An alternative name survives a system-only rename.
                // observe() has already verified each exact file/member pair.
                String target = new AS400Text(10, RawJournalPage.METADATA_CCSID)
                        .toObject(As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position())), 30).toString().trim();
                require(h.getFile().equals(file), "Historical rename names are not contiguous");
                if (table.table().equals(file)) {
                    table = new io.debezium.relational.TableId(table.catalog(), table.schema(), target);
                }
                file = target;
            }
            else if (h.getJournalCode() == 'D' && h.getEntryType().equals("CG")) {
                table = names.read(entry).changed(entry, table);
            }
            require(!bindings.needsValues(table), "Selected renamed rows require their unavailable historical SQL name");
        }
        return table;
    }

    /** Locate today's catalog name without moving a preceding ALTER's publication boundary. */
    static Following following(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order,
                               As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        if (before == null || entries.isEmpty() || entries.get(0).header().getJournalCode() != 'F') {
            return null;
        }
        var id = before.identity();
        for (int at = 1; at < entries.size(); at++) {
            var h = entries.get(at).header();
            if (moveOpening(entries.get(at)) && h.getFile().equals(id.physicalFile()) && h.getLibrary().equals(id.tableId().schema())) {
                return followingMove(before, entries, at, order, bindings);
            }
            if (h.getJournalCode() == 'D' && h.getEntryType().equals("FN")
                    && h.getFile().equals(id.physicalFile()) && h.getLibrary().equals(id.tableId().schema())) {
                var observed = observe(before, entries.subList(at, entries.size()), order, bindings.resolve(id.physicalFile(), id.tableId().schema()));
                return new Following(observed.file(), id.tableId().schema(), false, entries.subList(0, at),
                        observed.records().get(observed.records().size() - 1).position());
            }
        }
        return null;
    }

    private static Following followingMove(As400BindingDefinition before, List<RawJournalPage.Entry> entries, int opening,
                                           As400JournalOrder order, As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        var moved = move(before, entries.subList(opening, entries.size()), order);
        var current = moved.after();
        var through = moved.commit();
        var selected = bindings.resolve(before.identity().physicalFile(), before.identity().tableId().schema());
        for (int at = opening + 1; at < entries.size(); at++) {
            var entry = entries.get(at);
            var h = entry.header();
            if (order.compare(entry.position(), through) <= 0 || !h.getFile().equals(current.identity().physicalFile())
                    || !h.getLibrary().equals(current.schema().id().schema())) {
                continue;
            }
            if (moveOpening(entry)) {
                moved = move(current, entries.subList(at, entries.size()), order);
                current = moved.after();
                through = moved.commit();
                continue;
            }
            var binding = !bindings.needsValues(current.schema().id())
                    ? new As400SourceNormalizer.TableBinding(current.identity(), null, false, true, current.members(), selected.excludedLob())
                    : current.binding(65536);
            As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position(), (file, library) -> binding);
            String type = h.getJournalCode() + "/" + h.getEntryType();
            if (h.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType(type)) {
                require(h.getRecordInfo().objectType().equals("*QDDS")
                        && h.getRecordInfo().objectJournalId().equals(current.members().get(h.getMember())), "Moved row has a different member identity");
            }
            else {
                require(As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()), (file, library) -> binding),
                        "Later operation changes a moved file before metadata acquisition");
            }
        }
        return new Following(current.identity().physicalFile(), current.schema().id().schema(), true, entries.subList(0, opening), through);
    }

    static boolean sameDefinition(As400BindingDefinition before, As400BindingDefinition after) {
        return before != null && after != null && java.util.Objects.equals(before.schema().id().catalog(), after.schema().id().catalog())
                && before.schema().id().schema().equals(after.schema().id().schema())
                && before.identity().fileJournalId().equals(after.identity().fileJournalId())
                && before.identity().member().equals(after.identity().member())
                && before.identity().memberJournalId().equals(after.identity().memberJournalId())
                && before.members().equals(after.members())
                && (before.unobservedLayout() || before.identity().recordFormat().equals(after.identity().recordFormat())
                        && before.codecs().equals(after.codecs())
                        && before.schema().edit().tableId(after.schema().id()).create().equals(after.schema())
                        && before.excluded() == after.excluded() && before.usesRrn() == after.usesRrn());
    }

    static Observed observe(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order,
                            As400SourceNormalizer.TableBinding selected)
            throws Exception {
        require(before != null, "Rename requires a retained definition");
        return observe(As400SchemaChange.ObjectIdentity.of(before), before, entries, order, selected, false);
    }

    static void opening(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order,
                        As400SourceNormalizer.TableBinding selected)
            throws Exception {
        observe(As400SchemaChange.ObjectIdentity.of(before), before, entries, order, selected, true);
    }

    static Observed observeCreated(As400SchemaChange.ObjectIdentity identity, List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        return observe(identity, null, entries, order, null, false);
    }

    private static Observed observe(As400SchemaChange.ObjectIdentity identity, As400BindingDefinition before,
                                    List<RawJournalPage.Entry> entries, As400JournalOrder order, As400SourceNormalizer.TableBinding selected, boolean openingOnly)
            throws Exception {
        require(!entries.isEmpty(), "Rename requires complete native history");
        String library = identity.library();
        String file = identity.file();
        String oldFile = null;
        String producer = null;
        var members = new HashSet<String>();
        var cycles = new HashMap<java.math.BigInteger, String>();
        var marks = new ArrayList<As400SchemaChange.Mark>();
        As400SchemaChange.Miscellaneous miscellaneous = null;
        io.debezium.ibmi.db2.journal.retrieve.JournalPosition previous = null;
        boolean interveningRows = false;
        for (var entry : entries) {
            require(previous == null || order.compare(previous, entry.position()) < 0, "Rename evidence is not ordered");
            previous = entry.position();
            var h = entry.header();
            As400JournalRecordContent.digest(entry.data(), entry.position());
            String type = h.getJournalCode() + "/" + h.getEntryType();
            if (h.getJournalCode() == 'C' && cycles.containsKey(h.getCommitCycle())) {
                As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
                require(cycles.get(h.getCommitCycle()).equals(As400SchemaChange.producer(entry)), "Rename metadata outcome changed producer");
                if (type.equals("C/CM") || type.equals("C/RB")) {
                    require(h.getControlInfo().nestedCommitLevel() == 0, "Rename metadata has a nested terminal");
                    cycles.remove(h.getCommitCycle());
                }
                continue; // The ordinary lifecycle still validates every savepoint and terminal.
            }
            if (!h.getLibrary().equals(library) || !(h.getFile().equals(file) || !members.isEmpty() && h.getFile().equals(oldFile))) {
                continue;
            }
            if (members.isEmpty() && miscellaneous != null && miscellaneous.accept(entry)) {
                if (h.getCommitCycle().signum() > 0) {
                    cycles.put(h.getCommitCycle(), As400SchemaChange.producer(entry));
                }
                marks.add(As400SchemaChange.mark(entry, order));
                continue;
            }
            if (type.equals("D/FN") || !members.isEmpty()) {
                As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
                byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()));
                require(payload.length >= 60 && h.getCommitCycle().signum() == 0 && h.getControlInfo().nestedCommitLevel() == 0
                        && h.getControlInfo().flags() == 0 && (h.getControlInfo().indicatorFlag() == '0' || h.getControlInfo().indicatorFlag() == '1')
                        && h.getRecordInfo().objectNameIndicator() == '1' && h.getRecordInfo().recordNumber().signum() == 0,
                        "Rename has unsupported source metadata");
                var text = new AS400Text(10, RawJournalPage.METADATA_CCSID);
                String from = text.toObject(payload, 0).toString().trim();
                String to = text.toObject(payload, 30).toString().trim();
                String member = text.toObject(payload, 20).toString().trim();
                require(from.equals(h.getFile()) && text.toObject(payload, 10).toString().trim().equals(library)
                        && text.toObject(payload, 40).toString().trim().equals(library)
                        && member.equals(h.getMember()) && text.toObject(payload, 50).toString().trim().equals(member)
                        && !to.isBlank() && !from.equals(to), "Rename changes library/member or has inconsistent names");
                if (type.equals("D/FN")) {
                    require(members.isEmpty() && !interveningRows && (miscellaneous == null || !miscellaneous.pending())
                            && h.getRecordInfo().objectType().equals("*FILE") && member.isEmpty()
                            && h.getRecordInfo().objectJournalId().equals(identity.fileJid()),
                            "Rename overlaps another object, unresolved member, or unavailable intermediate row name");
                    oldFile = file;
                    file = to;
                    producer = As400SchemaChange.producer(entry);
                    members.addAll(identity.members().keySet());
                }
                else {
                    require(type.equals("F/MN") && from.equals(oldFile) && to.equals(file)
                            && producer.equals(As400SchemaChange.producer(entry)) && h.getRecordInfo().objectType().equals("*QDDS")
                            && h.getRecordInfo().objectJournalId().equals(identity.members().get(member)) && members.remove(member),
                            "Rename lacks an exact file/member association");
                    if (members.isEmpty()) {
                        miscellaneous = new As400SchemaChange.Miscellaneous(file, library, identity.fileJid(), identity.members());
                    }
                }
                marks.add(As400SchemaChange.mark(entry, order));
                if (openingOnly && type.equals("F/MN") && members.isEmpty()) {
                    return new Observed(file, List.copyOf(marks));
                }
                continue;
            }
            require(!marks.isEmpty(), "Physical rename must start with D/FN");
            if (h.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType(type)) {
                if (selected != null && selected.excluded()) {
                    var alias = alias(before, file);
                    As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position(),
                            (name, schema) -> new As400SourceNormalizer.TableBinding(alias.identity(), null, false, true, alias.members(), selected.excludedLob()));
                }
                else {
                    As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
                }
                require(h.getRecordInfo().objectType().equals("*QDDS")
                        && h.getRecordInfo().objectJournalId().equals(identity.members().get(h.getMember())), "Rename row has a different member identity");
                interveningRows = true;
                continue;
            }
            As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
            // These marks prevent ordinary open/close/authority records for an
            // intermediate name from being resolved against a later catalog alias.
            var nativeEntry = new As400JournalLifecycle.Entry(h, entry.data(), entry.position());
            boolean administrative;
            if (before == null) {
                administrative = As400JournalLifecycle.creationAdministrative(nativeEntry,
                        new As400SchemaChange.ObjectIdentity(file, library, identity.fileJid(), identity.member(), identity.members()), !interveningRows);
            }
            else {
                var alias = alias(before, file);
                var binding = selected.excluded()
                        ? new As400SourceNormalizer.TableBinding(alias.identity(), null, false, true, alias.members(), selected.excludedLob())
                        : alias.binding(65536);
                administrative = As400JournalLifecycle.administrative(nativeEntry, (name, schema) -> binding);
            }
            require(administrative, "Unqualified operation changes a renamed file before metadata acquisition");
            marks.add(As400SchemaChange.mark(entry, order));
        }
        if (marks.isEmpty() || !members.isEmpty() || !cycles.isEmpty() || miscellaneous != null && miscellaneous.pending()) {
            throw new As400SchemaTimeline.PendingChangeException();
        }
        return new Observed(file, List.copyOf(marks));
    }

    static As400BindingDefinition alias(As400BindingDefinition before, String file) {
        var id = before.identity();
        // Used only for identity checking, never to infer an SQL output name.
        return new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(id.tableId(), file, id.member(), id.fileJournalId(),
                id.memberJournalId(), id.recordFormat()), before.schema(), before.codecs(), before.members());
    }

    static As400BindingDefinition sqlName(As400BindingDefinition before, io.debezium.relational.TableId table) {
        var id = before.identity();
        return new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(table, id.physicalFile(), id.member(), id.fileJournalId(),
                id.memberJournalId(), id.recordFormat()), before.schema().edit().tableId(table).create(), before.codecs(), before.members());
    }

    static As400SchemaChange qualify(As400BindingDefinition before, As400BindingDefinition after, List<RawJournalPage.Entry> entries,
                                     As400JournalOrder order, As400SourceNormalizer.TableResolver verifiedBindings)
            throws Exception {
        var prefix = beforeMetadata(before, entries, order, verifiedBindings);
        if (prefix != null) {
            return prefix;
        }
        var deletion = deleted(before, entries, order, verifiedBindings);
        if (deletion != null) {
            return deletion;
        }
        var observed = observe(before, entries, order, verifiedBindings.resolve(before.identity().physicalFile(), before.identity().tableId().schema()));
        after = As400SchemaChange.retainEventKey(before, after);
        require(sameDefinition(before, after) && after.identity().physicalFile().equals(observed.file()),
                "Current catalog does not preserve the observed renamed file definition and identities");
        return new As400SchemaChange(before, after, observed.records());
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
