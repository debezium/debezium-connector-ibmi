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
    record CatalogLocation(String file, String library) {
    }

    private record LaterMove(As400SchemaChange.ObjectIdentity identity, List<RawJournalPage.Entry> prefix, JournalPosition through) {
    }

    private As400FileCreation() {
    }

    /** Checks owned object records before a transaction exposes a queryable table schema. */
    static final class Progress {
        private final io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader first;
        private final String fileJid;
        private final String producer;
        private final java.math.BigInteger cycle;
        private String member;
        private String memberJid;
        private int step;
        private int alterStep;
        private As400SchemaChange.Miscellaneous miscellaneous;
        private As400SchemaChange.LayoutPreservingClear clear;

        Progress(RawJournalPage.Entry opening) {
            first = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(opening.data(), 0);
            require(first.getJournalCode() == 'D' && first.getEntryType().equals("CT") && !first.getControlInfo().logicalFile(),
                    "Physical-file creation requires a physical D/CT");
            FileCreationEntryDecoder.metadata(first, opening.data(), 0);
            fileJid = first.getRecordInfo().objectJournalId();
            require(As400TransactionBuffer.TableIdentity.validJournalId(fileJid), "Creation lacks its physical-file identity");
            producer = As400SchemaChange.producer(opening);
            cycle = first.getCommitCycle();
        }

        boolean complete() {
            return step == As400SchemaChange.CREATION_TYPES.size();
        }

        As400SchemaChange.ObjectIdentity identity() {
            require(complete(), "Creation has no complete file/member association");
            return new As400SchemaChange.ObjectIdentity(first.getFile(), first.getLibrary(), fileJid, member, java.util.Map.of(member, memberJid));
        }

        void physical(RawJournalPage.Entry entry) {
            byte[] bytes = entry.data();
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(bytes, 0);
            var nativeEntry = new As400JournalLifecycle.Entry(header, bytes, entry.position());
            require(header.getFile().equals(first.getFile()) && header.getLibrary().equals(first.getLibrary()), "Creation object name changed");
            String type = nativeEntry.type();
            require(type.equals(As400SchemaChange.CREATION_TYPES.get(step)) && producer.equals(As400SchemaChange.producer(entry)),
                    "Incomplete or ambiguous native creation lifecycle");
            As400SourceMetadata.from(header, entry.position());
            if (type.equals("D/CT") || type.equals("F/MC")) {
                FileCreationEntryDecoder.metadata(header, bytes, 0);
            }
            else {
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position());
            }
            require((header.getCommitCycle().signum() == 0 || header.getCommitCycle().equals(cycle)) && header.getControlInfo().nestedCommitLevel() == 0
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
                        && new AS400Text(3, RawJournalPage.METADATA_CCSID).toObject(payload).toString().matches("[01]0[01]"),
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
            step++;
            if (complete()) {
                miscellaneous = new As400SchemaChange.Miscellaneous(first.getFile(), first.getLibrary(), fileJid, java.util.Map.of(member, memberJid));
                clear = new As400SchemaChange.LayoutPreservingClear(identity());
            }
        }

        void control(RawJournalPage.Entry entry) throws Exception {
            if (clear != null) {
                clear.control(entry);
            }
        }

        /** True routes an exactly validated clear through the ordinary physical lifecycle. */
        boolean deferred(RawJournalPage.Entry entry) throws Exception {
            var h = entry.header();
            require((h.getCommitCycle().signum() == 0 || h.getCommitCycle().equals(cycle))
                    && producer.equals(As400SchemaChange.producer(entry)), "Unfinished creation changed transaction producer or cycle");
            if (!complete()) {
                physical(entry);
                return false;
            }
            if (miscellaneous.accept(entry)) {
                return false;
            }
            if (clear.accept(entry)) {
                return true;
            }
            if (h.getJournalCode() == 'D' && h.getEntryType().equals("DT")) {
                require(alterStep == 0, "Deletion interrupts an unfinished created definition");
                As400FileDeletion.opening(identity(), entry);
                return false;
            }
            if (alterStep > 0 || As400SchemaChange.isOpening(entry)) {
                var nativeEntry = new As400JournalLifecycle.Entry(h, entry.data(), entry.position());
                require(nativeEntry.type().equals(List.of("F/CH", "F/EJ", "D/EF", "D/JF", "F/JM", "D/CG").get(alterStep)),
                        "Incomplete definition change of an uncommitted creation");
                if (alterStep < 5) {
                    identity().auxiliary(nativeEntry, alterStep == 0 ? 0 : 0x10);
                }
                else {
                    As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
                    identity().identity(h);
                    var change = new io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(h, entry.data(), 0);
                    require(change.changeType() == '0' && h.getCommitCycle().equals(cycle) && h.getControlInfo().nestedCommitLevel() > 0
                            && change.file().equals(first.getFile()) && change.library().equals(first.getLibrary())
                            && (change.member().isEmpty() || change.member().equals(member)), "Unqualified new-file definition change");
                }
                alterStep = (alterStep + 1) % 6;
                return false;
            }
            String type = h.getJournalCode() + "/" + h.getEntryType();
            if (type.equals("F/JC")) {
                As400SourceNormalizer.validateMemberJournalAttribute(h, entry.data(), entry.position());
            }
            else if (h.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType(type)) {
                As400SourceNormalizer.validateUnobservedRow(h, entry.data(), 0, entry.position());
            }
            else {
                As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
            }
            boolean file = h.getRecordInfo().objectType().equals("*FILE");
            require(h.getFile().equals(first.getFile()) && h.getLibrary().equals(first.getLibrary())
                    && h.getRecordInfo().objectNameIndicator() == '1'
                    && (file ? h.getMember().isEmpty() && h.getRecordInfo().objectJournalId().equals(fileJid)
                            : h.getRecordInfo().objectType().equals("*QDDS") && h.getMember().equals(member)
                                    && h.getRecordInfo().objectJournalId().equals(memberJid)),
                    "Unfinished creation changed file/member identity");
            require(As400JournalLifecycle.administrativeType(type)
                    || h.getJournalCode() == 'R' && h.getCommitCycle().equals(cycle)
                            && java.util.Set.of("PT", "PX", "UB", "UP", "DL", "BR", "DR", "UR", "R1").contains(h.getEntryType()),
                    "Another table transition precedes creation metadata acquisition");
            return false;
        }
    }

    /** Called only after the complete pre-catalog lifecycle has been qualified. */
    static CatalogLocation acquiredLocation(List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        var moved = laterMove(entries, order);
        if (moved != null) {
            return new CatalogLocation(moved.identity().file(), moved.identity().library());
        }
        var progress = new Progress(entries.get(0));
        for (int index = 0; index < entries.size(); index++) {
            var entry = entries.get(index);
            var h = entry.header();
            if (!h.getFile().equals(progress.first.getFile()) || !h.getLibrary().equals(progress.first.getLibrary())) {
                continue;
            }
            if (!progress.complete()) {
                progress.physical(entry);
            }
            else if (h.getJournalCode() == 'D' && h.getEntryType().equals("FN")) {
                return new CatalogLocation(As400FileRename.observeCreated(progress.identity(), entries.subList(index, entries.size()), order).file(),
                        progress.first.getLibrary());
            }
        }
        return new CatalogLocation(progress.first.getFile(), progress.first.getLibrary());
    }

    /** Follow only complete moves; no SQL identity or row layout is invented before acquisition. */
    private static LaterMove laterMove(List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        var progress = new Progress(entries.get(0));
        for (int index = 0; index < entries.size(); index++) {
            var entry = entries.get(index);
            var header = entry.header();
            if (!header.getFile().equals(progress.first.getFile()) || !header.getLibrary().equals(progress.first.getLibrary())) {
                continue;
            }
            if (!progress.complete()) {
                progress.physical(entry);
                continue;
            }
            if (header.getJournalCode() == 'D' && header.getEntryType().equals("FN")) {
                return null; // The physical-rename path owns this different name transition.
            }
            if (!As400FileRename.moveOpening(entry)) {
                continue;
            }
            var identity = progress.identity();
            var observed = As400FileRename.observeMove(identity, entries.subList(index, entries.size()), order);
            identity = new As400SchemaChange.ObjectIdentity(identity.file(), observed.library(), identity.fileJid(), identity.member(), identity.members());
            var through = observed.records().get(observed.records().size() - 1).position();
            for (int later = index + 1; later < entries.size(); later++) {
                var item = entries.get(later);
                var h = item.header();
                if (order.compare(item.position(), through) <= 0 || !h.getFile().equals(identity.file()) || !h.getLibrary().equals(identity.library())) {
                    continue;
                }
                if (As400FileRename.moveOpening(item)) {
                    observed = As400FileRename.observeMove(identity, entries.subList(later, entries.size()), order);
                    identity = new As400SchemaChange.ObjectIdentity(identity.file(), observed.library(), identity.fileJid(), identity.member(), identity.members());
                    through = observed.records().get(observed.records().size() - 1).position();
                    continue;
                }
                String type = h.getJournalCode() + "/" + h.getEntryType();
                if (h.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType(type)) {
                    As400SourceNormalizer.validateUnobservedRow(h, item.data(), 0, item.position());
                    identity.identity(h);
                }
                else {
                    require(As400JournalLifecycle.creationAdministrative(new As400JournalLifecycle.Entry(h, item.data(), item.position()), identity, false),
                            "Later operation changes a moved creation before metadata acquisition");
                }
            }
            return new LaterMove(identity, entries.subList(0, index), through);
        }
        return null;
    }

    /** A null definition performs only the pre-catalog lifecycle check. */
    static As400SchemaChange qualify(As400BindingDefinition after, List<RawJournalPage.Entry> entries,
                                     As400JournalOrder order, JournalPosition metadataStartedAfter, As400SourceNormalizer.TableResolver verifiedBindings)
            throws Exception {
        return qualify(after, entries, order, metadataStartedAfter, verifiedBindings, null, null);
    }

    static As400SchemaChange qualify(As400BindingDefinition after, List<RawJournalPage.Entry> entries,
                                     As400JournalOrder order, JournalPosition metadataStartedAfter, As400SourceNormalizer.TableResolver verifiedBindings,
                                     As400HistoricalTableNames.Reader names, String catalog)
            throws Exception {
        require(!entries.isEmpty(), "Creation requires its complete observed journal interval");
        var moved = laterMove(entries, order);
        if (moved != null) {
            As400SchemaChange.requireMetadataAfter(moved.through(), metadataStartedAfter, order, "Catalog was read before the created file move completed");
            As400BindingDefinition atCreation = null;
            if (after != null) {
                require(As400SchemaChange.ObjectIdentity.of(after).equals(moved.identity()), "Current catalog differs from the moved creation");
                var id = after.identity();
                var originalTable = new io.debezium.relational.TableId(id.tableId().catalog(), entries.get(0).header().getLibrary(), id.tableId().table());
                atCreation = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(originalTable, id.physicalFile(), id.member(), id.fileJournalId(),
                        id.memberJournalId(), id.recordFormat()), after.schema().edit().tableId(originalTable).create(), after.codecs(), after.members());
            }
            // Validate the complete creation/outcome and its earlier rows/names
            // under the original library. The move is replayed as separate history.
            return qualify(atCreation, moved.prefix(), order, metadataStartedAfter, verifiedBindings, names, catalog);
        }
        // The pre-catalog pass can prove lifecycle, but cannot decide output
        // selection from a system name. It returns no binding. The final pass
        // applies the actual SQL identity and rejects unavailable captured layouts
        // before history or output can advance.
        boolean needsHistoricalValues = after != null && verifiedBindings.needsValues(after.schema().id());
        var opening = entries.get(0);
        var progress = new Progress(opening);
        var first = progress.first;
        boolean laterRename = entries.stream().anyMatch(entry -> entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("FN")
                && entry.header().getFile().equals(first.getFile()) && entry.header().getLibrary().equals(first.getLibrary()));
        String fileJid = progress.fileJid;
        String producer = progress.producer;
        var cycle = progress.cycle;
        boolean transactional = cycle.signum() > 0;
        boolean committed = !transactional;
        boolean rolledBack = false;
        String member = null, memberJid = null;
        As400SchemaChange.Miscellaneous miscellaneous = null;
        boolean precedingRows = false;
        boolean precedingClear = false;
        var marks = new ArrayList<As400SchemaChange.Mark>();
        var administrative = new ArrayList<As400SchemaChange.Mark>();
        JournalPosition previous = null;
        for (int index = 0; index < entries.size(); index++) {
            var entry = entries.get(index);
            require(previous == null || order.compare(previous, entry.position()) < 0, "Creation evidence is out of order");
            previous = entry.position();
            byte[] bytes = entry.data();
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(bytes, 0);
            if (progress.clear != null) {
                progress.clear.control(entry);
            }
            if (transactional && header.getJournalCode() == 'C' && header.getCommitCycle().equals(cycle)) {
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position());
                var control = new io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder().decode(header, bytes, 0);
                require(producer.equals(As400SchemaChange.producer(entry)), "Creation outcome belongs to another producer");
                if (header.getEntryType().equals("CM")) {
                    require(!committed && marks.size() >= As400SchemaChange.CREATION_TYPES.size() && !miscellaneous.pending()
                            && header.getControlInfo().nestedCommitLevel() == 0
                            && control.outcome() == io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT,
                            "Creation did not durably commit");
                    committed = true;
                    marks.add(As400SchemaChange.mark(entry, order));
                }
                else if (header.getEntryType().equals("RB")) {
                    require(!committed && !rolledBack && marks.size() >= As400SchemaChange.CREATION_TYPES.size() && !miscellaneous.pending()
                            && header.getControlInfo().nestedCommitLevel() == 0
                            && control.outcome() == io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Outcome.FULL_ROLLBACK,
                            "Creation lacks a complete full rollback");
                    rolledBack = true;
                    marks.add(As400SchemaChange.mark(entry, order));
                }
                continue;
            }
            if (!header.getFile().equals(first.getFile()) || !header.getLibrary().equals(first.getLibrary())) {
                continue; // Other tables and transactions do not establish this file's creation.
            }
            var nativeEntry = new As400JournalLifecycle.Entry(header, bytes, entry.position());
            if (progress.clear != null && progress.clear.accept(entry)) {
                precedingClear = true;
                continue; // The ordinary lifecycle still validates and emits/discards the clear.
            }
            if (rolledBack) {
                As400SourceNormalizer.validateSource(header, bytes, 0, entry.position());
                boolean file = nativeEntry.type().equals("D/DF");
                require(producer.equals(As400SchemaChange.producer(entry)) && header.getCommitCycle().signum() == 0
                        && header.getControlInfo().nestedCommitLevel() == 0 && header.getControlInfo().flags() == 0
                        && header.getControlInfo().indicatorFlag() == '0' && header.getRecordInfo().recordNumber().signum() == 0
                        && header.getRecordInfo().objectNameIndicator() == '1'
                        && header.getRecordInfo().objectType().equals(file ? "*FILE" : "*QDDS")
                        && header.getRecordInfo().objectJournalId().equals(file ? fileJid : memberJid)
                        && header.getMember().equals(file ? "" : member)
                        && As400JournalLifecycle.payload(nativeEntry).length == 0
                        && nativeEntry.type().equals(marks.get(marks.size() - 1).type().equals("C/RB") ? "F/MD" : "D/DF"),
                        "Creation rollback lacks its exact physical deletion");
                marks.add(As400SchemaChange.mark(entry, order));
                if (file) {
                    return new As400SchemaChange(null, null, marks);
                }
                continue;
            }
            if (marks.size() >= As400SchemaChange.CREATION_TYPES.size()) {
                if (miscellaneous.accept(entry)) {
                    if (nativeEntry.type().equals("D/CG")
                            && new io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID)
                                    .decode(header, bytes, 0).changeType() == '1') {
                        require(!needsHistoricalValues || !precedingRows, "Created rows require an unavailable intermediate CL-command definition");
                    }
                    marks.add(As400SchemaChange.mark(entry, order));
                    continue;
                }
                if (nativeEntry.type().equals("D/DT")) {
                    var deleted = As400FileDeletion.qualifyEvidence(progress.identity(), entries.subList(index, entries.size()), order, false);
                    require(!deleted.rollback(), "Rolled-back DROP requires the created definition");
                    if (!committed) {
                        require(header.getCommitCycle().equals(cycle) && producer.equals(As400SchemaChange.producer(entry)),
                                "Uncommitted creation was deleted by another transaction");
                    }
                    marks.addAll(deleted.records());
                    var excluded = precedingRows ? As400ExcludedCreation.prove(progress.identity(), entries.subList(0, index),
                            marks.get(marks.size() - 1).position(), order, names, catalog, verifiedBindings) : null;
                    return new As400SchemaChange(null, null, marks, false, excluded);
                }
                if (nativeEntry.type().equals("D/FN")) {
                    boolean laterDeletion = entries.subList(index + 1, entries.size()).stream().anyMatch(item -> item.header().getJournalCode() == 'D'
                            && item.header().getEntryType().equals("DT") && item.header().getLibrary().equals(first.getLibrary())
                            && item.header().getRecordInfo().objectJournalId().equals(fileJid));
                    if (names != null && committed && !miscellaneous.pending() && !progress.clear.pending()
                            && (precedingRows || precedingClear || laterDeletion)) {
                        // Persist the proven original identity, not today's
                        // renamed catalog. Ordinary replay can then qualify the
                        // rename and eventual DROP as separate transitions.
                        var table = As400ExcludedCreation.excludedName(progress.identity(), entries.subList(0, index), names, catalog, verifiedBindings);
                        var identity = new As400TransactionBuffer.TableIdentity(table, first.getFile(), member, fileJid, memberJid,
                                As400BindingDefinition.UNOBSERVED_LAYOUT);
                        return new As400SchemaChange(null, As400BindingDefinition.excluded(identity), marks);
                    }
                    require(committed && !precedingRows && !precedingClear && !miscellaneous.pending() && !progress.clear.pending(),
                            "Renamed creation requires an unavailable earlier SQL name or an unfinished outcome");
                    var renamed = As400FileRename.observeCreated(progress.identity(), entries.subList(index, entries.size()), order);
                    marks.addAll(administrative);
                    marks.addAll(renamed.records());
                    marks.sort((a, b) -> order.compare(a.position(), b.position()));
                    As400SchemaChange.requireMetadataAfter(marks.get(marks.size() - 1).position(), metadataStartedAfter, order,
                            "Catalog was read before creation and rename completed");
                    if (after == null) {
                        return null;
                    }
                    var id = after.identity();
                    require(id.tableId().schema().equals(first.getLibrary()) && id.physicalFile().equals(renamed.file())
                            && id.fileJournalId().equals(fileJid) && id.member().equals(member) && id.memberJournalId().equals(memberJid)
                            && after.members().equals(progress.identity().members()), "Current catalog differs from the renamed creation");
                    return new As400SchemaChange(null, after, marks);
                }
                if (As400SchemaChange.isOpening(entry)) {
                    require(!needsHistoricalValues || !precedingRows, "Created rows require an unavailable intermediate table definition");
                    int deletionAt = deletionIndex(progress.identity(), entries, index + 1);
                    var deleted = deletionAt < 0 ? null
                            : As400FileDeletion.qualifyEvidence(progress.identity(), entries.subList(deletionAt, entries.size()), order, false);
                    require(deleted == null || !deleted.rollback(), "Rolled-back DROP requires the created definition");
                    var later = new ArrayList<>(entries.subList(index, deletionAt < 0 ? entries.size() : deletionAt));
                    if (deleted != null) {
                        // CREATE, ALTER and DROP can share one transaction. Its
                        // terminal follows D/DT; validate it in both lifecycles,
                        // but keep the deletion itself out of the ALTER grammar.
                        var through = deleted.records().get(deleted.records().size() - 1).position();
                        for (var item : entries.subList(deletionAt + 1, entries.size())) {
                            if (order.compare(item.position(), through) > 0) {
                                break;
                            }
                            if (item.header().getJournalCode() == 'C') {
                                later.add(item);
                            }
                        }
                        if (!committed) {
                            var deletion = entries.get(deletionAt);
                            require(deletion.header().getCommitCycle().equals(cycle) && producer.equals(As400SchemaChange.producer(deletion)),
                                    "Uncommitted creation was deleted by another transaction");
                        }
                    }
                    var qualified = As400SchemaChange.qualifyEvidence(progress.identity(), later, order, (file, library) -> {
                        require(after != null && file.equals(first.getFile()) && library.equals(first.getLibrary()),
                                "No acquired binding for new-file administrative evidence");
                        return after.binding(65536);
                    }, !needsHistoricalValues, true);
                    require(!qualified.rollback(), "New-file definition rollback requires its complete creation outcome");
                    As400SchemaChange.requireMetadataAfter(qualified.observedThrough(), metadataStartedAfter, order,
                            "Catalog was read before the created definition was restored");
                    if (!committed) {
                        var outcome = later.stream().filter(item -> item.header().getJournalCode() == 'C'
                                && item.header().getEntryType().equals("CM") && item.header().getCommitCycle().equals(cycle)).findFirst().orElseThrow(
                                        As400SchemaTimeline.PendingChangeException::new);
                        require(producer.equals(As400SchemaChange.producer(outcome))
                                && qualified.records().stream().anyMatch(mark -> mark.position().equals(outcome.position())),
                                "Creation and later definition have different outcomes");
                        committed = true;
                    }
                    marks.addAll(qualified.records());
                    if (deleted != null) {
                        // The shared commit may be in both proofs. Its bytes
                        // must agree, and history retains each position once.
                        marks.addAll(deleted.records());
                        var unique = new java.util.TreeMap<JournalPosition, As400SchemaChange.Mark>(order::compare);
                        for (var mark : marks) {
                            var prior = unique.putIfAbsent(mark.position(), mark);
                            require(prior == null || prior.equals(mark), "Creation and deletion disagree on a shared journal entry");
                        }
                        var prefix = entries.subList(0, deletionAt);
                        boolean rows = prefix.stream().anyMatch(item -> item.header().getJournalCode() == 'R'
                                && !As400JournalLifecycle.administrativeType("R/" + item.header().getEntryType())
                                && item.header().getFile().equals(first.getFile()) && item.header().getLibrary().equals(first.getLibrary()));
                        var excluded = rows ? As400ExcludedCreation.prove(progress.identity(), prefix, unique.lastKey(), order, names, catalog, verifiedBindings)
                                : null;
                        return new As400SchemaChange(null, null, new ArrayList<>(unique.values()), false, excluded);
                    }
                    break;
                }
                if (As400JournalLifecycle.administrativeType(nativeEntry.type())) {
                    As400JournalLifecycle.creationAdministrative(nativeEntry, progress.identity(), !needsHistoricalValues || !precedingRows);
                    if (List.of("D/AC", "D/DC").contains(nativeEntry.type())) {
                        // The first published row must use the acquired final
                        // key. No earlier row may require the unavailable key.
                        marks.add(As400SchemaChange.mark(entry, order));
                    }
                    else if (laterRename) {
                        administrative.add(As400SchemaChange.mark(entry, order));
                    }
                    continue;
                }
                require(header.getJournalCode() == 'R', "Another table transition precedes creation metadata acquisition");
                As400SourceNormalizer.validateUnobservedRow(header, bytes, 0, entry.position());
                precedingRows = true;
                require(header.getRecordInfo().objectJournalId().equals(memberJid) && header.getMember().equals(member)
                        && header.getRecordInfo().objectType().equals("*QDDS"), "Rows belong to a different created member");
                if (!committed) {
                    require(header.getCommitCycle().equals(cycle) && producer.equals(As400SchemaChange.producer(entry)),
                            "Uncommitted creation contains output from another transaction");
                }
                continue;
            }
            progress.physical(entry);
            member = progress.member;
            memberJid = progress.memberJid;
            marks.add(As400SchemaChange.mark(entry, order));
            if (marks.size() == As400SchemaChange.CREATION_TYPES.size()) {
                miscellaneous = progress.miscellaneous;
            }
        }
        if (rolledBack || !committed || miscellaneous == null || miscellaneous.pending() || progress.clear.pending()) {
            throw new As400SchemaTimeline.PendingChangeException();
        }
        As400SchemaChange.requireMetadataAfter(marks.get(marks.size() - 1).position(), metadataStartedAfter, order,
                "Catalog was read before creation completed");
        if (after == null) {
            return null;
        }
        var id = after.identity();
        require(id.tableId().schema().equals(first.getLibrary()) && id.physicalFile().equals(first.getFile()) && id.member().equals(member)
                && id.fileJournalId().equals(fileJid) && id.memberJournalId().equals(memberJid), "Current catalog differs from the observed creation");
        require(!needsHistoricalValues || entries.stream().noneMatch(entry -> entry.header().getJournalCode() == 'R'
                && entry.header().getFile().equals(first.getFile()) && entry.header().getLibrary().equals(first.getLibrary())
                && entry.header().getPointerHandle() != 0), "Selected creation requires complete row values, not pointer metadata");
        if (outputBeforeMetadata(progress.identity(), entries)) {
            require(names != null, "Creation output before metadata requires historical SQL names");
            var originalName = names.read(opening).creation(opening, catalog);
            var currentName = originalName;
            boolean selectedName = verifiedBindings.needsValues(originalName);
            boolean renamed = false;
            for (var entry : entries) {
                if (entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("CG")
                        && entry.header().getFile().equals(first.getFile()) && entry.header().getLibrary().equals(first.getLibrary())) {
                    var next = names.read(entry).changed(entry, currentName);
                    renamed |= !next.equals(currentName);
                    selectedName |= verifiedBindings.needsValues(next);
                    currentName = next;
                }
            }
            require(currentName.equals(after.schema().id()), "Acquired creation name differs from retained SQL-name history");
            require(!renamed || !selectedName, "Captured creation output requires its earlier SQL name before rename");
        }
        return new As400SchemaChange(null, after, marks);
    }

    /** Include suffixes qualified by ALTER; delegation must not bypass SQL-name history. */
    private static boolean outputBeforeMetadata(As400SchemaChange.ObjectIdentity identity, List<RawJournalPage.Entry> entries) throws java.io.IOException {
        boolean output = false;
        for (var entry : entries) {
            var header = entry.header();
            if (!header.getFile().equals(identity.file()) || !header.getLibrary().equals(identity.library())) {
                continue;
            }
            if (header.getJournalCode() == 'D' && header.getEntryType().equals("CG")) {
                var change = new io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(header, entry.data(), 0);
                if (output && change.changeType() == '2') {
                    return true;
                }
                output |= change.clearsRows();
            }
            output |= header.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType("R/" + header.getEntryType())
                    || header.getJournalCode() == 'F' && header.getEntryType().equals("CR");
        }
        return false;
    }

    private static int deletionIndex(As400SchemaChange.ObjectIdentity identity, List<RawJournalPage.Entry> entries, int from) {
        for (int index = from; index < entries.size(); index++) {
            var header = entries.get(index).header();
            if (header.getJournalCode() == 'D' && header.getEntryType().equals("DT")
                    && header.getFile().equals(identity.file()) && header.getLibrary().equals(identity.library())) {
                return index;
            }
        }
        return -1;
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
