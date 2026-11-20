/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;

/** Retire a binding only after the complete native physical-file deletion. */
final class As400FileDeletion {
    private As400FileDeletion() {
    }

    /** Validate the opening without querying metadata or assuming its eventual outcome. */
    static void opening(As400BindingDefinition before, RawJournalPage.Entry entry) {
        require(before != null, "Deletion requires a retained file binding");
        opening(As400SchemaChange.ObjectIdentity.of(before), entry);
    }

    static void opening(As400SchemaChange.ObjectIdentity id, RawJournalPage.Entry entry) {
        var h = entry.header();
        As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
        require(h.getJournalCode() == 'D' && h.getEntryType().equals("DT")
                && h.getFile().equals(id.file()) && h.getLibrary().equals(id.library())
                && h.getMember().isEmpty() && h.getRecordInfo().objectType().equals("*FILE")
                && h.getRecordInfo().objectJournalId().equals(id.fileJid()) && h.getRecordInfo().objectNameIndicator() == '1'
                && h.getRecordInfo().recordNumber().signum() == 0 && h.getControlInfo().nestedCommitLevel() == 0
                && h.getControlInfo().indicatorFlag() == '0', "Deletion object identity or transaction context changed");
        byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()));
        require(payload.length >= 60, "Deletion must begin with an object-level D/DT");
        var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
        require(name.toObject(payload, 0).toString().trim().equals(h.getFile())
                && name.toObject(payload, 10).toString().trim().equals(h.getLibrary())
                && new AS400Text(40, RawJournalPage.METADATA_CCSID).toObject(payload, 20).toString().isBlank(),
                "Deletion payload object identity differs");
    }

    static As400SchemaChange qualify(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        require(before != null && !entries.isEmpty(), "Deletion requires a retained file binding and native history");
        var evidence = qualifyEvidence(As400SchemaChange.ObjectIdentity.of(before), entries, order, true);
        return new As400SchemaChange(before, evidence.rollback() ? before : null, evidence.records());
    }

    record Evidence(List<As400SchemaChange.Mark> records, boolean rollback) {
        Evidence {
            records = List.copyOf(records);
        }
    }

    /** A completed row-free definition change followed by DROP needs no vanished layout. */
    static As400SchemaChange afterSchemaChange(As400BindingDefinition before, List<RawJournalPage.Entry> entries,
                                               As400JournalOrder order, As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        return afterSchemaChange(before, entries, order, bindings, null);
    }

    static As400SchemaChange afterSchemaChange(As400BindingDefinition before, List<RawJournalPage.Entry> entries,
                                               As400JournalOrder order, As400SourceNormalizer.TableResolver bindings,
                                               As400HistoricalTableNames.Reader names)
            throws Exception {
        if (before == null || entries.isEmpty() || entries.get(0).header().getJournalCode() != 'F'
                || !As400SchemaChange.isOpening(entries.get(0))) {
            return null;
        }
        var id = As400SchemaChange.ObjectIdentity.of(before);
        for (int at = 1; at < entries.size(); at++) {
            var h = entries.get(at).header();
            if (h.getJournalCode() != 'D' || !h.getEntryType().equals("DT") || !h.getLibrary().equals(id.library())
                    || !h.getFile().equals(id.file()) || !h.getRecordInfo().objectJournalId().equals(id.fileJid())) {
                continue;
            }
            var prefix = entries.subList(0, at);
            var namedClearPrefix = As400SchemaChange.metadataBeforeClear(before, entries, order, bindings, names);
            if (namedClearPrefix != null) {
                return namedClearPrefix;
            }
            var cycle = h.getCommitCycle();
            boolean sharedOutcome = cycle.signum() > 0 && prefix.stream().anyMatch(entry -> entry.header().getJournalCode() == 'D'
                    && entry.header().getEntryType().equals("CG") && entry.header().getFile().equals(id.file())
                    && entry.header().getLibrary().equals(id.library()) && entry.header().getCommitCycle().equals(cycle))
                    && prefix.stream().noneMatch(entry -> entry.header().getJournalCode() == 'C'
                            && entry.header().getEntryType().equals("CM") && entry.header().getCommitCycle().equals(cycle));
            if (sharedOutcome) {
                // One native transaction can ALTER, load and DROP an existing
                // file. Its terminal follows D/DT, so qualify the
                // terminal in both lifecycles without treating D/DT as ALTER.
                var deleted = qualifyEvidence(id, entries.subList(at, entries.size()), order, true);
                if (deleted.rollback()) {
                    var restored = new ArrayList<>(entries);
                    restored.remove(at); // Only the independently qualified D/DT is omitted from ALTER's grammar.
                    var changed = As400SchemaChange.qualifyEvidence(id, restored, order, bindings, false, false);
                    var terminal = deleted.records().get(deleted.records().size() - 1);
                    require(changed.rollback() && changed.records().contains(terminal), "ALTER and DROP require the same full rollback");
                    validateSharedRows(id, entries, entries.get(at), terminal.position(), order);
                    // The complete four-record restoration proves the retained
                    // definition, even for captured rows whose temporary layout
                    // was never observed. None of those rolled-back rows escape.
                    return new As400SchemaChange(before, before, merge(changed.records(), deleted.records(), order));
                }
                var extended = new ArrayList<>(prefix);
                var through = deleted.records().get(deleted.records().size() - 1).position();
                for (var item : entries.subList(at + 1, entries.size())) {
                    if (order.compare(item.position(), through) > 0) {
                        break;
                    }
                    if (item.header().getJournalCode() == 'C') {
                        extended.add(item);
                    }
                }
                var changed = As400SchemaChange.excludedCommittedPrefix(before, extended, order, bindings, names);
                require(changed != null && deleted.records().stream().anyMatch(mark -> mark.type().equals("C/CM")
                        && mark.position().equals(changed.commit())), "Shared ALTER/DROP requires unchanged excluded historical SQL names");
                validateSharedRows(id, prefix, entries.get(at), changed.commit(), order);
                return new As400SchemaChange(before, null, merge(changed.records(), deleted.records(), order));
            }
            boolean metadata = false;
            for (var entry : prefix) {
                var header = entry.header();
                if (header.getFile().equals(id.file()) && header.getLibrary().equals(id.library())) {
                    metadata |= As400SchemaChange.metadataOpening(entry);
                    // A row-free interval can still publish a truncate. Do not
                    // erase a possible SQL-name boundary by combining it with
                    // DROP when the names-aware prefix could not be proven.
                    require(!metadata || !As400SchemaChange.clearOpening(entry),
                            "Clears require their historical SQL name before deletion");
                }
            }
            boolean rows = prefix.stream().anyMatch(entry -> entry.header().getJournalCode() == 'R'
                    && !As400JournalLifecycle.administrativeType("R/" + entry.header().getEntryType())
                    && entry.header().getLibrary().equals(id.library())
                    && (entry.header().getFile().equals(id.file()) || id.members().containsValue(entry.header().getRecordInfo().objectJournalId())));
            if (rows) {
                // Preserve the known identity without guessing the vanished
                // layout. Its committed excluded prefix remains a separate
                // history event; ordinary replay will retire it at D/DT.
                var excluded = As400SchemaChange.excludedCommittedPrefix(before, prefix, order, bindings, names);
                if (excluded != null) {
                    return excluded;
                }
            }
            require(!rows, "Rows require the vanished changed definition before deletion");
            var changed = As400SchemaChange.qualifyEvidence(id, prefix, order, bindings, false, false);
            if (!changed.observedThrough().equals(changed.records().get(changed.records().size() - 1).position())) {
                return null; // A later rollback must retain its own replay/discard boundary.
            }
            require(!changed.rollback(), "Rolled-back schema change must retain its original definition");
            var deleted = qualifyEvidence(id, entries.subList(at, entries.size()), order, false);
            require(!deleted.rollback(), "Rolled-back DROP requires the changed definition");
            var marks = new ArrayList<>(changed.records());
            marks.addAll(deleted.records());
            return new As400SchemaChange(before, null, marks);
        }
        return null;
    }

    private static void validateSharedRows(As400SchemaChange.ObjectIdentity id, List<RawJournalPage.Entry> entries,
                                           RawJournalPage.Entry deletion, io.debezium.ibmi.db2.journal.retrieve.JournalPosition terminal,
                                           As400JournalOrder order) {
        for (var entry : entries) {
            if (order.compare(entry.position(), terminal) > 0) {
                break;
            }
            var h = entry.header();
            if (h.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType("R/" + h.getEntryType())
                    && h.getLibrary().equals(id.library()) && h.getFile().equals(id.file())) {
                id.identity(h);
                require(h.getCommitCycle().equals(deletion.header().getCommitCycle())
                        && As400SchemaChange.producer(entry).equals(As400SchemaChange.producer(deletion)),
                        "Shared ALTER/DROP row changed transaction or producer");
            }
        }
    }

    private static List<As400SchemaChange.Mark> merge(List<As400SchemaChange.Mark> changed, List<As400SchemaChange.Mark> deleted,
                                                      As400JournalOrder order) {
        var marks = new java.util.TreeMap<io.debezium.ibmi.db2.journal.retrieve.JournalPosition, As400SchemaChange.Mark>(order::compare);
        for (var mark : java.util.stream.Stream.concat(changed.stream(), deleted.stream()).toList()) {
            var previous = marks.putIfAbsent(mark.position(), mark);
            require(previous == null || previous.equals(mark), "ALTER and DROP disagree on their shared outcome");
        }
        return new ArrayList<>(marks.values());
    }

    static Evidence qualifyEvidence(As400SchemaChange.ObjectIdentity id, List<RawJournalPage.Entry> entries,
                                    As400JournalOrder order, boolean allowCompensation)
            throws Exception {
        require(!entries.isEmpty(), "Deletion requires its complete native history");
        var members = new HashSet<>(id.members().keySet());
        var marks = new ArrayList<As400SchemaChange.Mark>();
        var producer = As400SchemaChange.producer(entries.get(0));
        var cycle = entries.get(0).header().getCommitCycle();
        boolean committed = cycle.signum() == 0;
        boolean compensated = false;
        io.debezium.ibmi.db2.journal.retrieve.JournalPosition previous = null;
        for (var entry : entries) {
            require(previous == null || order.compare(previous, entry.position()) < 0, "Deletion history is out of order");
            previous = entry.position();
            As400JournalRecordContent.digest(entry.data(), entry.position());
            var h = entry.header();
            if (!marks.isEmpty() && cycle.signum() > 0 && h.getJournalCode() == 'C' && h.getCommitCycle().equals(cycle)) {
                require(!committed && producer.equals(As400SchemaChange.producer(entry)), "Deletion outcome has changed producer or repeated terminal");
                As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
                var terminal = new TransactionControlEntryDecoder().decode(h, entry.data(), 0);
                require(h.getControlInfo().nestedCommitLevel() == 0, "Nested deletion outcome is unqualified");
                marks.add(As400SchemaChange.mark(entry, order));
                if (terminal.outcome() == TransactionControlEntryDecoder.Outcome.FULL_ROLLBACK) {
                    return new Evidence(marks, true);
                }
                require(!compensated && terminal.outcome() == TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT,
                        "Deletion requires a durable terminal without unresolved rollback rows");
                committed = true;
                continue;
            }
            if (!h.getFile().equals(id.file()) || !h.getLibrary().equals(id.library())) {
                continue;
            }
            As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
            if (!marks.isEmpty() && !committed && h.getJournalCode() == 'R') {
                // A full DROP rollback first restores the file, then undoes rows
                // written earlier in the transaction. The normalizer/buffer must
                // still match each compensation to its original row during replay.
                id.identity(h);
                require(allowCompensation && h.getCommitCycle().equals(cycle) && h.getControlInfo().nestedCommitLevel() == 0
                        && producer.equals(As400SchemaChange.producer(entry))
                        && java.util.Set.of("BR", "DR", "UR", "R1").contains(h.getEntryType()),
                        "Deletion rollback row changed transaction, producer or operation");
                compensated = true;
                continue;
            }
            String type = h.getJournalCode() + "/" + h.getEntryType();
            boolean file = h.getJournalCode() == 'D';
            require(h.getCommitCycle().equals(marks.isEmpty() ? cycle : java.math.BigInteger.ZERO) && h.getControlInfo().nestedCommitLevel() == 0
                    && h.getControlInfo().indicatorFlag() == '0' && h.getRecordInfo().recordNumber().signum() == 0
                    && h.getRecordInfo().objectNameIndicator() == '1' && producer.equals(As400SchemaChange.producer(entry))
                    && h.getRecordInfo().objectType().equals(file ? "*FILE" : "*QDDS")
                    && h.getRecordInfo().objectJournalId().equals(file ? id.fileJid() : id.members().get(h.getMember()))
                    && (!file || h.getMember().isEmpty()), "Deletion object identity or transaction context changed");
            byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()));
            if (marks.isEmpty()) {
                opening(id, entry);
            }
            else if (type.equals("F/MD")) {
                require(committed && payload.length == 0 && members.remove(h.getMember()), "Uncommitted, repeated, foreign or malformed member deletion");
            }
            else {
                require(committed && type.equals("D/DF") && members.isEmpty() && payload.length == 0, "Incomplete physical-file deletion");
                marks.add(As400SchemaChange.mark(entry, order));
                // D/DF ends this object's lifetime. A later creation under the
                // same name has its own JIDs and must qualify independently.
                return new Evidence(marks, false);
            }
            marks.add(As400SchemaChange.mark(entry, order));
        }
        throw new As400SchemaTimeline.PendingChangeException();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
