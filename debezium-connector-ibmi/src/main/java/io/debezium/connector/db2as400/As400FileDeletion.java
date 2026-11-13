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

    static As400SchemaChange qualify(As400BindingDefinition before, List<RawJournalPage.Entry> entries, As400JournalOrder order) throws Exception {
        require(before != null && !entries.isEmpty(), "Deletion requires a retained file binding and native history");
        var id = before.identity();
        var members = new HashSet<>(before.members().keySet());
        var marks = new ArrayList<As400SchemaChange.Mark>();
        var producer = As400SchemaChange.producer(entries.get(0));
        var cycle = entries.get(0).header().getCommitCycle();
        boolean committed = cycle.signum() == 0;
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
                    return new As400SchemaChange(before, before, marks);
                }
                require(terminal.outcome() == TransactionControlEntryDecoder.Outcome.DURABLE_COMMIT, "Deletion requires a durable terminal");
                committed = true;
                continue;
            }
            if (!h.getFile().equals(id.physicalFile()) || !h.getLibrary().equals(id.tableId().schema())) {
                continue;
            }
            As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
            String type = h.getJournalCode() + "/" + h.getEntryType();
            boolean file = h.getJournalCode() == 'D';
            require(h.getCommitCycle().equals(marks.isEmpty() ? cycle : java.math.BigInteger.ZERO) && h.getControlInfo().nestedCommitLevel() == 0
                    && h.getControlInfo().indicatorFlag() == '0' && h.getRecordInfo().recordNumber().signum() == 0
                    && h.getRecordInfo().objectNameIndicator() == '1' && producer.equals(As400SchemaChange.producer(entry))
                    && h.getRecordInfo().objectType().equals(file ? "*FILE" : "*QDDS")
                    && h.getRecordInfo().objectJournalId().equals(file ? id.fileJournalId() : before.members().get(h.getMember()))
                    && (!file || h.getMember().isEmpty()), "Deletion object identity or transaction context changed");
            byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()));
            if (marks.isEmpty()) {
                require(type.equals("D/DT") && payload.length >= 60, "Deletion must begin with an object-level D/DT");
                var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
                require(name.toObject(payload, 0).toString().trim().equals(h.getFile())
                        && name.toObject(payload, 10).toString().trim().equals(h.getLibrary())
                        && new AS400Text(40, RawJournalPage.METADATA_CCSID).toObject(payload, 20).toString().isBlank(),
                        "Deletion payload object identity differs");
            }
            else if (type.equals("F/MD")) {
                require(committed && payload.length == 0 && members.remove(h.getMember()), "Uncommitted, repeated, foreign or malformed member deletion");
            }
            else {
                require(committed && type.equals("D/DF") && members.isEmpty() && payload.length == 0, "Incomplete physical-file deletion");
                marks.add(As400SchemaChange.mark(entry, order));
                // D/DF ends this object's lifetime. A later creation under the
                // same name has its own JIDs and must qualify independently.
                return new As400SchemaChange(before, null, marks);
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
