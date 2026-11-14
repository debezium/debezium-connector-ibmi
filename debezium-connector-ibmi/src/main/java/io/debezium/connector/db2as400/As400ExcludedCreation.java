/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.List;
import java.util.Set;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/**
 * An observed CREATE/load/DROP whose historical SQL names are all excluded.
 * This carries native identity and lifetime, never an invented record format.
 * F retains the entire lifetime; a fresh task re-establishes this proof.
 */
record As400ExcludedCreation(As400SchemaChange.ObjectIdentity identity, JournalPosition from, JournalPosition through) {
    static As400ExcludedCreation prove(As400SchemaChange.ObjectIdentity identity, List<RawJournalPage.Entry> prefix,
                                       JournalPosition through, As400JournalOrder order, As400HistoricalTableNames.Reader names,
                                       String catalog, As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        excludedName(identity, prefix, names, catalog, bindings);
        var proof = new As400ExcludedCreation(identity, prefix.get(0).position(), through);
        for (var entry : prefix) {
            var h = entry.header();
            if (h.getFile().equals(identity.file()) && h.getLibrary().equals(identity.library())
                    && h.getJournalCode() == 'R' && !As400JournalLifecycle.administrativeType("R/" + h.getEntryType())) {
                proof.validate(entry, order);
            }
        }
        return proof;
    }

    /** Exact SQL names in a qualified creation prefix, without inferring a row layout. */
    static io.debezium.relational.TableId excludedName(As400SchemaChange.ObjectIdentity identity, List<RawJournalPage.Entry> prefix,
                                                       As400HistoricalTableNames.Reader names, String catalog,
                                                       As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        require(names != null, "Deleted creation has rows requiring an unavailable historical definition");
        var opening = prefix.get(0);
        var table = names.read(opening).creation(opening, catalog);
        require(!bindings.needsValues(table), "Selected transient table requires its unavailable historical definition");
        for (var entry : prefix) {
            var h = entry.header();
            if (!h.getFile().equals(identity.file()) || !h.getLibrary().equals(identity.library())) {
                continue;
            }
            if (h.getJournalCode() == 'D' && h.getEntryType().equals("CG")) {
                table = names.read(entry).changed(entry, table);
                require(!bindings.needsValues(table), "Selected historical SQL name requires its unavailable definition");
            }
        }
        return table;
    }

    void validate(RawJournalPage.Entry entry, As400JournalOrder order) throws Exception {
        var h = entry.header();
        require(order.compare(entry.position(), from) > 0 && order.compare(entry.position(), through) < 0,
                "Excluded transient row escapes its retained lifetime");
        As400JournalRecordContent.digest(entry.data(), entry.position());
        As400SourceNormalizer.validateUnobservedRow(h, entry.data(), 0, entry.position());
        require(h.getJournalCode() == 'R' && Set.of("PT", "PX", "UB", "UP", "DL", "BR", "DR", "UR", "R1").contains(h.getEntryType())
                && h.getFile().equals(identity.file()) && h.getLibrary().equals(identity.library())
                && h.getRecordInfo().objectType().equals("*QDDS") && h.getRecordInfo().objectNameIndicator() == '1'
                && identity.members().containsKey(h.getMember())
                && identity.members().get(h.getMember()).equals(h.getRecordInfo().objectJournalId())
                && h.getRecordInfo().recordNumber().signum() > 0,
                "Excluded transient row changed its file/member identity or operation");
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
