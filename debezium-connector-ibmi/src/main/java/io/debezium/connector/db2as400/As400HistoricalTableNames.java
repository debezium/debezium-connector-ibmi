/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.relational.TableId;

/** Names from QDBRPLAY's read-only exit mode, bound to one retained journal entry. Never a row schema. */
record As400HistoricalTableNames(String digest, String library, String file, String oldSqlName, String newSqlName) {
    interface Reader {
        As400HistoricalTableNames read(RawJournalPage.Entry entry) throws Exception;
    }

    static As400HistoricalTableNames decode(RawJournalPage.Entry entry, byte[] scratch) throws Exception {
        require(entry.header().getJournalCode() == 'D' && java.util.List.of("CT", "CG").contains(entry.header().getEntryType()),
                "Historical names require a retained file creation/change");
        require(scratch != null && scratch.length == 8708, "Incomplete historical-name callback buffer");
        var input = ByteBuffer.wrap(scratch);
        int count = input.getInt(0);
        require(count > 0 && count <= 16, "Incomplete or excessive historical-name callbacks");
        Map<Integer, String> names = new HashMap<>();
        String library = identifier(entry.header().getLibrary());
        String file = identifier(entry.header().getFile());
        for (int i = 0; i < count; i++) {
            int at = 4 + i * 544;
            // QDBRPLAY's packed exit structure in QSYSINC/H: 537 bytes,
            // Object_Use at 532. Reject any other version rather than guessing.
            require(input.getInt(at) == 537 && input.getInt(at + 12) == 3 && scratch[at + 536] == (byte) 0xf0,
                    "Unsupported historical-name callback layout");
            int role = input.getInt(at + 532);
            require(role == 1 || role == 3 || role == 4, "Unexpected historical-name callback role");
            String name = field(scratch, at + 16, input.getInt(at + 4));
            require(library.equals(field(scratch, at + 274, input.getInt(at + 8))), "Historical name belongs to another library");
            require(names.putIfAbsent(role, name) == null, "Repeated or ambiguous historical SQL name");
        }
        require(file.equals(names.get(1)), "Historical names target another native file");
        boolean creation = entry.header().getEntryType().equals("CT");
        require(!creation || !names.containsKey(4), "Creation cannot carry a renamed SQL identity");
        require(!names.containsKey(4) || names.containsKey(3), "SQL rename lacks its prior name");
        return new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()), library, file,
                names.getOrDefault(3, creation ? file : null), names.get(4));
    }

    TableId creation(RawJournalPage.Entry entry, String catalog) throws Exception {
        verify(entry);
        require(entry.header().getEntryType().equals("CT") && oldSqlName != null && newSqlName == null,
                "Creation lacks its historical SQL name");
        return new TableId(catalog, library, oldSqlName);
    }

    TableId changed(RawJournalPage.Entry entry, TableId previous) throws Exception {
        verify(entry);
        require(entry.header().getEntryType().equals("CG") && previous.schema().equals(library)
                && (oldSqlName == null || oldSqlName.equals(previous.table())), "Historical SQL-name chain differs");
        if (newSqlName != null) {
            return new TableId(previous.catalog(), library, newSqlName);
        }
        // A miscellaneous SQL rename can remove an alternative long name.
        // Its callback contains the old name only; the journaled native target
        // becomes the SQL name. A full ALTER also carries the old name only,
        // but keeps it. LABEL/COMMENT omit both and never imply a name change.
        if (oldSqlName != null && new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID)
                .decode(entry.header(), entry.data(), 0).changeType() == '2') {
            return new TableId(previous.catalog(), library, file);
        }
        return previous;
    }

    private void verify(RawJournalPage.Entry entry) throws Exception {
        require(Objects.equals(digest, As400JournalRecordContent.digest(entry.data(), entry.position()))
                && library.equals(identifier(entry.header().getLibrary())) && file.equals(identifier(entry.header().getFile())),
                "Historical names do not match retained journal content");
    }

    private static String field(byte[] bytes, int offset, int length) {
        require(length > 0 && length <= 258, "Invalid historical SQL-name length");
        return identifier(new AS400Text(length, RawJournalPage.METADATA_CCSID).toObject(bytes, offset).toString());
    }

    static String identifier(String value) {
        require(value != null && !value.isBlank() && value.indexOf('\0') < 0, "Missing historical SQL identifier");
        if (!value.startsWith("\"")) {
            require(value.indexOf('"') < 0 && value.equals(value.trim()), "Invalid unquoted historical identifier");
            return value;
        }
        require(value.length() > 2 && value.endsWith("\""), "Incomplete delimited historical identifier");
        String inner = value.substring(1, value.length() - 1);
        require(!inner.replace("\"\"", "").contains("\""), "Malformed delimited historical identifier");
        return inner.replace("\"\"", "\"");
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
