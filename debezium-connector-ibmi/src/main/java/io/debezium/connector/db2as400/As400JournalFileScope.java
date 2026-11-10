/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/**
 * Complete current file inventory from WRKJRNA's documented output model and
 * independent journal counts. QTEMP output belongs only to the dedicated caller
 * connection. This observes scope; a surrounding native audit must still detect
 * membership/schema changes before an online boundary can be used.
 */
final class As400JournalFileScope {
    static final String FILES_SQL = """
            SELECT TRIM(CAST(QJOJRNLIB AS CHAR(10) CCSID 37)) AS JOURNAL_LIBRARY,
                   TRIM(CAST(QJOJOURNAL AS CHAR(10) CCSID 37)) AS JOURNAL_NAME,
                   TRIM(CAST(QJOASP AS CHAR(10) CCSID 37)) AS ASP,
                   TRIM(CAST(QJOLIBRARY AS CHAR(10) CCSID 37)) AS OBJECT_LIBRARY,
                   TRIM(CAST(QJOOBJECT AS CHAR(10) CCSID 37)) AS OBJECT_NAME,
                   TRIM(CAST(QJOOBJTYPE AS CHAR(7) CCSID 37)) AS OBJECT_TYPE,
                   TRIM(CAST(QJOFILETYP AS CHAR(2) CCSID 37)) AS FILE_TYPE,
                   CAST(QJOJRNTYPE AS CHAR(1) CCSID 37) AS JOURNALED_TYPE
            FROM QTEMP.DCGSCOPE
            """;
    static final String COUNTS_SQL = """
            SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_TYPE, JOURNAL_STATE,
                   JOURNALED_FILES, JOURNALED_MEMBERS, JOURNALED_LIBRARIES,
                   JOURNALED_DATA_AREAS, JOURNALED_DATA_QUEUES, JOURNALED_IFS_OBJECTS,
                   JOURNALED_ACCESS_PATHS, NUMBER_REMOTE_JOURNALS
            FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?
            """;
    private static final List<String> COUNTS = List.of("JOURNALED_FILES", "JOURNALED_MEMBERS", "JOURNALED_LIBRARIES", "JOURNALED_DATA_AREAS",
            "JOURNALED_DATA_QUEUES", "JOURNALED_IFS_OBJECTS", "JOURNALED_ACCESS_PATHS", "NUMBER_REMOTE_JOURNALS");

    record Scope(JournalReceiver journal, List<As400OperatorTableEvidence.Request> files, Set<String> journaledLibraries) {
        Scope {
            files = List.copyOf(files);
            journaledLibraries = Set.copyOf(journaledLibraries);
        }
    }

    private final JournalReceiver journal;
    private final int timeout;

    As400JournalFileScope(JournalReceiver journal, int timeout) {
        name(journal.library());
        name(journal.name());
        require(timeout > 0 && timeout <= 60, "Finite native inventory timeout required");
        this.journal = journal;
        this.timeout = timeout;
    }

    Scope collect(Connection connection) throws SQLException {
        check();
        require(connection.getAutoCommit() && connection.getTransactionIsolation() == Connection.TRANSACTION_NONE,
                "Dedicated nontransactional native metadata connection required");
        Map<String, Integer> before = counts(connection);
        require(before.get("JOURNALED_FILES") > 0 && before.get("JOURNALED_FILES") <= 50
                && before.get("JOURNALED_MEMBERS").equals(before.get("JOURNALED_FILES")) && before.get("JOURNALED_LIBRARIES") <= 50
                && COUNTS.subList(3, COUNTS.size()).stream().allMatch(field -> before.get(field) == 0), "Native scope is excessive or unsupported");
        // A fresh dedicated job owns this temporary file. Bound the native output
        // itself, not merely the later JDBC result, and never wait for an inquiry.
        for (String text : List.of("CHGJOB INQMSGRPY(*DFT)",
                "CRTDUPOBJ OBJ(QAWRKJRNA) FROMLIB(QSYS) OBJTYPE(*FILE) TOLIB(QTEMP) NEWOBJ(DCGSCOPE) DATA(*NO)",
                "CHGPF FILE(QTEMP/DCGSCOPE) SIZE(128 0 0)",
                "WRKJRNA JRN(" + journal.library() + "/" + journal.name()
                        + ") OUTPUT(*OUTFILE) OUTFILE(QTEMP/DCGSCOPE) OUTMBR(*FIRST *REPLACE) DETAIL(*OUTPUT)")) {
            check();
            try (var command = connection.prepareStatement("CALL QSYS2.QCMDEXC(?)")) {
                command.setQueryTimeout(timeout);
                command.setString(1, text);
                command.execute();
                require(command.getWarnings() == null && connection.getWarnings() == null, "Incomplete native scope command");
            }
        }
        var files = new ArrayList<As400OperatorTableEvidence.Request>();
        var libraries = new HashSet<String>();
        var objects = new HashSet<String>();
        try (var statement = connection.prepareStatement(FILES_SQL)) {
            statement.setQueryTimeout(timeout);
            statement.setMaxRows(129);
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    check();
                    require(objects.size() < 128, "Native journal scope exceeds observation bound");
                    exactJournal(rows);
                    require(text(rows, "ASP").equals("1") && text(rows, "JOURNALED_TYPE").equals("0"), "Unsupported native storage or journaling mode");
                    String type = text(rows, "OBJECT_TYPE"), library = text(rows, "OBJECT_LIBRARY"), object = text(rows, "OBJECT_NAME");
                    name(library);
                    name(object);
                    require(objects.add(library + "/" + object + "/" + type), "Duplicate native scope object");
                    if (type.equals("*FILE")) {
                        require(text(rows, "FILE_TYPE").equals("PF"), "Only physical data files can enter native clear scope");
                        files.add(new As400OperatorTableEvidence.Request(journal, library, object));
                    }
                    else if (type.equals("*LIB")) {
                        require(library.equals("QSYS") && libraries.add(object), "Ambiguous journaled library");
                    }
                    else {
                        throw new IllegalArgumentException("Unsupported journaled resource in native scope: " + type);
                    }
                    As400DcgNativeIo.noWarnings(rows, statement);
                }
                As400DcgNativeIo.noWarnings(rows, statement);
            }
        }
        require(!files.isEmpty() && files.size() <= 50 && before.equals(counts(connection)), "Empty, excessive or changed native journal scope");
        require(before.get("JOURNALED_FILES") == files.size() && before.get("JOURNALED_MEMBERS") == files.size()
                && before.get("JOURNALED_LIBRARIES") == libraries.size(), "Incomplete native file/member/library inventory");
        require(COUNTS.subList(3, COUNTS.size()).stream().allMatch(field -> before.get(field) == 0), "Unknown or unsupported journal resource");
        require(connection.getWarnings() == null, "Native scope connection warning");
        files.sort(java.util.Comparator.comparing(As400OperatorTableEvidence.Request::library).thenComparing(As400OperatorTableEvidence.Request::file));
        return new Scope(journal, files, libraries);
    }

    private Map<String, Integer> counts(Connection connection) throws SQLException {
        check();
        try (var statement = connection.prepareStatement(COUNTS_SQL)) {
            statement.setQueryTimeout(timeout);
            statement.setMaxRows(2);
            statement.setString(1, journal.library());
            statement.setString(2, journal.name());
            try (var rows = statement.executeQuery()) {
                require(rows.next(), "Missing independent journal scope counts");
                exactJournal(rows);
                require(text(rows, "JOURNAL_TYPE").equals("*LOCAL") && text(rows, "JOURNAL_STATE").equals("*ACTIVE"), "Unsupported journal scope");
                var result = new TreeMap<String, Integer>();
                for (String field : COUNTS) {
                    var count = rows.getBigDecimal(field);
                    require(count != null && count.signum() >= 0, "Missing or invalid native scope count");
                    result.put(field, count.intValueExact());
                }
                require(!rows.next(), "Ambiguous native journal scope");
                As400DcgNativeIo.noWarnings(rows, statement);
                return Map.copyOf(result);
            }
        }
    }

    private void exactJournal(ResultSet rows) throws SQLException {
        require(journal.library().equals(text(rows, "JOURNAL_LIBRARY")) && journal.name().equals(text(rows, "JOURNAL_NAME")), "Foreign native journal scope");
    }

    private static String text(ResultSet row, String field) throws SQLException {
        String value = row.getString(field);
        require(value != null && !value.isBlank(), "Missing native inventory field: " + field);
        return value.trim();
    }

    private static void name(String value) {
        require(value != null && value.matches("[A-Z#$@][A-Z0-9_#$@]{0,9}"), "Exact basic native scope name required");
    }

    private static void check() throws SQLException {
        if (Thread.currentThread().isInterrupted()) {
            throw new SQLException("Native scope discovery stopped");
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
