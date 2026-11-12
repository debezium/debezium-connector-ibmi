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
                   TRIM(CAST(QJOLIBRARY AS CHAR(10) CCSID 37)) AS OBJECT_LIBRARY,
                   TRIM(CAST(QJOOBJECT AS CHAR(10) CCSID 37)) AS OBJECT_NAME,
                   TRIM(CAST(QJOOBJTYPE AS CHAR(7) CCSID 37)) AS OBJECT_TYPE,
                   TRIM(CAST(QJOFILETYP AS CHAR(2) CCSID 37)) AS FILE_TYPE,
                   CAST(QJOJRNTYPE AS CHAR(1) CCSID 37) AS JOURNALED_TYPE
            FROM QTEMP.DCGSCOPE
            """;
    static final String COUNTS_SQL = """
            SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_TYPE, JOURNAL_STATE, ASP_NUMBER, JOURNAL_ASPGRP,
                   JOURNALED_FILES, JOURNALED_MEMBERS, JOURNALED_LIBRARIES,
                   JOURNALED_DATA_AREAS, JOURNALED_DATA_QUEUES, JOURNALED_IFS_OBJECTS,
                   JOURNALED_ACCESS_PATHS, NUMBER_REMOTE_JOURNALS
            FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?
            """;
    private static final List<String> COUNTS = List.of("JOURNALED_FILES", "JOURNALED_MEMBERS", "JOURNALED_LIBRARIES", "JOURNALED_DATA_AREAS",
            "JOURNALED_DATA_QUEUES", "JOURNALED_IFS_OBJECTS", "JOURNALED_ACCESS_PATHS", "NUMBER_REMOTE_JOURNALS");

    record Scope(JournalReceiver journal, List<As400OperatorTableEvidence.Request> files, Set<String> journaledLibraries, int journaledMembers,
            Set<String> logicalFiles) {
        Scope(JournalReceiver journal, List<As400OperatorTableEvidence.Request> files, Set<String> journaledLibraries) {
            this(journal, files, journaledLibraries, files.size());
        }

        Scope(JournalReceiver journal, List<As400OperatorTableEvidence.Request> files, Set<String> journaledLibraries, int journaledMembers) {
            this(journal, files, journaledLibraries, journaledMembers, Set.of());
        }

        Scope {
            files = List.copyOf(files);
            journaledLibraries = Set.copyOf(journaledLibraries);
            logicalFiles = Set.copyOf(logicalFiles);
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
        require(before.get("JOURNALED_FILES") > 0, "Native journal has no data files");
        int expectedObjects = Math.addExact(before.get("JOURNALED_FILES"), before.get("JOURNALED_LIBRARIES"));
        // CHGPF accepts at most 2147483646 records. One extra row detects a
        // growing inventory; its independent counts, not a connector file cap,
        // bound both native temporary storage and this read.
        require(expectedObjects < Integer.MAX_VALUE - 1, "Native inventory exceeds physical outfile capacity");
        // A fresh dedicated job owns this temporary file. Restrict WRKJRNA's
        // output to files/libraries: *OUTPUT also repeats physical files for
        // journaled access paths, and includes non-row resources. Their presence
        // does not make the physical-file inventory incomplete. Independent file
        // and member counts, plus the surrounding journal audit, remain required.
        for (String text : List.of("CHGJOB INQMSGRPY(*DFT)",
                "CRTDUPOBJ OBJ(QAWRKJRNA) FROMLIB(QSYS) OBJTYPE(*FILE) TOLIB(QTEMP) NEWOBJ(DCGSCOPE) DATA(*NO)",
                "CHGPF FILE(QTEMP/DCGSCOPE) SIZE(" + (expectedObjects + 1) + " 0 0)",
                "WRKJRNA JRN(" + journal.library() + "/" + journal.name()
                        + ") OUTPUT(*OUTFILE) OUTFILE(QTEMP/DCGSCOPE) OUTMBR(*FIRST *REPLACE) DETAIL(*JRNFILE *JRNLIB)")) {
            check();
            try (var command = connection.prepareStatement("CALL QSYS2.QCMDEXC(?)")) {
                command.setQueryTimeout(timeout);
                command.setString(1, text);
                command.execute();
                require(command.getWarnings() == null && connection.getWarnings() == null, "Incomplete native scope command");
            }
        }
        var files = new ArrayList<As400OperatorTableEvidence.Request>();
        var logicalFiles = new HashSet<String>();
        var libraries = new HashSet<String>();
        var objects = new HashSet<String>();
        try (var statement = connection.prepareStatement(FILES_SQL)) {
            statement.setQueryTimeout(timeout);
            statement.setMaxRows(expectedObjects + 1);
            statement.setFetchSize(Math.min(128, expectedObjects + 1));
            try (var rows = statement.executeQuery()) {
                while (rows.next()) {
                    check();
                    require(objects.size() < expectedObjects, "Native journal scope grew beyond independent counts");
                    exactJournal(rows);
                    // Storage is established by JOURNAL_INFO and the subsequent
                    // exact file/member identity reads. QJOASP is a character
                    // device field, not the documented numeric ASP_NUMBER.
                    require(text(rows, "JOURNALED_TYPE").equals("0"), "Unsupported native journaling mode");
                    String type = text(rows, "OBJECT_TYPE"), library = text(rows, "OBJECT_LIBRARY"), object = text(rows, "OBJECT_NAME");
                    name(library);
                    name(object);
                    require(objects.add(library + "/" + object + "/" + type), "Duplicate native scope object");
                    if (type.equals("*FILE")) {
                        String fileType = text(rows, "FILE_TYPE");
                        require(fileType.equals("PF") || fileType.equals("LF"), "Unknown native file type");
                        if (fileType.equals("PF")) {
                            files.add(new As400OperatorTableEvidence.Request(journal, library, object));
                        }
                        else {
                            logicalFiles.add(library + "/" + object);
                        }
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
        require(!files.isEmpty() && before.equals(counts(connection)), "Empty or changed native journal scope");
        require(before.get("JOURNALED_FILES") == files.size() + logicalFiles.size() && before.get("JOURNALED_MEMBERS") >= files.size()
                && before.get("JOURNALED_LIBRARIES") == libraries.size(), "Incomplete native file/member/library inventory");
        require(connection.getWarnings() == null, "Native scope connection warning");
        files.sort(java.util.Comparator.comparing(As400OperatorTableEvidence.Request::library).thenComparing(As400OperatorTableEvidence.Request::file));
        return new Scope(journal, files, libraries, before.get("JOURNALED_MEMBERS"), logicalFiles);
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
                var asp = rows.getBigDecimal("ASP_NUMBER");
                require(asp != null && As400OperatorJournalEvidence.systemBasicAsp(asp.toBigIntegerExact())
                        && "*SYSBAS".equals(rows.getString("JOURNAL_ASPGRP")), "Unsupported native journal storage");
                result.put("ASP_NUMBER", asp.intValueExact());
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
