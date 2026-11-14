/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.Connection;
import java.sql.SQLException;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/** Exact current resource inventory and held file locks for the operator-managed first start. */
final class As400DcgScope {
    static final String COUNTS = "SELECT JOURNALED_FILES, JOURNALED_MEMBERS, JOURNALED_DATA_AREAS, JOURNALED_DATA_QUEUES,"
            + " JOURNALED_IFS_OBJECTS, JOURNALED_ACCESS_PATHS, JOURNALED_LIBRARIES, NUMBER_REMOTE_JOURNALS FROM QSYS2.JOURNAL_INFO"
            + " WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?";
    static final String OBJECT = "SELECT OBJNAME, OBJTYPE, JOURNALED, JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_IMAGES"
            + " FROM TABLE(QSYS2.OBJECT_STATISTICS(?, ?, ?)) X";
    static final String MEMBERS = "SELECT NATIVE_TYPE, FILE_TYPE, NUMBER_MEMBERS FROM QSYS2.SYSFILES WHERE SYSTEM_TABLE_SCHEMA = ? AND SYSTEM_TABLE_NAME = ?";
    static final String LOCKS = "SELECT LOCK_STATE, LOCK_STATUS FROM TABLE(QSYS2.OBJECT_LOCK_INFO(?, ?, '*FILE', 0)) X WHERE JOB_NAME = ?";
    private final As400DcgConfig config;

    As400DcgScope(As400DcgConfig config) {
        this.config = config;
    }

    /** Selected row streams require both update images before the irreversible handoff. */
    static void requireImages(Connection connection, JournalReceiver journal, java.util.List<As400BindingDefinition> captured) throws SQLException {
        connection.setReadOnly(true);
        requireImage(connection.isReadOnly(), "Image validation requires a read-only connection");
        for (var definition : captured) {
            requireImage(!Thread.currentThread().isInterrupted(), "Image validation interrupted");
            As400OperatorJournalEvidence.warning(connection.getWarnings());
            try (var statement = connection.prepareStatement(OBJECT)) {
                statement.setQueryTimeout(5);
                statement.setMaxRows(2);
                statement.setString(1, definition.identity().tableId().schema());
                statement.setString(2, "*FILE");
                statement.setString(3, definition.identity().physicalFile());
                try (var rows = statement.executeQuery()) {
                    requireImage(rows.next(), "Captured native file is unavailable");
                    requireImage(definition.identity().physicalFile().equals(rows.getString(1).trim()) && "*FILE".equals(rows.getString(2))
                            && "YES".equals(rows.getString(3)) && journal.library().equals(rows.getString(4)) && journal.name().equals(rows.getString(5)),
                            "Captured native file journaling differs");
                    if (!"*BOTH".equals(rows.getString(6))) {
                        throw new As400NativeEvidenceException("Captured file " + definition.schema().id() + " requires IMAGES(*BOTH) before truncate enablement");
                    }
                    requireImage(!rows.next(), "Ambiguous captured native file");
                    As400DcgNativeIo.noWarnings(rows, statement);
                }
            }
            As400OperatorJournalEvidence.warning(connection.getWarnings());
            requireImage(connection.isReadOnly(), "Image validation connection changed");
        }
    }

    private static void requireImage(boolean condition, String reason) throws As400NativeEvidenceException {
        if (!condition) {
            throw new As400NativeEvidenceException(reason);
        }
    }

    void validate(Connection c) throws SQLException {
        counts(c);
        for (String table : config.controlled) {
            String[] parts = table.split("\\.");
            object(c, parts[0], "*FILE", parts[1]);
            try (var st = c.prepareStatement(MEMBERS)) {
                st.setQueryTimeout(5);
                st.setMaxRows(2);
                st.setString(1, parts[0]);
                st.setString(2, parts[1]);
                try (var rows = st.executeQuery()) {
                    require(rows.next(), "Controlled file is unavailable");
                    require("PHYSICAL".equals(rows.getString(1)) && "DATA".equals(rows.getString(2)) && rows.getInt(3) == 1 && !rows.wasNull(),
                            "Controlled journal files must be single-member physical data files");
                    require(!rows.next(), "Ambiguous controlled file");
                    As400DcgNativeIo.noWarnings(rows, st);
                }
            }
        }
        for (String library : config.journaledLibraries) {
            object(c, "QSYS", "*LIB", library);
        }
        counts(c);
    }

    private void object(Connection c, String library, String type, String name) throws SQLException {
        try (var st = c.prepareStatement(OBJECT)) {
            st.setQueryTimeout(5);
            st.setMaxRows(2);
            st.setString(1, library);
            st.setString(2, type);
            st.setString(3, name);
            try (var rows = st.executeQuery()) {
                require(rows.next(), "Controlled native object is unavailable");
                require(name.equals(rows.getString(1).trim()) && type.equals(rows.getString(2)) && "YES".equals(rows.getString(3))
                        && config.journal.library().equals(rows.getString(4)) && config.journal.name().equals(rows.getString(5)),
                        "Controlled native object journaling differs");
                require(!type.equals("*FILE") || "*BOTH".equals(rows.getString(6)), "Both journal images required");
                require(!rows.next(), "Ambiguous controlled native object");
                As400DcgNativeIo.noWarnings(rows, st);
            }
        }
    }

    private void counts(Connection c) throws SQLException {
        try (var st = c.prepareStatement(COUNTS)) {
            st.setQueryTimeout(5);
            st.setMaxRows(2);
            st.setString(1, config.journal.library());
            st.setString(2, config.journal.name());
            try (var rows = st.executeQuery()) {
                require(rows.next(), "Journal inventory is unavailable");
                for (int i = 1; i <= 8; i++) {
                    int expected = i <= 2 ? config.controlled.size() : i == 7 ? config.journaledLibraries.size() : 0;
                    require(rows.getInt(i) == expected && !rows.wasNull(), "Unknown or unsupported resource in controlled journal");
                }
                require(!rows.next(), "Ambiguous journal inventory");
                As400DcgNativeIo.noWarnings(rows, st);
            }
        }
    }

    void lock(Connection c) throws SQLException {
        for (String table : config.controlled) {
            String[] parts = table.split("\\.");
            try (var st = c.createStatement()) {
                st.setQueryTimeout(5);
                st.execute("LOCK TABLE \"" + parts[0] + "\".\"" + parts[1] + "\" IN EXCLUSIVE MODE ALLOW READ");
                require(st.getWarnings() == null, "Native lock warning");
            }
        }
        verifyLocks(c);
    }

    void verifyLocks(Connection c) throws SQLException {
        String job;
        try (var st = c.createStatement()) {
            st.setQueryTimeout(5);
            st.setMaxRows(2);
            try (var rows = st.executeQuery("SELECT QSYS2.JOB_NAME FROM SYSIBM.SYSDUMMY1")) {
                require(rows.next(), "Native session job is unavailable");
                job = rows.getString(1);
                require(job != null && !job.isBlank() && !rows.next(), "Ambiguous native session job");
                As400DcgNativeIo.noWarnings(rows, st);
            }
        }
        for (String table : config.controlled) {
            String[] parts = table.split("\\.");
            try (var st = c.prepareStatement(LOCKS)) {
                st.setQueryTimeout(5);
                st.setMaxRows(21);
                st.setString(1, parts[0]);
                st.setString(2, parts[1]);
                st.setString(3, job);
                boolean held = false;
                int count = 0;
                try (var rows = st.executeQuery()) {
                    while (rows.next()) {
                        require(++count <= 20, "Native lock evidence exceeds bound");
                        held |= "*EXCLRD".equals(rows.getString(1).trim()) && "HELD".equals(rows.getString(2).trim());
                    }
                    require(held, "Required native writer lock is not held for " + table);
                    As400DcgNativeIo.noWarnings(rows, st);
                }
            }
        }
    }

    private static void require(boolean valid, String error) throws SQLException {
        if (!valid) {
            throw new SQLException(error);
        }
    }
}
