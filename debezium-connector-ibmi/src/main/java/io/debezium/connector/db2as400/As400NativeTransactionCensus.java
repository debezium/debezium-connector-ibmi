/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.HashSet;
import java.util.Objects;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/**
 * Whole-journal empty-cycle check with verified cross-user visibility. It is valid
 * only UNDER a held journal-wide writer fence. This collector neither creates that
 * fence nor treats equality, zero pending changes or RESET as a terminal outcome.
 */
final class As400NativeTransactionCensus {
    static final String AUTHORITY_SQL = """
            SELECT QSYS2.SQL_CHECK_SPECIAL_AUTHORITY('*JOBCTL') AS JOBCTL,
                   QSYS2.SQL_CHECK_FUNCTION_USAGE('QIBM_DB_SQLADM') AS SQLADM,
                   QSYS2.SQL_CHECK_FUNCTION_USAGE('QIBM_DB_SYSMON') AS SYSMON
            FROM SYSIBM.SYSDUMMY1
            """;
    static final String CENSUS_SQL = """
            SELECT D.LOCK_SPACE_ID, D.LOGICAL_UNIT_OF_WORK_STATE, D.ASPGRP,
                   D.RESOURCE_LOCATION, D.XA_TRANSACTION_BRANCH_STATE,
                   J.JOURNAL_LIBRARY, J.JOURNAL_NAME, J.COMMIT_CYCLE, J.PENDING_CHANGES
            FROM QSYS2.DB_TRANSACTION_INFO D LEFT JOIN LATERAL
              (SELECT JOURNAL_LIBRARY, JOURNAL_NAME, COMMIT_CYCLE, PENDING_CHANGES
               FROM TABLE(QSYS2.DB_TRANSACTION_JOURNAL_INFO(D.LOCK_SPACE_ID)) T
               WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?) J ON 1 = 1
            """;
    private final As400JdbcConnectionFactory connections;
    private final int maximumDefinitions;
    private final int timeoutSeconds;

    As400NativeTransactionCensus(As400JdbcConnectionFactory connections, int maximumDefinitions, int timeoutSeconds) {
        this.connections = Objects.requireNonNull(connections);
        if (maximumDefinitions < 1 || maximumDefinitions > 4096 || timeoutSeconds < 1 || timeoutSeconds > 3600) {
            throw new IllegalArgumentException("Finite transaction census bounds required");
        }
        this.maximumDefinitions = maximumDefinitions;
        this.timeoutSeconds = timeoutSeconds;
    }

    int requireEmpty(JournalReceiver journal) throws SQLException {
        Objects.requireNonNull(journal);
        require(!Thread.currentThread().isInterrupted(), "Transaction census stopped");
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            check(connection);
            authority(connection);
            var definitions = new HashSet<String>();
            try (PreparedStatement statement = connection.prepareStatement(CENSUS_SQL)) {
                statement.setQueryTimeout(timeoutSeconds);
                statement.setMaxRows(maximumDefinitions + 1);
                statement.setString(1, journal.library());
                statement.setString(2, journal.name());
                try (ResultSet rows = statement.executeQuery()) {
                    warning(statement.getWarnings());
                    warning(rows.getWarnings());
                    while (rows.next()) {
                        check(connection);
                        warning(rows.getWarnings());
                        String space = text(rows, "LOCK_SPACE_ID");
                        require(space.length() == 20 && definitions.size() < maximumDefinitions && definitions.add(space),
                                "Incomplete, duplicate or excessive commitment definition census");
                        String library = rows.getString("JOURNAL_LIBRARY");
                        String name = rows.getString("JOURNAL_NAME");
                        BigDecimal cycle = rows.getBigDecimal("COMMIT_CYCLE");
                        BigDecimal pending = rows.getBigDecimal("PENDING_CHANGES");
                        if (library == null && name == null) {
                            require(cycle == null && pending == null, "Partial journal resource observation");
                            continue;
                        }
                        require(journal.library().equals(library) && journal.name().equals(name), "Mismatched journal resource");
                        require(cycle == null && pending != null && pending.compareTo(BigDecimal.ZERO) == 0,
                                "Journal transaction is open or outcome is unresolved");
                        require(text(rows, "LOGICAL_UNIT_OF_WORK_STATE").equals("RESET") && text(rows, "ASPGRP").equals("*SYSBAS")
                                && text(rows, "RESOURCE_LOCATION").equals("LOCAL") && rows.getString("XA_TRANSACTION_BRANCH_STATE") == null,
                                "Unsupported distributed, prepared or unresolved commitment definition");
                    }
                    warning(rows.getWarnings());
                }
                warning(statement.getWarnings());
            }
            // Check effective authority on the same job after enumeration too.
            authority(connection);
            check(connection);
            return definitions.size();
        }
    }

    private void authority(Connection connection) throws SQLException {
        check(connection);
        try (PreparedStatement statement = connection.prepareStatement(AUTHORITY_SQL)) {
            statement.setQueryTimeout(timeoutSeconds);
            statement.setMaxRows(2);
            try (ResultSet rows = statement.executeQuery()) {
                warning(statement.getWarnings());
                warning(rows.getWarnings());
                require(rows.next(), "Missing effective census authority");
                boolean authorized = false;
                for (String name : new String[]{ "JOBCTL", "SQLADM", "SYSMON" }) {
                    BigDecimal value = rows.getBigDecimal(name);
                    require(BigDecimal.ZERO.equals(value) || BigDecimal.ONE.equals(value), "Unknown effective census authority");
                    authorized |= BigDecimal.ONE.equals(value);
                }
                require(authorized && !rows.next(), "Whole-system commitment visibility is unavailable");
                warning(rows.getWarnings());
            }
            warning(statement.getWarnings());
        }
    }

    private static String text(ResultSet rows, String field) throws SQLException {
        String value = rows.getString(field);
        require(value != null && !value.isBlank(), "Missing transaction field " + field);
        return value.stripTrailing();
    }

    private static void check(Connection connection) throws SQLException {
        require(!Thread.currentThread().isInterrupted() && connection.isReadOnly(), "Transaction census stopped or not read-only");
        warning(connection.getWarnings());
    }

    private static void warning(SQLWarning warning) throws SQLException {
        if (warning != null) {
            throw new SQLException("Partial transaction census rejected", warning);
        }
    }

    private static void require(boolean condition, String message) throws SQLException {
        if (!condition) {
            throw new SQLException(message);
        }
    }
}
