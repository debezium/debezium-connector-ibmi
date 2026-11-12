/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/** Current local-journal directory/settings observations for ordinary CDC, not protected startup proof. */
final class As400OperatorJournalEvidence {
    static final String JOURNAL_SQL = """
            SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_TYPE, JOURNAL_STATE, ASP_NUMBER, JOURNAL_ASPGRP,
                   ATTACHED_JOURNAL_RECEIVER_LIBRARY, ATTACHED_JOURNAL_RECEIVER_NAME,
                   REMOVE_INTERNAL_ENTRIES, REMOVE_FIXED_LENGTH_DETAIL, MINIMIZE_ESD_FOR_FILES, JOURNAL_CACHE
            FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?
            """;
    static final String RETAINED_JOURNAL_SQL = JOURNAL_SQL.replace("JOURNAL_CACHE", "JOURNAL_CACHE, DELETE_RECEIVER_OPTION");
    static final String RECEIVERS_SQL = """
            SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_RECEIVER_LIBRARY, JOURNAL_RECEIVER_NAME,
                   JOURNAL_RECEIVER_ASP_NUMBER, STATUS, FIRST_SEQUENCE_NUMBER, LAST_SEQUENCE_NUMBER,
                   PREVIOUS_JOURNAL_RECEIVER_LIBRARY, PREVIOUS_JOURNAL_RECEIVER,
                   NEXT_JOURNAL_RECEIVER_LIBRARY, NEXT_JOURNAL_RECEIVER,
                   MINIMIZE_ESD_FOR_FILES, FIXED_LENGTH_DATA_INCLUDES_JOB_NAME, FIXED_LENGTH_DATA_INCLUDES_THREAD_ID,
                   FIXED_LENGTH_DATA_INCLUDES_LOGICAL_UNIT_OF_WORK_ID, FIXED_LENGTH_DATA_INCLUDES_TRANSACTION_ID
            FROM QSYS2.JOURNAL_RECEIVER_INFO WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?
            """;

    record Receiver(JournalReceiver name, String status, BigInteger first, BigInteger last,
            Optional<JournalReceiver> previous, Optional<JournalReceiver> next,
            String minimizeFiles, String job, String thread, String luw, String xid) {
        Receiver {
            Objects.requireNonNull(name);
            Objects.requireNonNull(previous);
            Objects.requireNonNull(next);
        }

        void requireReadableProfile() {
            if (status == null || !List.of("ONLINE", "SAVED", "ATTACHED").contains(status) || first == null || last == null || first.signum() <= 0
                    || first.compareTo(last) > 0 || last.bitLength() > 64 || !"NO".equals(minimizeFiles)
                    || !"YES".equals(job) || !"YES".equals(thread) || !"NO".equals(luw) || !"NO".equals(xid)) {
                throw new IllegalArgumentException("Required receiver is unavailable or has an unsupported recording profile");
            }
        }
    }

    record Observation(JournalReceiver journal, JournalReceiver attached, List<Receiver> receivers) {
        Observation {
            receivers = List.copyOf(receivers);
        }
    }

    record Limits(int maximumReceivers, int queryTimeoutSeconds) {
        Limits {
            if (maximumReceivers < 1 || maximumReceivers == Integer.MAX_VALUE || queryTimeoutSeconds < 1 || queryTimeoutSeconds > 3600) {
                throw new IllegalArgumentException("Finite journal evidence limits required");
            }
        }
    }

    private final As400JdbcConnectionFactory connections;
    private final JournalReceiver journal;
    private final Limits limits;
    private final boolean currentOnly;
    private final boolean requireRetention;
    private final boolean nativeCount;

    As400OperatorJournalEvidence(As400JdbcConnectionFactory connections, JournalReceiver journal, Limits limits) {
        this(connections, journal, limits, false);
    }

    As400OperatorJournalEvidence(As400JdbcConnectionFactory connections, JournalReceiver journal, Limits limits, boolean currentOnly) {
        this(connections, journal, limits, currentOnly, false);
    }

    /** Task-owned observations additionally qualify retention on the same native query. */
    static As400OperatorJournalEvidence retained(As400JdbcConnectionFactory connections, JournalReceiver journal, Limits limits) {
        return new As400OperatorJournalEvidence(connections, journal, limits, true, true);
    }

    /** Complete native inventory bounds replace arbitrary connector receiver ceilings. */
    static As400OperatorJournalEvidence directory(As400JdbcConnectionFactory connections, JournalReceiver journal) {
        return new As400OperatorJournalEvidence(connections, journal, new Limits(Integer.MAX_VALUE - 1, 5), false, false, true);
    }

    static As400OperatorJournalEvidence retained(As400JdbcConnectionFactory connections, JournalReceiver journal) {
        return new As400OperatorJournalEvidence(connections, journal, new Limits(Integer.MAX_VALUE - 1, 5), true, true, true);
    }

    private As400OperatorJournalEvidence(As400JdbcConnectionFactory connections, JournalReceiver journal, Limits limits,
                                         boolean currentOnly, boolean requireRetention) {
        this(connections, journal, limits, currentOnly, requireRetention, false);
    }

    private As400OperatorJournalEvidence(As400JdbcConnectionFactory connections, JournalReceiver journal, Limits limits,
                                         boolean currentOnly, boolean requireRetention, boolean nativeCount) {
        this.requireRetention = requireRetention;
        this.nativeCount = nativeCount;
        this.currentOnly = currentOnly;
        this.connections = Objects.requireNonNull(connections);
        this.journal = names(journal.library(), journal.name());
        this.limits = Objects.requireNonNull(limits);
    }

    Observation collect() throws SQLException {
        return collect(null, currentOnly);
    }

    /** Exact linked traversal from the retained replay floor, independent of old directory size. */
    Observation collectFrom(JournalReceiver floor) throws SQLException {
        return collect(Objects.requireNonNull(floor), true);
    }

    private Observation collect(JournalReceiver floor, boolean linked) throws SQLException {
        checkInterrupted();
        try (Connection connection = connections.open()) {
            return collectOn(connection, floor, linked);
        }
    }

    /** The interval owner closes this connection after both fresh observations. */
    Observation collectOn(Connection connection) throws SQLException {
        return collectOn(connection, null, currentOnly);
    }

    Observation collectFromOn(Connection connection, JournalReceiver floor) throws SQLException {
        return collectOn(connection, Objects.requireNonNull(floor), true);
    }

    private Observation collectOn(Connection connection, JournalReceiver floor, boolean linked) throws SQLException {
        checkInterrupted();
        try {
            connection.setReadOnly(true);
            List<Directory> directory = attachment(connection);
            if (directory.size() != 1) {
                throw new SQLException("Journal is absent or ambiguous");
            }
            Directory observed = directory.get(0);
            JournalReceiver attached = observed.attached();
            int maximum = Math.min(limits.maximumReceivers(), observed.count());
            Decode<Receiver> decoder = row -> {
                exactJournal(row);
                systemBasicAsp(row, "JOURNAL_RECEIVER_ASP_NUMBER");
                return new Receiver(names(text(row, "JOURNAL_RECEIVER_LIBRARY"), text(row, "JOURNAL_RECEIVER_NAME")), text(row, "STATUS"),
                        sequence(row, "FIRST_SEQUENCE_NUMBER"), sequence(row, "LAST_SEQUENCE_NUMBER"),
                        optionalNames(row, "PREVIOUS_JOURNAL_RECEIVER_LIBRARY", "PREVIOUS_JOURNAL_RECEIVER"),
                        optionalNames(row, "NEXT_JOURNAL_RECEIVER_LIBRARY", "NEXT_JOURNAL_RECEIVER"),
                        text(row, "MINIMIZE_ESD_FOR_FILES"), text(row, "FIXED_LENGTH_DATA_INCLUDES_JOB_NAME"), text(row, "FIXED_LENGTH_DATA_INCLUDES_THREAD_ID"),
                        text(row, "FIXED_LENGTH_DATA_INCLUDES_LOGICAL_UNIT_OF_WORK_ID"), text(row, "FIXED_LENGTH_DATA_INCLUDES_TRANSACTION_ID"));
            };
            List<Receiver> receivers;
            if (!linked) {
                receivers = query(connection, RECEIVERS_SQL, maximum, decoder);
                if (nativeCount) {
                    var after = attachment(connection);
                    if (after.size() != 1 || !observed.equals(after.get(0))) {
                        throw new As400OperatorCutReader.UnstableCutException("Receiver directory count or attachment changed during observation");
                    }
                    if (receivers.size() != observed.count()) {
                        throw new SQLException("Incomplete native receiver directory");
                    }
                }
            }
            else {
                receivers = new ArrayList<>();
                JournalReceiver next = floor == null ? attached : floor;
                var visited = new java.util.HashSet<JournalReceiver>();
                while (true) {
                    if (!visited.add(next) || receivers.size() >= maximum) {
                        throw new SQLException("Required receiver chain is cyclic or exceeds the retained-chain bound");
                    }
                    JournalReceiver exact = next;
                    var found = query(connection, RECEIVERS_SQL + " AND JOURNAL_RECEIVER_LIBRARY = ? AND JOURNAL_RECEIVER_NAME = ?", 1,
                            decoder, exact);
                    if (found.size() != 1 || !found.get(0).name().equals(exact)) {
                        throw new SQLException("Required exact receiver is absent or ambiguous");
                    }
                    Receiver receiver = found.get(0);
                    receiver.requireReadableProfile();
                    if (!receivers.isEmpty()) {
                        Receiver previous = receivers.get(receivers.size() - 1);
                        if (!receiver.previous().equals(Optional.of(previous.name())) || receiver.first().compareTo(previous.last()) <= 0) {
                            throw new SQLException("Receiver links or sequence continuity differ");
                        }
                    }
                    receivers.add(receiver);
                    if (next.equals(attached)) {
                        if (!"ATTACHED".equals(receiver.status()) || receiver.next().isPresent()) {
                            // Retry only an observed attachment change. A static
                            // contradictory row remains a hard validation failure.
                            if (!"ATTACHED".equals(receiver.status()) && receiver.next().isPresent()) {
                                var current = attachment(connection);
                                if (current.size() == 1 && !current.get(0).attached().equals(attached)) {
                                    throw new As400OperatorCutReader.UnstableCutException("Attachment changed between journal and receiver observations");
                                }
                            }
                            throw new SQLException("Inconsistent current receiver");
                        }
                        break;
                    }
                    if ("ATTACHED".equals(receiver.status()) || receiver.next().isEmpty()) {
                        throw new SQLException("Required receiver chain cannot reach the current attachment");
                    }
                    next = receiver.next().orElseThrow();
                }
            }
            if (receivers.isEmpty() || receivers.stream().map(Receiver::name).distinct().count() != receivers.size()) {
                throw new SQLException("Receiver directory is absent or ambiguous");
            }
            check(connection);
            return new Observation(journal, attached, receivers);
        }
        catch (As400OperatorCutReader.UnstableCutException race) {
            throw race;
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new SQLException("Invalid ordinary journal evidence", e);
        }
    }

    private record Directory(JournalReceiver attached, int count) {
    }

    private List<Directory> attachment(Connection connection) throws SQLException {
        String sql = requireRetention ? RETAINED_JOURNAL_SQL : JOURNAL_SQL;
        if (nativeCount) {
            sql = sql.replace("JOURNAL_CACHE", "JOURNAL_CACHE, NUMBER_JOURNAL_RECEIVERS");
        }
        return query(connection, sql, 1, row -> {
            exactJournal(row);
            exact(row, "JOURNAL_TYPE", "*LOCAL");
            exact(row, "JOURNAL_STATE", "*ACTIVE");
            exact(row, "JOURNAL_ASPGRP", "*SYSBAS");
            systemBasicAsp(row, "ASP_NUMBER");
            exact(row, "REMOVE_INTERNAL_ENTRIES", "NO");
            exact(row, "REMOVE_FIXED_LENGTH_DETAIL", "NO");
            exact(row, "MINIMIZE_ESD_FOR_FILES", "NO");
            // The first ordinary profile rejects caching; a delay alone is not a durable-tail proof.
            exact(row, "JOURNAL_CACHE", "NO");
            // Observe retention on this same exact journal row. A second query
            // adds a native round trip without strengthening this observation.
            if (requireRetention) {
                exact(row, "DELETE_RECEIVER_OPTION", "NO");
            }
            BigInteger count = nativeCount ? sequence(row, "NUMBER_JOURNAL_RECEIVERS") : BigInteger.valueOf(limits.maximumReceivers());
            if (count == null || count.signum() <= 0 || count.compareTo(BigInteger.valueOf(Integer.MAX_VALUE - 1)) > 0) {
                throw new SQLException("Missing or invalid native receiver count");
            }
            return new Directory(names(text(row, "ATTACHED_JOURNAL_RECEIVER_LIBRARY"), text(row, "ATTACHED_JOURNAL_RECEIVER_NAME")), count.intValueExact());
        });
    }

    private <T> List<T> query(Connection connection, String sql, int maximum, Decode<T> decode, JournalReceiver... exact) throws SQLException {
        check(connection);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setQueryTimeout(limits.queryTimeoutSeconds());
            statement.setMaxRows(maximum + 1);
            statement.setFetchSize(Math.min(maximum + 1, 128));
            statement.setString(1, journal.library());
            statement.setString(2, journal.name());
            if (exact.length != 0) {
                statement.setString(3, exact[0].library());
                statement.setString(4, exact[0].name());
            }
            warning(statement.getWarnings());
            check(connection);
            List<T> result = new ArrayList<>();
            try (ResultSet rows = statement.executeQuery()) {
                warning(statement.getWarnings());
                warning(rows.getWarnings());
                while (rows.next()) {
                    checkInterrupted();
                    warning(rows.getWarnings());
                    if (result.size() >= maximum) {
                        throw new SQLException("Journal observation exceeds row bound");
                    }
                    result.add(decode.read(rows));
                    warning(rows.getWarnings());
                }
                warning(rows.getWarnings());
            }
            warning(statement.getWarnings());
            check(connection);
            return List.copyOf(result);
        }
    }

    private void exactJournal(ResultSet row) throws SQLException {
        exact(row, "JOURNAL_LIBRARY", journal.library());
        exact(row, "JOURNAL_NAME", journal.name());
    }

    private static void exact(ResultSet row, String key, String expected) throws SQLException {
        if (!expected.equals(text(row, key))) {
            throw new SQLException("Unsupported journal observation: " + key);
        }
    }

    private static void systemBasicAsp(ResultSet row, String key) throws SQLException {
        if (!systemBasicAsp(sequence(row, key))) {
            throw new SQLException("Journal evidence requires the system or a basic user ASP (1-32)");
        }
    }

    /** QSYS.LIB names are shared by the system ASP and basic user ASPs, not independent ASP groups. */
    static boolean systemBasicAsp(BigInteger number) {
        return number != null && number.signum() > 0 && number.compareTo(BigInteger.valueOf(32)) <= 0;
    }

    private static BigInteger sequence(ResultSet row, String key) throws SQLException {
        BigDecimal value = row.getBigDecimal(key);
        if (value == null) {
            return null;
        }
        BigInteger integer = value.toBigIntegerExact();
        if (integer.signum() < 0 || integer.bitLength() > 64) {
            throw new SQLException("Invalid unsigned journal field " + key);
        }
        return integer;
    }

    private static String text(ResultSet row, String key) throws SQLException {
        String value = row.getString(key);
        return value == null ? null : value.stripTrailing();
    }

    private static Optional<JournalReceiver> optionalNames(ResultSet row, String libraryKey, String nameKey) throws SQLException {
        String library = text(row, libraryKey);
        String name = text(row, nameKey);
        if (library == null && name == null) {
            return Optional.empty();
        }
        return Optional.of(names(library, name));
    }

    private static JournalReceiver names(String library, String name) {
        if (library == null || name == null || !library.matches("[A-Z0-9_#$@]{1,10}") || !name.matches("[A-Z0-9_#$@]{1,10}")) {
            throw new IllegalArgumentException("Exact basic journal/receiver names required");
        }
        return new JournalReceiver(name, library);
    }

    private static void check(Connection connection) throws SQLException {
        checkInterrupted();
        warning(connection.getWarnings());
        if (!connection.isReadOnly()) {
            throw new SQLException("Journal evidence connection is not read-only");
        }
    }

    private static void checkInterrupted() throws SQLException {
        if (Thread.currentThread().isInterrupted()) {
            throw new SQLException("Journal evidence collection interrupted");
        }
    }

    private static void warning(SQLWarning warning) throws SQLException {
        if (warning != null) {
            // IBM metadata views can enumerate objects outside the selected
            // journal. SQL0462/01548 can accompany a disappearing or locked
            // object, or insufficient authority. None of that observation is
            // usable: a caller may only retry the entire unpublished interval.
            if (warning.getErrorCode() == 462 && "01548".equals(warning.getSQLState()) && warning.getNextException() == null) {
                throw new IncompleteObservationException(warning);
            }
            throw new SQLException("Partial journal evidence rejected", warning);
        }
    }

    static final class IncompleteObservationException extends SQLException {
        IncompleteObservationException(SQLWarning warning) {
            super("Partial journal evidence rejected; complete recollection required", warning.getSQLState(), warning.getErrorCode(), warning);
        }
    }

    private interface Decode<T> {
        T read(ResultSet rows) throws SQLException;
    }
}
