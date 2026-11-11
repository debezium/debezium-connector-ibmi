/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Native lower-bound discovery for retained transaction controls. A pending
 * receiver contains entries belonging to unfinished transactions. Terminals in
 * the surrounding raw interval also contribute their original cycle positions,
 * since a transaction can finish while the directory is sampled. This returns
 * neither a checkpoint nor startup authority. Callers must verify native object
 * identities, capture the complete bracket, and read/validate the discovered C/J
 * history before using its transaction inventory. Ordinary retention obligations
 * still apply; no permission to delete receivers is granted here.
 */
final class As400PendingReceiverEvidence {
    static final String DIRECTORY_SQL = """
            SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_TYPE, JOURNAL_STATE,
                   NUMBER_JOURNAL_RECEIVERS, ATTACHED_JOURNAL_RECEIVER_LIBRARY, ATTACHED_JOURNAL_RECEIVER_NAME,
                   JOURNAL_CACHE, REMOVE_INTERNAL_ENTRIES, REMOVE_FIXED_LENGTH_DETAIL
            FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?
            """;
    static final String RECEIVERS_SQL = """
            SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_RECEIVER_LIBRARY, JOURNAL_RECEIVER_NAME,
                   STATUS, FIRST_SEQUENCE_NUMBER, LAST_SEQUENCE_NUMBER,
                   PREVIOUS_JOURNAL_RECEIVER_LIBRARY, PREVIOUS_JOURNAL_RECEIVER,
                   NEXT_JOURNAL_RECEIVER_LIBRARY, NEXT_JOURNAL_RECEIVER, PENDING_TRANSACTIONS
            FROM QSYS2.JOURNAL_RECEIVER_INFO WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?
            """;

    record Receiver(JournalReceiver name, String status, BigInteger first, BigInteger last,
            Optional<JournalReceiver> previous, Optional<JournalReceiver> next, boolean pending) {
    }

    record Sample(JournalReceiver journal, JournalReceiver attached, List<Receiver> receivers) {
        Sample {
            receivers = List.copyOf(receivers);
            require(!receivers.isEmpty() && receivers.size() <= 1023, "Bounded complete receiver directory required");
            var names = new HashSet<JournalReceiver>();
            Receiver previous = null;
            for (Receiver receiver : receivers) {
                require(names.add(receiver.name()) && receiver.first().signum() > 0 && receiver.first().compareTo(receiver.last()) <= 0
                        && receiver.last().bitLength() <= 64 && List.of("ONLINE", "SAVED", "ATTACHED").contains(receiver.status()),
                        "Missing, repeated, unavailable or reset receiver");
                require(receiver.name().equals(attached) == receiver.status().equals("ATTACHED"), "Contradictory attachment");
                if (previous != null) {
                    require(receiver.previous().equals(Optional.of(previous.name())) && previous.next().equals(Optional.of(receiver.name()))
                            && previous.last().compareTo(receiver.first()) < 0, "Incomplete reciprocal receiver links or sequence reset");
                }
                previous = receiver;
            }
            require(previous.name().equals(attached) && previous.next().isEmpty(), "Directory must end at the attached receiver");
        }

        /** Join to an independently observed complete native directory, not names alone. */
        void verifyDirectory(As400OperatorJournalEvidence.Observation directory) {
            require(journal.equals(directory.journal()) && attached.equals(directory.attached())
                    && receivers.size() == directory.receivers().size(), "Pending and native directories differ");
            var ordered = directory.receivers().stream().sorted(Comparator.comparing(As400OperatorJournalEvidence.Receiver::first)).toList();
            for (int i = 0; i < receivers.size(); i++) {
                var pending = receivers.get(i);
                var nativeReceiver = ordered.get(i);
                require(pending.name().equals(nativeReceiver.name()) && pending.first().equals(nativeReceiver.first())
                        && pending.previous().equals(nativeReceiver.previous()) && pending.next().equals(nativeReceiver.next())
                        && As400NativeJournalWindow.sameReadableStatus(pending.status(), nativeReceiver.status())
                        && (pending.name().equals(attached) || pending.last().equals(nativeReceiver.last())), "Native pending receiver identity/range differs");
            }
        }

        void verifyWindow(As400OperatorJournalEvidence.Observation before, As400OperatorJournalEvidence.Observation after) {
            verifyDirectory(before);
            verifyDirectory(after);
            BigInteger sampled = receivers.get(receivers.size() - 1).last();
            BigInteger lower = before.receivers().stream().filter(r -> r.name().equals(attached)).findFirst().orElseThrow().last();
            BigInteger upper = after.receivers().stream().filter(r -> r.name().equals(attached)).findFirst().orElseThrow().last();
            require(lower.compareTo(sampled) <= 0 && sampled.compareTo(upper) <= 0, "Pending sample lies outside native directory observations");
        }

        JournalPosition floor(List<RawJournalPage.Entry> completeBracket) {
            require(!completeBracket.isEmpty() && completeBracket.size() <= 20_000, "Bounded complete raw observation bracket required");
            BigInteger floor = completeBracket.get(0).position().offset();
            for (Receiver receiver : receivers) {
                if (receiver.pending()) {
                    floor = floor.min(receiver.first());
                }
            }
            var headers = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID);
            BigInteger previous = null;
            long bytes = 0;
            for (var entry : completeBracket) {
                byte[] data = entry.data();
                require((bytes += data.length) <= 64 * 1024 * 1024, "Observation bracket exceeds byte budget");
                var position = entry.position();
                Receiver receiver = receivers.stream().filter(r -> r.name().equals(position.receiver())).findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Bracket receiver absent from pending directory"));
                require(receiver.first().compareTo(position.offset()) <= 0
                        && (receiver.name().equals(attached) || position.offset().compareTo(receiver.last()) <= 0)
                        && (previous == null || previous.compareTo(position.offset()) < 0), "Unordered or out-of-range observation bracket");
                previous = position.offset();
                var header = headers.decode(data, 0);
                var source = As400SourceMetadata.from(header, position);
                require(header.getJournalCode() != 'J' && !(header.getJournalCode() == 'C'
                        && List.of("BA", "CN").contains(header.getEntryType())), "Maintenance or abnormal recovery overlaps discovery");
                BigInteger cycle = source.commitCycle();
                require(cycle.signum() >= 0 && cycle.compareTo(position.offset()) <= 0, "Invalid original cycle coordinate");
                if (cycle.signum() > 0) {
                    floor = floor.min(cycle);
                }
            }
            BigInteger required = floor;
            var owners = receivers.stream().filter(r -> r.first().compareTo(required) <= 0 && required.compareTo(r.last()) <= 0).toList();
            require(owners.size() == 1, "Required original transaction history is unavailable");
            return new JournalPosition(required, owners.get(0).name());
        }
    }

    private record Directory(JournalReceiver attached, int count) {
    }

    private final As400JdbcConnectionFactory connections;
    private final JournalReceiver journal;
    private final int maximumReceivers;
    private final int timeout;

    As400PendingReceiverEvidence(As400JdbcConnectionFactory connections, JournalReceiver journal, int maximumReceivers, int timeout) {
        this.connections = Objects.requireNonNull(connections);
        this.journal = name(journal.library(), journal.name());
        require(maximumReceivers > 0 && maximumReceivers <= 1023 && timeout > 0 && timeout <= 60, "Finite receiver discovery bounds required");
        this.maximumReceivers = maximumReceivers;
        this.timeout = timeout;
    }

    Sample collect() throws SQLException {
        check();
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            Directory before = directory(connection);
            var receivers = new ArrayList<Receiver>();
            try (var statement = connection.prepareStatement(RECEIVERS_SQL)) {
                statement.setQueryTimeout(timeout);
                statement.setMaxRows(maximumReceivers + 1);
                statement.setString(1, journal.library());
                statement.setString(2, journal.name());
                try (var rows = statement.executeQuery()) {
                    while (rows.next()) {
                        check();
                        require(receivers.size() < maximumReceivers, "Receiver directory exceeds bound");
                        exactJournal(rows);
                        String pending = text(rows, "PENDING_TRANSACTIONS");
                        require(List.of("YES", "NO").contains(pending), "Unknown pending transaction state");
                        receivers.add(new Receiver(name(text(rows, "JOURNAL_RECEIVER_LIBRARY"), text(rows, "JOURNAL_RECEIVER_NAME")),
                                text(rows, "STATUS"), number(rows, "FIRST_SEQUENCE_NUMBER"), number(rows, "LAST_SEQUENCE_NUMBER"),
                                link(rows, "PREVIOUS_JOURNAL_RECEIVER_LIBRARY", "PREVIOUS_JOURNAL_RECEIVER"),
                                link(rows, "NEXT_JOURNAL_RECEIVER_LIBRARY", "NEXT_JOURNAL_RECEIVER"), pending.equals("YES")));
                        As400DcgNativeIo.noWarnings(rows, statement);
                    }
                    As400DcgNativeIo.noWarnings(rows, statement);
                }
            }
            require(before.equals(directory(connection)) && before.count() == receivers.size(), "Incomplete or changed native receiver directory");
            require(connection.getWarnings() == null, "Native receiver connection warning");
            // Sorting this SQL service can require an expensive materialized
            // plan. Own and bound every row first, then order the small directory.
            receivers.sort(Comparator.comparing(Receiver::first));
            return new Sample(journal, before.attached(), receivers);
        }
        catch (IllegalArgumentException | ArithmeticException error) {
            throw new SQLException("Cannot discover complete retained transaction history", error);
        }
    }

    private Directory directory(Connection connection) throws SQLException {
        check();
        try (var statement = connection.prepareStatement(DIRECTORY_SQL)) {
            statement.setQueryTimeout(timeout);
            statement.setMaxRows(2);
            statement.setString(1, journal.library());
            statement.setString(2, journal.name());
            try (var rows = statement.executeQuery()) {
                require(rows.next(), "Missing journal directory");
                exactJournal(rows);
                require(text(rows, "JOURNAL_TYPE").equals("*LOCAL") && text(rows, "JOURNAL_STATE").equals("*ACTIVE"), "Unsupported journal");
                for (String field : List.of("JOURNAL_CACHE", "REMOVE_INTERNAL_ENTRIES", "REMOVE_FIXED_LENGTH_DETAIL")) {
                    require(text(rows, field).equals("NO"), "Cached or filtered journal cannot establish control history");
                }
                int count = number(rows, "NUMBER_JOURNAL_RECEIVERS").intValueExact();
                require(count > 0 && count <= maximumReceivers, "Unavailable or excessive receiver count");
                var result = new Directory(name(text(rows, "ATTACHED_JOURNAL_RECEIVER_LIBRARY"), text(rows, "ATTACHED_JOURNAL_RECEIVER_NAME")), count);
                require(!rows.next(), "Ambiguous journal directory");
                As400DcgNativeIo.noWarnings(rows, statement);
                return result;
            }
        }
    }

    private void exactJournal(ResultSet row) throws SQLException {
        require(journal.equals(name(text(row, "JOURNAL_LIBRARY"), text(row, "JOURNAL_NAME"))), "Foreign journal directory");
    }

    private static Optional<JournalReceiver> link(ResultSet row, String library, String name) throws SQLException {
        String lib = row.getString(library), object = row.getString(name);
        require((lib == null) == (object == null), "Partial receiver link");
        return lib == null ? Optional.empty() : Optional.of(name(lib.trim(), object.trim()));
    }

    private static String text(ResultSet row, String field) throws SQLException {
        String value = row.getString(field);
        require(value != null && !value.isBlank(), "Missing native directory field: " + field);
        return value.trim();
    }

    private static BigInteger number(ResultSet row, String field) throws SQLException {
        var value = row.getBigDecimal(field);
        require(value != null, "Missing native directory number: " + field);
        BigInteger number = value.toBigIntegerExact();
        require(number.signum() >= 0 && number.bitLength() <= 64, "Invalid native directory number: " + field);
        return number;
    }

    private static JournalReceiver name(String library, String name) {
        require(library != null && name != null && library.matches("[A-Z#$@][A-Z0-9_#$@]{0,9}") && name.matches("[A-Z#$@][A-Z0-9_#$@]{0,9}"),
                "Exact basic native names required");
        return new JournalReceiver(name, library);
    }

    private static void check() throws SQLException {
        if (Thread.currentThread().isInterrupted()) {
            throw new SQLException("Receiver discovery interrupted");
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
