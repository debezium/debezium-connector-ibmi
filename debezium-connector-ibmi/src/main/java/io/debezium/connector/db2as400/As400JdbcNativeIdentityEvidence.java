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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded native object identity observations, NOT startup proof. There is no
 * Source/Inspection bridge, checkpoint token, retention lease or streaming hook.
 */
final class As400JdbcNativeIdentityEvidence {
    static final String API_PROFILE = "qsys2-ifs-object-statistics-candidate-v1";
    // Largest supported path: /QSYS.LIB/1234567890.LIB/1234567890.FILE/1234567890.MBR
    private static final int MAXIMUM_PATH_LENGTH = 55;
    private static final String NAME = "[A-Z0-9_#$@]{1,10}";
    static final String SQL = """
            SELECT CASE WHEN LENGTH(PATH_NAME) <= 55 THEN CAST(PATH_NAME AS VARCHAR(55) CCSID 1208) ELSE NULL END AS PATH_NAME,
                   LENGTH(PATH_NAME) AS PATH_LENGTH, OBJECT_TYPE, ASP_NUMBER, STORED_LOCAL,
                   CASE WHEN SYMBOLIC_LINK IS NULL THEN 0 ELSE 1 END AS HAS_SYMBOLIC_LINK,
                   FILE_SYSTEM_IDENTIFIER, FILE_IDENTIFIER_NUMBER, GENERATION_IDENTIFIER
            FROM TABLE(QSYS2.IFS_OBJECT_STATISTICS(
                START_PATH_NAME => ?, SUBTREE_DIRECTORIES => 'NO', OBJECT_TYPE_LIST => ?, IGNORE_ERRORS => 'NO')) AS O
            """;

    enum Kind {
        JOURNAL("*JRN", "JRN"),
        RECEIVER("*JRNRCV", "JRNRCV"),
        FILE("*FILE", "FILE"),
        MEMBER("*MBR", "MBR");

        private final String objectType;
        private final String suffix;

        Kind(String objectType, String suffix) {
            this.objectType = objectType;
            this.suffix = suffix;
        }

        String objectType() {
            return objectType;
        }
    }

    /** Explicit canonical names in QSYS.LIB, shared by the system and basic user ASPs. */
    record Target(String path, Kind kind) {
        Target {
            Objects.requireNonNull(path);
            Objects.requireNonNull(kind);
            String pattern = "/QSYS\\.LIB/" + NAME + "\\.LIB/"
                    + (kind == Kind.MEMBER ? NAME + "\\.FILE/" : "") + NAME + "\\." + kind.suffix;
            if (path.length() > MAXIMUM_PATH_LENGTH || !path.matches(pattern)) {
                throw new IllegalArgumentException("An exact canonical QSYS.LIB object path with basic system names is required");
            }
        }
    }

    /** Preserve the documented SQL BIGINT domain, including zero; no component is a wildcard. */
    record Identity(BigInteger fileSystem, BigInteger file, BigInteger generation) {
        Identity {
            for (BigInteger value : List.of(fileSystem, file, generation)) {
                value.longValueExact();
            }
        }
    }

    record Candidates(Map<Target, Identity> objects) {
        Candidates {
            objects = Map.copyOf(objects);
        }

        String apiProfile() {
            return API_PROFILE;
        }
    }

    /** Owned bytes between two matching observations; equality is NOT protection or source binding. */
    record ReadObservation(Candidates before, Candidates after, byte[] bytes) {
        ReadObservation {
            Objects.requireNonNull(before);
            Objects.requireNonNull(after);
            bytes = bytes.clone();
        }

        @Override
        public byte[] bytes() {
            return bytes.clone();
        }
    }

    record Limits(int maximumObjects, int queryTimeoutSeconds, int maximumReadBytes) {
        Limits {
            if (maximumObjects < 1 || queryTimeoutSeconds < 1 || queryTimeoutSeconds > 3600
                    || maximumReadBytes < 1 || maximumReadBytes > 16 * 1024 * 1024) {
                throw new IllegalArgumentException("Finite native evidence object, query-time and byte limits are required");
            }
        }
    }

    interface Read {
        /**
         * Read-only, bounded-time retrieval, at most maximumBytes; must not publish
         * or advance offsets. This collector cannot prove the reader's source binding.
         */
        byte[] read(int maximumBytes) throws SQLException;
    }

    private final As400JdbcConnectionFactory connections;
    private final Limits limits;

    As400JdbcNativeIdentityEvidence(As400JdbcConnectionFactory connections, Limits limits) {
        this.connections = Objects.requireNonNull(connections);
        this.limits = Objects.requireNonNull(limits);
    }

    Candidates collect(List<Target> requested) throws SQLException {
        List<Target> targets = targets(requested);
        interrupted();
        try (Connection connection = Objects.requireNonNull(connections.open())) {
            readOnly(connection);
            return collect(connection, targets);
        }
    }

    /** Borrow the caller's qualified interval connection; close only our statements and results. */
    Candidates collectOn(Connection connection, List<Target> requested) throws SQLException {
        List<Target> targets = targets(requested);
        interrupted();
        readOnly(Objects.requireNonNull(connection));
        return collect(connection, targets);
    }

    /** At most twice maximumObjects queries; nothing is returned on a mismatch or failed re-observation. */
    ReadObservation collectAroundRead(List<Target> requested, Read read) throws SQLException {
        List<Target> targets = targets(requested);
        Objects.requireNonNull(read);
        interrupted();
        try (Connection connection = Objects.requireNonNull(connections.open())) {
            readOnly(connection);
            Candidates before = collect(connection, targets);
            interrupted();
            byte[] borrowed = read.read(limits.maximumReadBytes());
            interrupted();
            if (borrowed == null || borrowed.length > limits.maximumReadBytes()) {
                throw new SQLException("Missing or oversized native evidence read");
            }
            byte[] owned = borrowed.clone();
            Candidates after = collect(connection, targets);
            if (!before.equals(after)) {
                throw new SQLException("Native identity changed during read; candidate bytes discarded");
            }
            return new ReadObservation(before, after, owned);
        }
    }

    private List<Target> targets(List<Target> requested) {
        if (requested.isEmpty() || requested.size() > limits.maximumObjects()) {
            throw new IllegalArgumentException("Distinct bounded native identity targets are required");
        }
        List<Target> result = List.copyOf(requested);
        if (result.isEmpty() || result.size() > limits.maximumObjects() || result.stream().distinct().count() != result.size()) {
            throw new IllegalArgumentException("Distinct bounded native identity targets are required");
        }
        return result;
    }

    private static void readOnly(Connection connection) throws SQLException {
        connection.setReadOnly(true);
        if (!connection.isReadOnly()) {
            throw new SQLException("Native evidence connection did not accept read-only mode");
        }
        noWarnings(connection.getWarnings());
    }

    private Candidates collect(Connection connection, List<Target> targets) throws SQLException {
        Map<Target, Identity> result = new LinkedHashMap<>();
        for (Target target : targets) {
            interrupted();
            // A warning from an earlier query/close must not disappear on the next execution.
            noWarnings(connection.getWarnings());
            if (!connection.isReadOnly()) {
                throw new SQLException("Native evidence connection is no longer read-only");
            }
            try (PreparedStatement statement = connection.prepareStatement(SQL)) {
                statement.setQueryTimeout(limits.queryTimeoutSeconds());
                statement.setMaxRows(2); // One candidate plus a duplicate/unexpected-row sentinel.
                statement.setString(1, target.path());
                statement.setString(2, target.kind().objectType());
                noWarnings(statement.getWarnings());
                noWarnings(connection.getWarnings());
                try (ResultSet rows = statement.executeQuery()) {
                    noWarnings(statement.getWarnings());
                    noWarnings(rows.getWarnings());
                    if (!rows.next()) {
                        throw new SQLException("Exact native identity object is absent or unavailable");
                    }
                    interrupted();
                    noWarnings(rows.getWarnings());
                    Identity identity = decode(rows, target);
                    // ResultSet.next() may clear warnings associated with the current row.
                    noWarnings(rows.getWarnings());
                    if (rows.next()) {
                        throw new SQLException("Exact native identity result is ambiguous");
                    }
                    noWarnings(rows.getWarnings());
                    noWarnings(statement.getWarnings());
                    result.put(target, identity);
                }
                noWarnings(statement.getWarnings());
            }
            noWarnings(connection.getWarnings());
            interrupted();
        }
        return new Candidates(result);
    }

    private static Identity decode(ResultSet row, Target target) throws SQLException {
        BigInteger pathLength = number(row, "PATH_LENGTH");
        String path = row.getString("PATH_NAME");
        if (!BigInteger.valueOf(target.path().length()).equals(pathLength) || !target.path().equals(path)
                || !target.kind().objectType().equals(row.getString("OBJECT_TYPE"))
                || !As400OperatorJournalEvidence.systemBasicAsp(number(row, "ASP_NUMBER")) || !"YES".equals(row.getString("STORED_LOCAL"))
                || !BigInteger.ZERO.equals(number(row, "HAS_SYMBOLIC_LINK"))) {
            throw new SQLException("Unexpected path, type, ASP, remote or symbolic native object");
        }
        return new Identity(number(row, "FILE_SYSTEM_IDENTIFIER"), number(row, "FILE_IDENTIFIER_NUMBER"), number(row, "GENERATION_IDENTIFIER"));
    }

    private static BigInteger number(ResultSet row, String column) throws SQLException {
        BigDecimal decimal = row.getBigDecimal(column);
        try {
            BigInteger value = Objects.requireNonNull(decimal).toBigIntegerExact();
            value.longValueExact();
            return value;
        }
        catch (ArithmeticException | NullPointerException e) {
            throw new SQLException("Invalid exact SQL integer " + column, e);
        }
    }

    private static void noWarnings(SQLWarning warning) throws SQLException {
        if (warning != null) {
            throw new SQLException("Native identity SQL warning; incomplete observations are rejected", warning.getSQLState(), warning.getErrorCode(), warning);
        }
    }

    private static void interrupted() throws SQLException {
        if (Thread.currentThread().isInterrupted()) {
            throw new SQLException("Native identity evidence collection interrupted");
        }
    }
}
