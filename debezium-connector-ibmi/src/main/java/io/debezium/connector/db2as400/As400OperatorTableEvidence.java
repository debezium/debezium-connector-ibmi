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

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.relational.TableId;

/**
 * Current paired object/journal and physical-format observations for the operator
 * retention profile. These are NOT protected startup proof or historical schema.
 * Caller must pin them in the checkpoint fingerprint and reject unsupported history
 * before publication. No bridge to the strict Source/Inspection contract is provided.
 */
final class As400OperatorTableEvidence {
    static final String OBJECT_SQL = """
            SELECT CASE WHEN LENGTH(PATH_NAME) <= 55 THEN CAST(PATH_NAME AS VARCHAR(55) CCSID 1208) ELSE NULL END AS PATH_NAME,
                   LENGTH(PATH_NAME) AS PATH_LENGTH, OBJECT_TYPE, ASP_NUMBER, STORED_LOCAL,
                   CASE WHEN SYMBOLIC_LINK IS NULL THEN 0 ELSE 1 END AS HAS_SYMBOLIC_LINK,
                   FILE_SYSTEM_IDENTIFIER, FILE_IDENTIFIER_NUMBER, GENERATION_IDENTIFIER,
                   JOURNALED, JOURNAL_LIBRARY, JOURNAL_NAME, HEX(JOURNAL_IDENTIFIER) AS OBJECT_JID
            FROM TABLE(QSYS2.IFS_OBJECT_STATISTICS(
                START_PATH_NAME => ?, SUBTREE_DIRECTORIES => 'NO', OBJECT_TYPE_LIST => ?, IGNORE_ERRORS => 'NO')) AS O
            """;
    static final String FORMAT_SQL = """
            SELECT SYSTEM_TABLE_SCHEMA, SYSTEM_TABLE_NAME, NATIVE_TYPE, FILE_TYPE,
                   NUMBER_MEMBERS, NUMBER_RECORD_FORMATS, FORMAT_LEVEL_ID, FORMAT_NAME, RECORD_LENGTH, NUMBER_FIELDS,
                   CONTAINS_LOB, CONTAINS_VARYING_LENGTH
            FROM QSYS2.SYSFILES WHERE SYSTEM_TABLE_SCHEMA = ? AND SYSTEM_TABLE_NAME = ?
            """;
    static final String MEMBER_SQL = """
            SELECT SYSTEM_TABLE_SCHEMA, SYSTEM_TABLE_NAME, SYSTEM_TABLE_MEMBER, TABLE_SCHEMA, TABLE_NAME
            FROM QSYS2.SYSPARTITIONSTAT WHERE SYSTEM_TABLE_SCHEMA = ? AND SYSTEM_TABLE_NAME = ?
            """;

    record Request(JournalReceiver journal, String library, String file) {
        Request {
            Objects.requireNonNull(journal);
            for (String name : List.of(journal.library(), journal.name(), library, file)) {
                if (!name.matches("[A-Z0-9_#$@]{1,10}")) {
                    throw new IllegalArgumentException("Exact basic system names are required");
                }
            }
        }
    }

    record NativeObject(Target target, Identity identity, String jid) {
        NativeObject {
            Objects.requireNonNull(target);
            Objects.requireNonNull(identity);
            if (!TableIdentity.validJournalId(jid)) {
                throw new IllegalArgumentException("Missing or invalid journal identifier");
            }
        }
    }

    record Format(String name, String level, int recordBytes, int fields, boolean varying) {
        Format {
            if (name == null || !name.matches("[A-Z0-9_#$@]{1,10}") || level == null || !level.matches("[0-9A-F]{13}")
                    || recordBytes < 1 || recordBytes > 16 * 1024 * 1024 || fields < 1 || fields > 4096) {
                throw new IllegalArgumentException("Missing or unsupported physical format");
            }
        }
    }

    record Observation(Request request, TableId table, String member, NativeObject fileObject, NativeObject memberObject, Format format) {
        Observation {
            Objects.requireNonNull(request);
            Objects.requireNonNull(table);
            Objects.requireNonNull(format);
            if (fileObject.jid().equals(memberObject.jid()) || fileObject.identity().equals(memberObject.identity())) {
                throw new IllegalArgumentException("File and member must be distinct native objects");
            }
        }

        TableIdentity tableIdentity() {
            // Both the native format level and byte/field shape are pinned. The decoder
            // policy must additionally fingerprint codec settings and column mapping.
            String layout = format.name() + ":" + format.level() + ":" + format.recordBytes() + ":" + format.fields() + ":" + format.varying();
            return new TableIdentity(table, request.file(), member, fileObject.jid(), memberObject.jid(), layout);
        }
    }

    record Limits(int maximumTables, int queryTimeoutSeconds) {
        Limits {
            if (maximumTables < 1 || maximumTables > 1024 || queryTimeoutSeconds < 1 || queryTimeoutSeconds > 3600) {
                throw new IllegalArgumentException("Finite table and query-time bounds required");
            }
        }
    }

    private final As400JdbcConnectionFactory connections;
    private final Limits limits;

    As400OperatorTableEvidence(As400JdbcConnectionFactory connections, Limits limits) {
        this.connections = Objects.requireNonNull(connections);
        this.limits = Objects.requireNonNull(limits);
    }

    /** Four queries per table, with a duplicate-row sentinel. All connections are owned and closed. */
    List<Observation> collect(List<Request> requested) throws SQLException {
        List<Request> requests = List.copyOf(requested);
        if (requests.isEmpty() || requests.size() > limits.maximumTables() || requests.stream().distinct().count() != requests.size()) {
            throw new IllegalArgumentException("Distinct bounded table requests required");
        }
        checkInterrupted();
        try (Connection connection = Objects.requireNonNull(connections.open())) {
            connection.setReadOnly(true);
            check(connection);
            List<Observation> result = new ArrayList<>();
            for (Request request : requests) {
                String member = one(connection, MEMBER_SQL, List.of(request.library(), request.file()), row -> {
                    exact(row, "SYSTEM_TABLE_SCHEMA", request.library());
                    exact(row, "SYSTEM_TABLE_NAME", request.file());
                    // First profile does not guess SQL alias/system-name mappings.
                    exact(row, "TABLE_SCHEMA", request.library());
                    exact(row, "TABLE_NAME", request.file());
                    String value = row.getString("SYSTEM_TABLE_MEMBER");
                    if (value == null || !value.trim().matches("[A-Z0-9_#$@]{1,10}")) {
                        throw new As400NativeEvidenceException("Invalid single-member name");
                    }
                    return value.trim();
                });
                Format format = one(connection, FORMAT_SQL, List.of(request.library(), request.file()), row -> {
                    exact(row, "SYSTEM_TABLE_SCHEMA", request.library());
                    exact(row, "SYSTEM_TABLE_NAME", request.file());
                    exact(row, "NATIVE_TYPE", "PHYSICAL");
                    exact(row, "FILE_TYPE", "DATA");
                    exact(row, "CONTAINS_LOB", "NO");
                    if (!number(row, "NUMBER_MEMBERS").equals(BigInteger.ONE) || !number(row, "NUMBER_RECORD_FORMATS").equals(BigInteger.ONE)) {
                        throw new As400NativeEvidenceException("Exactly one member and record format required");
                    }
                    String varying = row.getString("CONTAINS_VARYING_LENGTH");
                    if (!"YES".equals(varying) && !"NO".equals(varying)) {
                        throw new As400NativeEvidenceException("Unknown varying-length layout");
                    }
                    return new Format(trim(row, "FORMAT_NAME"), trim(row, "FORMAT_LEVEL_ID"), number(row, "RECORD_LENGTH").intValueExact(),
                            number(row, "NUMBER_FIELDS").intValueExact(), varying.equals("YES"));
                });
                String path = "/QSYS.LIB/" + request.library() + ".LIB/" + request.file() + ".FILE";
                NativeObject file = object(connection, request, new Target(path, Kind.FILE));
                NativeObject data = object(connection, request, new Target(path + "/" + member + ".MBR", Kind.MEMBER));
                result.add(new Observation(request, new TableId(null, request.library(), request.file()), member, file, data, format));
            }
            check(connection);
            return List.copyOf(result);
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new As400NativeEvidenceException("Invalid native table observation", e);
        }
    }

    private NativeObject object(Connection connection, Request request, Target target) throws SQLException {
        return one(connection, OBJECT_SQL, List.of(target.path(), target.kind().objectType()), row -> {
            exact(row, "PATH_NAME", target.path());
            exact(row, "OBJECT_TYPE", target.kind().objectType());
            exact(row, "STORED_LOCAL", "YES");
            exact(row, "JOURNALED", "YES");
            exact(row, "JOURNAL_LIBRARY", request.journal().library());
            exact(row, "JOURNAL_NAME", request.journal().name());
            if (!number(row, "PATH_LENGTH").equals(BigInteger.valueOf(target.path().length())) || !number(row, "ASP_NUMBER").equals(BigInteger.ONE)
                    || !number(row, "HAS_SYMBOLIC_LINK").equals(BigInteger.ZERO)) {
                throw new As400NativeEvidenceException("Unexpected path, ASP or symbolic object");
            }
            String jid = row.getString("OBJECT_JID");
            if (jid == null || !jid.matches("[0-9A-Fa-f]{20}")) {
                throw new As400NativeEvidenceException("Missing exact native journal identifier");
            }
            return new NativeObject(target, new Identity(number(row, "FILE_SYSTEM_IDENTIFIER"), number(row, "FILE_IDENTIFIER_NUMBER"),
                    number(row, "GENERATION_IDENTIFIER")), jid.toLowerCase(java.util.Locale.ROOT));
        });
    }

    private <T> T one(Connection connection, String sql, List<String> arguments, Decode<T> decode) throws SQLException {
        check(connection);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setQueryTimeout(limits.queryTimeoutSeconds());
            statement.setMaxRows(2);
            for (int i = 0; i < arguments.size(); i++) {
                statement.setString(i + 1, arguments.get(i));
            }
            warning(statement.getWarnings());
            check(connection);
            T result;
            try (ResultSet rows = statement.executeQuery()) {
                warning(statement.getWarnings());
                warning(rows.getWarnings());
                if (!rows.next()) {
                    throw new As400NativeEvidenceException("Required native table observation is absent");
                }
                warning(rows.getWarnings());
                checkInterrupted();
                result = decode.read(rows);
                warning(rows.getWarnings());
                if (rows.next()) {
                    throw new As400NativeEvidenceException("Native table observation is ambiguous");
                }
                warning(rows.getWarnings());
            }
            warning(statement.getWarnings());
            check(connection);
            return result;
        }
    }

    private static void exact(ResultSet row, String column, String expected) throws SQLException {
        // Only trailing fixed-CHAR padding is allowed; exact paths are never trimmed.
        String actual = row.getString(column);
        if (actual == null || !expected.equals(column.equals("PATH_NAME") ? actual : actual.stripTrailing())) {
            throw new As400NativeEvidenceException("Mismatched native table observation: " + column);
        }
    }

    private static String trim(ResultSet row, String column) throws SQLException {
        String value = row.getString(column);
        if (value == null) {
            throw new As400NativeEvidenceException("Missing native table field " + column);
        }
        return value.stripTrailing();
    }

    private static BigInteger number(ResultSet row, String column) throws SQLException {
        BigDecimal value = row.getBigDecimal(column);
        if (value == null) {
            throw new As400NativeEvidenceException("Missing native numeric field " + column);
        }
        BigInteger integer = value.toBigIntegerExact();
        integer.longValueExact();
        return integer;
    }

    private static void check(Connection connection) throws SQLException {
        checkInterrupted();
        warning(connection.getWarnings());
        if (!connection.isReadOnly()) {
            throw new As400NativeEvidenceException("Native table evidence connection is not read-only");
        }
    }

    private static void warning(SQLWarning value) throws SQLException {
        if (value != null) {
            throw new As400NativeEvidenceException("Native table query warning; partial evidence rejected", value);
        }
    }

    private static void checkInterrupted() throws SQLException {
        if (Thread.currentThread().isInterrupted()) {
            throw new SQLException("Native table evidence collection interrupted");
        }
    }

    private interface Decode<T> {
        T read(ResultSet row) throws SQLException;
    }
}
