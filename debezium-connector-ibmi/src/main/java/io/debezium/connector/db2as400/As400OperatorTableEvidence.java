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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
    private static final int CATALOG_BATCH_SIZE = 50;
    private static final int OBJECT_BATCH_TABLES = 8;
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
                    || recordBytes < 1 || recordBytes > 16 * 1024 * 1024 || fields < 1
                    || fields > io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder.MAX_COLUMNS) {
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
            if (maximumTables < 1 || queryTimeoutSeconds < 1 || queryTimeoutSeconds > 3600) {
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

    /** Bounded catalog batches and exact-path object queries. All connections are owned and closed. */
    List<Observation> collect(List<Request> requested) throws SQLException {
        List<Request> requests = List.copyOf(requested);
        if (requests.isEmpty() || requests.size() > limits.maximumTables()
                || requests.stream().map(request -> request.library() + "/" + request.file()).distinct().count() != requests.size()) {
            throw new IllegalArgumentException("Distinct bounded physical-file requests required");
        }
        checkInterrupted();
        try (Connection connection = Objects.requireNonNull(connections.open())) {
            connection.setReadOnly(true);
            check(connection);
            List<Observation> result = new ArrayList<>();
            // Catalog views otherwise repeat expensive native work per file. These
            // are fresh bounded reads, not a metadata cache; callers still compare
            // the complete observations before/after acquisition and the boundary.
            var members = catalog(connection, MEMBER_SQL, requests, As400OperatorTableEvidence::member);
            var formats = catalog(connection, FORMAT_SQL, requests, As400OperatorTableEvidence::format);
            for (int start = 0; start < requests.size(); start += OBJECT_BATCH_TABLES) {
                var batch = requests.subList(start, Math.min(start + OBJECT_BATCH_TABLES, requests.size()));
                var targets = new LinkedHashMap<Target, Request>();
                for (Request request : batch) {
                    String path = "/QSYS.LIB/" + request.library() + ".LIB/" + request.file() + ".FILE";
                    targets.put(new Target(path, Kind.FILE), request);
                    targets.put(new Target(path + "/" + members.get(request).member() + ".MBR", Kind.MEMBER), request);
                }
                var objects = objects(connection, targets);
                for (Request request : batch) {
                    String member = members.get(request).member();
                    String path = "/QSYS.LIB/" + request.library() + ".LIB/" + request.file() + ".FILE";
                    result.add(new Observation(request, new TableId(null, request.library(), members.get(request).table()), member,
                            objects.get(path), objects.get(path + "/" + member + ".MBR"), formats.get(request)));
                }
            }
            check(connection);
            return List.copyOf(result);
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new As400NativeEvidenceException("Invalid native table observation", e);
        }
    }

    private <T> Map<Request, T> catalog(Connection connection, String sql, List<Request> requests, CatalogDecode<T> decode) throws SQLException {
        var result = new LinkedHashMap<Request, T>();
        var libraries = requests.stream().collect(Collectors.groupingBy(Request::library, LinkedHashMap::new, Collectors.toList()));
        for (var library : libraries.values()) {
            for (int start = 0; start < library.size(); start += CATALOG_BATCH_SIZE) {
                var batch = library.subList(start, Math.min(start + CATALOG_BATCH_SIZE, library.size()));
                var expected = new LinkedHashMap<String, Request>();
                for (var request : batch) {
                    if (expected.put(request.file(), request) != null) {
                        throw new As400NativeEvidenceException("Ambiguous journal binding for one file");
                    }
                }
                String query = batch.size() == 1 ? sql
                        : sql.replace("SYSTEM_TABLE_NAME = ?",
                                "SYSTEM_TABLE_NAME IN (" + String.join(",", java.util.Collections.nCopies(batch.size(), "?")) + ")");
                check(connection);
                try (var statement = connection.prepareStatement(query)) {
                    statement.setQueryTimeout(limits.queryTimeoutSeconds());
                    statement.setMaxRows(batch.size() + 1);
                    statement.setString(1, batch.get(0).library());
                    for (int i = 0; i < batch.size(); i++) {
                        statement.setString(i + 2, batch.get(i).file());
                    }
                    warning(statement.getWarnings());
                    check(connection);
                    try (var rows = statement.executeQuery()) {
                        warning(statement.getWarnings());
                        warning(rows.getWarnings());
                        while (rows.next()) {
                            check(connection);
                            warning(rows.getWarnings());
                            exact(rows, "SYSTEM_TABLE_SCHEMA", batch.get(0).library());
                            var request = expected.remove(trim(rows, "SYSTEM_TABLE_NAME"));
                            if (request == null || result.containsKey(request)) {
                                throw new As400NativeEvidenceException("Duplicate or foreign native catalog observation");
                            }
                            result.put(request, decode.read(rows, request));
                            warning(rows.getWarnings());
                        }
                        warning(rows.getWarnings());
                    }
                    warning(statement.getWarnings());
                    check(connection);
                    if (!expected.isEmpty()) {
                        throw new As400NativeEvidenceException("Required native catalog observation is absent");
                    }
                }
            }
        }
        return result;
    }

    private record Member(String member, String table) {
    }

    private static Member member(ResultSet row, Request request) throws SQLException {
        // The exact physical-file lookup supplies its SQL name. Preserve it in
        // filters, topics and history; never infer it from the ten-byte file name.
        exact(row, "TABLE_SCHEMA", request.library());
        String table = row.getString("TABLE_NAME");
        if (table == null || table.isBlank() || table.length() > 128 || table.indexOf('\0') >= 0) {
            throw new As400NativeEvidenceException("Invalid SQL table identity");
        }
        String value = trim(row, "SYSTEM_TABLE_MEMBER");
        if (!value.matches("[A-Z0-9_#$@]{1,10}")) {
            throw new As400NativeEvidenceException("Invalid single-member name");
        }
        return new Member(value, table);
    }

    private static Format format(ResultSet row, Request request) throws SQLException {
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
    }

    private static NativeObject object(ResultSet row, Request request, Target target) throws SQLException {
        exact(row, "PATH_NAME", target.path());
        exact(row, "OBJECT_TYPE", target.kind().objectType());
        exact(row, "STORED_LOCAL", "YES");
        exact(row, "JOURNALED", "YES");
        exact(row, "JOURNAL_LIBRARY", request.journal().library());
        exact(row, "JOURNAL_NAME", request.journal().name());
        if (!number(row, "PATH_LENGTH").equals(BigInteger.valueOf(target.path().length())) || !As400OperatorJournalEvidence.systemBasicAsp(number(row, "ASP_NUMBER"))
                || !number(row, "HAS_SYMBOLIC_LINK").equals(BigInteger.ZERO)) {
            throw new As400NativeEvidenceException("Unexpected path, ASP or symbolic object");
        }
        String jid = row.getString("OBJECT_JID");
        if (jid == null || !jid.matches("[0-9A-Fa-f]{20}")) {
            throw new As400NativeEvidenceException("Missing exact native journal identifier");
        }
        return new NativeObject(target, new Identity(number(row, "FILE_SYSTEM_IDENTIFIER"), number(row, "FILE_IDENTIFIER_NUMBER"),
                number(row, "GENERATION_IDENTIFIER")), jid.toLowerCase(java.util.Locale.ROOT));
    }

    private Map<String, NativeObject> objects(Connection connection, Map<Target, Request> targets) throws SQLException {
        // UNION ALL preserves every service row, including duplicates that must
        // reject the observation. Only exact path/type pairs are bound; no scan,
        // deduplication, caching or metadata access inside the lock interval.
        // Limit the statement and result to sixteen objects, independent of the
        // complete journal scope. Each native function keeps IGNORE_ERRORS=NO.
        String sql = String.join(" UNION ALL ", Collections.nCopies(targets.size(), OBJECT_SQL));
        var expected = new LinkedHashMap<String, Target>();
        targets.keySet().forEach(target -> expected.put(target.path(), target));
        var result = new LinkedHashMap<String, NativeObject>();
        check(connection);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setQueryTimeout(limits.queryTimeoutSeconds());
            statement.setMaxRows(targets.size() + 1);
            statement.setFetchSize(targets.size() + 1);
            int parameter = 1;
            for (Target target : targets.keySet()) {
                statement.setString(parameter++, target.path());
                statement.setString(parameter++, target.kind().objectType());
            }
            warning(statement.getWarnings());
            check(connection);
            try (ResultSet rows = statement.executeQuery()) {
                warning(statement.getWarnings());
                warning(rows.getWarnings());
                while (rows.next()) {
                    check(connection);
                    warning(rows.getWarnings());
                    Target target = expected.remove(rows.getString("PATH_NAME"));
                    if (target == null) {
                        throw new As400NativeEvidenceException("Duplicate or foreign native object observation");
                    }
                    result.put(target.path(), object(rows, targets.get(target), target));
                    warning(rows.getWarnings());
                }
                warning(rows.getWarnings());
            }
            warning(statement.getWarnings());
            check(connection);
            if (!expected.isEmpty()) {
                throw new As400NativeEvidenceException("Required native object observation is absent");
            }
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

    private interface CatalogDecode<T> {
        T read(ResultSet row, Request request) throws SQLException;
    }
}
