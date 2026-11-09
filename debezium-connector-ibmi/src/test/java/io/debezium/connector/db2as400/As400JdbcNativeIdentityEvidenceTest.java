/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Limits;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;

class As400JdbcNativeIdentityEvidenceTest {
    private static final Target JOURNAL = new Target("/QSYS.LIB/LIB.LIB/JRN.JRN", Kind.JOURNAL);
    private static final Target RECEIVER = new Target("/QSYS.LIB/LIB.LIB/RCV.JRNRCV", Kind.RECEIVER);
    private static final Target FILE = new Target("/QSYS.LIB/LIB.LIB/FILE.FILE", Kind.FILE);
    private static final Target MEMBER = new Target("/QSYS.LIB/LIB.LIB/FILE.FILE/MBR.MBR", Kind.MEMBER);
    private static final List<Target> TARGETS = List.of(JOURNAL, RECEIVER, FILE, MEMBER);
    private static final Limits LIMITS = new Limits(4, 5, 16);
    private static final List<String> IDENTIFIERS = List.of("FILE_SYSTEM_IDENTIFIER", "FILE_IDENTIFIER_NUMBER", "GENERATION_IDENTIFIER");

    @Test
    void collectsExactOwnedCandidateTuplesUsingOnlyBoundedPreparedSelects() throws Exception {
        Fixture db = new Fixture();
        var candidates = collector(db).collect(TARGETS);
        assertThat(candidates.objects()).hasSize(4);
        assertThat(candidates.objects().get(JOURNAL).file()).isEqualTo(new BigInteger("9007199254740993"));
        assertThat(candidates.objects().get(JOURNAL).generation()).isEqualTo(BigInteger.ZERO);
        assertThat(candidates.objects().get(FILE).file()).isEqualTo(BigInteger.valueOf(Long.MAX_VALUE));
        assertThat(candidates.apiProfile()).isEqualTo("qsys2-ifs-object-statistics-candidate-v1");
        db.row(JOURNAL).put("FILE_IDENTIFIER_NUMBER", "1");
        assertThat(candidates.objects().get(JOURNAL).file()).isEqualTo(new BigInteger("9007199254740993"));
        assertThrows(UnsupportedOperationException.class, () -> candidates.objects().clear());
        assertThat(db.calls).hasSize(TARGETS.size());
        for (int i = 0; i < TARGETS.size(); i++) {
            Call call = db.calls.get(i);
            assertThat(call.parameters).containsExactlyInAnyOrderEntriesOf(Map.of(1, TARGETS.get(i).path(), 2, TARGETS.get(i).kind().objectType()));
            assertThat(call.sql).startsWith("SELECT ").contains("START_PATH_NAME => ?", "SUBTREE_DIRECTORIES => 'NO'", "OBJECT_TYPE_LIST => ?",
                    "IGNORE_ERRORS => 'NO'", "LENGTH(PATH_NAME)", "VARCHAR(55)")
                    .doesNotContain(TARGETS.get(i).path(), "*ALL", "*LIBL", "*CURLIB");
            verify(call.statement).setQueryTimeout(5);
            verify(call.statement).setMaxRows(2);
            verify(call.statement, never()).executeUpdate();
        }
        verify(db.connection).setReadOnly(true);
        verify(db.connection, never()).prepareCall(anyString());
        verify(db.connection, never()).createStatement();
        verify(db.connection, never()).commit();
        verify(db.connection, never()).rollback();
        db.assertClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "/", "/QSYS.LIB", "/QSYS.LIB/LIB.LIB", "QSYS.LIB/LIB.LIB/JRN.JRN", "/qsys.lib/lib.lib/jrn.jrn",
            "/QSYS.LIB/LIB.LIB/*.JRN", "/QSYS.LIB/LIB.LIB/JR?.JRN", "/QSYS.LIB/LIB.LIB/JRN.JRN/", "/QSYS.LIB//LIB.LIB/JRN.JRN",
            "/QSYS.LIB/LIB.LIB/../JRN.JRN", "/QSYS.LIB/LIB.LIB/./JRN.JRN", "/QSYS.LIB/LIB.LIB/JRN.FILE", "/QSYS.LIB/LIB.LIB/JRN.JRN/MBR.MBR",
            "/QSYS.LIB/TOOLONGNAME.LIB/JRN.JRN", "/QSYS.LIB/LIB.LIB/'JRN'.JRN", "/QSYS.LIB/LIB.LIB/J RN.JRN", "/QSYS.LIB/LIB.LIB/JRN.JRN\n",
            "/QSYS.LIB/LIB.LIB/JRN%2FOTHER.JRN", "/IASP/QSYS.LIB/LIB.LIB/JRN.JRN", "/QFileSvr.400/SERVER/QSYS.LIB/LIB.LIB/JRN.JRN" })
    void rejectsNonCanonicalOrAmbiguousTargetsBeforeAnyIo(String path) {
        assertThrows(IllegalArgumentException.class, () -> new Target(path, Kind.JOURNAL));
    }

    @Test
    void supportsMaximumBasicNamesWithoutTruncationAndBindsSpecialNameCharacters() throws Exception {
        Fixture db = new Fixture();
        Target target = new Target("/QSYS.LIB/A123456789.LIB/B123456789.FILE/C123456789.MBR", Kind.MEMBER);
        assertThat(target.path()).hasSize(55);
        Map<String, Object> row = new HashMap<>(db.row(MEMBER));
        row.put("PATH_NAME", target.path());
        row.put("PATH_LENGTH", 55);
        db.rows.put(target.path(), new ArrayList<>(List.of(row)));
        Target special = new Target("/QSYS.LIB/A$#@_.LIB/B$#@_.JRN", Kind.JOURNAL);
        Map<String, Object> specialRow = new HashMap<>(db.row(JOURNAL));
        specialRow.put("PATH_NAME", special.path());
        specialRow.put("PATH_LENGTH", special.path().length());
        db.rows.put(special.path(), new ArrayList<>(List.of(specialRow)));
        assertThat(collector(db).collect(List.of(target, special)).objects()).containsKeys(target, special);
        assertThat(db.calls.get(1).parameters).containsEntry(1, special.path());
        db.assertClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "absent", "duplicate", "extra-object", "path", "long-path", "prefix-only", "null-path", "path-length", "null-length",
            "type", "null-type", "asp", "null-asp", "remote", "null-local", "unknown-local", "symbolic", "null-symbolic" })
    void rejectsMissingAmbiguousOrWrongSourceObjects(String scenario) throws Exception {
        Fixture db = new Fixture();
        Map<String, Object> row = db.row(JOURNAL);
        switch (scenario) {
            case "absent" -> db.rows.get(JOURNAL.path()).clear();
            case "duplicate" -> db.rows.get(JOURNAL.path()).add(new HashMap<>(row));
            case "extra-object" -> db.rows.get(JOURNAL.path()).add(new HashMap<>(db.row(RECEIVER)));
            case "path" -> row.put("PATH_NAME", RECEIVER.path());
            case "long-path" -> row.put("PATH_NAME", "x".repeat(100));
            case "prefix-only" -> row.put("PATH_LENGTH", 16_000_000);
            case "null-path" -> row.put("PATH_NAME", null);
            case "path-length" -> row.put("PATH_LENGTH", 1);
            case "null-length" -> row.put("PATH_LENGTH", null);
            case "type" -> row.put("OBJECT_TYPE", "*SYMLNK");
            case "null-type" -> row.put("OBJECT_TYPE", null);
            case "asp" -> row.put("ASP_NUMBER", 2);
            case "null-asp" -> row.put("ASP_NUMBER", null);
            case "remote" -> row.put("STORED_LOCAL", "NO");
            case "null-local" -> row.put("STORED_LOCAL", null);
            case "unknown-local" -> row.put("STORED_LOCAL", "?");
            case "symbolic" -> row.put("HAS_SYMBOLIC_LINK", 1);
            case "null-symbolic" -> row.put("HAS_SYMBOLIC_LINK", null);
            default -> throw new AssertionError(scenario);
        }
        assertThrows(SQLException.class, () -> collector(db).collect(TARGETS));
        assertThat(db.calls).hasSize(1); // No retry, parent-directory enumeration or partial result.
        db.assertClosed();
    }

    static Stream<Arguments> invalidNumbers() {
        return IDENTIFIERS.stream().flatMap(column -> Stream.of(null, "1.5", "9223372036854775808", "-9223372036854775809")
                .map(value -> Arguments.of(column, value)));
    }

    @ParameterizedTest
    @MethodSource("invalidNumbers")
    void rejectsMissingFractionalOrOverflowIdentityComponents(String column, String value) throws Exception {
        Fixture db = new Fixture();
        db.row(JOURNAL).put(column, value);
        assertThrows(SQLException.class, () -> collector(db).collect(List.of(JOURNAL)));
        db.assertClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "-9223372036854775808", "9223372036854775807", "9007199254740993" })
    void preservesExactSqlBigintDomainWithoutZeroWildcard(String value) throws Exception {
        Fixture db = new Fixture();
        IDENTIFIERS.forEach(column -> db.row(JOURNAL).put(column, value));
        var identity = collector(db).collect(List.of(JOURNAL)).objects().get(JOURNAL);
        assertThat(identity.fileSystem()).isEqualTo(new BigInteger(value));
        assertThat(identity.file()).isEqualTo(new BigInteger(value));
        assertThat(identity.generation()).isEqualTo(new BigInteger(value));
        db.assertClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "not-read-only", "connection-warning", "statement-warning", "result-warning", "decode-warning", "late-result-warning",
            "late-statement-warning", "initial-result-warning",
            "close-connection-warning", "missing-service", "missing-column", "authority", "timeout", "mid-result-failure", "prepare-failure", "close-failure" })
    void failsOnIncompleteSqlAndClosesOwnedResources(String failure) throws Exception {
        Fixture db = new Fixture();
        db.failure = failure;
        assertThrows(SQLException.class, () -> collector(db).collect(TARGETS));
        assertThat(db.calls.size()).isLessThanOrEqualTo(1);
        db.assertClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "empty", "duplicate", "too-many" })
    void validatesAggregateQueryBoundBeforeOpeningConnection(String scenario) throws Exception {
        Fixture db = new Fixture();
        List<Target> targets = switch (scenario) {
            case "empty" -> List.of();
            case "duplicate" -> List.of(JOURNAL, JOURNAL);
            default -> TARGETS;
        };
        var collector = new As400JdbcNativeIdentityEvidence(db::open, new Limits(3, 5, 16));
        assertThrows(IllegalArgumentException.class, () -> collector.collect(targets));
        assertThat(db.opens).isZero();
        assertThat(db.calls).isEmpty();
    }

    @Test
    void limitsMustBeFinite() {
        assertThrows(IllegalArgumentException.class, () -> new Limits(0, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new Limits(1025, 1, 1));
        assertThrows(IllegalArgumentException.class, () -> new Limits(1, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new Limits(1, 3601, 1));
        assertThrows(IllegalArgumentException.class, () -> new Limits(1, 1, 0));
        assertThrows(IllegalArgumentException.class, () -> new Limits(1, 1, 16 * 1024 * 1024 + 1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "before-open", "during-query", "after-read" })
    void honorsInterruptionWithoutReturningCandidates(String stage) throws Exception {
        Fixture db = new Fixture();
        try {
            if (stage.equals("before-open")) {
                Thread.currentThread().interrupt();
            }
            if (stage.equals("during-query")) {
                db.beforeExecute = ignored -> Thread.currentThread().interrupt();
            }
            assertThrows(SQLException.class, () -> collector(db).collectAroundRead(TARGETS, maximum -> {
                if (stage.equals("after-read")) {
                    Thread.currentThread().interrupt();
                }
                return new byte[]{ 1 };
            }));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            if (stage.equals("before-open")) {
                assertThat(db.opens).isZero();
            }
            else {
                db.assertClosed();
            }
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    void copiesBytesBeforeReobservationAndReturnsOnlyOwnedCandidateData() throws Exception {
        Fixture db = new Fixture();
        byte[] borrowed = { 1, 2, 3 };
        List<Target> requested = new ArrayList<>(TARGETS);
        db.beforeExecute = query -> {
            if (query == TARGETS.size() + 1) {
                borrowed[0] = 99; // Simulate reuse of the RPC buffer during subsequent I/O.
            }
        };
        var result = collector(db).collectAroundRead(requested, maximum -> {
            assertThat(maximum).isEqualTo(16);
            requested.clear();
            return borrowed;
        });
        assertThat(result.before()).isEqualTo(result.after());
        assertThat(result.bytes()).containsExactly(1, 2, 3);
        result.bytes()[1] = 99;
        assertThat(result.bytes()).containsExactly(1, 2, 3);
        assertThat(db.calls).hasSize(2 * TARGETS.size());
        assertThat(db.opens).isEqualTo(1);
        db.assertClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "FILE_SYSTEM_IDENTIFIER", "FILE_IDENTIFIER_NUMBER", "GENERATION_IDENTIFIER" })
    void everyIdentityComponentDetectsSameNameReplacementAndDiscardsRead(String column) throws Exception {
        Fixture db = new Fixture();
        db.row(RECEIVER).put(column, "0"); // Zero is a concrete component, never a wildcard.
        AtomicBoolean read = new AtomicBoolean();
        var failure = assertThrows(SQLException.class, () -> collector(db).collectAroundRead(TARGETS, maximum -> {
            read.set(true);
            db.row(RECEIVER).put(column, "999"); // All other metadata and the path are unchanged.
            return new byte[]{ 1, 2, 3 };
        }));
        assertThat(failure.getMessage()).contains("identity changed");
        assertThat(read).isTrue();
        db.assertClosed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "null", "oversized", "read-failure", "after-missing", "after-warning", "read-only-lost" })
    void neverReturnsReadBytesWhenTheReadOrSecondObservationFails(String scenario) throws Exception {
        Fixture db = new Fixture();
        assertThrows(SQLException.class, () -> collector(db).collectAroundRead(TARGETS, maximum -> {
            switch (scenario) {
                case "null":
                    return null;
                case "oversized":
                    return new byte[maximum + 1];
                case "read-failure":
                    throw new SQLException("Read failed");
                case "after-missing":
                    db.rows.get(RECEIVER.path()).clear();
                    break;
                case "after-warning":
                    db.failure = "connection-warning";
                    break;
                case "read-only-lost":
                    db.failure = "not-read-only";
                    break;
                default:
                    throw new AssertionError(scenario);
            }
            return new byte[]{ 1 };
        }));
        db.assertClosed();
    }

    @Test
    void failedInitialObservationNeverInvokesReader() throws Exception {
        Fixture db = new Fixture();
        db.rows.get(JOURNAL.path()).clear();
        AtomicBoolean read = new AtomicBoolean();
        assertThrows(SQLException.class, () -> collector(db).collectAroundRead(TARGETS, maximum -> {
            read.set(true);
            return new byte[]{ 1 };
        }));
        assertThat(read).isFalse();
        db.assertClosed();
    }

    private static As400JdbcNativeIdentityEvidence collector(Fixture db) {
        return new As400JdbcNativeIdentityEvidence(db::open, LIMITS);
    }

    private static final class Call {
        final String sql;
        final PreparedStatement statement = mock(PreparedStatement.class);
        final Map<Integer, String> parameters = new HashMap<>();
        ResultSet results;
        int maximumRows;
        int index = -1;
        boolean decoded;

        Call(String sql) {
            this.sql = sql;
        }
    }

    /** Mock transport with hand-authored projected SQL rows; does not simulate SQL execution. */
    private static final class Fixture {
        final Map<String, List<Map<String, Object>>> rows = new LinkedHashMap<>();
        final Connection connection = mock(Connection.class);
        final List<Call> calls = new ArrayList<>();
        int opens;
        String failure = "";
        boolean statementClosed;
        IntConsumer beforeExecute = ignored -> {
        };

        Fixture() throws Exception {
            try (var stream = getClass().getResourceAsStream("/startup-evidence/native-identities.json")) {
                List<Map<String, Object>> values = new ObjectMapper().readValue(stream, new TypeReference<>() {
                });
                for (Map<String, Object> row : values) {
                    rows.put((String) row.get("PATH_NAME"), new ArrayList<>(List.of(row)));
                }
            }
            when(connection.isReadOnly()).thenAnswer(ignored -> !failure.equals("not-read-only"));
            when(connection.getWarnings()).thenAnswer(ignored -> failure.equals("connection-warning")
                    || (failure.equals("close-connection-warning") && statementClosed) ? new SQLWarning("Incomplete") : null);
            when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
                if (failure.equals("prepare-failure")) {
                    throw new SQLException("Prepare failed");
                }
                Call call = new Call(invocation.getArgument(0));
                assertThat(call.sql).isEqualTo(As400JdbcNativeIdentityEvidence.SQL);
                calls.add(call);
                doAnswer(i -> {
                    call.parameters.put(i.getArgument(0), i.getArgument(1));
                    return null;
                }).when(call.statement).setString(anyInt(), anyString());
                doAnswer(i -> {
                    call.maximumRows = i.getArgument(0);
                    return null;
                }).when(call.statement).setMaxRows(anyInt());
                doAnswer(i -> {
                    statementClosed = true;
                    if (failure.equals("close-failure")) {
                        throw new SQLException("Close failed");
                    }
                    return null;
                }).when(call.statement).close();
                when(call.statement.getWarnings()).thenAnswer(ignored -> failure.equals("statement-warning")
                        || (failure.equals("late-statement-warning") && call.index > 0) ? new SQLWarning("Truncated", "01004") : null);
                when(call.statement.executeQuery()).thenAnswer(ignored -> {
                    beforeExecute.accept(calls.size());
                    if (failure.equals("missing-service") || failure.equals("authority")) {
                        throw new SQLException(failure, failure.equals("authority") ? "42501" : "42704");
                    }
                    if (failure.equals("timeout")) {
                        throw new SQLTimeoutException("Timeout");
                    }
                    List<Map<String, Object>> source = rows.get(call.parameters.get(1));
                    assertThat(source).as("Exact path only; no parent query").isNotNull();
                    List<Map<String, Object>> bounded = source.subList(0, Math.min(source.size(), call.maximumRows));
                    call.results = mock(ResultSet.class);
                    when(call.results.next()).thenAnswer(i -> {
                        if (failure.equals("mid-result-failure") && call.index == 0) {
                            throw new SQLException("Partial response");
                        }
                        return ++call.index < bounded.size();
                    });
                    when(call.results.getString(anyString())).thenAnswer(i -> value(bounded, call.index, i.getArgument(0)));
                    when(call.results.getBigDecimal(anyString())).thenAnswer(i -> {
                        Object value = value(bounded, call.index, i.getArgument(0));
                        call.decoded = true;
                        return value == null ? null : new BigDecimal(value.toString());
                    });
                    when(call.results.getWarnings()).thenAnswer(i -> failure.equals("result-warning")
                            || (failure.equals("initial-result-warning") && call.index < 0)
                            || (failure.equals("decode-warning") && call.index == 0 && call.decoded)
                            || (failure.equals("late-result-warning") && call.index > 0) ? new SQLWarning("Incomplete", "01000") : null);
                    if (failure.equals("missing-column")) {
                        doThrow(new SQLException("Unknown column", "42703")).when(call.results).getBigDecimal("GENERATION_IDENTIFIER");
                    }
                    return call.results;
                });
                return call.statement;
            });
        }

        Connection open() {
            opens++;
            return connection;
        }

        Map<String, Object> row(Target target) {
            return rows.get(target.path()).get(0);
        }

        Object value(List<Map<String, Object>> values, int index, String column) throws SQLException {
            if (!values.get(index).containsKey(column)) {
                throw new SQLException("Missing fixture column " + column);
            }
            return values.get(index).get(column);
        }

        void assertClosed() throws SQLException {
            verify(connection).close();
            for (Call call : calls) {
                verify(call.statement).close();
                if (call.results != null) {
                    verify(call.results).close();
                }
            }
        }
    }
}
