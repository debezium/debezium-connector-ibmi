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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

class As400NativeIdentityProbeTest {
    private static final byte[] KEY = new byte[32];
    private static final String CONFIG = """
            source-label=source-a
            host=private.example.invalid
            user=PRIVATE
            key-file=/private/comparison.key
            os-version=7
            os-release=5
            ptf-group=SF99950
            ptf-level=11
            journal=/QSYS.LIB/LIB.LIB/JRN.JRN
            receiver=/QSYS.LIB/LIB.LIB/RCV.JRNRCV
            file=/QSYS.LIB/LIB.LIB/FILE.FILE
            member=/QSYS.LIB/LIB.LIB/FILE.FILE/MBR.MBR
            """;

    @TempDir
    Path temporary;

    @Test
    void offlinePlanNeedsNoCredentialsKeyOrDatabaseAndRejectsImplicitCollection() throws Exception {
        Path config = temporary.resolve("probe.properties");
        Files.writeString(config, CONFIG);
        assertThat(As400NativeIdentityProbe.command(new String[]{ "plan", config.toString() }, null)).isZero();
        assertThrows(IllegalArgumentException.class, () -> As400NativeIdentityProbe.command(new String[]{ config.toString() }, null));
        assertThrows(IllegalArgumentException.class, () -> As400NativeIdentityProbe.command(
                new String[]{ "collect", config.toString(), temporary.resolve("out.json").toString() }, null));
        assertThat(temporary.resolve("out.json")).doesNotExist();
        var plan = As400NativeIdentityProbe.plan(As400NativeIdentityProbe.Config.parse(CONFIG));
        assertThat(plan).containsEntry("status", "OFFLINE_PLAN").containsEntry("startup_proof", false);
        assertThat(json(plan)).doesNotContain("private.example", "PRIVATE", "/QSYS", "/private/comparison");
    }

    @Test
    void executesRealCollectorWithFixtureJdbcAndOnlyBoundedSelects() throws Exception {
        Fixture fixture = new Fixture();
        Map<String, Object> report = collect(fixture);
        assertThat(report).containsEntry("status", "OBSERVED_STABLE_CANDIDATES").containsEntry("startup_proof", false);
        assertThat((List<?>) report.get("observations")).hasSize(8);
        assertThat(fixture.connections).hasSize(10);
        assertThat(fixture.calls).hasSize(12);
        assertThat(fixture.calls.stream().filter(c -> c.sql.equals(As400JdbcNativeIdentityEvidence.SQL))).hasSize(8);
        for (Call call : fixture.calls) {
            assertThat(call.sql).startsWith("SELECT ");
            verify(call.statement).setMaxRows(2);
            verify(call.statement).setQueryTimeout(10);
            verify(call.statement, never()).executeUpdate();
            verify(call.statement).close();
            verify(call.rows).close();
        }
        for (Connection connection : fixture.connections) {
            verify(connection).setReadOnly(true);
            verify(connection).close();
            verify(connection, never()).prepareCall(anyString());
            verify(connection, never()).createStatement();
            verify(connection, never()).commit();
            verify(connection, never()).rollback();
        }
        for (Call call : fixture.calls.stream().filter(c -> c.sql.equals(As400NativeIdentityProbe.PTF_SQL)).toList()) {
            assertThat(call.parameters).containsEntry(1, "SF99950").containsEntry(2, 11);
        }
        String serialized = json(report);
        assertThat(serialized).doesNotContain("private.example", "PRIVATE", "/QSYS", "9007199254740993", "9223372036854775807");
        assertThat(serialized).contains("identity_lifetime", "maintenance_protection", "end_to_end_recovery");
    }

    @Test
    void detectsChangedIdentityAcrossNewConnectionsAndKeepsZeroConcrete() throws Exception {
        Fixture fixture = new Fixture();
        fixture.failure = "identity-change";
        Map<String, Object> report = collect(fixture);
        assertThat(report).containsEntry("status", "REJECTED");
        assertThat(json(report)).contains("CHANGED_BETWEEN_CONNECTIONS");
        assertThat(As400NativeIdentityProbe.token(KEY, "generation", "0")).isNotEqualTo(As400NativeIdentityProbe.token(KEY, "generation", "1"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "wrong-release", "wrong-ptf", "missing-profile", "duplicate-profile", "profile-warning", "profile-close",
            "profile-connection-warning", "not-read-only" })
    void profileFailurePreventsObjectQueries(String failure) throws Exception {
        Fixture fixture = new Fixture();
        fixture.failure = failure;
        Map<String, Object> report = collect(fixture);
        assertThat(report).containsEntry("status", "REJECTED");
        assertThat((List<?>) report.get("observations")).isEmpty();
        assertThat(fixture.calls).noneMatch(c -> c.sql.equals(As400JdbcNativeIdentityEvidence.SQL));
        fixture.connections.forEach(c -> {
            try {
                verify(c).close();
            }
            catch (SQLException e) {
                throw new AssertionError(e);
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "identity-absent", "identity-duplicate", "identity-sql", "identity-warning" })
    void individualObjectFailureCannotProduceAcceptedRunOrLeakErrors(String failure) throws Exception {
        Fixture fixture = new Fixture();
        fixture.failure = failure;
        var report = collect(fixture);
        assertThat(report).containsEntry("status", "REJECTED");
        assertThat(json(report)).doesNotContain("SECRET", "private.example", "/QSYS");
    }

    @Test
    void comparisonTokensAreStableDomainSeparatedAndKeyScoped() throws Exception {
        Fixture fixture = new Fixture();
        var first = collect(fixture);
        var second = collect(new Fixture());
        assertThat(first.get("observations")).isEqualTo(second.get("observations"));
        byte[] other = KEY.clone();
        other[0] = 1;
        assertThat(As400NativeIdentityProbe.token(KEY, "path", "value"))
                .isNotEqualTo(As400NativeIdentityProbe.token(other, "path", "value"))
                .isNotEqualTo(As400NativeIdentityProbe.token(KEY, "endpoint", "value"));
        assertThrows(IllegalArgumentException.class, () -> As400NativeIdentityProbe.collect(
                As400NativeIdentityProbe.Config.parse(CONFIG), () -> {
                    throw new AssertionError("No connection allowed");
                }, new byte[31]));
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "unknown", "duplicate", "host-options", "relative-key", "bad-member", "bad-number", "wildcard", "oversized" })
    void invalidConfigurationIsRejectedBeforeAnyIo(String failure) {
        String text = switch (failure) {
            case "missing" -> CONFIG.replace("user=PRIVATE\n", "");
            case "unknown" -> CONFIG + "password=SECRET\n";
            case "duplicate" -> CONFIG + "user=OTHER\n";
            case "host-options" -> CONFIG.replace("private.example.invalid", "example.invalid;secure=false");
            case "relative-key" -> CONFIG.replace("/private/comparison.key", "relative.key");
            case "bad-member" -> CONFIG.replace("FILE.FILE/MBR.MBR", "OTHER.FILE/MBR.MBR");
            case "bad-number" -> CONFIG.replace("ptf-level=11", "ptf-level=-1");
            case "wildcard" -> CONFIG.replace("JRN.JRN", "*.JRN");
            default -> CONFIG + "#".repeat(16_384);
        };
        assertThrows(IllegalArgumentException.class, () -> As400NativeIdentityProbe.Config.parse(text));
    }

    @Test
    void configuredDriverRequiresTlsReadOnlyModeAndFiniteTimeoutsWithoutConnecting() throws Exception {
        var source = As400NativeIdentityProbe.dataSource(As400NativeIdentityProbe.Config.parse(CONFIG), "unused".toCharArray());
        assertThat(source.isSecure()).isTrue();
        assertThat(source.getAccess()).isEqualTo("read only");
        assertThat(source.isPrompt()).isFalse();
        assertThat(source.isThreadUsed()).isFalse();
        assertThat(source.isTrace()).isFalse();
        assertThat(source.getLoginTimeout()).isEqualTo(10);
        assertThat(source.getSocketTimeout()).isEqualTo(15_000);
    }

    @Test
    void keyMustBePrivateRegularAndExactly32Bytes() throws Exception {
        Path key = temporary.resolve("key");
        Files.write(key, KEY);
        Files.setPosixFilePermissions(key, PosixFilePermissions.fromString("rw-------"));
        assertThat(As400NativeIdentityProbe.readKey(key)).isEqualTo(KEY);
        Files.setPosixFilePermissions(key, PosixFilePermissions.fromString("rw-r--r--"));
        assertThrows(java.io.IOException.class, () -> As400NativeIdentityProbe.readKey(key));
        Files.setPosixFilePermissions(key, PosixFilePermissions.fromString("rw-------"));
        Files.write(key, new byte[33]);
        assertThrows(java.io.IOException.class, () -> As400NativeIdentityProbe.readKey(key));
        Path link = temporary.resolve("link");
        Files.createSymbolicLink(link, key);
        assertThrows(java.io.IOException.class, () -> As400NativeIdentityProbe.readKey(link));
    }

    @Test
    void reportIsPrivateAndRefusesOverwriteOrSymlinkBeforeAnyConnection() throws Exception {
        Path report = temporary.resolve("report.json");
        var config = As400NativeIdentityProbe.Config.parse(CONFIG);
        Fixture fixture = new Fixture();
        assertThat(As400NativeIdentityProbe.writeReport(config, fixture::open, KEY, report)).isZero();
        assertThat(Files.getPosixFilePermissions(report)).isEqualTo(PosixFilePermissions.fromString("rw-------"));
        byte[] saved = Files.readAllBytes(report);
        assertThat(new ObjectMapper().readTree(saved).get("status").asText()).isEqualTo("OBSERVED_STABLE_CANDIDATES");
        As400JdbcConnectionFactory forbidden = () -> {
            throw new AssertionError("No connection allowed");
        };
        assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> As400NativeIdentityProbe.writeReport(config, forbidden, KEY, report));
        Path link = temporary.resolve("link.json");
        Files.createSymbolicLink(link, report);
        assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> As400NativeIdentityProbe.writeReport(config, forbidden, KEY, link));
        assertThat(Files.readAllBytes(report)).isEqualTo(saved);
    }

    @Test
    void rejectedObservationWritesCompleteFailureReportWithNonzeroResult() throws Exception {
        Fixture fixture = new Fixture();
        fixture.failure = "identity-sql";
        Path report = temporary.resolve("rejected.json");
        assertThat(As400NativeIdentityProbe.writeReport(As400NativeIdentityProbe.Config.parse(CONFIG), fixture::open, KEY, report)).isEqualTo(1);
        String content = Files.readString(report);
        assertThat(new ObjectMapper().readTree(content).get("status").asText()).isEqualTo("REJECTED");
        assertThat(content).doesNotContain("SECRET", "private.example", "/QSYS");
    }

    private static Map<String, Object> collect(Fixture fixture) throws Exception {
        return As400NativeIdentityProbe.collect(As400NativeIdentityProbe.Config.parse(CONFIG), fixture::open, KEY);
    }

    private static String json(Object value) throws Exception {
        return new ObjectMapper().writeValueAsString(value);
    }

    private static final class Call {
        final String sql;
        final PreparedStatement statement = mock(PreparedStatement.class);
        final ResultSet rows = mock(ResultSet.class);
        final Map<Integer, Object> parameters = new HashMap<>();

        Call(String sql) {
            this.sql = sql;
        }
    }

    /** Synthetic JDBC rows exercise the actual collector, not Db2 SQL execution. */
    private static final class Fixture {
        final List<Connection> connections = new ArrayList<>();
        final List<Call> calls = new ArrayList<>();
        final Map<String, Map<String, Object>> identities = new LinkedHashMap<>();
        String failure = "";

        Fixture() throws Exception {
            try (var in = getClass().getResourceAsStream("/startup-evidence/native-identities.json")) {
                List<Map<String, Object>> values = new ObjectMapper().readValue(in, new TypeReference<>() {
                });
                values.forEach(row -> identities.put(row.get("PATH_NAME").toString(), row));
            }
        }

        Connection open() throws SQLException {
            Connection connection = mock(Connection.class);
            connections.add(connection);
            when(connection.isReadOnly()).thenReturn(!failure.equals("not-read-only"));
            when(connection.getWarnings()).thenReturn(failure.equals("profile-connection-warning") ? new SQLWarning("SECRET") : null);
            if (failure.equals("profile-close")) {
                doAnswer(i -> {
                    throw new SQLException("SECRET");
                }).when(connection).close();
            }
            when(connection.prepareStatement(anyString())).thenAnswer(i -> {
                Call call = new Call(i.getArgument(0));
                calls.add(call);
                doAnswer(v -> {
                    call.parameters.put(v.getArgument(0), v.getArgument(1));
                    return null;
                })
                        .when(call.statement).setString(anyInt(), anyString());
                doAnswer(v -> {
                    call.parameters.put(v.getArgument(0), v.getArgument(1));
                    return null;
                })
                        .when(call.statement).setInt(anyInt(), anyInt());
                when(call.statement.executeQuery()).thenAnswer(v -> {
                    boolean identity = call.sql.equals(As400JdbcNativeIdentityEvidence.SQL);
                    Map<String, Object> row;
                    if (identity) {
                        if (failure.equals("identity-sql")) {
                            throw new SQLException("SECRET host=private.example.invalid path=/QSYS.LIB", "42501", -551);
                        }
                        row = new HashMap<>(identities.get(call.parameters.get(1)));
                        if (connections.size() > 5 && failure.equals("identity-change")) {
                            row.put("GENERATION_IDENTIFIER", "1");
                        }
                    }
                    else if (call.sql.equals(As400NativeIdentityProbe.RELEASE_SQL)) {
                        row = Map.of("OS_VERSION", "7", "OS_RELEASE", failure.equals("wrong-release") ? "4" : "5");
                    }
                    else if (call.sql.equals(As400NativeIdentityProbe.PTF_SQL)) {
                        row = Map.of("PTF_GROUP_NAME", "SF99950", "PTF_GROUP_LEVEL", "11", "PTF_GROUP_TARGET_RELEASE", "V7R5M0",
                                "PTF_GROUP_STATUS", failure.equals("wrong-ptf") ? "APPLY AT NEXT IPL" : "INSTALLED");
                    }
                    else {
                        throw new AssertionError("Unexpected SQL");
                    }
                    String prefix = identity ? "identity" : "profile";
                    int count = failure.equals(prefix + "-absent") || failure.equals("missing-profile") ? 0
                            : failure.equals(prefix + "-duplicate") || failure.equals("duplicate-profile") ? 2 : 1;
                    AtomicInteger index = new AtomicInteger();
                    when(call.rows.next()).thenAnswer(n -> index.getAndIncrement() < count);
                    when(call.rows.getString(anyString())).thenAnswer(n -> {
                        Object value = row.get(n.getArgument(0));
                        return value == null ? null : value.toString();
                    });
                    when(call.rows.getBigDecimal(anyString())).thenAnswer(n -> {
                        Object value = row.get(n.getArgument(0));
                        return value == null ? null : new BigDecimal(value.toString());
                    });
                    when(call.rows.getWarnings()).thenReturn(failure.equals(prefix + "-warning") ? new SQLWarning("SECRET", "01004") : null);
                    return call.rows;
                });
                return call.statement;
            });
            return connection;
        }
    }
}
