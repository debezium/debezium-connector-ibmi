/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.Console;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400JDBCDataSource;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Limits;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;

/** Manual, test-scope diagnostic. Never wired into a connector or run by Surefire/Failsafe. */
public final class As400NativeIdentityProbe {
    static final String RELEASE_SQL = "SELECT OS_VERSION, OS_RELEASE FROM SYSIBMADM.ENV_SYS_INFO";
    static final String PTF_SQL = """
            SELECT PTF_GROUP_NAME, PTF_GROUP_LEVEL, PTF_GROUP_TARGET_RELEASE, PTF_GROUP_STATUS
            FROM QSYS2.GROUP_PTF_INFO WHERE PTF_GROUP_NAME = ? AND PTF_GROUP_LEVEL = ?
            """;
    static final int LOGIN_SECONDS = 10;
    static final int SOCKET_MILLIS = 15_000;
    static final int QUERY_SECONDS = 10;
    static final int SAMPLES = 2;
    private static final Set<String> KEYS = Set.of("source-label", "host", "user", "key-file", "os-version", "os-release",
            "ptf-group", "ptf-level", "journal", "receiver", "file", "member");

    private As400NativeIdentityProbe() {
    }

    record Config(String label, String host, String user, Path keyFile, String version, String release,
            String ptfGroup, int ptfLevel, List<Target> targets) {
        static Config parse(String text) throws IOException {
            if (text.length() > 16_384) {
                throw new IllegalArgumentException("Oversized configuration");
            }
            Properties p = new Properties() {
                @Override
                public synchronized Object put(Object key, Object value) {
                    if (containsKey(key)) {
                        throw new IllegalArgumentException("Duplicate configuration key");
                    }
                    return super.put(key, value);
                }
            };
            p.load(new StringReader(text));
            if (!p.stringPropertyNames().equals(KEYS)) {
                throw new IllegalArgumentException("Missing or unknown configuration key");
            }
            String label = matching(p, "source-label", "[a-z0-9-]{1,40}");
            String host = matching(p, "host", "[A-Za-z0-9][A-Za-z0-9.-]{0,252}");
            String user = matching(p, "user", "[A-Za-z0-9_#$@]{1,10}");
            String version = matching(p, "os-version", "[0-9]");
            String release = matching(p, "os-release", "[0-9]");
            String ptf = matching(p, "ptf-group", "SF[0-9]{5}");
            int level = Integer.parseInt(matching(p, "ptf-level", "[0-9]{1,5}"));
            Path key = Path.of(p.getProperty("key-file"));
            if (!key.isAbsolute()) {
                throw new IllegalArgumentException("An absolute private key-file path is required");
            }
            List<Target> targets = List.of(new Target(p.getProperty("journal"), Kind.JOURNAL),
                    new Target(p.getProperty("receiver"), Kind.RECEIVER), new Target(p.getProperty("file"), Kind.FILE),
                    new Target(p.getProperty("member"), Kind.MEMBER));
            if (!targets.get(3).path().startsWith(targets.get(2).path() + "/")) {
                throw new IllegalArgumentException("Member must belong to the named file");
            }
            return new Config(label, host, user, key, version, release, ptf, level, targets);
        }

        private static String matching(Properties p, String key, String pattern) {
            String value = p.getProperty(key);
            if (!value.matches(pattern)) {
                throw new IllegalArgumentException("Invalid configuration field");
            }
            return value;
        }
    }

    /** No credential/key read, driver construction or connection in plan mode. */
    static Map<String, Object> plan(Config config) throws IOException {
        Map<String, Object> report = base(config);
        report.put("status", "OFFLINE_PLAN");
        report.put("target_kinds", config.targets().stream().map(t -> t.kind().name()).toList());
        report.put("expected_profile", expected(config));
        report.put("bounds", Map.of("samples", SAMPLES, "maximum_connections", 10, "maximum_selects", 12,
                "maximum_rows_per_select", 2, "login_seconds", LOGIN_SECONDS, "socket_milliseconds", SOCKET_MILLIS,
                "query_seconds", QUERY_SECONDS));
        return report;
    }

    static Map<String, Object> collect(Config config, As400JdbcConnectionFactory connections, byte[] key) throws IOException {
        if (key.length != 32) {
            throw new IllegalArgumentException("A 32-byte comparison key is required");
        }
        Map<String, Object> report = base(config);
        report.put("started_utc", Instant.now().toString());
        report.put("comparison_key_id", token(key, "key-id", "native-identity-probe-v1"));
        report.put("endpoint_token", token(key, "endpoint", config.host()));
        report.put("authority_token", token(key, "authority", config.user()));
        report.put("expected_profile", expected(config));
        List<Map<String, Object>> observations = new ArrayList<>();
        List<Map<String, Object>> profiles = new ArrayList<>();
        Map<Kind, Object> first = new LinkedHashMap<>();
        boolean accepted = true;
        for (int sample = 1; sample <= SAMPLES; sample++) {
            if (Thread.currentThread().isInterrupted()) {
                accepted = false;
                break;
            }
            Map<String, Object> profile = new LinkedHashMap<>();
            profile.put("sample", sample);
            try {
                readProfile(config, connections);
                profile.putAll(expected(config));
                profile.put("status", "MATCHED_EXPECTED_PROFILE");
            }
            catch (SQLException e) {
                profile.putAll(failure(e));
                profiles.add(profile);
                accepted = false;
                break; // No object observation under an unknown/mismatched profile.
            }
            profiles.add(profile);
            for (Target target : config.targets()) {
                Map<String, Object> observation = new LinkedHashMap<>();
                observation.put("sample", sample);
                observation.put("kind", target.kind().name());
                observation.put("path_token", token(key, "path", target.path()));
                try {
                    // A fresh owned connection per target; use the production collector unchanged.
                    var candidates = new As400JdbcNativeIdentityEvidence(connections, new Limits(1, QUERY_SECONDS, 1)).collect(List.of(target));
                    var id = candidates.objects().get(target);
                    Map<String, String> identity = Map.of("file_system", token(key, "file-system", id.fileSystem().toString()),
                            "file", token(key, "file", id.file().toString()), "generation", token(key, "generation", id.generation().toString()));
                    observation.put("identity_tokens", identity);
                    if (sample == 1) {
                        first.put(target.kind(), identity);
                    }
                    boolean stable = sample == 1 || identity.equals(first.get(target.kind()));
                    observation.put("status", stable ? "OBSERVED_CANDIDATE" : "CHANGED_BETWEEN_CONNECTIONS");
                    accepted &= stable;
                }
                catch (SQLException e) {
                    observation.putAll(failure(e));
                    accepted = false;
                }
                observations.add(observation);
            }
        }
        report.put("profiles", profiles);
        report.put("observations", observations);
        report.put("finished_utc", Instant.now().toString());
        report.put("status", accepted ? "OBSERVED_STABLE_CANDIDATES" : "REJECTED");
        return report;
    }

    private static Map<String, Object> base(Config config) throws IOException {
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("report_format", "ibmi-native-identity-probe-v1");
        report.put("source_label", config.label());
        report.put("api_profile", As400JdbcNativeIdentityEvidence.API_PROFILE);
        report.put("collector_class_sha256", classHash(As400JdbcNativeIdentityEvidence.class));
        report.put("harness_class_sha256", classHash(As400NativeIdentityProbe.class));
        report.put("identity_query_sha256", digest(As400JdbcNativeIdentityEvidence.SQL.getBytes(StandardCharsets.UTF_8)));
        report.put("startup_proof", false);
        report.put("unvalidated", List.of("identity_lifetime", "restore_or_clone_detection", "retention", "maintenance_protection",
                "source_binding", "raw_history", "transactional_streaming", "end_to_end_recovery"));
        return report;
    }

    private static Map<String, Object> expected(Config c) {
        return Map.of("os_version", c.version(), "os_release", c.release(), "ptf_group", c.ptfGroup(),
                "ptf_level", c.ptfLevel(), "ptf_target_release", "V" + c.version() + "R" + c.release() + "M0", "ptf_status", "INSTALLED");
    }

    private static void readProfile(Config config, As400JdbcConnectionFactory connections) throws SQLException {
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            if (!connection.isReadOnly()) {
                throw new SQLException("Read-only connection required");
            }
            noWarnings(connection.getWarnings());
            try (PreparedStatement statement = connection.prepareStatement(RELEASE_SQL)) {
                configure(statement);
                try (ResultSet rows = statement.executeQuery()) {
                    firstRow(connection, statement, rows);
                    if (!config.version().equals(rows.getString("OS_VERSION")) || !config.release().equals(rows.getString("OS_RELEASE"))) {
                        throw new SQLException("Unexpected release");
                    }
                    lastRow(connection, statement, rows);
                }
                noWarnings(statement.getWarnings());
            }
            noWarnings(connection.getWarnings());
            try (PreparedStatement statement = connection.prepareStatement(PTF_SQL)) {
                configure(statement);
                statement.setString(1, config.ptfGroup());
                statement.setInt(2, config.ptfLevel());
                noWarnings(statement.getWarnings());
                try (ResultSet rows = statement.executeQuery()) {
                    firstRow(connection, statement, rows);
                    BigDecimal level = rows.getBigDecimal("PTF_GROUP_LEVEL");
                    if (!config.ptfGroup().equals(rows.getString("PTF_GROUP_NAME")) || level == null
                            || level.compareTo(BigDecimal.valueOf(config.ptfLevel())) != 0
                            || !expected(config).get("ptf_target_release").equals(rows.getString("PTF_GROUP_TARGET_RELEASE"))
                            || !"INSTALLED".equals(rows.getString("PTF_GROUP_STATUS"))) {
                        throw new SQLException("Unexpected PTF profile");
                    }
                    lastRow(connection, statement, rows);
                }
                noWarnings(statement.getWarnings());
            }
            noWarnings(connection.getWarnings());
        }
    }

    private static void configure(PreparedStatement statement) throws SQLException {
        statement.setQueryTimeout(QUERY_SECONDS);
        statement.setMaxRows(2);
        noWarnings(statement.getWarnings());
    }

    private static void firstRow(Connection c, PreparedStatement s, ResultSet r) throws SQLException {
        noWarnings(c.getWarnings());
        noWarnings(s.getWarnings());
        noWarnings(r.getWarnings());
        if (!r.next()) {
            throw new SQLException("Missing profile evidence");
        }
        noWarnings(r.getWarnings());
    }

    private static void lastRow(Connection c, PreparedStatement s, ResultSet r) throws SQLException {
        noWarnings(r.getWarnings());
        if (r.next()) {
            throw new SQLException("Ambiguous profile evidence");
        }
        noWarnings(r.getWarnings());
        noWarnings(s.getWarnings());
        noWarnings(c.getWarnings());
    }

    private static void noWarnings(SQLWarning warning) throws SQLException {
        if (warning != null) {
            throw new SQLException("Incomplete profile evidence", warning.getSQLState(), warning.getErrorCode());
        }
    }

    private static Map<String, Object> failure(SQLException e) {
        // Never serialize exception messages, causes or stack traces: they can contain host/user/path data.
        String state = e.getSQLState();
        return Map.of("status", "REJECTED", "sql_state", state != null && state.matches("[A-Z0-9]{5}") ? state : "UNAVAILABLE",
                "vendor_code", e.getErrorCode());
    }

    static AS400JDBCDataSource dataSource(Config config, char[] password) throws SQLException {
        AS400JDBCDataSource source = new AS400JDBCDataSource();
        source.setServerName(config.host());
        source.setUser(config.user());
        source.setPassword(password);
        source.setPrompt(false);
        source.setSecure(true);
        source.setAccess("read only");
        source.setLoginTimeout(LOGIN_SECONDS);
        source.setSocketTimeout(SOCKET_MILLIS);
        source.setThreadUsed(false);
        source.setTrace(false);
        return source;
    }

    private static String classHash(Class<?> type) throws IOException {
        try (var stream = type.getResourceAsStream(type.getSimpleName() + ".class")) {
            if (stream == null) {
                throw new IOException("Missing class provenance");
            }
            return digest(stream.readAllBytes());
        }
    }

    private static String digest(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException("SHA-256 unavailable");
        }
    }

    static String token(byte[] key, String domain, String value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(("ibmi-native-probe-v1\0" + domain + "\0" + value).getBytes(StandardCharsets.UTF_8)));
        }
        catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC unavailable");
        }
    }

    static byte[] readKey(Path path) throws IOException {
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
                || !PosixFilePermissions.fromString("rw-------").containsAll(Files.getPosixFilePermissions(path, LinkOption.NOFOLLOW_LINKS))) {
            throw new IOException("Comparison key must be a private regular file");
        }
        try (var in = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            byte[] key = in.readNBytes(33);
            if (key.length != 32) {
                throw new IOException("Comparison key must contain exactly 32 bytes");
            }
            return key;
        }
    }

    static int writeReport(Config config, As400JdbcConnectionFactory connections, byte[] key, Path path) throws IOException {
        // Reserve a new private report before opening any source connection.
        try (var output = Files.newByteChannel(path, EnumSet.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))) {
            Map<String, Object> report = collect(config, connections, key);
            new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Channels.newOutputStream(output), report);
            return report.get("status").equals("OBSERVED_STABLE_CANDIDATES") ? 0 : 1;
        }
    }

    public static void main(String[] args) {
        int code;
        try {
            code = command(args, System.console());
        }
        catch (Exception e) {
            System.err.println("Probe failed; check configuration, private key, console, output path and approved source profile. No exception details are printed.");
            code = 2;
        }
        if (code != 0) {
            System.exit(code);
        }
    }

    static int command(String[] args, Console console) throws Exception {
        if (!((args.length == 2 && args[0].equals("plan")) || (args.length == 3 && args[0].equals("collect")))) {
            throw new IllegalArgumentException("Usage: plan CONFIG | collect CONFIG NEW_REPORT");
        }
        String text;
        try (var in = Files.newInputStream(Path.of(args[1]))) {
            byte[] bytes = in.readNBytes(16_385);
            if (bytes.length > 16_384) {
                throw new IOException("Oversized configuration");
            }
            text = new String(bytes, StandardCharsets.UTF_8);
        }
        Config config = Config.parse(text);
        ObjectMapper mapper = new ObjectMapper();
        if (args[0].equals("plan")) {
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(plan(config)));
            return 0;
        }
        if (console == null) {
            throw new IllegalArgumentException("An interactive console is required for the password");
        }
        byte[] key = readKey(config.keyFile());
        char[] password = null;
        try {
            password = console.readPassword("IBM i read-only probe password: ");
            if (password == null || password.length == 0) {
                throw new IllegalArgumentException("Missing password");
            }
            AS400JDBCDataSource source = dataSource(config, password);
            return writeReport(config, source::getConnection, key, Path.of(args[2]));
        }
        finally {
            if (password != null) {
                Arrays.fill(password, '\0');
            }
            Arrays.fill(key, (byte) 0);
        }
    }
}
