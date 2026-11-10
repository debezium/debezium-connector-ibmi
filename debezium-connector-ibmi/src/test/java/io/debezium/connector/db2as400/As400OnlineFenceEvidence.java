/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

/** Offline validation of server-captured metadata through the existing native validators; no startup authority. */
public final class As400OnlineFenceEvidence {
    private static final ObjectMapper JSON = new ObjectMapper();
    static final String PROFILE = "online-file-fence-candidate-v1";
    static final String SCOPED_PROFILE = "online-native-scope-fence-candidate-v1";
    static final String OFFLOCK_PROFILE = "offlock-native-metadata-candidate-v1";

    record Query(String sql, int maximumRows) {
    }

    static Map<String, Query> queries() {
        return Map.of("member", new Query(As400OperatorTableEvidence.MEMBER_SQL, 1),
                "format", new Query(As400OperatorTableEvidence.FORMAT_SQL, 1),
                "object", new Query(As400OperatorTableEvidence.OBJECT_SQL, 1),
                "columns", new Query(As400NativeBindingAcquisition.COLUMNS_SQL, 4096),
                "keys", new Query(As400NativeBindingAcquisition.KEYS_SQL, 4096),
                "journal", new Query(As400OperatorJournalEvidence.JOURNAL_SQL, 1),
                "receivers", new Query(As400OperatorJournalEvidence.RECEIVERS_SQL, 16),
                "identity", new Query(As400JdbcNativeIdentityEvidence.SQL, 1),
                "scope", new Query(As400OnlineLockScope.SQL, As400OnlineLockScope.MAX_ROWS),
                "scope_counts", new Query(As400OnlineLockScope.COUNTS, 1));
    }

    record Candidate(As400NativeBindingAcquisition.Acquired tables, As400OperatorJournalEvidence.Observation journal,
            As400JdbcNativeIdentityEvidence.Candidates identities, JournalPosition boundary) {
    }

    static Candidate validate(JsonNode captured) throws Exception {
        return validate(captured, false);
    }

    static Candidate validateOffLock(JsonNode captured) throws Exception {
        return validate(captured, true);
    }

    private static Candidate validate(JsonNode captured, boolean offLock) throws Exception {
        boolean discovered = offLock || SCOPED_PROFILE.equals(captured.path("profile").asText());
        boolean profile = offLock ? OFFLOCK_PROFILE.equals(captured.path("profile").asText())
                && captured.path("outside_lock").isBoolean() && captured.path("outside_lock").booleanValue()
                : discovered || PROFILE.equals(captured.path("profile").asText());
        require(profile && discovered == captured.has("native_scope")
                && captured.path("status").asText().equals(offLock ? "observed" : "released")
                && captured.has("startup_proof") && captured.get("startup_proof").isBoolean() && !captured.get("startup_proof").booleanValue(),
                "Explicit candidate profile required; no startup proof accepted");
        String library = captured.path("library").asText();
        require(library.matches("DCNC[0-9]{4}"), "Explicit isolated fixture required");
        var journal = new JournalReceiver("QSQJRN", library);
        List<As400OperatorTableEvidence.Request> requests = new ArrayList<>();
        for (JsonNode target : captured.path("targets")) {
            String file = target.path("file").asText();
            require(target.path("library").asText().equals(library) && file.matches("[A-Z][A-Z0-9_]{0,9}")
                    && (discovered ? target.path("member").asText().matches("[A-Z0-9_#$@]{1,10}") : target.path("member").asText().equals(file)),
                    "Exact fixture file/member binding required");
            requests.add(new As400OperatorTableEvidence.Request(journal, library, file));
        }
        require(!requests.isEmpty() && requests.size() <= 50 && requests.stream().distinct().count() == requests.size(), "Distinct bounded fence targets required");
        if (discovered) {
            var scope = captured.get("native_scope");
            var targets = As400OnlineLockScope.targets(scope.path("discovered"), scope.path("members"), library);
            require(JSON.valueToTree(targets).equals(captured.get("targets")), "Lock targets differ from complete native inventory");
            As400OnlineLockScope.same(scope.path("discovered"), scope.path("before"), library);
            As400OnlineLockScope.same(scope.path("before"), scope.path("after"), library);
        }
        var rows = new CapturedRows(captured.path("queries"));
        var journals = new As400OperatorJournalEvidence(rows::connection, journal, new As400OperatorJournalEvidence.Limits(16, 5));
        var identityReader = new As400JdbcNativeIdentityEvidence(rows::connection, new As400JdbcNativeIdentityEvidence.Limits(2, 5, 1024));
        var before = journals.collect();
        var identityBefore = identityReader.collect(identityTargets(before));
        var limits = new As400OperatorTableEvidence.Limits(50, 5);
        var tables = new As400OperatorTableEvidence(rows::connection, limits);
        var acquired = new As400NativeBindingAcquisition(rows::connection, tables, limits).collect(requests);
        for (int i = 0; i < requests.size(); i++) {
            require(acquired.observations().get(i).member().equals(captured.get("targets").get(i).path("member").asText()), "Acquired member differs from lock target");
        }
        var after = journals.collect();
        var identityAfter = identityReader.collect(identityTargets(after));
        require(before.attached().equals(after.attached()) && identityBefore.equals(identityAfter), "Journal or receiver changed during fence");
        require(before.receivers().size() == after.receivers().size(), "Receiver directory changed during fence");
        for (var old : before.receivers()) {
            var current = after.receivers().stream().filter(r -> r.name().equals(old.name())).findFirst().orElseThrow();
            require(old.status().equals(current.status()) && old.first().equals(current.first()) && old.previous().equals(current.previous())
                    && old.next().equals(current.next()) && old.minimizeFiles().equals(current.minimizeFiles()) && old.job().equals(current.job())
                    && old.thread().equals(current.thread()) && old.luw().equals(current.luw()) && old.xid().equals(current.xid())
                    && (old.status().equals("ATTACHED") ? current.last().compareTo(old.last()) >= 0 : current.last().equals(old.last())),
                    "Receiver chain, recording profile or range changed during fence");
        }
        var attached = after.receivers().stream().filter(r -> r.name().equals(after.attached())).toList();
        require(attached.size() == 1 && attached.get(0).status().equals("ATTACHED") && attached.get(0).next().isEmpty(), "Exact attached receiver required");
        // Historical directory rows are observations only. A later replay must
        // qualify every receiver it actually needs; this boundary admits none of them.
        attached.get(0).requireReadableProfile();
        require(rows.observedNames().equals(expectedNames(requests.size())), "Unexpected, missing or unconsumed metadata queries");
        return new Candidate(acquired, after, identityAfter, new JournalPosition(attached.get(0).last(), after.attached()));
    }

    private static List<As400JdbcNativeIdentityEvidence.Target> identityTargets(As400OperatorJournalEvidence.Observation directory) {
        return List.of(As400OnlineNativeEvidence.target(directory.journal(), As400JdbcNativeIdentityEvidence.Kind.JOURNAL),
                As400OnlineNativeEvidence.target(directory.attached(), As400JdbcNativeIdentityEvidence.Kind.RECEIVER));
    }

    private static Map<String, Integer> expectedNames(int tables) {
        return Map.of("member", 2 * tables, "format", 2 * tables, "object", 4 * tables, "columns", tables, "keys", tables,
                "journal", 2, "receivers", 2, "identity", 4);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    /** Supplies owned captured JDBC values. It never supplies a missing query or contacts a source. */
    private static final class CapturedRows {
        private final Map<List<String>, List<List<Map<String, Object>>>> values = new LinkedHashMap<>();
        private final Map<String, Integer> names = new LinkedHashMap<>();
        private final Map<List<String>, Integer> consumed = new LinkedHashMap<>();

        CapturedRows(JsonNode captured) {
            require(captured.isArray() && captured.size() <= 508, "Bounded metadata capture required");
            for (JsonNode query : captured) {
                String name = query.path("name").asText();
                Query definition = queries().get(name);
                require(definition != null && query.path("sql").asText().equals(definition.sql()), "Unknown metadata query");
                List<String> key = new ArrayList<>(List.of(definition.sql()));
                require(query.path("parameters").isArray(), "Missing query parameters");
                for (JsonNode p : query.get("parameters")) {
                    require(p.isTextual(), "Exact string parameters required");
                    key.add(p.textValue());
                }
                require(key.size() == 3 && query.path("rows").isArray() && query.path("rows").size() <= definition.maximumRows(),
                        "Incomplete or excessive captured rows");
                String phase = List.of("columns", "keys").contains(name) ? "layout" : values.containsKey(key) ? "after" : "before";
                require(query.path("phase").asText().equals(phase), "Missing or reordered independent metadata observation");
                List<Map<String, Object>> rows = JSON.convertValue(query.get("rows"), new TypeReference<>() {
                });
                values.computeIfAbsent(List.copyOf(key), ignored -> new ArrayList<>()).add(rows);
                names.merge(name, 1, Integer::sum);
            }
        }

        Map<String, Integer> observedNames() {
            require(consumed.keySet().equals(values.keySet()) && values.entrySet().stream().allMatch(e -> e.getValue().size() == consumed.get(e.getKey())),
                    "Unconsumed captured metadata");
            return Map.copyOf(names);
        }

        Connection connection() throws SQLException {
            Connection connection = mock(Connection.class);
            when(connection.isReadOnly()).thenReturn(true);
            when(connection.prepareStatement(anyString())).thenAnswer(call -> {
                String sql = call.getArgument(0);
                PreparedStatement statement = mock(PreparedStatement.class);
                Map<Integer, String> parameters = new LinkedHashMap<>();
                doAnswer(setting -> {
                    parameters.put(setting.getArgument(0), setting.getArgument(1));
                    return null;
                }).when(statement).setString(org.mockito.ArgumentMatchers.anyInt(), anyString());
                when(statement.executeQuery()).thenAnswer(execution -> {
                    List<String> key = List.of(sql, parameters.get(1), parameters.get(2));
                    require(parameters.size() == 2 && values.containsKey(key), "Native validator requested uncaptured evidence");
                    int response = consumed.merge(key, 1, Integer::sum) - 1;
                    require(response < values.get(key).size(), "Missing independent metadata observation");
                    List<Map<String, Object>> data = values.get(key).get(response);
                    AtomicInteger at = new AtomicInteger(-1);
                    ResultSet result = mock(ResultSet.class);
                    when(result.next()).thenAnswer(next -> at.incrementAndGet() < data.size());
                    when(result.getString(anyString())).thenAnswer(get -> {
                        Object value = column(data.get(at.get()), get.getArgument(0));
                        return value == null ? null : value.toString();
                    });
                    when(result.getBigDecimal(anyString())).thenAnswer(get -> {
                        Object value = column(data.get(at.get()), get.getArgument(0));
                        return value == null ? null : new BigDecimal(value.toString());
                    });
                    return result;
                });
                return statement;
            });
            return connection;
        }

        private Object column(Map<String, Object> row, String name) throws SQLException {
            if (!row.containsKey(name)) {
                throw new SQLException("Missing captured native column " + name);
            }
            return row.get(name);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("queries")) {
            JSON.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[1]).toFile(), queries());
            return;
        }
        boolean offLock = args.length == 3 && args[0].equals("offlock");
        if (args.length != 2 && !offLock) {
            throw new IllegalArgumentException("queries output.json OR [offlock] captured.json new-output-directory");
        }
        Path source = Path.of(args[offLock ? 1 : 0]);
        require(Files.size(source) <= 16 * 1024 * 1024, "Bounded candidate file required");
        Candidate candidate = offLock ? validateOffLock(JSON.readTree(source.toFile())) : validate(JSON.readTree(source.toFile()));
        Path output = Path.of(args[offLock ? 2 : 1]);
        Files.createDirectory(output);
        TableChanges changes = new TableChanges();
        candidate.tables().definitions().forEach(definition -> changes.create(definition.storedTable()));
        Document document = Document.create();
        document.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
        Files.writeString(output.resolve("bindings.json"), DocumentWriter.defaultWriter().write(document));
        if (offLock) {
            JSON.writerWithDefaultPrettyPrinter().writeValue(output.resolve("candidate.json").toFile(), Map.of("status", "metadata-consistent-unqualified",
                    "startup_proof", false, "maintenance_visibility_proven", false,
                    "tables", candidate.tables().observations().stream().map(Object::toString).toList(),
                    "identity", candidate.identities().objects().toString(), "source_sha256", As400OnlineHandoff.hash(Files.readAllBytes(source))));
            return;
        }
        JSON.writerWithDefaultPrettyPrinter().writeValue(output.resolve("candidate.json").toFile(), Map.of("status", "validated-candidate",
                "startup_proof", false, "boundary",
                Map.of("library", candidate.boundary().receiver().library(), "receiver", candidate.boundary().receiver().name(), "sequence",
                        candidate.boundary().offset().toString()),
                "tables", candidate.tables().observations().stream().map(Object::toString).toList(),
                "identity", candidate.identities().objects().toString(), "source_sha256", As400OnlineHandoff.hash(Files.readAllBytes(source))));
    }
}
