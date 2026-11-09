/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.SocketProperties;

import io.debezium.config.Configuration;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.TableChanges;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/** Explicit test entry point. Requires a separately owned, freshly created DCNI fixture. Never a connector mode. */
public class As400NativeInitializationProbe implements AutoCloseable {
    static final ObjectMapper JSON = new ObjectMapper();
    static final As400CaptureFingerprint.Policy POLICY = As400BindingHistoryTest.Fixture.POLICY;
    final Properties credentials;
    final String library;
    final String bootstrap;
    final String topic;
    final Path evidence;
    final JournalReceiver journal;
    final AS400 transport;
    final RetrieveJournal reader;
    final As400OperatorTableEvidence tables;
    final As400NativeBindingAcquisition acquisition;
    final As400NativeTransactionCensus census;
    final As400OperatorJournalEvidence journals;
    final As400JdbcNativeIdentityEvidence identities;
    final List<As400OperatorTableEvidence.Request> requests;
    final As400BindingHistory history;
    final As400OperatorCutReader.Limits cutLimits = new As400OperatorCutReader.Limits(16, 2000, 16_000_000);

    As400NativeInitializationProbe(Properties credentials, String library, Path evidence, String bootstrap, String topic) throws Exception {
        this.bootstrap = bootstrap;
        this.topic = topic;
        this.credentials = credentials;
        this.library = library;
        this.evidence = evidence;
        journal = new JournalReceiver("QSQJRN", library);
        transport = new AS400(credentials.getProperty("database.hostname"), credentials.getProperty("database.user"), credentials.getProperty("database.password"));
        SocketProperties sockets = new SocketProperties();
        sockets.setSoTimeout(15000);
        transport.setSocketProperties(sockets);
        transport.setThreadUsed(false);
        reader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> transport).withJournalInfo(new JournalInfo("QSQJRN", library, false))
                .withTransactionControls(true).withServerFiltering(false).withJournalBufferSize(1024 * 1024).withMaxServerSideEntries(1000).build(),
                new JournalInfoRetrieval(0, 0, 0));
        tables = new As400OperatorTableEvidence(() -> connection(false), new As400OperatorTableEvidence.Limits(4, 5));
        acquisition = new As400NativeBindingAcquisition(() -> connection(false), tables, new As400OperatorTableEvidence.Limits(4, 5));
        census = new As400NativeTransactionCensus(() -> connection(false), 1024, 5);
        journals = new As400OperatorJournalEvidence(() -> connection(false), journal, new As400OperatorJournalEvidence.Limits(16, 5));
        identities = new As400JdbcNativeIdentityEvidence(() -> connection(false), new As400JdbcNativeIdentityEvidence.Limits(20, 5, 65536));
        requests = List.of("A", "B", "M").stream().map(t -> new As400OperatorTableEvidence.Request(journal, library, t)).toList();
        Configuration config = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, bootstrap).with(KafkaSchemaHistory.TOPIC, topic)
                .with(SchemaHistory.NAME, topic).with(SchemaHistory.INTERNAL_CONNECTOR_CLASS, As400RpcConnector.class.getName())
                .with(SchemaHistory.INTERNAL_CONNECTOR_ID, topic).with(KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS, 50)
                .with(KafkaSchemaHistory.RECOVERY_POLL_ATTEMPTS, 40).build();
        history = new As400BindingHistory(As400KafkaBindingHistory::new, config, POLICY.sourcePartition());
    }

    Connection connection(boolean transaction) throws SQLException {
        Properties p = new Properties();
        p.setProperty("user", credentials.getProperty("database.user"));
        p.setProperty("password", credentials.getProperty("database.password"));
        p.setProperty("prompt", "false");
        p.setProperty("socket timeout", "15000");
        p.setProperty("thread used", "false");
        p.setProperty("lazy close", "false");
        Connection c = DriverManager.getConnection("jdbc:as400://" + credentials.getProperty("database.hostname"), p);
        try {
            c.setTransactionIsolation(Connection.TRANSACTION_NONE);
            try (var st = c.createStatement(); var rs = st.executeQuery("SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")) {
                assertThat(rs.next()).isTrue();
                String expected = credentials.getProperty("database.dbname");
                assertThat(expected).isNotBlank();
                assertThat(rs.getString(1).trim()).isEqualTo(expected);
            }
            if (transaction) {
                c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                c.setAutoCommit(false);
            }
            return c;
        }
        catch (Exception e) {
            c.close();
            throw e;
        }
    }

    void execute(Connection c, String sql) throws SQLException {
        try (var st = c.createStatement()) {
            st.setQueryTimeout(5);
            st.execute(sql);
        }
    }

    void save(String name, Object value) throws Exception {
        Path path = evidence.resolve(name);
        Files.writeString(path, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(value) + "\n");
        try (var c = FileChannel.open(path, StandardOpenOption.WRITE)) {
            c.force(true);
        }
        try (var c = FileChannel.open(evidence, StandardOpenOption.READ)) {
            c.force(true);
        }
    }

    String digest(Path path) throws Exception {
        return "sha256:" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)));
    }

    void metadata() throws Exception {
        var acquired = acquisition.collect(requests);
        var changes = new TableChanges();
        acquired.definitions().forEach(d -> changes.create(d.storedTable()));
        Document doc = Document.create();
        doc.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
        Files.writeString(evidence.resolve("native-bindings.json"), DocumentWriter.defaultWriter().write(doc));
        int before = census.requireEmpty(journal);
        try (Connection unrelated = connection(true)) {
            execute(unrelated, "INSERT INTO " + library + ".U VALUES (99)");
            Exception failure = assertThrows(SQLException.class, () -> census.requireEmpty(journal));
            assertThat(failure.getMessage()).contains("open or outcome is unresolved");
            save("uncaptured-cycle-rejection.json", Map.of("message", failure.getMessage(), "initial_checkpoint_exposed", false));
            unrelated.rollback();
        }
        int after = census.requireEmpty(journal);
        save("metadata-result.json", Map.of("status", "passed", "tables", acquired.definitions().size(), "definitions_before", before, "definitions_after", after,
                "binding_source", "native catalogs and exact file/member identities", "fixture_schemas_used", false));
    }

    void verifyLocks(Connection connection, String label) throws Exception {
        String job;
        try (var st = connection.createStatement(); var rs = st.executeQuery("SELECT QSYS2.JOB_NAME FROM SYSIBM.SYSDUMMY1")) {
            assertThat(rs.next()).isTrue();
            job = rs.getString(1);
        }
        List<Map<String, Object>> locks = new ArrayList<>();
        for (String table : List.of("A", "B", "M", "U")) {
            try (var st = connection
                    .prepareStatement("SELECT LOCK_STATE, LOCK_STATUS, JOB_NAME FROM TABLE(QSYS2.OBJECT_LOCK_INFO(?, ?, '*FILE', 0)) X WHERE JOB_NAME = ?")) {
                st.setQueryTimeout(5);
                st.setMaxRows(20);
                st.setString(1, library);
                st.setString(2, table);
                st.setString(3, job);
                boolean held = false;
                try (var rs = st.executeQuery()) {
                    while (rs.next()) {
                        String state = rs.getString(1).trim();
                        String status = rs.getString(2).trim();
                        locks.add(Map.of("table", table, "state", state, "status", status));
                        held |= state.equals("*EXCLRD") && status.equals("HELD");
                    }
                }
                save(label, locks);
                assertThat(held).as("native writer lock for %s", table).isTrue();
            }
        }
    }

    void initialize() throws Exception {
        assertThat(Files.exists(evidence.resolve("offset.json"))).isFalse();
        // Test-only controller owns every writer to this fresh journal. Native
        // EXCLRD file locks additionally block ordinary writes/DDL on all four
        // fixture files, including uncaptured U. They are NOT a general API-resource
        // or administrative fence and do not qualify a production Session provider.
        try (Connection locked = connection(true)) {
            for (String table : List.of("A", "B", "M", "U")) {
                execute(locked, "LOCK TABLE " + library + "." + table + " IN EXCLUSIVE MODE ALLOW READ");
            }
            verifyLocks(locked, "locks-before.json");
            String epoch = "native-" + library;
            var session = new As400NativeInitialization.Session() {
                final Map<String, Object> snapshot = new LinkedHashMap<>();

                public String checkHeld(JournalReceiver j, List<As400OperatorTableEvidence.Request> captured) throws Exception {
                    assertThat(j).isEqualTo(journal);
                    assertThat(captured).isEqualTo(requests);
                    assertThat(locked.isClosed()).isFalse();
                    assertThat(locked.getAutoCommit()).isFalse();
                    assertThat(locked.isValid(2)).isTrue();
                    return epoch;
                }

                public void prepareBaseline(String enrolled, List<As400BindingDefinition> definitions) throws Exception {
                    assertThat(enrolled).isEqualTo(epoch);
                    snapshot.put("epoch", epoch);
                    for (var definition : definitions) {
                        List<List<Object>> rows = new ArrayList<>();
                        String table = definition.identity().physicalFile();
                        try (var st = locked.createStatement(); var rs = st.executeQuery("SELECT * FROM " + library + "." + table + " ORDER BY ID")) {
                            while (rs.next()) {
                                assertThat(rows.size()).isLessThan(100);
                                List<Object> values = new ArrayList<>();
                                for (int i = 1; i <= definition.codecs().size(); i++) {
                                    values.add(rs.getObject(i));
                                }
                                rows.add(values);
                            }
                        }
                        snapshot.put(table, rows);
                    }
                }

                public String acknowledgeBaseline(String enrolled, As400NativeInitialization.Boundary boundary, List<As400BindingDefinition> definitions)
                        throws Exception {
                    assertThat(enrolled).isEqualTo(epoch);
                    assertThat(snapshot).containsKeys("A", "B", "M");
                    snapshot.put("boundary", boundary.position().toString());
                    save("baseline.json", snapshot);
                    assertThat(JSON.readTree(Files.readString(evidence.resolve("baseline.json"))).get("epoch").asText()).isEqualTo(epoch);
                    return digest(evidence.resolve("baseline.json"));
                }

                public void installCheckpoint(String enrolled, As400Checkpoint checkpoint, As400BindingHistory.Recovered bindings) throws Exception {
                    checkHeld(journal, requests);
                    verifyLocks(locked, "locks-before-checkpoint.json");
                    assertThat(enrolled).isEqualTo(epoch);
                    assertThat(bindings.initialization().orElseThrow().baselineReceipt()).isEqualTo(digest(evidence.resolve("baseline.json")));
                    var fresh = history.recover(checkpoint.toOffset(), checkpoint.journalIdentity(), POLICY);
                    assertThat(fresh.definitions()).isEqualTo(bindings.definitions());
                    // This experiment's durable output/offset store is a local fsync'd
                    // file. Kafka Connect output/offset installation remains a gate.
                    var brokerRecords = readHistory(bootstrap, topic);
                    assertThat(brokerRecords).hasSize(1);
                    assertThat(brokerRecords.get(0)).contains("initialization.baseline", bindings.initialization().orElseThrow().baselineReceipt());
                    save("independent-broker-history.json", brokerRecords);
                    save("offset.json", checkpoint.toOffset());
                }
            };
            var initializer = new As400NativeInitialization(acquisition, census, journals, identities, tables, reader, cutLimits, requests, POLICY, history);
            var checkpoint = initializer.initialize(session);
            save("initialize-result.json", Map.of("status", "passed", "offset", checkpoint.toOffset(), "history_before_checkpoint", true,
                    "source_fence_scope", "owned fixture controller plus native file locks; not a production Session provider"));
            locked.rollback();
        }
    }

    void transactions() throws Exception {
        try (Connection c = connection(true)) {
            execute(c, "TRUNCATE TABLE " + library + ".B REUSE STORAGE IGNORE DELETE TRIGGERS");
            execute(c, "INSERT INTO " + library + ".B VALUES (100)");
            c.commit();
        }
        try (Connection c = connection(false)) {
            execute(c, "INSERT INTO " + library + ".A VALUES (8)");
            execute(c, "INSERT INTO " + library + ".M (ID) VALUES (2)");
        }
        save("transactions-result.json", Map.of("status", "passed", "streaming", "disabled"));
    }

    void recover(boolean missing) throws Exception {
        Map<String, String> saved = JSON.readValue(Files.readString(evidence.resolve("offset.json")), new TypeReference<>() {
        });
        if (missing) {
            assertThrows(IllegalArgumentException.class, () -> history.recover(null, saved.get("journal_identity"), POLICY));
            save("missing-offset-result.json", Map.of("status", "passed", "output", 0));
            return;
        }
        var recovered = history.recover(saved, saved.get("journal_identity"), POLICY);
        var cut = new As400OperatorCutReader(journals, identities, journal, recovered, reader, cutLimits).read(saved);
        var bindings = recovered.definitions().stream().collect(Collectors.toMap(d -> d.identity().physicalFile(), d -> d.binding(65536)));
        List<SourceRecord> records = new ArrayList<>();
        List<Map<String, Object>> raw = new ArrayList<>();
        try (var replay = new As400ReplayCoordinator(saved, recovered.journalIdentity(), recovered.fingerprint(), cut.order(),
                new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000), 65536, 1000, recovered.resolver(65536),
                As400SourceNormalizer.SavepointPolicy.consumingTargets(), POLICY.sourcePartition(), records::add,
                new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), true);
                var output = new As400IncrementalLifecycleTest.Output(bindings, true).dispatcher) {
            for (var e : cut.entries()) {
                raw.add(Map.of("sequence", e.position().offset().toString(), "code", String.valueOf(e.header().getJournalCode()), "type", e.header().getEntryType(),
                        "hex", HexFormat.of().formatHex(e.data())));
                if (e.header().getJournalCode() == 'R' && e.header().getFile().equals("M") && e.header().getEntryType().equals("PT")) {
                    Object[] values = bindings.get("M").decoder().decode(e.header(), e.data(), 0);
                    assertThat(values).containsExactly(2, null, null, null, null, null, null, null, null, null, null);
                    save("native-nullable-row.json", Arrays.asList(values));
                }
                if (replay.accept(e.header(), e.data(), 0, e.position()).isPresent()) {
                    replay.deliver(output);
                }
            }
            replay.verifyOrdinaryCut();
            assertThat(As400BindingHistoryTest.data(records)).containsExactly("t:B", "c:B:100", "c:A:8", "c:M:2");
            save("recovered-offset.json", replay.checkpoint().toOffset());
        }
        save("raw-replay.json", raw);
        save("recover-result.json", Map.of("status", "passed", "data", As400BindingHistoryTest.data(records), "current_table_metadata_used", false,
                "output_schema_scope", "test dispatcher ID projection; full M values independently strict-decoded"));
    }

    private static List<String> readHistory(String bootstrap, String topic) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        properties.put("group.id", "dcg-binding-audit-" + UUID.randomUUID());
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", "false");
        properties.put("isolation.level", "read_committed");
        properties.put("default.api.timeout.ms", "10000");
        var partition = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(properties)) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            List<String> records = new ArrayList<>();
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (consumer.position(partition) < end && System.nanoTime() < deadline) {
                consumer.poll(Duration.ofMillis(200)).forEach(record -> records.add(record.value()));
            }
            assertThat(consumer.position(partition)).isGreaterThanOrEqualTo(end);
            return records;
        }
    }

    public void close() {
        transport.disconnectAllServices();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 5 || !args[0].matches("DCNI[0-9]{4}") || !args[2].matches("127\\.0\\.0\\.1:[0-9]+") || !args[3].startsWith("dcg-native-init-")) {
            throw new IllegalArgumentException("Owned native fixture, evidence, local Kafka, owned topic and phase required");
        }
        Properties credentials = new Properties();
        credentials.load(System.in);
        try (var probe = new As400NativeInitializationProbe(credentials, args[0], Path.of(args[1]), args[2], args[3])) {
            switch (args[4]) {
                case "metadata" -> probe.metadata();
                case "initialize" -> probe.initialize();
                case "transactions" -> probe.transactions();
                case "recover" -> probe.recover(false);
                case "missing-offset" -> probe.recover(true);
                default -> throw new IllegalArgumentException("Unknown phase");
            }
        }
    }
}
