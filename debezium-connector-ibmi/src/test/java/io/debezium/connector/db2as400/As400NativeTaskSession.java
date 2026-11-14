/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/**
 * Test-only native provider for the controller-owned DCNC fixture. The controller
 * owns all writers, DDL, maintenance and retention; file locks additionally cover
 * A/B/M/U. Neither a token nor those locks qualifies an arbitrary shared journal.
 */
class As400NativeTaskSession implements As400TaskInitialization, As400TaskRecovery {
    interface Factory {
        As400ContinuousConnectSource create(Path evidence) throws Exception;
    }

    private final Configuration configuration;
    final Path control;
    final String scenario;
    final String generation;
    final String library;
    final String epoch;
    final String topic;
    final Path evidence;
    private final Path worker;
    private final Factory factory;
    final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile As400ContinuousConnectSource source;
    private volatile Connection locked;
    private final Object snapshotIo = new Object();
    private volatile java.sql.Statement snapshotStatement;
    private volatile Exception snapshotCancellationFailure;
    private volatile String identity;
    private volatile String fingerprint;
    private volatile boolean released;
    private final long deadline = System.nanoTime() + Duration.ofMinutes(20).toNanos();
    private As400OperatorCutReader reader;
    private As400BindingHistory.Recovered recovered;
    private int reads;
    private int lastEntries;

    int tableRowLimit() {
        return 100;
    }

    As400ContinuousConnectSource nativeSource() {
        checkRunning();
        return source;
    }

    As400NativeTaskSession(Configuration configuration, Path control, Path worker, String generation, Map<String, ?> loaded, Factory factory) throws Exception {
        this.configuration = configuration;
        this.control = control;
        this.worker = worker;
        this.generation = generation;
        this.factory = factory;
        scenario = configuration.getString("name");
        library = configuration.getString("probe.library");
        topic = configuration.getString("probe.topic");
        if (library == null || !library.matches("DCNC[0-9]{4}") || !"native".equals(scenario) || !"events-native".equals(topic)
                || !generation.matches("native-w[0-9]+-[a-f0-9-]+")) {
            throw new IllegalArgumentException("Explicit native normal-task fixture required");
        }
        epoch = "native-" + library;
        evidence = control.resolve(generation);
        Files.createDirectory(evidence);
        if (loaded != null) {
            // Untrusted expectations only. Current native identity and immutable
            // binding history/policy are checked on the coordinator before output.
            identity = (String) loaded.get("journal_identity");
            fingerprint = (String) loaded.get("capture_fingerprint");
            As400Checkpoint.fromOffset(loaded, identity, fingerprint);
        }
    }

    @Override
    public As400TaskRecovery recovery() {
        return this;
    }

    @Override
    public As400Partition partition() {
        return new As400Partition("test");
    }

    @Override
    public String journalIdentity() {
        return identity;
    }

    @Override
    public String fingerprint() {
        return fingerprint;
    }

    @Override
    public JournalReceiver journal() {
        return new JournalReceiver("QSQJRN", library);
    }

    @Override
    public List<As400OperatorTableEvidence.Request> captured() {
        return List.of("A", "B", "M").stream().map(t -> new As400OperatorTableEvidence.Request(journal(), library, t)).toList();
    }

    @Override
    public void checkQualified(Configuration config) {
        checkRunning();
        As400RecoveryAcknowledgements.requireProfile(config.asMap());
        if (!"stdin-native-credentials.invalid".equals(config.getString("database.hostname"))
                || !library.equals(config.getString("database.schema")) || !"test".equals(config.getString("topic.prefix"))
                || !"true".equals(config.getString("provide.transaction.metadata"))
                || !"none".equals(config.getString("skipped.operations")) || !"1".equals(config.getString("tasks.max"))) {
            throw new IllegalArgumentException("Native normal-task profile differs");
        }
        try {
            Properties profile = new Properties();
            try (var in = Files.newInputStream(worker)) {
                profile.load(in);
            }
            if (!"enabled".equals(profile.getProperty("exactly.once.source.support")) || !"broker:9092".equals(profile.getProperty("bootstrap.servers"))
                    || !Files.readString(control.resolve(scenario + ".owned-fixture")).equals(library)) {
                throw new IllegalStateException("Pinned worker and explicit owned fixture required");
            }
        }
        catch (java.io.IOException error) {
            throw new IllegalStateException("Native runtime qualification unavailable", error);
        }
    }

    void checkRunning() {
        if (stopped.get() || Thread.currentThread().isInterrupted() || System.nanoTime() >= deadline) {
            throw new IllegalStateException("Native normal-task session stopped or deadline reached");
        }
    }

    private void ensureSource() throws Exception {
        checkQualified(configuration);
        if (source == null) {
            var created = factory.create(evidence);
            source = created;
            if (stopped.get()) {
                created.lifecycle().requestStop();
                created.guardedReader().cancelJob();
                throw new IllegalStateException("Stopped during native handle construction");
            }
            try (var connection = created.connection(false)) {
                checkRunning(); // Existing connection adapter requires CURRENT_SERVER=the configured server.
            }
        }
        var target = new Target("/QSYS.LIB/" + library + ".LIB/QSQJRN.JRN", Kind.JOURNAL);
        var objects = source.identities().collect(List.of(target));
        String current = As400OperatorCutReader.token(target, objects.objects().get(target));
        if (identity != null && !identity.equals(current)) {
            throw new IllegalArgumentException("Durable native journal identity differs from current object");
        }
        identity = current;
        checkRunning();
    }

    @Override
    public String enroll() throws Exception {
        checkQualified(configuration);
        requireToken();
        if (!Files.exists(control.resolve(scenario + ".initialize-request"))) {
            throw new IllegalStateException("Missing explicit native initialization enrollment");
        }
        requireEmptyGeneration();
        Path enrollment = control.resolve(scenario + ".enrolled");
        Files.writeString(enrollment, epoch, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        try (var channel = FileChannel.open(enrollment, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        try (var channel = FileChannel.open(control, StandardOpenOption.READ)) {
            channel.force(true);
        }
        ensureSource();
        locked = source.connection(true);
        for (String table : List.of("A", "B", "M", "U")) {
            checkRunning();
            requireToken();
            source.execute(locked, "LOCK TABLE " + library + "." + table + " IN EXCLUSIVE MODE ALLOW READ");
        }
        source.verifyLocks(locked, "locks-before.json");
        return checkHeld();
    }

    private void requireToken() throws Exception {
        if (!Files.readString(control.resolve(scenario + ".qualified")).equals(epoch)) {
            throw new IllegalStateException("Controller-owned native initialization session changed");
        }
    }

    @Override
    public String checkHeld() throws Exception {
        synchronized (snapshotIo) {
            return checkHeldSerial();
        }
    }

    private String checkHeldSerial() throws Exception {
        checkRunning();
        requireToken();
        Connection connection = locked;
        if (released || connection == null || connection.isClosed() || connection.getAutoCommit() || !validateConnection(connection)) {
            throw new IllegalStateException("Native initialization locks/session no longer held");
        }
        return epoch;
    }

    boolean validateConnection(Connection connection) throws Exception {
        return connection.isValid(2);
    }

    @Override
    public long remainingMillis() {
        checkRunning();
        return Math.max(1, (deadline - System.nanoTime()) / 1_000_000);
    }

    void trackSnapshotStatement(java.sql.Statement statement) {
        snapshotStatement = statement;
    }

    @Override
    public void scanBaseline(String token, List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
        var limits = chunkLimits().orElseThrow();
        requireEpoch(token);
        if (definitions.stream().anyMatch(d -> !library.equals(d.schema().id().schema()) || !List.of("A", "B", "M").contains(d.identity().physicalFile()))) {
            throw new IllegalArgumentException("Snapshot table outside owned native capture");
        }
        new As400NativeSnapshotScanner(locked, snapshotIo, () -> requireEpoch(token), this::checkRunning,
                this::trackSnapshotStatement, limits).scan(definitions, consumer);
        var changes = new io.debezium.relational.history.TableChanges();
        definitions.forEach(d -> changes.create(d.storedTable()));
        var document = io.debezium.document.Document.create();
        document.setArray("tables", new io.debezium.relational.history.JsonTableChangeSerializer().serialize(changes));
        Files.writeString(evidence.resolve("native-bindings.json"), io.debezium.document.DocumentWriter.defaultWriter().write(document));
        write(generation + ".snapshot-prepared.json", Map.of("source", "native JDBC bounded chunks; all cursors closed"));
    }

    @Override
    public As400NativeInitialization initializer() {
        return new As400NativeInitialization(source.acquisition, source.census, source.journals, source.identities(),
                source.tables, source.guardedReader(), source.cutLimits, source.requests, As400NativeInitializationProbe.POLICY, source.history);
    }

    @Override
    public List<Row> prepareBaseline(String token, List<As400BindingDefinition> definitions) throws Exception {
        requireEpoch(token);
        var baseline = new ArrayList<Row>();
        long bytes = 0;
        for (var definition : definitions) {
            String table = definition.identity().physicalFile();
            if (!List.of("A", "B", "M").contains(table) || !library.equals(definition.schema().id().schema())) {
                throw new IllegalArgumentException("Snapshot table outside owned native capture");
            }
            String columns = definition.schema().columns().stream().map(c -> "\"" + c.name().replace("\"", "\"\"") + "\"")
                    .collect(java.util.stream.Collectors.joining(","));
            try (var statement = locked.createStatement()) {
                statement.setQueryTimeout(5);
                statement.setMaxRows(tableRowLimit() + 1);
                try (var rows = statement.executeQuery("SELECT " + columns + " FROM " + library + "." + table + " ORDER BY ID")) {
                    int count = 0;
                    while (rows.next()) {
                        requireEpoch(token);
                        if (++count > tableRowLimit() || baseline.size() >= snapshotLimits().rows()) {
                            throw new IllegalStateException("Native snapshot exceeded its row bound");
                        }
                        Object[] values = new Object[definition.codecs().size()];
                        for (int column = 0; column < values.length; column++) {
                            values[column] = rows.getObject(column + 1);
                        }
                        var owned = As400RowData.copyOf(values, 65536);
                        bytes += owned.payloadBytes();
                        if (bytes > snapshotLimits().bytes()) {
                            throw new IllegalStateException("Native snapshot exceeded its byte bound");
                        }
                        baseline.add(new Row(definition.schema().id(), owned, Instant.now()));
                    }
                    if (rows.getWarnings() != null || statement.getWarnings() != null) {
                        throw new IllegalStateException("Native snapshot warning");
                    }
                }
            }
        }
        var changes = new io.debezium.relational.history.TableChanges();
        definitions.forEach(d -> changes.create(d.storedTable()));
        var document = io.debezium.document.Document.create();
        document.setArray("tables", new io.debezium.relational.history.JsonTableChangeSerializer().serialize(changes));
        Files.writeString(evidence.resolve("native-bindings.json"), io.debezium.document.DocumentWriter.defaultWriter().write(document));
        write(generation + ".snapshot-prepared.json", Map.of("rows", baseline.size(), "source", "native JDBC and native acquired codecs"));
        return baseline;
    }

    private void requireEpoch(String token) throws Exception {
        if (!checkHeld().equals(token)) {
            throw new IllegalArgumentException("Native initialization epoch changed");
        }
    }

    @Override
    public void checkpointAccepted(String token, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
        requireEpoch(token);
        if (!checkpoint.journalIdentity().equals(identity) || !history.initialization().orElseThrow().epoch().equals(epoch)) {
            throw new IllegalArgumentException("Native initialization checkpoint/history mismatch");
        }
        fingerprint = history.fingerprint();
        source.verifyLocks(locked, "locks-before-checkpoint.json");
        var observation = source.journals.collect();
        var attached = observation.receivers().stream().filter(r -> r.name().equals(observation.attached())).findFirst().orElseThrow();
        if (!checkpoint.completedThrough().receiver().equals(attached.name()) || !checkpoint.completedThrough().offset().equals(attached.last())) {
            throw new IllegalStateException("Native initial boundary changed before handoff");
        }
        source.save("native-boundary.json", Map.of("sequence", attached.last().toString(), "receiver", attached.name().name(), "journalIdentity", identity));
        phase("checkpoint-durable");
    }

    @Override
    public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
        ensureSource();
        var history = source.history.recover(durable, identity, As400NativeInitializationProbe.POLICY);
        if (!history.initialization().orElseThrow().epoch().equals(epoch) || !history.fingerprint().equals(fingerprint)) {
            throw new IllegalArgumentException("Recovered native initialization history differs");
        }
        reader = new As400OperatorCutReader(source.journals, source.identities(), source.journal, history, source.guardedReader(), source.cutLimits);
        recovered = history;
        write(generation + ".recovered.json", Map.of("checkpoint", durable, "fingerprint", fingerprint, "tables", history.definitions().size()));
        return history;
    }

    @Override
    public void handoff(String token, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
        requireEpoch(token);
        if (reader == null || history != recovered || !checkpoint.captureFingerprint().equals(history.fingerprint())) {
            throw new IllegalStateException("Native reader handoff has no validated continuation");
        }
        source.verifyLocks(locked, "locks-at-handoff.json");
        locked.rollback();
        locked.close();
        locked = null;
        released = true;
        write(generation + ".session-released.json", Map.of("initialization", true));
    }

    @Override
    public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
        checkQualified(configuration);
        if (reader == null || ++reads > 1000) {
            throw new IllegalStateException("Native continuation missing or bounded read limit exceeded");
        }
        if (reads == 1) {
            write(generation + ".handoff.json", Map.of("checkpoint", durable));
            if (!released) {
                write(generation + ".session-released.json", Map.of("initialization", false));
            }
        }
        // The real task only calls again after commit/readback. This reports the
        // previous accepted scan R alongside the exact durable F/W supplied now.
        Map<String, Object> progress = Map.of("checkpoint", durable, "R", scan.map(e -> Map.of("receiver", e.position().receiver().name(),
                "sequence", e.position().offset().toString())).orElse(Map.of()), "entries", lastEntries, "interval", reads - 1);
        write(generation + ".progress.json", progress);
        if (reads > 1) {
            Thread.sleep(500);
        }
        checkRunning();
        var cut = scan.isEmpty() ? reader.read(durable) : reader.readAfter(durable, scan.orElseThrow());
        checkRunning();
        source.save(String.format("interval-%04d-raw.json", reads), raw(cut));
        lastEntries = cut.entries().size();
        return cut;
    }

    private List<Map<String, Object>> raw(As400OperatorCutReader.Cut cut) throws Exception {
        var entries = new ArrayList<Map<String, Object>>();
        for (var entry : cut.entries()) {
            var row = new LinkedHashMap<String, Object>(Map.of("sequence", entry.position().offset().toString(), "receiver", entry.position().receiver().name(),
                    "code", String.valueOf(entry.header().getJournalCode()), "type", entry.header().getEntryType(), "cycle", entry.header().getCommitCycle().toString(),
                    "hex", HexFormat.of().formatHex(entry.data())));
            if (entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("CG")) {
                var decoded = new io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder().decode(entry.header(), entry.data(), 0);
                row.put("subtype", String.valueOf(decoded.changeType()));
                row.put("file", decoded.file());
            }
            entries.add(row);
        }
        return entries;
    }

    void phase(String phase) throws Exception {
        write(generation + "." + phase + ".json", Map.of("phase", phase));
        long until = System.nanoTime() + Duration.ofSeconds(180).toNanos();
        while (Files.exists(control.resolve(scenario + ".hold-" + phase)) && !Files.exists(control.resolve(generation + ".release-" + phase))) {
            checkRunning();
            if (!released && locked != null) {
                checkHeld();
            }
            if (System.nanoTime() >= until) {
                throw new IllegalStateException("Native phase deadline reached");
            }
            Thread.sleep(25);
        }
    }

    void requireEmptyGeneration() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "broker:9092");
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("enable.auto.commit", "false");
        properties.put("default.api.timeout.ms", "10000");
        properties.put("request.timeout.ms", "5000");
        properties.put("isolation.level", "read_uncommitted");
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            for (String name : List.of(topic, topic + "-A", topic + "-B", topic + "-M", topic + "-tx", topic + ".initialization", "history-" + scenario)) {
                if (consumer.partitionsFor(name).size() != 1) {
                    throw new IllegalStateException("Dedicated native one-partition generation required");
                }
                var partition = new TopicPartition(name, 0);
                if (consumer.beginningOffsets(List.of(partition)).get(partition) != 0 || consumer.endOffsets(List.of(partition)).get(partition) != 0) {
                    throw new IllegalStateException("Missing durable offset for existing native output/history");
                }
            }
        }
    }

    @Override
    public void requestStop() {
        if (stopped.compareAndSet(false, true)) {
            var current = source;
            if (current != null) {
                current.lifecycle().requestStop();
                current.guardedReader().cancelJob();
            }
            var snapshot = snapshotStatement;
            if (snapshot != null) {
                try {
                    snapshot.cancel();
                }
                catch (Exception error) {
                    snapshotCancellationFailure = error;
                }
            }
            event("stopRequested", Map.of("drained", false));
        }
    }

    @Override
    public void close() throws Exception {
        requestStop();
        if (closed.compareAndSet(false, true)) {
            // Cleanup may release physical locks, but never re-enrolls or emits a
            // successful session-release marker. The controller retains exclusion
            // and quarantines incomplete output/history until explicit teardown.
            try {
                if (locked != null) {
                    try {
                        locked.rollback();
                    }
                    finally {
                        locked.close();
                        locked = null;
                    }
                }
            }
            finally {
                if (source != null) {
                    source.close();
                }
            }
            if (snapshotCancellationFailure != null) {
                throw new IllegalStateException("Snapshot cancellation failed; drain unverified", snapshotCancellationFailure);
            }
            write(generation + ".drained.json", Map.of("stopped", true, "reads", reads));
        }
    }

    private void write(String name, Object value) {
        try {
            Path temporary = control.resolve(name + ".tmp");
            Files.writeString(temporary, As400NativeInitializationProbe.JSON.writeValueAsString(value));
            Files.move(temporary, control.resolve(name), java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        }
        catch (java.io.IOException error) {
            throw new IllegalStateException(error);
        }
    }

    private synchronized void event(String event, Map<String, ?> fields) {
        try {
            Files.writeString(control.resolve(generation + ".jsonl"), As400NativeInitializationProbe.JSON.writeValueAsString(
                    Map.of("event", event, "at", Instant.now().toString(), "fields", fields)) + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        catch (java.io.IOException error) {
            throw new IllegalStateException(error);
        }
    }
}
