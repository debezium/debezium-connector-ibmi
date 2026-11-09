/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400BindingHistoryTest.Fixture.POLICY;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.CONTROL;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.append;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.write;

import java.lang.reflect.Proxy;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.data.Envelope;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;

/**
 * Explicit test-only real edgeqa initialization/Connect integration. Controller
 * owns every fixture writer and retains receivers. Native file locks do not prove
 * a general production journal-wide fence. Credentials enter through stdin only.
 */
public class As400NativeConnectProbeConnector extends As400ConnectProbeConnector {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static Properties credentials;
    private static final Map<String, String> SOURCE = POLICY.sourcePartition();

    @Override
    public Class<? extends Task> taskClass() {
        return RuntimeTask.class;
    }

    @Override
    public boolean alterOffsets(Map<String, String> config, Map<Map<String, ?>, Map<String, ?>> offsets) {
        if (offsets.keySet().stream().anyMatch(p -> !SOURCE.equals(p))) {
            throw new IllegalArgumentException("Only the fixture partition may be altered");
        }
        return true;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("worker")) {
            credentials = new Properties();
            credentials.load(System.in);
            for (String key : List.of("database.hostname", "database.user", "database.password")) {
                if (credentials.getProperty(key, "").isBlank()) {
                    throw new IllegalArgumentException("Native credentials must be supplied through worker stdin");
                }
            }
            org.apache.kafka.connect.cli.ConnectDistributed.main(new String[]{ args[1] });
            return;
        }
        As400ConnectOwnershipProbeConnector.main(args);
    }

    static void requireIntegerTable(As400BindingDefinition definition) {
        if (definition.codecs().size() != 1 || !definition.codecs().get(0).kind().equals("BIN4")
                || !definition.schema().primaryKeyColumnNames().equals(List.of("ID"))
                || !definition.schema().columns().get(0).name().equals("ID") || definition.schema().columns().get(0).isOptional()) {
            throw new IllegalArgumentException("Native experiment requires the fully represented integer ID schema; no projection fallback");
        }
    }

    public static class RuntimeTask extends As400ConnectorTask {
        private CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext;
        private ChangeEventQueue<DataChangeEvent> output;
        private ErrorHandler errors;
        private final As400InitializationAcknowledgements acknowledgements = new As400InitializationAcknowledgements();
        private String generation;
        private String scenario;
        private String topic;
        private String epoch;
        private String library;
        private volatile As400NativeConnectSource nativeSource;
        private Thread initializer;
        private volatile boolean stopped;
        private boolean initializing;
        private int produced;
        private int polled;

        @Override
        public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {
            taskContext = super.preStart(config);
            return taskContext;
        }

        @Override
        protected ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> start(Configuration raw) {
            As400InitializationAcknowledgements.requireProfile(raw.asMap());
            var config = (As400ConnectorConfig) taskContext.getConfig();
            scenario = raw.getString("name");
            topic = raw.getString("probe.topic");
            library = raw.getString("probe.library");
            if (library == null || !library.matches("DCNC[0-9]{4}") || credentials == null) {
                throw new IllegalArgumentException("Explicit native fixture and stdin credentials required");
            }
            epoch = "native-" + library;
            generation = System.getenv("PROBE_WORKER") + "-" + UUID.randomUUID();
            Map<String, ?> loaded = context.offsetStorageReader().offset(SOURCE);
            write(generation + ".start.json", Map.of("generation", generation, "loaded", loaded == null ? Map.of() : loaded));
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
            output = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(config.getMaxBatchSize()).maxQueueSize(64)
                    .pollInterval(Duration.ofMillis(10)).loggingContextSupplier(() -> taskContext.configureLoggingContext("initialization-probe")).build();
            errors = new ErrorHandler(As400RpcConnector.class, config, output, null);
            inject("queue", output);
            inject("errorHandler", errors);
            initializer = new Thread(() -> {
                try {
                    initializeAndRead(loaded, config);
                }
                catch (Throwable error) {
                    write(generation + ".failed.json", Map.of("error", error.toString(), "stopped", stopped));
                    errors.setProducerThrowable(error);
                }
                finally {
                    if (nativeSource != null) {
                        nativeSource.close();
                    }
                    write(generation + ".drained.json", Map.of("stopped", stopped));
                }
            }, "controlled-native-initialization");
            initializer.start();
            // The coordinator only observes callbacks; real data dispatchers are
            // created for native cuts below. Supply a closeable bookkeeping stub
            // so BaseSourceTask.stop exercises its complete lifecycle.
            return new FixtureCoordinator(config, errors, org.mockito.Mockito.mock(EventDispatcher.class), null,
                    new StreamingChangeEventSource<As400Partition, As400OffsetContext>() {
                        @Override
                        public void execute(ChangeEventSourceContext context, As400Partition partition, As400OffsetContext offset) {
                            throw new IllegalStateException("No live IBM i source installed");
                        }

                        @Override
                        public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
                            append(generation, "coordinatorCommit", Map.of("checkpoint", offset));
                        }
                    });
        }

        private void initializeAndRead(Map<String, ?> loaded, As400ConnectorConfig config) throws Exception {
            initializing = loaded == null;
            checkAdmission();
            Path evidence = CONTROL.resolve(generation);
            Files.createDirectory(evidence);
            nativeSource = new As400NativeConnectSource(credentials, library, evidence, "broker:9092", "history-" + scenario);
            var source = nativeSource;
            // Read-only server identity check before any native lock or catalog work.
            try (var connection = source.connection(false)) {
                checkAdmission();
            }
            Map<String, ?> checkpoint;
            if (loaded == null) {
                requireEmptyGeneration();
                checkHeld();
                var enrollment = CONTROL.resolve(scenario + ".enrolled");
                Files.writeString(enrollment, epoch, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                try (var channel = FileChannel.open(enrollment, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
                try (var channel = FileChannel.open(CONTROL, StandardOpenOption.READ)) {
                    channel.force(true);
                }
                // Controller owns ALL writers/DDL/maintenance on this fresh journal;
                // native locks cover A/B/M/U, including uncaptured U. Not a general
                // production fence against API resources or administrative actors.
                try (var locked = source.connection(true)) {
                    for (String table : List.of("A", "B", "M", "U")) {
                        checkHeld();
                        source.execute(locked, "LOCK TABLE " + library + "." + table + " IN EXCLUSIVE MODE ALLOW READ");
                    }
                    source.verifyLocks(locked, "locks-before.json");
                    var initializer = new As400NativeInitialization(source.acquisition, source.census, source.journals, source.identities,
                            source.tables, source.guardedReader, source.cutLimits, source.requests, POLICY, source.history);
                    checkpoint = initializer.initialize(new As400NativeInitialization.Session() {
                        private final List<SourceRecord> baseline = new ArrayList<>();

                        @Override
                        public String checkHeld(io.debezium.ibmi.db2.journal.retrieve.JournalReceiver journal,
                                                List<As400OperatorTableEvidence.Request> captured)
                                throws Exception {
                            RuntimeTask.this.checkHeld();
                            if (!journal.equals(source.journal) || !captured.equals(source.requests) || locked.isClosed()
                                    || locked.getAutoCommit() || !locked.isValid(2)) {
                                throw new IllegalStateException("Native initialization session lost");
                            }
                            return epoch;
                        }

                        @Override
                        public void prepareBaseline(String token, List<As400BindingDefinition> definitions) throws Exception {
                            RuntimeTask.this.checkHeld();
                            for (var definition : definitions) {
                                requireIntegerTable(definition);
                                String table = definition.identity().physicalFile();
                                Schema row = SchemaBuilder.struct().name("snapshot." + table).field("ID", Schema.INT32_SCHEMA).optional().build();
                                Schema key = SchemaBuilder.struct().name("snapshot." + table + ".Key").field("ID", Schema.INT32_SCHEMA).build();
                                var offset = new As400OffsetContext(config);
                                offset.markSnapshotRecord(SnapshotRecord.TRUE);
                                offset.event(definition.identity().tableId(), Instant.now());
                                Envelope envelope = Envelope.defineSchema().withName("snapshot." + table + ".Envelope").withRecord(row)
                                        .withSource(offset.getSourceInfoSchema()).build();
                                try (var statement = locked.createStatement()) {
                                    statement.setQueryTimeout(5);
                                    statement.setMaxRows(101);
                                    try (var rows = statement.executeQuery("SELECT ID FROM " + library + "." + table + " ORDER BY ID")) {
                                        int count = 0;
                                        while (rows.next()) {
                                            RuntimeTask.this.checkHeld();
                                            Object value = rows.getObject(1);
                                            if (++count > 100 || !(value instanceof Integer id)) {
                                                throw new IllegalStateException("Incomplete or unsupported native snapshot");
                                            }
                                            baseline.add(new SourceRecord(SOURCE, pending("snapshot"), topic + "-" + table,
                                                    key, new Struct(key).put("ID", id), envelope.schema(),
                                                    envelope.read(new Struct(row).put("ID", id), offset.getSourceInfo(), Instant.now())));
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
                        }

                        @Override
                        public String acknowledgeBaseline(String token, As400NativeInitialization.Boundary boundary,
                                                          List<As400BindingDefinition> definitions)
                                throws Exception {
                            RuntimeTask.this.checkHeld();
                            source.save("native-boundary.json", Map.of("sequence", boundary.position().offset().toString(),
                                    "receiver", boundary.position().receiver().name(), "journalIdentity", boundary.journalIdentity()));
                            for (SourceRecord record : baseline) {
                                enqueue(record);
                            }
                            SourceRecord barrier = marker(pending("baseline-complete"), "BASELINE");
                            enqueue(barrier);
                            String receipt = awaitDurable(barrier);
                            gate("snapshot-acknowledged");
                            return receipt;
                        }

                        @Override
                        public void installCheckpoint(String token, As400Checkpoint cp, As400BindingHistory.Recovered recovered) throws Exception {
                            RuntimeTask.this.checkHeld();
                            source.verifyLocks(locked, "locks-before-checkpoint.json");
                            gate("history-durable");
                            SourceRecord barrier = marker(cp.toOffset(), "INITIAL");
                            enqueue(barrier);
                            awaitDurable(barrier);
                            gate("checkpoint-durable");
                        }
                    }).toOffset();
                    handoff(source, checkpoint);
                    locked.rollback();
                }
                initializing = false;
                write(generation + ".session-released.json", Map.of("initialization", true));
            }
            else {
                checkpoint = loaded;
                handoff(source, checkpoint);
                write(generation + ".session-released.json", Map.of("initialization", false));
            }
            // Each requested bounded cut starts from Kafka's DURABLE F/W and fresh
            // history. Open cycles are rebuilt; current file metadata is never read.
            String previous = "";
            int cuts = 0;
            long deadline = System.nanoTime() + Duration.ofMinutes(20).toNanos();
            while (!stopped && System.nanoTime() < deadline && cuts < 12) {
                checkRunning();
                Path request = CONTROL.resolve(generation + ".read");
                String label = Files.exists(request) ? Files.readString(request).trim() : "";
                if (!label.isEmpty() && !label.equals(previous)) {
                    if (!label.matches("[a-z0-9-]{1,40}")) {
                        throw new IllegalArgumentException("Bounded named native cut required");
                    }
                    previous = label;
                    cuts++;
                    Map<String, ?> durable = context.offsetStorageReader().offset(SOURCE);
                    ReplayResult next = replay(source, durable, label);
                    SourceRecord barrier = marker(next.checkpoint(), "CUT-" + label);
                    enqueue(barrier);
                    write(generation + "." + label + "-prepared.json", Map.of("checkpoint", next.checkpoint(), "from", durable, "R", next.scan()));
                    awaitDurable(barrier);
                    write(generation + "." + label + "-durable.json", Map.of("checkpoint", next.checkpoint(), "from", durable, "R", next.scan()));
                }
                Thread.sleep(50);
            }
            if (!stopped) {
                throw new IllegalStateException("Bounded native experiment deadline reached");
            }
        }

        private void handoff(As400NativeConnectSource source, Map<String, ?> checkpoint) throws Exception {
            checkAdmission();
            Object journal = checkpoint.get("journal_identity");
            if (!(journal instanceof String identity)) {
                throw new IllegalStateException("Explicit durable native F/W required");
            }
            var recovered = source.history.recover(checkpoint, identity, POLICY);
            recovered.definitions().forEach(As400NativeConnectProbeConnector::requireIntegerTable);
            if (!recovered.initialization().orElseThrow().epoch().equals(epoch)
                    || !checkpoint.equals(context.offsetStorageReader().offset(SOURCE))) {
                throw new IllegalStateException("Native reader handoff history/offset does not match");
            }
            write(generation + ".handoff.json", Map.of("checkpoint", checkpoint));
            gate("reader");
            checkAdmission();
        }

        private record ReplayResult(Map<String, String> checkpoint, String scan) {
        }

        private ReplayResult replay(As400NativeConnectSource source, Map<String, ?> saved, String label) throws Exception {
            checkRunning();
            var recovered = source.history.recover(saved, saved.get("journal_identity").toString(), POLICY);
            var cut = new As400OperatorCutReader(source.journals, source.identities, source.journal, recovered, source.guardedReader, source.cutLimits).read(saved);
            if (!cut.tables().isEmpty() || cut.historicalBindings().orElseThrow() != recovered) {
                throw new IllegalStateException("Native cut must use recovered historical bindings");
            }
            List<Map<String, Object>> raw = new ArrayList<>();
            for (var entry : cut.entries()) {
                Map<String, Object> item = new java.util.LinkedHashMap<>(Map.of("sequence", entry.position().offset().toString(),
                        "receiver", entry.position().receiver().name(), "code", String.valueOf(entry.header().getJournalCode()),
                        "type", entry.header().getEntryType(), "cycle", entry.header().getCommitCycle().toString(),
                        "hex", java.util.HexFormat.of().formatHex(entry.data())));
                if (entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("CG")) {
                    var decoded = new io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder().decode(entry.header(), entry.data(), 0);
                    item.put("subtype", String.valueOf(decoded.changeType()));
                    item.put("file", decoded.file());
                }
                raw.add(item);
            }
            source.save(label + "-raw.json", raw);
            Map<String, TableBinding> bindings = recovered.definitions().stream()
                    .collect(Collectors.toMap(d -> d.identity().physicalFile(), d -> d.binding(65536)));
            try (var replay = new As400ReplayCoordinator(saved, recovered.journalIdentity(), recovered.fingerprint(), cut.order(),
                    new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000), 65536, 1000, recovered.resolver(65536),
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), SOURCE, record -> {
                        checkRunning();
                        String table = record.topic().startsWith("test.") ? record.topic().substring(5) : "";
                        String destination = List.of("A", "B", "M").contains(table) ? topic + "-" + table : topic;
                        return enqueue(record.newRecord(destination, null, record.keySchema(), record.key(), record.valueSchema(), record.value(), record.timestamp()));
                    }, new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), true);
                    var dispatcher = new As400IncrementalLifecycleTest.Output(bindings, true).dispatcher) {
                for (var entry : cut.entries()) {
                    checkRunning();
                    if (replay.accept(entry.header(), entry.data(), 0, entry.position()).isPresent()) {
                        replay.deliver(dispatcher);
                    }
                }
                replay.verifyOrdinaryCut();
                return new ReplayResult(replay.checkpoint().toOffset(), replay.scanPosition().map(p -> p.offset().toString())
                        .orElse(replay.checkpoint().completedThrough().offset().toString()));
            }
        }

        private void requireEmptyGeneration() {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", "broker:9092");
            properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
            properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
            properties.put("enable.auto.commit", "false");
            properties.put("default.api.timeout.ms", "10000");
            properties.put("request.timeout.ms", "5000");
            properties.put("isolation.level", "read_uncommitted");
            try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
                for (String name : List.of(topic, topic + "-A", topic + "-B", topic + "-M", "history-" + scenario)) {
                    if (consumer.partitionsFor(name).size() != 1) {
                        throw new IllegalStateException("Dedicated one-partition initialization generation required");
                    }
                    var partition = new TopicPartition(name, 0);
                    if (consumer.beginningOffsets(List.of(partition)).get(partition) != 0
                            || consumer.endOffsets(List.of(partition)).get(partition) != 0) {
                        throw new IllegalStateException("Missing durable offset for an existing output/history generation");
                    }
                }
            }
        }

        private Map<String, String> pending(String phase) {
            return Map.of("initialization.version", "test-only-1", "initialization.epoch", epoch, "initialization.phase", phase);
        }

        private SourceRecord marker(Map<String, ?> offset, String kind) {
            return new SourceRecord(SOURCE, offset, topic, SchemaBuilder.string().name("probe.InitializationBarrier").build(), kind);
        }

        private String awaitDurable(SourceRecord record) throws Exception {
            String receipt = acknowledgements.await(record, Duration.ofSeconds(120));
            checkAdmission();
            if (!record.sourceOffset().equals(context.offsetStorageReader().offset(SOURCE))) {
                throw new IllegalStateException("Acknowledged record lacks matching durable source offset");
            }
            return receipt;
        }

        private void checkRunning() throws InterruptedException {
            if (stopped || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Test task stopped");
            }
        }

        private void checkAdmission() throws Exception {
            if (initializing) {
                checkHeld();
            }
            else {
                checkRunning();
            }
        }

        private void checkHeld() throws Exception {
            checkRunning();
            var qualification = CONTROL.resolve(scenario + ".qualified");
            if (!Files.exists(qualification) || !Files.readString(qualification).equals(epoch)) {
                throw new IllegalStateException("Qualified controlled journal-wide session required");
            }
        }

        private void gate(String phase) throws Exception {
            write(generation + "." + phase + ".json", Map.of("phase", phase));
            long deadline = System.nanoTime() + Duration.ofSeconds(180).toNanos();
            while (Files.exists(CONTROL.resolve(scenario + ".hold-" + phase))
                    && !Files.exists(CONTROL.resolve(generation + ".release-" + phase))) {
                checkAdmission();
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Bounded phase gate expired: " + phase);
                }
                Thread.sleep(25);
            }
            checkAdmission();
        }

        private void inject(String name, Object value) {
            try {
                var field = As400ConnectorTask.class.getDeclaredField(name);
                field.setAccessible(true);
                field.set(this, value);
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }

        private boolean enqueue(SourceRecord record) throws Exception {
            checkAdmission();
            String operation = record.value() instanceof Struct value && value.schema().field("op") != null ? value.getString("op") : "marker";
            record.headers().addString("probe", JSON.writeValueAsString(Map.of("op", operation, "index", produced++, "generation", generation,
                    "checkpoint", record.sourceOffset())));
            acknowledgements.register(record);
            output.enqueue(new DataChangeEvent(record));
            return true;
        }

        @Override
        protected List<SourceRecord> doPoll() throws InterruptedException {
            if (errors.getProducerThrowable() != null) {
                throw new IllegalStateException("Initialization failed", errors.getProducerThrowable());
            }
            try {
                var permit = CONTROL.resolve(generation + ".permit");
                if (!stopped && Files.exists(permit) && Integer.parseInt(Files.readString(permit).trim()) > polled) {
                    var records = super.doPoll();
                    polled += records.size();
                    return records;
                }
            }
            catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
            Thread.sleep(25);
            return List.of();
        }

        @Override
        public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
            try {
                acknowledgements.record(record, metadata);
                super.commitRecord(record, metadata);
                append(generation, "commitRecord", Map.of("offset", record.sourceOffset()));
            }
            catch (RuntimeException e) {
                errors.setProducerThrowable(e);
                throw e;
            }
        }

        @Override
        public void commit() throws InterruptedException {
            super.commit();
            acknowledgements.committed();
        }

        @Override
        protected void doStop() {
            stopped = true;
            acknowledgements.stop();
            if (nativeSource != null) {
                nativeSource.lifecycle.requestStop();
                nativeSource.guardedReader.cancelJob();
            }
            if (initializer != null) {
                initializer.interrupt();
                append(generation, "stopRequested", Map.of("drained", !initializer.isAlive()));
            }
            super.doStop();
        }
    }

    private static final class FixtureCoordinator extends ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> {
        private FixtureCoordinator(As400ConnectorConfig config, ErrorHandler errors, EventDispatcher<As400Partition, TableId> dispatcher,
                                   DatabaseSchema<TableId> schema, StreamingChangeEventSource<As400Partition, As400OffsetContext> source) {
            super(Offsets.of(new As400Partition("test"), null), errors, As400RpcConnector.class, config, null, null, dispatcher, schema, null, null, null);
            streamingSource = source;
            snapshotMetrics = (SnapshotChangeEventSourceMetrics<As400Partition>) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ SnapshotChangeEventSourceMetrics.class }, (proxy, method, args) -> null);
            streamingMetrics = (StreamingChangeEventSourceMetrics<As400Partition>) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ StreamingChangeEventSourceMetrics.class }, (proxy, method, args) -> null);
        }
    }
}
