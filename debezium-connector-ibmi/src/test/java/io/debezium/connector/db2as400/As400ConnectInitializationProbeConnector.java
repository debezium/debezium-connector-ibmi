/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400ConnectProbeConnector.CONTROL;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.append;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.write;
import static io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture.POLICY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.data.Envelope;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.schema.DatabaseSchema;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/**
 * Local-only native initialization/Connect integration. Native observations and
 * the journal-wide session are controlled fixtures, never a production fence.
 * Kafka owns publication and offsets; no custom owner or receipt gate is used.
 */
public class As400ConnectInitializationProbeConnector extends As400ConnectProbeConnector {
    private static final ObjectMapper JSON = new ObjectMapper();
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
        As400ConnectOwnershipProbeConnector.main(args);
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
            epoch = "controlled-" + scenario;
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
            initializeOutput(output, errors);
            initializer = new Thread(() -> {
                try {
                    initializeAndRead(loaded, config);
                }
                catch (Throwable error) {
                    write(generation + ".failed.json", Map.of("error", error.toString(), "stopped", stopped));
                    errors.setProducerThrowable(error);
                }
                finally {
                    write(generation + ".drained.json", Map.of("stopped", stopped));
                }
            }, "controlled-native-initialization");
            initializer.start();
            return new FixtureCoordinator(config, errors, null, null, new StreamingChangeEventSource<As400Partition, As400OffsetContext>() {
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
            var setup = new As400NativeInitializationTest.Setup(CONTROL);
            var fixture = setup.fixture;
            fixture.factory = As400KafkaBindingHistory::new;
            fixture.config = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "broker:9092")
                    .with(KafkaSchemaHistory.TOPIC, "history-" + scenario).with(SchemaHistory.NAME, scenario)
                    .with(SchemaHistory.INTERNAL_CONNECTOR_CLASS, As400RpcConnector.class.getName())
                    .with(SchemaHistory.INTERNAL_CONNECTOR_ID, scenario).with(KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS, 50)
                    .with(KafkaSchemaHistory.RECOVERY_POLL_ATTEMPTS, 40).build();
            Map<String, ?> checkpoint;
            if (loaded == null) {
                requireEmptyGeneration();
                checkHeld();
                // Durable, single-use TEST enrollment, not a publication owner/receipt.
                // A missing offset after any attempt is quarantined, even before history exists.
                var enrollment = CONTROL.resolve(scenario + ".enrolled");
                Files.writeString(enrollment, epoch, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                try (var channel = FileChannel.open(enrollment, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
                try (var channel = FileChannel.open(CONTROL, StandardOpenOption.READ)) {
                    channel.force(true);
                }
                checkpoint = setup.initializer().initialize(new As400NativeInitialization.Session() {
                    private final List<SourceRecord> baseline = new ArrayList<>();

                    @Override
                    public String checkHeld(io.debezium.ibmi.db2.journal.retrieve.JournalReceiver journal,
                                            List<As400OperatorTableEvidence.Request> captured)
                            throws Exception {
                        RuntimeTask.this.checkHeld();
                        if (!journal.equals(As400RecoveryTestSupport.Fixture.JOURNAL) || !captured.equals(setup.requests)) {
                            throw new IllegalStateException("Qualified session scope changed");
                        }
                        return epoch;
                    }

                    @Override
                    public void prepareBaseline(String token, List<As400BindingDefinition> definitions) throws Exception {
                        RuntimeTask.this.checkHeld();
                        // Real adapter ordering; fixture preparation cannot append source entries.
                        setup.session.prepareBaseline(token, definitions);
                        for (var definition : definitions) {
                            String table = definition.identity().tableId().table();
                            Schema row = SchemaBuilder.struct().name("snapshot." + table).field("ID", Schema.INT32_SCHEMA).optional().build();
                            Schema key = SchemaBuilder.struct().name("snapshot." + table + ".Key").field("ID", Schema.INT32_SCHEMA).build();
                            var source = new As400OffsetContext(config);
                            source.markSnapshotRecord(SnapshotRecord.TRUE);
                            source.event(definition.identity().tableId(), Instant.EPOCH);
                            Envelope envelope = Envelope.defineSchema().withName("snapshot." + table + ".Envelope").withRecord(row)
                                    .withSource(source.getSourceInfoSchema()).build();
                            for (int id = 1; id <= 3; id++) {
                                baseline.add(new SourceRecord(SOURCE, pending("snapshot"), topic + "-" + table, key, new Struct(key).put("ID", id),
                                        envelope.schema(), envelope.read(new Struct(row).put("ID", id), source.getSourceInfo(), Instant.EPOCH)));
                            }
                        }
                    }

                    @Override
                    public String acknowledgeBaseline(String token, As400NativeInitialization.Boundary boundary,
                                                      List<As400BindingDefinition> definitions)
                            throws Exception {
                        RuntimeTask.this.checkHeld();
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
                        gate("history-durable");
                        SourceRecord barrier = marker(cp.toOffset(), "INITIAL");
                        enqueue(barrier);
                        awaitDurable(barrier);
                        gate("checkpoint-durable");
                    }
                }).toOffset();
            }
            else {
                // Pending/absent/invalid offsets never trigger a new snapshot or manufacture W.
                checkpoint = loaded;
            }
            checkAdmission();
            var recovered = fixture.history().recover(checkpoint, fixture.journalIdentity, POLICY);
            if (!recovered.initialization().orElseThrow().epoch().equals(epoch)) {
                throw new IllegalStateException("Initialization session does not match recovered history");
            }
            Map<String, ?> durable = context.offsetStorageReader().offset(SOURCE);
            if (!checkpoint.equals(durable)) {
                throw new IllegalStateException("Reader handoff does not match durable Connect offset");
            }
            write(generation + ".handoff.json", Map.of("checkpoint", durable));
            gate("reader");
            checkAdmission();
            boolean firstHandoff = initializing;
            initializing = false;
            // The test controller's source fence ends at admitted handoff. Ordinary
            // recovery/output uses task cancellation and Kafka fencing, not a writer pause.
            write(generation + ".session-released.json", Map.of("initialization", firstHandoff));
            // Fresh history-backed raw reader and strict decoder/normalizer/buffer/dispatcher.
            // R can run to 64 while completed output W is 59. Neither is assumed durable here.
            ReplayResult next = replay(fixture, durable);
            enqueue(marker(next.checkpoint(), "RECOVERY"));
            write(generation + ".prepared.json", Map.of("checkpoint", next.checkpoint(), "R", next.scan()));
        }

        private record ReplayResult(Map<String, String> checkpoint, String scan) {
        }

        private ReplayResult replay(Fixture fixture, Map<String, ?> saved) throws Exception {
            checkRunning();
            var recovered = fixture.history().recover(saved, fixture.journalIdentity, POLICY);
            var journals = mock(As400OperatorJournalEvidence.class);
            var identities = mock(As400JdbcNativeIdentityEvidence.class);
            var transport = mock(RetrieveJournal.class);
            when(journals.collect()).thenReturn(new As400OperatorJournalEvidence.Observation(Fixture.JOURNAL, Fixture.RECEIVER,
                    List.of(new As400OperatorJournalEvidence.Receiver(Fixture.RECEIVER, "ATTACHED", BigInteger.valueOf(29), BigInteger.valueOf(64),
                            Optional.empty(), Optional.empty(), "NO", "YES", "YES", "NO", "NO"))));
            when(identities.collect(any())).thenAnswer(i -> new Candidates(((List<Target>) i.getArgument(0)).stream()
                    .collect(Collectors.toMap(t -> t, fixture.nativeObjects::get))));
            when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
                checkRunning();
                JournalPosition from = i.getArgument(0);
                JournalPosition to = i.getArgument(1);
                return new RawJournalPage(fixture.entries.stream().filter(e -> e.position().offset().compareTo(from.offset()) >= 0
                        && e.position().offset().compareTo(to.offset()) <= 0).toList(), Optional.empty(), 65536);
            });
            var cut = new As400OperatorCutReader(journals, identities, Fixture.JOURNAL, recovered, transport,
                    new As400OperatorCutReader.Limits(10, 100, 1_000_000)).read(saved);
            if (!cut.tables().isEmpty() || cut.historicalBindings().orElseThrow() != recovered) {
                throw new IllegalStateException("Reader handoff must use recovered bindings, not current table metadata");
            }
            Map<String, TableBinding> bindings = recovered.definitions().stream()
                    .collect(Collectors.toMap(d -> d.identity().physicalFile(), d -> d.binding(1024)));
            try (var replay = new As400ReplayCoordinator(saved, fixture.journalIdentity, recovered.fingerprint(), cut.order(),
                    new As400TransactionBuffer.Limits(16, 1000, 128, 65536), 1024, 100, recovered.resolver(1024),
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), SOURCE, record -> {
                        checkRunning();
                        String destination = record.topic().equals("test.A") ? topic + "-A" : record.topic().equals("test.B") ? topic + "-B" : topic;
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
                return new ReplayResult(replay.checkpoint().toOffset(), replay.scanPosition().orElseThrow().offset().toString());
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
                for (String name : List.of(topic, topic + "-A", topic + "-B", "history-" + scenario)) {
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
            checkHeld();
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
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
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
