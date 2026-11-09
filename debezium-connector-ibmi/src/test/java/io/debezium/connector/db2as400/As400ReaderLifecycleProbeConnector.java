/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400ConnectProbeConnector.CONTROL;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.FINGERPRINT;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.IDENTITY;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.JID;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.ORDER;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.PARTITION;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.RECEIVER;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.append;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.event;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.events;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.pos;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.write;

import java.lang.reflect.Proxy;
import java.math.BigInteger;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.data.Envelope;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;

/**
 * Incremental reader experiment only. Actual task/dispatcher with synthetic startup;
 * Connect owns publication and offsets. No receipt store, owner registry or IBM i connection.
 * Test-only transport is strict and cannot connect to IBM i. Existing probes remain unchanged.
 */
public class As400ReaderLifecycleProbeConnector extends As400ConnectOwnershipProbeConnector {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TableId TABLE = new TableId("DB", "LIB", "TEST");

    @Override
    public Class<? extends Task> taskClass() {
        return RuntimeTask.class;
    }

    public static class RuntimeTask extends As400ConnectorTask {
        private CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext;
        private ChangeEventQueue<DataChangeEvent> output;
        private ErrorHandler errors;
        private As400BatchDispatcher batchOutput;
        private String generation;
        private String topic;
        private volatile int produced;
        private As400ReaderFixture reader;
        private Thread readerThread;
        private int polled;
        private volatile boolean stopped;

        @Override
        public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {
            taskContext = super.preStart(config);
            return taskContext;
        }

        @Override
        protected ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> start(Configuration raw) {
            As400ConnectorConfig config = (As400ConnectorConfig) taskContext.getConfig();
            topic = raw.getString("probe.topic");
            generation = System.getenv("PROBE_WORKER") + "-" + UUID.randomUUID();
            Map<String, ?> loaded = context.offsetStorageReader().offset(PARTITION);
            write(generation + ".start.json", Map.of("generation", generation, "loaded", loaded == null ? Map.of() : loaded));
            // Every task is resume-only. The controller explicitly seeds a synthetic
            // checkpoint through Connect's stopped-connector offsets API before startup.
            // Missing offsets NEVER manufacture a new W=1, including on reassignment.
            Map<String, ?> initial = requireCheckpoint(loaded);
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
            output = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(config.getMaxBatchSize()).maxQueueSize(64)
                    .pollInterval(Duration.ofMillis(10)).loggingContextSupplier(() -> taskContext.configureLoggingContext("probe")).build();
            errors = new ErrorHandler(As400RpcConnector.class, config, output, null);
            // A test fixture injection, not a runtime option or an alternate implementation of doPoll().
            inject("queue", output);
            inject("errorHandler", errors);
            As400OffsetContext source = new As400OffsetContext(config, new JournalProcessedPosition(BigInteger.ONE, RECEIVER, Instant.EPOCH, true));
            FixtureSchema schema = new FixtureSchema(source);
            CompleteRowEntryDecoder decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
            TableIdentity table = new TableIdentity(TABLE, "TEST", "TEST", JID, As400ConnectProbeConnector.MEMBER_JID, "FORMAT_V1");
            try {
                reader = new As400ReaderFixture(generation, config,
                        As400Checkpoint.fromOffset(initial, IDENTITY, FINGERPRINT).replayFrom().offset().longValue(),
                        operation -> append(generation, "native", operation));
                reader.scan.getPosition().setProcessed(!As400Checkpoint.fromOffset(initial, IDENTITY, FINGERPRINT).replayInclusive());
            }
            catch (Exception e) {
                throw new IllegalStateException("Could not install test transport", e);
            }
            var initialized = new java.util.concurrent.CountDownLatch(1);
            readerThread = new Thread(() -> {
                try {
                    batchOutput = new As400BatchDispatcher(config, new As400Partition("synthetic"), schema,
                            new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
                                @Override
                                public String dataChangeTopic(TableId id) {
                                    return topic;
                                }

                                @Override
                                public String transactionTopic() {
                                    return topic + "-tx";
                                }

                                @Override
                                public String heartbeatTopic() {
                                    return topic + "-hb";
                                }
                            }, new DebeziumHeaderProducer(taskContext), Clock.system(), 20);
                    initialized.countDown();
                    readIncrementally(initial, table, decoder);
                }
                catch (Throwable error) {
                    errors.setProducerThrowable(error);
                    initialized.countDown();
                }
            }, "fixture-reader-" + generation);
            readerThread.setDaemon(true);
            readerThread.start();
            try {
                if (!initialized.await(30, java.util.concurrent.TimeUnit.SECONDS) || batchOutput == null) {
                    throw new IllegalStateException("Reader dispatcher initialization failed", errors.getProducerThrowable());
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            EventDispatcher<As400Partition, TableId> dispatcher = batchOutput.eventDispatcher();
            return new FixtureCoordinator(config, errors, dispatcher, schema, new StreamingChangeEventSource<As400Partition, As400OffsetContext>() {
                @Override
                public void execute(ChangeEventSourceContext context, As400Partition partition, As400OffsetContext offset) {
                    throw new IllegalStateException("No IBM i source is installed in the fixture");
                }

                @Override
                public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
                    append(generation, "coordinatorCommit", Map.of("checkpoint", offset));
                    // Observation only: no second checkpoint authority or publication permission.
                }
            });
        }

        private void readIncrementally(Map<String, ?> initial, TableIdentity table, CompleteRowEntryDecoder decoder) {
            Map<String, ?> lastCheckpoint = initial;
            String outcome = "completed";
            try (As400ReplayCoordinator replay = new As400ReplayCoordinator(initial, IDENTITY, FINGERPRINT, ORDER,
                    new Limits(4, 20, 10, 4096), 1024, 20,
                    (file, library) -> new As400SourceNormalizer.TableBinding(table, decoder),
                    (metadata, control) -> SavepointRollback.UNRESOLVED, PARTITION, this::enqueue)) {
                As400Checkpoint resume = replay.checkpoint();
                for (As400ConnectProbeConnector.Event event : events()) {
                    int comparison = ORDER.compare(event.position(), resume.replayFrom());
                    if (comparison < 0 || (comparison == 0 && !resume.replayInclusive())) {
                        continue;
                    }
                    if (stopped) {
                        break;
                    }
                    reader.read(event, () -> !stopped, (sequence, journal, header) -> {
                        try {
                            journal.decode((decoded, data, offset) -> {
                                // Raw bytes and headers came through the real decoder/read loop.
                                if (replay.accept(decoded, data, offset, pos(sequence.longValue())).isPresent()) {
                                    replay.deliver(batchOutput);
                                    batchOutput.publishCheckpoint(replay, true);
                                    write(generation + ".prepared.json", Map.of("count", produced,
                                            "checkpoint", replay.checkpoint().toOffset(), "scan", sequence.toString()));
                                }
                                return null;
                            });
                        }
                        catch (Exception e) {
                            throw new As400RpcConnection.RpcException("Incremental fixture decode failed", e);
                        }
                    }, () -> {
                        if (event.position().offset().intValueExact() == 7 && generation.startsWith("partition-w1-")
                                && resume.completedThrough().equals(pos(1))) {
                            holdNativeRead();
                        }
                        return null;
                    });
                    lastCheckpoint = replay.checkpoint().toOffset();
                    append(generation, "scan", Map.of("R", reader.scan.getPosition().getOffset().toString(), "checkpoint", lastCheckpoint));
                }
                if (!stopped) {
                    replay.verifyEndOfAvailableJournal();
                }
            }
            catch (Throwable error) {
                outcome = "error";
                append(generation, "native-error", Map.of("error", error.toString(), "stopped", stopped));
                if (!stopped) {
                    errors.setProducerThrowable(error);
                }
            }
            finally {
                try {
                    reader.close();
                }
                catch (Throwable error) {
                    outcome = "cleanup-error";
                    append(generation, "native-cleanup-error", Map.of("error", error.toString()));
                    errors.setProducerThrowable(error);
                }
                batchOutput.close();
                write(generation + ".native-finished.json", Map.of("outcome", outcome,
                        "R", reader.scan.getPosition().getOffset().toString(), "checkpoint", lastCheckpoint, "stopped", stopped));
            }
        }

        private void holdNativeRead() throws Exception {
            write(generation + ".native-blocked.json", Map.of("sequence", 7));
            long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
            while (!Files.exists(CONTROL.resolve(generation + ".native-release"))) {
                if (Files.exists(CONTROL.resolve(generation + ".native-disconnect"))
                        && !Files.exists(CONTROL.resolve(generation + ".native-disconnected.json"))) {
                    reader.disconnectTransport();
                    write(generation + ".native-disconnected.json", Map.of("sequence", 7));
                }
                if (System.nanoTime() > deadline) {
                    write(generation + ".hook-timeout.json", Map.of("hook", "native-read"));
                    throw new IllegalStateException("Native fault gate timed out");
                }
                try {
                    Thread.sleep(25);
                }
                catch (InterruptedException ignored) {
                    // Deliberately uncooperative transport: cancellation need not stop
                    // an already-running native call. The explicit bounded gate does.
                }
            }
            if (Files.exists(CONTROL.resolve("native-late-error"))) {
                throw new java.io.IOException("controlled late transport error");
            }
        }

        private void inject(String name, Object value) {
            try {
                java.lang.reflect.Field field = As400ConnectorTask.class.getDeclaredField(name);
                field.setAccessible(true);
                field.set(this, value);
            }
            catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }

        private boolean enqueue(SourceRecord record) throws Exception {
            Map<String, Object> probe = new HashMap<>();
            if (record.value() == null) {
                probe.put("op", "tombstone");
                probe.put("sequence", -1);
            }
            else {
                Struct value = (Struct) record.value();
                if (value.schema().field("op") != null) {
                    String operation = value.getString("op");
                    probe.put("op", operation.equals("c") ? "c:" + value.getStruct("after").getInt32("ID") : operation);
                    probe.put("sequence", Long.parseLong(value.getStruct("source").getString("sequence")));
                }
                else if (value.schema().field("status") != null) {
                    probe.put("op", value.getString("status"));
                    probe.put("sequence", value.getInt64("ts_ms") / 1000);
                }
                else {
                    probe.put("op", "HB");
                    probe.put("sequence", Long.parseLong(record.sourceOffset().get("completed_through.sequence").toString()));
                }
            }
            probe.put("generation", generation);
            probe.put("index", produced++);
            probe.put("checkpoint", record.sourceOffset());
            record.headers().addString("probe", JSON.writeValueAsString(probe));
            // Preserve the dispatcher's real key, envelope, source metadata and headers.
            output.enqueue(new DataChangeEvent(record));
            return true;
        }

        @Override
        protected List<SourceRecord> doPoll() throws InterruptedException {
            if (!stopped) {
                if (errors.getProducerThrowable() != null) {
                    throw new IllegalStateException("Fixture pipeline failed", errors.getProducerThrowable());
                }
                try {
                    var permit = CONTROL.resolve(generation + ".permit");
                    if (polled < produced && Files.exists(permit) && Integer.parseInt(Files.readString(permit).trim()) > polled) {
                        List<SourceRecord> records = super.doPoll();
                        for (SourceRecord record : records) {
                            append(generation, "poll", Map.of("value", probe(record), "offset", record.sourceOffset()));
                        }
                        polled += records.size();
                        return records;
                    }
                }
                catch (java.io.IOException e) {
                    throw new IllegalStateException(e);
                }
                // BaseSourceTask holds its lifecycle lock while invoking doPoll().
                // A fixture gate must yield empty polls so stop() can take that lock.
                Thread.sleep(25);
            }
            return List.of();
        }

        @Override
        public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
            Map<String, Object> probe = probe(record);
            try {
                super.commitRecord(record, metadata);
                append(generation, "commitRecord", Map.of("value", probe, "offset", record.sourceOffset()));
            }
            catch (RuntimeException e) {
                append(generation, "ackRejected", Map.of("value", probe, "error", e.toString()));
                errors.setProducerThrowable(e);
                throw e;
            }
        }

        @Override
        public void commit() throws InterruptedException {
            super.commit();
            append(generation, "commit", Map.of());
        }

        @Override
        protected void doStop() {
            stopped = true;
            if (reader != null) {
                try {
                    reader.close();
                    append(generation, "native-stop", Map.of("readerAlive", readerThread.isAlive()));
                }
                catch (Exception e) {
                    throw new IllegalStateException("Native fixture cleanup failed", e);
                }
            }
            if (generation != null) {
                append(generation, "stop", Map.of());
            }
            super.doStop();
        }

        private Map<String, Object> probe(SourceRecord record) {
            try {
                return JSON.readValue(record.headers().lastWithName("probe").value().toString(), Map.class);
            }
            catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }

    }

    private static final class FixtureSchema implements DatabaseSchema<TableId> {
        private final TableSchema schema;

        private FixtureSchema(As400OffsetContext offset) {
            Schema row = SchemaBuilder.struct().name("probe.Value").field("ID", Schema.INT32_SCHEMA).optional().build();
            Schema key = SchemaBuilder.struct().name("probe.Key").field("ID", Schema.INT32_SCHEMA).build();
            Envelope envelope = Envelope.defineSchema().withName("probe.Envelope").withRecord(row).withSource(offset.getSourceInfoSchema())
                    .withTransaction(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA).build();
            schema = new TableSchema(TABLE, key, data -> new Struct(key).put("ID", data[0]), envelope, row, data -> new Struct(row).put("ID", data[0]));
        }

        @Override
        public TableSchema schemaFor(TableId id) {
            return TABLE.equals(id) ? schema : null;
        }

        @Override
        public boolean tableInformationComplete() {
            return true;
        }

        @Override
        public boolean isHistorized() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    private static final class FixtureCoordinator extends ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> {
        private FixtureCoordinator(As400ConnectorConfig config, ErrorHandler errors, EventDispatcher<As400Partition, TableId> dispatcher,
                                   DatabaseSchema<TableId> schema, StreamingChangeEventSource<As400Partition, As400OffsetContext> source) {
            super(Offsets.of(new As400Partition("synthetic"), null), errors, As400RpcConnector.class, config, null, null, dispatcher, schema, null, null, null);
            streamingSource = source;
            snapshotMetrics = (SnapshotChangeEventSourceMetrics<As400Partition>) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ SnapshotChangeEventSourceMetrics.class }, (proxy, method, args) -> null);
            streamingMetrics = (StreamingChangeEventSourceMetrics<As400Partition>) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{ StreamingChangeEventSourceMetrics.class }, (proxy, method, args) -> null);
        }
    }
}
