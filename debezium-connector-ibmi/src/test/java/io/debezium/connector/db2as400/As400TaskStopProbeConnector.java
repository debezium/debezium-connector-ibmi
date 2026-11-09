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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.math.BigInteger;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
import io.debezium.pipeline.metrics.spi.ChangeEventSourceMetricsFactory;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.schema.DatabaseSchema;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Incremental reader experiment only. Actual task/dispatcher with synthetic startup;
 * Connect owns publication and offsets. No receipt store, owner registry or IBM i connection.
 * Test-only transport is strict and cannot connect to IBM i. Existing probes remain unchanged.
 */
public class As400TaskStopProbeConnector extends As400ConnectOwnershipProbeConnector {
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
        private As400TaskStopFixture fixture;
        private FixtureCoordinator coordinator;
        private final CountDownLatch initialized = new CountDownLatch(1);
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
                fixture = new As400TaskStopFixture(generation, config,
                        As400Checkpoint.fromOffset(initial, IDENTITY, FINGERPRINT).replayFrom().offset().longValue(),
                        operation -> append(generation, "native", operation));
                reader = fixture.reader;
                fixture.install(this);
                doAnswer(invocation -> {
                    invocation.callRealMethod();
                    write(generation + ".terminal-requested.json", Map.of("readerAlive", readerThread != null && readerThread.isAlive()));
                    append(generation, "terminal-requested", Map.of());
                    return null;
                }).when(reader.lifecycle).requestStop();
                reader.scan.getPosition().setProcessed(!As400Checkpoint.fromOffset(initial, IDENTITY, FINGERPRINT).replayInclusive());
            }
            catch (Exception e) {
                throw new IllegalStateException("Could not install test transport", e);
            }
            EventDispatcher<As400Partition, TableId> lifecycleDispatcher = mock(EventDispatcher.class, invocation -> {
                if (invocation.getMethod().getName().equals("close") && batchOutput != null) {
                    batchOutput.close();
                }
                return null;
            });
            coordinator = new FixtureCoordinator(config, errors, lifecycleDispatcher, schema,
                    context -> readStreaming(context, raw, initial, table, decoder, schema), generation);
            coordinator.start(taskContext, output, new As400EventMetadataProvider());
            try {
                if (!initialized.await(30, java.util.concurrent.TimeUnit.SECONDS) || batchOutput == null) {
                    throw new IllegalStateException("Streaming initialization failed", errors.getProducerThrowable());
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return coordinator;
        }

        private void readStreaming(ChangeEventSourceContext context, Configuration raw, Map<String, ?> initial,
                                   TableIdentity table, CompleteRowEntryDecoder decoder, FixtureSchema schema) {
            readerThread = Thread.currentThread();
            AtomicReference<Map<String, ?>> lastCheckpoint = new AtomicReference<>(initial);
            String outcome = "completed";
            As400ConnectorConfig config = (As400ConnectorConfig) taskContext.getConfig();
            try (As400ReplayCoordinator replay = new As400ReplayCoordinator(initial, IDENTITY, FINGERPRINT, ORDER,
                    new Limits(4, 20, 10, 4096), 1024, 20,
                    (file, library) -> new As400SourceNormalizer.TableBinding(table, decoder),
                    (metadata, control) -> SavepointRollback.UNRESOLVED, PARTITION, this::enqueue)) {
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
                As400Checkpoint resume = replay.checkpoint();
                var input = events().stream().filter(event -> {
                    int comparison = ORDER.compare(event.position(), resume.replayFrom());
                    return comparison > 0 || comparison == 0 && resume.replayInclusive();
                }).iterator();
                String mode = Files.readString(CONTROL.resolve("terminal-mode")).trim();
                boolean old = generation.startsWith("partition-w1-") && resume.completedThrough().equals(pos(1));
                reader.duringMetadata = connection -> holdNativeRead();
                doAnswer(invocation -> {
                    if (!context.isRunning() || !input.hasNext()) {
                        return io.debezium.ibmi.db2.journal.retrieve.RetrievalState.NotCalled;
                    }
                    var event = input.next();
                    boolean held = old && event.position().offset().intValueExact() == 7;
                    reader.prepare(event, () -> {
                        if (held && mode.startsWith("rpc")) {
                            holdNativeRead();
                        }
                        return null;
                    });
                    if (held && mode.startsWith("metadata")) {
                        reader.rpc.getCurrentPosition();
                    }
                    Object state = invocation.callRealMethod();
                    lastCheckpoint.set(replay.checkpoint().toOffset());
                    append(generation, "scan", Map.of("R", reader.scan.getPosition().getOffset().toString(), "checkpoint", lastCheckpoint.get()));
                    return state;
                }).when(reader.rpc).getJournalEntries(any(), any(), any(), any());
                // Adapt only unwired D/CG output/idle heartbeat policy. The real
                // streaming execute/retry loop and real RPC methods run unchanged.
                EventDispatcher<As400Partition, TableId> streamingDispatcher = mock(EventDispatcher.class, invocation -> {
                    if (invocation.getMethod().getName().equals("dispatchHeartbeatEventAlsoToIncrementalSnapshot")) {
                        batchOutput.publishCheckpoint(replay, false);
                        return null;
                    }
                    throw new AssertionError("Unexpected streaming dispatcher action " + invocation.getMethod().getName());
                });
                var source = fixture.source(streamingDispatcher, errors, (sequence, journal, header) -> {
                    try {
                        journal.decode((decoded, data, offset) -> {
                            if (replay.accept(decoded, data, offset, pos(sequence.longValue())).isPresent()) {
                                replay.deliver(batchOutput);
                                batchOutput.publishCheckpoint(replay, true);
                                lastCheckpoint.set(replay.checkpoint().toOffset());
                                write(generation + ".prepared.json", Map.of("count", produced,
                                        "checkpoint", lastCheckpoint.get(), "scan", sequence.toString()));
                            }
                            return null;
                        });
                    }
                    catch (Exception e) {
                        throw new As400RpcConnection.RpcException("Fixture decode", e);
                    }
                });
                coordinator.installSource(source);
                initialized.countDown();
                reader.nativeScope(() -> {
                    source.execute(context, new As400Partition("synthetic"), reader.scan);
                    // An external reconnect request after execute has stopped must
                    // also remain inert; no fixture publication/ownership gate.
                    source.closeAndReconnect();
                    return null;
                });
            }
            catch (Throwable error) {
                outcome = "error";
                append(generation, "native-error", Map.of("error", error.toString(), "running", context.isRunning()));
                if (context.isRunning()) {
                    errors.setProducerThrowable(error);
                }
            }
            finally {
                initialized.countDown();
                if (batchOutput != null) {
                    batchOutput.close();
                }
                write(generation + ".native-finished.json", Map.of("outcome", outcome,
                        "R", reader.scan.getPosition().getOffset().toString(), "checkpoint", lastCheckpoint.get(),
                        "contextRunning", context.isRunning(), "terminalRequested", reader.lifecycle.isStopped()));
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
            if (Files.readString(CONTROL.resolve("terminal-mode")).trim().endsWith("error")) {
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
            append(generation, "task-stop-enter", Map.of());
            try {
                // Real production doStop(): terminal RPC drain, then JDBC close.
                reader.nativeScope(() -> {
                    super.doStop();
                    return null;
                });
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            write(generation + ".terminal-complete.json", Map.of("rpcDisconnected", reader.allReaderTransportsDisconnected(),
                    "jdbcDisconnected", !fixture.jdbcConnected));
            append(generation, "stop", Map.of());
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
        private final java.util.function.Consumer<ChangeEventSourceContext> run;
        private final String generation;

        private FixtureCoordinator(As400ConnectorConfig config, ErrorHandler errors, EventDispatcher<As400Partition, TableId> dispatcher,
                                   DatabaseSchema<TableId> schema, java.util.function.Consumer<ChangeEventSourceContext> run, String generation) {
            super(Offsets.of(new As400Partition("synthetic"), null), errors, As400RpcConnector.class, config,
                    mock(ChangeEventSourceFactory.class),
                    mock(ChangeEventSourceMetricsFactory.class,
                            invocation -> invocation.getMethod().getReturnType().equals(boolean.class) ? true : mock(invocation.getMethod().getReturnType())),
                    dispatcher, schema, null, null, null);
            this.run = run;
            this.generation = generation;
        }

        @Override
        protected void executeChangeEventSources(CdcSourceTaskContext taskContext, SnapshotChangeEventSource<As400Partition, As400OffsetContext> snapshot,
                                                 Offsets<As400Partition, As400OffsetContext> offsets,
                                                 AtomicReference<LoggingContext.PreviousContext> previous, ChangeEventSourceContext context) {
            // Synthetic startup only. Real coordinator start/executor/context and
            // stop/deadlines surround the real streaming source execution.
            run.accept(context);
        }

        private void installSource(As400StreamingChangeEventSource source) {
            streamingSource = source;
        }

        @Override
        public synchronized void stop() throws InterruptedException {
            write(generation + ".coordinator-stop-requested.json", Map.of());
            super.stop();
            write(generation + ".coordinator-stop-returned.json", Map.of());
        }

        @Override
        public void commitOffset(Map<String, ?> partition, Map<String, ?> offset) {
            super.commitOffset(partition, offset);
            append(generation, "coordinatorCommit", Map.of("checkpoint", offset));
        }
    }
}
