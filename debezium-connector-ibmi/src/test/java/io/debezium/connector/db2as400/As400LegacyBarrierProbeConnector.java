/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.ConnectorTransactionBoundaries;
import org.apache.kafka.connect.source.SourceRecord;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * LOCAL TEST ONLY. Real legacy RPC/handler/dispatcher and task polling/callbacks;
 * controlled native transport, fixed fixture schema and synthetic admission.
 * No migration, source census, owner registry, receipt store or live IBM i access.
 */
public class As400LegacyBarrierProbeConnector extends As400ConnectProbeConnector {
    static final Schema CONTROL_SCHEMA = SchemaBuilder.string().name("probe.Control").build();
    static final String BARRIER = "probe.legacy.barrier";
    static final Map<String, String> SOURCE_PARTITION = Map.of("server", "test");

    @Override
    public Class<? extends Task> taskClass() {
        return BarrierTask.class;
    }

    @Override
    public ConnectorTransactionBoundaries canDefineTransactionBoundaries(Map<String, String> config) {
        return ConnectorTransactionBoundaries.SUPPORTED;
    }

    @Override
    public boolean alterOffsets(Map<String, String> config, Map<Map<String, ?>, Map<String, ?>> offsets) {
        if (offsets.keySet().stream().anyMatch(p -> !SOURCE_PARTITION.equals(p))) {
            throw new IllegalArgumentException("Only the test partition may be altered");
        }
        return true; // Public stopped-connector API, for intentional test setup/faults only.
    }

    public static class BarrierTask extends As400ConnectorTask {
        private CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext;
        private ChangeEventQueue<DataChangeEvent> output;
        private ErrorHandler errors;
        private As400ReaderFixture reader;
        private String generation;
        private String topic;
        private String policy;
        private boolean boundary;
        private volatile boolean stopped;
        private int polled;

        @Override
        public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {
            taskContext = super.preStart(config);
            return taskContext;
        }

        @Override
        protected ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> start(Configuration raw) {
            var config = (As400ConnectorConfig) taskContext.getConfig();
            generation = System.getenv("PROBE_WORKER") + "-" + UUID.randomUUID();
            topic = raw.getString("probe.topic");
            policy = raw.getString("probe.policy");
            boundary = "connector".equals(raw.getString("transaction.boundary"));
            As400BatchDispatcher.requireConfiguration(config); // A barrier cannot certify deliberately skipped failures.
            if (config.shouldProvideTransactionMetadata()) {
                throw new IllegalArgumentException("Legacy transaction metadata is outside this boundary proof");
            }
            var loaded = context.offsetStorageReader().offset(SOURCE_PARTITION);
            write(generation + ".start.json", Map.of("generation", generation, "loaded", loaded == null ? Map.of() : loaded));
            if (loaded == null || !"true".equals(loaded.get("snapshot_completed")) || !"true".equals(loaded.get("offset.processed"))) {
                throw new IllegalArgumentException("Missing or incomplete legacy offset");
            }
            if (loaded.containsKey(BARRIER) && (!"complete-output-v1".equals(loaded.get(BARRIER))
                    || !policy.equals(loaded.get("probe.policy")) || !"44".equals(loaded.get("offset.event_sequence"))
                    || !loaded.containsKey("probe.generation") || !"LIB".equals(loaded.get("offset.receiver_library"))
                    || !"RCV".equals(loaded.get("offset.receiver")))) {
                throw new IllegalArgumentException("Invalid test publication boundary");
            }
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
            output = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(1).maxQueueSize(64)
                    .pollInterval(Duration.ofMillis(10)).loggingContextSupplier(() -> taskContext.configureLoggingContext("barrier-probe")).build();
            errors = new ErrorHandler(As400RpcConnector.class, config, output, null);
            initializeOutput(output, errors);
            // Do NOT inject configuredTransactions: the candidate boundary spans several polls.
            try {
                reader = new As400ReaderFixture(generation, config, Long.parseLong(loaded.get("offset.event_sequence").toString()),
                        operation -> append(generation, "native", operation));
                reader.scan.preSnapshotCompletion();
                reader.scan.postSnapshotCompletion();
                var jdbc = mock(As400JdbcConnection.class);
                when(jdbc.getRealDatabaseName()).thenReturn("DB");
                when(jdbc.getLongName(anyString(), anyString())).thenAnswer(i -> i.getArgument(1));
                when(jdbc.isOnlyMember("LIB", "TEST", "TEST")).thenReturn(true);
                initializeConnections(reader.rpc, jdbc);
                var schema = new As400TaskStopProbeConnector.FixtureSchema(reader.scan);
                var topics = new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
                    @Override
                    public String dataChangeTopic(TableId table) {
                        return topic;
                    }
                };
                var recordsCreated = new AtomicInteger();
                var dispatcher = new EventDispatcher<As400Partition, TableId>(config, topics, schema, output, id -> true,
                        record -> {
                            if ("dispatch".equals(raw.getString("probe.failure")) && recordsCreated.incrementAndGet() == 2) {
                                write(generation + ".failure-held.json", Map.of("created", 1));
                                try {
                                    await("release-failure", () -> !stopped);
                                }
                                catch (Exception e) {
                                    throw new IllegalStateException("Fault gate interrupted", e);
                                }
                                throw new IllegalStateException("controlled dispatch failure after first subrecord");
                            }
                            return new DataChangeEvent(record);
                        }, null, new As400EventMetadataProvider(), new io.debezium.heartbeat.HeartbeatImpl(config.getHeartbeatInterval(),
                                topic + "-control", config.getLogicalName(), config.schemaNameAdjuster(), output),
                        config.schemaNameAdjuster(), null,
                        new DebeziumHeaderProducer(taskContext));
                var rowDecoder = mock(JdbcFileDecoder.class);
                var strict = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
                when(rowDecoder.decode(any(), any(), anyInt())).thenAnswer(i -> {
                    io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader header = i.getArgument(0);
                    if ("decode".equals(raw.getString("probe.failure")) && header.getSequenceNumber().intValueExact() == 42) {
                        byte[] malformed = ((byte[]) i.getArgument(1)).clone();
                        new com.ibm.as400.access.AS400Text(5, 37).toBytes("00003", malformed,
                                (int) i.getArgument(2) + header.getEntrySpecificDataOffset());
                        return strict.decode(header, malformed, i.getArgument(2));
                    }
                    return strict.decode(header, i.getArgument(1), i.getArgument(2));
                });
                var metadata = mock(As400DatabaseSchema.class);
                when(metadata.getFileDecoder()).thenReturn(rowDecoder);
                var source = new As400StreamingChangeEventSource(config, reader.rpc, jdbc, dispatcher, errors, Clock.system(), metadata);
                source.init(reader.scan);
                var coordinator = new As400TaskStopProbeConnector.FixtureCoordinator(config, errors, dispatcher, schema,
                        running -> produce(running, source, raw, loaded), generation);
                coordinator.start(taskContext, output, new As400EventMetadataProvider());
                return coordinator;
            }
            catch (Exception e) {
                throw new IllegalStateException("Could not install local transport", e);
            }
        }

        private void produce(ChangeEventSourceContext running, As400StreamingChangeEventSource source, Configuration raw, Map<String, ?> loaded) {
            try {
                if (loaded.containsKey(BARRIER)) {
                    write(generation + ".resumed-barrier.json", Map.of("offset", loaded));
                    return; // Resume a drained legacy task; this is NOT D/CG recovery admission.
                }
                write(generation + ".before-enqueue.json", Map.of());
                if (!await("release-read", running::isRunning)) {
                    return;
                }
                var consumer = source.processJournalEntries(new As400Partition("test"), reader.scan);
                String mode = raw.getString("probe.mode");
                var events = mode.equals("empty") ? List.<Event> of()
                        : mode.equals("row") ? List.of(event(42, "R/PT", 0, 2))
                                : List.of(event(40, "R/UB", 0, 1), event(42, "R/UP", 0, 2), event(43, "F/CR", 0, 0), event(44, "R/PT", 0, 3));
                for (var event : events) {
                    reader.read(event, running::isRunning, consumer, () -> null);
                    append(generation, "scan", Map.of("R", reader.scan.getPosition().getOffset().toString()));
                }
                if (mode.equals("empty")) {
                    output.enqueue(new DataChangeEvent(new SourceRecord(SOURCE_PARTITION, Map.copyOf(reader.scan.getOffset()), topic + "-control", 0,
                            Schema.STRING_SCHEMA, "heartbeat", CONTROL_SCHEMA, "legacy-idle")));
                }
                if (boundary) {
                    // Explicit synthetic pages end at their fully processed entry. Native continuation
                    // coordinates need separate evidence; never infer a cut by adding one to L.
                    Map<String, Object> cut = new HashMap<>(reader.scan.getOffset());
                    cut.put(BARRIER, "complete-output-v1");
                    cut.put("probe.generation", generation);
                    cut.put("probe.policy", policy);
                    output.enqueue(new DataChangeEvent(new SourceRecord(SOURCE_PARTITION, Map.copyOf(cut), topic + "-control", 0,
                            Schema.STRING_SCHEMA, "legacy-publication-barrier", CONTROL_SCHEMA, "drained")));
                }
                write(generation + ".prepared.json", Map.of("scan", reader.scan.getOffset(), "boundary", boundary));
            }
            catch (Exception e) {
                errors.setProducerThrowable(e);
                write(generation + ".producer-failure.json", Map.of("scan", reader.scan.getOffset(), "error", e.toString()));
            }
        }

        private boolean await(String suffix, java.util.function.BooleanSupplier running) throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(150).toNanos();
            while (!Files.exists(CONTROL.resolve(generation + "." + suffix))) {
                if (stopped || !running.getAsBoolean()) {
                    return false;
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Bounded fault gate timed out: " + suffix);
                }
                Thread.sleep(25);
            }
            return true;
        }

        @Override
        protected List<SourceRecord> doPoll() throws InterruptedException {
            if (errors.getProducerThrowable() != null) {
                // Surface the queue's actual ConnectException/RetriableException;
                // do not replace Debezium's retry classification with a fixture error.
                return super.doPoll();
            }
            try {
                var permit = CONTROL.resolve(generation + ".permit");
                if (!stopped && Files.exists(permit) && Integer.parseInt(Files.readString(permit).trim()) > polled) {
                    var records = super.doPoll();
                    for (var record : records) {
                        record.headers().addString("probe-generation", generation);
                        record.headers().addString("probe-index", Integer.toString(polled++));
                        if (record.sourceOffset().containsKey(BARRIER)) {
                            context.transactionContext().commitTransaction(record);
                        }
                        append(generation, "poll", Map.of("index", polled, "offset", record.sourceOffset()));
                    }
                    return records;
                }
                Thread.sleep(25); // Yield BaseSourceTask's lifecycle lock to stop().
                return List.of();
            }
            catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
            if (record.sourceOffset().containsKey(BARRIER) && Files.exists(CONTROL.resolve(generation + ".hold-callback"))) {
                write(generation + ".callback-held.json", Map.of("offset", record.sourceOffset()));
                try {
                    await("release-callback", () -> !stopped);
                }
                catch (InterruptedException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            super.commitRecord(record, metadata);
            append(generation, "commitRecord", Map.of("offset", record.sourceOffset()));
        }

        @Override
        public void commit() throws InterruptedException {
            super.commit();
            append(generation, "commit", Map.of());
        }

        @Override
        protected void doStop() {
            stopped = true;
            if (reader == null) {
                super.doStop();
                return;
            }
            try {
                reader.nativeScope(() -> {
                    super.doStop();
                    return null;
                });
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            write(generation + ".stopped.json", Map.of("disconnected", reader.allReaderTransportsDisconnected()));
        }

    }
}
