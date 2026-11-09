/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400BindingHistoryTest.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.relational.TableId;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/** Fixture provider with the actual task start/coordinator/queue/acknowledgements/stop. Never opens IBM i. */
public final class As400FirstStartProbeConnector extends As400ConnectProbeConnector {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    public Class<? extends Task> taskClass() {
        return FirstStartTask.class;
    }

    public static final class Topics extends AbstractTopicNamingStrategy<TableId> {
        private String topic;

        public Topics(Properties properties) {
            super(properties);
        }

        @Override
        public void configure(Properties properties) {
            super.configure(properties);
            topic = properties.getProperty("probe.topic");
        }

        @Override
        public String dataChangeTopic(TableId table) {
            return topic + "-" + table.table();
        }

        @Override
        public String heartbeatTopic() {
            return topic;
        }

        @Override
        public String transactionTopic() {
            return topic + "-tx";
        }
    }

    public static final class FirstStartTask extends As400ConnectorTask {
        private LocalSession source;
        private int polled;

        @Override
        As400TaskInitialization initializationForTesting(Configuration config) {
            String generation = System.getenv("PROBE_WORKER") + "-" + UUID.randomUUID();
            try {
                var loaded = context.offsetStorageReader().offset(new As400Partition("test").getSourcePartition());
                write(generation + ".start.json", Map.of("generation", generation, "loaded", loaded == null ? Map.of() : loaded));
                source = new LocalSession(config.getString("name"), generation, loaded == null);
                if (loaded != null) {
                    source.acceptedFingerprint = (String) loaded.get("capture_fingerprint");
                    // This is an untrusted expectation: normal loader and independent immutable history/policy recovery still validate it.
                    As400Checkpoint.fromOffset(loaded, source.journalIdentity(), source.fingerprint());
                    return null;
                }
                return source;
            }
            catch (Exception error) {
                write(generation + ".failed.json", Map.of("error", error.toString()));
                throw new IllegalStateException("Test first-start/resume admission failed", error);
            }
        }

        @Override
        As400TaskRecovery recoveryForTesting(Configuration config) {
            return source;
        }

        @Override
        protected List<SourceRecord> doPoll() throws InterruptedException {
            try {
                var failure = getErrorHandler().orElseThrow().getProducerThrowable();
                if (failure != null) {
                    throw new IllegalStateException("Normal task initialization failed", failure);
                }
                Path permit = CONTROL.resolve(source.generation + ".permit");
                if (Files.exists(permit) && Integer.parseInt(Files.readString(permit).trim()) > polled) {
                    var records = super.doPoll();
                    for (var record : records) {
                        String op = record.value() instanceof Struct value && value.schema().field("op") != null ? value.getString("op") : "marker";
                        record.headers().addString("probe", JSON.writeValueAsString(Map.of("op", op, "index", polled++, "generation", source.generation)));
                    }
                    return records;
                }
                Thread.sleep(20);
                return List.of();
            }
            catch (Exception error) {
                write(source.generation + ".failed.json", Map.of("error", error.toString()));
                if (error instanceof InterruptedException interrupted) {
                    throw interrupted;
                }
                throw new IllegalStateException(error);
            }
        }

        @Override
        public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
            super.commitRecord(record, metadata);
            append(source.generation, "commitRecord", Map.of("topic", record.topic(), "offset", record.sourceOffset()));
        }

        @Override
        public void commit() throws InterruptedException {
            super.commit();
            append(source.generation, "commit", Map.of());
        }
    }

    static final class LocalSession extends As400InitializationTaskFixture {
        final String scenario;
        final String generation;
        final boolean initializing;
        boolean readStarted;

        LocalSession(String scenario, String generation, boolean initializing) throws Exception {
            super(CONTROL);
            this.scenario = scenario;
            this.generation = generation;
            this.initializing = initializing;
            epoch = "controlled-" + scenario;
            tail = 64;
            history(fixture);
            history(setup.fixture);
            AtomicInteger acquisitions = new AtomicInteger();
            when(setup.acquisition.collect(anyList())).thenAnswer(i -> {
                int call = acquisitions.incrementAndGet();
                if (call == 2) {
                    phase("snapshot-acknowledged");
                }
                if (call == 3) {
                    phase("history-durable");
                }
                return setup.acquired;
            });
        }

        private void history(Fixture fixture) {
            fixture.factory = As400KafkaBindingHistory::new;
            fixture.config = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "broker:9092")
                    .with(KafkaSchemaHistory.TOPIC, "history-" + scenario).with(SchemaHistory.NAME, scenario)
                    .with(SchemaHistory.INTERNAL_CONNECTOR_CLASS, As400RpcConnector.class.getName())
                    .with(SchemaHistory.INTERNAL_CONNECTOR_ID, scenario).with(KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS, 50)
                    .with(KafkaSchemaHistory.RECOVERY_POLL_ATTEMPTS, 40).build();
        }

        @Override
        public void checkQualified(Configuration config) {
            super.checkQualified(config);
            try {
                Properties worker = new Properties();
                try (var stream = Files.newInputStream(Path.of("/worker.properties"))) {
                    worker.load(stream);
                }
                if (System.getenv("PROBE_WORKER") == null || !"enabled".equals(worker.getProperty("exactly.once.source.support"))
                        || !"broker:9092".equals(worker.getProperty("bootstrap.servers"))) {
                    throw new IllegalStateException("Pinned local EOS worker required");
                }
            }
            catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public String enroll() throws Exception {
            checkHeld();
            if (!Files.exists(CONTROL.resolve(scenario + ".initialize-request"))) {
                throw new IllegalStateException("No explicit fresh-generation enrollment requested");
            }
            requireEmptyGeneration();
            consumeEnrollment(CONTROL.resolve(scenario + ".enrolled"), epoch);
            return checkHeld();
        }

        @Override
        public String checkHeld() {
            super.checkHeld();
            try {
                if (!Files.readString(CONTROL.resolve(scenario + ".qualified")).equals(epoch)) {
                    throw new IllegalStateException("Controlled source session changed");
                }
            }
            catch (java.io.IOException e) {
                throw new IllegalStateException("Controlled source session is absent", e);
            }
            return epoch;
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
                for (String name : List.of("events-" + scenario, "events-" + scenario + "-A", "events-" + scenario + "-B",
                        "events-" + scenario + "-tx", "events-" + scenario + ".initialization", "history-" + scenario)) {
                    if (consumer.partitionsFor(name).size() != 1) {
                        throw new IllegalStateException("Dedicated one-partition generation required");
                    }
                    var partition = new TopicPartition(name, 0);
                    if (consumer.beginningOffsets(List.of(partition)).get(partition) != 0 || consumer.endOffsets(List.of(partition)).get(partition) != 0) {
                        throw new IllegalStateException("Missing offset for existing output/history generation");
                    }
                }
            }
        }

        void phase(String phase) throws Exception {
            write(generation + "." + phase + ".json", Map.of("phase", phase));
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (Files.exists(CONTROL.resolve(scenario + ".hold-" + phase)) && !Files.exists(CONTROL.resolve(generation + ".release-" + phase))) {
                if (stopped.get()) {
                    throw new InterruptedException("Task stopped at " + phase);
                }
                if (initializing && heldSession.get()) {
                    checkHeld();
                }
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Bounded phase gate expired: " + phase);
                }
                Thread.sleep(20);
            }
        }

        @Override
        public void checkpointAccepted(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
            super.checkpointAccepted(epoch, checkpoint, history);
            phase("checkpoint-durable");
        }

        @Override
        public void handoff(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
            super.handoff(epoch, checkpoint, history);
            write(generation + ".session-released.json", Map.of("epoch", epoch));
        }

        @Override
        public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
            // Discard cached schemas; only independently recovered history supplies the runtime.
            fixture.definitions.clear();
            var recovered = super.recover(durable);
            write(generation + ".recovered.json", Map.of("offset", durable, "fingerprint", recovered.fingerprint()));
            return recovered;
        }

        @Override
        public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
            if (!readStarted) {
                readStarted = true;
                write(generation + ".handoff.json", Map.of("offset", durable));
                phase("reader");
            }
            var result = super.read(durable, scan);
            write(generation + ".read.json", Map.of("offset", durable, "scan", scan.map(e -> e.position().offset().toString()).orElse("none"), "reads", reads.get()));
            return result;
        }

        @Override
        public void requestStop() {
            super.requestStop();
            write(generation + ".stop-requested.json", Map.of("reads", reads.get(), "closes", closes.get()));
        }

        @Override
        public void close() throws Exception {
            super.close();
            write(generation + ".drained.json", Map.of("reads", reads.get(), "closes", closes.get()));
        }
    }
}
