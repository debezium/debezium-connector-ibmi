/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/** Local fixture admission only; inherits the complete normal task startup/poll/stop lifecycle. */
public final class As400NormalTaskProbeConnector extends As400ConnectProbeConnector {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Override
    public Class<? extends Task> taskClass() {
        return NormalTask.class;
    }

    static void history(Fixture fixture, String name) {
        fixture.factory = As400KafkaBindingHistory::new;
        fixture.config = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "broker:9092")
                .with(KafkaSchemaHistory.TOPIC, "bindings-" + name).with(SchemaHistory.NAME, name)
                .with(SchemaHistory.INTERNAL_CONNECTOR_CLASS, As400RpcConnector.class.getName())
                .with(SchemaHistory.INTERNAL_CONNECTOR_ID, name).build();
    }

    public static final class NormalTask extends As400ConnectorTask {
        private String generation;

        @Override
        As400TaskRecovery recoveryForTesting(Configuration config) {
            try {
                String name = config.getString("name");
                generation = System.getenv("PROBE_WORKER") + "-" + UUID.randomUUID();
                Map<String, String> baseline = JSON.readValue(Files.readString(CONTROL.resolve(name + ".baseline.json")), new TypeReference<>() {
                });
                var source = new LocalSource(name, generation, baseline.get("capture_fingerprint"));
                var loaded = context.offsetStorageReader().offset(source.partition().getSourcePartition());
                write(generation + ".start.json", Map.of("generation", generation, "loaded", loaded == null ? Map.of() : loaded));
                return source;
            }
            catch (Exception e) {
                throw new IllegalStateException("Local normal-task admission failed", e);
            }
        }

        @Override
        public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
            super.commitRecord(record, metadata);
            append(generation, "commitRecord", Map.of("topic", record.topic(), "offset", record.sourceOffset()));
        }

        @Override
        public void commit() throws InterruptedException {
            super.commit();
            append(generation, "commit", Map.of());
        }
    }

    static final class LocalSource extends As400RecoveryTaskFixture {
        final String name;
        final String generation;

        LocalSource(String name, String generation, String fingerprint) throws Exception {
            super(CONTROL, fingerprint);
            this.name = name;
            this.generation = generation;
            history(fixture, name);
            // The replacement must use durable bindings, not fixture schema definitions.
            fixture.definitions.clear();
        }

        @Override
        public void checkQualified(Configuration config) {
            super.checkQualified(config);
            try {
                Properties worker = new Properties();
                try (var stream = Files.newInputStream(java.nio.file.Path.of("/worker.properties"))) {
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
        public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
            var result = super.recover(durable);
            write(generation + ".recovered.json", Map.of("offset", durable, "fingerprint", result.fingerprint(), "tables", result.definitions().size()));
            return result;
        }

        @Override
        public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
            tail = Long.parseLong(Files.readString(CONTROL.resolve(name + ".tail")).trim());
            if (Files.exists(CONTROL.resolve(generation + ".hold"))) {
                write(generation + ".held.json", Map.of("offset", durable, "scan", scan.map(e -> e.position().offset().toString()).orElse("none")));
                long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
                while (!Files.exists(CONTROL.resolve(generation + ".release"))) {
                    if (System.nanoTime() >= deadline) {
                        throw new IllegalStateException("Local read gate timed out");
                    }
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException e) {
                        // Deliberately emulate a driver that returns late after cancellation.
                    }
                }
            }
            var result = super.read(durable, scan);
            write(generation + ".read.json", Map.of("offset", durable, "tail", tail, "reads", reads.get()));
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

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("baseline") && args[1].matches("normal[a-z]+")) {
            Fixture fixture = new Fixture(CONTROL);
            history(fixture, args[1]);
            var offset = As400RecoveryTaskFixture.initialize(fixture);
            write(args[1] + ".baseline.json", offset);
            System.out.println(JSON.writeValueAsString(offset));
        }
        else {
            As400ConnectOwnershipProbeConnector.main(args);
        }
    }
}
