/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/** Manual local-Kafka history qualification. No IBM i connection or production task. */
public final class As400BindingHistoryProbe {
    public static void main(String[] args) throws Exception {
        if (args.length != 4 || !args[0].matches("127\\.0\\.0\\.1:[0-9]+") || !args[1].startsWith("dcg-binding-probe-")) {
            throw new IllegalArgumentException("Local bootstrap, owned topic, evidence directory and phase required");
        }
        String bootstrap = args[0];
        String topic = args[1];
        Path directory = Path.of(args[2]);
        String phase = args[3];
        Files.createDirectories(directory);
        var mapper = new ObjectMapper();
        var f = new As400BindingHistoryTest.Fixture(directory);
        f.factory = As400KafkaBindingHistory::new;
        f.config = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, bootstrap).with(KafkaSchemaHistory.TOPIC, topic)
                .with(SchemaHistory.NAME, topic).with(SchemaHistory.INTERNAL_CONNECTOR_CLASS, As400RpcConnector.class.getName())
                .with(SchemaHistory.INTERNAL_CONNECTOR_ID, topic).with(KafkaSchemaHistory.RECOVERY_POLL_INTERVAL_MS, 50)
                .with(KafkaSchemaHistory.RECOVERY_POLL_ATTEMPTS, 40).build();
        Path offset = directory.resolve("offset.json");
        List<SourceRecord> records = new ArrayList<>();
        if (phase.equals("initialize")) {
            assertThat(Files.exists(offset)).isFalse();
            var saved = f.initialize();
            // Independent consumer confirms a single, complete record exists before
            // the first source checkpoint is written by this experiment.
            var history = readHistory(bootstrap, topic);
            assertThat(history).hasSize(1);
            Files.writeString(directory.resolve("broker-initial.json"), mapper.writeValueAsString(history));
            Files.writeString(offset, mapper.writeValueAsString(saved));
        }
        else {
            Map<String, String> saved = mapper.readValue(Files.readString(offset), new TypeReference<>() {
            });
            String originalOffset = Files.readString(offset);
            f.definitions.clear(); // Replacement may NOT recover from fixture table definitions.
            switch (phase) {
                case "open-truncate", "commit", "open-delete", "rollback", "again" -> {
                    long through = switch (phase) {
                        case "open-truncate" -> 42;
                        case "commit" -> 44;
                        case "open-delete" -> 56;
                        default -> 64;
                    };
                    List<String> expected = switch (phase) {
                        case "open-truncate" -> List.of("c:A:8");
                        case "commit" -> List.of("t:B", "c:B:100");
                        case "open-delete" -> List.of("c:A:9");
                        default -> List.of();
                    };
                    var next = f.replay(saved, through, records::add);
                    assertThat(As400BindingHistoryTest.data(records)).containsExactlyElementsOf(expected);
                    Files.writeString(offset, mapper.writeValueAsString(next));
                }
                case "missing-offset" -> assertThrows(IllegalArgumentException.class,
                        () -> f.history().recover(null, f.journalIdentity, As400BindingHistoryTest.Fixture.POLICY));
                case "missing-history" -> {
                    f.config = f.config.edit().with(KafkaSchemaHistory.TOPIC, topic + "-absent").build();
                    assertThrows(Exception.class, () -> f.history().recover(saved, f.journalIdentity, As400BindingHistoryTest.Fixture.POLICY));
                }
                case "invalid-null", "invalid-empty", "invalid-source", "invalid-position", "invalid-json" -> {
                    String childTopic = topic + "-" + phase;
                    var child = new As400BindingHistoryTest.Fixture(directory);
                    child.factory = As400KafkaBindingHistory::new;
                    child.config = f.config.edit().with(KafkaSchemaHistory.TOPIC, childTopic).build();
                    var childOffset = child.initialize();
                    String invalid = switch (phase) {
                        case "invalid-null" -> null;
                        case "invalid-empty" -> "{}";
                        case "invalid-source" -> "{\"source\":{\"server\":\"test\"}}";
                        case "invalid-position" -> "{\"position\":{}}";
                        default -> "{broken json";
                    };
                    injectRecord(bootstrap, childTopic, invalid);
                    var brokerRecords = readHistory(bootstrap, childTopic);
                    assertThat(brokerRecords).hasSize(2);
                    Files.writeString(directory.resolve(phase + "-broker.json"), mapper.writeValueAsString(brokerRecords));
                    Files.writeString(directory.resolve(phase + "-seeded-offset.json"), mapper.writeValueAsString(childOffset));
                    if (!phase.equals("invalid-json")) {
                        // Preserve the actual base-provider reproduction: its listener
                        // never sees these records, and normal recovery returns both tables.
                        var permissive = new KafkaSchemaHistory();
                        try {
                            permissive.configure(child.config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
                            permissive.start();
                            var recovered = new Tables();
                            permissive.recover(As400BindingHistoryTest.Fixture.POLICY.sourcePartition(), Map.of(), recovered, null);
                            assertThat(recovered.tableIds()).hasSize(2);
                        }
                        finally {
                            permissive.stop();
                        }
                    }
                    var rejection = assertThrows(SchemaHistoryException.class,
                            () -> child.history().recover(childOffset, child.journalIdentity, As400BindingHistoryTest.Fixture.POLICY));
                    assertThat(rejection.getMessage()).contains(phase.equals("invalid-null") ? "Null binding history record" : "Malformed binding history at offset 1");
                    Files.writeString(directory.resolve(phase + "-rejection.txt"), rejection.getMessage() + "\n");
                }
                case "conflicting-history" -> {
                    var before = readHistory(bootstrap, topic);
                    assertThat(before).hasSize(1);
                    String changed = before.get(0).replace("00000000000000000001", "00000000000000000009");
                    assertThat(changed).isNotEqualTo(before.get(0));
                    injectRecord(bootstrap, topic, changed);
                    var after = readHistory(bootstrap, topic);
                    assertThat(after).hasSize(2);
                    Files.writeString(directory.resolve("broker-conflict.json"), mapper.writeValueAsString(after));
                    assertThrows(Exception.class, () -> f.history().recover(saved, f.journalIdentity, As400BindingHistoryTest.Fixture.POLICY));
                }
                default -> throw new IllegalArgumentException("Unknown phase");
            }
            if (phase.contains("missing") || phase.startsWith("invalid-") || phase.equals("conflicting-history")) {
                assertThat(records).isEmpty();
                assertThat(Files.readString(offset)).isEqualTo(originalOffset);
            }
        }
        Files.writeString(directory.resolve(phase + ".json"), mapper.writeValueAsString(Map.of("phase", phase, "status", "passed",
                "data", As400BindingHistoryTest.data(records), "offset", mapper.readTree(Files.readString(offset)),
                "scope", "real KafkaSchemaHistory; native fixture replay; synthetic journal identity evidence; no live source or Connect task")));
    }

    private static void injectRecord(String bootstrap, String topic, String value) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        try (var producer = new KafkaProducer<String, String>(props)) {
            producer.send(new ProducerRecord<>(topic, 0, null, value)).get(10, TimeUnit.SECONDS);
        }
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
}
