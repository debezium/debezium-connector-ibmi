/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.debezium.config.Configuration;

/** Read-only preflight; does not create topics or authorize publication/offsets. */
final class As400KafkaTopicGate {
    private As400KafkaTopicGate() {
    }

    static void require(Configuration config, List<String> topics, boolean empty, Runnable checkRunning) {
        if (topics.isEmpty() || topics.size() > 104 || topics.stream().distinct().count() != topics.size()) {
            throw new IllegalArgumentException("Distinct bounded data, control and history topics required");
        }
        checkRunning.run();
        Properties properties = config.subset("schema.history.internal.consumer.", true).asProperties();
        properties.put("bootstrap.servers", As400DcgConfig.required(config, "schema.history.internal.kafka.bootstrap.servers"));
        properties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        properties.put("enable.auto.commit", "false");
        properties.put("allow.auto.create.topics", "false");
        properties.put("default.api.timeout.ms", "10000");
        properties.put("request.timeout.ms", "5000");
        properties.put("isolation.level", "read_uncommitted");
        try (var consumer = new KafkaConsumer<byte[], byte[]>(properties)) {
            for (String name : topics) {
                checkRunning.run();
                if (consumer.partitionsFor(name, Duration.ofSeconds(10)).size() != 1) {
                    throw new IllegalStateException("Exactly one pre-created partition required for D/CG topic " + name);
                }
                checkRunning.run();
                var partition = new TopicPartition(name, 0);
                if (empty && (consumer.beginningOffsets(List.of(partition)).get(partition) != 0
                        || consumer.endOffsets(List.of(partition)).get(partition) != 0)) {
                    throw new IllegalStateException("Existing output/history cannot initialize a missing-offset generation");
                }
                checkRunning.run();
            }
        }
    }
}
