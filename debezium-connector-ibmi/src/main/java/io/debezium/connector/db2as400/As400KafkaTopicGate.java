/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.debezium.config.Configuration;

/** Read-only preflight; does not create topics or authorize publication/offsets. */
final class As400KafkaTopicGate {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(As400KafkaTopicGate.class);

    private As400KafkaTopicGate() {
    }

    static void require(Configuration config, List<String> topics, boolean empty, Runnable checkRunning) {
        require(config, topics, Set.copyOf(topics), empty, checkRunning);
    }

    static void require(Configuration config, List<String> topics, Set<String> orderedTopics, boolean empty, Runnable checkRunning) {
        if (!topics.containsAll(orderedTopics)) {
            throw new IllegalArgumentException("Ordered topics must belong to the original output/history inventory");
        }
        var routing = As400TopicRouting.from(config.asMap());
        String history = config.getString("schema.history.internal.kafka.topic");
        if (routing.predictable()) {
            topics = topics.stream().map(topic -> routing.destination(topic, history)).toList();
            orderedTopics = orderedTopics.stream().map(topic -> routing.destination(topic, history)).collect(java.util.stream.Collectors.toSet());
        }
        else {
            if (empty) {
                throw new IllegalArgumentException("Operator initialization requires predictable empty output destinations");
            }
            // Standard SMTs can select their topic from record values or remove
            // a record. The worker's successful processing/offset commit is the
            // acknowledgement; a guessed destination is not proof. History uses
            // its own producer and retains its strict, unrouted storage checks.
            topics = topics.stream().filter(topic -> topic.equals(history)).toList();
            orderedTopics = Set.copyOf(topics);
        }
        if (topics.isEmpty() || topics.stream().distinct().count() != topics.size()) {
            throw new IllegalArgumentException("Distinct data, control and history topics required");
        }
        checkRunning.run();
        if (!empty) {
            // Ordinary CDC lets Connect/the broker create data topics, including
            // tables discovered after startup. Only recovery/control storage
            // needs a pre-existing ordered destination in this profile.
            var required = orderedTopics;
            topics = topics.stream().filter(required::contains).toList();
            if (topics.isEmpty()) {
                return;
            }
        }
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
            int multipartition = 0;
            for (String name : topics) {
                checkRunning.run();
                int count = consumer.partitionsFor(name, Duration.ofSeconds(10)).size();
                if (count == 0 || orderedTopics.contains(name) && count != 1) {
                    throw new IllegalStateException("Pre-created topics and single-partition recovery/control history required: " + name);
                }
                multipartition += count > 1 ? 1 : 0;
                checkRunning.run();
                if (empty) {
                    var partitions = java.util.stream.IntStream.range(0, count).mapToObj(i -> new TopicPartition(name, i)).toList();
                    var beginning = consumer.beginningOffsets(partitions);
                    var end = consumer.endOffsets(partitions);
                    if (partitions.stream().anyMatch(p -> !Long.valueOf(0).equals(beginning.get(p)) || !Long.valueOf(0).equals(end.get(p)))) {
                        throw new IllegalStateException("Existing or incompletely observed output/history cannot initialize a missing-offset generation");
                    }
                }
                checkRunning.run();
            }
            if (multipartition > 0) {
                LOGGER.warn("{} output topics have multiple partitions. Kafka does not order keyless truncate events and row events across partitions.", multipartition);
            }
        }
    }
}
