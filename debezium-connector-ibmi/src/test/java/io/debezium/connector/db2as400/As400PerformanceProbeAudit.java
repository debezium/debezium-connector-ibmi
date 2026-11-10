/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Continuous independent local-broker observer; never loaded by the connector worker. */
public final class As400PerformanceProbeAudit {
    public static void main(String[] args) throws Exception {
        if ((args.length != 2 && args.length != 3) || !args[0].matches("[A-Z][A-Z0-9_]{0,9}") || !args[1].matches("DCNC[0-9]{4}")
                || (args.length == 3 && !args[2].equals("soak"))) {
            throw new IllegalArgumentException("Expected server and isolated fixture library");
        }
        Path root = Path.of("/control");
        Path output = root.resolve("performance-events.jsonl");
        if (Files.exists(output) || Files.exists(root.resolve("performance-ready")) || Files.exists(root.resolve("performance-stop"))) {
            throw new IllegalStateException("Fresh performance observer state required");
        }
        var properties = new Properties();
        properties.setProperty("bootstrap.servers", "broker:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("allow.auto.create.topics", "false");
        properties.setProperty("isolation.level", "read_committed");
        properties.setProperty("default.api.timeout.ms", "10000");
        var mapper = new ObjectMapper();
        long count = 0;
        long bytes = 0;
        long quietSince = System.nanoTime();
        boolean soak = args.length == 3;
        long deadline = System.nanoTime() + Duration.ofMinutes(soak ? 40 : 20).toNanos();
        try (var consumer = new KafkaConsumer<String, String>(properties);
                var writer = Files.newBufferedWriter(output)) {
            var partitions = List.of("A", "B", "M").stream()
                    .map(table -> new TopicPartition("test." + args[0] + "." + args[1] + "." + table, 0)).toList();
            consumer.assign(partitions);
            consumer.endOffsets(partitions).forEach(consumer::seek);
            Files.createFile(root.resolve("performance-ready"));
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(25));
                long observed = System.currentTimeMillis();
                if (!records.isEmpty()) {
                    quietSince = System.nanoTime();
                }
                for (var record : records) {
                    bytes += Math.max(0, record.serializedKeySize()) + Math.max(0, record.serializedValueSize());
                    if (++count > (soak ? 150000 : 60000) || bytes > (soak ? 256L : 128L) * 1024 * 1024) {
                        throw new IllegalStateException("Performance observer bound exceeded");
                    }
                    var row = new java.util.LinkedHashMap<String, Object>();
                    row.put("topic", record.topic());
                    row.put("offset", record.offset());
                    row.put("observed_ms", observed);
                    row.put("broker_timestamp_ms", record.timestamp());
                    row.put("key", record.key() == null ? null : mapper.readTree(record.key()));
                    row.put("value", record.value() == null ? null : mapper.readTree(record.value()));
                    writer.write(mapper.writeValueAsString(row));
                    writer.newLine();
                }
                writer.flush();
                if (Files.exists(root.resolve("performance-stop")) && System.nanoTime() - quietSince > Duration.ofSeconds(1).toNanos()) {
                    mapper.writeValue(root.resolve("performance-observer.json").toFile(), Map.of("status", "passed", "records", count, "bytes", bytes));
                    return;
                }
            }
            throw new IllegalStateException("Performance observer deadline exceeded");
        }
    }
}
