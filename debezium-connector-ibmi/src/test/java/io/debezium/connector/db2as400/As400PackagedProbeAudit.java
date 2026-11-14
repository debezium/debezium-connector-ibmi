/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Independent read-only, bounded audit process; never installed on the worker plugin path. */
public final class As400PackagedProbeAudit {
    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("offset-state")) {
            offsetState(args[1]);
            return;
        }
        if (args.length == 5 && args[0].equals("truncate-trigger")) {
            truncateTrigger(args);
            return;
        }
        if (args.length == 6 && args[0].equals("native")) {
            nativeJournal(args);
            return;
        }
        if (args.length != 4 || !args[0].equals("broker:9092") || !List.of("read_committed", "read_uncommitted").contains(args[2])
                || !args[3].startsWith("/control/")) {
            throw new IllegalArgumentException("local broker topic isolation /control/output.json");
        }
        var config = new java.util.Properties();
        config.setProperty("bootstrap.servers", args[0]);
        config.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.setProperty("enable.auto.commit", "false");
        config.setProperty("allow.auto.create.topics", "false");
        config.setProperty("isolation.level", args[2]);
        config.setProperty("default.api.timeout.ms", "10000");
        List<Map<String, Object>> result = new ArrayList<>();
        long bytes = 0;
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(config)) {
            var partitions = consumer.partitionsFor(args[1]).stream()
                    .map(p -> new org.apache.kafka.common.TopicPartition(p.topic(), p.partition())).toList();
            if (partitions.isEmpty() || partitions.size() > 1024) {
                throw new IllegalStateException("Missing or excessive audit partitions");
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            var end = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(25).toNanos();
            while (partitions.stream().anyMatch(p -> consumer.position(p) < end.get(p))) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Audit deadline exceeded");
                }
                for (var record : consumer.poll(Duration.ofMillis(100))) {
                    if (record.offset() >= end.get(new org.apache.kafka.common.TopicPartition(record.topic(), record.partition()))) {
                        continue;
                    }
                    bytes += Math.max(0, record.serializedKeySize()) + Math.max(0, record.serializedValueSize());
                    if (result.size() >= 60000 || bytes > 128L * 1024 * 1024) {
                        throw new IllegalStateException("Audit bound exceeded");
                    }
                    var row = new HashMap<String, Object>();
                    row.put("partition", record.partition());
                    row.put("offset", record.offset());
                    row.put("key", record.key());
                    row.put("value", record.value());
                    row.put("timestamp", record.timestamp());
                    result.add(row);
                }
            }
        }
        // Kafka has no cross-partition ordering. Stable partition/offset order
        // supports repeated audits without inventing a global event sequence.
        result.sort(java.util.Comparator.<Map<String, Object>, Integer> comparing(row -> (Integer) row.get("partition"))
                .thenComparingLong(row -> (Long) row.get("offset")));
        new com.fasterxml.jackson.databind.ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(args[3]).toFile(), result);
    }

    private static void offsetState(String output) throws Exception {
        if (!output.matches("/control/[a-z0-9-]+\\.json")) {
            throw new IllegalArgumentException("Owned local audit path required");
        }
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("observed", java.time.Instant.now().toString());
        try (var admin = org.apache.kafka.clients.admin.Admin.create(Map.of("bootstrap.servers", "broker:9092",
                "default.api.timeout.ms", 10000, "request.timeout.ms", 5000))) {
            String id = "group-native-native-0";
            var transaction = admin.describeTransactions(List.of(id)).description(id).get(10, java.util.concurrent.TimeUnit.SECONDS);
            result.put("transaction", Map.of("id", id, "state", transaction.state().toString(), "epoch", transaction.producerEpoch(),
                    "timeout_ms", transaction.transactionTimeoutMs(), "start_ms", transaction.transactionStartTimeMs().orElse(-1),
                    "partitions", transaction.topicPartitions().stream().map(Object::toString).sorted().toList()));
            for (var isolation : org.apache.kafka.common.IsolationLevel.values()) {
                var offsets = admin.listOffsets(Map.of(new org.apache.kafka.common.TopicPartition("offsets-native", 0),
                        org.apache.kafka.clients.admin.OffsetSpec.latest()), new org.apache.kafka.clients.admin.ListOffsetsOptions(isolation))
                        .all().get(10, java.util.concurrent.TimeUnit.SECONDS);
                result.put(isolation.toString(), offsets.values().iterator().next().offset());
            }
        }
        new com.fasterxml.jackson.databind.ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(output).toFile(), result);
    }

    private static void truncateTrigger(String[] args) throws Exception {
        if (!args[1].equals("broker:9092") || !args[2].matches("packaged\\.DCNC[0-9]{4}\\.B")
                || !args[4].matches("/control/[a-z0-9-]+\\.json")) {
            throw new IllegalArgumentException("Owned local fixture topic and trigger file required");
        }
        long first = Long.parseLong(args[3]);
        if (first < 0 || first > 100000 || java.nio.file.Files.exists(Path.of(args[4]))) {
            throw new IllegalArgumentException("Fresh bounded trigger required");
        }
        var config = new java.util.Properties();
        config.setProperty("bootstrap.servers", args[1]);
        config.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.setProperty("enable.auto.commit", "false");
        config.setProperty("allow.auto.create.topics", "false");
        config.setProperty("isolation.level", "read_committed");
        config.setProperty("default.api.timeout.ms", "10000");
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(config)) {
            var partition = new org.apache.kafka.common.TopicPartition(args[2], 0);
            consumer.assign(List.of(partition));
            consumer.seek(partition, first);
            consumer.endOffsets(List.of(partition));
            java.nio.file.Files.createFile(Path.of(args[4] + ".ready"));
            long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(25))) {
                    if (record.offset() - first > 10000) {
                        throw new IllegalStateException("Trigger record bound exceeded");
                    }
                    if (record.value() != null && "t".equals(mapper.readTree(record.value()).path("payload").path("op").asText())) {
                        if (record.key() != null) {
                            throw new IllegalStateException("Truncate key is not null");
                        }
                        // The host observes a completely written file and kills only
                        // its owned worker; the audit never controls the source task.
                        Path target = Path.of(args[4]);
                        Path temporary = target.resolveSibling(target.getFileName() + ".tmp");
                        mapper.writeValue(temporary.toFile(), Map.of("offset", record.offset(), "timestamp", record.timestamp(), "isolation", "read_committed"));
                        java.nio.file.Files.move(temporary, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
                        return;
                    }
                }
            }
            throw new IllegalStateException("No newly committed truncate observed before deadline");
        }
    }

    private static void nativeJournal(String[] args) throws Exception {
        String library = args[1];
        if (!library.matches("DCNC[0-9]{4}") || !args[2].matches("QSQJRN[0-9]{4}") || !args[5].startsWith("/control/")) {
            throw new IllegalArgumentException("Explicit owned fixture receiver and /control/output required");
        }
        var first = new java.math.BigInteger(args[3]);
        var last = new java.math.BigInteger(args[4]);
        if (first.signum() <= 0 || last.compareTo(first) < 0 || last.subtract(first).compareTo(java.math.BigInteger.valueOf(10000)) > 0) {
            throw new IllegalArgumentException("Bounded native audit interval required");
        }
        var credentials = new java.util.Properties();
        credentials.load(System.in);
        List<Map<String, Object>> result = new ArrayList<>();
        try (var source = new As400NativeConnectSource(credentials, library, Path.of("/control"), "broker:9092", "unused-audit");
                var connection = source.connection(false)) {
            // connection() verifies the configured server. This audit never creates history, takes
            // source locks, starts initialization, or publishes any records.
            connection.setReadOnly(true);
            var receiver = new io.debezium.ibmi.db2.journal.retrieve.JournalReceiver(args[2], library);
            var page = source.guardedReader.readRawPage(new io.debezium.ibmi.db2.journal.retrieve.JournalPosition(first, receiver),
                    new io.debezium.ibmi.db2.journal.retrieve.JournalPosition(last, receiver), 2000);
            if (page.continuation().isPresent()) {
                throw new IllegalStateException("Native audit does not fit one bounded page");
            }
            for (var entry : page.entries()) {
                result.add(Map.of("sequence", entry.position().offset().toString(), "receiver", receiver.name(), "library", library,
                        "code", Character.toString(entry.header().getJournalCode()), "type", entry.header().getEntryType(),
                        "hex", java.util.HexFormat.of().formatHex(entry.data())));
            }
        }
        new com.fasterxml.jackson.databind.ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(args[5]).toFile(), result);
    }
}
