/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Independent, bounded, read-only broker evidence for the local synthetic probe. No producer or IBM i client. */
public final class As400ConnectBrokerEvidence {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length != 2 || !args[0].matches("[a-z][a-z-]{0,30}") || !args[1].startsWith("/control/")) {
            throw new IllegalArgumentException("synthetic-scenario /control/output.json");
        }
        String scenario = args[0];
        List<TopicPartition> partitions = List.of("configs-" + scenario, "offsets-" + scenario, "status-" + scenario,
                "events-" + scenario, "events-" + scenario + "-tx", "events-" + scenario + "-hb")
                .stream().map(topic -> new TopicPartition(topic, 0)).toList();
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("started", Instant.now().toString());
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000, AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000))) {
            Map<String, Object> transactions = new LinkedHashMap<>();
            for (String id : List.of("connect-cluster-group-" + scenario, "group-" + scenario + "-" + scenario + "-0")) {
                try {
                    var description = admin.describeTransactions(List.of(id)).description(id).get(10, TimeUnit.SECONDS);
                    Map<String, Object> value = new LinkedHashMap<>();
                    value.put("state", description.state().toString());
                    value.put("producer_id", description.producerId());
                    value.put("producer_epoch", description.producerEpoch());
                    value.put("timeout_ms", description.transactionTimeoutMs());
                    value.put("start_ms", description.transactionStartTimeMs().isPresent() ? description.transactionStartTimeMs().getAsLong() : null);
                    value.put("partitions", description.topicPartitions().stream().map(Object::toString).sorted().toList());
                    transactions.put(id, value);
                }
                catch (Exception e) {
                    transactions.put(id, Map.of("error", e.toString()));
                }
            }
            report.put("transactions", transactions);
            for (IsolationLevel isolation : List.of(IsolationLevel.READ_COMMITTED, IsolationLevel.READ_UNCOMMITTED)) {
                Map<String, Object> sample = new LinkedHashMap<>();
                sample.put("started", Instant.now().toString());
                try {
                    Map<TopicPartition, OffsetSpec> request = partitions.stream().collect(Collectors.toMap(p -> p, p -> OffsetSpec.latest()));
                    var ends = admin.listOffsets(request, new ListOffsetsOptions(isolation)).all().get(10, TimeUnit.SECONDS);
                    Map<String, Long> endOffsets = new LinkedHashMap<>();
                    ends.forEach((p, v) -> endOffsets.put(p.topic(), v.offset()));
                    sample.put("admin_end_offsets", endOffsets);
                    sample.put("consumer", snapshot(partitions, isolation));
                }
                catch (Exception e) {
                    sample.put("error", e.toString());
                }
                sample.put("finished", Instant.now().toString());
                report.put(isolation.toString(), sample);
            }
        }
        report.put("finished", Instant.now().toString());
        JSON.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[1]).toFile(), report);
    }

    private static Map<String, Object> snapshot(List<TopicPartition> partitions, IsolationLevel isolation) {
        Map<String, Object> config = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false, ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation.toString(), ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.assign(partitions);
            Map<TopicPartition, Long> starts = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            starts.forEach(consumer::seek);
            Map<String, Object> topics = new LinkedHashMap<>();
            Map<String, List<Map<String, Object>>> records = new LinkedHashMap<>();
            partitions.forEach(p -> records.put(p.topic(), new ArrayList<>()));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            int count = 0;
            while (partitions.stream().anyMatch(p -> consumer.position(p) < ends.get(p))) {
                if (System.nanoTime() > deadline || count > 1000) {
                    throw new IllegalStateException("Evidence snapshot bound exceeded");
                }
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (record.offset() >= ends.get(new TopicPartition(record.topic(), record.partition()))) {
                        continue;
                    }
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("offset", record.offset());
                    row.put("key", record.key());
                    row.put("value", record.value());
                    row.put("timestamp", record.timestamp());
                    if (record.headers().lastHeader("probe") != null) {
                        row.put("probe", new String(record.headers().lastHeader("probe").value(), StandardCharsets.UTF_8));
                    }
                    records.get(record.topic()).add(row);
                    count++;
                }
            }
            for (TopicPartition p : partitions) {
                topics.put(p.topic(), Map.of("start", starts.get(p), "end", ends.get(p), "position", consumer.position(p), "records", records.get(p.topic())));
            }
            return topics;
        }
    }
}
