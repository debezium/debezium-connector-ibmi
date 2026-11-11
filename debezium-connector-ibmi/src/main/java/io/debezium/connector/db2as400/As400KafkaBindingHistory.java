/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/**
 * Standard Kafka history writes/storage with strict bounded recovery. The base
 * recovery loop skips null/invalid/unparseable records before notifying listeners.
 * A binding manifest must instead reject them, holes, or a moving history cut.
 * Online proposals use a bounded eligible prefix: excess stale appends cannot be
 * selected or invalidate an earlier acknowledged candidate. Strict baseline
 * history still rejects overflow. Dedicated one-partition, immutable history only.
 */
final class As400KafkaBindingHistory extends KafkaSchemaHistory {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private Properties readConfig;
    private String topic;
    private boolean candidateAppends;
    private As400BindingHistory.HistoryBudget budget = new As400BindingHistory.HistoryBudget();

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        candidateAppends = false;
        topic = config.getString(TOPIC);
        readConfig = config.subset(CONFIGURATION_FIELD_PREFIX_STRING + "consumer.", true).edit()
                .with(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(BOOTSTRAP_SERVERS))
                .with(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
                .with(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class)
                .with(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                .with(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false)
                .with(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none")
                .with(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted")
                .with(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 30_000)
                .with(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000).build().asProperties();
    }

    /** Only immutable online proposals may append beyond this read's fixed log cut. */
    void allowCandidateAppends() {
        candidateAppends = true;
    }

    As400BindingHistory.HistoryBudget recoveryBudget() {
        return budget;
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) throws InterruptedException {
        checkInterrupted();
        budget = new As400BindingHistory.HistoryBudget();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(readConfig)) {
            var partition = new TopicPartition(topic, 0);
            var partitions = List.of(partition);
            require(consumer.partitionsFor(topic, TIMEOUT).size() == 1, "Exactly one binding-history partition required");
            consumer.assign(partitions);
            require(consumer.beginningOffsets(partitions, TIMEOUT).get(partition) == 0, "Binding history prefix was removed");
            long end = consumer.endOffsets(partitions, TIMEOUT).get(partition);
            require(end > 0 && (candidateAppends || end <= As400BindingHistory.MAX_HISTORY_RECORDS), "Missing or oversized binding history");
            if (candidateAppends) {
                end = Math.min(end, As400BindingHistory.MAX_HISTORY_RECORDS);
            }
            consumer.seek(partition, 0);
            long next = 0;
            long deadline = System.nanoTime() + TIMEOUT.toNanos();
            recovery: while (next < end) {
                checkInterrupted();
                require(System.nanoTime() < deadline, "Incomplete binding history recovery");
                var batch = consumer.poll(Duration.ofMillis(100));
                for (var record : batch) {
                    checkInterrupted();
                    if (candidateAppends && next == end && record.offset() >= end) {
                        break;
                    }
                    require(record.offset() == next && record.offset() < end, "Missing, reordered or concurrently appended history record");
                    require(record.value() != null, "Null binding history record");
                    if (!budget.accept(record.value().length, candidateAppends)) {
                        break recovery;
                    }
                    HistoryRecord history;
                    try {
                        // Invalid UTF-8 and JSON are errors, never replacement text or skipped records.
                        String value = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(record.value())).toString();
                        history = new HistoryRecord(DocumentReader.defaultReader().read(value));
                        require(history.isValid(), "Incomplete binding history record");
                    }
                    catch (Exception e) {
                        throw new SchemaHistoryException("Malformed binding history at offset " + record.offset(), e);
                    }
                    records.accept(history);
                    next++;
                }
            }
            long currentEnd = consumer.endOffsets(partitions, TIMEOUT).get(partition);
            require(consumer.beginningOffsets(partitions, TIMEOUT).get(partition) == 0
                    && (candidateAppends ? currentEnd >= end : currentEnd == end), "Binding history changed during recovery");
        }
    }

    private static void checkInterrupted() throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Stopped during binding history recovery");
        }
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new SchemaHistoryException(message);
        }
    }
}
