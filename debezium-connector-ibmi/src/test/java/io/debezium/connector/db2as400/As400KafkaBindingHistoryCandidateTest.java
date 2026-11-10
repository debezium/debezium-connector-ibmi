/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

class As400KafkaBindingHistoryCandidateTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = { "candidate-growing", "strict-growing", "candidate-gap", "candidate-null", "candidate-removed", "candidate-truncated", "reconfigured" })
    void immutableCandidateReadAllowsOnlyAppendBeyondItsVerifiedCut(String mode) throws Exception {
        var f = new Fixture(directory);
        f.initialize();
        byte[] value = Files.readString(f.file).trim().getBytes(StandardCharsets.UTF_8);
        var partition = new TopicPartition("history", 0);
        var raw = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "unused.invalid:9092")
                .with(KafkaSchemaHistory.TOPIC, "history").build();
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, ignored) -> {
            when(consumer.partitionsFor(anyString(), any())).thenReturn(List.of(mock(PartitionInfo.class)));
            when(consumer.beginningOffsets(any(), any())).thenReturn(Map.of(partition, 0L), Map.of(partition, mode.equals("candidate-removed") ? 1L : 0L));
            when(consumer.endOffsets(any(), any())).thenReturn(Map.of(partition, 1L), Map.of(partition, mode.equals("candidate-truncated") ? 0L : 2L));
            var first = new ConsumerRecord<byte[], byte[]>("history", 0, mode.equals("candidate-gap") ? 1 : 0, null,
                    mode.equals("candidate-null") ? null : value);
            var appended = new ConsumerRecord<byte[], byte[]>("history", 0, 1, null, value);
            when(consumer.poll(any(java.time.Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(partition, List.of(first, appended)), Map.of()));
        })) {
            var history = new As400KafkaBindingHistory();
            history.configure(raw, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            if (!mode.equals("strict-growing")) {
                history.allowCandidateAppends();
            }
            if (mode.equals("reconfigured")) {
                history.configure(raw, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            }
            var records = new ArrayList<HistoryRecord>();
            if (mode.equals("candidate-growing")) {
                history.recoverRecords(records::add);
                assertThat(records).hasSize(1);
            }
            else {
                assertThrows(Exception.class, () -> history.recoverRecords(records::add));
            }
        }
    }
}
