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
    @ValueSource(strings = { "empty", "records", "removed-prefix", "multiple-partitions", "unavailable" })
    void shouldPermitFirstSnapshotOnlyWhenKafkaHistoryHasNeverContainedRecords(String state) throws Exception {
        var partition = new TopicPartition("history", 0);
        var info = mock(PartitionInfo.class);
        var config = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "unused.invalid:9092")
                .with(KafkaSchemaHistory.TOPIC, "history").build();
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, ignored) -> {
            when(consumer.listTopics()).thenReturn(Map.of("history", List.of(info)));
            when(consumer.partitionsFor(anyString(), any())).thenReturn(state.equals("multiple-partitions") ? List.of(info, info) : List.of(info));
            when(consumer.beginningOffsets(any(), any())).thenReturn(Map.of(partition, state.equals("removed-prefix") ? 9L : 0L));
            when(consumer.endOffsets(any(), any())).thenReturn(Map.of(partition, state.equals("empty") ? 0L : 9L));
            if (state.equals("unavailable")) {
                when(consumer.endOffsets(any(), any())).thenThrow(new org.apache.kafka.common.errors.TimeoutException("unavailable"));
            }
        })) {
            var history = new As400KafkaBindingHistory();
            history.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            if (state.equals("empty")) {
                history.requireEmptyForSnapshot();
            }
            else {
                assertThrows(Exception.class, history::requireEmptyForSnapshot);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "records", "bytes" })
    void shouldRecoverOnlyTheBoundedEligiblePrefixWhenLateProposalsExceedCapacity(String bound) throws Exception {
        var f = new Fixture(directory);
        f.initialize();
        String encoded = Files.readString(f.file).trim();
        if (bound.equals("bytes")) {
            var doc = io.debezium.document.DocumentReader.defaultReader().read(encoded);
            doc.setString("padding", "x".repeat(3 * 1024 * 1024));
            encoded = io.debezium.document.DocumentWriter.defaultWriter().write(doc);
        }
        byte[] value = encoded.getBytes(StandardCharsets.UTF_8);
        int eligible = Math.min(As400BindingHistory.MAX_HISTORY_RECORDS, As400BindingHistory.MAX_HISTORY_BYTES / value.length);
        var partition = new TopicPartition("history", 0);
        var input = new ArrayList<ConsumerRecord<byte[], byte[]>>();
        for (int i = 0; i <= eligible; i++) {
            input.add(new ConsumerRecord<>("history", 0, i, null, value));
        }
        var config = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "unused.invalid:9092")
                .with(KafkaSchemaHistory.TOPIC, "history").build();
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, ignored) -> {
            when(consumer.partitionsFor(anyString(), any())).thenReturn(List.of(mock(PartitionInfo.class)));
            when(consumer.beginningOffsets(any(), any())).thenReturn(Map.of(partition, 0L));
            when(consumer.endOffsets(any(), any())).thenReturn(Map.of(partition, (long) input.size()));
            when(consumer.poll(any(java.time.Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(partition, input), Map.of()));
        })) {
            for (int restart = 0; restart < 2; restart++) {
                var history = new As400KafkaBindingHistory();
                history.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
                history.allowCandidateAppends();
                var recovered = new ArrayList<HistoryRecord>();
                history.recoverRecords(recovered::add);
                assertThat(recovered).hasSize(eligible);
                assertThrows(As400BindingHistory.CandidateHistoryFullException.class, () -> history.recoveryBudget().requireAppend(value.length));
            }
            var strict = new As400KafkaBindingHistory();
            strict.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            assertThrows(Exception.class, () -> strict.recoverRecords(record -> {
            }));
            var paged = new As400KafkaBindingHistory();
            paged.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            paged.allowPagedCandidates();
            var count = new java.util.concurrent.atomic.AtomicInteger();
            paged.recoverRecords(record -> count.incrementAndGet());
            assertThat(count.get()).isEqualTo(input.size());
            paged.recoveryBudget().requireAppend(value.length);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "candidate-growing", "strict-growing", "candidate-gap", "candidate-null", "candidate-removed", "candidate-truncated", "reconfigured",
            "paged-growing", "paged-gap", "paged-null", "paged-removed", "paged-truncated" })
    void immutableCandidateReadAllowsOnlyAppendBeyondItsVerifiedCut(String mode) throws Exception {
        var f = new Fixture(directory);
        f.initialize();
        byte[] value = Files.readString(f.file).trim().getBytes(StandardCharsets.UTF_8);
        var partition = new TopicPartition("history", 0);
        var raw = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "unused.invalid:9092")
                .with(KafkaSchemaHistory.TOPIC, "history").build();
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, ignored) -> {
            when(consumer.partitionsFor(anyString(), any())).thenReturn(List.of(mock(PartitionInfo.class)));
            when(consumer.beginningOffsets(any(), any())).thenReturn(Map.of(partition, 0L), Map.of(partition, mode.endsWith("-removed") ? 1L : 0L));
            when(consumer.endOffsets(any(), any())).thenReturn(Map.of(partition, 1L), Map.of(partition, mode.endsWith("-truncated") ? 0L : 2L));
            var first = new ConsumerRecord<byte[], byte[]>("history", 0, mode.endsWith("-gap") ? 1 : 0, null,
                    mode.endsWith("-null") ? null : value);
            var appended = new ConsumerRecord<byte[], byte[]>("history", 0, 1, null, value);
            when(consumer.poll(any(java.time.Duration.class))).thenReturn(new ConsumerRecords<>(Map.of(partition, List.of(first, appended)), Map.of()));
        })) {
            var history = new As400KafkaBindingHistory();
            history.configure(raw, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            if (!mode.equals("strict-growing")) {
                history.allowCandidateAppends();
            }
            if (mode.startsWith("paged-")) {
                history.allowPagedCandidates();
            }
            if (mode.equals("reconfigured")) {
                history.configure(raw, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            }
            var records = new ArrayList<HistoryRecord>();
            if (mode.equals("candidate-growing") || mode.equals("paged-growing")) {
                history.recoverRecords(records::add);
                assertThat(records).hasSize(1);
            }
            else {
                assertThrows(Exception.class, () -> history.recoverRecords(records::add));
            }
        }
    }
}
