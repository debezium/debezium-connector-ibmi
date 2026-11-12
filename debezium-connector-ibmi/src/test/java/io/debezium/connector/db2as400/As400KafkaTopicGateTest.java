/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;

class As400KafkaTopicGateTest {
    @Test
    void shouldValidateUnroutedHistoryWithoutGuessingRecordDependentOutputDestinations() {
        var config = Configuration.create().with("schema.history.internal.kafka.bootstrap.servers", "unused.invalid:9092")
                .with("schema.history.internal.kafka.topic", "history").with("transforms", "unwrap")
                .with("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState")
                .with("transforms.unwrap.route.by.field", "destination").build();
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, context) -> when(consumer.partitionsFor(anyString(), any()))
                .thenReturn(List.of(mock(PartitionInfo.class))))) {
            As400KafkaTopicGate.require(config, List.of("data", "control", "history"), java.util.Set.of("control", "history"), false, () -> {
            });
            var consumer = consumers.constructed().get(0);
            verify(consumer).partitionsFor(org.mockito.ArgumentMatchers.eq("history"), any());
            verify(consumer, never()).partitionsFor(org.mockito.ArgumentMatchers.eq("data"), any());
            verify(consumer, never()).partitionsFor(org.mockito.ArgumentMatchers.eq("control"), any());
            assertThatThrownBy(() -> As400KafkaTopicGate.require(config, List.of("data", "control", "history"), true, () -> {
            })).hasMessageContaining("predictable empty output");
            assertThat(consumers.constructed()).hasSize(1);
        }
    }

    @Test
    void shouldAllowDataTopicCreationWhilePreflightingRoutedControlAndUnroutedHistory() {
        var properties = new java.util.HashMap<>(As400TopicRoutingTest.properties());
        properties.put("schema.history.internal.kafka.bootstrap.servers", "unused.invalid:9092");
        properties.put("schema.history.internal.kafka.topic", "history");
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, context) -> when(consumer.partitionsFor(anyString(), any()))
                .thenReturn(List.of(mock(PartitionInfo.class))))) {
            As400KafkaTopicGate.require(Configuration.from(properties), List.of("data", "control", "history"), java.util.Set.of("control", "history"),
                    false, () -> {
                    });
            var consumer = consumers.constructed().get(0);
            verify(consumer, never()).partitionsFor(org.mockito.ArgumentMatchers.eq("routed.data"), any());
            verify(consumer).partitionsFor(org.mockito.ArgumentMatchers.eq("routed.control"), any());
            verify(consumer).partitionsFor(org.mockito.ArgumentMatchers.eq("history"), any());
            verify(consumer, never()).partitionsFor(org.mockito.ArgumentMatchers.eq("routed.history"), any());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRejectInvalidTopicInventoriesBeforeOpeningAKafkaClient(boolean unknownOrderedTopic) {
        var properties = new java.util.HashMap<>(As400TopicRoutingTest.properties());
        properties.put("transforms.route.replacement", "history");
        properties.put("schema.history.internal.kafka.topic", "history");
        try (var consumers = mockConstruction(KafkaConsumer.class)) {
            assertThatThrownBy(() -> As400KafkaTopicGate.require(Configuration.from(properties),
                    unknownOrderedTopic ? List.of("data") : List.of("data", "history"),
                    java.util.Set.of(unknownOrderedTopic ? "unknown" : "history"), false, () -> {
                    })).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(unknownOrderedTopic ? "original output/history inventory" : "Distinct data, control and history");
            assertThat(consumers.constructed()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "existing", "empty", "partition-two-data", "missing-offset-observation", "multipartition-history" })
    void shouldAllowPartitionedDataWhileProtectingOrderedHistoryAndEveryInitializationPartition(String scenario) {
        var config = Configuration.create().with("schema.history.internal.kafka.bootstrap.servers", "unused.invalid:9092").build();
        var partition = new TopicPartition("data", 2);
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, context) -> {
            when(consumer.partitionsFor(anyString(), any())).thenAnswer(call -> {
                String name = call.getArgument(0);
                int count = name.equals("data") || scenario.equals("multipartition-history") ? 3 : 1;
                return java.util.stream.IntStream.range(0, count).mapToObj(i -> mock(PartitionInfo.class)).toList();
            });
            when(consumer.beginningOffsets(any())).thenAnswer(call -> ((List<TopicPartition>) call.getArgument(0)).stream()
                    .collect(java.util.stream.Collectors.toMap(p -> p, p -> 0L)));
            when(consumer.endOffsets(any())).thenAnswer(call -> ((List<TopicPartition>) call.getArgument(0)).stream()
                    .filter(p -> !scenario.equals("missing-offset-observation") || !p.equals(partition))
                    .collect(java.util.stream.Collectors.toMap(p -> p, p -> scenario.equals("partition-two-data") && p.equals(partition) ? 1L : 0L)));
        })) {
            Runnable call = () -> As400KafkaTopicGate.require(config, List.of("data", "history"), java.util.Set.of("history"),
                    !scenario.equals("existing"), () -> {
                    });
            if (List.of("existing", "empty").contains(scenario)) {
                call.run();
            }
            else {
                assertThatThrownBy(call::run).isInstanceOf(IllegalStateException.class);
            }
            verify(consumers.constructed().get(0)).close();
        }
    }

    @Test
    void shouldAvoidPerDataTopicRoundTripsWhenOnlyOrdinaryOutputIsRequired() {
        var config = Configuration.create().with("schema.history.internal.kafka.bootstrap.servers", "unused.invalid:9092").build();
        var topics = java.util.stream.IntStream.range(0, 1040).mapToObj(i -> "table-" + i).toList();
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, context) -> when(consumer.partitionsFor(anyString(), any()))
                .thenReturn(List.of(mock(PartitionInfo.class))))) {
            As400KafkaTopicGate.require(config, topics, java.util.Set.of(), false, () -> {
            });
            assertThat(consumers.constructed()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "existing", "new-empty", "existing-initialization", "missing", "multiple", "collision", "stopped", "late-stop" })
    void topicPreflightCannotCreateTopicsOrAdmitAmbiguousOrdering(String mode) {
        var config = Configuration.create().with("schema.history.internal.kafka.bootstrap.servers", "unused.invalid:9092").build();
        var checks = new AtomicInteger();
        try (var consumers = mockConstruction(KafkaConsumer.class, (consumer, context) -> {
            var properties = (Properties) context.arguments().get(0);
            assertThat(properties).containsEntry("allow.auto.create.topics", "false").containsEntry("enable.auto.commit", "false");
            when(consumer.partitionsFor(anyString(), any())).thenReturn(mode.equals("missing") ? List.of()
                    : mode.equals("multiple") ? List.of(mock(PartitionInfo.class), mock(PartitionInfo.class)) : List.of(mock(PartitionInfo.class)));
            when(consumer.beginningOffsets(any())).thenReturn(Map.of(new TopicPartition("data", 0), 0L));
            when(consumer.endOffsets(any())).thenReturn(Map.of(new TopicPartition("data", 0), mode.equals("new-empty") ? 0L : 1L));
        })) {
            Runnable call = () -> As400KafkaTopicGate.require(config, mode.equals("collision") ? List.of("data", "data") : List.of("data"),
                    mode.equals("new-empty") || mode.equals("existing-initialization"), () -> {
                        int seen = checks.incrementAndGet();
                        if (mode.equals("stopped") || mode.equals("late-stop") && seen == 3) {
                            throw new IllegalStateException("Stopped");
                        }
                    });
            if (List.of("existing", "new-empty").contains(mode)) {
                call.run();
            }
            else {
                assertThatThrownBy(call::run).isInstanceOf(RuntimeException.class);
            }
            if (List.of("collision", "stopped").contains(mode)) {
                assertThat(consumers.constructed()).isEmpty();
            }
            else {
                assertThat(consumers.constructed()).hasSize(1);
                verify(consumers.constructed().get(0)).close();
                if (mode.equals("late-stop")) {
                    verify(consumers.constructed().get(0), never()).beginningOffsets(any());
                }
            }
        }
    }
}
