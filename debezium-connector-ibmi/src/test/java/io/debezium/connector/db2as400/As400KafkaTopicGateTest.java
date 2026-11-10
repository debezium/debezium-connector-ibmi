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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;

class As400KafkaTopicGateTest {
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
