/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

class As400RecoveryAcknowledgementsTest {
    private static SourceRecord tombstone() {
        return new SourceRecord(Map.of("server", "test"), Map.of("F", "1", "W", "9"), "data", 0, Schema.INT32_SCHEMA, 1, null, null);
    }

    @Test
    void shouldKeepEqualTombstonesDistinctInConnectsValueKeyedAcknowledgementMap() throws Exception {
        var acknowledgements = new As400RecoveryAcknowledgements();
        var workerRecords = new LinkedHashMap<SourceRecord, RecordMetadata>();
        var records = new ArrayList<SourceRecord>();
        assertThat(tombstone()).isEqualTo(tombstone());
        for (int i = 0; i < 300; i++) {
            var record = tombstone();
            acknowledgements.register(record);
            workerRecords.put(record, new RecordMetadata(new TopicPartition("data", 0), i, 0, 0, 0, 0));
            records.add(record);
        }
        assertThat(workerRecords).hasSize(300);
        workerRecords.forEach(acknowledgements::record);
        acknowledgements.committed();
        assertThat(acknowledgements.await(records.get(299), Duration.ofMillis(10))).isEqualTo("kafka:data:0:299");
        acknowledgements.retireDurableThrough(records.get(299));
        assertThat(acknowledgements.hasRecorded()).isFalse();
        assertThat(records).allSatisfy(r -> {
            assertThat(r.value()).isNull();
            assertThat(r.key()).isEqualTo(1);
            assertThat(r.sourceOffset().get("W")).isEqualTo("9");
        });
    }

    @Test
    void shouldWaitThroughAPauseAndStillRejectAnUnacknowledgedCommit() throws Exception {
        var acknowledgements = new As400RecoveryAcknowledgements();
        var record = tombstone();
        acknowledgements.register(record);
        var checked = new CountDownLatch(3);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var waiting = executor.submit(() -> acknowledgements.awaitUntilStopped(record, checked::countDown));
            acknowledgements.committed();
            assertThat(checked.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(waiting.isDone()).isFalse();
            acknowledgements.record(record, new RecordMetadata(new TopicPartition("data", 0), 4, 0, 0, 0, 0));
            acknowledgements.committed();
            assertThat(waiting.get(5, TimeUnit.SECONDS)).isEqualTo("kafka:data:0:4");
        }
        finally {
            acknowledgements.stop();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldCancelAnUnboundedWaitOnStop() throws Exception {
        var acknowledgements = new As400RecoveryAcknowledgements();
        var record = tombstone();
        acknowledgements.register(record);
        acknowledgements.stop();
        assertThrows(IllegalStateException.class, () -> acknowledgements.awaitUntilStopped(record, () -> {
        }));
    }
}
