/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class As400InitializationAcknowledgementsTest {
    private SourceRecord record() {
        return new SourceRecord(Map.of("server", "test"), Map.of("initialization.phase", "baseline-complete"), "events-test", Schema.STRING_SCHEMA, "BASELINE");
    }

    private RecordMetadata metadata(String topic, int partition, long offset) {
        return new RecordMetadata(new TopicPartition(topic, partition), offset, 0, 0, 1, 1);
    }

    @Test
    void enqueueAndRecordAcknowledgementCannotReleaseBarrierBeforeCommit() throws Exception {
        var acknowledgements = new As400InitializationAcknowledgements();
        var record = record();
        acknowledgements.register(record);
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(record, Duration.ZERO));
        acknowledgements.committed();
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(record, Duration.ZERO));
        acknowledgements.record(record, metadata("events-test", 0, 7));
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(record, Duration.ZERO));
        acknowledgements.committed();
        assertThat(acknowledgements.await(record, Duration.ZERO)).isEqualTo("kafka:events-test:0:7");
    }

    @ParameterizedTest
    @ValueSource(strings = { "dropped", "wrong-topic", "wrong-partition", "missing-offset", "unregistered", "duplicate" })
    void invalidAcknowledgementPermanentlyFailsTheSession(String fault) {
        var acknowledgements = new As400InitializationAcknowledgements();
        var record = record();
        acknowledgements.register(record);
        RecordMetadata metadata = switch (fault) {
            case "dropped" -> null;
            case "wrong-topic" -> metadata("other", 0, 7);
            case "wrong-partition" -> metadata("events-test", 1, 7);
            case "missing-offset" -> metadata("events-test", 0, -1);
            default -> metadata("events-test", 0, 7);
        };
        if (fault.equals("duplicate")) {
            acknowledgements.record(record, metadata);
        }
        assertThrows(IllegalStateException.class, () -> acknowledgements.record(fault.equals("unregistered") ? record() : record, metadata));
        assertThrows(IllegalStateException.class, acknowledgements::committed);
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(record, Duration.ZERO));
    }

    @Test
    void laterBarrierCannotHideAnUnacknowledgedSnapshotRecord() {
        var acknowledgements = new As400InitializationAcknowledgements();
        acknowledgements.register(record());
        var barrier = record();
        acknowledgements.register(barrier);
        acknowledgements.record(barrier, metadata("events-test", 0, 7));
        acknowledgements.committed();
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(barrier, Duration.ZERO));
    }

    @Test
    void stopWakesWaitingInitializationAndRejectsLateCallbacks() throws Exception {
        var acknowledgements = new As400InitializationAcknowledgements();
        var record = record();
        acknowledgements.register(record);
        var waiting = CompletableFuture.supplyAsync(() -> assertThrows(IllegalStateException.class,
                () -> acknowledgements.await(record, Duration.ofSeconds(20))));
        acknowledgements.stop();
        assertThat(waiting.get(5, TimeUnit.SECONDS)).hasMessageContaining("stopped");
        assertThrows(IllegalStateException.class, () -> acknowledgements.record(record, metadata("events-test", 0, 7)));
        assertThrows(IllegalStateException.class, acknowledgements::committed);
        assertThrows(IllegalStateException.class, () -> acknowledgements.register(record()));
    }

    @Test
    void continuousDurablePrefixesDoNotExhaustLifetimeCapacityAndLateCallbacksStillFail() throws Exception {
        var acknowledgements = new As400InitializationAcknowledgements();
        SourceRecord last = null;
        for (int i = 0; i < 512; i++) {
            last = record();
            acknowledgements.register(last);
            acknowledgements.record(last, metadata("events-test", 0, i));
            acknowledgements.committed();
            assertThat(acknowledgements.await(last, Duration.ZERO)).isEqualTo("kafka:events-test:0:" + i);
            acknowledgements.retireDurableThrough(last);
        }
        SourceRecord retired = last;
        assertThrows(IllegalStateException.class, () -> acknowledgements.record(retired, metadata("events-test", 0, 511)));
        assertThrows(IllegalStateException.class, acknowledgements::committed);
    }

    @Test
    void retirementCannotHideIncompletePrefixOrUseAnEqualButUnregisteredRecord() throws Exception {
        var acknowledgements = new As400InitializationAcknowledgements();
        var first = record();
        var barrier = record();
        acknowledgements.register(first);
        acknowledgements.register(barrier);
        acknowledgements.record(barrier, metadata("events-test", 0, 8));
        acknowledgements.committed();
        assertThrows(IllegalStateException.class, () -> acknowledgements.retireDurableThrough(barrier));
        acknowledgements.record(first, metadata("events-test", 0, 7));
        acknowledgements.committed();
        assertThrows(IllegalStateException.class, () -> acknowledgements.retireDurableThrough(record()));
        assertThat(acknowledgements.await(barrier, Duration.ZERO)).isEqualTo("kafka:events-test:0:8");
        acknowledgements.retireDurableThrough(first);
        assertThat(acknowledgements.await(barrier, Duration.ZERO)).isEqualTo("kafka:events-test:0:8");
        acknowledgements.retireDurableThrough(barrier);
    }

    @ParameterizedTest
    @ValueSource(strings = { "exactly.once.support", "transaction.boundary", "errors.tolerance", "transforms", "event.converting.failure.handling.mode" })
    void unqualifiedRuntimeCannotUseCommitCallbacksAsSnapshotProof(String key) {
        Map<String, String> profile = new HashMap<>(Map.of("exactly.once.support", "requested", "transaction.boundary", "poll", "errors.tolerance", "none",
                "event.converting.failure.handling.mode", "fail"));
        As400InitializationAcknowledgements.requireProfile(profile);
        profile.put(key, "unqualified");
        assertThrows(IllegalArgumentException.class, () -> As400InitializationAcknowledgements.requireProfile(profile));
    }
}
