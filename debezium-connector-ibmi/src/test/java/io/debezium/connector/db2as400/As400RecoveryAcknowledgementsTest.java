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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class As400RecoveryAcknowledgementsTest {
    @Test
    void shouldReleaseOnlyTheSavedPrefixWhenDeliveryCallbacksRaceAnEarlierFlush() throws Exception {
        var initial = Map.of("F", "1", "W", "9");
        var acknowledgements = new As400RecoveryAcknowledgements(initial);
        var first = tombstone();
        var second = tombstone();
        var sentFirst = acknowledgements.register(first);
        var sentSecond = acknowledgements.register(second);
        assertThat(As400DeliveryOffset.strip(sentFirst.sourceOffset())).isEqualTo(initial);
        assertThat(As400DeliveryOffset.strip(sentSecond.sourceOffset())).isEqualTo(initial);
        assertThat(sentFirst.sourceOffset()).isNotEqualTo(sentSecond.sourceOffset());
        acknowledgements.record(sentFirst, null); // Embedded Engine has no Kafka metadata.
        acknowledgements.record(sentSecond, null); // Arrives after the worker captured the earlier flush.
        acknowledgements.committed(initial);
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(first, Duration.ZERO));
        acknowledgements.committed(sentFirst.sourceOffset());
        assertThat(acknowledgements.await(first, Duration.ZERO)).startsWith("source-offset:");
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(second, Duration.ZERO));
        acknowledgements.retireDurableThrough(first);
        acknowledgements.committed(sentFirst.sourceOffset()); // An empty commit must not promote the second record.
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(second, Duration.ZERO));
        acknowledgements.committed(sentSecond.sourceOffset());
        acknowledgements.await(second, Duration.ZERO);
        acknowledgements.retireDurableThrough(second);
        assertThat(acknowledgements.pendingCount()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "skipped", "foreign-session", "changed-position", "regressed", "missing" })
    void shouldRejectAnUnprovenSourceOffsetAndLatchTheFailure(String fault) throws Exception {
        var acknowledgements = new As400RecoveryAcknowledgements(Map.of("W", "9"));
        var first = tombstone();
        var second = tombstone();
        var sentFirst = acknowledgements.register(first);
        var sentSecond = acknowledgements.register(second);
        if (!fault.equals("skipped")) {
            acknowledgements.record(sentFirst, null);
        }
        acknowledgements.record(sentSecond, null);
        Map<String, ?> bad = sentSecond.sourceOffset();
        if (fault.equals("regressed")) {
            acknowledgements.committed(sentSecond.sourceOffset());
            bad = sentFirst.sourceOffset();
        }
        if (fault.equals("foreign-session") || fault.equals("changed-position")) {
            var changed = new java.util.HashMap<String, Object>(bad);
            changed.put(fault.equals("foreign-session") ? As400DeliveryOffset.SESSION : "W", fault.equals("foreign-session")
                    ? java.util.UUID.randomUUID().toString()
                    : "10");
            bad = changed;
        }
        if (fault.equals("missing")) {
            bad = null;
        }
        Map<String, ?> saved = bad;
        assertThrows(IllegalStateException.class, () -> acknowledgements.committed(saved));
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(second, Duration.ZERO));
    }

    @ParameterizedTest
    @ValueSource(strings = { "version", "session", "sequence", "unknown" })
    void shouldRejectMalformedDeliveryEnvelopesBeforeRecovery(String field) {
        var offset = new java.util.HashMap<String, Object>(new As400DeliveryOffset.Signer(java.util.UUID.randomUUID().toString()).stamp(Map.of("W", "9"), 1));
        offset.put("delivery." + field, "invalid");
        assertThrows(IllegalArgumentException.class, () -> As400DeliveryOffset.strip(offset));
        offset.remove("delivery." + field);
        if (!field.equals("unknown")) {
            assertThrows(IllegalArgumentException.class, () -> As400DeliveryOffset.strip(offset));
        }
    }

    @Test
    void shouldReleaseBoundedDeliveryStateAcrossManyBatchesWithoutAnOffsetFlush() throws Exception {
        var initial = Map.of("F", "1", "W", "9");
        var tracker = new As400RecoveryAcknowledgements(initial);
        tracker.startStreaming();
        Map<String, ?> intermediate = null;
        Map<String, ?> last = null;
        SourceRecord barrier = null;
        for (int i = 1; i <= 10240; i++) {
            // Deliberately equal records: retirement must use identity, not equals.
            barrier = tombstone();
            var sent = tracker.register(barrier);
            tracker.record(sent, null);
            last = sent.sourceOffset();
            if (i == 512) {
                intermediate = last;
            }
            if (i % 256 == 0) {
                assertThat(tracker.pendingCount()).isEqualTo(256);
                tracker.awaitDeliveredAndRetire(barrier, () -> {
                });
                assertThat(tracker.pendingCount()).isZero();
                assertThat(tracker.committedOffset()).isEqualTo(initial);
            }
        }
        assertThat(tracker.hasRecorded()).isTrue();
        tracker.committed(intermediate); // Can prove an old flushed prefix after all records have been released.
        assertThat(tracker.hasRecorded()).isTrue();
        tracker.committed(last);
        assertThat(tracker.hasRecorded()).isFalse();
        assertThat(tracker.pendingCount()).isZero();
        var changed = new java.util.HashMap<String, Object>(last);
        changed.put("W", "1000");
        assertThrows(IllegalStateException.class, () -> tracker.committed(changed));
    }

    @Test
    void shouldKeepBackpressureAndRejectFlushPastAnUndeliveredRecord() throws Exception {
        var tracker = new As400RecoveryAcknowledgements(Map.of("W", "9"));
        tracker.startStreaming();
        var first = tracker.register(tombstone());
        var barrier = tombstone();
        var second = tracker.register(barrier);
        tracker.record(second, null);
        var executor = Executors.newSingleThreadExecutor();
        var waiting = new CountDownLatch(1);
        try {
            var future = executor.submit(() -> {
                tracker.awaitDeliveredAndRetire(barrier, waiting::countDown);
                return true;
            });
            assertThat(waiting.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(future.isDone()).isFalse();
            tracker.record(first, null);
            assertThat(future.get(5, TimeUnit.SECONDS)).isTrue();
            assertThat(tracker.pendingCount()).isZero();
            tracker.committed(second.sourceOffset());
        }
        finally {
            tracker.stop();
            executor.shutdownNow();
        }
        var gap = new As400RecoveryAcknowledgements(Map.of("W", "9"));
        gap.startStreaming();
        gap.register(tombstone());
        var later = gap.register(tombstone());
        gap.record(later, null);
        assertThrows(IllegalStateException.class, () -> gap.committed(later.sourceOffset()));
    }

    @Test
    void shouldResumeAnOlderDeliveryEnvelopeWithoutTreatingItAsANewAcknowledgement() {
        var old = Map.of("W", "9", As400DeliveryOffset.VERSION, "1", As400DeliveryOffset.SESSION,
                java.util.UUID.randomUUID().toString(), As400DeliveryOffset.SEQUENCE, "300");
        var tracker = new As400RecoveryAcknowledgements(old);
        tracker.startStreaming();
        tracker.committed(old);
        assertThat(tracker.committedOffset()).isEqualTo(Map.of("W", "9"));
        assertThat(tracker.hasRecorded()).isFalse();
    }

    @Test
    void shouldRequireTheWholeCommittedPrefixAcrossProducerSelectedPartitions() throws Exception {
        var acknowledgements = new As400RecoveryAcknowledgements();
        var data = new SourceRecord(Map.of("server", "test"), Map.of("W", "9"), "data", Schema.INT32_SCHEMA, 1, Schema.INT32_SCHEMA, 2);
        var barrier = new SourceRecord(Map.of("server", "test"), Map.of("W", "10"), "control", 0, Schema.INT32_SCHEMA, 1, null, null);
        var sentData = acknowledgements.register(data);
        var sentBarrier = acknowledgements.register(barrier);
        // Producer acknowledgements can arrive in another order across partitions.
        acknowledgements.record(sentBarrier, new RecordMetadata(new TopicPartition("control", 0), 4, 0, 0, 0, 0));
        acknowledgements.record(sentData, new RecordMetadata(new TopicPartition("data", 2), 3, 0, 0, 0, 0));
        assertThrows(IllegalStateException.class, () -> acknowledgements.await(barrier, Duration.ZERO));
        acknowledgements.committed();
        assertThat(acknowledgements.await(barrier, Duration.ZERO)).isEqualTo("kafka:control:0:4");
        acknowledgements.retireDurableThrough(barrier);
        assertThat(acknowledgements.pendingCount()).isZero();

        var incomplete = new As400RecoveryAcknowledgements();
        var earlier = tombstone();
        var later = tombstone();
        incomplete.register(earlier);
        var sentLater = incomplete.register(later);
        incomplete.record(sentLater, new RecordMetadata(new TopicPartition("data", 0), 8, 0, 0, 0, 0));
        incomplete.committed();
        assertThrows(IllegalStateException.class, () -> incomplete.await(later, Duration.ZERO));
    }

    @Test
    void shouldRejectTheWrongPartitionWhenTheSourceExplicitlySelectsOne() {
        var acknowledgements = new As400RecoveryAcknowledgements();
        var record = tombstone();
        var sent = acknowledgements.register(record);
        assertThrows(IllegalStateException.class,
                () -> acknowledgements.record(sent, new RecordMetadata(new TopicPartition("data", 2), 4, 0, 0, 0, 0)));
        assertThrows(IllegalStateException.class, acknowledgements::committed);
    }

    private static SourceRecord tombstone() {
        return new SourceRecord(Map.of("server", "test"), Map.of("F", "1", "W", "9"), "data", 0, Schema.INT32_SCHEMA, 1, null, null);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRequireACommittedPrefixEvenWhenTheWorkerIntentionallyFiltersItsRecords(boolean sourceOffsets) throws Exception {
        var transformations = As400TopicRouting.from(Map.of("transforms", "drop", "transforms.drop.type", "org.apache.kafka.connect.transforms.Filter"));
        var tracker = sourceOffsets ? new As400RecoveryAcknowledgements(Map.of("W", "9"), transformations) : new As400RecoveryAcknowledgements(transformations);
        var first = tombstone();
        var barrier = tombstone();
        var sentFirst = tracker.register(first);
        var sentBarrier = tracker.register(barrier);
        try (var filter = new org.apache.kafka.connect.transforms.Filter<SourceRecord>()) {
            filter.configure(Map.of());
            assertThat(filter.apply(sentFirst)).isNull();
        }
        // Kafka's discarded record does not itself write an offset. A retained
        // progress record provides the durable barrier for the filtered prefix.
        tracker.record(sentBarrier, sourceOffsets ? null : new RecordMetadata(new TopicPartition("data", 0), 9, 0, 0, 0, 0));
        assertThrows(IllegalStateException.class, () -> tracker.await(barrier, Duration.ZERO));
        tracker.record(sentFirst, null);
        assertThrows(IllegalStateException.class, () -> tracker.await(barrier, Duration.ZERO));
        if (sourceOffsets) {
            tracker.committed(sentFirst.sourceOffset());
            assertThrows(IllegalStateException.class, () -> tracker.await(barrier, Duration.ZERO));
            tracker.committed(sentBarrier.sourceOffset());
        }
        else {
            tracker.committed();
        }
        tracker.await(barrier, Duration.ZERO);
        tracker.retireDurableThrough(barrier);
        assertThat(tracker.pendingCount()).isZero();
        assertThrows(IllegalStateException.class, () -> tracker.record(sentBarrier, null));
    }

    @ParameterizedTest
    @ValueSource(strings = { "partition", "offset", "receipt", "session" })
    void shouldRejectChangedSourceIdentityEvenWhenTransformsMayChangeThePayload(String fault) {
        var transformations = As400TopicRouting.from(Map.of("transforms", "unwrap", "transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState"));
        var tracker = new As400RecoveryAcknowledgements(Map.of("W", "9"), transformations);
        var sent = tracker.register(tombstone());
        var offset = new java.util.HashMap<String, Object>(sent.sourceOffset());
        if (!fault.equals("partition")) {
            offset.put(fault.equals("offset") ? "W" : "delivery." + fault, "changed");
        }
        var changed = new SourceRecord(fault.equals("partition") ? Map.of("server", "other") : sent.sourcePartition(), offset,
                "any.destination", 2, Schema.STRING_SCHEMA, "converted", Schema.STRING_SCHEMA, "converted");
        assertThrows(IllegalStateException.class, () -> tracker.record(changed, null));
        assertThrows(IllegalStateException.class, () -> tracker.committed(sent.sourceOffset()));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldAcknowledgeTheOriginalRecordOnlyAtItsConfiguredRoutedDestination(boolean sourceOffsets) throws Exception {
        var routing = As400TopicRouting.from(As400TopicRoutingTest.properties());
        var tracker = sourceOffsets ? new As400RecoveryAcknowledgements(Map.of("W", "9"), routing) : new As400RecoveryAcknowledgements(routing);
        var original = tombstone();
        var sent = tracker.register(original);
        assertThat(sent.topic()).isEqualTo("data"); // The worker, not the source, applies the SMT.
        tracker.record(sent, new RecordMetadata(new TopicPartition("routed.data", 0), 4, 0, 0, 0, 0));
        if (sourceOffsets) {
            tracker.committed(sent.sourceOffset());
        }
        else {
            tracker.committed();
        }
        tracker.await(original, Duration.ZERO);
        tracker.retireDurableThrough(original);
        assertThat(tracker.pendingCount()).isZero();

        var wrong = sourceOffsets ? new As400RecoveryAcknowledgements(Map.of("W", "9"), routing) : new As400RecoveryAcknowledgements(routing);
        var wrongSent = wrong.register(tombstone());
        assertThrows(IllegalStateException.class, () -> wrong.record(wrongSent, new RecordMetadata(new TopicPartition("data", 0), 4, 0, 0, 0, 0)));
        assertThrows(IllegalStateException.class, wrong::committed);
    }

    @ParameterizedTest
    @ValueSource(strings = { "offset", "topic", "payload", "partition", "headers" })
    void shouldRejectAnEngineCallbackThatChangesMoreThanTheQualifiedTopic(String fault) {
        var tracker = new As400RecoveryAcknowledgements(Map.of("W", "9"), As400TopicRouting.from(As400TopicRoutingTest.properties()));
        var sent = tracker.register(tombstone());
        var offset = new java.util.HashMap<String, Object>(sent.sourceOffset());
        if (fault.equals("offset")) {
            offset.put("W", "10");
        }
        var headers = sent.headers().duplicate();
        if (fault.equals("headers")) {
            headers.addString("unexpected", "value");
        }
        var changed = new SourceRecord(sent.sourcePartition(), offset, fault.equals("topic") ? "wrong.data" : "routed.data",
                fault.equals("partition") ? 1 : 0, sent.keySchema(), sent.key(), fault.equals("payload") ? Schema.STRING_SCHEMA : null,
                fault.equals("payload") ? "changed" : null, sent.timestamp(), headers);
        assertThrows(IllegalStateException.class, () -> tracker.record(changed, null));
        assertThrows(IllegalStateException.class, () -> tracker.committed(sent.sourceOffset()));
    }

    @Test
    void shouldKeepEqualTombstonesDistinctInConnectsValueKeyedAcknowledgementMap() throws Exception {
        var acknowledgements = new As400RecoveryAcknowledgements();
        var workerRecords = new LinkedHashMap<SourceRecord, RecordMetadata>();
        var records = new ArrayList<SourceRecord>();
        assertThat(tombstone()).isEqualTo(tombstone());
        for (int i = 0; i < 300; i++) {
            var record = tombstone();
            var sent = acknowledgements.register(record);
            workerRecords.put(sent, new RecordMetadata(new TopicPartition("data", 0), i, 0, 0, 0, 0));
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
        var sent = acknowledgements.register(record);
        var checked = new CountDownLatch(3);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var waiting = executor.submit(() -> acknowledgements.awaitUntilStopped(record, checked::countDown));
            acknowledgements.committed();
            assertThat(checked.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(waiting.isDone()).isFalse();
            acknowledgements.record(sent, new RecordMetadata(new TopicPartition("data", 0), 4, 0, 0, 0, 0));
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

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldOwnDeliveryHeadersWhenDeleteAndTombstonesShareTheirInputHeaders(boolean sourceOffsets) throws Exception {
        var tracker = sourceOffsets ? new As400RecoveryAcknowledgements(Map.of("W", "9")) : new As400RecoveryAcknowledgements();
        var shared = new org.apache.kafka.connect.header.ConnectHeaders().addString("application", "preserved");
        var originals = new ArrayList<SourceRecord>();
        var sent = new ArrayList<SourceRecord>();
        for (int i = 0; i < 3; i++) {
            var original = new SourceRecord(Map.of("server", "test"), Map.of("W", "9"), "data", 0, Schema.INT32_SCHEMA, 1,
                    i == 0 ? Schema.STRING_SCHEMA : null, i == 0 ? "deleted" : null, null, shared);
            assertThat(original.headers()).isSameAs(shared);
            originals.add(original);
            sent.add(tracker.register(original));
        }
        assertThat(shared.lastWithName(As400RecoveryAcknowledgements.DELIVERY_HEADER)).isNull();
        assertThat(sent).doesNotHaveDuplicates().allSatisfy(record -> {
            assertThat(record.headers()).isNotSameAs(shared);
            assertThat(record.headers().lastWithName("application").value()).isEqualTo("preserved");
            assertThat(As400DeliveryOffset.strip(record.sourceOffset())).isEqualTo(Map.of("W", "9"));
        });
        for (int i = 0; i < sent.size(); i++) {
            tracker.record(sent.get(i), sourceOffsets ? null : new RecordMetadata(new TopicPartition("data", 0), i, 0, 0, 0, 0));
        }
        if (sourceOffsets) {
            tracker.committed(sent.get(2).sourceOffset());
        }
        else {
            tracker.committed();
        }
        tracker.await(originals.get(2), Duration.ZERO);
        tracker.retireDurableThrough(originals.get(2));
        assertThat(tracker.pendingCount()).isZero();
        assertThrows(IllegalStateException.class, () -> tracker.record(sent.get(2), null));
    }
}
