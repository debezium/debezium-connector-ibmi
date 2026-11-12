/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.data.Envelope;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DatabaseSchema;

class As400BatchDispatcherTest {
    private static final String JOURNAL = "LIB/JRN/incarnation1";
    private static final String FINGERPRINT = "v1:" + "a".repeat(64);
    private static final String JID = "0102030405060708090a";
    private static final JournalReceiver RECEIVER = new JournalReceiver("RCV", "LIB");
    private static final JournalReceiver NEXT = new JournalReceiver("NEXT", "LIB");
    private static final TableId TABLE = new TableId("DB", "LIB", "TEST");
    private static final As400Partition PARTITION = new As400Partition("test");
    private static final As400JournalOrder ORDER = new As400JournalOrder(List.of(RECEIVER, NEXT), Map.of(RECEIVER, "receiver-instance", NEXT, "next-instance"),
            Map.of(RECEIVER, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(1000)),
                    NEXT, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(1000))));
    private static final As400Checkpoint INITIAL = As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT, pos(1), "receiver-instance");
    private static final List<String> OPERATIONS = List.of("BEGIN", "t", "c", "d", "tombstone", "c", "d", "tombstone", "c", "END");

    @ParameterizedTest
    @ValueSource(strings = { "warn", "skip" })
    void shouldHonorLossyRowPolicyButNeverSkipAFailedClear(String mode) throws Exception {
        var properties = Map.of("truncate.transactional.enabled", "true", "event.processing.failure.handling.mode", mode);
        var fixture = new Fixture(properties, 20);
        fixture.schema.failValue = 2;
        var records = new ArrayList<SourceRecord>();
        try (var output = fixture.output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactly("BEGIN", "t", "c", "c", "END");
            assertThat(value(records.get(records.size() - 1)).getInt64("event_count")).isEqualTo(3L);
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(9));
        }
        records.clear();
        fixture = new Fixture(properties, 20);
        fixture.schema.available = false;
        try (var output = fixture.output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            assertThatThrownBy(() -> replay.deliver(output)).isInstanceOf(As400ReplayCoordinator.RecoveryException.class)
                    .hasRootCauseMessage("Required buffered dispatch was rejected for DB.LIB.TEST");
            assertThat(records).isEmpty();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(INITIAL.completedThrough());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "warn", "skip" })
    void shouldDiscardPartialKeyChangeAndItsMetadataWhenRowDispatchIsSkipped(String mode) throws Exception {
        var fixture = new Fixture(Map.of("truncate.transactional.enabled", "true", "event.processing.failure.handling.mode", mode), 20);
        var listener = org.mockito.Mockito.mock(io.debezium.pipeline.source.spi.DataChangeEventListener.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            if (invocation.getArgument(5) == Envelope.Operation.CREATE
                    && ((Struct) invocation.getArgument(3)).getInt32("ID") == 2) {
                throw new IllegalStateException("Controlled row dispatch failure after old-key delete");
            }
            return null;
        }).when(listener).onEvent(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        fixture.output.eventDispatcher().setEventListener(listener);
        var records = new ArrayList<SourceRecord>();
        try (var output = fixture.output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactly("BEGIN", "t", "c", "d", "tombstone", "c", "END");
            assertThat(value(records.get(records.size() - 1)).getInt64("event_count")).isEqualTo(4L);
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(9));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "c", "d", "c,u,d" })
    void shouldHonorOrdinaryOperationFiltersWithoutSkippingTransactionRecovery(String skipped) throws Exception {
        var expected = switch (skipped) {
            case "c" -> List.of("BEGIN", "t", "d", "tombstone", "d", "tombstone", "END");
            case "d" -> List.of("BEGIN", "t", "c", "c", "c", "END");
            default -> List.of("BEGIN", "t", "END");
        };
        var records = new ArrayList<SourceRecord>();
        try (var output = new Fixture(Map.of("skipped.operations", skipped), 20).output;
                var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactlyElementsOf(expected);
            long delivered = expected.stream().filter(op -> List.of("t", "c", "d", "u").contains(op)).count();
            assertThat(value(records.get(records.size() - 1)).getInt64("event_count")).isEqualTo(delivered);
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(9));
            assertThat(records).allSatisfy(record -> assertThat(record.sourceOffset().get("completed_through.sequence")).isEqualTo("1"));
            assertThat(output.publishCheckpoint(replay, true)).isTrue();
            assertThat(records.get(records.size() - 1).sourceOffset().get("completed_through.sequence")).isEqualTo("9");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "json-schema", "json", "avro" })
    void convertersPreserveKeylessTruncateAndTombstones(String encoding) throws Exception {
        Fixture fixture = new Fixture(Map.of(), 20);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = fixture.output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactlyElementsOf(OPERATIONS);
            As400OutputContractTest.roundTrip(records, As400OutputContractTest.converter(encoding, true), As400OutputContractTest.converter(encoding, false));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void realMetadataKeyChangeAndTombstonesShareThePreviousCheckpoint(boolean online) throws Exception {
        Fixture fixture = new Fixture(Map.of("truncate.transactional.enabled", Boolean.toString(online)), 20);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = fixture.output; var replay = replay(INITIAL.toOffset(), record -> records.add(record))) {
            feedBatch(replay);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactlyElementsOf(OPERATIONS);
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(9));
            assertThat(records).allSatisfy(record -> {
                assertThat(record.sourcePartition()).isEqualTo(PARTITION.getSourcePartition());
                assertThat(record.sourceOffset().get("completed_through.sequence")).isEqualTo("1");
                assertThat(record.sourceOffset().get("replay_from.sequence")).isEqualTo("2");
                assertThatThrownBy(() -> record.sourceOffset().clear()).isInstanceOf(UnsupportedOperationException.class);
            });
            assertThat(records.get(0).topic()).isEqualTo("test.transaction");
            if (online) {
                assertThat(value(records.get(0)).getString("id")).isEqualTo("10");
            }
            else {
                assertThat(value(records.get(0)).getString("id")).startsWith("dcg-v1:");
            }
            assertThat(value(records.get(9)).getString("id")).isEqualTo(value(records.get(0)).getString("id"));
            assertThat(value(records.get(0)).getInt64("ts_ms")).isEqualTo(2_000L);
            assertThat(value(records.get(9)).getInt64("ts_ms")).isEqualTo(9_000L);
            assertThat(value(records.get(9)).getInt64("event_count")).isEqualTo(6L);
            assertThat(value(records.get(9)).<Struct> getArray("data_collections").get(0).getInt64("event_count")).isEqualTo(6L);
            List<SourceRecord> data = records.stream().filter(r -> r.value() != null && value(r).schema().field("op") != null).toList();
            assertThat(data).extracting(r -> value(r).getStruct("transaction").getInt64("total_order")).containsExactly(1L, 2L, 3L, 4L, 5L, 6L);
            assertThat(data).extracting(r -> value(r).getStruct("source").getString("sequence")).containsExactly("3", "4", "6", "6", "7", "8");
            assertThat(data).extracting(r -> value(r).getStruct("source").getString("rrn")).containsExactly(null, "1", "1", "1", "1", "1");
            assertThat(data).extracting(r -> value(r).getStruct("source").getInt64("ts_ms")).containsExactly(3000L, 4000L, 6000L, 6000L, 7000L, 8000L);
            assertThat(data).allSatisfy(r -> {
                assertThat(value(r).getStruct("source").getString("receiver")).isEqualTo("RCV");
                assertThat(value(r).getStruct("source").getString("receiver_library")).isEqualTo("LIB");
            });
            assertThat(records.get(1).key()).isNull();
            assertThat(records.get(1).keySchema()).isNull();
            assertThat(((Struct) records.get(3).key()).getInt32("ID")).isEqualTo(1);
            assertThat(((Struct) records.get(5).key()).getInt32("ID")).isEqualTo(2);
            assertThat(records.get(3).headers().lastWithName("__debezium.newkey")).isNotNull();
            assertThat(records.get(5).headers().lastWithName("__debezium.oldkey")).isNotNull();
            assertThat(records.get(4).key()).isEqualTo(records.get(3).key());
            assertThat(records.get(4).valueSchema()).isNull();
            assertThat(output.publishCheckpoint(replay, true)).isTrue();
            assertThat(records.get(10).topic()).isEqualTo("__debezium-heartbeat.test");
            assertThat(records.get(10).sourceOffset().get("completed_through.sequence")).isEqualTo("9");
            assertThat(output.publishCheckpoint(replay, false)).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "io.debezium.schema.SchemaTopicNamingStrategy", "io.debezium.connector.db2as400.HostnamePrefixNamingScheme" })
    void shouldUseLegacyTopicOverridesForEveryTransactionRecordAndCheckpoint(String strategy) throws Exception {
        var records = new ArrayList<SourceRecord>();
        try (var output = new Fixture(Map.of("topic.naming.strategy", strategy, "topic.delimiter", "-", "topic.heartbeat.prefix", "beats",
                "topic.transaction", "transactions"), 20).output;
                var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactlyElementsOf(OPERATIONS);
            assertThat(records.get(0).topic()).isEqualTo("test-transactions");
            assertThat(records.get(9).topic()).isEqualTo("test-transactions");
            String dataTopic = strategy.endsWith("HostnamePrefixNamingScheme") ? "test-LIB-TEST" : "test-DB-LIB-TEST";
            assertThat(records.subList(1, 9)).allSatisfy(record -> {
                assertThat(record.topic()).isEqualTo(dataTopic);
                assertThat(record.sourceOffset()).isEqualTo(records.get(0).sourceOffset());
                assertThat(record.sourceOffset().get("completed_through.sequence")).isEqualTo("1");
                assertThat(record.sourceOffset().get("replay_from.sequence")).isEqualTo("2");
            });
            assertThat(output.publishCheckpoint(replay, true)).isTrue();
            assertThat(records.get(10).topic()).isEqualTo("beats-test");
            assertThat(records.get(10).sourceOffset().get("completed_through.sequence")).isEqualTo("9");
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 })
    void freshDispatcherAndCoordinatorReplayEveryRealRecordAfterInterruptedOutput(int cut) throws Exception {
        List<SourceRecord> escaped = new ArrayList<>();
        Map<String, ?> durable;
        try (var output = new Fixture(Map.of(), 20).output;
                var first = replay(INITIAL.toOffset(), record -> escaped.size() < cut && escaped.add(record))) {
            feedBatch(first);
            assertThatThrownBy(() -> first.deliver(output)).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
            assertThat(first.checkpoint().completedThrough()).isEqualTo(pos(1));
            durable = escaped.isEmpty() ? INITIAL.toOffset() : escaped.get(escaped.size() - 1).sourceOffset();
        }
        List<SourceRecord> repeated = new ArrayList<>();
        try (var output = new Fixture(Map.of(), 20).output; var restarted = replay(durable, repeated::add)) {
            feedBatch(restarted);
            restarted.deliver(output);
            assertThat(repeated).extracting(As400BatchDispatcherTest::operation).containsExactlyElementsOf(OPERATIONS);
            assertThat(value(repeated.get(9)).getInt64("event_count")).isEqualTo(6L);
            for (int index = 0; index < escaped.size(); index++) {
                assertThat(repeated.get(index).key()).isEqualTo(escaped.get(index).key());
                assertThat(repeated.get(index).value()).isEqualTo(escaped.get(index).value());
                assertThat(repeated.get(index).sourceOffset()).isEqualTo(escaped.get(index).sourceOffset());
            }
        }
    }

    @Timeout(2)
    @ParameterizedTest
    @ValueSource(ints = { 1, 3, 8, 9 })
    void oversizeOutputFailsBeforeAnyRecordEscapesWithoutBlocking(int capacity) throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of(), capacity).output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            assertThatThrownBy(() -> replay.deliver(output)).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
            assertThat(records).isEmpty();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(1));
            assertThatThrownBy(() -> output.publishCheckpoint(replay, true)).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void missingSchemaFailsBeforeBeginOrClearIsPublished() throws Exception {
        Fixture fixture = new Fixture(Map.of(), 20);
        fixture.schema.available = false;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = fixture.output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            assertThatThrownBy(() -> replay.deliver(output)).hasRootCauseMessage("No metadata registered for captured table DB.LIB.TEST");
            assertThat(records).isEmpty();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(1));
        }
    }

    @Test
    void conversionFailureAfterEarlierChangesCannotLeakAPartialBatch() throws Exception {
        Fixture fixture = new Fixture(Map.of(), 20);
        fixture.schema.failValue = 2;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = fixture.output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            assertThatThrownBy(() -> replay.deliver(output)).hasRootCauseMessage("Fixture value conversion failed");
            assertThat(records).isEmpty();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(1));
        }
    }

    @Test
    void sourceCoordinatesRemainAttachedToEachChangeAcrossReceiverRollover() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of(), 20).output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feed(replay, 2, "C/SC", 0);
            feed(replay, 3, "D/CG", 8);
            feed(replay, NEXT, 4, "R/PT", 1);
            feed(replay, NEXT, 5, "C/CM", 0);
            replay.deliver(output);
            assertThat(value(records.get(1)).getStruct("source").getString("receiver")).isEqualTo("RCV");
            assertThat(value(records.get(1)).getStruct("source").getString("sequence")).isEqualTo("3");
            assertThat(value(records.get(1)).getStruct("source").getString("rrn")).isNull();
            assertThat(value(records.get(2)).getStruct("source").getString("receiver")).isEqualTo("NEXT");
            assertThat(value(records.get(2)).getStruct("source").getString("sequence")).isEqualTo("4");
            assertThat(value(records.get(2)).getStruct("source").getString("rrn")).isEqualTo("1");
            output.publishCheckpoint(replay, true);
            assertThat(records.get(4).sourceOffset().get("completed_through.receiver")).isEqualTo("NEXT");
            assertThat(records.get(0).sourceOffset().get("completed_through.receiver")).isEqualTo("RCV");
        }
    }

    @Test
    void updateWithoutKeyChangeRemainsOneDataEvent() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of(), 20).output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feed(replay, 2, "C/SC", 0);
            feed(replay, 3, "R/UB", 1);
            feed(replay, 4, "R/UP", 1);
            feed(replay, 5, "C/CM", 0);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactly("BEGIN", "u", "END");
            assertThat(value(records.get(2)).getInt64("event_count")).isEqualTo(1L);
            assertThat(value(records.get(1)).getStruct("before").getInt32("ID")).isEqualTo(1);
            assertThat(value(records.get(1)).getStruct("after").getInt32("ID")).isEqualTo(1);
        }
    }

    @Test
    void emptyCommittedCycleAdvancesOnlyThroughAnOrderedHeartbeat() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of(), 20).output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feed(replay, 2, "C/SC", 0);
            feed(replay, 3, "C/CM", 0);
            output.publishCheckpoint(replay, true);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactly("HB");
            assertThat(records.get(0).sourceOffset().get("completed_through.sequence")).isEqualTo("3");
        }
    }

    @Test
    void disabledMetadataAndTombstonesProduceOnlyRequiredData() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of("provide.transaction.metadata", "false", "tombstones.on.delete", "false"), 6).output;
                var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactly("t", "c", "d", "c", "d", "c");
            assertThat(records).allSatisfy(r -> assertThat(value(r).getStruct("transaction")).isNull());
        }
    }

    @Test
    void openAndRolledBackTransactionsOnlyPublishSafeHeartbeatProgress() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of(), 20).output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feed(replay, 2, "C/SC", 0);
            feed(replay, 3, "D/CG", 8);
            output.publishCheckpoint(replay, true);
            assertThat(records.get(0).sourceOffset().get("completed_through.sequence")).isEqualTo("1");
            assertThat(records.get(0).sourceOffset().get("replay_from.sequence")).isEqualTo("2");
            feed(replay, 4, "C/RB", 0);
            output.publishCheckpoint(replay, true);
            assertThat(records).extracting(As400BatchDispatcherTest::operation).containsExactly("HB", "HB");
            assertThat(records.get(1).sourceOffset().get("completed_through.sequence")).isEqualTo("4");
        }
    }

    @Test
    void shouldKeepCheckpointRecoveryWhenPublicHeartbeatsAndTombstonesAreDisabled() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of("heartbeat.interval.ms", "0", "tombstones.on.delete", "false"), 20).output;
                var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            replay.deliver(output);
            assertThat(records).extracting(As400BatchDispatcherTest::operation)
                    .containsExactlyElementsOf(OPERATIONS.stream().filter(op -> !op.equals("tombstone")).toList());
            assertThat(records).allSatisfy(record -> assertThat(record.sourceOffset().get("completed_through.sequence")).isEqualTo("1"));
            assertThat(output.publishCheckpoint(replay, false)).isFalse();
            assertThat(output.publishCheckpoint(replay, true)).isTrue();
            var barrier = records.get(records.size() - 1);
            assertThat(barrier.topic()).isEqualTo("__debezium-truncate.test");
            assertThat(barrier.sourceOffset().get("completed_through.sequence")).isEqualTo("9");
            try (var restarted = replay(barrier.sourceOffset(), ignored -> {
                throw new AssertionError("Completed batch must not be republished after a durable checkpoint");
            })) {
                assertThat(restarted.checkpoint().completedThrough()).isEqualTo(pos(9));
            }
        }
    }

    @Test
    void heartbeatTimerOutputsStayInsideTheFrozenBatch() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of("heartbeat.interval.ms", "1"), 30).output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            Thread.sleep(10); // Real Debezium timer; no scheduler or mutable offset callback.
            replay.deliver(output);
            assertThat(records).anyMatch(r -> operation(r).equals("HB"));
            assertThat(records).allSatisfy(r -> assertThat(r.sourceOffset().get("completed_through.sequence")).isEqualTo("1"));
            assertThat(records.stream().filter(r -> !operation(r).equals("HB"))).extracting(As400BatchDispatcherTest::operation).containsExactlyElementsOf(OPERATIONS);
        }
    }

    @Test
    void anotherThreadCannotPublishOrReopenTheAdapter() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of(), 20).output; var replay = replay(INITIAL.toOffset(), records::add)) {
            AtomicReference<Exception> failure = new AtomicReference<>();
            Thread thread = new Thread(() -> {
                try {
                    output.publishCheckpoint(replay, true);
                }
                catch (Exception e) {
                    failure.set(e);
                }
            });
            thread.start();
            thread.join();
            assertThat(failure.get()).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> output.publishCheckpoint(replay, true)).isInstanceOf(IllegalStateException.class);
            assertThat(records).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "fail", "warn", "skip" })
    void interruptionInsideRealDispatchStopsTheBatchAndPreservesCancellation(String mode) throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of("provide.transaction.metadata", "false", "truncate.transactional.enabled", "true",
                "event.processing.failure.handling.mode", mode), 20).output;
                var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            Thread.currentThread().interrupt();
            try {
                assertThatThrownBy(() -> replay.deliver(output)).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
                assertThat(Thread.currentThread().isInterrupted()).isTrue();
            }
            finally {
                Thread.interrupted();
            }
            assertThat(records).isEmpty();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(1));
        }
    }

    @Test
    void swallowedReentrantHeartbeatAttemptStillFailsTheWholeBatch() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of(), 20).output; var replay = replay(INITIAL.toOffset(), records::add)) {
            feedBatch(replay);
            assertThatThrownBy(() -> replay.deliver((batch, collector) -> output.prepare(batch, record -> {
                assertThatThrownBy(() -> output.publishCheckpoint(replay, true)).isInstanceOf(IllegalStateException.class);
                collector.accept(record);
            }))).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
            assertThat(records).isEmpty();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(1));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "heartbeat.interval.ms=-1", "skipped.operations=t", "event.processing.failure.handling.mode=warn",
            "event.converting.failure.handling.mode=warn" })
    void rejectsOutputDroppingConfigurations(String setting) {
        String[] parts = setting.split("=");
        assertThatThrownBy(() -> new Fixture(Map.of(parts[0], parts[1]), 20)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldCheckpointSkippedUnchangedUpdatesWithoutEmittingDataOrLosingLaterChanges() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Fixture(Map.of("skip.messages.without.change", "true", "heartbeat.interval.ms", "0", "provide.transaction.metadata", "false"), 20).output;
                var replay = replay(INITIAL.toOffset(), records::add)) {
            feed(replay, 2, "C/SC", 0);
            feed(replay, 3, "R/UB", 1);
            feed(replay, 4, "R/UP", 1);
            feed(replay, 5, "C/CM", 0);
            replay.deliver(output);
            assertThat(records).isEmpty();
            assertThat(output.publishCheckpoint(replay, true)).isTrue();
            assertThat(records).hasSize(1);
            assertThat(records.get(0).sourceOffset().get("completed_through.sequence")).isEqualTo("5");
            feed(replay, 6, "C/SC", 0);
            feed(replay, 7, "R/PT", 2);
            feed(replay, 8, "C/CM", 0);
            replay.deliver(output);
            assertThat(operation(records.get(1))).isEqualTo("c");
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(8));
        }
    }

    private static class Fixture extends As400ConnectorTask {
        private final FixtureSchema schema;
        private final As400BatchDispatcher output;

        private Fixture(Map<String, String> overrides, int capacity) {
            Configuration.Builder builder = Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                    .with("database.schema", "LIB").with("skipped.operations", "none").with("heartbeat.interval.ms", "3600000")
                    .with("provide.transaction.metadata", "true").with("event.converting.failure.handling.mode", "fail");
            overrides.forEach(builder::with);
            Configuration raw = builder.build();
            var context = preStart(raw);
            var config = (As400ConnectorConfig) context.getConfig();
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, context);
            schema = new FixtureSchema(new As400OffsetContext(config).getSourceInfoSchema());
            output = new As400BatchDispatcher(config, PARTITION, schema, config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true),
                    new DebeziumHeaderProducer(context), () -> 42_000L, capacity);
        }
    }

    private static class FixtureSchema implements DatabaseSchema<TableId> {
        private final TableSchema schema;
        private boolean available = true;
        private int failValue = -1;

        private FixtureSchema(Schema source) {
            Schema row = SchemaBuilder.struct().name("test.Value").field("ID", Schema.INT32_SCHEMA).optional().build();
            Schema key = SchemaBuilder.struct().name("test.Key").field("ID", Schema.INT32_SCHEMA).build();
            Envelope envelope = Envelope.defineSchema().withName("test.Envelope").withRecord(row).withSource(source)
                    .withTransaction(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA).build();
            schema = new TableSchema(TABLE, key, data -> new Struct(key).put("ID", data[0]), envelope, row, data -> {
                if (data[0].equals(failValue)) {
                    throw new IllegalArgumentException("Fixture value conversion failed");
                }
                return new Struct(row).put("ID", data[0]);
            });
        }

        @Override
        public TableSchema schemaFor(TableId id) {
            return available && TABLE.equals(id) ? schema : null;
        }

        @Override
        public boolean tableInformationComplete() {
            return true;
        }

        @Override
        public boolean isHistorized() {
            return false;
        }

        @Override
        public void close() {
        }
    }

    private As400ReplayCoordinator replay(Map<String, ?> loaded, As400ReplayCoordinator.RecordSink sink) {
        return replay(loaded, sink, ORDER, new Limits(10, 100, 20, 4096), 30);
    }

    private As400ReplayCoordinator replay(Map<String, ?> loaded, As400ReplayCoordinator.RecordSink sink,
                                          As400JournalOrder order, Limits limits, int outputRecords) {
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
        var table = new TableIdentity(TABLE, "TEST", "TEST", JID, As400ConnectProbeConnector.MEMBER_JID, "FORMAT_V1");
        return new As400ReplayCoordinator(loaded, JOURNAL, FINGERPRINT, order, limits, 1024, outputRecords,
                (file, library) -> new As400SourceNormalizer.TableBinding(table, decoder), (source, control) -> SavepointRollback.UNRESOLVED,
                PARTITION.getSourcePartition(), sink, null, false, spill());
    }

    protected As400SpillStore spill() {
        return null;
    }

    @Test
    void shouldDeliverLargeKeyChangeBatchWithinConfiguredBufferLimits() throws Exception {
        var limits = new As400DcgConfig(Configuration.from(As400DcgConfigTest.properties())).runtimeLimits;
        int changes = 6700;
        var order = new As400JournalOrder(List.of(RECEIVER), Map.of(RECEIVER, "receiver-instance"),
                Map.of(RECEIVER, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(2L * changes + 3))));
        var records = new ArrayList<SourceRecord>();
        try (var output = new Fixture(Map.of(), limits.outputRecords()).output;
                var replay = replay(INITIAL.toOffset(), records::add, order, limits.transactions(), limits.outputRecords())) {
            feed(replay, 2, "C/SC", 0);
            for (int i = 0; i < changes; i++) {
                feed(replay, 3L + 2L * i, "R/UB", i);
                feed(replay, 4L + 2L * i, "R/UP", i + 1);
            }
            feed(replay, 2L * changes + 3, "C/CM", 0);
            replay.deliver(output);
            assertThat(records).hasSize(3 * changes + 2);
            assertThat(records.stream().filter(r -> r.value() == null)).hasSize(changes);
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(2L * changes + 3));
        }
    }

    private static void feedBatch(As400ReplayCoordinator replay) {
        feed(replay, 2, "C/SC", 0);
        feed(replay, 3, "D/CG", 8);
        feed(replay, 4, "R/PT", 1);
        feed(replay, 5, "R/UB", 1);
        feed(replay, 6, "R/UP", 2);
        feed(replay, 7, "R/DL", 2);
        feed(replay, 8, "R/PT", 3);
        feed(replay, 9, "C/CM", 0);
    }

    private static void feed(As400ReplayCoordinator replay, long sequence, String operation, int value) {
        feed(replay, RECEIVER, sequence, operation, value);
    }

    private static void feed(As400ReplayCoordinator replay, JournalReceiver receiver, long sequence, String operation, int value) {
        byte[] payload;
        if (operation.startsWith("R/")) {
            payload = ByteBuffer.allocate(4).putInt(value).array();
        }
        else if (operation.equals("D/CG")) {
            payload = new byte[109];
            new AS400Text(10, 37).toBytes("TEST", payload, 0);
            new AS400Text(10, 37).toBytes("LIB", payload, 10);
            new AS400Text(10, 37).toBytes("", payload, 20);
            new AS400Text(1, 37).toBytes(Integer.toString(value), payload, 108);
        }
        else {
            payload = new byte[0];
        }
        byte[] data = new byte[256 + payload.length];
        new AS400Text(5, 37).toBytes(String.format("%05d", payload.length), data, 240);
        System.arraycopy(payload, 0, data, 256, payload.length);
        char flag = operation.equals("R/UB") || operation.equals("R/DL") ? '1' : '0';
        EntryHeader header = new EntryHeader(data.length, 0, payload.length == 0 ? 0 : 240, BigInteger.valueOf(sequence), BigInteger.ONE,
                Instant.ofEpochSecond(sequence), operation.charAt(0), operation.substring(2),
                "TEST      LIB       " + (operation.equals("D/CG") ? "          " : "TEST      "), BigInteger.TEN,
                data.length, 0, receiver.name(), receiver.library(), new EntryHeader.ControlInfo(flag, 0, false, false, (byte) 0),
                new EntryHeader.RecordInfo(BigInteger.ONE, BigInteger.ONE, "JOB", "USER", "123456",
                        operation.startsWith("R/") ? As400ConnectProbeConnector.MEMBER_JID : JID, '1', operation.startsWith("R/") ? "*QDDS" : "*FILE", "SYS", "PGM",
                        "LIB", "USER"));
        replay.accept(header, data, 0, new JournalPosition(BigInteger.valueOf(sequence), receiver));
    }

    private static JournalPosition pos(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), RECEIVER);
    }

    private static Struct value(SourceRecord record) {
        return (Struct) record.value();
    }

    private static String operation(SourceRecord record) {
        if (record.value() == null) {
            return "tombstone";
        }
        Struct value = value(record);
        return value.schema().field("op") != null ? value.getString("op") : value.schema().field("status") != null ? value.getString("status") : "HB";
    }
}
