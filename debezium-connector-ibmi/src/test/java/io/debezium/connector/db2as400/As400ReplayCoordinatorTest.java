/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400ReplayCoordinator.RecoveryException;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.relational.TableId;

class As400ReplayCoordinatorTest {
    private static As400JournalOrder order(List<JournalReceiver> receivers, Map<JournalReceiver, String> incarnations) {
        Map<JournalReceiver, As400JournalOrder.Range> ranges = new HashMap<>();
        receivers.forEach(receiver -> ranges.put(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE))));
        return new As400JournalOrder(receivers, incarnations, ranges);
    }

    private static As400Checkpoint checkpoint(String journal, String fingerprint, JournalPosition floor, boolean inclusive, JournalPosition completed) {
        return new As400Checkpoint(journal, fingerprint, floor, inclusive, completed,
                floor.receiver().equals(OLD) ? "old-instance" : "new-instance", completed.receiver().equals(OLD) ? "old-instance" : "new-instance");
    }

    private static final String IDENTITY = "LIB/JRN/incarnation1";
    private static final String FINGERPRINT = "v1:" + "a".repeat(64);
    private static final String JID = "0102030405060708090a";
    private static final String OTHER_FILE_JID = "1112030405060708090a";
    private static final String OTHER_MEMBER_JID = "1b1c1d1e1f2021222324";
    private static final JournalReceiver OLD = new JournalReceiver("ZOLD", "LIB");
    private static final JournalReceiver NEW = new JournalReceiver("ANEW", "LIB");
    private static final As400JournalOrder ORDER = order(List.of(OLD, NEW), Map.of(OLD, "old-instance", NEW, "new-instance"));
    private static final Map<String, String> PARTITION = Map.of("server", "test");
    private static final Limits LIMITS = new Limits(10, 100, 20, 4096);
    private static final As400Checkpoint INITIAL = As400Checkpoint.afterVerifiedBoundary(IDENTITY, FINGERPRINT, pos(1), "old-instance");
    private final QueueSink sink = new QueueSink();

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldKeepChunkProgressBehindEveryPartiallyDeliveredSnapshotBatch(boolean interrupted) throws Exception {
        var context = new io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext<TableId>(true);
        context.addDataCollectionNamesToSnapshot("snapshot-request", List.of("DB.LIB.TEST"), List.of(), "");
        context.maximumKey(new Object[]{ 900 });
        context.sendEvent(new Object[]{ 100 });
        var saved = INITIAL.withSnapshotState(As400IncrementalSnapshotState.capture(context));
        var replay = fresh(saved.toOffset());
        var factory = new As400ReplayCoordinator.RecordFactory() {
            @Override
            public void prepare(CommittedBatch batch, Consumer<SourceRecord> output) {
                throw new AssertionError("Expected independent snapshot signal preparation");
            }

            @Override
            public void prepareSignals(Consumer<SourceRecord> output) {
                for (int id : List.of(101, 102, 103)) {
                    context.sendEvent(new Object[]{ id });
                    output.accept(record("snapshot", id));
                }
            }

            @Override
            public Optional<Map<String, String>> snapshotState() {
                return Optional.of(As400IncrementalSnapshotState.capture(context));
            }
        };
        if (interrupted) {
            sink.failAt = 1;
            assertThrows(RecoveryException.class, () -> replay.deliverSignals(factory));
            assertThat(replay.checkpoint()).isEqualTo(saved);
        }
        else {
            replay.deliverSignals(factory);
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(saved.completedThrough());
            assertThat(As400IncrementalSnapshotState.restore(replay.checkpoint().snapshotState()).chunkEndPosititon()).containsExactly(103);
        }
        assertThat(sink.records).allSatisfy(record -> assertThat(record.sourceOffset()).isEqualTo(saved.toOffset()));
        sink.acknowledgeThrough(sink.records.size() - 1);
        var restart = fresh(durableRoundTrip(sink.durable));
        assertThat(As400IncrementalSnapshotState.restore(restart.checkpoint().snapshotState()).chunkEndPosititon()).containsExactly(100);
    }

    @Test
    void scanFloorAndCompletedBoundaryRemainSeparateAndSnapshotsDoNotMutate() throws Exception {
        As400ReplayCoordinator coordinator = fresh(INITIAL.toOffset());
        feed(coordinator, event(2, "C/SC", 10, 0));
        feed(coordinator, event(3, "D/CG", 10, 8));
        SourceRecord open = coordinator.publishCheckpoint(record("HEARTBEAT", 3));
        assertThat(coordinator.scanPosition()).contains(pos(3));
        assertThat(coordinator.checkpoint().replayFrom()).isEqualTo(pos(2));
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(1));
        feed(coordinator, event(4, "R/PT", 10, 7));
        feed(coordinator, event(5, "C/CM", 10, 0));
        assertThat(coordinator.scanPosition()).contains(pos(5));
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(1));
        coordinator.deliver(this::prepare);
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(5));
        assertThat(coordinator.checkpoint().replayInclusive()).isFalse();
        assertThat(coordinator.checkpoint().replayFrom()).isEqualTo(pos(5));
        assertThat(offsetFields(open.sourceOffset())).containsEntry("completed_through.sequence", "1").containsEntry("replay_from.sequence", "2");
        for (SourceRecord output : sink.records) {
            assertThat(offsetFields(output.sourceOffset())).containsEntry("completed_through.sequence", "1");
            assertThrows(UnsupportedOperationException.class, () -> output.sourceOffset().clear());
        }
        assertThat(offsetFields(coordinator.publishCheckpoint(record("HEARTBEAT", 5)).sourceOffset())).containsEntry("completed_through.sequence", "5");
    }

    static Stream<Arguments> interruptedOutputs() {
        return Stream.of("false", "exception", "interrupt").flatMap(mode -> IntStream.range(0, 10).mapToObj(cut -> Arguments.of(mode, cut)));
    }

    @ParameterizedTest
    @MethodSource("interruptedOutputs")
    void freshInstanceReplaysWholeBatchAfterEveryPartialDelivery(String mode, int cut) throws Exception {
        List<Event> events = multiRecordBatch();
        As400ReplayCoordinator first = fresh(INITIAL.toOffset());
        for (Event event : events) {
            feed(first, event);
        }
        sink.failAt = cut;
        sink.failure = mode;
        assertThrows(RecoveryException.class, () -> first.deliver(this::prepare));
        if (mode.equals("interrupt")) {
            assertThat(Thread.interrupted()).isTrue();
        }
        assertThat(first.checkpoint().completedThrough()).isEqualTo(pos(1));
        assertThat(first.checkpoint().replayFrom()).isEqualTo(pos(2));
        assertThrows(RecoveryException.class, () -> feed(first, event(10, "C/SC", 20, 0)));
        sink.acknowledgeThrough(sink.records.size() - 1);
        Map<String, ?> persisted = durableRoundTrip(sink.durable);
        first.close();
        sink.failAt = Integer.MAX_VALUE;
        int replayStart = sink.records.size();
        As400ReplayCoordinator restarted = fresh(persisted);
        replay(restarted, events);
        replay(restarted, List.of(event(10, "C/SC", 20, 0), event(11, "R/PT", 20, 4), event(12, "C/CM", 20, 0)));
        assertThat(sink.records.subList(replayStart, replayStart + 10)).extracting(SourceRecord::value)
                .containsExactly("BEGIN", "t", "c:1", "d:1", null, "c:2", "d:2", null, "c:3", "END");
        assertThat(sink.projection).containsExactlyInAnyOrder(3, 4);
        assertThat(restarted.checkpoint().completedThrough()).isEqualTo(pos(12));
        for (SourceRecord output : sink.records.subList(replayStart, replayStart + 10)) {
            assertThat(offsetFields(output.sourceOffset())).containsEntry("completed_through.sequence", "1");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "before-heartbeat", "heartbeat-enqueued", "heartbeat-durable" })
    void enqueueIsNotDurabilityAndRestartUsesOnlyAcknowledgedOffset(String crash) throws Exception {
        As400ReplayCoordinator first = fresh(INITIAL.toOffset());
        replay(first, multiRecordBatch());
        assertThat(first.checkpoint().completedThrough()).isEqualTo(pos(9));
        sink.acknowledgeThrough(sink.records.size() - 1);
        assertThat(offsetFields(sink.durable)).containsEntry("completed_through.sequence", "1");
        if (!crash.equals("before-heartbeat")) {
            first.publishCheckpoint(record("HEARTBEAT", 9));
        }
        if (crash.equals("heartbeat-durable")) {
            sink.acknowledgeThrough(sink.records.size() - 1);
        }
        Map<String, ?> durable = durableRoundTrip(sink.durable);
        int accepted = sink.records.size();
        first.close();
        As400ReplayCoordinator restarted = fresh(durable);
        replay(restarted, multiRecordBatch());
        assertThat(sink.records.size() - accepted).isEqualTo(crash.equals("heartbeat-durable") ? 0 : 10);
        assertThat(sink.projection).containsExactly(3);
    }

    @Test
    void completedInterleavedCycleIsSuppressedButEarlierRowsOfPendingCycleSurvive() throws Exception {
        As400Checkpoint saved = checkpoint(IDENTITY, FINGERPRINT, pos(100), true, pos(130));
        As400ReplayCoordinator restarted = fresh(durableRoundTrip(saved.toOffset()));
        feed(restarted, event(100, "C/SC", 10, 0));
        feed(restarted, event(110, "D/CG", 10, 8));
        feed(restarted, event(120, "C/SC", 20, 0));
        feed(restarted, event(OLD, 125, "R/PT", 20, 2, '0', "OTHER"));
        assertThat(restarted.checkpoint()).isEqualTo(saved);
        assertThat(feed(restarted, event(130, "C/CM", 20, 0))).isEmpty();
        assertThat(sink.records).isEmpty();
        assertThat(restarted.replaying()).isFalse();
        assertThat(restarted.checkpoint().replayFrom()).isEqualTo(pos(100));
        replay(restarted, List.of(event(140, "R/PT", 10, 1), event(150, "C/CM", 10, 0)));
        assertThat(sink.records).extracting(SourceRecord::value).containsExactly("BEGIN", "t", "c:1", "END");
        assertThat(sink.records.get(1).timestamp()).isEqualTo(110_000L);
        assertThat(offsetFields(sink.records.get(1).sourceOffset())).containsEntry("completed_through.sequence", "130");
    }

    @Test
    void generatedInterleavedCheckpointSurvivesPartialOutputAndFreshRestart() throws Exception {
        List<Event> prefix = List.of(event(2, "C/SC", 10, 0), event(3, "D/CG", 10, 8), event(4, "C/SC", 20, 0),
                event(OLD, 5, "R/PT", 20, 2, '0', "OTHER"), event(6, "C/CM", 20, 0));
        List<Event> suffix = List.of(event(7, "R/PT", 10, 1), event(8, "C/CM", 10, 0));
        As400ReplayCoordinator first = fresh(INITIAL.toOffset());
        replay(first, prefix);
        first.publishCheckpoint(record("HEARTBEAT", 6));
        sink.acknowledgeThrough(sink.records.size() - 1);
        assertThat(offsetFields(sink.durable)).containsEntry("replay_from.sequence", "2").containsEntry("completed_through.sequence", "6");
        for (Event event : suffix) {
            feed(first, event);
        }
        sink.failAt = sink.records.size() + 2; // BEGIN and truncate escape, reload does not.
        assertThrows(RecoveryException.class, () -> first.deliver(this::prepare));
        sink.acknowledgeThrough(sink.records.size() - 1);
        Map<String, ?> saved = durableRoundTrip(sink.durable);
        first.close();
        sink.failAt = Integer.MAX_VALUE;
        int beforeReplay = sink.records.size();
        As400ReplayCoordinator restarted = fresh(saved);
        replay(restarted, prefix);
        assertThat(sink.records).hasSize(beforeReplay); // B was already durable.
        replay(restarted, suffix);
        assertThat(sink.projection).containsExactly(1);
        assertThat(sink.records.subList(beforeReplay, sink.records.size())).extracting(SourceRecord::value)
                .containsExactly("BEGIN", "t", "c:1", "END");
    }

    @Test
    void resolvedOrphanDoesNotSuppressALaterReuseOfItsCycleIdentifier() throws Exception {
        As400ReplayCoordinator restarted = fresh(checkpoint(IDENTITY, FINGERPRINT, pos(100), true, pos(150)).toOffset());
        replay(restarted, List.of(event(100, "C/SC", 10, 0), event(110, "R/UP", 20, 1), event(120, "C/CM", 20, 0),
                event(125, "C/SC", 20, 0), event(130, "R/PT", 20, 2), event(150, "C/CM", 10, 0)));
        assertThat(restarted.checkpoint().replayFrom()).isEqualTo(pos(125));
        replay(restarted, List.of(event(160, "C/CM", 20, 0)));
        assertThat(sink.records).extracting(SourceRecord::value).containsExactly("BEGIN", "c:2", "END");
    }

    @Test
    void verifiedTailMustContainRequiredFloorAndFenceButMayHaveAnOpenCycle() throws Exception {
        As400Checkpoint saved = checkpoint(IDENTITY, FINGERPRINT, pos(100), true, pos(130));
        As400ReplayCoordinator missingFloor = fresh(saved.toOffset());
        assertThrows(RecoveryException.class, missingFloor::verifyEndOfAvailableJournal);
        As400ReplayCoordinator missingFence = fresh(saved.toOffset());
        feed(missingFence, event(100, "C/SC", 10, 0));
        assertThrows(RecoveryException.class, missingFence::verifyEndOfAvailableJournal);
        assertThat(missingFence.checkpoint()).isEqualTo(saved);
        As400ReplayCoordinator live = fresh(INITIAL.toOffset());
        live.verifyEndOfAvailableJournal();
        feed(live, event(2, "C/SC", 10, 0));
        live.verifyEndOfAvailableJournal();
        assertThat(live.checkpoint().completedThrough()).isEqualTo(pos(1));
    }

    @ParameterizedTest
    @ValueSource(strings = { "C/CM", "C/RB" })
    void orphanSuffixDoesNotRequirePreFloorImagesOrSavepoints(String terminal) throws Exception {
        As400Checkpoint saved = checkpoint(IDENTITY, FINGERPRINT, pos(100), true, pos(130));
        As400ReplayCoordinator restarted = fresh(saved.toOffset());
        feed(restarted, event(100, "C/SC", 10, 0));
        feed(restarted, event(110, "D/CG", 10, 3));
        // Cycle 20's SC, UB and SB were before F. These records must never reach row/savepoint normalization.
        feed(restarted, event(120, "R/UP", 20, 2));
        feed(restarted, event(121, "C/SU", 20, 81));
        feed(restarted, event(122, "C/SQ", 20, 82));
        assertThat(restarted.checkpoint()).isEqualTo(saved);
        feed(restarted, event(130, terminal, 20, 0));
        replay(restarted, List.of(event(140, "R/PT", 10, 7), event(150, "C/CM", 10, 0)));
        assertThat(sink.projection).containsExactly(7);
        assertThat(sink.records).extracting(SourceRecord::value).containsExactly("BEGIN", "t", "c:7", "END");
    }

    @Test
    void orphanMustResolveByFenceAndPostFenceCyclesRequireStart() throws Exception {
        As400Checkpoint saved = checkpoint(IDENTITY, FINGERPRINT, pos(100), true, pos(130));
        As400ReplayCoordinator restarted = fresh(saved.toOffset());
        feed(restarted, event(100, "C/SC", 10, 0));
        feed(restarted, event(120, "R/UP", 20, 1));
        assertThrows(RecoveryException.class, () -> feed(restarted, event(130, "C/CM", 10, 0)));
        assertThat(restarted.checkpoint()).isEqualTo(saved);
        assertThat(sink.records).isEmpty();
        As400ReplayCoordinator clean = fresh(As400Checkpoint.afterVerifiedBoundary(IDENTITY, FINGERPRINT, pos(130), "old-instance").toOffset());
        assertThrows(RecoveryException.class, () -> feed(clean, event(140, "R/PT", 20, 2)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "floor", "fence", "order", "receiver", "unsupported-orphan", "unsafe-orphan-commit" })
    void replayIntegrityFailuresStopBeforePublishing(String failure) throws Exception {
        As400Checkpoint saved = checkpoint(IDENTITY, FINGERPRINT, pos(100), true, pos(130));
        As400ReplayCoordinator restarted = fresh(saved.toOffset());
        if (failure.equals("floor")) {
            assertThrows(RecoveryException.class, () -> feed(restarted, event(101, "C/SC", 10, 0)));
        }
        else {
            feed(restarted, event(100, "C/SC", 10, 0));
            Event bad = switch (failure) {
                case "fence" -> event(140, "C/CM", 10, 0);
                case "order" -> event(100, "R/PT", 10, 1);
                case "receiver" -> event(new JournalReceiver("UNKNOWN", "LIB"), 120, "R/PT", 10, 1, '0');
                case "unsupported-orphan" -> event(120, "R/ZZ", 20, 1);
                default -> event(OLD, 130, "C/CM", 20, 0, '4');
            };
            assertThrows(RecoveryException.class, () -> feed(restarted, bad));
        }
        assertThat(sink.records).isEmpty();
        assertThat(restarted.checkpoint()).isEqualTo(saved);
        assertThrows(RecoveryException.class, () -> restarted.publishCheckpoint(record("HEARTBEAT", 130)));
    }

    @Test
    void rollbackAndEmptyOrFilteredCommitsAdvanceWWithoutPublishingTransactionMetadata() throws Exception {
        As400ReplayCoordinator coordinator = fresh(INITIAL.toOffset());
        feed(coordinator, event(2, "C/SC", 10, 0));
        feed(coordinator, event(3, "C/SC", 20, 0));
        feed(coordinator, event(4, "D/CG", 20, 8));
        feed(coordinator, event(5, "C/RB", 20, 0));
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(5));
        assertThat(coordinator.checkpoint().replayFrom()).isEqualTo(pos(2));
        assertThat(feed(coordinator, event(6, "C/CM", 10, 0))).isEmpty();
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(6));
        assertThat(coordinator.checkpoint().replayInclusive()).isFalse();
        feed(coordinator, event(7, "C/SC", 30, 0));
        feed(coordinator, event(8, "R/PT", 30, 1));
        feed(coordinator, event(9, "C/CM", 30, 0));
        coordinator.deliver((batch, records) -> {
        });
        assertThat(sink.records).isEmpty();
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(9));
        coordinator.publishCheckpoint(record("HEARTBEAT", 9));
        sink.acknowledgeThrough(0);
        assertThat(offsetFields(sink.durable)).containsEntry("completed_through.sequence", "9");
    }

    @Test
    void entireBatchIsPreparedBeforeAnyRecordCanEscapeAndOutputIsBounded() throws Exception {
        As400ReplayCoordinator coordinator = fresh(INITIAL.toOffset());
        for (Event event : multiRecordBatch()) {
            feed(coordinator, event);
        }
        assertThrows(RecoveryException.class, () -> coordinator.deliver((batch, records) -> {
            records.accept(record("t", 3));
            throw new IllegalArgumentException("Schema/serialization preflight failed");
        }));
        assertThat(sink.records).isEmpty();
        As400ReplayCoordinator limited = fresh(INITIAL.toOffset(), LIMITS, 1);
        for (Event event : multiRecordBatch()) {
            feed(limited, event);
        }
        assertThrows(RecoveryException.class, () -> limited.deliver(this::prepare));
        assertThat(sink.records).isEmpty();
        assertThat(limited.checkpoint().completedThrough()).isEqualTo(pos(1));
    }

    @Test
    void outputPartitionMismatchCannotAdvanceCheckpoint() throws Exception {
        As400ReplayCoordinator coordinator = fresh(INITIAL.toOffset());
        for (Event event : multiRecordBatch()) {
            feed(coordinator, event);
        }
        assertThrows(RecoveryException.class, () -> coordinator.deliver((batch, records) -> records.accept(
                new SourceRecord(Map.of("server", "wrong"), Map.of(), "topic", Schema.STRING_SCHEMA, "t"))));
        assertThat(sink.records).isEmpty();
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(1));
    }

    @Test
    void swallowedPreparationErrorStillPreventsPartialPublication() throws Exception {
        As400ReplayCoordinator coordinator = fresh(INITIAL.toOffset(), LIMITS, 1);
        for (Event event : multiRecordBatch()) {
            feed(coordinator, event);
        }
        assertThrows(RecoveryException.class, () -> coordinator.deliver((batch, records) -> {
            records.accept(record("t", 3));
            try {
                records.accept(record("c:1", 4));
            }
            catch (RecoveryException ignored) {
                // Simulate an incorrect adapter that catches and skips a preparation error.
            }
        }));
        assertThat(sink.records).isEmpty();
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(1));
    }

    @Test
    void failedCheckpointPublicationDoesNotMakeEnqueuedDataDurable() throws Exception {
        As400ReplayCoordinator first = fresh(INITIAL.toOffset());
        replay(first, multiRecordBatch());
        sink.acknowledgeThrough(sink.records.size() - 1);
        sink.failAt = sink.records.size();
        assertThrows(RecoveryException.class, () -> first.publishCheckpoint(record("HEARTBEAT", 9)));
        assertThat(first.checkpoint().completedThrough()).isEqualTo(pos(9)); // enqueue progress only
        assertThat(offsetFields(sink.durable)).containsEntry("completed_through.sequence", "1");
        sink.failAt = Integer.MAX_VALUE;
        int beforeReplay = sink.records.size();
        As400ReplayCoordinator restarted = fresh(durableRoundTrip(sink.durable));
        replay(restarted, multiRecordBatch());
        assertThat(sink.records.size() - beforeReplay).isEqualTo(10);
        assertThat(sink.projection).containsExactly(3);
    }

    @Test
    void internalHeartbeatsAndKeyChangeSubrecordsAllUseFrozenPreviousW() throws Exception {
        As400ReplayCoordinator coordinator = fresh(INITIAL.toOffset());
        for (Event event : multiRecordBatch()) {
            feed(coordinator, event);
        }
        coordinator.deliver((batch, records) -> {
            prepare(batch, records);
            records.accept(record("INTERNAL_HEARTBEAT", 9));
        });
        assertThat(sink.records).hasSize(11);
        assertThat(sink.records).extracting(SourceRecord::sourceOffset).allSatisfy(offset -> {
            assertThat(offsetFields(offset)).containsEntry("completed_through.sequence", "1").containsEntry("replay_from.sequence", "2");
        });
        assertThat(sink.records.get(4).value()).isNull();
    }

    @Test
    void pendingBatchCannotBeOvertakenByScanAndClosedStateCannotBeReused() throws Exception {
        As400ReplayCoordinator coordinator = fresh(INITIAL.toOffset());
        for (Event event : multiRecordBatch()) {
            feed(coordinator, event);
        }
        assertThrows(RecoveryException.class, () -> feed(coordinator, event(10, "C/SC", 20, 0)));
        assertThrows(RecoveryException.class, () -> coordinator.deliver(this::prepare));
        assertThat(sink.records).isEmpty();
        assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(pos(1));
        coordinator.close();
        assertThrows(RecoveryException.class, () -> coordinator.publishCheckpoint(record("HEARTBEAT", 9)));
    }

    @Test
    void receiverRolloverUsesVerifiedChainWithSequenceResetAndRetainsOriginalEventPosition() throws Exception {
        As400Checkpoint saved = checkpoint(IDENTITY, FINGERPRINT, pos(900), true, new JournalPosition(BigInteger.TWO, NEW));
        As400ReplayCoordinator restarted = fresh(saved.toOffset());
        feed(restarted, event(900, "C/SC", 10, 0));
        feed(restarted, event(950, "R/PT", 10, 1));
        feed(restarted, event(NEW, 1, "C/SC", 20, 0, '0'));
        feed(restarted, event(NEW, 2, "C/CM", 20, 0, '0'));
        CommittedBatch batch = feed(restarted, event(NEW, 3, "C/CM", 10, 0, '0')).orElseThrow();
        assertThat(batch.changes().get(0).source().position()).isEqualTo(pos(950));
        restarted.deliver(this::prepare);
        assertThat(restarted.checkpoint().completedThrough()).isEqualTo(new JournalPosition(BigInteger.valueOf(3), NEW));
    }

    @Test
    void orphanAndKnownCyclesShareOneBoundedCapacity() throws Exception {
        As400Checkpoint saved = checkpoint(IDENTITY, FINGERPRINT, pos(100), true, pos(150));
        As400ReplayCoordinator restarted = fresh(saved.toOffset(), new Limits(2, 100, 20, 4096), 100);
        feed(restarted, event(100, "C/SC", 10, 0));
        feed(restarted, event(110, "R/UP", 20, 1));
        assertThrows(RecoveryException.class, () -> feed(restarted, event(120, "R/UP", 30, 1)));
        assertThat(restarted.scanPosition()).contains(pos(110));
        assertThat(restarted.checkpoint()).isEqualTo(saved);
    }

    private As400ReplayCoordinator fresh(Map<String, ?> offset) {
        return fresh(offset, LIMITS, 100);
    }

    private As400ReplayCoordinator fresh(Map<String, ?> offset, Limits limits, int outputLimit) {
        // A fresh decoder, normalizer, buffer and coordinator on every restart; only durable JSON survives.
        CompleteRowEntryDecoder decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
        TableIdentity table = new TableIdentity(new TableId("DB", "LIB", "TEST"), "TEST", "TEST", JID, As400ConnectProbeConnector.MEMBER_JID, "FORMAT_V1");
        TableIdentity other = new TableIdentity(new TableId("DB", "LIB", "OTHER"), "OTHER", "OTHER", OTHER_FILE_JID, OTHER_MEMBER_JID, "FORMAT_V1");
        return new As400ReplayCoordinator(offset, IDENTITY, FINGERPRINT, ORDER, limits, 1024, outputLimit,
                (file, library) -> new As400SourceNormalizer.TableBinding(file.equals("OTHER") ? other : table, decoder),
                (source, control) -> SavepointRollback.UNRESOLVED,
                PARTITION, sink::enqueue, null, false, spill());
    }

    protected As400SpillStore spill() {
        return null;
    }

    private Optional<CommittedBatch> feed(As400ReplayCoordinator coordinator, Event event) {
        return coordinator.accept(event.header(), event.data(), 0, event.position());
    }

    private void replay(As400ReplayCoordinator coordinator, List<Event> events) {
        As400Checkpoint resume = coordinator.checkpoint();
        for (Event event : events) {
            int comparison = ORDER.compare(event.position(), resume.replayFrom());
            if (comparison < 0 || (comparison == 0 && !resume.replayInclusive())) {
                continue;
            }
            if (feed(coordinator, event).isPresent()) {
                coordinator.deliver(this::prepare);
            }
        }
    }

    private void prepare(CommittedBatch batch, Consumer<SourceRecord> records) {
        records.accept(record("BEGIN", batch.startSource().position().offset().longValue()));
        batch.changes().forEach(change -> {
            long timestamp = change.source().timestamp().getEpochSecond();
            switch (change.operation()) {
                case TRUNCATE -> records.accept(record("t", timestamp));
                case CREATE -> records.accept(record("c:" + change.after().values()[0], timestamp));
                case DELETE -> {
                    records.accept(record("d:" + change.before().values()[0], timestamp));
                    records.accept(record(null, timestamp));
                }
                case UPDATE -> {
                    records.accept(record("d:" + change.before().values()[0], timestamp));
                    records.accept(record(null, timestamp));
                    records.accept(record("c:" + change.after().values()[0], timestamp));
                }
                default -> throw new IllegalArgumentException("Unexpected test operation");
            }
        });
        records.accept(record("END", batch.commitPosition().offset().longValue()));
    }

    private static SourceRecord record(String value, long timestamp) {
        // Deliberately unsafe supplied offset: coordinator must replace it for EVERY output record.
        return new SourceRecord(PARTITION, Map.of("scan", "9999"), "topic", 0, null, null, Schema.OPTIONAL_STRING_SCHEMA, value, timestamp * 1000);
    }

    private static Map<String, Object> offsetFields(Map<String, ?> offset) {
        return new HashMap<>(offset);
    }

    private static Map<String, ?> durableRoundTrip(Map<String, ?> offset) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsBytes(offset), new TypeReference<Map<String, Object>>() {
        });
    }

    private static List<Event> multiRecordBatch() {
        return List.of(event(2, "C/SC", 10, 0), event(3, "D/CG", 10, 8), event(4, "R/PT", 10, 1),
                event(5, "R/UB", 10, 1), event(6, "R/UP", 10, 2), event(7, "R/DL", 10, 2), event(8, "R/PT", 10, 3), event(9, "C/CM", 10, 0));
    }

    private static JournalPosition pos(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), OLD);
    }

    private static Event event(long sequence, String operation, long cycle, int value) {
        return event(OLD, sequence, operation, cycle, value, operation.equals("R/UB") || operation.equals("R/DL") ? '1' : '0');
    }

    private static Event event(JournalReceiver receiver, long sequence, String operation, long cycle, int value, char flag) {
        return event(receiver, sequence, operation, cycle, value, flag, "TEST");
    }

    private static Event event(JournalReceiver receiver, long sequence, String operation, long cycle, int value, char flag, String file) {
        byte[] payload;
        if (operation.startsWith("R/")) {
            payload = ByteBuffer.allocate(4).putInt(value).array();
        }
        else if (operation.equals("D/CG")) {
            payload = new byte[109];
            new AS400Text(10, 37).toBytes(file, payload, 0);
            new AS400Text(10, 37).toBytes("LIB", payload, 10);
            new AS400Text(10, 37).toBytes("", payload, 20);
            new AS400Text(1, 37).toBytes(Integer.toString(value), payload, 108);
        }
        else if (operation.equals("C/SU") || operation.equals("C/SQ")) {
            payload = new AS400Text(20, 37).toBytes(String.format("%020d", value));
        }
        else {
            payload = new byte[0];
        }
        byte[] data = new byte[256 + payload.length];
        new AS400Text(5, 37).toBytes(String.format("%05d", payload.length), data, 240);
        System.arraycopy(payload, 0, data, 256, payload.length);
        EntryHeader header = new EntryHeader(data.length, 0, payload.length == 0 ? 0 : 240, BigInteger.valueOf(sequence), BigInteger.ONE,
                Instant.ofEpochSecond(sequence), operation.charAt(0), operation.substring(2),
                String.format("%-10s%-10s%-10s", file, "LIB", operation.equals("D/CG") ? "" : file), BigInteger.valueOf(cycle),
                data.length, 0, receiver.name(), receiver.library(), new EntryHeader.ControlInfo(flag, 0, false, false, (byte) 0),
                new EntryHeader.RecordInfo(BigInteger.ONE, BigInteger.ONE, "JOB", "USER", "123456",
                        operation.startsWith("R/") ? (file.equals("OTHER") ? OTHER_MEMBER_JID : As400ConnectProbeConnector.MEMBER_JID)
                                : (file.equals("OTHER") ? OTHER_FILE_JID : JID),
                        '1', operation.startsWith("R/") ? "*QDDS" : "*FILE", "SYS", "PGM",
                        "LIB", "USER"));
        return new Event(header, data, new JournalPosition(BigInteger.valueOf(sequence), receiver));
    }

    private record Event(EntryHeader header, byte[] data, JournalPosition position) {
    }

    /** Models a source-partition queue with prefix acknowledgements, not a Kafka integration test. */
    private static class QueueSink {
        private final List<SourceRecord> records = new ArrayList<>();
        private final Set<Integer> projection = new HashSet<>(Set.of(99));
        private Map<String, ?> durable = INITIAL.toOffset();
        private int acknowledged = -1;
        private int failAt = Integer.MAX_VALUE;
        private String failure = "false";

        boolean enqueue(SourceRecord record) throws Exception {
            if (records.size() == failAt) {
                if (failure.equals("exception")) {
                    throw new IllegalStateException("Injected queue failure");
                }
                if (failure.equals("interrupt")) {
                    throw new InterruptedException("Injected interrupt");
                }
                return false;
            }
            records.add(record);
            if (record.value() instanceof String value) {
                if (value.equals("t")) {
                    projection.clear();
                }
                else if (value.startsWith("c:")) {
                    projection.add(Integer.valueOf(value.substring(2)));
                }
                else if (value.startsWith("d:")) {
                    projection.remove(Integer.valueOf(value.substring(2)));
                }
            }
            return true;
        }

        void acknowledgeThrough(int index) {
            if (index < acknowledged || index >= records.size()) {
                throw new IllegalArgumentException("Invalid acknowledgement prefix");
            }
            while (acknowledged < index) {
                durable = records.get(++acknowledged).sourceOffset();
            }
        }
    }
}
