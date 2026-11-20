/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400CheckpointTest {
    @Test
    void shouldRestoreStandardIncrementalSnapshotProgressWithoutChangingJournalCoordinates() throws Exception {
        var context = new io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext<io.debezium.relational.TableId>(true);
        context.addDataCollectionNamesToSnapshot(null, List.of("DB.LIB.ORDERS", "DB.LIB.ITEMS"), List.of(), "");
        context.maximumKey(new Object[]{ 900 });
        context.sendEvent(new Object[]{ 256 });
        var saved = initial().withSnapshotState(As400IncrementalSnapshotState.capture(context));
        var json = new ObjectMapper();
        Map<String, Object> restoredOffset = json.readValue(json.writeValueAsBytes(saved.toOffset()), new TypeReference<>() {
        });
        var restored = As400Checkpoint.fromOffset(restoredOffset, IDENTITY, FINGERPRINT);
        assertThat(restored).isEqualTo(saved);
        assertThat(restored.replayFrom()).isEqualTo(initial().replayFrom());
        assertThat(restored.completedThrough()).isEqualTo(initial().completedThrough());
        var chunk = As400IncrementalSnapshotState.restore(restored.snapshotState());
        assertThat(chunk.currentDataCollectionId().getId()).isEqualTo(io.debezium.relational.TableId.parse("DB.LIB.ORDERS"));
        assertThat(chunk.chunkEndPosititon()).containsExactly(256);
        assertThat(chunk.maximumKey().orElseThrow()).containsExactly(900);
        assertThat(chunk.dataCollectionsToBeSnapshottedCount()).isEqualTo(2);
        assertThat(restored.withSnapshotState(Map.of()).toOffset()).isEqualTo(initial().toOffset());
        assertThrows(IllegalArgumentException.class, () -> legacyLoader().load(restoredOffset));
        restoredOffset.remove(As400IncrementalSnapshotState.COLLECTIONS);
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(restoredOffset, IDENTITY, FINGERPRINT));
    }

    private static As400JournalOrder order(List<JournalReceiver> receivers, Map<JournalReceiver, String> incarnations) {
        Map<JournalReceiver, As400JournalOrder.Range> ranges = new HashMap<>();
        receivers.forEach(receiver -> ranges.put(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE))));
        return new As400JournalOrder(receivers, incarnations, ranges);
    }

    @Test
    void shouldRetainSchemaDependenciesAlongsideIncrementalSnapshotProgress() throws Exception {
        var context = new io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext<io.debezium.relational.TableId>(true);
        context.addDataCollectionNamesToSnapshot(null, List.of("DB.LIB.ORDERS"), List.of(), "");
        var saved = initial().withSnapshotState(As400IncrementalSnapshotState.capture(context))
                .withSchemaState(new As400SchemaState(As400SchemaState.ORIGIN, new As400SchemaState.Cursor(1, "a".repeat(64))));
        Map<String, Object> offset = new ObjectMapper().readValue(new ObjectMapper().writeValueAsBytes(saved.toOffset()), new TypeReference<>() {
        });
        assertThat(offset).containsEntry(As400Checkpoint.VERSION, "4");
        assertThat(As400Checkpoint.fromOffset(offset, IDENTITY, FINGERPRINT)).isEqualTo(saved);
        assertThat(saved.withSnapshotState(Map.of()).schemaState()).isEqualTo(saved.schemaState());
        for (String field : saved.schemaState().fields().keySet()) {
            var missing = new HashMap<>(offset);
            missing.remove(field);
            assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(missing, IDENTITY, FINGERPRINT));
        }
        offset.put("schema.floor.ordinal", "2");
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(offset, IDENTITY, FINGERPRINT));
    }

    private static As400Checkpoint checkpoint(String journal, String fingerprint, JournalPosition floor, boolean inclusive, JournalPosition completed) {
        return new As400Checkpoint(journal, fingerprint, floor, inclusive, completed,
                floor.receiver().equals(OLD) ? "old-instance" : "new-instance", completed.receiver().equals(OLD) ? "old-instance" : "new-instance");
    }

    private static final String IDENTITY = "LIB/JRN/incarnation1";
    private static final String FINGERPRINT = "v1:" + "a".repeat(64);
    private static final JournalReceiver OLD = new JournalReceiver("ZOLD", "LIB");
    private static final JournalReceiver NEW = new JournalReceiver("ANEW", "LIB");

    @Test
    void immutableOffsetSurvivesJsonRoundTripWithUnsignedSequencesAndExplicitInclusivity() throws Exception {
        As400Checkpoint checkpoint = checkpoint(IDENTITY, FINGERPRINT,
                new JournalPosition(new BigInteger("18446744073709551615"), OLD), true, new JournalPosition(BigInteger.TWO, NEW));
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> decoded = mapper.readValue(mapper.writeValueAsBytes(checkpoint.toOffset()), new TypeReference<>() {
        });
        assertThat(As400Checkpoint.fromOffset(decoded, IDENTITY, FINGERPRINT)).isEqualTo(checkpoint);
        assertThrows(UnsupportedOperationException.class, () -> checkpoint.toOffset().put("completed_through.sequence", "100"));
        assertThat(checkpoint.toOffset()).doesNotContainKeys("scan", As400OffsetContext.EVENT_SEQUENCE, As400OffsetContext.PROCESSED);
        assertThat(order(List.of(OLD, NEW), Map.of(OLD, "old-instance", NEW, "new-instance")).compare(checkpoint.replayFrom(),
                checkpoint.completedThrough())).isNegative();
    }

    @ParameterizedTest
    @ValueSource(strings = { "offset.version", "journal_identity", "capture_fingerprint", "replay_from.sequence", "replay_from.receiver",
            "replay_from.receiver_library", "replay_from.receiver_identity", "completed_through.receiver_identity", "replay_from.inclusive", "completed_through.sequence",
            "completed_through.receiver", "completed_through.receiver_library" })
    void rejectsMissingFieldsRatherThanGuessingRecoveryPosition(String key) {
        Map<String, Object> offset = new HashMap<>(initial().toOffset());
        offset.remove(key);
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(offset, IDENTITY, FINGERPRINT));
    }

    @ParameterizedTest
    @ValueSource(strings = { "1", "3", "4", "", "2.0" })
    void shouldRejectUnsupportedVersionsAndSnapshotVersionWithoutChunkState(String version) {
        Map<String, Object> offset = new HashMap<>(initial().toOffset());
        offset.put(As400Checkpoint.VERSION, version);
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(offset, IDENTITY, FINGERPRINT));
        assertThrows(IllegalArgumentException.class, () -> legacyLoader().load(offset));
    }

    @Test
    void legacyLoaderRefusesV2AndV2LoaderRefusesLegacyOrMixedOffsets() {
        assertThrows(IllegalArgumentException.class, () -> legacyLoader().load(initial().toOffset()));
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(Map.of(As400OffsetContext.EVENT_SEQUENCE, "1"), IDENTITY, FINGERPRINT));
        Map<String, Object> mixed = new HashMap<>(initial().toOffset());
        mixed.put(As400OffsetContext.EVENT_SEQUENCE, "9999");
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(mixed, IDENTITY, FINGERPRINT));
    }

    @ParameterizedTest
    @ValueSource(strings = { "0", "-1", "18446744073709551616", "01", "+1", " 1", "1.0", "" })
    void rejectsMalformedOrOverflowingSequences(String sequence) {
        Map<String, Object> offset = new HashMap<>(initial().toOffset());
        offset.put("replay_from.sequence", sequence);
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(offset, IDENTITY, FINGERPRINT));
    }

    @Test
    void rejectsIdentityFingerprintAndInclusivityChanges() {
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(initial().toOffset(), "another-journal", FINGERPRINT));
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(initial().toOffset(), IDENTITY, "v1:" + "b".repeat(64)));
        Map<String, Object> offset = new HashMap<>(initial().toOffset());
        offset.put("replay_from.inclusive", "FALSE");
        assertThrows(IllegalArgumentException.class, () -> As400Checkpoint.fromOffset(offset, IDENTITY, FINGERPRINT));
        assertThrows(IllegalArgumentException.class, () -> checkpoint(IDENTITY, FINGERPRINT,
                new JournalPosition(BigInteger.TWO, OLD), false, new JournalPosition(BigInteger.ONE, OLD)));
    }

    @Test
    void floorMayFollowPreviousCompletedBoundaryWhileABatchIsInFlight() {
        As400Checkpoint checkpoint = checkpoint(IDENTITY, FINGERPRINT, new JournalPosition(BigInteger.TEN, OLD), true,
                new JournalPosition(BigInteger.ONE, OLD));
        assertThat(As400Checkpoint.fromOffset(checkpoint.toOffset(), IDENTITY, FINGERPRINT)).isEqualTo(checkpoint);
    }

    @Test
    void receiverOrderRequiresKnownDistinctReceiversAndDoesNotSortNames() {
        As400JournalOrder order = order(List.of(OLD, NEW), Map.of(OLD, "old-instance", NEW, "new-instance"));
        assertThat(order.compare(new JournalPosition(BigInteger.valueOf(999), OLD), new JournalPosition(BigInteger.ONE, NEW))).isNegative();
        assertThrows(IllegalArgumentException.class, () -> order(List.of(OLD, OLD), Map.of(OLD, "old-instance")));
        assertThrows(IllegalArgumentException.class, () -> order.compare(new JournalPosition(BigInteger.ONE, new JournalReceiver("MISSING", "LIB")),
                new JournalPosition(BigInteger.ONE, OLD)));
    }

    private As400Checkpoint initial() {
        return As400Checkpoint.afterVerifiedBoundary(IDENTITY, FINGERPRINT, new JournalPosition(BigInteger.ONE, OLD), "old-instance");
    }

    private As400OffsetContext.Loader legacyLoader() {
        return new As400OffsetContext.Loader(new As400ConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "server").with(As400ConnectorConfig.DATABASE_NAME, "server").build()));
    }
}
