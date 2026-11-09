/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.connector.db2as400.As400ReplayCoordinator.RecoveryException;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;

/**
 * Characterization of unresolved native lifecycle evidence. Artificial checkpoint
 * anchors and component projections below are test inputs, never startup proof or
 * a production filtering/savepoint policy. Raw native headers are not rewritten.
 */
class As400NativeLifecycleRecoveryTest {
    private static final String JOURNAL = "FIXTURE_ONLY";
    private static final String RECEIVER = "FIXTURE_ONLY_RECEIVER";
    private static final String FINGERPRINT = "v1:" + "a".repeat(64);
    private static final Map<String, String> PARTITION = Map.of("server", "fixture");
    private static final Limits LIMITS = new Limits(16, 1000, 100, 65536);

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "D_commit", "T_rollback", "D_rollback", "T_savepoint", "D_savepoint", "interleaved" })
    void completeNativeStreamPreservesFloorAndCompletionOnFailureAndFreshRestart(String name) throws Exception {
        List<Entry> entries = entries(name);
        List<SourceRecord> output = new ArrayList<>();
        As400Checkpoint initial = initial(entries);
        As400Checkpoint saved;
        Entry unsupported = entries.stream().filter(e -> e.type().equals("F/CH")).findFirst().orElseThrow();
        Entry start = entries.stream().filter(e -> e.type().equals("C/SC")).findFirst().orElseThrow();
        try (var coordinator = coordinator(initial.toOffset(), output)) {
            for (Entry entry : entries) {
                if (entry == unsupported) {
                    break;
                }
                assertThat(feed(coordinator, entry)).isEmpty();
            }
            saved = coordinator.checkpoint();
            assertThat(saved.replayFrom()).isEqualTo(start.position());
            assertThat(saved.replayInclusive()).isTrue();
            assertThat(saved.completedThrough()).isEqualTo(initial.completedThrough());
            assertRejectedWithoutProgress(coordinator, unsupported, output);
        }
        // Round-trip only the offset representation; all buffer/normalizer state is new.
        try (var restarted = coordinator(roundTrip(saved.toOffset()), output)) {
            for (Entry entry : entries) {
                if (entry.position().offset().compareTo(saved.replayFrom().offset()) < 0) {
                    continue;
                }
                if (entry == unsupported) {
                    break;
                }
                assertThat(feed(restarted, entry)).isEmpty();
            }
            assertRejectedWithoutProgress(restarted, unsupported, output);
            assertThat(restarted.checkpoint()).isEqualTo(saved);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "D_commit", "T_rollback", "D_rollback", "T_savepoint", "D_savepoint", "interleaved" })
    void treatingOnlyLegacyFileChangeAsAdvisoryDoesNotResolveJournalingTransitions(String name) throws Exception {
        List<Entry> entries = entries(name);
        List<SourceRecord> output = new ArrayList<>();
        try (var coordinator = coordinator(initial(entries).toOffset(), output)) {
            for (Entry entry : entries) {
                if (entry.type().equals("F/CH")) {
                    // Deliberate counterfactual projection, not an accepted stream filter.
                    continue;
                }
                if (entry.type().equals("F/EJ")) {
                    assertThat(entry.header().getCommitCycle()).isZero();
                    assertThat(entry.header().getControlInfo().flags()).isEqualTo((byte) 0x10);
                    assertRejectedWithoutProgress(coordinator, entry, output);
                    return;
                }
                assertThat(feed(coordinator, entry)).isEmpty();
            }
            throw new AssertionError("Missing native end-journaling companion");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_rollback", "D_rollback" })
    void nativePostRollbackTailCannotBeBypassedByRestartingAfterCompletion(String name) throws Exception {
        List<Entry> entries = entries(name);
        List<SourceRecord> output = new ArrayList<>();
        int terminal = -1;
        As400Checkpoint saved;
        try (var coordinator = coordinator(initial(entries).toOffset(), output)) {
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                // Isolate the already-tested C/R/CG component to expose the later tail.
                if (component(entry)) {
                    assertThat(feed(coordinator, entry)).isEmpty();
                }
                if (entry.type().equals("C/RB")) {
                    terminal = i;
                    break;
                }
            }
            assertThat(terminal).isNotNegative();
            saved = coordinator.checkpoint();
            assertThat(saved.completedThrough()).isEqualTo(entries.get(terminal).position());
            assertThat(saved.replayFrom()).isEqualTo(saved.completedThrough());
            assertThat(saved.replayInclusive()).isFalse();
            Entry tail = entries.get(terminal + 1);
            assertThat(tail.type()).isEqualTo("F/EJ");
            assertThat(tail.header().getCommitCycle()).isZero();
            assertRejectedWithoutProgress(coordinator, tail, output);
        }
        try (var restarted = coordinator(roundTrip(saved.toOffset()), output)) {
            assertRejectedWithoutProgress(restarted, entries.get(terminal + 1), output);
            assertThat(restarted.checkpoint()).isEqualTo(saved);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_savepoint", "D_savepoint" })
    void matchingNativeFinalRowsDoNotDistinguishSavepointTargetLifetimes(String name) throws Exception {
        ComponentResult release = savepointComponent(name, SavepointRollback.RELEASE_TARGET);
        ComponentResult retain = savepointComponent(name, SavepointRollback.RETAIN_TARGET);
        String table = name.startsWith("T") ? "TS2" : "DS2";
        String marker = name.startsWith("T") ? "1200" : "2200";
        assertThat(release.operations()).containsExactly("c:MARK:" + marker, "c:" + table + ":303");
        assertThat(retain.operations()).isEqualTo(release.operations());
        // Both policies fit these captures, but leave different internal marker state.
        assertThat(release.remainingSavepoints()).hasSize(3).isNotEqualTo(retain.remainingSavepoints());
        for (int i = 0; i < release.remainingSavepoints().size(); i++) {
            assertThat(retain.remainingSavepoints().get(i)).isGreaterThan(release.remainingSavepoints().get(i));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_savepoint", "D_savepoint" })
    void nativeSavepointMemberDeletionHasNoIgnoreFlagOrCommitCycle(String name) throws Exception {
        List<Entry> deletions = entries(name).stream().filter(e -> e.type().equals("F/MD")).toList();
        assertThat(deletions).hasSize(2);
        for (Entry entry : deletions) {
            assertThat(entry.header().getControlInfo().flags()).isEqualTo((byte) 0);
            assertThat(entry.header().getCommitCycle()).isZero();
            assertThat(entry.header().getRecordInfo().objectType()).isEqualTo("*QDDS");
            // Isolate each native tail record: its ordinary flag value is not permission to ignore it.
            List<SourceRecord> output = new ArrayList<>();
            try (var coordinator = coordinator(initial(List.of(entry)).toOffset(), output)) {
                assertRejectedWithoutProgress(coordinator, entry, output);
            }
        }
    }

    private static ComponentResult savepointComponent(String name, SavepointRollback policy) throws Exception {
        Map<String, TableBinding> bindings = bindings();
        List<String> operations = new ArrayList<>();
        List<Integer> remaining = new ArrayList<>();
        try (var buffer = new As400TransactionBuffer(JOURNAL, LIMITS)) {
            var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024, (source, control) -> policy);
            for (Entry entry : entries(name)) {
                if (!component(entry)) {
                    continue;
                }
                var batch = normalizer.accept(entry.header(), entry.bytes(), 0, entry.position());
                if (entry.type().equals("C/SU")) {
                    remaining.add(buffer.usage().savepoints());
                }
                if (batch.isPresent()) {
                    for (var change : batch.orElseThrow().changes()) {
                        operations.add(change.operation().code() + ":" + change.table().physicalFile()
                                + (change.after() == null ? "" : ":" + change.after().values()[0]));
                    }
                    buffer.completeBatch(batch.orElseThrow());
                }
            }
            assertThat(buffer.usage().transactions()).isZero();
        }
        return new ComponentResult(operations, remaining);
    }

    private static void assertRejectedWithoutProgress(As400ReplayCoordinator coordinator, Entry entry, List<SourceRecord> output) {
        As400Checkpoint before = coordinator.checkpoint();
        Optional<JournalPosition> scan = coordinator.scanPosition();
        assertThrows(RecoveryException.class, () -> feed(coordinator, entry));
        assertThat(coordinator.checkpoint()).isEqualTo(before);
        assertThat(coordinator.scanPosition()).isEqualTo(scan);
        assertThat(output).isEmpty();
        assertThrows(RecoveryException.class, () -> coordinator.publishCheckpoint(
                new SourceRecord(PARTITION, Map.of(), "fixture-heartbeat", null, null, Schema.STRING_SCHEMA, "checkpoint")));
        assertThat(output).isEmpty();
        assertThat(coordinator.checkpoint()).isEqualTo(before);
    }

    private static As400ReplayCoordinator coordinator(Map<String, ?> offset, List<SourceRecord> output) throws Exception {
        As400Checkpoint checkpoint = As400Checkpoint.fromOffset(offset, JOURNAL, FINGERPRINT);
        JournalReceiver receiver = checkpoint.replayFrom().receiver();
        var order = new As400JournalOrder(List.of(receiver), Map.of(receiver, RECEIVER),
                Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(10000))));
        Map<String, TableBinding> bindings = bindings();
        return new As400ReplayCoordinator(offset, JOURNAL, FINGERPRINT, order, LIMITS, 1024, 1000,
                (file, library) -> bindings.get(file), (source, control) -> SavepointRollback.UNRESOLVED,
                PARTITION, output::add);
    }

    private static As400Checkpoint initial(List<Entry> entries) {
        Entry first = entries.get(0);
        JournalPosition boundary = new JournalPosition(first.position().offset().subtract(BigInteger.ONE), first.position().receiver());
        return As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT, boundary, RECEIVER);
    }

    private static Optional<CommittedBatch> feed(As400ReplayCoordinator coordinator, Entry entry) {
        return coordinator.accept(entry.header(), entry.bytes(), 0, entry.position());
    }

    private static boolean component(Entry entry) {
        return entry.header().getJournalCode() == 'C' || entry.header().getJournalCode() == 'R' || entry.type().equals("D/CG");
    }

    private static Map<String, String> roundTrip(Map<String, String> offset) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsBytes(offset), new TypeReference<Map<String, String>>() {
        });
    }

    private static Map<String, TableBinding> bindings() throws IOException {
        Map<String, TableBinding> result = new HashMap<>();
        var row = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
        for (JsonNode table : fixture("thread-enabled-bindings.json").get("tables")) {
            String file = table.get("file").asText();
            result.put(file, new TableBinding(new TableIdentity(new TableId("AEQA", table.get("library").asText(), file), file,
                    table.get("member").asText(), table.get("fileJournalId").asText(), table.get("memberJournalId").asText(), "FIXTURE_INT_NOT_NULL"), row));
        }
        return result;
    }

    private static JsonNode fixture(String name) throws IOException {
        try (var stream = As400NativeLifecycleRecoveryTest.class.getResourceAsStream("/native-transactions/" + name)) {
            return new ObjectMapper().readTree(stream);
        }
    }

    private static List<Entry> entries(String name) throws IOException {
        for (JsonNode scenario : fixture("thread-enabled.json").get("cases")) {
            if (scenario.get("name").asText().equals(name)) {
                List<Entry> result = new ArrayList<>();
                for (JsonNode raw : scenario.get("entries")) {
                    byte[] bytes = HexFormat.of().parseHex(raw.get("hex").asText());
                    EntryHeader header = new EntryHeaderDecoder().decode(bytes, 0);
                    assertThat(header.getSequenceNumber().toString()).isEqualTo(raw.get("sequence").asText());
                    result.add(new Entry(bytes, header, new JournalPosition(header.getSequenceNumber(),
                            new JournalReceiver(raw.get("receiver").asText(), raw.get("receiver_library").asText()))));
                }
                return result;
            }
        }
        throw new IllegalArgumentException("Missing native scenario " + name);
    }

    private record Entry(byte[] bytes, EntryHeader header, JournalPosition position) {
        String type() {
            return header.getJournalCode() + "/" + header.getEntryType();
        }
    }

    private record ComponentResult(List<String> operations, List<Integer> remainingSavepoints) {
    }
}
