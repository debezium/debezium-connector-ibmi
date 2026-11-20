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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.connector.db2as400.As400SourceNormalizer.NormalizationException;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;

/** Native thread values, candidate fixture bindings only; no startup proof or publication. */
class As400NativeThreadReplayTest {
    private static final CompleteRowEntryDecoder ROW = new CompleteRowEntryDecoder(
            new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);

    static Stream<Arguments> cases() throws IOException {
        List<Arguments> result = new ArrayList<>();
        for (JsonNode scenario : fixture("thread-enabled.json").get("cases")) {
            result.add(Arguments.of(scenario.get("name").asText(), scenario));
        }
        return result.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void actualNativeThreadBytesSurviveDecoding(String name, JsonNode scenario) {
        int rows = 0;
        for (Entry entry : entries(scenario)) {
            if (entry.header().getJournalCode() == 'R') {
                BigInteger actualThread = new BigInteger(1, Arrays.copyOfRange(entry.bytes(), 40, 48));
                assertThat(actualThread).isPositive();
                var info = entry.header().getRecordInfo();
                assertThat(info.threadId()).isEqualTo(actualThread);
                assertThat(info.jobName()).isNotBlank();
                assertThat(info.jobUser()).isNotBlank();
                assertThat(info.jobNumber()).matches("[0-9]{6}").isNotEqualTo("000000");
                assertThat(info.objectType()).isEqualTo("*QDDS");
                rows++;
            }
        }
        assertThat(rows).isPositive();
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "D_commit", "T_rollback", "D_rollback", "interleaved", "update_commit" })
    void componentReplayUsesNativeThreadsAndPreservesOwnedValuesAndMetadata(String name) throws Exception {
        Replay replay = replay(name, true);
        assertThat(replay.rejectedType()).isNull();
        List<String> expected = switch (name) {
            case "T_commit" -> List.of("c:MARK:1000", "t:TC2", "c:TC2:101");
            case "D_commit" -> List.of("c:MARK:2000", "t:DC2", "c:DC2:101");
            case "interleaved" -> List.of("t:IB2", "c:IB2:501", "t:IA2", "c:IA2:401", "c:IA2:402");
            case "update_commit" -> List.of("u:UPD:11");
            default -> List.of();
        };
        assertThat(operations(replay.batches())).containsExactlyElementsOf(expected);
        if (name.equals("interleaved")) {
            assertThat(replay.batches()).hasSize(2);
            assertThat(replay.batches().get(0).transactionId().commitCycle()).isGreaterThan(replay.batches().get(1).transactionId().commitCycle());
            assertThat(replay.batches().get(0).commitPosition().offset()).isLessThan(replay.batches().get(1).commitPosition().offset());
        }
    }

    @Test
    void nativeUpdatePairsMatchingProducerAndRecordWithoutDroppingOtherEntries() throws Exception {
        Replay replay = replay("update_commit", false);
        assertThat(replay.rejectedType()).isNull();
        assertThat(replay.batches()).hasSize(1);
        var changes = replay.batches().get(0).changes();
        assertThat(changes).hasSize(1);
        var change = changes.get(0);
        assertThat(change.before().values()).containsExactly(1);
        assertThat(change.after().values()).containsExactly(11);
        var before = change.before().source().recordInfo();
        var after = change.source().recordInfo();
        assertThat(before.threadId()).isPositive().isEqualTo(after.threadId());
        assertThat(before.jobName()).isEqualTo(after.jobName());
        assertThat(before.jobUser()).isEqualTo(after.jobUser());
        assertThat(before.jobNumber()).isEqualTo(after.jobNumber());
        assertThat(before.systemName()).isEqualTo(after.systemName());
        assertThat(before.recordNumber()).isEqualTo(after.recordNumber());
        assertThat(before.objectJournalId()).isEqualTo(after.objectJournalId());
        assertThat(change.before().source().entryType()).isEqualTo("UB");
        assertThat(change.source().entryType()).isEqualTo("UP");
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_savepoint", "D_savepoint" })
    void realThreadMetadataDoesNotAuthorizeUnresolvedSavepointRollback(String name) throws Exception {
        Replay replay = replay(name, true);
        assertThat(replay.rejectedType()).isEqualTo("C/SU");
        assertThat(replay.reason()).contains("Savepoint rollback disposition is unresolved");
        assertThat(replay.batches()).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "D_commit", "T_rollback", "D_rollback", "T_savepoint", "D_savepoint", "interleaved" })
    void realThreadMetadataDoesNotAuthorizeNativeJournalingTransitions(String name) throws Exception {
        Replay replay = replay(name, false);
        assertThat(replay.rejectedType()).isEqualTo("F/CH");
        // The native transition has cycle zero and hits that guard before operation classification.
        assertThat(replay.reason()).isEqualTo("Cycle-zero operation requires the future output coordinator");
        assertThat(replay.batches()).isEmpty();
    }

    private static Replay replay(String name, boolean component) throws Exception {
        JsonNode scenario = null;
        for (JsonNode candidate : fixture("thread-enabled.json").get("cases")) {
            if (candidate.get("name").asText().equals(name)) {
                scenario = candidate;
            }
        }
        assertThat(scenario).isNotNull();
        Map<String, TableBinding> bindings = new HashMap<>();
        for (JsonNode table : fixture("thread-enabled-bindings.json").get("tables")) {
            String file = table.get("file").asText();
            bindings.put(file, new TableBinding(new TableIdentity(new TableId("TEST", table.get("library").asText(), file), file,
                    table.get("member").asText(), table.get("fileJournalId").asText(), table.get("memberJournalId").asText(), "FIXTURE_INT_NOT_NULL"), ROW));
        }
        List<Entry> entries = entries(scenario);
        Map<BigInteger, As400SourceMetadata> sources = new HashMap<>();
        entries.forEach(e -> sources.put(e.header().getSequenceNumber(), As400SourceMetadata.from(e.header(), e.position())));
        List<CommittedBatch> batches = new ArrayList<>();
        try (var buffer = new As400TransactionBuffer("FIXTURE_ONLY", new Limits(16, 1000, 100, 65536))) {
            var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024);
            for (Entry entry : entries) {
                EntryHeader header = entry.header();
                // Component contract only; complete-stream cases above retain the F/D transitions.
                if (component && !(header.getJournalCode() == 'C' || header.getJournalCode() == 'R'
                        || header.getJournalCode() == 'D' && header.getEntryType().equals("CG"))) {
                    continue;
                }
                try {
                    var batch = normalizer.accept(header, entry.bytes(), 0, entry.position());
                    Arrays.fill(entry.bytes(), (byte) 0);
                    if (batch.isPresent()) {
                        batches.add(batch.get());
                        buffer.completeBatch(batch.get());
                    }
                }
                catch (NormalizationException failure) {
                    assertThat(assertThrows(NormalizationException.class,
                            () -> normalizer.accept(header, entry.bytes(), 0, entry.position())).getMessage()).contains("is stopped");
                    assertThrows(As400TransactionBuffer.BufferException.class, buffer::pendingBatch);
                    return new Replay(batches, header.getJournalCode() + "/" + header.getEntryType(), failure.getCause().getMessage());
                }
            }
            assertThat(buffer.usage().transactions()).isZero();
        }
        // Check after every transport buffer has been overwritten, including images retained before a commit.
        for (var batch : batches) {
            for (var change : batch.changes()) {
                assertThat(change.table()).isEqualTo(bindings.get(change.table().physicalFile()).identity());
                assertThat(change.source()).isEqualTo(sources.get(change.position().offset()));
                if (change.record() != null) {
                    assertThat(change.record().recordNumber()).isEqualTo(change.source().recordInfo().recordNumber());
                    assertThat(change.source().recordInfo().threadId()).isPositive();
                }
                if (change.before() != null) {
                    assertThat(change.before().source()).isEqualTo(sources.get(change.before().source().position().offset()));
                }
            }
        }
        return new Replay(batches, null, null);
    }

    private static List<String> operations(List<CommittedBatch> batches) {
        List<String> result = new ArrayList<>();
        for (var batch : batches) {
            for (var change : batch.changes()) {
                result.add(change.operation().code() + ":" + change.table().physicalFile() + (change.after() == null ? "" : ":" + change.after().values()[0]));
            }
        }
        return result;
    }

    private static JsonNode fixture(String name) throws IOException {
        try (var stream = As400NativeThreadReplayTest.class.getResourceAsStream("/native-transactions/" + name)) {
            return new ObjectMapper().readTree(stream);
        }
    }

    private static List<Entry> entries(JsonNode scenario) {
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

    private record Entry(byte[] bytes, EntryHeader header, JournalPosition position) {
    }

    private record Replay(List<CommittedBatch> batches, String rejectedType, String reason) {
    }
}
