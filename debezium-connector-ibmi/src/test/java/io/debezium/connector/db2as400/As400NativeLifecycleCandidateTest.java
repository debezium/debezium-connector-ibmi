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
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400NativeLifecycleCandidate.Input;
import io.debezium.connector.db2as400.As400SourceNormalizer.NormalizationException;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;

class As400NativeLifecycleCandidateTest {
    private static final Limits LIMITS = new Limits(32, 1000, 128, 65536);

    static Stream<Arguments> positiveCaptures() {
        return Stream.concat(
                Stream.of("T_commit", "D_commit", "T_rollback", "D_rollback", "T_savepoint", "D_savepoint", "interleaved", "update_commit")
                        .map(name -> Arguments.of("thread-enabled", name)),
                Stream.of("truncate_commit", "delete_commit", "savepoint_reuse", "statement_failure", "atomic_statement_failure")
                        .map(name -> Arguments.of("lifecycle-discrimination", name)));
    }

    @ParameterizedTest
    @MethodSource("positiveCaptures")
    void boundedCandidateValidatesEveryCapturedRecordBeforeComponentReplay(String fixture, String name) throws Exception {
        var bindings = bindings(fixture);
        List<Input> input = entries(fixture, name);
        var result = As400NativeLifecycleCandidate.inspect(input, bindings);
        List<String> actual = replay(result.component(), bindings);
        List<String> expected = switch (name) {
            case "T_commit" -> List.of("c:MARK:1000", "t:TC2", "c:TC2:101");
            case "D_commit" -> List.of("c:MARK:2000", "t:DC2", "c:DC2:101");
            case "T_rollback", "D_rollback" -> List.of();
            case "T_savepoint" -> List.of("c:MARK:1200", "c:TS2:303");
            case "D_savepoint" -> List.of("c:MARK:2200", "c:DS2:303");
            case "interleaved" -> List.of("t:IB2", "c:IB2:501", "t:IA2", "c:IA2:401", "c:IA2:402");
            case "update_commit" -> List.of("u:UPD:11");
            case "truncate_commit" -> List.of("t:TC", "c:TC:101");
            case "delete_commit" -> List.of("t:DC", "c:DC:101");
            case "savepoint_reuse" -> List.of("c:SP:10", "c:SP:13");
            case "statement_failure" -> List.of("c:ERR:10", "c:ERR:11", "c:ERR:13");
            case "atomic_statement_failure" -> List.of("c:ERR:20", "c:ERR:24");
            default -> throw new AssertionError(name);
        };
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void realUnloggedWriteIsAbsentFromJournalAndBalancedUnflaggedGapIsRejected() throws Exception {
        List<Input> input = entries("lifecycle-discrimination", "manual_journal_gap");
        assertThat(input.stream().map(Input::type)).containsExactly("F/EJ", "D/EF", "D/JF", "F/JM", "R/PT");
        assertThat(input.subList(0, 4)).allSatisfy(e -> assertThat(e.header().getControlInfo().flags()).isEqualTo((byte) 0));
        var bindings = bindings("lifecycle-discrimination");
        Input row = input.get(4);
        assertThat(bindings.get("GAP").decoder().decode(row.header(), row.bytes(), 0)).containsExactly(1000);
        JsonNode scenario = scenario("lifecycle-discrimination", "manual_journal_gap");
        assertThat(scenario.get("source_rows").get("GAP").toString()).isEqualTo("[1,2,3,999,1000]");
        assertThrows(IllegalArgumentException.class, () -> As400NativeLifecycleCandidate.inspect(input, bindings));
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-ch", "missing-ej", "reordered", "ordinary-gap", "extra-flags", "pointer", "wrong-jid", "wrong-member",
            "wrong-thread", "wrong-cycle", "after-only", "wrong-subtype", "wrong-target", "incomplete" })
    void incompleteOrMismatchedCompanionsCannotProduceAProjection(String fault) throws Exception {
        List<Input> input = entries("thread-enabled", "T_commit");
        int ch = index(input, "F/CH");
        int ej = index(input, "F/EJ");
        int cg = index(input, "D/CG");
        switch (fault) {
            case "missing-ch" -> input.remove(ch);
            case "missing-ej" -> input.remove(ej);
            case "reordered" -> java.util.Collections.swap(input, ej + 1, ej + 2);
            case "incomplete" -> input = new ArrayList<>(input.subList(0, cg));
            default -> {
                int target = fault.equals("after-only") ? index(input, "D/JF")
                        : fault.equals("wrong-subtype") ? cg
                                : fault.equals("wrong-target") ? index(input, "C/SQ") : ej;
                Input old = input.get(target);
                byte[] bytes = old.bytes().clone();
                int payload = old.header().getEntrySpecificDataOffset() + 16;
                switch (fault) {
                    case "ordinary-gap" -> bytes[218] = 0;
                    case "extra-flags" -> bytes[218] |= 0x20;
                    case "pointer" -> ByteBuffer.wrap(bytes).putInt(72, 1);
                    case "wrong-jid" -> bytes[197] ^= 1;
                    case "wrong-member" -> System.arraycopy(new AS400Text(10).toBytes("OTHER"), 0, bytes, 177, 10);
                    case "wrong-thread" -> ByteBuffer.wrap(bytes).putLong(40, 999);
                    case "wrong-cycle" -> ByteBuffer.wrap(bytes).putLong(64, 999);
                    case "after-only" -> bytes[216] = new AS400Text(1).toBytes("0")[0];
                    case "wrong-subtype" -> bytes[payload + 108] = new AS400Text(1).toBytes("5")[0];
                    case "wrong-target" -> System.arraycopy(new AS400Text(20).toBytes("00000000000000000001"), 0, bytes, payload, 20);
                    default -> throw new AssertionError(fault);
                }
                input.set(target, new Input(new EntryHeaderDecoder().decode(bytes, 0), bytes, old.position()));
            }
        }
        List<Input> damaged = input;
        assertThrows(IllegalArgumentException.class, () -> As400NativeLifecycleCandidate.inspect(damaged, bindings("thread-enabled")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_rollback", "D_rollback" })
    void completedRollbackStillRequiresEarlierContextUntilCheckpointPassesTail(String name) throws Exception {
        List<Input> input = entries("thread-enabled", name);
        var result = As400NativeLifecycleCandidate.inspect(input, bindings("thread-enabled"));
        assertThat(result.recovery()).hasSize(1);
        var recovery = result.recovery().get(0);
        assertThat(recovery.start()).isEqualTo(input.get(index(input, "C/SC")).position());
        assertThat(recovery.neededAt(recovery.rollback())).isTrue();
        assertThat(recovery.neededAt(recovery.tailEnd())).isFalse();
        // An ordinary F=W=RB restart has lost the relation; it must not silently skip the tail.
        List<Input> suffix = input.subList(index(input, "C/RB") + 1, input.size());
        assertThrows(IllegalArgumentException.class, () -> As400NativeLifecycleCandidate.inspect(suffix, bindings("thread-enabled")));
        // Retaining the original SC reconstructs every expectation with a fresh matcher.
        var replay = As400NativeLifecycleCandidate.inspect(input.subList(index(input, "C/SC"), input.size()), bindings("thread-enabled"));
        assertThat(replay.recovery()).isEqualTo(result.recovery());
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_rollback", "D_rollback" })
    void contextFloorSurvivesSerializedOffsetsAndFreshRealCoordinator(String name) throws Exception {
        var bindings = bindings("thread-enabled");
        var input = entries("thread-enabled", name);
        var candidate = As400NativeLifecycleCandidate.inspect(input, bindings);
        var receiver = input.get(0).position().receiver();
        String identity = "FIXTURE_ONLY";
        String fingerprint = "v1:" + "a".repeat(64);
        var order = new As400JournalOrder(List.of(receiver), Map.of(receiver, identity),
                Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(10000))));
        var before = new JournalPosition(input.get(0).position().offset().subtract(BigInteger.ONE), receiver);
        As400Checkpoint checkpoint = As400Checkpoint.afterVerifiedBoundary(identity, fingerprint, before, identity);
        for (int restart = 0; restart < 2; restart++) {
            var mapper = new ObjectMapper();
            Map<String, String> saved = mapper.readValue(mapper.writeValueAsBytes(checkpoint.toOffset()), new TypeReference<Map<String, String>>() {
            });
            try (var coordinator = new As400ReplayCoordinator(saved, identity, fingerprint, order, LIMITS, 1024, 1000,
                    (file, library) -> bindings.get(file), As400SourceNormalizer.SavepointPolicy.consumingTargets(), Map.of("server", "fixture"),
                    record -> {
                        throw new AssertionError("Rolled-back data must not be published");
                    })) {
                for (Input entry : candidate.component()) {
                    if (entry.position().offset().compareTo(checkpoint.replayFrom().offset()) < 0) {
                        continue;
                    }
                    assertThat(coordinator.accept(entry.header(), entry.bytes(), 0, entry.position())).isEmpty();
                }
                coordinator.verifyEndOfAvailableJournal();
                checkpoint = candidate.retainContext(coordinator.checkpoint());
                var context = candidate.recovery().get(0);
                assertThat(checkpoint.replayFrom()).isEqualTo(context.start());
                assertThat(checkpoint.replayInclusive()).isTrue();
                assertThat(checkpoint.completedThrough()).isEqualTo(context.rollback());
                assertThat(coordinator.scanPosition()).contains(context.rollback());
                assertThat(input.get(input.size() - 1).position().offset()).isGreaterThan(checkpoint.completedThrough().offset());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "no-rollback", "flagged-delete", "wrong-jid", "wrong-thread", "no-replacement-marker" })
    void memberDeletionNeedsMatchingRollbackAndReplacementScope(String fault) throws Exception {
        List<Input> input = entries("thread-enabled", "T_savepoint");
        int md = index(input, "F/MD");
        if (fault.equals("no-rollback")) {
            input.remove(md - 1);
        }
        else if (fault.equals("no-replacement-marker")) {
            input.remove(md + 4);
        }
        else {
            Input old = input.get(md);
            byte[] bytes = old.bytes().clone();
            switch (fault) {
                case "flagged-delete" -> bytes[218] = 0x10;
                case "wrong-jid" -> bytes[197] ^= 1;
                case "wrong-thread" -> ByteBuffer.wrap(bytes).putLong(40, 999);
                default -> throw new AssertionError(fault);
            }
            input.set(md, new Input(new EntryHeaderDecoder().decode(bytes, 0), bytes, old.position()));
        }
        assertThrows(IllegalArgumentException.class, () -> As400NativeLifecycleCandidate.inspect(input, bindings("thread-enabled")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "SQ", "SU" })
    void conservativePolicyRejectsLaterReferenceToConsumedNativeTarget(String type) throws Exception {
        List<Input> input = entries("lifecycle-discrimination", "savepoint_reuse");
        int firstUndo = index(input, "C/SU");
        Input first = input.get(firstUndo);
        BigInteger target = new io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder()
                .decode(first.header(), first.bytes(), 0).savepointSequence();
        Input later = input.subList(firstUndo + 1, input.size()).stream().filter(e -> e.type().equals("C/" + type)).findFirst().orElseThrow();
        byte[] bytes = later.bytes().clone();
        int payload = later.header().getEntrySpecificDataOffset() + 16;
        System.arraycopy(new AS400Text(20).toBytes(String.format("%020d", target)), 0, bytes, payload, 20);
        input.set(input.indexOf(later), new Input(new EntryHeaderDecoder().decode(bytes, 0), bytes, later.position()));
        var bindings = bindings("lifecycle-discrimination");
        assertThrows(NormalizationException.class, () -> replay(input, bindings));
    }

    @Test
    void defaultNormalizerStillRejectsSavepointRollbackWithoutExplicitPolicy() throws Exception {
        var bindings = bindings("lifecycle-discrimination");
        try (var buffer = new As400TransactionBuffer("FIXTURE", LIMITS)) {
            var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024);
            for (Input input : entries("lifecycle-discrimination", "savepoint_reuse")) {
                if (input.type().equals("C/SU")) {
                    assertThrows(NormalizationException.class, () -> normalizer.accept(input.header(), input.bytes(), 0, input.position()));
                    return;
                }
                assertThat(normalizer.accept(input.header(), input.bytes(), 0, input.position())).isEmpty();
            }
            throw new AssertionError("Missing native rollback");
        }
    }

    private static List<String> replay(List<Input> input, Map<String, TableBinding> bindings) {
        List<String> result = new ArrayList<>();
        try (var buffer = new As400TransactionBuffer("FIXTURE", LIMITS)) {
            var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024,
                    As400SourceNormalizer.SavepointPolicy.consumingTargets());
            for (Input entry : input) {
                var batch = normalizer.accept(entry.header(), entry.bytes(), 0, entry.position());
                if (batch.isPresent()) {
                    for (var change : batch.orElseThrow().changes()) {
                        result.add(change.operation().code() + ":" + change.table().physicalFile()
                                + (change.after() == null ? "" : ":" + change.after().values()[0]));
                    }
                    buffer.completeBatch(batch.orElseThrow());
                }
            }
            assertThat(buffer.usage().transactions()).isZero();
        }
        return result;
    }

    private static int index(List<Input> entries, String type) {
        for (int i = 0; i < entries.size(); i++) {
            if (entries.get(i).type().equals(type)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Missing " + type);
    }

    private static JsonNode fixture(String name) throws IOException {
        try (var stream = As400NativeLifecycleCandidateTest.class.getResourceAsStream("/native-transactions/" + name + ".json")) {
            return new ObjectMapper().readTree(stream);
        }
    }

    private static JsonNode scenario(String fixture, String name) throws IOException {
        for (JsonNode scenario : fixture(fixture).get("cases")) {
            if (scenario.get("name").asText().equals(name)) {
                return scenario;
            }
        }
        throw new IllegalArgumentException("Missing scenario " + name);
    }

    private static List<Input> entries(String fixture, String name) throws IOException {
        List<Input> entries = new ArrayList<>();
        for (JsonNode raw : scenario(fixture, name).get("entries")) {
            byte[] bytes = HexFormat.of().parseHex(raw.get("hex").asText());
            var header = new EntryHeaderDecoder().decode(bytes, 0);
            entries.add(new Input(header, bytes, new JournalPosition(header.getSequenceNumber(),
                    new JournalReceiver(raw.get("receiver").asText(), raw.get("receiver_library").asText()))));
        }
        return entries;
    }

    private static Map<String, TableBinding> bindings(String fixture) throws IOException {
        Map<String, TableBinding> result = new HashMap<>();
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
        for (JsonNode table : fixture(fixture + "-bindings").get("tables")) {
            String file = table.get("file").asText();
            result.put(file, new TableBinding(new TableIdentity(new TableId("AEQA", table.get("library").asText(), file), file,
                    table.get("member").asText(), table.get("fileJournalId").asText(), table.get("memberJournalId").asText(), "FIXTURE_INT_NOT_NULL"), decoder));
        }
        return result;
    }
}
