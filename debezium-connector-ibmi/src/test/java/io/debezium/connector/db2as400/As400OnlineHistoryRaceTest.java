/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.relational.history.HistoryRecord;

class As400OnlineHistoryRaceTest {
    @TempDir
    Path directory;

    private static Map<String, String> legacy() {
        return Map.of("offset.event_sequence", "28", "offset.receiver", Fixture.RECEIVER.name(), "offset.receiver_library", Fixture.RECEIVER.library(),
                "offset.processed", "true", "snapshot_completed", "true");
    }

    @Test
    void actualTaskReplaysOpenClearFromTheSelectedEpochAfterLateProposalAndRejectsOffsetLoss() throws Exception {
        var f = new Fixture(directory);
        var pending = f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions, Fixture.POLICY,
                new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        var initial = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, pending.fingerprint(), Fixture.position(29), f.receiverIdentity).toOffset();
        Map<String, ?> saved;
        try (var first = new As400RecoveryTaskTest.Running(onlineSource(pending.fingerprint()), initial)) {
            first.start();
            var prefix = first.throughCheckpoint("42");
            assertThat(As400RecoveryTestSupport.data(prefix)).containsExactly("c:DCOC1107.A:8");
            first.ack(prefix);
            saved = first.durable.get();
            assertThat(saved.get("replay_from.sequence")).isEqualTo("31");
        }
        var otherDirectory = directory.resolve("late");
        Files.createDirectory(otherDirectory);
        var late = new Fixture(otherDirectory);
        late.history().recordOnlineCandidate(late.journalIdentity, Fixture.position(31), late.receiverIdentity, late.definitions, Fixture.POLICY,
                new As400BindingHistory.Discovery(Fixture.position(29), late.receiverIdentity), legacy());
        Files.writeString(f.file, Files.readString(late.file), java.nio.file.StandardOpenOption.APPEND);
        String retained = Files.readString(f.file);
        try (var replacement = new As400RecoveryTaskTest.Running(onlineSource(pending.fingerprint()), saved)) {
            replacement.input.tail = 64;
            replacement.start();
            var rest = replacement.throughCheckpoint("59");
            assertThat(As400RecoveryTestSupport.data(rest)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            replacement.ack(rest);
            assertThat(replacement.durable.get().get("capture_fingerprint")).isEqualTo(pending.fingerprint());
        }
        try (var missing = new As400RecoveryTaskTest.Running(onlineSource(pending.fingerprint()), null)) {
            assertThrows(Exception.class, missing::start);
            assertThat(missing.input.recoveries).hasValue(0);
        }
        assertThat(Files.readString(f.file)).isEqualTo(retained);
    }

    private As400RecoveryTaskFixture onlineSource(String fingerprint) throws Exception {
        return new As400RecoveryTaskFixture(directory, fingerprint) {
            @Override
            public boolean allowLegacyHandoffRecovery() {
                return true;
            }

            @Override
            As400BindingHistory.Recovered recoverHistory(Map<String, ?> durable) throws Exception {
                return fixture.history().recoverOnlineCandidate(durable, journalIdentity(), Fixture.POLICY);
            }
        };
    }

    @ParameterizedTest
    @ValueSource(strings = { "late-proposal", "duplicate", "missing-offset", "missing-selected", "corrupt-unselected", "strict-recovery", "default-admission" })
    void connectSelectedCandidateSurvivesALateProposalWithoutWeakeningStrictHistory(String fault) throws Exception {
        var f = new Fixture(directory);
        var replacement = new As400BindingHistory(() -> new As400RecoveryTestSupport.FileHistory(f.file), f.config, Fixture.POLICY.sourcePartition());
        var accepted = new AtomicReference<Map<String, String>>();
        f.factory = () -> new As400RecoveryTestSupport.FileHistory(f.file) {
            @Override
            protected void storeRecord(HistoryRecord record) {
                try {
                    var selected = replacement.recordOnlineCandidate(f.journalIdentity, Fixture.position(30), f.receiverIdentity, f.definitions,
                            Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
                    // Controlled offset-store input for local recovery selection;
                    // actual Kafka publication/acknowledgement is tested separately.
                    accepted.set(As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, selected.fingerprint(), Fixture.position(30), f.receiverIdentity).toOffset());
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                super.storeRecord(record);
            }
        };
        var selected = f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        // This old task can append only a proposal; the replacement's fenced
        // durable offset remains the sole authority for publication/recovery.
        assertThat(selected.origin()).isEqualTo(Fixture.position(29));
        assertThat(selected.fingerprint()).isNotEqualTo(accepted.get().get("capture_fingerprint"));
        var lines = new java.util.ArrayList<>(Files.readAllLines(f.file));
        assertThat(lines).hasSize(2 * (f.definitions.size() + 1));
        assertThat(replacement.pendingOnlineCandidate(legacy(), Fixture.POLICY).orElseThrow().fingerprint()).isEqualTo(selected.fingerprint());
        switch (fault) {
            case "duplicate" -> lines.add(lines.get(0));
            case "missing-selected" -> lines.remove(0);
            case "corrupt-unselected" -> lines.set(lines.size() - 1, lines.get(lines.size() - 1).replace("\"sequence\":\"29\"", "\"sequence\":\"31\""));
            default -> {
            }
        }
        Files.write(f.file, lines);
        if (fault.equals("missing-offset")) {
            assertThrows(Exception.class, () -> replacement.recoverOnlineCandidate(null, f.journalIdentity, Fixture.POLICY));
            assertThrows(Exception.class, () -> replacement.pendingOnlineCandidate(null, Fixture.POLICY));
        }
        else if (fault.equals("strict-recovery")) {
            assertThrows(Exception.class, () -> replacement.recover(accepted.get(), f.journalIdentity, Fixture.POLICY));
        }
        else if (fault.equals("missing-selected") || fault.equals("corrupt-unselected")) {
            assertThrows(Exception.class, () -> replacement.recoverOnlineCandidate(accepted.get(), f.journalIdentity, Fixture.POLICY));
        }
        else {
            var recovered = replacement.recoverOnlineCandidate(accepted.get(), f.journalIdentity, Fixture.POLICY);
            assertThat(recovered.fingerprint()).isEqualTo(accepted.get().get("capture_fingerprint"));
            assertThat(recovered.handoff()).isEmpty();
            if (fault.equals("default-admission")) {
                assertThrows(Exception.class, () -> recovered.requireStartupOrigin(accepted.get(), false));
            }
            else {
                recovered.requireStartupOrigin(accepted.get(), true);
            }
        }
    }

    @Test
    void shouldSupersedeOnlyUnpublishedCandidatesAndKeepTheDurableBarrierSelectionAcrossRestart() throws Exception {
        var f = new Fixture(directory);
        var first = f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        var firstOffset = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, first.fingerprint(), Fixture.position(29), f.receiverIdentity);
        var barrier = As400LegacyHandoff.bind(Map.of("offset.event_sequence", "30", "offset.receiver", Fixture.RECEIVER.name(),
                "offset.receiver_library", Fixture.RECEIVER.library(), "offset.processed", "true", "snapshot_completed", "true"), firstOffset);
        var second = f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(30), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        assertThat(second.fingerprint()).isNotEqualTo(first.fingerprint());
        var fresh = new Fixture(directory).history();
        assertThat(fresh.pendingOnlineCandidate(legacy(), Fixture.POLICY).orElseThrow().fingerprint()).isEqualTo(second.fingerprint());
        assertThat(fresh.pendingOnlineCandidate(barrier, Fixture.POLICY).orElseThrow().fingerprint()).isEqualTo(first.fingerprint());
        var historicalBarrier = new java.util.HashMap<>(barrier);
        historicalBarrier.remove(As400LegacyHandoff.FINGERPRINT);
        assertThrows(Exception.class, () -> fresh.pendingOnlineCandidate(historicalBarrier, Fixture.POLICY)); // v2 never issued an unselected barrier.
        assertThrows(Exception.class, () -> fresh.recordOnlineCandidate(f.journalIdentity, Fixture.position(31), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), barrier));
        assertThat(fresh.recoverOnlineCandidate(firstOffset.toOffset(), f.journalIdentity, Fixture.POLICY).fingerprint()).isEqualTo(first.fingerprint());
        assertThat(Files.readAllLines(f.file)).hasSize(2 * (f.definitions.size() + 1));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldPreserveSelectedHistoryBeyondTheOldLimitAndALateWriterRace(boolean race) throws Exception {
        var f = new Fixture(directory);
        var first = f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        var initial = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, first.fingerprint(), Fixture.position(29), f.receiverIdentity).toOffset();
        // Canonical duplicate proposals model retries beyond the old lifetime limit.
        String firstRecord = Files.readString(f.file);
        Files.writeString(f.file, firstRecord.repeat(As400BindingHistory.MAX_HISTORY_RECORDS - 1));
        var fresh = new Fixture(directory).history();
        var winner = new AtomicReference<As400BindingHistory.Pending>();
        if (race) {
            f.factory = () -> new As400RecoveryTestSupport.FileHistory(f.file) {
                @Override
                protected void storeRecord(HistoryRecord record) {
                    try {
                        // Replacement completes while an old proposal is still arriving.
                        // Appended table pages cannot replace an acknowledged selection.
                        winner.set(fresh.recordOnlineCandidate(f.journalIdentity, Fixture.position(31), f.receiverIdentity, f.definitions,
                                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy()));
                    }
                    catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    super.storeRecord(record);
                }
            };
            f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(30), f.receiverIdentity, f.definitions,
                    Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        }
        else {
            winner.set(fresh.recordOnlineCandidate(f.journalIdentity, Fixture.position(30), f.receiverIdentity, f.definitions,
                    Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy()));
        }
        assertThat(Files.readAllLines(f.file).size()).isGreaterThan(As400BindingHistory.MAX_HISTORY_RECORDS);
        var restarted = new Fixture(directory).history();
        var newest = restarted.recordOnlineCandidate(f.journalIdentity, Fixture.position(32), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        String retained = Files.readString(f.file);
        assertThat(restarted.pendingOnlineCandidate(legacy(), Fixture.POLICY).orElseThrow().fingerprint()).isEqualTo(newest.fingerprint());
        var winnerOffset = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, winner.get().fingerprint(), winner.get().origin(), f.receiverIdentity);
        assertThat(restarted.recoverOnlineCandidate(winnerOffset.toOffset(), f.journalIdentity, Fixture.POLICY).fingerprint()).isEqualTo(winner.get().fingerprint());
        assertThat(restarted.recoverOnlineCandidate(initial, f.journalIdentity, Fixture.POLICY).fingerprint()).isEqualTo(first.fingerprint());
        // The earlier acknowledged epoch still drives actual fresh task replay.
        try (var task = new As400RecoveryTaskTest.Running(onlineSource(first.fingerprint()), initial)) {
            task.input.tail = 64;
            task.start();
            var records = task.throughCheckpoint("59");
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:DCOC1107.A:8", "t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            task.ack(records);
        }
        assertThat(Files.readString(f.file)).isEqualTo(retained);
    }

    @Test
    void strictHistoryRejectsALateUnfencedProposalEvenAfterReplacementActivation() throws Exception {
        var f = new Fixture(directory);
        var replacement = new As400BindingHistory(() -> new As400RecoveryTestSupport.FileHistory(f.file), f.config, Fixture.POLICY.sourcePartition());
        var accepted = new AtomicReference<Map<String, String>>();
        f.factory = () -> new As400RecoveryTestSupport.FileHistory(f.file) {
            @Override
            protected void storeRecord(HistoryRecord record) {
                try {
                    // Old task already observed exists()==false and is paused at
                    // its history append. Its independent producer is not Connect's
                    // fenced source producer. Replacement completes first.
                    var initial = replacement.recordPendingBaseline(f.journalIdentity, Fixture.position(30), f.receiverIdentity, f.definitions,
                            Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity));
                    var legacy = Map.of("offset.event_sequence", "30", "offset.receiver", Fixture.RECEIVER.name(),
                            "offset.receiver_library", Fixture.RECEIVER.library(), "offset.processed", "true", "snapshot_completed", "true");
                    var handoff = new As400LegacyHandoff(initial, As400LegacyHandoff.bind(legacy, initial), f.receiverIdentity);
                    replacement.recordLegacyHandoff(handoff, Fixture.POLICY, As400LegacyActivationTest.order(f), handoff::barrier);
                    accepted.set(initial.toOffset());
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
                super.storeRecord(record);
            }
        };
        assertThrows(Exception.class, () -> f.history().recordPendingBaseline(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity)));
        assertThat(accepted.get()).isNotNull();
        assertThat(Files.readAllLines(f.file)).hasSize(3);
        assertThrows(Exception.class, () -> replacement.recover(accepted.get(), f.journalIdentity, Fixture.POLICY));
    }

    @ParameterizedTest
    @ValueSource(strings = { "total-bytes", "oversized-utf8-record" })
    void shouldBoundIndividualRecordsWithoutLimitingTotalHistoryBytes(String fault) throws Exception {
        var f = new Fixture(directory);
        var definition = f.definitions.get(0);
        var schema = definition.schema().edit().addAttribute(io.debezium.relational.Attribute.editor().name("padding")
                .value(fault.equals("total-bytes") ? "x".repeat(3 * 1024 * 1024) : "\u20ac".repeat(1500000)).create()).create();
        f.definitions.set(0, new As400BindingDefinition(definition.identity(), schema, definition.codecs()));
        if (fault.equals("oversized-utf8-record")) {
            assertThrows(IllegalArgumentException.class,
                    () -> f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions,
                            Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy()));
            assertThat(Files.exists(f.file)).isFalse();
            return;
        }
        var first = f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        String record = Files.readString(f.file);
        int eligible = As400BindingHistory.MAX_HISTORY_BYTES / record.stripTrailing().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        Files.writeString(f.file, record.repeat(eligible));
        String retained = Files.readString(f.file);
        f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(30), f.receiverIdentity, f.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), legacy());
        assertThat(Files.readString(f.file)).startsWith(retained);
        assertThat(Files.size(f.file)).isGreaterThan(As400BindingHistory.MAX_HISTORY_BYTES);
        var initial = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, first.fingerprint(), Fixture.position(29), f.receiverIdentity);
        assertThat(new Fixture(directory).history().recoverOnlineCandidate(initial.toOffset(), f.journalIdentity, Fixture.POLICY).fingerprint())
                .isEqualTo(first.fingerprint());
    }
}
