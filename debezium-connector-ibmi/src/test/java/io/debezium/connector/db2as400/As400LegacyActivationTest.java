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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;

class As400LegacyActivationTest {
    @TempDir
    Path directory;

    static As400LegacyHandoff handoff(Fixture fixture, Map<String, String> initial) {
        var checkpoint = As400Checkpoint.fromOffset(initial, fixture.journalIdentity, initial.get("capture_fingerprint"));
        var legacy = Map.of("offset.event_sequence", "29", "offset.receiver", Fixture.RECEIVER.name(),
                "offset.receiver_library", Fixture.RECEIVER.library(), "offset.processed", "true", "offset.time", "0", "snapshot_completed", "true");
        return new As400LegacyHandoff(checkpoint, As400LegacyHandoff.bind(legacy, checkpoint), fixture.receiverIdentity);
    }

    static As400JournalOrder order(Fixture f) {
        return new As400JournalOrder(List.of(Fixture.RECEIVER), Map.of(Fixture.RECEIVER, f.receiverIdentity),
                Map.of(Fixture.RECEIVER, new As400JournalOrder.Range(java.math.BigInteger.valueOf(29), java.math.BigInteger.valueOf(64))));
    }

    @Test
    void pendingHistoryRecoveryNeverInventsCheckpointAndRequiresExactBarrierAfterActivation() throws Exception {
        var f = new Fixture(directory);
        var legacy = Map.of("offset.event_sequence", "28", "offset.receiver", Fixture.RECEIVER.name(),
                "offset.receiver_library", Fixture.RECEIVER.library(), "offset.processed", "true", "snapshot_completed", "true");
        assertThat(f.history().pending(legacy, Fixture.POLICY)).isEmpty();
        assertThat(Files.exists(f.file)).isFalse();
        var initial = f.initialize();
        String original = Files.readString(f.file);
        var pending = f.history().pending(legacy, Fixture.POLICY).orElseThrow();
        assertThat(pending.origin()).isEqualTo(Fixture.position(29));
        assertThat(pending.definitions()).isEqualTo(f.definitions);
        assertThat(pending.activation()).isEmpty();
        assertThrows(Exception.class, () -> f.history().recover(legacy, f.journalIdentity, Fixture.POLICY));
        var activation = handoff(f, initial);
        // Durable barrier without activation is still only pending source facts.
        assertThat(f.history().pending(activation.barrier(), Fixture.POLICY).orElseThrow().activation()).isEmpty();
        assertThat(Files.readString(f.file)).isEqualTo(original);
        f.history().recordLegacyHandoff(activation, Fixture.POLICY, order(f), activation::barrier);
        var activated = f.history().pending(activation.barrier(), Fixture.POLICY).orElseThrow();
        assertThat(activated.activation()).contains(activation);
        assertThrows(Exception.class, () -> f.history().pending(legacy, Fixture.POLICY));
        var changed = new HashMap<>(activation.barrier());
        changed.put("offset.time", "999");
        assertThrows(Exception.class, () -> f.history().pending(changed, Fixture.POLICY));
        Files.delete(f.file);
        assertThrows(Exception.class, () -> f.history().pending(activation.barrier(), Fixture.POLICY));
        assertThat(Files.exists(f.file)).isFalse();
    }

    @Test
    void invalidPendingOffsetFailsBeforeOpeningHistory() throws Exception {
        var f = new Fixture(directory);
        var opens = new java.util.concurrent.atomic.AtomicInteger();
        f.factory = () -> {
            opens.incrementAndGet();
            return new As400RecoveryTestSupport.FileHistory(f.file);
        };
        var legacy = Map.of("offset.event_sequence", "28", "offset.receiver", Fixture.RECEIVER.name(),
                "offset.receiver_library", Fixture.RECEIVER.library(), "offset.processed", "true", "snapshot_completed", "true");
        assertThrows(Exception.class, () -> f.history().pending(null, Fixture.POLICY));
        for (String field : legacy.keySet()) {
            var missing = new HashMap<>(legacy);
            missing.remove(field);
            assertThrows(Exception.class, () -> f.history().pending(missing, Fixture.POLICY));
        }
        for (String field : List.of(As400Checkpoint.VERSION, "snapshot", As400LegacyHandoff.VERSION, As400LegacyHandoff.CHECKPOINT)) {
            var ambiguous = new HashMap<>(legacy);
            ambiguous.put(field, "1");
            assertThrows(Exception.class, () -> f.history().pending(ambiguous, Fixture.POLICY));
        }
        assertThat(opens).hasValue(0);
    }

    @Test
    void nativePendingHistoryPersistsDiscoveryFloorSeparatelyFromSchemaAndPublicationPositions() throws Exception {
        var f = new Fixture(directory);
        var discovery = new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity);
        var baseline = f.history().recordPendingBaseline(f.journalIdentity, Fixture.position(42), f.receiverIdentity,
                f.definitions, Fixture.POLICY, discovery);
        var legacy = Map.of("offset.event_sequence", "28", "offset.receiver", Fixture.RECEIVER.name(),
                "offset.receiver_library", Fixture.RECEIVER.library(), "offset.processed", "true", "snapshot_completed", "true");
        var pending = f.history().pending(legacy, Fixture.POLICY).orElseThrow();
        assertThat(pending.origin()).isEqualTo(Fixture.position(42));
        assertThat(pending.discovery()).contains(discovery);
        assertThat(baseline.replayFrom()).isEqualTo(Fixture.position(42));
        assertThat(pending.activation()).isEmpty();
        var loaded = f.history().recover(baseline.toOffset(), f.journalIdentity, Fixture.POLICY);
        assertThrows(Exception.class, () -> loaded.requireStartupOrigin(baseline.toOffset(), true));
        String original = Files.readString(f.file);
        for (String field : List.of("discovery.sequence", "discovery.receiver", "discovery.library", "discovery.receiver.identity")) {
            var json = new com.fasterxml.jackson.databind.ObjectMapper();
            var damaged = (com.fasterxml.jackson.databind.node.ObjectNode) json.readTree(original);
            ((com.fasterxml.jackson.databind.node.ObjectNode) damaged.path("position")).remove(field);
            Files.writeString(f.file, damaged + "\n");
            assertThrows(Exception.class, () -> f.history().pending(legacy, Fixture.POLICY));
        }
        Files.writeString(f.file, original);
        assertThrows(Exception.class, () -> f.history().recordPendingBaseline(f.journalIdentity, Fixture.position(42), f.receiverIdentity,
                f.definitions, Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(43), f.receiverIdentity)));
        assertThrows(Exception.class, () -> f.history().recordPendingBaseline(f.journalIdentity, Fixture.position(42), f.receiverIdentity,
                f.definitions, Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), "changed-native-object")));
        assertThat(Files.readString(f.file)).isEqualTo(original);
    }

    @Test
    void pendingHistoryCannotStartAndActivationRequiresExactDurableBarrierWithFreshReadback() throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        var activation = handoff(f, initial);
        var history = f.history();
        var pending = history.recover(initial, f.journalIdentity, Fixture.POLICY);
        assertThrows(Exception.class, () -> pending.requireStartupOrigin(initial, true));
        for (Map<String, ?> wrong : List.<Map<String, ?>> of(initial, Map.of(), Map.of("offset.processed", "true"))) {
            assertThrows(Exception.class, () -> history.recordLegacyHandoff(activation, Fixture.POLICY, order(f), () -> wrong));
            assertThat(Files.readAllLines(f.file)).hasSize(1);
        }
        assertThrows(Exception.class, () -> history.recordLegacyHandoff(activation, Fixture.POLICY, order(f), () -> null));
        var accepted = history.recordLegacyHandoff(activation, Fixture.POLICY, order(f), activation::barrier);
        assertThat(accepted.initialization()).isEmpty();
        assertThat(accepted.handoff()).contains(activation);
        accepted.requireStartupOrigin(initial, true);
        assertThrows(Exception.class, () -> accepted.requireStartupOrigin(initial, false));
        // Crash after history acknowledgement, before F/W commit: the durable
        // legacy barrier recovers the same activation without appending another.
        f.history().recordLegacyHandoff(activation, Fixture.POLICY, order(f), activation::barrier);
        assertThat(Files.readAllLines(f.file)).hasSize(2);
        var changed = new HashMap<>(activation.barrier());
        changed.put("offset.time", "1");
        var conflict = new As400LegacyHandoff(activation.initial(), changed, f.receiverIdentity);
        assertThrows(Exception.class, () -> f.history().recordLegacyHandoff(conflict, Fixture.POLICY, order(f), conflict::barrier));
        assertThat(Files.readAllLines(f.file)).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-activation", "reordered", "changed-checkpoint", "changed-barrier", "foreign-source", "extra-field", "default-admission" })
    void normalTaskRejectsUncertainActivationBeforeJournalReadOrOutput(String fault) throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        var activation = handoff(f, initial);
        f.history().recordLegacyHandoff(activation, Fixture.POLICY, order(f), activation::barrier);
        var lines = new ArrayList<>(Files.readAllLines(f.file));
        switch (fault) {
            case "missing-activation" -> lines.remove(1);
            case "reordered" -> java.util.Collections.reverse(lines);
            case "changed-checkpoint" ->
                lines.set(1, lines.get(1).replace("\"checkpoint.completed_through.sequence\":\"29\"", "\"checkpoint.completed_through.sequence\":\"30\""));
            case "changed-barrier" -> lines.set(1, lines.get(1).replace("\"legacy.offset.time\":\"0\"", "\"legacy.offset.time\":\"1\""));
            case "foreign-source" -> lines.set(1, lines.get(1).replace("\"server\":\"test\"", "\"server\":\"other\""));
            case "extra-field" -> lines.set(1, lines.get(1).replace("\"kind\":\"legacy-handoff\"", "\"kind\":\"legacy-handoff\",\"foreign\":\"1\""));
        }
        Files.write(f.file, lines);
        try (var run = new As400RecoveryTaskTest.Running(source(initial, !fault.equals("default-admission")), initial)) {
            run.start();
            As400RecoveryTaskTest.await(() -> run.input.stopped.get());
            assertThrows(Exception.class, run::next);
            assertThat(run.input.reads).hasValue(0);
            assertThat(run.durable.get()).isEqualTo(initial);
        }
    }

    @Test
    void actualNormalTaskResumesAnInterruptedClearWithoutSnapshotOrInventedCompletedOutput() throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        var activation = handoff(f, initial);
        f.history().recordLegacyHandoff(activation, Fixture.POLICY, order(f), activation::barrier);
        Map<String, ?> saved;
        try (var run = new As400RecoveryTaskTest.Running(source(initial, true), initial)) {
            run.input.tail = 44;
            run.start();
            List<SourceRecord> output = new ArrayList<>();
            do {
                output.addAll(run.next());
            } while (!As400RecoveryTestSupport.data(output).contains("t:DCOC1107.B"));
            run.ack(output);
            saved = run.durable.get();
            assertThat(saved.get("completed_through.sequence")).isEqualTo("42");
            assertThat(saved.get("replay_from.sequence")).isEqualTo("31");
            assertThat(As400RecoveryTestSupport.data(output)).containsExactly("c:DCOC1107.A:8", "t:DCOC1107.B");
        }
        try (var replacement = new As400RecoveryTaskTest.Running(source(initial, true), saved)) {
            replacement.input.tail = 64;
            replacement.start();
            var remaining = replacement.throughCheckpoint("59");
            assertThat(As400RecoveryTestSupport.data(remaining)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            replacement.ack(remaining);
            assertThat(replacement.durable.get().get("completed_through.sequence")).isEqualTo("59");
        }
    }

    @Test
    void offsetMovementDuringActivationCannotAuthorizeCheckpointPublication() throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        var activation = handoff(f, initial);
        var durable = new AtomicReference<Map<String, ?>>(activation.barrier());
        f.factory = () -> new As400RecoveryTestSupport.FileHistory(f.file) {
            @Override
            protected void storeRecord(io.debezium.relational.history.HistoryRecord record) {
                super.storeRecord(record);
                durable.set(initial);
            }
        };
        assertThrows(Exception.class, () -> f.history().recordLegacyHandoff(activation, Fixture.POLICY, order(f), durable::get));
        assertThat(Files.readAllLines(f.file)).hasSize(2);
    }

    @Test
    void initializedSnapshotKeepsItsOriginalOriginAndCannotBeReplaced() throws Exception {
        var f = new Fixture(directory);
        var initial = As400RecoveryTaskFixture.initialize(f);
        var activation = handoff(f, initial);
        var recovered = f.history().recover(initial, f.journalIdentity, Fixture.POLICY);
        recovered.requireStartupOrigin(initial, false);
        assertThat(recovered.initialization()).isPresent();
        assertThat(recovered.handoff()).isEmpty();
        assertThrows(Exception.class, () -> f.history().recordLegacyHandoff(activation, Fixture.POLICY, order(f), activation::barrier));
        assertThrows(Exception.class, () -> f.history().pending(activation.barrier(), Fixture.POLICY));
        assertThat(Files.readAllLines(f.file)).hasSize(1);
    }

    @Test
    void missingConnectTransactionCapabilityClosesAdmissionBeforeHistoryRead() throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        try (var run = new As400RecoveryTaskTest.Running(source(initial, true), initial)) {
            var properties = new HashMap<>(As400RecoveryTaskTest.properties());
            properties.put("transaction.boundary", "connector");
            run.started = true;
            assertThrows(Exception.class, () -> run.task.start(properties));
            assertThat(run.input.recoveries).hasValue(0);
            assertThat(run.input.closes).hasValue(1);
            assertThat(run.input.stopped).isTrue();
            assertThat(run.durable.get()).isEqualTo(initial);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "barrier", "migration" })
    void sameNormalTaskTransitionsWithoutRestartAndStopPreservesLastAcknowledgedPhase(String interruptedPhase) throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        var initialBarrier = handoff(f, initial).barrier();
        var seed = new HashMap<>(initialBarrier);
        seed.remove(As400LegacyHandoff.VERSION);
        seed.remove(As400LegacyHandoff.CHECKPOINT);
        seed.remove(As400LegacyHandoff.FINGERPRINT);
        Map<String, ?> saved;
        try (var run = new As400RecoveryTaskTest.Running(new TransitionSource(directory, initial), seed)) {
            var transactions = org.mockito.Mockito.mock(org.apache.kafka.connect.source.TransactionContext.class);
            org.mockito.Mockito.when(run.context.transactionContext()).thenReturn(transactions);
            ((TransitionSource) run.input).durable = run.durable::get;
            connectorStart(run);
            var prefix = run.next();
            assertThat(prefix).hasSize(1);
            assertThat(prefix.get(0).value()).isEqualTo("legacy-row");
            org.mockito.Mockito.verifyNoInteractions(transactions);
            var barrier = run.next();
            assertThat(barrier.get(0).value()).isEqualTo("legacy-barrier");
            org.mockito.Mockito.verify(transactions).commitTransaction();
            assertThat(run.input.reads).hasValue(0);
            assertThat(Files.readAllLines(f.file)).hasSize(1);
            if (interruptedPhase.equals("migration")) {
                var batch = new ArrayList<>(prefix);
                batch.addAll(barrier);
                run.ack(batch);
                var migration = run.next();
                assertThat(migration.get(0).value()).isEqualTo("migration");
                assertThat(Files.readAllLines(f.file)).hasSize(2);
                assertThat(run.input.reads).hasValue(0);
            }
            saved = run.durable.get();
        }
        assertThat(saved).doesNotContainKey(As400Checkpoint.VERSION);
        try (var replacement = new As400RecoveryTaskTest.Running(new TransitionSource(directory, initial), saved)) {
            org.mockito.Mockito.when(replacement.context.transactionContext())
                    .thenReturn(org.mockito.Mockito.mock(org.apache.kafka.connect.source.TransactionContext.class));
            ((TransitionSource) replacement.input).durable = replacement.durable::get;
            connectorStart(replacement);
            if (interruptedPhase.equals("barrier")) {
                var prefix = new ArrayList<>(replacement.next());
                prefix.addAll(replacement.next());
                replacement.ack(prefix);
            }
            var migration = replacement.next();
            assertThat(migration.get(0).value()).isEqualTo("migration");
            replacement.ack(migration);
            var data = replacement.throughCheckpoint("42");
            assertThat(As400RecoveryTestSupport.data(data)).containsExactly("c:DCOC1107.A:8");
            replacement.ack(data);
            assertThat(replacement.input.reads.get()).isPositive();
            assertThat(Files.readAllLines(f.file)).hasSize(2);
        }
    }

    @Test
    void legacyOffsetChangedAfterTaskLoadRejectsBeforeHandoffOrPublication() throws Exception {
        var f = new Fixture(directory);
        var initial = f.initialize();
        var seed = new HashMap<>(handoff(f, initial).barrier());
        seed.remove(As400LegacyHandoff.VERSION);
        seed.remove(As400LegacyHandoff.CHECKPOINT);
        seed.remove(As400LegacyHandoff.FINGERPRINT);
        var changed = new HashMap<>(seed);
        changed.put("offset.time", "1");
        var input = new TransitionSource(directory, initial);
        try (var run = new As400RecoveryTaskTest.Running(input, seed)) {
            org.mockito.Mockito.when(run.context.transactionContext())
                    .thenReturn(org.mockito.Mockito.mock(org.apache.kafka.connect.source.TransactionContext.class));
            input.durable = run.durable::get;
            org.mockito.Mockito.when(run.context.offsetStorageReader().offsets(org.mockito.ArgumentMatchers.any())).thenAnswer(invocation -> {
                run.durable.set(changed);
                return Map.of(input.partition().getSourcePartition(), seed);
            });
            connectorStart(run);
            As400RecoveryTaskTest.await(input.stopped::get);
            assertThrows(Exception.class, run::next);
            assertThat(input.handoffs).isZero();
            assertThat(input.reads).hasValue(0);
            assertThat(run.durable.get()).isEqualTo(changed);
            assertThat(Files.readAllLines(f.file)).hasSize(1);
        }
    }

    private static void connectorStart(As400RecoveryTaskTest.Running run) {
        var properties = new HashMap<>(As400RecoveryTaskTest.properties());
        properties.put("transaction.boundary", "connector");
        run.started = true;
        run.task.start(properties);
    }

    static class TransitionSource extends As400RecoveryTaskFixture implements As400TaskHandoff {
        final Map<String, String> initial;
        java.util.function.Supplier<Map<String, ?>> durable;
        volatile int handoffs;

        TransitionSource(Path directory, Map<String, String> initial) throws Exception {
            super(directory, initial.get("capture_fingerprint"));
            this.initial = initial;
        }

        @Override
        public boolean allowLegacyHandoffRecovery() {
            return true;
        }

        @Override
        public void checkQualified(io.debezium.config.Configuration configuration) {
            if (sourceOffsetAcknowledgements()) {
                super.checkQualified(configuration);
            }
            else {
                As400RecoveryAcknowledgements.requireConnectorProfile(configuration.asMap());
            }
            if (!qualified.get() || stopped.get()) {
                throw new IllegalStateException("Controlled admission ended");
            }
        }

        @Override
        public As400Checkpoint handoff(Map<String, ?> legacy, Output output) throws Exception {
            handoffs++;
            var checkpoint = As400Checkpoint.fromOffset(initial, fixture.journalIdentity, expectedFingerprint);
            Map<String, String> barrier;
            if (!legacy.containsKey(As400LegacyHandoff.VERSION)) {
                output.emit(record(legacy, "legacy-row"));
                barrier = As400LegacyHandoff.bind(legacy, checkpoint);
                var marker = record(barrier, "legacy-barrier");
                output.barrier(marker);
                output.acknowledge(marker);
            }
            else {
                barrier = As400LegacyHandoff.strings(legacy);
            }
            var activation = new As400LegacyHandoff(checkpoint, barrier, fixture.receiverIdentity);
            fixture.history().recordLegacyHandoff(activation, Fixture.POLICY, order(fixture), () -> As400DeliveryOffset.strip(durable.get()));
            var marker = record(initial, "migration");
            output.barrier(marker);
            output.acknowledge(marker);
            return checkpoint;
        }

        private SourceRecord record(Map<String, ?> offset, String value) {
            return new SourceRecord(partition().getSourcePartition(), offset, "handoff-control", null, null,
                    org.apache.kafka.connect.data.SchemaBuilder.string().name("test.handoff.control").build(), value);
        }
    }

    private As400RecoveryTaskFixture source(Map<String, String> initial, boolean admitted) throws Exception {
        return new As400RecoveryTaskFixture(directory, initial.get("capture_fingerprint")) {
            @Override
            public boolean allowLegacyHandoffRecovery() {
                return admitted;
            }
        };
    }
}
