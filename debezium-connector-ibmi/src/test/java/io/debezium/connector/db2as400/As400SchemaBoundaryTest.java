/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Actual native ALTER and reversal: identical endpoint bindings cannot replace a complete journal. */
class As400SchemaBoundaryTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir
    Path directory;

    private JsonNode fixture() throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/equal-width-alter.json")) {
            var fixture = As400OnlineFenceEvidenceTest.withUnverifiedMetadataForReplay(JSON.readTree(stream));
            assertThat(fixture.path("startup_proof").asBoolean()).isFalse();
            assertThat(fixture.path("cleanup").path("status").asText()).isEqualTo("passed");
            return fixture;
        }
    }

    private List<Entry> entries(JsonNode fixture) throws Exception {
        var result = new ArrayList<Entry>();
        for (var item : fixture.path("capture").path("entries")) {
            result.add(As400OnlineCensusReplay.load(Base64.getDecoder().decode(item.path("raw").asText()), item));
        }
        return result;
    }

    private final class SchemaFixture {
        final List<As400BindingDefinition> before;
        final As400BindingDefinition oldB;
        final As400BindingDefinition newB;
        final List<RawJournalPage.Entry> captured;
        final As400JournalOrder order;
        final String journal;
        final As400CaptureFingerprint.Policy policy;
        final Configuration config;
        final As400Checkpoint initial;
        final As400SchemaChange change;
        final As400BindingDefinition excludedLob;
        final long observedThrough;
        int refreshes;

        SchemaFixture() throws Exception {
            this(false);
        }

        SchemaFixture(boolean withExcludedLob) throws Exception {
            var json = fixture();
            before = new ArrayList<>(As400OnlineFenceEvidence.validateOffLock(json.path("before")).tables().definitions());
            oldB = before.stream().filter(d -> d.identity().physicalFile().equals("B")).findFirst().orElseThrow();
            newB = As400OnlineFenceEvidence.validateOffLock(json.path("changed")).tables().definitions().stream()
                    .filter(d -> d.identity().physicalFile().equals("B")).findFirst().orElseThrow();
            captured = new ArrayList<>(entries(json).stream()
                    .map(entry -> new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes())).toList());
            observedThrough = withExcludedLob ? 62 : 61;
            if (withExcludedLob) {
                try (var input = getClass().getResourceAsStream("/native-transactions/excluded-lob.json")) {
                    var lob = JSON.readTree(input);
                    var identity = lob.path("excluded");
                    var schema = oldB.identity().tableId().schema();
                    excludedLob = As400BindingDefinition.excluded(new As400TransactionBuffer.TableIdentity(
                            new io.debezium.relational.TableId(oldB.identity().tableId().catalog(), schema, "X"), "X", "X",
                            identity.path("file_jid").asText(), identity.path("member_jid").asText(), identity.path("format").asText()), true);
                    before.add(excludedLob);
                    // Controlled interleaving: a captured native pointer row
                    // arrives during metadata observation, after B's ALTER.
                    // Do not include the original fixture's second ALTER cycle.
                    captured.removeIf(entry -> entry.position().offset().longValueExact() > 61);
                    byte[] raw = Base64.getDecoder().decode(lob.path("entries").get(0).path("raw").asText());
                    java.nio.ByteBuffer.wrap(raw).putLong(24, observedThrough);
                    new com.ibm.as400.access.AS400Text(10, 37).toBytes(schema, raw, 167);
                    captured.add(new RawJournalPage.Entry(position(observedThrough), new EntryHeaderDecoder(37).decode(raw, 0), raw));
                }
            }
            else {
                excludedLob = null;
            }
            var nativeWindow = As400OnlineNativeEvidence.restore(json.path("capture").path("native_identity_evidence"));
            journal = nativeWindow.journalToken();
            var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
            As400OperatorCutReader.chain(nativeWindow.before().directory(), captured.get(0).position().receiver())
                    .forEach(receiver -> ranges.put(receiver.name(), new As400JournalOrder.Range(receiver.first(), receiver.last())));
            order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), nativeWindow.receiverTokens(), ranges);
            var original = As400RecoveryTestSupport.Fixture.POLICY;
            policy = new As400CaptureFingerprint.Policy(original.filters(), original.skippedOperations(), original.decoder(), original.schema(),
                    original.output(), false, true, Map.of("server", "offline"));
            config = Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName())
                    .with(FileSchemaHistory.FILE_PATH, directory.resolve("schema-history.jsonl").toString()).build();
            var baseline = position(48);
            var selected = history().recordOnlineCandidate(journal, baseline, order.incarnation(baseline), before, policy,
                    new As400BindingHistory.Discovery(baseline, order.incarnation(baseline)),
                    Map.of("offset.event_sequence", "47", "offset.receiver", baseline.receiver().name(), "offset.receiver_library", baseline.receiver().library(),
                            "offset.processed", "true", "snapshot_completed", "true"));
            initial = As400Checkpoint.afterVerifiedBoundary(journal, selected.fingerprint(), baseline, order.incarnation(baseline));
            change = As400SchemaChange.qualify(oldB, newB, range(52, observedThrough), order, position(61), bindings());
        }

        As400SourceNormalizer.TableResolver bindings() {
            return (file, library) -> before.stream()
                    .filter(definition -> definition.identity().physicalFile().equals(file) && definition.identity().tableId().schema().equals(library))
                    .findFirst().orElseThrow().binding(65536);
        }

        JournalPosition position(long sequence) {
            return new JournalPosition(java.math.BigInteger.valueOf(sequence), captured.get(0).position().receiver());
        }

        List<RawJournalPage.Entry> range(long first, long last) {
            return captured.stream().filter(entry -> entry.position().offset().longValueExact() >= first && entry.position().offset().longValueExact() <= last).toList();
        }

        As400BindingHistory history() {
            return new As400BindingHistory(As400BindingHistory.storage(config), config, policy.sourcePartition());
        }

        As400BindingHistory.Recovered recover(As400Checkpoint checkpoint) throws Exception {
            return history().recoverOnlineCandidate(checkpoint.toOffset(), journal, policy);
        }

        List<RawJournalPage.Entry> withUnrelatedOpenTransaction() {
            // Controlled interleaving around the unchanged captured DDL bytes:
            // another thread starts a cycle and holds F while B changes schema.
            byte[] start = range(50, 50).get(0).data();
            java.nio.ByteBuffer.wrap(start).putLong(24, 49).putLong(40, 9999).putLong(64, 49);
            var values = new ArrayList<RawJournalPage.Entry>();
            values.add(new RawJournalPage.Entry(position(49), new EntryHeaderDecoder().decode(start, 0), start));
            values.addAll(range(50, observedThrough));
            return values;
        }

        As400ContinuousRuntime runtime(As400Checkpoint checkpoint, As400BindingHistory.Recovered restored,
                                       As400OnlineCensusRecovery.Output output, As400ReplayCoordinator.RecordSink sink, boolean disk) {
            var input = new As400ContinuousRuntime.Intervals() {
                @Override
                public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) {
                    var rows = withUnrelatedOpenTransaction().stream().filter(entry -> scan.isPresent()
                            ? order.compare(entry.position(), scan.orElseThrow().position()) > 0
                            : order.compare(entry.position(), checkpoint.replayFrom()) >= (checkpoint.replayInclusive() ? 0 : 1)).toList();
                    return new As400OperatorCutReader.Cut(checkpoint, order, position(observedThrough), rows, List.of(), Optional.of(restored));
                }

                @Override
                public As400SchemaChange schemaChange(As400BindingDefinition old, RawJournalPage.Entry opening, As400SourceNormalizer.TableResolver verifiedBindings)
                        throws Exception {
                    refreshes++;
                    assertThat(old).isEqualTo(oldB);
                    assertThat(opening.position()).isEqualTo(position(52));
                    return As400SchemaChange.qualify(oldB, newB, range(52, observedThrough), order, position(61), verifiedBindings);
                }
            };
            var limits = new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000),
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), 1000, false,
                    disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null);
            return new As400ContinuousRuntime(checkpoint.toOffset(), restored, input, output.dispatcher, sink, policy.sourcePartition(), () -> {
            }, limits, output.schemas::refresh);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRefreshAndRestartWhenVerifiedExcludedLobRowsArriveDuringMetadataObservation(boolean disk) throws Exception {
        var f = new SchemaFixture(true);
        var saved = f.initial;
        for (int restart = 0; restart < 2; restart++) {
            var history = f.recover(saved);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(history.definitions(), false, ".*\\.B");
                    var runtime = f.runtime(saved, history, output, records::add, disk)) {
                var progress = runtime.poll(saved.toOffset());
                var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
                assertThat(data).hasSize(restart == 0 ? 1 : 0);
                saved = As400Checkpoint.fromOffset(progress.checkpoint(), f.journal, f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(62));
            }
        }
        assertThat(f.refreshes).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-binding", "captured", "member-jid", "minimized", "defaulted", "truncated" })
    void shouldRejectUnqualifiedInterleavedPointersBeforeAcceptingSchemaHistory(String fault) throws Exception {
        var f = new SchemaFixture(true);
        var records = new ArrayList<>(f.range(52, 62));
        var original = records.remove(records.size() - 1);
        byte[] raw = original.data();
        switch (fault) {
            case "member-jid" -> raw[206] ^= 1;
            case "minimized" -> raw[218] |= 0x08;
            case "defaulted" -> raw[original.header().getNullValueOffest() + 4] = (byte) 0xf9;
            case "truncated" -> raw = java.util.Arrays.copyOf(raw, raw.length - 1);
            default -> {
            }
        }
        records.add(new RawJournalPage.Entry(original.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        As400SourceNormalizer.TableResolver bindings = (file, library) -> {
            var binding = f.bindings().resolve(file, library);
            return fault.equals("captured") && file.equals("X")
                    ? new As400SourceNormalizer.TableBinding(binding.identity(), f.oldB.binding(65536).decoder())
                    : binding;
        };
        assertThatThrownBy(() -> {
            if (fault.equals("missing-binding")) {
                As400SchemaChange.qualify(f.oldB, f.newB, records, f.order, f.position(61));
            }
            else {
                As400SchemaChange.qualify(f.oldB, f.newB, records, f.order, f.position(61), bindings);
            }
        }).isInstanceOf(Exception.class);
        assertThat(f.recover(f.initial).definitions()).contains(f.oldB).doesNotContain(f.newB);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRefreshOneObservedDefinitionAndReplayItWithoutReacquiringTheCatalog(boolean disk) throws Exception {
        var f = new SchemaFixture();
        var saved = f.initial;
        for (int restart = 0; restart < 2; restart++) {
            var history = f.recover(saved);
            assertThat(history.definitions()).contains(f.oldB);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(history.definitions(), false);
                    var runtime = f.runtime(saved, history, output, records::add, disk)) {
                var progress = runtime.poll(saved.toOffset());
                var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
                if (restart == 0) {
                    assertThat(data).hasSize(1);
                    assertThat(((Struct) data.get(0).value()).getStruct("after").getFloat32("ID")).isEqualTo(101f);
                }
                else {
                    assertThat(data).isEmpty();
                }
                saved = As400Checkpoint.fromOffset(progress.checkpoint(), f.journal, f.initial.captureFingerprint());
                assertThat(saved.replayFrom()).isEqualTo(f.position(49));
                assertThat(saved.completedThrough()).isEqualTo(f.position(61));
                assertThat(saved.schemaState().floor()).isEqualTo(As400SchemaState.ORIGIN);
                assertThat(saved.schemaState().completed().ordinal()).isEqualTo(1);
                assertThat(progress.checkpoint()).containsEntry(As400Checkpoint.VERSION, "4");
            }
        }
        assertThat(f.refreshes).isEqualTo(1);
        var afterDdl = As400Checkpoint.afterVerifiedBoundary(f.journal, saved.captureFingerprint(), saved.completedThrough(), saved.completedReceiverIdentity())
                .withSchemaState(new As400SchemaState(saved.schemaState().completed(), saved.schemaState().completed()));
        assertThat(f.recover(afterDdl).definitions()).contains(f.newB).doesNotContain(f.oldB);
        var path = directory.resolve("schema-history.jsonl");
        var records = Files.readAllLines(path);
        Files.write(path, records.stream().filter(line -> !line.contains(As400SchemaHistory.VERSION)).toList());
        assertThatThrownBy(() -> f.recover(afterDdl)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schema history");
    }

    @Test
    void shouldReuseDurableSchemaHistoryAfterOutputDeliveryFails() throws Exception {
        var f = new SchemaFixture();
        var restored = f.recover(f.initial);
        try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                var runtime = f.runtime(f.initial, restored, output, record -> {
                    throw new InterruptedException("Interrupted after schema persistence");
                }, false)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
        }
        finally {
            Thread.interrupted();
        }
        var records = new ArrayList<SourceRecord>();
        var retry = f.recover(f.initial);
        try (var output = new As400OnlineCensusRecovery.Output(retry.definitions(), false);
                var runtime = f.runtime(f.initial, retry, output, records::add, false)) {
            var progress = runtime.poll(f.initial.toOffset());
            assertThat(progress.checkpoint()).containsEntry("completed_through.sequence", "61");
        }
        assertThat(records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null)).hasSize(1);
        assertThat(f.refreshes).isEqualTo(1);
        assertThat(Files.readAllLines(directory.resolve("schema-history.jsonl")).stream().filter(line -> line.contains(As400SchemaHistory.VERSION))).hasSize(1);
    }

    @Test
    void shouldRejectAnAmbiguousCurrentDefinitionOrAnUncommittedSchemaBoundary() throws Exception {
        var f = new SchemaFixture();
        assertThatThrownBy(() -> As400SchemaChange.qualify(f.oldB, f.newB, f.range(52, 58), f.order, f.position(58)))
                .isInstanceOf(As400SchemaTimeline.PendingChangeException.class);
        assertThatThrownBy(() -> As400SchemaChange.qualify(f.oldB, f.newB, f.range(52, 61), f.order, f.position(57)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("before the schema committed");
        assertThatThrownBy(() -> As400SchemaChange.qualify(f.oldB, f.oldB, f.range(52, 77), f.order, f.position(77)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Another table transition");
        var original = f.range(57, 57).get(0);
        byte[] changed = original.data();
        changed[original.header().getEntrySpecificDataOffset() + 18] ^= 1;
        assertThatThrownBy(() -> f.change.verifyRecord(new RawJournalPage.Entry(original.position(), original.header(), changed), f.order))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bytes or receiver identity changed");
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 32, 0, 33 })
    void shouldRecognizeReceiverPlacementWithoutAcceptingUnsupportedStorage(int asp) throws Exception {
        var f = new SchemaFixture();
        for (var mark : f.change.records()) {
            var entry = f.captured.stream().filter(value -> value.position().equals(mark.position())).findFirst().orElseThrow();
            byte[] original = entry.data();
            var input = java.nio.ByteBuffer.wrap(original);
            assertThat(input.getInt(20)).isZero();
            byte[] placed = new byte[original.length + 32];
            System.arraycopy(original, 0, placed, 0, 236);
            System.arraycopy(original, 236, placed, 268, original.length - 236);
            var receiver = new com.ibm.as400.access.AS400Text(10, RawJournalPage.METADATA_CCSID);
            receiver.toBytes(entry.position().receiver().name(), placed, 236);
            receiver.toBytes(entry.position().receiver().library(), placed, 246);
            receiver.toBytes("*SYSBAS", placed, 256);
            var output = java.nio.ByteBuffer.wrap(placed);
            output.putShort(266, (short) asp).putInt(20, 236);
            for (int field : new int[]{ 4, 8 }) {
                if (input.getInt(field) != 0) {
                    output.putInt(field, input.getInt(field) + 32);
                }
            }
            var moved = new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(placed, 0), placed);
            if (asp == 0 || asp == 33) {
                assertThatThrownBy(() -> f.change.verifyRecord(moved, f.order)).hasMessageContaining("receiver context");
            }
            else {
                f.change.verifyRecord(moved, f.order);
            }
        }
    }

    @Test
    void shouldRejectDiscardedSchemaDependenciesAndLegacyOffsetsAfterAnObservedChange() throws Exception {
        var f = new SchemaFixture();
        var history = f.recover(f.initial);
        var retained = history.schemaHistory().append(As400SchemaState.ORIGIN, f.change);
        var afterDdl = As400Checkpoint.afterVerifiedBoundary(f.journal, f.initial.captureFingerprint(), f.position(61), f.order.incarnation(f.position(61)));
        var restored = f.recover(afterDdl);
        assertThatThrownBy(() -> restored.verify(afterDdl, f.order)).isInstanceOf(IllegalArgumentException.class)
                .hasRootCauseMessage("Replay floor omits a required schema dependency");
        var forged = afterDdl.withSchemaState(new As400SchemaState(retained.cursor(), new As400SchemaState.Cursor(2, "a".repeat(64))));
        assertThatThrownBy(() -> f.recover(forged)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("schema history");
        assertThatThrownBy(() -> f.history().pendingOnlineCandidate(Map.of("offset.event_sequence", "61", "offset.receiver", f.position(61).receiver().name(),
                "offset.receiver_library", f.position(61).receiver().library(), "offset.processed", "true", "snapshot_completed", "true"), f.policy))
                .isInstanceOf(IllegalArgumentException.class).hasStackTraceContaining("legacy offset cannot replace it");
    }

    @Test
    void shouldPublishNothingWhenSchemaHistoryCannotBeReadOrAppended() throws Exception {
        var f = new SchemaFixture();
        var restored = f.recover(f.initial);
        var records = new ArrayList<SourceRecord>();
        var before = Files.readAllBytes(directory.resolve("schema-history.jsonl"));
        try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                var runtime = f.runtime(f.initial, restored, output, records::add, false);
                var channel = java.nio.channels.FileChannel.open(directory.resolve("schema-history.jsonl"), java.nio.file.StandardOpenOption.WRITE);
                var lock = channel.lock()) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(Exception.class);
            assertThat(records).isEmpty();
            assertThat(Files.readAllBytes(directory.resolve("schema-history.jsonl"))).isEqualTo(before);
        }
    }

    @Test
    void equalWidthAlterAndReversalPreserveJidsAndRestoreFormatButCannotValidateIntermediateRows() throws Exception {
        var fixture = fixture();
        var before = As400OnlineFenceEvidence.validateOffLock(fixture.path("before")).tables().definitions();
        var changed = As400OnlineFenceEvidence.validateOffLock(fixture.path("changed")).tables().definitions();
        var restored = As400OnlineFenceEvidence.validateOffLock(fixture.path("restored")).tables().definitions();
        assertThat(restored).isEqualTo(before);
        var oldB = before.stream().filter(d -> d.identity().physicalFile().equals("B")).findFirst().orElseThrow();
        var newB = changed.stream().filter(d -> d.identity().physicalFile().equals("B")).findFirst().orElseThrow();
        assertThat(newB.identity().fileJournalId()).isEqualTo(oldB.identity().fileJournalId());
        assertThat(newB.identity().memberJournalId()).isEqualTo(oldB.identity().memberJournalId());
        assertThat(newB.identity().recordFormat()).isNotEqualTo(oldB.identity().recordFormat());
        Entry row = entries(fixture).stream().filter(e -> e.position().offset().intValueExact() == 61).findFirst().orElseThrow();
        var header = new EntryHeaderDecoder().decode(row.bytes(), 0);
        // Deliberate diagnostic omission of maintenance, NEVER an admitted replay path.
        assertThat(newB.binding(1024).values(header, row.bytes(), 0)).containsExactly(101.0f);
        assertThat(oldB.binding(1024).values(header, row.bytes(), 0)).containsExactly(1120534528);
    }

    @Test
    void completeNativeTraceRejectsAlterBeforePublicationAndRetainsCheckpointAcrossFreshRestart() throws Exception {
        var fixture = fixture();
        var definitions = As400OnlineFenceEvidence.validateOffLock(fixture.path("before")).tables().definitions();
        var input = entries(fixture);
        var nativeWindow = As400OnlineNativeEvidence.restore(fixture.path("capture").path("native_identity_evidence"));
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        As400OperatorCutReader.chain(nativeWindow.before().directory(), input.get(0).position().receiver()).forEach(r -> {
            r.requireReadableProfile();
            ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last()));
        });
        var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), nativeWindow.receiverTokens(), ranges);
        var baseline = input.get(0).position();
        assertThat(baseline.offset().intValueExact()).isEqualTo(48);
        var checkpoint = As400Checkpoint.afterVerifiedBoundary(nativeWindow.journalToken(), "v1:" + "a".repeat(64), baseline, order.incarnation(baseline));
        Map<String, ?> offset = checkpoint.toOffset();
        for (int restart = 0; restart < 2; restart++) {
            var output = new ArrayList<SourceRecord>();
            var bindings = new LinkedHashMap<String, As400SourceNormalizer.TableBinding>();
            definitions.forEach(d -> bindings.put(d.identity().tableId().schema() + "/" + d.identity().physicalFile(), d.binding(1024)));
            try (var dispatcher = new As400OnlineCensusRecovery.Output(definitions, false);
                    var replay = new As400ReplayCoordinator(offset, nativeWindow.journalToken(), "v1:" + "a".repeat(64), order,
                            new As400TransactionBuffer.Limits(128, 40_000, 2048, 64 * 1024 * 1024), 1024, 20_000,
                            (file, library) -> bindings.get(library + "/" + file), As400SourceNormalizer.SavepointPolicy.consumingTargets(),
                            Map.of("server", "offline"), output::add, new As400JournalLifecycle.Limits(20_000, 64 * 1024 * 1024, 128, 2048, 1024, 4096), true)) {
                for (Entry entry : input) {
                    int sequence = entry.position().offset().intValueExact();
                    if (sequence <= 48) {
                        continue;
                    }
                    if (sequence == 52) {
                        assertThatThrownBy(() -> replay.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position()))
                                .isInstanceOf(As400ReplayCoordinator.RecoveryException.class).hasRootCauseMessage("Unsupported legacy change");
                        break;
                    }
                    if (replay.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position()).isPresent()) {
                        replay.deliver(dispatcher.dispatcher);
                    }
                }
                assertThat(output).isEmpty();
                assertThat(replay.checkpoint().toOffset()).isEqualTo(offset);
                var laterRow = input.get(input.size() - 1);
                assertThatThrownBy(() -> replay.accept(new EntryHeaderDecoder().decode(laterRow.bytes(), 0), laterRow.bytes(), 0, laterRow.position()))
                        .isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
                assertThat(output).isEmpty();
                assertThat(replay.checkpoint().toOffset()).isEqualTo(offset);
                offset = JSON.readValue(JSON.writeValueAsBytes(offset), new TypeReference<Map<String, Object>>() {
                });
            }
        }
    }
}
