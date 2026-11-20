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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.storage.file.history.FileSchemaHistory;

class As400FileDeletionTest {
    @TempDir
    Path directory;
    private final JournalReceiver receiver = new JournalReceiver("QSQJRN0002", "DCNC0004");
    private final As400JournalOrder order = new As400JournalOrder(List.of(receiver), Map.of(receiver, "receiver"),
            Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(100))));
    private final List<RawJournalPage.Entry> entries = new ArrayList<>();
    private final List<RawJournalPage.Entry> metadata = new ArrayList<>();
    private final List<RawJournalPage.Entry> rollbackRows = new ArrayList<>();
    private List<As400BindingDefinition> definitions;
    private As400BindingDefinition oldTable;
    private As400BindingDefinition newTable;
    private As400Checkpoint initial;
    private Configuration config;
    private final As400CaptureFingerprint.Policy policy = new As400CaptureFingerprint.Policy(Map.of(), java.util.Set.of(), Map.of("version", "strict-row-v1"),
            Map.of("version", "fixed-v1"), Map.of("version", "ordinary-v1"), false, true, Map.of("server", "offline"));

    @BeforeEach
    void loadNativeDeletionAndRecreation() throws Exception {
        initialize("file-deletion.json", 48, "history.jsonl");
    }

    private void initialize(String resource, long boundary, String historyFile) throws Exception {
        entries.clear();
        metadata.clear();
        rollbackRows.clear();
        var json = new ObjectMapper().readTree(getClass().getResourceAsStream("/native-transactions/" + resource));
        definitions = As400BindingEncoding.decode(DocumentReader.defaultReader().read(json.path("before").toString()).getArray("tables"));
        oldTable = definitions.stream().filter(d -> d.identity().physicalFile().equals("U")).findFirst().orElseThrow();
        newTable = As400BindingEncoding.decode(DocumentReader.defaultReader().read(json.path("after").toString()).getArray("tables"))
                .stream().filter(d -> d.identity().physicalFile().equals("U")).findFirst().orElseThrow();
        for (var item : json.path("entries")) {
            entries.add(captured(item));
        }
        for (var item : json.path("metadata_entries")) {
            metadata.add(captured(item));
        }
        for (var item : json.path("rollback_rows")) {
            rollbackRows.add(captured(item));
        }
        config = Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName())
                .with(FileSchemaHistory.FILE_PATH, directory.resolve(historyFile).toString()).build();
        var selected = history().recordOnlineCandidate("journal", position(boundary), "receiver", definitions, policy,
                new As400BindingHistory.Discovery(position(boundary), "receiver"),
                Map.of("offset.event_sequence", Long.toString(boundary - 1), "offset.receiver", receiver.name(), "offset.receiver_library", receiver.library(),
                        "offset.processed", "true", "snapshot_completed", "true"));
        initial = As400Checkpoint.afterVerifiedBoundary("journal", selected.fingerprint(), position(boundary), "receiver");
    }

    private RawJournalPage.Entry captured(com.fasterxml.jackson.databind.JsonNode item) throws Exception {
        byte[] raw = Base64.getDecoder().decode(item.path("raw").asText());
        assertThat(java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(raw)))
                .isEqualTo(item.path("sha256").asText());
        return entry(raw);
    }

    private As400BindingHistory history() {
        return new As400BindingHistory(As400BindingHistory.storage(config), config, policy.sourcePartition());
    }

    private As400BindingHistory.Recovered recover(As400Checkpoint saved) throws Exception {
        return history().recoverOnlineCandidate(saved.toOffset(), "journal", policy);
    }

    private JournalPosition position(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), receiver);
    }

    private RawJournalPage.Entry entry(byte[] raw) {
        var header = new EntryHeaderDecoder(37).decode(raw, 0);
        return new RawJournalPage.Entry(new JournalPosition(header.getSequenceNumber(), receiver), header, raw);
    }

    private List<RawJournalPage.Entry> from(long sequence) {
        return entries.stream().filter(e -> e.position().offset().longValueExact() >= sequence).toList();
    }

    private As400SchemaTimeline timeline(As400Checkpoint saved) throws Exception {
        var retained = recover(saved);
        return new As400SchemaTimeline(retained.schemaHistory(), retained.definitions(), saved, order, (before, opening, bindings) -> {
            var interval = from(opening.position().offset().longValueExact());
            return opening.header().getEntryType().equals("DT") ? As400FileDeletion.qualify(before, interval, order)
                    : As400SchemaChange.qualify(null, newTable, interval, order, position(63));
        }, ignored -> {
        }, null);
    }

    private As400Checkpoint completed(As400SchemaTimeline timeline, long through) {
        return timeline.schemaCheckpoint(As400Checkpoint.afterVerifiedBoundary("journal", initial.captureFingerprint(), position(through), "receiver"));
    }

    private RawJournalPage.Entry at(RawJournalPage.Entry original, long sequence, long cycle, RawJournalPage.Entry producer) {
        byte[] raw = original.data();
        ByteBuffer.wrap(raw).putLong(24, sequence).putLong(64, cycle);
        if (producer != null) {
            byte[] source = producer.data();
            System.arraycopy(source, 40, raw, 40, 8);
            System.arraycopy(source, 101, raw, 101, 26);
            System.arraycopy(source, 208, raw, 208, 8);
        }
        return entry(raw);
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldPublishUnrelatedRowsAndRecoverAnOpenDropThroughItsOutcome(boolean disk, boolean rollback) throws Exception {
        initialize("transactional-file-deletion.json", 44, "open-drop.jsonl");
        var source = new java.util.HashMap<Long, RawJournalPage.Entry>();
        entries.forEach(e -> source.put(e.position().offset().longValueExact(), e));
        var opening = source.get(rollback ? 47L : 50L);
        var dropped = definitions.stream().filter(d -> d.identity().physicalFile().equals(opening.header().getFile())).findFirst().orElseThrow();
        byte[] beforeDrop = at(rollbackRows.get(0), 49, 48, opening).data();
        var name = new com.ibm.as400.access.AS400Text(10, 37);
        name.toBytes(dropped.identity().physicalFile(), beforeDrop, 157);
        name.toBytes(dropped.identity().member(), beforeDrop, 177);
        System.arraycopy(java.util.HexFormat.of().parseHex(dropped.identity().memberJournalId()), 0, beforeDrop, 197, 10);
        entries.clear();
        entries.add(at(source.get(49L), 48, 48, opening));
        entries.add(entry(beforeDrop));
        entries.add(at(opening, 50, 48, null));
        entries.add(at(source.get(57L), 51, 0, null));
        entries.add(at(source.get(57L), 52, 48, opening));
        if (rollback) {
            byte[] undo = at(rollbackRows.get(1), 58, 48, opening).data();
            System.arraycopy(beforeDrop, 157, undo, 157, 51);
            entries.add(entry(undo));
        }
        entries.add(at(source.get(rollback ? 48L : 51L), 60, 48, opening));
        if (!rollback) {
            entries.add(at(source.get(53L), 62, 0, null));
            entries.add(at(source.get(55L), 64, 0, null));
        }
        entries.add(at(source.get(57L), 70, 0, null));
        var saved = initial;
        int[] acquisitions = { 0 };
        for (int restart = 0; restart < 4; restart++) {
            long through = restart < 2 ? 52 : 70;
            var recovered = recover(saved);
            var records = new ArrayList<SourceRecord>();
            var intervals = new As400ContinuousRuntime.Intervals() {
                @Override
                public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) {
                    var checkpoint = As400Checkpoint.fromOffset(durable, "journal", initial.captureFingerprint());
                    var selected = entries.stream().filter(e -> e.position().offset().longValueExact() <= through)
                            .filter(e -> scan.isPresent() ? order.compare(e.position(), scan.orElseThrow().position()) > 0
                                    : order.compare(e.position(), checkpoint.replayFrom()) >= (checkpoint.replayInclusive() ? 0 : 1))
                            .toList();
                    return new As400OperatorCutReader.Cut(checkpoint, order, position(through), selected, List.of(), Optional.of(recovered));
                }

                @Override
                public As400SchemaChange schemaChange(As400BindingDefinition before, RawJournalPage.Entry first,
                                                      As400SourceNormalizer.TableResolver bindings)
                        throws Exception {
                    acquisitions[0]++;
                    return As400FileDeletion.qualify(before, from(first.position().offset().longValueExact()).stream()
                            .filter(e -> e.position().offset().longValueExact() <= through).toList(), order);
                }
            };
            var limits = new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000),
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), 1000, false,
                    disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null);
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = new As400ContinuousRuntime(saved.toOffset(), recovered, intervals, output.dispatcher, records::add,
                            policy.sourcePartition(), () -> {
                            }, limits, output.schemas::refresh)) {
                var progress = runtime.poll(saved.toOffset());
                assertThat(progress.deferred()).isFalse();
                saved = As400Checkpoint.fromOffset(progress.checkpoint(), "journal", initial.captureFingerprint());
                if (restart < 2) {
                    assertThat(acquisitions[0]).isZero();
                    assertThat(progress.scan()).contains(position(52));
                    assertThat(saved.replayFrom()).isEqualTo(position(48));
                    assertThat(saved.completedThrough()).isEqualTo(position(51));
                    assertThat(saved.schemaState()).isEqualTo(As400SchemaState.EMPTY);
                    assertThat(runtime.bufferUsage()[1]).isPositive();
                }
                else {
                    assertThat(saved.completedThrough()).isEqualTo(position(70));
                    assertThat(runtime.bufferUsage()[1]).isZero();
                }
            }
            assertThat(As400RecoveryTestSupport.data(records)).hasSize(restart == 0 ? 1 : restart == 2 ? rollback ? 1 : 3 : 0);
            assertThat(records.stream()
                    .filter(record -> record.topic().endsWith("." + dropped.identity().physicalFile())))
                    .hasSize(restart == 2 && !rollback ? 1 : 0);
            if (restart >= 2) {
                var history = recover(saved);
                assertThat(history.definitions().contains(dropped)).isEqualTo(rollback);
                var change = history.schemaHistory().find(position(50)).change();
                assertThat(change.deferred()).isTrue();
                assertThat(change.visibility()).isEqualTo(position(60));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid", "foreign-member", "foreign-cycle", "foreign-producer", "forward-row", "commit" })
    void shouldRequireMatchingFullRollbackForRowsUndoneAfterDrop(String scenario) throws Exception {
        initialize("transactional-file-deletion.json", 44, "rollback-row-validation.jsonl");
        var opening = entries.stream().filter(e -> e.position().equals(position(50))).findFirst().orElseThrow();
        var terminal = entries.stream().filter(e -> e.position().equals(position(48))).findFirst().orElseThrow();
        byte[] undo = at(rollbackRows.get(1), 58, 49, opening).data();
        if (scenario.equals("foreign-member")) {
            undo[197] ^= 1;
        }
        if (scenario.equals("foreign-cycle")) {
            ByteBuffer.wrap(undo).putLong(64, 48);
        }
        if (scenario.equals("foreign-producer")) {
            undo[126] ^= 1;
        }
        if (scenario.equals("forward-row")) {
            new com.ibm.as400.access.AS400Text(2, 37).toBytes("PT", undo, 99);
        }
        byte[] completion = at(terminal, 60, 49, opening).data();
        if (scenario.equals("commit")) {
            new com.ibm.as400.access.AS400Text(2, 37).toBytes("CM", completion, 99);
        }
        var interval = List.of(opening, entry(undo), entry(completion));
        if (scenario.equals("valid")) {
            var change = As400FileDeletion.qualify(oldTable, interval, order);
            assertThat(change.after()).isEqualTo(oldTable);
            assertThat(change.records()).hasSize(2); // Compensation rows do not inflate schema history.
        }
        else {
            assertThatThrownBy(() -> As400FileDeletion.qualify(oldTable, interval, order)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldRetireTheExactFileDurablyAndAdmitARecreatedNameWithNewIdentities() throws Exception {
        var timeline = timeline(initial);
        for (var e : from(49)) {
            if (e.position().offset().longValueExact() > 51) {
                break;
            }
            timeline.before(e);
            assertThat(timeline.creationLifecycle(e)).isTrue();
        }
        var interrupted = completed(timeline, 52);
        assertThat(interrupted.replayFrom()).isEqualTo(position(49));
        assertThat(interrupted.replayInclusive()).isTrue();
        assertThat(recover(interrupted).definitions()).contains(oldTable);
        timeline = timeline(interrupted); // New instance, recovered from serialized file history.
        for (var e : from(49)) {
            if (e.position().offset().longValueExact() > 55) {
                break;
            }
            timeline.before(e);
        }
        var retired = completed(timeline, 55);
        assertThat(retired.schemaState().completed().ordinal()).isEqualTo(1);
        assertThat(recover(retired).definitions()).doesNotContain(oldTable).hasSize(definitions.size() - 1);
        var restarted = timeline(retired);
        assertThatThrownBy(() -> restarted.resolve("U", receiver.library())).hasMessageContaining("Missing recovered schema binding");
        for (var e : from(58)) {
            restarted.before(e);
        }
        var recreated = completed(restarted, 63);
        assertThat(recover(recreated).definitions()).contains(newTable).doesNotContain(oldTable);
        assertThat(newTable.identity().fileJournalId()).isNotEqualTo(oldTable.identity().fileJournalId());
        assertThat(timeline(recreated).resolve("U", receiver.library()).identity()).isEqualTo(newTable.identity());
    }

    @ParameterizedTest
    @ValueSource(strings = { "partial", "missing-member", "member-jid", "file-jid", "producer", "payload", "commit-cycle", "reordered" })
    void shouldRejectIncompleteOrMismatchedDeletionWithoutRetiringTheBinding(String fault) throws Exception {
        var interval = new ArrayList<>(from(49).stream().filter(e -> e.position().offset().longValueExact() <= 53).toList());
        switch (fault) {
            case "partial" -> interval.remove(interval.size() - 1);
            case "missing-member" -> interval.remove(1);
            case "reordered" -> java.util.Collections.swap(interval, 1, 2);
            default -> {
                int at = fault.equals("member-jid") ? 1 : 0;
                byte[] raw = interval.get(at).data();
                switch (fault) {
                    case "file-jid", "member-jid" -> raw[206] ^= 1;
                    case "producer" -> raw[121] ^= 1;
                    case "commit-cycle" -> ByteBuffer.wrap(raw).putLong(64, 1);
                    case "payload" -> raw[ByteBuffer.wrap(raw).getInt(8) + 16] ^= 1;
                    default -> throw new AssertionError(fault);
                }
                interval.set(at, entry(raw));
            }
        }
        assertThatThrownBy(() -> As400FileDeletion.qualify(oldTable, interval, order)).isInstanceOf(RuntimeException.class);
        assertThat(recover(initial).definitions()).contains(oldTable);
    }

    @ParameterizedTest
    @ValueSource(strings = { "D/GT", "D/RV", "D/AC", "D/DC", "D/TC", "D/TD" })
    void shouldAcceptNativeObjectMetadataWithExactIdentity(String type) throws Exception {
        var e = java.util.stream.Stream.concat(entries.stream(), metadata.stream())
                .filter(raw -> (raw.header().getJournalCode() + "/" + raw.header().getEntryType()).equals(type)).findFirst().orElseThrow();
        var binding = oldTable.binding(65536);
        var excluded = new As400SourceNormalizer.TableBinding(binding.identity(), null, false, true, binding.members());
        assertThat(As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(e.header(), e.data(), e.position()), (file, library) -> binding)).isTrue();
        assertThat(As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(e.header(), e.data(), e.position()), (file, library) -> excluded)).isTrue();
        byte[] raw = e.data();
        raw[206] ^= 1;
        var changed = entry(raw);
        assertThatThrownBy(() -> As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(changed.header(), raw, changed.position()),
                (file, library) -> binding)).hasMessageContaining("identity mismatch");
        assertThatThrownBy(() -> As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(changed.header(), raw, changed.position()),
                (file, library) -> excluded)).hasMessageContaining("identity mismatch");
    }

    @Test
    void shouldRetainARolledBackDropAndPassBothTerminalsToTransactionRecovery() throws Exception {
        initialize("transactional-file-deletion.json", 44, "transactional-history.jsonl");
        var timeline = timeline(initial);
        var controls = new ArrayList<String>();
        try (var lifecycle = new As400JournalLifecycle(new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), timeline, order, initial, true)) {
            for (var e : from(45)) {
                timeline.before(e);
                lifecycle.accept(e.header(), e.data(), 0, e.position());
                java.util.Optional<As400JournalLifecycle.Entry> ready;
                while ((ready = lifecycle.poll()).isPresent()) {
                    if (ready.orElseThrow().header().getJournalCode() == 'C') {
                        controls.add(ready.orElseThrow().type());
                    }
                }
                if (e.position().equals(position(50))) {
                    // The future deletion is retained, but commit must still be
                    // able to resolve older buffered rows for this file.
                    assertThat(timeline.resolve("U", receiver.library()).identity()).isEqualTo(oldTable.identity());
                }
            }
            lifecycle.verifyTail();
        }
        assertThat(controls).containsExactly("C/BC", "C/SC", "C/RB", "C/SC", "C/CM");
        var saved = completed(timeline, 57);
        var recovered = recover(saved).definitions();
        assertThat(recovered).doesNotContain(oldTable).contains(definitions.stream().filter(d -> d.identity().physicalFile().equals("B")).findFirst().orElseThrow());
        assertThat(timeline(saved).resolve("B", receiver.library()).identity().physicalFile()).isEqualTo("B");
        assertThatThrownBy(() -> timeline(saved).resolve("U", receiver.library())).hasMessageContaining("Missing recovered schema binding");
    }
}
