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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Native pointer descriptors and controls, never synthetic LOB values or startup proof. */
class As400ExcludedLobTest {
    private static final JournalReceiver RECEIVER = new JournalReceiver("QSQJRN0002", "DCNC0004");
    private static final String FINGERPRINT = "v2:" + "a".repeat(64);
    private final As400JournalOrder order = new As400JournalOrder(List.of(RECEIVER), Map.of(RECEIVER, "fixture-receiver"),
            Map.of(RECEIVER, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(100))));
    private final As400Checkpoint initial = As400Checkpoint.afterVerifiedBoundary("fixture-journal", FINGERPRINT, position(51), "fixture-receiver");
    private final As400BindingDefinition captured;
    private final As400BindingDefinition excluded;
    private final List<RawJournalPage.Entry> entries = new ArrayList<>();
    @TempDir
    Path directory;

    As400ExcludedLobTest() throws Exception {
        try (var input = getClass().getResourceAsStream("/native-transactions/excluded-lob.json")) {
            var json = new ObjectMapper().readTree(input);
            assertThat(json.path("startup_proof").asBoolean()).isFalse();
            captured = As400BindingEncoding.decode(DocumentReader.defaultReader().read(json.toString()).getArray("tables")).get(0);
            var lob = json.path("excluded");
            excluded = As400BindingDefinition.excluded(new As400TransactionBuffer.TableIdentity(
                    new TableId(null, lob.path("library").asText(), "X"), "X", "X", lob.path("file_jid").asText(),
                    lob.path("member_jid").asText(), lob.path("format").asText()), true);
            for (var item : json.path("entries")) {
                entries.add(entry(Base64.getDecoder().decode(item.path("raw").asText())));
            }
        }
    }

    private static JournalPosition position(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), RECEIVER);
    }

    private static RawJournalPage.Entry entry(byte[] raw) {
        var header = new EntryHeaderDecoder(37).decode(raw, 0);
        return new RawJournalPage.Entry(new JournalPosition(header.getSequenceNumber(), RECEIVER), header, raw);
    }

    private As400ReplayCoordinator replay(As400Checkpoint saved, As400ReplayCoordinator.RecordSink sink,
                                          As400SpillStore spill, As400SourceNormalizer.TableBinding lob) {
        return new As400ReplayCoordinator(saved.toOffset(), "fixture-journal", FINGERPRINT, order,
                new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000), 65536, 1000,
                (file, library) -> file.equals("X") ? lob : captured.binding(65536),
                As400SourceNormalizer.SavepointPolicy.consumingTargets(), Map.of("server", "offline"), sink,
                new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), true, spill);
    }

    private void consume(As400ReplayCoordinator replay, As400Checkpoint saved, long through,
                         As400OnlineCensusRecovery.Output output)
            throws Exception {
        for (var item : entries) {
            if (item.position().offset().longValueExact() <= through
                    && order.compare(item.position(), saved.replayFrom()) >= (saved.replayInclusive() ? 0 : 1)) {
                if (replay.accept(item.header(), item.data(), 0, item.position()).isPresent()) {
                    replay.deliver(output.dispatcher);
                }
            }
        }
        replay.verifyOrdinaryCut();
    }

    private static List<SourceRecord> data(List<SourceRecord> records) {
        return records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRecoverMixedTransactionsWithoutPublishingExcludedLobRowsOrRollbackValues(boolean disk) throws Exception {
        var saved = initial;
        var records = new ArrayList<SourceRecord>();
        for (long through : new long[]{ 67, 89, 89 }) {
            records.clear();
            try (var spill = disk ? new As400SpillStore(new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024)) : null;
                    var output = new As400OnlineCensusRecovery.Output(List.of(captured, excluded), false, "DCNC0004.A");
                    var replay = replay(saved, records::add, spill, excluded.binding(65536))) {
                consume(replay, saved, through, output);
                if (through == 67) {
                    assertThat(data(records)).isEmpty();
                    assertThat(order.compare(replay.checkpoint().replayFrom(), position(65))).isLessThanOrEqualTo(0);
                    assertThat(order.compare(replay.checkpoint().completedThrough(), position(68))).isNegative();
                }
                else if (!saved.completedThrough().equals(position(89))) {
                    assertThat(data(records)).hasSize(1);
                    var row = data(records).get(0);
                    assertThat(((Struct) row.value()).getString("op")).isEqualTo("c");
                    assertThat(((Struct) row.value()).getStruct("after").getInt32("ID")).isEqualTo(700);
                    assertThat(((Struct) row.key()).getInt32("ID")).isEqualTo(700);
                }
                else {
                    assertThat(data(records)).isEmpty();
                }
                saved = replay.checkpoint();
            }
        }
        assertThat(saved.completedThrough()).isEqualTo(position(89));
    }

    @Test
    void shouldPreserveExcludedPointerAuthorityThroughContinuousSelectionAndFreshHistoryRecovery() throws Exception {
        var config = Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName())
                .with(FileSchemaHistory.FILE_PATH, directory.resolve("history.jsonl").toString()).build();
        var base = As400RecoveryTestSupport.Fixture.POLICY;
        var policy = new As400CaptureFingerprint.Policy(base.filters(), base.skippedOperations(), base.decoder(), base.schema(), base.output(),
                false, true, Map.of("server", "offline"));
        var history = new As400BindingHistory(As400BindingHistory.storage(config), config, policy.sourcePartition());
        var selected = history.recordOnlineCandidate("fixture-journal", position(51), "fixture-receiver", List.of(captured, excluded), policy,
                new As400BindingHistory.Discovery(position(51), "fixture-receiver"),
                Map.of("offset.event_sequence", "50", "offset.receiver", RECEIVER.name(), "offset.receiver_library", RECEIVER.library(),
                        "offset.processed", "true", "snapshot_completed", "true"));
        var saved = As400Checkpoint.afterVerifiedBoundary("fixture-journal", selected.fingerprint(), position(51), "fixture-receiver");
        for (long through : new long[]{ 67, 89 }) {
            var fresh = new As400BindingHistory(As400BindingHistory.storage(config), config, policy.sourcePartition())
                    .recoverOnlineCandidate(saved.toOffset(), "fixture-journal", policy);
            var records = new ArrayList<SourceRecord>();
            var input = new As400ContinuousRuntime.Intervals() {
                @Override
                public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) {
                    var checkpoint = As400Checkpoint.fromOffset(durable, "fixture-journal", selected.fingerprint());
                    return new As400OperatorCutReader.Cut(checkpoint, order, position(through), entries.stream()
                            .filter(row -> row.position().offset().longValueExact() <= through
                                    && order.compare(row.position(), checkpoint.replayFrom()) >= (checkpoint.replayInclusive() ? 0 : 1))
                            .toList(),
                            List.of(), Optional.of(fresh));
                }
            };
            try (var output = new As400OnlineCensusRecovery.Output(fresh.definitions(), false, "DCNC0004.A");
                    var runtime = new As400ContinuousRuntime(saved.toOffset(), fresh, input, output.dispatcher, records::add,
                            policy.sourcePartition(), () -> {
                            })) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", selected.fingerprint());
            }
            assertThat(data(records)).hasSize(through == 67 ? 0 : 1);
        }
        assertThat(saved.completedThrough()).isEqualTo(position(89));
    }

    @Test
    void shouldReplayCapturedOutputAfterInterruptedMixedTransactionDelivery() throws Exception {
        try (var output = new As400OnlineCensusRecovery.Output(List.of(captured, excluded), false, "DCNC0004.A");
                var replay = replay(initial, record -> {
                    throw new InterruptedException("delivery interrupted");
                }, null, excluded.binding(65536))) {
            assertThatThrownBy(() -> consume(replay, initial, 68, output)).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
            assertThat(order.compare(replay.checkpoint().completedThrough(), position(68))).isNegative();
        }
        finally {
            Thread.interrupted();
        }
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(List.of(captured, excluded), false, "DCNC0004.A");
                var replay = replay(initial, records::add, null, excluded.binding(65536))) {
            consume(replay, initial, 89, output);
        }
        assertThat(data(records)).hasSize(1);
        assertThat(((Struct) data(records).get(0).value()).getStruct("after").getInt32("ID")).isEqualTo(700);
    }

    @ParameterizedTest
    @ValueSource(strings = { "captured", "ordinary-excluded", "member-jid", "minimized", "defaulted", "truncated", "format" })
    void shouldRejectUnqualifiedPointerRowsBeforeOutputOrCheckpointAdvancement(String fault) throws Exception {
        byte[] raw = entries.get(0).data();
        var binding = excluded.binding(65536);
        switch (fault) {
            case "captured" -> binding = new As400SourceNormalizer.TableBinding(excluded.identity(), captured.binding(65536).decoder());
            case "ordinary-excluded" -> binding = As400BindingDefinition.excluded(excluded.identity()).binding(65536);
            case "member-jid" -> raw[206] ^= 1;
            case "minimized" -> raw[218] |= 0x08;
            case "defaulted" -> raw[entries.get(0).header().getNullValueOffest() + 4] = (byte) 0xf9;
            case "truncated" -> raw = java.util.Arrays.copyOf(raw, raw.length - 1);
            case "format" -> raw[entries.get(0).header().getEntrySpecificDataOffset() + 4] = (byte) 0xf7;
            default -> throw new AssertionError(fault);
        }
        var changed = entry(raw);
        var records = new ArrayList<SourceRecord>();
        try (var replay = replay(initial, records::add, null, binding)) {
            assertThatThrownBy(() -> replay.accept(changed.header(), changed.data(), 0, changed.position()))
                    .isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
            assertThat(replay.checkpoint()).isEqualTo(initial);
            assertThat(records).isEmpty();
        }
    }

    @Test
    void shouldCompareStableExcludedMetadataWithoutClaimingExternalLobValueIntegrity() throws Exception {
        var original = entries.get(0);
        byte[] changed = original.data();
        ByteBuffer.wrap(changed).putInt(72, 987);
        changed[original.header().getEntrySpecificDataOffset() + 16 + 47] = 123;
        assertThat(As400JournalRecordContent.digest(changed, original.position()))
                .isEqualTo(As400JournalRecordContent.digest(original.data(), original.position()));
        changed[original.header().getNullValueOffest() + 5] = (byte) 0xf1;
        assertThat(As400JournalRecordContent.digest(changed, original.position()))
                .isNotEqualTo(As400JournalRecordContent.digest(original.data(), original.position()));
    }
}
