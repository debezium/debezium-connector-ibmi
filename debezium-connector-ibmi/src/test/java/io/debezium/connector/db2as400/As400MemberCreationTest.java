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
import java.nio.file.Files;
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

/** Captured member association and rows; synthetic history is not native startup proof. */
class As400MemberCreationTest {
    @TempDir
    Path directory;

    private final class Fixture {
        final JournalReceiver receiver = new JournalReceiver("QSQJRN0002", "DCNC0002");
        final As400JournalOrder order = new As400JournalOrder(List.of(receiver), Map.of(receiver, "fixture-receiver"),
                Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(200))));
        final String journal = "fixture-journal";
        final As400CaptureFingerprint.Policy policy;
        final Configuration config = Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName())
                .with(FileSchemaHistory.FILE_PATH, directory.resolve("history.jsonl").toString()).build();
        final As400BindingDefinition before;
        final As400BindingDefinition after;
        final List<RawJournalPage.Entry> entries = new ArrayList<>();
        final As400Checkpoint initial;
        int refreshes;

        Fixture() throws Exception {
            var original = As400RecoveryTestSupport.Fixture.POLICY;
            policy = new As400CaptureFingerprint.Policy(original.filters(), original.skippedOperations(), original.decoder(), original.schema(),
                    original.output(), false, true, Map.of("server", "offline"));
            try (var input = getClass().getResourceAsStream("/native-transactions/new-member.json")) {
                var json = new ObjectMapper().readTree(input);
                assertThat(json.path("startup_proof").asBoolean()).isFalse();
                before = As400BindingEncoding.decode(DocumentReader.defaultReader().read(json.path("before").toString()).getArray("tables")).get(0);
                after = As400BindingEncoding.decode(DocumentReader.defaultReader().read(json.path("after").toString()).getArray("tables")).get(0);
                for (var item : json.path("entries")) {
                    byte[] raw = Base64.getDecoder().decode(item.path("raw").asText());
                    var sequence = ByteBuffer.wrap(raw);
                    sequence.putLong(24, sequence.getLong(24) * 2);
                    entries.add(entry(raw));
                }
            }
            // An existing member emits a row after F/JM but before D/MA/F/MC.
            // Only its placement and identity are controlled; the native row
            // layout and creation sequence remain those of the captured fixture.
            byte[] interleaved = entries.get(entries.size() - 1).data();
            ByteBuffer.wrap(interleaved).putLong(24, 109);
            new com.ibm.as400.access.AS400Text(10, 37).toBytes("K", interleaved, 177);
            System.arraycopy(java.util.HexFormat.of().parseHex(before.identity().memberJournalId()), 0, interleaved, 197, 10);
            ByteBuffer.wrap(interleaved).putInt(entries.get(entries.size() - 1).header().getEntrySpecificDataOffset() + 16, 699);
            entries.add(1, entry(interleaved));
            var boundary = position(106);
            var selected = history().recordOnlineCandidate(journal, boundary, order.incarnation(boundary), List.of(before), policy,
                    new As400BindingHistory.Discovery(boundary, order.incarnation(boundary)),
                    Map.of("offset.event_sequence", "105", "offset.receiver", receiver.name(), "offset.receiver_library", receiver.library(),
                            "offset.processed", "true", "snapshot_completed", "true"));
            initial = As400Checkpoint.afterVerifiedBoundary(journal, selected.fingerprint(), boundary, order.incarnation(boundary));
        }

        JournalPosition position(long sequence) {
            return new JournalPosition(BigInteger.valueOf(sequence), receiver);
        }

        RawJournalPage.Entry entry(byte[] raw) {
            var header = new EntryHeaderDecoder(37).decode(raw, 0);
            return new RawJournalPage.Entry(new JournalPosition(header.getSequenceNumber(), receiver), header, raw);
        }

        As400BindingHistory history() {
            return new As400BindingHistory(As400BindingHistory.storage(config), config, policy.sourcePartition());
        }

        As400BindingHistory.Recovered recover(As400Checkpoint saved) throws Exception {
            return history().recoverOnlineCandidate(saved.toOffset(), journal, policy);
        }

        As400ContinuousRuntime runtime(As400Checkpoint saved, As400BindingHistory.Recovered restored,
                                       As400OnlineCensusRecovery.Output output, As400ReplayCoordinator.RecordSink sink, boolean disk, long through) {
            var input = new As400ContinuousRuntime.Intervals() {
                @Override
                public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) {
                    var checkpoint = As400Checkpoint.fromOffset(durable, journal, initial.captureFingerprint());
                    var rows = entries.stream().filter(entry -> entry.position().offset().longValueExact() <= through)
                            .filter(entry -> scan.isPresent() ? order.compare(entry.position(), scan.orElseThrow().position()) > 0
                                    : order.compare(entry.position(), checkpoint.replayFrom()) >= (checkpoint.replayInclusive() ? 0 : 1))
                            .toList();
                    return new As400OperatorCutReader.Cut(checkpoint, order, position(through), rows, List.of(), Optional.of(restored));
                }

                @Override
                public As400SchemaChange schemaChange(As400BindingDefinition old, RawJournalPage.Entry opening, As400SourceNormalizer.TableResolver verifiedBindings)
                        throws Exception {
                    refreshes++;
                    assertThat(old).isEqualTo(before);
                    assertThat(opening.position()).isEqualTo(position(108));
                    return As400MemberCreation.qualify(old, entries, order);
                }
            };
            var limits = new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000),
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), 1000, false,
                    disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null);
            return new As400ContinuousRuntime(saved.toOffset(), restored, input, output.dispatcher, sink, policy.sourcePartition(), () -> {
            }, limits, output.schemas::refresh);
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldRecoverAnInterruptedMemberAssociationBeforePublishingRows(boolean disk, boolean remoteEligible) throws Exception {
        var f = new Fixture();
        if (remoteEligible) {
            byte[] raw = f.entries.get(0).data();
            raw[f.entries.get(0).header().getEntrySpecificDataOffset() + 16 + 2] = (byte) 0xf1;
            f.entries.set(0, f.entry(raw));
        }
        var saved = f.initial;
        var restored = f.recover(saved);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                var runtime = f.runtime(saved, restored, output, records::add, disk, 110)) {
            saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), f.journal, f.initial.captureFingerprint());
        }
        assertThat(data(records)).hasSize(1);
        assertThat(((Struct) data(records).get(0).value()).getStruct("after").getInt32("ID")).isEqualTo(699);
        assertThat(saved.replayFrom()).isEqualTo(f.position(108));
        assertThat(saved.replayInclusive()).isTrue();
        assertThat(saved.completedThrough()).isEqualTo(f.position(109));
        assertThat(saved.schemaState().floor()).isEqualTo(As400SchemaState.ORIGIN);
        assertThat(saved.schemaState().completed().ordinal()).isEqualTo(1);
        assertThat(f.recover(saved).definitions()).containsExactly(f.before);
        for (int restart = 0; restart < 2; restart++) {
            records.clear();
            restored = f.recover(saved);
            try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                    var runtime = f.runtime(saved, restored, output, records::add, disk, 116)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), f.journal, f.initial.captureFingerprint());
            }
            if (restart == 0) {
                assertThat(data(records)).hasSize(1);
                var row = data(records).get(0);
                assertThat(((Struct) row.key()).getInt32("ID")).isEqualTo(700);
                assertThat(((Struct) row.value()).getStruct("after").getInt32("ID")).isEqualTo(700);
            }
            else {
                assertThat(data(records)).isEmpty();
            }
        }
        assertThat(saved.schemaState().floor().ordinal()).isEqualTo(1);
        assertThat(f.recover(saved).definitions()).containsExactly(f.after);
        assertThat(f.refreshes).isEqualTo(1);
    }

    @Test
    void shouldReusePersistedMemberHistoryWhenRowDeliveryIsInterrupted() throws Exception {
        var f = new Fixture();
        var restored = f.recover(f.initial);
        try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                var runtime = f.runtime(f.initial, restored, output, record -> {
                    throw new InterruptedException("delivery interrupted");
                }, false, 116)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
        }
        finally {
            Thread.interrupted();
        }
        var records = new ArrayList<SourceRecord>();
        var retry = f.recover(f.initial);
        try (var output = new As400OnlineCensusRecovery.Output(retry.definitions(), false);
                var runtime = f.runtime(f.initial, retry, output, records::add, false, 116)) {
            runtime.poll(f.initial.toOffset());
        }
        assertThat(data(records)).hasSize(2);
        assertThat(f.refreshes).isEqualTo(1);
        assertThat(Files.readAllLines(directory.resolve("history.jsonl")).stream().filter(line -> line.contains(As400SchemaHistory.VERSION))).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "partial", "missing", "reordered", "file-id", "member-id", "producer", "inherited-journaling", "association" })
    void shouldRejectIncompleteOrMismatchedNativeMemberEvidence(String fault) throws Exception {
        var f = new Fixture();
        var entries = new ArrayList<>(f.entries);
        switch (fault) {
            case "partial" -> entries.subList(3, entries.size()).clear();
            case "missing" -> entries.remove(2);
            case "reordered" -> java.util.Collections.swap(entries, 2, 3);
            default -> {
                int index = fault.equals("member-id") ? 3 : fault.equals("inherited-journaling") ? 0 : 2;
                var entry = entries.get(index);
                byte[] raw = entry.data();
                if (fault.equals("producer")) {
                    ByteBuffer.wrap(raw).putLong(40, 999);
                }
                else {
                    int at = switch (fault) {
                        case "association" -> entry.header().getEntrySpecificDataOffset() + 16 + 9;
                        case "inherited-journaling" -> entry.header().getEntrySpecificDataOffset() + 16 + 1;
                        default -> 206;
                    };
                    raw[at] ^= 1;
                }
                entries.set(index, f.entry(raw));
            }
        }
        assertThatThrownBy(() -> As400MemberCreation.qualify(f.before, entries, f.order)).isInstanceOf(RuntimeException.class);
        assertThat(f.recover(f.initial).definitions()).containsExactly(f.before);
        assertThat(Files.readString(directory.resolve("history.jsonl"))).doesNotContain(As400SchemaHistory.VERSION);
    }

    private static List<SourceRecord> data(List<SourceRecord> records) {
        return records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
    }
}
