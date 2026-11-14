/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
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
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.document.DocumentReader;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Captured creation metadata with controlled interleaving; never substitutes for native startup proof. */
class As400FileCreationTest {
    @TempDir
    Path directory;

    private final class Fixture {
        final JournalReceiver receiver = new JournalReceiver("QSQJRN0002", "DCNC0001");
        final As400JournalOrder order = new As400JournalOrder(List.of(receiver), Map.of(receiver, "fixture-receiver"),
                Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(200))));
        final String journal = "fixture-journal";
        final As400CaptureFingerprint.Policy policy;
        final Configuration config = Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName())
                .with(FileSchemaHistory.FILE_PATH, directory.resolve("history.jsonl").toString()).build();
        final List<As400BindingDefinition> before;
        final As400BindingDefinition created;
        final List<RawJournalPage.Entry> entries = new ArrayList<>();
        final As400SchemaChange change;
        final As400Checkpoint initial;
        int refreshes;

        Fixture() throws Exception {
            var original = As400RecoveryTestSupport.Fixture.POLICY;
            policy = new As400CaptureFingerprint.Policy(original.filters(), original.skippedOperations(), original.decoder(), original.schema(),
                    original.output(), false, true, Map.of("server", "offline"));
            try (var input = getClass().getResourceAsStream("/native-transactions/new-file.json")) {
                var json = new ObjectMapper().readTree(input);
                assertThat(json.path("startup_proof").asBoolean()).isFalse();
                before = As400BindingEncoding.decode(DocumentReader.defaultReader().read(json.path("before").toString()).getArray("tables"));
                created = As400BindingEncoding.decode(DocumentReader.defaultReader().read(json.path("after").toString()).getArray("tables")).get(0);
                byte[] unrelated = Base64.getDecoder().decode(json.path("entries").get(0).path("raw").asText());
                for (var item : json.path("entries")) {
                    long sequence = item.path("position").path("sequence").asLong();
                    if (sequence == 44) {
                        continue;
                    }
                    entries.add(entry(Base64.getDecoder().decode(item.path("raw").asText()), sequence * 2));
                    if (sequence == 48) {
                        entries.add(entry(unrelated, 97));
                    }
                }
            }
            var boundary = position(88);
            var selected = history().recordOnlineCandidate(journal, boundary, order.incarnation(boundary), before, policy,
                    new As400BindingHistory.Discovery(boundary, order.incarnation(boundary)),
                    Map.of("offset.event_sequence", "87", "offset.receiver", receiver.name(), "offset.receiver_library", receiver.library(),
                            "offset.processed", "true", "snapshot_completed", "true"));
            initial = As400Checkpoint.afterVerifiedBoundary(journal, selected.fingerprint(), boundary, order.incarnation(boundary));
            change = As400SchemaChange.qualify(null, created, entries, order, position(106));
        }

        JournalPosition position(long sequence) {
            return new JournalPosition(BigInteger.valueOf(sequence), receiver);
        }

        RawJournalPage.Entry entry(byte[] raw, long sequence) {
            ByteBuffer.wrap(raw).putLong(24, sequence);
            return new RawJournalPage.Entry(position(sequence), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        }

        As400BindingHistory history() {
            return new As400BindingHistory(As400BindingHistory.storage(config), config, policy.sourcePartition());
        }

        As400BindingHistory.Recovered recover(As400Checkpoint saved) throws Exception {
            return history().recoverOnlineCandidate(saved.toOffset(), journal, policy);
        }

        As400ContinuousRuntime runtime(As400Checkpoint saved, As400BindingHistory.Recovered restored,
                                       As400OnlineCensusRecovery.Output output, List<SourceRecord> records, boolean disk, long through) {
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
                public As400SchemaChange schemaChange(As400BindingDefinition old, RawJournalPage.Entry opening, As400SourceNormalizer.TableResolver verifiedBindings) {
                    refreshes++;
                    assertThat(old).isNull();
                    assertThat(opening.position()).isEqualTo(position(94));
                    return change;
                }
            };
            var limits = new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000),
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), 1000, false,
                    disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null);
            return new As400ContinuousRuntime(saved.toOffset(), restored, input, output.dispatcher, records::add, policy.sourcePartition(), () -> {
            }, limits, output.schemas::refresh);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDiscoverANewTableAndRetainItsCreationFloorWhileUnrelatedOutputAdvances(boolean disk) throws Exception {
        var f = new Fixture();
        var saved = f.initial;
        var restored = f.recover(saved);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                var runtime = f.runtime(saved, restored, output, records, disk, 97)) {
            saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), f.journal, f.initial.captureFingerprint());
        }
        assertThat(data(records)).hasSize(1);
        assertThat(saved.replayFrom()).isEqualTo(f.position(94));
        assertThat(saved.replayInclusive()).isTrue();
        assertThat(saved.completedThrough()).isEqualTo(f.position(97));
        assertThat(saved.schemaState().floor()).isEqualTo(As400SchemaState.ORIGIN);
        assertThat(saved.schemaState().completed().ordinal()).isEqualTo(1);
        for (int restart = 0; restart < 2; restart++) {
            records.clear();
            restored = f.recover(saved);
            try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                    var runtime = f.runtime(saved, restored, output, records, disk, 106)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), f.journal, f.initial.captureFingerprint());
            }
            if (restart == 0) {
                var values = data(records);
                assertThat(values).hasSize(1);
                assertThat(((Struct) values.get(0).key()).getInt32("ID")).isEqualTo(700);
                assertThat(((Struct) values.get(0).value()).getStruct("after").getString("NOTE")).isEqualTo("new");
            }
            else {
                assertThat(data(records)).isEmpty();
            }
        }
        assertThat(f.refreshes).isEqualTo(1);
        assertThat(saved.replayFrom()).isEqualTo(f.position(106));
        assertThat(saved.schemaState().floor().ordinal()).isEqualTo(1);
        assertThat(f.recover(saved).definitions()).contains(f.created);
        var path = directory.resolve("history.jsonl");
        Files.write(path, Files.readAllLines(path).stream().filter(line -> !line.contains(As400SchemaHistory.VERSION)).toList());
        var required = saved;
        assertThatThrownBy(() -> f.recover(required)).hasMessageContaining("schema history");
    }

    private static List<SourceRecord> data(List<SourceRecord> records) {
        return records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
    }

    @ParameterizedTest
    @ValueSource(strings = { "partial", "missing", "reordered", "file-id", "member-id", "before-catalog", "later-ddl" })
    void shouldRejectIncompleteOrChangedCreationEvidence(String fault) throws Exception {
        var f = new Fixture();
        var entries = new ArrayList<>(f.entries);
        var through = f.position(106);
        switch (fault) {
            case "partial" -> entries.subList(4, entries.size()).clear();
            case "missing" -> entries.remove(3);
            case "reordered" -> java.util.Collections.swap(entries, 3, 4);
            case "before-catalog" -> through = f.position(100);
            case "later-ddl" -> {
                byte[] raw = entries.get(0).data();
                entries.add(f.entry(raw, 108));
                through = f.position(108);
            }
            default -> {
                int index = fault.equals("file-id") ? 1 : 3;
                byte[] raw = entries.get(index).data();
                raw[206] ^= 1;
                entries.set(index, f.entry(raw, entries.get(index).position().offset().longValueExact()));
            }
        }
        var sampled = through;
        assertThatThrownBy(() -> As400SchemaChange.qualify(null, f.created, entries, f.order, sampled)).isInstanceOf(RuntimeException.class);
        assertThat(Files.readString(directory.resolve("history.jsonl"))).doesNotContain(As400SchemaHistory.VERSION);
        assertThat(f.recover(f.initial).definitions()).doesNotContain(f.created);
    }

    @Test
    void shouldCompareDocumentedCreationMetadataWithoutPersistingJobLocalPointers() throws Exception {
        var f = new Fixture();
        var original = f.entries.get(0);
        byte[] raw = original.data();
        ByteBuffer.wrap(raw).putInt(72, 29);
        int payload = original.header().getEntrySpecificDataOffset() + 16;
        raw[payload + 60] ^= 1;
        raw[payload + 144] ^= 1;
        var anotherCall = f.entry(raw, 94);
        assertThat(As400JournalRecordContent.digest(anotherCall.data(), anotherCall.position()))
                .isEqualTo(As400JournalRecordContent.digest(original.data(), original.position()));
        f.change.verifyRecord(anotherCall, f.order);
        raw[206] ^= 1;
        var wrongIdentity = f.entry(raw, 94);
        assertThatThrownBy(() -> f.change.verifyRecord(wrongIdentity, f.order)).hasMessageContaining("identity changed");
    }

    @ParameterizedTest
    @ValueSource(strings = { "fresh-observation", "moving-catalog", "identity-mismatch", "stopped" })
    void shouldRetryOnlyAQualifiedCatalogRaceWithinTheObservationBudget(String scenario) throws Exception {
        var f = new Fixture();
        var attempts = new java.util.concurrent.atomic.AtomicInteger();
        var observations = new ArrayList<As400SchemaChange>();
        var entries = new ArrayList<>(f.entries);
        if (scenario.equals("identity-mismatch")) {
            byte[] raw = entries.get(1).data();
            raw[206] ^= 1;
            entries.set(1, f.entry(raw, entries.get(1).position().offset().longValueExact()));
        }
        org.assertj.core.api.ThrowableAssert.ThrowingCallable collect = () -> observations.add(As400NativeSchemaRefresh.observe(
                entries.get(0).position(), () -> {
                    if (scenario.equals("stopped") && attempts.get() > 0) {
                        throw new IllegalStateException("Stopped before retry");
                    }
                }, () -> As400SchemaChange.qualify(null, f.created, entries, f.order,
                        f.position(attempts.incrementAndGet() == 1 || scenario.equals("moving-catalog") ? 100 : 106))));
        if (scenario.equals("fresh-observation")) {
            assertThatCode(collect).doesNotThrowAnyException();
            assertThat(observations).containsExactly(f.change);
        }
        else {
            assertThatThrownBy(collect).isInstanceOf(scenario.equals("moving-catalog") ? As400SchemaTimeline.PendingChangeException.class
                    : scenario.equals("stopped") ? IllegalStateException.class : IllegalArgumentException.class);
            assertThat(observations).isEmpty();
        }
        assertThat(attempts).hasValue(scenario.equals("fresh-observation") || scenario.equals("moving-catalog") ? 2 : 1);
        assertThat(f.recover(f.initial).definitions()).doesNotContain(f.created);
        assertThat(Files.readString(directory.resolve("history.jsonl"))).doesNotContain(As400SchemaHistory.VERSION);
    }
}
