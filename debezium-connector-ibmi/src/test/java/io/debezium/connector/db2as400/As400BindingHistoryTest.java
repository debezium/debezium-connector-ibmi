/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400RecoveryTestSupport.data;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.FileHistory;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

class As400BindingHistoryTest {
    @TempDir
    Path directory;

    @Test
    void shouldRecoverMoreThanOneThousandExcludedIdentitiesFromDurableHistory() throws Exception {
        var fixture = new Fixture(directory);
        var original = fixture.definitions.get(0).identity();
        fixture.definitions.clear();
        for (int i = 0; i < 1040; i++) {
            String file = String.format(java.util.Locale.ROOT, "F%04d", i);
            var identity = new As400TransactionBuffer.TableIdentity(new io.debezium.relational.TableId(null, "LIB", file), file, file,
                    String.format(java.util.Locale.ROOT, "%020x", 2L * i + 100), String.format(java.util.Locale.ROOT, "%020x", 2L * i + 101),
                    original.recordFormat());
            fixture.definitions.add(As400BindingDefinition.excluded(identity));
        }
        var saved = fixture.initialize();
        var recovered = new Fixture(directory).history().recover(saved, fixture.journalIdentity, Fixture.POLICY);
        assertThat(recovered.definitions()).containsExactlyElementsOf(fixture.definitions);
        assertThat(recovered.resolver(1024).resolve("F1039", "LIB").excluded()).isTrue();
    }

    @Test
    void shouldRecoverEveryMemberAndRejectChangedSecondaryMemberHistory() throws Exception {
        var fixture = new Fixture(directory);
        var definition = fixture.definitions.get(0);
        var members = new java.util.TreeMap<>(definition.members());
        members.put("SECOND", "00000000000000000009");
        fixture.definitions.set(0, new As400BindingDefinition(definition.identity(), definition.schema(), definition.codecs(), members));
        var saved = fixture.initialize();
        var recovered = new Fixture(directory).history().recover(saved, fixture.journalIdentity, Fixture.POLICY);
        var binding = recovered.resolver(1024).resolve(definition.identity().physicalFile(), definition.identity().tableId().schema(), "SECOND");
        assertThat(binding.identity().member()).isEqualTo("SECOND");
        assertThat(binding.identity().tableId()).isEqualTo(definition.identity().tableId());
        assertThat(binding.identity().memberJournalId()).isEqualTo("00000000000000000009");
        var original = Files.readString(fixture.file);
        Files.writeString(fixture.file, original.replace("00000000000000000009", "0000000000000000000a"));
        assertThrows(Exception.class, () -> new Fixture(directory).history().recover(saved, fixture.journalIdentity, Fixture.POLICY));
    }

    @Test
    void freshHistoryAndReaderInstancesPublishOtherTableBeforeNativeClearTerminal() throws Exception {
        Fixture f = new Fixture(directory);
        Map<String, String> saved = f.initialize();
        List<SourceRecord> records = new ArrayList<>();
        saved = f.replay(saved, 42, records::add);
        assertThat(data(records)).containsExactly("c:A:8");
        assertThat(saved).containsEntry("replay_from.sequence", "31").containsEntry("completed_through.sequence", "42");
        f = new Fixture(directory);
        saved = f.replay(saved, 44, records::add);
        assertThat(data(records)).containsExactly("c:A:8", "t:B", "c:B:100");
        saved = f.replay(saved, 56, records::add);
        assertThat(data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
        assertThat(saved).containsEntry("replay_from.sequence", "46").containsEntry("completed_through.sequence", "56");
        f = new Fixture(directory);
        saved = f.replay(saved, 64, records::add);
        assertThat(data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
        assertThat(saved).containsEntry("completed_through.sequence", "59");
        records.clear();
        new Fixture(directory).replay(saved, 64, records::add);
        assertThat(data(records)).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void failedClearOutputRetainsDurableIndependentProgressAndReplaysWholeBatch(int accepted) throws Exception {
        Fixture f = new Fixture(directory);
        var saved = f.replay(f.initialize(), 42, record -> true);
        List<SourceRecord> records = new ArrayList<>();
        assertThrows(As400ReplayCoordinator.RecoveryException.class,
                () -> f.replay(saved, 44, record -> records.size() < accepted && records.add(record)));
        records.forEach(r -> assertThat(r.sourceOffset().get("completed_through.sequence")).isEqualTo("42"));
        new Fixture(directory).replay(saved, 64, records::add);
        var result = data(records);
        assertThat(result).doesNotContain("c:A:8", "c:B:200");
        assertThat(result.subList(result.size() - 3, result.size())).containsExactly("t:B", "c:B:100", "c:A:9");
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "truncated", "file-jid", "member-jid", "nullable", "key", "codec", "format", "version", "foreign", "future", "conflict" })
    void missingDamagedOrChangedHistoryRejectsBeforeSourceReadOrOutput(String fault) throws Exception {
        Fixture f = new Fixture(directory);
        var saved = f.initialize();
        String original = Files.readString(f.file);
        String changed = switch (fault) {
            case "missing" -> "";
            case "truncated" -> original.substring(0, original.length() / 2);
            case "file-jid" -> original.replace("00000000000000000001", "00000000000000000009");
            case "member-jid" -> original.replace("00000000000000000002", "00000000000000000009");
            case "nullable", "key" -> corruptBinding(original, fault);
            case "codec" -> original.replace("BIN4:4:0:0", "FLOAT4:4:0:0");
            case "format" -> original.replace("29087D5938DDA", "29087D5938DDB");
            case "version" -> original.replace("\"version\":\"1\"", "\"version\":\"2\"");
            case "foreign" -> original.replace("\"server\":\"test\"", "\"server\":\"foreign\"");
            case "future" -> original.replace("\"sequence\":\"29\"", "\"sequence\":\"43\"");
            case "conflict" -> original.replace("00000000000000000001", "00000000000000000009") + original;
            default -> throw new AssertionError(fault);
        };
        assertThat(changed).isNotEqualTo(original);
        Files.writeString(f.file, changed);
        assertThrows(Exception.class, () -> f.history().recover(saved, f.journalIdentity, Fixture.POLICY));
        var legacy = Map.of("offset.event_sequence", "28", "offset.receiver", Fixture.RECEIVER.name(),
                "offset.receiver_library", Fixture.RECEIVER.library(), "offset.processed", "true", "snapshot_completed", "true");
        if (!fault.equals("missing")) {
            assertThrows(Exception.class, () -> f.history().pending(legacy, Fixture.POLICY));
        }
        assertThat(saved).containsEntry("completed_through.sequence", "29");
    }

    private static String corruptBinding(String history, String fault) throws Exception {
        var json = new com.fasterxml.jackson.databind.ObjectMapper();
        var record = json.readTree(history);
        var position = (com.fasterxml.jackson.databind.node.ObjectNode) record.path("position");
        var bindings = json.readTree(position.path("bindings").asText());
        var table = (com.fasterxml.jackson.databind.node.ObjectNode) bindings.path("tables").get(0).path("table");
        if (fault.equals("nullable")) {
            ((com.fasterxml.jackson.databind.node.ObjectNode) table.path("columns").get(0)).put("optional", true);
        }
        else {
            table.set("primaryKeyColumnNames", json.createArrayNode());
        }
        position.put("bindings", json.writeValueAsString(bindings));
        return json.writeValueAsString(record) + "\n";
    }

    @Test
    void shouldRecoverHistoryWrittenByThePreviousPluginWithoutChangingItsCheckpoint() throws Exception {
        var f = new Fixture(directory);
        try (var history = getClass().getResourceAsStream("/binding-history/legacy-v1-history.jsonl");
                var offset = getClass().getResourceAsStream("/binding-history/legacy-v1-offset.json")) {
            Files.copy(history, f.file);
            var saved = new com.fasterxml.jackson.databind.ObjectMapper().readValue(offset, Map.class);
            var recovered = f.history().recover(saved, f.journalIdentity, Fixture.POLICY);
            assertThat(recovered.definitions()).isEqualTo(f.definitions);
            var output = new ArrayList<SourceRecord>();
            var checkpoint = f.replay(saved, 64, output::add);
            assertThat(data(output)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
            assertThat(checkpoint.get("capture_fingerprint")).isEqualTo(saved.get("capture_fingerprint"));
            assertThat(checkpoint.get("completed_through.sequence")).isEqualTo("59");
        }
    }

    @Test
    void shouldStoreBindingsOutsideCoreTableChangesAndRejectUnknownEncoding() throws Exception {
        var f = new Fixture(directory);
        var saved = f.initialize();
        var json = new com.fasterxml.jackson.databind.ObjectMapper();
        var record = json.readTree(Files.readString(f.file));
        assertThat(record.path("tableChanges").isEmpty()).isTrue();
        assertThat(record.path("position").path("binding.encoding").asText()).isEqualTo("1");
        assertThat(f.history().recover(saved, f.journalIdentity, Fixture.POLICY).definitions()).isEqualTo(f.definitions);
        ((com.fasterxml.jackson.databind.node.ObjectNode) record.path("position")).put("binding.encoding", "future");
        Files.writeString(f.file, json.writeValueAsString(record) + "\n");
        assertThrows(Exception.class, () -> f.history().recover(saved, f.journalIdentity, Fixture.POLICY));
    }

    @Test
    void exactDuplicateInitializationIsIdempotentButCannotReplaceAnExistingEpoch() throws Exception {
        Fixture f = new Fixture(directory);
        var saved = f.initialize();
        String original = Files.readString(f.file);
        assertThat(f.initialize()).isEqualTo(saved);
        assertThat(Files.readString(f.file)).isEqualTo(original);
        Files.writeString(f.file, original + original);
        f.history().recover(saved, f.journalIdentity, Fixture.POLICY);
        assertThrows(Exception.class, () -> f.history().recordBaseline(f.journalIdentity, Fixture.position(42), f.receiverIdentity, f.definitions, Fixture.POLICY));
        assertThat(Files.readString(f.file)).isEqualTo(original + original);
    }

    @ParameterizedTest
    @ValueSource(strings = { "throw-before", "drop-write", "partial-write", "throw-after", "readback-fails" })
    void failedOrUnconfirmedManifestWriteCannotReturnInitialCheckpoint(String fault) throws Exception {
        Fixture f = new Fixture(directory);
        AtomicInteger instances = new AtomicInteger();
        Supplier<SchemaHistory> factory = () -> {
            int instance = instances.incrementAndGet();
            return new FileHistory(f.file) {
                @Override
                protected void storeRecord(HistoryRecord record) {
                    if (fault.equals("throw-before")) {
                        throw new IllegalStateException("injected write failure");
                    }
                    if (!fault.equals("drop-write")) {
                        super.storeRecord(record);
                    }
                    if (fault.equals("partial-write")) {
                        try {
                            Files.writeString(f.file, "{\"source\":");
                        }
                        catch (Exception e) {
                            throw new IllegalStateException(e);
                        }
                    }
                    if (fault.equals("throw-after")) {
                        throw new IllegalStateException("injected lost acknowledgement");
                    }
                }

                @Override
                protected void recoverRecords(Consumer<HistoryRecord> records) {
                    if (instance > 1 && fault.equals("readback-fails")) {
                        throw new IllegalStateException("injected readback failure");
                    }
                    super.recoverRecords(records);
                }
            };
        };
        var history = new As400BindingHistory(factory, Configuration.empty(), Fixture.POLICY.sourcePartition());
        assertThrows(Exception.class, () -> history.recordBaseline(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions, Fixture.POLICY));
        // Persisted-but-unacknowledged initialization can be retried explicitly;
        // ordinary resume never invents its missing offset.
        if (fault.equals("throw-after") || fault.equals("readback-fails")) {
            assertThat(f.initialize()).containsEntry("completed_through.sequence", "29");
        }
    }

    @Test
    void missingCheckpointRejectsBeforeHistoryIsOpenedEvenWhenManifestExists() throws Exception {
        Fixture f = new Fixture(directory);
        f.initialize();
        String original = Files.readString(f.file);
        var history = new As400BindingHistory(() -> {
            throw new AssertionError("must not open history");
        }, Configuration.empty(), Fixture.POLICY.sourcePartition());
        assertThrows(IllegalArgumentException.class, () -> history.recover(null, f.journalIdentity, Fixture.POLICY));
        assertThrows(IllegalArgumentException.class, () -> history.recover(Map.of("position", "29"), f.journalIdentity, Fixture.POLICY));
        assertThat(Files.readString(f.file)).isEqualTo(original);
    }

    @Test
    void changedEffectivePolicyAndCheckpointBeforeBindingBoundaryReject() throws Exception {
        Fixture f = new Fixture(directory);
        var saved = f.initialize();
        var p = Fixture.POLICY;
        var changed = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(),
                Map.of("version", "different-output"), p.transactionMetadata(), p.tombstones(), p.sourcePartition());
        assertThrows(Exception.class, () -> f.history().recover(saved, f.journalIdentity, changed));
        var tooEarly = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, saved.get("capture_fingerprint"), Fixture.position(28), f.receiverIdentity);
        assertThrows(Exception.class, () -> f.history().recover(tooEarly.toOffset(), f.journalIdentity, p));
    }

    @Test
    void twoTablesCannotClaimTheSamePhysicalJournalIdentityAtInitialization() throws Exception {
        Fixture f = new Fixture(directory);
        var first = f.definitions.get(0).identity();
        var second = f.definitions.get(1);
        var id = second.identity();
        f.definitions.set(1, new As400BindingDefinition(new TableIdentity(id.tableId(), id.physicalFile(), id.member(),
                first.fileJournalId(), id.memberJournalId(), id.recordFormat()), second.schema(), second.codecs()));
        assertThrows(IllegalArgumentException.class, f::initialize);
        assertThat(Files.exists(f.file)).isFalse();
    }

    @Test
    void unqualifiedKafkaProviderCannotSilentlySkipMalformedRecords() throws Exception {
        Fixture f = new Fixture(directory);
        var saved = f.initialize();
        f.factory = KafkaSchemaHistory::new;
        assertThrows(IllegalArgumentException.class, f::initialize);
        assertThrows(IllegalArgumentException.class, () -> f.history().recover(saved, f.journalIdentity, Fixture.POLICY));
    }

    @Test
    void fixedBindingEpochSurvivesFloorMovingToANewerVerifiedReceiverWithoutRetainingBaselineReceiver() throws Exception {
        Fixture f = new Fixture(directory);
        var saved = f.initialize();
        var next = new JournalReceiver("QSQJRN0004", "DCOC1107");
        var position = new JournalPosition(BigInteger.valueOf(100), next);
        var checkpoint = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, saved.get("capture_fingerprint"), position, "next-receiver");
        var recovered = f.history().recover(checkpoint.toOffset(), f.journalIdentity, Fixture.POLICY);
        var order = new As400JournalOrder(List.of(next), Map.of(next, "next-receiver"),
                Map.of(next, new As400JournalOrder.Range(BigInteger.valueOf(90), BigInteger.valueOf(110))));
        recovered.verify(checkpoint, order);
        var replaced = new As400JournalOrder(List.of(next), Map.of(next, "replaced-receiver"),
                Map.of(next, new As400JournalOrder.Range(BigInteger.valueOf(90), BigInteger.valueOf(110))));
        assertThrows(Exception.class, () -> recovered.verify(checkpoint, replaced));
    }

    @Test
    void restoredBindingsCannotAuthorizeChangedJournalMissingAnchorsOrWrongRowJid() throws Exception {
        Fixture f = new Fixture(directory);
        var saved = f.initialize();
        assertThrows(Exception.class, () -> f.history().recover(saved, "different-journal", Fixture.POLICY));
        f.entries.removeIf(e -> e.position().equals(Fixture.position(29)));
        assertThrows(Exception.class, () -> f.replay(saved, 42, record -> {
            throw new AssertionError("no publication");
        }));
        Fixture changed = new Fixture(directory);
        int index = java.util.stream.IntStream.range(0, changed.entries.size()).filter(i -> changed.entries.get(i).position().equals(Fixture.position(42))).findFirst()
                .orElseThrow();
        byte[] bytes = changed.entries.get(index).data();
        bytes[206] ^= 1;
        changed.entries.set(index, new RawJournalPage.Entry(Fixture.position(42), new EntryHeaderDecoder().decode(bytes, 0), bytes));
        assertThrows(Exception.class, () -> changed.replay(saved, 42, record -> {
            throw new AssertionError("no publication");
        }));
    }

}
