/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400BindingDefinition.Codec;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.AbstractSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

class As400BindingHistoryTest {
    @TempDir
    Path directory;

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
            case "nullable" -> original.replace("\"optional\":false", "\"optional\":true");
            case "key" -> original.replace("\"primaryKeyColumnNames\":[\"ID\"]", "\"primaryKeyColumnNames\":[]");
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
        assertThat(saved).containsEntry("completed_through.sequence", "29");
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

    static List<String> data(List<SourceRecord> records) {
        return records.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("op") != null).map(r -> {
            var value = (Struct) r.value();
            String op = value.getString("op");
            return op + ":" + r.topic().substring(5) + (op.equals("t") ? "" : ":" + value.getStruct("after").getInt32("ID"));
        }).toList();
    }

    static final class Fixture {
        static final JournalReceiver JOURNAL = new JournalReceiver("QSQJRN", "DCOC1107");
        static final JournalReceiver RECEIVER = new JournalReceiver("QSQJRN0003", "DCOC1107");
        static final As400CaptureFingerprint.Policy POLICY = new As400CaptureFingerprint.Policy(Map.of(), Set.of(), Map.of("version", "strict-row-v1"),
                Map.of("version", "fixed-v1"), Map.of("version", "ordinary-v1"), true, true, Map.of("server", "test"));
        final Path file;
        final List<As400BindingDefinition> definitions = new ArrayList<>();
        final List<RawJournalPage.Entry> entries = new ArrayList<>();
        final Map<Target, Identity> nativeObjects = Map.of(target(JOURNAL, Kind.JOURNAL), new Identity(BigInteger.ONE, BigInteger.ONE, BigInteger.ONE),
                target(RECEIVER, Kind.RECEIVER), new Identity(BigInteger.ONE, BigInteger.TWO, BigInteger.ONE));
        final String journalIdentity = As400OperatorCutReader.token(target(JOURNAL, Kind.JOURNAL), nativeObjects.get(target(JOURNAL, Kind.JOURNAL)));
        final String receiverIdentity = As400OperatorCutReader.token(target(RECEIVER, Kind.RECEIVER), nativeObjects.get(target(RECEIVER, Kind.RECEIVER)));
        Supplier<SchemaHistory> factory;
        Configuration config = Configuration.empty();

        Fixture(Path directory) throws Exception {
            file = directory.resolve("history.jsonl");
            factory = () -> new FileHistory(file);
            try (var input = getClass().getResourceAsStream("/native-transactions/open-clear-progress.json")) {
                var json = new ObjectMapper().readTree(input);
                for (var table : json.get("tables")) {
                    String name = table.get("file").asText();
                    var id = new TableId(null, table.get("library").asText(), name);
                    var identity = new TableIdentity(id, name, table.get("member").asText(), table.get("file_jid").asText(),
                            table.get("member_jid").asText(), table.get("format").asText());
                    var schema = Table.editor().tableId(id).addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER")
                            .length(4).optional(false).create()).setPrimaryKeyNames("ID").create();
                    definitions.add(new As400BindingDefinition(identity, schema, List.of(new Codec("BIN4", 4, 0, 0))));
                }
                for (var raw : json.get("entries")) {
                    byte[] bytes = HexFormat.of().parseHex(raw.get("hex").asText());
                    entries.add(new RawJournalPage.Entry(position(raw.get("sequence").asLong()), new EntryHeaderDecoder().decode(bytes, 0), bytes));
                }
            }
        }

        As400BindingHistory history() {
            return new As400BindingHistory(factory, config, POLICY.sourcePartition());
        }

        Map<String, String> initialize() throws Exception {
            return history().recordBaseline(journalIdentity, position(29), receiverIdentity, definitions, POLICY).toOffset();
        }

        Map<String, String> replay(Map<String, ?> saved, long through, As400ReplayCoordinator.RecordSink sink) throws Exception {
            var recovered = history().recover(saved, journalIdentity, POLICY);
            var journals = mock(As400OperatorJournalEvidence.class);
            var identities = mock(As400JdbcNativeIdentityEvidence.class);
            var transport = mock(RetrieveJournal.class);
            when(journals.collect()).thenReturn(new As400OperatorJournalEvidence.Observation(JOURNAL, RECEIVER,
                    List.of(new As400OperatorJournalEvidence.Receiver(RECEIVER, "ATTACHED", BigInteger.valueOf(29), BigInteger.valueOf(through),
                            Optional.empty(), Optional.empty(), "NO", "YES", "YES", "NO", "NO"))));
            when(identities.collect(any())).thenAnswer(i -> new Candidates(((List<Target>) i.getArgument(0)).stream()
                    .collect(Collectors.toMap(t -> t, nativeObjects::get))));
            when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
                JournalPosition from = i.getArgument(0);
                JournalPosition to = i.getArgument(1);
                return new RawJournalPage(entries.stream().filter(e -> e.position().offset().compareTo(from.offset()) >= 0
                        && e.position().offset().compareTo(to.offset()) <= 0).toList(), Optional.empty(), 65536);
            });
            // No table-evidence collector exists in this path. Mocked journal/native
            // tokens test plumbing only; the raw entry payloads are captured IBM i bytes.
            var cut = new As400OperatorCutReader(journals, identities, JOURNAL, recovered, transport,
                    new As400OperatorCutReader.Limits(10, 100, 1_000_000)).read(saved);
            assertThat(cut.tables()).isEmpty();
            assertThat(cut.historicalBindings()).contains(recovered);
            Map<String, TableBinding> bindings = recovered.definitions().stream().collect(Collectors.toMap(d -> d.identity().physicalFile(), d -> d.binding(1024)));
            try (var replay = new As400ReplayCoordinator(saved, journalIdentity, recovered.fingerprint(), cut.order(),
                    new As400TransactionBuffer.Limits(16, 1000, 128, 65536), 1024, 100, recovered.resolver(1024),
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), POLICY.sourcePartition(), sink,
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), true);
                    var output = new As400IncrementalLifecycleTest.Output(bindings, true).dispatcher) {
                for (var e : cut.entries()) {
                    if (replay.accept(e.header(), e.data(), 0, e.position()).isPresent()) {
                        replay.deliver(output);
                    }
                }
                replay.verifyOrdinaryCut();
                return replay.checkpoint().toOffset();
            }
        }

        static JournalPosition position(long sequence) {
            return new JournalPosition(BigInteger.valueOf(sequence), RECEIVER);
        }

        static Target target(JournalReceiver name, Kind kind) {
            return new Target("/QSYS.LIB/" + name.library() + ".LIB/" + name.name() + (kind == Kind.JOURNAL ? ".JRN" : ".JRNRCV"), kind);
        }
    }

    /** Test-only fault-injectable file backend; production uses standard storage. */
    static class FileHistory extends AbstractSchemaHistory {
        private final Path file;

        FileHistory(Path file) {
            this.file = file;
        }

        @Override
        protected void storeRecord(HistoryRecord record) {
            try {
                Files.writeString(file, DocumentWriter.defaultWriter().write(record.document()) + "\n", StandardOpenOption.APPEND);
                try (var channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    channel.force(true);
                }
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        protected void recoverRecords(Consumer<HistoryRecord> records) {
            try {
                for (String line : Files.readAllLines(file)) {
                    records.accept(new HistoryRecord(DocumentReader.defaultReader().read(line)));
                }
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void initializeStorage() {
            try {
                Files.createFile(file);
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean storageExists() {
            return Files.exists(file);
        }

        @Override
        public boolean exists() {
            try {
                return storageExists() && Files.size(file) > 0;
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
