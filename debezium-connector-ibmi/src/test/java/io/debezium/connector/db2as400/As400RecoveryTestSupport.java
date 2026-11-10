/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

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

/** Shared captured journal, binding history and replay fixtures; no live source access. */
final class As400RecoveryTestSupport {
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
            when(journals.collect()).thenReturn(receiverDirectory(through));
            when(identities.collect(any())).thenAnswer(i -> nativeCandidates(i.getArgument(0)));
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

        As400OperatorJournalEvidence.Observation receiverDirectory(long tail) {
            return new As400OperatorJournalEvidence.Observation(JOURNAL, RECEIVER,
                    List.of(new As400OperatorJournalEvidence.Receiver(RECEIVER, "ATTACHED", BigInteger.valueOf(29), BigInteger.valueOf(tail),
                            Optional.empty(), Optional.empty(), "NO", "YES", "YES", "NO", "NO")));
        }

        Candidates nativeCandidates(List<Target> targets) {
            return new Candidates(targets.stream().collect(Collectors.toMap(t -> t, nativeObjects::get)));
        }

        RawJournalPage.Entry entry(long sequence) {
            return entries.stream().filter(e -> e.position().equals(position(sequence))).findFirst().orElseThrow();
        }

        RawJournalPage.Entry userMarker(long sequence) {
            byte[] raw = entry(29).data();
            java.nio.ByteBuffer.wrap(raw).putLong(24, sequence).putLong(64, 0);
            new com.ibm.as400.access.AS400Text(3, 37).toBytes("UDN", raw, 98);
            new com.ibm.as400.access.AS400Text(30, 37).toBytes("", raw, 157);
            new com.ibm.as400.access.AS400Text(1, 37).toBytes("0", raw, 217);
            new com.ibm.as400.access.AS400Text(10, 37).toBytes("", raw, 219);
            return new RawJournalPage.Entry(position(sequence), new EntryHeaderDecoder().decode(raw, 0), raw);
        }

        static Map<String, String> legacyOffset(long sequence) {
            return Map.of("offset.event_sequence", Long.toString(sequence), "offset.receiver", RECEIVER.name(),
                    "offset.receiver_library", RECEIVER.library(), "offset.time", "0", "offset.processed", "true", "snapshot_completed", "true");
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
