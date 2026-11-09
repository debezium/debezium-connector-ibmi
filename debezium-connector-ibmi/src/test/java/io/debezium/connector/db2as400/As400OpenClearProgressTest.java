/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

/**
 * Native cuts taken while B's clear was still open, with independently committed
 * A rows. Bindings/layouts come from the controlled fixture, not a production
 * startup protocol. No sequence, thread or commit-cycle substitution is used.
 */
class As400OpenClearProgressTest {
    private static final String JOURNAL = "open-clear-fixture-journal";
    private static final String FINGERPRINT = "v1:" + "e".repeat(64);
    private static final JournalReceiver RECEIVER = new JournalReceiver("QSQJRN0003", "DCOC1107");
    private static final As400Partition PARTITION = new As400Partition("test");
    private static final As400Checkpoint INITIAL = As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT, position(29), "fixture-receiver");
    private static final List<String> EXPECTED = List.of("c:A:8", "t:B", "c:B:100", "c:A:9");

    @TempDir
    Path directory;

    @Test
    void unrelatedCommitsFlowBeforeClearCompletesAndFreshInstancesRecoverBothOutcomes() throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        Fixture fixture = new Fixture();
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(INITIAL.toOffset(), 42));
            replay.verifyOrdinaryCut();
            assertThat(data(records)).containsExactly("c:A:8");
            assertThat(replay.scanPosition().orElseThrow()).isEqualTo(position(42));
            assertThat(replay.checkpoint().replayFrom()).isEqualTo(position(31));
            assertThat(replay.checkpoint().replayInclusive()).isTrue();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(42));
            assertThat(replay.bufferUsage()[0]).isEqualTo(1);
            assertThat(replay.bufferUsage()[1]).isPositive();
            assertThat(replay.oldestTransactionTimestamp()).isPositive();
            saved = checkpoint(replay);
        }
        fixture = new Fixture();
        try (var replay = fixture.replay(saved, records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(saved, 44));
            replay.verifyEndOfAvailableJournal();
            assertThat(data(records)).containsExactly("c:A:8", "t:B", "c:B:100");
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(43));
            saved = checkpoint(replay);
        }
        fixture = new Fixture();
        try (var replay = fixture.replay(saved, records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(saved, 56));
            replay.verifyOrdinaryCut();
            assertThat(data(records)).containsExactlyElementsOf(EXPECTED);
            assertThat(replay.checkpoint().replayFrom()).isEqualTo(position(46));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(56));
            saved = checkpoint(replay);
        }
        fixture = new Fixture();
        try (var replay = fixture.replay(saved, records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(saved, 64));
            replay.verifyEndOfAvailableJournal();
            assertThat(data(records)).containsExactlyElementsOf(EXPECTED);
            assertThat(replay.scanPosition().orElseThrow()).isEqualTo(position(64));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(59));
            assertThat(replay.bufferUsage()).containsExactly(0, 0);
            assertThat(replay.oldestTransactionTimestamp()).isEqualTo(-1);
        }
    }

    @Test
    void standardTableHistoryFormatCanRestoreBindingsAndProgressAcrossAnOpenClear() throws Exception {
        // Format feasibility only: this does not exercise SchemaHistory storage,
        // Kafka durability, epoch selection, or crash-safe history initialization.
        Fixture fixture = new Fixture();
        Path history = directory.resolve("tables.json");
        Files.writeString(history, fixture.history());
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(INITIAL.toOffset(), 42));
            replay.verifyOrdinaryCut();
            assertThat(data(records)).containsExactly("c:A:8");
            saved = checkpoint(replay);
        }
        Path offset = directory.resolve("offset.json");
        Files.writeString(offset, new ObjectMapper().writeValueAsString(saved));
        fixture.bindings.clear();
        records.clear();
        // The replacement receives independently read bytes only. It does not
        // reload table metadata from the fixture or query IBM i during recovery.
        fixture = new Fixture(Files.readString(history));
        saved = new ObjectMapper().readValue(Files.readString(offset), new TypeReference<Map<String, String>>() {
        });
        try (var replay = fixture.replay(saved, records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(saved, 42));
            replay.verifyOrdinaryCut();
            assertThat(data(records)).isEmpty();
            fixture.run(replay, output, fixture.entries.stream().filter(e -> e.position().offset().intValueExact() > 42).toList());
            replay.verifyEndOfAvailableJournal();
        }
        assertThat(data(records)).containsExactly("t:B", "c:B:100", "c:A:9");
    }

    static IntStream entryCuts() {
        return IntStream.rangeClosed(0, 33);
    }

    @ParameterizedTest
    @MethodSource("entryCuts")
    void restartAtEveryNativeEntryPreservesIndependentProgressAndRollback(int cut) throws Exception {
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        Fixture fixture = new Fixture();
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.entries.subList(0, cut));
            saved = checkpoint(replay);
        }
        fixture = new Fixture();
        try (var replay = fixture.replay(saved, records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(saved, 64));
            replay.verifyEndOfAvailableJournal();
        }
        assertThat(data(records)).containsExactlyElementsOf(EXPECTED);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void interruptedClearBatchKeepsPreviouslyCompletedIndependentRowOutOfReplay(int accepted) throws Exception {
        Fixture fixture = new Fixture();
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(INITIAL.toOffset(), 42));
            saved = checkpoint(replay);
        }
        records.clear();
        try (var replay = fixture.replay(saved, record -> records.size() < accepted && records.add(record)); var output = fixture.output()) {
            assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> fixture.run(replay, output, fixture.between(saved, 44)));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(42));
            for (SourceRecord record : records) {
                assertThat(record.sourceOffset().get("completed_through.sequence")).isEqualTo("42");
                assertThat(record.sourceOffset().get("replay_from.sequence")).isEqualTo("31");
            }
        }
        try (var replay = fixture.replay(saved, records::add); var output = fixture.output()) {
            fixture.run(replay, output, fixture.between(saved, 64));
            replay.verifyEndOfAvailableJournal();
        }
        List<String> delivered = data(records);
        assertThat(delivered).doesNotContain("c:A:8", "c:B:200");
        assertThat(delivered.subList(delivered.size() - 3, delivered.size())).containsExactly("t:B", "c:B:100", "c:A:9");
    }

    @ParameterizedTest
    @ValueSource(strings = { "file", "member", "missing" })
    void retainedBindingStillRejectsMissingOrWrongIdentityBeforePublication(String fault) throws Exception {
        Fixture fixture = new Fixture();
        var binding = fixture.bindings.get("B");
        var id = binding.identity();
        if (fault.equals("missing")) {
            fixture.bindings.remove("B");
        }
        else {
            fixture.bindings.put("B", new TableBinding(new TableIdentity(id.tableId(), id.physicalFile(), id.member(),
                    fault.equals("file") ? "00000000000000000009" : id.fileJournalId(),
                    fault.equals("member") ? "00000000000000000009" : id.memberJournalId(), id.recordFormat()), binding.decoder()));
        }
        List<SourceRecord> records = new ArrayList<>();
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add); var output = fixture.output()) {
            assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> fixture.run(replay, output, fixture.entries));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(INITIAL.completedThrough());
        }
        assertThat(records).isEmpty();
    }

    private record Entry(EntryHeader header, byte[] bytes) {
        JournalPosition position() {
            return new JournalPosition(header.getSequenceNumber(), RECEIVER);
        }
    }

    private static class Fixture {
        final Map<String, TableBinding> bindings = new HashMap<>();
        final List<Entry> entries = new ArrayList<>();

        Fixture() throws Exception {
            this(null);
        }

        Fixture(String history) throws Exception {
            try (var input = getClass().getResourceAsStream("/native-transactions/open-clear-progress.json")) {
                var json = new ObjectMapper().readTree(input);
                if (history == null) {
                    for (var table : json.get("tables")) {
                        String file = table.get("file").asText();
                        var id = new TableIdentity(new TableId(null, table.get("library").asText(), file), file, table.get("member").asText(),
                                table.get("file_jid").asText(), table.get("member_jid").asText(), table.get("format").asText());
                        bind(id);
                    }
                }
                else {
                    var document = DocumentReader.defaultReader().read(history);
                    for (var change : new JsonTableChangeSerializer().deserialize(document.getArray("tableChanges"), false)) {
                        Table table = change.getTable();
                        assertThat(change.getType()).isEqualTo(TableChanges.TableChangeType.CREATE);
                        assertThat(table.columns()).containsExactly(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER)
                                .type("INTEGER").length(4).optional(false).create());
                        assertThat(table.primaryKeyColumnNames()).containsExactly("ID");
                        assertThat(attribute(table, "fixture.codec")).isEqualTo("bin4-not-null-v1");
                        bind(new TableIdentity(table.id(), attribute(table, "ibmi.file"), attribute(table, "ibmi.member"),
                                attribute(table, "ibmi.file.jid"), attribute(table, "ibmi.member.jid"), attribute(table, "ibmi.format")));
                    }
                    assertThat(bindings).containsOnlyKeys("A", "B");
                }
                for (var raw : json.get("entries")) {
                    byte[] bytes = HexFormat.of().parseHex(raw.get("hex").asText());
                    var entry = new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes);
                    if (entry.position().offset().intValueExact() > 29) {
                        entries.add(entry);
                    }
                }
            }
            assertThat(entries).hasSize(33);
        }

        private void bind(TableIdentity id) {
            var previous = bindings.putIfAbsent(id.physicalFile(),
                    new TableBinding(id, new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 1024)));
            assertThat(previous).isNull();
        }

        String history() throws Exception {
            var changes = new TableChanges();
            for (String name : List.of("A", "B")) {
                var id = bindings.get(name).identity();
                var table = Table.editor().tableId(id.tableId())
                        .addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER").length(4).optional(false).create())
                        .setPrimaryKeyNames("ID");
                Map.of("ibmi.file", id.physicalFile(), "ibmi.member", id.member(), "ibmi.file.jid", id.fileJournalId(),
                        "ibmi.member.jid", id.memberJournalId(), "ibmi.format", id.recordFormat(), "fixture.codec", "bin4-not-null-v1")
                        .forEach((key, value) -> table.addAttribute(Attribute.editor().name(key).value(value).create()));
                changes.create(table.create());
            }
            var document = Document.create();
            document.setArray("tableChanges", new JsonTableChangeSerializer().serialize(changes));
            return DocumentWriter.defaultWriter().write(document);
        }

        private static String attribute(Table table, String name) {
            assertThat(table.attributeWithName(name)).isNotNull();
            return table.attributeWithName(name).value();
        }

        As400ReplayCoordinator replay(Map<String, ?> saved, As400ReplayCoordinator.RecordSink sink) {
            var order = new As400JournalOrder(List.of(RECEIVER), Map.of(RECEIVER, "fixture-receiver"),
                    Map.of(RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(29), BigInteger.valueOf(64))));
            return new As400ReplayCoordinator(saved, JOURNAL, FINGERPRINT, order, new As400TransactionBuffer.Limits(16, 1000, 128, 65536),
                    1024, 100, (file, library) -> bindings.get(file), As400SourceNormalizer.SavepointPolicy.consumingTargets(),
                    PARTITION.getSourcePartition(), sink, new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), true);
        }

        As400BatchDispatcher output() {
            return new As400IncrementalLifecycleTest.Output(bindings, true).dispatcher;
        }

        void run(As400ReplayCoordinator replay, As400BatchDispatcher output, List<Entry> input) {
            for (Entry e : input) {
                if (replay.accept(e.header(), e.bytes(), 0, e.position()).isPresent()) {
                    replay.deliver(output);
                }
            }
        }

        List<Entry> between(Map<String, ?> saved, long through) {
            var checkpoint = As400Checkpoint.fromOffset(saved, JOURNAL, FINGERPRINT);
            return entries.stream().filter(e -> {
                int c = e.position().offset().compareTo(checkpoint.replayFrom().offset());
                return (c > 0 || c == 0 && checkpoint.replayInclusive()) && e.position().offset().longValueExact() <= through;
            }).toList();
        }
    }

    private static Map<String, ?> checkpoint(As400ReplayCoordinator replay) throws Exception {
        var heartbeat = new SourceRecord(PARTITION.getSourcePartition(), Map.of(), "heartbeat", Schema.STRING_SCHEMA, "heartbeat");
        var mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsBytes(replay.publishCheckpoint(heartbeat).sourceOffset()), new TypeReference<Map<String, String>>() {
        });
    }

    private static JournalPosition position(long value) {
        return new JournalPosition(BigInteger.valueOf(value), RECEIVER);
    }

    private static List<String> data(List<SourceRecord> records) {
        return records.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("op") != null).map(r -> {
            var s = (Struct) r.value();
            String op = s.getString("op");
            return op + ":" + r.topic().substring(5) + (op.equals("t") ? "" : ":" + s.getStruct(op.equals("d") ? "before" : "after").getInt32("ID"));
        }).toList();
    }
}
