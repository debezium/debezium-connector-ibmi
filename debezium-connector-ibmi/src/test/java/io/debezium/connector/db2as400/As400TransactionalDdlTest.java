/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Types;
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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.storage.file.history.FileSchemaHistory;

/** Native rollback and member-removal bytes; controlled bindings are not startup proof. */
class As400TransactionalDdlTest {
    @TempDir
    Path directory;

    private final class Fixture {
        final JournalReceiver receiver = new JournalReceiver("QSQJRN0002", "DCNC0005");
        final As400JournalOrder order = new As400JournalOrder(List.of(receiver), Map.of(receiver, "fixture-receiver"),
                Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(100000))));
        final List<RawJournalPage.Entry> entries = new ArrayList<>();
        final As400BindingDefinition before;
        final Map<String, As400BindingDefinition> after = new java.util.HashMap<>();
        long cutThrough = 100000;
        long schemaThrough = Long.MAX_VALUE;
        int schemaReads;
        int failAfterRecords = -1;
        As400HistoricalTableNames.Reader historicalNames;
        long payloadBudget = 1_000_000;
        final List<As400BindingDefinition> baseline;
        final As400CaptureFingerprint.Policy policy = new As400CaptureFingerprint.Policy(Map.of(), java.util.Set.of(), Map.of("version", "strict-row-v1"),
                Map.of("version", "fixed-v1"), Map.of("version", "ordinary-v1"), false, true, Map.of("server", "offline"));
        final Configuration config = Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName())
                .with(FileSchemaHistory.FILE_PATH, directory.resolve("history.jsonl").toString()).build();
        final As400Checkpoint initial;
        final boolean longSqlNames;

        Fixture() throws Exception {
            this(false);
        }

        Fixture(boolean longSqlNames) throws Exception {
            this.longSqlNames = longSqlNames;
            try (var input = getClass().getResourceAsStream("/native-transactions/transactional-ddl-rollback.json")) {
                var json = new ObjectMapper().readTree(input);
                assertThat(json.path("startup_proof").asBoolean()).isFalse();
                for (var item : json.path("entries")) {
                    byte[] raw = Base64.getDecoder().decode(item.path("raw").asText());
                    var header = new EntryHeaderDecoder(37).decode(raw, 0);
                    entries.add(new RawJournalPage.Entry(position(header.getSequenceNumber().longValueExact()), header, raw));
                }
            }
            before = binding("U", entry(83).header().getRecordInfo().objectJournalId(), entry(44).header().getRecordInfo().objectJournalId());
            baseline = List.of(before, binding("A", "0000000000000000ffff", entry(100).header().getRecordInfo().objectJournalId()));
            var selected = history().recordOnlineCandidate("fixture-journal", position(44), "fixture-receiver", baseline, policy,
                    new As400BindingHistory.Discovery(position(44), "fixture-receiver"),
                    Map.of("offset.event_sequence", "43", "offset.receiver", receiver.name(), "offset.receiver_library", receiver.library(),
                            "offset.processed", "true", "snapshot_completed", "true"));
            initial = As400Checkpoint.afterVerifiedBoundary("fixture-journal", selected.fingerprint(), position(44), "fixture-receiver");
        }

        As400BindingDefinition binding(String file, String fileJid, String memberJid) {
            var id = new TableId(null, receiver.library(), longSqlNames && file.equals("U") ? "LONG_ORIGINAL_NAME" : file);
            var identity = new As400TransactionBuffer.TableIdentity(id, file, file, fileJid, memberJid, file + ":0123456789ABC:4:1:false");
            var schema = Table.editor().tableId(id).addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER")
                    .length(4).optional(false).create()).setPrimaryKeyNames("ID").create();
            return new As400BindingDefinition(identity, schema, List.of(new As400BindingDefinition.Codec("BIN4", 4, 0, 0)));
        }

        JournalPosition position(long sequence) {
            return new JournalPosition(BigInteger.valueOf(sequence), receiver);
        }

        RawJournalPage.Entry entry(long sequence) {
            return entries.stream().filter(e -> e.position().equals(position(sequence))).findFirst().orElseThrow();
        }

        List<RawJournalPage.Entry> range(long first, long last) {
            return entries.stream().filter(e -> e.position().offset().longValueExact() >= first && e.position().offset().longValueExact() <= last).toList();
        }

        RawJournalPage.Entry copy(RawJournalPage.Entry original, long sequence, Long cycle, Long target,
                                  RawJournalPage.Entry producer, String type) {
            byte[] raw = original.data();
            var buffer = java.nio.ByteBuffer.wrap(raw);
            buffer.putLong(24, sequence);
            if (cycle != null) {
                buffer.putLong(64, cycle);
            }
            if (type != null) {
                new com.ibm.as400.access.AS400Text(2, 37).toBytes(type, raw, 99);
            }
            if (target != null) {
                new com.ibm.as400.access.AS400Text(20, 37).toBytes(String.format("%020d", target), raw,
                        original.header().getEntrySpecificDataOffset() + 16);
            }
            if (producer != null) {
                byte[] source = producer.data();
                System.arraycopy(source, 40, raw, 40, 8);
                System.arraycopy(source, 101, raw, 101, 26);
                System.arraycopy(source, 208, raw, 208, 8);
            }
            return new RawJournalPage.Entry(position(sequence), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        }

        RawJournalPage.Entry payload(RawJournalPage.Entry original, long sequence, String type, byte[] payload) {
            var changed = copy(original, sequence, 0L, null, null, type);
            int at = changed.header().getEntrySpecificDataOffset();
            byte[] raw = java.util.Arrays.copyOf(changed.data(), at + 16 + payload.length);
            java.nio.ByteBuffer.wrap(raw).putInt(4, 0).putInt(232, 0);
            new com.ibm.as400.access.AS400Text(5, 37).toBytes(String.format("%05d", payload.length), raw, at);
            System.arraycopy(payload, 0, raw, at + 16, payload.length);
            return new RawJournalPage.Entry(position(sequence), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        }

        List<RawJournalPage.Entry> metadataChange(long sequence, As400BindingDefinition table, long cycle, int level, RawJournalPage.Entry producer) {
            var opening = forTable(payload(entry(78), sequence, "CH", new byte[]{ 0x40 }), table);
            byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(entry(83).header(), entry(83).data(), entry(83).position()));
            payload = java.util.Arrays.copyOf(payload, 109);
            payload[108] = (byte) 0xf2;
            var completion = forTable(payload(entry(83), sequence + 1, "CG", payload), table);
            byte[] raw = copy(completion, sequence + 1, cycle, null, producer, null).data();
            java.nio.ByteBuffer.wrap(raw).putInt(232, level);
            return List.of(copy(opening, sequence, 0L, null, producer, null),
                    new RawJournalPage.Entry(position(sequence + 1), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        }

        List<RawJournalPage.Entry> laterClear() {
            var result = new ArrayList<RawJournalPage.Entry>();
            result.add(copy(entry(75), 90, 90L, null, null, null));
            result.add(copy(entry(76), 91, 90L, null, null, null));
            result.add(payload(entry(78), 92, "CH", new byte[]{ (byte) 0xf3 }));
            for (long sequence = 79; sequence <= 82; sequence++) {
                result.add(copy(entry(sequence), sequence + 14, null, null, null, null));
            }
            byte[] clear = copy(entry(83), 97, 90L, null, null, null).data();
            clear[entry(83).header().getEntrySpecificDataOffset() + 16 + 108] = (byte) 0xf3;
            java.nio.ByteBuffer.wrap(clear).putInt(232, Math.toIntExact(entry(76).header().getControlInfo().nestedCommitLevel()));
            result.add(new RawJournalPage.Entry(position(97), new EntryHeaderDecoder(37).decode(clear, 0), clear));
            result.add(copy(entry(85), 98, 90L, 91L, null, null));
            return result;
        }

        As400BindingDefinition creationAndClear(boolean rollback) {
            var created = binding("ROLLBACKT", entry(59).header().getRecordInfo().objectJournalId(), entry(61).header().getRecordInfo().objectJournalId());
            // Native CREATE -> empty TRUNCATE -> INSERT shares one cycle.
            // An INSERT before TRUNCATE would instead fail with SQL0910.
            var journal = new ArrayList<>(range(58, 63));
            for (var item : laterClear()) {
                if (item.header().getEntryType().equals("SC")) {
                    continue;
                }
                var owned = copy(item, item.position().offset().longValueExact(), item.header().getCommitCycle().signum() == 0 ? 0L : 58L,
                        null, entry(59), null);
                journal.add(item.header().getJournalCode() == 'C' ? owned : forTable(owned, created));
            }
            var row = copy(entry(64), 101, 58L, null, entry(59), null);
            byte[] bytes = row.data();
            java.nio.ByteBuffer.wrap(bytes).putInt(row.header().getEntrySpecificDataOffset() + 16, 2);
            journal.add(new RawJournalPage.Entry(position(101), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
            journal.add(copy(entry(100), 102, 0L, null, null, null));
            journal.add(copy(entry(67), 104, 58L, null, null, rollback ? "RB" : "CM"));
            if (rollback) {
                for (var item : range(91, 94)) {
                    journal.add(forTable(copy(item, item.position().offset().longValueExact() + 14, 0L, null, entry(59), null), created));
                }
                journal.add(copy(entry(69), 109, 0L, null, null, null));
                journal.add(copy(entry(71), 110, 0L, null, null, null));
            }
            else {
                after.put("ROLLBACKT", created);
            }
            journal.add(copy(entry(100), 112, 0L, null, null, null));
            entries.clear();
            entries.addAll(journal);
            return created;
        }

        void abandonedCreationClear(boolean rollback) {
            var inserted = entry(64);
            creationAndClear(rollback);
            entries.add(6, copy(inserted, 64, 58L, null, null, null));
            // Native SQL0910 / fast-DELETE fallback: F/CH then C/SU,
            // before any journaling change or D/CG. Existing rows survive it.
            entries.removeIf(item -> item.position().offset().longValueExact() >= 93 && item.position().offset().longValueExact() <= 97
                    || item.position().offset().longValueExact() >= 105 && item.position().offset().longValueExact() <= 108);
            var release = entry(98);
            entries.set(entries.indexOf(release), copy(release, 98, 58L, 91L, null, "SU"));
        }

        As400BindingDefinition renamed(As400BindingDefinition original, String file) {
            var id = original.identity();
            var table = new TableId(id.tableId().catalog(), id.tableId().schema(), file);
            return new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(table, file, id.member(), id.fileJournalId(),
                    id.memberJournalId(), id.recordFormat()), original.schema().edit().tableId(table).create(), original.codecs(), original.members());
        }

        As400BindingDefinition sqlRenamed(As400BindingDefinition original, String name) {
            var id = original.identity();
            var renamedId = new TableId(id.tableId().catalog(), id.tableId().schema(), name);
            return new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(renamedId, id.physicalFile(), id.member(), id.fileJournalId(),
                    id.memberJournalId(), id.recordFormat()), original.schema().edit().tableId(renamedId).create(), original.codecs(), original.members());
        }

        List<RawJournalPage.Entry> rename(long sequence, As400BindingDefinition original, String target) {
            var result = new ArrayList<RawJournalPage.Entry>();
            for (boolean file : new boolean[]{ true, false }) {
                var template = file ? entry(83) : entry(78);
                byte[] names = new com.ibm.as400.access.AS400Text(60, 37).toBytes(String.format("%-10s%-10s%-10s%-10s%-10s%-10s",
                        original.identity().physicalFile(), receiver.library(), file ? "" : original.identity().member(),
                        target, receiver.library(), file ? "" : original.identity().member()));
                var changed = payload(template, sequence + result.size(), file ? "FN" : "MN", names);
                byte[] raw = changed.data();
                new com.ibm.as400.access.AS400Text(10, 37).toBytes(original.identity().physicalFile(), raw, 157);
                new com.ibm.as400.access.AS400Text(10, 37).toBytes(file ? "" : original.identity().member(), raw, 177);
                System.arraycopy(java.util.HexFormat.of().parseHex(file ? original.identity().fileJournalId() : original.identity().memberJournalId()),
                        0, raw, 197, 10);
                result.add(new RawJournalPage.Entry(changed.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
            }
            return result;
        }

        RawJournalPage.Entry atFile(RawJournalPage.Entry entry, String file) {
            byte[] raw = entry.data();
            var name = new com.ibm.as400.access.AS400Text(10, 37);
            name.toBytes(file, raw, 157);
            if (entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("CG")) {
                name.toBytes(file, raw, entry.header().getEntrySpecificDataOffset() + 16);
            }
            return new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        }

        RawJournalPage.Entry forTable(RawJournalPage.Entry original, As400BindingDefinition table) {
            byte[] raw = original.data();
            byte[] oldName = new com.ibm.as400.access.AS400Text(10, 37).toBytes(original.header().getFile());
            byte[] newName = new com.ibm.as400.access.AS400Text(10, 37).toBytes(table.identity().physicalFile());
            // Controlled interleaving of captured layouts: replace complete fixed-width
            // names in both the header and its object/member payload, and the exact JID.
            for (int at = 0; at <= raw.length - oldName.length; at++) {
                if (java.util.Arrays.equals(raw, at, at + oldName.length, oldName, 0, oldName.length)) {
                    System.arraycopy(newName, 0, raw, at, newName.length);
                }
            }
            String jid = original.header().getRecordInfo().objectType().equals("*FILE") ? table.identity().fileJournalId() : table.identity().memberJournalId();
            System.arraycopy(java.util.HexFormat.of().parseHex(jid), 0, raw, 197, 10);
            return new RawJournalPage.Entry(original.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        }

        List<RawJournalPage.Entry> drop(long sequence, As400BindingDefinition definition, String file, long cycle) {
            byte[] name = new com.ibm.as400.access.AS400Text(60, 37).toBytes(String.format("%-10s%-10s%-40s", file, receiver.library(), ""));
            var result = new ArrayList<RawJournalPage.Entry>();
            var opening = atFile(forTable(payload(entry(83), sequence, "DT", name), definition), file);
            result.add(copy(opening, sequence, cycle, null, entry(78), null));
            if (cycle > 0) {
                result.add(copy(entry(67), sequence + 1, cycle, null, entry(78), "CM"));
            }
            result.add(atFile(forTable(payload(entry(78), sequence + 2, "MD", new byte[0]), definition), file));
            result.add(atFile(forTable(payload(entry(83), sequence + 3, "DF", new byte[0]), definition), file));
            return result;
        }

        RawJournalPage.Entry widenedRow(long sequence, long cycle, RawJournalPage.Entry producer, As400BindingDefinition table) {
            var changed = entry(87);
            byte[] raw = changed.data();
            int indicators = changed.header().getNullValueOffest();
            java.nio.ByteBuffer.wrap(raw).putInt(indicators, 2).putInt(changed.header().getEntrySpecificDataOffset() + 16, 7001)
                    .putInt(changed.header().getEntrySpecificDataOffset() + 20, 7);
            raw[indicators + 4] = raw[indicators + 5] = (byte) 0xf0;
            return forTable(copy(new RawJournalPage.Entry(changed.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw),
                    sequence, cycle, null, producer, null), table);
        }

        As400BindingDefinition widened() {
            return widened(before);
        }

        As400BindingDefinition widened(As400BindingDefinition original) {
            var id = original.identity();
            return new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(id.tableId(), id.physicalFile(), id.member(),
                    id.fileJournalId(), id.memberJournalId(), id.physicalFile() + ":0123456789ABD:8:2:false"),
                    original.schema().edit().addColumn(Column.editor().name("X1").position(2).jdbcType(Types.INTEGER).type("INTEGER")
                            .length(4).optional(true).create()).create(),
                    List.of(new As400BindingDefinition.Codec("BIN4", 4, 0, 0), new As400BindingDefinition.Codec("BIN4", 4, 0, 0)));
        }

        void committedAlterWithEarlierLifecycleAndLaterOpenTransaction() {
            var source = entries.stream().collect(java.util.stream.Collectors.toMap(e -> e.position().offset().longValueExact(), e -> e));
            var producer = source.get(78L);
            entries.clear();
            entries.add(copy(source.get(56L), 1001, null, null, null, null));
            entries.add(copy(source.get(58L), 1002, 1002L, null, null, null));
            entries.add(copy(source.get(74L), 1003, null, null, null, null));
            entries.add(copy(source.get(75L), 1004, 1004L, null, null, null));
            entries.add(copy(source.get(76L), 1005, 1004L, null, null, null));
            entries.add(copy(source.get(77L), 1006, 1004L, null, null, null));
            for (long seq = 78; seq <= 82; seq++) {
                entries.add(copy(source.get(seq), 1007 + seq - 78, null, null, null, null));
            }
            entries.add(copy(source.get(83L), 1012, 1004L, null, null, null));
            entries.add(copy(source.get(84L), 1013, 1004L, 1006L, null, null));
            entries.add(copy(source.get(85L), 1014, 1004L, 1005L, null, null));
            entries.add(copy(source.get(90L), 1015, 1004L, null, producer, "CM"));
            entries.add(copy(source.get(95L), 1016, null, null, null, null));
            byte[] other = copy(source.get(58L), 1017, 1017L, null, null, null).data();
            java.nio.ByteBuffer.wrap(other).putLong(40, 999);
            new com.ibm.as400.access.AS400Text(6, 37).toBytes("000009", other, 121);
            entries.add(new RawJournalPage.Entry(position(1017), new EntryHeaderDecoder(37).decode(other, 0), other));
            long coordinate = 1018;
            for (long seq : new long[]{ 59, 60, 61, 62, 63, 64, 66, 67, 69, 71, 73, 100 }) {
                var entry = source.get(seq);
                entries.add(copy(entry, coordinate++, entry.header().getCommitCycle().signum() == 0 ? null : 1002L, null, null, null));
            }
            after.put("U", widened());
        }

        void openAlter(boolean rollback) {
            var producer = entry(78);
            var journal = new ArrayList<>(range(74, 85));
            journal.add(copy(entry(100), 86, null, null, null, null));
            journal.add(copy(entry(100), 87, 75L, null, producer, null));
            journal.add(widenedRow(88, 75, producer, before));
            journal.add(copy(entry(90), 90, 75L, null, producer, rollback ? "RB" : "CM"));
            if (rollback) {
                journal.addAll(range(91, 94));
            }
            journal.add(entry(100));
            entries.clear();
            entries.addAll(journal);
            if (!rollback) {
                after.put("U", widened());
            }
        }

        void sharedAlterAndDrop() {
            var journal = new ArrayList<>(range(75, 85));
            journal.add(widenedRow(87, 75, entry(78), before));
            journal.add(copy(entry(100), 88, 0L, null, null, null));
            journal.addAll(drop(90, before, "U", 75));
            journal.add(entry(100));
            entries.clear();
            entries.addAll(journal);
            after.put("U", null);
            historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                    entry.header().getLibrary(), entry.header().getFile(), "U", null);
        }

        void sharedAlterAndDropRollback() {
            var journal = new ArrayList<>(range(75, 85));
            var row = widenedRow(87, 75, entry(78), before);
            journal.add(row);
            journal.add(copy(entry(100), 88, 0L, null, null, null));
            journal.add(drop(90, before, "U", 75).get(0));
            journal.add(copy(row, 91, 75L, null, entry(78), "DR"));
            journal.add(copy(entry(90), 92, 75L, null, entry(78), "RB"));
            for (var restored : range(91, 94)) {
                journal.add(copy(restored, restored.position().offset().longValueExact() + 2, null, null, null, null));
            }
            journal.add(copy(entry(44), 98, 0L, null, null, null));
            journal.add(entry(100));
            entries.clear();
            entries.addAll(journal);
            after.put("U", before);
        }

        void alterWithInternalSavepoint(boolean repeated, String fault) throws Exception {
            var journal = new ArrayList<>(range(74, 85));
            var producer = entry(78);
            if (repeated) {
                journal.add(copy(entry(76), 86, 75L, null, producer, null));
                journal.add(copy(entry(77), 87, 75L, null, producer, null));
                for (long sequence = 78; sequence <= 83; sequence++) {
                    journal.add(copy(entry(sequence), sequence + 10, null, null, producer, null));
                }
                journal.add(copy(entry(84), 94, 75L, 87L, producer, null));
                journal.add(copy(entry(85), 95, 75L, 86L, producer, null));
            }
            journal.add(copy(entry(90), 96, 75L, null, producer, "CM"));
            journal.add(widenedRow(97, 0, producer, before));
            journal.add(entry(100));
            long boundary = repeated ? 90 : 80;
            var innerStart = copy(entry(77), boundary + 1, 75L, null, producer, null);
            var innerEnd = copy(entry(84), boundary + 2, 75L, boundary + 1, producer, fault.equals("rollback") ? "SU" : null);
            // Native renamed-file ALTER: D/EF, C/SB (one level deeper),
            // C/SQ targeting that SB, D/JF. The original ALTER scope survives.
            var revised = new ArrayList<RawJournalPage.Entry>();
            for (var entry : journal) {
                long sequence = entry.position().offset().longValueExact();
                Long target = entry.header().getEntryType().equals("SQ")
                        ? new io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder().decode(entry.header(), entry.data(), 0)
                                .savepointSequence().longValueExact()
                        : null;
                if (target != null && target > boundary) {
                    target += 2;
                }
                revised.add(copy(entry, sequence > boundary ? sequence + 2 : sequence, null, target, null, null));
                if (sequence == boundary) {
                    for (var control : List.of(innerStart, innerEnd)) {
                        if (fault.equals("missing-release") && control == innerEnd) {
                            continue;
                        }
                        byte[] raw = control.data();
                        java.nio.ByteBuffer.wrap(raw).putInt(232, fault.equals("outer-level") ? 3 : 4);
                        if (control == innerEnd && fault.equals("wrong-cycle")) {
                            java.nio.ByteBuffer.wrap(raw).putLong(64, 123);
                        }
                        if (control == innerEnd && fault.equals("wrong-producer")) {
                            raw[126] = (byte) 0xf9;
                        }
                        var changed = new RawJournalPage.Entry(control.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw);
                        revised.add(control == innerEnd && fault.equals("wrong-target")
                                ? copy(changed, boundary + 2, null, 77L, null, null)
                                : changed);
                    }
                }
            }
            entries.clear();
            entries.addAll(revised);
            after.put("U", widened());
        }

        As400BindingHistory history() {
            return new As400BindingHistory(As400BindingHistory.storage(config), config, policy.sourcePartition());
        }

        As400ContinuousRuntime runtime(As400Checkpoint checkpoint, As400BindingHistory.Recovered recovered,
                                       As400OnlineCensusRecovery.Output output, List<SourceRecord> records, boolean disk, long through) {
            var intervals = new As400ContinuousRuntime.Intervals() {
                @Override
                public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) {
                    var selected = entries.stream()
                            .filter(e -> !e.header().getFile().equals("PD") && e.position().offset().longValueExact() <= Math.min(through, cutThrough))
                            .filter(e -> scan.isPresent() ? order.compare(e.position(), scan.orElseThrow().position()) > 0
                                    : order.compare(e.position(), checkpoint.replayFrom()) >= (checkpoint.replayInclusive() ? 0 : 1))
                            .toList();
                    var visible = new As400JournalOrder(List.of(receiver), Map.of(receiver, "fixture-receiver"),
                            Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(cutThrough))));
                    return new As400OperatorCutReader.Cut(As400Checkpoint.fromOffset(durable, "fixture-journal", initial.captureFingerprint()),
                            visible, position(Math.min(through, cutThrough)), selected, List.of(), Optional.of(recovered));
                }

                @Override
                public As400SchemaChange schemaChange(As400BindingDefinition old, RawJournalPage.Entry opening, As400SourceNormalizer.TableResolver bindings)
                        throws Exception {
                    schemaReads++;
                    var evidence = range(opening.position().offset().longValueExact(), schemaThrough);
                    if (As400FileRename.moveOpening(opening)) {
                        return As400FileRename.move(old, evidence, order);
                    }
                    if (opening.header().getJournalCode() == 'D' && opening.header().getEntryType().equals("DT")) {
                        return As400FileDeletion.qualify(old, evidence, order);
                    }
                    if (old == null && historicalNames != null) {
                        return As400FileCreation.qualify(after.get(opening.header().getFile()), evidence, order,
                                entries.get(entries.size() - 1).position(), bindings, historicalNames, null);
                    }
                    if (As400MemberCreation.opening(old, opening)) {
                        return As400MemberCreation.qualify(old, evidence, order);
                    }
                    if (As400MemberCreation.deletionOpening(old, opening)) {
                        return As400MemberCreation.deletion(old, evidence, order);
                    }
                    if (As400MemberCreation.renameOpening(old, opening)) {
                        return As400MemberCreation.rename(old, opening, order);
                    }
                    var retired = As400FileDeletion.afterSchemaChange(old, evidence, order, bindings, historicalNames);
                    if (retired != null) {
                        return retired;
                    }
                    var prefix = As400SchemaChange.metadataBeforeTransition(old, evidence, order, bindings, historicalNames);
                    if (prefix != null) {
                        return prefix;
                    }
                    try {
                        return As400NativeSchemaRefresh.qualifyAcquired(old, after.getOrDefault(opening.header().getFile(), old),
                                evidence, order, entries.get(entries.size() - 1).position(), bindings, historicalNames);
                    }
                    catch (As400SchemaTimeline.PendingChangeException pending) {
                        var excluded = As400SchemaChange.excludedCommittedPrefix(old, evidence, order, bindings, historicalNames);
                        if (excluded != null) {
                            return excluded;
                        }
                        throw pending;
                    }
                }
            };
            var limits = new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(16, 1000, 128, payloadBudget),
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), 1000, false,
                    disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null);
            return new As400ContinuousRuntime(checkpoint.toOffset(), recovered, intervals, output.dispatcher, record -> {
                if (records.size() == failAfterRecords) {
                    throw new java.io.IOException("Interrupted test delivery");
                }
                return records.add(record);
            }, policy.sourcePartition(), () -> {
            }, limits, output.schemas::refresh);
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldRecoverAnAlterWithAnInternalSavepointWithoutReleasingItsOuterScope(boolean disk, boolean repeated) throws Exception {
        var f = new Fixture();
        f.alterWithInternalSavepoint(repeated, "valid");
        var saved = f.initial;
        for (int attempt = 0; attempt < 3; attempt++) {
            f.cutThrough = f.schemaThrough = attempt == 0 ? repeated ? 91 : 81 : 102;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 102)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                if (attempt == 0) {
                    assertThat(saved).isEqualTo(f.initial);
                    assertThat(records).isEmpty();
                    assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
                }
                else {
                    assertThat(saved.completedThrough()).isEqualTo(f.position(102));
                    var data = As400RecoveryTestSupport.data(records);
                    assertThat(data).hasSize(attempt == 1 ? 2 : 0);
                    if (attempt == 1) {
                        var row = records.stream().filter(record -> record.topic().endsWith(".U")).findFirst().orElseThrow();
                        assertThat(((Struct) row.value()).getStruct("after").getInt32("X1")).isEqualTo(7);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRecoverACommittedAlterBeforeALaterFullyRolledBackAlter(boolean disk) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(75, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, f.entry(78), "CM"));
        for (var item : f.range(75, 85)) {
            Long target = item.header().getJournalCode() == 'C' && item.header().getEntryType().equals("SQ")
                    ? new io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder().decode(item.header(), item.data(), 0)
                            .savepointSequence().longValueExact() + 25
                    : null;
            journal.add(f.copy(item, item.position().offset().longValueExact() + 25,
                    item.header().getCommitCycle().signum() > 0 ? 100L : null, target, null, null));
        }
        journal.add(f.copy(f.entry(90), 111, 100L, null, null, null));
        for (var item : f.range(91, 94)) {
            journal.add(f.copy(item, item.position().offset().longValueExact() + 21, null, null, null, null));
        }
        journal.add(f.copy(f.entry(100), 120, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", f.widened());
        assertThatThrownBy(() -> As400SchemaChange.qualify(f.before, f.widened(), f.range(78, 120), f.order, f.position(111)))
                .isInstanceOf(As400SchemaChange.MetadataMovedException.class);
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 120)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(120));
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 0 ? 1 : 0);
                var first = recovered.schemaHistory().find(f.position(78)).change();
                var second = recovered.schemaHistory().find(f.position(103)).change();
                assertThat(first.after()).isEqualTo(f.widened());
                assertThat(first.rolledBackAlter()).isFalse();
                assertThat(second.before()).isEqualTo(first.after());
                assertThat(second.after()).isEqualTo(first.after());
                assertThat(second.rolledBackAlter()).isTrue();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldPublishUnrelatedRowsWhileALaterExcludedAlterIsOpenAcrossRestart(boolean disk) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(75, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, f.entry(78), "CM"));
        for (var item : f.range(75, 85)) {
            Long target = item.header().getJournalCode() == 'C' && item.header().getEntryType().equals("SQ")
                    ? new io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder().decode(item.header(), item.data(), 0)
                            .savepointSequence().longValueExact() + 25
                    : null;
            journal.add(f.copy(item, item.position().offset().longValueExact() + 25,
                    item.header().getCommitCycle().signum() > 0 ? 100L : null, target, null, null));
        }
        journal.add(f.copy(f.entry(100), 112, 0L, null, null, null));
        journal.add(f.copy(f.entry(90), 120, 100L, null, null, null));
        for (var item : f.range(91, 94)) {
            journal.add(f.copy(item, item.position().offset().longValueExact() + 30, null, null, null, null));
        }
        journal.add(f.widenedRow(130, 0, f.entry(78), f.before));
        journal.add(f.copy(f.entry(100), 131, 0L, null, null, null));
        journal.add(f.widenedRow(140, 0, f.entry(78), f.before));
        journal.add(f.copy(f.entry(100), 145, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", f.widened());
        f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                entry.header().getLibrary(), "U", "U", null);
        var saved = f.initial;
        for (long through : new long[]{ 112, 112, 131, 145, 145 }) {
            f.cutThrough = f.schemaThrough = through;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            boolean expanded = through == 145;
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false,
                    f.receiver.library() + (expanded ? "\\.(A|U)" : "\\.A"));
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                var previous = saved.completedThrough();
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                var data = As400RecoveryTestSupport.data(records);
                assertThat(data).hasSize(previous.equals(saved.completedThrough()) ? 0 : expanded ? 2 : 1);
                if (!expanded) {
                    assertThat(records).noneMatch(record -> record.topic().endsWith(".U"));
                }
                else if (!data.isEmpty()) {
                    var restored = records.stream().filter(record -> record.topic().endsWith(".U")).findFirst().orElseThrow();
                    assertThat(((Struct) restored.value()).getStruct("after").getInt32("X1")).isEqualTo(7);
                }
                assertThat(recovered.schemaHistory().find(f.position(78)).change().after().unobservedLayout()).isTrue();
                if (through == 112) {
                    assertThat(saved.replayFrom()).isEqualTo(f.position(100));
                    assertThat(saved.replayInclusive()).isTrue();
                }
                if (through >= 131) {
                    var rollback = recovered.schemaHistory().find(f.position(103)).change();
                    assertThat(rollback.rolledBackAlter()).isTrue();
                    assertThat(rollback.before().unobservedLayout()).isTrue();
                    assertThat(rollback.after()).isEqualTo(f.widened());
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "stable", "later-metadata", "later-key", "different-object", "known-layout" })
    void shouldReacquireOnlyTheStableRestoredLayoutOfAnExcludedIdentity(String scenario) throws Exception {
        var f = new Fixture();
        var unknown = scenario.equals("known-layout") ? f.before : f.before.withoutObservedLayout();
        var acquired = f.widened();
        var journal = new ArrayList<>(f.range(78, 94));
        if (scenario.equals("later-metadata")) {
            journal.addAll(f.metadataChange(100, f.before, 0, 0, f.entry(78)));
        }
        if (scenario.equals("later-key")) {
            journal.add(f.payload(f.entry(83), 100, "AC", new byte[0]));
        }
        if (scenario.equals("different-object")) {
            var id = acquired.identity();
            acquired = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(id.tableId(), id.physicalFile(), id.member(),
                    "00000000000000009999", id.memberJournalId(), id.recordFormat()), acquired.schema(), acquired.codecs(), acquired.members());
            var mismatched = acquired;
            assertThatThrownBy(() -> As400SchemaChange.qualify(unknown, mismatched, journal, f.order, f.position(110)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cannot replace a file/member");
        }
        else {
            var change = As400SchemaChange.qualify(unknown, acquired, journal, f.order, f.position(110));
            assertThat(change.rolledBackAlter()).isTrue();
            assertThat(change.after()).isEqualTo(scenario.equals("stable") ? acquired : unknown);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "wrong-target", "wrong-cycle", "wrong-producer", "outer-level", "missing-release", "rollback" })
    void shouldRejectAnAlterWhoseInternalSavepointDoesNotCloseItsExactScope(String fault) throws Exception {
        var f = new Fixture();
        f.alterWithInternalSavepoint(false, fault);
        assertThatThrownBy(() -> As400SchemaChange.qualify(f.before, f.widened(), f.range(78, 102), f.order, f.position(102)))
                .isInstanceOf(RuntimeException.class);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 102)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
            assertThat(records).isEmpty();
            assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid-member", "valid-file", "valid-chgpf", "wrong-producer", "wrong-identity", "wrong-file-jid", "changed-schema", "changed-chgpf",
            "clear-subtype",
            "missing-completion" })
    void shouldQualifyMiscellaneousMetadataChangesWithoutHidingLayoutOrIdentityChanges(String scenario) throws Exception {
        var f = new Fixture();
        var opening = f.payload(f.entry(78), 110, "CH", new byte[]{ 0x40 });
        if (scenario.equals("valid-file") || scenario.equals("wrong-file-jid")) {
            byte[] file = opening.data();
            System.arraycopy(f.entry(83).data(), 157, file, 157, 50);
            System.arraycopy(f.entry(83).data(), 219, file, 219, 10);
            if (scenario.equals("wrong-file-jid")) {
                System.arraycopy(f.entry(78).data(), 197, file, 197, 10);
            }
            opening = new RawJournalPage.Entry(opening.position(), new EntryHeaderDecoder(37).decode(file, 0), file);
        }
        byte[] payload = As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(f.entry(83).header(), f.entry(83).data(), f.entry(83).position()));
        payload = java.util.Arrays.copyOf(payload, 109);
        payload[108] = scenario.equals("clear-subtype") ? (byte) 0xf8 : scenario.endsWith("chgpf") ? (byte) 0xf1 : (byte) 0xf2;
        var completion = f.payload(f.entry(83), 111, "CG", payload);
        byte[] raw = completion.data();
        if (scenario.equals("wrong-producer")) {
            raw[126] = (byte) 0xf9;
        }
        if (scenario.equals("wrong-identity")) {
            raw[206] ^= 1;
        }
        completion = new RawJournalPage.Entry(completion.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        var entries = scenario.equals("missing-completion") ? List.of(opening) : List.of(opening, completion);
        if (!scenario.startsWith("valid-")) {
            String message = switch (scenario) {
                case "missing-completion" -> "has not completed";
                case "wrong-producer" -> "matching D/CG";
                case "wrong-identity", "wrong-file-jid" -> "identity mismatch";
                case "changed-schema", "changed-chgpf" -> "altered a captured table";
                default -> "identity or subtype differs";
            };
            assertThatThrownBy(() -> As400SchemaChange.qualify(f.before, scenario.startsWith("changed-") ? f.widened() : f.before,
                    entries, f.order, f.position(111))).isInstanceOf(RuntimeException.class).hasMessageContaining(message);
            return;
        }
        // Two ordinary metadata statements can be observed together.
        f.entries.clear();
        f.entries.addAll(entries);
        f.entries.add(f.payload(opening, 112, "CH", new byte[]{ 0x40 }));
        f.entries.add(f.payload(completion, 113, "CG", payload));
        var change = As400SchemaChange.qualify(f.before, f.before, f.entries, f.order, f.position(113));
        assertThat(change.records()).hasSize(4);
        assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
        var saved = f.initial;
        for (int restart = 0; restart < 2; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, false, 113)) {
                var progress = runtime.poll(saved.toOffset());
                assertThat(progress.scan()).contains(f.position(113));
                saved = As400Checkpoint.fromOffset(progress.checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            assertThat(records).isEmpty();
        }
    }

    @ParameterizedTest
    @CsvSource({ "before,false", "before,true", "after,false", "after,true", "transaction,false", "transaction,true", "create,false", "create,true", "inside,false",
            "inside,true" })
    void shouldReplayMetadataNeighborsWithoutLosingTheSchemaBoundary(String placement, boolean disk) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (placement.equals("create")) {
            var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
            for (var entry : f.range(59, 63)) {
                journal.add(f.copy(entry, entry.position().offset().longValueExact(), 0L, null, null, null));
            }
            journal.addAll(f.metadataChange(65, created, 0, 0, f.entry(59)));
            journal.add(f.copy(f.entry(64), 68, 0L, null, null, null));
            f.after.put("ROLLBACKT", created);
        }
        else if (placement.equals("transaction")) {
            journal.addAll(f.range(74, 77));
            journal.addAll(f.metadataChange(78, f.before, 75, 3, f.entry(78)));
            journal.add(f.copy(f.entry(84), 80, null, null, null, null));
            journal.add(f.copy(f.entry(85), 81, null, null, null, null));
            journal.add(f.copy(f.entry(90), 82, 75L, null, f.entry(78), "CM"));
        }
        else {
            if (placement.equals("before")) {
                journal.addAll(f.metadataChange(70, f.before, 0, 0, f.entry(78)));
            }
            journal.addAll(f.range(74, 85));
            if (placement.equals("inside")) {
                journal.add(f.copy(f.entry(76), 86, 75L, null, f.entry(78), null));
                journal.addAll(f.metadataChange(87, f.before, 75, 2, f.entry(78)));
                journal.add(f.copy(f.entry(85), 89, 75L, 86L, f.entry(78), null));
                journal.add(f.copy(f.entry(90), 90, 75L, null, f.entry(78), "CM"));
            }
            else {
                journal.add(f.copy(f.entry(90), 86, 75L, null, f.entry(78), "CM"));
            }
            if (placement.equals("after")) {
                journal.addAll(f.metadataChange(87, f.before, 0, 0, f.entry(78)));
            }
            journal.add(f.widenedRow(placement.equals("inside") ? 91 : 89, 0, f.entry(78), f.before));
            f.after.put("U", f.widened());
        }
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        var saved = f.initial;
        for (int restart = 0; restart < 2; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 100)) {
                var progress = runtime.poll(saved.toOffset());
                assertThat(progress.checkpoint()).containsEntry("completed_through.sequence", "100");
                saved = As400Checkpoint.fromOffset(progress.checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            if (restart == 0) {
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(placement.equals("transaction") ? 1 : 2);
                if (placement.equals("before") || placement.equals("after") || placement.equals("inside")) {
                    var changed = records.stream().filter(record -> record.topic().endsWith(".U")).findFirst().orElseThrow();
                    assertThat(((org.apache.kafka.connect.data.Struct) changed.value()).getStruct("after").getInt32("X1")).isEqualTo(7);
                }
            }
            else {
                assertThat(records).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "cycle", "level", "missing-marker", "missing-pair" })
    void shouldRejectUnprovenTransactionalMetadataWithoutPublishingOrAdvancing(String fault) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(74, fault.equals("missing-marker") ? 76 : 77));
        var pair = f.metadataChange(78, f.before, fault.equals("cycle") ? 58 : 75, fault.equals("level") ? 2 : 3, f.entry(78));
        journal.addAll(fault.equals("missing-pair") ? pair.subList(0, 1) : pair);
        if (!fault.equals("missing-pair")) {
            journal.add(f.copy(f.entry(84), 80, null, null, null, null));
            journal.add(f.copy(f.entry(85), 81, null, null, null, null));
            journal.add(f.copy(f.entry(90), 82, 75L, null, f.entry(78), "CM"));
        }
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 100)) {
            if (fault.equals("missing-pair")) {
                var progress = runtime.poll(f.initial.toOffset());
                assertThat(progress.deferred()).isTrue();
                assertThat(progress.checkpoint()).containsEntry("completed_through.sequence", "44");
            }
            else {
                assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
            }
        }
        assertThat(records).isEmpty();
    }

    @Test
    void shouldRejectCapturedRowsBetweenMetadataAndAnUnavailableLaterDefinition() throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.metadataChange(70, f.before, 0, 0, f.entry(78)));
        journal.add(f.copy(f.entry(44), 72, 0L, null, null, null));
        journal.addAll(f.range(74, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, f.entry(78), "CM"));
        assertThat(As400SchemaChange.metadataBeforeTransition(f.before, journal, f.order, (file, library) -> f.before.binding(65536))).isNull();
        assertThatThrownBy(() -> As400SchemaChange.qualify(f.before, f.widened(), journal, f.order, f.position(86)))
                .hasMessageContaining("Captured rows require an unavailable intermediate table definition");
    }

    @ParameterizedTest
    @CsvSource({ "3,false", "3,true", "8,false", "8,true" })
    void shouldPublishALaterClearAndReloadAfterRecoveringThePrecedingAlter(char subtype, boolean disk) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(74, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, f.entry(78), "CM"));
        journal.add(f.copy(f.entry(75), 90, 90L, null, null, null));
        journal.add(f.copy(f.entry(76), 91, 90L, null, null, null));
        journal.add(f.payload(f.entry(78), 92, "CH", new com.ibm.as400.access.AS400Text(1, 37).toBytes(String.valueOf(subtype))));
        for (long sequence = 79; sequence <= 82; sequence++) {
            journal.add(f.copy(f.entry(sequence), sequence + 14, null, null, null, null));
        }
        byte[] clear = f.copy(f.entry(83), 97, 90L, null, null, null).data();
        clear[f.entry(83).header().getEntrySpecificDataOffset() + 16 + 108] = subtype == '3' ? (byte) 0xf3 : (byte) 0xf8;
        java.nio.ByteBuffer.wrap(clear).putInt(232, Math.toIntExact(f.entry(76).header().getControlInfo().nestedCommitLevel()));
        journal.add(new RawJournalPage.Entry(f.position(97), new EntryHeaderDecoder(37).decode(clear, 0), clear));
        journal.add(f.copy(f.entry(85), 98, 90L, 91L, null, null));
        journal.add(f.widenedRow(99, 90, f.entry(78), f.widened()));
        journal.add(f.copy(f.entry(90), 100, 90L, null, f.entry(78), "CM"));
        f.after.put("U", f.widened());
        f.entries.clear();
        f.entries.addAll(journal);
        var saved = f.initial;
        for (int restart = 0; restart < 3; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, restart == 0 ? 98 : 100)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            if (restart == 1) {
                var values = records.stream().filter(record -> record.topic().endsWith(".U") && record.value() != null)
                        .map(record -> (org.apache.kafka.connect.data.Struct) record.value()).toList();
                assertThat(values).extracting(value -> value.getString("op"))
                        .containsExactly("t", "c");
                assertThat(values.get(1).getStruct("after").getInt32("X1")).isEqualTo(7);
            }
            else {
                assertThat(As400RecoveryTestSupport.data(records)).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldRetainRenameIdentityAcrossUnrelatedProgressAndFreshRestart(boolean roundTrip, boolean disk) throws Exception {
        var f = new Fixture();
        var renamed = f.renamed(f.before, "RENAMED");
        var pair = f.rename(110, f.before, "RENAMED");
        var journal = new ArrayList<RawJournalPage.Entry>();
        journal.add(pair.get(0));
        journal.add(f.copy(f.entry(100), 111, 0L, null, null, null));
        journal.add(f.copy(pair.get(1), 112, 0L, null, null, null));
        f.metadataChange(113, f.before, 0, 0, f.entry(78)).forEach(entry -> journal.add(f.atFile(entry, "RENAMED")));
        if (roundTrip) {
            journal.addAll(f.rename(115, renamed, "U"));
        }
        var target = roundTrip ? f.before : renamed;
        journal.add(f.atFile(f.copy(f.entry(44), 120, 0L, null, null, null), target.identity().physicalFile()));
        f.after.put("U", target);
        f.entries.clear();
        f.entries.addAll(journal);
        var saved = f.initial;
        for (int restart = 0; restart < 3; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, restart == 0 ? 111 : 120)) {
                var progress = runtime.poll(saved.toOffset());
                saved = As400Checkpoint.fromOffset(progress.checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                if (!roundTrip) {
                    assertThatThrownBy(() -> output.schemas.schemaFor(f.before.schema().id())).hasMessageContaining("absent from recovered native schema history");
                    assertThat(output.schemas.schemaFor(renamed.schema().id())).isNotNull();
                }
            }
            if (restart == 0) {
                assertThat(saved.toOffset()).containsEntry("completed_through.sequence", "111").containsEntry("replay_from.sequence", "110");
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(1);
            }
            else if (restart == 1) {
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(1);
                assertThat(records.stream().filter(r -> r.topic().endsWith("." + target.schema().id().table())).count()).isEqualTo(1);
            }
            else {
                assertThat(records).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "file-jid", "member-jid", "cycle", "producer", "library", "missing-member", "changed-layout", "intermediate-rows" })
    void shouldRejectRenameWithoutCompleteMatchingIdentityAndDefinition(String fault) throws Exception {
        var f = new Fixture();
        var after = f.renamed(f.before, "RENAMED");
        var entries = new ArrayList<>(f.rename(110, f.before, "RENAMED"));
        int index = fault.equals("file-jid") ? 0 : 1;
        byte[] raw = entries.get(index).data();
        switch (fault) {
            case "file-jid", "member-jid" -> raw[206] ^= 1;
            case "cycle" -> java.nio.ByteBuffer.wrap(raw).putLong(64, 75);
            case "producer" -> raw[126] = (byte) 0xf9;
            case "library" -> new com.ibm.as400.access.AS400Text(10, 37).toBytes("ELSEWHERE", raw,
                    entries.get(index).header().getEntrySpecificDataOffset() + 16 + 40);
        }
        entries.set(index, new RawJournalPage.Entry(entries.get(index).position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        if (fault.equals("missing-member")) {
            entries.remove(1);
        }
        if (fault.equals("changed-layout")) {
            after = f.widened(after);
        }
        if (fault.equals("intermediate-rows")) {
            entries.add(f.atFile(f.copy(f.entry(44), 112, 0L, null, null, null), "RENAMED"));
            entries.addAll(f.rename(113, after, "U"));
            after = f.before;
        }
        var definition = after;
        assertThatThrownBy(() -> As400FileRename.qualify(f.before, definition, entries, f.order, (file, library) -> f.before.binding(65536)))
                .isInstanceOf(RuntimeException.class);
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldRecoverTheOldNameMetadataPrefixBeforeAPhysicalRenameOfLongSqlNames(boolean disk, boolean invalidMember) throws Exception {
        var f = new Fixture(true);
        var alias = f.renamed(f.before, "RENAMED");
        var id = alias.identity();
        var sqlName = new TableId(null, f.receiver.library(), "LONG_RENAMED_NAME");
        var renamed = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(sqlName, id.physicalFile(), id.member(), id.fileJournalId(),
                id.memberJournalId(), id.recordFormat()), alias.schema().edit().tableId(sqlName).create(), alias.codecs(), alias.members());
        var journal = new ArrayList<>(f.range(75, 76));
        journal.addAll(f.metadataChange(78, f.before, 75, Math.toIntExact(f.entry(76).header().getControlInfo().nestedCommitLevel()), f.entry(78)));
        journal.add(f.copy(f.entry(85), 80, 75L, 76L, null, null));
        journal.add(f.copy(f.entry(90), 81, 75L, null, null, "CM"));
        var pair = f.rename(82, f.before, "RENAMED");
        journal.add(pair.get(0));
        byte[] member = pair.get(1).data();
        if (invalidMember) {
            member[197] ^= 1;
        }
        journal.add(new RawJournalPage.Entry(pair.get(1).position(), new EntryHeaderDecoder(37).decode(member, 0), member));
        journal.add(f.atFile(f.copy(f.entry(44), 85, 0L, null, null, null), "RENAMED"));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", renamed);
        var saved = f.initial;
        for (int attempt = 0; attempt < (invalidMember ? 1 : 3); attempt++) {
            var initial = saved;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, attempt == 0 ? 81 : 100)) {
                if (invalidMember) {
                    assertThatThrownBy(() -> runtime.poll(initial.toOffset())).hasStackTraceContaining("exact file/member association");
                    assertThat(records).isEmpty();
                    assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(attempt == 0 ? 81 : 100));
                    assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 1 ? 2 : 0);
                    if (attempt == 1) {
                        assertThat(records.stream().filter(record -> record.topic().endsWith(".LONG_RENAMED_NAME"))).hasSize(1);
                        assertThat(records).noneMatch(record -> record.topic().endsWith(".LONG_ORIGINAL_NAME"));
                    }
                }
            }
        }
    }

    @Test
    void shouldRetainThePublishedKeyWhenARenameObservesLaterConstraintState() throws Exception {
        var f = new Fixture();
        var renamed = f.renamed(f.before, "RENAMED");
        var acquired = new As400BindingDefinition(renamed.identity(), renamed.schema().edit().setPrimaryKeyNames(List.of()).create(),
                renamed.codecs(), renamed.members());
        var change = As400FileRename.qualify(f.before, acquired, f.rename(110, f.before, "RENAMED"), f.order,
                (file, library) -> f.before.binding(65536));
        assertThat(change.after()).isEqualTo(renamed);
        assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
    }

    @ParameterizedTest
    @CsvSource({ "false,both", "true,both", "false,old", "true,old", "false,new", "true,new", "false,neither", "true,neither" })
    void shouldRecoverASqlOnlyRenameWithTheSamePhysicalIdentityAndConfiguredSelection(boolean disk, String selection) throws Exception {
        var f = new Fixture(true);
        var renamed = f.sqlRenamed(f.before, "LONG_RENAMED_NAME");
        var producer = f.entry(78);
        var journal = new ArrayList<RawJournalPage.Entry>();
        journal.add(f.copy(f.entry(75), 110, 110L, null, producer, null));
        journal.add(f.copy(f.entry(76), 111, 110L, null, producer, null));
        journal.addAll(f.metadataChange(112, f.before, 110, 2, producer));
        journal.add(f.copy(f.entry(85), 114, 110L, 111L, producer, null));
        journal.add(f.copy(f.entry(67), 115, 110L, null, producer, "CM"));
        journal.add(f.copy(f.entry(44), 116, 0L, null, null, null));
        journal.add(f.copy(f.entry(100), 117, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", renamed);
        String included = "DCNC0005." + switch (selection) {
            case "both" -> "(A|LONG_ORIGINAL_NAME|LONG_RENAMED_NAME)";
            case "old" -> "(A|LONG_ORIGINAL_NAME)";
            case "new" -> "(A|LONG_RENAMED_NAME)";
            default -> "A";
        };
        boolean publishes = selection.equals("both") || selection.equals("new");
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, included);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 117)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(117));
                var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
                assertThat(data).hasSize(attempt == 0 ? publishes ? 2 : 1 : 0);
                assertThat(data).noneMatch(record -> record.topic().endsWith(".LONG_ORIGINAL_NAME"));
                if (attempt == 0 && publishes) {
                    assertThat(data.get(0).topic()).endsWith(".LONG_RENAMED_NAME");
                    assertThat(((Struct) data.get(0).key()).getInt32("ID")).isEqualTo(3);
                }
                var change = recovered.schemaHistory().find(f.position(112)).change();
                assertThat(change.after()).isEqualTo(renamed);
                assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,true", "true,true", "false,false", "true,false" })
    void shouldRecoverAnAlterBeforeALaterPhysicalRenameWithoutMovingEarlierRowsToTheNewTopic(boolean disk, boolean selected) throws Exception {
        var f = new Fixture();
        var changed = f.widened();
        var renamed = f.renamed(changed, "RENAMED");
        var producer = f.entry(78);
        var journal = new ArrayList<>(f.range(74, 85));
        journal.add(f.copy(f.entry(67), 86, 75L, null, producer, "CM"));
        journal.add(f.widenedRow(87, 0, producer, changed));
        journal.add(f.copy(f.entry(100), 88, 0L, null, null, null));
        journal.addAll(f.rename(90, changed, "RENAMED"));
        journal.add(f.atFile(f.widenedRow(94, 0, producer, changed), "RENAMED"));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", renamed);
        var saved = f.initial;
        for (long through : new long[]{ 88, 100, 100 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            boolean replay = saved.completedThrough().equals(f.position(100));
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, selected ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
                assertThat(data).hasSize(replay ? 0 : selected ? 2 : 1);
                if (!replay && selected) {
                    assertThat(data.get(0).topic()).endsWith(through == 88 ? ".U" : ".RENAMED");
                    assertThat(((Struct) data.get(0).value()).getStruct("after").getInt32("X1")).isEqualTo(7);
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldRecoverAnEmptyCreationRenamedBeforeItsMetadataIsAcquired(boolean disk, boolean transactional) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var renamed = f.renamed(created, "RENAMED");
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (transactional) {
            journal.add(f.entry(58));
        }
        for (var entry : f.range(59, 63)) {
            journal.add(f.copy(entry, entry.position().offset().longValueExact(), transactional ? null : 0L, null, null, null));
        }
        journal.add(f.copy(f.entry(100), 65, 0L, null, null, null));
        if (transactional) {
            journal.add(f.copy(f.entry(67), 67, 58L, null, null, "CM"));
        }
        journal.addAll(f.rename(90, created, "RENAMED"));
        journal.add(f.atFile(f.copy(f.entry(64), 94, 0L, null, null, null), "RENAMED"));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", renamed);
        assertThat(As400SchemaChange.qualify(null, null, f.range(59, 100), f.order, f.position(100))).isNull();
        assertThat(As400FileCreation.acquiredLocation(f.range(59, 100), f.order))
                .isEqualTo(new As400FileCreation.CatalogLocation("RENAMED", "DCNC0005"));
        var saved = f.initial;
        for (long through : new long[]{ 65, 100, 100 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            boolean replay = saved.completedThrough().equals(f.position(100));
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(replay ? 0 : through == 65 ? 1 : 2);
                assertThat(records).noneMatch(record -> record.topic().endsWith(".ROLLBACKT"));
                if (through == 65) {
                    assertThat(saved.replayFrom()).isEqualTo(f.position(transactional ? 58 : 59));
                    assertThat(saved.replayInclusive()).isTrue();
                }
                else if (!replay) {
                    assertThat(records).anyMatch(record -> record.topic().endsWith(".RENAMED"));
                    var change = recovered.schemaHistory().find(f.position(59)).change();
                    assertThat(change.after()).isEqualTo(renamed);
                    assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "earlier-row", "member-identity", "catalog-name", "catalog-identity", "name-collision" })
    void shouldRejectAnAmbiguousCreationRenameBeforePublishingOrRecordingHistory(String fault) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        String name = fault.equals("name-collision") ? "A" : "RENAMED";
        var acquired = f.renamed(created, name);
        var journal = new ArrayList<RawJournalPage.Entry>();
        for (var entry : f.range(59, fault.equals("earlier-row") ? 64 : 63)) {
            journal.add(f.copy(entry, entry.position().offset().longValueExact(), 0L, null, null, null));
        }
        journal.addAll(f.rename(90, created, name));
        if (fault.equals("member-identity")) {
            var entry = journal.remove(journal.size() - 1);
            byte[] bytes = entry.data();
            bytes[206] ^= 1;
            journal.add(new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        if (!fault.equals("name-collision")) {
            journal.add(f.entry(100));
        }
        if (fault.equals("catalog-name")) {
            acquired = created;
        }
        if (fault.equals("catalog-identity")) {
            acquired = f.binding(name, "0000000000000000fffe", created.identity().memberJournalId());
        }
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", acquired);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 100)) {
            String cause = switch (fault) {
                case "earlier-row" -> "unavailable earlier SQL name";
                case "member-identity" -> "exact file/member association";
                case "name-collision" -> "collides with another retained file";
                default -> "catalog differs from the renamed creation";
            };
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).hasStackTraceContaining(cause);
            assertThat(records).isEmpty();
            assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "drop", "rename", "move", "already-observed", "foreign-file", "foreign-library", "foreign-jid", "member", "unrelated" })
    void shouldRetryAMissingCatalogOnlyForANewerDisappearanceOfTheSameFile(String scenario) throws Exception {
        var f = new Fixture();
        var original = scenario.equals("move") ? fileMove(f, 120, "DCNC0005", "DCNC0006").get(0)
                : scenario.equals("rename") ? f.rename(120, f.before, "RENAMED").get(0) : f.drop(120, f.before, "U", 0).get(0);
        byte[] raw = original.data();
        var text = new com.ibm.as400.access.AS400Text(10, 37);
        switch (scenario) {
            case "foreign-file" -> text.toBytes("OTHER", raw, 157);
            case "foreign-library" -> text.toBytes("OTHER", raw, 167);
            case "foreign-jid" -> raw[206] ^= 1;
            case "member" -> text.toBytes("*QDDS", raw, 219);
            case "unrelated" -> new com.ibm.as400.access.AS400Text(2, 37).toBytes("GT", raw, 99);
        }
        var changed = new RawJournalPage.Entry(original.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        assertThat(As400NativeSchemaRefresh.disappearedAfter(f.entry(78), "U", f.before.identity().fileJournalId(),
                f.position(scenario.equals("already-observed") ? 120 : 110),
                List.of(changed), f.order)).isEqualTo(scenario.equals("drop") || scenario.equals("rename") || scenario.equals("move"));
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldRecoverARowFreeAlterThenDropWithoutAcquiringTheDeletedDefinition(boolean disk, boolean transactionalDrop) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(75, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, null, "CM"));
        journal.add(f.copy(f.entry(100), 90, 0L, null, null, null));
        if (transactionalDrop) {
            journal.add(f.copy(f.entry(75), 115, 115L, null, f.entry(78), null));
        }
        journal.addAll(f.drop(120, f.before, "U", transactionalDrop ? 115 : 0));
        journal.add(f.copy(f.entry(100), 125, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", null);
        var saved = f.initial;
        for (long through : new long[]{ 86, 125, 125 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            boolean replay = saved.completedThrough().equals(f.position(125));
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(replay || through == 86 ? 0 : 2);
                assertThat(records).noneMatch(record -> record.topic().endsWith(".U"));
                if (through == 86) {
                    assertThat(saved.replayFrom().offset()).isLessThanOrEqualTo(BigInteger.valueOf(78));
                }
                var change = recovered.schemaHistory().find(f.position(78)).change();
                assertThat(change.after()).isNull();
                assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "row", "missing-member", "missing-deletion", "foreign-file", "rollback" })
    void shouldRejectAnUnprovenAlterThenDropWithoutDiscardingRowsOrRecordingHistory(String fault) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(75, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, null, "CM"));
        if (fault.equals("row")) {
            journal.add(f.copy(f.entry(44), 90, 0L, null, null, null));
        }
        var drop = new ArrayList<>(f.drop(120, f.before, "U", 0));
        if (fault.equals("missing-member")) {
            drop.remove(1);
        }
        if (fault.equals("missing-deletion")) {
            drop.remove(2);
        }
        if (fault.equals("foreign-file")) {
            byte[] bytes = drop.get(0).data();
            bytes[206] ^= 1;
            drop.set(0, new RawJournalPage.Entry(drop.get(0).position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        if (fault.equals("rollback")) {
            journal.add(f.copy(f.entry(75), 115, 115L, null, f.entry(78), null));
            drop = new ArrayList<>(List.of(f.drop(120, f.before, "U", 115).get(0), f.copy(f.entry(90), 121, 115L, null, null, "RB")));
        }
        journal.addAll(drop);
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", null);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 125)) {
            if (fault.equals("missing-deletion")) {
                runtime.poll(f.initial.toOffset());
            }
            else {
                assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(Exception.class);
            }
            assertThat(records).noneMatch(record -> record.topic().endsWith(".U"));
            assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldRecoverARenameThenDropWithoutInventingTheVanishedSqlName(boolean disk, boolean transactional) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.rename(110, f.before, "RENAMED"));
        journal.add(f.copy(f.entry(100), 112, 0L, null, null, null));
        var limit = f.atFile(f.payload(f.entry(78), 113, "IL", new byte[8]), "RENAMED");
        byte[] limitBytes = limit.data();
        new com.ibm.as400.access.AS400Text(1, 37).toBytes("R", limitBytes, 98);
        journal.add(new RawJournalPage.Entry(limit.position(), new EntryHeaderDecoder(37).decode(limitBytes, 0), limitBytes));
        if (transactional) {
            journal.add(f.copy(f.entry(75), 115, 115L, null, f.entry(78), null));
        }
        journal.addAll(f.drop(120, f.before, "RENAMED", transactional ? 115 : 0));
        journal.add(f.copy(f.entry(100), 124, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", null); // The dropped object has no catalog definition.
        var saved = f.initial;
        for (long through : new long[]{ 112, 124, 124 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            boolean replay = saved.completedThrough().equals(f.position(124));
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(replay ? 0 : 1);
                assertThat(records).noneMatch(record -> record.topic().endsWith(".U") || record.topic().endsWith(".RENAMED"));
                if (through == 112) {
                    assertThat(saved.replayFrom()).isEqualTo(f.position(110));
                    assertThat(saved.replayInclusive()).isTrue();
                }
                var change = recovered.schemaHistory().find(f.position(110)).change();
                assertThat(change.after()).isNull();
                assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldKeepAReplacementBindingWhenTheRenamedOldFileIsDroppedLater(boolean disk, boolean metadataBeforeRename) throws Exception {
        var f = new Fixture();
        var replacement = f.binding("U", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (metadataBeforeRename) {
            journal.add(f.copy(f.entry(75), 104, 104L, null, f.entry(78), null));
            journal.add(f.copy(f.entry(76), 105, 104L, null, f.entry(78), null));
            journal.addAll(f.metadataChange(106, f.before, 104, Math.toIntExact(f.entry(76).header().getControlInfo().nestedCommitLevel()), f.entry(78)));
            journal.add(f.copy(f.entry(85), 108, 104L, 105L, null, null));
            journal.add(f.copy(f.entry(90), 109, 104L, null, null, "CM"));
        }
        journal.addAll(f.rename(110, f.before, "OLD"));
        journal.add(f.copy(f.entry(100), 112, 0L, null, null, null));
        for (var entry : f.range(59, 64)) {
            journal.add(f.forTable(f.copy(entry, 114 + entry.position().offset().longValueExact() - 59, 0L, null, null, null), replacement));
        }
        journal.addAll(f.drop(120, f.before, "OLD", 0));
        var later = f.forTable(f.copy(f.entry(64), 124, 0L, null, null, null), replacement);
        byte[] raw = later.data();
        java.nio.ByteBuffer.wrap(raw).putInt(later.header().getEntrySpecificDataOffset() + 16, 2);
        journal.add(new RawJournalPage.Entry(later.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        journal.add(f.copy(f.entry(100), 125, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", replacement);
        var saved = f.initial;
        for (long through : new long[]{ 112, 125, 125 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            boolean replay = saved.completedThrough().equals(f.position(125));
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(replay ? 0 : through == 112 ? 1 : 3);
                assertThat(records).noneMatch(record -> record.topic().endsWith(".OLD"));
                if (through == 125) {
                    assertThat(f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy).definitions()).contains(replacement);
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,created-empty", "true,created-empty", "false,created-loaded", "true,created-loaded",
            "false,known-short", "true,known-short", "false,known-long", "true,known-long", "false,known-long-both", "true,known-long-both", "false,known-long-short",
            "true,known-long-short",
            "false,selected-short", "true,selected-short",
            "false,selected-old", "true,selected-old", "false,selected-new", "true,selected-new", "false,missing-names", "true,missing-names",
            "false,selected-created", "true,selected-created", "false,foreign-member", "true,foreign-member" })
    void shouldPreserveProvenExcludedNamesThroughCreationRenameRowsDropAndRestart(boolean disk, String scenario) throws Exception {
        boolean created = scenario.startsWith("created") || scenario.equals("selected-created");
        boolean longName = scenario.startsWith("known-long") || scenario.equals("selected-short");
        boolean shortName = scenario.equals("known-long-short") || scenario.equals("selected-short");
        var f = new Fixture(longName);
        var original = f.entries.stream().collect(java.util.stream.Collectors.toMap(e -> e.position().offset().longValueExact(), e -> e));
        var definition = created ? f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(),
                f.entry(61).header().getRecordInfo().objectJournalId()) : f.before;
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (created) {
            for (var entry : f.range(59, scenario.equals("created-empty") ? 63 : 64)) {
                journal.add(f.copy(entry, entry.position().offset().longValueExact(), 0L, null, null, null));
            }
            journal.add(f.copy(original.get(100L), 70, 0L, null, null, null));
        }
        if (scenario.equals("known-long-both")) {
            journal.addAll(f.metadataChange(87, definition, 0, 0, original.get(78L)));
        }
        var pair = new ArrayList<>(f.rename(shortName ? 86 : 90, definition, "RENAMED"));
        if (scenario.equals("foreign-member")) {
            byte[] bytes = pair.get(1).data();
            bytes[206] ^= 1;
            pair.set(1, new RawJournalPage.Entry(pair.get(1).position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        journal.addAll(pair);
        if (shortName) {
            for (var entry : f.metadataChange(88, definition, 0, 0, original.get(78L))) {
                journal.add(f.atFile(entry, "RENAMED"));
            }
        }
        if (!scenario.equals("created-empty")) {
            journal.add(f.atFile(f.copy(original.get(created ? 64L : 44L), 92, 0L, null, null, null), "RENAMED"));
        }
        journal.add(f.copy(original.get(100L), 93, 0L, null, null, null));
        journal.addAll(f.drop(94, definition, "RENAMED", 0));
        journal.add(original.get(100L));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put(definition.identity().physicalFile(), null);
        if (!scenario.equals("missing-names")) {
            // Creation supplies the original SQL name; only the long-name
            // metadata pair carries a replacement SQL name in these cases.
            f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                    entry.header().getLibrary(), entry.header().getFile(),
                    entry.header().getEntryType().equals("CT") ? "ROLLBACKT" : "LONG_ORIGINAL_NAME",
                    entry.header().getEntryType().equals("CT") || shortName ? null : "LONG_RENAMED_NAME");
        }
        boolean rejected = scenario.startsWith("selected") || scenario.equals("missing-names") || scenario.equals("foreign-member");
        String selected = switch (scenario) {
            case "selected-old" -> "(A|U)";
            case "selected-new", "selected-short" -> "(A|RENAMED)";
            case "selected-created" -> "(A|ROLLBACKT)";
            default -> "A";
        };
        var saved = f.initial;
        for (int attempt = 0; attempt < (rejected ? 2 : 3); attempt++) {
            var input = saved;
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            long through = !rejected && attempt == 0 ? created ? 70 : 93 : 100;
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, "DCNC0005\\." + selected);
                    var runtime = f.runtime(input, recovered, output, records, disk, through)) {
                if (rejected) {
                    assertThatThrownBy(() -> runtime.poll(input.toOffset())).hasStackTraceContaining(switch (scenario) {
                        case "selected-created" -> "Selected transient table";
                        case "missing-names" -> "vanished SQL name";
                        case "foreign-member" -> "exact file/member association";
                        default -> "Selected renamed rows";
                    });
                    assertThat(records).isEmpty();
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                    assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 2 ? 0 : created && attempt == 1 ? 2 : 1);
                    assertThat(records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null))
                            .allMatch(record -> record.topic().endsWith(".A"));
                    var state = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
                    if (through == 100) {
                        assertThat(state.definitions()).noneMatch(binding -> binding.identity().fileJournalId().equals(definition.identity().fileJournalId()));
                    }
                    else if (created) {
                        assertThat(state.definitions().stream().filter(binding -> binding.identity().fileJournalId().equals(definition.identity().fileJournalId())))
                                .hasSize(1).allMatch(As400BindingDefinition::unobservedLayout);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "excluded", "selected", "missing-names", "changed-name" })
    void shouldUseHistoricalExclusionWhenDropFirstAppearsAfterCatalogAcquisition(String scenario) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(78, 85));
        journal.add(f.copy(f.entry(67), 86, 75L, null, f.entry(78), "CM"));
        journal.add(f.widenedRow(87, 0, f.entry(78), f.before));
        journal.addAll(f.drop(90, f.before, "U", 0));
        var id = f.before.identity();
        As400SourceNormalizer.TableResolver bindings = new As400SourceNormalizer.TableResolver() {
            @Override
            public As400SourceNormalizer.TableBinding resolve(String file, String library) {
                return new As400SourceNormalizer.TableBinding(id, null, false, !scenario.equals("selected"), f.before.members());
            }

            @Override
            public boolean needsValues(TableId table) {
                return scenario.equals("selected") || !table.equals(id.tableId());
            }
        };
        var reads = new ArrayList<JournalPosition>();
        As400HistoricalTableNames.Reader names = scenario.equals("missing-names") ? null : entry -> {
            reads.add(entry.position());
            return new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                    entry.header().getLibrary(), entry.header().getFile(), "U", scenario.equals("changed-name") ? "A" : null);
        };
        if (scenario.equals("excluded")) {
            var change = As400NativeSchemaRefresh.qualifyAcquired(f.before, f.widened(), journal, f.order, f.position(86), bindings, names);
            assertThat(change.after().unobservedLayout()).isTrue();
            assertThat(change.commit()).isEqualTo(f.position(86));
            assertThat(reads).containsExactly(f.position(83));
            assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
        }
        else {
            assertThatThrownBy(() -> As400NativeSchemaRefresh.qualifyAcquired(f.before, f.widened(), journal, f.order, f.position(86), bindings, names))
                    .hasStackTraceContaining("vanished changed definition");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "earlier-row", "foreign-drop", "missing-member", "missing-deletion" })
    void shouldRejectAnUnprovenRenameDropBeforeRetiringItsBinding(String fault) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.rename(110, f.before, "RENAMED"));
        if (fault.equals("earlier-row")) {
            journal.add(f.atFile(f.copy(f.entry(44), 112, 0L, null, null, null), "RENAMED"));
        }
        var drop = new ArrayList<>(f.drop(120, f.before, "RENAMED", 0));
        if (fault.equals("foreign-drop")) {
            var entry = drop.get(0);
            byte[] bytes = entry.data();
            bytes[206] ^= 1;
            drop.set(0, new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        if (fault.equals("missing-member")) {
            drop.remove(1);
        }
        if (fault.equals("missing-deletion")) {
            drop.remove(2);
        }
        journal.addAll(drop);
        String cause = switch (fault) {
            case "earlier-row" -> "vanished SQL name";
            case "foreign-drop" -> "Unqualified operation";
            case "missing-member" -> "Incomplete physical-file deletion";
            default -> "PendingChangeException";
        };
        assertThatThrownBy(() -> As400FileRename.qualify(f.before, null, journal, f.order, (file, library) -> f.before.binding(65536)))
                .hasStackTraceContaining(cause);
    }

    @ParameterizedTest
    @ValueSource(strings = { "changed-layout", "changed-file", "rows-between", "pending-row", "pending-before-image" })
    void shouldRejectSqlNameChangesThatCannotPreserveCapturedRowsBeforeRecordingHistory(String fault) throws Exception {
        var f = new Fixture(true);
        var renamed = f.sqlRenamed(f.before, "LONG_RENAMED_NAME");
        if (fault.equals("changed-layout")) {
            renamed = f.widened(renamed);
        }
        if (fault.equals("changed-file")) {
            renamed = f.renamed(renamed, "ANOTHER");
        }
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (fault.equals("pending-row")) {
            journal.add(f.copy(f.entry(75), 105, 105L, null, f.entry(78), null));
            journal.add(f.copy(f.entry(44), 106, 105L, null, f.entry(78), null));
        }
        if (fault.equals("pending-before-image")) {
            journal.add(f.copy(f.entry(44), 106, 0L, null, null, "UB"));
            byte[] bytes = journal.get(0).data();
            bytes[216] = (byte) 0xf1;
            journal.set(0, new RawJournalPage.Entry(f.position(106), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        journal.addAll(f.metadataChange(110, f.before, 0, 0, f.entry(78)));
        if (fault.equals("rows-between")) {
            journal.add(f.copy(f.entry(44), 112, 0L, null, null, null));
            journal.addAll(f.metadataChange(114, f.before, 0, 0, f.entry(78)));
        }
        journal.add(f.copy(f.entry(100), 120, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", renamed);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 120)) {
            String cause = switch (fault) {
                case "pending-row" -> "pending changes";
                case "pending-before-image" -> "incomplete autocommit update";
                case "rows-between" -> "intermediate SQL table name";
                default -> "Miscellaneous change altered";
            };
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).hasStackTraceContaining(cause);
            assertThat(records).isEmpty();
            assertThat(recovered.schemaHistory().find(f.position(110))).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "DJ/11", "DJ/13", "JC/11", "JC/13" })
    void shouldApplyJournalAttributeSafetyToTheCorrectFileOrMemberBinding(String layout) throws Exception {
        String type = layout.split("/")[0];
        var f = new Fixture();
        var original = f.entry(type.equals("DJ") ? 83 : 78);
        for (String value : List.of("1*BOTH", "1*AFTER", "2*NONE", "2*OPNCLO", "3*NO", "5*NO", "5*YES", "5*UNKNOWN", "4*NONE", "1*UNKNOWN")) {
            byte[] payload = new byte[Integer.parseInt(layout.split("/")[1])];
            new com.ibm.as400.access.AS400Text(11, 37).toBytes(value, payload, 0);
            var raw = f.payload(original, 110, type, payload);
            if (type.equals("JC")) {
                byte[] bytes = raw.data();
                bytes[218] = 0x10;
                raw = new RawJournalPage.Entry(raw.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes);
            }
            var entry = new As400JournalLifecycle.Entry(raw.header(), raw.data(), raw.position());
            for (boolean excluded : List.of(false, true)) {
                var normal = f.before.binding(65536);
                var binding = new As400SourceNormalizer.TableBinding(normal.identity(), excluded ? null : normal.decoder(), false, excluded, normal.members());
                boolean supported = List.of("1*BOTH", "2*NONE", "2*OPNCLO", "3*NO", "5*NO", "5*YES").contains(value) || excluded && value.equals("1*AFTER");
                if (supported) {
                    assertThat(As400JournalLifecycle.administrative(entry, (file, library) -> binding)).isTrue();
                }
                else {
                    assertThatThrownBy(() -> As400JournalLifecycle.administrative(entry, (file, library) -> binding))
                            .hasMessageContaining("cannot preserve");
                }
            }
        }
        byte[] truncated = new byte[12];
        new com.ibm.as400.access.AS400Text(11, 37).toBytes("1*BOTH", truncated, 0);
        var malformed = f.payload(original, 110, type, truncated);
        var malformedEntry = new As400JournalLifecycle.Entry(malformed.header(), malformed.data(), malformed.position());
        assertThatThrownBy(() -> As400JournalLifecycle.administrative(malformedEntry, (file, library) -> f.before.binding(65536)))
                .hasMessageContaining("Unqualified journal attribute layout");
        if (type.equals("JC")) {
            var minimized = f.payload(original, 110, type, java.util.Arrays.copyOf(truncated, 11));
            byte[] bytes = minimized.data();
            bytes[218] = 0x18; // Ignore-on-apply must never hide a minimized source record.
            var minimizedEntry = new As400JournalLifecycle.Entry(new EntryHeaderDecoder(37).decode(bytes, 0), bytes, minimized.position());
            assertThatThrownBy(() -> As400JournalLifecycle.administrative(minimizedEntry, (file, library) -> f.before.binding(65536)))
                    .hasMessageContaining("minimized");
        }
    }

    @Test
    void shouldPreserveCaptureSelectionWhenMetadataHistoryIncludesAnImageAttributeChange() throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.metadataChange(110, f.before, 0, 0, f.entry(78)));
        byte[] payload = new com.ibm.as400.access.AS400Text(11, 37).toBytes("1*AFTER");
        journal.add(f.payload(f.entry(83), 112, "DJ", payload));
        var normal = f.before.binding(65536);
        var excluded = new As400SourceNormalizer.TableBinding(normal.identity(), null, false, true, normal.members());
        var qualified = As400SchemaChange.qualify(f.before, f.before, journal, f.order, f.position(112), (file, library) -> excluded);
        assertThat(qualified.records()).hasSize(2);
        assertThatThrownBy(() -> As400SchemaChange.qualify(f.before, f.before, journal, f.order, f.position(112), (file, library) -> normal))
                .hasMessageContaining("cannot preserve the captured history profile");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRecoverWhenLifecycleRetentionMovesBehindAnAppliedSchema(boolean disk) throws Exception {
        var f = new Fixture();
        f.committedAlterWithEarlierLifecycleAndLaterOpenTransaction();
        var saved = f.initial;
        for (int restart = 0; restart < 3; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 1029)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            assertThat(saved.replayFrom()).isEqualTo(f.position(1002));
            assertThat(saved.replayInclusive()).isTrue();
            assertThat(saved.completedThrough()).isEqualTo(f.position(1029));
            assertThat(saved.schemaState().floor().ordinal()).isZero();
            assertThat(saved.schemaState().completed().ordinal()).isEqualTo(1);
            assertThat(records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null))
                    .hasSize(restart == 0 ? 1 : 0);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRepairAnOverAdvancedSavedSchemaFloorOnlyWithItsCompleteRetainedTransition(boolean missingOpening) throws Exception {
        var f = new Fixture();
        f.committedAlterWithEarlierLifecycleAndLaterOpenTransaction();
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        As400Checkpoint saved;
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, new ArrayList<>(), false, 1029)) {
            saved = As400Checkpoint.fromOffset(runtime.poll(f.initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
        }
        var advanced = new As400SchemaState(saved.schemaState().completed(), saved.schemaState().completed());
        var oldCheckpoint = new As400Checkpoint(saved.journalIdentity(), saved.captureFingerprint(),
                missingOpening ? f.position(1008) : saved.replayFrom(), true, saved.completedThrough(), saved.replayReceiverIdentity(),
                saved.completedReceiverIdentity(), saved.snapshotState(), advanced);
        var restarted = f.history().recoverOnlineCandidate(oldCheckpoint.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(restarted.definitions(), false);
                var runtime = f.runtime(oldCheckpoint, restarted, output, records, false, 1029)) {
            if (missingOpening) {
                assertThatThrownBy(() -> runtime.poll(oldCheckpoint.toOffset())).hasMessageContaining("inconsistent schema history");
            }
            else {
                var repaired = As400Checkpoint.fromOffset(runtime.poll(oldCheckpoint.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(repaired.replayFrom()).isEqualTo(saved.replayFrom());
                assertThat(repaired.completedThrough()).isEqualTo(saved.completedThrough());
                assertThat(repaired.schemaState()).isEqualTo(saved.schemaState());
            }
        }
        assertThat(records).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "U", "A", "wrong-target", "wrong-level", "wrong-producer" })
    void shouldTrackLaterStatementSavepointsAfterAlterWithoutBorrowingAnotherScope(String scenario) throws Exception {
        var f = new Fixture();
        var statement = new java.util.TreeMap<Long, RawJournalPage.Entry>();
        try (var input = getClass().getResourceAsStream("/native-transactions/transactional-ddl-rollback.json")) {
            for (var item : new ObjectMapper().readTree(input).path("administrative_entries")) {
                byte[] bytes = Base64.getDecoder().decode(item.path("raw").asText());
                var header = new EntryHeaderDecoder(37).decode(bytes, 0);
                statement.put(header.getSequenceNumber().longValueExact(),
                        new RawJournalPage.Entry(f.position(header.getSequenceNumber().longValueExact()), header, bytes));
            }
        }
        var entries = new ArrayList<>(f.range(78, 85));
        entries.add(f.copy(statement.get(57L), 86, 75L, null, f.entry(78), null));
        var grant = f.copy(statement.get(58L), 87, 75L, null, f.entry(78), null);
        byte[] raw = grant.data();
        String file = scenario.equals("A") ? "A" : "U";
        new com.ibm.as400.access.AS400Text(10, 37).toBytes(file, raw, 157);
        var binding = f.baseline.stream().filter(table -> table.identity().physicalFile().equals(file)).findFirst().orElseThrow();
        System.arraycopy(java.util.HexFormat.of().parseHex(binding.identity().fileJournalId()), 0, raw, 197, 10);
        entries.add(new RawJournalPage.Entry(grant.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        var release = f.copy(statement.get(59L), 88, 75L, scenario.equals("wrong-target") ? 87L : 86L, f.entry(78), null);
        raw = release.data();
        if (scenario.equals("wrong-level")) {
            java.nio.ByteBuffer.wrap(raw).putInt(232, 3);
        }
        if (scenario.equals("wrong-producer")) {
            raw[126] = (byte) 0xf9;
        }
        entries.add(new RawJournalPage.Entry(release.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        entries.add(f.copy(f.entry(90), 89, 75L, null, f.entry(78), "CM"));
        if (scenario.startsWith("wrong-")) {
            assertThatThrownBy(() -> As400SchemaChange.qualify(f.before, f.widened(), entries, f.order, f.position(89)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        else {
            var change = As400SchemaChange.qualify(f.before, f.widened(), entries, f.order, f.position(89));
            assertThat(change.commit()).isEqualTo(f.position(89));
            assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "commit", "rollback", "missing-undo", "foreign-producer", "foreign-member", "foreign-rollback" })
    void shouldValidateALaterClearAndItsUndoWithoutConsumingThemAsSchemaChanges(String scenario) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(78, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, null, "CM"));
        journal.addAll(f.laterClear());
        journal.add(f.copy(f.entry(90), 99, scenario.equals("foreign-rollback") ? 89L : 90L, null, null,
                scenario.equals("commit") ? "CM" : "RB"));
        if (!scenario.equals("commit")) {
            for (long sequence = 91; sequence <= (scenario.equals("missing-undo") ? 93 : 94); sequence++) {
                byte[] bytes = f.copy(f.entry(sequence), sequence + 9, null, null, null, null).data();
                if (sequence == 91 && scenario.equals("foreign-producer")) {
                    bytes[126] ^= 1;
                }
                if (sequence == 91 && scenario.equals("foreign-member")) {
                    bytes[197] ^= 1;
                }
                journal.add(new RawJournalPage.Entry(f.position(sequence + 9), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
            }
        }
        if (List.of("commit", "rollback").contains(scenario)) {
            var change = As400SchemaChange.qualify(f.before, f.widened(), journal, f.order, f.position(103));
            assertThat(change.commit()).isEqualTo(f.position(86));
            assertThat(change.records()).allMatch(mark -> mark.position().offset().longValueExact() < 90);
        }
        else {
            assertThatThrownBy(() -> As400SchemaChange.qualify(f.before, f.widened(), journal, f.order, f.position(103)))
                    .isInstanceOf(RuntimeException.class);
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldReplayOnlyTheCommittedRowAfterARowSavepointRollbackFollowingAlter(boolean disk, boolean wrongScope) throws Exception {
        var f = new Fixture();
        var producer = f.entry(78);
        var journal = new ArrayList<>(f.range(74, 85));
        journal.add(f.copy(f.entry(76), 86, 75L, null, producer, null));
        var inserted = f.copy(f.entry(100), 87, 75L, null, producer, null);
        byte[] raw = inserted.data();
        java.nio.ByteBuffer.wrap(raw).putInt(inserted.header().getEntrySpecificDataOffset() + 16, 7003);
        inserted = new RawJournalPage.Entry(inserted.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        journal.add(inserted);
        journal.add(f.copy(inserted, 88, 75L, null, producer, "DR"));
        journal.add(f.copy(f.entry(85), 89, 75L, wrongScope ? 76L : 86L, producer, "SU"));
        journal.add(f.copy(f.entry(76), 90, 75L, null, producer, null));
        raw = f.copy(inserted, 91, 75L, null, producer, null).data();
        java.nio.ByteBuffer.wrap(raw).putInt(inserted.header().getEntrySpecificDataOffset() + 16, 7004);
        journal.add(new RawJournalPage.Entry(f.position(91), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        journal.add(f.copy(f.entry(85), 92, 75L, 90L, producer, null));
        journal.add(f.copy(f.entry(67), 93, 75L, null, producer, "CM"));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", f.widened());
        var saved = f.initial;
        for (int attempt = 0; attempt < (wrongScope ? 1 : 2); attempt++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            var initial = saved;
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 100)) {
                if (wrongScope) {
                    assertThatThrownBy(() -> runtime.poll(initial.toOffset())).hasStackTraceContaining("savepoint");
                    assertThat(records).isEmpty();
                    assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                    var data = records.stream().filter(record -> record.topic().endsWith(".A")).toList();
                    assertThat(data).hasSize(attempt == 0 ? 2 : 0);
                    if (attempt == 0) {
                        assertThat(((Struct) data.get(0).value()).getStruct("after").getInt32("ID")).isEqualTo(7004);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "creation", "alter", "repeated-command" })
    void shouldRejectCommandChangesThatCouldHideAnIntermediateCapturedRowLayout(String scenario) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var table = scenario.equals("creation") ? created : f.before;
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (scenario.equals("creation")) {
            for (var entry : f.range(59, 64)) {
                journal.add(f.copy(entry, entry.position().offset().longValueExact(), 0L, null, null, null));
            }
        }
        else if (scenario.equals("alter")) {
            journal.addAll(f.range(78, 85));
            journal.add(f.copy(f.entry(90), 86, 75L, null, null, "CM"));
            journal.add(f.widenedRow(87, 0, f.entry(78), f.before));
        }
        for (long sequence : scenario.equals("repeated-command") ? new long[]{ 90, 94 } : new long[]{ 90 }) {
            var command = f.metadataChange(sequence, table, 0, 0, f.entry(78));
            journal.add(command.get(0));
            byte[] raw = command.get(1).data();
            raw[command.get(1).header().getEntrySpecificDataOffset() + 16 + 108] = (byte) 0xf1;
            journal.add(new RawJournalPage.Entry(command.get(1).position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
            if (scenario.equals("repeated-command") && sequence == 90) {
                journal.add(f.copy(f.entry(44), 92, 0L, null, null, null));
            }
        }
        assertThatThrownBy(() -> As400SchemaChange.qualify(scenario.equals("creation") ? null : f.before,
                scenario.equals("alter") ? f.widened() : table, journal, f.order, f.position(100)))
                .hasMessageContaining("intermediate CL-command definition");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRecoverExcludedCreatedRowsBeforeACommandChangesTheirLayout(boolean disk) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        for (var entry : f.range(59, 64)) {
            journal.add(f.copy(entry, entry.position().offset().longValueExact(), 0L, null, null, null));
        }
        var command = f.metadataChange(90, created, 0, 0, f.entry(78));
        journal.add(command.get(0));
        byte[] raw = command.get(1).data();
        raw[command.get(1).header().getEntrySpecificDataOffset() + 16 + 108] = (byte) 0xf1;
        journal.add(new RawJournalPage.Entry(command.get(1).position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        journal.add(f.widenedRow(92, 0, f.entry(59), created));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", f.widened(created));
        assertThat(As400SchemaChange.qualify(null, null, journal, f.order, f.position(100))).isNull();
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, "DCNC0005.A");
                    var runtime = f.runtime(saved, recovered, output, records, disk, 100)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 0 ? 1 : 0);
                assertThat(records).noneMatch(record -> record.topic().endsWith(".ROLLBACKT"));
                assertThat(recovered.schemaHistory().find(f.position(59)).change().after()).isEqualTo(f.widened(created));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReplayARolledBackClearSeenDuringAlterWithoutPublishingItOrLosingTheNextRow(boolean disk) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(74, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, null, "CM"));
        journal.addAll(f.laterClear());
        journal.add(f.copy(f.entry(90), 99, 90L, null, null, "RB"));
        for (var entry : f.range(91, 94)) {
            journal.add(f.copy(entry, entry.position().offset().longValueExact() + 9, null, null, null, null));
        }
        journal.add(f.copy(f.entry(100), 104, null, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", f.widened());
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 104)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(104));
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 0 ? 1 : 0);
                assertThat(records).noneMatch(record -> record.topic().endsWith(".U"));
                if (attempt == 0) {
                    var row = records.stream().filter(record -> record.topic().endsWith(".A")).findFirst().orElseThrow();
                    assertThat(((Struct) row.value()).getString("op")).isEqualTo("c");
                    assertThat(((Struct) row.value()).getStruct("source").getString("sequence")).isEqualTo("104");
                }
            }
        }
    }

    @Test
    void shouldWaitForStreamingCutToCoverAnIndependentlyObservedSchemaCommit() throws Exception {
        var f = new Fixture();
        var producer = f.entry(78);
        var journal = new ArrayList<>(f.range(74, 85));
        journal.add(f.copy(f.entry(90), 86, 75L, null, producer, "CM"));
        journal.add(f.copy(f.entry(100), 87, null, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", f.widened());
        f.cutThrough = 85;
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 87)) {
            var deferred = runtime.poll(f.initial.toOffset());
            assertThat(deferred.deferred()).isFalse();
            assertThat(deferred.scan().orElseThrow()).isEqualTo(f.position(85));
            assertThat(f.schemaReads).isZero();
            assertThat(records).isEmpty();
            assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
            f.cutThrough = 87;
            var resumed = runtime.poll(deferred.checkpoint());
            assertThat(resumed.deferred()).isFalse();
            assertThat(resumed.checkpoint()).containsEntry("completed_through.sequence", "87");
            assertThat(records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null)).hasSize(1);
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false,false", "false,true,false", "true,false,false", "true,true,false",
            "false,false,true", "false,true,true", "true,false,true", "true,true,true" })
    void shouldPublishUnrelatedRowsAndRecoverAnUnfinishedAlterWithoutRepeatingCompletedOutput(boolean disk, boolean rollback, boolean metadataPrefix) throws Exception {
        var f = new Fixture();
        f.openAlter(rollback);
        if (metadataPrefix) {
            f.entries.addAll(0, f.metadataChange(70, f.before, 0, 0, f.entry(78)));
        }
        var saved = f.initial;
        // Each iteration uses a fresh runtime and rereads immutable history.
        for (int restart = 0; restart < 4; restart++) {
            f.cutThrough = restart < 2 ? 88 : 100;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 100)) {
                var progress = runtime.poll(saved.toOffset());
                assertThat(progress.deferred()).isFalse();
                if (restart < 3) {
                    assertThat(progress.scan().orElseThrow()).isEqualTo(f.position(restart < 2 ? 88 : 100));
                }
                else {
                    assertThat(progress.scan()).isEmpty();
                }
                saved = As400Checkpoint.fromOffset(progress.checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                if (restart < 2) {
                    assertThat(f.schemaReads).isEqualTo(metadataPrefix ? 1 : 0);
                    assertThat(saved.completedThrough()).isEqualTo(f.position(86));
                    assertThat(saved.replayFrom()).isEqualTo(f.position(75));
                    assertThat(saved.replayInclusive()).isTrue();
                    assertThat(saved.schemaState().floor().ordinal()).isEqualTo(metadataPrefix ? 1 : 0);
                    assertThat(saved.schemaState().completed().ordinal()).isEqualTo(metadataPrefix ? 1 : 0);
                    assertThat(runtime.bufferUsage()[1]).isPositive();
                }
                else {
                    assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                    assertThat(saved.schemaState().completed().ordinal()).isEqualTo(metadataPrefix ? 2 : 1);
                    assertThat(runtime.bufferUsage()[1]).isZero();
                }
            }
            var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
            assertThat(data).hasSize(restart == 0 ? 1 : restart == 2 ? rollback ? 1 : 3 : 0);
            if (restart == 2 && !rollback) {
                var changed = data.stream().filter(record -> record.topic().endsWith(".U")).findFirst().orElseThrow();
                assertThat(((Struct) changed.value()).getStruct("after").getInt32("ID")).isEqualTo(7001);
                assertThat(((Struct) changed.value()).getStruct("after").getInt32("X1")).isEqualTo(7);
            }
            if (restart >= 2) {
                var change = recovered.schemaHistory().find(f.position(78)).change();
                assertThat(change.deferred()).isTrue();
                assertThat(change.visibility()).isEqualTo(f.position(90));
                assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldRetainAnUnfinishedCreationWhileUnrelatedOutputAndRestartContinue(boolean disk, boolean rollback) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<>(f.range(58, 64));
        journal.add(f.copy(f.entry(100), 65, 0L, null, null, null));
        if (rollback) {
            journal.addAll(f.range(66, 71));
        }
        else {
            journal.add(f.copy(f.entry(67), 67, 58L, null, null, "CM"));
            f.after.put("ROLLBACKT", created);
        }
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        var saved = f.initial;
        for (int restart = 0; restart < 4; restart++) {
            f.schemaThrough = f.cutThrough = restart < 2 ? 66 : 100;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 100)) {
                var progress = runtime.poll(saved.toOffset());
                assertThat(progress.deferred()).isFalse();
                saved = As400Checkpoint.fromOffset(progress.checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                if (restart < 2) {
                    assertThat(f.schemaReads).isZero();
                    assertThat(saved.completedThrough()).isEqualTo(f.position(65));
                    assertThat(saved.replayFrom()).isEqualTo(f.position(58));
                    assertThat(saved.replayInclusive()).isTrue();
                    assertThat(saved.schemaState()).isEqualTo(As400SchemaState.EMPTY);
                    assertThat(runtime.bufferUsage()[1]).isPositive();
                }
                else {
                    assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                    assertThat(runtime.bufferUsage()[1]).isZero();
                }
            }
            var data = As400RecoveryTestSupport.data(records);
            assertThat(data).hasSize(restart == 0 ? 1 : restart == 2 ? rollback ? 1 : 2 : 0);
            if (restart >= 2) {
                var history = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
                assertThat(history.definitions().contains(created)).isEqualTo(!rollback);
                if (!rollback) {
                    var change = history.schemaHistory().find(f.position(59)).change();
                    assertThat(change.visibility()).isEqualTo(f.position(67));
                    assertThat(change.deferred()).isTrue();
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,valid", "true,valid", "false,missing-savepoint", "true,missing-savepoint", "false,wrong-level",
            "true,wrong-level", "false,wrong-producer", "true,wrong-producer", "false,wrong-identity", "true,wrong-identity" })
    void shouldValidateMetadataScopeAndIdentityBeforeStagingAnUnfinishedCreation(boolean disk, String scenario) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<>(f.range(58, 64));
        if (!scenario.equals("missing-savepoint")) {
            journal.add(f.copy(f.entry(76), 65, 58L, null, f.entry(59), null));
        }
        int level = Math.toIntExact(f.entry(76).header().getControlInfo().nestedCommitLevel());
        var metadata = f.metadataChange(66, created, 58, scenario.equals("wrong-level") ? level + 1 : level, f.entry(59));
        journal.add(metadata.get(0));
        byte[] raw = metadata.get(1).data();
        if (scenario.equals("wrong-producer")) {
            raw[126] ^= 1;
        }
        if (scenario.equals("wrong-identity")) {
            raw[197] ^= 1;
        }
        journal.add(new RawJournalPage.Entry(f.position(67), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.schemaThrough = f.cutThrough = 100;
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, disk, 100)) {
            if (scenario.equals("valid")) {
                var saved = As400Checkpoint.fromOffset(runtime.poll(f.initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                assertThat(saved.replayFrom()).isEqualTo(f.position(58));
                assertThat(saved.replayInclusive()).isTrue();
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(1);
            }
            else {
                assertThatThrownBy(() -> runtime.poll(f.initial.toOffset()))
                        .hasStackTraceContaining(scenario.equals("wrong-producer") ? "producer"
                                : scenario.equals("wrong-identity") ? "identity" : "savepoint scope");
                assertThat(records).isEmpty();
            }
            assertThat(f.schemaReads).isZero();
            assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldReplayRowsAndClearAfterCreationWithoutConsumingTheClearAsSchemaMetadata(boolean disk, boolean rollback) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        for (var item : f.range(59, 64)) {
            journal.add(f.copy(item, item.position().offset().longValueExact(), 0L, null, null, null));
        }
        for (var item : f.laterClear()) {
            journal.add(item.header().getJournalCode() == 'C' ? item : f.forTable(item, created));
        }
        journal.add(f.copy(f.entry(90), 99, 90L, null, null, rollback ? "RB" : "CM"));
        if (rollback) {
            for (var item : f.range(91, 94)) {
                journal.add(f.forTable(f.copy(item, item.position().offset().longValueExact() + 9, null, null, null, null), created));
            }
        }
        journal.add(f.copy(f.entry(100), 104, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", created);
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 104)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(104));
                var changes = records.stream().filter(record -> record.topic().endsWith(".ROLLBACKT") && record.value() != null)
                        .map(record -> ((Struct) record.value()).getString("op")).toList();
                assertThat(changes).containsExactlyElementsOf(attempt == 1 ? List.of() : rollback ? List.of("c") : List.of("c", "t"));
                var change = recovered.schemaHistory().find(f.position(59)).change();
                assertThat(change.records()).allMatch(mark -> mark.position().offset().longValueExact() < 90);
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 1 ? 0 : rollback ? 2 : 3);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldRecoverCreationAndClearInOneTransactionWithoutNormalizingItsPhysicalCompanions(boolean disk, boolean rollback) throws Exception {
        var f = new Fixture();
        var created = f.creationAndClear(rollback);
        var saved = f.initial;
        for (int attempt = 0; attempt < 4; attempt++) {
            f.schemaThrough = f.cutThrough = attempt < 2 ? 103 : 112;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 112)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                var changes = records.stream().filter(record -> record.topic().endsWith(".ROLLBACKT") && record.value() != null)
                        .map(record -> ((Struct) record.value()).getString("op")).toList();
                assertThat(changes).containsExactlyElementsOf(attempt == 2 && !rollback ? List.of("t", "c") : List.of());
                var unrelated = records.stream().filter(record -> record.topic().endsWith(".A") && record.value() != null).toList();
                assertThat(unrelated).hasSize(attempt == 0 || attempt == 2 ? 1 : 0);
                if (attempt < 2) {
                    assertThat(saved.completedThrough()).isEqualTo(f.position(102));
                    assertThat(saved.replayFrom()).isEqualTo(f.position(58));
                    assertThat(saved.replayInclusive()).isTrue();
                    assertThat(f.schemaReads).isZero();
                    assertThat(runtime.bufferUsage()[1]).isPositive();
                }
                else {
                    assertThat(saved.completedThrough()).isEqualTo(f.position(112));
                    assertThat(runtime.bufferUsage()[1]).isZero();
                    assertThat(f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy).definitions().contains(created))
                            .isEqualTo(!rollback);
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,missing-scope", "true,missing-scope", "false,wrong-scope", "true,wrong-scope",
            "false,missing-companion", "true,missing-companion", "false,wrong-identity", "true,wrong-identity" })
    void shouldRejectAnUnqualifiedClearOfAnUncommittedCreationBeforePublishingOrAdvancing(boolean disk, String fault) throws Exception {
        var f = new Fixture();
        f.creationAndClear(false);
        if (fault.equals("missing-scope") || fault.equals("missing-companion")) {
            f.entries.removeIf(entry -> entry.position().equals(f.position(fault.equals("missing-scope") ? 91 : 93)));
        }
        else {
            long sequence = fault.equals("wrong-scope") ? 97 : 93;
            var original = f.entry(sequence);
            byte[] bytes = original.data();
            if (fault.equals("wrong-scope")) {
                java.nio.ByteBuffer.wrap(bytes).putInt(232, Math.toIntExact(original.header().getControlInfo().nestedCommitLevel()) + 1);
            }
            else {
                bytes[197] ^= 1;
            }
            f.entries.set(f.entries.indexOf(original), new RawJournalPage.Entry(original.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, disk, 112)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset()))
                    .hasStackTraceContaining(fault.endsWith("scope") ? "scope" : fault.equals("wrong-identity") ? "identity" : "Incomplete clear");
            assertThat(records).isEmpty();
            assertThat(f.schemaReads).isZero();
            assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReplayTheWholeCreatedTableClearBatchAfterInterruptedDelivery(boolean disk) throws Exception {
        var f = new Fixture();
        f.creationAndClear(false);
        var saved = f.initial;
        for (int attempt = 0; attempt < 3; attempt++) {
            f.schemaThrough = f.cutThrough = attempt == 0 ? 103 : 112;
            f.failAfterRecords = attempt == 1 ? 1 : -1;
            var restartOffset = saved.toOffset();
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 112)) {
                if (attempt == 1) {
                    assertThatThrownBy(() -> runtime.poll(restartOffset)).hasStackTraceContaining("Interrupted test delivery");
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                }
                var changes = records.stream().filter(record -> record.topic().endsWith(".ROLLBACKT") && record.value() != null).toList();
                assertThat(changes.stream().map(record -> ((Struct) record.value()).getString("op")))
                        .containsExactlyElementsOf(attempt == 0 ? List.of() : attempt == 1 ? List.of("t") : List.of("t", "c"));
                if (attempt == 1) {
                    assertThat(changes).allSatisfy(record -> restartOffset.forEach((key, value) -> assertThat(record.sourceOffset().get(key)).isEqualTo(value)));
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldKeepCreatedRowsWhenAClearIsAbandonedBeforeChangingJournaling(boolean disk, boolean rollback) throws Exception {
        var f = new Fixture();
        f.abandonedCreationClear(rollback);
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 112)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                var changes = records.stream().filter(record -> record.topic().endsWith(".ROLLBACKT") && record.value() != null)
                        .map(record -> ((Struct) record.value()).getString("op")).toList();
                assertThat(changes).containsExactlyElementsOf(attempt == 0 && !rollback ? List.of("c", "c") : List.of());
                assertThat(saved.completedThrough()).isEqualTo(f.position(112));
                assertThat(runtime.bufferUsage()[1]).isZero();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldRejectAbandonedCreationClearWithWrongScopeOrStartedJournalingChange(boolean disk, boolean physicalStarted) throws Exception {
        var f = new Fixture();
        var companion = f.entry(79);
        f.abandonedCreationClear(false);
        if (physicalStarted) {
            f.entries.add(f.entries.indexOf(f.entry(92)) + 1,
                    f.forTable(f.copy(companion, 93, 0L, null, f.entry(59), null), f.after.get("ROLLBACKT")));
        }
        else {
            var rollback = f.entry(98);
            f.entries.set(f.entries.indexOf(rollback), f.copy(rollback, 98, 58L, 90L, null, null));
        }
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, disk, 112)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset()))
                    .hasStackTraceContaining(physicalStarted ? "lifecycle sequence" : "abandoned-clear scope");
            assertThat(records).isEmpty();
            assertThat(f.schemaReads).isZero();
            assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false,false,false", "true,false,false,false", "false,true,false,false", "true,true,false,false",
            "false,false,true,false", "true,true,true,false", "false,false,true,true", "true,false,true,true",
            "false,true,true,true", "true,true,true,true" })
    void shouldRetireADeletedCreationOnlyWhenItsRowsAreProvenExcluded(boolean disk, boolean transactional, boolean earlierRow, boolean provenExcluded)
            throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (transactional) {
            journal.add(f.entry(58));
        }
        for (var item : f.range(59, earlierRow ? 64 : 63)) {
            journal.add(f.copy(item, item.position().offset().longValueExact(), transactional ? null : 0L, null, null, null));
        }
        journal.add(f.copy(f.entry(100), 65, 0L, null, null, null));
        byte[] name = new com.ibm.as400.access.AS400Text(60, 37).toBytes(String.format("%-10s%-50s", "U", f.receiver.library()));
        var deletion = f.forTable(f.payload(f.entry(83), 70, "DT", name), created);
        journal.add(f.copy(deletion, 70, transactional ? 58L : 0L, null, f.entry(59), null));
        if (transactional) {
            journal.add(f.copy(f.entry(67), 80, 58L, null, f.entry(59), "CM"));
        }
        journal.add(f.copy(f.entry(69), 90, 0L, null, f.entry(59), null));
        journal.add(f.copy(f.entry(71), 91, 0L, null, f.entry(59), null));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        if (provenExcluded) {
            f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                    entry.header().getLibrary(), entry.header().getFile(), "ROLLBACKT", null);
        }
        boolean rejected = earlierRow && !provenExcluded;
        var saved = f.initial;
        for (int attempt = 0; attempt < (rejected ? 1 : provenExcluded ? 3 : 2); attempt++) {
            var initial = saved;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, provenExcluded ? "DCNC0005.A" : ".*");
                    var runtime = f.runtime(saved, recovered, output, records, disk, provenExcluded && attempt == 0 ? 65 : 100)) {
                if (rejected) {
                    assertThatThrownBy(() -> runtime.poll(initial.toOffset())).hasStackTraceContaining("unavailable historical definition");
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(provenExcluded && attempt == 0 ? 65 : 100));
                    if (provenExcluded && attempt == 0) {
                        assertThat(saved.replayFrom().offset()).isLessThanOrEqualTo(BigInteger.valueOf(59));
                        assertThat(saved.replayInclusive()).isTrue();
                    }
                    assertThat(As400RecoveryTestSupport.data(records)).hasSize(provenExcluded ? attempt < 2 ? 1 : 0 : attempt == 0 ? 2 : 0);
                    assertThat(records).noneMatch(record -> record.topic().endsWith(".ROLLBACKT"));
                }
                assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
                assertThat(recovered.definitions()).doesNotContain(created);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false,false,false", "true,false,false,false", "false,false,true,false", "true,false,true,false",
            "false,true,true,false", "true,true,true,false", "false,false,true,true", "true,false,true,true", "false,true,true,true", "true,true,true,true" })
    void shouldRetireCreatedAndAlteredTablesOnlyAfterProvingTheirRowsExcluded(boolean disk, boolean transactional, boolean rows, boolean selected)
            throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (transactional) {
            journal.add(f.entry(58));
        }
        for (var item : f.range(59, rows ? 64 : 63)) {
            journal.add(f.copy(item, item.position().offset().longValueExact(), transactional ? null : 0L, null, null, null));
        }
        journal.add(f.copy(f.entry(100), 65, 0L, null, null, null));
        for (var item : f.range(transactional ? 76 : 75, 85)) {
            var changed = f.copy(item, item.position().offset().longValueExact(),
                    item.header().getCommitCycle().signum() == 0 ? null : transactional ? 58L : 75L, null, f.entry(59), null);
            journal.add(item.header().getJournalCode() == 'C' ? changed : f.forTable(changed, created));
        }
        if (!transactional) {
            journal.add(f.copy(f.entry(67), 86, 75L, null, f.entry(59), "CM"));
        }
        if (rows) {
            journal.add(f.widenedRow(87, transactional ? 58 : 0, f.entry(59), created));
        }
        for (var item : f.drop(90, created, "ROLLBACKT", transactional ? 58 : 0)) {
            journal.add(f.copy(item, item.position().offset().longValueExact(), null, null, f.entry(59), null));
        }
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                entry.header().getLibrary(), entry.header().getFile(), "ROLLBACKT", null);
        var saved = f.initial;
        for (int attempt = 0; attempt < (selected ? 2 : 3); attempt++) {
            var input = saved;
            long through = !selected && attempt == 0 ? 65 : 100;
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, selected ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(input, recovered, output, records, disk, through)) {
                if (selected) {
                    assertThatThrownBy(() -> runtime.poll(input.toOffset())).hasStackTraceContaining("unavailable historical definition");
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                    assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt < 2 ? 1 : 0);
                    if (through == 65) {
                        assertThat(saved.replayFrom().offset()).isLessThanOrEqualTo(BigInteger.valueOf(59));
                        assertThat(saved.replayInclusive()).isTrue();
                    }
                }
                assertThat(records).noneMatch(record -> record.topic().endsWith(".ROLLBACKT"));
                assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
                assertThat(f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy).definitions())
                        .containsExactlyInAnyOrderElementsOf(f.baseline);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,retained", "true,retained", "false,dropped", "true,dropped", "false,deferred", "true,deferred", "false,rollback", "true,rollback",
            "false,selected", "true,selected", "false,foreign-member", "true,foreign-member" })
    void shouldConsumeNewPointerRowsOnlyAfterExclusionOrFullRollbackIsProven(boolean disk, String scenario) throws Exception {
        var f = new Fixture();
        var original = f.entries.stream().collect(java.util.stream.Collectors.toMap(e -> e.position().offset().longValueExact(), e -> e));
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(),
                f.entry(61).header().getRecordInfo().objectJournalId());
        RawJournalPage.Entry capturedPointer = null;
        String format;
        try (var input = getClass().getResourceAsStream("/native-transactions/excluded-lob.json")) {
            var json = new ObjectMapper().readTree(input);
            assertThat(json.path("startup_proof").asBoolean()).isFalse();
            format = json.path("excluded").path("format").asText();
            for (var item : json.path("entries")) {
                byte[] raw = Base64.getDecoder().decode(item.path("raw").asText());
                var header = new EntryHeaderDecoder(37).decode(raw, 0);
                if (header.getJournalCode() == 'R' && header.getEntryType().equals("PT") && header.getPointerHandle() != 0) {
                    capturedPointer = new RawJournalPage.Entry(f.position(64), header, raw);
                    break;
                }
            }
        }
        assertThat(capturedPointer).as("Repository fixture contains an actual captured pointer-backed insert").isNotNull();
        byte[] raw = capturedPointer.data();
        var nativeName = new com.ibm.as400.access.AS400Text(10, 37);
        nativeName.toBytes("ROLLBACKT", raw, 157);
        nativeName.toBytes(f.receiver.library(), raw, 167);
        nativeName.toBytes("ROLLBACKT", raw, 177);
        System.arraycopy(java.util.HexFormat.of().parseHex(created.identity().memberJournalId()), 0, raw, 197, 10);
        var rewritten = new RawJournalPage.Entry(f.position(64), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        boolean transactional = scenario.equals("deferred") || scenario.equals("rollback");
        var pointer = f.copy(rewritten, 64, transactional ? 58L : 0L, null, original.get(59L), null);
        var id = created.identity();
        var lob = As400BindingDefinition.excluded(new As400TransactionBuffer.TableIdentity(id.tableId(), id.physicalFile(), id.member(),
                id.fileJournalId(), id.memberJournalId(), format), true);
        // Positive control on the exact same bytes: the ordinary pre-existing
        // excluded-LOB validation accepts the bounded pointer envelope.
        As400SourceNormalizer.validateSource(pointer.header(), pointer.data(), 0, pointer.position(), (file, library) -> lob.binding(65536));
        if (scenario.equals("foreign-member")) {
            byte[] changed = pointer.data();
            changed[206] ^= 1;
            pointer = new RawJournalPage.Entry(pointer.position(), new EntryHeaderDecoder(37).decode(changed, 0), changed);
        }
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (transactional) {
            journal.add(original.get(58L));
        }
        for (var entry : f.range(59, 63)) {
            journal.add(f.copy(entry, entry.position().offset().longValueExact(), transactional ? 58L : 0L, null, null, null));
        }
        journal.add(pointer);
        if (scenario.equals("rollback")) {
            journal.add(f.copy(pointer, 66, 58L, null, original.get(59L), "DR"));
            journal.addAll(f.range(67, 71));
        }
        else if (transactional) {
            journal.add(f.copy(original.get(67L), 67, 58L, null, original.get(59L), "CM"));
        }
        if (scenario.equals("dropped")) {
            journal.addAll(f.drop(70, created, "ROLLBACKT", 0));
        }
        journal.add(original.get(100L));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", scenario.equals("dropped") ? null : lob);
        f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                entry.header().getLibrary(), entry.header().getFile(), "ROLLBACKT", null);
        boolean rejected = scenario.equals("selected") || scenario.equals("foreign-member");
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var input = saved;
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, scenario.equals("selected") ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(input, recovered, output, records, disk, 100)) {
                if (rejected) {
                    assertThatThrownBy(() -> runtime.poll(input.toOffset()))
                            .hasStackTraceContaining(scenario.equals("selected") ? "Selected creation requires complete row values" : "different created member");
                    assertThat(records).isEmpty();
                    assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                    var data = As400RecoveryTestSupport.data(records);
                    assertThat(data).hasSize(attempt == 0 ? 1 : 0);
                    assertThat(records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null))
                            .allMatch(record -> record.topic().endsWith(".A"));
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,excluded", "true,excluded", "false,selected", "true,selected", "false,missing-names", "true,renamed" })
    void shouldReplayAKnownExcludedAlterBeforeRowsAndDropWithoutInventingItsLayout(boolean disk, String scenario) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(75, 85));
        journal.add(f.copy(f.entry(67), 86, 75L, null, f.entry(78), "CM"));
        journal.add(f.widenedRow(87, 0, f.entry(78), f.before));
        journal.add(f.copy(f.entry(100), 88, 0L, null, null, null));
        journal.addAll(f.drop(90, f.before, "U", 0));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", null);
        if (!scenario.equals("missing-names")) {
            f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                    entry.header().getLibrary(), entry.header().getFile(), "U", scenario.equals("renamed") ? "A" : null);
        }
        boolean rejected = !scenario.equals("excluded");
        var saved = f.initial;
        for (int attempt = 0; attempt < (rejected ? 2 : 3); attempt++) {
            var input = saved;
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            long through = !rejected && attempt == 0 ? 88 : 100;
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, scenario.equals("selected") ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(input, recovered, output, records, disk, through)) {
                if (rejected) {
                    assertThatThrownBy(() -> runtime.poll(input.toOffset())).hasStackTraceContaining("vanished changed definition");
                    assertThat(records).isEmpty();
                    assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                    assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt < 2 ? 1 : 0);
                    assertThat(records).noneMatch(record -> record.topic().endsWith(".U"));
                    var state = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
                    assertThat(state.definitions().stream().filter(definition -> definition.identity().physicalFile().equals("U")))
                            .allMatch(As400BindingDefinition::unobservedLayout);
                    if (through == 100) {
                        assertThat(state.definitions()).noneMatch(definition -> definition.identity().physicalFile().equals("U"));
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRecoverAnExcludedAlterRowAndDropSharingOneCommitWithoutBlockingOtherTables(boolean disk) throws Exception {
        var f = new Fixture();
        f.sharedAlterAndDrop();
        var saved = f.initial;
        for (long through : new long[]{ 88, 91, 100, 100 }) {
            var input = saved;
            boolean replay = input.completedThrough().equals(f.position(100));
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, "DCNC0005.A");
                    var runtime = f.runtime(input, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(replay || through == 91 ? 0 : 1);
                assertThat(records).noneMatch(record -> record.topic().endsWith(".U"));
                if (through < 100) {
                    assertThat(saved.replayFrom()).isEqualTo(f.position(75));
                    assertThat(saved.replayInclusive()).isTrue();
                }
                if (through == 88) {
                    assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
                }
                else {
                    var change = recovered.schemaHistory().find(f.position(78)).change();
                    assertThat(change.after()).isNull();
                    assertThat(change.visibility()).isEqualTo(f.position(91));
                    assertThat(change.commit()).isEqualTo(f.position(93));
                    assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldRestoreKnownRowsAfterSharedAlterAndDropRollbackAcrossRestart(boolean disk, boolean selected) throws Exception {
        var f = new Fixture();
        f.sharedAlterAndDropRollback();
        var saved = f.initial;
        for (long through : new long[]{ 88, 92, 100, 100 }) {
            var input = saved;
            boolean replay = input.completedThrough().equals(f.position(100));
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, selected ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(input, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
                assertThat(data).hasSize(replay || through == 92 ? 0 : selected && through == 100 ? 2 : 1);
                assertThat(data).allMatch(record -> ((Struct) record.value()).getString("op").equals("c"));
                assertThat(data).noneMatch(record -> ((Struct) record.value()).getStruct("after").getInt32("ID") == 7001);
                if (selected && through == 100 && !replay) {
                    var restored = data.stream().filter(record -> record.topic().endsWith(".U")).findFirst().orElseThrow();
                    assertThat(((Struct) restored.value()).getStruct("after").schema().field("X1")).isNull();
                }
                if (through < 100) {
                    assertThat(saved.replayFrom()).isEqualTo(f.position(75));
                    assertThat(saved.replayInclusive()).isTrue();
                }
                if (through > 88) {
                    var change = recovered.schemaHistory().find(f.position(78)).change();
                    assertThat(change.after()).isEqualTo(f.before);
                    assertThat(change.visibility()).isEqualTo(f.position(92));
                    assertThat(change.commit()).isEqualTo(f.position(96));
                    assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRetainSharedRollbackHistoryUntilPhysicalRestorationIsComplete(boolean disk) throws Exception {
        var f = new Fixture();
        f.sharedAlterAndDropRollback();
        var saved = f.initial;
        for (long through : new long[]{ 88, 92, 95, 100 }) {
            f.schemaThrough = f.cutThrough = through;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(As400RecoveryTestSupport.data(records)).hasSize(through == 88 ? 1 : through == 100 ? 2 : 0);
                if (through < 100) {
                    assertThat(saved.replayFrom()).isEqualTo(f.position(75));
                    assertThat(saved.replayInclusive()).isTrue();
                    assertThat(saved.completedThrough()).isEqualTo(f.position(88));
                    assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
                }
                else {
                    assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                    assertThat(recovered.schemaHistory().find(f.position(78)).change().after()).isEqualTo(f.before);
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({
            "compensation-member,91,Schema object identity changed",
            "compensation-cycle,91,Row from another cycle overlaps an unfinished schema change",
            "compensation-producer,91,Row/clear lacks matching producer and cycle",
            "original-cycle,87,Row from another cycle overlaps an unfinished schema change",
            "restoration-member,96,Schema object identity changed",
            "restoration-producer,96,Incomplete schema rollback restoration",
            "nested-rollback,92,Nested deletion outcome is unqualified"
    })
    void shouldRejectUnprovenSharedRollbackBeforeInstallingHistory(String fault, long sequence, String message) throws Exception {
        var f = new Fixture();
        f.sharedAlterAndDropRollback();
        var entry = f.entry(sequence);
        byte[] bytes = entry.data();
        if (fault.endsWith("member")) {
            bytes[206] ^= 1;
        }
        else if (fault.endsWith("cycle")) {
            java.nio.ByteBuffer.wrap(bytes).putLong(64, 74);
        }
        else if (fault.endsWith("producer")) {
            bytes[126] = (byte) 0xf9;
        }
        else {
            java.nio.ByteBuffer.wrap(bytes).putInt(232, 1);
        }
        f.entries.set(f.entries.indexOf(entry), new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 100)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).hasStackTraceContaining(message);
            assertThat(records).noneMatch(record -> record.topic().endsWith(".U"));
            assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRevalidateCompensationIdentityWhenReplayingSavedSharedRollback(boolean disk) throws Exception {
        var f = new Fixture();
        f.sharedAlterAndDropRollback();
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        As400Checkpoint saved;
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, new ArrayList<>(), disk, 92)) {
            saved = As400Checkpoint.fromOffset(runtime.poll(f.initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
        }
        var retained = recovered.schemaHistory().find(f.position(78)).fields();
        var compensation = f.entry(91);
        byte[] bytes = compensation.data();
        bytes[206] ^= 1;
        f.entries.set(f.entries.indexOf(compensation), new RawJournalPage.Entry(compensation.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        for (int attempt = 0; attempt < 2; attempt++) {
            var restarted = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(restarted.definitions(), false);
                    var runtime = f.runtime(saved, restarted, output, records, disk, 100)) {
                assertThatThrownBy(() -> runtime.poll(saved.toOffset())).hasStackTraceContaining("Rolled-back schema row belongs to another transaction or member");
                assertThat(records).isEmpty();
                assertThat(restarted.schemaHistory().find(f.position(78)).fields()).isEqualTo(retained);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,selected", "true,selected", "false,missing-names", "true,missing-names", "false,renamed", "true,renamed" })
    void shouldRecheckHistoricalExclusionWhenRestartingInsideACommittedAlterAndDrop(boolean disk, String fault) throws Exception {
        var f = new Fixture();
        f.sharedAlterAndDrop();
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        As400Checkpoint saved;
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, "DCNC0005.A");
                var runtime = f.runtime(f.initial, recovered, output, new ArrayList<>(), disk, 91)) {
            saved = As400Checkpoint.fromOffset(runtime.poll(f.initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            assertThat(saved.replayFrom()).isEqualTo(f.position(75));
            assertThat(saved.completedThrough()).isEqualTo(f.position(91));
        }
        var retained = recovered.schemaHistory().find(f.position(78)).fields();
        if (fault.equals("missing-names")) {
            f.historicalNames = null;
        }
        else if (fault.equals("renamed")) {
            f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                    entry.header().getLibrary(), entry.header().getFile(), "U", "A");
        }
        for (int attempt = 0; attempt < 2; attempt++) {
            int reads = f.schemaReads;
            var restarted = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(restarted.definitions(), false, fault.equals("selected") ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(saved, restarted, output, records, disk, 100)) {
                assertThatThrownBy(() -> runtime.poll(saved.toOffset())).hasStackTraceContaining("unchanged excluded historical SQL names");
                assertThat(records).isEmpty();
                assertThat(f.schemaReads).isGreaterThan(reads);
                assertThat(restarted.schemaHistory().find(f.position(78)).fields()).isEqualTo(retained);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "selected", "missing-names", "renamed", "foreign-member", "wrong-cycle", "wrong-producer", "row-cycle", "row-producer",
            "missing-member-deletion" })
    void shouldRejectUnprovenSharedAlterAndDropBeforeInstallingHistory(String fault) throws Exception {
        var f = new Fixture();
        f.sharedAlterAndDrop();
        if (fault.equals("missing-names")) {
            f.historicalNames = null;
        }
        else if (fault.equals("renamed")) {
            f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                    entry.header().getLibrary(), entry.header().getFile(), "U", "A");
        }
        if (fault.equals("missing-member-deletion")) {
            f.entries.removeIf(entry -> entry.position().equals(f.position(92)));
        }
        else if (List.of("foreign-member", "wrong-cycle", "wrong-producer", "row-cycle", "row-producer").contains(fault)) {
            long position = fault.equals("foreign-member") || fault.startsWith("row-") ? 87 : 90;
            for (int at = 0; at < f.entries.size(); at++) {
                var entry = f.entries.get(at);
                if (!entry.position().equals(f.position(position))) {
                    continue;
                }
                byte[] bytes = entry.data();
                if (fault.equals("foreign-member")) {
                    bytes[206] ^= 1;
                }
                else if (fault.equals("wrong-cycle") || fault.equals("row-cycle")) {
                    java.nio.ByteBuffer.wrap(bytes).putLong(64, 74);
                }
                else {
                    bytes[126] = (byte) 0xf9;
                }
                f.entries.set(at, new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
            }
        }
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, fault.equals("selected") ? ".*" : "DCNC0005.A");
                var runtime = f.runtime(f.initial, recovered, output, records, false, 100)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(Exception.class);
            assertThat(records).noneMatch(record -> record.topic().endsWith(".U"));
            assertThat(recovered.schemaHistory().find(f.position(78))).isNull();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false,true", "false,true,true", "true,false,true", "true,true,true", "false,true,false", "true,true,false" })
    void shouldAcquireANewTablesFinalKeyOnlyBeforeAnyRowNeedsThePreviousKey(boolean disk, boolean earlierRow, boolean selected) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        for (var item : f.range(59, earlierRow ? 64 : 63)) {
            journal.add(f.copy(item, item.position().offset().longValueExact(), 0L, null, null, null));
        }
        byte[] name = new com.ibm.as400.access.AS400Text(60, 37).toBytes(String.format("%-10s%-50s", "U", f.receiver.library()));
        journal.add(f.forTable(f.payload(f.entry(83), 65, "AC", name), created));
        journal.add(f.copy(f.entry(64), 66, 0L, null, null, null));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", created);
        // The pre-catalog pass proves lifecycle only. Output selection and the
        // first key are checked again after acquiring the actual SQL definition.
        assertThat(As400SchemaChange.qualify(null, null, f.range(59, 100), f.order, f.position(100))).isNull();
        boolean rejected = earlierRow && selected;
        var saved = f.initial;
        for (int attempt = 0; attempt < (rejected ? 1 : 2); attempt++) {
            var initial = saved;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, selected ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(saved, recovered, output, records, disk, 100)) {
                if (rejected) {
                    assertThatThrownBy(() -> runtime.poll(initial.toOffset())).hasStackTraceContaining("output key schema");
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                    assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 0 ? selected ? 2 : 1 : 0);
                    if (attempt == 0 && selected) {
                        var row = records.stream().filter(record -> record.topic().endsWith(".ROLLBACKT")).findFirst().orElseThrow();
                        var key = (Struct) row.key();
                        assertThat(key.schema().fields()).hasSize(1);
                        assertThat(key.getInt32("ID")).isEqualTo(((Struct) row.value()).getStruct("after").getInt32("ID"));
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false,false,true", "true,false,false,true", "false,true,false,true", "true,true,false,true", "false,false,true,true",
            "true,true,true,true", "false,false,true,false", "true,false,true,false", "false,true,true,false", "true,true,true,false" })
    void shouldRecoverCreationFollowedByAlterOnlyWhenNoRowRequiresTheIntermediateDefinition(boolean disk, boolean transactional, boolean earlierRow, boolean selected)
            throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        if (transactional) {
            journal.add(f.entry(58));
        }
        for (var item : f.range(59, 63)) {
            journal.add(f.copy(item, item.position().offset().longValueExact(), transactional ? null : 0L, null, null, null));
        }
        if (earlierRow) {
            journal.add(f.copy(f.entry(64), 64, transactional ? null : 0L, null, null, null));
        }
        journal.add(f.copy(f.entry(100), 65, 0L, null, null, null));
        long cycle = transactional ? 58 : 75;
        if (!transactional) {
            journal.add(f.copy(f.entry(75), 75, 75L, null, f.entry(59), null));
        }
        for (var item : f.range(76, 85)) {
            var changed = f.copy(item, item.position().offset().longValueExact(), item.header().getCommitCycle().signum() == 0 ? null : cycle,
                    null, f.entry(59), null);
            journal.add(item.header().getJournalCode() == 'C' ? changed : f.forTable(changed, created));
        }
        journal.add(f.copy(f.entry(67), 86, cycle, null, f.entry(59), "CM"));
        journal.add(f.widenedRow(87, 0, f.entry(59), created));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", f.widened(created));
        assertThat(As400SchemaChange.qualify(null, null, f.range(59, 100), f.order, f.position(100))).isNull();
        boolean rejected = earlierRow && selected;
        var saved = f.initial;
        for (int attempt = 0; attempt < (rejected ? 1 : selected ? 2 : 3); attempt++) {
            long through = !selected && attempt == 0 ? 65 : 100;
            var initial = saved;
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, selected ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                if (rejected) {
                    assertThatThrownBy(() -> runtime.poll(initial.toOffset())).hasStackTraceContaining("intermediate table definition");
                    assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                    if (through == 65) {
                        assertThat(saved.replayFrom()).isEqualTo(f.position(transactional ? 58 : 59));
                        assertThat(saved.replayInclusive()).isTrue();
                    }
                    var data = As400RecoveryTestSupport.data(records);
                    assertThat(data).hasSize(selected ? attempt == 0 ? 3 : 0 : attempt < 2 ? 1 : 0);
                    if (!selected) {
                        assertThat(records).noneMatch(record -> record.topic().endsWith(".ROLLBACKT"));
                    }
                    if (attempt == 0 && selected) {
                        var row = records.stream().filter(record -> record.topic().endsWith(".ROLLBACKT") && record.value() != null).findFirst().orElseThrow();
                        assertThat(((Struct) row.value()).getStruct("after").getInt32("X1")).isEqualTo(7);
                    }
                    if (transactional && through == 65) {
                        assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
                    }
                    else {
                        var change = recovered.schemaHistory().find(f.position(59)).change();
                        assertThat(change.commit()).isEqualTo(f.position(86));
                        assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,original", "true,original", "false,renamed", "true,renamed", "false,excluded", "true,excluded", "false,label", "true,label",
            "false,short-original", "true,short-original", "false,short-renamed", "true,short-renamed" })
    void shouldPreserveHistoricalSelectionWhenAnAlterPrecedesCreatedRowsAndMetadata(boolean disk, String selection) throws Exception {
        var f = new Fixture();
        var created = f.sqlRenamed(f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(),
                f.entry(61).header().getRecordInfo().objectJournalId()), "LONG_ORIGINAL_NAME");
        var acquired = f.sqlRenamed(f.widened(created),
                selection.equals("label") ? "LONG_ORIGINAL_NAME" : selection.startsWith("short-") ? "ROLLBACKT" : "LONG_RENAMED_NAME");
        var journal = new ArrayList<RawJournalPage.Entry>();
        for (var item : f.range(59, 63)) {
            journal.add(f.copy(item, item.position().offset().longValueExact(), 0L, null, null, null));
        }
        for (var item : f.range(75, 85)) {
            var changed = f.copy(item, item.position().offset().longValueExact(), null, null, f.entry(59), null);
            journal.add(item.header().getJournalCode() == 'C' ? changed : f.forTable(changed, created));
        }
        journal.add(f.copy(f.entry(67), 86, 75L, null, f.entry(59), "CM"));
        journal.add(f.widenedRow(87, 0, f.entry(59), created));
        journal.addAll(f.metadataChange(90, created, 0, 0, f.entry(59)));
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", acquired);
        var nameReads = new ArrayList<JournalPosition>();
        f.historicalNames = entry -> {
            nameReads.add(entry.position());
            return new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                    entry.header().getLibrary(), entry.header().getFile(),
                    entry.position().equals(f.position(91)) && selection.equals("label") ? null : "LONG_ORIGINAL_NAME",
                    entry.position().equals(f.position(91)) && !selection.equals("label") && !selection.startsWith("short-") ? "LONG_RENAMED_NAME" : null);
        };
        String selected = switch (selection) {
            case "original", "short-original", "label" -> "(A|LONG_ORIGINAL_NAME)";
            case "renamed" -> "(A|LONG_RENAMED_NAME)";
            case "short-renamed" -> "(A|ROLLBACKT)";
            default -> "A";
        };
        boolean rejected = selection.equals("original") || selection.equals("renamed") || selection.startsWith("short-");
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var input = saved;
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, "DCNC0005\\." + selected);
                    var runtime = f.runtime(input, recovered, output, records, disk, 100)) {
                if (rejected) {
                    assertThatThrownBy(() -> runtime.poll(input.toOffset())).hasStackTraceContaining("earlier SQL name before rename");
                    assertThat(records).isEmpty();
                    assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
                    assertThat(f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy).definitions())
                            .containsExactlyInAnyOrderElementsOf(f.baseline);
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(100));
                    assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 0 ? selection.equals("label") ? 2 : 1 : 0);
                    if (selection.equals("label") && attempt == 0) {
                        var row = records.stream().filter(record -> record.topic().endsWith(".LONG_ORIGINAL_NAME")).findFirst().orElseThrow();
                        assertThat(((Struct) row.value()).getStruct("after").getInt32("X1")).isEqualTo(7);
                    }
                    else {
                        assertThat(records).noneMatch(record -> record.topic().endsWith(".LONG_RENAMED_NAME"));
                    }
                }
            }
        }
        assertThat(nameReads).contains(f.position(59), f.position(83), f.position(91));
    }

    @ParameterizedTest
    @CsvSource({ "false,old", "true,old", "false,new", "true,new", "false,excluded", "true,excluded", "false,label", "true,label",
            "false,short-old", "true,short-old", "false,short-new", "true,short-new" })
    void shouldKeepAnEmptyClearBoundToItsHistoricalSqlNameBeforeRename(boolean disk, String selection) throws Exception {
        var f = new Fixture();
        var source = f.entries.stream().collect(java.util.stream.Collectors.toMap(e -> e.position().offset().longValueExact(), e -> e));
        var created = f.sqlRenamed(f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(),
                f.entry(61).header().getRecordInfo().objectJournalId()), "LONG_ORIGINAL_NAME");
        var renamed = f.sqlRenamed(created, selection.equals("label") ? "LONG_ORIGINAL_NAME" : selection.startsWith("short-") ? "ROLLBACKT" : "LONG_RENAMED_NAME");
        var metadata = f.metadataChange(110, created, 108, 2, source.get(59L));
        f.creationAndClear(false);
        // Keep the real empty-clear lifecycle and durable creation commit;
        // remove every row so this probes the clear's SQL name independently.
        var journal = new ArrayList<>(f.entries.stream().filter(entry -> entry.header().getJournalCode() != 'R').toList());
        journal.add(f.copy(source.get(75L), 108, 108L, null, source.get(59L), null));
        journal.add(f.copy(source.get(76L), 109, 108L, null, source.get(59L), null));
        journal.addAll(metadata);
        journal.add(f.copy(source.get(85L), 112, 108L, 109L, source.get(59L), null));
        journal.add(f.copy(source.get(67L), 113, 108L, null, source.get(59L), "CM"));
        journal.add(f.copy(source.get(100L), 116, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", renamed);
        f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                entry.header().getLibrary(), entry.header().getFile(),
                entry.position().equals(f.position(111)) && selection.equals("label") ? null : "LONG_ORIGINAL_NAME",
                entry.position().equals(f.position(111)) && !selection.equals("label") && !selection.startsWith("short-") ? "LONG_RENAMED_NAME" : null);
        boolean rejected = selection.equals("old") || selection.equals("new") || selection.startsWith("short-");
        String selected = switch (selection) {
            case "old", "short-old", "label" -> "(A|LONG_ORIGINAL_NAME)";
            case "new" -> "(A|LONG_RENAMED_NAME)";
            case "short-new" -> "(A|ROLLBACKT)";
            default -> "A";
        };
        var saved = f.initial;
        for (int attempt = 0; attempt < 2; attempt++) {
            var input = saved;
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, "DCNC0005\\." + selected);
                    var runtime = f.runtime(input, recovered, output, records, disk, 116)) {
                if (rejected) {
                    assertThatThrownBy(() -> runtime.poll(input.toOffset())).hasStackTraceContaining("earlier SQL name before rename");
                    assertThat(records).isEmpty();
                    assertThat(recovered.schemaHistory().find(f.position(59))).isNull();
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    assertThat(saved.completedThrough()).isEqualTo(f.position(116));
                    assertThat(As400RecoveryTestSupport.data(records)).hasSize(attempt == 0 ? selection.equals("label") ? 2 : 1 : 0);
                    if (selection.equals("label") && attempt == 0) {
                        var clear = records.stream().filter(record -> record.topic().endsWith(".LONG_ORIGINAL_NAME")).findFirst().orElseThrow();
                        assertThat(((Struct) clear.value()).getString("op")).isEqualTo("t");
                        assertThat(clear.key()).isNull();
                    }
                    assertThat(records).noneMatch(record -> record.topic().endsWith(".LONG_RENAMED_NAME"));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReplayAnInterruptedSchemaBatchWithoutRepeatingTheEarlierCompletedPrefix(boolean disk) throws Exception {
        var f = new Fixture();
        f.openAlter(false);
        f.cutThrough = 88;
        var saved = f.initial;
        for (int attempt = 0; attempt < 3; attempt++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            var input = saved;
            f.failAfterRecords = attempt == 1 ? 1 : -1;
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 100)) {
                if (attempt == 1) {
                    assertThatThrownBy(() -> runtime.poll(input.toOffset())).hasStackTraceContaining("Interrupted test delivery");
                    assertThat(records).hasSize(1);
                    assertThat(records.get(0).sourceOffset().get("completed_through.sequence")).isEqualTo("86");
                }
                else {
                    saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                    var sequences = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null)
                            .map(record -> ((Struct) record.value()).getStruct("source").getString("sequence")).toList();
                    assertThat(sequences).containsExactlyElementsOf(attempt == 0 ? List.of("86") : List.of("87", "88", "100"));
                }
            }
            f.cutThrough = 100;
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldEnforceTheSharedPayloadBudgetBeforeRetainingUnfinishedSchemaRows(boolean disk, boolean creation) throws Exception {
        var f = new Fixture();
        if (creation) {
            var journal = f.range(58, 64);
            f.entries.clear();
            f.entries.addAll(journal);
            f.cutThrough = 64;
        }
        else {
            f.openAlter(false);
            f.cutThrough = 88;
        }
        f.payloadBudget = 300;
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, disk, 100)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).hasStackTraceContaining("capacity exceeded");
            assertThat(records).isEmpty();
            assertThat(f.schemaReads).isZero();
            assertThat(recovered.schemaHistory().find(f.position(creation ? 59 : 78))).isNull();
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldRecoverReverseCommitOrderWithDeferredOrExistingEagerSchemaHistory(boolean disk, boolean existingEagerHistory) throws Exception {
        var f = new Fixture();
        var uProducer = f.entry(78);
        byte[] producer = f.entry(100).data();
        java.nio.ByteBuffer.wrap(producer).putLong(40, 999);
        new com.ibm.as400.access.AS400Text(6, 37).toBytes("000009", producer, 121);
        var aProducer = new RawJournalPage.Entry(f.position(100), new EntryHeaderDecoder(37).decode(producer, 0), producer);
        var a = f.baseline.get(1);
        var journal = new ArrayList<>(f.range(74, 85));
        journal.add(f.copy(f.entry(75), 91, 91L, null, aProducer, null));
        journal.add(f.copy(f.entry(76), 92, 91L, null, aProducer, null));
        journal.add(f.copy(f.entry(77), 93, 91L, null, aProducer, null));
        for (long sequence = 78; sequence <= 83; sequence++) {
            var original = f.entry(sequence);
            journal.add(f.forTable(f.copy(original, sequence + 16,
                    original.header().getCommitCycle().signum() == 0 ? null : 91L, null, aProducer, null), a));
        }
        journal.add(f.copy(f.entry(84), 100, 91L, 93L, aProducer, null));
        journal.add(f.copy(f.entry(85), 101, 91L, 92L, aProducer, null));
        journal.add(f.widenedRow(102, 91, aProducer, a));
        journal.add(f.copy(f.entry(90), 103, 91L, null, aProducer, "CM"));
        journal.add(f.widenedRow(104, 75, uProducer, f.before));
        journal.add(f.copy(f.entry(90), 105, 75L, null, uProducer, "CM"));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", f.widened());
        f.after.put("A", f.widened(a));
        if (existingEagerHistory) {
            var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
            var oldChange = As400SchemaChange.qualify(f.before, f.widened(), f.range(78, 105), f.order, f.position(105));
            var oldRecord = recovered.schemaHistory().append(As400SchemaState.ORIGIN, oldChange);
            assertThat(oldRecord.fields()).doesNotContainKey("change.visibility");
            assertThat(As400SchemaHistory.Record.restore(oldRecord.fields())).isEqualTo(oldRecord);
        }
        var saved = f.initial;
        for (int restart = 0; restart < 3; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), true);
                    var runtime = f.runtime(saved, recovered, output, records, disk, restart == 0 ? 103 : 105)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
            assertThat(data).hasSize(restart < 2 ? 1 : 0);
            if (restart < 2) {
                assertThat(data.get(0).topic()).endsWith(restart == 0 ? ".A" : ".U");
                assertThat(((Struct) data.get(0).value()).getStruct("after").getInt32("X1")).isEqualTo(7);
            }
            assertThat(saved.completedThrough()).isEqualTo(f.position(restart == 0 ? 103 : 105));
            assertThat(saved.schemaState().completed().ordinal()).isEqualTo(restart == 0 && !existingEagerHistory ? 1 : 2);
            if (restart >= 1) {
                assertThat(recovered.schemaHistory().find(f.position(78)).cursor().ordinal()).isEqualTo(existingEagerHistory ? 1 : 2);
                assertThat(recovered.schemaHistory().find(f.position(94)).cursor().ordinal()).isEqualTo(existingEagerHistory ? 2 : 1);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRetainExcludedIntermediateRowsWithoutTreatingARepeatedAlterAsAnotherLayoutInstallation(boolean disk) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(f.range(74, 85));
        journal.add(f.widenedRow(87, 75, f.entry(78), f.before));
        journal.add(f.copy(f.entry(76), 92, 75L, null, null, null));
        journal.add(f.copy(f.entry(77), 93, 75L, null, null, null));
        for (long sequence = 78; sequence <= 83; sequence++) {
            journal.add(f.copy(f.entry(sequence), sequence + 16, null, null, null, null));
        }
        journal.add(f.copy(f.entry(84), 100, 75L, 93L, null, null));
        journal.add(f.copy(f.entry(85), 101, 75L, 92L, null, null));
        journal.add(f.copy(f.entry(90), 105, 75L, null, f.entry(78), "CM"));
        journal.add(f.copy(f.entry(100), 106, null, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", f.widened());
        var saved = f.initial;
        for (int restart = 0; restart < 2; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, ".*\\.A");
                    var runtime = f.runtime(saved, recovered, output, records, disk, 106)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            assertThat(records).hasSize(restart == 0 ? 1 : 0);
            assertThat(saved.completedThrough()).isEqualTo(f.position(106));
            assertThat(saved.schemaState().completed().ordinal()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRetainRollbackSchemaUntilAnEarlierClearReleasesTheDeferredTerminal(boolean disk) throws Exception {
        var f = new Fixture();
        var a = f.baseline.get(1);
        var journal = new ArrayList<>(f.range(74, 77));
        for (long sequence = 78; sequence <= 83; sequence++) {
            var entry = f.forTable(f.entry(sequence), a);
            byte[] raw = entry.data();
            if (sequence == 78 || sequence == 83) {
                raw[entry.header().getEntrySpecificDataOffset() + 16 + (sequence == 83 ? 108 : 0)] = (byte) 0xf8;
            }
            journal.add(new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        }
        journal.addAll(f.range(84, 85));
        journal.add(f.copy(f.entry(76), 92, 75L, null, null, null));
        journal.add(f.copy(f.entry(77), 93, 75L, null, null, null));
        for (long sequence = 78; sequence <= 83; sequence++) {
            journal.add(f.copy(f.entry(sequence), sequence + 16, null, null, null, null));
        }
        journal.add(f.copy(f.entry(84), 100, 75L, 93L, null, null));
        journal.add(f.copy(f.entry(85), 101, 75L, 92L, null, null));
        journal.add(f.widenedRow(104, 75, f.entry(78), f.before));
        journal.add(f.copy(f.entry(90), 110, 75L, null, null, null));
        for (long sequence = 91; sequence <= 94; sequence++) {
            journal.add(f.copy(f.entry(sequence), sequence + 20, null, null, null, null));
        }
        for (long sequence = 91; sequence <= 94; sequence++) {
            journal.add(f.forTable(f.copy(f.entry(sequence), sequence + 24, null, null, null, null), a));
        }
        journal.add(f.copy(f.entry(100), 120, null, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        var saved = f.initial;
        for (int restart = 0; restart < 3; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, restart == 0 ? 118 : 120)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            assertThat(records).hasSize(restart == 1 ? 1 : 0);
            assertThat(saved.completedThrough()).isEqualTo(f.position(restart == 0 ? 110 : 120));
            assertThat(saved.schemaState().completed().ordinal()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDiscardRolledBackDdlRowsAndRecoverBeforePhysicalRestorationFinishes(boolean disk) throws Exception {
        var f = new Fixture();
        var rollback = As400SchemaChange.qualify(f.before, f.before, f.range(78, 94), f.order, f.position(94));
        assertThat(rollback.records()).noneMatch(mark -> mark.type().startsWith("R/"));
        assertThat(As400SchemaChange.restore(rollback.fields())).isEqualTo(rollback);
        var saved = f.initial;
        for (int restart = 0; restart < 3; restart++) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, restart == 0 ? 90 : 100)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            var data = records.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("op") != null).toList();
            assertThat(data).hasSize(restart == 1 ? 1 : 0);
            if (restart == 1) {
                assertThat(((Struct) data.get(0).value()).getStruct("after").getInt32("ID")).isEqualTo(10000);
            }
            if (restart == 0) {
                assertThat(saved.completedThrough()).isEqualTo(f.position(90));
                assertThat(saved.replayFrom()).isEqualTo(f.position(75));
            }
            assertThat(f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy).definitions()).containsExactlyInAnyOrderElementsOf(f.baseline);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "cycle", "member", "producer" })
    void shouldNotDiscardForeignRowsInsideARolledBackSchemaInterval(String fault) throws Exception {
        var f = new Fixture();
        var row = f.entry(87);
        byte[] raw = row.data();
        switch (fault) {
            case "cycle" -> java.nio.ByteBuffer.wrap(raw).putLong(64, 999);
            case "member" -> raw[206] ^= 1;
            case "producer" -> raw[126] = (byte) 0xf9;
            default -> throw new IllegalArgumentException(fault);
        }
        f.entries.set(f.entries.indexOf(row), new RawJournalPage.Entry(row.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, true, 100)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(RuntimeException.class)
                    .hasStackTraceContaining(fault.equals("producer") ? "producer" : fault.equals("cycle") ? "another cycle" : "another transaction or member");
        }
        assertThat(records).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(longs = { 66, 67, 69, 89, 90, 93 })
    void shouldWaitForCompletePhysicalRollbackWithoutSubstitutingTheCurrentCatalog(long through) throws Exception {
        var f = new Fixture();
        boolean creation = through < 75;
        assertThatThrownBy(() -> As400SchemaChange.qualify(creation ? null : f.before, creation ? null : f.before,
                f.range(creation ? 59 : 78, through), f.order, f.position(through)))
                .isInstanceOf(As400SchemaTimeline.PendingChangeException.class);
    }

    @ParameterizedTest
    @CsvSource({ "GT,valid", "GT,missing-scope", "GT,wrong-scope", "GT,producer",
            "AC,valid", "AC,missing-scope", "AC,wrong-scope", "AC,producer",
            "DC,valid", "DC,missing-scope", "DC,wrong-scope", "DC,producer" })
    void shouldAcceptTransactionalObjectMetadataOnlyWithinItsRealSavepoint(String type, String fault) throws Exception {
        var f = new Fixture();
        f.entries.clear();
        try (var input = getClass().getResourceAsStream("/native-transactions/transactional-ddl-rollback.json")) {
            for (var item : new ObjectMapper().readTree(input).path("administrative_entries")) {
                byte[] raw = Base64.getDecoder().decode(item.path("raw").asText());
                var header = new EntryHeaderDecoder(37).decode(raw, 0);
                if (fault.equals("missing-scope") && header.getEntryType().equals("SB")) {
                    continue;
                }
                if (header.getJournalCode() == 'D') {
                    new com.ibm.as400.access.AS400Text(2, 37).toBytes(type, raw, 99);
                    System.arraycopy(java.util.HexFormat.of().parseHex(f.before.identity().fileJournalId()), 0, raw, 197, 10);
                    if (fault.equals("wrong-scope")) {
                        java.nio.ByteBuffer.wrap(raw).putInt(232, 1);
                    }
                    if (fault.equals("producer")) {
                        raw[126] = (byte) 0xf9;
                    }
                }
                f.entries.add(new RawJournalPage.Entry(f.position(header.getSequenceNumber().longValueExact()), new EntryHeaderDecoder(37).decode(raw, 0), raw));
            }
        }
        var restored = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                var runtime = f.runtime(f.initial, restored, output, records, false, 66)) {
            if (fault.equals("valid")) {
                var progress = runtime.poll(f.initial.toOffset());
                assertThat(progress.checkpoint()).containsEntry("completed_through.sequence", "65");
            }
            else {
                assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
            }
        }
        assertThat(records).isEmpty();
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "false,true", "true,false", "true,true" })
    void shouldRetainPublishedKeysAcrossConstraintChangesAndRestartWithAnOpenTransaction(boolean disk, boolean metadataAfterRemoval) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<RawJournalPage.Entry>();
        byte[] name = new com.ibm.as400.access.AS400Text(60, 37).toBytes(String.format("%-10s%-50s", "U", f.receiver.library()));
        var added = f.payload(f.entry(83), 77, "AC", name);
        byte[] raw = f.copy(added, 77, 75L, null, f.entry(78), null).data();
        java.nio.ByteBuffer.wrap(raw).putInt(232, Math.toIntExact(f.entry(76).header().getControlInfo().nestedCommitLevel()));
        journal.add(new RawJournalPage.Entry(f.position(77), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        journal.addAll(f.range(75, 76));
        journal.add(f.copy(f.entry(85), 78, 75L, 76L, null, null));
        journal.add(f.copy(f.entry(90), 80, 75L, null, null, "CM"));
        journal.add(f.payload(f.entry(83), 81, "DC", name));
        if (metadataAfterRemoval) {
            journal.addAll(f.metadataChange(84, f.before, 0, 0, f.entry(78)));
        }
        for (long sequence : new long[]{ 72, 79, 82 }) {
            var row = f.copy(f.entry(44), sequence, sequence == 79 ? 75L : 0L, null, f.entry(78), null);
            raw = row.data();
            java.nio.ByteBuffer.wrap(raw).putInt(row.header().getEntrySpecificDataOffset() + 16, (int) sequence);
            journal.add(new RawJournalPage.Entry(f.position(sequence), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        }
        journal.add(f.entry(100));
        journal.sort(java.util.Comparator.comparing(entry -> entry.position().offset()));
        f.entries.clear();
        f.entries.addAll(journal);
        // A current catalog may have lost its primary key. Constraint records
        // must not retroactively replace the key chosen for this capture.
        f.after.put("U", new As400BindingDefinition(f.before.identity(), f.before.schema().edit().setPrimaryKeyNames(List.of()).create(),
                f.before.codecs(), f.before.members()));
        var saved = f.initial;
        for (int attempt = 0; attempt < 3; attempt++) {
            var restored = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(restored.definitions(), false);
                    var runtime = f.runtime(saved, restored, output, records, disk, attempt == 0 ? 79 : 100)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                var rows = records.stream().filter(record -> record.topic().endsWith(".U")).toList();
                assertThat(rows.stream().map(row -> ((Struct) row.key()).getInt32("ID")))
                        .containsExactlyElementsOf(attempt == 0 ? List.of(72) : attempt == 1 ? List.of(79, 82) : List.of());
                assertThat(rows).allSatisfy(row -> assertThat(((Struct) row.value()).getStruct("after").getInt32("ID"))
                        .isEqualTo(((Struct) row.key()).getInt32("ID")));
                assertThat(saved.completedThrough()).isEqualTo(f.position(attempt == 0 ? 72 : 100));
                if (attempt == 0) {
                    assertThat(saved.replayFrom()).isEqualTo(f.position(72));
                    assertThat(saved.replayInclusive()).isFalse();
                }
                if (metadataAfterRemoval && attempt > 0) {
                    assertThat(saved.schemaState().completed().ordinal()).isEqualTo(1);
                    assertThat(f.schemaReads).isEqualTo(1);
                }
                else {
                    assertThat(saved.schemaState()).isEqualTo(As400SchemaState.EMPTY);
                    assertThat(f.schemaReads).isZero();
                }
            }
        }
    }

    @Test
    void shouldRejectChangedRollbackProducerBeforeAcceptingItsRestoration() throws Exception {
        var f = new Fixture();
        var rows = new ArrayList<>(f.range(78, 94));
        byte[] raw = rows.get(rows.size() - 1).data();
        raw[126] = (byte) 0xf9;
        rows.set(rows.size() - 1, new RawJournalPage.Entry(f.position(94), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        assertThatThrownBy(() -> As400SchemaChange.qualify(f.before, f.before, rows, f.order, f.position(94)))
                .hasMessageContaining("restoration");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRetireOnlyTheRemovedMemberAndPreserveThePhysicalFile(boolean primary) throws Exception {
        var f = new Fixture();
        var id = new As400TransactionBuffer.TableIdentity(new TableId(null, f.receiver.library(), "PD"), "PD", "PD",
                f.entry(47).header().getRecordInfo().objectJournalId(), f.entry(51).header().getRecordInfo().objectJournalId(), "PD:2B50BDA2B97E5:10:1:false");
        var one = As400BindingDefinition.excluded(id);
        var before = new As400BindingDefinition(id, one.schema(), one.codecs(),
                Map.of("PD", id.memberJournalId(), "M2", f.entry(54).header().getRecordInfo().objectJournalId()));
        var records = new ArrayList<RawJournalPage.Entry>();
        for (var original : f.range(96, 98)) {
            byte[] raw = original.data();
            if (primary) {
                var name = new com.ibm.as400.access.AS400Text(10, 37);
                name.toBytes("PD", raw, 177);
                System.arraycopy(java.util.HexFormat.of().parseHex(id.memberJournalId()), 0, raw, 197, 10);
                if (original.header().getEntryType().equals("DM")) {
                    name.toBytes("PD", raw, original.header().getEntrySpecificDataOffset() + 16 + 20);
                }
            }
            records.add(new RawJournalPage.Entry(original.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw));
        }
        var change = As400MemberCreation.deletion(before, records, f.order);
        assertThat(change.after().members()).containsOnlyKeys(primary ? "M2" : "PD");
        assertThat(change.after().identity().fileJournalId()).isEqualTo(before.identity().fileJournalId());
        assertThat(As400SchemaChange.restore(change.fields())).isEqualTo(change);
        assertThatThrownBy(() -> As400MemberCreation.deletion(before, records.subList(0, 1), f.order))
                .isInstanceOf(As400SchemaTimeline.PendingChangeException.class);
        byte[] wrong = records.get(1).data();
        wrong[206] ^= 1;
        records.set(1, new RawJournalPage.Entry(f.position(98), new EntryHeaderDecoder(37).decode(wrong, 0), wrong));
        assertThatThrownBy(() -> As400MemberCreation.deletion(before, records, f.order)).hasMessageContaining("identity");
    }

    private static java.util.stream.Stream<Arguments> clearNameCases() {
        return java.util.stream.Stream.of(false, true).flatMap(disk -> java.util.stream.Stream.of("old", "new", "both", "label", "rollback-old", "rollback-new")
                .flatMap(variant -> java.util.stream.Stream.of(3, 8).map(subtype -> Arguments.of(disk, variant, subtype))));
    }

    private static String nameSelection(String variant) {
        if (variant.equals("both")) {
            return "DCNC0005\\.(A|LONG_ORIGINAL_NAME|LONG_RENAMED_NAME)";
        }
        boolean newName = variant.equals("new") || variant.equals("rollback-new");
        return "DCNC0005\\.(A|" + (newName ? "LONG_RENAMED_NAME" : "LONG_ORIGINAL_NAME") + ")";
    }

    private static List<String> nameLedger(List<SourceRecord> records) {
        return records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null)
                .map(record -> record.topic() + ":" + ((Struct) record.value()).getString("op")).toList();
    }

    private List<SourceRecord> recoverNameChanges(Fixture f, boolean disk, String variant, long... cuts) throws Exception {
        var saved = f.initial;
        var all = new ArrayList<SourceRecord>();
        for (int attempt = 0; attempt < cuts.length; attempt++) {
            var input = saved;
            var recovered = f.history().recoverOnlineCandidate(input.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, nameSelection(variant));
                    var runtime = f.runtime(input, recovered, output, records, disk, cuts[attempt])) {
                saved = As400Checkpoint.fromOffset(runtime.poll(input.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            if (attempt == cuts.length - 1) {
                assertThat(records).as("Completed fresh recovery must not duplicate output").isEmpty();
            }
            all.addAll(records);
        }
        return all;
    }

    private void clearOutcome(Fixture f, List<RawJournalPage.Entry> journal, boolean rollback, int subtype) {
        for (var entry : f.laterClear()) {
            byte[] bytes = entry.data();
            if (entry.header().getEntryType().equals("CH")) {
                bytes[entry.header().getEntrySpecificDataOffset() + 16] = (byte) (0xf0 + subtype);
            }
            else if (entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("CG")) {
                bytes[entry.header().getEntrySpecificDataOffset() + 16 + 108] = (byte) (0xf0 + subtype);
            }
            journal.add(new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        journal.add(f.copy(f.entry(90), 99, 90L, null, f.entry(78), rollback ? "RB" : "CM"));
        if (rollback) {
            for (var restored : f.range(91, 94)) {
                journal.add(f.copy(restored, restored.position().offset().longValueExact() + 9, null, null, f.entry(78), null));
            }
        }
    }

    private void metadataTransaction(Fixture f, List<RawJournalPage.Entry> journal, long sc,
                                     As400BindingDefinition table, boolean rollback) {
        journal.add(f.copy(f.entry(75), sc, sc, null, f.entry(78), null));
        journal.add(f.copy(f.entry(76), sc + 1, sc, null, f.entry(78), null));
        for (var entry : f.metadataChange(sc + 2, f.before, sc, 2, f.entry(78))) {
            journal.add(f.atFile(entry, table.identity().physicalFile()));
        }
        journal.add(f.copy(f.entry(85), sc + 4, sc, sc + 1, f.entry(78), null));
        journal.add(f.copy(f.entry(90), sc + 5, sc, null, f.entry(78), rollback ? "RB" : "CM"));
    }

    @ParameterizedTest
    @MethodSource("clearNameCases")
    void shouldPreserveClearTopicAcrossLaterSqlRename(boolean disk, String variant, int subtype) throws Exception {
        var f = new Fixture(true);
        boolean rolledBackClear = variant.startsWith("rollback-");
        var after = f.sqlRenamed(f.before, variant.equals("label") ? "LONG_ORIGINAL_NAME" : "LONG_RENAMED_NAME");
        var journal = new ArrayList<>(f.metadataChange(70, f.before, 0, 0, f.entry(78)));
        journal.add(f.copy(f.entry(100), 88, 0L, null, null, null));
        clearOutcome(f, journal, rolledBackClear, subtype);
        journal.add(f.copy(f.entry(100), 104, 0L, null, null, null));
        metadataTransaction(f, journal, 110, f.before, false);
        journal.add(f.copy(f.entry(100), 120, 0L, null, null, null));
        f.after.put("U", after);
        f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                entry.header().getLibrary(), entry.header().getFile(),
                entry.position().equals(f.position(113)) && !variant.equals("label") ? "LONG_ORIGINAL_NAME" : null,
                entry.position().equals(f.position(113)) && !variant.equals("label") ? "LONG_RENAMED_NAME" : null);
        f.entries.clear();
        f.entries.addAll(journal);
        var all = recoverNameChanges(f, disk, variant, 88, 104, 120, 120);
        var clears = nameLedger(all).stream().filter(item -> item.endsWith(":t")).toList();
        boolean selectedEarlier = !variant.equals("new") && !variant.equals("rollback-new");
        assertThat(clears).hasSize(!rolledBackClear && selectedEarlier ? 1 : 0);
        assertThat(clears).allMatch(item -> item.contains(".LONG_ORIGINAL_NAME:t"));
    }

    @ParameterizedTest
    @MethodSource("clearNameCases")
    void shouldPreserveClearTopicAfterSqlRenameBeforeDrop(boolean disk, String variant, int subtype) throws Exception {
        var f = new Fixture(true);
        boolean rolledBackClear = variant.startsWith("rollback-");
        var renamed = f.sqlRenamed(f.before, variant.equals("label") ? "LONG_ORIGINAL_NAME" : "LONG_RENAMED_NAME");
        var journal = new ArrayList<RawJournalPage.Entry>();
        metadataTransaction(f, journal, 68, f.before, false);
        journal.add(f.copy(f.entry(100), 88, 0L, null, null, null));
        clearOutcome(f, journal, rolledBackClear, subtype);
        journal.add(f.copy(f.entry(100), 104, 0L, null, null, null));
        journal.addAll(f.drop(120, renamed, "U", 0));
        journal.add(f.copy(f.entry(100), 124, 0L, null, null, null));
        f.after.put("U", renamed);
        f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                entry.header().getLibrary(), entry.header().getFile(),
                entry.position().equals(f.position(71)) && !variant.equals("label") ? "LONG_ORIGINAL_NAME" : null,
                entry.position().equals(f.position(71)) && !variant.equals("label") ? "LONG_RENAMED_NAME" : null);
        f.entries.clear();
        f.entries.addAll(journal);
        var all = recoverNameChanges(f, disk, variant, 88, 104, 124, 124);
        var clears = nameLedger(all).stream().filter(item -> item.endsWith(":t")).toList();
        boolean selectedAtClear = variant.equals("new") || variant.equals("both") || variant.equals("label");
        assertThat(clears).hasSize(!rolledBackClear && selectedAtClear ? 1 : 0);
        assertThat(clears).allMatch(item -> item.contains("." + renamed.schema().id().table() + ":t"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-reader", "wrong-digest", "wrong-prior-name", "wrong-file", "wrong-producer", "rolled-back-name", "savepoint-rollback" })
    void shouldRejectUnprovenClearNameBeforeDrop(String fault) throws Exception {
        var f = new Fixture(true);
        var journal = new ArrayList<RawJournalPage.Entry>();
        metadataTransaction(f, journal, 68, f.before, fault.equals("rolled-back-name"));
        if (fault.equals("savepoint-rollback")) {
            journal.set(4, f.copy(journal.get(4), 72, 68L, 69L, f.entry(78), "SU"));
        }
        if (fault.equals("wrong-producer")) {
            var terminal = journal.get(journal.size() - 1);
            byte[] bytes = terminal.data();
            bytes[126] = (byte) 0xf9;
            journal.set(journal.size() - 1, new RawJournalPage.Entry(terminal.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        clearOutcome(f, journal, false, 3);
        journal.addAll(f.drop(120, f.before, "U", 0));
        As400HistoricalTableNames.Reader names = fault.equals("missing-reader") ? null
                : entry -> new As400HistoricalTableNames(fault.equals("wrong-digest") ? "0".repeat(64)
                        : As400JournalRecordContent.digest(entry.data(), entry.position()), entry.header().getLibrary(),
                        fault.equals("wrong-file") ? "FOREIGN" : entry.header().getFile(),
                        fault.equals("wrong-prior-name") ? "FOREIGN" : "LONG_ORIGINAL_NAME", "LONG_RENAMED_NAME");
        As400SourceNormalizer.TableResolver bindings = (file, library) -> f.before.binding(65536);
        var window = journal.subList(2, journal.size()); // Acquisition starts at F/CH, after the transaction controls.
        assertThatThrownBy(() -> As400FileDeletion.afterSchemaChange(f.before, window, f.order, bindings, names))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> As400NativeSchemaRefresh.qualifyAcquired(f.before, f.before, window, f.order, f.position(124), bindings, names))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private RawJournalPage.Entry row(Fixture f, long seq, int id, As400BindingDefinition table) {
        var entry = f.atFile(f.copy(f.entry(44), seq, 0L, null, null, null), table.identity().physicalFile());
        assertThat(entry.header().getMember()).isEqualTo(table.identity().member());
        assertThat(entry.header().getRecordInfo().objectJournalId()).isEqualTo(table.identity().memberJournalId());
        byte[] bytes = entry.data();
        java.nio.ByteBuffer.wrap(bytes).putInt(entry.header().getEntrySpecificDataOffset() + 16, id);
        return new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes);
    }

    @ParameterizedTest
    @CsvSource({ "false,old", "true,old", "false,new", "true,new", "false,label", "true,label",
            "false,rollback-old", "true,rollback-old", "false,rollback-new", "true,rollback-new" })
    void shouldPreserveRowTopicsAcrossPhysicalAndSqlRenames(boolean disk, String variant) throws Exception {
        var f = new Fixture(true);
        boolean rollbackRename = variant.startsWith("rollback-");
        boolean changedName = !rollbackRename && !variant.equals("label");
        var physicalAlias = f.sqlRenamed(f.renamed(f.before, "RENAMED"), "LONG_ORIGINAL_NAME");
        var acquired = f.sqlRenamed(physicalAlias, changedName ? "LONG_RENAMED_NAME" : "LONG_ORIGINAL_NAME");
        var journal = new ArrayList<>(f.rename(80, f.before, "RENAMED"));
        journal.add(row(f, 82, 303, physicalAlias));
        metadataTransaction(f, journal, 90, physicalAlias, rollbackRename);
        journal.add(row(f, 96, 304, physicalAlias));
        journal.add(f.entry(100));
        f.after.put("U", acquired);
        f.after.put("RENAMED", acquired);
        f.historicalNames = entry -> new As400HistoricalTableNames(As400JournalRecordContent.digest(entry.data(), entry.position()),
                entry.header().getLibrary(), entry.header().getFile(), variant.equals("label") ? null : "LONG_ORIGINAL_NAME",
                variant.equals("label") ? null : "LONG_RENAMED_NAME");
        f.entries.clear();
        f.entries.addAll(journal);
        var all = recoverNameChanges(f, disk, variant, 82, 100, 100);
        var rows = all.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null)
                .map(record -> record.topic() + ":" + ((Struct) record.value()).getString("op") + ":"
                        + ((Struct) record.value()).getStruct("after").getInt32("ID"))
                .toList().stream().filter(item -> !item.contains(".A:")).toList();
        boolean selectsNew = variant.equals("new") || variant.equals("rollback-new");
        var expected = new ArrayList<String>();
        if (!selectsNew) {
            expected.add("offline.DCNC0005.LONG_ORIGINAL_NAME:c:303");
        }
        if (changedName ? selectsNew : !selectsNew) {
            expected.add("offline.DCNC0005." + (changedName ? "LONG_RENAMED_NAME" : "LONG_ORIGINAL_NAME") + ":c:304");
        }
        assertThat(rows).containsExactlyElementsOf(expected);
    }

    private List<RawJournalPage.Entry> clearWithInternalSavepoint(Fixture f, boolean rollback, String fault) {
        var records = new ArrayList<RawJournalPage.Entry>();
        clearOutcome(f, records, rollback, 8);
        var result = new ArrayList<RawJournalPage.Entry>();
        for (var entry : records) {
            long sequence = entry.position().offset().longValueExact();
            result.add(f.copy(entry, sequence > 94 ? sequence + 2 : sequence, null, null, null, null));
            if (sequence == 94) {
                long level = f.entry(76).header().getControlInfo().nestedCommitLevel() + 1;
                var start = f.copy(f.entry(76), 95, fault.equals("wrong-cycle") ? 123L : 90L, null, f.entry(78), null);
                var end = f.copy(f.entry(85), 96, 90L, fault.equals("wrong-target") ? 91L : 95L, f.entry(78), null);
                for (var control : List.of(start, end)) {
                    if (control == end && fault.equals("missing-release")) {
                        continue;
                    }
                    byte[] bytes = control.data();
                    java.nio.ByteBuffer.wrap(bytes).putInt(232, Math.toIntExact(fault.equals("outer-level") ? level - 1 : level));
                    if (control == start && fault.equals("wrong-producer")) {
                        bytes[126] = (byte) 0xf9;
                    }
                    result.add(new RawJournalPage.Entry(control.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
                }
            }
        }
        result.add(f.copy(f.entry(100), 107, 0L, null, null, null));
        return result;
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldRecoverClearAcrossItsInternalSavepointWithoutReleasingTheOuterStatement(boolean disk, boolean rollback) throws Exception {
        var f = new Fixture();
        var entries = clearWithInternalSavepoint(f, rollback, "valid");
        f.entries.clear();
        f.entries.addAll(entries);
        var saved = f.initial;
        var all = new ArrayList<SourceRecord>();
        for (long cut : new long[]{ 95, 96, 107, 107 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, cut)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            if (cut <= 96) {
                assertThat(records).isEmpty();
                assertThat(saved.completedThrough()).isEqualTo(f.initial.completedThrough());
                assertThat(saved.replayFrom()).isEqualTo(f.initial.replayFrom());
            }
            all.addAll(records);
        }
        assertThat(nameLedger(all)).containsExactlyElementsOf(rollback ? List.of("offline.DCNC0005.A:c")
                : List.of("offline.DCNC0005.U:t", "offline.DCNC0005.A:c"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "wrong-cycle", "wrong-target", "outer-level", "missing-release", "wrong-producer" })
    void shouldRejectClearWithAnUnmatchedInternalSavepoint(String fault) throws Exception {
        var f = new Fixture();
        var entries = clearWithInternalSavepoint(f, false, fault);
        f.entries.clear();
        f.entries.addAll(entries);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 107)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
        }
        assertThat(records).isEmpty();
    }

    @Test
    void shouldRequireExplicitMemberlessHistoryWithoutRevivingTheRetiredIdentity() throws Exception {
        var f = new Fixture();
        assertThatThrownBy(() -> new As400BindingDefinition(f.before.identity(), f.before.schema(), f.before.codecs(), Map.of()))
                .hasMessageContaining("Canonical member is absent");
        var empty = f.before.withoutMembers();
        var stored = empty.storedTable();
        assertThat(stored.attributeWithName("ibmi.binding.version").value()).isEqualTo("3");
        assertThat(As400BindingDefinition.restore(stored).members()).isEmpty();
        var legacy = stored.edit().removeAttribute("ibmi.binding.version")
                .addAttribute(io.debezium.relational.Attribute.editor().name("ibmi.binding.version").value("2").create()).create();
        assertThatThrownBy(() -> As400BindingDefinition.restore(legacy)).hasMessageContaining("Version 2 requires");
        var extra = stored.edit().addAttribute(io.debezium.relational.Attribute.editor().name("ibmi.binding.members.U")
                .value(f.before.identity().memberJournalId()).create()).create();
        assertThatThrownBy(() -> As400BindingDefinition.restore(extra)).hasMessageContaining("unsupported binding attributes");
    }

    private List<RawJournalPage.Entry> lastMemberRemoval(Fixture f) {
        var result = new ArrayList<RawJournalPage.Entry>();
        int next = 70;
        for (int sequence : new int[]{ 96, 98 }) {
            var entry = f.copy(f.forTable(f.entry(sequence), f.before), next++, 0L, null, null, null);
            byte[] bytes = entry.data();
            var name = new com.ibm.as400.access.AS400Text(10, 37);
            name.toBytes(f.before.identity().member(), bytes, 177);
            if (sequence == 96) {
                name.toBytes(f.before.identity().member(), bytes, entry.header().getEntrySpecificDataOffset() + 16 + 20);
            }
            result.add(new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        return result;
    }

    @ParameterizedTest
    @CsvSource({ "false,U,false", "true,U,false", "false,M2,false", "true,M2,false", "false,U,true", "true,U,true", "false,M2,true", "true,M2,true" })
    void shouldRecoverPhysicalFileWithoutMembersAndResumeOnlyAfterItsNewAssociation(boolean disk, String member, boolean excluded) throws Exception {
        var f = new Fixture();
        var removal = lastMemberRemoval(f);
        var empty = As400MemberCreation.deletion(f.before, removal, f.order).after();
        assertThat(empty.memberless()).isTrue();
        assertThat(empty.identities()).isEmpty();
        assertThat(As400BindingDefinition.restore(empty.storedTable())).isEqualTo(empty);
        assertThatThrownBy(() -> empty.binding(65536).forMember("U")).hasMessageContaining("Unknown member");
        assertThatThrownBy(() -> empty.binding(65536).values(f.entry(44).header(), f.entry(44).data(), 0)).hasMessageContaining("no active member");
        assertThatThrownBy(() -> empty.binding(65536).requireTableWideClear()).hasMessageContaining("no active member");
        var journal = new ArrayList<>(removal);
        journal.add(f.copy(f.entry(100), 72, 0L, null, null, null));
        var id = f.before.identity();
        String jid = f.entry(54).header().getRecordInfo().objectJournalId();
        var newIdentity = new As400TransactionBuffer.TableIdentity(id.tableId(), id.physicalFile(), member, id.fileJournalId(), jid, id.recordFormat());
        var replacement = new As400BindingDefinition(newIdentity, f.before.schema(), f.before.codecs());
        var text = new com.ibm.as400.access.AS400Text(10, 37);
        for (long seq = 52; seq <= 54; seq++) {
            var entry = f.copy(f.forTable(f.entry(seq), replacement), seq + 28, 0L, null, null, null);
            byte[] bytes = entry.data();
            boolean file = entry.header().getJournalCode() == 'D';
            text.toBytes(file ? "" : member, bytes, 177);
            if (file) {
                text.toBytes(member, bytes, entry.header().getEntrySpecificDataOffset() + 16 + 30);
            }
            else if (entry.header().getEntryType().equals("MC")) {
                text.toBytes(member, bytes, entry.header().getEntrySpecificDataOffset() + 16 + 20);
            }
            journal.add(new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        var row = f.copy(f.forTable(f.entry(44), replacement), 83, 0L, null, null, null);
        byte[] bytes = row.data();
        text.toBytes(member, bytes, 177);
        journal.add(new RawJournalPage.Entry(row.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        journal.add(f.copy(f.entry(100), 84, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        var saved = f.initial;
        var all = new ArrayList<SourceRecord>();
        int attempt = 0;
        for (long cut : new long[]{ 70, 72, 72, 80, 84, 84 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, excluded ? "DCNC0005.A" : "DCNC0005.(A|U)");
                    var runtime = f.runtime(saved, recovered, output, records, disk, cut)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            if (attempt == 1) {
                assertThat(nameLedger(records)).containsExactly("offline.DCNC0005.A:c");
            }
            if (attempt == 2 || attempt == 5) {
                assertThat(records).isEmpty();
            }
            all.addAll(records);
            attempt++;
        }
        assertThat(nameLedger(all)).containsExactlyElementsOf(excluded ? List.of("offline.DCNC0005.A:c", "offline.DCNC0005.A:c")
                : List.of("offline.DCNC0005.A:c", "offline.DCNC0005.U:c", "offline.DCNC0005.A:c"));
        assertThat(saved.completedThrough()).isEqualTo(f.position(84));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRejectRowsFromTheRemovedMemberAcrossFreshRecovery(boolean disk) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(lastMemberRemoval(f));
        journal.add(f.copy(f.entry(100), 72, 0L, null, null, null));
        journal.add(f.copy(f.entry(44), 80, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        var saved = f.initial;
        var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(saved, recovered, output, new ArrayList<>(), disk, 72)) {
            saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
        }
        assertThat(saved.completedThrough()).isEqualTo(f.position(72));
        var retained = saved;
        byte[] retainedHistory = java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"));
        for (int attempt = 0; attempt < 2; attempt++) {
            recovered = f.history().recoverOnlineCandidate(retained.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(retained, recovered, output, records, disk, 80)) {
                assertThatThrownBy(() -> runtime.poll(retained.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class)
                        .hasRootCauseMessage("Unknown member in verified file binding");
            }
            assertThat(records).isEmpty();
            assertThat(java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(retainedHistory);
        }
    }

    private RawJournalPage.Entry memberRename(Fixture f, long sequence, String from, String to) {
        var entry = f.rename(sequence - 1, f.before, "RENAMED").get(1);
        var name = new com.ibm.as400.access.AS400Text(10, 37);
        byte[] bytes = entry.data();
        int payload = entry.header().getEntrySpecificDataOffset() + 16;
        name.toBytes(from, bytes, 177);
        name.toBytes(from, bytes, payload + 20);
        name.toBytes(f.before.identity().physicalFile(), bytes, payload + 30);
        name.toBytes(to, bytes, payload + 50);
        return new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes);
    }

    private RawJournalPage.Entry memberRow(Fixture f, long sequence, String member, int value) {
        var entry = f.copy(f.entry(44), sequence, 0L, null, null, null);
        byte[] bytes = entry.data();
        new com.ibm.as400.access.AS400Text(10, 37).toBytes(member, bytes, 177);
        java.nio.ByteBuffer.wrap(bytes).putInt(entry.header().getEntrySpecificDataOffset() + 16, value);
        return new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes);
    }

    @ParameterizedTest
    @CsvSource({ "false,false", "true,false", "false,true", "true,true" })
    void shouldRetainRowKeysAndTopicAcrossMemberRenameAndFreshRecovery(boolean disk, boolean excluded) throws Exception {
        var f = new Fixture();
        var journal = List.of(memberRename(f, 70, "U", "RENAMED"), f.copy(f.entry(100), 71, 0L, null, null, null),
                memberRow(f, 72, "RENAMED", 701), memberRename(f, 80, "RENAMED", "U"),
                memberRow(f, 81, "U", 702), f.copy(f.entry(100), 82, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        var saved = f.initial;
        var all = new ArrayList<SourceRecord>();
        int attempt = 0;
        for (long cut : new long[]{ 70, 72, 72, 80, 82, 82 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, excluded ? "DCNC0005.A" : "DCNC0005.(A|U)");
                    var runtime = f.runtime(saved, recovered, output, records, disk, cut)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            if (attempt == 2 || attempt == 5) {
                assertThat(records).isEmpty();
            }
            all.addAll(records);
            attempt++;
        }
        assertThat(nameLedger(all)).containsExactlyElementsOf(excluded ? List.of("offline.DCNC0005.A:c", "offline.DCNC0005.A:c")
                : List.of("offline.DCNC0005.A:c", "offline.DCNC0005.U:c", "offline.DCNC0005.U:c", "offline.DCNC0005.A:c"));
        assertThat(all.stream().filter(record -> record.topic().endsWith(".U"))
                .map(record -> ((org.apache.kafka.connect.data.Struct) record.key()).getInt32("ID")).toList())
                .containsExactlyElementsOf(excluded ? List.of() : List.of(701, 702));
        assertThat(saved.completedThrough()).isEqualTo(f.position(82));
    }

    @ParameterizedTest
    @ValueSource(strings = { "foreign-jid", "foreign-file", "foreign-library", "unknown-member", "same-name", "transactional", "short-payload" })
    void shouldRejectUnprovenMemberRenameBeforeOutputOrHistory(String fault) throws Exception {
        var f = new Fixture();
        var entry = memberRename(f, 70, "U", "RENAMED");
        byte[] bytes = entry.data();
        var text = new com.ibm.as400.access.AS400Text(10, 37);
        int payload = entry.header().getEntrySpecificDataOffset() + 16;
        switch (fault) {
            case "foreign-jid" -> bytes[206] ^= 1;
            case "foreign-file" -> text.toBytes("FOREIGN", bytes, payload + 30);
            case "foreign-library" -> text.toBytes("FOREIGN", bytes, payload + 40);
            case "unknown-member" -> text.toBytes("UNKNOWN", bytes, 177);
            case "same-name" -> text.toBytes("U", bytes, payload + 50);
            case "transactional" -> java.nio.ByteBuffer.wrap(bytes).putLong(64, 70);
            case "short-payload" -> bytes = f.payload(entry, 70, "MN", new byte[50]).data();
            default -> throw new AssertionError(fault);
        }
        f.entries.clear();
        f.entries.add(new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        byte[] history = java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"));
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 70)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("Member rename");
        }
        assertThat(records).isEmpty();
        assertThat(java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRejectTheFormerMemberNameAfterRecoveringItsRename(boolean disk) throws Exception {
        var f = new Fixture();
        var rename = memberRename(f, 70, "U", "RENAMED");
        var progress = f.copy(f.entry(100), 71, 0L, null, null, null);
        var stale = memberRow(f, 72, "U", 701);
        f.entries.clear();
        f.entries.addAll(List.of(rename, progress, stale));
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        As400Checkpoint saved;
        var completed = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, completed, disk, 71)) {
            saved = As400Checkpoint.fromOffset(runtime.poll(f.initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
        }
        assertThat(nameLedger(completed)).containsExactly("offline.DCNC0005.A:c");
        assertThat(saved.completedThrough()).isEqualTo(f.position(71));
        byte[] history = java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"));
        for (int attempt = 0; attempt < 2; attempt++) {
            recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 72)) {
                assertThatThrownBy(() -> runtime.poll(saved.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class)
                        .hasRootCauseMessage("Unknown member in verified file binding");
            }
            assertThat(records).isEmpty();
            assertThat(java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
        }
    }

    @Test
    void shouldRenameOnlyTheProvenMemberWithoutReplacingThePrimaryOrAnotherMember() throws Exception {
        var f = new Fixture();
        var id = f.before.identity();
        var secondJid = f.entry(54).header().getRecordInfo().objectJournalId();
        var members = new java.util.TreeMap<>(Map.of(id.member(), id.memberJournalId(), "M2", secondJid));
        var before = new As400BindingDefinition(id, f.before.schema(), f.before.codecs(), members);
        var entry = memberRename(f, 70, "M2", "RENAMED");
        byte[] bytes = entry.data();
        System.arraycopy(java.util.HexFormat.of().parseHex(secondJid), 0, bytes, 197, 10);
        var owned = new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes);
        var after = As400MemberCreation.rename(before, owned, f.order).after();
        assertThat(after.identity()).isEqualTo(id);
        assertThat(after.members()).containsExactlyEntriesOf(new java.util.TreeMap<>(Map.of(id.member(), id.memberJournalId(), "RENAMED", secondJid)));
        assertThat(after.binding(65536).forMember(id.member()).identity()).isEqualTo(id);
        assertThat(after.binding(65536).forMember("RENAMED").identity().memberJournalId()).isEqualTo(secondJid);
        new com.ibm.as400.access.AS400Text(10, 37).toBytes(id.member(), bytes, entry.header().getEntrySpecificDataOffset() + 16 + 50);
        var collision = new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes);
        assertThatThrownBy(() -> As400MemberCreation.rename(before, collision, f.order)).hasMessageContaining("distinct member names");
    }

    private List<RawJournalPage.Entry> fileMove(Fixture f, long sequence, String from, String to) {
        return fileMove(f, f.before, sequence, from, to);
    }

    private List<RawJournalPage.Entry> fileMove(Fixture f, As400BindingDefinition before, long sequence, String from, String to) {
        var result = new ArrayList<RawJournalPage.Entry>();
        var text = new com.ibm.as400.access.AS400Text(10, 37);
        for (var entry : f.rename(sequence, before, "RENAMED")) {
            byte[] bytes = entry.data();
            boolean file = entry.header().getJournalCode() == 'D';
            new com.ibm.as400.access.AS400Text(2, 37).toBytes(file ? "FM" : "MM", bytes, 99);
            text.toBytes(from, bytes, 167);
            int payload = entry.header().getEntrySpecificDataOffset() + 16;
            text.toBytes(from, bytes, payload + 10);
            text.toBytes(before.identity().physicalFile(), bytes, payload + 30);
            text.toBytes(to, bytes, payload + 40);
            result.add(new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        return result;
    }

    private RawJournalPage.Entry inLibrary(RawJournalPage.Entry entry, String library) {
        byte[] bytes = entry.data();
        new com.ibm.as400.access.AS400Text(10, 37).toBytes(library, bytes, 167);
        return new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes);
    }

    @ParameterizedTest
    @CsvSource({ "false,both", "true,both", "false,excluded", "true,excluded", "false,old", "true,old", "false,new", "true,new" })
    void shouldPreserveUnrelatedOutputAndRowIdentityAcrossFileMovesAndFreshRecovery(boolean disk, String selection) throws Exception {
        var f = new Fixture(true);
        var journal = new ArrayList<>(fileMove(f, 70, "DCNC0005", "DCNC0006"));
        journal.add(1, f.copy(f.entry(100), 71, 0L, null, null, null));
        journal.set(2, f.copy(journal.get(2), 72, 0L, null, null, null));
        journal.add(inLibrary(memberRow(f, 73, "U", 701), "DCNC0006"));
        journal.addAll(fileMove(f, 80, "DCNC0006", "DCNC0005"));
        journal.add(memberRow(f, 82, "U", 702));
        journal.add(f.copy(f.entry(100), 83, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        String include = switch (selection) {
            case "old" -> "DCNC0005.A,DCNC0005.LONG_ORIGINAL_NAME";
            case "new" -> "DCNC0005.A,DCNC0006.LONG_ORIGINAL_NAME";
            case "both" -> "DCNC0005.A,DCNC000[56].LONG_ORIGINAL_NAME";
            default -> "DCNC0005.A";
        };
        var expectedTopics = new ArrayList<>(List.of("offline.DCNC0005.A:c"));
        var expectedIds = new ArrayList<Integer>();
        if (selection.equals("new") || selection.equals("both")) {
            expectedTopics.add("offline.DCNC0006.LONG_ORIGINAL_NAME:c");
            expectedIds.add(701);
        }
        if (selection.equals("old") || selection.equals("both")) {
            expectedTopics.add("offline.DCNC0005.LONG_ORIGINAL_NAME:c");
            expectedIds.add(702);
        }
        expectedTopics.add("offline.DCNC0005.A:c");
        var saved = f.initial;
        var all = new ArrayList<SourceRecord>();
        for (long cut : new long[]{ 71, 73, 73, 80, 83, 83 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false,
                    include);
                    var runtime = f.runtime(saved, recovered, output, records, disk, cut)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
            }
            if (cut == 71) {
                assertThat(saved.completedThrough()).isEqualTo(f.position(71));
                assertThat(saved.replayFrom()).isEqualTo(f.position(70));
                assertThat(saved.replayInclusive()).isTrue();
            }
            all.addAll(records);
        }
        assertThat(nameLedger(all)).containsExactlyElementsOf(expectedTopics);
        assertThat(all.stream().filter(record -> record.topic().endsWith(".LONG_ORIGINAL_NAME"))
                .map(record -> ((org.apache.kafka.connect.data.Struct) record.key()).getInt32("ID")).toList())
                .containsExactlyElementsOf(expectedIds);
        assertThat(saved.completedThrough()).isEqualTo(f.position(83));
    }

    @ParameterizedTest
    @ValueSource(strings = { "file-jid", "member-jid", "file-name", "member-name", "library", "same-library", "producer", "cycle", "short-payload" })
    void shouldRejectUnprovenFileMovesBeforeOutputAndHistory(String fault) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(fileMove(f, 70, "DCNC0005", "DCNC0006"));
        int at = fault.equals("file-jid") || fault.equals("same-library") ? 0 : 1;
        var entry = journal.get(at);
        byte[] bytes = entry.data();
        var text = new com.ibm.as400.access.AS400Text(10, 37);
        int payload = entry.header().getEntrySpecificDataOffset() + 16;
        switch (fault) {
            case "file-jid", "member-jid" -> bytes[206] ^= 1;
            case "file-name" -> text.toBytes("FOREIGN", bytes, payload + 30);
            case "member-name" -> text.toBytes("FOREIGN", bytes, payload + 50);
            case "library" -> text.toBytes("DCNC0007", bytes, payload + 40);
            case "same-library" -> text.toBytes("DCNC0005", bytes, payload + 40);
            case "producer" -> bytes[126] ^= 1;
            case "cycle" -> java.nio.ByteBuffer.wrap(bytes).putLong(64, 70);
            case "short-payload" -> bytes = f.payload(entry, 71, "MM", new byte[50]).data();
            default -> throw new AssertionError(fault);
        }
        journal.set(at, new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        f.entries.clear();
        f.entries.addAll(journal);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        byte[] history = java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"));
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 71)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageStartingWith("File move");
        }
        assertThat(records).isEmpty();
        assertThat(java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRejectRowsUnderTheFormerLibraryAfterMoveRecovery(boolean disk) throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<>(fileMove(f, 70, "DCNC0005", "DCNC0006"));
        journal.add(f.copy(f.entry(100), 72, 0L, null, null, null));
        journal.add(memberRow(f, 73, "U", 701));
        f.entries.clear();
        f.entries.addAll(journal);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        As400Checkpoint saved;
        var published = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, published, disk, 72)) {
            saved = As400Checkpoint.fromOffset(runtime.poll(f.initial.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
        }
        assertThat(nameLedger(published)).containsExactly("offline.DCNC0005.A:c");
        assertThat(saved.completedThrough()).isEqualTo(f.position(72));
        byte[] history = java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"));
        for (int attempt = 0; attempt < 2; attempt++) {
            recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                    var runtime = f.runtime(saved, recovered, output, records, disk, 73)) {
                assertThatThrownBy(() -> runtime.poll(saved.toOffset())).isInstanceOf(As400ReplayCoordinator.RecoveryException.class)
                        .hasRootCauseMessage("Missing recovered schema binding");
            }
            assertThat(records).isEmpty();
            assertThat(java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
        }
    }

    @Test
    void shouldRequireEveryRetainedMemberBeforeSavingAFileMove() throws Exception {
        var f = new Fixture();
        var id = f.before.identity();
        String secondJid = f.entry(54).header().getRecordInfo().objectJournalId();
        var before = new As400BindingDefinition(id, f.before.schema(), f.before.codecs(),
                new java.util.TreeMap<>(Map.of(id.member(), id.memberJournalId(), "M2", secondJid)));
        var journal = new ArrayList<>(fileMove(f, 70, "DCNC0005", "DCNC0006"));
        assertThatThrownBy(() -> As400FileRename.move(before, journal, f.order)).isInstanceOf(As400SchemaTimeline.PendingChangeException.class);
        var second = f.copy(journal.get(1), 72, 0L, null, null, null);
        byte[] bytes = second.data();
        var text = new com.ibm.as400.access.AS400Text(10, 37);
        text.toBytes("M2", bytes, 177);
        text.toBytes("M2", bytes, second.header().getEntrySpecificDataOffset() + 16 + 20);
        text.toBytes("M2", bytes, second.header().getEntrySpecificDataOffset() + 16 + 50);
        System.arraycopy(java.util.HexFormat.of().parseHex(secondJid), 0, bytes, 197, 10);
        journal.add(new RawJournalPage.Entry(second.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        var moved = As400FileRename.move(before, journal, f.order);
        assertThat(moved.after().members()).isEqualTo(before.members());
        assertThat(moved.after().identity().tableId()).isEqualTo(new TableId(null, "DCNC0006", "U"));
        assertThat(moved.records()).hasSize(3);
        assertThat(moved.after().schema().edit().tableId(before.schema().id()).create()).isEqualTo(before.schema());
    }

    @Test
    void shouldNotMoveAFileWithUncommittedRows() throws Exception {
        var f = new Fixture();
        var journal = new ArrayList<RawJournalPage.Entry>();
        journal.add(f.copy(f.entry(75), 60, 60L, null, null, null));
        journal.add(f.copy(memberRow(f, 65, "U", 701), 65, 60L, null, f.entry(75), null));
        journal.addAll(fileMove(f, 70, "DCNC0005", "DCNC0006"));
        f.entries.clear();
        f.entries.addAll(journal);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        var records = new ArrayList<SourceRecord>();
        byte[] history = java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"));
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 71)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).hasMessageContaining("Schema refresh crosses pending changes");
        }
        assertThat(f.schemaReads).isZero();
        assertThat(records).isEmpty();
        assertThat(java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
    }

    private As400BindingDefinition movedDefinition(As400BindingDefinition before, String library) {
        var id = before.identity();
        var table = new TableId(id.tableId().catalog(), library, id.tableId().table());
        return new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(table, id.physicalFile(), id.member(), id.fileJournalId(),
                id.memberJournalId(), id.recordFormat()), before.schema().edit().tableId(table).create(), before.codecs(), before.members());
    }

    private List<RawJournalPage.Entry> alterThenMove(Fixture f) {
        var changed = f.widened();
        var producer = f.entry(78);
        var journal = new ArrayList<>(f.range(74, 85));
        journal.add(f.copy(f.entry(67), 86, 75L, null, producer, "CM"));
        journal.add(f.widenedRow(87, 0, producer, changed));
        journal.add(f.copy(f.entry(100), 88, 0L, null, null, null));
        journal.addAll(fileMove(f, 90, "DCNC0005", "DCNC0006"));
        journal.add(inLibrary(f.widenedRow(94, 0, producer, changed), "DCNC0006"));
        return journal;
    }

    @ParameterizedTest
    @CsvSource({ "false,true,false", "true,true,false", "false,false,false", "true,false,false", "false,true,true", "true,true,true" })
    void shouldRecoverAnAlterBeforeALaterFileMoveWithRowsUnderTheirOriginalLibraries(boolean disk, boolean selected, boolean roundTrip) throws Exception {
        var f = new Fixture(true);
        var changed = f.widened();
        var moved = movedDefinition(changed, "DCNC0006");
        var journal = alterThenMove(f);
        if (roundTrip) {
            journal.addAll(fileMove(f, 96, "DCNC0006", "DCNC0005"));
            journal.add(f.widenedRow(98, 0, f.entry(78), changed));
        }
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", roundTrip ? changed : moved);
        var saved = f.initial;
        for (long through : new long[]{ 88, 100, 100 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            boolean replay = saved.completedThrough().equals(f.position(100));
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, selected ? ".*" : "DCNC0005.A");
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
                assertThat(data).hasSize(replay ? 0 : selected ? roundTrip && through == 100 ? 3 : 2 : 1);
                if (!replay && selected) {
                    assertThat(data.get(0).topic()).isEqualTo(through == 88 ? "offline.DCNC0005.LONG_ORIGINAL_NAME" : "offline.DCNC0006.LONG_ORIGINAL_NAME");
                    assertThat(((Struct) data.get(0).key()).getInt32("ID")).isEqualTo(7001);
                    assertThat(((Struct) data.get(0).value()).getStruct("after").getInt32("X1")).isEqualTo(7);
                    if (roundTrip && through == 100) {
                        assertThat(data.get(1).topic()).isEqualTo("offline.DCNC0005.LONG_ORIGINAL_NAME");
                        assertThat(((Struct) data.get(1).value()).getStruct("after").getInt32("X1")).isEqualTo(7);
                    }
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "catalog-library", "catalog-sql-name", "catalog-file-jid", "catalog-member-jid", "row-jid", "later-layout" })
    void shouldRejectAnUnprovenAlterMoveCatalogBeforePublishingOrSavingHistory(String fault) throws Exception {
        var f = new Fixture(true);
        var journal = alterThenMove(f);
        var acquired = movedDefinition(f.widened(), "DCNC0006");
        if (fault.equals("catalog-library")) {
            acquired = movedDefinition(acquired, "DCNC0007");
        }
        else if (fault.equals("catalog-sql-name")) {
            acquired = f.sqlRenamed(acquired, "UNPROVEN_NAME");
        }
        else if (fault.startsWith("catalog-")) {
            var id = acquired.identity();
            var memberJid = fault.equals("catalog-member-jid") ? "0000000000000000fffe" : id.memberJournalId();
            acquired = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(id.tableId(), id.physicalFile(), id.member(),
                    fault.equals("catalog-file-jid") ? "0000000000000000fffe" : id.fileJournalId(), memberJid, id.recordFormat()),
                    acquired.schema(), acquired.codecs(), Map.of(id.member(), memberJid));
        }
        else if (fault.equals("row-jid")) {
            var row = journal.remove(journal.size() - 1);
            byte[] bytes = row.data();
            bytes[206] ^= 1;
            journal.add(new RawJournalPage.Entry(row.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        else {
            journal.add(inLibrary(f.copy(f.entry(78), 96, 0L, null, null, null), "DCNC0006"));
        }
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("U", acquired);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        byte[] history = java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"));
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 100)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(records).isEmpty();
        assertThat(java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
    }

    @Test
    void shouldReobserveAnAlterWhenItsFileMovesAfterCatalogAcquisition() throws Exception {
        var f = new Fixture(true);
        var journal = alterThenMove(f);
        var evidence = journal.stream().filter(entry -> entry.position().offset().longValueExact() >= 78).toList();
        assertThatThrownBy(() -> As400NativeSchemaRefresh.qualifyAcquired(f.before, f.widened(), evidence, f.order,
                f.position(88), (file, library) -> f.before.binding(65536)))
                .isInstanceOf(As400SchemaChange.MetadataMovedException.class);
        var secondMove = fileMove(f, 120, "DCNC0006", "DCNC0007");
        assertThat(As400NativeSchemaRefresh.disappearedAfter("U", "DCNC0006", f.before.identity().fileJournalId(), f.position(110), secondMove, f.order)).isTrue();
        assertThat(As400NativeSchemaRefresh.disappearedAfter("U", "DCNC0005", f.before.identity().fileJournalId(), f.position(110), secondMove, f.order)).isFalse();
    }

    @ParameterizedTest
    @CsvSource({ "false,old", "true,old", "false,new", "true,new", "false,excluded", "true,excluded" })
    void shouldRecoverACreationBeforeALibraryMoveWithoutLosingEarlierRows(boolean disk, String selection) throws Exception {
        var f = new Fixture();
        var created = f.sqlRenamed(f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(),
                f.entry(61).header().getRecordInfo().objectJournalId()), "LONG_CREATED_NAME");
        var journal = new ArrayList<RawJournalPage.Entry>();
        for (var entry : f.range(59, 63)) {
            journal.add(f.copy(entry, entry.position().offset().longValueExact(), 0L, null, null, null));
        }
        for (int index = 0; index < 2; index++) {
            if (index == 1) {
                journal.add(f.copy(f.entry(100), 68, 0L, null, null, null));
                var moved = fileMove(f, created, 70, "DCNC0005", "DCNC0006");
                journal.add(moved.get(0));
                journal.add(f.copy(f.entry(100), 71, 0L, null, null, null));
                journal.add(f.copy(moved.get(1), 72, 0L, null, null, null));
            }
            var row = f.copy(f.entry(64), index == 0 ? 66 : 73, 0L, null, null, null);
            byte[] bytes = row.data();
            java.nio.ByteBuffer.wrap(bytes).putInt(row.header().getEntrySpecificDataOffset() + 16, 811 + index);
            row = new RawJournalPage.Entry(row.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes);
            journal.add(index == 0 ? row : inLibrary(row, "DCNC0006"));
        }
        journal.add(f.copy(f.entry(100), 80, 0L, null, null, null));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", movedDefinition(created, "DCNC0006"));
        assertThat(As400FileCreation.acquiredLocation(journal, f.order))
                .isEqualTo(new As400FileCreation.CatalogLocation("ROLLBACKT", "DCNC0006"));
        String include = "DCNC0005.A" + (selection.equals("excluded") ? ""
                : "," + (selection.equals("old") ? "DCNC0005" : "DCNC0006") + ".LONG_CREATED_NAME");
        var saved = f.initial;
        var all = new ArrayList<SourceRecord>();
        for (long through : new long[]{ 68, 71, 80, 80 }) {
            var recovered = f.history().recoverOnlineCandidate(saved.toOffset(), "fixture-journal", f.policy);
            var records = new ArrayList<SourceRecord>();
            try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false, include);
                    var runtime = f.runtime(saved, recovered, output, records, disk, through)) {
                saved = As400Checkpoint.fromOffset(runtime.poll(saved.toOffset()).checkpoint(), "fixture-journal", f.initial.captureFingerprint());
                assertThat(saved.completedThrough()).isEqualTo(f.position(through));
                if (through == 71) {
                    assertThat(saved.replayFrom()).isEqualTo(f.position(70));
                    assertThat(saved.replayInclusive()).isTrue();
                }
                all.addAll(records);
            }
        }
        var expected = new ArrayList<String>();
        if (selection.equals("old")) {
            expected.add("offline.DCNC0005.LONG_CREATED_NAME:c");
        }
        expected.add("offline.DCNC0005.A:c");
        expected.add("offline.DCNC0005.A:c");
        if (selection.equals("new")) {
            expected.add("offline.DCNC0006.LONG_CREATED_NAME:c");
        }
        expected.add("offline.DCNC0005.A:c");
        assertThat(nameLedger(all)).containsExactlyElementsOf(expected);
        assertThat(all.stream().filter(record -> record.topic().endsWith(".LONG_CREATED_NAME"))
                .map(record -> ((Struct) record.key()).getInt32("ID")).toList())
                .containsExactlyElementsOf(selection.equals("excluded") ? List.of() : List.of(selection.equals("old") ? 811 : 812));
    }

    @ParameterizedTest
    @ValueSource(strings = { "catalog-library", "catalog-file-jid", "catalog-member-jid", "move-member-jid", "moved-row-jid", "later-layout" })
    void shouldRejectUnprovenMovedCreationsBeforeOutputOrHistory(String fault) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        for (var entry : f.range(59, 63)) {
            journal.add(f.copy(entry, entry.position().offset().longValueExact(), 0L, null, null, null));
        }
        journal.addAll(fileMove(f, created, 70, "DCNC0005", "DCNC0006"));
        journal.add(inLibrary(f.copy(f.entry(64), 74, 0L, null, null, null), "DCNC0006"));
        var acquired = movedDefinition(created, "DCNC0006");
        if (fault.equals("catalog-library")) {
            acquired = movedDefinition(acquired, "DCNC0007");
        }
        else if (fault.startsWith("catalog-")) {
            var id = acquired.identity();
            var memberJid = fault.equals("catalog-member-jid") ? "0000000000000000fffe" : id.memberJournalId();
            acquired = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(id.tableId(), id.physicalFile(), id.member(),
                    fault.equals("catalog-file-jid") ? "0000000000000000fffe" : id.fileJournalId(), memberJid, id.recordFormat()),
                    acquired.schema(), acquired.codecs(), Map.of(id.member(), memberJid));
        }
        else if (fault.endsWith("jid")) {
            int at = journal.size() - (fault.equals("move-member-jid") ? 2 : 1);
            var entry = journal.get(at);
            byte[] bytes = entry.data();
            bytes[206] ^= 1;
            journal.set(at, new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(37).decode(bytes, 0), bytes));
        }
        else {
            journal.add(inLibrary(f.forTable(f.copy(f.entry(78), 76, 0L, null, null, null), created), "DCNC0006"));
        }
        journal.add(f.entry(100));
        f.entries.clear();
        f.entries.addAll(journal);
        f.after.put("ROLLBACKT", acquired);
        var recovered = f.history().recoverOnlineCandidate(f.initial.toOffset(), "fixture-journal", f.policy);
        byte[] history = java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"));
        var records = new ArrayList<SourceRecord>();
        try (var output = new As400OnlineCensusRecovery.Output(recovered.definitions(), false);
                var runtime = f.runtime(f.initial, recovered, output, records, false, 100)) {
            assertThatThrownBy(() -> runtime.poll(f.initial.toOffset())).isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(records).isEmpty();
        assertThat(java.nio.file.Files.readAllBytes(directory.resolve("history.jsonl"))).isEqualTo(history);
    }

    @ParameterizedTest
    @ValueSource(strings = { "unfinished-creation", "missing-member", "late-move" })
    void shouldRequireCompleteCreationAndMoveEvidenceBeforeCatalogUse(String scenario) throws Exception {
        var f = new Fixture();
        var created = f.binding("ROLLBACKT", f.entry(59).header().getRecordInfo().objectJournalId(), f.entry(61).header().getRecordInfo().objectJournalId());
        var journal = new ArrayList<RawJournalPage.Entry>();
        for (var entry : f.range(59, 63)) {
            journal.add(f.copy(entry, entry.position().offset().longValueExact(), scenario.equals("unfinished-creation") ? null : 0L, null, null, null));
        }
        var moved = fileMove(f, created, 70, "DCNC0005", "DCNC0006");
        journal.addAll(scenario.equals("missing-member") ? moved.subList(0, 1) : moved);
        assertThatThrownBy(() -> As400FileCreation.qualify(movedDefinition(created, "DCNC0006"), journal, f.order,
                f.position(scenario.equals("late-move") ? 68 : 100), (file, library) -> created.binding(65536)))
                .isInstanceOf(scenario.equals("late-move") ? As400SchemaChange.MetadataMovedException.class : As400SchemaTimeline.PendingChangeException.class);
    }

}
