/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;

/** Actual native mixed history; artificial initial checkpoint is not a source baseline protocol. */
class As400OrdinaryCdcTest {
    private static final String JOURNAL = "fixture-journal";
    private static final String FINGERPRINT = "v1:" + "f".repeat(64);
    private static final JournalReceiver RECEIVER = new JournalReceiver("QSQJRN0002", "DCOA1107");
    private static final As400Partition PARTITION = new As400Partition("test");
    private static final As400Checkpoint INITIAL = As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT, position(24), "fixture-receiver");
    private static final List<String> EXPECTED = List.of("d:A:1", "c:A:4", "d:A:2", "c:A:5", "t:A", "c:A:10", "t:A", "c:A:11", "t:B", "c:B:100");

    @ParameterizedTest
    @ValueSource(strings = { "commit", "rollback" })
    void shouldRejectNativeNoncommitInsertAfterAnOpenClearWithoutOutputOrCheckpointAdvancement(String terminal) throws Exception {
        var json = new ObjectMapper().readTree(getClass().getResourceAsStream("/native-transactions/noncommit-after-clear.json"));
        var bindings = capturedBindings(json);
        var scenario = java.util.stream.StreamSupport.stream(json.get("cases").spliterator(), false)
                .filter(c -> c.path("terminal").asText().equals(terminal)).findFirst().orElseThrow();
        var receiver = new JournalReceiver(json.path("receiver").asText(), json.path("library").asText());
        long baseline = scenario.path("baseline").asLong();
        long through = scenario.path("through").asLong();
        var initial = As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT, new JournalPosition(BigInteger.valueOf(baseline), receiver), "fixture-receiver");
        var order = new As400JournalOrder(List.of(receiver), Map.of(receiver, "fixture-receiver"),
                Map.of(receiver, new As400JournalOrder.Range(BigInteger.valueOf(baseline), BigInteger.valueOf(through))));
        List<SourceRecord> records = new ArrayList<>();
        for (int restart = 0; restart < 2; restart++) {
            try (var replay = new As400ReplayCoordinator(roundTrip(initial.toOffset()), JOURNAL, FINGERPRINT, order,
                    new As400TransactionBuffer.Limits(16, 1000, 128, 65536), 1024, 100, (file, library) -> bindings.get(file),
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), PARTITION.getSourcePartition(), records::add,
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), true);
                    var output = new As400IncrementalLifecycleTest.Output(bindings, true).dispatcher) {
                for (var raw : json.get("entries")) {
                    long sequence = raw.path("sequence").asLong();
                    if (sequence <= baseline || sequence > through) {
                        continue;
                    }
                    byte[] bytes = HexFormat.of().parseHex(raw.path("hex").asText());
                    assertThat(HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)))
                            .isEqualTo(raw.path("sha256").asText());
                    var header = new EntryHeaderDecoder().decode(bytes, 0);
                    var position = new JournalPosition(BigInteger.valueOf(sequence), receiver);
                    if (sequence == scenario.path("rejected_sequence").asLong()) {
                        assertThat(header.getCommitCycle()).isEqualTo(BigInteger.ZERO);
                        var failure = assertThrows(As400ReplayCoordinator.RecoveryException.class,
                                () -> replay.accept(header, bytes, 0, position));
                        assertThat(failure).hasRootCauseMessage("Change from another commit cycle overlaps an uncommitted clear of "
                                + receiver.library() + "." + scenario.path("table").asText());
                        break;
                    }
                    if (replay.accept(header, bytes, 0, position).isPresent()) {
                        replay.deliver(output);
                    }
                }
                assertThat(records).isEmpty();
                assertThat(replay.checkpoint()).isEqualTo(initial);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "F/OP", "F/CL", "F/MS", "D/DH", "D/GT", "R/IL", "F/SS", "D/DW", "F/RG", "F/RM" })
    void shouldTreatCapturedAdministrativeEntriesAsNoOutputWithoutRelaxingIdentityOrPayloadChecks(String type) throws Exception {
        var json = new ObjectMapper().readTree(getClass().getResourceAsStream("/native-transactions/"
                + (type.equals("R/IL") ? "record-limit.json"
                        : List.of("F/SS", "D/DW").contains(type) ? "save-while-active.json"
                                : List.of("F/RG", "F/RM").contains(type) ? "reorganization.json" : "administrative-entries.json")));
        var bindings = capturedBindings(json);
        var raw = java.util.stream.StreamSupport.stream(json.get("entries").spliterator(), false)
                .filter(e -> e.path("type").asText().equals(type)).findFirst().orElseThrow();
        byte[] captured = HexFormat.of().parseHex(raw.path("hex").asText());
        assertThat(HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(captured)))
                .isEqualTo(raw.path("sha256").asText());
        var receiver = new JournalReceiver(json.path("receiver").asText(), json.path("library").asText());
        var position = new JournalPosition(new BigInteger(raw.path("sequence").asText()), receiver);
        var initial = As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT,
                new JournalPosition(position.offset().subtract(BigInteger.ONE), receiver), "fixture-receiver");
        var order = new As400JournalOrder(List.of(receiver), Map.of(receiver, "fixture-receiver"),
                Map.of(receiver, new As400JournalOrder.Range(initial.replayFrom().offset(), position.offset())));
        List<SourceRecord> records = new ArrayList<>();
        var faults = new ArrayList<>(List.of("none", "none-restart", "identity", "payload", "transaction", "record-number", "strict-profile"));
        if (type.equals("F/RG") || type.equals("F/RM")) {
            faults.add("rrn-key");
        }
        if (type.equals("F/RM")) {
            faults.add("concurrent-reorganization");
        }
        for (String fault : faults) {
            byte[] bytes = captured.clone();
            if (fault.equals("identity")) {
                bytes[206] ^= 1;
            }
            if (fault.equals("payload")) {
                // Preserve a structurally readable entry but shorten its metadata payload.
                int offset = java.nio.ByteBuffer.wrap(bytes).getInt(8);
                if (offset == 0) {
                    offset = 236;
                    bytes = java.util.Arrays.copyOf(bytes, Math.max(bytes.length, 253));
                    java.nio.ByteBuffer.wrap(bytes).putInt(0, 0).putInt(8, offset);
                }
                new com.ibm.as400.access.AS400Text(5, 37).toBytes("00001", bytes, offset);
            }
            if (fault.equals("transaction")) {
                java.nio.ByteBuffer.wrap(bytes).putLong(64, 1);
            }
            if (fault.equals("record-number")) {
                java.nio.ByteBuffer.wrap(bytes).putLong(56, 1);
            }
            if (fault.equals("concurrent-reorganization")) {
                new com.ibm.as400.access.AS400Text(1, 37).toBytes("F", bytes, java.nio.ByteBuffer.wrap(bytes).getInt(8) + 16 + 108);
            }
            final byte[] input = bytes;
            var header = new EntryHeaderDecoder().decode(input, 0);
            try (var replay = new As400ReplayCoordinator(roundTrip(initial.toOffset()), JOURNAL, FINGERPRINT, order,
                    new As400TransactionBuffer.Limits(16, 1000, 128, 65536), 1024, 100, (file, library) -> {
                        var binding = bindings.get(file);
                        return fault.equals("rrn-key") ? new TableBinding(binding.identity(), binding.decoder(), true) : binding;
                    },
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), PARTITION.getSourcePartition(), records::add,
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), !fault.equals("strict-profile"))) {
                if (fault.startsWith("none")) {
                    assertThat(replay.accept(header, input, 0, position)).isEmpty();
                    replay.verifyEndOfAvailableJournal();
                    assertThat(replay.scanPosition()).contains(position);
                }
                else {
                    assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> replay.accept(header, input, 0, position));
                }
                assertThat(replay.checkpoint()).isEqualTo(initial);
                assertThat(records).isEmpty();
            }
        }
    }

    private static Map<String, TableBinding> capturedBindings(com.fasterxml.jackson.databind.JsonNode json) throws Exception {
        var bindings = new HashMap<String, TableBinding>();
        var array = io.debezium.document.DocumentReader.defaultReader().read("{\"tables\":" + json.get("bindings") + "}").getArray("tables");
        for (var change : new io.debezium.relational.history.JsonTableChangeSerializer().deserialize(array, false)) {
            var binding = As400BindingDefinition.restore(change.getTable());
            bindings.put(binding.identity().physicalFile(), binding.binding(1024));
        }
        return bindings;
    }

    @Test
    void mixesAutocommitRowsNativeClearsCommittedSqlClearAndRollbackThroughActualDispatcher() throws Exception {
        Fixture fixture = new Fixture();
        List<SourceRecord> records = new ArrayList<>();
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, fixture.entries);
            replay.verifyEndOfAvailableJournal();
            assertThat(data(records)).containsExactlyElementsOf(EXPECTED);
            assertThat(records.stream().filter(r -> operation(r).equals("BEGIN")).count()).isEqualTo(1);
            assertThat(records.stream().filter(r -> operation(r).equals("END")).count()).isEqualTo(1);
            for (SourceRecord record : records) {
                assertThat(record.sourceOffset()).doesNotContainKeys("scan", "R", "position");
                if (isData(record)) {
                    Struct value = (Struct) record.value();
                    boolean transactional = record.topic().equals("test.B");
                    assertThat(value.getStruct("transaction") != null).isEqualTo(transactional);
                    if (value.getString("op").equals("t")) {
                        assertThat(record.key()).isNull();
                    }
                }
            }
            assertProjection(records);
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(61));
            assertThat(replay.scanPosition().orElseThrow()).isEqualTo(position(66));
        }
    }

    static IntStream entryCuts() {
        return IntStream.rangeClosed(0, 39);
    }

    @ParameterizedTest
    @MethodSource("entryCuts")
    void freshInstanceRestartsAfterEveryNativeEntryWithOnlySerializedCheckpoint(int cut) throws Exception {
        Fixture fixture = new Fixture();
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, fixture.entries.subList(0, cut));
            saved = roundTrip(replay.publishCheckpoint(heartbeat()).sourceOffset());
            if (cut == 1) {
                assertThat(saved.get("replay_from.sequence")).isEqualTo("24");
                assertThat(saved.get("replay_from.inclusive")).isEqualTo("false");
                assertThat(saved.get("completed_through.sequence")).isEqualTo("24");
            }
        }
        try (var replay = fixture.replay(saved, records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, fixture.resume(saved));
            replay.verifyEndOfAvailableJournal();
        }
        assertThat(data(records)).containsExactlyElementsOf(EXPECTED);
        assertProjection(records);
    }

    static IntStream outputCuts() {
        // Key-changing UP emits delete + tombstone + create; metadata surrounds only C/CM output.
        return IntStream.range(0, 14);
    }

    @ParameterizedTest
    @MethodSource("outputCuts")
    void interruptedActualOutputReplaysFromDurablePreviousWIncludingKeyChangeAndTruncate(int cut) throws Exception {
        Fixture fixture = new Fixture();
        List<SourceRecord> records = new ArrayList<>();
        try (var replay = fixture.replay(INITIAL.toOffset(), record -> records.size() < cut && records.add(record), true); var output = fixture.output()) {
            assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> fixture.run(replay, output, fixture.entries));
        }
        Map<String, ?> saved = roundTrip(records.isEmpty() ? INITIAL.toOffset() : records.get(records.size() - 1).sourceOffset());
        try (var replay = fixture.replay(saved, records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, fixture.resume(saved));
            replay.verifyEndOfAvailableJournal();
        }
        assertProjection(records);
    }

    @Test
    void statisticsRequestPreservesPendingBeforeImageAndDurableFwAcrossFreshRestart() throws Exception {
        Fixture f = new Fixture();
        Entry before = f.entries.get(0);
        Entry advisory = statistics(before, 26);
        Entry after = resequence(f.entries.get(1), 27);
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        try (var replay = f.replay(INITIAL.toOffset(), records::add, true); var output = f.output()) {
            f.run(replay, output, List.of(before));
            var boundary = replay.checkpoint();
            f.run(replay, output, List.of(advisory));
            assertThat(replay.checkpoint()).isEqualTo(boundary);
            assertThat(replay.scanPosition()).contains(position(26));
            assertThat(records).isEmpty();
            saved = roundTrip(replay.checkpoint().toOffset());
        }
        try (var replay = f.replay(saved, records::add, true); var output = f.output()) {
            f.run(replay, output, List.of(before, advisory, after));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(27));
        }
        assertThat(data(records)).containsExactly("d:A:1", "c:A:4");
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void capturedNativeStatisticsIsAdvisoryOnlyInOrdinaryProfileAcrossRestart(boolean ordinary) throws Exception {
        byte[] bytes;
        try (var input = getClass().getResourceAsStream("/native-transactions/statistics-request.json")) {
            bytes = HexFormat.of().parseHex(new ObjectMapper().readTree(input).get("entries").get(0).get("hex").asText());
        }
        var header = new EntryHeaderDecoder().decode(bytes, 0);
        var receiver = new JournalReceiver("QSQJRN0003", "DCNC7707");
        var position = new JournalPosition(BigInteger.valueOf(183), receiver);
        var initial = As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT,
                new JournalPosition(BigInteger.valueOf(182), receiver), "fixture-receiver");
        var order = new As400JournalOrder(List.of(receiver), Map.of(receiver, "fixture-receiver"),
                Map.of(receiver, new As400JournalOrder.Range(BigInteger.valueOf(182), BigInteger.valueOf(183))));
        // Fixed decoder fixture, not proof of a native file/member association.
        // Only the observed member JID participates in statistics validation.
        var identity = new TableIdentity(new TableId(null, "DCNC7707", "M"), "M", "M", "01010101010101010101",
                "e9400030ed650e42000c", "unused-statistics-format");
        var binding = new TableBinding(identity, new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 1024));
        assertThat(header.getSequenceNumber()).isEqualTo(position.offset());
        assertThat(header.getRecordInfo().threadId()).isPositive();
        assertThat(header.getRecordInfo().objectJournalId()).isEqualTo(identity.memberJournalId());
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved = roundTrip(initial.toOffset());
        for (int attempt = 0; attempt < 2; attempt++) {
            try (var replay = new As400ReplayCoordinator(saved, JOURNAL, FINGERPRINT, order,
                    new As400TransactionBuffer.Limits(16, 1000, 128, 65536), 1024, 100,
                    (file, library) -> file.equals("M") && library.equals("DCNC7707") ? binding : null,
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), PARTITION.getSourcePartition(), records::add,
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), ordinary)) {
                if (ordinary) {
                    assertThat(replay.accept(header, bytes, 0, position)).isEmpty();
                    replay.verifyEndOfAvailableJournal();
                    assertThat(replay.scanPosition()).contains(position);
                }
                else {
                    assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> replay.accept(header, bytes, 0, position));
                }
                assertThat(replay.checkpoint()).isEqualTo(initial);
                assertThat(records).isEmpty();
                saved = roundTrip(replay.checkpoint().toOffset());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "A", "B", "D", "unknown", "short", "file", "member", "rename", "jid", "type", "cycle", "flags", "count", "strict" })
    void statisticsCannotHideStructuralChangesOrAmbiguousIdentity(String fault) throws Exception {
        Fixture f = new Fixture();
        byte[] bytes = statistics(f.entries.get(0), 25).bytes().clone();
        var text = new com.ibm.as400.access.AS400Text(1, 37);
        switch (fault) {
            case "A", "B", "D" -> text.toBytes(fault, bytes, 252 + 108);
            case "unknown" -> text.toBytes("Z", bytes, 252 + 108);
            case "short" -> new com.ibm.as400.access.AS400Text(5, 37).toBytes("00108", bytes, 236);
            case "file" -> text.toBytes("Z", bytes, 252);
            case "member" -> text.toBytes("Z", bytes, 272);
            case "rename" -> text.toBytes("Z", bytes, 282);
            case "jid" -> bytes[197] ^= 1;
            case "type" -> new com.ibm.as400.access.AS400Text(10, 37).toBytes("*FILE", bytes, 219);
            case "cycle" -> java.nio.ByteBuffer.wrap(bytes).putLong(64, 10);
            case "flags" -> bytes[218] = 0x20;
            case "count" -> java.nio.ByteBuffer.wrap(bytes).putLong(56, 1);
        }
        List<SourceRecord> records = new ArrayList<>();
        try (var replay = f.replay(INITIAL.toOffset(), records::add, !fault.equals("strict")); var output = f.output()) {
            assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> f.run(replay, output, List.of(entry(bytes))));
            assertThat(replay.checkpoint()).isEqualTo(INITIAL);
            assertThat(records).isEmpty();
        }
    }

    private static Entry statistics(Entry row, long sequence) {
        // Synthetic RJNE0200 header and documented object-level layout. Native
        // source qualification separately checks the actual statistics records.
        byte[] bytes = java.util.Arrays.copyOf(row.bytes(), 252 + 109);
        java.util.Arrays.fill(bytes, 0, 24, (byte) 0);
        java.nio.ByteBuffer.wrap(bytes).putInt(8, 236).putLong(24, sequence).putLong(56, 0).putLong(64, 0).putInt(72, 0).putInt(232, 0);
        new com.ibm.as400.access.AS400Text(1, 37).toBytes("F", bytes, 98);
        new com.ibm.as400.access.AS400Text(2, 37).toBytes("CB", bytes, 99);
        new com.ibm.as400.access.AS400Text(1, 37).toBytes("0", bytes, 216);
        bytes[218] = 0;
        java.util.Arrays.fill(bytes, 236, bytes.length, (byte) 0);
        new com.ibm.as400.access.AS400Text(5, 37).toBytes("00109", bytes, 236);
        new com.ibm.as400.access.AS400Text(10, 37).toBytes(row.header().getFile(), bytes, 252);
        new com.ibm.as400.access.AS400Text(10, 37).toBytes(row.header().getLibrary(), bytes, 262);
        new com.ibm.as400.access.AS400Text(10, 37).toBytes(row.header().getMember(), bytes, 272);
        new com.ibm.as400.access.AS400Text(30, 37).toBytes("", bytes, 282);
        new com.ibm.as400.access.AS400Text(1, 37).toBytes("C", bytes, 360);
        return entry(bytes);
    }

    @Test
    void physicalReorganizationCannotAdvanceOutputOrBeSkippedAfterRestart() throws Exception {
        Fixture fixture = new Fixture();
        // Native row identity cannot be trusted across RGZPFM. Synthetic F/RG
        // header exercises the ordinary reader's rejection, not live RGZPFM support.
        byte[] bytes = fixture.entries.get(0).bytes().clone();
        new com.ibm.as400.access.AS400Text(1, 37).toBytes("F", bytes, 98);
        new com.ibm.as400.access.AS400Text(2, 37).toBytes("RG", bytes, 99);
        Entry reorganization = entry(bytes);
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> durable = roundTrip(INITIAL.toOffset());
        for (int attempt = 0; attempt < 2; attempt++) {
            try (var replay = fixture.replay(durable, records::add, true); var output = fixture.output()) {
                assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> fixture.run(replay, output, List.of(reorganization)));
                assertThat(records).isEmpty();
                assertThat(replay.checkpoint()).isEqualTo(INITIAL);
                durable = roundTrip(replay.checkpoint().toOffset());
            }
        }
    }

    @Test
    void missingOffsetAndDefaultStrictProfileStillRejectBeforeAnyOutput() throws Exception {
        Fixture fixture = new Fixture();
        List<SourceRecord> records = new ArrayList<>();
        assertThrows(IllegalArgumentException.class, () -> fixture.replay(null, records::add, true));
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add, false); var output = fixture.output()) {
            assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> fixture.run(replay, output, fixture.entries));
            assertThat(replay.checkpoint()).isEqualTo(INITIAL);
        }
        assertThat(records).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-before", "wrong-thread", "wrong-jid", "clear-between-images" })
    void incompleteAmbiguousOrChangedAutocommitUpdatesCannotAdvanceW(String fault) throws Exception {
        Fixture fixture = new Fixture();
        List<SourceRecord> records = new ArrayList<>();
        List<Entry> input = new ArrayList<>(fixture.entries.subList(0, 2));
        if (fault.equals("missing-before")) {
            input.remove(0);
        }
        else if (fault.equals("clear-between-images")) {
            input.set(1, fixture.entries.stream().filter(e -> e.header().getEntryType().equals("CR")).findFirst().orElseThrow());
        }
        else {
            byte[] bytes = input.get(1).bytes().clone();
            bytes[fault.equals("wrong-thread") ? 47 : 206] ^= 1;
            input.set(1, entry(bytes));
        }
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add, true); var output = fixture.output()) {
            assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> fixture.run(replay, output, input));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(INITIAL.completedThrough());
        }
        assertThat(records).isEmpty();
    }

    @Test
    void pendingAutocommitBeforeImagePinsFloorAcrossAnInterleavedTransactionCompletion() throws Exception {
        Fixture fixture = new Fixture();
        Entry before = fixture.entries.get(0);
        Entry after = resequence(fixture.entries.get(1), 50);
        Entry start = fixture.entries.stream().filter(e -> e.position().offset().intValueExact() == 36).findFirst().orElseThrow();
        Entry commit = fixture.entries.stream().filter(e -> e.position().offset().intValueExact() == 46).findFirst().orElseThrow();
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, List.of(before, start, commit));
            assertThat(replay.checkpoint().replayFrom()).isEqualTo(position(25));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(46));
            saved = roundTrip(replay.publishCheckpoint(heartbeat()).sourceOffset());
        }
        try (var replay = fixture.replay(saved, records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, List.of(before, start, commit, after));
            replay.verifyEndOfAvailableJournal();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(50));
        }
        assertThat(data(records)).containsExactly("d:A:1", "c:A:4");
    }

    @Test
    void completedAutocommitUpdateCanHaveItsBeforeImageBeforeTheRemainingTransactionFloor() throws Exception {
        Fixture fixture = new Fixture();
        Entry before = fixture.entries.get(0);
        Entry after = resequence(fixture.entries.get(1), 50);
        Entry start = fixture.entries.stream().filter(e -> e.position().offset().intValueExact() == 36).findFirst().orElseThrow();
        Entry commit = resequence(fixture.entries.stream().filter(e -> e.position().offset().intValueExact() == 46).findFirst().orElseThrow(), 60);
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        try (var replay = fixture.replay(INITIAL.toOffset(), records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, List.of(before, start, after));
            assertThat(replay.checkpoint().replayFrom()).isEqualTo(position(36));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(50));
            saved = roundTrip(replay.publishCheckpoint(heartbeat()).sourceOffset());
        }
        try (var replay = fixture.replay(saved, records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, List.of(start, after, commit));
            replay.verifyEndOfAvailableJournal();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(position(60));
        }
        assertThat(data(records)).containsExactly("d:A:1", "c:A:4");
    }

    private static Entry resequence(Entry original, long sequence) {
        // Synthetic interleaving of native payloads; this is not a claimed live ordering.
        byte[] bytes = original.bytes().clone();
        java.nio.ByteBuffer.wrap(bytes).putLong(24, sequence);
        return entry(bytes);
    }

    static IntStream rolloverCuts() {
        return IntStream.rangeClosed(0, 12);
    }

    @ParameterizedTest
    @MethodSource("rolloverCuts")
    void actualTransactionAcrossReceiverChangePreservesReplayFloorAndCommitReceiver(int cut) throws Exception {
        Fixture fixture = new Fixture(true);
        var initial = As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT, position(66), "fixture-receiver");
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        try (var replay = fixture.replay(initial.toOffset(), records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, fixture.entries.subList(0, cut));
            replay.verifyOrdinaryCut();
            saved = roundTrip(replay.publishCheckpoint(heartbeat()).sourceOffset());
        }
        try (var replay = fixture.replay(saved, records::add, true); var output = fixture.output()) {
            fixture.run(replay, output, fixture.resume(saved));
            replay.verifyEndOfAvailableJournal();
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(new JournalPosition(BigInteger.valueOf(80), new JournalReceiver("QSQJRN0003", "DCOA1107")));
        }
        assertThat(data(records)).containsExactly("t:B", "c:B:200");
    }

    @Test
    void legacyNativeLifecycleProfileStillRejectsReceiverChangeBeforePublishing() throws Exception {
        Fixture fixture = new Fixture(true);
        var initial = As400Checkpoint.afterVerifiedBoundary(JOURNAL, FINGERPRINT, position(66), "fixture-receiver");
        List<SourceRecord> records = new ArrayList<>();
        try (var replay = fixture.replay(initial.toOffset(), records::add, false); var output = fixture.output()) {
            assertThrows(As400ReplayCoordinator.RecoveryException.class, () -> fixture.run(replay, output, fixture.entries));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(initial.completedThrough());
        }
        assertThat(records).isEmpty();
    }

    private record Entry(EntryHeader header, byte[] bytes, JournalReceiver receiver) {
        JournalPosition position() {
            return new JournalPosition(header.getSequenceNumber(), receiver);
        }
    }

    private static Entry entry(byte[] bytes) {
        return new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes, RECEIVER);
    }

    private static class Fixture {
        final Map<String, TableBinding> bindings = new HashMap<>();
        final List<Entry> entries = new ArrayList<>();

        final boolean rollover;

        Fixture() throws Exception {
            this(false);
        }

        Fixture(boolean rollover) throws Exception {
            this.rollover = rollover;
            try (var input = getClass().getResourceAsStream("/native-transactions/ordinary-cdc.json")) {
                var json = new ObjectMapper().readTree(input);
                for (var table : json.get("tables")) {
                    String file = table.get("file").asText();
                    var id = new TableIdentity(new TableId(null, table.get("library").asText(), file), file, table.get("member").asText(),
                            table.get("file_jid").asText(), table.get("member_jid").asText(), table.get("format").asText());
                    bindings.put(file, new TableBinding(id, new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 1024)));
                }
                for (var raw : json.get("entries")) {
                    byte[] bytes = HexFormat.of().parseHex(raw.get("hex").asText());
                    var entry = new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes, new JournalReceiver(raw.get("receiver").asText(), RECEIVER.library()));
                    long sequence = entry.position().offset().longValueExact();
                    if (rollover ? sequence > 66 : sequence > 24 && sequence <= 66) {
                        entries.add(entry);
                    }
                }
            }
            assertThat(entries).hasSize(rollover ? 12 : 39);
        }

        As400ReplayCoordinator replay(Map<String, ?> saved, As400ReplayCoordinator.RecordSink sink, boolean ordinary) {
            var next = new JournalReceiver("QSQJRN0003", RECEIVER.library());
            var order = rollover
                    ? new As400JournalOrder(List.of(RECEIVER, next), Map.of(RECEIVER, "fixture-receiver", next, "next-receiver"),
                            Map.of(RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(24), BigInteger.valueOf(77)),
                                    next, new As400JournalOrder.Range(BigInteger.valueOf(78), BigInteger.valueOf(81))))
                    : new As400JournalOrder(List.of(RECEIVER), Map.of(RECEIVER, "fixture-receiver"),
                            Map.of(RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(24), BigInteger.valueOf(66))));
            return new As400ReplayCoordinator(saved, JOURNAL, FINGERPRINT, order, new As400TransactionBuffer.Limits(16, 1000, 128, 65536),
                    1024, 100, (file, library) -> bindings.get(file), As400SourceNormalizer.SavepointPolicy.consumingTargets(),
                    PARTITION.getSourcePartition(), sink, new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), ordinary);
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

        List<Entry> resume(Map<String, ?> saved) {
            var checkpoint = As400Checkpoint.fromOffset(saved, JOURNAL, FINGERPRINT);
            return entries.stream().filter(e -> {
                int c = e.position().offset().compareTo(checkpoint.replayFrom().offset());
                return c > 0 || c == 0 && checkpoint.replayInclusive();
            }).toList();
        }
    }

    private static SourceRecord heartbeat() {
        return new SourceRecord(PARTITION.getSourcePartition(), Map.of(), "heartbeat", Schema.STRING_SCHEMA, "heartbeat");
    }

    private static Map<String, ?> roundTrip(Map<String, ?> offset) throws Exception {
        var mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsBytes(offset), new TypeReference<Map<String, String>>() {
        });
    }

    private static JournalPosition position(long value) {
        return new JournalPosition(BigInteger.valueOf(value), RECEIVER);
    }

    private static boolean isData(SourceRecord record) {
        return record.value() instanceof Struct s && s.schema().field("op") != null;
    }

    private static String operation(SourceRecord record) {
        return record.value() instanceof Struct s && s.schema().field("status") != null ? s.getString("status") : "other";
    }

    private static List<String> data(List<SourceRecord> records) {
        return records.stream().filter(As400OrdinaryCdcTest::isData).map(r -> {
            var s = (Struct) r.value();
            String op = s.getString("op");
            return op + ":" + r.topic().substring(5) + (op.equals("t") ? "" : ":" + s.getStruct(op.equals("d") ? "before" : "after").getInt32("ID"));
        }).toList();
    }

    private static void assertProjection(List<SourceRecord> records) {
        Map<String, TreeSet<Integer>> tables = new HashMap<>();
        tables.put("test.A", new TreeSet<>(List.of(1, 2, 3)));
        tables.put("test.B", new TreeSet<>(List.of(1, 2, 3)));
        for (SourceRecord r : records) {
            if (!isData(r)) {
                continue;
            }
            var value = (Struct) r.value();
            var table = tables.get(r.topic());
            switch (value.getString("op")) {
                case "t" -> table.clear();
                case "d" -> table.remove(value.getStruct("before").getInt32("ID"));
                case "c" -> table.add(value.getStruct("after").getInt32("ID"));
                default -> throw new AssertionError("Unexpected fixture operation");
            }
        }
        assertThat(tables.get("test.A")).containsExactly(11);
        assertThat(tables.get("test.B")).containsExactly(100);
    }
}
