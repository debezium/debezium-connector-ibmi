/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.As400JournalLifecycle.Entry;
import io.debezium.connector.db2as400.As400ReplayCoordinator.RecoveryException;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.data.Envelope;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.FirstHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.OffsetStatus;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.txmetadata.TransactionMonitor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.schema.DatabaseSchema;

class As400IncrementalLifecycleTest {
    private static final String IDENTITY = "FIXTURE_ONLY";
    private static final String FINGERPRINT = "v1:" + "a".repeat(64);
    private static final As400Partition PARTITION = new As400Partition("test");
    private static final As400JournalLifecycle.Limits LIMITS = new As400JournalLifecycle.Limits(100, 65536, 32, 128, 100, 64);

    static Stream<Arguments> captures() {
        return Stream.of(Stream.of("T_commit", "D_commit", "T_rollback", "D_rollback", "T_savepoint", "D_savepoint", "interleaved", "update_commit")
                .map(name -> Arguments.of("thread-enabled", name)),
                Stream.of("truncate_commit", "delete_commit", "savepoint_reuse", "statement_failure", "atomic_statement_failure")
                        .map(name -> Arguments.of("lifecycle-discrimination", name)),
                Stream.of("multiple_clear_commit", "multiple_clear_rollback", "multiple_clear_savepoint", "nested_clear_savepoint",
                        "non_top_clear_savepoint", "two_table_clear_rollback", "two_table_clear_savepoint")
                        .map(name -> Arguments.of("multiple-clears", name)))
                .flatMap(stream -> stream);
    }

    static Stream<Arguments> interruptions() {
        return captures().flatMap(args -> {
            String fixture = (String) args.get()[0];
            String name = (String) args.get()[1];
            try {
                return IntStream.rangeClosed(0, entries(fixture, name).size()).mapToObj(cut -> Arguments.of(fixture, name, cut));
            }
            catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @ParameterizedTest
    @MethodSource("captures")
    void completeNativeStreamsReachRealDispatcherWithoutTwoPassProjection(String fixture, String name) throws Exception {
        var entries = entries(fixture, name);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings(fixture)).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), fixture, entries, records::add, LIMITS)) {
            for (Entry entry : entries) {
                feed(coordinator, entry, output);
                assertThat(coordinator.scanPosition()).contains(entry.position());
            }
            coordinator.verifyEndOfAvailableJournal();
            assertThat(data(records)).containsExactlyElementsOf(expected(name));
            assertThat(records.stream().filter(r -> operation(r).equals("t"))).allSatisfy(r -> {
                assertThat(r.key()).isNull();
                assertThat(r.keySchema()).isNull();
            });
            List<String> sourceSequences = records.stream().filter(As400IncrementalLifecycleTest::isData)
                    .map(r -> ((Struct) r.value()).getStruct("source").getString("sequence")).toList();
            assertThat(sourceSequences).allMatch(s -> entries.stream().anyMatch(e -> e.position().offset().toString().equals(s)));
            if (fixture.equals("multiple-clears")) {
                assertNativeFinalRows(name, records);
            }
        }
    }

    /** Local projection against independently read native outcomes, not a live sink claim. */
    private static void assertNativeFinalRows(String name, List<SourceRecord> records) throws IOException {
        JsonNode scenario = null;
        for (JsonNode candidate : fixture("multiple-clears").get("cases")) {
            if (candidate.get("name").asText().equals(name)) {
                scenario = candidate;
            }
        }
        assertThat(scenario).isNotNull();
        Map<String, Set<Integer>> rows = new HashMap<>();
        scenario.get("tables").forEach(table -> rows.put(table.asText(), new TreeSet<>(List.of(1, 2, 3))));
        for (SourceRecord record : records) {
            if (!isData(record)) {
                continue;
            }
            var state = rows.get(record.topic().substring("test.".length()));
            var value = (Struct) record.value();
            switch (value.getString("op")) {
                case "t" -> state.clear();
                case "c" -> state.add(value.getStruct("after").getInt32("ID"));
                case "d" -> state.remove(value.getStruct("before").getInt32("ID"));
                default -> throw new AssertionError("Unexpected captured operation");
            }
        }
        var expected = scenario.get("source_rows");
        rows.forEach((table, values) -> {
            List<Integer> nativeRows = new ArrayList<>();
            expected.get(table).forEach(value -> nativeRows.add(value.asInt()));
            assertThat(values).as(table).containsExactlyElementsOf(nativeRows);
        });
    }

    @ParameterizedTest
    @MethodSource("interruptions")
    void heartbeatAndFreshInstanceAtEveryNativeEntryPreserveExactlyCompletedOutput(String fixture, String name, int cut) throws Exception {
        var entries = entries(fixture, name);
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> durable;
        try (var output = new Output(bindings(fixture)).dispatcher;
                var first = coordinator(initial(entries).toOffset(), fixture, entries, records::add, LIMITS)) {
            for (Entry entry : entries.subList(0, cut)) {
                feed(first, entry, output);
            }
            SourceRecord heartbeat = first.publishCheckpoint(heartbeat());
            durable = roundTrip(heartbeat.sourceOffset());
            assertThat(durable.keySet()).noneMatch(k -> k.contains("scan"));
        }
        try (var output = new Output(bindings(fixture)).dispatcher;
                var resumed = coordinator(durable, fixture, entries, records::add, LIMITS)) {
            for (Entry entry : resumedEntries(entries, durable)) {
                feed(resumed, entry, output);
            }
            resumed.verifyEndOfAvailableJournal();
        }
        assertThat(data(records)).containsExactlyElementsOf(expected(name));
    }

    static Stream<Arguments> deliveryInterruptions() {
        return Stream.concat(IntStream.range(0, 4).mapToObj(cut -> Arguments.of("lifecycle-discrimination", "truncate_commit", cut)),
                IntStream.range(0, 7).mapToObj(cut -> Arguments.of("multiple-clears", "multiple_clear_commit", cut)));
    }

    @ParameterizedTest
    @MethodSource("deliveryInterruptions")
    void interruptedRealDispatcherDeliveryReplaysTheWholeNativeClearBatch(String fixture, String name, int cut) throws Exception {
        var entries = entries(fixture, name);
        List<SourceRecord> escaped = new ArrayList<>();
        Map<String, ?> durable = initial(entries).toOffset();
        try (var output = new Output(bindings(fixture)).dispatcher;
                var first = coordinator(durable, fixture, entries, record -> escaped.size() < cut && escaped.add(record), LIMITS)) {
            assertThrows(RecoveryException.class, () -> {
                for (Entry entry : entries) {
                    feed(first, entry, output);
                }
            });
            assertThat(first.checkpoint().completedThrough()).isEqualTo(initial(entries).completedThrough());
            if (!escaped.isEmpty()) {
                durable = roundTrip(escaped.get(escaped.size() - 1).sourceOffset());
            }
        }
        List<SourceRecord> repeated = new ArrayList<>();
        try (var output = new Output(bindings(fixture)).dispatcher;
                var resumed = coordinator(durable, fixture, entries, repeated::add, LIMITS)) {
            for (Entry entry : resumedEntries(entries, durable)) {
                feed(resumed, entry, output);
            }
            resumed.verifyEndOfAvailableJournal();
        }
        if (name.equals("truncate_commit")) {
            assertThat(repeated).extracting(As400IncrementalLifecycleTest::operation).containsExactly("BEGIN", "t", "c", "END");
        }
        else {
            assertThat(repeated).hasSize(7);
            assertThat(repeated.get(4).value()).isNull(); // Required delete tombstone also repeats.
        }
        assertThat(data(repeated)).containsExactlyElementsOf(expected(name));
        for (int i = 0; i < escaped.size(); i++) {
            assertThat(repeated.get(i).value()).isEqualTo(escaped.get(i).value());
            assertThat(repeated.get(i).sourceOffset()).isEqualTo(escaped.get(i).sourceOffset());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_rollback", "D_rollback" })
    void rollbackCheckpointPinsItsStartAfterTheLastRawCleanupEntry(String name) throws Exception {
        var entries = entries("thread-enabled", name);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), "thread-enabled", entries, records::add, LIMITS)) {
            for (Entry entry : entries) {
                feed(coordinator, entry, output);
                if (entry.type().equals("C/RB")) {
                    assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(initial(entries).completedThrough());
                }
            }
            var checkpoint = coordinator.checkpoint();
            assertThat(checkpoint.replayFrom()).isEqualTo(entries.get(index(entries, "C/SC")).position());
            assertThat(checkpoint.replayInclusive()).isTrue();
            assertThat(checkpoint.completedThrough()).isEqualTo(entries.get(index(entries, "C/RB")).position());
            assertThat(coordinator.scanPosition()).contains(entries.get(entries.size() - 1).position());
            assertThat(records).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "F/CH", "F/EJ", "D/CG", "C/RB", "F/MD" })
    void verifiedTailInsideAnUnfinishedWindowFailsWithoutChangingTheSafeCheckpoint(String type) throws Exception {
        String name = type.equals("F/MD") ? "T_savepoint" : type.equals("C/RB") ? "T_rollback" : "T_commit";
        var entries = entries("thread-enabled", name);
        int end = index(entries, type) + 1;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), "thread-enabled", entries, records::add, LIMITS)) {
            for (Entry entry : entries.subList(0, end)) {
                feed(coordinator, entry, output);
            }
            var safe = coordinator.checkpoint();
            assertThrows(RecoveryException.class, coordinator::verifyEndOfAvailableJournal);
            assertThat(coordinator.checkpoint()).isEqualTo(safe);
            assertThrows(RecoveryException.class, () -> coordinator.publishCheckpoint(heartbeat()));
            assertThat(records).isEmpty();
        }
    }

    @Test
    void actualMissingWriteGapStillStopsBeforeAnyPublication() throws Exception {
        var entries = entries("lifecycle-discrimination", "manual_journal_gap");
        List<SourceRecord> records = new ArrayList<>();
        try (var coordinator = coordinator(initial(entries).toOffset(), "lifecycle-discrimination", entries, records::add, LIMITS)) {
            Entry first = entries.get(0);
            assertThrows(RecoveryException.class, () -> coordinator.accept(first.header(), first.bytes(), 0, first.position()));
            assertThat(coordinator.checkpoint()).isEqualTo(initial(entries));
            assertThat(records).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "flags", "jid", "thread", "cycle", "pointer", "profile", "missing-ch", "missing-ej", "receiver" })
    void mismatchedOrMissingCompanionsNeverReachOutput(String fault) throws Exception {
        var entries = entries("thread-enabled", "T_commit");
        int target = index(entries, "F/EJ");
        if (fault.equals("missing-ch") || fault.equals("missing-ej")) {
            entries.remove(index(entries, fault.equals("missing-ch") ? "F/CH" : "F/EJ"));
        }
        else {
            if (fault.equals("profile")) {
                target = index(entries, "D/JF");
            }
            Entry old = entries.get(target);
            byte[] bytes = old.bytes().clone();
            switch (fault) {
                case "flags" -> bytes[218] = 0;
                case "jid" -> bytes[197] ^= 1;
                case "thread" -> ByteBuffer.wrap(bytes).putLong(40, 999);
                case "cycle" -> ByteBuffer.wrap(bytes).putLong(64, 999);
                case "pointer" -> ByteBuffer.wrap(bytes).putInt(72, 1);
                case "profile" -> bytes[216] = new AS400Text(1).toBytes("0")[0];
                case "receiver" -> {
                }
                default -> throw new AssertionError(fault);
            }
            JournalPosition position = fault.equals("receiver") ? new JournalPosition(old.position().offset(), new JournalReceiver("OTHER", "LIB")) : old.position();
            entries.set(target, new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes, position));
        }
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), "thread-enabled", entries, records::add, LIMITS)) {
            assertThrows(RecoveryException.class, () -> {
                for (Entry entry : entries) {
                    feed(coordinator, entry, output);
                }
            });
            assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(initial(entries).completedThrough());
            assertThat(records).isEmpty();
        }
    }

    private static void feed(As400ReplayCoordinator coordinator, Entry entry, As400BatchDispatcher output) {
        if (coordinator.accept(entry.header(), entry.bytes(), 0, entry.position()).isPresent()) {
            coordinator.deliver(output);
        }
    }

    static Stream<Arguments> rpcPages() {
        return captures().flatMap(a -> Stream.of(1, 3, 100).map(size -> Arguments.of(a.get()[0], a.get()[1], size)));
    }

    @ParameterizedTest
    @MethodSource("rpcPages")
    void realRpcLoopAndEntryDecoderKeepRawScanSeparateFromDurableRecovery(String fixture, String name, int size) throws Exception {
        var entries = entries(fixture, name);
        var reader = new RpcPages(initial(entries));
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings(fixture)).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), fixture, entries, records::add, LIMITS)) {
            for (int i = 0; i < entries.size(); i += size) {
                var page = entries.subList(i, Math.min(i + size, entries.size()));
                reader.read(page, coordinator, output);
                var last = page.get(page.size() - 1).position();
                assertThat(reader.scan.getPosition().asJournalPosition()).isEqualTo(last);
                assertThat(coordinator.scanPosition()).contains(last);
                assertThat(coordinator.publishCheckpoint(heartbeat()).sourceOffset()).isEqualTo(coordinator.checkpoint().toOffset());
            }
            coordinator.verifyEndOfAvailableJournal();
            assertThat(data(records)).containsExactlyElementsOf(expected(name));
            if (name.endsWith("_rollback")) {
                assertThat(coordinator.checkpoint().completedThrough().offset()).isLessThan(reader.scan.getPosition().getOffset());
                assertThat(coordinator.checkpoint().replayFrom()).isEqualTo(entries.get(index(entries, "C/SC")).position());
            }
        }
    }

    @Test
    void invalidLifecycleStopsTheRealRpcLoopBeforeItsPositionAdvancesPastThatEntry() throws Exception {
        var entries = entries("thread-enabled", "T_commit");
        int badIndex = index(entries, "F/EJ");
        Entry old = entries.get(badIndex);
        byte[] bytes = old.bytes().clone();
        bytes[218] = 0;
        entries.set(badIndex, new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes, old.position()));
        var reader = new RpcPages(initial(entries));
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), "thread-enabled", entries, records::add, LIMITS)) {
            assertThrows(RecoveryException.class, () -> reader.read(entries, coordinator, output));
            assertThat(reader.scan.getPosition().asJournalPosition()).isEqualTo(entries.get(badIndex - 1).position());
            assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(initial(entries).completedThrough());
            assertThat(records).isEmpty();
        }
    }

    /** Native transport is replaced with owned pages; RPC iteration/decoding/position updates are real. */
    private static final class RpcPages {
        final As400RpcConnection rpc = mock(As400RpcConnection.class, CALLS_REAL_METHODS);
        final RetrieveJournal journal;
        final As400OffsetContext scan;

        RpcPages(As400Checkpoint checkpoint) throws Exception {
            var lifecycle = new JournalReadLifecycle();
            journal = spy(new RetrieveJournal(new RetrieveConfigBuilder().withAs400(rpc).withJournalInfo(new JournalInfo("JRN", "LIB", false))
                    .withJournalBufferSize(65536).withMaxServerSideEntries(100).withTransactionControls(true).build(), mock(JournalInfoRetrieval.class), lifecycle));
            set(rpc, "lifecycle", lifecycle);
            set(rpc, "retrieveJournal", journal);
            Field periodic = As400RpcConnection.class.getDeclaredField("periodic");
            periodic.setAccessible(true);
            periodic.set(rpc, mock(periodic.getType()));
            var config = As400ReaderFixture.config(false);
            scan = new As400OffsetContext(config, new JournalProcessedPosition(checkpoint.replayFrom(), Instant.EPOCH, !checkpoint.replayInclusive()));
        }

        void read(List<Entry> entries, As400ReplayCoordinator coordinator, As400BatchDispatcher output) throws Exception {
            byte[] page = new byte[64 + entries.stream().mapToInt(e -> e.bytes().length).sum()];
            int offset = 64;
            for (int i = 0; i < entries.size(); i++) {
                byte[] bytes = entries.get(i).bytes();
                System.arraycopy(bytes, 0, page, offset, bytes.length);
                ByteBuffer.wrap(page).putInt(offset, i == entries.size() - 1 ? 0 : bytes.length);
                offset += bytes.length;
            }
            Entry last = entries.get(entries.size() - 1);
            doAnswer(invocation -> {
                JournalProcessedPosition previous = invocation.getArgument(0);
                var next = new JournalProcessedPosition(last.position(), last.header().getTimestamp(), true);
                journal.setOutputData(page, new FirstHeader(page.length, 64, entries.size(), OffsetStatus.DATA, next),
                        new JournalProcessedPosition(previous));
                set(journal, "offset", -1);
                return RetrievalState.Success;
            }).when(journal).retrieveJournal(any(JournalProcessedPosition.class));
            ChangeEventSourceContext context = mock(ChangeEventSourceContext.class);
            when(context.isRunning()).thenReturn(true);
            rpc.getJournalEntries(context, scan, (sequence, current, header) -> {
                try {
                    current.decode((h, data, entryOffset) -> {
                        if (coordinator.accept(h, data, entryOffset, current.getPosition().asJournalPosition()).isPresent()) {
                            coordinator.deliver(output);
                        }
                        return null;
                    });
                }
                catch (RuntimeException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw new IOException(e);
                }
            }, mock(WatchDog.class));
            Arrays.fill(page, (byte) 0);
        }

        private static void set(Object object, String field, Object value) throws Exception {
            Field member = object.getClass().getDeclaredField(field);
            member.setAccessible(true);
            member.set(object, value);
        }
    }

    @Test
    void completedCycleThatStartedBeforeAnOpenCycleStillKeepsItsLifecycleStartForReplay() throws Exception {
        var nativeEntries = entries("thread-enabled", "interleaved");
        Entry bStart = nativeEntries.stream().filter(e -> e.type().equals("C/SC")).skip(1).findFirst().orElseThrow();
        nativeEntries.remove(bStart);
        nativeEntries.add(index(nativeEntries, "C/SC"), bStart);
        // This is a synthetic interleaving of captured records, not a new native trace.
        var entries = resequence(nativeEntries);
        int bCommit = index(entries, "C/CM");
        List<SourceRecord> records = new ArrayList<>();
        Map<String, ?> saved;
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var first = coordinator(initial(entries).toOffset(), "thread-enabled", entries, records::add, LIMITS)) {
            for (Entry entry : entries.subList(0, bCommit + 1)) {
                feed(first, entry, output);
            }
            assertThat(data(records)).containsExactly("t:IB2", "c:IB2:501");
            var checkpoint = first.checkpoint();
            assertThat(checkpoint.completedThrough()).isEqualTo(entries.get(bCommit).position());
            assertThat(checkpoint.replayFrom()).isEqualTo(entries.get(index(entries, "C/SC")).position());
            assertThat(checkpoint.replayInclusive()).isTrue();
            saved = roundTrip(first.publishCheckpoint(heartbeat()).sourceOffset());
        }
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var resumed = coordinator(saved, "thread-enabled", entries, records::add, LIMITS)) {
            for (Entry entry : resumedEntries(entries, saved)) {
                feed(resumed, entry, output);
            }
            resumed.verifyEndOfAvailableJournal();
            assertThat(resumed.checkpoint().replayInclusive()).isFalse();
            assertThat(resumed.checkpoint().replayFrom()).isEqualTo(entries.get(entries.size() - 1).position());
        }
        assertThat(data(records)).containsExactlyElementsOf(expected("interleaved"));
    }

    @Test
    void anotherProducersCommitWaitsForAnOverlappingWindowAndOwnedRowsSurviveRpcReuse() throws Exception {
        var entries = overlapping();
        int earlyCommit = index(entries, "C/CM");
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), "thread-enabled", entries, records::add, LIMITS)) {
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                // Nonzero buffer offset plus immediate reuse after callback return.
                byte[] page = new byte[37 + entry.bytes().length + 19];
                System.arraycopy(entry.bytes(), 0, page, 37, entry.bytes().length);
                boolean ready = coordinator.accept(entry.header(), page, 37, entry.position()).isPresent();
                Arrays.fill(page, (byte) 0);
                if (ready) {
                    coordinator.deliver(output);
                }
                if (i <= earlyCommit) {
                    assertThat(records).isEmpty();
                    assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(initial(entries).completedThrough());
                }
            }
            coordinator.verifyEndOfAvailableJournal();
        }
        assertThat(data(records)).containsExactlyElementsOf(expected("interleaved"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "entries", "bytes", "cycles", "markers", "clears", "dependencies" })
    void capacityExhaustionStopsWithoutDroppingPendingHistory(String limit) throws Exception {
        List<Entry> entries = limit.equals("markers") ? entries("thread-enabled", "T_savepoint") : overlapping();
        As400JournalLifecycle.Limits bounds = switch (limit) {
            case "entries" -> new As400JournalLifecycle.Limits(1, 65536, 32, 128, 100, 64);
            case "bytes" -> new As400JournalLifecycle.Limits(100, 236, 32, 128, 100, 64);
            case "cycles" -> new As400JournalLifecycle.Limits(100, 65536, 1, 128, 100, 64);
            case "markers" -> new As400JournalLifecycle.Limits(100, 65536, 32, 1, 100, 64);
            case "clears" -> new As400JournalLifecycle.Limits(100, 65536, 32, 128, 1, 64);
            case "dependencies" -> new As400JournalLifecycle.Limits(100, 65536, 32, 128, 100, 1);
            default -> throw new AssertionError(limit);
        };
        if (limit.equals("clears")) {
            // Complete A's clear while A remains open, then begin B's clear.
            entries = entries("thread-enabled", "interleaved");
        }
        var boundedEntries = entries;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), "thread-enabled", entries, records::add, bounds)) {
            assertThrows(RecoveryException.class, () -> {
                for (Entry entry : boundedEntries) {
                    feed(coordinator, entry, output);
                }
            });
            assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(initial(entries).completedThrough());
            assertThat(records).isEmpty();
        }
    }

    @Test
    void fullRollbackSuffixCannotResumeWithoutTheRetainedStart() throws Exception {
        var entries = entries("thread-enabled", "T_rollback");
        var rb = entries.get(index(entries, "C/RB")).position();
        var bad = As400Checkpoint.afterVerifiedBoundary(IDENTITY, FINGERPRINT, rb, IDENTITY);
        Entry tail = entries.get(index(entries, "C/RB") + 1);
        List<SourceRecord> records = new ArrayList<>();
        try (var coordinator = coordinator(bad.toOffset(), "thread-enabled", entries, records::add, LIMITS)) {
            assertThrows(RecoveryException.class, () -> coordinator.accept(tail.header(), tail.bytes(), 0, tail.position()));
            assertThat(coordinator.checkpoint()).isEqualTo(bad);
            assertThat(records).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { -1, 5, 6 })
    void multipleHeldCommitsDrainInOrderAndSecondBatchFailureKeepsItsRecoveryBoundary(int failAt) throws Exception {
        var input = overlapping();
        int bStart = IntStream.range(0, input.size()).filter(i -> input.get(i).type().equals("C/SC")).skip(1).findFirst().orElseThrow();
        int bCommit = index(input, "C/CM");
        Entry start = renumber(input.get(bStart), 2000, 2000);
        Entry row = renumber(input.get(index(input, "R/PT")), 2001, 2000);
        ByteBuffer.wrap(row.bytes()).putInt(row.header().getEntrySpecificDataOffset() + 16, 502);
        ByteBuffer.wrap(row.bytes()).putLong(56, 2);
        row = new Entry(new EntryHeaderDecoder().decode(row.bytes(), 0), row.bytes(), row.position());
        Entry commit = renumber(input.get(bCommit), 2002, 2000);
        input.addAll(bCommit + 1, List.of(start, row, commit));
        var entries = resequence(input);
        List<SourceRecord> escaped = new ArrayList<>();
        Map<String, ?> saved = initial(entries).toOffset();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var first = coordinator(saved, "thread-enabled", entries, record -> (failAt < 0 || escaped.size() < failAt) && escaped.add(record), LIMITS)) {
            if (failAt < 0) {
                for (Entry entry : entries) {
                    feed(first, entry, output);
                }
                first.verifyEndOfAvailableJournal();
                assertThat(data(escaped)).containsExactly("t:IB2", "c:IB2:501", "c:IB2:502", "t:IA2", "c:IA2:401", "c:IA2:402");
                return;
            }
            assertThrows(RecoveryException.class, () -> {
                for (Entry entry : entries) {
                    feed(first, entry, output);
                }
            });
            saved = roundTrip(escaped.get(escaped.size() - 1).sourceOffset());
            assertThat(saved.get("completed_through.sequence")).isEqualTo(entries.get(index(entries, "C/CM")).position().offset().toString());
        }
        List<SourceRecord> repeated = new ArrayList<>();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var resumed = coordinator(saved, "thread-enabled", entries, repeated::add, LIMITS)) {
            for (Entry entry : resumedEntries(entries, saved)) {
                feed(resumed, entry, output);
            }
            resumed.verifyEndOfAvailableJournal();
        }
        assertThat(data(repeated)).containsExactly("c:IB2:502", "t:IA2", "c:IA2:401", "c:IA2:402");
    }

    @ParameterizedTest
    @ValueSource(strings = { "cycle", "level" })
    void replayedCompletionMarkerMustStillMatchTheLifecycleCycleAndScope(String fault) throws Exception {
        var entries = entries("thread-enabled", "T_commit");
        int sq = index(entries, "C/SQ");
        Entry old = entries.get(sq);
        byte[] bytes = old.bytes().clone();
        if (fault.equals("cycle")) {
            ByteBuffer.wrap(bytes).putLong(64, 999);
        }
        else {
            ByteBuffer.wrap(bytes).putInt(232, 99);
        }
        entries.set(sq, new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes, old.position()));
        var loaded = new As400Checkpoint(IDENTITY, FINGERPRINT, entries.get(index(entries, "C/SC")).position(), true,
                entries.get(index(entries, "C/CM")).position(), IDENTITY, IDENTITY);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("thread-enabled")).dispatcher;
                var coordinator = coordinator(loaded.toOffset(), "thread-enabled", entries, records::add, LIMITS)) {
            assertThrows(RecoveryException.class, () -> {
                for (Entry entry : resumedEntries(entries, loaded.toOffset())) {
                    feed(coordinator, entry, output);
                }
            });
            assertThat(coordinator.checkpoint()).isEqualTo(loaded);
            assertThat(records).isEmpty();
        }
    }

    private static Entry renumber(Entry entry, long sequence, long cycle) {
        byte[] bytes = entry.bytes().clone();
        ByteBuffer.wrap(bytes).putLong(24, sequence).putLong(64, cycle);
        var header = new EntryHeaderDecoder().decode(bytes, 0);
        return new Entry(header, bytes, new JournalPosition(header.getSequenceNumber(), entry.position().receiver()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "cycle", "level", "marker", "producer", "after-end-journaling" })
    void abandonedOptimizationMustCancelItsExactUnstartedScope(String fault) throws Exception {
        var entries = entries("multiple-clears", "multiple_clear_commit");
        int su = index(entries, "C/SU");
        Entry old = entries.get(su);
        byte[] bytes = old.bytes().clone();
        switch (fault) {
            case "cycle" -> ByteBuffer.wrap(bytes).putLong(64, 999);
            case "level" -> ByteBuffer.wrap(bytes).putInt(232, 99);
            case "marker" -> System.arraycopy(new AS400Text(20).toBytes("999"), 0, bytes, old.header().getEntrySpecificDataOffset() + 16, 20);
            case "producer" -> ByteBuffer.wrap(bytes).putLong(40, old.header().getRecordInfo().threadId().longValue() + 1);
            case "after-end-journaling" -> entries.add(su++, entries.get(index(entries, "F/EJ")));
            default -> throw new AssertionError(fault);
        }
        entries.set(su, new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes, old.position()));
        if (fault.equals("after-end-journaling")) {
            // Use fresh synthetic coordinates so the test reaches the admission
            // rule rather than failing on a duplicated sequence number.
            var copy = new ArrayList<Entry>();
            for (int i = 0; i < entries.size(); i++) {
                Entry entry = entries.get(i);
                if (i == su - 1) {
                    entry = renumber(entry, 9000, 0);
                }
                copy.add(entry);
            }
            entries = resequence(copy);
        }
        var input = entries;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("multiple-clears")).dispatcher;
                var coordinator = coordinator(initial(input).toOffset(), "multiple-clears", input, records::add, LIMITS)) {
            assertThrows(RecoveryException.class, () -> {
                for (Entry entry : input) {
                    feed(coordinator, entry, output);
                }
            });
            assertThat(records).isEmpty();
            assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(initial(input).completedThrough());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "wrong-table", "wrong-flags", "early-marker" })
    void multiTableRollbackRequiresEveryExactReverseOrderedTransition(String fault) throws Exception {
        var entries = entries("multiple-clears", "two_table_clear_savepoint");
        int first = index(entries, "F/MD");
        int second = first + 4;
        Entry old = entries.get(second);
        byte[] bytes = old.bytes().clone();
        if (fault.equals("missing")) {
            entries.subList(second, second + 4).clear();
        }
        else if (fault.equals("early-marker")) {
            Entry replacement = entries.get(second + 4);
            bytes = replacement.bytes().clone();
            ByteBuffer.wrap(bytes).putLong(24, old.position().offset().longValue());
            entries.set(second, new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes, old.position()));
        }
        else {
            if (fault.equals("wrong-table")) {
                bytes = entries.get(first).bytes().clone();
                ByteBuffer.wrap(bytes).putLong(24, old.position().offset().longValue());
            }
            else {
                bytes[218] = 0x10;
            }
            entries.set(second, new Entry(new EntryHeaderDecoder().decode(bytes, 0), bytes, old.position()));
        }
        List<SourceRecord> records = new ArrayList<>();
        try (var output = new Output(bindings("multiple-clears")).dispatcher;
                var coordinator = coordinator(initial(entries).toOffset(), "multiple-clears", entries, records::add, LIMITS)) {
            assertThrows(RecoveryException.class, () -> {
                for (Entry entry : entries) {
                    feed(coordinator, entry, output);
                }
            });
            assertThat(records).isEmpty();
            assertThat(coordinator.checkpoint().completedThrough()).isEqualTo(initial(entries).completedThrough());
        }
    }

    private static List<Entry> overlapping() throws IOException {
        var entries = entries("thread-enabled", "interleaved");
        int begin = IntStream.range(0, entries.size()).filter(i -> entries.get(i).type().equals("C/SC")).skip(1).findFirst().orElseThrow();
        int end = index(entries, "C/CM") + 1;
        List<Entry> b = new ArrayList<>(entries.subList(begin, end));
        entries.subList(begin, end).clear();
        entries.addAll(index(entries, "F/CH") + 1, b);
        return resequence(entries);
    }

    /** Synthetic interleaving only: remap sequence references consistently, preserve values/producers. */
    private static List<Entry> resequence(List<Entry> entries) {
        Map<BigInteger, BigInteger> sequence = new HashMap<>();
        for (int i = 0; i < entries.size(); i++) {
            sequence.put(entries.get(i).position().offset(), BigInteger.valueOf(1000 + i));
        }
        List<Entry> result = new ArrayList<>();
        for (Entry entry : entries) {
            byte[] bytes = entry.bytes().clone();
            ByteBuffer.wrap(bytes).putLong(24, sequence.get(entry.position().offset()).longValue());
            if (entry.header().getCommitCycle().signum() > 0) {
                ByteBuffer.wrap(bytes).putLong(64, sequence.get(entry.header().getCommitCycle()).longValue());
            }
            if (entry.type().equals("C/SQ") || entry.type().equals("C/SU")) {
                int offset = entry.header().getEntrySpecificDataOffset() + 16;
                BigInteger target = new BigInteger(new AS400Text(20).toObject(bytes, offset).toString());
                new AS400Text(20).toBytes(String.format("%020d", sequence.get(target)), bytes, offset);
            }
            var header = new EntryHeaderDecoder().decode(bytes, 0);
            result.add(new Entry(header, bytes, new JournalPosition(header.getSequenceNumber(), entry.position().receiver())));
        }
        return result;
    }

    private static List<Entry> resumedEntries(List<Entry> entries, Map<String, ?> saved) {
        var checkpoint = As400Checkpoint.fromOffset(saved, IDENTITY, FINGERPRINT);
        return entries.stream().filter(e -> {
            int cmp = e.position().offset().compareTo(checkpoint.replayFrom().offset());
            return cmp > 0 || (cmp == 0 && checkpoint.replayInclusive());
        }).toList();
    }

    private static As400ReplayCoordinator coordinator(Map<String, ?> saved, String fixture, List<Entry> entries,
                                                      As400ReplayCoordinator.RecordSink sink, As400JournalLifecycle.Limits limits)
            throws IOException {
        var bindings = bindings(fixture);
        var receiver = entries.get(0).position().receiver();
        var order = new As400JournalOrder(List.of(receiver), Map.of(receiver, IDENTITY),
                Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(100000))));
        return new As400ReplayCoordinator(saved, IDENTITY, FINGERPRINT, order, new As400TransactionBuffer.Limits(32, 1000, 128, 65536),
                1024, 100, (file, library) -> bindings.get(file), As400SourceNormalizer.SavepointPolicy.consumingTargets(),
                PARTITION.getSourcePartition(), sink, limits);
    }

    private static As400Checkpoint initial(List<Entry> entries) {
        var first = entries.get(0).position();
        return As400Checkpoint.afterVerifiedBoundary(IDENTITY, FINGERPRINT,
                new JournalPosition(first.offset().subtract(BigInteger.ONE), first.receiver()), IDENTITY);
    }

    private static Map<String, ?> roundTrip(Map<String, ?> saved) throws IOException {
        var mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsBytes(saved), new TypeReference<Map<String, String>>() {
        });
    }

    private static SourceRecord heartbeat() {
        return new SourceRecord(PARTITION.getSourcePartition(), Map.of(), "heartbeat", Schema.STRING_SCHEMA, "heartbeat");
    }

    private static boolean isData(SourceRecord record) {
        return record.value() instanceof Struct value && value.schema().field("op") != null;
    }

    private static String operation(SourceRecord record) {
        if (isData(record)) {
            return ((Struct) record.value()).getString("op");
        }
        return record.value() instanceof Struct value && value.schema().field("status") != null ? value.getString("status") : "heartbeat";
    }

    private static List<String> data(List<SourceRecord> records) {
        return records.stream().filter(As400IncrementalLifecycleTest::isData).map(r -> {
            var value = (Struct) r.value();
            String op = value.getString("op");
            return op + ":" + r.topic().substring("test.".length())
                    + (op.equals("t") ? "" : ":" + value.getStruct(op.equals("d") ? "before" : "after").getInt32("ID"));
        }).toList();
    }

    private static List<String> expected(String name) {
        return switch (name) {
            case "T_commit" -> List.of("c:MARK:1000", "t:TC2", "c:TC2:101");
            case "D_commit" -> List.of("c:MARK:2000", "t:DC2", "c:DC2:101");
            case "T_rollback", "D_rollback" -> List.of();
            case "T_savepoint" -> List.of("c:MARK:1200", "c:TS2:303");
            case "D_savepoint" -> List.of("c:MARK:2200", "c:DS2:303");
            case "interleaved" -> List.of("t:IB2", "c:IB2:501", "t:IA2", "c:IA2:401", "c:IA2:402");
            case "update_commit" -> List.of("d:UPD:1", "c:UPD:11");
            case "truncate_commit" -> List.of("t:TC", "c:TC:101");
            case "delete_commit" -> List.of("t:DC", "c:DC:101");
            case "savepoint_reuse" -> List.of("c:SP:10", "c:SP:13");
            case "statement_failure" -> List.of("c:ERR:10", "c:ERR:11", "c:ERR:13");
            case "atomic_statement_failure" -> List.of("c:ERR:20", "c:ERR:24");
            case "multiple_clear_commit" -> List.of("t:MC", "c:MC:10", "d:MC:10", "c:MC:11");
            case "multiple_clear_rollback", "multiple_clear_savepoint", "two_table_clear_rollback" -> List.of();
            case "nested_clear_savepoint" -> List.of("c:NC:13");
            case "non_top_clear_savepoint" -> List.of("c:NR:12");
            case "two_table_clear_savepoint" -> List.of("c:A:22", "c:B:23");
            default -> throw new AssertionError(name);
        };
    }

    private static JsonNode fixture(String name) throws IOException {
        try (var input = As400IncrementalLifecycleTest.class.getResourceAsStream("/native-transactions/" + name + ".json")) {
            return new ObjectMapper().readTree(input);
        }
    }

    private static List<Entry> entries(String fixture, String name) throws IOException {
        for (JsonNode scenario : fixture(fixture).get("cases")) {
            if (!scenario.get("name").asText().equals(name)) {
                continue;
            }
            List<Entry> result = new ArrayList<>();
            for (JsonNode raw : scenario.get("entries")) {
                byte[] bytes = HexFormat.of().parseHex(raw.get("hex").asText());
                var header = new EntryHeaderDecoder().decode(bytes, 0);
                result.add(new Entry(header, bytes, new JournalPosition(header.getSequenceNumber(),
                        new JournalReceiver(raw.get("receiver").asText(), raw.get("receiver_library").asText()))));
            }
            return result;
        }
        throw new IllegalArgumentException("Missing scenario " + name);
    }

    private static Map<String, TableBinding> bindings(String fixture) throws IOException {
        Map<String, TableBinding> result = new HashMap<>();
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
        for (JsonNode table : fixture(fixture + "-bindings").get("tables")) {
            String file = table.get("file").asText();
            result.put(file, new TableBinding(new TableIdentity(new TableId("AEQA", table.get("library").asText(), file), file,
                    table.get("member").asText(), table.get("fileJournalId").asText(), table.get("memberJournalId").asText(), "FIXTURE_INT_NOT_NULL"), decoder));
        }
        return result;
    }

    private static int index(List<Entry> entries, String type) {
        return IntStream.range(0, entries.size()).filter(i -> entries.get(i).type().equals(type)).findFirst().orElseThrow();
    }

    /** Real task/service initialization and dispatcher; schemas are controlled fixtures. */
    static final class Output extends As400ConnectorTask {
        final As400BatchDispatcher dispatcher;

        Output(Map<String, TableBinding> bindings) {
            this(bindings, false);
        }

        Output(Map<String, TableBinding> bindings, boolean ordinary) {
            var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "AEQA")
                    .with("database.schema", "FIXTURE").with("skipped.operations", "none").with("heartbeat.interval.ms", "3600000")
                    .with("provide.transaction.metadata", "true").with("event.converting.failure.handling.mode", "fail").build();
            var context = preStart(raw);
            var config = (As400ConnectorConfig) context.getConfig();
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, context);
            Map<TableId, TableSchema> schemas = new HashMap<>();
            bindings.values().forEach(binding -> {
                var id = binding.identity().tableId();
                Schema row = SchemaBuilder.struct().name("test." + id.table() + ".Value").field("ID", Schema.INT32_SCHEMA).optional().build();
                Schema key = SchemaBuilder.struct().name("test." + id.table() + ".Key").field("ID", Schema.INT32_SCHEMA).build();
                Envelope envelope = Envelope.defineSchema().withName("test." + id.table() + ".Envelope").withRecord(row)
                        .withSource(new As400OffsetContext(config).getSourceInfoSchema()).withTransaction(TransactionMonitor.TRANSACTION_BLOCK_SCHEMA).build();
                schemas.put(id, new TableSchema(id, key, data -> new Struct(key).put("ID", data[0]), envelope, row,
                        data -> new Struct(row).put("ID", data[0])));
            });
            DatabaseSchema<TableId> schema = new DatabaseSchema<>() {
                @Override
                public TableSchema schemaFor(TableId id) {
                    return schemas.get(id);
                }

                @Override
                public boolean tableInformationComplete() {
                    return true;
                }

                @Override
                public boolean isHistorized() {
                    return false;
                }

                @Override
                public void close() {
                }
            };
            dispatcher = new As400BatchDispatcher(config, PARTITION, schema, new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
                @Override
                public String dataChangeTopic(TableId id) {
                    return "test." + id.table();
                }
            }, new DebeziumHeaderProducer(context), () -> 42_000L, 100, ordinary);
        }
    }
}
