/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.TableId;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Clock;

/** Actual prefix handler/dispatcher/delivery; native qualification is replaced by retained fixture history. */
class As400LegacyClearBufferTest {
    private static final As400Partition PARTITION = new As400Partition("test");
    private static final As400ContinuousRuntime.Limits LIMITS = new As400ContinuousRuntime.Limits(
            new As400TransactionBuffer.Limits(32, 1000, 128, 65536), new As400JournalLifecycle.Limits(100, 65536, 32, 128, 100, 64), 100);

    @TempDir
    Path directory;

    @ParameterizedTest
    @MethodSource("io.debezium.connector.db2as400.As400IncrementalLifecycleTest#deferredClears")
    void shouldDeliverOnlyTheResolvedTableOutcomeAndReleaseItsHeldOffset(String fixture, String scenario) throws Exception {
        try (var run = new Running(fixture, scenario)) {
            for (var entry : run.entries) {
                run.process(entry);
                if (run.buffer.pending()) {
                    assertThat(run.delivery.safeOffset(run.scan.getOffset())).isNotEqualTo(run.scan.getOffset());
                }
                run.buffer.resolve(entry.position(), "FIXTURE_ONLY");
            }
            assertThat(run.buffer.pending()).isFalse();
            run.delivery.flush(run.scan.getOffset());
            assertThat(run.acknowledged.get(run.acknowledged.size() - 1)).isEqualTo(run.scan.getOffset());
            var affected = run.entries.stream().filter(e -> e.header().getJournalCode() == 'D' && e.header().getEntryType().equals("CG"))
                    .map(e -> e.header().getFile()).collect(java.util.stream.Collectors.toSet());
            var actual = As400IncrementalLifecycleTest.data(run.records).stream().filter(value -> affected.contains(value.split(":")[1])).toList();
            var expected = As400IncrementalLifecycleTest.expected(scenario).stream().filter(value -> affected.contains(value.split(":")[1])).toList();
            assertThat(actual).containsExactlyElementsOf(expected);
            var cycle = run.entries.stream().filter(e -> e.header().getJournalCode() == 'D' && e.header().getEntryType().equals("CG"))
                    .findFirst().orElseThrow().header().getCommitCycle().toString();
            var markers = run.records.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("status") != null
                    && cycle.equals(s.getString("id"))).map(r -> (Struct) r.value()).toList();
            assertThat(markers).extracting(s -> s.getString("status")).containsExactly("BEGIN", "END");
            long count = run.records.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("op") != null
                    && s.getStruct("transaction") != null && cycle.equals(s.getStruct("transaction").getString("id"))).count();
            assertThat(markers.get(1).getInt64("event_count")).isEqualTo(count);
            assertThat(run.heldRecords).allSatisfy(record -> assertThat(record.sourceOffset()).isEqualTo(run.pin));
            assertThat(run.records).noneMatch(r -> r.sourceOffset().containsKey(As400Checkpoint.VERSION));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "T_rollback" })
    void shouldInvalidateAnIncrementalSnapshotWindowOnlyForTheCommittedClear(String scenario) throws Exception {
        try (var run = new Running("thread-enabled", scenario)) {
            var snapshot = mock(As400TruncateAwareIncrementalSnapshot.class);
            run.dispatcher.setIncrementalSnapshotChangeEventSource(Optional.of(snapshot));
            for (var entry : run.entries) {
                run.process(entry);
                run.buffer.resolve(entry.position(), "FIXTURE_ONLY");
            }
            if (scenario.equals("T_commit")) {
                org.mockito.Mockito.verify(snapshot).truncate(run.bindings.get("TC2").identity().tableId());
            }
            else {
                org.mockito.Mockito.verify(snapshot, org.mockito.Mockito.never()).truncate(any());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "open-clear", "partial-dispatch" })
    void shouldReconstructTheHeldClearFromTheAcknowledgedOffsetInAFreshInstance(String interruption) throws Exception {
        Map<String, ?> saved;
        try (var first = new Running("thread-enabled", "T_commit")) {
            if (interruption.equals("partial-dispatch")) {
                first.fault = interruption;
                assertThatThrownBy(() -> {
                    for (var entry : first.entries) {
                        first.process(entry);
                        first.buffer.resolve(entry.position(), "FIXTURE_ONLY");
                    }
                }).isInstanceOf(InterruptedException.class);
                assertThat(first.heldRecords).hasSize(1);
            }
            else {
                for (var entry : first.entries) {
                    first.process(entry);
                    if (first.buffer.pending()) {
                        break;
                    }
                }
            }
            saved = Map.copyOf(first.acknowledged.get(first.acknowledged.size() - 1));
            assertThat(saved).isEqualTo(first.pin);
        }
        try (var replacement = new Running("thread-enabled", "T_commit", saved)) {
            var from = new BigInteger(saved.get(As400OffsetContext.EVENT_SEQUENCE).toString());
            for (var entry : replacement.entries) {
                if (entry.position().offset().compareTo(from) > 0
                        || entry.position().offset().equals(from) && saved.get(As400OffsetContext.PROCESSED).equals("false")) {
                    replacement.process(entry);
                    replacement.buffer.resolve(entry.position(), "FIXTURE_ONLY");
                }
            }
            assertThat(replacement.buffer.pending()).isFalse();
            assertThat(As400IncrementalLifecycleTest.data(replacement.records)).containsExactly("t:TC2", "c:TC2:101");
            replacement.delivery.flush(replacement.scan.getOffset());
            assertThat(new BigInteger(replacement.acknowledged.get(replacement.acknowledged.size() - 1)
                    .get(As400OffsetContext.EVENT_SEQUENCE).toString())).isGreaterThan(from);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-start", "wrong-binding", "dispatch" })
    void shouldKeepTheOffsetPinnedWhenHistoryQualificationOrOutcomeDispatchFails(String fault) throws Exception {
        try (var run = new Running("thread-enabled", "T_commit")) {
            run.fault = fault;
            assertThatThrownBy(() -> {
                for (var entry : run.entries) {
                    run.process(entry);
                    run.buffer.resolve(entry.position(), "FIXTURE_ONLY");
                }
            }).isInstanceOf(Exception.class);
            assertThat(run.buffer.pending()).isTrue();
            assertThat(run.pin).isNotNull();
            assertThat(run.acknowledged.get(run.acknowledged.size() - 1)).isEqualTo(run.pin);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDeliverFollowingMemberClearInOrderIncludingRestartFromThePinnedOffset(boolean restart) throws Exception {
        Map<String, ?> saved = null;
        if (restart) {
            try (var first = new Running("thread-enabled", "T_commit")) {
                for (var entry : first.entries) {
                    first.process(entry);
                    if (first.buffer.pending()) {
                        break;
                    }
                }
                saved = first.acknowledged.get(first.acknowledged.size() - 1);
            }
        }
        try (var run = new Running("thread-enabled", "T_commit", saved)) {
            run.appendMemberClearAndReload();
            var snapshot = mock(As400TruncateAwareIncrementalSnapshot.class);
            run.dispatcher.setIncrementalSnapshotChangeEventSource(Optional.of(snapshot));
            var from = new JournalProcessedPosition(run.scan.getPosition());
            for (var entry : run.entries) {
                int relative = entry.position().offset().compareTo(from.getOffset());
                if (relative > 0 || relative == 0 && !from.processed()) {
                    run.process(entry);
                }
            }
            // The complete commit, F/CR and reload share one observed prefix.
            assertThat(run.buffer.pending()).isTrue();
            run.buffer.resolve(run.entries.get(run.entries.size() - 1).position(), "FIXTURE_ONLY");
            assertThat(run.buffer.pending()).isFalse();
            assertThat(As400IncrementalLifecycleTest.data(run.records)).containsExactlyElementsOf(restart
                    ? List.of("t:TC2", "c:TC2:101", "t:TC2", "c:TC2:101")
                    : List.of("c:MARK:1000", "t:TC2", "c:TC2:101", "t:TC2", "c:TC2:101"));
            org.mockito.Mockito.verify(snapshot, org.mockito.Mockito.times(2)).truncate(run.bindings.get("TC2").identity().tableId());
            assertThat(run.heldRecords).allSatisfy(record -> assertThat(record.sourceOffset()).isEqualTo(run.pin));
            run.delivery.flush(run.scan.getOffset());
            assertThat(run.acknowledged.get(run.acknowledged.size() - 1)).isEqualTo(run.scan.getOffset());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "member-jid", "file-jid", "multiple-members", "transactional", "incomplete" })
    void shouldRejectFollowingMemberClearWithoutItsExactCompleteSingleMemberBinding(String fault) throws Exception {
        try (var run = new Running("thread-enabled", "T_commit")) {
            run.appendMemberClearAndReload();
            var entry = run.entries.get(run.entries.size() - 2);
            var binding = run.bindings.get("TC2");
            byte[] raw = entry.data().clone();
            switch (fault) {
                case "member-jid" -> raw[197] ^= 1;
                case "file-jid" -> System.arraycopy(java.util.HexFormat.of().parseHex(binding.identity().fileJournalId()), 0, raw, 197, 10);
                case "transactional" -> java.nio.ByteBuffer.wrap(raw).putLong(64, 1);
                case "incomplete" -> raw[218] |= 0x20;
                case "multiple-members" -> binding = new As400SourceNormalizer.TableBinding(binding.identity(), binding.decoder(), false, false,
                        Map.of(binding.identity().member(), binding.identity().memberJournalId(), "OTHER", "ff".repeat(10)));
                default -> throw new AssertionError(fault);
            }
            var selected = binding;
            var changed = new RawJournalPage.Entry(entry.position(), new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder(37).decode(raw, 0), raw);
            assertThatThrownBy(() -> As400NativeLegacyClear.preservesDefinition(changed, (file, library) -> selected))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private final class Running extends As400ConnectorTask implements AutoCloseable {
        final List<RawJournalPage.Entry> entries;
        final Map<String, As400SourceNormalizer.TableBinding> bindings;
        final As400OffsetContext scan;
        final List<SourceRecord> records = new ArrayList<>();
        final List<SourceRecord> heldRecords = new ArrayList<>();
        final List<Map<String, ?>> acknowledged = new ArrayList<>();
        final List<AutoCloseable> replays = new ArrayList<>();
        final As400LegacySnapshotBootstrap.Delivery delivery;
        final As400EventDispatcher dispatcher;
        final As400LegacyClearBuffer buffer;
        final As400SpillStore store;
        Map<String, ?> pin;
        String fault = "";

        Running(String fixture, String scenario) throws Exception {
            this(fixture, scenario, null);
        }

        Running(String fixture, String scenario, Map<String, ?> saved) throws Exception {
            entries = new ArrayList<>(As400IncrementalLifecycleTest.entries(fixture, scenario).stream()
                    .map(e -> new RawJournalPage.Entry(e.position(), e.header(), e.bytes())).toList());
            bindings = As400IncrementalLifecycleTest.bindings(fixture);
            var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "TEST")
                    .with("database.schema", bindings.values().iterator().next().identity().tableId().schema())
                    .with("skipped.operations", "none").with("provide.transaction.metadata", true).build();
            var context = preStart(raw);
            var config = (As400ConnectorConfig) context.getConfig();
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, context);
            var start = entries.get(0);
            scan = saved == null ? new As400OffsetContext(config, new JournalProcessedPosition(start.position(), start.header().getTime(), false))
                    : new As400OffsetContext.Loader(config).load(saved);
            delivery = new As400LegacySnapshotBootstrap.Delivery(new As400TaskHandoff.Output() {
                @Override
                public void emit(SourceRecord record) throws Exception {
                    if (pin != null) {
                        if (fault.equals("dispatch") || fault.equals("partial-dispatch") && heldRecords.size() == 1) {
                            throw new InterruptedException("Controlled outcome interruption");
                        }
                        heldRecords.add(record);
                    }
                    records.add(record);
                }

                @Override
                public void barrier(SourceRecord record) {
                }

                @Override
                public String acknowledge(SourceRecord record) {
                    acknowledged.add(record.sourceOffset());
                    return "acknowledged";
                }
            }, offset -> new SourceRecord(PARTITION.getSourcePartition(), offset, "control", null, "boundary"));
            var queue = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(256).maxQueueSize(1024).pollInterval(Duration.ZERO)
                    .loggingContextSupplier(() -> context.configureLoggingContext("deferred-clear-test")).queueProvider(delivery).build();
            var topics = new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
                @Override
                public String dataChangeTopic(TableId id) {
                    return "test." + id.table();
                }
            };
            dispatcher = new As400EventDispatcher(config, topics, new As400TaskStopProbeConnector.FixtureSchema(scan,
                    bindings.values().stream().map(b -> b.identity().tableId()).toArray(TableId[]::new)), queue,
                    config.getTableFilters().dataCollectionFilter(), DataChangeEvent::new, new As400EventMetadataProvider(),
                    config.schemaNameAdjuster(), null, new DebeziumHeaderProducer(context));
            var jdbc = mock(As400JdbcConnection.class);
            when(jdbc.getRealDatabaseName()).thenReturn("TEST");
            when(jdbc.getLongName(anyString(), anyString())).thenAnswer(i -> i.getArgument(1));
            when(jdbc.isOnlyMember(anyString(), anyString(), anyString())).thenReturn(true);
            var schema = mock(As400DatabaseSchema.class);
            when(schema.getRecordFormat(anyString(), anyString())).thenReturn(Optional.of(mock(io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo.class)));
            var decoder = mock(JdbcFileDecoder.class);
            when(schema.getFileDecoder()).thenReturn(decoder);
            when(decoder.decode(any(), any(), anyInt())).thenAnswer(i -> {
                io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader h = i.getArgument(0);
                // The unchanged legacy decoder also handles compensating DR
                // images on unrelated tables; the native buffer never decodes these.
                if (h.getEntryType().equals("DR")) {
                    byte[] bytes = i.getArgument(1);
                    return new Object[]{ new com.ibm.as400.access.AS400Bin4().toObject(bytes, h.getEntrySpecificDataOffset() + 16) };
                }
                return bindings.get(h.getFile()).values(h, i.getArgument(1), i.getArgument(2));
            });
            var source = new As400StreamingChangeEventSource(config, null, jdbc, dispatcher, mock(ErrorHandler.class), Clock.system(), schema);
            store = new As400SpillStore(As400HandoffWork.settings(directory, 16 * 1024 * 1024));
            buffer = new As400LegacyClearBuffer(PARTITION, scan, source, delivery, this::prepare, 32, store);
        }

        void process(RawJournalPage.Entry entry) throws Exception {
            buffer.process(entry);
            scan.setPosition(new JournalProcessedPosition(entry.position(), entry.header().getTime(), true));
            if (buffer.pending() && pin == null) {
                pin = Map.copyOf(buffer.safeOffset(scan.getOffset()));
            }
        }

        void appendMemberClearAndReload() {
            var row = entries.stream().filter(e -> e.header().getJournalCode() == 'R' && e.header().getEntryType().equals("PT")
                    && e.header().getFile().equals("TC2")).findFirst().orElseThrow();
            long sequence = entries.get(entries.size() - 1).position().offset().longValueExact() + 1;
            // Synthetic F/CR ordering, retaining the captured member identity.
            byte[] clear = java.util.Arrays.copyOf(row.data(), 236);
            java.nio.ByteBuffer.wrap(clear).putInt(0, 0).putInt(4, 0).putInt(8, 0).putInt(20, 0)
                    .putLong(24, sequence).putLong(56, 0).putLong(64, 0);
            new com.ibm.as400.access.AS400Text(3, 37).toBytes("FCR", clear, 98);
            var decoder = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder(37);
            entries.add(new RawJournalPage.Entry(new JournalPosition(BigInteger.valueOf(sequence), row.position().receiver()), decoder.decode(clear, 0), clear));
            byte[] reload = row.data().clone();
            java.nio.ByteBuffer.wrap(reload).putLong(24, sequence + 1).putLong(64, 0);
            entries.add(new RawJournalPage.Entry(new JournalPosition(BigInteger.valueOf(sequence + 1), row.position().receiver()), decoder.decode(reload, 0), reload));
        }

        Optional<As400NativeLegacyClear.Prepared> prepare(BigInteger cycle, List<As400NativeLegacyClear.Target> targets,
                                                          JournalPosition through, String journal)
                throws Exception {
            var selected = new LinkedHashMap<String, As400SourceNormalizer.TableBinding>();
            var first = new LinkedHashMap<String, JournalPosition>();
            targets.forEach(t -> {
                first.put(t.key(), t.firstClear());
                selected.put(t.key(), bindings.get(t.file()));
            });
            if (fault.equals("wrong-binding")) {
                selected.replaceAll((key, value) -> bindings.values().stream().filter(b -> !b.equals(value)).findFirst().orElseThrow());
            }
            var start = entries.stream().filter(e -> e.position().offset().equals(cycle)).findFirst().orElseThrow();
            var receiver = start.position().receiver();
            var order = new As400JournalOrder(List.of(receiver), Map.of(receiver, "FIXTURE_ONLY"),
                    Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(100000))));
            var replayStore = new As400SpillStore(As400HandoffWork.settings(directory, 16 * 1024 * 1024));
            var replay = new As400LegacyClearReplay(journal, cycle, start.position(), first, selected, order, LIMITS, replayStore);
            replays.add(replay);
            replays.add(replayStore);
            var retained = entries.stream().filter(e -> e.position().offset().compareTo(cycle) >= 0
                    && e.position().offset().compareTo(through.offset()) <= 0).toList();
            for (var entry : retained) {
                if (!fault.equals("missing-start") || !entry.position().equals(start.position())) {
                    replay.accept(entry);
                }
            }
            if (replay.resolution().isEmpty()) {
                return Optional.empty();
            }
            for (var entry : retained) {
                if (entry.position().offset().compareTo(replay.resolution().orElseThrow().consumedThrough().offset()) > 0
                        && (entry.header().getJournalCode() == 'D' || entry.header().getJournalCode() == 'F')
                        && !As400NativeLegacyClear.preservesDefinition(entry, (file, library) -> bindings.get(file))) {
                    throw new IllegalArgumentException("Unqualified schema transition after deferred clear");
                }
            }
            var work = new As400HandoffWork(As400HandoffWork.settings(directory, 16 * 1024 * 1024), LIMITS,
                    () -> {
                        throw new AssertionError("No synthetic native capture");
                    }, () -> {
                    });
            var result = new As400NativeLegacyClear.Prepared(work);
            result.entries = retained;
            result.definitions = List.of(); // The dispatcher above already owns the fixture's exact schemas.
            result.resolution = replay.resolution().orElseThrow();
            return Optional.of(result);
        }

        @Override
        public void close() throws Exception {
            dispatcher.close();
            for (var replay : replays) {
                replay.close();
            }
            store.close();
        }
    }
}
