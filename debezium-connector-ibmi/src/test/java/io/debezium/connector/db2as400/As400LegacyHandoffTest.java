/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalCriteria;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;

/**
 * Conditional migration proof, entirely outside production startup. Captured bytes
 * exercise the real decoder, lifecycle validator, buffer, checkpoint and dispatcher.
 * The fixture supplies a known closed history anchor, stable historical bindings,
 * complete read coverage and exclusive legacy ownership. An ordinary legacy offset
 * supplies NONE of those facts; see docs/dcg-architecture.md (online handoff).
 */
class As400LegacyHandoffTest {
    private static final Map<String, String> BOTH = Map.of("table.include.list", "DCOC1107\\.(A|B)");
    private static final Set<String> LEGACY_TYPES = Set.of("PT", "PX", "UP", "UB", "DL", "DR", "CT", "CG", "CR", "SC", "CM");
    private static final Set<String> OFFSET_KEYS = Set.of(As400OffsetContext.EVENT_SEQUENCE, As400OffsetContext.EVENT_TIME,
            As400OffsetContext.RECEIVER, As400OffsetContext.RECEIVER_LIBRARY, As400OffsetContext.PROCESSED, "snapshot_completed");

    @TempDir
    Path directory;

    @Test
    void openCycleWithoutPublishedChangesMigratesWithoutSnapshotAndOtherTableProgresses() throws Exception {
        var h = new Harness(directory, BOTH);
        var evidence = h.provisionFixtureEvidence();
        var legacy = h.legacy(31);
        var candidate = h.prepare(legacy, evidence, BOTH);
        assertThat(candidate).containsEntry("completed_through.sequence", "29"); // Never relabel legacy L=31 as W.
        assertThat(legacy.get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("31");
        assertThat(h.output).isEmpty();

        // Model a durable offset acknowledgement, then discard all live state.
        persist(candidate);
        h = new Harness(directory, BOTH);
        var open = h.run(load(), 42, Integer.MAX_VALUE);
        assertThat(As400RecoveryTestSupport.data(h.output)).containsExactly("c:A:8");
        assertThat(open).containsEntry("replay_from.sequence", "31").containsEntry("completed_through.sequence", "42");
        persist(open);
        h = new Harness(directory, BOTH);
        var done = h.run(load(), 64, Integer.MAX_VALUE);
        assertThat(As400RecoveryTestSupport.data(h.output)).containsExactly("t:B", "c:B:100", "c:A:9");
        assertThat(done).containsEntry("completed_through.sequence", "59");
        assertThat(h.output).filteredOn(r -> r.value() instanceof Struct s && s.schema().field("op") != null)
                .allSatisfy(r -> assertThat(((Struct) r.value()).getString("op")).isNotEqualTo("r"));
    }

    @ParameterizedTest
    @ValueSource(longs = { 43, 44, 46 })
    void completedLegacyPrefixIsNotRepublishedAndCrossingRollbackProducesNoClear(long cursor) throws Exception {
        var h = new Harness(directory, BOTH);
        var candidate = h.prepare(h.legacy(cursor), h.provisionFixtureEvidence(), BOTH);
        assertThat(candidate).containsEntry("completed_through.sequence", "43");
        assertThat(h.output).isEmpty();
        persist(candidate);
        h = new Harness(directory, BOTH);
        h.run(load(), 64, Integer.MAX_VALUE);
        // A:8 and B:100 are already in the legacy prefix. The old skipped clear is
        // not retroactively emitted: this is future capture, not historical repair.
        assertThat(As400RecoveryTestSupport.data(h.output)).containsExactly("c:A:9");
    }

    @ParameterizedTest
    @ValueSource(longs = { 40, 42, 55, 56 })
    void alreadyPublishedOrSkippedOpenPrefixRejectsBeforeAnyOutputOrOffsetChange(long cursor) throws Exception {
        var h = new Harness(directory, BOTH);
        var evidence = h.provisionFixtureEvidence();
        var legacy = h.legacy(cursor);
        persist(legacy);
        String history = Files.readString(h.fixture.file);
        assertThatThrownBy(() -> h.prepare(legacy, evidence, BOTH)).hasMessageContaining("Open cycle has a selected legacy prefix");
        assertThat(load()).isEqualTo(legacy);
        assertThat(Files.readString(h.fixture.file)).isEqualTo(history);
        assertThat(h.output).isEmpty();
    }

    @Test
    void oneEntryAheadPublicationMakesAnOtherwiseEmptyPrefixAmbiguous() throws Exception {
        var h = new Harness(directory, Map.of("table.include.list", "DCOC1107\\.A"));
        var evidence = h.provisionFixtureEvidence();
        // B is excluded and may remain open, but A:8 at 42 can already have been
        // published with offset 40. The cursor is not an exact emission fence.
        assertThatThrownBy(() -> h.prepare(h.legacy(40), evidence, h.selection)).hasMessageContaining("Ambiguous legacy dispatch overlap");
        assertThat(h.output).isEmpty();
    }

    @Test
    void anExactPublicationCutRemovesOnlyTheLaggingCursorAmbiguity() throws Exception {
        var selection = Map.of("table.include.list", "DCOC1107\\.A");
        var h = new Harness(directory, selection);
        var evidence = h.provisionFixtureEvidence();
        var legacy = h.legacy(40);
        assertThatThrownBy(() -> h.prepare(legacy, evidence, selection)).hasMessageContaining("Ambiguous legacy dispatch overlap");
        // A conditional input from an acknowledged barrier, not an IBM i discovery
        // result. The local Kafka probe independently exercises the acknowledgement.
        var exact = h.prepareAtAcknowledgedFixtureCut(legacy, evidence, selection);
        assertThat(exact.get("completed_through.sequence")).isNotEqualTo("40");
        persist(exact);
        var restarted = new Harness(directory, selection);
        var throughRow = restarted.run(load(), 42, Integer.MAX_VALUE);
        assertThat(As400RecoveryTestSupport.data(restarted.output)).containsExactly("c:A:8");
        assertThat(throughRow).containsEntry("replay_from.sequence", "31").containsEntry("completed_through.sequence", "42");
    }

    @Test
    void acknowledgedCutCannotRepairADirtyOpenPrefixOrMissingHistory() throws Exception {
        var h = new Harness(directory, BOTH);
        var evidence = h.provisionFixtureEvidence();
        assertThatThrownBy(() -> h.prepareAtAcknowledgedFixtureCut(h.legacy(40), evidence, BOTH))
                .hasMessageContaining("Open cycle has a selected legacy prefix");
        h.fixture.entries.removeIf(e -> e.position().offset().longValueExact() == 31);
        assertThatThrownBy(() -> h.prepareAtAcknowledgedFixtureCut(h.legacy(44), evidence, BOTH))
                .hasMessageContaining("Complete retained history coverage is unavailable");
        assertThat(h.output).isEmpty();
    }

    @Test
    void realRpcCallbackExposesPreviousPositionWhileCurrentRowHasItsOwnSourceSequence() throws Exception {
        var h = new Harness(directory, BOTH);
        var current = h.fixture.entries.stream().filter(e -> e.position().offset().intValueExact() == 42).findFirst().orElseThrow();
        var offset = new As400OffsetContext.Loader(h.config).load(h.legacy(40));
        var rpc = mock(As400RpcConnection.class, CALLS_REAL_METHODS);
        var journal = mock(RetrieveJournal.class);
        set(rpc, "lifecycle", new JournalReadLifecycle());
        set(rpc, "retrieveJournal", journal);
        Field periodic = As400RpcConnection.class.getDeclaredField("periodic");
        periodic.setAccessible(true);
        periodic.set(rpc, mock(periodic.getType()));
        when(journal.retrieveJournal(any())).thenReturn(RetrievalState.Success);
        when(journal.nextEntry()).thenReturn(true, false);
        when(journal.getEntryHeader()).thenReturn(current.header());
        when(journal.getPosition()).thenReturn(new JournalProcessedPosition(current.position(), current.header().getTimestamp(), true));
        List<Map<String, ?>> offsets = new ArrayList<>();
        var context = mock(ChangeEventSourceContext.class);
        when(context.isRunning()).thenReturn(true);
        rpc.getJournalEntries(context, offset, (sequence, reader, header) -> {
            offset.updateSourceInfo(header);
            assertThat(offset.getSourceInfo().getString("sequence")).isEqualTo("42");
            offsets.add(Map.copyOf(offset.getOffset()));
        }, mock(WatchDog.class));
        assertThat(offsets).hasSize(1);
        assertThat(offsets.get(0).get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("40");
        assertThat(offset.getOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
        Field types = RetrieveJournal.class.getDeclaredField("REQURED_ENTRY_TYPES");
        types.setAccessible(true);
        assertThat(java.util.Arrays.stream((RetrievalCriteria.JournalEntryType[]) types.get(null)).map(RetrievalCriteria.JournalEntryType::getKey)
                .collect(Collectors.toSet())).isEqualTo(LEGACY_TYPES);
    }

    @ParameterizedTest
    @ValueSource(strings = { "A", "B" })
    void standardTableSelectionFiltersAllOperationsWithoutAPerTableTruncateMode(String table) throws Exception {
        var selection = Map.of("table.include.list", "DCOC1107\\." + table);
        var h = new Harness(directory, selection);
        var saved = h.prepare(h.legacy(31), h.provisionFixtureEvidence(), selection);
        h.run(saved, 64, Integer.MAX_VALUE);
        assertThat(As400RecoveryTestSupport.data(h.output)).containsExactlyElementsOf(table.equals("A")
                ? List.of("c:A:8", "c:A:9")
                : List.of("t:B", "c:B:100"));
    }

    @Test
    void excludedTableStillRequiresCompleteTransactionAndBindingContext() throws Exception {
        var selection = Map.of("table.include.list", "DCOC1107\\.A");
        var h = new Harness(directory, selection);
        var evidence = h.provisionFixtureEvidence();
        var candidate = h.prepare(h.legacy(42), evidence, selection);
        assertThat(candidate).containsEntry("replay_from.sequence", "31").containsEntry("completed_through.sequence", "42");
        assertThat(As400RecoveryTestSupport.data(h.output)).isEmpty();
        h.run(candidate, 64, Integer.MAX_VALUE);
        assertThat(As400RecoveryTestSupport.data(h.output)).containsExactly("c:A:9");
        Files.writeString(h.fixture.file, Files.readString(h.fixture.file).replace("00000000000000000003", "00000000000000000009"));
        h.output.clear();
        assertThatThrownBy(() -> h.prepare(h.legacy(42), evidence, selection)).hasRootCauseInstanceOf(IllegalArgumentException.class);
        assertThat(h.output).isEmpty();
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void interruptedDeliveryKeepsDurableFWAndFreshInstanceReplaysTheWholeUnacknowledgedBatch(int accepted) throws Exception {
        var h = new Harness(directory, BOTH);
        var migrated = h.prepare(h.legacy(31), h.provisionFixtureEvidence(), BOTH);
        var durable = h.run(migrated, 42, Integer.MAX_VALUE);
        persist(durable);
        var failed = new Harness(directory, BOTH);
        assertThatThrownBy(() -> failed.run(load(), 44, accepted)).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
        assertThat(load()).isEqualTo(durable);
        failed.output.forEach(r -> assertThat(r.sourceOffset()).isEqualTo(durable));
        var replacement = new Harness(directory, BOTH);
        replacement.run(load(), 64, Integer.MAX_VALUE);
        assertThat(As400RecoveryTestSupport.data(replacement.output)).containsExactly("t:B", "c:B:100", "c:A:9");
        // An arbitrary sink can have seen a prefix twice. Kafka atomic publication
        // is not simulated by this list, and this test makes no exactly-once claim.
    }

    @Test
    void crashBeforeMigrationAcknowledgementLeavesLegacyOffsetAndCanRecomputeCandidate() throws Exception {
        var h = new Harness(directory, BOTH);
        var evidence = h.provisionFixtureEvidence();
        var legacy = h.legacy(31);
        persist(legacy);
        var candidate = h.prepare(load(), evidence, BOTH);
        assertThat(load()).isEqualTo(legacy);
        h = new Harness(directory, BOTH);
        assertThat(h.prepare(load(), evidence, BOTH)).isEqualTo(candidate);
        assertThat(h.output).isEmpty();
        persist(candidate);
        h = new Harness(directory, BOTH);
        h.run(load(), 64, Integer.MAX_VALUE);
        assertThat(As400RecoveryTestSupport.data(h.output)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
    }

    @ParameterizedTest
    @ValueSource(strings = { "offset-missing", "snapshot-pending", "unprocessed", "receiver", "selection", "anchor", "coverage",
            "start-missing", "whole-cycle-missing", "reordered", "history-missing", "history-corrupt" })
    void insufficientOrIncompatibleEvidenceRejectsWithoutChangingExistingState(String fault) throws Exception {
        var h = new Harness(directory, BOTH);
        var evidence = h.provisionFixtureEvidence();
        Map<String, Object> legacy = new java.util.HashMap<>(h.legacy(46));
        Map<String, String> previousSelection = BOTH;
        switch (fault) {
            case "offset-missing" -> legacy.clear();
            case "snapshot-pending" -> legacy.put("snapshot_completed", "false");
            case "unprocessed" -> legacy.put(As400OffsetContext.PROCESSED, "false");
            case "receiver" -> legacy.put(As400OffsetContext.RECEIVER, "REPLACED");
            case "selection" -> previousSelection = Map.of("table.include.list", "DCOC1107\\.A");
            case "anchor" -> evidence = new FixtureEvidence(Map.of(), evidence.coverageDigest());
            case "coverage" -> evidence = new FixtureEvidence(evidence.closedAnchor(), "unproved");
            case "start-missing" -> h.fixture.entries.removeIf(e -> e.position().offset().intValueExact() == 31);
            case "whole-cycle-missing" -> h.fixture.entries.removeIf(e -> e.header().getCommitCycle().equals(BigInteger.valueOf(31)));
            case "reordered" -> java.util.Collections.swap(h.fixture.entries, 2, 3);
            case "history-missing" -> Files.delete(h.fixture.file);
            case "history-corrupt" -> Files.writeString(h.fixture.file, "{}");
            default -> throw new AssertionError(fault);
        }
        persist(legacy);
        String history = Files.exists(h.fixture.file) ? Files.readString(h.fixture.file) : "ABSENT";
        final var input = evidence;
        final var filters = previousSelection;
        assertThatThrownBy(() -> h.prepare(legacy, input, filters)).isInstanceOf(Exception.class);
        assertThat(load()).isEqualTo(legacy);
        assertThat(Files.exists(h.fixture.file) ? Files.readString(h.fixture.file) : "ABSENT").isEqualTo(history);
        assertThat(h.output).isEmpty();
    }

    @Test
    void productionOffsetLoaderStillRejectsAutomaticLegacyMigration() throws Exception {
        var h = new Harness(directory, BOTH);
        var evidence = h.provisionFixtureEvidence();
        var anchor = evidence.closedAnchor();
        assertThatThrownBy(() -> As400Checkpoint.fromOffset(h.legacy(31), h.fixture.journalIdentity, anchor.get("capture_fingerprint")))
                .hasMessageContaining("legacy migration is not automatic");
        assertThatThrownBy(() -> new As400OffsetContext.Loader(h.config).load(anchor)).hasMessageContaining("buffered recovery");
    }

    private void persist(Map<String, ?> offset) throws Exception {
        Files.writeString(directory.resolve("durable-offset.json"), new ObjectMapper().writeValueAsString(offset));
    }

    private Map<String, String> load() throws Exception {
        return new ObjectMapper().readValue(Files.readString(directory.resolve("durable-offset.json")), new TypeReference<>() {
        });
    }

    private static void set(Object object, String name, Object value) throws Exception {
        Field field = As400RpcConnection.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(object, value);
    }

    /** Fixture facts, NOT evidence collectible from an ordinary legacy offset today. */
    private record FixtureEvidence(Map<String, String> closedAnchor, String coverageDigest) {
        FixtureEvidence {
            closedAnchor = Map.copyOf(closedAnchor);
        }
    }

    private static final class Harness {
        final Fixture fixture;
        final Map<String, String> selection;
        final As400ConnectorConfig config;
        final As400CaptureFingerprint.Policy policy;
        final List<SourceRecord> output = new ArrayList<>();

        Harness(Path directory, Map<String, String> selection) throws Exception {
            fixture = new Fixture(directory);
            this.selection = Map.copyOf(selection);
            config = new As400ConnectorConfig(Configuration.from(selection).edit().with("topic.prefix", "test")
                    .with("database.dbname", "AEQA").with("database.schema", "DCOC1107").with("provide.transaction.metadata", false).build());
            var original = Fixture.POLICY;
            policy = new As400CaptureFingerprint.Policy(selection, original.skippedOperations(), original.decoder(), original.schema(), original.output(),
                    original.transactionMetadata(), original.tombstones(), original.sourcePartition());
        }

        FixtureEvidence provisionFixtureEvidence() throws Exception {
            // This controlled capture has a closed boundary at 29. We do not infer
            // that boundary, historical schemas or retention from the legacy cursor.
            var anchor = fixture.history().recordBaseline(fixture.journalIdentity, Fixture.position(29), fixture.receiverIdentity, fixture.definitions, policy);
            return new FixtureEvidence(anchor.toOffset(), digest());
        }

        Map<String, ?> legacy(long sequence) {
            var position = new JournalProcessedPosition(Fixture.position(sequence), Instant.ofEpochSecond(1_800_000_000L), true);
            return new As400OffsetContext(config, position, true, new SignalBasedIncrementalSnapshotContext<>(true)).getOffset();
        }

        Map<String, String> prepare(Map<String, ?> legacy, FixtureEvidence evidence, Map<String, String> previousSelection) throws Exception {
            return prepare(legacy, evidence, previousSelection, false);
        }

        // Explicit conditional proof input, never inferred from an ordinary offset.
        // No production caller or migration path can supply this fixture assumption.
        Map<String, String> prepareAtAcknowledgedFixtureCut(Map<String, ?> cut, FixtureEvidence evidence, Map<String, String> previousSelection) throws Exception {
            return prepare(cut, evidence, previousSelection, true);
        }

        private Map<String, String> prepare(Map<String, ?> legacy, FixtureEvidence evidence, Map<String, String> previousSelection,
                                            boolean exactPublicationCut)
                throws Exception {
            require(legacy != null && legacy.keySet().equals(OFFSET_KEYS), "Complete ordinary legacy offset required");
            require("true".equals(legacy.get("snapshot_completed")) && "true".equals(legacy.get(As400OffsetContext.PROCESSED)),
                    "Completed baseline and processed legacy position required");
            require(selection.equals(previousSelection), "Capture selection cannot change during migration");
            require(!config.shouldProvideTransactionMetadata(), "Legacy transaction metadata overlap is outside this proof");
            var loaded = new As400OffsetContext.Loader(config).load(legacy);
            require(loaded.getOffset().equals(legacy), "Noncanonical legacy offset");
            var cursor = loaded.getPosition().asJournalPosition();
            var recovered = fixture.history().recover(evidence.closedAnchor(), fixture.journalIdentity, policy);
            var anchor = As400Checkpoint.fromOffset(evidence.closedAnchor(), fixture.journalIdentity, recovered.fingerprint());
            require(!anchor.replayInclusive() && anchor.replayFrom().equals(anchor.completedThrough()), "Closed history anchor required");
            // A digest of an independently retained complete fixture detects lost
            // whole cycles as well as missing starts. It is NOT native continuity proof.
            require(digest().equals(evidence.coverageDigest()), "Complete retained history coverage is unavailable");
            var order = order();
            require(order.compare(cursor, anchor.completedThrough()) > 0, "Legacy cursor must follow the evidence anchor");
            require(fixture.entries.stream().anyMatch(e -> e.position().equals(cursor)), "Legacy cursor is absent from history");
            Set<BigInteger> selectedPrefixes = new HashSet<>();
            try (var replay = replay(anchor.toOffset(), recovered, record -> true);
                    var dispatcher = dispatcher(recovered)) {
                for (var entry : fixture.entries) {
                    if (order.compare(entry.position(), anchor.completedThrough()) <= 0 || order.compare(entry.position(), cursor) > 0) {
                        continue;
                    }
                    var header = entry.header();
                    if (header.getCommitCycle().signum() > 0 && selected(entry)
                            && (header.getJournalCode() == 'R' || header.getJournalCode() == 'D' && header.getEntryType().equals("CG"))) {
                        selectedPrefixes.add(header.getCommitCycle());
                    }
                    if (replay.accept(header, entry.data(), 0, entry.position()).isPresent()) {
                        replay.deliver(dispatcher); // Validate complete prefix, suppress its external publication.
                    }
                    if (header.getJournalCode() == 'C' && Set.of("CM", "RB").contains(header.getEntryType())) {
                        selectedPrefixes.remove(header.getCommitCycle());
                    }
                }
                replay.verifyOrdinaryCut();
                require(selectedPrefixes.isEmpty(), "Open cycle has a selected legacy prefix; rollback or skipped clear cannot be repaired from an offset");
                // The legacy callback stamps the previous returned entry. The next
                // legacy-returned row may already be durable. Server file filtering
                // and transactionControls=true are outside this fixture's profile.
                var next = fixture.entries.stream().filter(e -> order.compare(e.position(), cursor) > 0
                        && LEGACY_TYPES.contains(e.header().getEntryType())).findFirst().orElseThrow();
                require(exactPublicationCut || !(selected(next) && (next.header().getJournalCode() == 'R' && !next.header().getEntryType().equals("UB")
                        || next.header().getJournalCode() == 'F' && next.header().getEntryType().equals("CR"))),
                        "Ambiguous legacy dispatch overlap; current row may already be published");
                return replay.checkpoint().toOffset(); // Candidate only; no durable migration or output here.
            }
        }

        Map<String, String> run(Map<String, ?> durable, long through, int accepted) throws Exception {
            var recovered = fixture.history().recover(durable, fixture.journalIdentity, policy);
            var checkpoint = As400Checkpoint.fromOffset(durable, fixture.journalIdentity, recovered.fingerprint());
            var order = order();
            try (var replay = replay(durable, recovered, record -> output.size() < accepted && output.add(record));
                    var dispatcher = dispatcher(recovered)) {
                for (var entry : fixture.entries) {
                    int start = order.compare(entry.position(), checkpoint.replayFrom());
                    if ((start > 0 || start == 0 && checkpoint.replayInclusive()) && entry.position().offset().longValueExact() <= through) {
                        if (replay.accept(entry.header(), entry.data(), 0, entry.position()).isPresent()) {
                            replay.deliver(dispatcher);
                        }
                    }
                }
                replay.verifyOrdinaryCut();
                // Merely scanning R does not persist anything. The caller must model
                // acknowledgement of this ordered checkpoint before replacing durable F/W.
                return replay.publishCheckpoint(new SourceRecord(policy.sourcePartition(), Map.of(), "heartbeat", Schema.STRING_SCHEMA, "checkpoint")).sourceOffset()
                        .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> (String) e.getValue()));
            }
        }

        private As400ReplayCoordinator replay(Map<String, ?> saved, As400BindingHistory.Recovered recovered, As400ReplayCoordinator.RecordSink sink) {
            return new As400ReplayCoordinator(saved, fixture.journalIdentity, recovered.fingerprint(), order(),
                    new As400TransactionBuffer.Limits(16, 1000, 128, 65536), 1024, 100, recovered.resolver(1024),
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), policy.sourcePartition(), sink,
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), true);
        }

        private As400BatchDispatcher dispatcher(As400BindingHistory.Recovered recovered) {
            Map<String, TableBinding> bindings = recovered.definitions().stream()
                    .collect(Collectors.toMap(d -> d.identity().physicalFile(), d -> d.binding(1024)));
            return new As400IncrementalLifecycleTest.Output(bindings, true, selection).dispatcher;
        }

        private boolean selected(RawJournalPage.Entry entry) {
            return fixture.definitions.stream().map(As400BindingDefinition::identity)
                    .anyMatch(id -> id.physicalFile().equals(entry.header().getFile()) && id.tableId().schema().equals(entry.header().getLibrary())
                            && config.getTableFilters().dataCollectionFilter().isIncluded(id.tableId()));
        }

        private As400JournalOrder order() {
            return new As400JournalOrder(List.of(Fixture.RECEIVER), Map.of(Fixture.RECEIVER, fixture.receiverIdentity),
                    Map.of(Fixture.RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(29), BigInteger.valueOf(64))));
        }

        private String digest() throws Exception {
            var digest = MessageDigest.getInstance("SHA-256");
            for (var entry : fixture.entries) {
                digest.update(entry.position().toString().getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
                digest.update(entry.data());
            }
            return HexFormat.of().formatHex(digest.digest());
        }

        private static void require(boolean condition, String message) {
            if (!condition) {
                throw new IllegalArgumentException(message);
            }
        }
    }
}
