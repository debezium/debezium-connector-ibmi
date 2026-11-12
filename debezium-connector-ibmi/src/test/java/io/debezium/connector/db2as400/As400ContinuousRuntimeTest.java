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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

class As400ContinuousRuntimeTest {
    @TempDir
    Path directory;

    @Test
    void shouldKeepPendingSnapshotProgressThroughHistoryRecoveryAndNativeReaderCuts() throws Exception {
        var f = new Source(directory);
        var chunk = new io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext<io.debezium.relational.TableId>(true);
        chunk.addDataCollectionNamesToSnapshot("snapshot", List.of("DB.LIB.TEST"), List.of(), "");
        chunk.maximumKey(new Object[]{ 900 });
        chunk.sendEvent(new Object[]{ 256 });
        var state = As400IncrementalSnapshotState.capture(chunk);
        var saved = As400Checkpoint.fromOffset(f.initial, f.fixture.journalIdentity, f.history.fingerprint()).withSnapshotState(state).toOffset();
        assertThat(f.fixture.history().recover(saved, f.fixture.journalIdentity, Fixture.POLICY).fingerprint()).isEqualTo(f.history.fingerprint());
        var records = new ArrayList<SourceRecord>();
        try (var output = f.output(); var runtime = f.runtime(saved, f::read, output, records::add)) {
            var completed = runtime.poll(saved).checkpoint();
            assertThat(completed.get("completed_through.sequence")).isEqualTo("42");
            assertThat(As400IncrementalSnapshotState.fields(completed)).isEqualTo(state);
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
            assertThat(records).allSatisfy(record -> assertThat(As400IncrementalSnapshotState.fields(record.sourceOffset())).isEqualTo(state));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReadSeveralCutsWhileDurableOffsetsLagAndReplayFromOnlyTheSavedCheckpoint(boolean disk) throws Exception {
        Source f = new Source(directory);
        f.storage = disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            Map<String, ?> published = f.initial;
            for (long tail : new long[]{ 42, 44, 56, 64 }) {
                f.tail = tail;
                published = runtime.poll(published, f.initial).checkpoint();
            }
            assertThat(published.get("completed_through.sequence")).isEqualTo("59");
            assertThat(f.initial.get("completed_through.sequence")).isEqualTo("29");
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
            assertThat(f.ranges).contains("29:29").doesNotContain("31:44", "31:56", "46:64");
        }
        assertNoSpillDirectories();
        records.clear();
        Source replacement = new Source(directory);
        replacement.storage = f.storage;
        replacement.tail = 64;
        try (var output = replacement.output(); var runtime = replacement.runtime(f.initial, replacement::read, output, records::add)) {
            assertThat(runtime.poll(f.initial).checkpoint()).containsEntry("completed_through.sequence", "59");
        }
        assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
        assertNoSpillDirectories();
    }

    @Test
    void shouldPollLongRetainedChainsAndStillRejectALateReceiverReplacement() throws Exception {
        Source f = new Source(directory);
        var polls = new AtomicInteger();
        As400ContinuousRuntime.Intervals intervals = (durable, scan) -> {
            var cut = f.read(durable, scan);
            var names = new ArrayList<io.debezium.ibmi.db2.journal.retrieve.JournalReceiver>();
            var identities = new java.util.HashMap<io.debezium.ibmi.db2.journal.retrieve.JournalReceiver, String>();
            var ranges = new java.util.HashMap<io.debezium.ibmi.db2.journal.retrieve.JournalReceiver, As400JournalOrder.Range>();
            names.add(Fixture.RECEIVER);
            identities.put(Fixture.RECEIVER, cut.order().incarnation(cut.checkpoint().replayFrom()));
            ranges.put(Fixture.RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(29), BigInteger.valueOf(f.tail)));
            // The interval provider supplies a verified retained chain. These
            // later receivers have no selected entries; durable F remains pinned.
            boolean replaced = polls.incrementAndGet() == 3;
            for (int i = 1; i <= 256; i++) {
                var receiver = new io.debezium.ibmi.db2.journal.retrieve.JournalReceiver("R" + i, "LIB");
                names.add(receiver);
                identities.put(receiver, replaced && i == 256 ? "replaced" : "generation-" + i);
                var sequence = BigInteger.valueOf(f.tail + i);
                ranges.put(receiver, new As400JournalOrder.Range(sequence, sequence));
            }
            var order = new As400JournalOrder(names, identities, ranges);
            return new As400OperatorCutReader.Cut(cut.checkpoint(), order,
                    new JournalPosition(BigInteger.valueOf(f.tail + 256), names.get(256)), cut.entries(), cut.tables(), cut.historicalBindings(), cut.complete());
        };
        var records = new ArrayList<SourceRecord>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, intervals, output, records::add)) {
            var first = runtime.poll(f.initial);
            var idle = runtime.poll(first.checkpoint(), f.initial);
            assertThat(idle.entries()).isZero();
            assertThat(idle.scan()).contains(Fixture.position(42));
            assertThat(idle.checkpoint()).isEqualTo(first.checkpoint());
            assertThrows(IllegalArgumentException.class, () -> runtime.poll(first.checkpoint(), f.initial));
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
            assertThat(f.initial).containsEntry("completed_through.sequence", "29");
        }
        polls.set(0);
        records.clear();
        try (var output = f.output(); var replacement = f.runtime(f.initial, intervals, output, records::add)) {
            replacement.poll(f.initial);
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "regression", "ahead", "missing-history" })
    void shouldRejectUnsafeDurablePositionsEvenAfterTheReaderHasProgressed(String fault) throws Exception {
        Source f = new Source(directory);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            var published = runtime.poll(f.initial).checkpoint();
            Map<String, ?> invalid = f.initial;
            if (fault.equals("regression")) {
                runtime.poll(published, published);
            }
            else if (fault.equals("ahead")) {
                var changed = new java.util.HashMap<>(published);
                changed.put("completed_through.sequence", "59");
                invalid = changed;
            }
            else {
                f.fixture.entries.removeIf(entry -> entry.position().equals(Fixture.position(29)));
            }
            var checkpoint = invalid;
            assertThrows(Exception.class, () -> runtime.poll(published, checkpoint));
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void bufferSurvivesIncrementalAndIdlePollsWithoutReplayingPreviouslyScannedEntries(boolean disk) throws Exception {
        Source f = new Source(directory);
        f.storage = disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            var saved = runtime.poll(f.initial).checkpoint();
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
            f.ranges.clear();
            assertThat(runtime.poll(saved).entries()).isZero();
            assertThat(f.ranges).containsExactly("31:31", "42:42");
            f.tail = 44;
            saved = runtime.poll(saved).checkpoint();
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100");
            f.tail = 56;
            saved = runtime.poll(saved).checkpoint();
            f.tail = 64;
            var finalPoll = runtime.poll(saved);
            assertThat(finalPoll.scan()).contains(Fixture.position(64));
            assertThat(finalPoll.checkpoint()).containsEntry("completed_through.sequence", "59");
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
            assertThat(f.ranges).doesNotContain("31:44", "31:56", "46:64");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void freshInstanceUsesOnlyDurableCheckpointAndRebuildsOpenCycle(boolean disk) throws Exception {
        Source f = new Source(directory);
        f.storage = disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null;
        List<SourceRecord> records = new ArrayList<>();
        Map<String, String> saved;
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            saved = runtime.poll(f.initial).checkpoint();
        }
        assertNoSpillDirectories();
        Source replacement = new Source(directory);
        replacement.storage = f.storage;
        replacement.tail = 64;
        try (var output = replacement.output(); var runtime = replacement.runtime(saved, replacement::read, output, records::add)) {
            assertThat(runtime.poll(saved).checkpoint()).containsEntry("completed_through.sequence", "59");
        }
        assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
        assertNoSpillDirectories();
    }

    private void assertNoSpillDirectories() throws Exception {
        try (var paths = java.nio.file.Files.list(directory)) {
            assertThat(paths.filter(p -> p.getFileName().toString().startsWith("ibmi-truncate-")).toList()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRebuildAnExcludedOpenTransactionWhenSelectionChangesAtRestart(boolean disk) throws Exception {
        var f = new Source(directory);
        f.storage = disk ? new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024) : null;
        var records = new ArrayList<SourceRecord>();
        Map<String, String> saved;
        var bindings = f.history.definitions().stream().collect(Collectors.toMap(d -> d.identity().physicalFile(), d -> d.binding(65536)));
        var table = bindings.get("A").identity().tableId();
        try (var output = new As400IncrementalLifecycleTest.Output(bindings, true,
                Map.of("table.include.list", java.util.regex.Pattern.quote(table.schema() + "." + table.table()))).dispatcher;
                var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            saved = runtime.poll(f.initial).checkpoint();
            assertThat(saved).containsEntry("completed_through.sequence", "42").containsEntry("replay_from.sequence", "31");
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
        }
        assertNoSpillDirectories();
        var replacement = new Source(directory);
        replacement.storage = f.storage;
        replacement.tail = 64;
        try (var output = replacement.output(); var runtime = replacement.runtime(saved, replacement::read, output, records::add)) {
            assertThat(runtime.poll(saved).checkpoint()).containsEntry("completed_through.sequence", "59");
        }
        // The completed A prefix is suppressed; the newly selected B transaction
        // is reconstructed from F, including its earlier clear and inserted row.
        assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
        assertNoSpillDirectories();
    }

    @ParameterizedTest
    @ValueSource(strings = { "success", "open", "read", "close" })
    void shouldOwnOneFreshMetadataConnectionPerIntervalAndPublishNothingOnFailure(String fault) throws Exception {
        var connections = List.of(mock(java.sql.Connection.class), mock(java.sql.Connection.class));
        var opens = new AtomicInteger();
        if (fault.equals("close")) {
            org.mockito.Mockito.doThrow(new java.sql.SQLException("Cannot close interval")).when(connections.get(0)).close();
        }
        var f = new Source(directory, new As400OperatorCutReader.Limits(16, 100, 1_000_000), () -> {
            int number = opens.getAndIncrement();
            if (fault.equals("open")) {
                throw new java.sql.SQLException("Cannot open interval");
            }
            return connections.get(number);
        });
        assertThrows(IllegalArgumentException.class, () -> f.reader.read(Map.of()));
        assertThat(opens).hasValue(0);
        if (fault.equals("read")) {
            f.fixture.entries.clear();
        }
        var records = new ArrayList<SourceRecord>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            if (fault.equals("success")) {
                var progress = runtime.poll(f.initial);
                runtime.poll(progress.checkpoint(), f.initial);
                assertThat(opens).hasValue(2);
                assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
                for (var connection : connections) {
                    org.mockito.Mockito.verify(connection).close();
                }
            }
            else {
                assertThrows(Exception.class, () -> runtime.poll(f.initial));
                assertThat(records).isEmpty();
                assertThat(opens).hasValue(1);
                if (!fault.equals("open")) {
                    org.mockito.Mockito.verify(connections.get(0)).close();
                }
                assertThat(f.initial).containsEntry("completed_through.sequence", "29");
            }
        }
    }

    @Test
    void shouldNotRetryAnAttachmentRaceWhoseIntervalFailedToClose() throws Exception {
        var f = new Source(directory);
        var attempts = new AtomicInteger();
        var records = new ArrayList<SourceRecord>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, (durable, scan) -> {
            attempts.incrementAndGet();
            var race = new As400OperatorCutReader.UnstableCutException("Receiver rotated");
            race.addSuppressed(new java.sql.SQLException("Connection drain failed"));
            throw race;
        }, output, records::add)) {
            assertThrows(As400OperatorCutReader.UnstableCutException.class, () -> runtime.poll(f.initial));
            assertThat(attempts).hasValue(1);
            assertThat(records).isEmpty();
        }
    }

    @Test
    void shouldRetainDurableCheckpointAndRemoveSpillFilesWhenDeliveryFails() throws Exception {
        Source f = new Source(directory);
        f.storage = new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, record -> {
            records.add(record);
            return false;
        })) {
            assertThrows(Exception.class, () -> runtime.poll(f.initial));
            assertNoSpillDirectories();
            assertThat(records).isNotEmpty().allSatisfy(record -> assertThat(record.sourceOffset().get("completed_through.sequence"))
                    .isEqualTo(f.initial.get("completed_through.sequence")));
            assertThrows(Exception.class, () -> runtime.poll(f.initial));
        }
        assertNoSpillDirectories();
    }

    @Test
    void shouldPublishNothingAndRecoverFromTheSameOffsetWhenPreparedOutputExhaustsDiskBudget() throws Exception {
        Source f = new Source(directory);
        var budget = As400ContinuousRuntime.Limits.experimental();
        var settings = new As400SpillStore.Settings(directory, 1, 16, 65536);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = new As400ContinuousRuntime(f.initial, f.history, f::read, (batch, collector) -> {
            output.prepare(batch, record -> {
                // An otherwise valid Connect record whose prepared representation
                // exceeds the remaining local budget. The sink must see nothing.
                record.headers().addBytes("large-header", new byte[65536]);
                collector.accept(record);
            });
        }, records::add, Fixture.POLICY.sourcePartition(), () -> {
        }, new As400ContinuousRuntime.Limits(budget.transactions(), budget.lifecycle(), budget.outputRecords(), false, settings))) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> runtime.poll(f.initial))
                    .hasStackTraceContaining("spill data exceeded its disk budget");
            assertThat(records).isEmpty();
            assertNoSpillDirectories();
        }
        Source replacement = new Source(directory);
        replacement.storage = new As400SpillStore.Settings(directory, 1, 16, 8 * 1024 * 1024);
        try (var output = replacement.output(); var runtime = replacement.runtime(f.initial, replacement::read, output, records::add)) {
            assertThat(runtime.poll(f.initial).checkpoint()).containsEntry("completed_through.sequence", "42");
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
        }
        assertNoSpillDirectories();
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-floor", "changed-scan", "stale-offset" })
    void historyOrDurabilityFailuresStopBeforeFurtherOutput(String fault) throws Exception {
        Source f = new Source(directory);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            var saved = runtime.poll(f.initial).checkpoint();
            f.tail = 44;
            if (fault.equals("missing-floor")) {
                f.fixture.entries.removeIf(e -> e.position().equals(Fixture.position(31)));
            }
            if (fault.equals("changed-scan")) {
                for (int i = 0; i < f.fixture.entries.size(); i++) {
                    var e = f.fixture.entries.get(i);
                    if (e.position().equals(Fixture.position(42))) {
                        byte[] changed = e.data();
                        changed[changed.length - 1] ^= 1;
                        f.fixture.entries.set(i, new RawJournalPage.Entry(e.position(), e.header(), changed));
                    }
                }
            }
            f.ranges.clear();
            assertThrows(Exception.class, () -> runtime.poll(fault.equals("stale-offset") ? f.initial : saved));
            if (fault.equals("stale-offset")) {
                assertThat(f.ranges).isEmpty();
            }
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
            assertThrows(Exception.class, () -> runtime.poll(saved));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "fingerprint", "scan-as-offset" })
    void invalidDurableStateRejectsBeforeAnySourceRead(String fault) throws Exception {
        Source f = new Source(directory);
        Map<String, String> invalid = new java.util.HashMap<>(f.initial);
        if (fault.equals("fingerprint")) {
            invalid.put("capture_fingerprint", "v1:" + "0".repeat(64));
        }
        else if (fault.equals("scan-as-offset")) {
            invalid.put("scan.sequence", "42");
        }
        try (var output = f.output()) {
            assertThrows(IllegalArgumentException.class, () -> f.runtime(fault.equals("missing") ? null : invalid, (offset, scan) -> {
                throw new AssertionError("Invalid durable state opened the source");
            }, output, record -> {
                throw new AssertionError("Invalid durable state published");
            }));
        }
        assertThat(f.ranges).isEmpty();
    }

    @Test
    void onlyUnpublishedAttachmentRacesCanRetryAndRetriesAreBounded() throws Exception {
        Source f = new Source(directory);
        AtomicInteger attempts = new AtomicInteger();
        try (var output = f.output(); var runtime = f.runtime(f.initial, (offset, scan) -> {
            if (attempts.incrementAndGet() <= 2) {
                throw new As400OperatorCutReader.UnstableCutException("rotation");
            }
            return f.read(offset, scan);
        }, output, r -> true)) {
            assertThat(runtime.poll(f.initial).retries()).isEqualTo(2);
        }
        attempts.set(0);
        try (var output = f.output(); var runtime = f.runtime(f.initial, (offset, scan) -> {
            attempts.incrementAndGet();
            throw new As400OperatorCutReader.UnstableCutException("rotation");
        }, output, r -> {
            throw new AssertionError("No unverified output");
        })) {
            assertThrows(Exception.class, () -> runtime.poll(f.initial));
            assertThat(attempts).hasValue(3);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "recovers", "persistent", "mixed-race", "close-failure", "cancelled" })
    void shouldRecollectIncompleteIntervalsWithoutAdvancingScanOrDurableOffsets(String fault) throws Exception {
        var f = new Source(directory);
        var records = new ArrayList<SourceRecord>();
        var attempts = new AtomicInteger();
        var observedOffsets = new ArrayList<Map<String, ?>>();
        var observedScans = new ArrayList<Optional<RawJournalPage.Entry>>();
        var stopped = new AtomicReference<As400ContinuousRuntime>();
        var budget = As400ContinuousRuntime.Limits.experimental();
        try (var output = f.output(); var runtime = new As400ContinuousRuntime(f.initial, f.history, (durable, scan) -> {
            int attempt = attempts.incrementAndGet();
            if (attempt == 1) {
                return f.read(durable, scan);
            }
            observedOffsets.add(durable);
            observedScans.add(scan);
            if (attempt > 3 && fault.equals("recovers")) {
                return f.read(durable, scan);
            }
            if (attempt > 3 && fault.equals("mixed-race")) {
                throw new As400OperatorCutReader.UnstableCutException("Rotated after partial observation");
            }
            var incomplete = new As400OperatorJournalEvidence.IncompleteObservationException(new java.sql.SQLWarning("Incomplete", "01548", 462));
            if (fault.equals("close-failure")) {
                incomplete.addSuppressed(new java.sql.SQLException("Connection drain failed"));
            }
            if (fault.equals("cancelled")) {
                stopped.get().requestStop();
            }
            throw incomplete;
        }, output, records::add, Fixture.POLICY.sourcePartition(), () -> {
        }, new As400ContinuousRuntime.Limits(budget.transactions(), budget.lifecycle(), budget.outputRecords(), true))) {
            stopped.set(runtime);
            var first = runtime.poll(f.initial);
            int sent = records.size();
            f.tail = 64;
            if (fault.equals("recovers")) {
                var progress = runtime.poll(first.checkpoint(), f.initial);
                assertThat(progress.retries()).isEqualTo(2);
                assertThat(progress.checkpoint()).containsEntry("completed_through.sequence", "59");
                assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
            }
            else {
                assertThrows(Exception.class, () -> runtime.poll(first.checkpoint(), f.initial));
                assertThat(records).hasSize(sent);
            }
            assertThat(attempts).hasValue(List.of("recovers", "persistent", "mixed-race").contains(fault) ? 4 : 2);
            assertThat(observedOffsets).allSatisfy(offset -> assertThat(offset).isEqualTo(f.initial));
            assertThat(observedScans).allSatisfy(scan -> assertThat(scan.orElseThrow().position()).isEqualTo(first.scan().orElseThrow()));
            assertThat(f.initial).containsEntry("completed_through.sequence", "29");
        }
    }

    @Test
    void configuredProfileYieldsOnlyDiscardedRacesAndResumesFromUnchangedFw() throws Exception {
        Source f = new Source(directory);
        AtomicInteger attempts = new AtomicInteger();
        var records = new java.util.ArrayList<org.apache.kafka.connect.source.SourceRecord>();
        var budget = As400ContinuousRuntime.Limits.experimental();
        try (var output = f.output(); var runtime = new As400ContinuousRuntime(f.initial, f.history, (offset, scan) -> {
            if (attempts.incrementAndGet() <= 6) {
                throw new As400OperatorCutReader.UnstableCutException("rotation");
            }
            return f.read(offset, scan);
        }, output, r -> {
            records.add(r);
            return true;
        }, Fixture.POLICY.sourcePartition(), () -> {
        },
                new As400ContinuousRuntime.Limits(budget.transactions(), budget.lifecycle(), budget.outputRecords(), true))) {
            for (int i = 0; i < 2; i++) {
                var progress = runtime.poll(f.initial);
                assertThat(progress.deferred()).isTrue();
                assertThat(progress.checkpoint()).isEqualTo(f.initial);
                assertThat(progress.scan()).isEmpty();
                assertThat(records).isEmpty();
            }
            var progress = runtime.poll(f.initial);
            assertThat(progress.deferred()).isFalse();
            assertThat(progress.entries()).isPositive();
            assertThat(records).isNotEmpty();
        }
    }

    @Test
    void deferredRefreshPreservesExistingScanAndPermanentEvidenceFailureStillStops() throws Exception {
        Source f = new Source(directory);
        AtomicInteger attempt = new AtomicInteger();
        var budget = As400ContinuousRuntime.Limits.experimental();
        var records = new java.util.ArrayList<org.apache.kafka.connect.source.SourceRecord>();
        try (var output = f.output(); var runtime = new As400ContinuousRuntime(f.initial, f.history, (offset, scan) -> {
            int n = attempt.incrementAndGet();
            if (n >= 2 && n <= 4) {
                throw new As400OperatorCutReader.UnstableCutException("attachment moved");
            }
            if (n > 4) {
                throw new java.sql.SQLException("receiver missing");
            }
            return f.read(offset, scan);
        }, output, r -> {
            records.add(r);
            return true;
        }, Fixture.POLICY.sourcePartition(), () -> {
        },
                new As400ContinuousRuntime.Limits(budget.transactions(), budget.lifecycle(), budget.outputRecords(), true))) {
            var prior = runtime.poll(f.initial);
            int count = records.size();
            var deferred = runtime.poll(prior.checkpoint());
            assertThat(deferred.deferred()).isTrue();
            assertThat(deferred.scan()).isEqualTo(prior.scan());
            assertThat(deferred.checkpoint()).isEqualTo(prior.checkpoint());
            assertThat(records).hasSize(count);
            assertThrows(java.sql.SQLException.class, () -> runtime.poll(prior.checkpoint()));
            assertThat(records).hasSize(count);
            assertThrows(Exception.class, () -> runtime.poll(prior.checkpoint()));
        }
    }

    @Test
    void terminalStopWhileReadIsHeldPreventsPublicationAndDrainWaitsForRead() throws Exception {
        Source f = new Source(directory);
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch drained = new CountDownLatch(1);
        AtomicReference<As400ContinuousRuntime> running = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread owner = new Thread(() -> {
            try (var output = f.output(); var runtime = f.runtime(f.initial, (offset, scan) -> {
                held.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("Release timed out");
                }
                return f.read(offset, scan);
            }, output, r -> {
                throw new AssertionError("Stopped reader published");
            })) {
                running.set(runtime);
                assertThrows(Exception.class, () -> runtime.poll(f.initial));
                assertThrows(Exception.class, () -> runtime.poll(f.initial));
            }
            catch (Throwable error) {
                failure.set(error);
            }
        });
        owner.start();
        assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
        running.get().requestStop();
        Thread waiter = new Thread(() -> {
            running.get().awaitDrained();
            drained.countDown();
        });
        waiter.start();
        try {
            assertThat(drained.await(100, TimeUnit.MILLISECONDS)).isFalse();
        }
        finally {
            release.countDown();
            owner.join(5000);
            waiter.join(5000);
        }
        assertThat(owner.isAlive()).isFalse();
        assertThat(drained.getCount()).isZero();
        assertThat(failure.get()).isNull();
    }

    static final class Source {
        final Fixture fixture;
        final Map<String, String> initial;
        final As400BindingHistory.Recovered history;
        final As400OperatorCutReader reader;
        final List<String> ranges = new ArrayList<>();
        long tail = 42;
        As400SpillStore.Settings storage;

        Source(Path directory) throws Exception {
            this(directory, new As400OperatorCutReader.Limits(16, 100, 1_000_000));
        }

        Source(Path directory, As400OperatorCutReader.Limits limits) throws Exception {
            this(directory, limits, null);
        }

        Source(Path directory, As400OperatorCutReader.Limits limits, As400JdbcConnectionFactory connections) throws Exception {
            fixture = new Fixture(directory);
            initial = fixture.initialize();
            history = fixture.history().recover(initial, fixture.journalIdentity, Fixture.POLICY);
            var journals = mock(As400OperatorJournalEvidence.class);
            var identities = mock(As400JdbcNativeIdentityEvidence.class);
            var transport = mock(RetrieveJournal.class);
            when(journals.collect()).thenAnswer(i -> new As400OperatorJournalEvidence.Observation(Fixture.JOURNAL, Fixture.RECEIVER,
                    List.of(new As400OperatorJournalEvidence.Receiver(Fixture.RECEIVER, "ATTACHED", BigInteger.valueOf(29), BigInteger.valueOf(tail),
                            Optional.empty(), Optional.empty(), "NO", "YES", "YES", "NO", "NO"))));
            when(journals.collectFrom(any())).thenAnswer(i -> journals.collect());
            when(identities.collect(any())).thenAnswer(i -> new Candidates(((List<Target>) i.getArgument(0)).stream()
                    .collect(Collectors.toMap(t -> t, fixture.nativeObjects::get))));
            when(journals.collectOn(any())).thenAnswer(i -> journals.collect());
            when(journals.collectFromOn(any(), any())).thenAnswer(i -> journals.collectFrom(i.getArgument(1)));
            when(identities.collectOn(any(), any())).thenAnswer(i -> identities.collect(i.getArgument(1)));
            when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
                JournalPosition from = i.getArgument(0);
                JournalPosition through = i.getArgument(1);
                ranges.add(from.offset() + ":" + through.offset());
                var available = fixture.entries.stream().filter(e -> e.position().offset().compareTo(from.offset()) >= 0
                        && e.position().offset().compareTo(through.offset()) <= 0).toList();
                int count = Math.min((int) i.getArgument(2), available.size());
                return new RawJournalPage(available.subList(0, count), count < available.size() ? Optional.of(available.get(count).position()) : Optional.empty(), 65536);
            });
            reader = new As400OperatorCutReader(journals, identities, Fixture.JOURNAL, history, transport, limits, connections);
        }

        As400OperatorCutReader.Cut read(Map<String, ?> offset, Optional<RawJournalPage.Entry> scan) throws Exception {
            return scan.isPresent() ? reader.readAfter(offset, scan.orElseThrow()) : reader.read(offset);
        }

        As400BatchDispatcher output() {
            return new As400IncrementalLifecycleTest.Output(history.definitions().stream()
                    .collect(Collectors.toMap(d -> d.identity().physicalFile(), d -> d.binding(65536))), true).dispatcher;
        }

        As400ContinuousRuntime runtime(Map<String, ?> durable, As400ContinuousRuntime.Intervals source, As400BatchDispatcher output,
                                       As400ReplayCoordinator.RecordSink sink) {
            var limits = As400ContinuousRuntime.Limits.experimental();
            return new As400ContinuousRuntime(durable, history, source, output, sink, Fixture.POLICY.sourcePartition(), () -> {
            }, new As400ContinuousRuntime.Limits(limits.transactions(), limits.lifecycle(), limits.outputRecords(), false, storage));
        }
    }
}
