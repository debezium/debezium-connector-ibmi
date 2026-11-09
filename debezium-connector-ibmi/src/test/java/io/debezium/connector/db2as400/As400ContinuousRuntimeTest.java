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

import io.debezium.connector.db2as400.As400BindingHistoryTest.Fixture;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

class As400ContinuousRuntimeTest {
    @TempDir
    Path directory;

    @Test
    void bufferSurvivesIncrementalAndIdlePollsWithoutReplayingPreviouslyScannedEntries() throws Exception {
        Source f = new Source(directory);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            var saved = runtime.poll(f.initial).checkpoint();
            assertThat(As400BindingHistoryTest.data(records)).containsExactly("c:A:8");
            f.ranges.clear();
            assertThat(runtime.poll(saved).entries()).isZero();
            assertThat(f.ranges).containsExactly("31:31", "42:42");
            f.tail = 44;
            saved = runtime.poll(saved).checkpoint();
            assertThat(As400BindingHistoryTest.data(records)).containsExactly("c:A:8", "t:B", "c:B:100");
            f.tail = 56;
            saved = runtime.poll(saved).checkpoint();
            f.tail = 64;
            var finalPoll = runtime.poll(saved);
            assertThat(finalPoll.scan()).contains(Fixture.position(64));
            assertThat(finalPoll.checkpoint()).containsEntry("completed_through.sequence", "59");
            assertThat(As400BindingHistoryTest.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
            assertThat(f.ranges).doesNotContain("31:44", "31:56", "46:64");
        }
    }

    @Test
    void freshInstanceUsesOnlyDurableCheckpointAndRebuildsOpenCycle() throws Exception {
        Source f = new Source(directory);
        List<SourceRecord> records = new ArrayList<>();
        Map<String, String> saved;
        try (var output = f.output(); var runtime = f.runtime(f.initial, f::read, output, records::add)) {
            saved = runtime.poll(f.initial).checkpoint();
        }
        Source replacement = new Source(directory);
        replacement.tail = 64;
        try (var output = replacement.output(); var runtime = replacement.runtime(saved, replacement::read, output, records::add)) {
            assertThat(runtime.poll(saved).checkpoint()).containsEntry("completed_through.sequence", "59");
        }
        assertThat(As400BindingHistoryTest.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
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
            assertThat(As400BindingHistoryTest.data(records)).containsExactly("c:A:8");
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

        Source(Path directory) throws Exception {
            this(directory, new As400OperatorCutReader.Limits(16, 100, 1_000_000));
        }

        Source(Path directory, As400OperatorCutReader.Limits limits) throws Exception {
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
            when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
                JournalPosition from = i.getArgument(0);
                JournalPosition through = i.getArgument(1);
                ranges.add(from.offset() + ":" + through.offset());
                var available = fixture.entries.stream().filter(e -> e.position().offset().compareTo(from.offset()) >= 0
                        && e.position().offset().compareTo(through.offset()) <= 0).toList();
                int count = Math.min((int) i.getArgument(2), available.size());
                return new RawJournalPage(available.subList(0, count), count < available.size() ? Optional.of(available.get(count).position()) : Optional.empty(), 65536);
            });
            reader = new As400OperatorCutReader(journals, identities, Fixture.JOURNAL, history, transport, limits);
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
            return new As400ContinuousRuntime(durable, history, source, output, sink, Fixture.POLICY.sourcePartition(), () -> {
            });
        }
    }
}
