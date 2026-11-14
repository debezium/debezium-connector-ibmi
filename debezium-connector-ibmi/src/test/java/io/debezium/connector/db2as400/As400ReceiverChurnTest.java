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
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400ReceiverChurnTest {
    @TempDir
    Path directory;

    @Test
    void shouldCrossEmptyReceiversWithoutAdvancingToASyntheticPositionAndResumeAfterRestart() throws Exception {
        var source = new As400ContinuousRuntimeTest.Source(directory, new As400OperatorCutReader.Limits(128, 10000, 16_000_000));
        source.tail = 64;
        var current = new AtomicReference<As400OperatorCutReader>(source.reader);
        var records = new ArrayList<SourceRecord>();
        var added = new ArrayList<RawJournalPage.Entry>();
        var continuation = continuation(source, 130, added);
        Map<String, String> saved;
        try (var output = source.output();
                var runtime = source.runtime(source.initial,
                        (offset, scan) -> current.get().readPrefix(offset, scan.orElse(null)), output, records::add)) {
            var progress = runtime.poll(source.initial);
            assertThat(progress.scan().orElseThrow().offset()).isEqualTo(BigInteger.valueOf(64));
            saved = progress.checkpoint();
            current.set(continuation);
            progress = runtime.poll(saved);
            assertThat(progress.scan()).contains(added.get(0).position());
            assertThat(progress.checkpoint()).containsEntry("completed_through.sequence", added.get(0).position().offset().toString());
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9", "c:A:9");
        }
        records.clear();
        try (var output = source.output();
                var replacement = source.runtime(saved,
                        (offset, scan) -> continuation.readPrefix(offset, scan.orElse(null)), output, records::add)) {
            var progress = replacement.poll(saved);
            if (!progress.scan().equals(Optional.of(added.get(0).position()))) {
                progress = replacement.poll(progress.checkpoint());
            }
            assertThat(progress.scan()).contains(added.get(0).position());
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:9");
        }
    }

    @Test
    void shouldReuseVerifiedEmptyDetachedRangesWithoutCachingTheAttachedReceiverOrInventingProgress() throws Exception {
        var source = new As400ContinuousRuntimeTest.Source(directory, new As400OperatorCutReader.Limits(128, 10000, 16_000_000));
        source.tail = 64;
        var current = new AtomicReference<As400OperatorCutReader>(source.reader);
        var pages = new java.util.concurrent.atomic.AtomicInteger();
        var publish = new java.util.concurrent.atomic.AtomicBoolean();
        var added = new ArrayList<RawJournalPage.Entry>();
        var reader = continuation(source, 1000, added, pages, publish);
        var records = new ArrayList<SourceRecord>();
        try (var output = source.output();
                var runtime = source.runtime(source.initial,
                        (offset, scan) -> current.get().readPrefix(offset, scan.orElse(null)), output, records::add)) {
            var settled = runtime.poll(source.initial);
            current.set(reader);
            records.clear();
            var first = runtime.poll(settled.checkpoint());
            assertThat(pages.get()).isGreaterThanOrEqualTo(1000);
            assertThat(first.scan()).isEqualTo(settled.scan());
            assertThat(first.checkpoint()).isEqualTo(settled.checkpoint());
            pages.set(0);
            var idle = runtime.poll(first.checkpoint());
            assertThat(pages.get()).isLessThanOrEqualTo(5);
            assertThat(idle.scan()).isEqualTo(first.scan());
            assertThat(idle.checkpoint()).isEqualTo(first.checkpoint());
            assertThat(records).isEmpty();
            publish.set(true);
            pages.set(0);
            var resumed = runtime.poll(idle.checkpoint());
            assertThat(pages.get()).isLessThanOrEqualTo(5);
            assertThat(resumed.scan()).contains(added.get(0).position());
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:9");
        }
    }

    private static As400OperatorJournalEvidence.Receiver receiver(JournalReceiver name, String status, long first, long last,
                                                                  JournalReceiver previous, JournalReceiver next) {
        return new As400OperatorJournalEvidence.Receiver(name, status, BigInteger.valueOf(first), BigInteger.valueOf(last),
                Optional.ofNullable(previous), Optional.ofNullable(next), "NO", "YES", "YES", "NO", "NO");
    }

    /** Real cut reader over retained anchors, empty receivers and a final row. */
    private As400OperatorCutReader continuation(As400ContinuousRuntimeTest.Source source, int empty, List<RawJournalPage.Entry> added) {
        return continuation(source, empty, added, new java.util.concurrent.atomic.AtomicInteger(), new java.util.concurrent.atomic.AtomicBoolean(true));
    }

    private As400OperatorCutReader continuation(As400ContinuousRuntimeTest.Source source, int empty, List<RawJournalPage.Entry> added,
                                                java.util.concurrent.atomic.AtomicInteger pages, java.util.concurrent.atomic.AtomicBoolean publish) {
        var names = new ArrayList<JournalReceiver>();
        for (int i = 0; i <= empty; i++) {
            names.add(new JournalReceiver(String.format("QSQJRN%04d", 4 + i), Fixture.RECEIVER.library()));
        }
        var receivers = new ArrayList<As400OperatorJournalEvidence.Receiver>();
        receivers.add(receiver(Fixture.RECEIVER, "ONLINE", 29, 64, null, names.get(0)));
        Map<Target, Identity> objects = new HashMap<>(source.fixture.nativeObjects);
        long next = 65;
        for (int i = 0; i < names.size(); i++) {
            boolean attached = i == names.size() - 1;
            // Each CHGJRN writes only journal-code J records (not part of the CDC D/R/C/F selection).
            receivers.add(receiver(names.get(i), attached ? "ATTACHED" : "ONLINE", next, next + 1, i == 0 ? Fixture.RECEIVER : names.get(i - 1),
                    attached ? null : names.get(i + 1)));
            objects.put(Fixture.target(names.get(i), Kind.RECEIVER), new Identity(BigInteger.ONE, BigInteger.valueOf(100 + i), BigInteger.ONE));
            if (attached) {
                // A real autocommit row (fixture R/PT A, sequence 56) lands in the attached receiver.
                byte[] raw = source.fixture.entry(56).data().clone();
                ByteBuffer.wrap(raw).putLong(24, next + 1);
                added.add(new RawJournalPage.Entry(new JournalPosition(BigInteger.valueOf(next + 1), names.get(i)), new EntryHeaderDecoder().decode(raw, 0), raw));
            }
            next += 2;
        }
        var observation = new As400OperatorJournalEvidence.Observation(Fixture.JOURNAL, names.get(names.size() - 1), receivers);
        var journals = mock(As400OperatorJournalEvidence.class);
        var identities = mock(As400JdbcNativeIdentityEvidence.class);
        var transport = mock(RetrieveJournal.class);
        try {
            when(journals.collect()).thenReturn(observation);
            when(journals.collectFrom(any())).thenReturn(observation);
            when(identities.collect(any())).thenAnswer(i -> {
                List<Target> targets = i.getArgument(0);
                return new Candidates(targets.stream().collect(Collectors.toMap(t -> t, objects::get)));
            });
            when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
                JournalPosition from = i.getArgument(0);
                JournalPosition through = i.getArgument(1);
                var all = new ArrayList<>(source.fixture.entries);
                pages.incrementAndGet();
                if (publish.get()) {
                    all.addAll(added);
                }
                var available = all.stream().filter(e -> e.position().receiver().equals(from.receiver())
                        && e.position().offset().compareTo(from.offset()) >= 0 && e.position().offset().compareTo(through.offset()) <= 0).toList();
                int count = Math.min((int) i.getArgument(2), available.size());
                return new RawJournalPage(available.subList(0, count), count < available.size() ? Optional.of(available.get(count).position()) : Optional.empty(), 65536);
            });
        }
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return new As400OperatorCutReader(journals, identities, Fixture.JOURNAL, source.history, transport, new As400OperatorCutReader.Limits(128, 10000, 16_000_000));
    }

}
