/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

/** Local controlled I/O only. Actual task/coordinator/runtime/dispatcher are never replaced. */
class As400RecoveryTaskFixture implements As400TaskRecovery {
    final Fixture fixture;
    final String expectedFingerprint;
    final AtomicBoolean qualified = new AtomicBoolean(true);
    final AtomicBoolean stopped = new AtomicBoolean();
    final AtomicInteger recoveries = new AtomicInteger();
    final AtomicInteger reads = new AtomicInteger();
    final AtomicInteger closes = new AtomicInteger();
    final CountDownLatch held = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);
    volatile String hold = "";
    volatile long tail = 42;
    volatile String failure = "";
    private As400OperatorCutReader reader;

    As400RecoveryTaskFixture(Path directory, String expectedFingerprint) throws Exception {
        fixture = new Fixture(directory);
        this.expectedFingerprint = expectedFingerprint;
    }

    static Map<String, String> initialize(Fixture fixture) throws Exception {
        return fixture.history().recordInitializedBaseline(fixture.journalIdentity, Fixture.position(29), fixture.receiverIdentity,
                fixture.definitions, Fixture.POLICY, new As400BindingHistory.Initialization("local-epoch", "acknowledged-baseline")).toOffset();
    }

    @Override
    public As400Partition partition() {
        return new As400Partition("test");
    }

    @Override
    public String journalIdentity() {
        return fixture.journalIdentity;
    }

    @Override
    public String fingerprint() {
        return expectedFingerprint;
    }

    @Override
    public void checkQualified(Configuration config) {
        As400RecoveryAcknowledgements.requireProfile(config.asMap());
        if (!qualified.get() || stopped.get() || !"no-live-ibmi.invalid".equals(config.getString("database.hostname"))) {
            throw new IllegalStateException("Local qualified admission ended");
        }
    }

    @Override
    public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
        recoveries.incrementAndGet();
        gate("history");
        var history = recoverHistory(durable);
        var journals = mock(As400OperatorJournalEvidence.class);
        var identities = mock(As400JdbcNativeIdentityEvidence.class);
        var transport = mock(RetrieveJournal.class);
        when(journals.collect()).thenAnswer(i -> new As400OperatorJournalEvidence.Observation(Fixture.JOURNAL, Fixture.RECEIVER,
                List.of(new As400OperatorJournalEvidence.Receiver(Fixture.RECEIVER, "ATTACHED", BigInteger.valueOf(29), BigInteger.valueOf(tail),
                        Optional.empty(), Optional.empty(), "NO", "YES", "YES", "NO", "NO"))));
        when(identities.collect(any())).thenAnswer(i -> new Candidates(((List<Target>) i.getArgument(0)).stream()
                .collect(Collectors.toMap(t -> t, fixture.nativeObjects::get))));
        when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
            JournalPosition from = i.getArgument(0);
            JournalPosition through = i.getArgument(1);
            return new RawJournalPage(fixture.entries.stream().filter(e -> e.position().offset().compareTo(from.offset()) >= 0
                    && e.position().offset().compareTo(through.offset()) <= 0).toList(), Optional.empty(), 65536);
        });
        reader = new As400OperatorCutReader(journals, identities, Fixture.JOURNAL, history, transport, new As400OperatorCutReader.Limits(16, 100, 1_000_000));
        return history;
    }

    As400BindingHistory.Recovered recoverHistory(Map<String, ?> durable) throws Exception {
        return fixture.history().recover(durable, journalIdentity(), Fixture.POLICY);
    }

    @Override
    public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
        reads.incrementAndGet();
        if (failure.equals("read-interrupted")) {
            throw new InterruptedException("Injected unexpected reader interruption");
        }
        gate("read");
        return scan.isPresent() ? reader.readAfter(durable, scan.orElseThrow()) : reader.read(durable);
    }

    void gate(String stage) throws Exception {
        if (stage.equals(hold)) {
            held.countDown();
            boolean interrupted = false;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            try {
                while (release.getCount() != 0) {
                    if (System.nanoTime() >= deadline) {
                        throw new IllegalStateException("Test gate was not released");
                    }
                    try {
                        release.await(20, TimeUnit.MILLISECONDS);
                    }
                    catch (InterruptedException e) {
                        interrupted = true; // Model a late native call that ignores cancellation.
                    }
                }
            }
            finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public void requestStop() {
        stopped.set(true);
        if (failure.equals("cancel")) {
            throw new IllegalStateException("Injected cancellation failure");
        }
    }

    @Override
    public void close() throws Exception {
        gate("close");
        closes.incrementAndGet();
        if (failure.equals("close")) {
            throw new IllegalStateException("Injected cleanup failure");
        }
    }
}
