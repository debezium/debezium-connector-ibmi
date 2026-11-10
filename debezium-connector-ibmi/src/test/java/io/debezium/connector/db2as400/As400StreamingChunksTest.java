/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.source.TransactionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.connector.db2as400.As400RecoveryTaskTest.Running;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400StreamingChunksTest {
    @TempDir
    Path directory;

    @Test
    void largeNativeShapedBatchRetiresAcknowledgementsAndReplaysInterruptedPrefix() throws Exception {
        var baseline = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        Map<String, ?> saved;
        try (var run = start(new LargeBatch(directory, baseline.get("capture_fingerprint")), baseline)) {
            int clears = 0;
            int rows = 0;
            while (rows < 600) {
                var records = run.next();
                assertThat(retained(run)).isLessThanOrEqualTo(256);
                for (String event : As400RecoveryTestSupport.data(records)) {
                    if (event.equals("t:DCOC1107.B")) {
                        clears++;
                    }
                    if (event.startsWith("c:DCOC1107.B:")) {
                        rows++;
                    }
                }
                run.ack(records);
            }
            assertThat(clears).isEqualTo(1);
            saved = run.durable.get();
            assertThat(saved.get("completed_through.sequence")).isEqualTo("42");
            assertThat(saved.get("replay_from.sequence")).isEqualTo("31");
        }
        try (var run = start(new LargeBatch(directory, baseline.get("capture_fingerprint")), saved)) {
            int rows = 0;
            int clears = 0;
            boolean done = false;
            while (!done) {
                var records = run.next();
                assertThat(retained(run)).isLessThanOrEqualTo(256);
                for (String event : As400RecoveryTestSupport.data(records)) {
                    if (event.equals("t:DCOC1107.B")) {
                        clears++;
                    }
                    if (event.startsWith("c:DCOC1107.B:")) {
                        rows++;
                    }
                }
                done = records.stream().anyMatch(r -> "5043".equals(r.sourceOffset().get("completed_through.sequence")));
                run.ack(records);
            }
            assertThat(rows).isEqualTo(5000);
            assertThat(clears).isEqualTo(1);
            assertThat(run.durable.get().get("completed_through.sequence")).isEqualTo("5043");
        }
    }

    @Test
    void streamingPrefixCannotRetireOnCallbacksWithoutMatchingOffsetReadback() throws Exception {
        var baseline = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        var input = new LargeBatch(directory, baseline.get("capture_fingerprint"));
        try (var run = start(input, baseline)) {
            int count = 0;
            while (count < 256) {
                var records = run.next();
                count += records.size();
                run.producerAcks(records);
                run.task.commit(); // deliberately do not update the independent offset reader
            }
            As400RecoveryTaskTest.await(() -> input.stopped.get());
            assertThrows(Exception.class, run::next);
            assertThat(run.durable.get()).isEqualTo(baseline);
            assertThat(count).isEqualTo(256);
        }
    }

    @Test
    void transactionBoundaryRequestFailureCannotAdvanceDurableFw() throws Exception {
        var baseline = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        try (var run = new Running(directory, baseline, baseline)) {
            run.start();
            var context = mock(TransactionContext.class);
            doThrow(new IllegalStateException("worker transaction failure")).when(context).commitTransaction();
            boundary(run, context);
            assertThrows(IllegalStateException.class, run::next);
            verify(context).commitTransaction();
            assertThat(run.durable.get()).isEqualTo(baseline);
        }
    }

    @Test
    void actualTaskRequestsPublicFrameworkCommitForReturnedRecords() throws Exception {
        var baseline = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        try (var run = new Running(directory, baseline, baseline)) {
            run.start();
            var context = mock(TransactionContext.class);
            boundary(run, context);
            var records = run.next();
            assertThat(records).isNotEmpty();
            verify(context).commitTransaction();
            assertThat(run.durable.get()).isEqualTo(baseline);
        }
    }

    static void boundary(Running run, TransactionContext context) throws Exception {
        // Only the public framework boundary is controlled; task/dispatcher and
        // acknowledgement code are real. Packaged native runs cover admission.
        var field = As400ConnectorTask.class.getDeclaredField("configuredTransactions");
        field.setAccessible(true);
        field.set(run.task, context);
    }

    static int retained(Running run) throws Exception {
        var source = As400ConnectorTask.class.getDeclaredField("recoverySource");
        source.setAccessible(true);
        var trackerField = As400RecoverySource.class.getDeclaredField("acknowledgements");
        trackerField.setAccessible(true);
        var tracker = trackerField.get(source.get(run.task));
        var order = As400RecoveryAcknowledgements.class.getDeclaredField("order");
        order.setAccessible(true);
        synchronized (tracker) {
            return ((List<?>) order.get(tracker)).size();
        }
    }

    Running start(LargeBatch source, Map<String, ?> saved) {
        var run = new Running(source, saved);
        var p = new java.util.HashMap<>(As400RecoveryTaskTest.properties());
        p.put("max.batch.size", "128");
        p.put("max.queue.size", "1024");
        run.started = true;
        run.task.start(p);
        return run;
    }

    /** Synthetic size expansion of captured native entry bytes, not new native semantic evidence. */
    static class LargeBatch extends As400RecoveryTaskFixture {
        As400BindingHistory.Recovered history;
        final List<RawJournalPage.Entry> expanded = new ArrayList<>();

        LargeBatch(Path directory, String fingerprint) throws Exception {
            super(directory, fingerprint);
            expanded.addAll(fixture.entries.stream().filter(e -> e.position().offset().longValue() <= 42 && e.position().offset().longValue() != 40).toList());
            var row = fixture.entries.stream().filter(e -> e.position().offset().longValue() == 40).findFirst().orElseThrow();
            var commit = fixture.entries.stream().filter(e -> e.position().offset().longValue() == 43).findFirst().orElseThrow();
            for (int i = 0; i < 5000; i++) {
                expanded.add(expand(row, 43 + i, 100 + i));
            }
            expanded.add(expand(commit, 5043, -1));
        }

        static RawJournalPage.Entry expand(RawJournalPage.Entry original, int sequence, int id) {
            byte[] bytes = original.data();
            var buffer = ByteBuffer.wrap(bytes);
            buffer.putLong(24, sequence);
            if (id >= 0) {
                buffer.putLong(56, id);
                buffer.putInt(original.header().getEntrySpecificDataOffset() + 16, id);
            }
            return new RawJournalPage.Entry(Fixture.position(sequence), new EntryHeaderDecoder().decode(bytes, 0), bytes);
        }

        @Override
        public As400ContinuousRuntime.Limits runtimeLimits() {
            return new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(16, 10000, 128, 16_000_000),
                    new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), 20002);
        }

        @Override
        public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
            return history = super.recover(durable);
        }

        @Override
        public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) {
            reads.incrementAndGet();
            var cp = As400Checkpoint.fromOffset(durable, journalIdentity(), fingerprint());
            var order = new As400JournalOrder(List.of(Fixture.RECEIVER), Map.of(Fixture.RECEIVER, fixture.receiverIdentity),
                    Map.of(Fixture.RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(29), BigInteger.valueOf(5043))));
            var from = scan.map(RawJournalPage.Entry::position).orElse(cp.replayFrom());
            var entries = expanded.stream().filter(e -> e.position().offset().compareTo(from.offset()) > 0
                    || scan.isEmpty() && cp.replayInclusive() && e.position().equals(from)).toList();
            return new As400OperatorCutReader.Cut(cp, order, Fixture.position(5043), entries, List.of(), Optional.of(history));
        }
    }
}
