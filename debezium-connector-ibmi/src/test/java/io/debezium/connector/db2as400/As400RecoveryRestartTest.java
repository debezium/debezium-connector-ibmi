/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.SocketException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

class As400RecoveryRestartTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReopenFreshSessionFromDurablePositionsAfterConnectionLoss(boolean ordinaryOffsets) throws Exception {
        var initial = As400RecoveryTaskFixture.initialize(new Fixture(directory));
        var durable = new AtomicReference<Map<String, ?>>(initial);
        var disconnect = new AtomicBoolean();
        var disconnectDuringPollAdmission = new AtomicBoolean();
        Thread poller = Thread.currentThread();
        var first = new As400RecoveryTaskFixture(directory, initial.get("capture_fingerprint")) {
            @Override
            public void checkQualified(Configuration config) {
                if (Thread.currentThread() == poller && disconnectDuringPollAdmission.compareAndSet(true, false)) {
                    disconnect.set(true);
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(stopped::get);
                }
                super.checkQualified(config);
            }

            @Override
            public boolean supportsTaskRestart() {
                return true;
            }

            @Override
            public As400OperatorCutReader.Cut read(Map<String, ?> saved, Optional<RawJournalPage.Entry> scan) throws Exception {
                if (disconnect.get()) {
                    throw new SocketException("Controlled transport disconnected");
                }
                return super.read(saved, scan);
            }
        };
        first.sourceOffsets = ordinaryOffsets;
        var second = new As400RecoveryTaskFixture(directory, initial.get("capture_fingerprint"));
        second.sourceOffsets = ordinaryOffsets;
        second.tail = 64;
        var admissions = new AtomicInteger();
        var task = new As400ConnectorTask() {
            @Override
            As400TaskRecovery recoveryForTesting(Configuration config) {
                if (admissions.incrementAndGet() == 1) {
                    return first;
                }
                assertThat(first.stopped).isTrue();
                assertThat(first.closes).hasValue(1);
                assertThat(durable.get().get("completed_through.sequence")).isEqualTo("42");
                return second;
            }
        };
        var offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(any())).thenAnswer(i -> durable.get());
        when(offsets.offsets(any())).thenAnswer(i -> Map.of(first.partition().getSourcePartition(), durable.get()));
        var context = mock(SourceTaskContext.class);
        when(context.offsetStorageReader()).thenReturn(offsets);
        task.initialize(context);
        var properties = new HashMap<>(As400RecoveryTaskTest.properties());
        properties.put("retriable.restart.connector.wait.ms", "1");
        properties.put("errors.max.retries", "1");
        try {
            task.start(properties);
            var before = through(task, "42");
            acknowledge(task, durable, before);
            assertThat(As400RecoveryTestSupport.data(before)).containsExactly("c:DCOC1107.A:8");
            disconnectDuringPollAdmission.set(true);
            var after = through(task, "59");
            assertThat(admissions).hasValue(2);
            assertThat(second.recoveries).hasValue(1);
            assertThat(As400RecoveryTestSupport.data(after)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            acknowledge(task, durable, after);
        }
        finally {
            task.stop();
        }
        assertThat(second.closes).hasValue(1);
    }

    @Test
    void shouldRetainRetryBudgetAndRejectEvidenceErrorsEvenWithTransportCauses() throws Exception {
        var raw = Configuration.from(As400RecoveryTaskTest.properties()).edit().with("errors.max.retries", 1)
                .with("errors.retry.delay.initial.ms", 1).with("custom.retriable.exception", ".*").build();
        var config = new As400ConnectorConfig(raw);
        var firstQueue = queue();
        var first = new As400RecoveryErrorHandler(config, firstQueue, null, true);
        first.setProducerThrowable(new java.sql.SQLException("connection lost", "08006"));
        assertThatThrownBy(firstQueue::poll).isInstanceOf(RetriableException.class);
        var nextQueue = queue();
        var next = new As400RecoveryErrorHandler(config, nextQueue, first, true);
        next.setProducerThrowable(new SocketException("connection lost again"));
        assertThatThrownBy(nextQueue::poll).isExactlyInstanceOf(ConnectException.class);
        for (Throwable failure : List.of(new As400SourceNormalizer.NormalizationException("bad row", new SocketException()),
                new As400ReplayCoordinator.RecoveryException("bad checkpoint", new SocketException()),
                new IllegalArgumentException("missing history", new SocketException()), new java.io.IOException("corrupt local history"))) {
            var queue = queue();
            new As400RecoveryErrorHandler(config, queue, null, true).setProducerThrowable(failure);
            assertThatThrownBy(queue::poll).isExactlyInstanceOf(ConnectException.class);
        }
        var unverified = new SocketException("disconnected");
        unverified.addSuppressed(new IllegalStateException("close failed"));
        var queue = queue();
        new As400RecoveryErrorHandler(config, queue, null, true).setProducerThrowable(unverified);
        assertThatThrownBy(queue::poll).isExactlyInstanceOf(ConnectException.class);
        var strictQueue = queue();
        new As400RecoveryErrorHandler(config, strictQueue, null, false).setProducerThrowable(new SocketException());
        assertThatThrownBy(strictQueue::poll).isExactlyInstanceOf(ConnectException.class);
    }

    private static ChangeEventQueue<io.debezium.pipeline.DataChangeEvent> queue() {
        return new ChangeEventQueue.Builder<io.debezium.pipeline.DataChangeEvent>().maxBatchSize(1).maxQueueSize(8)
                .pollInterval(Duration.ofMillis(1)).loggingContextSupplier(() -> io.debezium.util.LoggingContext.forConnector("ibmi", "test", "restart")).build();
    }

    private static List<SourceRecord> through(As400ConnectorTask task, String completed) throws Exception {
        var records = new ArrayList<SourceRecord>();
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            List<SourceRecord> next;
            try {
                next = task.poll();
            }
            catch (RetriableException expected) {
                // Connect invokes poll again; BaseSourceTask owns backoff and
                // reconstructs the task's source rather than reusing its reader.
                continue;
            }
            if (next != null) {
                records.addAll(next);
                if (next.stream().anyMatch(record -> record.topic().startsWith("__debezium-heartbeat")
                        && completed.equals(record.sourceOffset().get("completed_through.sequence")))) {
                    return records;
                }
            }
        }
        throw new AssertionError("Task did not reach its durable output boundary");
    }

    private static void acknowledge(As400ConnectorTask task, AtomicReference<Map<String, ?>> durable, List<SourceRecord> records) throws Exception {
        for (var record : records) {
            task.commitRecord(record, As400RecoveryTaskTest.metadata(record));
        }
        durable.set(records.get(records.size() - 1).sourceOffset());
        task.commit();
    }
}
