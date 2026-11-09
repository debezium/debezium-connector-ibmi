/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400RecoveryTaskTest.await;
import static io.debezium.connector.db2as400.As400RecoveryTaskTest.properties;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400InitializationTaskTest.Running;

class As400ChunkedInitializationTaskTest {
    @TempDir
    Path directory;

    Running running(As400ChunkedInitializationFixture fixture) throws Exception {
        var run = new Running(fixture, null, input -> input);
        var properties = new HashMap<>(properties());
        properties.put("max.batch.size", "128");
        properties.put("max.queue.size", "1024");
        run.started = true;
        run.task.start(properties);
        return run;
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 9000, 9001, 12000, 30000 })
    void completeSnapshotBeyondOldCeilingRetiresEachChunkAndKeepsNormalHandoff(int count) throws Exception {
        var fixture = new As400ChunkedInitializationFixture(directory, count);
        try (var run = running(fixture)) {
            int rows = 0;
            long chunks = 0;
            Map<String, Integer> keys = new HashMap<>();
            while (true) {
                List<SourceRecord> records = run.next();
                assertThat(retained(run)).isLessThanOrEqualTo(257);
                for (var record : records) {
                    assertThat(record.sourceOffset().get("initialization.version")).isEqualTo("chunks-1");
                    if (record.value() instanceof Struct row) {
                        assertThat(row.getString("op")).isEqualTo("r");
                        int id = keys.merge(record.topic(), 1, Integer::sum);
                        assertThat(row.getStruct("after").getInt32("ID")).isEqualTo(id);
                        assertThat(((Struct) record.key()).getInt32("ID")).isEqualTo(id);
                        rows++;
                    }
                    else if ("CHUNK".equals(record.value())) {
                        chunks++;
                        assertThat(record.sourceOffset().get("initialization.chunk")).isEqualTo(Long.toString(chunks));
                    }
                }
                if (records.stream().anyMatch(r -> "BASELINE".equals(r.value()))) {
                    assertThat(rows).isEqualTo(count);
                    assertThat(fixture.delivered).isEqualTo(count);
                    assertThat(records.get(records.size() - 1).sourceOffset().get("initialization.rows")).isEqualTo(Integer.toString(count));
                    assertThat(run.durable.get() == null || !run.durable.get().containsKey("offset.version")).isTrue();
                    run.ack(records);
                    break;
                }
                run.ack(records);
            }
            var initial = run.through("INITIAL");
            assertThat(initial.get(initial.size() - 1).sourceOffset().get("offset.version")).isEqualTo("2");
            run.ack(initial);
            run.ack(run.throughCheckpoint("42"));
            assertThat(fixture.handoffs.get()).isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-eof", "duplicate-eof", "after-chunk", "budget" })
    void incompleteScanCannotInstallHistoryOrFwAndFreshInstanceRejects(String fault) throws Exception {
        var fixture = new As400ChunkedInitializationFixture(directory, 600);
        fixture.chunkFault = fault;
        if (fault.equals("budget")) {
            fixture.limits = new As400SnapshotChunks.Limits(256, 1_000_000, 256, 100000);
        }
        Map<String, ?> saved;
        try (var run = running(fixture)) {
            run.ack(run.through("CHUNK"));
            assertThrows(Exception.class, () -> {
                while (true) {
                    run.ack(run.next());
                }
            });
            saved = run.durable.get();
            assertThat(saved.get("initialization.version")).isEqualTo("chunks-1");
            assertThat(saved).doesNotContainKey("offset.version");
            assertThat(Files.exists(fixture.setup.fixture.file)).isFalse();
            assertThat(fixture.handoffs.get()).isZero();
        }
        try (var replacement = new Running(directory, saved)) {
            assertThrows(Exception.class, replacement::start);
            assertThat(replacement.durable.get()).isEqualTo(saved);
        }
        try (var missing = new Running(directory, null)) {
            missing.start();
            assertThrows(Exception.class, missing::next);
            assertThat(missing.durable.get()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "no-commit", "stale-offset", "lost-session" })
    void chunkIsNotRetiredWithoutCurrentDurabilityAndSession(String fault) throws Exception {
        var fixture = new As400ChunkedInitializationFixture(directory, 600);
        try (var run = running(fixture)) {
            var first = run.through("CHUNK");
            run.producerAcks(first);
            if (fault.equals("no-commit")) {
                assertThat(fixture.delivered).isZero();
                assertThat(retained(run)).isEqualTo(256); // 255 published rows plus barrier; one final row held locally.
                run.commit(first);
                await(() -> fixture.delivered >= 256);
            }
            else if (fault.equals("stale-offset")) {
                run.durable.set(first.get(0).sourceOffset());
                run.task.commit();
                assertThrows(Exception.class, run::next);
                assertThat(fixture.delivered).isZero();
            }
            else {
                fixture.heldSession.set(false);
                assertThrows(Exception.class, () -> run.commit(first));
                assertThat(fixture.delivered).isZero();
            }
            assertThat(fixture.handoffs.get()).isZero();
        }
    }

    @Test
    void chunkRecordCallbacksStageLocallyButCommitStillValidatesNativeSession() throws Exception {
        var forbidProbe = new java.util.concurrent.atomic.AtomicBoolean();
        Thread callbacks = Thread.currentThread();
        var fixture = new As400ChunkedInitializationFixture(directory, 600) {
            @Override
            public String checkHeld() {
                if (forbidProbe.get() && Thread.currentThread() == callbacks) {
                    throw new AssertionError("Per-record callback made a native probe");
                }
                return super.checkHeld();
            }
        };
        try (var run = running(fixture)) {
            var first = run.through("CHUNK");
            forbidProbe.set(true);
            run.producerAcks(first);
            forbidProbe.set(false);
            assertThat(fixture.delivered).isZero();
            fixture.heldSession.set(false);
            assertThrows(Exception.class, () -> run.commit(first));
            assertThat(fixture.delivered).isZero();
        }
    }

    @Test
    void previousChunkOffsetCannotAcknowledgeTheNextChunk() throws Exception {
        var fixture = new As400ChunkedInitializationFixture(directory, 600);
        try (var run = running(fixture)) {
            run.ack(run.through("CHUNK"));
            var previous = run.durable.get();
            var next = run.through("CHUNK");
            assertThat(next.get(next.size() - 1).sourceOffset()).isNotEqualTo(previous);
            run.producerAcks(next);
            run.task.commit(); // Deliberately retain the previous exact durable barrier.
            assertThrows(Exception.class, run::next);
            assertThat(run.durable.get()).isEqualTo(previous);
            assertThat(fixture.delivered).isEqualTo(256);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "type", "null-key", "projection" })
    void invalidOwnedRowsRejectBeforeExposingAnyRecordInTheChunk(String fault) throws Exception {
        var fixture = new As400ChunkedInitializationFixture(directory, 600);
        fixture.rowFault = fault;
        try (var run = running(fixture)) {
            assertThrows(Exception.class, run::next);
            assertThat(run.durable.get()).isNull();
            assertThat(fixture.delivered).isZero();
        }
    }

    @Test
    void lateAcknowledgementAfterStopCannotResumeAChunk() throws Exception {
        var fixture = new As400ChunkedInitializationFixture(directory, 600);
        try (var run = running(fixture)) {
            var first = run.through("CHUNK");
            run.task.stop();
            assertThrows(Exception.class, () -> run.producerAcks(first));
            assertThat(fixture.delivered).isZero();
            assertThat(run.durable.get()).isNull();
        }
    }

    static int retained(Running run) throws Exception {
        var sourceField = As400ConnectorTask.class.getDeclaredField("recoverySource");
        sourceField.setAccessible(true);
        var trackerField = As400RecoverySource.class.getDeclaredField("acknowledgements");
        trackerField.setAccessible(true);
        var tracker = trackerField.get(sourceField.get(run.task));
        var orderField = As400RecoveryAcknowledgements.class.getDeclaredField("order");
        orderField.setAccessible(true);
        synchronized (tracker) {
            return ((List<?>) orderField.get(tracker)).size();
        }
    }
}
