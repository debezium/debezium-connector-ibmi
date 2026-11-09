/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400ConnectProbeConnector.FINGERPRINT;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.IDENTITY;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.JID;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.ORDER;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.PARTITION;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.events;
import static io.debezium.connector.db2as400.As400ConnectProbeConnector.pos;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.connector.db2as400.As400ConnectProbeConnector.Event;
import io.debezium.connector.db2as400.As400ReaderFixture.Boundary;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.relational.TableId;

/**
 * Terminal stop rejects subsequent stages and drains admitted calls. Transport
 * and the prefix-acknowledgement store are models; Kafka is tested separately.
 */
class As400ReaderStartupStopTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<String, ?> INITIAL = As400Checkpoint.afterVerifiedBoundary(IDENTITY, FINGERPRINT, pos(1), "synthetic-receiver").toOffset();

    static Stream<Arguments> boundaries() {
        return Stream.of(Boundary.values()).flatMap(point -> Stream.of(false, true)
                .flatMap(secure -> Stream.of(false, true).map(error -> Arguments.of(point, secure, error))));
    }

    @ParameterizedTest(name = "{0}, secure={1}, lateError={2}")
    @MethodSource("boundaries")
    void terminalStopDrainsAdmittedWorkWithoutChangingReplacementOrDurableFw(Boundary point, boolean secure, boolean lateError) throws Exception {
        Map<String, ?> saved;
        // Save a real coordinator checkpoint while a truncate transaction is open.
        // Restarted readers must include F=2 even though durable W is still 1.
        try (Pipeline seed = new Pipeline("seed", secure, INITIAL)) {
            for (Event event : events().subList(0, 4)) {
                seed.read(event, () -> true, () -> null);
            }
            seed.replay.publishCheckpoint(record("checkpoint"));
            seed.sink.acknowledge();
            saved = roundTrip(seed.sink.durable);
        }
        assertThat(saved.get("replay_from.sequence")).isEqualTo("2");
        assertThat(saved.get("replay_from.inclusive")).isEqualTo("true");
        assertThat(saved.get("completed_through.sequence")).isEqualTo("1");

        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("boundary", point.name());
        evidence.put("secure", secure);
        evidence.put("late_error", lateError);
        evidence.put("durability_oracle", "local prefix-acknowledgement model; not Kafka");
        evidence.put("native_quiescence", "local admission/drain only; native behavior unverified");
        evidence.put("saved_fw", saved);
        try (Pipeline old = new Pipeline("old", secure, saved);
                Pipeline replacement = new Pipeline("replacement", secure, roundTrip(saved))) {
            evidence.put("old_operations", old.reader.operations);
            evidence.put("replacement_operations", replacement.reader.operations);
            boolean cold = point == Boundary.RECEIVER_LIST;
            if (!cold) {
                // Restore the open transaction before pausing the commit's read.
                for (Event event : events().subList(0, 4)) {
                    old.read(event, () -> true, () -> null);
                }
            }
            var coordinatorScanBefore = old.replay.scanPosition();
            BigInteger rpcScanBefore = old.reader.scan.getPosition().getOffset();
            int consumersBefore = old.consumed.get();
            AtomicBoolean running = new AtomicBoolean(true);
            CountDownLatch reached = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<Map<String, Boolean>> stateAtGate = new AtomicReference<>();
            old.reader.boundaryHook = at -> {
                if (at == point) {
                    stateAtGate.set(old.reader.activeReadState());
                    reached.countDown();
                    await(release);
                }
            };
            CountDownLatch closed = new CountDownLatch(1);
            doAnswer(invocation -> {
                invocation.callRealMethod();
                if (old.reader.lifecycle.isStopped()) {
                    closed.countDown();
                }
                return null;
            }).when(old.reader.rpc).close();
            AtomicReference<Thread> stopThread = new AtomicReference<>();
            var executor = executor(3);
            try {
                var pending = executor.submit(() -> {
                    old.read(events().get(cold ? 0 : 4), running::get, () -> {
                        if (lateError) {
                            throw new java.io.IOException("controlled post-close transport error");
                        }
                        return null;
                    });
                    return null;
                });
                await(reached);
                boolean registered = point == Boundary.REGISTERED_RUN || point == Boundary.REGISTERED_ADMISSION;
                assertThat(stateAtGate.get()).containsEntry("registered", registered)
                        .containsEntry("cancelled", false);
                running.set(false);
                var stopping = executor.submit(() -> {
                    stopThread.set(Thread.currentThread());
                    old.reader.stop();
                    return Thread.currentThread().isInterrupted();
                });
                await(closed);
                assertThat(old.reader.lifecycle.isStopped()).isTrue();
                assertThat(stopping.isDone()).isFalse();
                stopThread.get().interrupt();
                // A second stop cannot turn a pending drain into successful stop.
                var repeatedStop = executor.submit(() -> {
                    old.reader.stop();
                    return null;
                });
                int afterClose = old.reader.operations.size();
                evidence.put("state_at_gate", stateAtGate.get());
                evidence.put("state_after_close", old.reader.activeReadState());
                evidence.put("old_rpc_scan_before", rpcScanBefore.toString());
                int cancelled = registered ? 1 : 0;
                assertThat(actions(old.reader.operations, "job-end")).isEqualTo(cancelled);
                assertThat(old.reader.activeReadState()).containsEntry("registered", registered)
                        .containsEntry("cancelled", registered);
                assertThat(old.sink.durable).isEqualTo(saved);
                assertThat(old.sink.records).isEmpty();

                // An independent reader reconstructs from the saved F, then its
                // prefix store accepts W=6 while the old reader is still stopped.
                for (Event event : events().subList(0, 5)) {
                    replacement.read(event, () -> true, () -> null);
                }
                replacement.sink.acknowledge();
                Map<String, ?> accepted = roundTrip(replacement.sink.durable);
                assertThat(accepted.get("completed_through.sequence")).isEqualTo("6");
                assertThat(replacement.sink.projection).containsExactlyInAnyOrder(1, 2);
                evidence.put("replacement_accepted_before_late", accepted);
                var replacementCheckpoint = replacement.replay.checkpoint();
                List<Map<String, Object>> replacementActions = List.copyOf(replacement.reader.operations);
                int replacementRecords = replacement.sink.records.size();

                release.countDown();
                var error = assertThrows(ExecutionException.class, () -> pending.get(10, TimeUnit.SECONDS));
                assertThat(error).hasStackTraceContaining(point == Boundary.REGISTERED_RUN && lateError
                        ? "controlled post-close transport error"
                        : "Journal reader is terminally stopped");
                assertThat(stopping.get(10, TimeUnit.SECONDS)).isTrue();
                repeatedStop.get(10, TimeUnit.SECONDS);

                List<Map<String, Object>> continued = old.reader.operations.subList(afterClose, old.reader.operations.size());
                // A run already entered before stop may finish during the drain.
                // Earlier stages must never reach run, including exact admission.
                assertThat(actions(continued, "read-enter")).isEqualTo(point == Boundary.REGISTERED_RUN ? 1 : 0);
                long newRunInvocations = continued.stream().filter(e -> e.get("action").equals("boundary")
                        && ((Map<?, ?>) e.get("fields")).get("point").equals(Boundary.REGISTERED_RUN.name())).count();
                assertThat(newRunInvocations).isZero();
                evidence.put("new_run_invocations_after_stop_request", newRunInvocations);
                assertThat(actions(continued, "connect")).isZero();
                assertThat(actions(continued, "job-logging")).isZero();
                assertThat(actions(continued, "receiver-metadata")).isZero();
                assertThat(old.reader.connectionCount()).isEqualTo(1);
                assertThat(old.reader.nativeScope(() -> old.reader.rpc.isValid())).isFalse();
                assertThat(old.reader.allReaderTransportsDisconnected()).isTrue();
                int afterStop = old.reader.operations.size();
                assertThrows(java.io.IOException.class, () -> old.reader.reconnect());
                assertThrows(java.io.IOException.class, () -> old.read(events().get(4), () -> true, () -> null));
                assertThat(old.reader.operations).hasSize(afterStop);
                evidence.put("post_stop_operations", List.copyOf(old.reader.operations.subList(afterStop, old.reader.operations.size())));
                assertThat(old.reader.activeReadState()).containsEntry("registered", false).containsEntry("cancelled", false);
                assertThat(old.consumed).hasValue(consumersBefore);
                assertThat(old.replay.scanPosition()).isEqualTo(coordinatorScanBefore);
                assertThat(old.replay.checkpoint().completedThrough()).isEqualTo(pos(1));
                assertThat(old.sink.records).isEmpty();
                assertThat(old.sink.durable).isEqualTo(saved);
                assertThat(old.reader.scan.getPosition().getOffset()).isEqualTo(rpcScanBefore);
                assertThat(old.reader.scan.getPosition().processed()).isEqualTo(!cold);
                assertThat(replacement.replay.checkpoint()).isEqualTo(replacementCheckpoint);
                assertThat(replacement.sink.durable).isEqualTo(accepted);
                assertThat(replacement.sink.records).hasSize(replacementRecords);
                assertThat(replacement.reader.operations).isEqualTo(replacementActions);
                evidence.put("post_close_operations", List.copyOf(continued));
                evidence.put("old_rpc_scan_after", old.reader.scan.getPosition().getOffset().toString());
                evidence.put("old_coordinator_scan_after", old.replay.scanPosition().map(p -> p.offset().toString()).orElse("none"));
                evidence.put("old_fw_after", roundTrip(old.sink.durable));
                evidence.put("old_records_after", old.sink.records.size());
                evidence.put("replacement_accepted_after_late", roundTrip(replacement.sink.durable));

                // Late old work does not stall subsequent replacement progress.
                for (Event event : events().subList(5, 8)) {
                    replacement.read(event, () -> true, () -> null);
                }
                replacement.replay.verifyEndOfAvailableJournal();
                replacement.sink.acknowledge();
                assertThat(replacement.sink.durable.get("completed_through.sequence")).isEqualTo("9");
                assertThat(replacement.sink.projection).containsExactlyInAnyOrder(1, 2, 3);
                evidence.put("replacement_final_fw", roundTrip(replacement.sink.durable));
                evidence.put("replacement_final_projection", Set.copyOf(replacement.sink.projection));
                assertThat(replacement.sink.records).allSatisfy(r -> assertThat(r.sourceOffset()).doesNotContainKey("scan"));
                assertThat(actions(replacement.reader.operations, "job-end")).isZero();
                assertThat(actions(old.reader.operations, "undeclared-action")).isZero();
                assertThat(actions(replacement.reader.operations, "undeclared-action")).isZero();
            }
            finally {
                release.countDown();
                executor.shutdownNow();
                assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
                // Safety cleanup even if an assertion failed; stop must have
                // already drained and disconnected in successful cases.
                old.reader.close();
                old.reader.boundaryHook = at -> {
                };
            }
            assertThat(old.reader.nativeScope(() -> old.reader.rpc.isValid())).isFalse();
        }
        // Optional external evidence destination; ordinary unit runs write no files.
        String destination = System.getProperty("dcg.startup.stop.evidence");
        if (destination != null) {
            Path directory = Path.of(destination);
            Files.createDirectories(directory);
            evidence.put("final_cleanup", "completed after reader thread termination");
            JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve(point.name().toLowerCase(java.util.Locale.ROOT)
                    + "-secure-" + secure + "-error-" + lateError + ".json").toFile(), evidence);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void taskStopRejectsFirstReadAndReconnectWithoutNativeWork(boolean secure) throws Exception {
        As400ReaderFixture reader = new As400ReaderFixture("never-started", As400ReaderFixture.config(secure), 1, e -> {
        });
        As400ConnectorTask task = new As400ConnectorTask();
        As400ReaderFixture.set(task, "rpcConnection", reader.rpc);
        reader.nativeScope(() -> {
            task.doStop();
            task.doStop();
            return null;
        });
        assertThrows(java.io.IOException.class, reader::reconnect);
        assertThrows(java.io.IOException.class, () -> reader.read(events().get(0), () -> true, (s, j, h) -> {
            throw new AssertionError("Stopped reader delivered a record");
        }, () -> null));
        assertThat(reader.operations).isEmpty();
        assertThat(reader.scan.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failedSetupReleasesAdmissionForTerminalStop(boolean secure) throws Exception {
        As400ReaderFixture reader = new As400ReaderFixture("failed-setup", As400ReaderFixture.config(secure), 1, e -> {
        });
        reader.boundaryHook = point -> {
            if (point == Boundary.SERVER_JOB) {
                throw new java.io.IOException("controlled setup failure");
            }
        };
        assertThat(assertThrows(RuntimeException.class, () -> reader.read(events().get(0), () -> true, (s, j, h) -> {
            throw new AssertionError("Failed reader delivered a record");
        }, () -> null))).hasStackTraceContaining("controlled setup failure");
        // Run stop on a bounded test executor: a leaked admission must fail the test.
        var executor = executor(1);
        try {
            executor.submit(() -> {
                reader.stop();
                return null;
            }).get(10, TimeUnit.SECONDS);
            assertThat(reader.activeReadState()).containsEntry("registered", false);
            assertThat(actions(reader.operations, "job-end")).isZero();
            assertThat(reader.nativeScope(() -> reader.rpc.isValid())).isFalse();
        }
        finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void stopDrainsAndCleansCapturedTransportAlreadyDetachedByClose(boolean secure) throws Exception {
        As400ReaderFixture reader = new As400ReaderFixture("detached", As400ReaderFixture.config(secure), 1, e -> {
        });
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        reader.boundaryHook = point -> {
            if (point == Boundary.REGISTERED_RUN) {
                entered.countDown();
                await(release);
            }
        };
        reader.reconnectInsideAdmittedRun = true;
        var executor = executor(2);
        try {
            var read = executor.submit(() -> {
                reader.read(events().get(0), () -> false, (s, j, h) -> {
                    throw new AssertionError("Stopped reader delivered a record");
                }, () -> {
                    throw new AssertionError("controlled driver Error after reconnect");
                });
                return null;
            });
            await(entered);
            reader.close(); // Detach the connection while the admitted run is held.
            doAnswer(invocation -> {
                invocation.callRealMethod();
                closed.countDown();
                return null;
            }).when(reader.rpc).close();
            var stopped = executor.submit(() -> {
                reader.stop();
                return null;
            });
            await(closed);
            assertThat(stopped.isDone()).isFalse();
            release.countDown();
            assertThat(assertThrows(ExecutionException.class, () -> read.get(10, TimeUnit.SECONDS)))
                    .hasCauseInstanceOf(AssertionError.class).hasStackTraceContaining("controlled driver Error after reconnect");
            stopped.get(10, TimeUnit.SECONDS);
            assertThat(reader.connectionCount()).isEqualTo(2);
            assertThat(reader.allReaderTransportsDisconnected()).isTrue();
            assertThat(reader.activeReadState()).containsEntry("registered", false);
            assertThat(actions(reader.operations, "job-end")).isEqualTo(1);
            assertThat(reader.scan.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
            assertThrows(java.io.IOException.class, reader::reconnect);
            assertThat(reader.connectionCount()).isEqualTo(2);
        }
        finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            reader.close();
        }
    }

    static Stream<Arguments> metadataOperations() {
        return Stream.of(false, true).flatMap(secure -> Stream.of(false, true)
                .flatMap(position -> Stream.of(false, true).map(error -> Arguments.of(secure, position, error))));
    }

    @ParameterizedTest(name = "metadata secure={0}, currentPosition={1}, error={2}")
    @MethodSource("metadataOperations")
    void terminalStopCleansDetachedMetadataTransport(boolean secure, boolean currentPosition, boolean error) throws Exception {
        As400ConnectorConfig config = As400ReaderFixture.config(secure);
        As400ReaderFixture reader = new As400ReaderFixture("metadata", config, 1, e -> {
        });
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch closed = new CountDownLatch(1);
        reader.duringMetadata = connection -> {
            entered.countDown();
            await(release);
            connection.connectService(com.ibm.as400.access.AS400.COMMAND);
            if (error) {
                throw new java.io.IOException("controlled metadata error after reconnect");
            }
        };
        var executor = executor(2);
        try {
            var lookup = executor.submit(() -> reader.nativeScope(() -> currentPosition
                    ? reader.rpc.getCurrentPosition()
                    : reader.rpc.validateLogPosition(null, reader.scan, config)));
            await(entered);
            reader.close();
            // Cover both null and replacement RPC-field captures at stop.
            if (error) {
                reader.reconnect();
            }
            doAnswer(invocation -> {
                invocation.callRealMethod();
                closed.countDown();
                return null;
            }).when(reader.rpc).close();
            var stopped = executor.submit(() -> {
                reader.stop();
                return null;
            });
            await(closed);
            assertThat(stopped.isDone()).isFalse();
            release.countDown();
            if (currentPosition) {
                assertThat(assertThrows(ExecutionException.class, () -> lookup.get(10, TimeUnit.SECONDS)))
                        .hasStackTraceContaining(error ? "controlled metadata error after reconnect" : "Journal reader is terminally stopped");
            }
            else {
                assertThat(lookup.get(10, TimeUnit.SECONDS)).isEqualTo(false);
            }
            stopped.get(10, TimeUnit.SECONDS);
            assertThat(reader.connectionCount()).isEqualTo(error ? 3 : 2);
            assertThat(reader.allReaderTransportsDisconnected()).isTrue();
            assertThat(reader.activeReadState()).containsEntry("registered", false);
            assertThat(actions(reader.operations, "job-end")).isZero();
            assertThat(reader.scan.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
            int afterStop = reader.operations.size();
            reader.nativeScope(() -> {
                assertThrows(As400RpcConnection.RpcException.class, () -> reader.rpc.getCurrentPosition());
                assertThat(reader.rpc.validateLogPosition(null, reader.scan, config)).isFalse();
                return null;
            });
            assertThrows(java.io.IOException.class, reader::reconnect);
            assertThat(reader.operations).hasSize(afterStop);
        }
        finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            reader.close();
        }
    }

    private static java.util.concurrent.ExecutorService executor(int threads) {
        return Executors.newFixedThreadPool(threads, action -> {
            Thread thread = new Thread(action, "terminal-stop-test");
            thread.setDaemon(true); // A regression must not hang the entire test JVM.
            return thread;
        });
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("Startup/stop test deadline expired");
        }
    }

    private static long actions(List<Map<String, Object>> operations, String action) {
        return operations.stream().filter(e -> e.get("action").equals(action)).count();
    }

    private static Map<String, ?> roundTrip(Map<String, ?> checkpoint) throws Exception {
        return JSON.readValue(JSON.writeValueAsBytes(checkpoint), Map.class);
    }

    private static SourceRecord record(String value) {
        // Coordinator must replace this deliberately unsafe offset on every output.
        return new SourceRecord(PARTITION, Map.of("scan", "999"), "fixture", null, null, Schema.STRING_SCHEMA, value);
    }

    private static final class Pipeline implements AutoCloseable {
        private final As400ReaderFixture reader;
        private final As400ReplayCoordinator replay;
        private final PrefixStore sink;
        private final AtomicInteger consumed = new AtomicInteger();

        private Pipeline(String owner, boolean secure, Map<String, ?> saved) throws Exception {
            sink = new PrefixStore(saved);
            As400Checkpoint checkpoint = As400Checkpoint.fromOffset(saved, IDENTITY, FINGERPRINT);
            reader = new As400ReaderFixture(owner, As400ReaderFixture.config(secure), checkpoint.replayFrom().offset().longValue(), e -> {
            });
            reader.scan.getPosition().setProcessed(!checkpoint.replayInclusive());
            var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
            var table = new TableIdentity(new TableId("DB", "LIB", "TEST"), "TEST", "TEST", JID, As400ConnectProbeConnector.MEMBER_JID, "FORMAT_V1");
            replay = new As400ReplayCoordinator(saved, IDENTITY, FINGERPRINT, ORDER, new Limits(4, 20, 10, 4096), 1024, 20,
                    (file, library) -> new As400SourceNormalizer.TableBinding(table, decoder),
                    (source, control) -> SavepointRollback.UNRESOLVED, PARTITION, sink::enqueue);
        }

        private void read(Event event, java.util.function.BooleanSupplier running, java.util.concurrent.Callable<Void> transport) throws Exception {
            reader.read(event, running, (sequence, journal, header) -> {
                consumed.incrementAndGet();
                try {
                    journal.decode((h, data, offset) -> {
                        if (replay.accept(h, data, offset, pos(sequence.longValue())).isPresent()) {
                            replay.deliver((batch, records) -> {
                                for (var change : batch.changes()) {
                                    switch (change.operation()) {
                                        case TRUNCATE -> records.accept(record("t"));
                                        case CREATE -> records.accept(record("c:" + change.after().values()[0]));
                                        default -> throw new AssertionError("Unexpected fixture change");
                                    }
                                }
                            });
                            replay.publishCheckpoint(record("checkpoint"));
                        }
                        return null;
                    });
                }
                catch (Exception e) {
                    throw new As400RpcConnection.RpcException("Startup/stop fixture", e);
                }
            }, transport);
        }

        @Override
        public void close() throws Exception {
            reader.close();
            replay.close();
        }
    }

    /** Prefix acknowledgement MODEL; it does not prove Kafka publication/fencing. */
    private static final class PrefixStore {
        private final List<SourceRecord> records = new ArrayList<>();
        private final Set<Integer> projection = new HashSet<>(Set.of(99));
        private Map<String, ?> durable;
        private int acknowledged;

        private PrefixStore(Map<String, ?> checkpoint) {
            durable = Map.copyOf(checkpoint);
        }

        private boolean enqueue(SourceRecord record) {
            records.add(record);
            return true;
        }

        private void acknowledge() {
            while (acknowledged < records.size()) {
                SourceRecord record = records.get(acknowledged++);
                String value = record.value().toString();
                if (value.equals("t")) {
                    projection.clear();
                }
                else if (value.startsWith("c:")) {
                    projection.add(Integer.parseInt(value.substring(2)));
                }
                durable = Map.copyOf(record.sourceOffset());
            }
        }
    }
}
