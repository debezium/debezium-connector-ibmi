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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.relational.TableId;

class As400ReaderLifecycleTest {
    private static final Map<String, ?> INITIAL = As400Checkpoint.afterVerifiedBoundary(IDENTITY, FINGERPRINT, pos(1), "synthetic-receiver").toOffset();

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void realIncrementalRpcPreservesOwnedRowsMetadataAndDurableFloor(boolean secure) throws Exception {
        As400ReaderFixture reader = fixture("reader", secure);
        List<CommittedBatch> batches = new ArrayList<>();
        try (As400ReplayCoordinator replay = replay(INITIAL)) {
            for (var event : events()) {
                reader.read(event, () -> true, (sequence, journal, header) -> {
                    try {
                        journal.decode((decoded, data, offset) -> {
                            assertThat(decoded.getRecordInfo()).isEqualTo(event.header().getRecordInfo());
                            assertThat(decoded.getTime()).isEqualTo(event.header().getTime());
                            assertThat(offset).isEqualTo(64);
                            if (replay.accept(decoded, data, offset, pos(sequence.longValue())).isPresent()) {
                                replay.deliver((batch, records) -> batches.add(batch));
                            }
                            // The next native call may reuse/overwrite its buffer. Owned row values must survive.
                            java.util.Arrays.fill(data, (byte) 0);
                            return null;
                        });
                    }
                    catch (Exception e) {
                        throw new As400RpcConnection.RpcException("fixture decode", e);
                    }
                }, () -> null);
                assertThat(reader.scan.getPosition().getOffset()).isEqualTo(event.position().offset());
                assertThat(INITIAL.get("completed_through.sequence")).isEqualTo("1");
            }
            assertThat(replay.scanPosition()).contains(pos(9));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(9)); // enqueue progress, not durable
            assertThat(batches).hasSize(2);
            assertThat(batches.get(0).changes()).hasSize(3);
            assertThat(batches.get(0).changes().get(1).after().values()).containsExactly(1);
            assertThat(batches.get(0).changes().get(2).after().values()).containsExactly(2);
            assertThat(batches.get(1).changes().get(0).after().values()).containsExactly(3);
            assertThat(batches.get(0).changes().get(0).source().position()).isEqualTo(pos(3));
        }
        reader.reconnect();
        reader.close();
        reader.close();
        assertThat(reader.connectionCount()).isEqualTo(2);
        assertThat(actions(reader, "undeclared-action")).isZero();
        assertThat(actions(reader, "job-logging")).isEqualTo(8);
        try (As400ReplayCoordinator restarted = replay(INITIAL)) {
            assertThat(restarted.checkpoint().replayFrom()).isEqualTo(pos(1));
            assertThat(restarted.checkpoint().completedThrough()).isEqualTo(pos(1));
        }
    }

    @Test
    void freshReaderRestartsAtInclusiveDurableFloorAfterInterruptedBatchDelivery() throws Exception {
        List<org.apache.kafka.connect.source.SourceRecord> accepted = new ArrayList<>();
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
        var table = new TableIdentity(new TableId("DB", "LIB", "TEST"), "TEST", "TEST", JID, As400ConnectProbeConnector.MEMBER_JID, "FORMAT_V1");
        As400ReaderFixture first = fixture("interrupted", false);
        try (var replay = new As400ReplayCoordinator(INITIAL, IDENTITY, FINGERPRINT, ORDER, new Limits(4, 20, 10, 4096), 1024, 20,
                (file, library) -> new As400SourceNormalizer.TableBinding(table, decoder), (source, control) -> SavepointRollback.UNRESOLVED,
                PARTITION, record -> {
                    if (!accepted.isEmpty()) {
                        return false; // interrupt delivery after the first accepted record
                    }
                    accepted.add(record);
                    return true;
                })) {
            for (var event : events().subList(0, 5)) {
                java.util.concurrent.Callable<Void> read = () -> {
                    first.read(event, () -> true, (sequence, journal, header) -> {
                        try {
                            journal.decode((h, data, offset) -> {
                                if (replay.accept(h, data, offset, pos(sequence.longValue())).isPresent()) {
                                    replay.deliver((batch, records) -> {
                                        for (String value : List.of("t", "c:1", "c:2")) {
                                            records.accept(new org.apache.kafka.connect.source.SourceRecord(PARTITION, Map.of("scan", "999"),
                                                    "fixture", null, null, org.apache.kafka.connect.data.Schema.STRING_SCHEMA, value));
                                        }
                                    });
                                }
                                return null;
                            });
                        }
                        catch (Exception e) {
                            throw new As400RpcConnection.RpcException("first reader", e);
                        }
                    }, () -> null);
                    return null;
                };
                if (event.position().equals(pos(6))) {
                    assertThrows(As400RpcConnection.RpcException.class, read::call);
                    assertThat(first.scan.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(5));
                }
                else {
                    read.call();
                }
            }
        }
        first.close();
        // Only a prefix made it durable; no checkpoint heartbeat was accepted.
        Map<String, ?> durable = accepted.get(0).sourceOffset();
        assertThat(durable.get("replay_from.sequence")).isEqualTo("2");
        assertThat(durable.get("completed_through.sequence")).isEqualTo("1");
        assertThat(durable).doesNotContainKey("scan");
        As400ReaderFixture restarted = new As400ReaderFixture("restarted", As400ReaderFixture.config(false), 2, e -> {
        });
        restarted.scan.getPosition().setProcessed(false);
        List<CommittedBatch> replayed = new ArrayList<>();
        try (var replay = replay(durable)) {
            for (var event : events()) {
                restarted.read(event, () -> true, (sequence, journal, header) -> {
                    try {
                        journal.decode((h, data, offset) -> {
                            if (replay.accept(h, data, offset, pos(sequence.longValue())).isPresent()) {
                                replay.deliver((batch, records) -> replayed.add(batch));
                            }
                            return null;
                        });
                    }
                    catch (Exception e) {
                        throw new As400RpcConnection.RpcException("restarted reader", e);
                    }
                }, () -> null);
            }
            assertThat(replayed).hasSize(2);
            assertThat(replayed.get(0).changes()).hasSize(3);
            assertThat(replayed.get(0).changes().get(0).source().position()).isEqualTo(pos(3));
            assertThat(replay.checkpoint().completedThrough()).isEqualTo(pos(9));
            assertThat(durable.get("completed_through.sequence")).isEqualTo("1");
        }
        restarted.close();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void cancelReconnectRepeatedStopAndLateDataCannotTouchReplacement(boolean secure) throws Exception {
        race(secure, false);
    }

    @Test
    void lateErrorAfterCancellationCannotTouchReplacement() throws Exception {
        race(false, true);
    }

    private void race(boolean secure, boolean error) throws Exception {
        As400ReaderFixture old = fixture("old", secure);
        As400ReaderFixture replacement = fixture("replacement", secure);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger delivered = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try (As400ReplayCoordinator replay = replay(INITIAL)) {
            var pending = executor.submit(() -> old.read(events().get(0), running::get,
                    (sequence, journal, header) -> delivered.incrementAndGet(), () -> {
                        entered.countDown();
                        if (!release.await(10, TimeUnit.SECONDS)) {
                            throw new AssertionError("late read deadline");
                        }
                        if (error) {
                            throw new java.io.IOException("controlled late transport error");
                        }
                        return null;
                    }));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            running.set(false);
            old.close();
            old.reconnect();
            old.close();
            old.close();
            for (var event : events()) {
                replacement.read(event, () -> true, (sequence, journal, header) -> {
                    try {
                        journal.decode((h, data, offset) -> {
                            if (replay.accept(h, data, offset, pos(sequence.longValue())).isPresent()) {
                                replay.deliver((batch, records) -> {
                                });
                            }
                            return null;
                        });
                    }
                    catch (Exception e) {
                        throw new As400RpcConnection.RpcException("replacement", e);
                    }
                }, () -> null);
            }
            var replacementCheckpoint = replay.checkpoint();
            assertThat(replacementCheckpoint.completedThrough()).isEqualTo(pos(9));
            int replacementActions = replacement.operations.size();
            release.countDown();
            if (error) {
                var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> pending.get(10, TimeUnit.SECONDS));
                assertThat(failure).hasStackTraceContaining("controlled late transport error");
            }
            else {
                pending.get(10, TimeUnit.SECONDS);
                // Legacy nextEntry runs before the running check. R moves despite zero delivery.
                assertThat(old.scan.getPosition().getOffset()).isEqualTo(BigInteger.TWO);
            }
            assertThat(delivered).hasValue(0);
            assertThat(INITIAL.get("completed_through.sequence")).isEqualTo("1");
            assertThat(replay.checkpoint()).isEqualTo(replacementCheckpoint);
            assertThat(replacement.scan.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(9));
            assertThat(replacement.operations).hasSize(replacementActions);
            assertThat(actions(old, "job-end")).isGreaterThanOrEqualTo(1);
            assertThat(actions(replacement, "job-end")).isZero();
            assertThat(actions(old, "undeclared-action")).isZero();
            assertThat(actions(replacement, "undeclared-action")).isZero();
        }
        finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            old.close();
            replacement.close();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void disconnectedReadCanReconnectAndRepeatedCloseCancelsOnlyOnce(boolean secure) throws Exception {
        As400ReaderFixture reader = fixture("disconnected", secure);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger delivered = new AtomicInteger();
        var executor = Executors.newSingleThreadExecutor();
        try {
            var pending = executor.submit(() -> reader.read(events().get(0), () -> false, (a, b, c) -> delivered.incrementAndGet(), () -> {
                entered.countDown();
                await(release);
                return null;
            }));
            await(entered);
            reader.disconnectTransport();
            // Exercise reconnect's own close path while the original native call
            // is still held. No manual cancellation or copied lifecycle loop.
            reader.nativeScope(() -> reader.rpc.connection());
            reader.close();
            reader.close();
            assertThat(reader.connectionCount()).isEqualTo(2);
            assertThat(actions(reader, "job-end")).isEqualTo(1);
            assertThat(actions(reader, "copy-connection")).isEqualTo(1);
            assertThat(disconnects(reader, "cancellation")).isEqualTo(1);
            assertThat(disconnects(reader, "reader")).isEqualTo(2);
            assertThat(actions(reader, "undeclared-action")).isZero();
            release.countDown();
            pending.get(10, TimeUnit.SECONDS);
            assertThat(delivered).hasValue(0);
            assertThat(INITIAL.get("completed_through.sequence")).isEqualTo("1");
            reader.close();
            assertThat(actions(reader, "job-end")).isEqualTo(1);
        }
        finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            reader.close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "io", "runtime", "error" })
    void failedReadInvalidatesJobHandleBeforeASecondRead(String failure) throws Exception {
        As400ReaderFixture reader = fixture("failed-read", false);
        Throwable thrown = assertThrows(Throwable.class, () -> reader.read(events().get(0), () -> true, (a, b, c) -> {
            throw new AssertionError("A failed read must not deliver");
        }, () -> {
            switch (failure) {
                case "io" -> throw new java.io.IOException("controlled read failure");
                case "runtime" -> throw new IllegalStateException("controlled read failure");
                default -> throw new AssertionError("controlled read failure");
            }
        }));
        assertThat(thrown).hasStackTraceContaining("controlled read failure");
        assertThat(reader.scan.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        reader.close();
        assertThat(actions(reader, "job-end")).isZero();
        AtomicInteger delivered = new AtomicInteger();
        reader.read(events().get(0), () -> true, (a, b, c) -> delivered.incrementAndGet(), () -> null);
        assertThat(delivered).hasValue(1);
        reader.close();
        assertThat(actions(reader, "job-end")).isZero();
        assertThat(actions(reader, "undeclared-action")).isZero();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void cancellationFailureStillClosesTemporaryAndReaderConnections(boolean secure) throws Exception {
        As400ReaderFixture reader = fixture("cancel-failure", secure);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        reader.duringCancellation = () -> {
            throw new java.io.IOException("controlled Job.end failure");
        };
        // Even failure during cleanup of the cancellation copy cannot prevent
        // disconnect of the captured main connection.
        reader.failCancellationDisconnect = true;
        var executor = Executors.newSingleThreadExecutor();
        try {
            var pending = executor.submit(() -> reader.read(events().get(0), () -> false, (a, b, c) -> {
                throw new AssertionError("Stopped reader delivered data");
            }, () -> {
                entered.countDown();
                await(release);
                return null;
            }));
            await(entered);
            reader.disconnectTransport();
            reader.close();
            reader.close();
            assertThat(actions(reader, "job-end")).isEqualTo(1);
            assertThat(disconnects(reader, "cancellation")).isEqualTo(1);
            assertThat(disconnects(reader, "reader")).isEqualTo(1);
            assertThat(reader.connectionCount()).isEqualTo(1);
            release.countDown();
            pending.get(10, TimeUnit.SECONDS);
            assertThat(actions(reader, "undeclared-action")).isZero();
        }
        finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            reader.close();
        }
    }

    @Test
    void completionAndRepeatedCloseWaitForCancellationWhileReplacementProgresses() throws Exception {
        As400ReaderFixture old = fixture("old-cancelling", false);
        As400ReaderFixture replacement = fixture("new-progressing", false);
        CountDownLatch readEntered = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch cancelEntered = new CountDownLatch(1);
        CountDownLatch releaseCancel = new CountDownLatch(1);
        old.duringCancellation = () -> {
            cancelEntered.countDown();
            await(releaseCancel);
            return null;
        };
        var executor = Executors.newFixedThreadPool(3);
        try {
            var read = executor.submit(() -> old.read(events().get(0), () -> false, (a, b, c) -> {
                throw new AssertionError("Stopped old reader delivered data");
            }, () -> {
                readEntered.countDown();
                await(releaseRead);
                return null;
            }));
            await(readEntered);
            old.disconnectTransport();
            var close = executor.submit(() -> {
                old.close();
                return null;
            });
            await(cancelEntered);
            var repeated = executor.submit(() -> {
                old.close();
                return null;
            });
            releaseRead.countDown();
            AtomicInteger delivered = new AtomicInteger();
            for (var event : events()) {
                replacement.read(event, () -> true, (a, b, c) -> delivered.incrementAndGet(), () -> null);
            }
            assertThat(delivered).hasValue(8);
            assertThat(replacement.scan.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(9));
            assertThat(close.isDone()).isFalse();
            assertThat(actions(replacement, "job-end")).isZero();
            releaseCancel.countDown();
            close.get(10, TimeUnit.SECONDS);
            repeated.get(10, TimeUnit.SECONDS);
            read.get(10, TimeUnit.SECONDS);
            assertThat(actions(old, "job-end")).isEqualTo(1);
            assertThat(disconnects(old, "cancellation")).isEqualTo(1);
            assertThat(disconnects(old, "reader")).isEqualTo(1);
            assertThat(actions(old, "undeclared-action")).isZero();
            // A subsequent read can register normally, and old completion never
            // clears a replacement fixture's state or permits stale publication.
            old.read(events().get(1), () -> false, (a, b, c) -> {
            }, () -> null);
            old.close();
            assertThat(actions(old, "job-end")).isEqualTo(1);
        }
        finally {
            releaseRead.countDown();
            releaseCancel.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            old.close();
            replacement.close();
        }
    }

    @Test
    void concurrentReconnectWaitsForOldCloseAndSurvivesLateReadCompletion() throws Exception {
        As400ReaderFixture reader = fixture("overlapping-reconnect", false);
        CountDownLatch readEntered = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(1);
        CountDownLatch cancelEntered = new CountDownLatch(1);
        CountDownLatch releaseCancel = new CountDownLatch(1);
        CountDownLatch reconnectEntered = new CountDownLatch(1);
        reader.duringCancellation = () -> {
            cancelEntered.countDown();
            await(releaseCancel);
            return null;
        };
        var executor = Executors.newFixedThreadPool(3);
        try {
            var read = executor.submit(() -> reader.read(events().get(0), () -> false, (a, b, c) -> {
                throw new AssertionError("Stopped reader delivered data");
            }, () -> {
                readEntered.countDown();
                await(releaseRead);
                return null;
            }));
            await(readEntered);
            reader.disconnectTransport();
            var close = executor.submit(() -> {
                reader.close();
                return null;
            });
            await(cancelEntered);
            var reconnect = executor.submit(() -> reader.nativeScope(() -> {
                reconnectEntered.countDown();
                return reader.rpc.connection();
            }));
            await(reconnectEntered);
            assertThat(reconnect.isDone()).isFalse();
            assertThat(reader.connectionCount()).isEqualTo(1);
            releaseCancel.countDown();
            close.get(10, TimeUnit.SECONDS);
            reconnect.get(10, TimeUnit.SECONDS);
            assertThat(reader.nativeScope(() -> reader.rpc.isValid())).isTrue();
            assertThat(reader.connectionCount()).isEqualTo(2);
            assertThat(disconnects(reader, "reader")).isEqualTo(1);
            releaseRead.countDown();
            read.get(10, TimeUnit.SECONDS);
            assertThat(reader.nativeScope(() -> reader.rpc.isValid())).isTrue();
            assertThat(actions(reader, "job-end")).isEqualTo(1);
            assertThat(disconnects(reader, "cancellation")).isEqualTo(1);
            assertThat(actions(reader, "undeclared-action")).isZero();
        }
        finally {
            releaseRead.countDown();
            releaseCancel.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            reader.close();
        }
        assertThat(disconnects(reader, "reader")).isEqualTo(2);
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("Lifecycle test deadline expired");
        }
    }

    private static long disconnects(As400ReaderFixture reader, String role) {
        return reader.operations.stream().filter(e -> e.get("action").equals("disconnect")
                && ((Map<?, ?>) e.get("fields")).get("role").equals(role)).count();
    }

    @Test
    void transportRejectsUndeclaredNativeActions() throws Exception {
        As400ReaderFixture reader = fixture("strict", false);
        assertThrows(AssertionError.class, () -> reader.nativeScope(() -> reader.rpc.connection().getSystemName()));
        assertThat(actions(reader, "undeclared-action")).isEqualTo(1);
        reader.close();
    }

    private static As400ReaderFixture fixture(String owner, boolean secure) throws Exception {
        return new As400ReaderFixture(owner, As400ReaderFixture.config(secure), 1, event -> {
        });
    }

    private static long actions(As400ReaderFixture fixture, String action) {
        return fixture.operations.stream().filter(event -> event.get("action").equals(action)).count();
    }

    private static As400ReplayCoordinator replay(Map<String, ?> checkpoint) {
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
        var table = new TableIdentity(new TableId("DB", "LIB", "TEST"), "TEST", "TEST", JID, As400ConnectProbeConnector.MEMBER_JID, "FORMAT_V1");
        return new As400ReplayCoordinator(checkpoint, IDENTITY, FINGERPRINT, ORDER, new Limits(4, 20, 10, 4096), 1024, 20,
                (file, library) -> new As400SourceNormalizer.TableBinding(table, decoder), (source, control) -> SavepointRollback.UNRESOLVED,
                PARTITION, record -> true);
    }
}
