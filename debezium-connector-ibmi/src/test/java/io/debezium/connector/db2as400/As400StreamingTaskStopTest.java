/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLNonTransientConnectionException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;

class As400StreamingTaskStopTest {
    private static final As400Partition PARTITION = new As400Partition("synthetic");

    static Stream<Arguments> errors() {
        return Stream.of("io", "sql", "interrupted", "metadata", "empty").flatMap(kind -> Stream.of(false, true).map(secure -> Arguments.of(kind, secure)));
    }

    @ParameterizedTest
    @MethodSource("errors")
    void stoppedContextCannotRetryOrPublishIdleHeartbeat(String kind, boolean secure) throws Exception {
        var fixture = fixture("stopped", secure);
        AtomicBoolean running = new AtomicBoolean(true);
        var dispatcher = mock(EventDispatcher.class);
        var source = fixture.source(dispatcher, mock(ErrorHandler.class), (s, j, h) -> {
            throw new AssertionError("Unexpected record");
        });
        doAnswer(invocation -> {
            running.set(false);
            return switch (kind) {
                case "io" -> throw new IOException("late RPC failure");
                case "sql" -> throw new SQLNonTransientConnectionException("late JDBC failure");
                case "interrupted" -> throw new InterruptedException("late interruption");
                case "metadata" -> throw new As400RpcConnection.RpcException("late metadata failure");
                default -> RetrievalState.NotCalled;
            };
        }).when(fixture.reader.rpc).getJournalEntries(any(), any(), any(), any());
        fixture.reader.nativeScope(() -> {
            source.execute(context(running), PARTITION, fixture.reader.scan);
            source.closeAndReconnect();
            source.rateLimittedClose();
            return null;
        });
        assertThat(fixture.reader.operations).isEmpty();
        assertThat(fixture.jdbcEvents).isEmpty();
        assertThat(fixture.reader.scan.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        verify(dispatcher, never()).dispatchHeartbeatEventAlsoToIncrementalSnapshot(any(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void runningContextStillReconnectsBothTransports(boolean secure) throws Exception {
        var fixture = fixture("running", secure);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger calls = new AtomicInteger();
        var source = fixture.source(mock(EventDispatcher.class), mock(ErrorHandler.class), (s, j, h) -> {
        });
        doAnswer(invocation -> {
            if (calls.getAndIncrement() == 0) {
                throw new IOException("ordinary connection failure");
            }
            running.set(false);
            return RetrievalState.NotCalled;
        }).when(fixture.reader.rpc).getJournalEntries(any(), any(), any(), any());
        fixture.reader.nativeScope(() -> {
            source.execute(context(running), PARTITION, fixture.reader.scan);
            return null;
        });
        assertThat(fixture.reader.connectionCount()).isEqualTo(1);
        assertThat(fixture.jdbcConnected).isTrue();
        assertThat(actions(fixture.jdbcEvents, "jdbc-connect-enter")).isEqualTo(1);
        As400ConnectorTask task = new As400ConnectorTask();
        fixture.install(task);
        fixture.reader.nativeScope(() -> {
            task.doStop();
            return null;
        });
        assertThat(fixture.jdbcConnected).isFalse();
        assertThat(fixture.reader.allReaderTransportsDisconnected()).isTrue();
    }

    static Stream<Arguments> stages() {
        return Stream.of("rpc-close", "rpc-connect", "jdbc-close", "jdbc-connect")
                .flatMap(stage -> Stream.of(false, true).map(secure -> Arguments.of(stage, secure)));
    }

    @ParameterizedTest
    @MethodSource("stages")
    void taskStopDrainsAdmittedRetryAndPreventsLaterStages(String stage, boolean secure) throws Exception {
        var old = fixture("old", secure);
        var replacement = fixture("replacement", secure);
        AtomicBoolean running = new AtomicBoolean(true);
        var source = old.source(mock(EventDispatcher.class), mock(ErrorHandler.class), (s, j, h) -> {
        });
        CountDownLatch reached = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch requested = new CountDownLatch(1);
        AtomicBoolean once = new AtomicBoolean();
        As400TaskStopFixture.Hook gate = action -> {
            if (stage.equals(action) && once.compareAndSet(false, true)) {
                reached.countDown();
                await(release);
            }
        };
        old.jdbcHook = action -> gate.call("jdbc-" + action);
        doAnswer(i -> {
            Object value = i.callRealMethod();
            gate.call("rpc-close");
            return value;
        }).when(old.reader.rpc).close();
        doAnswer(i -> {
            Object value = i.callRealMethod();
            gate.call("rpc-connect");
            return value;
        }).when(old.reader.rpc).connection();
        doAnswer(i -> {
            i.callRealMethod();
            requested.countDown();
            return null;
        }).when(old.reader.lifecycle).requestStop();
        doAnswer(i -> {
            throw new IOException("retry fixture");
        }).when(old.reader.rpc).getJournalEntries(any(), any(), any(), any());
        As400ConnectorTask task = new As400ConnectorTask();
        old.install(task);
        var executor = Executors.newFixedThreadPool(2, action -> {
            Thread thread = new Thread(action, "task-stop-test");
            thread.setDaemon(true);
            return thread;
        });
        try {
            var reading = executor.submit(() -> old.reader.nativeScope(() -> {
                source.execute(context(running), PARTITION, old.reader.scan);
                return null;
            }));
            await(reached);
            running.set(false);
            var stopping = executor.submit(() -> old.reader.nativeScope(() -> {
                task.doStop();
                return null;
            }));
            await(requested);
            assertThat(stopping.isDone()).isFalse();
            // Independent replacement can reconnect while the old task drains.
            replacement.reader.nativeScope(() -> {
                replacement.source(mock(EventDispatcher.class), mock(ErrorHandler.class), (s, j, h) -> {
                }).closeAndReconnect();
                return null;
            });
            assertThat(replacement.jdbcConnected).isTrue();
            int replacementOperations = replacement.reader.operations.size();
            release.countDown();
            reading.get(10, TimeUnit.SECONDS);
            stopping.get(10, TimeUnit.SECONDS);
            assertThat(actions(old.jdbcEvents, "jdbc-connect-enter")).isEqualTo(stage.equals("jdbc-connect") ? 1 : 0);
            assertThat(old.jdbcConnected).isFalse();
            assertThat(old.reader.allReaderTransportsDisconnected()).isTrue();
            assertThat(replacement.jdbcConnected).isTrue();
            assertThat(replacement.reader.operations).hasSize(replacementOperations);
            int operations = old.reader.operations.size();
            int jdbcOperations = old.jdbcEvents.size();
            old.reader.nativeScope(() -> {
                source.closeAndReconnect();
                return null;
            });
            assertThat(old.reader.operations).hasSize(operations);
            assertThat(old.jdbcEvents).hasSize(jdbcOperations);
            assertThat(old.reader.scan.getPosition().getOffset()).isEqualTo(BigInteger.ONE);
        }
        finally {
            release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            old.reader.close();
            replacement.reader.close();
            replacement.jdbc.close();
        }
    }

    private static As400TaskStopFixture fixture(String owner, boolean secure) throws Exception {
        var config = new As400ConnectorConfig(Configuration.from(As400ReaderFixture.config(secure).getConfig().asMap())
                .edit().with("poll.interval.ms", 1).build());
        return new As400TaskStopFixture(owner, config, 1, event -> {
        });
    }

    private static ChangeEventSourceContext context(AtomicBoolean running) {
        return mock(ChangeEventSourceContext.class, invocation -> {
            if (invocation.getMethod().getName().equals("isRunning")) {
                return running.get();
            }
            throw new AssertionError("Unexpected context action " + invocation.getMethod().getName());
        });
    }

    private static long actions(List<Map<String, Object>> events, String action) {
        return events.stream().filter(e -> action.equals(e.get("action"))).count();
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        if (!latch.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("Task stop gate timed out");
        }
    }
}
