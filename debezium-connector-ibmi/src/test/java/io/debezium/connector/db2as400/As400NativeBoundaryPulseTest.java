/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400NativeBoundaryPulseTest {
    private static final JournalReceiver JOURNAL = new JournalReceiver("JRN", "LIB");
    private static final As400ShortBoundaryProgram PROGRAM = new As400ShortBoundaryProgram(JOURNAL,
            List.of(new As400ShortBoundaryProgram.Target("LIB", "FILE", "DATA")), "A".repeat(32), "B".repeat(64));

    @Test
    void successRequiresCompletedServerCallIndependentDrainAndExactReceipt() throws Exception {
        var fixture = new Fixture();
        try (var pulse = fixture.open()) {
            var completion = pulse.attempt(PROGRAM).orElseThrow();
            assertThat(completion.job()).isEqualTo("123456/TEST/JOB");
            assertThat(completion.thread()).isEqualTo(BigInteger.ONE);
            pulse.finish(PROGRAM);
            assertThrows(IllegalArgumentException.class, () -> pulse.finish(PROGRAM));
            assertThrows(IllegalArgumentException.class, () -> pulse.attempt(PROGRAM));
        }
        int call = fixture.events.indexOf("pulse");
        assertThat(fixture.events.subList(call, call + 2)).containsExactly("pulse", "pulse-returned");
        assertThat(fixture.events).containsSubsequence("locks", "pulse", "pulse-returned", "locks", "receipt", "closing");
        assertThat(fixture.commands).contains("CHKOBJ OBJ(QSYS/LIB) OBJTYPE(*LIB) AUT(*EXECUTE)", "CHKOBJ OBJ(LIB/FILE) OBJTYPE(*FILE) AUT(*OBJOPR *READ)");
        fixture.closed();
    }

    @Test
    void onlyGeneratedTerminalRefusalWithNoLocksOrSuccessReceiptIsRetryable() throws Exception {
        var f = new Fixture();
        f.failure = new SQLException("Generated program completed its failure/release blocks", "75001");
        f.receipt = false;
        try (var pulse = f.open()) {
            assertThat(pulse.attempt(PROGRAM)).isEmpty();
        }
        assertThat(f.events).containsSubsequence("pulse", "pulse-returned", "locks", "receipt");
        assertThat(f.events).doesNotContain("closing");
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "network", "chained-failure", "timeout", "query", "warning", "held-before", "held-after", "authority", "foreign-receipt",
            "missing-receipt", "failure-with-receipt", "receipt-warning", "stopped-after-call" })
    void uncertaintyNeverBecomesACompletedCandidateOrRetry(String fault) throws Exception {
        var f = new Fixture();
        switch (fault) {
            case "network" -> f.failure = new SQLException("Connection lost", "08006");
            case "chained-failure" -> {
                f.failure = new SQLException("Generated signal", "75001");
                f.failure.setNextException(new SQLException("Connection lost", "08006"));
            }
            case "timeout" -> f.failure = new SQLException("Timeout", "HYT00");
            case "query" -> f.failure = new SQLException("Program invalid", "42601");
            case "warning" -> f.statementWarning = true;
            case "held-before" -> f.heldAt = 1;
            case "held-after" -> f.heldAt = 2;
            case "authority" -> f.authorityFailure = true;
            case "foreign-receipt" -> f.foreign = true;
            case "missing-receipt" -> f.receipt = false;
            case "failure-with-receipt" -> f.failure = new SQLException("Generated failure", "75001");
            case "receipt-warning" -> f.receiptWarning = true;
            case "stopped-after-call" -> f.stopOnReturn = true;
            default -> throw new AssertionError(fault);
        }
        try (var pulse = f.open()) {
            assertThrows(Exception.class, () -> pulse.attempt(PROGRAM));
            assertThrows(Exception.class, () -> pulse.finish(PROGRAM));
        }
        assertThat(f.events).doesNotContain("closing");
        if (List.of("network", "chained-failure", "timeout", "query", "warning", "stopped-after-call").contains(fault)) {
            assertThat(f.events).doesNotContain("receipt");
            assertThat(f.lockQueries).hasValue(1);
        }
        if (List.of("held-before", "authority").contains(fault)) {
            assertThat(f.events).doesNotContain("pulse");
        }
        f.closed();
    }

    @Test
    void lateNativeReturnAfterStopCannotStartMoreWorkWhileReplacementCompletes() throws Exception {
        var old = new Fixture();
        old.entered = new CountDownLatch(1);
        old.release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try {
            var result = executor.submit(() -> {
                try (var pulse = old.open()) {
                    return pulse.attempt(PROGRAM);
                }
            });
            assertThat(old.entered.await(5, TimeUnit.SECONDS)).isTrue();
            old.stopped.set(true);
            var replacement = new Fixture();
            try (var pulse = replacement.open()) {
                assertThat(pulse.attempt(PROGRAM)).isPresent();
                pulse.finish(PROGRAM);
            }
            replacement.closed();
            assertThat(result.isDone()).isFalse(); // Stop request is not completed drain.
            old.release.countDown();
            assertThrows(java.util.concurrent.ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
            assertThat(old.events).doesNotContain("receipt", "closing");
            assertThat(old.lockQueries).hasValue(1);
            old.closed();
        }
        finally {
            old.release.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class Fixture {
        final List<Connection> connections = new ArrayList<>();
        final List<String> events = new ArrayList<>();
        final List<String> commands = new ArrayList<>();
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicInteger lockQueries = new AtomicInteger();
        boolean receipt = true, foreign, statementWarning, receiptWarning, authorityFailure, stopOnReturn;
        int heldAt;
        SQLException failure;
        CountDownLatch entered, release;

        As400NativeBoundaryPulse open() throws SQLException {
            return new As400NativeBoundaryPulse(this::connection, () -> {
                if (stopped.get()) {
                    throw new IllegalStateException("terminal stop");
                }
            }, JOURNAL, PROGRAM.nonce());
        }

        Connection connection() throws SQLException {
            var connection = mock(Connection.class);
            connections.add(connection);
            when(connection.getAutoCommit()).thenReturn(true);
            when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_NONE);
            when(connection.prepareStatement(anyString())).thenAnswer(call -> {
                String sql = call.getArgument(0);
                var statement = mock(PreparedStatement.class);
                var parameter = new AtomicReference<String>();
                org.mockito.Mockito.doAnswer(set -> {
                    if (set.<Integer> getArgument(0) == 1) {
                        parameter.set(set.getArgument(1));
                    }
                    return null;
                })
                        .when(statement).setString(org.mockito.ArgumentMatchers.anyInt(), anyString());
                when(statement.execute()).thenAnswer(execute -> {
                    if (sql.equals(PROGRAM.statement())) {
                        events.add("pulse");
                        if (entered != null) {
                            entered.countDown();
                            if (!release.await(5, TimeUnit.SECONDS)) {
                                throw new AssertionError("held call timeout");
                            }
                        }
                        events.add("pulse-returned");
                        if (stopOnReturn) {
                            stopped.set(true);
                        }
                        if (failure != null) {
                            throw failure;
                        }
                    }
                    else if (parameter.get() != null) {
                        commands.add(parameter.get());
                        if (parameter.get().startsWith("CHKOBJ") && authorityFailure) {
                            throw new SQLException("Authority missing", "38501");
                        }
                        if (parameter.get().equals(PROGRAM.closing())) {
                            events.add("closing");
                        }
                    }
                    return false;
                });
                when(statement.getWarnings()).thenAnswer(w -> sql.equals(PROGRAM.statement()) && statementWarning ? new SQLWarning("incomplete") : null);
                when(statement.executeQuery()).thenAnswer(execute -> {
                    List<Map<String, Object>> data;
                    if (sql.equals(As400NativeBoundaryPulse.HOLDER_SQL)) {
                        data = List.of(Map.of("JOB", "123456/TEST/JOB", "THREAD_HEX", "0000000000000001"));
                    }
                    else if (sql.equals(As400NativeBoundaryPulse.LOCKS_SQL)) {
                        events.add("locks");
                        data = lockQueries.incrementAndGet() == heldAt ? List.of(Map.of("JOB_NAME", "123456/TEST/JOB")) : List.of();
                    }
                    else if (sql.equals(As400NativeBoundaryPulse.RESULT_SQL)) {
                        events.add("receipt");
                        data = receipt ? List.of(Map.of("ATTEMPT", foreign ? "C".repeat(32) : PROGRAM.nonce(), "SCOPE_DIGEST", PROGRAM.scopeDigest(),
                                "ACQUIRED", Timestamp.valueOf("2026-11-09 20:00:00.001"), "RELEASED", Timestamp.valueOf("2026-11-09 20:00:00.010"))) : List.of();
                    }
                    else {
                        throw new AssertionError(sql);
                    }
                    var rows = mock(ResultSet.class);
                    var index = new AtomicInteger(-1);
                    when(rows.next()).thenAnswer(next -> index.incrementAndGet() < data.size());
                    when(rows.getString(anyString())).thenAnswer(get -> data.get(index.get()).get(get.getArgument(0)));
                    when(rows.getTimestamp(anyString())).thenAnswer(get -> data.get(index.get()).get(get.getArgument(0)));
                    when(rows.getWarnings()).thenReturn(sql.equals(As400NativeBoundaryPulse.RESULT_SQL) && receiptWarning ? new SQLWarning("partial") : null);
                    return rows;
                });
                return statement;
            });
            return connection;
        }

        void closed() throws SQLException {
            for (var connection : connections) {
                verify(connection).close();
            }
        }
    }
}
