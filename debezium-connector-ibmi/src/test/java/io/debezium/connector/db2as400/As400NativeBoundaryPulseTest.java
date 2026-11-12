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
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400NativeBoundaryPulseTest {
    private static final JournalReceiver JOURNAL = new JournalReceiver("JRN", "LIB");
    private static final As400ShortBoundaryProgram PROGRAM = new As400ShortBoundaryProgram(JOURNAL,
            List.of(new As400ShortBoundaryProgram.Target("LIB", "FILE", "DATA")), "A".repeat(32), "B".repeat(64));
    private static final As400ShortBoundaryProgram MULTIPLE_FILES = new As400ShortBoundaryProgram(JOURNAL,
            List.of(new As400ShortBoundaryProgram.Target("LIB", "FILE", "DATA"), new As400ShortBoundaryProgram.Target("LIB", "SECOND", "DATA"),
                    new As400ShortBoundaryProgram.Target("OTHER", "FILE", "DATA")),
            PROGRAM.nonce(), PROGRAM.scopeDigest());

    @Test
    void shouldReuseObserverStatementsWhileCheckingEveryFileAndRebindingEveryParameter() throws Exception {
        var f = new Fixture();
        f.program = MULTIPLE_FILES;
        try (var pulse = f.open()) {
            assertThat(pulse.attempt(f.program)).isPresent();
            pulse.finish(f.program);
        }
        var commands = new ArrayList<String>();
        var locks = new ArrayList<List<String>>();
        for (int pass = 0; pass < 2; pass++) {
            for (var target : f.program.targets()) {
                commands.add("CHKOBJ OBJ(QSYS/" + target.library() + ") OBJTYPE(*LIB) AUT(*EXECUTE)");
                commands.add("CHKOBJ OBJ(" + target.library() + "/" + target.file() + ") OBJTYPE(*FILE) AUT(*OBJOPR *READ)");
                locks.add(List.of(target.library(), target.file(), "123456/TEST/JOB"));
            }
            assertThat(f.prepared.get(f.connections.get(pass + 1)))
                    .containsExactly("CALL QSYS2.QCMDEXC(?)", As400NativeBoundaryPulse.LOCKS_SQL);
        }
        assertThat(f.commands.stream().filter(command -> command.startsWith("CHKOBJ"))).containsExactlyElementsOf(commands);
        assertThat(f.observedLocks).containsExactlyElementsOf(locks);
        f.closed();
    }

    static Stream<Arguments> observationFailures() {
        return Stream.of("authority", "authority-warning", "held", "query", "query-warning", "result-warning", "cursor-warning", "cursor-close", "stop-binding",
                "stop-cursor").flatMap(fault -> Stream.of(Arguments.of(fault, 2), Arguments.of(fault, 5)));
    }

    @ParameterizedTest
    @MethodSource("observationFailures")
    void shouldRejectALaterFileObservationWithoutClaimingDrainOrStartingAnotherFile(String fault, int at) throws Exception {
        var f = new Fixture();
        f.program = MULTIPLE_FILES;
        f.observationFault = fault;
        f.observationFaultAt = at;
        var pulse = f.open();
        assertThrows(Exception.class, () -> pulse.attempt(f.program));
        assertThrows(Exception.class, () -> pulse.finish(f.program));
        assertThat(f.events).doesNotContain("receipt", "closing");
        assertThat(f.lockQueries).hasValue(List.of("authority", "authority-warning", "stop-binding").contains(fault) ? at - 1 : at);
        if (at < 3) {
            assertThat(f.events).doesNotContain("pulse");
            pulse.close();
        }
        else {
            assertThat(assertThrows(SQLException.class, pulse::close).getMessage()).contains("cleanup unverified");
        }
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2 })
    void shouldStopAfterObserverCloseWithoutStartingWorkOrDiscardingCompletedDrain(int pass) throws Exception {
        var f = new Fixture();
        f.program = MULTIPLE_FILES;
        f.stopAfterObserverClose = pass;
        try (var pulse = f.open()) {
            assertThrows(IllegalStateException.class, () -> pulse.attempt(f.program));
        } // Both a stopped preflight and a completed independent drain close cleanly.
        assertThat(f.lockQueries).hasValue(pass * MULTIPLE_FILES.targets().size());
        assertThat(f.events).doesNotContain("receipt", "closing");
        assertThat(f.events.contains("pulse")).isEqualTo(pass == 2);
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "short-batch", "failed-row", "warning", "stop" })
    void shouldStartNoAllocationsWhenPreparingTheCommandLedgerFails(String fault) throws Exception {
        var f = new Fixture();
        f.program = new As400ShortBoundaryProgram(JOURNAL, java.util.stream.IntStream.range(0, 51)
                .mapToObj(i -> new As400ShortBoundaryProgram.Target("LIB", String.format(java.util.Locale.ROOT, "F%03d", i), "DATA")).toList(),
                PROGRAM.nonce(), PROGRAM.scopeDigest());
        f.ledgerFault = fault;
        try (var pulse = f.open()) {
            assertThrows(Exception.class, () -> pulse.attempt(f.program));
        }
        assertThat(f.events).contains("batch-populated").doesNotContain("pulse", "batch-release", "receipt", "closing");
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "success", "75001", "75002", "network", "lost-release" })
    void shouldRequireIndependentDrainForEveryBatchAndNeverRetryAnUncertainResponse(String outcome) throws Exception {
        var f = new Fixture();
        f.program = new As400ShortBoundaryProgram(JOURNAL, java.util.stream.IntStream.range(0, 51)
                .mapToObj(i -> new As400ShortBoundaryProgram.Target("LIB", String.format(java.util.Locale.ROOT, "F%03d", i), "DATA")).toList(),
                PROGRAM.nonce(), PROGRAM.scopeDigest());
        f.receipt = outcome.equals("success");
        if (!f.receipt) {
            f.failure = new SQLException("Controlled result", outcome.equals("network") ? "08006" : outcome.equals("lost-release") ? "75002" : outcome);
        }
        if (outcome.equals("lost-release")) {
            f.clientReleaseFailure = new SQLException("Release response lost", "08006");
        }
        var pulse = f.open();
        if (outcome.equals("network") || outcome.equals("lost-release")) {
            assertThrows(SQLException.class, () -> pulse.attempt(f.program));
            assertThrows(SQLException.class, pulse::close);
            assertThat(f.events).doesNotContain("receipt", "closing");
        }
        else {
            assertThat(pulse.attempt(f.program).isPresent()).isEqualTo(f.receipt);
            if (f.receipt) {
                pulse.finish(f.program);
            }
            pulse.close();
            assertThat(f.lockQueries).hasValue(102);
            assertThat(f.events).containsSubsequence("batch-prepare", "batch-populated", "pulse", "pulse-returned", "locks", "receipt");
        }
        assertThat(f.events.stream().filter("batch-release"::equals)).hasSize(outcome.equals("75002") || outcome.equals("lost-release") ? 1 : 0);
        f.closed();
    }

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

    @Test
    void shouldRetryReleaseOnlyAfterTheServerReturnsItsExplicitReleaseFailure() throws Exception {
        var f = new Fixture();
        f.failure = new SQLException("All server release attempts completed", "75002");
        f.receipt = false;
        try (var pulse = f.open()) {
            assertThat(pulse.attempt(PROGRAM)).isEmpty();
            assertThrows(IllegalArgumentException.class, () -> pulse.finish(PROGRAM));
        }
        assertThat(f.commands).filteredOn(PROGRAM.release()::equals).hasSize(1);
        assertThat(f.events).containsSubsequence("pulse-returned", "locks", "receipt").doesNotContain("closing");
        f.closed();
    }

    @Test
    void shouldNeverRetryAnUncertainClientReleaseOrClaimDrained() throws Exception {
        var f = new Fixture();
        f.failure = new SQLException("All server release attempts completed", "75002");
        f.clientReleaseFailure = new SQLException("Release response lost", "08006");
        f.receipt = false;
        var pulse = f.open();
        assertThrows(SQLException.class, () -> pulse.attempt(PROGRAM));
        assertThat(assertThrows(SQLException.class, pulse::close).getMessage()).contains("cleanup unverified");
        assertThat(f.commands).filteredOn(PROGRAM.release()::equals).hasSize(1);
        assertThat(f.events).doesNotContain("receipt", "closing");
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
        var pulse = f.open();
        try {
            assertThrows(Exception.class, () -> pulse.attempt(PROGRAM));
            assertThrows(Exception.class, () -> pulse.finish(PROGRAM));
        }
        finally {
            if (List.of("network", "chained-failure", "timeout", "query", "warning", "stopped-after-call", "held-after").contains(fault)) {
                assertThat(assertThrows(SQLException.class, pulse::close).getMessage()).contains("cleanup unverified");
            }
            else {
                pulse.close();
            }
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
        As400ShortBoundaryProgram program = PROGRAM;
        final List<Connection> connections = new ArrayList<>();
        final List<String> events = new ArrayList<>();
        final List<String> commands = new ArrayList<>();
        final List<PreparedStatement> statements = new ArrayList<>();
        final List<ResultSet> results = new ArrayList<>();
        final Map<Connection, List<String>> prepared = new java.util.IdentityHashMap<>();
        final List<List<String>> observedLocks = new ArrayList<>();
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicInteger lockQueries = new AtomicInteger();
        boolean receipt = true, foreign, statementWarning, receiptWarning, authorityFailure, stopOnReturn;
        int heldAt;
        SQLException failure, clientReleaseFailure;
        String ledgerFault = "";
        String observationFault = "";
        int observationFaultAt;
        int stopAfterObserverClose;
        CountDownLatch entered, release;

        As400NativeBoundaryPulse open() throws SQLException {
            return new As400NativeBoundaryPulse(this::connection, () -> {
                if (stopped.get()) {
                    throw new IllegalStateException("terminal stop");
                }
            }, JOURNAL, program.nonce());
        }

        Connection connection() throws SQLException {
            var connection = mock(Connection.class);
            connections.add(connection);
            prepared.put(connection, new ArrayList<>());
            int observer = connections.size() - 1;
            org.mockito.Mockito.doAnswer(close -> {
                if (stopAfterObserverClose > 0 && observer == stopAfterObserverClose) {
                    stopped.set(true);
                }
                return null;
            }).when(connection).close();
            when(connection.getAutoCommit()).thenReturn(true);
            when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_NONE);
            when(connection.prepareStatement(anyString())).thenAnswer(call -> {
                String sql = call.getArgument(0);
                prepared.get(connection).add(sql);
                var statement = mock(PreparedStatement.class);
                statements.add(statement);
                var parameter = new AtomicReference<String>();
                var parameters = new java.util.HashMap<Integer, String>();
                var cursorWarning = new AtomicBoolean();
                if (sql.equals(As400ShortBoundaryProgram.INSERT_BATCH_SQL)) {
                    var pending = new AtomicInteger();
                    org.mockito.Mockito.doAnswer(add -> {
                        pending.incrementAndGet();
                        return null;
                    }).when(statement).addBatch();
                    when(statement.executeBatch()).thenAnswer(execute -> {
                        events.add("batch-populated");
                        int[] counts = new int[pending.getAndSet(0)];
                        java.util.Arrays.fill(counts, 1);
                        if (ledgerFault.equals("failed-row")) {
                            counts[0] = java.sql.Statement.EXECUTE_FAILED;
                        }
                        if (ledgerFault.equals("stop")) {
                            stopped.set(true);
                        }
                        if (ledgerFault.equals("short-batch")) {
                            return new int[0];
                        }
                        return counts;
                    });
                }
                org.mockito.Mockito.doAnswer(set -> {
                    parameters.put(set.getArgument(0), set.getArgument(1));
                    if (set.<Integer> getArgument(0) == 1) {
                        parameter.set(set.getArgument(1));
                    }
                    if (sql.equals(As400NativeBoundaryPulse.LOCKS_SQL) && set.<Integer> getArgument(0) == 3
                            && observationFaultIs("stop-binding", lockQueries.get() + 1)) {
                        stopped.set(true);
                    }
                    return null;
                })
                        .when(statement).setString(org.mockito.ArgumentMatchers.anyInt(), anyString());
                when(statement.execute()).thenAnswer(execute -> {
                    if (sql.equals(program.statement())) {
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
                    else if (sql.equals(As400ShortBoundaryProgram.PREPARE_BATCHES_SQL)) {
                        events.add("batch-prepare");
                    }
                    else if (program.batched() && sql.equals(program.retryBatchRelease())) {
                        events.add("batch-release");
                        if (clientReleaseFailure != null) {
                            throw clientReleaseFailure;
                        }
                    }
                    else if (parameter.get() != null) {
                        commands.add(parameter.get());
                        if (!program.batched() && parameter.get().equals(program.release()) && clientReleaseFailure != null) {
                            throw clientReleaseFailure;
                        }
                        if (parameter.get().startsWith("CHKOBJ") && authorityFailure) {
                            throw new SQLException("Authority missing", "38501");
                        }
                        if (parameter.get().startsWith("CHKOBJ") && parameter.get().contains("OBJTYPE(*FILE)")
                                && observationFaultIs("authority", lockQueries.get() + 1)) {
                            throw new SQLException("Later file authority missing", "38501");
                        }
                        if (parameter.get().equals(program.closing())) {
                            events.add("closing");
                        }
                    }
                    return false;
                });
                when(statement.getWarnings()).thenAnswer(w -> {
                    boolean pulseWarning = sql.equals(program.statement()) && statementWarning;
                    boolean ledgerWarning = sql.equals(As400ShortBoundaryProgram.INSERT_BATCH_SQL) && ledgerFault.equals("warning");
                    boolean lockWarning = sql.equals(As400NativeBoundaryPulse.LOCKS_SQL)
                            && (cursorWarning.get() || observationFaultIs("query-warning", lockQueries.get()));
                    boolean authorityWarning = parameter.get() != null && parameter.get().startsWith("CHKOBJ") && parameter.get().contains("OBJTYPE(*FILE)")
                            && observationFaultIs("authority-warning", lockQueries.get() + 1);
                    return pulseWarning || ledgerWarning || lockWarning || authorityWarning ? new SQLWarning("incomplete") : null;
                });
                when(statement.executeQuery()).thenAnswer(execute -> {
                    List<Map<String, Object>> data;
                    if (sql.equals(As400NativeBoundaryPulse.HOLDER_SQL)) {
                        data = List.of(Map.of("JOB", "123456/TEST/JOB", "THREAD_HEX", "0000000000000001"));
                    }
                    else if (sql.equals(As400NativeBoundaryPulse.LOCKS_SQL)) {
                        verify(statement).setMaxRows(1);
                        cursorWarning.set(false); // JDBC re-execution clears statement warnings.
                        events.add("locks");
                        observedLocks.add(List.of(parameters.get(1), parameters.get(2), parameters.get(3)));
                        int query = lockQueries.incrementAndGet();
                        if (observationFaultIs("query", query)) {
                            throw new SQLException("Incomplete later lock query");
                        }
                        data = query == heldAt || observationFaultIs("held", query)
                                ? List.of(Map.of("JOB_NAME", "123456/TEST/JOB"))
                                : List.of();
                    }
                    else if (sql.equals(As400NativeBoundaryPulse.RESULT_SQL)) {
                        events.add("receipt");
                        data = receipt ? List.of(Map.of("ATTEMPT", foreign ? "C".repeat(32) : program.nonce(), "SCOPE_DIGEST", program.scopeDigest(),
                                "ACQUIRED", Timestamp.valueOf("2026-11-09 20:00:00.001"), "RELEASED", Timestamp.valueOf("2026-11-09 20:00:00.010"))) : List.of();
                    }
                    else {
                        throw new AssertionError(sql);
                    }
                    var rows = mock(ResultSet.class);
                    results.add(rows);
                    var index = new AtomicInteger(-1);
                    when(rows.next()).thenAnswer(next -> index.incrementAndGet() < data.size());
                    when(rows.getString(anyString())).thenAnswer(get -> data.get(index.get()).get(get.getArgument(0)));
                    when(rows.getTimestamp(anyString())).thenAnswer(get -> data.get(index.get()).get(get.getArgument(0)));
                    when(rows.getWarnings()).thenReturn(sql.equals(As400NativeBoundaryPulse.RESULT_SQL) && receiptWarning
                            || sql.equals(As400NativeBoundaryPulse.LOCKS_SQL) && observationFaultIs("result-warning", lockQueries.get())
                                    ? new SQLWarning("partial")
                                    : null);
                    if (sql.equals(As400NativeBoundaryPulse.LOCKS_SQL) && lockQueries.get() == observationFaultAt) {
                        org.mockito.Mockito.doAnswer(close -> {
                            if (observationFault.equals("cursor-close")) {
                                throw new SQLException("Cursor close failed");
                            }
                            cursorWarning.set(observationFault.equals("cursor-warning"));
                            if (observationFault.equals("stop-cursor")) {
                                stopped.set(true);
                            }
                            return null;
                        }).when(rows).close();
                    }
                    return rows;
                });
                return statement;
            });
            return connection;
        }

        boolean observationFaultIs(String type, int query) {
            return observationFault.equals(type) && observationFaultAt == query;
        }

        void closed() throws SQLException {
            for (var statement : statements) {
                verify(statement).close();
                verify(statement).setQueryTimeout(5);
            }
            for (var result : results) {
                verify(result).close();
            }
            for (var connection : connections) {
                verify(connection).close();
            }
        }
    }
}
