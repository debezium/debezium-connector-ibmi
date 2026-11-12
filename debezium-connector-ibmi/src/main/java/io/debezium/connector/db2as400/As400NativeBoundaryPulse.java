/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/** Owns one synchronous native attempt; metadata and independent lock checks are outside the held call. */
final class As400NativeBoundaryPulse implements AutoCloseable {
    static final class UnverifiedCleanup extends SQLException {
        UnverifiedCleanup(String job, SQLException cause) {
            super("Native pulse cleanup unverified for owned job " + job
                    + "; independently inspect its allocations before retrying; connection close is not lock-release proof", cause);
        }
    }

    static boolean hasUnverifiedCleanup(Throwable failure) {
        var pending = new java.util.ArrayDeque<Throwable>();
        var seen = new java.util.HashSet<Throwable>();
        pending.add(failure);
        while (!pending.isEmpty()) {
            var item = pending.removeFirst();
            if (!seen.add(item)) {
                continue;
            }
            if (item instanceof UnverifiedCleanup) {
                return true;
            }
            if (item.getCause() != null) {
                pending.addLast(item.getCause());
            }
            pending.addAll(java.util.Arrays.asList(item.getSuppressed()));
        }
        return false;
    }

    static final String HOLDER_SQL = "SELECT QSYS2.JOB_NAME AS JOB, HEX(QSYS2.THREAD_ID) AS THREAD_HEX FROM SYSIBM.SYSDUMMY1";
    static final String LOCKS_SQL = "SELECT JOB_NAME, LOCK_STATE, LOCK_STATUS, SYSTEM_TABLE_MEMBER FROM QSYS2.OBJECT_LOCK_INFO "
            + "WHERE SYSTEM_OBJECT_SCHEMA=? AND SYSTEM_OBJECT_NAME=? AND OBJECT_TYPE='*FILE' "
            + "AND JOB_NAME=? AND LOCK_STATE='*EXCLRD' AND LOCK_STATUS='HELD'";
    static final String RESULT_SQL = "SELECT ATTEMPT,SCOPE_DIGEST,ACQUIRED,RELEASED FROM SESSION.DCGPULSE";
    private static final String COMMAND_SQL = "CALL QSYS2.QCMDEXC(?)";
    private final As400JdbcConnectionFactory connections;
    private final Runnable checkRunning;
    private final JournalReceiver journal;
    private final String nonce;
    private final Connection holder;
    private final String job;
    private final BigInteger thread;
    private boolean attempted;
    private boolean allocationStarted;
    private boolean releaseVerified;
    private boolean released;
    private boolean accepted;
    private boolean finished;
    private boolean closed;

    As400NativeBoundaryPulse(As400JdbcConnectionFactory connections, Runnable checkRunning, JournalReceiver journal, String nonce) throws SQLException {
        // Validate interpolation before opening any native handle.
        String opening = As400ShortBoundaryProgram.opening(journal, nonce);
        this.connections = connections;
        this.checkRunning = checkRunning;
        this.journal = journal;
        this.nonce = nonce;
        checkRunning.run();
        holder = connections.open();
        try {
            require(holder.getAutoCommit() && holder.getTransactionIsolation() == Connection.TRANSACTION_NONE && !holder.isReadOnly(),
                    "Dedicated nontransactional pulse job required");
            try (var statement = holder.prepareStatement(HOLDER_SQL)) {
                statement.setQueryTimeout(5);
                statement.setMaxRows(2);
                try (var rows = statement.executeQuery()) {
                    require(rows.next(), "Missing native pulse job");
                    job = rows.getString("JOB");
                    String hex = rows.getString("THREAD_HEX");
                    require(job != null && job.matches("[0-9]{6}/[A-Z0-9_#$@]{1,10}/[A-Z0-9_#$@]{1,10}")
                            && hex != null && hex.matches("[A-F0-9]{16}"), "Incomplete native pulse producer");
                    thread = new BigInteger(hex, 16);
                    require(thread.signum() > 0 && !rows.next(), "Ambiguous native pulse producer");
                    As400DcgNativeIo.noWarnings(rows, statement);
                }
            }
            execute(As400ShortBoundaryProgram.PREPARE_SQL);
            command(opening);
        }
        catch (Throwable failure) {
            try {
                holder.close();
            }
            catch (Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    Optional<As400ShortBoundaryAudit.Completion> attempt(As400ShortBoundaryProgram program) throws SQLException {
        check();
        require(!attempted && program.journal().equals(journal) && program.nonce().equals(nonce), "Pulse cannot be reused or redirected");
        attempted = true;
        requireNoAllocations(program);
        check();
        if (program.batched()) {
            execute(As400ShortBoundaryProgram.PREPARE_BATCHES_SQL);
            prepareBatches(program);
        }
        boolean success = true;
        try {
            // No client callback, query, force or metadata operation is reachable
            // between allocation and release: both happen inside this one call.
            allocationStarted = true;
            execute(program.statement());
        }
        catch (SQLException error) {
            // This is the generated program's final signal, after its release
            // block. Network/API/compiler failures never imply completed drain.
            if ((!"75001".equals(error.getSQLState()) && !"75002".equals(error.getSQLState())) || error.getNextException() != null) {
                throw error;
            }
            if ("75002".equals(error.getSQLState())) {
                // The server returned from all release handlers. Retry only on
                // this dedicated holder, never after a lost/uncertain response.
                // Pre-existing allocations were excluded before starting it.
                try {
                    if (program.batched()) {
                        execute(program.retryBatchRelease());
                    }
                    else {
                        command(program.release());
                    }
                }
                catch (SQLException failure) {
                    // This response may itself be lost. Do not turn an unknown
                    // transport result into another native command attempt.
                    failure.addSuppressed(error);
                    throw failure;
                }
            }
            success = false;
        }
        check();
        requireNoAllocations(program);
        releaseVerified = true;
        check();
        try (var statement = holder.prepareStatement(RESULT_SQL)) {
            statement.setQueryTimeout(5);
            statement.setMaxRows(2);
            try (var rows = statement.executeQuery()) {
                boolean found = rows.next();
                As400DcgNativeIo.noWarnings(rows, statement);
                if (!success) {
                    require(!found, "Failed native pulse left success evidence");
                    released = true;
                    return Optional.empty();
                }
                require(found && nonce.equals(rows.getString("ATTEMPT")) && program.scopeDigest().equals(rows.getString("SCOPE_DIGEST")),
                        "Missing or foreign pulse completion");
                var acquired = rows.getTimestamp("ACQUIRED");
                var ended = rows.getTimestamp("RELEASED");
                require(acquired != null && ended != null, "Missing native pulse times");
                var completion = new As400ShortBoundaryAudit.Completion(job, thread, acquired.toLocalDateTime(), ended.toLocalDateTime());
                require(!rows.next(), "Duplicate pulse completion");
                As400DcgNativeIo.noWarnings(rows, statement);
                check();
                released = true;
                accepted = true;
                return Optional.of(completion);
            }
        }
    }

    void finish(As400ShortBoundaryProgram program) throws SQLException {
        check();
        require(released && accepted && !finished && program.journal().equals(journal) && program.nonce().equals(nonce),
                "Undrained, reused or foreign pulse cannot close the audit");
        finished = true; // An uncertain forced write must not be repeated on this attempt.
        command(program.closing()); // Forced only after all metadata and release checks.
    }

    private void prepareBatches(As400ShortBoundaryProgram program) throws SQLException {
        // Bounded parameter records are populated before any allocation. No
        // generated SQL or JDBC call grows with the complete file inventory.
        try (var statement = holder.prepareStatement(As400ShortBoundaryProgram.INSERT_BATCH_SQL)) {
            statement.setQueryTimeout(5);
            int pending = 0;
            for (int i = 0; i < program.batchCount(); i++) {
                check();
                var batch = program.allocationBatch(i);
                statement.setInt(1, i);
                statement.setString(2, batch.allocate());
                statement.setString(3, batch.release());
                statement.addBatch();
                pending++;
                if (pending == 32 || i == program.batchCount() - 1) {
                    var counts = statement.executeBatch();
                    require(counts.length == pending && java.util.Arrays.stream(counts).allMatch(count -> count == 1),
                            "Incomplete allocation command ledger");
                    require(statement.getWarnings() == null && holder.getWarnings() == null, "Allocation command ledger warning");
                    statement.clearBatch();
                    pending = 0;
                }
            }
        }
        check();
    }

    private void requireNoAllocations(As400ShortBoundaryProgram program) throws SQLException {
        check();
        try (var observer = connections.open()) {
            check();
            // Reuse statement handles only within this fresh observer job. Every
            // file still gets both authority checks and a new lock observation;
            // neither results nor authority are cached between files or passes.
            try (var authority = observer.prepareStatement(COMMAND_SQL)) {
                authority.setQueryTimeout(5);
                check();
                try (var statement = observer.prepareStatement(LOCKS_SQL)) {
                    statement.setQueryTimeout(5);
                    statement.setMaxRows(1);
                    for (var target : program.targets()) {
                        check();
                        // The view can hide objects lacking authority. CHKOBJ
                        // verifies access in this observer job without granting it.
                        command(observer, authority, "CHKOBJ OBJ(QSYS/" + target.library() + ") OBJTYPE(*LIB) AUT(*EXECUTE)");
                        command(observer, authority, "CHKOBJ OBJ(" + target.library() + "/" + target.file() + ") OBJTYPE(*FILE) AUT(*OBJOPR *READ)");
                        statement.setString(1, target.library());
                        statement.setString(2, target.file());
                        statement.setString(3, job);
                        check();
                        try (var rows = statement.executeQuery()) {
                            As400DcgNativeIo.noWarnings(rows, statement);
                            require(!rows.next(), "Native pulse allocation remains held or predates this attempt");
                            As400DcgNativeIo.noWarnings(rows, statement);
                        }
                        check();
                        // Re-execution clears statement warnings. Inspect them
                        // after cursor close, before the handle can be reused.
                        require(statement.getWarnings() == null && observer.getWarnings() == null, "Incomplete independent pulse lock observation");
                    }
                }
            }
            check();
            require(observer.getWarnings() == null, "Incomplete independent pulse lock observation");
        }
    }

    private void command(String command) throws SQLException {
        command(holder, command);
    }

    private void command(Connection connection, String command) throws SQLException {
        check();
        try (var statement = connection.prepareStatement(COMMAND_SQL)) {
            statement.setQueryTimeout(5);
            command(connection, statement, command);
        }
        check();
    }

    private void command(Connection connection, PreparedStatement statement, String command) throws SQLException {
        check();
        statement.setString(1, command);
        check();
        statement.execute();
        require(statement.getWarnings() == null && connection.getWarnings() == null, "Native command warning");
        check();
    }

    private void execute(String sql) throws SQLException {
        check();
        try (var statement = holder.prepareStatement(sql)) {
            statement.setQueryTimeout(5);
            statement.execute();
            require(statement.getWarnings() == null && holder.getWarnings() == null, "Native pulse statement warning");
        }
        check();
    }

    private void check() {
        checkRunning.run();
        require(!closed && !Thread.currentThread().isInterrupted(), "Native boundary pulse stopped");
    }

    @Override
    public void close() throws SQLException {
        closed = true;
        SQLException failure = null;
        try {
            holder.close(); // A pooled server job can retain *JOB locks after disconnect.
        }
        catch (SQLException error) {
            failure = error;
        }
        if (allocationStarted && !releaseVerified) {
            throw new UnverifiedCleanup(job, failure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
