/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/** Owns one synchronous native attempt; metadata and independent lock checks are outside the held call. */
final class As400NativeBoundaryPulse implements AutoCloseable {
    static final String HOLDER_SQL = "SELECT QSYS2.JOB_NAME AS JOB, HEX(QSYS2.THREAD_ID) AS THREAD_HEX FROM SYSIBM.SYSDUMMY1";
    static final String LOCKS_SQL = "SELECT JOB_NAME, LOCK_STATE, LOCK_STATUS, SYSTEM_TABLE_MEMBER FROM QSYS2.OBJECT_LOCK_INFO "
            + "WHERE SYSTEM_OBJECT_SCHEMA=? AND SYSTEM_OBJECT_NAME=? AND OBJECT_TYPE='*FILE' "
            + "AND JOB_NAME=? AND LOCK_STATE='*EXCLRD' AND LOCK_STATUS='HELD'";
    static final String RESULT_SQL = "SELECT ATTEMPT,SCOPE_DIGEST,ACQUIRED,RELEASED FROM SESSION.DCGPULSE";
    private final As400JdbcConnectionFactory connections;
    private final Runnable checkRunning;
    private final JournalReceiver journal;
    private final String nonce;
    private final Connection holder;
    private final String job;
    private final BigInteger thread;
    private boolean attempted;
    private boolean released;
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
        boolean success = true;
        try {
            // No client callback, query, force or metadata operation is reachable
            // between allocation and release: both happen inside this one call.
            execute(program.statement());
        }
        catch (SQLException error) {
            // This is the generated program's final signal, after its release
            // block. Network/API/compiler failures never imply completed drain.
            if (!"75001".equals(error.getSQLState()) || error.getNextException() != null) {
                throw error;
            }
            success = false;
        }
        check();
        requireNoAllocations(program);
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
                return Optional.of(completion);
            }
        }
    }

    void finish(As400ShortBoundaryProgram program) throws SQLException {
        check();
        require(released && !finished && program.journal().equals(journal) && program.nonce().equals(nonce), "Undrained, reused or foreign pulse cannot close the audit");
        finished = true; // An uncertain forced write must not be repeated on this attempt.
        command(program.closing()); // Forced only after all metadata and release checks.
    }

    private void requireNoAllocations(As400ShortBoundaryProgram program) throws SQLException {
        check();
        try (var observer = connections.open()) {
            for (var target : program.targets()) {
                check();
                // The documented view can hide objects lacking authority. Check
                // effective authority in this observer job, not the holder's job.
                // CHKOBJ only checks access; it grants nothing and reads no rows.
                command(observer, "CHKOBJ OBJ(QSYS/" + target.library() + ") OBJTYPE(*LIB) AUT(*EXECUTE)");
                command(observer, "CHKOBJ OBJ(" + target.library() + "/" + target.file() + ") OBJTYPE(*FILE) AUT(*OBJOPR *READ)");
                try (var statement = observer.prepareStatement(LOCKS_SQL)) {
                    statement.setQueryTimeout(5);
                    statement.setMaxRows(1);
                    statement.setString(1, target.library());
                    statement.setString(2, target.file());
                    statement.setString(3, job);
                    try (var rows = statement.executeQuery()) {
                        require(!rows.next(), "Native pulse allocation remains held or predates this attempt");
                        As400DcgNativeIo.noWarnings(rows, statement);
                    }
                }
            }
            require(observer.getWarnings() == null, "Incomplete independent pulse lock observation");
        }
    }

    private void command(String command) throws SQLException {
        command(holder, command);
    }

    private void command(Connection connection, String command) throws SQLException {
        check();
        try (var statement = connection.prepareStatement("CALL QSYS2.QCMDEXC(?)")) {
            statement.setQueryTimeout(5);
            statement.setString(1, command);
            statement.execute();
            require(statement.getWarnings() == null && connection.getWarnings() == null, "Native command warning");
        }
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
        holder.close(); // Called by the synchronous owner after its native call returns.
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
