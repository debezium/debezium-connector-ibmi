/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;

class As400ContinuousConnectSourceTest {
    @Test
    void stoppedMetadataConnectionCannotStartStatementsOrEscapeThroughRawHandles() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var delegate = mock(Connection.class);
        var connection = (Connection) As400ContinuousConnectSource.guard(delegate, lifecycle, lifecycle.enter());
        assertThrows(SQLException.class, () -> connection.unwrap(Connection.class));
        lifecycle.requestStop();
        assertThrows(SQLException.class, connection::createStatement);
        verify(delegate, never()).createStatement();
        connection.rollback();
        connection.close();
        lifecycle.awaitDrained();
        verify(delegate).rollback();
        verify(delegate).close();
    }

    @Test
    void alreadyPreparedStatementCannotStartExecutionAfterTerminalStop() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var delegate = mock(Connection.class);
        var statement = mock(Statement.class);
        when(delegate.createStatement()).thenReturn(statement);
        var connection = (Connection) As400ContinuousConnectSource.guard(delegate, lifecycle, lifecycle.enter());
        var guarded = connection.createStatement();
        lifecycle.requestStop();
        assertThrows(SQLException.class, () -> guarded.execute("SELECT 1"));
        verify(statement, never()).execute("SELECT 1");
        guarded.close();
        connection.close();
        lifecycle.awaitDrained();
    }

    @Test
    void terminalStopStillAllowsCancellationOfAlreadyAdmittedSnapshotWork() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var statement = mock(Statement.class);
        var guarded = (Statement) As400ContinuousConnectSource.guard(statement, lifecycle, null);
        lifecycle.requestStop();
        guarded.cancel();
        verify(statement).cancel();
        assertThrows(SQLException.class, () -> guarded.executeQuery("SELECT 1"));
        verify(statement, never()).executeQuery("SELECT 1");
        guarded.close();
        lifecycle.awaitDrained();
    }

    @Test
    void admittedMetadataCallMustDrainWhileReplacementMakesIndependentProgress() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var statement = mock(Statement.class);
        when(statement.execute("SELECT 1")).thenAnswer(call -> {
            entered.countDown();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return true;
        });
        var guarded = (Statement) As400ContinuousConnectSource.guard(statement, lifecycle, null);
        var threads = Executors.newFixedThreadPool(2);
        try {
            var read = threads.submit(() -> guarded.execute("SELECT 1"));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            lifecycle.requestStop();
            var drain = threads.submit(lifecycle::awaitDrained);
            try {
                var replacement = new JournalReadLifecycle();
                try (var lease = replacement.enter()) {
                    replacement.checkOpen();
                }
                assertThat(read.isDone()).isFalse();
                assertThat(drain.isDone()).isFalse();
                assertThrows(SQLException.class, () -> guarded.execute("SELECT 2"));
            }
            finally {
                release.countDown();
            }
            assertThat(read.get(5, TimeUnit.SECONDS)).isTrue();
            drain.get(5, TimeUnit.SECONDS);
            verify(statement, never()).execute("SELECT 2");
        }
        finally {
            release.countDown();
            threads.shutdownNow();
        }
    }
}
