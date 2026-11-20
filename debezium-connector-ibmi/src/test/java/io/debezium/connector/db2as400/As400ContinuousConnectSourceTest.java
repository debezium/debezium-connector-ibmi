/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;

class As400ContinuousConnectSourceTest {
    @TempDir
    Path evidence;

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldDisconnectRpcAndReportCloseFailureWhenStoppedDuringConnectionSetup(boolean closeFails) throws Exception {
        var raw = mock(Connection.class);
        var statement = mock(Statement.class);
        var rows = mock(ResultSet.class);
        when(raw.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")).thenReturn(rows);
        when(rows.next()).thenReturn(true);
        var closeFailure = new SQLException("close failed after stop");
        if (closeFails) {
            doThrow(closeFailure).when(raw).close();
        }
        try (var transports = mockConstruction(AS400.class);
                var driver = mockStatic(DriverManager.class)) {
            driver.when(() -> DriverManager.getConnection(anyString(), any(Properties.class))).thenReturn(raw);
            var source = source();
            when(rows.getString(1)).thenAnswer(call -> {
                source.lifecycle().requestStop();
                return "TESTDB";
            });

            var openingFailure = assertThrows(SQLException.class, () -> source.connection(false));
            verify(raw).close();
            if (closeFails) {
                assertThat((Throwable) openingFailure).isSameAs(closeFailure);
                assertThat(assertThrows(IllegalStateException.class, source::close))
                        .hasMessageContaining("drain unverified").hasCause(closeFailure);
            }
            else {
                assertThat((Throwable) openingFailure).hasMessageContaining("stopped during connection");
                source.close();
            }
            verify(transports.constructed().get(0)).disconnectAllServices();
            assertThrows(SQLException.class, () -> source.connection(false));
            driver.verify(() -> DriverManager.getConnection(anyString(), any(Properties.class)));
        }
    }

    @Test
    void shouldDisconnectRpcWhenAnAdmittedJdbcConnectionFailsToClose() throws Exception {
        try (var transports = mockConstruction(AS400.class)) {
            var source = source();
            var raw = mock(Connection.class);
            var closeFailure = new SQLException("admitted close failed");
            doThrow(closeFailure).when(raw).close();
            var connection = (Connection) As400ContinuousConnectSource.guard(raw, source.lifecycle(), source.lifecycle().enter());

            assertThat((Throwable) assertThrows(SQLException.class, connection::close)).isSameAs(closeFailure);
            assertThat(assertThrows(IllegalStateException.class, source::close))
                    .hasMessageContaining("drain unverified").hasCause(closeFailure);
            verify(transports.constructed().get(0)).disconnectAllServices();
            assertThrows(SQLException.class, () -> source.connection(false));
        }
    }

    private As400ContinuousConnectSource source() throws Exception {
        var properties = new Properties();
        properties.setProperty("database.hostname", "unused.invalid");
        properties.setProperty("database.user", "test");
        properties.setProperty("database.password", "test");
        properties.setProperty("database.dbname", "TESTDB");
        return new As400ContinuousConnectSource(properties, "DCNC0000", evidence, "127.0.0.1:9092", "test-history");
    }

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
