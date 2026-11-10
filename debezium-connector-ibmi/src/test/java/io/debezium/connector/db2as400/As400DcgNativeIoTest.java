/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;

class As400DcgNativeIoTest {
    @Test
    void legacyJdbcConstructionFailureClosesItsAdmittedNativeConnection() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var raw = mock(Connection.class);
        var statement = mock(java.sql.Statement.class);
        when(raw.isValid(3)).thenReturn(true);
        when(raw.createStatement()).thenReturn(statement);
        when(statement.executeQuery("values ( CURRENT_SERVER )")).thenThrow(new SQLException("database identity unavailable"));
        var guarded = (Connection) As400DcgNativeIo.guard(raw, lifecycle, lifecycle.enter());
        var config = new As400ConnectorConfig(Configuration.from(As400DcgConfigTest.properties()));
        assertThrows(RuntimeException.class, () -> new As400JdbcConnection(config.getJdbcConfig(), ignored -> guarded));
        verify(raw).close();
        lifecycle.requestStop();
        lifecycle.awaitDrained();
        clearInvocations(raw, statement);
        assertThrows(SQLException.class, () -> guarded.createStatement());
        verifyNoInteractions(raw, statement);
    }

    @Test
    void failedSetupAndFailedCleanupCannotReportSuccessfulDrain() throws Exception {
        var raw = mock(Connection.class);
        doThrow(new SQLException("setup failed")).when(raw).setTransactionIsolation(Connection.TRANSACTION_NONE);
        doThrow(new SQLException("close failed")).when(raw).close();
        try (var drivers = org.mockito.Mockito.mockConstruction(com.ibm.as400.access.AS400JDBCDriver.class,
                (driver, context) -> when(driver.connect(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(raw))) {
            var io = new As400DcgNativeIo(new As400DcgConfig(Configuration.from(As400DcgConfigTest.properties())));
            var setup = assertThrows(SQLException.class, () -> io.connection(false));
            assertThat(setup.getMessage()).isEqualTo("setup failed");
            assertThat(setup.getSuppressed()).hasSize(1);
            var drain = assertThrows(IllegalStateException.class, io::close);
            assertThat(drain.getMessage()).contains("drain unverified");
            assertThat(drain.getCause().getMessage()).isEqualTo("close failed");
            assertThat(io.lifecycle.isStopped()).isTrue();
            assertThrows(SQLException.class, () -> io.connection(false));
            verify(raw).close();
            assertThat(drivers.constructed()).hasSize(1);
        }
    }

    @Test
    void isolatedPluginUsesItsOwnDriverWithoutDependingOnGlobalSpiDiscovery() throws Exception {
        var raw = mock(Connection.class);
        var statement = mock(java.sql.Statement.class);
        var rows = mock(ResultSet.class);
        when(raw.createStatement()).thenReturn(statement);
        when(statement.executeQuery("SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")).thenReturn(rows);
        when(rows.next()).thenReturn(true, false);
        when(rows.getString(1)).thenReturn("TESTDB");
        try (var drivers = org.mockito.Mockito.mockConstruction(com.ibm.as400.access.AS400JDBCDriver.class,
                (driver, context) -> when(driver.connect(org.mockito.ArgumentMatchers.anyString(), org.mockito.ArgumentMatchers.any())).thenReturn(raw));
                var io = new As400DcgNativeIo(new As400DcgConfig(Configuration.from(As400DcgConfigTest.properties())));
                var connection = io.connection(false)) {
            assertThat(drivers.constructed()).hasSize(1);
            verify(drivers.constructed().get(0)).connect(org.mockito.ArgumentMatchers.eq("jdbc:as400://unreachable.invalid"),
                    org.mockito.ArgumentMatchers.argThat(p -> "false".equals(p.getProperty("prompt")) && "15000".equals(p.getProperty("socket timeout"))
                            && "15".equals(p.getProperty("login timeout")) && "false".equals(p.getProperty("lazy close"))));
        }
        verify(raw).close();
    }

    @Test
    void terminalStopRejectsEveryNewNativeOperationAndHandleEscape() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var raw = mock(Connection.class);
        var statement = mock(PreparedStatement.class);
        var rows = mock(ResultSet.class);
        when(raw.prepareStatement("SELECT 1")).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        var guarded = (Connection) As400DcgNativeIo.guard(raw, lifecycle, lifecycle.enter());
        var prepared = guarded.prepareStatement("SELECT 1");
        var result = prepared.executeQuery();
        when(statement.getConnection()).thenReturn(raw);
        assertThrows(SQLException.class, prepared::getConnection);
        assertThrows(SQLException.class, () -> guarded.unwrap(Connection.class));
        clearInvocations(raw, statement, rows);
        lifecycle.requestStop();
        assertThrows(SQLException.class, () -> guarded.prepareStatement("SELECT 2"));
        assertThrows(SQLException.class, prepared::executeQuery);
        assertThrows(SQLException.class, result::next);
        verifyNoInteractions(raw, statement, rows);
        prepared.cancel();
        result.close();
        prepared.close();
        guarded.rollback();
        guarded.close();
        lifecycle.awaitDrained();
        verify(statement).cancel();
        verify(raw).rollback();
        verify(raw).close();
    }

    @Test
    void metadataHandlesRemainInsideTerminalAdmission() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var raw = mock(Connection.class);
        var metadata = mock(java.sql.DatabaseMetaData.class);
        var rows = mock(ResultSet.class);
        var columns = mock(java.sql.ResultSetMetaData.class);
        when(raw.getMetaData()).thenReturn(metadata);
        when(metadata.getTables(null, null, null, null)).thenReturn(rows);
        when(rows.getMetaData()).thenReturn(columns);
        var connection = (Connection) As400DcgNativeIo.guard(raw, lifecycle, lifecycle.enter());
        var guardedMetadata = connection.getMetaData();
        var guardedRows = guardedMetadata.getTables(null, null, null, null);
        var guardedColumns = guardedRows.getMetaData();
        clearInvocations(raw, metadata, rows, columns);
        lifecycle.requestStop();
        assertThrows(SQLException.class, () -> guardedMetadata.getTables(null, null, null, null));
        assertThrows(SQLException.class, guardedColumns::getColumnCount);
        verifyNoInteractions(raw, metadata, rows, columns);
        guardedRows.close();
        connection.close();
        lifecycle.awaitDrained();
    }

    @Test
    void lateMetadataCannotStartReadingAfterStopWhileReplacementProgresses() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var raw = mock(Connection.class);
        var statement = mock(PreparedStatement.class);
        var rows = mock(ResultSet.class);
        var started = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var draining = new CountDownLatch(1);
        when(raw.prepareStatement("SELECT 1")).thenReturn(statement);
        when(statement.executeQuery()).thenAnswer(invocation -> {
            started.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            return rows;
        });
        var guarded = (Connection) As400DcgNativeIo.guard(raw, lifecycle, lifecycle.enter());
        var prepared = guarded.prepareStatement("SELECT 1");
        var pool = Executors.newFixedThreadPool(2);
        try {
            var pending = pool.submit((java.util.concurrent.Callable<ResultSet>) prepared::executeQuery);
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            lifecycle.requestStop();
            prepared.cancel();
            var drain = pool.submit(() -> {
                draining.countDown();
                lifecycle.awaitDrained();
            });
            assertThat(draining.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(drain.isDone()).isFalse();
            var replacement = new JournalReadLifecycle();
            try (var lease = replacement.enter()) {
                replacement.checkOpen();
            }
            release.countDown();
            var late = pending.get(5, TimeUnit.SECONDS);
            assertThrows(SQLException.class, late::next);
            verifyNoInteractions(rows);
            late.close();
            prepared.close();
            guarded.close();
            drain.get(5, TimeUnit.SECONDS);
        }
        finally {
            release.countDown();
            guarded.close();
            pool.shutdownNow();
        }
    }

    @Test
    void failedNativeCloseDoesNotClaimDrain() throws Exception {
        var lifecycle = new JournalReadLifecycle();
        var raw = mock(Connection.class);
        doThrow(new SQLException("still open")).doNothing().when(raw).close();
        var guarded = (Connection) As400DcgNativeIo.guard(raw, lifecycle, lifecycle.enter());
        lifecycle.requestStop();
        assertThrows(SQLException.class, guarded::close);
        var pool = Executors.newSingleThreadExecutor();
        var started = new CountDownLatch(1);
        try {
            var drain = pool.submit(() -> {
                started.countDown();
                lifecycle.awaitDrained();
            });
            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(drain.isDone()).isFalse();
            guarded.close();
            drain.get(5, TimeUnit.SECONDS);
        }
        finally {
            guarded.close();
            pool.shutdownNow();
        }
    }
}
