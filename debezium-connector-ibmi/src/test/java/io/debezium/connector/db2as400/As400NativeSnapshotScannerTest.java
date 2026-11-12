/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class As400NativeSnapshotScannerTest {
    @TempDir
    Path directory;

    @Test
    void shouldSnapshotKeylessRowsInPhysicalOrderWithoutAddingAKeyColumn() throws Exception {
        var original = new As400RecoveryTestSupport.Fixture(directory).definitions.get(0);
        var definition = new As400BindingDefinition(original.identity(), original.schema().edit().setPrimaryKeyNames(List.of()).create(), original.codecs());
        var connection = mock(Connection.class);
        var statement = mock(Statement.class);
        var rows = mock(ResultSet.class);
        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(rows);
        when(rows.next()).thenReturn(true, true, false);
        when(rows.getObject(1)).thenReturn(7, 7);
        var observed = new ArrayList<Object[]>();
        var completed = new AtomicBoolean();
        new As400NativeSnapshotScanner(connection, new Object(), () -> {
        }, () -> {
        }, s -> {
        }, new As400SnapshotChunks.Limits(1, 1000, 100, 1000)).scan(List.of(definition), new As400SnapshotChunks.Consumer() {
            @Override
            public void chunk(io.debezium.relational.TableId table, List<As400TaskInitialization.Row> chunk) {
                chunk.forEach(row -> observed.add(row.values().values()));
            }

            @Override
            public void complete(io.debezium.relational.TableId table) {
                completed.set(true);
            }
        });
        assertThat(completed).isTrue();
        assertThat(observed).hasSize(2).allSatisfy(row -> assertThat(row).containsExactly(7));
        var query = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(statement).executeQuery(query.capture());
        assertThat(query.getValue()).endsWith(" T ORDER BY RRN(T)").doesNotContain("CAST(RRN(T) AS BIGINT)");
        verify(rows).close();
        verify(statement).close();
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid-gaps", "duplicate", "reordered", "zero", "null", "negative" })
    void rrnSnapshotUsesNativeRecordNumbersAndRejectsAmbiguousIdentity(String scenario) throws Exception {
        var original = new As400RecoveryTestSupport.Fixture(directory).definitions.get(0);
        var physical = original.schema().edit().setPrimaryKeyNames(java.util.List.of())
                .addAttribute(io.debezium.relational.Attribute.editor().name(As400BindingDefinition.KEY_MODE).value("rrn").create()).create();
        var definition = new As400BindingDefinition(original.identity(), physical, original.codecs());
        var connection = mock(Connection.class);
        var statement = mock(Statement.class);
        var result = mock(ResultSet.class);
        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(result);
        when(result.next()).thenReturn(true, true, false);
        when(result.getObject(1)).thenReturn(7);
        when(result.getLong(2)).thenReturn(2L, 9L);
        switch (scenario) {
            case "duplicate" -> when(result.getLong(2)).thenReturn(2L, 2L);
            case "reordered" -> when(result.getLong(2)).thenReturn(9L, 2L);
            case "zero" -> when(result.getLong(2)).thenReturn(0L);
            case "negative" -> when(result.getLong(2)).thenReturn(-1L);
            case "null" -> when(result.wasNull()).thenReturn(true);
        }
        var complete = new AtomicBoolean();
        List<Long> observed = new ArrayList<>();
        var scanner = new As400NativeSnapshotScanner(connection, new Object(), () -> {
        }, () -> {
        }, s -> {
        },
                new As400SnapshotChunks.Limits(1, 1000, 100, 1000));
        var consumer = new As400SnapshotChunks.Consumer() {
            @Override
            public void chunk(io.debezium.relational.TableId table, List<As400TaskInitialization.Row> rows) {
                rows.forEach(row -> observed.add((Long) row.values().values()[1]));
            }

            @Override
            public void complete(io.debezium.relational.TableId table) {
                complete.set(true);
            }
        };
        if (scenario.equals("valid-gaps")) {
            scanner.scan(List.of(definition), consumer);
            assertThat(observed).containsExactly(2L, 9L);
            assertThat(complete).isTrue();
        }
        else {
            assertThrows(IllegalStateException.class, () -> scanner.scan(List.of(definition), consumer));
            assertThat(complete).isFalse();
        }
        var query = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(statement).executeQuery(query.capture());
        assertThat(query.getValue()).contains("CAST(RRN(T) AS BIGINT)").endsWith(" T ORDER BY RRN(T)");
        verify(result).close();
        verify(statement).close();
    }

    @Test
    void stoppedHeldFetchIsCancelledAndCannotCompleteWhileReplacementProgresses() throws Exception {
        var definition = new As400RecoveryTestSupport.Fixture(directory).definitions.get(0);
        var lifecycle = new io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle();
        var connection = mock(Connection.class);
        var statement = mock(Statement.class);
        var result = mock(ResultSet.class);
        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(result);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var cancelled = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        when(result.next()).thenAnswer(i -> {
            entered.countDown();
            assertThat(cancelled.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(release.await(5, TimeUnit.SECONDS)).isTrue();
            return true;
        });
        org.mockito.Mockito.doAnswer(i -> {
            cancelled.countDown();
            return null;
        }).when(statement).cancel();
        var guarded = (Connection) As400ContinuousConnectSource.guard(connection, lifecycle, lifecycle.enter());
        var active = new java.util.concurrent.atomic.AtomicReference<Statement>();
        var stopped = new AtomicBoolean();
        var output = new AtomicInteger();
        var scanner = new As400NativeSnapshotScanner(guarded, new Object(), lifecycle::checkOpen, () -> {
            if (stopped.get()) {
                throw new IllegalStateException("terminal stop");
            }
        }, active::set, new As400SnapshotChunks.Limits(256, 1000, 100, 1000));
        var read = CompletableFuture.runAsync(() -> assertThrows(Exception.class, () -> scanner.scan(List.of(definition), new As400SnapshotChunks.Consumer() {
            @Override
            public void chunk(io.debezium.relational.TableId table, List<As400TaskInitialization.Row> rows) {
                output.incrementAndGet();
            }

            @Override
            public void complete(io.debezium.relational.TableId table) {
                output.incrementAndGet();
            }
        })));
        try {
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            stopped.set(true);
            lifecycle.requestStop();
            active.get().cancel();
            var drain = CompletableFuture.runAsync(lifecycle::awaitDrained);
            var replacement = new io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle();
            try (var lease = replacement.enter()) {
                replacement.checkOpen();
            }
            assertThat(read.isDone()).isFalse();
            assertThat(drain.isDone()).isFalse();
            release.countDown();
            read.get(5, TimeUnit.SECONDS);
            guarded.close();
            drain.get(5, TimeUnit.SECONDS);
            assertThat(output).hasValue(0);
            verify(result).next();
            verify(result).close();
            verify(statement).close();
        }
        finally {
            cancelled.countDown();
            release.countDown();
            guarded.close();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3, 513 })
    void byteLookaheadPreservesRowsAndEofRequiresClose(int count) throws Exception {
        var f = new As400RecoveryTestSupport.Fixture(directory);
        var definition = f.definitions.get(0);
        var connection = mock(Connection.class);
        var statement = mock(Statement.class);
        var result = mock(ResultSet.class);
        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(result);
        var index = new AtomicInteger();
        when(result.next()).thenAnswer(i -> index.incrementAndGet() <= count);
        when(result.getObject(1)).thenAnswer(i -> index.get());
        List<Integer> observed = new ArrayList<>();
        Object io = new Object();
        var validations = new AtomicInteger();
        var complete = new AtomicBoolean();
        var scanner = new As400NativeSnapshotScanner(connection, io, validations::incrementAndGet, () -> {
        }, s -> {
        }, new As400SnapshotChunks.Limits(256, 16, Math.max(1, count), Math.max(1, count * 8L)));
        scanner.scan(List.of(definition), new As400SnapshotChunks.Consumer() {
            @Override
            public void chunk(io.debezium.relational.TableId table, List<As400TaskInitialization.Row> rows) throws Exception {
                assertThat(rows).hasSizeLessThanOrEqualTo(2);
                rows.forEach(row -> observed.add((Integer) row.values().values()[0]));
                // Publication/commit probes on another thread can acquire native
                // serialization while the scanner waits on the consumer.
                CompletableFuture.runAsync(() -> {
                    synchronized (io) {
                        validations.incrementAndGet();
                    }
                }).get(2, TimeUnit.SECONDS);
            }

            @Override
            public void complete(io.debezium.relational.TableId table) throws Exception {
                verify(result).close();
                verify(statement).close();
                complete.set(true);
            }
        });
        assertThat(observed).containsExactlyElementsOf(java.util.stream.IntStream.rangeClosed(1, count).boxed().toList());
        assertThat(complete).isTrue();
        verify(statement).setMaxRows(0);
        verify(statement).setQueryTimeout(5);
    }

    @ParameterizedTest
    @ValueSource(strings = { "fetch", "warning", "close", "row-size", "total", "native-loss", "stop" })
    void uncertainNativeReadNeverReportsComplete(String fault) throws Exception {
        var definition = new As400RecoveryTestSupport.Fixture(directory).definitions.get(0);
        var connection = mock(Connection.class);
        var statement = mock(Statement.class);
        var result = mock(ResultSet.class);
        when(connection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(result);
        when(result.next()).thenReturn(true, true, false);
        when(result.getObject(1)).thenReturn(1);
        switch (fault) {
            case "fetch" -> when(result.next()).thenThrow(new java.sql.SQLException("lost"));
            case "warning" -> when(result.getWarnings()).thenReturn(new java.sql.SQLWarning("truncated"));
            case "close" -> doThrow(new java.sql.SQLException("close uncertain")).when(result).close();
            case "row-size" -> when(result.getObject(1)).thenReturn(new byte[65537]);
        }
        var complete = new AtomicBoolean();
        var validations = new AtomicInteger();
        var scanner = new As400NativeSnapshotScanner(connection, new Object(), () -> {
            if (fault.equals("native-loss") && validations.incrementAndGet() == 3) {
                throw new IllegalStateException("lost session after read");
            }
        }, () -> {
            if (fault.equals("stop")) {
                throw new IllegalStateException("stopped");
            }
        }, s -> {
        }, new As400SnapshotChunks.Limits(256, 1000, fault.equals("total") ? 1 : 100, 1000));
        assertThrows(Exception.class, () -> scanner.scan(List.of(definition), new As400SnapshotChunks.Consumer() {
            @Override
            public void chunk(io.debezium.relational.TableId table, List<As400TaskInitialization.Row> rows) {
            }

            @Override
            public void complete(io.debezium.relational.TableId table) {
                complete.set(true);
            }
        }));
        assertThat(complete).isFalse();
        verify(statement).close();
    }
}
