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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400OperatorJournalEvidenceTest {
    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 32 })
    void shouldReadSystemBasicReceiversEvenWhenTheirStoragePoolDiffersFromTheJournal(int asp) throws Exception {
        var f = new Fixture();
        f.data.get(0).put("ASP_NUMBER", 32);
        f.data.get(1).put("JOURNAL_RECEIVER_ASP_NUMBER", asp);
        assertThat(f.collect().receivers()).hasSize(1);
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "before", "after", "unknown", "chained", "wrong-state", "wrong-code" })
    void shouldDiscardTheEntireObservationWhenNativeMetadataReturnsAWarning(String fault) throws Exception {
        var f = new Fixture();
        var warning = new SQLWarning("Native metadata incomplete", fault.equals("wrong-state") ? "01000" : "01548",
                fault.equals("wrong-code") ? 0 : 462);
        if (fault.equals("chained")) {
            warning.setNextWarning(new SQLWarning("Another warning"));
        }
        if (fault.equals("unknown")) {
            warning = new SQLWarning("Unknown warning");
        }
        if (fault.equals("after")) {
            when(f.statements.get(1).getWarnings()).thenReturn(null, null, warning);
        }
        else {
            when(f.statements.get(0).getWarnings()).thenReturn(null, warning);
        }
        var failure = assertThrows(SQLException.class, f::collect);
        if (List.of("before", "after").contains(fault)) {
            assertThat((Throwable) failure).isInstanceOf(As400OperatorJournalEvidence.IncompleteObservationException.class);
            assertThat(failure.getSQLState()).isEqualTo("01548");
            assertThat(failure.getErrorCode()).isEqualTo(462);
        }
        else {
            assertThat((Throwable) failure).isNotInstanceOf(As400OperatorJournalEvidence.IncompleteObservationException.class);
        }
        assertThat(failure.getCause()).isSameAs(warning);
        int opened = fault.equals("after") ? 2 : 1;
        for (int i = 0; i < opened; i++) {
            verify(f.statements.get(i)).close();
            verify(f.rows.get(i)).close();
        }
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid", "missing", "extra", "duplicate", "changed-count", "changed-attachment", "missing-count", "fractional-count" })
    void shouldUseTheObservedNativeCountAndRejectIncompleteOrChangedLargeDirectories(String fault) throws Exception {
        int count = 5000;
        var f = new Fixture();
        var journal = f.data.get(0);
        journal.put("NUMBER_JOURNAL_RECEIVERS", count);
        journal.put("ATTACHED_JOURNAL_RECEIVER_NAME", "R" + (count - 1));
        var after = new HashMap<>(journal);
        if (fault.equals("changed-count")) {
            after.put("NUMBER_JOURNAL_RECEIVERS", count + 1);
        }
        if (fault.equals("changed-attachment")) {
            after.put("ATTACHED_JOURNAL_RECEIVER_NAME", "OTHER");
        }
        if (fault.equals("missing-count")) {
            journal.remove("NUMBER_JOURNAL_RECEIVERS");
        }
        if (fault.equals("fractional-count")) {
            journal.put("NUMBER_JOURNAL_RECEIVERS", "1.5");
        }
        f.append(after);
        int returned = count + (fault.equals("extra") ? 1 : fault.equals("missing") ? -1 : 0);
        var at = new AtomicInteger(-1);
        when(f.rows.get(1).next()).thenAnswer(call -> at.incrementAndGet() < returned);
        when(f.rows.get(1).getString(anyString())).thenAnswer(call -> {
            String field = call.getArgument(0);
            return switch (field) {
                case "JOURNAL_RECEIVER_NAME" -> "R" + (fault.equals("duplicate") ? 0 : at.get());
                case "STATUS" -> at.get() == count - 1 ? "ATTACHED" : "ONLINE";
                case "PREVIOUS_JOURNAL_RECEIVER" -> at.get() == 0 ? null : "R" + (at.get() - 1);
                case "PREVIOUS_JOURNAL_RECEIVER_LIBRARY" -> at.get() == 0 ? null : "LIB";
                case "NEXT_JOURNAL_RECEIVER" -> at.get() == count - 1 ? null : "R" + (at.get() + 1);
                case "NEXT_JOURNAL_RECEIVER_LIBRARY" -> at.get() == count - 1 ? null : "LIB";
                default -> f.data.get(1).get(field);
            };
        });
        when(f.rows.get(1).getBigDecimal(anyString())).thenAnswer(call -> switch ((String) call.getArgument(0)) {
            case "FIRST_SEQUENCE_NUMBER" -> BigDecimal.valueOf(at.get() * 10L + 1);
            case "LAST_SEQUENCE_NUMBER" -> BigDecimal.valueOf((at.get() + 1L) * 10);
            default -> BigDecimal.ONE;
        });
        var collector = As400OperatorJournalEvidence.directory(() -> f.connection, new JournalReceiver("JRN", "LIB"));
        if (fault.equals("valid")) {
            var observed = collector.collect();
            assertThat(observed.receivers()).hasSize(count);
            assertThat(As400OperatorCutReader.chain(observed, new JournalReceiver("R0", "LIB"))).hasSize(count);
            verify(f.statements.get(1)).setMaxRows(count + 1);
            verify(f.statements.get(1)).setFetchSize(128);
            f.closed();
        }
        else {
            assertThrows(Exception.class, collector::collect);
            verify(f.connection).close();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "NO", "YES", "UNKNOWN", "null", "missing", "duplicate", "wrong-journal", "warning" })
    void shouldValidateRetentionOnTheExactDirectoryRowBeforeReadingReceivers(String fault) throws Exception {
        var f = new Fixture();
        f.data.get(0).put("DELETE_RECEIVER_OPTION", fault.equals("null") ? null : List.of("YES", "UNKNOWN").contains(fault) ? fault : "NO");
        switch (fault) {
            case "missing" -> when(f.rows.get(0).next()).thenReturn(false);
            case "duplicate" -> when(f.rows.get(0).next()).thenReturn(true);
            case "wrong-journal" -> f.data.get(0).put("JOURNAL_NAME", "OTHER");
            case "warning" -> when(f.rows.get(0).getWarnings()).thenReturn(new SQLWarning("Incomplete observation"));
        }
        var collector = As400OperatorJournalEvidence.retained(() -> f.connection, new JournalReceiver("JRN", "LIB"),
                new As400OperatorJournalEvidence.Limits(2, 5));
        if (fault.equals("NO")) {
            assertThat(collector.collect().receivers()).hasSize(1);
        }
        else {
            assertThrows(SQLException.class, collector::collect);
            verify(f.connection, never()).prepareStatement(org.mockito.ArgumentMatchers.startsWith(As400OperatorJournalEvidence.RECEIVERS_SQL));
        }
        verify(f.connection).prepareStatement(As400OperatorJournalEvidence.RETAINED_JOURNAL_SQL);
        verify(f.statements.get(0)).setString(1, "LIB");
        verify(f.statements.get(0)).setString(2, "JRN");
        verify(f.statements.get(0)).setMaxRows(2);
        verify(f.statements.get(0)).setQueryTimeout(5);
        verify(f.statements.get(0)).close();
        verify(f.rows.get(0)).close();
        verify(f.connection).close();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldTraverseOnlyTheRequiredNativeChainAndRejectAnUnderreportedCount(boolean underreported) throws Exception {
        var f = new Fixture();
        int count = 129;
        f.data.get(0).put("NUMBER_JOURNAL_RECEIVERS", underreported ? count - 1 : count);
        f.data.get(0).put("ATTACHED_JOURNAL_RECEIVER_NAME", "R" + (count - 1));
        for (int i = 0; i < count; i++) {
            var row = i == 0 ? f.data.get(1) : new HashMap<>(f.data.get(1));
            row.put("JOURNAL_RECEIVER_NAME", "R" + i);
            row.put("STATUS", i == count - 1 ? "ATTACHED" : "ONLINE");
            row.put("FIRST_SEQUENCE_NUMBER", i * 10L + 1);
            row.put("LAST_SEQUENCE_NUMBER", (i + 1L) * 10);
            row.put("PREVIOUS_JOURNAL_RECEIVER_LIBRARY", i == 0 ? null : "LIB");
            row.put("PREVIOUS_JOURNAL_RECEIVER", i == 0 ? null : "R" + (i - 1));
            row.put("NEXT_JOURNAL_RECEIVER_LIBRARY", i == count - 1 ? null : "LIB");
            row.put("NEXT_JOURNAL_RECEIVER", i == count - 1 ? null : "R" + (i + 1));
            if (i > 0) {
                f.append(row);
            }
        }
        var collector = As400OperatorJournalEvidence.retained(() -> f.connection, new JournalReceiver("JRN", "LIB"));
        if (underreported) {
            assertThrows(SQLException.class, () -> collector.collectFrom(new JournalReceiver("R0", "LIB")));
        }
        else {
            var observed = collector.collectFrom(new JournalReceiver("R0", "LIB"));
            assertThat(As400OperatorCutReader.chain(observed, new JournalReceiver("R0", "LIB"))).hasSize(count);
            for (var statement : f.statements) {
                verify(statement).setMaxRows(2);
            }
            f.closed();
        }
    }

    @Test
    void collectsBoundedReadOnlyCurrentDirectoryAndRequiresRecordingProfile() throws Exception {
        Fixture f = new Fixture();
        var observation = f.collect();
        assertThat(observation.attached()).isEqualTo(new JournalReceiver("RCV", "LIB"));
        assertThat(observation.receivers()).hasSize(1);
        observation.receivers().get(0).requireReadableProfile();
        for (PreparedStatement statement : f.statements) {
            verify(statement).setString(1, "LIB");
            verify(statement).setString(2, "JRN");
            verify(statement).setQueryTimeout(5);
            verify(statement).close();
            verify(statement, never()).executeUpdate();
        }
        verify(f.statements.get(0)).setMaxRows(2);
        verify(f.statements.get(1)).setMaxRows(3);
        f.closed();
    }

    @Test
    void exactAttachedObservationIgnoresUnneededHistoricalDirectory() throws Exception {
        Fixture f = new Fixture();
        var observation = new As400OperatorJournalEvidence(() -> f.connection, new JournalReceiver("JRN", "LIB"),
                new As400OperatorJournalEvidence.Limits(2, 5), true).collect();
        assertThat(observation.receivers()).hasSize(1);
        verify(f.connection).prepareStatement(As400OperatorJournalEvidence.RECEIVERS_SQL + " AND JOURNAL_RECEIVER_LIBRARY = ? AND JOURNAL_RECEIVER_NAME = ?");
        verify(f.statements.get(1)).setString(3, "LIB");
        verify(f.statements.get(1)).setString(4, "RCV");
        verify(f.statements.get(1)).setMaxRows(2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid", "missing", "duplicate", "wrong-name", "not-attached", "warning", "dangling-next" })
    void requiredReceiverIsSelectedExactlyAndFailsClosed(String fault) throws Exception {
        Fixture f = new Fixture();
        switch (fault) {
            case "missing" -> when(f.rows.get(1).next()).thenReturn(false);
            case "duplicate" -> when(f.rows.get(1).next()).thenReturn(true);
            case "wrong-name" -> f.data.get(1).put("JOURNAL_RECEIVER_NAME", "OTHER");
            case "not-attached" -> f.data.get(1).put("STATUS", "ONLINE");
            case "warning" -> when(f.rows.get(1).getWarnings()).thenReturn(new SQLWarning("Partial"));
            case "dangling-next" -> {
                f.data.get(1).put("NEXT_JOURNAL_RECEIVER", "NEXT");
                f.data.get(1).put("NEXT_JOURNAL_RECEIVER_LIBRARY", "LIB");
            }
            default -> {
            }
        }
        var collector = new As400OperatorJournalEvidence(() -> f.connection, new JournalReceiver("JRN", "LIB"), new As400OperatorJournalEvidence.Limits(2, 5));
        if (fault.equals("valid")) {
            assertThat(collector.collectFrom(new JournalReceiver("RCV", "LIB")).receivers()).hasSize(1);
        }
        else {
            assertThrows(SQLException.class, () -> collector.collectFrom(new JournalReceiver("RCV", "LIB")));
        }
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid", "backlink", "reset", "cycle", "bound" })
    void linkedCollectionRequiresReciprocalContinuousBoundedChain(String fault) throws Exception {
        Fixture f = new Fixture();
        f.data.get(0).put("ATTACHED_JOURNAL_RECEIVER_NAME", "NEXT");
        var first = f.data.get(1);
        first.put("STATUS", "ONLINE");
        first.put("NEXT_JOURNAL_RECEIVER", "NEXT");
        first.put("NEXT_JOURNAL_RECEIVER_LIBRARY", "LIB");
        var next = new HashMap<>(first);
        next.put("JOURNAL_RECEIVER_NAME", "NEXT");
        next.put("STATUS", "ATTACHED");
        next.put("FIRST_SEQUENCE_NUMBER", 100);
        next.put("LAST_SEQUENCE_NUMBER", 200);
        next.put("NEXT_JOURNAL_RECEIVER", null);
        next.put("NEXT_JOURNAL_RECEIVER_LIBRARY", null);
        next.put("PREVIOUS_JOURNAL_RECEIVER", "RCV");
        next.put("PREVIOUS_JOURNAL_RECEIVER_LIBRARY", "LIB");
        f.append(next);
        switch (fault) {
            case "backlink" -> next.put("PREVIOUS_JOURNAL_RECEIVER", "WRONG");
            case "reset" -> next.put("FIRST_SEQUENCE_NUMBER", 1);
            case "cycle" -> first.put("NEXT_JOURNAL_RECEIVER", "RCV");
            default -> {
            }
        }
        var collector = new As400OperatorJournalEvidence(() -> f.connection, new JournalReceiver("JRN", "LIB"),
                new As400OperatorJournalEvidence.Limits(fault.equals("bound") ? 1 : 2, 5));
        if (fault.equals("valid")) {
            assertThat(collector.collectFrom(new JournalReceiver("RCV", "LIB")).receivers()).extracting(r -> r.name().name()).containsExactly("RCV", "NEXT");
            verify(f.statements.get(2)).setString(4, "NEXT");
        }
        else {
            assertThrows(SQLException.class, () -> collector.collectFrom(new JournalReceiver("RCV", "LIB")));
        }
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void onlyProvenConcurrentAttachmentChangeIsRetryable(boolean changed) throws Exception {
        Fixture f = new Fixture();
        f.data.get(1).put("STATUS", "ONLINE");
        f.data.get(1).put("NEXT_JOURNAL_RECEIVER", "NEXT");
        f.data.get(1).put("NEXT_JOURNAL_RECEIVER_LIBRARY", "LIB");
        var refreshed = new HashMap<>(f.data.get(0));
        if (changed) {
            refreshed.put("ATTACHED_JOURNAL_RECEIVER_NAME", "NEXT");
        }
        f.append(refreshed);
        var collector = new As400OperatorJournalEvidence(() -> f.connection, new JournalReceiver("JRN", "LIB"), new As400OperatorJournalEvidence.Limits(2, 5));
        if (changed) {
            assertThrows(As400OperatorCutReader.UnstableCutException.class, () -> collector.collectFrom(new JournalReceiver("RCV", "LIB")));
        }
        else {
            assertThrows(SQLException.class, () -> collector.collectFrom(new JournalReceiver("RCV", "LIB")));
        }
        f.closed();
    }

    static Stream<Arguments> invalid() {
        List<Arguments> values = new ArrayList<>();
        for (String field : List.of("JOURNAL_LIBRARY", "JOURNAL_NAME", "JOURNAL_TYPE", "JOURNAL_STATE", "JOURNAL_ASPGRP",
                "REMOVE_INTERNAL_ENTRIES", "REMOVE_FIXED_LENGTH_DETAIL", "MINIMIZE_ESD_FOR_FILES", "JOURNAL_CACHE")) {
            values.add(Arguments.of(0, field, null));
            values.add(Arguments.of(0, field, "UNKNOWN"));
        }
        for (int row : List.of(0, 1)) {
            String asp = row == 0 ? "ASP_NUMBER" : "JOURNAL_RECEIVER_ASP_NUMBER";
            for (Object value : new Object[]{ null, 0, 33, new BigDecimal("1.5") }) {
                values.add(Arguments.of(row, asp, value));
            }
        }
        for (String field : List.of("JOURNAL_LIBRARY", "JOURNAL_NAME")) {
            values.add(Arguments.of(1, field, "OTHER"));
        }
        for (String field : List.of("STATUS", "MINIMIZE_ESD_FOR_FILES", "FIXED_LENGTH_DATA_INCLUDES_JOB_NAME", "FIXED_LENGTH_DATA_INCLUDES_THREAD_ID",
                "FIXED_LENGTH_DATA_INCLUDES_LOGICAL_UNIT_OF_WORK_ID", "FIXED_LENGTH_DATA_INCLUDES_TRANSACTION_ID")) {
            values.add(Arguments.of(1, field, null));
            values.add(Arguments.of(1, field, "UNKNOWN"));
        }
        for (String field : List.of("FIRST_SEQUENCE_NUMBER", "LAST_SEQUENCE_NUMBER")) {
            for (Object value : new Object[]{ null, 0, -1, new BigDecimal("1.5"), new BigDecimal("18446744073709551616") }) {
                values.add(Arguments.of(1, field, value));
            }
        }
        values.add(Arguments.of(1, "FIRST_SEQUENCE_NUMBER", 100));
        values.add(Arguments.of(1, "PREVIOUS_JOURNAL_RECEIVER", "RCV0")); // Incomplete pair.
        values.add(Arguments.of(1, "NEXT_JOURNAL_RECEIVER_LIBRARY", "LIB"));
        return values.stream();
    }

    @ParameterizedTest
    @MethodSource("invalid")
    void rejectsIncompleteUnsupportedOrAmbiguousEvidence(int row, String field, Object value) throws Exception {
        Fixture f = new Fixture();
        f.data.get(row).put(field, value);
        assertThrows(Exception.class, () -> f.collect().receivers().forEach(As400OperatorJournalEvidence.Receiver::requireReadableProfile));
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-journal", "duplicate-journal", "missing-directory", "excess-directory", "warning", "partial", "readonly" })
    void rejectsMissingPartialAndUnboundedQueries(String fault) throws Exception {
        Fixture f = new Fixture();
        switch (fault) {
            case "missing-journal" -> when(f.rows.get(0).next()).thenReturn(false);
            case "duplicate-journal" -> when(f.rows.get(0).next()).thenReturn(true);
            case "missing-directory" -> when(f.rows.get(1).next()).thenReturn(false);
            case "excess-directory" -> when(f.rows.get(1).next()).thenReturn(true);
            case "warning" -> when(f.rows.get(1).getWarnings()).thenReturn(new SQLWarning("Partial"));
            case "partial" -> when(f.rows.get(1).next()).thenReturn(true).thenThrow(new SQLException("Partial"));
            case "readonly" -> when(f.connection.isReadOnly()).thenReturn(false);
            default -> throw new AssertionError(fault);
        }
        assertThrows(SQLException.class, f::collect);
        f.closed();
    }

    @Test
    void shouldReobserveReceiverTailOnBorrowedConnectionWithoutOwningItsClose() throws Exception {
        var f = new Fixture();
        f.append(f.data.get(0));
        f.append(f.data.get(1));
        var evidence = new As400OperatorJournalEvidence(() -> {
            throw new AssertionError("Borrowed interval must not open another connection");
        }, new JournalReceiver("JRN", "LIB"), new As400OperatorJournalEvidence.Limits(2, 5));
        try (var connection = f.connection) {
            var first = evidence.collectOn(connection);
            f.data.get(1).put("LAST_SEQUENCE_NUMBER", 100);
            var second = evidence.collectOn(connection);
            assertThat(first.receivers().get(0).last()).isEqualTo(BigInteger.valueOf(99));
            assertThat(second.receivers().get(0).last()).isEqualTo(BigInteger.valueOf(100));
            verify(connection, never()).close();
            for (var statement : f.statements) {
                verify(statement).close();
            }
            for (var rows : f.rows) {
                verify(rows).close();
            }
        }
        f.closed();
    }

    private static final class Fixture {
        final Connection connection = mock(Connection.class);
        final List<PreparedStatement> statements = new ArrayList<>();
        final List<ResultSet> rows = new ArrayList<>();
        final List<Map<String, Object>> data = new ArrayList<>();

        Fixture() throws Exception {
            Map<String, Object> journal = new HashMap<>(Map.of("JOURNAL_LIBRARY", "LIB", "JOURNAL_NAME", "JRN", "JOURNAL_TYPE", "*LOCAL",
                    "JOURNAL_STATE", "*ACTIVE", "JOURNAL_ASPGRP", "*SYSBAS", "ASP_NUMBER", 1, "ATTACHED_JOURNAL_RECEIVER_LIBRARY", "LIB",
                    "ATTACHED_JOURNAL_RECEIVER_NAME", "RCV", "REMOVE_INTERNAL_ENTRIES", "NO", "REMOVE_FIXED_LENGTH_DETAIL", "NO"));
            journal.put("MINIMIZE_ESD_FOR_FILES", "NO");
            journal.put("JOURNAL_CACHE", "NO");
            journal.put("DELETE_RECEIVER_OPTION", "NO");
            data.add(journal);
            Map<String, Object> receiver = new HashMap<>(Map.of("JOURNAL_LIBRARY", "LIB", "JOURNAL_NAME", "JRN", "JOURNAL_RECEIVER_LIBRARY", "LIB",
                    "JOURNAL_RECEIVER_NAME", "RCV", "JOURNAL_RECEIVER_ASP_NUMBER", 1, "STATUS", "ATTACHED", "FIRST_SEQUENCE_NUMBER", 1,
                    "LAST_SEQUENCE_NUMBER", 99, "MINIMIZE_ESD_FOR_FILES", "NO"));
            receiver.put("FIXED_LENGTH_DATA_INCLUDES_JOB_NAME", "YES");
            receiver.put("FIXED_LENGTH_DATA_INCLUDES_THREAD_ID", "YES");
            receiver.put("FIXED_LENGTH_DATA_INCLUDES_LOGICAL_UNIT_OF_WORK_ID", "NO");
            receiver.put("FIXED_LENGTH_DATA_INCLUDES_TRANSACTION_ID", "NO");
            data.add(receiver);
            when(connection.isReadOnly()).thenReturn(true);
            for (Map<String, Object> row : data) {
                append(row);
            }
            AtomicInteger index = new AtomicInteger();
            when(connection.prepareStatement(anyString())).thenAnswer(i -> statements.get(index.getAndIncrement()));
        }

        void append(Map<String, Object> row) throws Exception {
            PreparedStatement statement = mock(PreparedStatement.class);
            ResultSet result = mock(ResultSet.class);
            when(statement.executeQuery()).thenReturn(result);
            when(result.next()).thenReturn(true, false);
            when(result.getString(anyString())).thenAnswer(i -> row.get(i.getArgument(0)) == null ? null : row.get(i.getArgument(0)).toString());
            when(result.getBigDecimal(anyString())).thenAnswer(i -> row.get(i.getArgument(0)) == null ? null : new BigDecimal(row.get(i.getArgument(0)).toString()));
            rows.add(result);
            statements.add(statement);
        }

        As400OperatorJournalEvidence.Observation collect() throws SQLException {
            return new As400OperatorJournalEvidence(() -> connection, new JournalReceiver("JRN", "LIB"), new As400OperatorJournalEvidence.Limits(2, 5)).collect();
        }

        void closed() throws SQLException {
            verify(connection).close();
            verify(connection, never()).prepareCall(anyString());
            verify(connection, never()).commit();
            verify(connection, never()).rollback();
        }
    }
}
