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
            for (Object value : new Object[]{ null, 0, 2, new BigDecimal("1.5") }) {
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
