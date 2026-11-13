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
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400PendingReceiverEvidenceTest {
    @ParameterizedTest
    @ValueSource(strings = { "valid", "missing", "extra", "changed-count", "late-unknown" })
    void shouldValidatePendingHistoryAcrossLargeNativeDirectories(String fault) throws Exception {
        var fixture = new Fixture();
        int count = 5000;
        fixture.before.put("NUMBER_JOURNAL_RECEIVERS", count);
        fixture.before.put("ATTACHED_JOURNAL_RECEIVER_NAME", "R" + (count - 1));
        fixture.after.putAll(fixture.before);
        fixture.receiverRows.clear();
        for (int i = 0; i < count; i++) {
            var row = new HashMap<>(fixture.receiver);
            row.put("JOURNAL_RECEIVER_NAME", "R" + i);
            row.put("STATUS", i == count - 1 ? "ATTACHED" : "ONLINE");
            row.put("FIRST_SEQUENCE_NUMBER", i * 10L + 1);
            row.put("LAST_SEQUENCE_NUMBER", (i + 1L) * 10);
            row.put("PREVIOUS_JOURNAL_RECEIVER_LIBRARY", i == 0 ? null : "LIB");
            row.put("PREVIOUS_JOURNAL_RECEIVER", i == 0 ? null : "R" + (i - 1));
            row.put("NEXT_JOURNAL_RECEIVER_LIBRARY", i == count - 1 ? null : "LIB");
            row.put("NEXT_JOURNAL_RECEIVER", i == count - 1 ? null : "R" + (i + 1));
            fixture.receiverRows.add(row);
        }
        switch (fault) {
            case "missing" -> fixture.receiverRows.remove(count - 1);
            case "extra" -> fixture.receiverRows.add(fixture.receiver);
            case "changed-count" -> fixture.after.put("NUMBER_JOURNAL_RECEIVERS", count - 1);
            case "late-unknown" -> fixture.receiverRows.get(count - 1).put("PENDING_TRANSACTIONS", "UNKNOWN");
        }
        var collector = new As400PendingReceiverEvidence(() -> fixture.connection, new JournalReceiver("JRN", "LIB"), Integer.MAX_VALUE - 1, 5);
        if (fault.equals("valid")) {
            assertThat(collector.collect().receivers()).hasSize(count).allMatch(As400PendingReceiverEvidence.Receiver::pending);
            verify(fixture.statements.get(1)).setMaxRows(count + 1);
            verify(fixture.statements.get(1)).setFetchSize(128);
        }
        else {
            assertThrows(SQLException.class, collector::collect);
        }
        verify(fixture.connection).close();
    }

    @Test
    void collectsCompleteReadOnlyParameterizedDirectoryWithBothCounts() throws Exception {
        var fixture = new Fixture();
        var sample = fixture.collect();
        assertThat(sample.receivers()).hasSize(1);
        assertThat(sample.receivers().get(0).pending()).isTrue();
        verify(fixture.connection).setReadOnly(true);
        verify(fixture.connection).close();
        assertThat(fixture.statements).hasSize(3);
        for (var statement : fixture.statements) {
            verify(statement).setQueryTimeout(5);
            verify(statement).setMaxRows(2);
            verify(statement).setString(1, "LIB");
            verify(statement).setString(2, "JRN");
            verify(statement).close();
            verify(statement, never()).executeUpdate();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-flag", "unknown-flag", "count", "changed-count", "changed-attachment", "duplicate", "missing",
            "partial-link", "dangling", "overflow", "fraction", "warning", "query-error", "foreign", "cache", "filtered" })
    void partialAmbiguousOrWarnedEvidenceCannotBecomeEmptyHistory(String fault) throws Exception {
        var fixture = new Fixture();
        switch (fault) {
            case "missing-flag" -> fixture.receiver.remove("PENDING_TRANSACTIONS");
            case "unknown-flag" -> fixture.receiver.put("PENDING_TRANSACTIONS", "UNKNOWN");
            case "count" -> fixture.before.put("NUMBER_JOURNAL_RECEIVERS", 0);
            case "changed-count" -> fixture.after.put("NUMBER_JOURNAL_RECEIVERS", 0);
            case "changed-attachment" -> fixture.after.put("ATTACHED_JOURNAL_RECEIVER_NAME", "OTHER");
            case "duplicate" -> fixture.receiverRows.add(fixture.receiver);
            case "missing" -> fixture.receiverRows.clear();
            case "partial-link" -> fixture.receiver.put("PREVIOUS_JOURNAL_RECEIVER", "OLD");
            case "dangling" -> {
                fixture.receiver.put("NEXT_JOURNAL_RECEIVER_LIBRARY", "LIB");
                fixture.receiver.put("NEXT_JOURNAL_RECEIVER", "NEXT");
            }
            case "overflow" -> fixture.receiver.put("LAST_SEQUENCE_NUMBER", new BigDecimal("18446744073709551616"));
            case "fraction" -> fixture.receiver.put("FIRST_SEQUENCE_NUMBER", new BigDecimal("1.5"));
            case "warning" -> fixture.warning = true;
            case "query-error" -> fixture.queryError = true;
            case "foreign" -> fixture.receiver.put("JOURNAL_NAME", "OTHER");
            case "cache" -> fixture.before.put("JOURNAL_CACHE", "YES");
            case "filtered" -> fixture.before.put("REMOVE_INTERNAL_ENTRIES", "YES");
            default -> throw new AssertionError(fault);
        }
        assertThrows(SQLException.class, fixture::collect);
        verify(fixture.connection).close();
        for (var statement : fixture.statements) {
            verify(statement).close();
        }
    }

    @Test
    void interruptionRejectsBeforeOpeningAnyNativeConnection() throws Exception {
        var fixture = new Fixture();
        Thread.currentThread().interrupt();
        try {
            assertThrows(SQLException.class, fixture::collect);
            verify(fixture.connection, never()).setReadOnly(true);
            assertThat(fixture.statements).isEmpty();
        }
        finally {
            Thread.interrupted();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void nativeTerminalRequiresItsOriginalStartEvenAfterPendingFlagClears(boolean pending) throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/receiver-control-history.json")) {
            var fixture = new ObjectMapper().readTree(stream);
            var item = java.util.stream.StreamSupport.stream(fixture.path("entries").spliterator(), false)
                    .filter(e -> e.path("type").asText().equals("C/CM") && new BigInteger(e.path("cycle").asText()).signum() > 0).findFirst().orElseThrow();
            byte[] bytes = Base64.getDecoder().decode(item.path("base64").asText());
            var position = As400OnlineHandoff.position(item.path("position"));
            var cycle = new EntryHeaderDecoder().decode(bytes, 0).getCommitCycle();
            var receiver = new As400PendingReceiverEvidence.Receiver(position.receiver(), "ATTACHED", BigInteger.ONE, position.offset(),
                    Optional.empty(), Optional.empty(), pending);
            var sample = new As400PendingReceiverEvidence.Sample(new JournalReceiver("QSQJRN", position.receiver().library()),
                    position.receiver(), List.of(receiver));
            var floor = sample.floor(List.of(new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(bytes, 0), bytes)));
            assertThat(floor.offset()).isEqualTo(pending ? BigInteger.ONE : cycle);
            assertThat(floor.offset()).isLessThan(position.offset());
            var missing = new As400PendingReceiverEvidence.Sample(sample.journal(), sample.attached(),
                    List.of(new As400PendingReceiverEvidence.Receiver(receiver.name(), receiver.status(), cycle.add(BigInteger.ONE), receiver.last(),
                            Optional.empty(), Optional.empty(), false)));
            assertThrows(IllegalArgumentException.class,
                    () -> missing.floor(List.of(new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(bytes, 0), bytes))));
        }
    }

    @Test
    void silentDetachedPendingReceiverKeepsHistoryBelowAnUnrelatedNativeRow() throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/prospective-boundary.json")) {
            var fixture = new ObjectMapper().readTree(stream);
            var item = java.util.stream.StreamSupport.stream(fixture.path("prospective-rows").path("entries").spliterator(), false)
                    .filter(e -> e.path("type").asText().equals("R/PT") && e.path("cycle").asText().equals("0")).findFirst().orElseThrow();
            byte[] bytes = Base64.getDecoder().decode(item.path("raw").asText());
            var position = As400OnlineHandoff.position(item.path("position"));
            var old = new JournalReceiver("OLD", position.receiver().library());
            var sample = new As400PendingReceiverEvidence.Sample(new JournalReceiver("QSQJRN", old.library()), position.receiver(), List.of(
                    new As400PendingReceiverEvidence.Receiver(old, "ONLINE", BigInteger.ONE, BigInteger.TEN, Optional.empty(), Optional.of(position.receiver()), true),
                    new As400PendingReceiverEvidence.Receiver(position.receiver(), "ATTACHED", BigInteger.valueOf(11), position.offset(),
                            Optional.of(old), Optional.empty(), false)));
            assertThat(sample.floor(List.of(new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(bytes, 0), bytes))))
                    .isEqualTo(new JournalPosition(BigInteger.ONE, old));
            assertThat(As400NativeHistoryDiscoveryProbe.restore(new ObjectMapper().valueToTree(As400NativeHistoryDiscoveryProbe.pending(sample)))).isEqualTo(sample);
        }
    }

    @ParameterizedTest
    @CsvSource({ "FREED,false", "FREED,true", "PARTIAL,false", "PARTIAL,true", "RESET,false", "RESET,true", "MISSING_LINK,false", "MISSING_LINK,true" })
    void shouldIgnoreUnavailableHistoricalReceiversOnlyWhenNoRequiredHistoryDependsOnThem(String condition, boolean pending) throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/prospective-boundary.json")) {
            var fixture = new ObjectMapper().readTree(stream);
            var item = java.util.stream.StreamSupport.stream(fixture.path("prospective-rows").path("entries").spliterator(), false)
                    .filter(e -> e.path("type").asText().equals("R/PT") && e.path("cycle").asText().equals("0")).findFirst().orElseThrow();
            byte[] bytes = Base64.getDecoder().decode(item.path("raw").asText());
            var position = As400OnlineHandoff.position(item.path("position"));
            var old = new JournalReceiver("OLD", position.receiver().library());
            var sample = new As400PendingReceiverEvidence.Sample(new JournalReceiver("QSQJRN", old.library()), position.receiver(), List.of(
                    new As400PendingReceiverEvidence.Receiver(old, List.of("FREED", "PARTIAL").contains(condition) ? condition : "SAVED",
                            BigInteger.ONE, BigInteger.TEN, Optional.empty(), condition.equals("MISSING_LINK") ? Optional.empty() : Optional.of(position.receiver()),
                            pending),
                    new As400PendingReceiverEvidence.Receiver(position.receiver(), "ATTACHED", condition.equals("RESET") ? BigInteger.ONE : BigInteger.valueOf(11),
                            position.offset(), Optional.of(old), Optional.empty(), false)));
            var bracket = List.of(new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(bytes, 0), bytes));
            if (pending) {
                assertThrows(IllegalArgumentException.class, () -> sample.floor(bracket));
            }
            else {
                assertThat(sample.floor(bracket)).isEqualTo(position);
            }
        }
    }

    @Test
    void shouldDeferChangedDirectoryEvidenceRatherThanFailTheLegacyTask() throws Exception {
        var fixture = new Fixture();
        fixture.after.put("NUMBER_JOURNAL_RECEIVERS", 2);
        assertThrows(As400NativeEvidenceException.class, fixture::collect);
        verify(fixture.connection).close();
    }

    private static final class Fixture {
        final Connection connection = mock(Connection.class);
        final List<PreparedStatement> statements = new ArrayList<>();
        final Map<String, Object> before = new HashMap<>(Map.of("JOURNAL_LIBRARY", "LIB", "JOURNAL_NAME", "JRN", "JOURNAL_TYPE", "*LOCAL",
                "JOURNAL_STATE", "*ACTIVE", "NUMBER_JOURNAL_RECEIVERS", 1, "ATTACHED_JOURNAL_RECEIVER_LIBRARY", "LIB", "ATTACHED_JOURNAL_RECEIVER_NAME", "RCV",
                "JOURNAL_CACHE", "NO", "REMOVE_INTERNAL_ENTRIES", "NO", "REMOVE_FIXED_LENGTH_DETAIL", "NO"));
        final Map<String, Object> after = new HashMap<>(before);
        final Map<String, Object> receiver = new HashMap<>(Map.of("JOURNAL_LIBRARY", "LIB", "JOURNAL_NAME", "JRN", "JOURNAL_RECEIVER_LIBRARY", "LIB",
                "JOURNAL_RECEIVER_NAME", "RCV", "STATUS", "ATTACHED", "FIRST_SEQUENCE_NUMBER", 1, "LAST_SEQUENCE_NUMBER", 99, "PENDING_TRANSACTIONS", "YES"));
        final List<Map<String, Object>> receiverRows = new ArrayList<>(List.of(receiver));
        boolean warning;
        boolean queryError;
        int directories;

        Fixture() throws Exception {
            when(connection.prepareStatement(anyString())).thenAnswer(call -> {
                String sql = call.getArgument(0);
                PreparedStatement statement = mock(PreparedStatement.class);
                statements.add(statement);
                var data = sql.equals(As400PendingReceiverEvidence.RECEIVERS_SQL) ? receiverRows : List.of(directories++ == 0 ? before : after);
                when(statement.executeQuery()).thenAnswer(execute -> {
                    if (queryError) {
                        throw new SQLException("native unavailable");
                    }
                    var rows = mock(ResultSet.class);
                    AtomicInteger index = new AtomicInteger(-1);
                    when(rows.next()).thenAnswer(next -> index.incrementAndGet() < data.size());
                    when(rows.getString(anyString())).thenAnswer(get -> {
                        var value = data.get(index.get()).get(get.getArgument(0));
                        return value == null ? null : value.toString();
                    });
                    when(rows.getBigDecimal(anyString())).thenAnswer(get -> {
                        var value = data.get(index.get()).get(get.getArgument(0));
                        return value == null ? null : new BigDecimal(value.toString());
                    });
                    when(rows.getWarnings()).thenReturn(warning ? new SQLWarning("incomplete") : null);
                    return rows;
                });
                return statement;
            });
        }

        As400PendingReceiverEvidence.Sample collect() throws SQLException {
            return new As400PendingReceiverEvidence(() -> connection, new JournalReceiver("JRN", "LIB"), 1, 5).collect();
        }
    }
}
