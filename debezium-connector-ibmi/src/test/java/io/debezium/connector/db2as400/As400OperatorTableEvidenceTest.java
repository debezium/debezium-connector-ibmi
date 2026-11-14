/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400OperatorTableEvidenceTest {
    private static final As400OperatorTableEvidence.Request REQUEST = new As400OperatorTableEvidence.Request(new JournalReceiver("QSQJRN", "LIB"), "LIB", "A");

    @Test
    void shouldObtainDistinctPairedJournalIdentifiersAndPhysicalFormatWithoutProducingStrictProof() throws Exception {
        Fixture f = new Fixture();
        var observation = f.collect().get(0);
        assertThat(observation.tableIdentity().fileJournalId()).isEqualTo("e9400014bcfbce3e0003");
        assertThat(observation.tableIdentity().memberJournalId()).isEqualTo("e9400014bcfbce3e0004");
        assertThat(observation.tableIdentity().recordFormat()).isEqualTo("A:29087D5938DDA:4:1:false");
        assertThat(observation.fileObject().identity()).isNotEqualTo(observation.memberObject().identity());
        verify(f.statements.get(0)).setString(1, "LIB");
        verify(f.statements.get(0)).setString(2, "A");
        verify(f.statements.get(2)).setString(1, "/QSYS.LIB/LIB.LIB/A.FILE");
        verify(f.statements.get(2)).setString(2, "*FILE");
        verify(f.statements.get(2)).setString(3, "/QSYS.LIB/LIB.LIB/A.FILE/A.MBR");
        verify(f.statements.get(2)).setString(4, "*MBR");
        verify(f.statements.get(0)).setMaxRows(2);
        verify(f.statements.get(1)).setMaxRows(2);
        verify(f.statements.get(2)).setMaxRows(3);
        verify(f.statements.get(2)).setFetchSize(3);
        for (var statement : f.statements) {
            verify(statement).setQueryTimeout(5);
            verify(statement).close();
            verify(statement, never()).executeUpdate();
        }
        f.closed();
        f.data.get(2).put("OBJECT_JID", "11111111111111111111");
        assertThat(observation.tableIdentity().fileJournalId()).isEqualTo("e9400014bcfbce3e0003");
    }

    static Stream<Arguments> mismatches() {
        List<Arguments> cases = new ArrayList<>();
        for (int row : List.of(0, 1)) {
            for (String column : List.of("SYSTEM_TABLE_SCHEMA", "SYSTEM_TABLE_NAME")) {
                cases.add(Arguments.of(row, column, "OTHER"));
                cases.add(Arguments.of(row, column, null));
            }
        }
        for (String column : List.of("TABLE_SCHEMA", "TABLE_NAME", "SYSTEM_TABLE_MEMBER")) {
            cases.add(Arguments.of(0, column, column.equals("SYSTEM_TABLE_MEMBER") ? "*FIRST" : column.equals("TABLE_NAME") ? "" : "OTHER"));
            cases.add(Arguments.of(0, column, null));
        }
        for (String column : List.of("NUMBER_MEMBERS", "NUMBER_RECORD_FORMATS")) {
            for (Object value : new Object[]{ null, 0, 2, new BigDecimal("1.5") }) {
                cases.add(Arguments.of(1, column, value));
            }
        }
        for (String column : List.of("NATIVE_TYPE", "FILE_TYPE", "FORMAT_LEVEL_ID", "FORMAT_NAME", "CONTAINS_LOB", "CONTAINS_VARYING_LENGTH")) {
            cases.add(Arguments.of(1, column, null));
            cases.add(Arguments.of(1, column, column.equals("FORMAT_NAME") ? "*FIRST" : "UNKNOWN"));
        }
        cases.add(Arguments.of(1, "CONTAINS_LOB", "UNKNOWN"));
        cases.add(Arguments.of(1, "RECORD_LENGTH", 0));
        cases.add(Arguments.of(1, "NUMBER_FIELDS", 8001));
        for (int row : List.of(2, 3)) {
            for (String column : List.of("PATH_NAME", "PATH_LENGTH", "OBJECT_TYPE", "ASP_NUMBER", "STORED_LOCAL", "HAS_SYMBOLIC_LINK",
                    "JOURNALED", "JOURNAL_LIBRARY", "JOURNAL_NAME", "OBJECT_JID", "FILE_SYSTEM_IDENTIFIER", "FILE_IDENTIFIER_NUMBER", "GENERATION_IDENTIFIER")) {
                cases.add(Arguments.of(row, column, null));
            }
            cases.add(Arguments.of(row, "PATH_NAME", "/QSYS.LIB/LIB.LIB/OTHER.FILE"));
            cases.add(Arguments.of(row, "OBJECT_TYPE", row == 2 ? "*MBR" : "*FILE"));
            cases.add(Arguments.of(row, "ASP_NUMBER", 0));
            cases.add(Arguments.of(row, "ASP_NUMBER", 33));
            cases.add(Arguments.of(row, "STORED_LOCAL", "NO"));
            cases.add(Arguments.of(row, "HAS_SYMBOLIC_LINK", 1));
            cases.add(Arguments.of(row, "JOURNALED", "NO"));
            cases.add(Arguments.of(row, "JOURNAL_NAME", "OTHER"));
            cases.add(Arguments.of(row, "JOURNAL_LIBRARY", "OTHER"));
            cases.add(Arguments.of(row, "OBJECT_JID", "00000000000000000000"));
            cases.add(Arguments.of(row, "OBJECT_JID", "40404040404040404040"));
            cases.add(Arguments.of(row, "OBJECT_JID", "123"));
            cases.add(Arguments.of(row, "FILE_IDENTIFIER_NUMBER", new BigDecimal("9223372036854775808")));
        }
        cases.add(Arguments.of(3, "OBJECT_JID", "E9400014BCFBCE3E0003"));
        return cases.stream();
    }

    @ParameterizedTest
    @CsvSource({ "1,false,DATA", "2,false,DATA", "32,false,DATA", "1,true,DATA", "1,false,SOURCE" })
    void shouldRetainPhysicalIdentityAndLobCapabilityInTheSystemOrBasicAspNamespace(int asp, boolean lob, String fileType) throws Exception {
        var fixture = new Fixture();
        fixture.data.get(2).put("ASP_NUMBER", asp);
        fixture.data.get(3).put("ASP_NUMBER", asp);
        fixture.data.get(1).put("CONTAINS_LOB", lob ? "YES" : "NO");
        fixture.data.get(1).put("FILE_TYPE", fileType);
        var observed = fixture.collect().get(0);
        assertThat(observed.tableIdentity().member()).isEqualTo("A");
        assertThat(observed.format().lob()).isEqualTo(lob);
        assertThat(observed.fileObject().identity()).isNotEqualTo(observed.memberObject().identity());
        fixture.closed();
    }

    @Test
    void shouldResolveTheSqlTableNameFromTheExactPhysicalFileObservation() throws Exception {
        var fixture = new Fixture();
        fixture.data.get(0).put("TABLE_NAME", "LONG_ORDER_TABLE");
        var table = fixture.collect().get(0);
        assertThat(table.table().table()).isEqualTo("LONG_ORDER_TABLE");
        assertThat(table.tableIdentity().physicalFile()).isEqualTo("A");
        assertThat(table.tableIdentity().member()).isEqualTo("A");
        assertThat(table.fileObject().target().path()).isEqualTo("/QSYS.LIB/LIB.LIB/A.FILE");
        fixture.closed();
    }

    @ParameterizedTest
    @MethodSource("mismatches")
    void shouldRejectIncompleteAndMismatchedBindings(int row, String column, Object value) throws Exception {
        Fixture f = new Fixture();
        f.data.get(row).put(column, value);
        assertThrows(SQLException.class, f::collect);
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2 })
    void shouldRejectDuplicateMissingWarningAndFailedQueries(int fault) throws Exception {
        for (String type : List.of("missing", "duplicate", "warning", "failure")) {
            Fixture f = new Fixture();
            if (type.equals("missing")) {
                when(f.rows.get(fault).next()).thenReturn(false);
            }
            if (type.equals("duplicate")) {
                when(f.rows.get(fault).next()).thenReturn(true);
            }
            if (type.equals("warning")) {
                when(f.rows.get(fault).getWarnings()).thenReturn(new SQLWarning("Partial"));
            }
            if (type.equals("failure")) {
                when(f.statements.get(fault).executeQuery()).thenThrow(new SQLException("Unavailable"));
            }
            var failure = assertThrows(SQLException.class, f::collect);
            if (type.equals("missing") && fault == 0) {
                assertThat((Throwable) failure).isInstanceOfSatisfying(As400NativeEvidenceException.class,
                        absent -> assertThat(absent.catalogAbsent()).isTrue());
            }
            if (!type.equals("missing")) {
                assertThat(failure instanceof As400NativeEvidenceException invalid && invalid.catalogAbsent()).isFalse();
            }
            f.closed();
        }
    }

    @Test
    void shouldRejectIdenticalNativeObjectsAndReadOnlyRefusal() throws Exception {
        Fixture f = new Fixture();
        for (String key : List.of("FILE_SYSTEM_IDENTIFIER", "FILE_IDENTIFIER_NUMBER", "GENERATION_IDENTIFIER")) {
            f.data.get(3).put(key, f.data.get(2).get(key));
        }
        assertThrows(SQLException.class, f::collect);
        f.closed();
        f = new Fixture();
        when(f.connection.isReadOnly()).thenReturn(false);
        assertThrows(SQLException.class, f::collect);
        verify(f.connection, never()).prepareStatement(anyString());
        f.closed();
    }

    @Test
    void shouldReadBoundedCatalogAndObjectBatchesAcrossLibrariesWithoutChangingRequestOrder() throws Exception {
        var fixture = new BatchFixture();
        var requests = new ArrayList<As400OperatorTableEvidence.Request>();
        for (int i = 0; i < 51; i++) {
            requests.add(new As400OperatorTableEvidence.Request(REQUEST.journal(), "LIB", "T" + i));
        }
        requests.add(new As400OperatorTableEvidence.Request(REQUEST.journal(), "OTHER", "T0"));
        var observations = fixture.collect(requests);
        assertThat(observations).extracting(As400OperatorTableEvidence.Observation::request).containsExactlyElementsOf(requests);
        assertThat(observations).allSatisfy(observation -> {
            assertThat(observation.member()).isEqualTo(observation.request().file());
            assertThat(observation.fileObject().target().path()).isEqualTo("/QSYS.LIB/" + observation.request().library() + ".LIB/"
                    + observation.request().file() + ".FILE");
        });
        assertThat(fixture.catalogBatches).containsExactly(50, 1, 1, 50, 1, 1);
        assertThat(fixture.objectBatches).containsExactly(16, 16, 16, 16, 16, 16, 8);
        assertThat(fixture.statements).hasSize(13);
        fixture.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "duplicate", "foreign", "library", "warning", "failure" })
    void shouldRejectIncompleteOrAmbiguousCatalogBatchBeforeReadingObjects(String fault) throws Exception {
        for (String query : List.of("SYSPARTITIONSTAT", "SYSFILES")) {
            var fixture = new BatchFixture();
            fixture.fault = fault;
            fixture.faultQuery = query;
            assertThrows(SQLException.class, () -> fixture.collect(List.of(REQUEST,
                    new As400OperatorTableEvidence.Request(REQUEST.journal(), "LIB", "B"))));
            assertThat(fixture.objectQueries).isZero();
            fixture.closed();
        }
    }

    @Test
    void shouldRejectConflictingJournalsForOnePhysicalFileBeforeOpeningAConnection() throws Exception {
        var fixture = new BatchFixture();
        // These requests straddle catalog batches but share an object batch.
        // A target map must never silently replace one requested journal.
        var requests = java.util.stream.IntStream.range(0, 51)
                .mapToObj(i -> new As400OperatorTableEvidence.Request(i == 49 ? new JournalReceiver("OTHER", "LIB") : REQUEST.journal(),
                        "LIB", "T" + Math.min(i, 49)))
                .toList();
        assertThrows(IllegalArgumentException.class, () -> fixture.collect(requests));
        org.mockito.Mockito.verifyNoInteractions(fixture.connection);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "duplicate", "foreign", "type", "journal", "warning", "late-warning", "failure", "interrupted" })
    void shouldRejectIncompleteOrAmbiguousObjectBatchWithoutStartingAnotherBatch(String fault) throws Exception {
        var fixture = new BatchFixture();
        fixture.fault = fault;
        fixture.faultQuery = "IFS_OBJECT_STATISTICS";
        var requests = java.util.stream.IntStream.range(0, 9)
                .mapToObj(i -> new As400OperatorTableEvidence.Request(REQUEST.journal(), "LIB", "T" + i)).toList();
        try {
            assertThrows(SQLException.class, () -> fixture.collect(requests));
            assertThat(fixture.objectBatches).containsExactly(16);
            fixture.closed();
        }
        finally {
            Thread.interrupted();
        }
    }

    @Test
    void shouldAcquireEveryMemberInBoundedExactObjectQueries() throws Exception {
        var fixture = new BatchFixture();
        fixture.memberCount = 20;
        var observed = fixture.collect(List.of(REQUEST)).get(0);
        assertThat(observed.memberObjects()).hasSize(20);
        assertThat(observed.memberJournalIds().values()).doesNotHaveDuplicates();
        assertThat(observed.member()).isEqualTo("A");
        assertThat(fixture.objectBatches).containsExactly(16, 5);
        fixture.closed();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReobserveTheCompleteTableOnABorrowedConnectionAndRejectLateWarnings(boolean warning) throws Exception {
        var fixture = new BatchFixture();
        var evidence = new As400OperatorTableEvidence(() -> {
            throw new AssertionError("Borrowed observation must not open another connection");
        }, new As400OperatorTableEvidence.Limits(2, 5));
        try (var connection = fixture.connection) {
            var first = evidence.collectOn(connection, List.of(REQUEST));
            fixture.memberCount = 2;
            if (warning) {
                fixture.fault = "late-warning";
                fixture.faultQuery = "IFS_OBJECT_STATISTICS";
                assertThrows(SQLException.class, () -> evidence.collectOn(connection, List.of(REQUEST)));
            }
            else {
                var second = evidence.collectOn(connection, List.of(REQUEST));
                assertThat(second).isNotEqualTo(first);
                assertThat(first.get(0).memberObjects()).hasSize(1);
                assertThat(second.get(0).memberObjects()).hasSize(2);
            }
            assertThat(fixture.catalogBatches).hasSize(4);
            assertThat(fixture.objectBatches).containsExactly(2, 3);
            verify(connection, never()).close();
        }
        fixture.closed();
    }

    @Test
    void shouldUseTheExactNativeMemberDirectoryForAProgramDescribedFile() throws Exception {
        var fixture = new BatchFixture();
        fixture.programDescribed = true;
        fixture.memberCount = 3;
        var observations = fixture.collect(List.of(new As400OperatorTableEvidence.Request(new JournalReceiver("QSQJRN", "LIB"), "LIB", "A")));
        assertThat(observations).hasSize(1);
        assertThat(observations.get(0).format().programDescribed()).isTrue();
        assertThat(observations.get(0).memberObjects()).hasSize(3);
        fixture.closed();
    }

    @ParameterizedTest
    @ValueSource(strings = { "duplicate", "foreign", "journal", "warning", "late-warning", "missing" })
    void shouldRejectIncompleteOrForeignProgramDescribedMembers(String fault) throws Exception {
        var fixture = new BatchFixture();
        fixture.programDescribed = true;
        fixture.memberCount = 3;
        fixture.fault = fault;
        fixture.faultQuery = "IFS_OBJECT_STATISTICS";
        assertThatThrownBy(() -> fixture.collect(List.of(new As400OperatorTableEvidence.Request(new JournalReceiver("QSQJRN", "LIB"), "LIB", "A"))))
                .isInstanceOf(SQLException.class);
        fixture.closed();
    }

    static class BatchFixture {
        final Connection connection = mock(Connection.class);
        final List<PreparedStatement> statements = new ArrayList<>();
        final List<ResultSet> results = new ArrayList<>();
        final List<Integer> catalogBatches = new ArrayList<>();
        final List<Integer> objectBatches = new ArrayList<>();
        String fault = "";
        String faultQuery = "";
        int objectQueries;
        int memberCount = 1;
        boolean programDescribed;

        BatchFixture() throws Exception {
            var template = new Fixture().data;
            when(connection.isReadOnly()).thenReturn(true);
            when(connection.prepareStatement(anyString())).thenAnswer(call -> {
                String sql = call.getArgument(0);
                var statement = mock(PreparedStatement.class);
                statements.add(statement);
                var parameters = new HashMap<Integer, String>();
                doAnswer(binding -> {
                    parameters.put(binding.getArgument(0), binding.getArgument(1));
                    return null;
                }).when(statement).setString(anyInt(), anyString());
                when(statement.executeQuery()).thenAnswer(execution -> {
                    var values = new ArrayList<Map<String, Object>>();
                    boolean catalog = !sql.contains("IFS_OBJECT_STATISTICS");
                    if (catalog) {
                        int count = parameters.size() - 1;
                        assertThat(count).isBetween(1, 50);
                        verify(statement).setMaxRows(count * (sql.contains("SYSPARTITIONSTAT") ? memberCount : 1) + 1);
                        catalogBatches.add(count);
                        for (int i = 0; i < count; i++) {
                            var value = new HashMap<>(template.get(sql.contains("SYSPARTITIONSTAT") ? 0 : 1));
                            value.put("SYSTEM_TABLE_SCHEMA", parameters.get(1));
                            value.put("TABLE_SCHEMA", parameters.get(1));
                            value.put("SYSTEM_TABLE_NAME", parameters.get(i + 2));
                            value.put("TABLE_NAME", parameters.get(i + 2));
                            value.put("SYSTEM_TABLE_MEMBER", parameters.get(i + 2));
                            value.put("NUMBER_MEMBERS", memberCount);
                            value.put("PROGRAM_DESCRIBED", programDescribed ? "YES" : "NO");
                            values.add(value);
                            if (sql.contains("SYSPARTITIONSTAT")) {
                                for (int member = 1; member < memberCount; member++) {
                                    var extra = new HashMap<>(value);
                                    extra.put("SYSTEM_TABLE_MEMBER", String.format(java.util.Locale.ROOT, "M%04d", member));
                                    values.add(extra);
                                }
                            }
                        }
                        Collections.reverse(values); // Catalog row order is not a binding rule.
                        if (programDescribed && sql.contains("SYSPARTITIONSTAT")) {
                            values.clear();
                        }
                    }
                    else {
                        objectQueries++;
                        int count = parameters.size() / 2;
                        boolean memberDirectory = count == 1 && parameters.get(2).equals("*MBR") && parameters.get(1).endsWith(".FILE");
                        assertThat(count).isBetween(1, 16);
                        assertThat(sql.split("UNION ALL", -1)).hasSize(count);
                        if (memberDirectory) {
                            count = memberCount;
                        }
                        verify(statement).setMaxRows(count + 1);
                        verify(statement).setFetchSize(count + 1);
                        objectBatches.add(count);
                        for (int i = 0; i < count; i++) {
                            var value = new HashMap<>(template.get(!memberDirectory && parameters.get(i * 2 + 2).equals("*FILE") ? 2 : 3));
                            String path = memberDirectory ? parameters.get(1) + "/" + (i == 0 ? "A" : String.format(java.util.Locale.ROOT, "M%04d", i)) + ".MBR"
                                    : parameters.get(i * 2 + 1);
                            value.put("PATH_NAME", path);
                            value.put("PATH_LENGTH", path.length());
                            long identity = Integer.toUnsignedLong(path.hashCode()) + 1;
                            value.put("OBJECT_JID", String.format(java.util.Locale.ROOT, "%020x", identity));
                            value.put("FILE_IDENTIFIER_NUMBER", identity);
                            values.add(value);
                        }
                        Collections.reverse(values); // UNION result order is not a binding rule either.
                    }
                    boolean faulty = sql.contains(faultQuery);
                    if (faulty) {
                        switch (fault) {
                            case "missing" -> values.remove(0);
                            case "duplicate" -> values.add(values.get(0));
                            case "foreign" -> values.get(0).put(catalog ? "SYSTEM_TABLE_NAME" : "PATH_NAME", "UNKNOWN");
                            case "library" -> values.get(0).put("SYSTEM_TABLE_SCHEMA", "UNKNOWN");
                            case "type" -> values.get(0).put("OBJECT_TYPE", "*FILE");
                            case "journal" -> values.get(0).put("JOURNAL_NAME", "OTHER");
                            case "failure" -> throw new SQLException("Catalog unavailable");
                            default -> {
                            }
                        }
                    }
                    var rows = mock(ResultSet.class);
                    results.add(rows);
                    var index = new AtomicInteger(-1);
                    when(rows.next()).thenAnswer(ignored -> {
                        int current = index.incrementAndGet();
                        if (faulty && fault.equals("interrupted") && current == values.size() - 1) {
                            Thread.currentThread().interrupt();
                        }
                        return current < values.size();
                    });
                    when(rows.getString(anyString())).thenAnswer(field -> {
                        Object value = values.get(index.get()).get(field.getArgument(0));
                        return value == null ? null : value.toString();
                    });
                    when(rows.getBigDecimal(anyString())).thenAnswer(field -> {
                        Object value = values.get(index.get()).get(field.getArgument(0));
                        return value == null ? null : new BigDecimal(value.toString());
                    });
                    if (faulty && fault.equals("warning")) {
                        when(rows.getWarnings()).thenReturn(new SQLWarning("Incomplete catalog"));
                    }
                    if (faulty && fault.equals("late-warning")) {
                        when(rows.getWarnings()).thenAnswer(ignored -> index.get() >= values.size() ? new SQLWarning("Incomplete native object read") : null);
                    }
                    return rows;
                });
                return statement;
            });
        }

        List<As400OperatorTableEvidence.Observation> collect(List<As400OperatorTableEvidence.Request> requests) throws SQLException {
            return new As400OperatorTableEvidence(() -> connection, new As400OperatorTableEvidence.Limits(100, 5)).collect(requests);
        }

        void closed() throws Exception {
            verify(connection).close();
            for (var statement : statements) {
                verify(statement).setQueryTimeout(5);
                verify(statement).close();
                verify(statement, never()).executeUpdate();
            }
            for (var result : results) {
                verify(result).close();
            }
        }
    }

    static class Fixture {
        final Connection connection = mock(Connection.class);
        final List<PreparedStatement> statements = new ArrayList<>();
        final List<ResultSet> rows = new ArrayList<>();
        final List<Map<String, Object>> data = new ArrayList<>();

        Fixture() throws Exception {
            data.add(new HashMap<>(Map.of("SYSTEM_TABLE_SCHEMA", "LIB", "SYSTEM_TABLE_NAME", "A", "TABLE_SCHEMA", "LIB", "TABLE_NAME", "A", "SYSTEM_TABLE_MEMBER", "A")));
            var format = new HashMap<String, Object>(Map.of("SYSTEM_TABLE_SCHEMA", "LIB", "SYSTEM_TABLE_NAME", "A", "NATIVE_TYPE", "PHYSICAL", "FILE_TYPE", "DATA",
                    "NUMBER_MEMBERS", 1, "NUMBER_RECORD_FORMATS", 1, "FORMAT_NAME", "A", "FORMAT_LEVEL_ID", "29087D5938DDA", "RECORD_LENGTH", 4, "NUMBER_FIELDS", 1));
            format.put("CONTAINS_LOB", "NO");
            format.put("PROGRAM_DESCRIBED", "NO");
            format.put("CONTAINS_VARYING_LENGTH", "NO");
            data.add(format);
            for (int i = 0; i < 2; i++) {
                String path = "/QSYS.LIB/LIB.LIB/A.FILE" + (i == 0 ? "" : "/A.MBR");
                Map<String, Object> object = new HashMap<>(
                        Map.of("PATH_NAME", path, "PATH_LENGTH", path.length(), "OBJECT_TYPE", i == 0 ? "*FILE" : "*MBR", "ASP_NUMBER", 1,
                                "STORED_LOCAL", "YES", "HAS_SYMBOLIC_LINK", 0, "FILE_SYSTEM_IDENTIFIER", 141016661229569L, "FILE_IDENTIFIER_NUMBER", 56788 + i,
                                "GENERATION_IDENTIFIER", 2829710027L - i));
                object.put("JOURNALED", "YES");
                object.put("JOURNAL_LIBRARY", "LIB");
                object.put("JOURNAL_NAME", "QSQJRN");
                object.put("OBJECT_JID", i == 0 ? "E9400014BCFBCE3E0003" : "E9400014BCFBCE3E0004");
                data.add(object);
            }
            when(connection.isReadOnly()).thenReturn(true);
            for (int query = 0; query < 3; query++) {
                var values = query < 2 ? List.of(data.get(1 - query)) : data.subList(2, 4);
                ResultSet result = mock(ResultSet.class);
                PreparedStatement statement = mock(PreparedStatement.class);
                when(statement.executeQuery()).thenReturn(result);
                var index = new AtomicInteger(-1);
                when(result.next()).thenAnswer(ignored -> index.incrementAndGet() < values.size());
                when(result.getString(anyString())).thenAnswer(i -> {
                    Object value = values.get(index.get()).get(i.getArgument(0));
                    return value == null ? null : value.toString();
                });
                when(result.getBigDecimal(anyString())).thenAnswer(i -> {
                    Object value = values.get(index.get()).get(i.getArgument(0));
                    return value == null ? null : new BigDecimal(value.toString());
                });
                rows.add(result);
                statements.add(statement);
            }
            AtomicInteger index = new AtomicInteger();
            when(connection.prepareStatement(anyString())).thenAnswer(i -> statements.get(index.getAndIncrement()));
        }

        List<As400OperatorTableEvidence.Observation> collect() throws SQLException {
            return new As400OperatorTableEvidence(() -> connection, new As400OperatorTableEvidence.Limits(2, 5)).collect(List.of(REQUEST));
        }

        void closed() throws Exception {
            verify(connection).close();
            verify(connection, never()).prepareCall(anyString());
            verify(connection, never()).commit();
            verify(connection, never()).rollback();
        }
    }
}
