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

class As400OperatorTableEvidenceTest {
    private static final As400OperatorTableEvidence.Request REQUEST = new As400OperatorTableEvidence.Request(new JournalReceiver("QSQJRN", "LIB"), "LIB", "A");

    @Test
    void obtainsDistinctPairedJournalIdentifiersAndPhysicalFormatWithoutProducingStrictProof() throws Exception {
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
        verify(f.statements.get(3)).setString(1, "/QSYS.LIB/LIB.LIB/A.FILE/A.MBR");
        verify(f.statements.get(3)).setString(2, "*MBR");
        for (var statement : f.statements) {
            verify(statement).setQueryTimeout(5);
            verify(statement).setMaxRows(2);
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
            cases.add(Arguments.of(0, column, column.equals("SYSTEM_TABLE_MEMBER") ? "*FIRST" : "OTHER"));
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
        cases.add(Arguments.of(1, "CONTAINS_LOB", "YES"));
        cases.add(Arguments.of(1, "RECORD_LENGTH", 0));
        cases.add(Arguments.of(1, "NUMBER_FIELDS", 4097));
        for (int row : List.of(2, 3)) {
            for (String column : List.of("PATH_NAME", "PATH_LENGTH", "OBJECT_TYPE", "ASP_NUMBER", "STORED_LOCAL", "HAS_SYMBOLIC_LINK",
                    "JOURNALED", "JOURNAL_LIBRARY", "JOURNAL_NAME", "OBJECT_JID", "FILE_SYSTEM_IDENTIFIER", "FILE_IDENTIFIER_NUMBER", "GENERATION_IDENTIFIER")) {
                cases.add(Arguments.of(row, column, null));
            }
            cases.add(Arguments.of(row, "PATH_NAME", "/QSYS.LIB/LIB.LIB/OTHER.FILE"));
            cases.add(Arguments.of(row, "OBJECT_TYPE", row == 2 ? "*MBR" : "*FILE"));
            cases.add(Arguments.of(row, "ASP_NUMBER", 2));
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
    @MethodSource("mismatches")
    void rejectsIncompleteAndMismatchedBindings(int row, String column, Object value) throws Exception {
        Fixture f = new Fixture();
        f.data.get(row).put(column, value);
        assertThrows(SQLException.class, f::collect);
        f.closed();
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 2, 3 })
    void rejectsDuplicateMissingWarningAndFailedQueries(int fault) throws Exception {
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
            assertThrows(SQLException.class, f::collect);
            f.closed();
        }
    }

    @Test
    void rejectsIdenticalNativeObjectsAndReadOnlyRefusal() throws Exception {
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
            for (var row : data) {
                ResultSet result = mock(ResultSet.class);
                PreparedStatement statement = mock(PreparedStatement.class);
                when(statement.executeQuery()).thenReturn(result);
                when(result.next()).thenReturn(true, false);
                when(result.getString(anyString())).thenAnswer(i -> row.get(i.getArgument(0)) == null ? null : row.get(i.getArgument(0)).toString());
                when(result.getBigDecimal(anyString())).thenAnswer(i -> row.get(i.getArgument(0)) == null ? null : new BigDecimal(row.get(i.getArgument(0)).toString()));
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
