/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400NativeTransactionCensusTest {
    @Test
    void completeAuthorizedCensusIncludesUncapturedTableCycles() throws Exception {
        Fixture f = new Fixture();
        assertThat(f.census().requireEmpty(Fixture.JOURNAL)).isEqualTo(1);
        f.resource.put("COMMIT_CYCLE", 42);
        // No pending row changes does NOT make a cycle terminal.
        assertThrows(SQLException.class, () -> f.census().requireEmpty(Fixture.JOURNAL));
        verify(f.connection, atLeastOnce()).close();
    }

    @ParameterizedTest
    @CsvSource({ "LOGICAL_UNIT_OF_WORK_STATE,PREPARED", "LOGICAL_UNIT_OF_WORK_STATE,COMMITTED", "LOGICAL_UNIT_OF_WORK_STATE,ROLLBACK IN PROGRESS",
            "RESOURCE_LOCATION,REMOTE", "ASPGRP,IASP", "XA_TRANSACTION_BRANCH_STATE,IDLE", "JOURNAL_NAME,OTHER", "JOURNAL_LIBRARY,OTHER",
            "PENDING_CHANGES,1", "COMMIT_CYCLE,0", "COMMIT_CYCLE,99" })
    void unresolvedOrForeignResourceRejects(String key, String value) throws Exception {
        Fixture f = new Fixture();
        f.resource.put(key, value);
        assertThrows(SQLException.class, () -> f.census().requireEmpty(Fixture.JOURNAL));
    }

    @ParameterizedTest
    @ValueSource(strings = { "LOCK_SPACE_ID", "LOGICAL_UNIT_OF_WORK_STATE", "ASPGRP", "RESOURCE_LOCATION", "JOURNAL_LIBRARY", "JOURNAL_NAME", "PENDING_CHANGES" })
    void partialEvidenceRejects(String key) throws Exception {
        Fixture f = new Fixture();
        f.resource.remove(key);
        assertThrows(SQLException.class, () -> f.census().requireEmpty(Fixture.JOURNAL));
    }

    @ParameterizedTest
    @ValueSource(strings = { "authority", "unknown-authority", "authority-loss", "duplicate", "limit", "warning", "readonly" })
    void neverMistakesRestrictedVisibilityOrTruncationForEmpty(String fault) throws Exception {
        Fixture f = new Fixture();
        switch (fault) {
            case "authority" -> f.authority.put("SQLADM", 0);
            case "unknown-authority" -> f.authority.put("SQLADM", null);
            case "authority-loss" -> f.loseAuthority = true;
            case "duplicate" -> f.resources.add(f.resource);
            case "limit" -> {
                for (int i = 1; i < 4; i++) {
                    var row = new HashMap<>(f.resource);
                    row.put("LOCK_SPACE_ID", String.format("%020d", i));
                    f.resources.add(row);
                }
            }
            case "warning" -> when(f.connection.getWarnings()).thenReturn(new SQLWarning("incomplete"));
            case "readonly" -> when(f.connection.isReadOnly()).thenReturn(false);
        }
        assertThrows(SQLException.class, () -> f.census().requireEmpty(Fixture.JOURNAL));
        if (fault.equals("authority") || fault.equals("unknown-authority")) {
            verify(f.connection, never()).prepareStatement(As400NativeTransactionCensus.CENSUS_SQL);
        }
    }

    static class Fixture {
        static final JournalReceiver JOURNAL = new JournalReceiver("QSQJRN", "LIB");
        final Connection connection = mock(Connection.class);
        final Map<String, Object> authority = new HashMap<>(Map.of("JOBCTL", 0, "SQLADM", 1, "SYSMON", 0));
        final Map<String, Object> resource = new HashMap<>(Map.of("LOCK_SPACE_ID", "00000000000000000000", "LOGICAL_UNIT_OF_WORK_STATE", "RESET",
                "ASPGRP", "*SYSBAS", "RESOURCE_LOCATION", "LOCAL", "JOURNAL_LIBRARY", "LIB", "JOURNAL_NAME", "QSQJRN", "PENDING_CHANGES", 0));
        final List<Map<String, Object>> resources = new ArrayList<>(List.of(resource));
        boolean loseAuthority;

        Fixture() throws Exception {
            when(connection.isReadOnly()).thenReturn(true);
            when(connection.prepareStatement(anyString())).thenAnswer(i -> {
                String sql = i.getArgument(0);
                PreparedStatement st = mock(PreparedStatement.class);
                when(st.executeQuery()).thenAnswer(q -> {
                    if (sql.equals(As400NativeTransactionCensus.CENSUS_SQL)) {
                        if (loseAuthority) {
                            authority.put("SQLADM", 0);
                        }
                        return As400NativeBindingAcquisitionTest.rows(resources);
                    }
                    return As400NativeBindingAcquisitionTest.rows(List.of(new HashMap<>(authority)));
                });
                return st;
            });
        }

        As400NativeTransactionCensus census() {
            return new As400NativeTransactionCensus(() -> connection, 3, 5);
        }
    }
}
