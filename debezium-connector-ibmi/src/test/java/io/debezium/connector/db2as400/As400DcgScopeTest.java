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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class As400DcgScopeTest {
    @Test
    void completeInventoryIncludesUncapturedFilesAndUsesBoundedExactQueries() throws Exception {
        var f = new Fixture("");
        f.scope.validate(f.connection);
        f.scope.lock(f.connection);
        assertThat(f.objects).containsExactly("APPDATA/ITEMS", "APPDATA.ORDERS".replace('.', '/'), "APPDATA/OTHER");
        assertThat(f.locks).containsExactly("APPDATA/ITEMS", "APPDATA/ORDERS", "APPDATA/OTHER");
        assertThat(f.limits).allMatch(n -> n == 2 || n == 21);
    }

    @ParameterizedTest
    @ValueSource(strings = { "extra-file", "extra-member", "api-resource", "unknown-library", "missing", "duplicate", "stopped-journaling",
            "wrong-journal", "after-images", "multimember", "logical", "null-count", "warning" })
    void incompleteOrUnsupportedScopeCannotQualify(String fault) throws Exception {
        var f = new Fixture(fault);
        assertThrows(SQLException.class, () -> f.scope.validate(f.connection));
    }

    @Test
    void MissingLockOrWarningCannotQualifySession() throws Exception {
        for (String fault : List.of("missing-lock", "lock-warning")) {
            var f = new Fixture(fault);
            assertThrows(SQLException.class, () -> f.scope.verifyLocks(f.connection));
        }
    }

    static final class Fixture {
        final Connection connection = mock(Connection.class);
        final As400DcgScope scope = new As400DcgScope(new As400DcgConfig(io.debezium.config.Configuration.from(As400DcgConfigTest.properties())));
        final List<String> objects = new ArrayList<>();
        final List<String> locks = new ArrayList<>();
        final List<Integer> limits = new ArrayList<>();

        Fixture(String fault) throws Exception {
            when(connection.prepareStatement(anyString())).thenAnswer(call -> {
                String sql = call.getArgument(0);
                var st = mock(PreparedStatement.class);
                Map<Integer, String> params = new HashMap<>();
                doAnswer(a -> {
                    params.put(a.getArgument(0), a.getArgument(1));
                    return null;
                }).when(st).setString(anyInt(), anyString());
                doAnswer(a -> {
                    limits.add(a.getArgument(0));
                    return null;
                }).when(st).setMaxRows(anyInt());
                when(st.executeQuery()).thenAnswer(a -> {
                    List<Object[]> rows;
                    if (sql.equals(As400DcgScope.COUNTS)) {
                        rows = List.<Object[]> of(new Object[]{ fault.equals("extra-file") ? 4 : 3, fault.equals("extra-member") ? 4 : 3,
                                fault.equals("api-resource") ? 1 : 0, 0, 0, 0, fault.equals("unknown-library") ? 1 : 0, 0 });
                    }
                    else if (sql.equals(As400DcgScope.OBJECT)) {
                        objects.add(params.get(1) + "/" + params.get(3));
                        rows = fault.equals("missing") ? List.of()
                                : List.<Object[]> of(new Object[]{ params.get(3), "*FILE",
                                        fault.equals("stopped-journaling") ? "NO" : "YES", "JOURNALS", fault.equals("wrong-journal") ? "OTHER" : "APPJRN",
                                        fault.equals("after-images") ? "*AFTER" : "*BOTH" });
                    }
                    else if (sql.equals(As400DcgScope.MEMBERS)) {
                        rows = List.<Object[]> of(new Object[]{ fault.equals("logical") ? "LOGICAL" : "PHYSICAL", "DATA", fault.equals("multimember") ? 2 : 1 });
                    }
                    else if (sql.equals(As400DcgScope.LOCKS)) {
                        locks.add(params.get(1) + "/" + params.get(2));
                        rows = fault.equals("missing-lock") ? List.of() : List.<Object[]> of(new Object[]{ "*EXCLRD", "HELD" });
                    }
                    else {
                        throw new AssertionError(sql);
                    }
                    if (fault.equals("duplicate")) {
                        rows = new ArrayList<>(rows);
                        rows.add(rows.get(0));
                    }
                    return result(rows, fault);
                });
                return st;
            });
            var st = mock(Statement.class);
            when(connection.createStatement()).thenReturn(st);
            when(st.executeQuery(anyString())).thenAnswer(a -> result(List.<Object[]> of(new Object[]{ "123456/USER/JOB" }), ""));
        }

        private ResultSet result(List<Object[]> rows, String fault) throws Exception {
            var rs = mock(ResultSet.class);
            AtomicInteger index = new AtomicInteger(-1);
            when(rs.next()).thenAnswer(a -> index.incrementAndGet() < rows.size());
            when(rs.getString(anyInt())).thenAnswer(a -> String.valueOf(rows.get(index.get())[(int) a.getArgument(0) - 1]));
            when(rs.getInt(anyInt())).thenAnswer(a -> (int) rows.get(index.get())[(int) a.getArgument(0) - 1]);
            when(rs.wasNull()).thenReturn(fault.equals("null-count"));
            if (fault.equals("warning") || fault.equals("lock-warning")) {
                when(rs.getWarnings()).thenReturn(new SQLWarning("partial authority"));
            }
            return rs;
        }
    }
}
