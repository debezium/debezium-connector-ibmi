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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400JournalFileScopeTest {
    @Test
    void actualNativeScopeIncludesUnconfiguredFileAndUsesBoundedJobLocalOutput() throws Exception {
        var fixture = new Fixture();
        Collections.reverse(fixture.objects);
        var result = fixture.collect();
        assertThat(result.files()).extracting(As400OperatorTableEvidence.Request::file).containsExactly("A", "B", "EXTRA", "M", "U");
        assertThat(result.journaledLibraries()).containsExactly("DCNC0946");
        assertThat(fixture.commands).containsExactly("CHGJOB INQMSGRPY(*DFT)",
                "CRTDUPOBJ OBJ(QAWRKJRNA) FROMLIB(QSYS) OBJTYPE(*FILE) TOLIB(QTEMP) NEWOBJ(DCGSCOPE) DATA(*NO)",
                "CHGPF FILE(QTEMP/DCGSCOPE) SIZE(128 0 0)",
                "WRKJRNA JRN(DCNC0946/QSQJRN) OUTPUT(*OUTFILE) OUTFILE(QTEMP/DCGSCOPE) OUTMBR(*FIRST *REPLACE) DETAIL(*OUTPUT)");
        for (var statement : fixture.statements) {
            verify(statement).setQueryTimeout(5);
            verify(statement).close();
        }
        verify(fixture.connection, never()).commit();
        verify(fixture.connection, never()).close(); // Caller owns the dedicated connection.
    }

    @ParameterizedTest
    @ValueSource(strings = { "extra-member", "remote", "missing-count", "excessive", "fraction", "duplicate-file", "missing-file", "foreign",
            "logical", "unknown-resource", "asp", "implicit", "changed-count", "command-error", "command-warning", "row-warning", "transaction" })
    void incompleteOrUnsupportedNativeScopeRejects(String fault) throws Exception {
        var f = new Fixture();
        switch (fault) {
            case "extra-member" -> f.before.put("JOURNALED_MEMBERS", 6);
            case "remote" -> f.before.put("NUMBER_REMOTE_JOURNALS", 1);
            case "missing-count" -> f.before.remove("JOURNALED_FILES");
            case "excessive" -> f.before.put("JOURNALED_FILES", 51);
            case "fraction" -> f.before.put("JOURNALED_FILES", 5.5);
            case "duplicate-file" -> f.objects.add(f.objects.get(0));
            case "missing-file" -> f.objects.remove(0);
            case "foreign" -> f.objects.get(0).put("JOURNAL_NAME", "OTHER");
            case "logical" -> f.objects.get(0).put("FILE_TYPE", "LF");
            case "unknown-resource" -> f.objects.get(0).put("OBJECT_TYPE", "*DTAARA");
            case "asp" -> f.objects.get(0).put("ASP", "2");
            case "implicit" -> f.objects.get(0).put("JOURNALED_TYPE", "1");
            case "changed-count" -> f.after.put("JOURNALED_FILES", 6);
            case "command-error" -> f.commandError = true;
            case "command-warning" -> f.commandWarning = true;
            case "row-warning" -> f.rowWarning = true;
            case "transaction" -> when(f.connection.getAutoCommit()).thenReturn(false);
            default -> throw new AssertionError(fault);
        }
        assertThrows(Exception.class, f::collect);
        for (var statement : f.statements) {
            verify(statement).close();
        }
        if (List.of("extra-member", "remote", "missing-count", "excessive", "fraction", "transaction").contains(fault)) {
            assertThat(f.commands).isEmpty();
        }
    }

    @Test
    void stoppedDiscoveryCannotStartNativeTemporaryWork() throws Exception {
        var f = new Fixture();
        Thread.currentThread().interrupt();
        try {
            assertThrows(SQLException.class, f::collect);
            assertThat(f.statements).isEmpty();
        }
        finally {
            Thread.interrupted();
        }
    }

    private static final class Fixture {
        final Connection connection = mock(Connection.class);
        final List<PreparedStatement> statements = new ArrayList<>();
        final List<String> commands = new ArrayList<>();
        final List<ObjectNode> objects = new ArrayList<>();
        final ObjectNode before;
        final ObjectNode after;
        boolean commandError, commandWarning, rowWarning;
        int counts;

        Fixture() throws Exception {
            try (var stream = getClass().getResourceAsStream("/native-transactions/automatic-fence-native-scope.json")) {
                var fixture = new ObjectMapper().readTree(stream).path("native_scope").path("discovered");
                before = fixture.path("counts").get(0).deepCopy();
                after = before.deepCopy();
                fixture.path("objects").forEach(row -> objects.add(row.deepCopy()));
            }
            when(connection.getAutoCommit()).thenReturn(true);
            when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_NONE);
            when(connection.prepareStatement(anyString())).thenAnswer(call -> {
                String sql = call.getArgument(0);
                var statement = mock(PreparedStatement.class);
                statements.add(statement);
                if (sql.equals("CALL QSYS2.QCMDEXC(?)")) {
                    org.mockito.Mockito.doAnswer(set -> {
                        commands.add(set.getArgument(1));
                        return null;
                    }).when(statement).setString(org.mockito.ArgumentMatchers.eq(1), anyString());
                    when(statement.execute()).thenAnswer(execute -> {
                        if (commandError) {
                            throw new SQLException("native output failed");
                        }
                        return false;
                    });
                    when(statement.getWarnings()).thenAnswer(w -> commandWarning ? new SQLWarning("incomplete") : null);
                }
                else {
                    List<? extends JsonNode> data = sql.equals(As400JournalFileScope.FILES_SQL) ? objects : List.of(counts++ == 0 ? before : after);
                    when(statement.executeQuery()).thenAnswer(execute -> {
                        var rows = mock(ResultSet.class);
                        var index = new AtomicInteger(-1);
                        when(rows.next()).thenAnswer(next -> index.incrementAndGet() < data.size());
                        when(rows.getString(anyString())).thenAnswer(get -> data.get(index.get()).path(get.<String> getArgument(0)).asText(null));
                        when(rows.getBigDecimal(anyString())).thenAnswer(get -> {
                            var value = data.get(index.get()).path(get.<String> getArgument(0));
                            return value.isMissingNode() ? null : new BigDecimal(value.asText());
                        });
                        when(rows.getWarnings()).thenAnswer(w -> rowWarning ? new SQLWarning("incomplete") : null);
                        return rows;
                    });
                }
                return statement;
            });
        }

        As400JournalFileScope.Scope collect() throws SQLException {
            return new As400JournalFileScope(new JournalReceiver("QSQJRN", "DCNC0946"), 5).collect(connection);
        }
    }
}
