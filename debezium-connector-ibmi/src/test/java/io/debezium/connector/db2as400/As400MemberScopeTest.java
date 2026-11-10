/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.jupiter.api.Test;

import io.debezium.jdbc.JdbcConnection.ResultSetMapper;
import io.debezium.jdbc.JdbcConnection.StatementPreparer;

class As400MemberScopeTest {
    @Test
    void acceptsOnlyMatchingSingleMember() throws Exception {
        assertThat(check("MEMBER", true, false)).isTrue();
    }

    @Test
    void rejectsMissingMismatchedOrMultipleMembers() throws Exception {
        assertThat(check("MEMBER", false, false)).isFalse();
        assertThat(check("OTHER", true, false)).isFalse();
        assertThat(check("", true, false)).isFalse();
        assertThat(check("MEMBER", true, true)).isFalse();
        assertThat(check("", true, true)).isFalse();
    }

    private boolean check(String member, boolean first, boolean second) throws Exception {
        As400JdbcConnection connection = mock(As400JdbcConnection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet result = mock(ResultSet.class);
        when(result.next()).thenReturn(first, second);
        when(result.getString(1)).thenReturn("MEMBER");
        when(connection.isOnlyMember(anyString(), anyString(), anyString())).thenCallRealMethod();
        doAnswer(i -> {
            StatementPreparer preparer = i.getArgument(1);
            ResultSetMapper<Boolean> mapper = i.getArgument(2);
            preparer.accept(statement);
            return mapper.apply(result);
        }).when(connection).prepareQueryAndMap(anyString(), any(), any());
        boolean matches = connection.isOnlyMember("LIB", "TEST", member);
        verify(statement).setString(1, "LIB");
        verify(statement).setString(2, "TEST");
        return matches;
    }
}
