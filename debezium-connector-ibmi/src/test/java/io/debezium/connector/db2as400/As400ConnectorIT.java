/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

public class As400ConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE = "TESTT";

    @BeforeEach
    public void before() throws SQLException {
        initializeConnectorTestFramework();
        TestHelper.testConnection().execute(
                "DELETE FROM " + TABLE,
                "INSERT INTO " + TABLE + " VALUES (1, 'first')");
    }

    @Test
    public void shouldSnapshotAndStream() throws Exception {
        Testing.Print.enable();
        final var config = TestHelper.defaultConfig(TABLE);

        start(As400RpcConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        var records = consumeRecordsByTopic(1);

        TestHelper.testConnection().execute(
                "INSERT INTO " + TABLE + " VALUES (2, 'second')",
                "INSERT INTO " + TABLE + " VALUES (3, 'third')");

        records = consumeRecordsByTopic(2);

        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }

    // The SQL case validates only systems where TRUNCATE IMMEDIATE produces an F/CR entry.
    @ParameterizedTest
    @ValueSource(strings = {
            "CALL QSYS2.QCMDEXC('CLRPFM FILE(DTEST/TESTT) MBR(TESTT)')",
            "TRUNCATE TABLE DTEST.TESTT IMMEDIATE"
    })
    public void shouldCaptureClearAndThenReload(String clear) throws Exception {
        final var config = TestHelper.defaultConfig(TABLE).edit().with("skipped.operations", "none").build();
        start(As400RpcConnector.class, config);
        try {
            assertConnectorIsRunning();
            consumeRecordsByTopic(1);
            try (var connection = TestHelper.testConnection()) {
                // Exercise the non-transactional path. Auto-commit alone does not disable commitment control on IBM i.
                connection.connection().setTransactionIsolation(Connection.TRANSACTION_NONE);
                connection.execute(clear, "INSERT INTO DTEST.TESTT VALUES (2, 'reloaded')");
            }
            var records = consumeRecordsByTopic(2).allRecordsInOrder();
            assertThat(records).hasSize(2);
            assertThat(records.get(0).key()).isNull();
            assertThat(records.get(0).keySchema()).isNull();
            Struct truncate = (Struct) records.get(0).value();
            assertThat(truncate.getString("op")).isEqualTo("t");
            assertThat(truncate.get("before")).isNull();
            assertThat(truncate.get("after")).isNull();
            assertThat(((Struct) records.get(1).value()).getString("op")).isEqualTo("c");
            assertNoRecordsToConsume();
        }
        finally {
            stopConnector();
        }
    }
}
