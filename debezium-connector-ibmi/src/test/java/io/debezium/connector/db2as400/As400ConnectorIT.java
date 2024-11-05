/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import io.debezium.connector.db2as400.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.util.Testing;

public class As400ConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE = "TESTT";

    @Before
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
}
