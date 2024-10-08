/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2as400.util.TestHelper;
import io.debezium.embedded.AbstractConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

public class As400ConnectorIT extends AbstractConnectorTest {
    private static final Logger log = LoggerFactory.getLogger(As400ConnectorIT.class);

    private static final String TABLE = "TEST_TABLE";

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
        // Testing.Debug.enable();
        final var config = TestHelper.defaultConfig(TABLE);

        start(As400RpcConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        var records = consumeRecordsByTopic(1);

        records.print();

        // JdbcConnection conn = TestHelper.testConnection().setAutoCommit(true);
        // conn.execute("INSERT INTO " + TABLE + " VALUES (2, 'second')",
        // "INSERT INTO " + TABLE + " VALUES (3, 'third')");

        JdbcConnection conn = TestHelper.testConnection().setAutoCommit(false);
        conn.executeWithoutCommitting("INSERT INTO " + TABLE + " VALUES (2, 'second')",
                "INSERT INTO " + TABLE + " VALUES (3, 'third')");
        conn.rollback();
        // conn.commit();

        records = consumeRecordsByTopic(2);

        records.print();

        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }
}
