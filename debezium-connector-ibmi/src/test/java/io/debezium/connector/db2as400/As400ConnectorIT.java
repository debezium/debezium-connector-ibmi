/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.db2as400.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.util.Testing;

public class As400ConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE = "TABLE2";

    @BeforeEach
    public void before() throws SQLException {
        initializeConnectorTestFramework();
        TestHelper.testConnection().execute("DELETE FROM " + TABLE, "INSERT INTO " + TABLE + " VALUES (1, 'first')");
    }

    @Test
    public void shouldSnapshotAndStream() throws Exception {
        Testing.Print.enable();
        final var config = TestHelper.defaultConfig(TABLE);

        start(As400RpcConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        var records = consumeRecordsByTopic(1);

        TestHelper.testConnection().execute("INSERT INTO " + TABLE + " VALUES (2, 'second')", "INSERT INTO " + TABLE + " VALUES (3, 'third')");

        records = consumeRecordsByTopic(2);

        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void testCommitRollback() throws InterruptedException, SQLException {
        Testing.Print.enable();
        // Testing.Debug.enable();
        final var config = TestHelper.defaultConfig(TABLE);

        start(As400RpcConnector.class, config);
        // Wait for snapshot completion
        var records = consumeRecordsByTopic(1);
        TestHelper.testConnection().execute("INSERT INTO " + TABLE + " VALUES (2, 'second')");
        records.print();
        JdbcConnection conn = TestHelper.testConnection().setAutoCommit(true);
        conn.execute("INSERT INTO " + TABLE + " VALUES (2, 'second')", "INSERT INTO " + TABLE + " VALUES (3, 'third')");
        records = consumeRecordsByTopic(3);
        conn = TestHelper.testConnection().setAutoCommit(false);
        conn.executeWithoutCommitting("INSERT INTO " + TABLE + " VALUES (2, 'second')", "INSERT INTO " + TABLE + " VALUES (3, 'third')");

        // ROLLBACK -> no event produced
        // conn.rollback();
        // records = consumeRecordsByTopic(0);

        // COMMIT -> 2 last inserts are produced
        conn.commit();
        records = consumeRecordsByTopic(2);

        records.print();
        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void testUpdateWithTransactionManagement() throws InterruptedException, SQLException {
        Testing.Print.enable();
        final var config = TestHelper.defaultConfig(TABLE);

        start(As400RpcConnector.class, config);
        // Wait for snapshot completion
        var records = consumeRecordsByTopic(1);

        // Insert rows with autocommit so they are available for updates
        JdbcConnection conn = TestHelper.testConnection().setAutoCommit(true);
        conn.execute(
                "INSERT INTO " + TABLE + " VALUES (10, 'ten')",
                "INSERT INTO " + TABLE + " VALUES (11, 'eleven')");
        records = consumeRecordsByTopic(2);
        records.print();

        // Update within an explicit transaction — events should be buffered until commit
        conn = TestHelper.testConnection().setAutoCommit(false);
        conn.executeWithoutCommitting(
                "UPDATE " + TABLE + " SET COLUMN2 = 'TEN-UPDATED' WHERE COLUMN1 = 10",
                "UPDATE " + TABLE + " SET COLUMN2 = 'ELEVEN-UPDATED' WHERE COLUMN1 = 11");
        conn.commit();
        // Expect 2 update events (before image + after image pair per row)
        records = consumeRecordsByTopic(2);
        records.print();

        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }

    @Test
    public void testMixedOperationsWithRollbackAndCommit() throws InterruptedException, SQLException {
        Testing.Print.enable();
        final var config = TestHelper.defaultConfig(TABLE);

        start(As400RpcConnector.class, config);
        // Wait for snapshot (row 1 from @BeforeEach)
        var records = consumeRecordsByTopic(1);

        // --- Setup: insert seed rows with autocommit ---
        JdbcConnection setup = TestHelper.testConnection().setAutoCommit(true);
        setup.execute(
                "INSERT INTO " + TABLE + " VALUES (20, 'twenty')",
                "INSERT INTO " + TABLE + " VALUES (21, 'twenty-one')",
                "INSERT INTO " + TABLE + " VALUES (22, 'twenty-two')");
        records = consumeRecordsByTopic(3);
        records.print();

        // --- Transaction 1: rollback — no events should be produced ---
        JdbcConnection tx1 = TestHelper.testConnection().setAutoCommit(false);
        tx1.executeWithoutCommitting(
                "INSERT INTO " + TABLE + " VALUES (30, 'thirty')",
                "UPDATE " + TABLE + " SET COLUMN2 = 'ROLLED-BACK' WHERE COLUMN1 = 20",
                "DELETE FROM " + TABLE + " WHERE COLUMN1 = 21");
        tx1.rollback();

        // --- Transaction 2: commit with mixed INSERT, UPDATE, DELETE ---
        // These events should only appear after commit
        JdbcConnection tx2 = TestHelper.testConnection().setAutoCommit(false);
        tx2.executeWithoutCommitting(
                "INSERT INTO " + TABLE + " VALUES (31, 'thirty-one')",
                "UPDATE " + TABLE + " SET COLUMN2 = 'TWENTY-UPDATED' WHERE COLUMN1 = 20",
                "DELETE FROM " + TABLE + " WHERE COLUMN1 = 22");
        tx2.commit();
        // Expect 4 records: 1 insert + 1 update + 1 delete
        records = consumeRecordsByTopic(3);
        records.print();

        // tombstone (null value after delete for log compaction)
        records = consumeRecordsByTopic(1,false);
        records.print();

        // --- Autocommit operation after transaction — should dispatch immediately ---
        setup.setAutoCommit(true);
        setup.execute("INSERT INTO " + TABLE + " VALUES (32, 'thirty-two')");
        records = consumeRecordsByTopic(1);
        records.print();

        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }
}
