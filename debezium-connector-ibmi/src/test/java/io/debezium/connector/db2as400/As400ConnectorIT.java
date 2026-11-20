/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Locale;
import java.util.stream.Stream;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.debezium.connector.db2as400.util.TestHelper;
import io.debezium.embedded.async.AbstractAsyncEngineConnectorTest;
import io.debezium.embedded.util.MetricsHelper;
import io.debezium.util.Testing;

public class As400ConnectorIT extends AbstractAsyncEngineConnectorTest {

    private static final String TABLE = "TESTT";
    private static final String QUALIFIED_TABLE = TestHelper.schemaName() + "." + TABLE;

    @BeforeEach
    public void before() throws SQLException {
        initializeConnectorTestFramework();
        try (var connection = TestHelper.testConnection()) {
            connection.execute(
                    "DELETE FROM " + QUALIFIED_TABLE,
                    "INSERT INTO " + QUALIFIED_TABLE + " VALUES (1, 'first')");
        }
    }

    @Test
    public void shouldSnapshotAndStream() throws Exception {
        Testing.Print.enable();
        final var config = TestHelper.defaultConfig(TABLE);

        start(As400RpcConnector.class, config);
        assertConnectorIsRunning();

        // Wait for snapshot completion
        var records = consumeRecordsByTopic(1);

        try (var connection = TestHelper.testConnection()) {
            connection.execute(
                    "INSERT INTO " + QUALIFIED_TABLE + " VALUES (2, 'second')",
                    "INSERT INTO " + QUALIFIED_TABLE + " VALUES (3, 'third')");
        }

        records = consumeRecordsByTopic(2);

        assertNoRecordsToConsume();
        stopConnector();
        assertConnectorNotRunning();
    }

    private static Stream<Arguments> clearCases() {
        return Stream.of("initial", "no_data").flatMap(snapshotMode -> Stream.of(
                "CALL QSYS2.QCMDEXC('CLRPFM FILE(" + TestHelper.schemaName() + "/" + TABLE + ") MBR(" + TABLE + ")')",
                "TRUNCATE TABLE " + QUALIFIED_TABLE + " IMMEDIATE")
                .map(clear -> Arguments.of(clear, snapshotMode)));
    }

    // The SQL case validates only systems where TRUNCATE IMMEDIATE produces an F/CR entry.
    @ParameterizedTest
    @MethodSource("clearCases")
    public void shouldCaptureClearAndThenReload(String clear, String snapshotMode) throws Exception {
        final var config = TestHelper.defaultConfig(TABLE).edit()
                .with("skipped.operations", "none").with("snapshot.mode", snapshotMode).build();
        start(As400RpcConnector.class, config);
        try {
            assertConnectorIsRunning();
            if (snapshotMode.equals("initial")) {
                consumeRecordsByTopic(1);
            }
            // This connector does not update the standard Connected metric. A journal poll proves readiness
            // without requiring a row event before the clear, including in no_data mode.
            await().atMost(Duration.ofSeconds(30)).ignoreExceptions().until(() -> MetricsHelper.<Long> getStreamingMetric(
                    Module.contextName().toLowerCase(Locale.ROOT), "server1", "streaming", "JournalOffset") > 0);
            try (var connection = TestHelper.testConnection()) {
                // Exercise the non-transactional path. Auto-commit alone does not disable commitment control on IBM i.
                connection.connection().setTransactionIsolation(Connection.TRANSACTION_NONE);
                connection.execute(clear, "INSERT INTO " + QUALIFIED_TABLE + " VALUES (2, 'reloaded')");
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
