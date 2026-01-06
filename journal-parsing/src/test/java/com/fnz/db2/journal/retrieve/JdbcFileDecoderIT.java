/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheHash;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import io.debezium.ibmi.db2.journal.test.TestConnector;

public class JdbcFileDecoderIT {

    private static final Logger log = LoggerFactory.getLogger(JdbcFileDecoderIT.class);

    static Connect<Connection, SQLException> sqlConnect;
    static String database;

    static String schema = "MSDECODER";

    @BeforeAll
    public static void setup() throws Exception {
        final TestConnector connector = new TestConnector();
        sqlConnect = connector.getJdbc();
        database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());

        setupSchemaAndTable();
    }

    @Test
    void testToDataType() throws Exception {
        final JdbcFileDecoder decoder = new JdbcFileDecoder(sqlConnect, database, new SchemaCacheHash(), -1, -1);
        final Optional<TableInfo> info = decoder.getRecordFormat("DCDTBL", schema);
        assertTrue(info.isPresent());
        assertEquals(List.of("ID"), info.get().getPrimaryKeys());
    }

    public static void setupSchemaAndTable() throws SQLException {
        final Connection con = sqlConnect.connection(); // debezium doesn't close connections
        try (Statement stmt = con.createStatement()) {
            createSchema(stmt);
            createEmptyTable(stmt);

            log.debug("create schema");
        }
    }

    private static void createSchema(Statement stmt) throws SQLException {
        try (ResultSet rs = stmt
                .executeQuery("select count(1) from qsys2.systables where table_schema='MSDECODER'")) {
            if (rs.next()) {
                final int found = rs.getInt(1);
                if (found < 1) {
                    log.debug("create schema");
                    stmt.executeUpdate("create schema MSDECODER");
                }
            }
        }
    }

    private static void createEmptyTable(Statement stmt) throws SQLException {
        try {
            stmt.executeUpdate("drop TABLE MSDECODER.DCDTBL");
        }
        catch (final SQLException e) {
            // ignore we don't care if it doesn't exist yet
        }

        stmt.executeUpdate(
                "CREATE OR replace TABLE MSDECODER.DCDTBL (id int, text char(10) ccsid 285, primary key (id))");
    }
}