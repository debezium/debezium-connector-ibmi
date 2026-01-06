/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalEntryType;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfig;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheHash;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.Structure;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.test.TestConnector;

// this file is utf-8

class JournalEntryDecoderTestIT {
    private static final Logger log = LoggerFactory.getLogger(JournalEntryDecoderTestIT.class);

    static Connect<AS400, IOException> as400Connect;
    static Connect<Connection, SQLException> sqlConnect;

    static String schema = "MSTYPE1";

    @BeforeAll
    public static void setup() throws Exception {
        final TestConnector connector = new TestConnector();
        as400Connect = connector.getAs400();
        sqlConnect = connector.getJdbc();

        setupSchema();
    }

    @Test
    public void testCharacterTypes() throws Exception {
        final long cacheWait = JournalInfoRetrieval.getJournalCacheDurationInMilliseconds(sqlConnect);
        final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval(cacheWait, 0, 2000);
        final JournalInfo journal = journalInfoRetrieval.getJournal(as400Connect.connection(), schema);
        final Instant now = Instant.now();
        final JournalPosition current = journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), journal);
        final JournalProcessedPosition position = new JournalProcessedPosition(current, now, true);
        log.debug("starting at {}", position);

        insertData();

        final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal).build();
        final RetrieveJournal rj = new RetrieveJournal(config, journalInfoRetrieval);

        final SchemaCacheHash schemaHash = new SchemaCacheHash();

        final String database = JdbcFileDecoder.getDatabaseName(sqlConnect.connection());
        final JdbcFileDecoder fileDecoder = new JdbcFileDecoder(sqlConnect, database, schemaHash, -1, -1);
        boolean foundInsert = false;
        RetrievalState success = RetrievalState.Success;

        for (int i = 0; success.hasData() && !foundInsert && i < 100; i++) { // journal entries are not available straight away
            success = rj.retrieveJournal(position);
            log.info("position {} : {}", position, success);
            // verify that the journal entry can be decoded
            while (success.hasData() && !foundInsert && rj.nextEntry()) {
                final EntryHeader eheader = rj.getEntryHeader();
                log.info(eheader.toString());
                final JournalEntryType journalEntryType = eheader.getJournalEntryType();
                log.info("journal type {}", journalEntryType);
                if (journalEntryType == null) {
                    continue;
                }
                switch (journalEntryType) {
                    case ADD_ROW2, ADD_ROW1:
                        log.info("add row {} : {}", eheader.getObjectName(), eheader.getLibrary());
                        if ("CHARTYPES".equals(eheader.getFile()) && "MSTYPE1".equals(eheader.getLibrary())) {
                            final Object[] values = rj.decode(fileDecoder);
                            final AS400Text conv = new AS400Text(10, 37);
                            final TableInfo ti = schemaHash.retrieve(database, schema, "CHARTYPES");
                            assertNotNull(ti);
                            final List<Structure> structures = ti.getStructure();
                            final Map<String, Object> map = toValues(structures, values);
                            assertEquals("fixed", ((String) map.get("FCHAR")).trim());
                            assertEquals("var", ((String) map.get("VCHAR")).trim());
                            assertEquals("Paßstraße", ((String) map.get("FGRAPH")).trim());
                            assertEquals("Maſʒſtab", ((String) map.get("VGRAPH")).trim());
                            log.info(bytesToHex(conv.toBytes("abcdefghij")));
                            log.info(bytesToHex(((byte[]) map.get("FBIN"))));
                            assertArrayEquals(conv.toBytes("abcdefghij"), ((byte[]) map.get("FBIN")), "fixed binary");
                            assertArrayEquals(conv.toBytes("klnmopqrst"), ((byte[]) map.get("VBIN")), "var binary");

                            foundInsert = true;
                        }
                        break;
                    default:
                        break;
                }
                final JournalProcessedPosition next = rj.getPosition();
                if (position.equals(next)) {
                    Thread.sleep(100);
                }
                position.setPosition(next);
            }
        }

        assertEquals(true, foundInsert, "insert journal entry found");
    }

    private void insertData() throws SQLException {
        final Connection con = sqlConnect.connection(); // debezium doesn't close connections
        try (Statement stmt = con.createStatement()) {

            // bin is ebidic a385a2a3 not ascii 74657374
            final int added = stmt.executeUpdate("INSERT INTO MSTYPE1.CHARTYPES (fchar, vchar, "
                    + "fgraph, "
                    + " vgraph, "
                    + "fbin, vbin) "
                    + "VALUES ('fixed', 'var',"
                    + " 'Paßstraße', "
                    + "'Maſʒſtab', "
                    + "CAST('abcdefghij' AS BINARY(10)) , CAST('klnmopqrst' AS VARBINARY(10)))");

            assertEquals(1, added, "inserted one row");

            // verify we can find the data using sql
            try (ResultSet rs = stmt
                    .executeQuery("""
                            select fchar, vchar, fgraph,"""
                            + "vgraph,"
                            + "fbin, vbin from MSTYPE1.CHARTYPES")) {
                final AS400Text conv = new AS400Text(10, 37);
                if (rs.next()) {
                    assertEquals("fixed", rs.getString("fchar").trim());
                    assertEquals("var", rs.getString("vchar").trim());
                    assertEquals("Paßstraße", rs.getString("fgraph").trim());
                    assertEquals("Maſʒſtab", rs.getString("vgraph").trim());
                    assertArrayEquals(conv.toBytes("abcdefghij"), rs.getBytes("fbin"));
                    assertArrayEquals(conv.toBytes("klnmopqrst"), rs.getBytes("vbin"));
                }
                else {
                    fail("should find a result");
                }
            }
        }
    }

    private static void setupSchema() throws SQLException {
        final Connection con = sqlConnect.connection(); // debezium doesn't close connections
        try (Statement stmt = con.createStatement()) {
            createSchema(stmt);
            createEmptyTable(stmt);
        }
    }

    private static void createSchema(Statement stmt) throws SQLException {
        try (ResultSet rs = stmt
                .executeQuery("select count(1) from qsys2.systables where table_schema='MSTYPE1'")) {
            if (rs.next()) {
                final int found = rs.getInt(1);
                if (found < 1) {
                    log.debug("create schema");
                    stmt.executeUpdate("create schema MSTYPE1");
                }
            }
        }

        log.debug("create schema");
    }

    private static void createEmptyTable(Statement stmt) throws SQLException {
        try {
            stmt.executeUpdate("drop TABLE MSTYPE1.CHARTYPES");
        }
        catch (final SQLException e) {
            e.printStackTrace();
            // ignore we don't care if it doesn't exist yet
        }

        stmt.executeUpdate("""
                CREATE TABLE MSTYPE1.CHARTYPES (
                        fchar CHAR(12), vchar VARCHAR(11),
                        fgraph GRAPHIC(10) CCSID 13488,  """
                + "vgraph VARGRAPHIC(10) CCSID 13488,"
                + " fbin binary(10),  vbin varbinary(10)  )");

        stmt.executeUpdate("delete from MSTYPE1.CHARTYPES");
    }

    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);

    public static String bytesToHex(byte[] bytes) {
        final byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            final int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    public Map<String, Object> toValues(List<Structure> structures, Object[] values) {
        final Map<String, Object> map = new HashMap<>();

        for (int i = 0; i < structures.size(); i++) {
            map.put(structures.get(i).getName(), values[i]);
        }
        return map;
    }

}
