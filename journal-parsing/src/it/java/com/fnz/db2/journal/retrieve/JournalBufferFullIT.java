package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.OffsetStatus;
import io.debezium.ibmi.db2.journal.test.TestConnector;

/**
 * Tests to verify we retrieve all data when there is a continuation offset set
 * in the data returned Making this happen requires
 *
 * - having a small buffer
 *
 * - having enough data ready to fill the buffer
 *
 * A series of update statements populate the journal before trying to retrieve
 * the data
 *
 * disable the cleanup if you want to keep the journals to see what happened
 */
class JournalBufferFullIT {
	private static final int MAX_ENTRIES_TO_FETCH = 1000;

	private static final Logger log = LoggerFactory.getLogger(JournalBufferFullIT.class);

	static final String SCHEMA = "BUF_FULL"; // note upper case
	static final String TABLE = "SEQ1"; // note upper case

	private static final int MAX_BUFFER_SIZE = 380;
	private static final int MAX_UPDATES = 3;

	static TestConnector connector = null;
	static JournalInfo journal;

	final static List<FileFilter> includes = List.of(new FileFilter(SCHEMA, TABLE));

	int nextValue = 0;
	boolean continuationSet = false;

	@BeforeAll
	public static void setup() throws Exception {
		log.debug("setting up");
		connector = new TestConnector(SCHEMA);
		final Connect<Connection, SQLException> jdbcCon = connector.getJdbc();
		setupTables(jdbcCon.connection());
		JournalInfoRetrieval journalInfoRegrieval = new JournalInfoRetrieval();
		journal = journalInfoRegrieval.getJournal(connector.getAs400().connection(), SCHEMA, includes);
		log.debug("journal {}", journal);

		log.debug("set up");
	}

	@AfterAll
	public static void teardown() throws Exception {
		cleanup(connector.getJdbc().connection(), connector.getAs400().connection(), journal);
	}

	public JournalBufferFullIT() {
	}

	@Test
	void testWithContinuationOffset() throws Exception {
		final ITRetrievalHelper helper = new ITRetrievalHelper(connector, journal, includes, MAX_BUFFER_SIZE, MAX_ENTRIES_TO_FETCH,
				this::processUpdate,
				this::verifyContinuation);

		helper.setStartPositionToNow();

		log.debug("insert data");
		final int updated = insertRealData(connector, 10, MAX_UPDATES);
		assertEquals(MAX_UPDATES, updated);

		helper.retrieveJournalEntries(x -> x);
		log.debug("verify");
		assertEquals(true, continuationSet);
		assertEquals(MAX_UPDATES, nextValue);
	}

	Void verifyContinuation(RetrieveJournal r) {
		continuationSet |= r.getFirstHeader().status() == OffsetStatus.MORE_DATA_NEW_OFFSET;
		return null;
	}


	public Void processUpdate(final Object[] fields, TableInfo tableInfo, String tableName) {
		log.debug("found {}, table", tableInfo.getStructure().get(1).getName());;
		assertEquals("VALUE", tableInfo.getStructure().get(1).getName());
		assertEquals(nextValue, (Integer) fields[1]);
		final int i = (Integer) fields[1];
		log.debug("found {} expecting {}", i, nextValue);
		assertEquals(nextValue, i);
		nextValue++;

		return null;
	}

	static int insertRealData(TestConnector connector, int batchSize, int size) {
		int updatedRows = 0;
		try {
			final Connection con = connector.getJdbc().connection();
			try (final PreparedStatement ps = con
					.prepareStatement(String.format("update %s.%s set value=?", SCHEMA, TABLE))) {
				for (int i = 0; i < size;) {
					for (int j = 0; j < batchSize && i < size; j++, i++) {
						ps.setInt(1, i);
						log.info("setting to {}", i);
						ps.addBatch();
					}
					final int[] s = ps.executeBatch();
					for (final int x : s) {
						updatedRows += x;
					}
					con.commit();
				}
			}
		} catch (final Exception e) {
			log.error("inserting failed", e);
		}
		return updatedRows;
	}

	private static void setupTables(Connection con) throws SQLException {
		ITUtilities.createSchema(con, SCHEMA);
		ITUtilities.truncateAllTables(con, SCHEMA); // if tables still exist

		try (final Statement st = con.createStatement()) {
			st.executeUpdate(String.format(
					"CREATE OR replace TABLE %s.%s (id int, value int, primary key (id))", SCHEMA, TABLE));
			st.executeUpdate(String.format("insert into %s.%s (id, value) values (1, -100)", SCHEMA, TABLE));
			con.commit();
		}
	}


	static void cleanup(Connection con, AS400 as400, JournalInfo journal) throws Exception {
		ITUtilities.deleteReceiversDropSchema(con, as400, journal);
	}
}
