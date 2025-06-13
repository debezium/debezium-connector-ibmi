package com.fnz.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;
import com.fnz.db2.journal.test.TestConnector;
import com.ibm.as400.access.AS400;

class JournalConcurrentUpdatesResetJournalsIT {
	private static final int MAX_BUFFER_SIZE = 1024 * 64;
	private static final int MAX_ENTRIES_TO_FETCH = 200;

	private static final Logger log = LoggerFactory.getLogger(JournalConcurrentUpdatesResetJournalsIT.class);

	static final String SCHEMA = "JRN_CUP"; // note upper case
	static final String TABLE = "SEQ1"; // note upper case
	static final String IGNORE_TABLE = "IGNORE";

	private static final int MAX_UPDATES = 500;

	static TestConnector connector = null;
	static JournalInfo journal;

	final static List<FileFilter> includes = List.of(new FileFilter(SCHEMA, TABLE));

	int nextValue = 0;
	volatile int dummyUpdates = 0;
	volatile int realUpdates = 0;

	// TODO test journal changeover

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

	public JournalConcurrentUpdatesResetJournalsIT() {
	}

	@Test
	void testConcurrentUpdatesAndJournalReset() throws Exception {
		final ITRetrievalHelper helper = new ITRetrievalHelper(connector, journal, includes, MAX_BUFFER_SIZE,
				MAX_ENTRIES_TO_FETCH, this::processUpdate, r -> null);

		helper.setStartPositionToNow();

		log.debug("insert data");
		final Thread t = new Thread(() -> insertingRealData(connector, 51, journal));
		t.start();

		final Thread dt = new Thread(() -> insertingDummyData(connector, 51, journal));
		dt.start();

		helper.retrieveJournalEntries(x -> {
			log.debug("waiting for {}={}", nextValue, MAX_UPDATES);
			return (nextValue != MAX_UPDATES);
		});
		log.debug("verify");
		assertEquals(MAX_UPDATES, nextValue);
		assertEquals(realUpdates, MAX_UPDATES);
		assertTrue(dummyUpdates > 0);
	}

	public Void processUpdate(final Object[] fields, TableInfo tableInfo, String tableName) {
		log.debug("found {}, table", tableInfo.getStructure().get(1).getName());
		assertEquals("VALUE", tableInfo.getStructure().get(1).getName());
		assertEquals(nextValue, (Integer) fields[1]);
		final int i = (Integer) fields[1];
		log.debug("found {} expecting {}", i, nextValue);
		assertEquals(nextValue, i);
		nextValue++;

		return null;
	}

	void insertingRealData(TestConnector connector, int maxBatchSize, JournalInfo journal) {
		try {
			try (Connection con = connector.getNewJdbcConnection()) {
				final AS400 as400 = connector.getAs400().connection();
				final Random r = new Random();

				try (final PreparedStatement ps = con
						.prepareStatement(String.format("update %s.%s set value=?", SCHEMA, TABLE))) {
					for (int i = 0; i < MAX_UPDATES;) {

						final int n1 = r.nextInt(maxBatchSize);
						for (int j = 0; j < n1 && i < MAX_UPDATES; j++, i++) {
							ps.setInt(1, i);
							ps.addBatch();
						}
						final int[] s = ps.executeBatch();
						for (final int x : s) {
							realUpdates += x;
						}
						// ITUtilities.as400Command(as400, "sudo/powerup");
						//					log.info("add new receiver {}", i);
						ITUtilities.as400Command(as400, String.format("CHGJRN JRN(%s/%s) JRNRCV(*GEN) SEQOPT(*RESET)",
								journal.journalLibrary(), journal.journalName()));
						//					final int d = r.nextInt(50);
						//					Thread.sleep(d);
						// ITUtilities.as400Command(as400, "sudo/powerdown");
					}
				}
			}
		} catch (final Exception e) {
			log.error("inserting failed", e);
		}

	}

	void insertingDummyData(TestConnector connector, int maxBatchSize, JournalInfo journal) {
		try {
			try (Connection con = connector.getNewJdbcConnection()) {
				final Random r = new Random();

				try (final PreparedStatement ps = con
						.prepareStatement(String.format("update %s.%s set value=?", SCHEMA, IGNORE_TABLE))) {
					for (int i = 0; i < MAX_UPDATES;) {

						final int n1 = r.nextInt(maxBatchSize);
						for (int j = 0; j < n1 && i < MAX_UPDATES; j++, i++) {
							ps.setInt(1, -i);
							ps.addBatch();
						}
						final int[] s = ps.executeBatch();
						for (final int x : s) {
							dummyUpdates += x;
						}
						con.commit();
					}
				}
			}
		} catch (final Exception e) {
			log.error("inserting failed", e);
		}
	}

	private static void setupTables(Connection con) throws SQLException {
		ITUtilities.createSchema(con, SCHEMA);
		ITUtilities.truncateAllTables(con, SCHEMA); // if tables still exist

		try (final Statement st = con.createStatement()) {
			st.executeUpdate(String.format("CREATE OR replace TABLE %s.%s (id int, value int, primary key (id))",
					SCHEMA, TABLE));
			st.executeUpdate(String.format("insert into %s.%s (id, value) values (1, -100)", SCHEMA, TABLE));
			st.executeUpdate(String.format("CREATE OR replace TABLE %s.%s (id int, value int, primary key (id))", SCHEMA, IGNORE_TABLE));
			st.executeUpdate(String.format("insert into %s.%s (id, value) values (0, 0)", SCHEMA, IGNORE_TABLE));
			con.commit();
		}
	}

	static void cleanup(Connection con, AS400 as400, JournalInfo journal) throws Exception {
		ITUtilities.deleteReceiversDropSchema(con, as400, journal);
	}
}
