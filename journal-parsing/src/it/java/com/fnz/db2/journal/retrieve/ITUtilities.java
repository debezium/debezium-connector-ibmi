package com.fnz.db2.journal.retrieve;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400SecurityException;
import com.ibm.as400.access.CommandCall;
import com.ibm.as400.access.ErrorCompletingRequestException;

import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;

public class ITUtilities {
	private static final Logger log = LoggerFactory.getLogger(JournalBufferFullIT.class);

	public ITUtilities() {
	}

	public static List<String> allTables(Connection con, String schema) throws SQLException {
		final List<String> tables = new ArrayList<>();

		try (final PreparedStatement ps = con.prepareStatement(
				"select table_name, SYSTEM_TABLE from qsys2.systables where SYSTEM_TABLE='N' and table_schema=?")) {
			ps.setString(1, schema);
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					final String table = rs.getString(1);
					tables.add(table);
				}
			}
		}
		return tables;
	}

	public static void truncateAllTables(Connection con, String schema) throws SQLException {
		final List<String> tables = allTables(con, schema);
		try (final Statement stable = con.createStatement()) {
			for (final String table : tables) {
				log.info("truncating {}.{}", schema, table);
				stable.executeUpdate(String.format("truncate table %s.%s", schema, table));
			}
		}
	}

	public static void dropAllTables(Connection con, String schema) throws SQLException {
		final List<String> tables = allTables(con, schema);
		try (final Statement stable = con.createStatement()) {
			for (final String table : tables) {
				stable.executeUpdate(String.format("drop table %s.%s", schema, table));
			}
		}
	}

	public static void createSchema(Connection con, String schema) throws SQLException {
		boolean exists = false;
		try (final PreparedStatement ps = con
				.prepareStatement("SELECT * FROM TABLE (QSYS2.OBJECT_STATISTICS(?,'LIB')) AS A")) {
			ps.setString(1, schema);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					exists = true;
				}
			}
		}
		try (final Statement st = con.createStatement()) {
			if (!exists) {
				st.executeUpdate(String.format("create schema %s", schema));
			}
		}
	}

	public static boolean as400Command(AS400 as400, String command) throws AS400SecurityException,
	ErrorCompletingRequestException, IOException, InterruptedException, PropertyVetoException {
		final CommandCall call = new CommandCall(as400);
		final boolean result = call.run(command);
		final AS400Message[] messagelist = call.getMessageList();
		for (int j = 0; j < messagelist.length; ++j) {
			// Show each message.
			log.info("command {} reponse {}", command, messagelist[j].getText());
		}
		return result;
	}

	static void deleteReceiversDropSchema(Connection con, AS400 as400, JournalInfo journal) throws Exception {
		dropAllTables(con, journal.journalLibrary());

		final JournalInfoRetrieval journalReceivers = new JournalInfoRetrieval();
		// get receiver list before deleting the receivers from the library
		final List<DetailedJournalReceiver> receviers = journalReceivers.getReceivers(as400, journal);

		as400Command(as400,
				String.format("ENDJRNPF FILE(*ALL) JRN(%s/%s)", journal.journalLibrary(), journal.journalName()));

		as400Command(as400, String.format("DLTJRN JRN(%s/%s)", journal.journalLibrary(), journal.journalName()));

		for (final DetailedJournalReceiver djr : receviers) {
			as400Command(as400, String.format("DLTJRNRCV JRNRCV(%s/%s) DLTOPT(*IGNINQMSG)", journal.journalLibrary(),
					djr.info().receiver().name()));
		}

		try (final Statement st = con.createStatement()) {
			st.executeUpdate(String.format("drop schema %s", journal.journalLibrary()));
		}
	}
}
