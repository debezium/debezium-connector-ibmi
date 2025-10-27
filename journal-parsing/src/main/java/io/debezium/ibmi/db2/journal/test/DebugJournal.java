/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.data.types.Diagnostics;
import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.JournalRecordDecoder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfig;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.FirstHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.OffsetStatus;

public class DebugJournal {
    private static final Logger log = LoggerFactory.getLogger(DebugJournal.class);

    public static void main(String[] args) throws Exception {
        final TestConnector connector = new TestConnector();
        final Connect<AS400, IOException> as400Connect = connector.getAs400();
        final Connect<Connection, SQLException> sqlConnect = connector.getJdbc();
        final String schema = connector.getSchema();
        JournalInfoRetrieval jir = new JournalInfoRetrieval(30000, 5000, 2000);

        final byte[] data = Files.readAllBytes(Paths.get("C:\\dev\\kafka\\journal-parsing\\good-journal\\201218-0616-0"));
        final JournalInfo journal = jir.getJournal(as400Connect.connection(), schema);
        final RetrieveConfig config = new RetrieveConfigBuilder().withAs400(as400Connect).withJournalInfo(journal).build();
        final RetrieveJournal rnj = new RetrieveJournal(config, jir);

        rnj.setOutputData(data,
                new FirstHeader(data.length, 0, data.length, OffsetStatus.NO_DATA, new JournalProcessedPosition()),
                new JournalProcessedPosition());
        if (rnj.nextEntry()) {
            final EntryHeader entry = rnj.getEntryHeader();
            final String code = String.format("%s %s", entry.getJournalCode(), entry.getEntryType());
            log.info("code: {}", code);
            switch (code) {
                case "J NR":
                    final JournalReceiver receiver = rnj.decode(new JournalRecordDecoder());
                    log.info("receiver {}", receiver);
                    break;
                default:
                    break;

            }
            log.info("header {}", rnj.getEntryHeader().toString());
            rnj.dumpEntry();
            log.info("dump from entry start");
            log.info(Diagnostics.binAsHex(data, rnj.getOffset() + rnj.getEntryHeader().getEntrySpecificDataOffset(), rnj.getEntryHeader().getLength()));
            log.info(Diagnostics.binAsEbcdic(data, rnj.getOffset() + rnj.getEntryHeader().getEntrySpecificDataOffset(), rnj.getEntryHeader().getLength()));

            rnj.dumpEntry();
        }
    }

}
