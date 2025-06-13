/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.test;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;

public class JournalInfoMain {
    private static final Logger log = LoggerFactory.getLogger(JournalInfoMain.class);

    public static void main(String[] args) throws Exception {
        TestConnector connector = new TestConnector();
        Connect<AS400, IOException> as400Connect = connector.getAs400();
        String schema = connector.getSchema();
        JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();
        JournalInfo ji = journalInfoRetrieval.getJournal(as400Connect.connection(), schema, "PERSON");

        List<DetailedJournalReceiver> jri = journalInfoRetrieval.getReceivers(as400Connect.connection(), ji);
        log.info("all {}", jri);

        log.info(DetailedJournalReceiver.lastJoined(jri).toString());

        // for (DetailedJournalReceiver j : jri) {
        // log.info("receiver {}", j);
        // }
        JournalReceiver jr = journalInfoRetrieval.getReceiver(as400Connect.connection(), ji);
        log.info("Journal info {}", jr);

        log.info("current position: {}", journalInfoRetrieval.getCurrentPosition(as400Connect.connection(), ji));
        log.info("size {}", jri.size());
    }
}
