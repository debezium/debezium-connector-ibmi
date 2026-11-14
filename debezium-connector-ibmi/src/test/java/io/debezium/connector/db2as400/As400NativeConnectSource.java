/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Path;
import java.util.Properties;

import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

/** Reuses the qualified native adapters; adds terminal admission for this test reader. */
final class As400NativeConnectSource extends As400NativeInitializationProbe {
    final JournalReadLifecycle lifecycle = new JournalReadLifecycle();
    final RetrieveJournal guardedReader;

    As400NativeConnectSource(Properties credentials, String library, Path evidence, String bootstrap, String topic) throws Exception {
        super(credentials, library, evidence, bootstrap, topic);
        if (!library.matches("DCNC[0-9]{4}")) {
            throw new IllegalArgumentException("Explicit owned native Connect fixture required");
        }
        guardedReader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> transport)
                .withJournalInfo(new JournalInfo("QSQJRN", library, false)).withTransactionControls(true).withServerFiltering(false)
                .withJournalBufferSize(1024 * 1024).withMaxServerSideEntries(1000).build(), new JournalInfoRetrieval(0, 0, 0), lifecycle);
    }

    @Override
    public void close() {
        lifecycle.requestStop();
        guardedReader.cancelJob();
        lifecycle.awaitDrained();
        super.close();
    }
}
