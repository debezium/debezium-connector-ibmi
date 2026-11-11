/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

/** Reuses the qualified native adapters; adds terminal admission for this test reader. */
final class As400ContinuousConnectSource extends As400NativeInitializationProbe {
    final JournalReadLifecycle lifecycle = new JournalReadLifecycle();
    final RetrieveJournal guardedReader;

    As400ContinuousConnectSource(Properties credentials, String library, Path evidence, String bootstrap, String topic) throws Exception {
        super(credentials, library, evidence, bootstrap, topic);
        if (!library.matches("DCNC[0-9]{4}")) {
            throw new IllegalArgumentException("Explicit owned native Connect fixture required");
        }
        guardedReader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> transport)
                .withJournalInfo(new JournalInfo("QSQJRN", library, false)).withTransactionControls(true).withServerFiltering(false)
                .withJournalBufferSize(1024 * 1024).withMaxServerSideEntries(1000).build(), new JournalInfoRetrieval(0, 0, 0), lifecycle) {
            private int pageNumber;

            @Override
            public io.debezium.ibmi.db2.journal.retrieve.RawJournalPage readRawPage(io.debezium.ibmi.db2.journal.retrieve.JournalPosition from,
                                                                                    io.debezium.ibmi.db2.journal.retrieve.JournalPosition through,
                                                                                    int maximumEntries)
                    throws Exception {
                var page = super.readRawPage(from, through, maximumEntries);
                save(String.format("page-%04d.json", ++pageNumber), java.util.Map.of("from", from.offset().toString(),
                        "through", through.offset().toString(), "receiver", from.receiver().name(), "entries", page.entries().stream()
                                .map(e -> java.util.Map.of("sequence", e.position().offset().toString(), "hex", java.util.HexFormat.of().formatHex(e.data()))).toList()));
                return page;
            }
        };
    }

    @Override
    Connection connection(boolean transaction) throws SQLException {
        JournalReadLifecycle.Lease lease;
        try {
            lease = lifecycle.enter();
        }
        catch (java.io.IOException error) {
            throw new SQLException("Native JDBC source stopped", error);
        }
        try {
            Connection connection = super.connection(transaction);
            if (lifecycle.isStopped()) {
                connection.close();
                throw new SQLException("Native JDBC source stopped during connection");
            }
            return (Connection) guard(connection, lifecycle, lease);
        }
        catch (Exception error) {
            lease.close();
            throw error;
        }
    }

    static Object guard(Object delegate, JournalReadLifecycle lifecycle, JournalReadLifecycle.Lease connectionLease) {
        return As400DcgNativeIo.guard(delegate, lifecycle, connectionLease);
    }

    JournalReadLifecycle lifecycle() {
        return lifecycle;
    }

    RetrieveJournal guardedReader() {
        return guardedReader;
    }

    As400JdbcNativeIdentityEvidence identities() {
        return identities;
    }

    @Override
    public void close() {
        lifecycle.requestStop();
        guardedReader.cancelJob();
        lifecycle.awaitDrained();
        super.close();
    }
}
