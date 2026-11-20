/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/** Finite controller-owned transactions; stdin credentials and isolated fixture SQL. */
public class As400NativeConnectTransactions {
    private static final List<String> MODES = List.of("truncate-commit", "delete-rollback", "truncate-rollback", "delete-commit", "temporal-commit", "temporal-rollback");

    public static void main(String[] args) throws Exception {
        if ((args.length < 3 || args.length > 5) || !args[0].matches("DCNC[0-9]{4}")
                || !MODES.contains(args[2])) {
            throw new IllegalArgumentException("Owned fixture, evidence directory and bounded transaction case required");
        }
        Properties credentials = new Properties();
        credentials.load(System.in);
        String library = args[0];
        Path evidence = Path.of(args[1]);
        String mode = args[2];
        int rowCount = args.length >= 4 ? Integer.parseInt(args[3]) : 3;
        int iteration = args.length == 5 ? Integer.parseInt(args[4]) : 0;
        if (rowCount < 3 || rowCount > 5000) {
            throw new IllegalArgumentException("Bounded fixture reload count required");
        }
        if (iteration < 0 || iteration > 24) {
            throw new IllegalArgumentException("Bounded fixture iteration required");
        }
        if (!Files.readString(evidence.resolve("qualified")).equals("native-" + library)) {
            throw new IllegalStateException("Owned controller enrollment required before fixture writes");
        }
        int index = MODES.indexOf(mode);
        boolean temporal = mode.startsWith("temporal");
        try (var source = new As400NativeConnectSource(credentials, library, evidence, "broker:9092", "unused-controller");
                var transaction = source.connection(false)) {
            boolean fastDelete = mode.startsWith("delete");
            try {
                if (fastDelete) {
                    // Dedicated job-local QTEMP override, as in the earlier
                    // native transaction experiment; no shared QAQQINI changes.
                    source.execute(transaction, "CALL QSYS2.OVERRIDE_QAQQINI(1)");
                    source.execute(transaction, "CALL QSYS2.OVERRIDE_QAQQINI(2, 'SQL_FAST_DELETE_ROW_COUNT', '1')");
                    try (var statement = transaction.createStatement()) {
                        statement.setQueryTimeout(5);
                        try (var rows = statement.executeQuery("SELECT QQVAL FROM QTEMP.QAQQINI WHERE QQPARM='SQL_FAST_DELETE_ROW_COUNT'")) {
                            if (!rows.next() || !"1".equals(rows.getString(1).trim()) || rows.next()) {
                                throw new IllegalStateException("Job-local fast-delete threshold was not established");
                            }
                        }
                    }
                    source.save("fast-delete-profile.json", Map.of("scope", "dedicated job QTEMP", "SQL_FAST_DELETE_ROW_COUNT", "1"));
                }
                transaction.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
                transaction.setAutoCommit(false);
                source.execute(transaction, mode.startsWith("truncate") || temporal
                        ? "TRUNCATE TABLE " + library + (temporal ? ".M" : ".B") + " REUSE STORAGE IGNORE DELETE TRIGGERS"
                        : "DELETE FROM " + library + ".B");
                int first = iteration == 0 ? 100 * (index + 1) : iteration * 10000;
                if (temporal) {
                    source.execute(transaction, "INSERT INTO " + library + ".M (ID,DT,TM,TS0,TS3,TS6,TS9) VALUES "
                            + "(201,DATE('2024-02-29'),TIME('23:59:59'),TIMESTAMP('2024-02-29 23:59:59'),TIMESTAMP('2024-02-29 23:59:59.123'),TIMESTAMP('2024-02-29 23:59:59.123456'),CAST('2024-02-29 23:59:59.123456789' AS TIMESTAMP(9))),"
                            + "(202,NULL,NULL,NULL,NULL,NULL,NULL),"
                            + "(203,DATE('0001-01-01'),TIME('24:00:00'),TIMESTAMP('0001-01-01 00:00:00'),TIMESTAMP('0001-01-01 00:00:00.123'),TIMESTAMP('0001-01-01 00:00:00.123456'),CAST('1969-12-31 23:59:59.999999999' AS TIMESTAMP(9)))");
                }
                for (int offset = 0; !temporal && offset < rowCount; offset += 100) {
                    var values = new java.util.ArrayList<String>();
                    for (int n = offset; n < Math.min(rowCount, offset + 100); n++) {
                        values.add("(" + (first + n) + ")");
                    }
                    source.execute(transaction, "INSERT INTO " + library + ".B VALUES " + String.join(",", values));
                }
                int unrelatedId = temporal ? 204 + index : iteration == 0 ? 8 + index : 100000 + iteration;
                try (var unrelated = source.connection(false)) {
                    source.execute(unrelated, "INSERT INTO " + library + ".A VALUES (" + unrelatedId + ")");
                }
                source.save(mode + "-open.json",
                        Map.of("mode", mode, "unrelated_id", unrelatedId, "buffered_ids", temporal ? List.of(201, 202, 203) : List.of(first, first + 1, first + 2),
                                "row_count", temporal ? 3 : rowCount));
                long deadline = System.nanoTime() + Duration.ofMinutes(12).toNanos();
                while (!Files.exists(evidence.resolve(mode + ".finish"))) {
                    if (System.nanoTime() > deadline || Thread.currentThread().isInterrupted() || Files.exists(evidence.resolve(mode + ".abort"))) {
                        throw new IllegalStateException("Native transaction deadline reached; rolling back");
                    }
                    Thread.sleep(100);
                }
                if (mode.endsWith("commit")) {
                    transaction.commit();
                }
                else {
                    transaction.rollback();
                }
                source.save(mode + "-terminal.json", Map.of("mode", mode, "outcome", mode.endsWith("commit") ? "commit" : "rollback"));
            }
            finally {
                try {
                    if (!transaction.getAutoCommit()) {
                        transaction.rollback();
                    }
                }
                finally {
                    if (fastDelete) {
                        source.execute(transaction, "CALL QSYS2.OVERRIDE_QAQQINI(3)");
                        source.save("fast-delete-profile-discarded.json", Map.of("discarded", true));
                    }
                }
            }
        }
    }
}
