/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.debezium.connector.db2as400.As400TaskInitialization.Row;

/** Bounded native snapshot scanner; cursor and probes serialize, Kafka waits do not hold the native lock. */
final class As400NativeSnapshotScanner {
    interface Check {
        void run() throws Exception;
    }

    private final Connection connection;
    private final Object io;
    private final Check held;
    private final Runnable local;
    private final Consumer<Statement> active;
    private final As400SnapshotChunks.Limits limits;
    private long totalRows;
    private long totalBytes;

    As400NativeSnapshotScanner(Connection connection, Object io, Check held, Runnable local, Consumer<Statement> active, As400SnapshotChunks.Limits limits) {
        this.connection = connection;
        this.io = io;
        this.held = held;
        this.local = local;
        this.active = active;
        this.limits = limits;
    }

    void scan(List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
        for (var definition : definitions) {
            var temporalDecoders = definition.codecs().stream().map(c -> c.temporal()
                    ? (io.debezium.ibmi.db2.journal.data.types.AS400Temporal) c.decoder()
                    : null).toList();
            String projection = java.util.stream.IntStream.range(0, definition.codecs().size()).mapToObj(i -> {
                String name = quote(definition.schema().columns().get(i).name());
                return switch (definition.codecs().get(i).kind()) {
                    case "DATE_ISO", "TIME_ISO" -> "CHAR(" + name + ", ISO)";
                    case "TIMESTAMP_ISO" -> "CHAR(" + name + ")";
                    default -> name;
                };
            }).collect(Collectors.joining(","));
            String keys = definition.schema().primaryKeyColumnNames().stream().map(As400NativeSnapshotScanner::quote).collect(Collectors.joining(","));
            if (definition.usesRrn()) {
                projection += ",CAST(RRN(T) AS BIGINT)";
            }
            if (definition.usesRrn() || keys.isEmpty()) {
                // Physical order is stable under the qualified snapshot lock.
                // It does not make the record number part of the public key.
                keys = "RRN(T)";
            }
            long previousRrn = 0;
            Statement statement = null;
            ResultSet rows = null;
            try {
                synchronized (io) {
                    held.run();
                    statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                    active.accept(statement);
                    local.run();
                    statement.setQueryTimeout(5);
                    statement.setFetchSize(limits.rows());
                    // A configured max-row cutoff must never masquerade as EOF.
                    statement.setMaxRows(0);
                    rows = statement.executeQuery("SELECT " + projection + " FROM " + quote(definition.schema().id().schema()) + "."
                            + quote(definition.schema().id().table()) + " T ORDER BY " + keys);
                }
                boolean eof = false;
                Row lookahead = null;
                while (!eof || lookahead != null) {
                    List<Row> chunk = new ArrayList<>();
                    long bytes = 0;
                    synchronized (io) {
                        held.run();
                        while (chunk.size() < limits.rows()) {
                            local.run();
                            Row row;
                            if (lookahead != null) {
                                row = lookahead;
                                lookahead = null;
                            }
                            else {
                                if (!rows.next()) {
                                    eof = true;
                                    break;
                                }
                                local.run();
                                Object[] values = new Object[definition.codecs().size()];
                                for (int c = 0; c < values.length; c++) {
                                    var temporal = temporalDecoders.get(c);
                                    if (temporal != null) {
                                        String value = rows.getString(c + 1);
                                        values[c] = value == null ? null : temporal.parse(value);
                                    }
                                    else {
                                        values[c] = rows.getObject(c + 1);
                                    }
                                }
                                if (definition.usesRrn()) {
                                    long rrn = rows.getLong(values.length + 1);
                                    if (rows.wasNull() || rrn <= previousRrn) {
                                        throw new IllegalStateException("Missing, duplicate or reordered snapshot relative record number");
                                    }
                                    previousRrn = rrn;
                                    values = As400BindingDefinition.withRrn(values, BigInteger.valueOf(rrn));
                                }
                                var owned = As400RowData.copyOf(values, 65536);
                                row = new Row(definition.schema().id(), owned, Instant.now());
                                totalRows = Math.incrementExact(totalRows);
                                totalBytes = Math.addExact(totalBytes, owned.payloadBytes());
                                if (totalRows > limits.totalRows() || totalBytes > limits.totalBytes()) {
                                    throw new IllegalStateException("Native snapshot exceeded its total budget");
                                }
                            }
                            long size = row.values().payloadBytes();
                            if (size > limits.bytes()) {
                                throw new IllegalStateException("Native row does not fit snapshot chunk");
                            }
                            if (size > limits.bytes() - bytes) {
                                lookahead = row;
                                break;
                            }
                            chunk.add(row);
                            bytes += size;
                        }
                        warnings(rows, statement);
                        held.run();
                    }
                    if (!chunk.isEmpty()) {
                        // No native mutex while the actual task waits for Kafka.
                        consumer.chunk(definition.schema().id(), List.copyOf(chunk));
                    }
                }
                synchronized (io) {
                    warnings(rows, statement);
                }
            }
            finally {
                synchronized (io) {
                    try {
                        if (rows != null) {
                            rows.close();
                        }
                    }
                    finally {
                        try {
                            if (statement != null) {
                                statement.close();
                            }
                        }
                        finally {
                            active.accept(null);
                        }
                    }
                }
            }
            synchronized (io) {
                held.run();
            }
            consumer.complete(definition.schema().id());
        }
    }

    private static void warnings(ResultSet rows, Statement statement) throws Exception {
        if (rows.getWarnings() != null || statement.getWarnings() != null) {
            throw new IllegalStateException("Native snapshot warning");
        }
    }

    private static String quote(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
}
