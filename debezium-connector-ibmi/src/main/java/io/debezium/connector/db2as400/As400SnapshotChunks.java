/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import io.debezium.connector.db2as400.As400TaskInitialization.Row;
import io.debezium.relational.TableId;

/** One synchronous pending chunk at a time. No F/W, resumable cursor or retained row history. */
final class As400SnapshotChunks {
    record Limits(int rows, long bytes, long totalRows, long totalBytes) {
        Limits {
            if (rows < 1 || rows > 256 || bytes < 1 || bytes > 1_000_000 || totalRows < 1 || totalBytes < 1) {
                throw new IllegalArgumentException("Unsupported snapshot chunk/attempt limits");
            }
        }
    }

    interface Consumer {
        void chunk(TableId table, List<Row> rows) throws Exception;

        void complete(TableId table) throws Exception;
    }

    interface Delivery {
        String deliver(List<Row> rows, Map<String, String> dataOffset, Map<String, String> barrierOffset) throws Exception;
    }

    private final String epoch;
    private final Limits limits;
    private final List<TableId> tables;
    private final Delivery delivery;
    private final Map<String, String> completions = new TreeMap<>();
    private String digest;
    private int tableIndex;
    private long tableRows;
    private long tableBytes;
    private long totalRows;
    private long totalBytes;
    private long ordinal;
    private volatile boolean failed;
    private boolean finished;
    private boolean delivering;
    private final Thread owner = Thread.currentThread();

    As400SnapshotChunks(String epoch, Limits limits, List<As400BindingDefinition> definitions, Delivery delivery) throws Exception {
        new As400BindingHistory.Initialization(epoch, "pending");
        this.epoch = epoch;
        this.limits = limits;
        this.delivery = delivery;
        tables = definitions.stream().map(d -> d.schema().id()).toList();
        if (tables.isEmpty() || tables.size() > 100 || tables.stream().distinct().count() != tables.size()) {
            throw new IllegalArgumentException("Bounded unique snapshot table set required");
        }
        Map<String, String> bindings = new TreeMap<>();
        for (int i = 0; i < definitions.size(); i++) {
            var changes = new io.debezium.relational.history.TableChanges();
            changes.create(definitions.get(i).storedTable());
            var document = io.debezium.document.Document.create();
            document.setArray("tables", new io.debezium.relational.history.JsonTableChangeSerializer().serialize(changes));
            bindings.put(Integer.toString(i), io.debezium.document.DocumentWriter.defaultWriter().write(document));
        }
        digest = As400CaptureFingerprint.hash("snapshot-chunks-v1-bindings", bindings);
    }

    Consumer consumer() {
        return new Consumer() {
            @Override
            public void chunk(TableId table, List<Row> rows) throws Exception {
                requireTable(table);
                try {
                    if (rows.isEmpty() || rows.size() > limits.rows()) {
                        throw new IllegalArgumentException("Snapshot chunk row bound exceeded");
                    }
                    List<Row> owned = List.copyOf(rows);
                    long bytes = 0;
                    for (var row : owned) {
                        if (!row.table().equals(table) || row.values().payloadBytes() > 65536) {
                            throw new IllegalArgumentException("Snapshot chunk row/table differs");
                        }
                        bytes = Math.addExact(bytes, row.values().payloadBytes());
                    }
                    long nextRows = Math.addExact(totalRows, owned.size());
                    long nextBytes = Math.addExact(totalBytes, bytes);
                    if (bytes > limits.bytes() || nextRows > limits.totalRows() || nextBytes > limits.totalBytes()) {
                        throw new IllegalArgumentException("Snapshot chunk or total budget exceeded");
                    }
                    long next = Math.incrementExact(ordinal);
                    var fields = new TreeMap<>(Map.of("previous", digest, "epoch", epoch, "ordinal", Long.toString(next),
                            "table.index", Integer.toString(tableIndex), "rows", Integer.toString(owned.size()), "bytes", Long.toString(bytes)));
                    String identity = As400CaptureFingerprint.hash("snapshot-chunks-v1-chunk", fields);
                    delivering = true;
                    String receipt = delivery.deliver(owned, pending("chunk-data", next, identity), pending("chunk-complete", next, identity));
                    new As400BindingHistory.Initialization(epoch, receipt);
                    fields.put("receipt", receipt);
                    digest = As400CaptureFingerprint.hash("snapshot-chunks-v1-receipt", fields);
                    totalRows = nextRows;
                    totalBytes = nextBytes;
                    tableRows = Math.addExact(tableRows, owned.size());
                    tableBytes = Math.addExact(tableBytes, bytes);
                    ordinal = next;
                }
                catch (Exception error) {
                    failed = true;
                    throw error;
                }
                finally {
                    delivering = false;
                }
            }

            @Override
            public void complete(TableId table) {
                requireTable(table);
                completions.put("table." + tableIndex + ".rows", Long.toString(tableRows));
                completions.put("table." + tableIndex + ".bytes", Long.toString(tableBytes));
                completions.put("table." + tableIndex + ".eof", "true");
                tableIndex++;
                tableRows = 0;
                tableBytes = 0;
            }
        };
    }

    private void requireTable(TableId table) {
        if (Thread.currentThread() != owner || failed || finished || delivering || tableIndex == tables.size() || !tables.get(tableIndex).equals(table)) {
            failed = true;
            throw new IllegalStateException("Unexpected, concurrent or incomplete snapshot table sequence");
        }
    }

    void finish() {
        if (Thread.currentThread() != owner || failed || delivering || finished || tableIndex != tables.size()) {
            failed = true;
            throw new IllegalStateException("Snapshot requires complete EOF/closed-table evidence");
        }
        completions.put("receipts", digest);
        completions.put("chunks", Long.toString(ordinal));
        completions.put("rows", Long.toString(totalRows));
        completions.put("bytes", Long.toString(totalBytes));
        digest = As400CaptureFingerprint.hash("snapshot-chunks-v1-complete", completions);
        finished = true;
    }

    Map<String, String> baseline(As400NativeInitialization.Boundary boundary) {
        if (!finished || failed) {
            throw new IllegalStateException("Snapshot completion missing");
        }
        var fields = new HashMap<>(pending("baseline-complete", ordinal, digest));
        fields.put("initialization.rows", Long.toString(totalRows));
        fields.put("initialization.bytes", Long.toString(totalBytes));
        fields.put("initialization.journal", boundary.journalIdentity());
        fields.put("initialization.receiver.identity", boundary.receiverIdentity());
        fields.put("initialization.receiver", boundary.position().receiver().name());
        fields.put("initialization.receiver.library", boundary.position().receiver().library());
        fields.put("initialization.sequence", boundary.position().offset().toString());
        return Map.copyOf(fields);
    }

    private Map<String, String> pending(String phase, long chunk, String identity) {
        return Map.of("initialization.version", "chunks-1", "initialization.epoch", epoch, "initialization.phase", phase,
                "initialization.chunk", Long.toString(chunk), "initialization.summary", identity);
    }
}
