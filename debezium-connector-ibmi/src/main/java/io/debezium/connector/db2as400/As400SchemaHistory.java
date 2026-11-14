/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import io.debezium.connector.db2as400.As400SchemaState.Cursor;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;

/** Append-only observed schema changes in the connector's existing history store. */
final class As400SchemaHistory {
    static final String VERSION = "7.schema";

    interface Store {
        void read(Consumer<Map<String, String>> records) throws Exception;

        void append(Map<String, String> record) throws Exception;
    }

    record Record(String fingerprint, Cursor previous, Cursor cursor, As400SchemaChange change) {
        Record {
            if (fingerprint == null || !fingerprint.matches("v2:[0-9a-f]{64}") || cursor.ordinal() != Math.addExact(previous.ordinal(), 1)) {
                throw new IllegalArgumentException("Invalid schema-history chain");
            }
        }

        static Record create(String fingerprint, Cursor previous, As400SchemaChange change) throws Exception {
            var fields = unsigned(fingerprint, previous, change);
            return new Record(fingerprint, previous,
                    new Cursor(previous.ordinal() + 1, As400CaptureFingerprint.hash("ibmi-schema-history-v1", fields)), change);
        }

        Map<String, String> fields() throws Exception {
            var fields = unsigned(fingerprint, previous, change);
            fields.put("digest", cursor.hash());
            return Map.copyOf(fields);
        }

        private static TreeMap<String, String> unsigned(String fingerprint, Cursor previous, As400SchemaChange change) throws Exception {
            var fields = new TreeMap<String, String>();
            fields.put("version", VERSION);
            fields.put("fingerprint", fingerprint);
            fields.put("ordinal", Long.toString(Math.addExact(previous.ordinal(), 1)));
            fields.put("previous", previous.hash());
            change.fields().forEach((key, value) -> fields.put("change." + key, value));
            return fields;
        }

        static Record restore(Map<String, String> fields) throws Exception {
            var changes = new TreeMap<String, String>();
            fields.forEach((key, value) -> {
                if (key.startsWith("change.")) {
                    changes.put(key.substring(7), value);
                }
            });
            long ordinal = Long.parseLong(fields.get("ordinal"));
            var previous = new Cursor(Math.subtractExact(ordinal, 1), fields.get("previous"));
            var restored = create(fields.get("fingerprint"), previous, As400SchemaChange.restore(changes));
            require(restored.fields().equals(fields), "Changed or noncanonical schema-history record");
            return restored;
        }
    }

    private final String fingerprint;
    private final Store store;
    private final List<As400BindingDefinition> baseline;

    As400SchemaHistory(String fingerprint, List<As400BindingDefinition> baseline, Store store) {
        this.fingerprint = fingerprint;
        this.baseline = List.copyOf(baseline);
        this.store = store;
    }

    /** Recover only the definitions at F; independently require every schema dependency through W. */
    List<As400BindingDefinition> recover(As400SchemaState required) throws Exception {
        var tables = new TreeMap<String, As400BindingDefinition>();
        baseline.forEach(table -> tables.put(key(table), table));
        var floor = new ArrayList<As400BindingDefinition>();
        if (required.floor().equals(As400SchemaState.ORIGIN)) {
            floor.addAll(tables.values());
        }
        Cursor[] previous = { As400SchemaState.ORIGIN };
        boolean[] completed = { required.completed().equals(As400SchemaState.ORIGIN) };
        boolean[] floorFound = { required.floor().equals(As400SchemaState.ORIGIN) };
        read(record -> {
            if (record.cursor().ordinal() < previous[0].ordinal()) {
                return; // A delayed duplicate append cannot move the selected chain backwards.
            }
            if (record.cursor().ordinal() == previous[0].ordinal()) {
                require(record.cursor().equals(previous[0]), "Conflicting schema-history append");
                return;
            }
            require(record.previous().equals(previous[0]), "Missing or reordered schema history");
            var change = record.change();
            String key = key(change.before() == null ? change.after() : change.before());
            require(java.util.Objects.equals(change.before(), tables.get(key)), "Schema history lost its previous table definition");
            String next = change.after() == null ? key : key(change.after());
            require(next.equals(key) || !tables.containsKey(next), "Schema history rename collides with another retained file");
            if (!next.equals(key)) {
                tables.remove(key);
            }
            if (change.after() == null) {
                tables.remove(key);
            }
            else {
                tables.put(next, change.after());
            }
            previous[0] = record.cursor();
            if (previous[0].ordinal() == required.floor().ordinal()) {
                require(previous[0].equals(required.floor()), "Replay-floor schema history changed");
                floor.addAll(tables.values());
                floorFound[0] = true;
            }
            if (previous[0].ordinal() == required.completed().ordinal()) {
                require(previous[0].equals(required.completed()), "Completed-output schema history changed");
                completed[0] = true;
            }
        });
        require(floorFound[0] && completed[0], "Required schema history is missing");
        return List.copyOf(floor);
    }

    Record find(JournalPosition start) throws Exception {
        Record[] found = { null };
        read(record -> {
            if (record.change().start().equals(start)) {
                require(found[0] == null || found[0].equals(record), "Conflicting definitions for one observed schema change");
                found[0] = record;
            }
        });
        return found[0];
    }

    void verifyFloor(As400Checkpoint checkpoint, As400JournalOrder order) throws Exception {
        Record[] following = { null };
        read(record -> {
            if (record.previous().equals(checkpoint.schemaState().floor())) {
                require(following[0] == null || following[0].equals(record), "Conflicting schema history after the saved floor");
                following[0] = record;
            }
        });
        if (following[0] != null) {
            int compared = order.compare(following[0].change().visibility(), checkpoint.replayFrom());
            require(compared > 0 || compared == 0 && checkpoint.replayInclusive(), "Replay floor omits a required schema dependency");
        }
    }

    /** Repair an over-advanced schema cursor only from the unchanged, fully retained F/W interval. */
    As400SchemaState reconcileFloor(As400Checkpoint checkpoint, As400JournalOrder order) throws Exception {
        Cursor[] floor = { checkpoint.schemaState().floor() };
        read(record -> {
            if (record.cursor().ordinal() > checkpoint.schemaState().floor().ordinal()) {
                return;
            }
            var change = record.change();
            var mark = change.records().stream().filter(item -> item.position().equals(change.visibility())).findFirst().orElseThrow();
            if (order.covers(mark.position(), mark.receiverIdentity())) {
                int compared = order.compare(mark.position(), checkpoint.replayFrom());
                if (compared > 0 || compared == 0 && checkpoint.replayInclusive()) {
                    var opening = change.records().get(0);
                    require(order.covers(opening.position(), opening.receiverIdentity())
                            && order.compare(opening.position(), checkpoint.replayFrom()) >= (checkpoint.replayInclusive() ? 0 : 1),
                            "Schema cursor repair requires the complete original transition at F");
                    if (record.previous().ordinal() < floor[0].ordinal()) {
                        floor[0] = record.previous();
                    }
                }
            }
        });
        var corrected = new As400SchemaState(floor[0], checkpoint.schemaState().completed());
        verifyFloor(checkpoint.withSchemaState(corrected), order);
        return corrected;
    }

    Record append(Cursor previous, As400SchemaChange change) throws Exception {
        var proposed = Record.create(fingerprint, previous, change);
        var retained = find(change.start());
        if (retained == null) {
            store.append(proposed.fields());
            retained = find(change.start());
        }
        require(proposed.equals(retained), "Schema history did not durably retain the exact transition");
        return retained;
    }

    private void read(Consumer<Record> visitor) throws Exception {
        store.read(fields -> {
            try {
                var record = Record.restore(fields);
                if (record.fingerprint().equals(fingerprint)) {
                    visitor.accept(record);
                }
            }
            catch (Exception failure) {
                throw new IllegalArgumentException("Cannot recover schema history", failure);
            }
        });
    }

    static String key(As400BindingDefinition table) {
        return table.identity().tableId().schema() + "/" + table.identity().physicalFile();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
