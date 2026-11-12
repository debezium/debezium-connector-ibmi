/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.relational.TableId;

/** Standard Debezium chunk progress, frozen at the same publication boundary as F/W. */
final class As400IncrementalSnapshotState {
    static final String COLLECTIONS = "incremental_snapshot_collections";
    static final Set<String> KEYS = Set.of(COLLECTIONS, "incremental_snapshot_primary_key", "incremental_snapshot_maximum_key",
            "incremental_snapshot_total_rows", "incremental_snapshot_correlation_id");
    private static final int MAX_CHARACTERS = 4 * 1024 * 1024;

    private As400IncrementalSnapshotState() {
    }

    static Map<String, String> capture(IncrementalSnapshotContext<?> context) {
        return fields(context.store(new HashMap<>()));
    }

    static Map<String, String> fields(Map<String, ?> values) {
        var state = new HashMap<String, String>();
        long characters = 0;
        for (var entry : values.entrySet()) {
            if (entry.getKey() == null) {
                throw new IllegalArgumentException("Missing offset field name");
            }
            if (!entry.getKey().startsWith("incremental_snapshot")) {
                continue;
            }
            if (!KEYS.contains(entry.getKey())) {
                throw new IllegalArgumentException("Unsupported incremental snapshot offset field " + entry.getKey());
            }
            if (entry.getValue() == null && entry.getKey().equals("incremental_snapshot_correlation_id")) {
                continue; // Core represents an absent optional correlation id with null.
            }
            if (!(entry.getValue() instanceof String value)) {
                throw new IllegalArgumentException("Malformed incremental snapshot offset field " + entry.getKey());
            }
            characters += value.length();
            if (characters > MAX_CHARACTERS) {
                throw new IllegalArgumentException("Incremental snapshot offset exceeds the bounded recovery record");
            }
            state.put(entry.getKey(), value);
        }
        if (!state.isEmpty() && (!state.containsKey(COLLECTIONS) || !state.containsKey("incremental_snapshot_primary_key")
                || !state.containsKey("incremental_snapshot_maximum_key"))) {
            throw new IllegalArgumentException("Incomplete incremental snapshot checkpoint");
        }
        return Map.copyOf(state);
    }

    static IncrementalSnapshotContext<TableId> restore(Map<String, ?> values) {
        return SignalBasedIncrementalSnapshotContext.load(fields(values), true);
    }
}
