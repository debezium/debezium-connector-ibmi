/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;
import java.util.TreeMap;

/** An interrupted requested snapshot retains its original authoritative F/W. */
final class As400FullSnapshotState {
    private static final String PENDING = "full_snapshot.pending";

    private As400FullSnapshotState() {
    }

    static boolean pending(Map<String, ?> saved) {
        if (saved == null || !saved.containsKey(PENDING)) {
            return false;
        }
        base(saved); // Reject malformed pending state before it can request work.
        return true;
    }

    static Map<String, ?> base(Map<String, ?> saved) {
        if (saved == null || !saved.containsKey(PENDING)) {
            return saved;
        }
        if (!"1".equals(saved.get(PENDING))) {
            throw new IllegalArgumentException("Unsupported full-snapshot restart state");
        }
        var fields = new TreeMap<String, Object>(saved);
        fields.remove(PENDING);
        return As400Checkpoint.fromOffset(fields, (String) fields.get("journal_identity"), (String) fields.get("capture_fingerprint")).toOffset();
    }

    static Map<String, String> pending(As400Checkpoint original) {
        var fields = new TreeMap<>(original.toOffset());
        fields.put(PENDING, "1");
        return Map.copyOf(fields);
    }
}
