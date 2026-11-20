/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;
import java.util.Set;

/** Schema-history dependencies at F and W; these never represent the live scan cursor. */
record As400SchemaState(As400SchemaState.Cursor floor, As400SchemaState.Cursor completed) {
    static final Cursor ORIGIN = new Cursor(0, "origin");
    static final As400SchemaState EMPTY = new As400SchemaState(ORIGIN, ORIGIN);
    static final Set<String> KEYS = Set.of("schema.floor.ordinal", "schema.floor.hash", "schema.completed.ordinal", "schema.completed.hash");

    record Cursor(long ordinal, String hash) {
        Cursor {
            if (ordinal < 0 || hash == null || (ordinal == 0 ? !hash.equals("origin") : !hash.matches("[0-9a-f]{64}"))) {
                throw new IllegalArgumentException("Invalid schema-history cursor");
            }
        }
    }

    As400SchemaState {
        if (floor == null || completed == null || floor.ordinal() > completed.ordinal()
                || floor.ordinal() == completed.ordinal() && !floor.equals(completed)) {
            throw new IllegalArgumentException("Conflicting schema-history dependencies");
        }
    }

    Map<String, String> fields() {
        return completed.equals(ORIGIN) ? Map.of()
                : Map.of("schema.floor.ordinal", Long.toString(floor.ordinal()), "schema.floor.hash", floor.hash(),
                        "schema.completed.ordinal", Long.toString(completed.ordinal()), "schema.completed.hash", completed.hash());
    }

    static As400SchemaState from(Map<String, ?> offset) {
        if (KEYS.stream().noneMatch(offset::containsKey)) {
            return EMPTY;
        }
        var state = new As400SchemaState(cursor(offset, "floor"), cursor(offset, "completed"));
        if (state.equals(EMPTY)) {
            throw new IllegalArgumentException("Empty schema dependencies must use the original checkpoint representation");
        }
        return state;
    }

    private static Cursor cursor(Map<String, ?> offset, String name) {
        Object number = offset.get("schema." + name + ".ordinal"), hash = offset.get("schema." + name + ".hash");
        if (!(number instanceof String value) || !value.matches("0|[1-9][0-9]{0,18}") || !(hash instanceof String digest)) {
            throw new IllegalArgumentException("Incomplete schema-history dependency");
        }
        return new Cursor(Long.parseLong(value), digest);
    }
}
