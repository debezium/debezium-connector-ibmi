/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class JournalEntryTypeTest {

    @Test
    public void checkNoDuplicateEnums() {
        Map<String, JournalEntryType> found = new HashMap<>();
        for (final JournalEntryType e : JournalEntryType.values()) {
            assertFalse(found.containsKey(e.code), "map must not contain duplicate values found enums " + e + ", " + found.get(e.code));
            found.put(e.code, e);
        }
    }

}
