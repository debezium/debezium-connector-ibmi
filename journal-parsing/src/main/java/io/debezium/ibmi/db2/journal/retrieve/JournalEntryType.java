/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.util.HashMap;
import java.util.Map;

public enum JournalEntryType {

    ADD_ROW1("R.PX"),
    ADD_ROW2("R.PT"),
    AFTER_IMAGE("R.UP"),
    ROLLBACK_AFTER_IMAGE("R.UR"),
    BEFORE_IMAGE("R.UB"),
    ROLLBACK_BEFORE_IMAGE("R.BR"),
    DELETE_ROW("R.DL"),
    ROLLBACK_DELETE_ROW("R.DR"),
    FILE_CREATED("D.CT"),
    FILE_CHANGE("D.CG"),
    START_COMMIT("C.SC"),
    END_COMMIT("C.CM"),
    OPEN("F.OP"),
    CLOSE("F.CL");

    public final String code;

    JournalEntryType(String code) {
        this.code = code;
    }

    private static final Map<String, JournalEntryType> BY_LABEL = new HashMap<>();

    static {
        for (final JournalEntryType e : values()) {
            BY_LABEL.put(e.code, e);
        }
    }

    public static JournalEntryType toValue(String code) {
        if (code == null) {
            return null;
        }
        return BY_LABEL.get(code);
    }
}
