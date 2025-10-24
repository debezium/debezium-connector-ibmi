/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

public record JournalInfo(String journalName, String journalLibrary, boolean isCaching) {

    public JournalInfo(String journalName, String journalLibrary, boolean isCaching) {
        if (journalName == null || journalName.trim().length() == 0 || journalName.trim().length() > 10) {
            throw new IllegalArgumentException("receiver name must not be null and length must be <= to 10.");
        }
        if (journalLibrary == null || journalLibrary.trim().length() == 0 || journalLibrary.trim().length() > 10) {
            throw new IllegalArgumentException("receiverLibrary name must not be null and length must be <= to 10.");
        }
        this.journalName = journalName.trim();
        this.journalLibrary = journalLibrary.trim();
        this.isCaching = isCaching;
    }

    @Override
    public String toString() {
        return String.format("JournalInfo [journalName=%s, journalLibrary=%s, isCaching=%s]", journalName,
                journalLibrary, isCaching);
    }
}
