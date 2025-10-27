/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.exception;

public class RetrieveJournalException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RetrieveJournalException(String message) {
        super(message);
    }
}