/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.exception;

/**
 * Action needed to restream or we have lost data
 */
public class LostJournalException extends Exception {

    public LostJournalException(String message) {
        super(message);
    }

    public LostJournalException(Throwable cause) {
        super(cause);
    }

    public LostJournalException(String message, Throwable cause) {
        super(message, cause);
    }

    public LostJournalException(String message, Throwable cause, boolean enableSuppression,
                                boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
