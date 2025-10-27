package io.debezium.ibmi.db2.journal.retrieve.exception;

public class RetrieveJournalException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public RetrieveJournalException(String message) {
        super(message);
    }
}