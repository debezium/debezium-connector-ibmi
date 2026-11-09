/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Objects;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/** Immutable event coordinates, separate from the future durable replay checkpoint. */
record As400SourceMetadata(JournalPosition position, Instant timestamp, BigInteger systemSequence,
        BigInteger commitCycle, char journalCode, String entryType, EntryHeader.RecordInfo recordInfo,
        EntryHeader.ControlInfo controlInfo) {
    As400SourceMetadata {
        Objects.requireNonNull(position);
        Objects.requireNonNull(timestamp);
        Objects.requireNonNull(systemSequence);
        Objects.requireNonNull(commitCycle);
        Objects.requireNonNull(entryType);
        Objects.requireNonNull(recordInfo);
        Objects.requireNonNull(controlInfo);
    }

    static As400SourceMetadata from(EntryHeader header, JournalPosition resolvedPosition) {
        if (!resolvedPosition.offset().equals(header.getSequenceNumber())) {
            throw new IllegalArgumentException("Resolved position is not the current entry position");
        }
        if ((!header.getReceiver().isEmpty() && !header.getReceiver().equals(resolvedPosition.receiver().name()))
                || (!header.getReceiverLibrary().isEmpty() && !header.getReceiverLibrary().equals(resolvedPosition.receiver().library()))) {
            throw new IllegalArgumentException("Resolved receiver conflicts with entry header");
        }
        return new As400SourceMetadata(resolvedPosition, header.getTimestamp(), header.getSystemSequenceNumber(),
                header.getCommitCycle(), header.getJournalCode(), header.getEntryType(), header.getRecordInfo(), header.getControlInfo());
    }
}
