/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

class RetrieveJournalTest {
    private JournalProcessedPosition firstPosition() {
        JournalProcessedPosition positionMatches = new JournalProcessedPosition(BigInteger.ONE, new JournalReceiver("receiver", "receiverLibrary"),
                Instant.ofEpochMilli(1l), true);
        return positionMatches;
    }

    @Test
    public void testAlreadyProcessedOffsetSequenceReceiverAndReceiverLibrary() {
        JournalProcessedPosition positionMatches = firstPosition();
        EntryHeader entryHeader = new EntryHeader(-1, -1, -1l, positionMatches.getOffset(), BigInteger.TWO,
                Instant.ofEpochMilli(2l), 'Z', "UB", "OBJECT", BigInteger.TEN, -1, -1,
                positionMatches.getReceiver().name(), positionMatches.getReceiver().library(), BigInteger.TWO);
        assertTrue(RetrieveJournal.alreadyProcessed(positionMatches, entryHeader), "compare sequence number, library and receiver match");

        JournalProcessedPosition positionIncorrectReceiver = new JournalProcessedPosition(BigInteger.ONE, new JournalReceiver("receiverDoesn'tMatch", "receiverLibrary"),
                Instant.now(), true);
        assertFalse(RetrieveJournal.alreadyProcessed(positionIncorrectReceiver, entryHeader), "compare where only receiver name doesn't match");

        JournalProcessedPosition positionIncorrectReceiverLibrary = new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("receiver", "receiverLibraryDoesn'tMatch"), Instant.now(), true);
        assertFalse(RetrieveJournal.alreadyProcessed(positionIncorrectReceiverLibrary, entryHeader), "compare where only receiver name doesn't match");

        JournalProcessedPosition positionIncorrectSequence = new JournalProcessedPosition(BigInteger.TWO, new JournalReceiver("receiver", "receiverLibrary"),
                Instant.now(), true);
        assertFalse(RetrieveJournal.alreadyProcessed(positionIncorrectSequence, entryHeader), "compare where only receiver name doesn't match");
    }

    @Test
    public void testAlreadyProcessedOffsetSequenceMissingReceiverAndReceiverLibrary() {
        JournalProcessedPosition positionMatches = firstPosition();
        EntryHeader entryHeader = new EntryHeader(-1, -1, -1l, positionMatches.getOffset(), BigInteger.TWO,
                Instant.ofEpochMilli(2l), 'Z', "UB", "OBJECT", BigInteger.TEN, -1, -1,
                positionMatches.getReceiver().name(), positionMatches.getReceiver().library(), BigInteger.TWO);
        assertTrue(RetrieveJournal.alreadyProcessed(positionMatches, entryHeader), "compare only sequence number, library and receiver empty");

        EntryHeader entryHeaderDifferentOffset = new EntryHeader(-1, -1, -1l, positionMatches.getOffset().add(BigInteger.ONE), BigInteger.TWO,
                Instant.ofEpochMilli(2l), 'Z', "UB", "OBJECT", BigInteger.TEN, -1, -1,
                "", "", BigInteger.TWO);
        assertFalse(RetrieveJournal.alreadyProcessed(positionMatches, entryHeaderDifferentOffset), "compare only sequence number, library and receiver empty");

    }

    @Test
    public void testAlreadyProcessedOffsetNotProcessed() {
        JournalProcessedPosition positionMatches = firstPosition();
        JournalProcessedPosition positionMatchesNotProcessed = firstPosition().setProcessed(false);

        EntryHeader entryHeader = new EntryHeader(-1, -1, -1l, positionMatchesNotProcessed.getOffset(), BigInteger.TWO,
                Instant.ofEpochMilli(2l), 'Z', "UB", "OBJECT", BigInteger.TEN, -1, -1,
                positionMatchesNotProcessed.getReceiver().name(), positionMatchesNotProcessed.getReceiver().library(), BigInteger.TWO);

        assertTrue(RetrieveJournal.alreadyProcessed(positionMatches, entryHeader), "compare sequence number, library and receiver match");
        assertFalse(RetrieveJournal.alreadyProcessed(positionMatchesNotProcessed, entryHeader), "compare sequence number, library and receiver match");

        EntryHeader entryHeaderEmptyReceiver = new EntryHeader(-1, -1, -1l, positionMatchesNotProcessed.getOffset(), BigInteger.TWO,
                Instant.ofEpochMilli(2l), 'Z', "UB", "OBJECT", BigInteger.TEN, -1, -1,
                "", "", BigInteger.TWO);

        assertTrue(RetrieveJournal.alreadyProcessed(positionMatches, entryHeaderEmptyReceiver), "compare sequence number, library and receiver match");
        assertFalse(RetrieveJournal.alreadyProcessed(positionMatchesNotProcessed, entryHeaderEmptyReceiver), "compare only sequence number, library and receiver empty");
    }
}
