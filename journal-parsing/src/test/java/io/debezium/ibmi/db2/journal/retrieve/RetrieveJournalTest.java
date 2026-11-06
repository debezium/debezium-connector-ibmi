/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.Job;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.ServiceProgramCall;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

class RetrieveJournalTest {
    @Test
    void requestsNativeClearsAndFileChangesFromServer() throws Exception {
        AS400 as400 = mock(AS400.class);
        AS400Message noMatchingEntries = mock(AS400Message.class);
        when(noMatchingEntries.getID()).thenReturn("CPF7062");
        RetrieveConfig config = new RetrieveConfigBuilder().withAs400(() -> as400)
                .withJournalInfo(new JournalInfo("JRN", "LIB", false)).build();
        RetrieveJournal journal = new RetrieveJournal(config, mock(JournalInfoRetrieval.class));
        try (var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> {
            when(call.getServerJob()).thenReturn(mock(Job.class));
            when(call.getMessageList()).thenReturn(new AS400Message[]{ noMatchingEntries });
        })) {
            JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.ONE,
                    new JournalReceiver("RCV", "LIB"), Instant.EPOCH, true);
            journal.retrieveJournal(start, new PositionRange(false, start, new JournalPosition(BigInteger.TEN, start.getReceiver())));
            ArgumentCaptor<ProgramParameter[]> parameters = ArgumentCaptor.forClass(ProgramParameter[].class);
            verify(calls.constructed().get(0)).setProgram(anyString(), parameters.capture());
            byte[] criteria = parameters.getValue()[4].getInputData();
            ByteBuffer buffer = ByteBuffer.wrap(criteria);
            int count = buffer.getInt();
            boolean requestsF = false;
            boolean requestsCR = false;
            boolean requestsCG = false;
            for (int i = 0, at = 4; i < count; i++) {
                int length = buffer.getInt(at);
                int key = buffer.getInt(at + 4);
                if (key == 7 || key == 8) {
                    int entries = buffer.getInt(at + 12);
                    int stride = key == 7 ? 20 : 10;
                    for (int j = 0; j < entries; j++) {
                        String value = ((String) new AS400Text(10).toObject(criteria, at + 16 + j * stride)).trim();
                        requestsF |= key == 7 && value.equals("F");
                        requestsCR |= key == 8 && value.equals("CR");
                        requestsCG |= key == 8 && value.equals("CG");
                    }
                }
                at += length;
            }
            assertTrue(requestsF, "Native member operations must pass the journal-code filter");
            assertTrue(requestsCR, "CLRPFM must pass the entry-type filter");
            assertTrue(requestsCG, "Existing file-change entries must still pass the entry-type filter");
        }
    }

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
                positionMatches.getReceiver().name(), positionMatches.getReceiver().library());
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
                positionMatches.getReceiver().name(), positionMatches.getReceiver().library());
        assertTrue(RetrieveJournal.alreadyProcessed(positionMatches, entryHeader), "compare only sequence number, library and receiver empty");

        EntryHeader entryHeaderDifferentOffset = new EntryHeader(-1, -1, -1l, positionMatches.getOffset().add(BigInteger.ONE), BigInteger.TWO,
                Instant.ofEpochMilli(2l), 'Z', "UB", "OBJECT", BigInteger.TEN, -1, -1,
                "", "");
        assertFalse(RetrieveJournal.alreadyProcessed(positionMatches, entryHeaderDifferentOffset), "compare only sequence number, library and receiver empty");

    }

    @Test
    public void testAlreadyProcessedOffsetNotProcessed() {
        JournalProcessedPosition positionMatches = firstPosition();
        JournalProcessedPosition positionMatchesNotProcessed = firstPosition().setProcessed(false);

        EntryHeader entryHeader = new EntryHeader(-1, -1, -1l, positionMatchesNotProcessed.getOffset(), BigInteger.TWO,
                Instant.ofEpochMilli(2l), 'Z', "UB", "OBJECT", BigInteger.TEN, -1, -1,
                positionMatchesNotProcessed.getReceiver().name(), positionMatchesNotProcessed.getReceiver().library());

        assertTrue(RetrieveJournal.alreadyProcessed(positionMatches, entryHeader), "compare sequence number, library and receiver match");
        assertFalse(RetrieveJournal.alreadyProcessed(positionMatchesNotProcessed, entryHeader), "compare sequence number, library and receiver match");

        EntryHeader entryHeaderEmptyReceiver = new EntryHeader(-1, -1, -1l, positionMatchesNotProcessed.getOffset(), BigInteger.TWO,
                Instant.ofEpochMilli(2l), 'Z', "UB", "OBJECT", BigInteger.TEN, -1, -1,
                "", "");

        assertTrue(RetrieveJournal.alreadyProcessed(positionMatches, entryHeaderEmptyReceiver), "compare sequence number, library and receiver match");
        assertFalse(RetrieveJournal.alreadyProcessed(positionMatchesNotProcessed, entryHeaderEmptyReceiver), "compare only sequence number, library and receiver empty");
    }
}
