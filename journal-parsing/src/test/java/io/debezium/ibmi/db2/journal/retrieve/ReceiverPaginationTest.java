/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalStatus;

@ExtendWith(MockitoExtension.class)
class ReceiverPaginationTest {
    ReceiverPagination receivers;
    @Mock
    JournalInfoRetrieval journalInfoRetrieval;
    JournalInfo journalInfo = new JournalInfo("journal", "journallib", false);
    @Mock
    AS400 as400;

    DetailedJournalReceiver dr3 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("j3", "jlib"),
            new Date(3), JournalStatus.Attached, Optional.of(1)),
            BigInteger.valueOf(21), BigInteger.valueOf(32),
            Optional.empty(), 1, 1);
    DetailedJournalReceiver dr2 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("j2", "jlib"),
            new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)),
            BigInteger.valueOf(11), BigInteger.valueOf(20),
            Optional.of(dr3.info().receiver()), 1, 1);
    DetailedJournalReceiver dr1 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("j1", "jlib"),
            new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)),
            BigInteger.ONE, BigInteger.valueOf(10),
            Optional.of(dr2.info().receiver()), 1, 1);

    @BeforeEach
    public void setUp() throws Exception {
    }

    @Test
    void findToEndOfFirstEntry() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);

        final List<DetailedJournalReceiver> list = Arrays.asList(dr1, dr2);

        when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list);

        when(journalInfoRetrieval.getDelayedDetailedJournalReceiver(any(), any())).thenReturn(Optional.of(dr2));

        final JournalProcessedPosition startPosition = new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        final Optional<PositionRange> result = jreceivers.findRange(as400, startPosition);
        final PositionRange rangeAnswer = new PositionRange(false, startPosition,
                new JournalPosition(dr1.end(), dr1.info().receiver()));
        assertEquals(rangeAnswer, result.get());
    }

    @Test
    void findSecondWhenFirstReceiverFinished() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);

        final List<DetailedJournalReceiver> list = Arrays.asList(dr1, dr2);

        when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list);

        when(journalInfoRetrieval.getDelayedDetailedJournalReceiver(any(), any())).thenReturn(Optional.of(dr2));

        final JournalProcessedPosition startPosition = new JournalProcessedPosition(dr1.end(),
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        final Optional<PositionRange> result = jreceivers.findRange(as400, startPosition);
        final JournalProcessedPosition startExpected = new JournalProcessedPosition(dr2.start(),
                new JournalReceiver("j2", "jlib"), Instant.ofEpochSecond(0), false);
        final PositionRange rangeExpected = new PositionRange(false, startExpected,
                new JournalPosition(dr2.end(), dr2.info().receiver()));
        assertEquals(rangeExpected, result.get());
    }

    @Test
    void findFirstWhenFirstReceiverAtEndButNotProcessed() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);

        final List<DetailedJournalReceiver> list = Arrays.asList(dr1, dr2);

        when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list);

        when(journalInfoRetrieval.getDelayedDetailedJournalReceiver(any(), any())).thenReturn(Optional.of(dr2));

        final JournalProcessedPosition startPosition = new JournalProcessedPosition(dr1.end(),
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), false);
        final Optional<PositionRange> result = jreceivers.findRange(as400, startPosition);

        final PositionRange rangeExpected = new PositionRange(false, startPosition,
                new JournalPosition(dr1.end(), dr1.info().receiver()));
        assertEquals(rangeExpected, result.get());
    }

    @Test
    void testFindRangeWhenAtEndOfList() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);

        final List<DetailedJournalReceiver> list = List.of(dr1, dr2);
        JournalProcessedPosition start = new JournalProcessedPosition(dr2.end(), dr2.info().receiver(), Instant.ofEpochSecond(0), true);
        final Optional<PositionRange> position = jreceivers.findPosition(start, BigInteger.valueOf(100), list);

        assertTrue(position.isPresent());
        assertEquals(dr2.info().receiver(), position.get().end().getReceiver());
        assertEquals(dr2.end(), position.get().end().offset());
    }

    @Test
    public void paginateInSameReceiverRestrict() throws Exception {
        final BigInteger maxServerSideEntriesBI = BigInteger.valueOf(5);
        final ReceiverPagination pagination = new ReceiverPagination(journalInfoRetrieval, maxServerSideEntriesBI.intValue(), journalInfo);
        final JournalProcessedPosition startPosition = new JournalProcessedPosition(dr1.start().add(BigInteger.ONE),
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        PositionRange range = pagination.paginateInSameReceiver(startPosition, dr1, maxServerSideEntriesBI);

        final PositionRange rangeExpected = new PositionRange(false, startPosition,
                new JournalPosition(startPosition.getOffset().add(maxServerSideEntriesBI), dr1.info().receiver()));
        assertEquals(rangeExpected, range);
    }

    @Test
    public void paginateInSameReceiverToEnd() throws Exception {
        final BigInteger maxServerSideEntriesBI = BigInteger.valueOf(100);
        final ReceiverPagination pagination = new ReceiverPagination(journalInfoRetrieval, maxServerSideEntriesBI.intValue(), journalInfo);
        final JournalProcessedPosition startPosition = new JournalProcessedPosition(dr1.start().add(BigInteger.ONE),
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        PositionRange range = pagination.paginateInSameReceiver(startPosition, dr1, maxServerSideEntriesBI);

        final PositionRange rangeExpected = new PositionRange(false, startPosition,
                new JournalPosition(dr1.end(), dr1.info().receiver()));
        assertEquals(rangeExpected, range);
    }

    @Test
    void testUpdateEndPosition() {
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(20), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = Arrays.asList(j1, j2);

        final DetailedJournalReceiver endPosition = new DetailedJournalReceiver(
                new JournalReceiverInfo(j2.info().receiver(), new Date(2), JournalStatus.OnlineSavedDetached,
                        Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(200), Optional.empty(), 1, 1);
        ReceiverPagination.updateEndPosition(list, endPosition);

        assertEquals(endPosition, list.get(1));
    }

    @Test
    void testFindMissingCurrentReceiver() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(20), Optional.empty(), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(3),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(21), BigInteger.valueOf(31), Optional.of(new JournalReceiver("j4", "jlib")), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2);
        final Optional<PositionRange> position = jreceivers
                .findPosition(new JournalProcessedPosition(j3.start(), j3.info().receiver(),
                        Instant.ofEpochSecond(0), true), BigInteger.valueOf(15), list);
        assertTrue(position.isEmpty());
    }

    @Test
    void testStartEqualsEndNotProcessed() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1);

        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(10),
                j1.info().receiver(), Instant.ofEpochSecond(0), false);

        final Optional<PositionRange> found = jreceivers.findPosition(start, BigInteger.valueOf(15), list);
        assertEquals(start, found.get().start());
        assertEquals(start.asJournalPosition(), found.get().end());
        assertFalse(found.get().startEqualsEnd());
    }

    @Test
    void testStartEqualsEndProcessed() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1);

        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(10),
                j1.info().receiver(), Instant.ofEpochSecond(0), true);

        final Optional<PositionRange> found = jreceivers
                .findPosition(start, BigInteger.valueOf(15), list);
        assertEquals(start, found.get().start());
        assertEquals(start.asJournalPosition(), found.get().end());
        assertTrue(found.get().startEqualsEnd());
    }
}
