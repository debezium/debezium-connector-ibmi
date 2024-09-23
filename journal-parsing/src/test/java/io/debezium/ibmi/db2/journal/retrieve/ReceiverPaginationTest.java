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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalStatus;

@ExtendWith(MockitoExtension.class)
class ReceiverPaginationTest {
    ReceiverPagination receivers;
    @Mock
    JournalInfoRetrieval journalInfoRetrieval;
    JournalInfo journalInfo = new JournalInfo("journal", "journallib");
    @Mock
    AS400 as400;

    DetailedJournalReceiver dr3 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("j3", "jlib"),
            new Date(3), JournalStatus.Attached, Optional.of(1)), BigInteger.valueOf(9), BigInteger.valueOf(17),
            Optional.empty(), 1, 1);
    DetailedJournalReceiver dr2 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("j2", "jlib"),
            new Date(2), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.valueOf(3),
            BigInteger.valueOf(8),
            Optional.of(dr3.info().receiver()), 1, 1);
    DetailedJournalReceiver dr1 = new DetailedJournalReceiver(new JournalReceiverInfo(new JournalReceiver("j1", "jlib"),
            new Date(1), JournalStatus.OnlineSavedDetached, Optional.of(1)), BigInteger.ONE, BigInteger.TWO,
            Optional.of(dr2.info().receiver()), 1, 1);

    @BeforeEach
    public void setUp() throws Exception {
    }

    @Test
    void findRangeWithinCurrentPosistion() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);

        final List<DetailedJournalReceiver> list = Arrays.asList(dr1);

        when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list);

        when(journalInfoRetrieval.getCurrentDetailedJournalReceiver(any(), any())).thenReturn(dr1);

        final JournalProcessedPosition startPosition = new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        final PositionRange result = jreceivers.findRange(as400, startPosition);
        final PositionRange rangeAnswer = new PositionRange(false, startPosition,
                new JournalPosition(dr1.end(), dr1.info().receiver()));
        assertEquals(rangeAnswer, result);
    }

    @Test
    void findRangeInList() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);

        final List<DetailedJournalReceiver> list = Arrays.asList(dr1, dr2);

        when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list);

        when(journalInfoRetrieval.getCurrentDetailedJournalReceiver(any(), any())).thenReturn(dr2);

        final JournalProcessedPosition startPosition = new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        final PositionRange result = jreceivers.findRange(as400, startPosition);
        final PositionRange rangeAnswer = new PositionRange(false, startPosition,
                new JournalPosition(dr2.end(), dr2.info().receiver()));
        assertEquals(rangeAnswer, result);
    }

    @Test
    void findRangeReFetchList() throws Exception {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 15, journalInfo);

        final DetailedJournalReceiver detailedEnd = dr2;
        final List<DetailedJournalReceiver> list = Arrays.asList(dr1, detailedEnd);

        final DetailedJournalReceiver detailedEnd2 = dr3;
        final List<DetailedJournalReceiver> list2 = Arrays.asList(dr1, dr2, detailedEnd2);
        when(journalInfoRetrieval.getReceivers(any(), any())).thenReturn(list).thenReturn(list2);
        when(journalInfoRetrieval.getCurrentDetailedJournalReceiver(any(), any())).thenReturn(detailedEnd)
                .thenReturn(detailedEnd2);

        final JournalProcessedPosition startPosition = new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        final PositionRange result = jreceivers.findRange(as400, startPosition);
        final PositionRange rangeAnswer = new PositionRange(false, startPosition,
                new JournalPosition(BigInteger.valueOf(8), detailedEnd.info().receiver()));
        assertEquals(rangeAnswer, result);

        final JournalProcessedPosition startPosition2 = new JournalProcessedPosition(BigInteger.valueOf(2),
                new JournalReceiver("j2", "jlib"), Instant.ofEpochSecond(0), true);
        final PositionRange result2 = jreceivers.findRange(as400, startPosition2);
        final PositionRange rangeAnswer2 = new PositionRange(false, startPosition2,
                new JournalPosition(BigInteger.valueOf(17), detailedEnd2.info().receiver()));
        assertEquals(rangeAnswer2, result2);

    }

    @Test
    void testFindRangeMidFirstEntry() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(5), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(6), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(3),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(21), BigInteger.valueOf(22), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = Arrays.asList(j1, j2, j3);
        final Optional<PositionRange> position = jreceivers.findPosition(
                new JournalProcessedPosition(BigInteger.ONE, j1.info().receiver(), Instant.ofEpochSecond(0), true),
                BigInteger.valueOf(3), list, j3);
        assertTrue(position.isPresent());
        assertEquals("j1", position.get().end().getReceiver().name());
        assertEquals(BigInteger.valueOf(4), position.get().end().getOffset());
    }

    @Test
    void testFindRangeMidSecondEntryReset() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(3),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(21), BigInteger.valueOf(30), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = Arrays.asList(j1, j2, j3);
        final Optional<PositionRange> position = jreceivers.findPosition(
                new JournalProcessedPosition(BigInteger.ONE, j1.info().receiver(), Instant.ofEpochSecond(0), true),
                BigInteger.valueOf(15), list, j3);
        assertTrue(position.isPresent());
        assertEquals("j2", position.get().end().getReceiver().name());
        assertEquals(BigInteger.valueOf(16), position.get().end().getOffset());
    }

    @Test
    void testFindRangeMidSecondContiguous() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(2), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(3), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(3),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(21), BigInteger.valueOf(22), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = Arrays.asList(j1, j2, j3);
        final Optional<PositionRange> position = jreceivers.findPosition(
                new JournalProcessedPosition(BigInteger.ONE, j1.info().receiver(), Instant.ofEpochSecond(0), true),
                BigInteger.valueOf(10), list, j3);
        assertTrue(position.isPresent());
        assertEquals("j2", position.get().end().getReceiver().name());
        assertEquals(BigInteger.valueOf(11), position.get().end().getOffset());
    }

    @Test
    void testFindRangeMidEndEntry() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(2), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(3), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(3),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(21), BigInteger.valueOf(35), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = Arrays.asList(j1, j2, j3);
        final Optional<PositionRange> position = jreceivers.findPosition(
                new JournalProcessedPosition(BigInteger.ONE, j1.info().receiver(), Instant.ofEpochSecond(0), true),
                BigInteger.valueOf(30), list, j3);
        assertTrue(position.isPresent());
        assertEquals("j3", position.get().end().getReceiver().name());
        assertEquals(BigInteger.valueOf(31), position.get().end().getOffset());
    }

    @Test
    void testFindRangePastEnd() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(2), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(3), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(3),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(21), BigInteger.valueOf(35), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2, j3);
        final Optional<PositionRange> position = jreceivers.findPosition(
                new JournalProcessedPosition(BigInteger.ONE, j1.info().receiver(), Instant.ofEpochSecond(0), true),
                BigInteger.valueOf(100), list, j3);
        assertTrue(position.isPresent());
        assertEquals("j3", position.get().end().getReceiver().name());
        assertEquals(BigInteger.valueOf(35), position.get().end().getOffset());
    }

    @Test
    void testFindRangeEqualsEnd() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 10, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1);
        final Optional<PositionRange> position = jreceivers.findPosition(
                new JournalProcessedPosition(BigInteger.ONE, j1.info().receiver(), Instant.ofEpochSecond(0), true),
                BigInteger.valueOf(100), list, j1);
        assertTrue(position.isPresent());
        assertEquals("j1", position.get().end().getReceiver().name());
        assertEquals(BigInteger.valueOf(10), position.get().end().getOffset());
    }

    @Test
    void testFindMidStartingMid() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(3),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(21), BigInteger.valueOf(30), Optional.of(new JournalReceiver("j4", "jlib")), 1, 1);
        final DetailedJournalReceiver j4 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j4", "jlib"), new Date(4),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(41), BigInteger.valueOf(50), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2, j3, j4);
        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(25),
                j3.info().receiver(), Instant.ofEpochSecond(0), true);
        final Optional<PositionRange> position = jreceivers.findPosition(start, BigInteger.valueOf(10), list, j4);
        assertTrue(position.isPresent());
        assertEquals("j4", position.get().end().getReceiver().name());
        assertEquals(BigInteger.valueOf(45), position.get().end().getOffset());

    }

    @Test
    void testFindStartingPastEnd() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(20), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2);
        final Optional<PositionRange> position = jreceivers
                .findPosition(new JournalProcessedPosition(BigInteger.valueOf(30), new JournalReceiver("j3", "jlib"),
                        Instant.ofEpochSecond(0), true), BigInteger.valueOf(15), list, j2);
        assertTrue(position.isEmpty());
    }

    @Test
    void testPaginateInSameReceiverEnd() throws Exception {
        final int maxOffset = 1000;
        final BigInteger maxServerSideEntriesBI = BigInteger.valueOf(maxOffset);
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, maxOffset, journalInfo);

        final JournalProcessedPosition startPosition = new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        final DetailedJournalReceiver endJournalPosition = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(100), Optional.empty(), 1, 1);

        final PositionRange range = jreceivers.paginateInSameReceiver(startPosition, endJournalPosition,
                maxServerSideEntriesBI);
        assertEquals(endJournalPosition.end(), range.end().getOffset());
    }

    @Test
    void testPaginateInSameReceiverLimited() throws Exception {
        final int maxOffset = 10;
        final BigInteger maxServerSideEntriesBI = BigInteger.valueOf(maxOffset);
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, maxOffset, journalInfo);

        final JournalProcessedPosition startPosition = new JournalProcessedPosition(BigInteger.ONE,
                new JournalReceiver("j1", "jlib"), Instant.ofEpochSecond(0), true);
        final DetailedJournalReceiver endJournalPosition = new DetailedJournalReceiver(
                new JournalReceiverInfo(startPosition.getReceiver(), new Date(1), JournalStatus.OnlineSavedDetached,
                        Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(100), Optional.empty(), 1, 1);

        final PositionRange range = jreceivers.paginateInSameReceiver(startPosition, endJournalPosition,
                maxServerSideEntriesBI);
        assertEquals(startPosition.getOffset().add(maxServerSideEntriesBI), range.end().getOffset());
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
    void testFindMissingCurrentReceiver() {
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
                .findPosition(new JournalProcessedPosition(BigInteger.valueOf(30), j1.info().receiver(),
                        Instant.ofEpochSecond(0), true), BigInteger.valueOf(15), list, j3);
        assertTrue(position.isEmpty());
    }

    @Test
    void testContainsEndPosition() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final List<DetailedJournalReceiver> list = List.of(dr2, dr3);
        assertTrue(jreceivers.containsEndPosition(list, dr3), "last entry found");
        assertTrue(jreceivers.containsEndPosition(list, dr2), "first entry found");
    }

    @Test
    void testNotContainsEndPosition() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final List<DetailedJournalReceiver> list = List.of(dr2, dr3);
        final boolean found = jreceivers.containsEndPosition(list, dr1);
        assertFalse(found, "receiver does not exist in list");
    }

    @Test
    void testStartEqualsEndNotProcessed() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1);

        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(10),
                j1.info().receiver(), Instant.ofEpochSecond(0), false);

        final Optional<PositionRange> found = jreceivers.findPosition(start, BigInteger.valueOf(15), list, j1);
        assertEquals(start, found.get().start());
        assertEquals(start.asJournalPosition(), found.get().end());
        assertFalse(found.get().startEqualsEnd());
    }

    @Test
    void testStartEqualsEndProcessed() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1);

        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(10),
                j1.info().receiver(), Instant.ofEpochSecond(0), true);

        final Optional<PositionRange> found = jreceivers
                .findPosition(start, BigInteger.valueOf(15), list, j1);
        assertEquals(start, found.get().start());
        assertEquals(start.asJournalPosition(), found.get().end());
        assertTrue(found.get().startEqualsEnd());
    }

    @Test
    void testStartEqualsEndProcessedResetReceiver() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(1), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2);

        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(10),
                j1.info().receiver(), Instant.ofEpochSecond(10), true);

        final Optional<PositionRange> found = jreceivers.findPosition(start, BigInteger.valueOf(20), list, j2);
        assertEquals(
                new JournalProcessedPosition(JournalPosition.startPosition(j2), start.getTimeOfLastProcessed(), false),
                found.get().start());
        assertEquals(JournalPosition.endPosition(j2), found.get().end());
        assertFalse(found.get().startEqualsEnd());
    }

    @Test
    void testStartEqualsEndNotProcessedResetReceivers() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(1), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2);

        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(10),
                j1.info().receiver(), Instant.ofEpochSecond(10), false);

        final Optional<PositionRange> found = jreceivers.findPosition(start, BigInteger.valueOf(20), list, j2);
        assertEquals(start, found.get().start());
        assertEquals(JournalPosition.endPosition(j1), found.get().end());
        assertFalse(found.get().startEqualsEnd());
    }

    @Test
    void testStartEqualsEndProcessedResetReceiversPaginate() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 100, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2);

        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(10),
                j1.info().receiver(), Instant.ofEpochSecond(10), true);

        final Optional<PositionRange> found = jreceivers.findPosition(start, BigInteger.valueOf(5), list, j2);
        assertEquals(
                new JournalProcessedPosition(JournalPosition.startPosition(j2), start.getTimeOfLastProcessed(), false),
                found.get().start());
        assertEquals(new JournalPosition(BigInteger.valueOf(5l), j2.info().receiver()), found.get().end());
        assertFalse(found.get().startEqualsEnd());
    }

    private static final Logger log = LoggerFactory.getLogger(ReceiverPaginationTest.class);

    @Test
    void testStopBeforeJournalResetsPaginateOver() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 20, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(4),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(100), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2, j3);
        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(5),
                j1.info().receiver(), Instant.ofEpochSecond(0), true);
        final Optional<PositionRange> position = jreceivers.findPosition(start, BigInteger.valueOf(16), list, j1);
        assertTrue(position.isPresent());
        assertEquals("j2", position.get().end().getReceiver().name());
        assertEquals(j2.end(), position.get().end().getOffset());

    }

    @Test
    void testStopBeforeJournalResetsPaginateExact() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 20, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(4),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(100), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2, j3);
        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(5),
                j1.info().receiver(), Instant.ofEpochSecond(0), true);
        final Optional<PositionRange> position = jreceivers.findPosition(start, BigInteger.valueOf(15), list, j1);
        assertTrue(position.isPresent());
        assertEquals("j2", position.get().end().getReceiver().name());
        assertEquals(j2.end(), position.get().end().getOffset());

    }

    @Test
    void testStopOneBeforeJournalResetsPaginate() {
        final ReceiverPagination jreceivers = new ReceiverPagination(journalInfoRetrieval, 20, journalInfo);
        final DetailedJournalReceiver j1 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j1", "jlib"), new Date(1),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(10), Optional.of(new JournalReceiver("j2", "jlib")), 1, 1);
        final DetailedJournalReceiver j2 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j2", "jlib"), new Date(2),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(11), BigInteger.valueOf(20), Optional.of(new JournalReceiver("j3", "jlib")), 1, 1);
        final DetailedJournalReceiver j3 = new DetailedJournalReceiver(
                new JournalReceiverInfo(new JournalReceiver("j3", "jlib"), new Date(4),
                        JournalStatus.OnlineSavedDetached, Optional.of(1)),
                BigInteger.valueOf(1), BigInteger.valueOf(100), Optional.empty(), 1, 1);
        final List<DetailedJournalReceiver> list = List.of(j1, j2, j3);
        final JournalProcessedPosition start = new JournalProcessedPosition(BigInteger.valueOf(5),
                j1.info().receiver(), Instant.ofEpochSecond(0), true);
        final Optional<PositionRange> position = jreceivers.findPosition(start, BigInteger.valueOf(14), list, j1);
        assertTrue(position.isPresent());
        assertEquals("j2", position.get().end().getReceiver().name());
        assertEquals(j2.end().subtract(BigInteger.ONE), position.get().end().getOffset());
    }
}
