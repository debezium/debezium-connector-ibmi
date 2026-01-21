/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.exception.InvalidPositionException;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;

public class ReceiverPagination {
    static final Logger log = LoggerFactory.getLogger(ReceiverPagination.class);

    private final JournalInfoRetrieval journalInfoRetrieval;
    private final JournalInfo journalInfo;
    private final BigInteger maxServerSideEntriesBI;
    private DetailedJournalReceiver cachedEndPosition;
    private List<DetailedJournalReceiver> cachedReceivers = null;

    ReceiverPagination(JournalInfoRetrieval journalInfoRetrieval, int maxServerSideEntries, JournalInfo journalInfo) {
        this.journalInfoRetrieval = journalInfoRetrieval;
        maxServerSideEntriesBI = BigInteger.valueOf(maxServerSideEntries);
        this.journalInfo = journalInfo;
    }

    Optional<PositionRange> findRange(AS400 as400, JournalProcessedPosition startPosition) throws Exception {
        final Optional<DetailedJournalReceiver> endPositionOpt = journalInfoRetrieval.getDelayedDetailedJournalReceiver(as400, journalInfo);

        return endPositionOpt.map(endPosition -> {
            try {
                return _findRange(as400, startPosition, endPosition);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    PositionRange _findRange(AS400 as400, JournalProcessedPosition startPosition, DetailedJournalReceiver endPosition) throws Exception {
        final BigInteger start = startPosition.getOffset();
        final boolean fromBeginning = !startPosition.isOffsetSet() || start.equals(BigInteger.ZERO);

        if (cachedEndPosition == null) {
            cachedEndPosition = endPosition;
        }

        if (cachedReceivers == null) {
            cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
        }

        if (fromBeginning) {
            DetailedJournalReceiver first = cachedReceivers.get(0);
            startPosition = new JournalProcessedPosition(first.start(), first.info().receiver(), Instant.EPOCH, false);
        }

        if (cachedEndPosition.isSameReceiver(endPosition)) {
            // refresh end position in cached list
            updateEndPosition(cachedReceivers, endPosition);
            // we're currently on the same journal just check the relative offset is within range
            // don't update the cache as we are not going to know the real end offset for this journal receiver until we move on to the next
            if (startPosition.isSameReceiver(endPosition)) {
                return paginateInSameReceiver(startPosition, endPosition, maxServerSideEntriesBI);
            }
        }
        else {
            // last call to current position won't include the correct end offset so we need to refresh the list
            cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
            cachedEndPosition = endPosition;
        }

        Optional<PositionRange> endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers);
        if (endOpt.isEmpty()) {
            log.warn("retrying to find end offset");
            cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
            endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers);
            if (endOpt.isEmpty()) {
                throw new InvalidPositionException("unable to find receiver " + startPosition + " in " + cachedReceivers);
            }
        }

        log.debug("end {} journals {}", endPosition, cachedReceivers);

        final JournalProcessedPosition startf = new JournalProcessedPosition(startPosition);
        return endOpt.orElseGet(
                () -> new PositionRange(fromBeginning, startf,
                        new JournalPosition(endPosition.end(), endPosition.info().receiver())));
    }

    static void updateEndPosition(List<DetailedJournalReceiver> list, DetailedJournalReceiver endPosition) {
        // should be last entry
        for (int i = list.size() - 1; i >= 0; i--) {
            final DetailedJournalReceiver d = list.get(i);
            if (d.isSameReceiver(endPosition)) {
                list.set(i, endPosition);
                return;
            }
        }
        list.add(endPosition);
    }

    /**
     * only valid when startPosition and endJournalPosition are the same receiver and library
     * @param startPosition
     * @param receiver
     * @param maxServerSideEntriesBI
     * @return
     * @throws Exception
     */
    PositionRange paginateInSameReceiver(JournalProcessedPosition startPosition, DetailedJournalReceiver receiver, BigInteger maxServerSideEntriesBI)
            throws Exception {
        if (!startPosition.isSameReceiver(receiver)) {
            throw new Exception(String.format("Error this method is only valid for same receiver start %s, end %s", startPosition, receiver));
        }
        final BigInteger diff = receiver.end().subtract(startPosition.getOffset());
        if (diff.compareTo(maxServerSideEntriesBI) > 0) { // check
            final BigInteger restricted = startPosition.getOffset().add(maxServerSideEntriesBI);
            return new PositionRange(false, startPosition,
                    new JournalPosition(restricted, startPosition.getReceiver()));
        }
        return new PositionRange(false, startPosition,
                new JournalPosition(receiver.end(), startPosition.getReceiver()));
    }

    /**
     * should handle reset offset numbers between subsequent entries in the list
     * @param startPosition
     * @param maxEntries
     * @param receivers
     * @return try and find end position at most offsetFromStart from start using the receiver list
     */
    Optional<PositionRange> findPosition(JournalProcessedPosition startPosition, BigInteger maxEntries,
                                         List<DetailedJournalReceiver> receivers)
            throws Exception {

        for (Iterator<DetailedJournalReceiver> it = receivers.iterator(); it.hasNext();) {
            DetailedJournalReceiver nextReceiver = it.next();
            if (nextReceiver.isSameReceiver(startPosition)) {
                if (startEqualsEndAndProcessed(startPosition, nextReceiver)) {
                    if (it.hasNext()) {
                        nextReceiver = it.next();
                        startPosition.setPosition(new JournalPosition(nextReceiver.start(), nextReceiver.info().receiver()), false);
                        return Optional.of(
                                paginateInSameReceiver(startPosition, nextReceiver, maxEntries));
                    }
                    else {
                        // we're at the end and we've processed everything
                        return Optional.of(
                                paginateInSameReceiver(startPosition, nextReceiver, maxEntries));
                    }
                }
                return Optional.of(
                        paginateInSameReceiver(startPosition, nextReceiver, maxEntries));
            }
        }

        log.error("Current position {} not found in available receivers {}", startPosition, receivers);
        return Optional.empty();
    }

    private boolean startEqualsEndAndProcessed(JournalProcessedPosition start, DetailedJournalReceiver last) {
        return start.processed() && start.getOffset().equals(last.end());
    }

    boolean containsEndPosition(List<DetailedJournalReceiver> receivers, DetailedJournalReceiver endPosition) {
        boolean containsEndPosition = false;
        for (int i = receivers.size() - 1; i >= 0; i--) {
            if (receivers.get(i).info().receiver().equals(endPosition.info().receiver())) {
                containsEndPosition = true;
            }
        }
        return containsEndPosition;
    }

}
