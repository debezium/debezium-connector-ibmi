/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

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

    PositionRange findRange(AS400 as400, JournalProcessedPosition startPosition) throws Exception {
        final BigInteger start = startPosition.getOffset();
        final boolean fromBeginning = !startPosition.isOffsetSet() || start.equals(BigInteger.ZERO);

        final DetailedJournalReceiver endPosition = journalInfoRetrieval.getCurrentDetailedJournalReceiver(as400, journalInfo);

        if (fromBeginning) {
            return new PositionRange(fromBeginning, startPosition,
                    new JournalPosition(endPosition.end(), endPosition.info().receiver()));
        }

        if (cachedEndPosition == null) {
            cachedEndPosition = endPosition;
        }

        if (cachedReceivers == null) {
            cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
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

        Optional<PositionRange> endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers,
                cachedEndPosition);
        if (endOpt.isEmpty()) {
            log.warn("retrying to find end offset");
            cachedReceivers = journalInfoRetrieval.getReceivers(as400, journalInfo);
            endOpt = findPosition(startPosition, maxServerSideEntriesBI, cachedReceivers, endPosition);
        }

        log.debug("end {} journals {}", endPosition, cachedReceivers);

        return endOpt.orElseGet(
                () -> new PositionRange(fromBeginning, startPosition,
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
     * @param endJournalPosition
     * @param maxServerSideEntriesBI
     * @return
     * @throws Exception
     */
    PositionRange paginateInSameReceiver(JournalProcessedPosition startPosition, DetailedJournalReceiver endJournalPosition, BigInteger maxServerSideEntriesBI)
            throws Exception {
        if (!startPosition.isSameReceiver(endJournalPosition)) {
            throw new Exception(String.format("Error this method is only valid for same receiver start %s, end %s", startPosition, endJournalPosition));
        }
        final BigInteger diff = endJournalPosition.end().subtract(startPosition.getOffset());
        if (diff.compareTo(maxServerSideEntriesBI) > 0) {
            final BigInteger restricted = startPosition.getOffset().add(maxServerSideEntriesBI);
            return new PositionRange(false, startPosition,
                    new JournalPosition(restricted, startPosition.getReceiver()));
        }
        return new PositionRange(false, startPosition,
                new JournalPosition(endJournalPosition.end(), startPosition.getReceiver()));
    }

    /**
     * should handle reset offset numbers between subsequent entries in the list
     * @param startPosition
     * @param maxEntries
     * @param receivers
     * @return try and find end position at most offsetFromStart from start using the receiver list
     */
    Optional<PositionRange> findPosition(JournalProcessedPosition startPosition, BigInteger maxEntries,
                                         List<DetailedJournalReceiver> receivers, DetailedJournalReceiver endPosition) {

        if (!containsEndPosition(receivers, endPosition)) {
            log.warn("unable to find active journal {} in receiver list", endPosition);
            return Optional.empty();
        }

        final RangeFinder finder = new RangeFinder(startPosition, maxEntries);
        for (int i = 0; i < receivers.size(); i++) {
            final Optional<PositionRange> range = finder.next(receivers.get(i));
            if (range.isPresent()) {
                return range;
            }
        }
        final Optional<PositionRange> range = finder.endRange();
        if (!finder.startFound()) {
            log.warn("Current position {} not found in available receivers {}", startPosition, receivers);
        }
        return range;
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

    static class RangeFinder {
        private boolean found = false;
        private DetailedJournalReceiver lastReceiver = null;
        private BigInteger remaining;
        private final JournalProcessedPosition startPosition;

        RangeFinder(JournalProcessedPosition startPosition, BigInteger maxEntries) {
            this.remaining = maxEntries;
            this.startPosition = startPosition;
        }

        public Optional<PositionRange> next(DetailedJournalReceiver nextReceiver) {
            if (found) {
                // if the next journal has wrapped use just go to the end of the previous one
                if (lastReceiver != null && nextReceiver.start().compareTo(lastReceiver.end()) < 0) {
                    // we're at the end and we've processed it move start on to next receiver
                    if (startEqualsEndAndProcessed(startPosition, lastReceiver)) {
                        startPosition.setPosition(new JournalPosition(nextReceiver.start(), nextReceiver.info().receiver()), false);
                    }
                    else {
                        // the only way we can get here is if we have already checked for pagination
                        return Optional.of(new PositionRange(false, startPosition,
                                new JournalPosition(lastReceiver.end(), lastReceiver.info().receiver())));
                    }
                }

                final Optional<PositionRange> r = rangeWithinCurrentPosition(nextReceiver, nextReceiver.start());
                if (r.isPresent()) {
                    return r;
                }
            }
            if (nextReceiver.isSameReceiver(startPosition)) {
                found = true;
                final Optional<PositionRange> r = rangeWithinCurrentPosition(nextReceiver, startPosition.getOffset());
                if (r.isPresent()) {
                    return r;
                }
            }
            lastReceiver = nextReceiver;
            return Optional.empty();
        }

        // adding one to the range and then adding as we include both ends
        // but we must not use the add one when setting the end point
        // i.e. 1-> 10 is a total of 10 entries but the range can only go to 10
        private Optional<PositionRange> rangeWithinCurrentPosition(DetailedJournalReceiver nextReceiver,
                                                                   BigInteger currentOffset) {
            final BigInteger difference = nextReceiver.end().subtract(currentOffset);
            final BigInteger entriesInJournal = difference.add(BigInteger.ONE); // add one as range is inclusive
            if (remaining.compareTo(difference) <= 0) { // range is inclusive but don't go past end when adding
                // remaining
                final BigInteger offset = currentOffset.add(remaining);
                return Optional.of(new PositionRange(false, startPosition,
                        new JournalPosition(offset, nextReceiver.info().receiver())));
            }
            remaining = remaining.subtract(entriesInJournal);
            return Optional.empty();
        }

        public Optional<PositionRange> endRange() {
            if (found && lastReceiver != null) {
                return Optional.of(
                        new PositionRange(false, startPosition, JournalPosition.endPosition(lastReceiver)));
            }
            return Optional.empty();
        }

        public boolean startFound() {
            return found;
        }

        private boolean startEqualsEndAndProcessed(JournalProcessedPosition start, DetailedJournalReceiver last) {
            return start.processed() && start.getOffset().equals(last.end());
        }
    }
}
