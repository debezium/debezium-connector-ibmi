/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

public record DetailedJournalReceiver(JournalReceiverInfo info, BigInteger start, BigInteger end,
        Optional<JournalReceiver> nextReceiver, long maxEntryLength, long numberOfEntries) {

    private static final Logger log = LoggerFactory.getLogger(DetailedJournalReceiver.class);

    public DetailedJournalReceiver withStatus(JournalStatus status) {
        return new DetailedJournalReceiver(info().withStatus(status), this.start, this.end, this.nextReceiver,
                this.maxEntryLength, this.numberOfEntries);
    }

    public boolean isSameReceiver(JournalProcessedPosition position) {
        if (info == null || position == null) {
            return false;
        }
        return info.receiver().equals(position.getReceiver());
    }

    public boolean isSameReceiver(DetailedJournalReceiver otherReceiver) {
        if (info == null || otherReceiver == null || otherReceiver.info() == null) {
            return false;
        }
        return info.receiver().equals(otherReceiver.info().receiver());
    }

    public boolean isAttached() {
        if (info != null) {
            return JournalStatus.Attached.equals(info.status());
        }
        return false;
    }

    // simplistic way of looking for gaps in receivers
    public static List<DetailedJournalReceiver> lastJoined(List<DetailedJournalReceiver> list) {
        final List<DetailedJournalReceiver> attached = list.stream().filter(x -> {
            if (x.info().attachTime() != null) {
                return true;
            }
            log.info("deleting journal {} will null start time", x);
            return false;
        }).collect(Collectors.toList()); // exclude any that have never been
                                         // attached
        attached.sort((DetailedJournalReceiver f, DetailedJournalReceiver s) -> f.info().attachTime()
                .compareTo(s.info().attachTime()));
        final int last = indexOfLastJoined(attached);
        if (last == 0) {
            return attached;
        }
        if (last >= attached.size()) {
            log.warn("no available receivers");
            return Collections.<DetailedJournalReceiver> emptyList();
        }
        log.info("restricting as receiver is unavailble {}", attached.get(last - 1));

        return attached.subList(last, attached.size());
    }

    public static int indexOfLastJoined(List<DetailedJournalReceiver> list) {
        final int size = list.size();
        int last = 0;
        for (int i = 0; i < size; i++) {
            if (!list.get(i).isJoined()) {
                last = i + 1;
            }
        }
        return last;
    }

    public boolean isJoined() {
        return isJoined(this.info.status());
    }

    public static boolean isJoined(JournalStatus status) {
        return switch (status) {
            case Attached, OnlineSavedDetached, SavedDetchedNotFreed -> true;
            default -> false;
        };
    }
}