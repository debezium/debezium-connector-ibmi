/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;

public class DelayedDetailedJournalReceiver {
    private static final Logger log = LoggerFactory.getLogger(DelayedDetailedJournalReceiver.class);

    record TimedPosition(long timestamp, DetailedJournalReceiver position) {
    }

    private static final int PADDING = 100;
    private static final int MAX_ENTRIES = 1000; // assuming 35 seconds 1000 entries allows 350ms granularity
    private final int entries;
    private final TimedPosition[] timedPositions;
    private int addPosition = 0;
    private int getPosition = 0;
    private long lastAdded = Long.MIN_VALUE;
    private long minDelayBetweenReadings;
    private long delayMs;
    private final Supplier<Long> timeSupplier;
    static final double TIMING_ALLOWANCE_PERCENT = 20.0;
    static final double TIMING_ALLOWANCE_RATIO = TIMING_ALLOWANCE_PERCENT / 100.0;

    public DelayedDetailedJournalReceiver(long delayMs, long pollIntervalMs) {
        this(delayMs, pollIntervalMs, System::currentTimeMillis);
    }

    public DelayedDetailedJournalReceiver(long delayMs, long pollIntervalMs, Supplier<Long> timeSupplier) {
        int t = (int) ((delayMs / pollIntervalMs) * (1.0 + TIMING_ALLOWANCE_PERCENT) + 1); // allow 20% and round up
        if (t > MAX_ENTRIES) {
            t = MAX_ENTRIES;
        }
        entries = t + PADDING;
        timedPositions = new TimedPosition[entries];
        minDelayBetweenReadings = delayMs / entries;
        this.delayMs = delayMs;
        this.timeSupplier = timeSupplier;
    }

    public boolean addDetailedReceiver(DetailedJournalReceiver position) {
        long now = timeSupplier.get();
        long allowedTime = lastAdded + minDelayBetweenReadingsWithAllowance();
        if (now < allowedTime) {
            log.debug("not adding as buffer has resolution {} and last added was at {}", minDelayBetweenReadings, lastAdded);
            return false;
        }
        lastAdded = now;
        if (timedPositions[addPosition] != null) {
            log.warn("adding faster than consuming, skipping");
            return false;
        }
        timedPositions[addPosition] = new TimedPosition(now, position);
        addPosition = (addPosition + 1) % entries;
        return true;
    }

    long minDelayBetweenReadings() {
        return minDelayBetweenReadings;
    }

    long minDelayBetweenReadingsWithAllowance() {
        return (long) (minDelayBetweenReadings * (1.0 - TIMING_ALLOWANCE_RATIO));
    }

    public DetailedJournalReceiver getDelayedReceiver() {
        long now = timeSupplier.get();
        TimedPosition tp = timedPositions[getPosition];
        if (tp == null) {
            log.debug("no receivers avaiable");
            return null;
        }
        if (now < tp.timestamp + delayMs) {
            log.debug("next receiver is still too recent {} need to wait until {} currently {}", tp.timestamp, tp.timestamp + delayMs, now);
            return null;
        }
        TimedPosition nextTp = tp;
        do {
            timedPositions[getPosition] = null;
            getPosition = (getPosition + 1) % entries;
            tp = nextTp;
            nextTp = timedPositions[getPosition];
        } while (nextTp != null && now >= nextTp.timestamp + delayMs);
        return tp.position;
    }
}