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
    private long delayMilliseconds;
    private final Supplier<Long> timeSupplier;

    public DelayedDetailedJournalReceiver(long delayMilliseconds, long pollIntervalMs) {
        this(delayMilliseconds, pollIntervalMs, System::currentTimeMillis);
    }

    public DelayedDetailedJournalReceiver(long delayMilliseconds, long pollIntervalMs, Supplier<Long> timeSupplier) {
        int t = (int) ((delayMilliseconds / pollIntervalMs) * 1.2 + 1); // allow 20% and round up
        if (t > MAX_ENTRIES) {
            t = MAX_ENTRIES;
        }
        entries = t + PADDING;
        timedPositions = new TimedPosition[entries];
        minDelayBetweenReadings = delayMilliseconds / entries;
        this.delayMilliseconds = delayMilliseconds;
        this.timeSupplier = timeSupplier;
    }

    public void addDetailedReceiver(DetailedJournalReceiver position) {
        long now = timeSupplier.get();
        if (now < minDelayBetweenReadings + lastAdded) {
            log.debug("not adding as buffer has resolution {} and last added was at {}", minDelayBetweenReadings, lastAdded);
            return;
        }
        lastAdded = now;
        if (timedPositions[addPosition] != null) {
            log.warn("adding faster than consuming, skipping");
        }
        timedPositions[addPosition] = new TimedPosition(now, position);
        addPosition = (addPosition + 1) % entries;
    }

    public DetailedJournalReceiver getDelayedReceiver() {
        long now = timeSupplier.get();
        TimedPosition tp = timedPositions[getPosition];
        if (tp == null) {
            log.debug("no receivers avaiable");
            return null;
        }
        if (now < tp.timestamp + delayMilliseconds) {
            log.debug("next receiver is still too recent {} need to wait until {} currently {}", tp.timestamp, tp.timestamp + delayMilliseconds, now);
            return null;
        }
        TimedPosition nextTp = tp;
        do {
            timedPositions[getPosition] = null;
            getPosition = (getPosition + 1) % entries;
            tp = nextTp;
            nextTp = timedPositions[getPosition];
        } while (nextTp != null && now >= nextTp.timestamp + delayMilliseconds);
        return tp.position;
    }
}