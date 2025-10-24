package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalStatus;

class DelayedDetailedJournalReceiverTest {
    private AtomicLong fakeTime;
    private Supplier<Long> timeSupplier;
    private DelayedDetailedJournalReceiver buffer;

    private DetailedJournalReceiver createReceiver(String id) {
        JournalReceiverInfo info = new JournalReceiverInfo(null, new Date(0), JournalStatus.Attached, Optional.empty());
        return new DetailedJournalReceiver(info, BigInteger.ZERO, BigInteger.ZERO, Optional.empty(), 0L, 0L);
    }

    @BeforeEach
    void setup() {
        fakeTime = new AtomicLong(0);
        timeSupplier = fakeTime::get;
        // 2 second delay, 1000ms poll interval
        buffer = new DelayedDetailedJournalReceiver(2000, 1000, timeSupplier);
    }

    @Test
    void testReceiverIsDelayed() {
        DetailedJournalReceiver receiver = createReceiver("A");
        buffer.addDetailedReceiver(receiver);
        // Should not be available before 2 seconds
        assertNull(buffer.getDelayedReceiver(), "Should be null before delay");
        fakeTime.addAndGet(1999);
        assertNull(buffer.getDelayedReceiver(), "Should still be null just before delay");
        fakeTime.addAndGet(2); // Now at 2001ms
        DetailedJournalReceiver result = buffer.getDelayedReceiver();
        assertEquals(receiver, result, "Should return receiver after delay");
    }

    @Test
    void testMultipleReceiversOrderAndDelay() {
        DetailedJournalReceiver r1 = createReceiver("R1");
        DetailedJournalReceiver r2 = createReceiver("R2");
        buffer.addDetailedReceiver(r1);
        fakeTime.addAndGet(1000); // 1s later
        buffer.addDetailedReceiver(r2);
        // Not enough time for either
        fakeTime.addAndGet(999);
        assertNull(buffer.getDelayedReceiver(), "Should be null before delay for r1");
        fakeTime.addAndGet(2); // Now at 2001ms since r1
        DetailedJournalReceiver result1 = buffer.getDelayedReceiver();
        assertEquals(r1, result1, "Should return r1 after delay");
        assertNull(buffer.getDelayedReceiver(), "Should be null before delay for r2");
        fakeTime.addAndGet(999); // Now at 2000ms since r2
        fakeTime.addAndGet(2); // Now at 2001ms since r2
        DetailedJournalReceiver result2 = buffer.getDelayedReceiver();
        assertEquals(r2, result2, "Should return r2 after delay");
    }

    @Test
    void testAddTooQuicklyIsSkipped() {
        DetailedJournalReceiver r1 = createReceiver("R1");
        DetailedJournalReceiver r2 = createReceiver("R2");
        buffer.addDetailedReceiver(r1);
        // Try to add r2 immediately, should be skipped
        buffer.addDetailedReceiver(r2);
        fakeTime.addAndGet(2001);
        DetailedJournalReceiver result = buffer.getDelayedReceiver();
        assertEquals(r1, result, "Should return r1 after delay");
        assertNull(buffer.getDelayedReceiver(), "Should be null after r1 consumed");
    }

    @Test
    void testNullIfNothingAdded() {
        assertNull(buffer.getDelayedReceiver());
    }
}