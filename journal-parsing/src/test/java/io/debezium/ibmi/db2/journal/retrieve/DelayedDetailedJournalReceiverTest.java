/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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

    @Test
    void testReceiverIsAvailableWithinTimingMargin() {
        DetailedJournalReceiver receiver = createReceiver("MARGIN");
        buffer.addDetailedReceiver(receiver);
        // 20% margin for 2000ms delay is 400ms
        long margin = (long) (2000 * 0.2);
        // Test just before lower bound (should be null)
        fakeTime.addAndGet(1599); // 2000 - 401
        assertNull(buffer.getDelayedReceiver(), "Should be null before margin lower bound");
        // Test at lower bound (should be available)
        fakeTime.addAndGet(1); // Now at 1600ms
        DetailedJournalReceiver resultLower = buffer.getDelayedReceiver();
        assertEquals(receiver, resultLower, "Should return receiver at margin lower bound");
        // Add another receiver for upper bound test
        DetailedJournalReceiver receiver2 = createReceiver("MARGIN2");
        buffer.addDetailedReceiver(receiver2);
        fakeTime.addAndGet(799); // 1600 + 799 = 2399ms
        assertNull(buffer.getDelayedReceiver(), "Should be null just before margin upper bound");
        fakeTime.addAndGet(1); // Now at 2400ms
        DetailedJournalReceiver resultUpper = buffer.getDelayedReceiver();
        assertEquals(receiver2, resultUpper, "Should return receiver at margin upper bound");
    }

    @Test
    void testAddRateAllows20PercentFasterButRetrievalIsStrict() {
        DetailedJournalReceiver receiver1 = createReceiver("FAST1");
        buffer.addDetailedReceiver(receiver1);
        // Try to add another receiver 20% faster than poll interval
        long minInterval = (long) (2000 * (1 - DelayedDetailedJournalReceiver.TIMING_ALLOWANCE_RATIO) + 1); // buffer.entries includes 20% margin + 1 for rounding
        fakeTime.addAndGet(minInterval - 1); // Just before allowed interval
        DetailedJournalReceiver receiver2 = createReceiver("FAST2");
        buffer.addDetailedReceiver(receiver2); // Should be skipped
        // Add after allowed interval
        fakeTime.addAndGet(1); // Now at minInterval
        DetailedJournalReceiver receiver3 = createReceiver("FAST3");
        buffer.addDetailedReceiver(receiver3); // Should be accepted
        // Advance time to allow retrieval
        fakeTime.addAndGet(2000);
        DetailedJournalReceiver result1 = buffer.getDelayedReceiver();
        assertEquals(receiver1, result1, "Should return first receiver after delay");
        DetailedJournalReceiver result2 = buffer.getDelayedReceiver();
        assertEquals(receiver3, result2, "Should return third receiver after delay (second was skipped)");
        assertNull(buffer.getDelayedReceiver(), "No more receivers should be available");
    }
}