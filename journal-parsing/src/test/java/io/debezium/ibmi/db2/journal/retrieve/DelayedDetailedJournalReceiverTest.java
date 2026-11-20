/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalStatus;

class DelayedDetailedJournalReceiverTest {
    private AtomicLong time;
    private DelayedDetailedJournalReceiver buffer;

    @BeforeEach
    void setUpClockAndBuffer() {
        time = new AtomicLong();
        buffer = new DelayedDetailedJournalReceiver(2000, 1000, time::get);
    }

    @Test
    void shouldExposeFirstReceiverOnlyWhenConfiguredDelayHasElapsed() {
        assertNull(buffer.getDelayedReceiver());
        var receiver = receiver("FIRST");
        assertTrue(buffer.addDetailedReceiver(receiver));
        assertNull(buffer.getDelayedReceiver());
        time.set(1999);
        assertNull(buffer.getDelayedReceiver());
        time.set(2000);
        assertEquals(receiver, buffer.getDelayedReceiver());
    }

    @Test
    void shouldReturnNewestMatureReceiverWhenSeveralSamplesAreQueued() {
        var first = receiver("FIRST");
        var second = receiver("SECOND");
        var third = receiver("THIRD");
        buffer.addDetailedReceiver(first);
        time.set(1000);
        buffer.addDetailedReceiver(second);
        time.set(2000);
        buffer.addDetailedReceiver(third);
        assertEquals(first, buffer.getDelayedReceiver());
        time.set(2999);
        assertEquals(first, buffer.getDelayedReceiver());
        time.set(3500);
        assertEquals(second, buffer.getDelayedReceiver());
        time.set(4000);
        assertEquals(third, buffer.getDelayedReceiver());
    }

    @Test
    void shouldKeepAcceptedSamplesWhenRapidAdditionIsRejected() {
        var first = receiver("FIRST");
        var rejected = receiver("REJECTED");
        var next = receiver("NEXT");
        assertTrue(buffer.addDetailedReceiver(first));
        assertFalse(buffer.addDetailedReceiver(rejected));
        time.set(1000);
        assertTrue(buffer.addDetailedReceiver(next));
        time.set(2000);
        assertEquals(first, buffer.getDelayedReceiver());
        time.set(2999);
        assertEquals(first, buffer.getDelayedReceiver());
        time.set(3000);
        assertEquals(next, buffer.getDelayedReceiver());
    }

    private static DetailedJournalReceiver receiver(String name) {
        var info = new JournalReceiverInfo(new JournalReceiver(name, "LIB"), new Date(0), JournalStatus.Attached, Optional.empty());
        return new DetailedJournalReceiver(info, BigInteger.ZERO, BigInteger.ZERO, Optional.empty(), 0L, 0L);
    }
}
