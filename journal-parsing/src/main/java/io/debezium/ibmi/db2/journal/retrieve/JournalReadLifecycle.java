/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.io.IOException;

/**
 * Terminal admission and drain barrier, shared by one RPC connection and reader.
 * An admitted operation can finish after stop is requested. Stop is complete only
 * after all leases have closed; checking the flag alone is not that barrier.
 */
public final class JournalReadLifecycle {
    private volatile boolean stopped;
    private int admitted;

    public Lease enter() throws IOException {
        synchronized (this) {
            checkOpen();
            admitted++;
            return new Lease();
        }
    }

    public void checkOpen() throws IOException {
        if (stopped) {
            throw new IOException("Journal reader is terminally stopped");
        }
    }

    public boolean isStopped() {
        return stopped;
    }

    public synchronized void requestStop() {
        stopped = true;
    }

    /**
     * Call from the stopping thread, never from inside a lease. Cancellation must
     * happen before waiting and without this monitor. There is no native deadline
     * guarantee: an unresponsive driver leaves stop pending, not falsely complete.
     * Interruption does not waive the barrier; the interrupt status is restored.
     */
    public synchronized void awaitDrained() {
        boolean interrupted = false;
        while (admitted != 0) {
            try {
                wait();
            }
            catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public final class Lease implements AutoCloseable {
        private boolean closed;

        private Lease() {
        }

        @Override
        public void close() {
            synchronized (JournalReadLifecycle.this) {
                if (!closed) {
                    closed = true;
                    admitted--;
                    JournalReadLifecycle.this.notifyAll();
                }
            }
        }
    }
}
