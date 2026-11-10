/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class WatchDogTest {
    @Test
    void shouldInterruptBlockedWorkerWhenUpdatesStop() throws Exception {
        var ready = new CountDownLatch(1);
        var interrupted = new CountDownLatch(1);
        var worker = new Thread(() -> {
            ready.countDown();
            try {
                new CountDownLatch(1).await();
            }
            catch (InterruptedException expected) {
                interrupted.countDown();
            }
        }, "watchdog-test-worker");
        var watchdog = new WatchDog(worker, 10);
        var monitor = new Thread(watchdog, "watchdog-test-monitor");
        worker.setDaemon(true);
        monitor.setDaemon(true);
        try {
            worker.start();
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            monitor.start();
            assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
        }
        finally {
            watchdog.stop();
            worker.interrupt();
            monitor.join(5000);
            worker.join(5000);
        }
        assertThat(monitor.isAlive()).isFalse();
        assertThat(worker.isAlive()).isFalse();
    }
}
