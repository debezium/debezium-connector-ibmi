/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WatchDog implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(As400StreamingChangeEventSource.class);

    private volatile long lastSeen = System.currentTimeMillis();
    private final Thread notify;
    private volatile boolean running = true;
    private final long wait;
    private volatile Thread watchDogThread;

    public WatchDog(Thread notify, long wait) {
        this.notify = notify;
        this.wait = wait;
    }

    @Override
    public void run() {
        watchDogThread = Thread.currentThread();
        while (running) {
            try {
                Thread.sleep(wait);
                long now = System.currentTimeMillis();
                if (now - lastSeen > wait) {
                    log.warn("No update since {} interrupting streaming thread {}", new Date(lastSeen), notify.getName());
                    notify.interrupt();
                }
            }
            catch (InterruptedException e) {
                log.info("Interrupted is shuttingdown: {}", !running);
            }
        }

    }

    public void start() {
        lastSeen = System.currentTimeMillis();
        new Thread(this, "watchdog_" + notify.getName()).start();
    }

    public void stop() {
        this.running = false;
        if (watchDogThread != null) {
            watchDogThread.interrupt();
        }
    }

    public void alive() {
        lastSeen = System.currentTimeMillis();
    }
}
