/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** One owned cleanup operation. A bounded wait never certifies unfinished native work. */
final class As400AsyncCleanup {
    static final Duration STOP_TIMEOUT = Duration.ofSeconds(30);
    private final String name;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private boolean started;

    interface Operation {
        void run() throws Exception;
    }

    As400AsyncCleanup(String name) {
        this.name = name;
    }

    synchronized void start(Operation operation) {
        if (started) {
            return;
        }
        started = true;
        Thread worker = new Thread(() -> {
            try {
                operation.run();
                completion.complete(null);
            }
            catch (Throwable failure) {
                completion.completeExceptionally(failure);
            }
        }, name);
        worker.setDaemon(true);
        worker.start();
    }

    void await(Duration timeout) {
        try {
            completion.get(Math.max(0, timeout.toNanos()), TimeUnit.NANOSECONDS);
        }
        catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(name + " interrupted; drain unverified", failure);
        }
        catch (TimeoutException failure) {
            throw new IllegalStateException(name + " deadline exceeded; drain unverified, cleanup continues", failure);
        }
        catch (ExecutionException failure) {
            throw new IllegalStateException(name + " failed; drain unverified", failure.getCause());
        }
    }

    /** Only a cleanup worker may wait without a deadline; callers use await(Duration). */
    void awaitInCleanupWorker() {
        completion.join();
    }

    boolean completed() {
        return completion.isDone() && !completion.isCompletedExceptionally();
    }
}
