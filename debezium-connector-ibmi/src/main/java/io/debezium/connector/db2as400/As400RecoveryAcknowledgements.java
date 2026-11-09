/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;

/** Ordered commit barrier used by admitted transactional Kafka Connect source tasks. */
final class As400RecoveryAcknowledgements {
    private final Map<SourceRecord, String> pending = new IdentityHashMap<>();
    private final Map<SourceRecord, String> durable = new IdentityHashMap<>();
    private final List<SourceRecord> order = new ArrayList<>();
    private final Set<SourceRecord> snapshotChunks = Collections.newSetFromMap(new IdentityHashMap<>());
    private boolean stopped;
    private RuntimeException failure;

    static void requireProfile(Map<String, String> properties) {
        if (!"requested".equals(properties.get("exactly.once.support")) || !"poll".equals(properties.get("transaction.boundary"))
                || !"none".equals(properties.get("errors.tolerance")) || !properties.getOrDefault("transforms", "").isEmpty()
                || !"fail".equals(properties.get("event.converting.failure.handling.mode"))) {
            throw new IllegalArgumentException("Qualified test EOS/poll profile without filtering or tolerated errors required");
        }
        // Worker EOS enablement/version is independently fixed and recorded by the
        // launcher. These connector settings alone do NOT qualify another runtime.
    }

    synchronized void register(SourceRecord record) {
        register(record, false);
    }

    synchronized void register(SourceRecord record, boolean snapshotChunk) {
        requireActive();
        if (order.size() >= 10000 || pending.containsKey(record) || durable.containsKey(record)) {
            throw new IllegalStateException("Duplicate acknowledgement registration");
        }
        pending.put(record, null);
        order.add(record);
        if (snapshotChunk) {
            snapshotChunks.add(record);
        }
    }

    synchronized boolean snapshotChunk(SourceRecord record) {
        requireActive();
        return snapshotChunks.contains(record);
    }

    synchronized void record(SourceRecord record, RecordMetadata metadata) {
        requireActive();
        if (!pending.containsKey(record) || metadata == null || !metadata.hasOffset() || metadata.partition() != 0
                || !metadata.topic().equals(record.topic()) || pending.get(record) != null) {
            failure = new IllegalStateException("Dropped, unexpected or mismatched snapshot/offset acknowledgement");
            notifyAll();
            throw failure;
        }
        pending.put(record, "kafka:" + metadata.topic() + ":" + metadata.partition() + ":" + metadata.offset());
    }

    synchronized void committed() {
        requireActive();
        // ExactlyOnceWorkerSourceTask calls commitRecord and then commit only
        // after producer.commitTransaction (records AND source offsets) succeeds.
        // Ordinary SourceTask callback semantics alone do not establish this.
        pending.entrySet().removeIf(entry -> {
            if (entry.getValue() != null) {
                durable.put(entry.getKey(), entry.getValue());
                return true;
            }
            return false;
        });
        notifyAll();
    }

    /** No-op commits must not create durability for registered but unacknowledged records. */
    synchronized boolean hasRecorded() {
        requireActive();
        return pending.values().stream().anyMatch(java.util.Objects::nonNull);
    }

    synchronized String await(SourceRecord record, Duration timeout) throws InterruptedException {
        return await(record, timeout, () -> {
        });
    }

    synchronized String await(SourceRecord record, Duration timeout, Runnable check) throws InterruptedException {
        long end = System.nanoTime() + timeout.toNanos();
        while (true) {
            check.run();
            requireActive();
            String receipt = durable.get(record);
            if (receipt != null) {
                for (SourceRecord earlier : order) {
                    if (earlier == record) {
                        return receipt;
                    }
                    if (!durable.containsKey(earlier)) {
                        failure = new IllegalStateException("Acknowledgement barrier skipped an earlier record");
                        throw failure;
                    }
                }
            }
            if (!pending.containsKey(record)) {
                throw new IllegalStateException("Unregistered acknowledgement barrier");
            }
            long remaining = end - System.nanoTime();
            if (remaining <= 0) {
                throw new IllegalStateException("Uncertain durable acknowledgement");
            }
            wait(Math.max(1, Math.min(100, remaining / 1_000_000)));
        }
    }

    /** Continuous callers release memory only after an acknowledged barrier and independent offset readback. */
    synchronized void retireDurableThrough(SourceRecord barrier) {
        requireActive();
        int count = 0;
        boolean found = false;
        for (SourceRecord record : order) {
            if (!durable.containsKey(record)) {
                throw new IllegalStateException("Cannot retire an incomplete acknowledgement prefix");
            }
            count++;
            if (record == barrier) {
                found = true;
                break;
            }
        }
        if (!found) {
            throw new IllegalStateException("Unregistered acknowledgement retirement");
        }
        for (SourceRecord record : order.subList(0, count)) {
            durable.remove(record);
            snapshotChunks.remove(record);
        }
        order.subList(0, count).clear();
    }

    synchronized void stop() {
        stopped = true;
        notifyAll();
    }

    private void requireActive() {
        if (stopped) {
            throw new IllegalStateException("Acknowledgement session stopped; drain is separate");
        }
        if (failure != null) {
            throw failure;
        }
    }
}
