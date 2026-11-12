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

/** Ordered delivery barrier. Kafka transactions and source-offset readback have separate contracts. */
final class As400RecoveryAcknowledgements {
    static final String DELIVERY_HEADER = "__debezium.ibmi.delivery";
    private final String deliverySession = java.util.UUID.randomUUID().toString();
    private long deliveryOrdinal;
    private final Map<SourceRecord, String> pending = new IdentityHashMap<>();
    private final Map<SourceRecord, String> durable = new IdentityHashMap<>();
    private final List<SourceRecord> order = new ArrayList<>();
    private final Set<SourceRecord> snapshotChunks = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Map<SourceRecord, SourceRecord> delivered = new IdentityHashMap<>();
    private final Map<SourceRecord, SourceRecord> originals = new IdentityHashMap<>();
    private final Map<String, SourceRecord> routedDeliveries = new java.util.HashMap<>();
    private final boolean sourceOffsets;
    private final As400TopicRouting routing;
    private final As400DeliveryOffset.Signer signer;
    private Map<String, ?> lastCommittedOffset;
    private long committedOrdinal;
    private long deliveredOrdinal;
    private int contiguousDeliveries;
    private boolean streaming;
    private boolean stopped;
    private RuntimeException failure;

    As400RecoveryAcknowledgements() {
        this(As400TopicRouting.from(Map.of()));
    }

    As400RecoveryAcknowledgements(As400TopicRouting routing) {
        sourceOffsets = false;
        signer = null;
        this.routing = routing;
    }

    As400RecoveryAcknowledgements(Map<String, ?> initialOffset) {
        this(initialOffset, As400TopicRouting.from(Map.of()));
    }

    As400RecoveryAcknowledgements(Map<String, ?> initialOffset, As400TopicRouting routing) {
        sourceOffsets = true;
        this.routing = routing;
        signer = new As400DeliveryOffset.Signer(deliverySession);
        As400DeliveryOffset.strip(initialOffset);
        lastCommittedOffset = initialOffset == null ? null : Map.copyOf(initialOffset);
    }

    static void requireSourceOffsetProfile(Map<String, String> properties) {
        As400TopicRouting.from(properties);
        if (!"none".equals(properties.getOrDefault("errors.tolerance", "none"))
                || !conversionPolicyAllowed(properties)
                || !"ORDERED".equals(properties.getOrDefault("record.processing.order", "ORDERED"))) {
            throw new IllegalArgumentException("Ordered source-offset delivery without tolerated errors required");
        }
    }

    static void requireProfile(Map<String, String> properties) {
        requireProfile(properties, "poll");
    }

    static void requireConnectorProfile(Map<String, String> properties) {
        requireProfile(properties, "connector");
    }

    private static void requireProfile(Map<String, String> properties, String boundary) {
        As400TopicRouting.from(properties);
        if (!"requested".equals(properties.get("exactly.once.support")) || !boundary.equals(properties.get("transaction.boundary"))
                || !"none".equals(properties.getOrDefault("errors.tolerance", "none"))
                || !conversionPolicyAllowed(properties)) {
            throw new IllegalArgumentException("Qualified test EOS transaction profile without tolerated errors required");
        }
        // Worker EOS enablement/version is independently fixed and recorded by the
        // launcher. These connector settings alone do NOT qualify another runtime.
    }

    private static boolean conversionPolicyAllowed(Map<String, String> properties) {
        // A source column conversion policy is distinct from tolerating a failed
        // Kafka Converter/SMT or producer send. Those still cannot acknowledge F/W.
        return "true".equals(properties.get("truncate.transactional.enabled"))
                ? java.util.Set.of("fail", "warn", "skip").contains(properties.getOrDefault("event.converting.failure.handling.mode", "warn"))
                : "fail".equals(properties.get("event.converting.failure.handling.mode"));
    }

    synchronized int pendingCount() {
        return order.size();
    }

    /** Initialization/handoff keep their durable barriers; only steady streaming can release on delivery. */
    synchronized void startStreaming() {
        requireActive();
        if (!sourceOffsets || !order.isEmpty() || deliveredOrdinal != committedOrdinal) {
            throw new IllegalStateException("Streaming delivery requires a fully acknowledged startup boundary");
        }
        streaming = true;
    }

    synchronized Map<String, ?> committedOffset() {
        requireActive();
        return As400DeliveryOffset.strip(lastCommittedOffset);
    }

    synchronized SourceRecord register(SourceRecord record) {
        return register(record, false);
    }

    synchronized SourceRecord register(SourceRecord record, boolean snapshotChunk) {
        requireActive();
        if (routing.predictable()) {
            routing.topic(record.topic()); // Reject invalid replacements before queue admission.
        }
        if (order.size() >= 10000 || pending.containsKey(record) || durable.containsKey(record)) {
            throw new IllegalStateException("Duplicate acknowledgement registration");
        }
        if (record.headers().lastWithName(DELIVERY_HEADER) != null) {
            throw new IllegalStateException("Recovery delivery header is reserved");
        }
        // Connect's EOS worker keys acknowledgements by SourceRecord.equals.
        // Repeated same-key tombstones with a frozen F/W must remain distinct.
        long sequence = Math.incrementExact(deliveryOrdinal);
        deliveryOrdinal = sequence;
        // The legacy dispatcher can share Headers between a delete and its
        // tombstone. Own the published record in both delivery contracts: adding
        // our marker to borrowed headers corrupts the next registration.
        SourceRecord published = new SourceRecord(record.sourcePartition(), sourceOffsets ? signer.stamp(record.sourceOffset(), sequence) : record.sourceOffset(),
                record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), record.value(),
                record.timestamp(), record.headers().duplicate());
        if (!sourceOffsets) {
            published.headers().addString(DELIVERY_HEADER, deliverySession + ":" + sequence);
        }
        delivered.put(record, published);
        originals.put(published, record);
        if (sourceOffsets && !routing.settings().isEmpty()) {
            routedDeliveries.put(Long.toString(sequence), record);
        }
        pending.put(record, null);
        order.add(record);
        if (snapshotChunk) {
            snapshotChunks.add(record);
        }
        return published;
    }

    synchronized boolean snapshotChunk(SourceRecord record) {
        requireActive();
        return snapshotChunks.contains(original(record));
    }

    private SourceRecord original(SourceRecord record) {
        SourceRecord original = originals.get(record);
        if (original == null && sourceOffsets && !routedDeliveries.isEmpty() && record.sourceOffset() != null) {
            // Engine returns the transformed SourceRecord, whereas Connect calls
            // commitRecord with the original. The complete source partition and
            // signed offset must match the registered delivery. Configured SMTs
            // may change keys, values, headers and topics; the worker owns those
            // operations. A pure routing chain can be checked more strictly.
            var candidate = routedDeliveries.get(record.sourceOffset().get(As400DeliveryOffset.SEQUENCE));
            var published = delivered.get(candidate);
            if (published != null && record.sourcePartition().equals(published.sourcePartition()) && record.sourceOffset().equals(published.sourceOffset())
                    && (!routing.predictable() || record.equals(published.newRecord(routing.topic(published.topic()), published.kafkaPartition(),
                            published.keySchema(), published.key(), published.valueSchema(), published.value(), published.timestamp(), published.headers())))) {
                original = candidate;
            }
        }
        return original == null ? record : original;
    }

    synchronized boolean isDeliveryOf(SourceRecord published, SourceRecord original) {
        return published != null && delivered.get(original) == published;
    }

    synchronized void record(SourceRecord record, RecordMetadata metadata) {
        requireActive();
        SourceRecord original = original(record);
        if (!pending.containsKey(original) || original == record
                || metadata == null && !sourceOffsets && routing.predictable() || metadata != null && (!metadata.hasOffset() || metadata.partition() < 0
                        || metadata.topic() == null || metadata.topic().isEmpty()
                        || routing.predictable() && (record.kafkaPartition() != null && metadata.partition() != record.kafkaPartition()
                                || !metadata.topic().equals(routing.topic(delivered.get(original).topic()))))
                || pending.get(original) != null) {
            failure = new IllegalStateException("Dropped, unexpected or mismatched snapshot/offset acknowledgement");
            notifyAll();
            throw failure;
        }
        // With errors.tolerance=none a missing Kafka receipt on the standard
        // transform path is an intentional filter. EOS reports that processed
        // prefix after a successful transaction commit; a retained progress
        // record must still carry the checkpoint into Kafka's offset writer.
        pending.put(original, metadata == null ? "source-processed" : "kafka:" + metadata.topic() + ":" + metadata.partition() + ":" + metadata.offset());
        if (sourceOffsets) {
            while (contiguousDeliveries < order.size()) {
                SourceRecord next = order.get(contiguousDeliveries);
                if (pending.get(next) == null && !durable.containsKey(next)) {
                    break;
                }
                deliveredOrdinal = Long.parseLong((String) delivered.get(next).sourceOffset().get(As400DeliveryOffset.SEQUENCE));
                contiguousDeliveries++;
            }
            notifyAll();
        }
    }

    synchronized void committed() {
        requireActive();
        if (sourceOffsets) {
            throw new IllegalStateException("Source-offset delivery requires offset-store readback");
        }
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

    /** A flush may finish while newer delivery callbacks arrive. Promote only its exact saved prefix. */
    synchronized void committed(Map<String, ?> saved) {
        requireActive();
        if (!sourceOffsets) {
            committed();
            return;
        }
        try {
            As400DeliveryOffset.strip(saved);
            if (java.util.Objects.equals(lastCommittedOffset, saved)) {
                return; // No-op commit, or an offset flush that did not include any new record.
            }
            long savedOrdinal = signer.verify(saved);
            if (savedOrdinal <= committedOrdinal || savedOrdinal > deliveredOrdinal) {
                throw new IllegalStateException("Durable delivery offset regressed or skipped an unacknowledged record");
            }
            if (streaming) {
                // The receipt proves the exact emitted F/W after the record itself
                // has been released. Only the contiguous delivered prefix qualifies.
                lastCommittedOffset = Map.copyOf(saved);
                committedOrdinal = savedOrdinal;
                return;
            }
            int end = -1;
            for (int i = 0; i < order.size(); i++) {
                if (delivered.get(order.get(i)).sourceOffset().equals(saved)) {
                    end = i;
                    break;
                }
            }
            if (end < 0) {
                throw new IllegalStateException("Durable source offset is not a registered delivery prefix");
            }
            // Validate the entire prefix before changing any state.
            for (SourceRecord record : order.subList(0, end + 1)) {
                if (!durable.containsKey(record) && pending.get(record) == null) {
                    throw new IllegalStateException("Durable source offset skipped an unacknowledged record");
                }
            }
            for (SourceRecord record : order.subList(0, end + 1)) {
                if (pending.containsKey(record)) {
                    pending.remove(record);
                    durable.put(record, "source-offset:" + deliverySession + ":" + delivered.get(record).sourceOffset().get(As400DeliveryOffset.SEQUENCE));
                }
            }
            lastCommittedOffset = Map.copyOf(saved);
            committedOrdinal = savedOrdinal;
            notifyAll();
        }
        catch (RuntimeException error) {
            failure = error;
            notifyAll();
            throw error;
        }
    }

    /** No-op commits must not create durability for registered but unacknowledged records. */
    synchronized boolean hasRecorded() {
        requireActive();
        return sourceOffsets && deliveredOrdinal > committedOrdinal || pending.values().stream().anyMatch(java.util.Objects::nonNull);
    }

    /** Bounded output backpressure, independent of the framework's offset flush interval. */
    synchronized void awaitDeliveredAndRetire(SourceRecord barrier, Runnable check) throws InterruptedException {
        requireActive();
        if (!streaming || !delivered.containsKey(barrier)) {
            throw new IllegalStateException("Unregistered streaming delivery barrier");
        }
        long ordinal = Long.parseLong((String) delivered.get(barrier).sourceOffset().get(As400DeliveryOffset.SEQUENCE));
        while (deliveredOrdinal < ordinal) {
            check.run();
            requireActive();
            wait(100);
        }
        check.run();
        requireActive();
        int count = 0;
        while (order.get(count++) != barrier) {
            // Identity matters for equal same-key tombstones with frozen F/W.
        }
        retire(count);
    }

    synchronized String await(SourceRecord record, Duration timeout) throws InterruptedException {
        return await(record, timeout, () -> {
        });
    }

    synchronized String await(SourceRecord record, Duration timeout, Runnable check) throws InterruptedException {
        return awaitInternal(record, timeout, check);
    }

    /** Paused or backpressured streaming has no elapsed-time durability failure. */
    synchronized String awaitUntilStopped(SourceRecord record, Runnable check) throws InterruptedException {
        return awaitInternal(record, null, check);
    }

    private String awaitInternal(SourceRecord record, Duration timeout, Runnable check) throws InterruptedException {
        long end = timeout == null ? 0 : System.nanoTime() + timeout.toNanos();
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
            long remaining = timeout == null ? 100_000_000L : end - System.nanoTime();
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
        retire(count);
    }

    private void retire(int count) {
        for (SourceRecord record : order.subList(0, count)) {
            pending.remove(record);
            durable.remove(record);
            snapshotChunks.remove(record);
            SourceRecord published = delivered.remove(record);
            originals.remove(published);
            if (sourceOffsets) {
                routedDeliveries.remove(published.sourceOffset().get(As400DeliveryOffset.SEQUENCE));
            }
        }
        order.subList(0, count).clear();
        if (sourceOffsets) {
            contiguousDeliveries -= count;
        }
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
