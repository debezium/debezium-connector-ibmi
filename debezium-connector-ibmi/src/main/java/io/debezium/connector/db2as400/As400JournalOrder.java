/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/**
 * Orders a caller-verified receiver chain, never receiver names or system timestamps.
 * Bootstrap must verify incarnation, continuity and availability of replay data.
 * Names may include an older W receiver needed only for ordering, not replay reads.
 */
final class As400JournalOrder {
    private final Thread owner = Thread.currentThread();
    private Map<JournalReceiver, Integer> order;
    private Map<JournalReceiver, String> incarnations;
    private Map<JournalReceiver, Range> ranges;

    record Range(BigInteger first, BigInteger last) {
        Range {
            if (first == null || last == null || first.signum() <= 0 || first.compareTo(last) > 0
                    || last.bitLength() > 64) {
                throw new IllegalArgumentException("Invalid receiver sequence range");
            }
        }
    }

    As400JournalOrder(List<JournalReceiver> verifiedChain, Map<JournalReceiver, String> incarnations, Map<JournalReceiver, Range> ranges) {
        if (verifiedChain == null || verifiedChain.isEmpty()) {
            throw new IllegalArgumentException("A verified receiver chain is required");
        }
        Map<JournalReceiver, Integer> positions = new HashMap<>();
        for (JournalReceiver receiver : verifiedChain) {
            As400Checkpoint.validatePosition(new JournalPosition(BigInteger.ONE, receiver));
            if (positions.putIfAbsent(receiver, positions.size()) != null) {
                throw new IllegalArgumentException("Repeated receiver in verified chain");
            }
        }
        order = Map.copyOf(positions);
        this.incarnations = Map.copyOf(incarnations);
        this.ranges = Map.copyOf(ranges);
        if (!order.keySet().equals(this.ranges.keySet()) || !order.keySet().equals(this.incarnations.keySet()) || this.incarnations.values().stream()
                .anyMatch(value -> value.isBlank() || value.length() > 256 || value.startsWith("*"))) {
            throw new IllegalArgumentException("Every verified receiver requires its incarnation");
        }
    }

    int compare(JournalPosition first, JournalPosition second) {
        validate(first);
        validate(second);
        int receivers = Integer.compare(index(first.receiver()), index(second.receiver()));
        return receivers == 0 ? first.offset().compareTo(second.offset()) : receivers;
    }

    String incarnation(JournalPosition position) {
        validate(position);
        return incarnations.get(position.receiver());
    }

    int receiverCount() {
        return order.size();
    }

    /** A separately observed schema outcome cannot extend the streaming reader's verified cut. */
    boolean covers(JournalPosition position, String receiverIdentity) {
        As400Checkpoint.validatePosition(position);
        var range = ranges.get(position.receiver());
        if (range == null) {
            return false;
        }
        if (!incarnations.get(position.receiver()).equals(receiverIdentity) || position.offset().compareTo(range.first()) < 0) {
            throw new IllegalArgumentException("Schema outcome changed a verified receiver identity or range");
        }
        return position.offset().compareTo(range.last()) <= 0;
    }

    /** Single-owner extension from newly verified evidence; never replace or reorder an existing receiver. */
    void extendVerified(As400JournalOrder next, int maximumReceivers) {
        if (Thread.currentThread() != owner) {
            throw new IllegalStateException("Only the reader owner can extend its verified order");
        }
        var oldNames = order.keySet().stream().sorted(java.util.Comparator.comparingInt(order::get)).toList();
        var nextNames = next.order.keySet().stream().sorted(java.util.Comparator.comparingInt(next.order::get)).toList();
        int start = oldNames.indexOf(nextNames.get(0));
        if (start < 0 || nextNames.size() < oldNames.size() - start) {
            throw new IllegalArgumentException("Refreshed evidence must overlap the complete existing tail");
        }
        var updatedOrder = new HashMap<>(order);
        var updatedIdentities = new HashMap<>(incarnations);
        var updatedRanges = new HashMap<>(ranges);
        for (int i = 0; i < nextNames.size(); i++) {
            JournalReceiver name = nextNames.get(i);
            Range incoming = next.ranges.get(name);
            int oldIndex = start + i;
            if (oldIndex < oldNames.size()) {
                Range previous = ranges.get(name);
                if (!oldNames.get(oldIndex).equals(name) || !incarnations.get(name).equals(next.incarnations.get(name))
                        || !previous.first().equals(incoming.first()) || incoming.last().compareTo(previous.last()) < 0
                        || oldIndex < oldNames.size() - 1 && !previous.last().equals(incoming.last())) {
                    throw new IllegalArgumentException("Verified receiver history changed or regressed");
                }
            }
            else {
                if (updatedOrder.containsKey(name) || updatedOrder.size() >= maximumReceivers
                        || incoming.first().compareTo(next.ranges.get(nextNames.get(i - 1)).last()) <= 0) {
                    throw new IllegalArgumentException("Invalid or excessive receiver extension");
                }
                updatedOrder.put(name, updatedOrder.size());
                updatedIdentities.put(name, next.incarnations.get(name));
            }
            updatedRanges.put(name, incoming);
        }
        // Nothing mutates until every overlap and new successor has passed.
        order = Map.copyOf(updatedOrder);
        incarnations = Map.copyOf(updatedIdentities);
        ranges = Map.copyOf(updatedRanges);
    }

    /** Retire only ordering metadata below durable F, retaining the two startup anchors. */
    void refreshVerified(As400JournalOrder next, JournalPosition floor, List<JournalPosition> startupAnchors, int maximumRetained) {
        if (Thread.currentThread() != owner || startupAnchors.size() > 2 || maximumRetained < 1) {
            throw new IllegalArgumentException("Owner and bounded startup anchors required");
        }
        next.validate(floor);
        var nextNames = next.order.keySet().stream().sorted(java.util.Comparator.comparingInt(next.order::get)).toList();
        if (!nextNames.get(0).equals(floor.receiver()) || nextNames.size() > maximumRetained) {
            throw new IllegalArgumentException("Refreshed chain must start at durable F and respect its bound");
        }
        var names = order.keySet().stream().sorted(java.util.Comparator.comparingInt(order::get)).toList();
        var candidate = new As400JournalOrder(names, incarnations, ranges);
        // Validate the entire old overlap before any retirement or mutation.
        candidate.extendVerified(next, Math.addExact(order.size(), maximumRetained));
        startupAnchors.forEach(candidate::validate);
        var pins = startupAnchors.stream().map(JournalPosition::receiver).collect(java.util.stream.Collectors.toSet());
        var kept = candidate.order.keySet().stream().sorted(java.util.Comparator.comparingInt(candidate.order::get))
                .filter(receiver -> candidate.index(receiver) >= candidate.index(floor.receiver()) || pins.contains(receiver)).toList();
        if (kept.size() > maximumRetained + pins.size()) {
            throw new IllegalArgumentException("Retained receiver ordering exceeds its bound");
        }
        var positions = new HashMap<JournalReceiver, Integer>();
        var identities = new HashMap<JournalReceiver, String>();
        var windows = new HashMap<JournalReceiver, Range>();
        for (JournalReceiver receiver : kept) {
            positions.put(receiver, positions.size());
            identities.put(receiver, candidate.incarnations.get(receiver));
            windows.put(receiver, candidate.ranges.get(receiver));
        }
        order = Map.copyOf(positions);
        incarnations = Map.copyOf(identities);
        ranges = Map.copyOf(windows);
    }

    private void validate(JournalPosition position) {
        As400Checkpoint.validatePosition(position);
        index(position.receiver());
        Range range = ranges.get(position.receiver());
        if (position.offset().compareTo(range.first()) < 0 || position.offset().compareTo(range.last()) > 0) {
            throw new IllegalArgumentException("Position is outside the verified receiver window; refresh startup evidence");
        }
    }

    private int index(JournalReceiver receiver) {
        Integer index = order.get(receiver);
        if (index == null) {
            throw new IllegalArgumentException("Receiver is absent from the verified chain: " + receiver);
        }
        return index;
    }
}
