/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/** Validates explicit incarnation-qualified links. Does not use legacy gap-trimming caches. */
final class As400ReceiverChain {
    enum Availability {
        ATTACHED,
        RETAINED,
        UNAVAILABLE,
        PARTIAL
    }

    record Identity(JournalReceiver receiver, String incarnation) {
        Identity {
            As400Checkpoint.validatePosition(new JournalPosition(BigInteger.ONE, receiver));
            token(incarnation);
        }
    }

    /**
     * Metadata for one source incarnation. next and sequenceReset come from source
     * linkage, not inferred names, attach timestamps or sequence adjacency.
     * Ranges describe the complete receiver at the inspection high-water mark.
     */
    record Receiver(Identity identity, String journalIdentity, BigInteger first, BigInteger last,
            Availability availability, Optional<Identity> next, boolean sequenceReset) {
        Receiver {
            Objects.requireNonNull(identity);
            token(journalIdentity);
            As400Checkpoint.validatePosition(new JournalPosition(first, identity.receiver()));
            As400Checkpoint.validatePosition(new JournalPosition(last, identity.receiver()));
            if (first.compareTo(last) > 0) {
                throw new IllegalArgumentException("Invalid receiver range");
            }
            Objects.requireNonNull(availability);
            Objects.requireNonNull(next);
            if (next.isEmpty() && sequenceReset) {
                throw new IllegalArgumentException("Sequence reset requires a successor");
            }
        }
    }

    private final List<Receiver> chain;
    private final Map<JournalReceiver, Receiver> receivers;
    private final As400JournalOrder order;

    As400ReceiverChain(String journalIdentity, List<Receiver> observed, JournalPosition tail) {
        token(journalIdentity);
        if (observed.isEmpty() || observed.size() > 100_000) {
            throw new IllegalArgumentException("Missing or excessive receiver metadata");
        }
        Map<JournalReceiver, Receiver> byName = new HashMap<>();
        Set<Identity> identities = new HashSet<>();
        Set<Identity> successors = new HashSet<>();
        for (Receiver receiver : observed) {
            if (!receiver.journalIdentity().equals(journalIdentity) || !identities.add(receiver.identity())
                    || byName.putIfAbsent(receiver.identity().receiver(), receiver) != null) {
                throw new IllegalArgumentException("Mixed journal or repeated/recreated receiver");
            }
            if (receiver.next().isPresent() && !successors.add(receiver.next().orElseThrow())) {
                throw new IllegalArgumentException("Forked receiver chain");
            }
        }
        List<Receiver> heads = observed.stream().filter(r -> !successors.contains(r.identity())).toList();
        if (heads.size() != 1) {
            throw new IllegalArgumentException("Disconnected or cyclic receiver chain");
        }
        List<Receiver> ordered = new ArrayList<>();
        Set<Identity> visited = new HashSet<>();
        Receiver current = heads.get(0);
        while (true) {
            if (!visited.add(current.identity())) {
                throw new IllegalArgumentException("Cyclic receiver chain");
            }
            ordered.add(current);
            if (current.next().isEmpty()) {
                break;
            }
            if (current.availability() == Availability.ATTACHED) {
                throw new IllegalArgumentException("Attached receiver is not the chain tail");
            }
            Receiver next = byName.get(current.next().orElseThrow().receiver());
            if (next == null || !next.identity().equals(current.next().orElseThrow())) {
                throw new IllegalArgumentException("Missing or replaced linked receiver");
            }
            if (!current.sequenceReset() && next.first().compareTo(current.last()) <= 0) {
                throw new IllegalArgumentException("Unverified journal sequence reset");
            }
            current = next;
        }
        if (visited.size() != observed.size() || current.availability() != Availability.ATTACHED
                || !new JournalPosition(current.last(), current.identity().receiver()).equals(tail)) {
            throw new IllegalArgumentException("Incomplete chain or inconsistent attached high-water mark");
        }
        chain = List.copyOf(ordered);
        receivers = Map.copyOf(byName);
        Map<JournalReceiver, String> incarnations = new HashMap<>();
        Map<JournalReceiver, As400JournalOrder.Range> ranges = new HashMap<>();
        chain.forEach(r -> {
            incarnations.put(r.identity().receiver(), r.identity().incarnation());
            ranges.put(r.identity().receiver(), new As400JournalOrder.Range(r.first(), r.last()));
        });
        order = new As400JournalOrder(chain.stream().map(r -> r.identity().receiver()).toList(), incarnations, ranges);
    }

    As400JournalOrder order() {
        return order;
    }

    void verify(As400Checkpoint checkpoint) {
        verifyPosition(checkpoint.replayFrom(), checkpoint.replayReceiverIdentity());
        verifyPosition(checkpoint.completedThrough(), checkpoint.completedReceiverIdentity());
        int floor = index(checkpoint.replayFrom().receiver());
        // Even an exclusive floor requires the receiver anchor, not a guessed successor.
        for (Receiver receiver : chain.subList(floor, chain.size())) {
            if (receiver.availability() != Availability.RETAINED && receiver.availability() != Availability.ATTACHED) {
                throw new IllegalArgumentException("Required replay receiver is unavailable or partial");
            }
        }
    }

    void verifyPosition(JournalPosition position, String incarnation) {
        As400Checkpoint.validatePosition(position);
        Receiver receiver = receivers.get(position.receiver());
        if (receiver == null || !receiver.identity().incarnation().equals(incarnation)
                || position.offset().compareTo(receiver.first()) < 0 || position.offset().compareTo(receiver.last()) > 0) {
            throw new IllegalArgumentException("Position is outside its retained receiver incarnation/range");
        }
    }

    private int index(JournalReceiver receiver) {
        for (int i = 0; i < chain.size(); i++) {
            if (chain.get(i).identity().receiver().equals(receiver)) {
                return i;
            }
        }
        throw new IllegalArgumentException("Unknown receiver");
    }

    static void token(String value) {
        if (value == null || value.isBlank() || value.length() > 256 || value.startsWith("*")) {
            throw new IllegalArgumentException("Missing source incarnation/evidence token");
        }
    }
}
