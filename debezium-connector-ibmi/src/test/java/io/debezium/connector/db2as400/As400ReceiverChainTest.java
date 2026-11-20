/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400ReceiverChain.Availability;
import io.debezium.connector.db2as400.As400ReceiverChain.Identity;
import io.debezium.connector.db2as400.As400ReceiverChain.Receiver;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400ReceiverChainTest {
    private static final Identity OLD = new Identity(new JournalReceiver("ZOLD", "LIB"), "old-instance");
    private static final Identity MIDDLE = new Identity(new JournalReceiver("MIDDLE", "LIB"), "middle-instance");
    private static final Identity NEW = new Identity(new JournalReceiver("ANEW", "LIB"), "new-instance");
    private static final String JOURNAL = "journal-instance";
    private static final String FINGERPRINT = "v1:" + "a".repeat(64);

    @Test
    void followsExplicitLinksInUnorderedCatalogAndHandlesVerifiedSequenceReset() {
        As400ReceiverChain chain = new As400ReceiverChain(JOURNAL, receivers(), pos(NEW, 10));
        assertThat(chain.order().compare(pos(OLD, 999), pos(NEW, 1))).isNegative();
        assertThat(chain.order().incarnation(pos(MIDDLE, 1))).isEqualTo(MIDDLE.incarnation());
        chain.verify(new As400Checkpoint(JOURNAL, FINGERPRINT, pos(OLD, 900), true, pos(NEW, 2), OLD.incarnation(), NEW.incarnation()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-link", "cycle", "fork", "disconnected", "duplicate", "same-name-recreated", "replacement-link", "journal",
            "unverified-reset", "attached-middle", "no-attached-tail", "wrong-tail", "empty" })
    void rejectsBrokenOrAmbiguousChainsInsteadOfTrimmingToTheNewestSuffix(String failure) {
        List<Receiver> input = new ArrayList<>(receivers());
        JournalPosition tail = pos(NEW, 10);
        switch (failure) {
            case "missing-link" -> input.remove(2); // middle
            case "cycle" -> input.set(0, receiver(NEW, 1, 10, Availability.RETAINED, OLD, false));
            case "fork" -> input.add(receiver(new Identity(new JournalReceiver("FORK", "LIB"), "fork"), 500, 999, Availability.RETAINED, MIDDLE, true));
            case "disconnected" -> input.add(receiver(new Identity(new JournalReceiver("OTHER", "LIB"), "other"), 1, 10, Availability.RETAINED, null, false));
            case "duplicate" -> input.add(input.get(1));
            case "same-name-recreated" -> input.add(receiver(new Identity(OLD.receiver(), "replacement"), 500, 999, Availability.RETAINED, MIDDLE, true));
            case "replacement-link" -> input.set(2, receiver(new Identity(MIDDLE.receiver(), "replacement"), 1, 100, Availability.RETAINED, NEW, true));
            case "journal" ->
                input.set(2, new Receiver(MIDDLE, "another-journal", BigInteger.ONE, BigInteger.valueOf(100), Availability.RETAINED, Optional.of(NEW), true));
            case "unverified-reset" -> input.set(1, receiver(OLD, 500, 999, Availability.RETAINED, MIDDLE, false));
            case "attached-middle" -> input.set(2, receiver(MIDDLE, 1, 100, Availability.ATTACHED, NEW, true));
            case "no-attached-tail" -> input.set(0, receiver(NEW, 1, 10, Availability.RETAINED, null, false));
            case "wrong-tail" -> tail = pos(NEW, 11);
            case "empty" -> input.clear();
            default -> throw new IllegalArgumentException(failure);
        }
        JournalPosition finalTail = tail;
        assertThrows(IllegalArgumentException.class, () -> new As400ReceiverChain(JOURNAL, input, finalTail));
    }

    @ParameterizedTest
    @ValueSource(strings = { "floor-unavailable", "floor-partial", "middle-unavailable", "middle-partial", "floor-outside-range", "completed-outside-range",
            "incarnation" })
    void validatesEveryRequiredReceiverAndBothAnchors(String failure) {
        List<Receiver> input = new ArrayList<>(receivers());
        JournalPosition floor = pos(OLD, 900);
        JournalPosition completed = pos(NEW, 2);
        String floorIdentity = OLD.incarnation();
        switch (failure) {
            case "floor-unavailable" -> input.set(1, receiver(OLD, 500, 999, Availability.UNAVAILABLE, MIDDLE, true));
            case "floor-partial" -> input.set(1, receiver(OLD, 500, 999, Availability.PARTIAL, MIDDLE, true));
            case "middle-unavailable" -> input.set(2, receiver(MIDDLE, 1, 100, Availability.UNAVAILABLE, NEW, true));
            case "middle-partial" -> input.set(2, receiver(MIDDLE, 1, 100, Availability.PARTIAL, NEW, true));
            case "floor-outside-range" -> floor = pos(OLD, 499);
            case "completed-outside-range" -> completed = pos(NEW, 11);
            case "incarnation" -> floorIdentity = "recreated";
            default -> throw new IllegalArgumentException(failure);
        }
        As400ReceiverChain chain = new As400ReceiverChain(JOURNAL, input, pos(NEW, 10));
        As400Checkpoint checkpoint = new As400Checkpoint(JOURNAL, FINGERPRINT, floor, true, completed, floorIdentity, NEW.incarnation());
        assertThrows(IllegalArgumentException.class, () -> chain.verify(checkpoint));
    }

    @Test
    void unsignedRangesAndIdentityAreValidatedWithoutComputingSequenceSuccessors() {
        assertThrows(IllegalArgumentException.class, () -> receiver(OLD, 100, 99, Availability.RETAINED, null, false));
        assertThrows(IllegalArgumentException.class, () -> new Identity(OLD.receiver(), ""));
        assertThrows(IllegalArgumentException.class, () -> new Receiver(OLD, JOURNAL, BigInteger.ONE,
                BigInteger.ONE.shiftLeft(64), Availability.RETAINED, Optional.empty(), false));
        assertThrows(IllegalArgumentException.class, () -> receiver(OLD, 1, 100, Availability.RETAINED, null, true));
    }

    private static List<Receiver> receivers() {
        return List.of(receiver(NEW, 1, 10, Availability.ATTACHED, null, false),
                receiver(OLD, 500, 999, Availability.RETAINED, MIDDLE, true),
                receiver(MIDDLE, 1, 100, Availability.RETAINED, NEW, true));
    }

    private static Receiver receiver(Identity identity, long first, long last, Availability availability, Identity next, boolean reset) {
        return new Receiver(identity, JOURNAL, BigInteger.valueOf(first), BigInteger.valueOf(last), availability, Optional.ofNullable(next), reset);
    }

    private static JournalPosition pos(Identity receiver, long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), receiver.receiver());
    }
}
