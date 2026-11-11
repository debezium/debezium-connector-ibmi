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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400NativeJournalWindow.Sample;
import io.debezium.connector.db2as400.As400NativeJournalWindow.Window;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Receiver;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400NativeJournalWindowTest {
    private static final JournalReceiver JOURNAL = new JournalReceiver("JRN", "LIB");

    @ParameterizedTest
    @ValueSource(ints = { 17, 78, 128 })
    void shouldAcceptRequiredRetainedChainsBeyondSixteenReceivers(int count) {
        var sample = sample(count, "ONLINE");
        var from = new JournalPosition(BigInteger.ONE, receiver(0));
        var through = new JournalPosition(BigInteger.valueOf(count * 10L), receiver(count - 1));
        assertThat(new Window(from, through, sample, sample).receiverTokens()).hasSize(count);
    }

    @Test
    void shouldIgnoreUnneededOlderDirectoryReceiversWithoutWeakeningRequiredIdentityChecks() {
        var current = sample(1, "ONLINE");
        var directory = new ArrayList<>(current.directory().receivers());
        for (int i = 1; i < 689; i++) {
            directory.add(new Receiver(receiver(i), "ONLINE", BigInteger.ONE, BigInteger.TEN, Optional.empty(), Optional.empty(),
                    "YES", "NO", "NO", "NO", "NO"));
        }
        var observed = new Sample(new Observation(JOURNAL, receiver(0), directory), current.identities());
        var from = new JournalPosition(BigInteger.ONE, receiver(0));
        var through = new JournalPosition(BigInteger.TEN, receiver(0));
        assertThat(new Window(from, through, observed, current).receiverTokens()).containsOnlyKeys(receiver(0));
        assertThrows(IllegalArgumentException.class, () -> new Window(from, through, observed,
                new Sample(current.directory(), new Candidates(Map.of()))));
    }

    @Test
    void shouldAcceptReceiverSaveButRejectUnavailableOrReplacedHistory() {
        var before = sample(2, "ONLINE");
        var saved = sample(2, "SAVED");
        var from = new JournalPosition(BigInteger.ONE, receiver(0));
        var through = new JournalPosition(BigInteger.valueOf(20), receiver(1));
        assertThat(new Window(from, through, before, saved).receiverTokens()).hasSize(2);
        As400NativeJournalWindow.requireStableDirectory(before, saved);
        assertThrows(IllegalArgumentException.class, () -> new Window(from, through, before, sample(2, "FREED")));
        var objects = new HashMap<>(saved.identities().objects());
        objects.put(As400NativeJournalWindow.target(receiver(0), Kind.RECEIVER), new Identity(BigInteger.ONE, BigInteger.TWO, BigInteger.TWO));
        assertThrows(IllegalArgumentException.class, () -> new Window(from, through, before, new Sample(saved.directory(), new Candidates(objects))));
    }

    @Test
    void shouldDiscardACutWhenTheReceiverRotatesAndAcceptAFreshObservation() {
        var before = sample(1, "ONLINE");
        var after = sample(2, "ONLINE");
        var from = new JournalPosition(BigInteger.ONE, receiver(0));
        var through = new JournalPosition(BigInteger.TEN, receiver(0));
        assertThrows(As400OperatorCutReader.UnstableCutException.class, () -> new Window(from, through, before, after));
        assertThat(new Window(from, through, after, after).receiverTokens()).hasSize(2);
        assertThrows(As400OperatorCutReader.UnstableCutException.class, () -> As400NativeJournalWindow.requireStableDirectory(before, after));
    }

    private static Sample sample(int count, String detachedStatus) {
        List<Receiver> rows = new ArrayList<>();
        Map<Target, Identity> objects = new HashMap<>();
        objects.put(As400NativeJournalWindow.target(JOURNAL, Kind.JOURNAL), new Identity(BigInteger.ONE, BigInteger.ONE, BigInteger.ONE));
        for (int i = 0; i < count; i++) {
            rows.add(new Receiver(receiver(i), i == count - 1 ? "ATTACHED" : detachedStatus,
                    BigInteger.valueOf(i * 10L + 1), BigInteger.valueOf((i + 1L) * 10),
                    i == 0 ? Optional.empty() : Optional.of(receiver(i - 1)),
                    i == count - 1 ? Optional.empty() : Optional.of(receiver(i + 1)), "NO", "YES", "YES", "NO", "NO"));
            objects.put(As400NativeJournalWindow.target(receiver(i), Kind.RECEIVER), new Identity(BigInteger.ONE, BigInteger.valueOf(i + 2), BigInteger.ONE));
        }
        return new Sample(new Observation(JOURNAL, receiver(count - 1), rows), new Candidates(objects));
    }

    private static JournalReceiver receiver(int i) {
        return new JournalReceiver("RCV" + i, "LIB");
    }
}
