/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400JournalOrderExtensionTest {
    private static final JournalReceiver A = new JournalReceiver("A", "LIB");
    private static final JournalReceiver B = new JournalReceiver("B", "LIB");

    @Test
    void verifiedGrowthAndSuccessorKeepEarlierPositionsAvailableForBufferedTransactions() {
        var order = single(A, "a", 1, 10);
        order.extendVerified(single(A, "a", 1, 20), 2);
        var rotated = new As400JournalOrder(List.of(A, B), Map.of(A, "a", B, "b"), Map.of(A, range(1, 22), B, range(23, 30)));
        order.extendVerified(rotated, 2);
        order.extendVerified(single(B, "b", 23, 40), 2);
        assertThat(order.compare(pos(A, 1), pos(B, 40))).isNegative();
        assertThat(order.incarnation(pos(A, 22))).isEqualTo("a");
        assertThat(order.incarnation(pos(B, 40))).isEqualTo("b");
    }

    @ParameterizedTest
    @ValueSource(strings = { "identity", "shrink", "first", "missing-overlap", "excess", "sealed-growth", "reset" })
    void changedHistoryCannotPartiallyExtendExistingOrder(String fault) {
        var order = single(A, "a", 1, 10);
        if (fault.equals("sealed-growth")) {
            order.extendVerified(new As400JournalOrder(List.of(A, B), Map.of(A, "a", B, "b"), Map.of(A, range(1, 10), B, range(11, 20))), 2);
        }
        var incoming = switch (fault) {
            case "identity" -> single(A, "changed", 1, 30);
            case "shrink" -> single(A, "a", 1, 9);
            case "first" -> single(A, "a", 2, 30);
            case "missing-overlap" -> single(B, "b", 11, 30);
            case "sealed-growth" -> new As400JournalOrder(List.of(A, B), Map.of(A, "a", B, "b"), Map.of(A, range(1, 11), B, range(11, 30)));
            case "reset" -> new As400JournalOrder(List.of(A, B), Map.of(A, "a", B, "b"), Map.of(A, range(1, 10), B, range(1, 30)));
            default -> new As400JournalOrder(List.of(A, B), Map.of(A, "a", B, "b"), Map.of(A, range(1, 10), B, range(11, 30)));
        };
        assertThrows(IllegalArgumentException.class, () -> order.extendVerified(incoming, fault.equals("excess") ? 1 : 2));
        assertThat(order.incarnation(pos(A, 10))).isEqualTo("a");
        assertThrows(IllegalArgumentException.class, () -> order.incarnation(pos(A, 11)));
    }

    @Test
    void arbitraryLifetimeRotationsRetainOnlyLiveChainAndStartupAnchors() {
        var order = single(A, "a", 1, 10);
        var previous = A;
        for (int i = 1; i <= 1000; i++) {
            var next = new JournalReceiver("R" + i, "LIB");
            long first = (i - 1L) * 10 + 1;
            var chain = new As400JournalOrder(List.of(previous, next), Map.of(previous, i == 1 ? "a" : "i" + (i - 1), next, "i" + i),
                    Map.of(previous, range(first, first + 9), next, range(first + 10, first + 19)));
            order.refreshVerified(chain, pos(previous, first), List.of(pos(A, 1), pos(A, 10)), 2);
            assertThat(order.compare(pos(A, 1), pos(next, first + 10))).isNegative();
            if (i > 2) {
                var retired = new JournalReceiver("R" + (i - 2), "LIB");
                long retiredSequence = (i - 2L) * 10 + 1;
                assertThrows(IllegalArgumentException.class, () -> order.incarnation(pos(retired, retiredSequence)));
            }
            previous = next;
        }
    }

    @Test
    void retirementCannotHideChangedOverlapOrLosePinnedIdentity() {
        var order = new As400JournalOrder(List.of(A, B), Map.of(A, "a", B, "b"), Map.of(A, range(1, 10), B, range(11, 20)));
        assertThrows(IllegalArgumentException.class, () -> order.refreshVerified(single(B, "replaced", 11, 30), pos(B, 11), List.of(pos(A, 1)), 1));
        assertThat(order.incarnation(pos(A, 10))).isEqualTo("a");
        assertThat(order.incarnation(pos(B, 20))).isEqualTo("b");
        assertThrows(IllegalArgumentException.class, () -> order.incarnation(pos(B, 30)));
    }

    private static As400JournalOrder single(JournalReceiver receiver, String identity, long first, long last) {
        return new As400JournalOrder(List.of(receiver), Map.of(receiver, identity), Map.of(receiver, range(first, last)));
    }

    private static As400JournalOrder.Range range(long first, long last) {
        return new As400JournalOrder.Range(BigInteger.valueOf(first), BigInteger.valueOf(last));
    }

    private static JournalPosition pos(JournalReceiver receiver, long offset) {
        return new JournalPosition(BigInteger.valueOf(offset), receiver);
    }
}
