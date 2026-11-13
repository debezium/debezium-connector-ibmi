/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400NativeLegacyClearTest {
    @ParameterizedTest
    @ValueSource(strings = { "same-receiver", "rotated", "required-freed", "missing", "reset", "wrong-link" })
    void shouldFindTheOriginalCycleOnlyWithinTheReadableLinkedSequenceEpoch(String condition) {
        var journal = new JournalReceiver("JRN", "LIB");
        var first = new JournalReceiver("R1", "LIB");
        var second = new JournalReceiver("R2", "LIB");
        var old = new JournalReceiver("OLD", "LIB");
        var before = new As400OperatorJournalEvidence.Receiver(first, condition.equals("required-freed") ? "FREED" : "SAVED",
                BigInteger.ONE, BigInteger.valueOf(condition.equals("reset") ? 900 : 100), Optional.of(old),
                Optional.of(condition.equals("wrong-link") ? old : second), "NO", "YES", "YES", "NO", "NO");
        var attached = new As400OperatorJournalEvidence.Receiver(second, "ATTACHED", BigInteger.valueOf(101), BigInteger.valueOf(200),
                Optional.of(first), Optional.empty(), "NO", "YES", "YES", "NO", "NO");
        // A previous sequence epoch may reuse cycle numbers but cannot supply
        // this transaction's SC. Its unreadable status is irrelevant here.
        var unused = new As400OperatorJournalEvidence.Receiver(old, "FREED", BigInteger.ONE, BigInteger.valueOf(999),
                Optional.empty(), Optional.of(first), "NO", "YES", "YES", "NO", "NO");
        var directory = new As400OperatorJournalEvidence.Observation(journal, second,
                condition.equals("missing") ? List.of(attached) : List.of(unused, before, attached));
        var through = new JournalPosition(BigInteger.valueOf(180), second);
        var cycle = BigInteger.valueOf(condition.equals("same-receiver") ? 110 : 90);
        if (List.of("same-receiver", "rotated").contains(condition)) {
            assertThat(As400NativeLegacyClear.originalStart(directory, through, cycle))
                    .isEqualTo(new JournalPosition(cycle, condition.equals("same-receiver") ? second : first));
        }
        else {
            assertThatThrownBy(() -> As400NativeLegacyClear.originalStart(directory, through, cycle)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
