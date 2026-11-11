/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400LegacyRowFailureTest.Fixture;
import io.debezium.connector.db2as400.As400StreamingChangeEventSource.TransactionMetadataException;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.txmetadata.TransactionContext;

/** Actual legacy dispatcher/monitor/RPC behavior, with controlled journal input. */
class As400LegacyTransactionMetadataTest {
    private static Fixture enabled() throws Exception {
        return new Fixture(Map.of("provide.transaction.metadata", "true"), false);
    }

    @ParameterizedTest
    @CsvSource({ "PT,c", "PX,c", "UP,u", "DL,d", "DR,d" })
    void autocommitRowsHaveNoTransactionBlockOrSyntheticMarkers(String type, String operation) throws Exception {
        try (var f = enabled()) {
            var inactive = f.offset.getTransactionContext();
            send(f, 41, "R/" + type, 0, 1);
            var output = records(f);
            assertThat(data(output)).hasSize(1);
            assertThat(data(output).get(0).getString("op")).isEqualTo(operation);
            assertThat(data(output).get(0).get("transaction")).isNull();
            assertThat(markers(output)).isEmpty();
            assertThat(f.offset.getTransactionContext()).isSameAs(inactive);
            assertThat(inactive.isTransactionInProgress()).isFalse();
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(41));
            verifyNoInteractions(f.errors);
        }
    }

    @Test
    void autocommitKeyChangeAndClearPreserveTheirWireRecords() throws Exception {
        try (var f = enabled()) {
            send(f, 41, "R/UB", 0, 1);
            send(f, 42, "R/UP", 0, 2);
            // A controlled single-member fixture; no native membership query.
            when(f.jdbc.isOnlyMember("LIB", "TEST", "TEST")).thenReturn(true);
            send(f, 43, "F/CR", 0, 0);
            var output = records(f);
            assertThat(output).hasSize(4);
            assertThat(data(output)).extracting(v -> v.getString("op")).containsExactly("d", "c", "t");
            assertThat(output.get(1).value()).isNull();
            assertThat(output.get(3).key()).isNull();
            assertThat(data(output)).allSatisfy(v -> assertThat(v.get("transaction")).isNull());
            assertThat(markers(output)).isEmpty();
            verifyNoInteractions(f.errors);
        }
    }

    @Test
    void interleavedCyclesAndAutocommitKeepIndependentMarkersAndExactCounts() throws Exception {
        try (var f = enabled()) {
            send(f, 41, "C/SC", 41, 0);
            send(f, 42, "C/SC", 42, 0);
            send(f, 43, "R/PT", 41, 1);
            send(f, 44, "R/UB", 41, 1);
            send(f, 45, "R/UP", 41, 2); // d, tombstone, c: two counted change envelopes.
            send(f, 46, "R/PT", 42, 9);
            send(f, 47, "R/PT", 0, 10); // Must not implicitly commit either open cycle.
            send(f, 48, "C/CM", 41, 0);
            send(f, 49, "C/CM", 42, 0);
            var output = records(f);
            assertThat(output).hasSize(10);
            var markers = markers(output);
            assertThat(markers).extracting(v -> v.getString("status") + ":" + v.getString("id"))
                    .containsExactly("BEGIN:41", "BEGIN:42", "END:41", "END:42");
            assertEnd(markers.get(2), 3L);
            assertEnd(markers.get(3), 1L);
            var rows = data(output);
            assertThat(rows).extracting(v -> v.getString("op")).containsExactly("c", "d", "c", "c", "c");
            assertThat(rows.subList(0, 3)).allSatisfy(v -> assertThat(v.getStruct("transaction").getString("id")).isEqualTo("41"));
            assertThat(rows.subList(0, 3)).extracting(v -> v.getStruct("transaction").getInt64("total_order")).containsExactly(1L, 2L, 3L);
            assertThat(rows.subList(0, 3)).extracting(v -> v.getStruct("transaction").getInt64("data_collection_order")).containsExactly(1L, 2L, 3L);
            assertThat(rows.get(3).getStruct("transaction").getString("id")).isEqualTo("42");
            assertThat(rows.get(3).getStruct("transaction").getInt64("total_order")).isEqualTo(1L);
            assertThat(rows.get(4).get("transaction")).isNull();
            assertThat(f.offset.getTransactionContext().isTransactionInProgress()).isFalse();
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(49));
            verifyNoInteractions(f.errors);
        }
    }

    @Test
    void emptyCommitProducesOneBeginAndOneZeroCountEnd() throws Exception {
        try (var f = enabled()) {
            send(f, 41, "C/SC", 41, 0);
            send(f, 42, "C/CM", 41, 0);
            var markers = markers(records(f));
            assertThat(markers).extracting(v -> v.getString("status")).containsExactly("BEGIN", "END");
            assertThat(markers.get(1).getInt64("event_count")).isZero();
            assertThat(markers.get(1).getArray("data_collections")).isEmpty();
            verifyNoInteractions(f.errors);
        }
    }

    @Test
    void disabledMetadataKeepsRowsAndOmitsMarkers() throws Exception {
        try (var f = new Fixture(Map.of(), false)) {
            send(f, 41, "C/SC", 41, 0);
            send(f, 42, "R/PT", 41, 1);
            send(f, 43, "C/CM", 41, 0);
            send(f, 44, "R/PT", 0, 2);
            var output = records(f);
            assertThat(output).hasSize(2);
            assertThat(markers(output)).isEmpty();
            assertThat(data(output)).allSatisfy(v -> assertThat(v.get("transaction")).isNull());
            verifyNoInteractions(f.errors);
        }
    }

    @Test
    void unknownCommitCannotEndAnotherCycle() throws Exception {
        try (var f = enabled()) {
            send(f, 41, "C/SC", 41, 0);
            send(f, 42, "R/PT", 41, 1);
            send(f, 43, "C/CM", 99, 0);
            send(f, 45, "C/CM", 41, 0);
            var output = records(f);
            assertThat(markers(output)).extracting(v -> v.getString("status") + ":" + v.getString("id")).containsExactly("BEGIN:41", "END:41");
            assertEnd(markers(output).get(1), 1L);
            verifyNoInteractions(f.errors);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "PT", "PX", "UP", "DL", "DR" })
    void transactionalDmlIsCountedOnce(String type) throws Exception {
        try (var f = enabled()) {
            send(f, 41, "C/SC", 41, 0);
            send(f, 42, "R/" + type, 41, 1);
            send(f, 43, "C/CM", 41, 0);
            var output = records(f);
            assertEnd(markers(output).get(1), 1L);
            assertThat(data(output)).hasSize(1);
            assertThat(data(output).get(0).getStruct("transaction").getInt64("total_order")).isEqualTo(1L);
            verifyNoInteractions(f.errors);
        }
    }

    @Test
    void missingPositiveCycleIsNotMislabeledAsAutocommit() throws Exception {
        try (var f = enabled()) {
            send(f, 41, "R/PT", 99, 1);
            send(f, 42, "C/CM", 99, 0);
            var output = records(f);
            assertThat(data(output)).hasSize(1);
            assertThat(data(output).get(0).getStruct("transaction").getString("id")).isEqualTo("99");
            assertThat(markers(output)).extracting(v -> v.getString("status")).containsExactly("BEGIN", "END");
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            verifyNoInteractions(f.errors);
        }
        try (var f = new Fixture(Map.of(), false)) {
            send(f, 41, "R/PT", 99, 1);
            verifyNoInteractions(f.errors);
            assertThat(data(records(f))).hasSize(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "SC", "CM" })
    void metadataDispatchFailureStopsBeforeJournalAdvancement(String type) throws Exception {
        try (var f = enabled()) {
            long before = 40;
            if (type.equals("CM")) {
                send(f, 41, "C/SC", 41, 0);
                send(f, 42, "R/PT", 41, 1);
                records(f);
                before = 42;
            }
            var failure = new IllegalStateException("controlled marker failure");
            if (type.equals("SC")) {
                doThrow(failure).when(f.dispatcher).dispatchTransactionStartedEvent(any(), anyString(), any(), any());
            }
            else {
                doThrow(failure).when(f.dispatcher).dispatchTransactionCommittedEvent(any(), any(), any());
            }
            clearInvocations(f.journal, f.rpc);
            long next = before + 1;
            var error = org.junit.jupiter.api.Assertions.assertThrows(TransactionMetadataException.class, () -> send(f, next, "C/" + type, 41, 0));
            assertThat(error).hasCause(failure);
            verifyNoInteractions(f.errors);
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(before));
            verify(f.journal, never()).getPosition();
            verify(f.journal).nextEntry();
            assertThat(records(f)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "warn", "skip" })
    void explicitLossyModesStillPermitMarkerFailure(String mode) throws Exception {
        try (var f = new Fixture(Map.of("provide.transaction.metadata", "true", "event.processing.failure.handling.mode", mode), false)) {
            doThrow(new IllegalStateException("controlled marker failure")).when(f.dispatcher).dispatchTransactionStartedEvent(any(), anyString(), any(), any());
            send(f, 41, "C/SC", 41, 0);
            verifyNoInteractions(f.errors);
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(41));
            assertThat(records(f)).isEmpty();
        }
    }

    @Test
    void inactiveContextIsOwnedByItsOffsetAndNeverClearsAnActiveCycle() throws Exception {
        try (var f = enabled(); var other = enabled()) {
            var inactive = f.offset.getTransactionContext();
            assertThat(inactive).isNotSameAs(other.offset.getTransactionContext());
            var active = new TransactionContext();
            active.beginTransaction("41");
            f.offset.setTransaction(active);
            f.offset.setTransaction(null);
            assertThat(f.offset.getTransactionContext()).isSameAs(inactive);
            assertThat(active.isTransactionInProgress()).isTrue();
            assertThat(inactive.isTransactionInProgress()).isFalse();
            var saved = Map.copyOf(f.offset.getOffset());
            f.offset.setTransaction(active);
            assertThat(f.offset.getOffset()).isEqualTo(saved); // No offset-format/recovery migration hidden in this fix.
            var restored = new As400OffsetContext.Loader(f.config).load(saved);
            assertThat(restored.getTransactionContext().isTransactionInProgress()).isFalse();
        }
    }

    private static void send(Fixture f, long sequence, String operation, int cycle, int value) throws Exception {
        when(f.journal.decode(f.decoder)).thenReturn(new Object[]{ value });
        f.run(As400ConnectProbeConnector.event(sequence, operation, cycle, value));
    }

    private static List<SourceRecord> records(Fixture f) throws Exception {
        return f.queue.poll().stream().map(DataChangeEvent::getRecord).toList();
    }

    private static List<Struct> data(List<SourceRecord> output) {
        return output.stream().map(SourceRecord::value).filter(v -> v instanceof Struct s && s.schema().field("op") != null).map(Struct.class::cast).toList();
    }

    private static List<Struct> markers(List<SourceRecord> output) {
        return output.stream().map(SourceRecord::value).filter(v -> v instanceof Struct s && s.schema().field("status") != null).map(Struct.class::cast).toList();
    }

    private static void assertEnd(Struct end, long count) {
        assertThat(end.getString("status")).isEqualTo("END");
        assertThat(end.getInt64("event_count")).isEqualTo(count);
        List<Struct> collections = end.getArray("data_collections");
        assertThat(collections).hasSize(1);
        assertThat(collections.get(0).getString("data_collection")).isEqualTo("DB.LIB.TEST");
        assertThat(collections.get(0).getInt64("event_count")).isEqualTo(count);
    }
}
