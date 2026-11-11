/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Receiver;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/** Evidence/transport faults at the bounded cut boundary; no synthetic strict startup proof. */
class As400OperatorCutReaderTest {
    private static final JournalReceiver JOURNAL = new JournalReceiver("JRN", "LIB");
    private static final JournalReceiver R1 = new JournalReceiver("R1", "LIB");
    private static final JournalReceiver R2 = new JournalReceiver("R2", "LIB");
    private static final As400CaptureFingerprint.Policy POLICY = new As400CaptureFingerprint.Policy(Map.of(), Set.of(),
            Map.of("version", "strict-row-v1"), Map.of("version", "fixed-v1"), Map.of("version", "ordinary-v1"), true, true, Map.of("server", "fixture"));

    @Test
    void readsNativeSelectedEntriesAcrossVerifiedReceiversAndReobservesBeforeReturningOwnedCut() throws Exception {
        Fixture f = new Fixture();
        var cut = f.reader().read(f.offset());
        assertThat(cut.entries()).hasSize(f.entries.size() - 1);
        assertThat(cut.entries().get(0).position()).isEqualTo(position(25));
        assertThat(cut.tail()).isEqualTo(position(81));
        assertThat(cut.order().compare(position(76), position(79))).isNegative();
        assertThat(cut.checkpoint().completedThrough()).isEqualTo(position(24));
        verify(f.transport).readRawPage(position(24), position(77), 100);
        verify(f.transport).readRawPage(position(78), position(81), 50);
        assertThat(cut.entries().get(0).header().getSequenceNumber()).isEqualTo(BigInteger.valueOf(25));
    }

    @Test
    void shouldContinueAcrossReceiverSaveWithoutChangingDurableOffsets() throws Exception {
        Fixture f = new Fixture();
        var saved = new Observation(JOURNAL, R2, List.of(receiver(R1, "SAVED", 24, 77, Optional.empty(), Optional.of(R2)),
                receiver(R2, "ATTACHED", 78, 81, Optional.of(R1), Optional.empty())));
        when(f.journals.collect()).thenReturn(f.directory, saved);
        var offset = Map.copyOf(f.offset());
        var cut = f.reader().read(offset);
        assertThat(cut.entries()).hasSize(f.entries.size() - 1);
        assertThat(cut.checkpoint().toOffset()).isEqualTo(offset);
    }

    @Test
    void missingOrLegacyOffsetsRejectBeforeOpeningAnySourceConnection() throws Exception {
        Fixture f = new Fixture();
        for (Map<String, ?> offset : List.<Map<String, ?>> of(Map.of(), Map.of("position", "24"))) {
            assertThrows(IllegalArgumentException.class, () -> f.reader().read(offset));
        }
        assertThrows(IllegalArgumentException.class, () -> f.reader().read(null));
        verifyNoInteractions(f.journals, f.identities, f.tables, f.transport);
    }

    @ParameterizedTest
    @ValueSource(strings = { "journal", "receiver", "fingerprint", "table", "range", "floor-after-w" })
    void incompatibleOffsetsAndAssociationsRejectBeforeReadingHistory(String fault) throws Exception {
        Fixture f = new Fixture();
        var offset = new HashMap<String, String>(f.offset());
        switch (fault) {
            case "journal" -> offset.put("journal_identity", "other-journal");
            case "receiver" -> {
                offset.put("replay_from.receiver_identity", "other-receiver");
                offset.put("completed_through.receiver_identity", "other-receiver");
            }
            case "fingerprint" -> offset.put("capture_fingerprint", "v1:" + "a".repeat(64));
            case "table" -> when(f.tables.collect(any())).thenReturn(List.of());
            case "range" -> {
                offset.put("replay_from.sequence", "23");
                offset.put("completed_through.sequence", "23");
            }
            case "floor-after-w" -> {
                offset.put("replay_from.sequence", "25");
                offset.put("replay_from.inclusive", "true");
            }
            default -> throw new AssertionError(fault);
        }
        assertThrows(IllegalArgumentException.class, () -> f.reader().read(offset));
        verifyNoInteractions(f.transport);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "fork", "reciprocal", "reset", "cycle", "duplicate", "profile", "attached", "unavailable" })
    void invalidReceiverDirectoryRejectsBeforeRetrieval(String fault) throws Exception {
        Fixture f = new Fixture();
        List<Receiver> receivers = new ArrayList<>(f.directory.receivers());
        switch (fault) {
            case "missing" -> receivers.remove(0);
            case "fork" -> receivers.add(receiver(new JournalReceiver("EXTRA", "LIB"), "ONLINE", 1, 23, Optional.empty(), Optional.of(R2)));
            case "reciprocal" -> receivers.set(1, receiver(R2, "ATTACHED", 78, 81, Optional.empty(), Optional.empty()));
            case "reset" -> receivers.set(1, receiver(R2, "ATTACHED", 70, 81, Optional.of(R1), Optional.empty()));
            case "cycle" -> receivers.set(1, receiver(R2, "ONLINE", 78, 81, Optional.of(R1), Optional.of(R1)));
            case "duplicate" -> receivers.add(receivers.get(0));
            case "profile" -> receivers.set(0,
                    new Receiver(R1, "ONLINE", BigInteger.valueOf(24), BigInteger.valueOf(77), Optional.empty(), Optional.of(R2), "YES", "YES", "YES", "NO", "NO"));
            case "attached" -> receivers.set(0, receiver(R1, "ATTACHED", 24, 77, Optional.empty(), Optional.of(R2)));
            case "unavailable" -> receivers.set(0, receiver(R1, "PARTIAL", 24, 77, Optional.empty(), Optional.of(R2)));
            default -> throw new AssertionError(fault);
        }
        when(f.journals.collect()).thenReturn(new Observation(JOURNAL, R2, receivers));
        assertThrows(IllegalArgumentException.class, () -> f.reader().read(f.offset()));
        verifyNoInteractions(f.transport);
    }

    @ParameterizedTest
    @ValueSource(strings = { "floor", "w", "failed-page", "changed-table", "changed-native", "changed-directory", "shrunk-tail", "page-bound", "entry-bound",
            "byte-bound" })
    void incompleteOrChangedCutNeverReturnsPartialEvidence(String fault) throws Exception {
        Fixture f = new Fixture();
        var offset = new HashMap<>(f.offset());
        switch (fault) {
            case "floor" -> f.entries.remove(0);
            case "w" -> {
                offset.put("replay_from.inclusive", "true");
                offset.put("completed_through.sequence", "27"); // A legal numeric gap is not a present W anchor.
            }
            case "failed-page" -> when(f.transport.readRawPage(position(78), position(81), 50)).thenThrow(new IllegalStateException("Partial"));
            case "changed-table" -> when(f.tables.collect(any())).thenReturn(f.observations, List.of());
            case "changed-native" -> doAnswer(i -> {
                List<Target> targets = i.getArgument(0);
                var result = new HashMap<>(f.nativeObjects);
                if (++f.identityReads == 3) {
                    result.put(target(R1, Kind.RECEIVER), new Identity(BigInteger.ONE, BigInteger.TEN, BigInteger.TWO));
                }
                return candidates(result, targets);
            }).when(f.identities).collect(any());
            case "changed-directory" -> when(f.journals.collect()).thenReturn(f.directory, new Observation(JOURNAL, R1, f.directory.receivers()));
            case "shrunk-tail" -> when(f.journals.collect()).thenReturn(f.directory, new Observation(JOURNAL, R2,
                    List.of(f.directory.receivers().get(0), receiver(R2, "ATTACHED", 78, 80, Optional.of(R1), Optional.empty()))));
            case "page-bound" -> f.limits = new As400OperatorCutReader.Limits(1, 100, 1_000_000);
            case "entry-bound" -> f.limits = new As400OperatorCutReader.Limits(10, 40, 1_000_000);
            case "byte-bound" -> f.limits = new As400OperatorCutReader.Limits(10, 100, 236);
            default -> throw new AssertionError(fault);
        }
        assertThrows(Exception.class, () -> f.reader().read(offset));
        // The input durable map cannot be advanced by a failed read.
        assertThat(offset.get("completed_through.sequence")).isEqualTo(fault.equals("w") ? "27" : "24");
    }

    @Test
    void nativeContinuationAndGrowingAttachedTailAreHandledWithoutSequenceGuessing() throws Exception {
        Fixture f = new Fixture();
        when(f.transport.readRawPage(position(24), position(77), 100)).thenReturn(new RawJournalPage(f.entries.subList(0, 2), Optional.of(position(26)), 600));
        when(f.journals.collect()).thenReturn(f.directory, new Observation(JOURNAL, R2,
                List.of(f.directory.receivers().get(0), receiver(R2, "ATTACHED", 78, 100, Optional.of(R1), Optional.empty()))));
        var cut = f.reader().read(f.offset());
        assertThat(cut.entries()).hasSize(f.entries.size() - 1);
        assertThat(cut.tail()).isEqualTo(position(81));
        verify(f.transport).readRawPage(position(26), position(77), 98);
    }

    @Test
    void completeEmptySelectionInLaterReceiverStillRequiresAllAnchorsAndReobservation() throws Exception {
        Fixture f = new Fixture();
        f.entries.removeIf(entry -> entry.position().receiver().equals(R2));
        var cut = f.reader().read(f.offset());
        assertThat(cut.tail()).isEqualTo(position(81));
        assertThat(cut.entries()).hasSize(f.entries.size() - 1);
        assertThat(cut.checkpoint().toOffset()).isEqualTo(f.offset());

        Fixture missing = new Fixture();
        missing.entries.clear();
        assertThrows(IllegalArgumentException.class, () -> missing.reader().read(missing.offset()));

        Fixture changed = new Fixture();
        changed.entries.removeIf(entry -> entry.position().receiver().equals(R2));
        when(changed.journals.collect()).thenReturn(changed.directory, new Observation(JOURNAL, R1, changed.directory.receivers()));
        assertThrows(IllegalArgumentException.class, () -> changed.reader().read(changed.offset()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2 })
    void temporarilyAbsentMemberRejectsWholeCutAndFreshReadKeepsDurableCheckpoint(int failedObservation) throws Exception {
        Fixture f = new Fixture();
        Map<String, String> offset = new HashMap<>(f.offset());
        Map<String, String> durable = Map.copyOf(offset);
        int[] observations = { 0 };
        // Native open TRUNCATE can make SYSPARTITIONSTAT return no member. This
        // must not be interpreted as a removed capture or an empty journal cut.
        when(f.tables.collect(any())).thenAnswer(invocation -> {
            if (++observations[0] == failedObservation) {
                throw new SQLException("Required native table observation is absent");
            }
            return f.observations;
        });
        assertThrows(SQLException.class, () -> f.reader().read(offset));
        assertThat(offset).isEqualTo(durable);
        if (failedObservation == 1) {
            verifyNoInteractions(f.transport);
        }

        // Once the source transaction ends, reread every required anchor using a
        // new reader. Failed speculative retrieval cannot become a restart offset.
        var recovered = f.reader().read(offset);
        assertThat(recovered.checkpoint().toOffset()).isEqualTo(durable);
        assertThat(recovered.entries()).hasSize(f.entries.size() - 1);
        assertThat(recovered.entries().get(0).position()).isEqualTo(position(25));
    }

    private static JournalPosition position(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), sequence < 78 ? R1 : R2);
    }

    private static Target target(JournalReceiver name, Kind kind) {
        return new Target("/QSYS.LIB/LIB.LIB/" + name.name() + (kind == Kind.JOURNAL ? ".JRN" : ".JRNRCV"), kind);
    }

    private static Receiver receiver(JournalReceiver name, String status, long first, long last, Optional<JournalReceiver> previous, Optional<JournalReceiver> next) {
        return new Receiver(name, status, BigInteger.valueOf(first), BigInteger.valueOf(last), previous, next, "NO", "YES", "YES", "NO", "NO");
    }

    private static Candidates candidates(Map<Target, Identity> source, List<Target> targets) {
        return new Candidates(targets.stream().collect(Collectors.toMap(t -> t, source::get)));
    }

    private static final class Fixture {
        final As400OperatorJournalEvidence journals = mock(As400OperatorJournalEvidence.class);
        final As400JdbcNativeIdentityEvidence identities = mock(As400JdbcNativeIdentityEvidence.class);
        final As400OperatorTableEvidence tables = mock(As400OperatorTableEvidence.class);
        final RetrieveJournal transport = mock(RetrieveJournal.class);
        final List<As400OperatorTableEvidence.Observation> observations;
        final Map<Target, Identity> nativeObjects = new HashMap<>();
        final Observation directory = new Observation(JOURNAL, R2, List.of(receiver(R1, "ONLINE", 24, 77, Optional.empty(), Optional.of(R2)),
                receiver(R2, "ATTACHED", 78, 81, Optional.of(R1), Optional.empty())));
        final List<RawJournalPage.Entry> entries = new ArrayList<>();
        As400OperatorCutReader.Limits limits = new As400OperatorCutReader.Limits(10, 100, 1_000_000);
        int identityReads;

        Fixture() throws Exception {
            var table = new As400OperatorTableEvidenceTest.Fixture().collect().get(0);
            observations = List.of(new As400OperatorTableEvidence.Observation(new As400OperatorTableEvidence.Request(JOURNAL, "LIB", "A"),
                    table.table(), table.member(), table.fileObject(), table.memberObject(), table.format()));
            int index = 1;
            for (Target target : List.of(target(JOURNAL, Kind.JOURNAL), target(R1, Kind.RECEIVER), target(R2, Kind.RECEIVER))) {
                nativeObjects.put(target, new Identity(BigInteger.ONE, BigInteger.valueOf(index++), BigInteger.ONE));
            }
            when(journals.collect()).thenReturn(directory);
            when(identities.collect(any())).thenAnswer(i -> candidates(nativeObjects, i.getArgument(0)));
            when(tables.collect(any())).thenReturn(observations);
            try (var input = getClass().getResourceAsStream("/native-transactions/ordinary-cdc.json")) {
                Map<String, Object> fixture = new ObjectMapper().readValue(input, new TypeReference<>() {
                });
                for (Map<?, ?> entry : (List<Map<?, ?>>) fixture.get("entries")) {
                    byte[] bytes = HexFormat.of().parseHex(entry.get("hex").toString());
                    long sequence = Long.parseLong(entry.get("sequence").toString());
                    entries.add(new RawJournalPage.Entry(position(sequence), new EntryHeaderDecoder().decode(bytes, 0), bytes));
                }
            }
            when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
                JournalPosition from = i.getArgument(0);
                JournalPosition to = i.getArgument(1);
                List<RawJournalPage.Entry> selected = entries.stream().filter(e -> e.position().receiver().equals(from.receiver())
                        && e.position().offset().compareTo(from.offset()) >= 0 && e.position().offset().compareTo(to.offset()) <= 0).toList();
                if (selected.size() > (int) i.getArgument(2)) {
                    throw new IllegalArgumentException("Excess response");
                }
                return new RawJournalPage(selected, Optional.empty(), 8192);
            });
        }

        Map<String, String> offset() {
            Target jt = target(JOURNAL, Kind.JOURNAL);
            Target rt = target(R1, Kind.RECEIVER);
            return As400Checkpoint.afterVerifiedBoundary(As400OperatorCutReader.token(jt, nativeObjects.get(jt)),
                    POLICY.fingerprint(observations.stream().map(As400OperatorTableEvidence.Observation::tableIdentity).toList()), position(24),
                    As400OperatorCutReader.token(rt, nativeObjects.get(rt))).toOffset();
        }

        As400OperatorCutReader reader() {
            return new As400OperatorCutReader(journals, identities, tables, observations.stream().map(As400OperatorTableEvidence.Observation::request).toList(),
                    transport, limits, observations.stream().map(As400OperatorTableEvidence.Observation::tableIdentity).toList(), POLICY);
        }
    }
}
