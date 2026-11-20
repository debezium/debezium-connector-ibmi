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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

class As400LegacyNativePrefixTest {
    @TempDir
    Path directory;

    private Fixture fixture;
    private As400OperatorJournalEvidence journals;
    private As400JdbcNativeIdentityEvidence identities;
    private RetrieveJournal transport;
    private As400StreamingChangeEventSource handler;
    private As400OffsetContext scan;
    private Map<String, String> durable;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final AtomicBoolean identityChanged = new AtomicBoolean();

    @BeforeEach
    void setUpNativeSource() throws Exception {
        fixture = new Fixture(directory);
        journals = mock(As400OperatorJournalEvidence.class);
        identities = mock(As400JdbcNativeIdentityEvidence.class);
        transport = mock(RetrieveJournal.class);
        handler = mock(As400StreamingChangeEventSource.class);
        when(journals.collectFrom(any())).thenAnswer(i -> fixture.receiverDirectory(64));
        when(identities.collect(any())).thenAnswer(i -> identityChanged.get()
                ? new As400JdbcNativeIdentityEvidence.Candidates(Map.of())
                : fixture.nativeCandidates(i.getArgument(0)));
        durable = Fixture.legacyOffset(29);
        var config = Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB").with("database.schema", "DCOC1107").build();
        scan = new As400OffsetContext.Loader(new As400ConnectorConfig(config)).load(durable);
    }

    private As400LegacyNativePrefix prefix(int entryLimit) {
        return prefix(entryLimit, null);
    }

    private As400LegacyNativePrefix prefix(int entryLimit, As400LegacyClearBuffer clears) {
        return new As400LegacyNativePrefix(journals, identities, transport, handler, new As400Partition("test"), scan, () -> {
            if (stopped.get()) {
                throw new IllegalStateException("Terminal stop");
            }
        }, new As400OperatorCutReader.Limits(16, entryLimit, 1_000_000), clears);
    }

    @org.junit.jupiter.api.Test
    void shouldReturnAValidatedPartialPrefixWhenTheNextEntryWouldExceedItsByteBudget() throws Exception {
        fixture.entries.subList(4, fixture.entries.size()).clear();
        when(transport.readAuditHistoryPage(any(), any(), anyInt())).thenAnswer(i -> new RetrieveJournal.AuditPage(
                fixture.entries.stream().filter(e -> e.position().equals(i.getArgument(0))).toList(), Optional.empty(), 1024));
        when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
            JournalPosition from = i.getArgument(0);
            return new RawJournalPage(fixture.entries.stream().filter(e -> e.position().offset().compareTo(from.offset()) >= 0).toList(),
                    Optional.empty(), 1024);
        });
        var prefix = new As400LegacyNativePrefix(journals, identities, transport, handler, new As400Partition("test"), scan,
                () -> {
                }, new As400OperatorCutReader.Limits(16, 100, 1120));
        assertThat(prefix.publishNext().orElseThrow().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("31");
        assertThat(prefix.publishNext().orElseThrow().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("32");
        for (int sequence = 30; sequence <= 32; sequence++) {
            verify(handler).processOwnedEntry(any(), any(), org.mockito.ArgumentMatchers.eq(fixture.entry(sequence)));
        }
    }

    @org.junit.jupiter.api.Test
    void shouldCrossMoreEmptyReceiversThanThePageBudgetWithoutInventingProgress() throws Exception {
        var names = new ArrayList<io.debezium.ibmi.db2.journal.retrieve.JournalReceiver>();
        names.add(Fixture.RECEIVER);
        for (int i = 1; i < 40; i++) {
            names.add(new io.debezium.ibmi.db2.journal.retrieve.JournalReceiver("RCV" + i, Fixture.RECEIVER.library()));
        }
        var receivers = new ArrayList<As400OperatorJournalEvidence.Receiver>();
        for (int i = 0; i < names.size(); i++) {
            receivers.add(new As400OperatorJournalEvidence.Receiver(names.get(i), i == names.size() - 1 ? "ATTACHED" : "SAVED",
                    java.math.BigInteger.valueOf(29 + i * 10), java.math.BigInteger.valueOf(38 + i * 10),
                    i == 0 ? Optional.empty() : Optional.of(names.get(i - 1)),
                    i == names.size() - 1 ? Optional.empty() : Optional.of(names.get(i + 1)), "NO", "YES", "YES", "NO", "NO"));
        }
        var directory = new As400OperatorJournalEvidence.Observation(Fixture.JOURNAL, names.get(names.size() - 1), receivers);
        when(journals.collectFrom(any())).thenReturn(directory);
        doAnswer(i -> {
            List<As400JdbcNativeIdentityEvidence.Target> targets = i.getArgument(0);
            var objects = new java.util.HashMap<As400JdbcNativeIdentityEvidence.Target, As400JdbcNativeIdentityEvidence.Identity>();
            for (int n = 0; n < targets.size(); n++) {
                objects.put(targets.get(n), new As400JdbcNativeIdentityEvidence.Identity(java.math.BigInteger.ONE,
                        java.math.BigInteger.valueOf(n + 1), java.math.BigInteger.ONE));
            }
            return new As400JdbcNativeIdentityEvidence.Candidates(objects);
        }).when(identities).collect(any());
        var anchor = fixture.entries.get(0);
        var position = new JournalPosition(java.math.BigInteger.valueOf(424), names.get(names.size() - 1));
        byte[] bytes = fixture.userMarker(424).data();
        int receiver = java.nio.ByteBuffer.wrap(bytes).getInt(20);
        assertThat(receiver).isPositive();
        new com.ibm.as400.access.AS400Text(10, 37).toBytes(position.receiver().name(), bytes, receiver);
        var marker = new RawJournalPage.Entry(position,
                new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(bytes, 0), bytes);
        when(transport.readAuditHistoryPage(any(), any(), anyInt()))
                .thenAnswer(i -> new RetrieveJournal.AuditPage(List.of(i.getArgument(0).equals(position) ? marker : anchor), Optional.empty(), 1024));
        when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
            JournalPosition from = i.getArgument(0);
            return new RawJournalPage(from.receiver().equals(Fixture.RECEIVER) ? List.of(anchor) : List.of(), Optional.empty(), 1024);
        });
        var prefix = prefix(100);
        assertThat(prefix.publishNext()).isEmpty();
        assertThat(prefix.scan()).isEqualTo(anchor.position());
        assertThat(prefix.advanceThroughMarker(position)).isPresent();
        assertThat(prefix.scan()).isEqualTo(position);
        verify(handler, never()).processOwnedEntry(any(), any(), any()); // No CDC record or synthetic row was dispatched.
    }

    @org.junit.jupiter.api.Test
    void shouldOfferACompletedRollbackBoundaryWhenItsMetadataBecomesAvailableOnAnIdlePoll() throws Exception {
        var anchor = fixture.entries.get(0);
        when(transport.readAuditHistoryPage(any(), any(), anyInt()))
                .thenReturn(new RetrieveJournal.AuditPage(List.of(anchor), Optional.empty(), 1024));
        when(transport.readRawPage(any(), any(), anyInt())).thenReturn(new RawJournalPage(List.of(anchor), Optional.empty(), 1024));
        var clears = mock(As400LegacyClearBuffer.class);
        var pending = new AtomicBoolean(true);
        when(clears.pending()).thenAnswer(i -> pending.get());
        when(clears.safeOffset(any())).thenAnswer(i -> i.getArgument(0));
        doAnswer(i -> {
            assertThat(i.getArgument(0, JournalPosition.class)).isEqualTo(anchor.position());
            pending.set(false);
            return null;
        }).when(clears).resolve(any(), any());
        var prefix = prefix(100, clears);
        assertThat(prefix.publishNext()).contains(scan.getOffset());
        assertThat(prefix.hasDeferredWork()).isFalse();
        assertThat(prefix.publishNext()).isEmpty(); // Completion is offered only once, without inventing source progress.
    }

    @org.junit.jupiter.api.Test
    void shouldValidateOnlyRequiredReceiverIdentitiesWhenTheDirectoryIncludesOlderHistory() throws Exception {
        var current = fixture.receiverDirectory(64).receivers().get(0);
        var old = new io.debezium.ibmi.db2.journal.retrieve.JournalReceiver("OLD", Fixture.RECEIVER.library());
        var older = new As400OperatorJournalEvidence.Receiver(old, "ONLINE", java.math.BigInteger.ONE, java.math.BigInteger.valueOf(28),
                Optional.empty(), Optional.of(Fixture.RECEIVER), "NO", "YES", "YES", "NO", "NO");
        when(journals.collectFrom(any())).thenReturn(new As400OperatorJournalEvidence.Observation(Fixture.JOURNAL, Fixture.RECEIVER,
                List.of(older, current)));
        var anchor = fixture.entries.get(0);
        when(transport.readAuditHistoryPage(any(), any(), anyInt()))
                .thenReturn(new RetrieveJournal.AuditPage(List.of(anchor), Optional.empty(), 1024));
        when(transport.readRawPage(any(), any(), anyInt()))
                .thenReturn(new RawJournalPage(List.of(anchor), Optional.empty(), 1024));
        var prefix = prefix(100);
        assertThat(prefix.publishNext()).isEmpty();
        assertThat(prefix.completePosition()).isPresent();
        assertThat(prefix.scan()).isEqualTo(anchor.position());
        verify(handler, never()).processOwnedEntry(any(), any(), any());
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReplayUnprocessedAnchorBeforeAcknowledgingItsPosition(boolean fails) throws Exception {
        var anchor = fixture.entries.get(0);
        scan.getPosition().setProcessed(false);
        when(transport.readAuditHistoryPage(any(), any(), anyInt()))
                .thenReturn(new RetrieveJournal.AuditPage(List.of(anchor), Optional.empty(), 1024));
        when(transport.readRawPage(any(), any(), anyInt()))
                .thenReturn(new RawJournalPage(List.of(anchor), Optional.empty(), 1024));
        doAnswer(invocation -> {
            assertThat(scan.getPosition().processed()).isFalse();
            if (fails) {
                throw new IllegalStateException("dispatch failed before acknowledgement");
            }
            return null;
        }).when(handler).processOwnedEntry(any(), any(), any());
        var prefix = prefix(100);
        if (fails) {
            assertThrows(IllegalStateException.class, prefix::publishNext);
            assertThat(scan.getPosition().processed()).isFalse();
            assertThat(prefix.completePosition()).isEmpty();
        }
        else {
            var complete = prefix.publishNext().orElseThrow();
            assertThat(complete.get("offset.processed")).isEqualTo("true");
            assertThat(prefix.publishNext()).isEmpty();
        }
        verify(handler).processOwnedEntry(any(), any(), org.mockito.ArgumentMatchers.eq(anchor));
        assertThat(prefix.scan()).isEqualTo(anchor.position());
    }

    @ParameterizedTest
    @ValueSource(strings = { "complete", "remaining-row", "continuation", "before-image", "missing", "wrong-type", "object", "commit", "stop", "identity" })
    void shouldAdvanceToObservedMarkerOnlyWhenInterveningCdcAndPendingImagesAreAbsent(String fault) throws Exception {
        var f = fixture;
        var anchor = f.entries.get(0);
        byte[] bytes = f.userMarker(40).data();
        java.nio.ByteBuffer.wrap(bytes).putLong(64, fault.equals("commit") ? 7 : 0);
        new com.ibm.as400.access.AS400Text(3, 37).toBytes(fault.equals("wrong-type") ? "UXX" : "UDN", bytes, 98);
        new com.ibm.as400.access.AS400Text(30, 37).toBytes(fault.equals("object") ? "A" : "", bytes, 157);
        var marker = new RawJournalPage.Entry(Fixture.position(40),
                new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(bytes, 0), bytes);
        var proving = new AtomicBoolean();
        when(transport.readAuditHistoryPage(any(), any(), anyInt())).thenAnswer(invocation -> {
            if (!proving.get()) {
                return new RetrieveJournal.AuditPage(List.of(anchor), Optional.empty(), 1024);
            }
            identityChanged.set(fault.equals("identity"));
            if (fault.equals("stop")) {
                stopped.set(true);
            }
            return new RetrieveJournal.AuditPage(fault.equals("missing") ? List.of() : List.of(marker), Optional.empty(), 1024);
        });
        when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(invocation -> new RawJournalPage(
                proving.get() && fault.equals("remaining-row") ? List.of(anchor, f.entries.get(1)) : List.of(anchor),
                proving.get() && fault.equals("continuation") ? Optional.of(Fixture.position(30)) : Optional.empty(), 1024));
        var prefix = prefix(100);
        assertThat(prefix.publishNext()).isEmpty();
        proving.set(true);
        when(handler.hasPendingBeforeImage()).thenReturn(fault.equals("before-image"));
        if (fault.equals("complete")) {
            var result = prefix.advanceThroughMarker(marker.position()).orElseThrow();
            assertThat(result.get("offset.event_sequence")).isEqualTo("40");
            assertThat(prefix.scan()).isEqualTo(marker.position()); // Never metadata tail 64.
            assertThat(prefix.completePosition()).contains(result);
        }
        else {
            if (List.of("remaining-row", "continuation", "before-image").contains(fault)) {
                assertThat(prefix.advanceThroughMarker(marker.position())).isEmpty();
            }
            else {
                assertThrows(Exception.class, () -> prefix.advanceThroughMarker(marker.position()));
            }
            assertThat(prefix.scan()).isEqualTo(Fixture.position(29));
            if (!stopped.get()) {
                assertThat(prefix.completePosition()).isEmpty();
            }
        }
        assertThat(durable.get("offset.event_sequence")).isEqualTo("29"); // No Kafka acknowledgement was fabricated.
        verify(handler, never()).processOwnedEntry(any(), any(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = { "complete", "limited", "cached-before", "resume-before", "idle", "idle-before", "missing-anchor", "dispatch", "late-stop",
            "stop-after-dispatch",
            "changed-identity",
            "bad-continuation", "duplicate" })
    void shouldOfferProgressOnlyWhenTheOwnedPrefixDispatchCompletes(String fault) throws Exception {
        var f = fixture;
        if (fault.equals("resume-before") || fault.equals("idle-before")) {
            var anchor = f.entries.get(0);
            byte[] bytes = anchor.data();
            new com.ibm.as400.access.AS400Text(3, 37).toBytes("RUB", bytes, 98);
            f.entries.set(0, new RawJournalPage.Entry(anchor.position(),
                    new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(bytes, 0), bytes));
        }
        if (fault.startsWith("idle")) {
            f.entries.subList(1, f.entries.size()).clear();
        }
        when(transport.readAuditHistoryPage(any(), any(), anyInt())).thenAnswer(invocation -> {
            var from = (JournalPosition) invocation.getArgument(0);
            return new RetrieveJournal.AuditPage(fault.equals("missing-anchor") ? List.of()
                    : f.entries.stream().filter(e -> e.position().equals(from)).toList(), Optional.empty(), 1024);
        });
        when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(invocation -> {
            identityChanged.set(fault.equals("changed-identity"));
            if (fault.equals("late-stop")) {
                stopped.set(true);
            }
            JournalPosition from = invocation.getArgument(0);
            int limit = invocation.getArgument(2);
            var entries = new ArrayList<>(f.entries.stream().filter(e -> e.position().offset().compareTo(from.offset()) >= 0).limit(limit).toList());
            if (fault.equals("duplicate")) {
                entries.set(1, entries.get(0));
            }
            return new RawJournalPage(entries, fault.equals("bad-continuation") ? Optional.of(from) : Optional.empty(), 1024);
        });
        var processed = new ArrayList<JournalPosition>();
        doAnswer(invocation -> {
            if (fault.equals("dispatch")) {
                throw new As400StreamingChangeEventSource.RowProcessingException("dispatch rejected", null);
            }
            if (fault.equals("stop-after-dispatch")) {
                stopped.set(true);
            }
            processed.add(((RawJournalPage.Entry) invocation.getArgument(2)).position());
            return null;
        }).when(handler).processOwnedEntry(any(), any(), any());
        when(handler.hasPendingBeforeImage()).thenReturn(fault.equals("cached-before") || fault.equals("idle-before"));
        var prefix = prefix(fault.equals("limited") ? 2 : 100);
        assertThat(prefix.completePosition()).isEmpty();
        if (fault.startsWith("idle")) {
            assertThat(prefix.publishNext()).isEmpty();
            assertThat(prefix.scan()).isEqualTo(Fixture.position(29));
            assertThat(prefix.completePosition().isPresent()).isEqualTo(fault.equals("idle"));
            if (fault.equals("idle")) {
                assertThat(prefix.completePosition().orElseThrow()).isEqualTo(scan.getOffset());
                verify(handler, never()).processOwnedEntry(any(), any(), any());
                // Idle polling cannot invent progress, and failure revokes the
                // previously validated candidate before any caller can reuse it.
                assertThat(prefix.publishNext()).isEmpty();
                when(transport.readAuditHistoryPage(any(), any(), anyInt())).thenReturn(new RetrieveJournal.AuditPage(List.of(), Optional.empty(), 0));
                assertThrows(Exception.class, prefix::publishNext);
                assertThat(prefix.completePosition()).isEmpty();
            }
            else {
                assertThat(processed).containsExactly(Fixture.position(29));
            }
            assertThat(durable.get("offset.event_sequence")).isEqualTo("29");
        }
        else if (List.of("complete", "limited", "cached-before", "resume-before").contains(fault)) {
            var progress = prefix.publishNext();
            assertThat(prefix.scan()).isEqualTo(processed.get(processed.size() - 1));
            assertThat(progress.isPresent()).isEqualTo(!fault.equals("cached-before"));
            assertThat(prefix.completePosition()).isEqualTo(progress);
            assertThat(durable.get("offset.event_sequence")).isEqualTo("29");
            if (fault.equals("resume-before")) {
                assertThat(processed.get(0)).isEqualTo(Fixture.position(29));
                prefix.publishNext();
                assertThat(processed.stream().filter(p -> p.equals(Fixture.position(29))).count()).isEqualTo(1);
            }
            if (fault.equals("limited")) {
                assertThat(prefix.scan()).isEqualTo(Fixture.position(31));
                prefix.publishNext();
                assertThat(processed).containsExactly(Fixture.position(30), Fixture.position(31), Fixture.position(32), Fixture.position(33));
            }
        }
        else {
            assertThrows(Exception.class, prefix::publishNext);
            assertThat(prefix.scan()).isEqualTo(Fixture.position(29));
            assertThat(durable.get("offset.event_sequence")).isEqualTo("29");
            if (!stopped.get()) {
                assertThat(prefix.completePosition()).isEmpty();
            }
            if (!List.of("dispatch", "stop-after-dispatch").contains(fault)) {
                verifyNoInteractions(handler);
            }
        }
    }
}
