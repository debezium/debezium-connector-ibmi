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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Text;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400EmptyOnlineOriginTest {
    @TempDir
    Path directory;

    private NativeFixture f;

    @BeforeEach
    void setUpNativeSource() throws Exception {
        f = new NativeFixture(directory);
    }

    @ParameterizedTest
    @ValueSource(strings = { "complete", "prior-open", "missing", "wrong-type", "continuation", "later-cut", "stop" })
    void shouldPrepareEmptyOriginOnlyWhenMarkerAndTransactionHistoryAreComplete(String fault) throws Exception {
        if (fault.equals("prior-open")) {
            f.controls.addAll(f.source.entries.stream().filter(e -> e.header().getJournalCode() == 'C'
                    && e.position().offset().compareTo(BigInteger.valueOf(40)) < 0).toList());
        }
        f.missing.set(fault.equals("missing"));
        f.wrongType.set(fault.equals("wrong-type"));
        f.continuation.set(fault.equals("continuation"));
        f.stopped.set(fault.equals("stop"));
        var pending = f.pending();
        var cut = Fixture.position(fault.equals("later-cut") ? 41 : 40);
        if (!fault.equals("complete")) {
            Class<? extends RuntimeException> expected = switch (fault) {
                case "stop" -> IllegalStateException.class;
                case "prior-open" -> As400ProspectiveHandoffReplay.PriorEpochPending.class;
                default -> IllegalArgumentException.class;
            };
            assertThrows(expected, () -> f.history().resume(pending, cut, f.config, f.policy));
            return;
        }
        var result = f.history().resume(pending, cut, f.config, f.policy);
        var checkpoint = result.replay().candidate().checkpoint();
        assertThat(checkpoint.replayFrom()).isEqualTo(cut);
        assertThat(checkpoint.completedThrough()).isEqualTo(cut);
        assertThat(checkpoint.replayInclusive()).isFalse();
        assertThat(result.replay().projection().entries()).isEmpty();
        assertThat(result.controls().inventory().openThrough()).isEmpty();
        // Preparing source evidence neither publishes nor advances the supplied legacy offset.
        assertThat(f.legacy.get("offset.event_sequence")).isEqualTo("29");
    }

    @Test
    void shouldReturnOnlyNewCdcAndPreserveDurableOffsetWhenRecoveringAnIdleOrigin() throws Exception {
        var pending = f.pending();
        var offset = f.history().resume(pending, Fixture.position(40), f.config, f.policy).replay().candidate().checkpoint().toOffset();
        for (int restart = 0; restart < 2; restart++) {
            var recovered = f.source.history().recoverOnlineCandidate(offset, f.source.journalIdentity, f.policy);
            recovered.requireStartupOrigin(offset, true);
            var idle = f.reader(recovered).readPrefix(offset, null);
            assertThat(idle.entries()).isEmpty();
            assertThat(idle.checkpoint().toOffset()).isEqualTo(offset);
        }
        var row = f.source.entry(42);
        f.rows.add(row);
        var recovered = f.source.history().recoverOnlineCandidate(offset, f.source.journalIdentity, f.policy);
        var progressed = f.reader(recovered).readPrefix(offset, null);
        assertThat(progressed.entries()).containsExactly(row);
        assertThat(progressed.checkpoint().toOffset()).isEqualTo(offset); // R cannot acknowledge F/W.
        assertThat(f.reader(recovered).readPrefix(offset, row).entries()).isEmpty();
        f.missing.set(true);
        assertThrows(IllegalArgumentException.class, () -> f.reader(recovered).readPrefix(offset, row));
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "wrong-type", "continuation" })
    void shouldRejectRestartBeforeReturningRowsWhenOriginMarkerIsUnverified(String fault) throws Exception {
        var offset = f.history().resume(f.pending(), Fixture.position(40), f.config, f.policy).replay().candidate().checkpoint().toOffset();
        f.missing.set(fault.equals("missing"));
        f.wrongType.set(fault.equals("wrong-type"));
        f.continuation.set(fault.equals("continuation"));
        f.rows.add(f.source.entry(42));
        var recovered = f.source.history().recoverOnlineCandidate(offset, f.source.journalIdentity, f.policy);
        assertThrows(IllegalArgumentException.class, () -> f.reader(recovered).readPrefix(offset, null));
    }

    @Test
    void shouldMakeBoundedProgressWhenTheOriginMarkerRequiresItsOwnRead() throws Exception {
        var offset = f.history().resume(f.pending(), Fixture.position(40), f.config, f.policy).replay().candidate().checkpoint().toOffset();
        f.rows.addAll(f.source.entries.stream().filter(e -> e.position().offset().compareTo(BigInteger.valueOf(42)) >= 0).limit(2).toList());
        var recovered = f.source.history().recoverOnlineCandidate(offset, f.source.journalIdentity, f.policy);
        var reader = new As400OperatorCutReader(f.journals, f.identities, Fixture.JOURNAL, recovered, f.transport,
                new As400OperatorCutReader.Limits(3, 2, 1_000_000));
        var cut = reader.readPrefix(offset, null);
        assertThat(cut.entries()).containsExactly(f.rows.get(0));
        assertThat(cut.tail()).isEqualTo(f.rows.get(0).position());
        assertThat(cut.checkpoint().toOffset()).isEqualTo(offset);
    }

    private static final class NativeFixture {
        final Fixture source;
        final As400CaptureFingerprint.Policy policy;
        final As400ConnectorConfig config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test")
                .with("database.dbname", "DB").with("database.schema", "DCOC1107").build());
        final As400OperatorJournalEvidence journals = mock(As400OperatorJournalEvidence.class);
        final As400JdbcNativeIdentityEvidence identities = mock(As400JdbcNativeIdentityEvidence.class);
        final RetrieveJournal transport = mock(RetrieveJournal.class);
        final List<RawJournalPage.Entry> controls = new ArrayList<>();
        final List<RawJournalPage.Entry> rows = new ArrayList<>();
        final AtomicBoolean missing = new AtomicBoolean();
        final AtomicBoolean wrongType = new AtomicBoolean();
        final AtomicBoolean continuation = new AtomicBoolean();
        final AtomicBoolean stopped = new AtomicBoolean();
        final Map<String, String> legacy = Fixture.legacyOffset(29);

        NativeFixture(Path directory) throws Exception {
            source = new Fixture(directory);
            var p = Fixture.POLICY;
            policy = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), p.output(), false,
                    p.tombstones(), p.sourcePartition());
            var observation = source.receiverDirectory(64);
            when(journals.collect()).thenReturn(observation);
            when(journals.collectFrom(any())).thenReturn(observation);
            when(identities.collect(any())).thenAnswer(i -> source.nativeCandidates(i.getArgument(0)));
            when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> {
                JournalPosition from = i.getArgument(0);
                JournalPosition to = i.getArgument(1);
                int limit = i.getArgument(2);
                var selected = rows.stream().filter(e -> e.position().offset().compareTo(from.offset()) >= 0
                        && e.position().offset().compareTo(to.offset()) <= 0).toList();
                return new RawJournalPage(selected.stream().limit(limit).toList(),
                        selected.size() > limit ? Optional.of(selected.get(limit).position()) : Optional.empty(), 1024);
            });
            when(transport.readControlHistoryPage(any(), any(), anyInt()))
                    .thenAnswer(i -> new RetrieveJournal.ControlPage(controls, Optional.empty(), 1024));
            when(transport.readAuditHistoryPage(any(), any(), anyInt())).thenAnswer(i -> {
                assertThat(i.<JournalPosition> getArgument(0)).isEqualTo(Fixture.position(40));
                assertThat(i.<JournalPosition> getArgument(1)).isEqualTo(Fixture.position(40));
                byte[] bytes = source.userMarker(40).data();
                new AS400Text(3, 37).toBytes(wrongType.get() ? "UXX" : "UDN", bytes, 98);
                var marker = new RawJournalPage.Entry(Fixture.position(40), new EntryHeaderDecoder().decode(bytes, 0), bytes);
                return new RetrieveJournal.AuditPage(missing.get() ? List.of() : List.of(marker),
                        continuation.get() ? Optional.of(Fixture.position(41)) : Optional.empty(), 1024);
            });
        }

        As400BindingHistory.Pending pending() throws Exception {
            return source.history().recordOnlineCandidate(source.journalIdentity, Fixture.position(40), source.receiverIdentity, source.definitions, policy,
                    new As400BindingHistory.Discovery(Fixture.position(29), source.receiverIdentity), legacy);
        }

        As400NativeHandoffHistory history() {
            return new As400NativeHandoffHistory(journals, identities, transport, () -> {
                if (stopped.get()) {
                    throw new IllegalStateException("Stopped");
                }
            });
        }

        As400OperatorCutReader reader(As400BindingHistory.Recovered recovered) {
            return new As400OperatorCutReader(journals, identities, Fixture.JOURNAL, recovered, transport,
                    new As400OperatorCutReader.Limits(16, 100, 1_000_000));
        }
    }
}
