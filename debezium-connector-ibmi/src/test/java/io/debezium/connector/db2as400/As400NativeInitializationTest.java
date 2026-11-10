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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Format;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.NativeObject;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Request;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

class As400NativeInitializationTest {
    @TempDir
    Path directory;

    @Test
    void acknowledgedBaselineAndFreshHistoryReadbackPrecedeEveryInitialOffset() throws Exception {
        Setup s = new Setup(directory);
        var checkpoint = s.initializer().initialize(s.session);
        assertThat(s.installed).containsExactly(checkpoint);
        var recovered = s.fixture.history().recover(checkpoint.toOffset(), checkpoint.journalIdentity(), Fixture.POLICY);
        assertThat(recovered.initialization()).contains(new As400BindingHistory.Initialization("epoch", "baseline-ack"));
        assertThat(checkpoint.replayFrom()).isEqualTo(Fixture.position(29));
        assertThat(checkpoint.completedThrough()).isEqualTo(Fixture.position(29));
        assertThat(checkpoint.replayInclusive()).isFalse();
        verify(s.census, times(4)).requireEmpty(Fixture.JOURNAL);
        verify(s.acquisition, times(3)).collect(s.requests);
        // A new instance cannot treat the existing manifest as permission to start
        // again if Kafka's source offset disappeared.
        assertThrows(Exception.class, () -> new Fixture(directory).history().recover(null, checkpoint.journalIdentity(), Fixture.POLICY));
    }

    @ParameterizedTest
    @ValueSource(strings = { "no-session", "open-unrelated-cycle", "missing-anchor", "prepare-failure", "snapshot-failure", "lost-fence", "changed-schema",
            "history-failure",
            "lost-after-history", "changed-after-history", "offset-failure" })
    void interruptedInitializationNeverReleasesAnUnverifiedCheckpoint(String fault) throws Exception {
        Setup s = new Setup(directory);
        switch (fault) {
            case "open-unrelated-cycle" -> doThrow(new IllegalStateException("open cycle on uncaptured file")).when(s.census).requireEmpty(Fixture.JOURNAL);
            case "missing-anchor" -> when(s.reader.readRawPage(any(), any(), anyInt())).thenReturn(new RawJournalPage(List.of(), Optional.empty(), 64));
            case "prepare-failure" -> doThrow(new IllegalStateException("snapshot preparation interrupted")).when(s.session).prepareBaseline(anyString(), anyList());
            case "snapshot-failure" -> doThrow(new IllegalStateException("not acknowledged")).when(s.session).acknowledgeBaseline(anyString(), any(), anyList());
            case "lost-fence" -> when(s.session.checkHeld(any(), anyList())).thenReturn("epoch", "changed");
            case "changed-schema" -> when(s.acquisition.collect(anyList())).thenReturn(s.acquired, new As400NativeBindingAcquisition.Acquired(List.of(), List.of()));
            case "history-failure", "lost-after-history", "changed-after-history" -> s.fixture.factory = () -> new As400RecoveryTestSupport.FileHistory(s.fixture.file) {
                @Override
                protected void storeRecord(io.debezium.relational.history.HistoryRecord record) {
                    super.storeRecord(record);
                    if (fault.equals("history-failure")) {
                        throw new IllegalStateException("lost history ack");
                    }
                    try {
                        if (fault.equals("lost-after-history")) {
                            when(s.session.checkHeld(any(), anyList())).thenReturn("expired");
                        }
                        else {
                            when(s.acquisition.collect(anyList())).thenReturn(new As400NativeBindingAcquisition.Acquired(List.of(), List.of()));
                        }
                    }
                    catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }
            };
            case "offset-failure" -> doThrow(new IllegalStateException("lost offset ack")).when(s.session).installCheckpoint(anyString(), any(), any());
        }
        assertThrows(Exception.class, () -> s.initializer().initialize(fault.equals("no-session") ? null : s.session));
        assertThat(s.installed).isEmpty();
        if (!fault.contains("history") && !fault.equals("offset-failure")) {
            assertThat(Files.exists(s.fixture.file)).isFalse();
        }
        if (!fault.equals("offset-failure")) {
            verify(s.session, never()).installCheckpoint(anyString(), any(), any());
        }
        assertThrows(Exception.class, () -> s.fixture.history().recover(null, s.fixture.journalIdentity, Fixture.POLICY));
    }

    @Test
    void persistedBaselineReceiptCannotBeChangedOrDroppedOnRecovery() throws Exception {
        Setup s = new Setup(directory);
        var checkpoint = s.initializer().initialize(s.session);
        String raw = Files.readString(s.fixture.file);
        Files.writeString(s.fixture.file, raw.replace("baseline-ack", "different-ack"));
        assertThrows(Exception.class, () -> s.fixture.history().recover(checkpoint.toOffset(), checkpoint.journalIdentity(), Fixture.POLICY));
    }

    static class Setup {
        final Fixture fixture;
        final As400NativeBindingAcquisition acquisition = mock(As400NativeBindingAcquisition.class);
        final As400NativeTransactionCensus census = mock(As400NativeTransactionCensus.class);
        final As400OperatorJournalEvidence journals = mock(As400OperatorJournalEvidence.class);
        final As400JdbcNativeIdentityEvidence identities = mock(As400JdbcNativeIdentityEvidence.class);
        final As400OperatorTableEvidence tables = mock(As400OperatorTableEvidence.class);
        final RetrieveJournal reader = mock(RetrieveJournal.class);
        final As400NativeInitialization.Session session = mock(As400NativeInitialization.Session.class);
        final List<As400Checkpoint> installed = new ArrayList<>();
        final List<Request> requests;
        final As400NativeBindingAcquisition.Acquired acquired;

        Setup(Path directory) throws Exception {
            fixture = new Fixture(directory);
            List<Observation> observations = new ArrayList<>();
            for (var d : fixture.definitions) {
                var id = d.identity();
                String base = "/QSYS.LIB/" + id.tableId().schema() + ".LIB/" + id.physicalFile() + ".FILE";
                String[] f = id.recordFormat().split(":");
                var file = new NativeObject(new Target(base, Kind.FILE), new Identity(BigInteger.ONE, BigInteger.valueOf(10 + observations.size()), BigInteger.ONE),
                        id.fileJournalId());
                var member = new NativeObject(new Target(base + "/" + id.member() + ".MBR", Kind.MEMBER),
                        new Identity(BigInteger.ONE, BigInteger.valueOf(20 + observations.size()), BigInteger.ONE), id.memberJournalId());
                observations.add(new Observation(new Request(Fixture.JOURNAL, id.tableId().schema(), id.physicalFile()), id.tableId(), id.member(), file, member,
                        new Format(f[0], f[1], Integer.parseInt(f[2]), Integer.parseInt(f[3]), Boolean.parseBoolean(f[4]))));
            }
            requests = observations.stream().map(Observation::request).toList();
            acquired = new As400NativeBindingAcquisition.Acquired(observations, fixture.definitions);
            when(acquisition.collect(anyList())).thenReturn(acquired);
            when(tables.collect(anyList())).thenReturn(observations);
            AtomicBoolean prepared = new AtomicBoolean();
            doAnswer(i -> {
                prepared.set(true);
                return null;
            }).when(session).prepareBaseline(anyString(), anyList());
            when(journals.collect()).thenAnswer(i -> {
                assertThat(prepared).isTrue();
                return new As400OperatorJournalEvidence.Observation(Fixture.JOURNAL, Fixture.RECEIVER,
                        List.of(new As400OperatorJournalEvidence.Receiver(Fixture.RECEIVER, "ATTACHED", BigInteger.valueOf(29), BigInteger.valueOf(29),
                                Optional.empty(), Optional.empty(), "NO", "YES", "YES", "NO", "NO")));
            });
            when(identities.collect(any()))
                    .thenAnswer(i -> new Candidates(((List<Target>) i.getArgument(0)).stream().collect(Collectors.toMap(t -> t, fixture.nativeObjects::get))));
            when(reader.readRawPage(any(), any(), anyInt()))
                    .thenReturn(new RawJournalPage(fixture.entries.stream().filter(e -> e.position().offset().intValueExact() == 29).toList(), Optional.empty(), 65536));
            when(session.checkHeld(any(), anyList())).thenReturn("epoch");
            when(session.acknowledgeBaseline(anyString(), any(), anyList())).thenReturn("baseline-ack");
            doAnswer(i -> {
                As400Checkpoint cp = i.getArgument(1);
                assertThat(Files.readString(fixture.file)).contains("initialization.baseline", "baseline-ack");
                assertThat(fixture.history().recover(cp.toOffset(), cp.journalIdentity(), Fixture.POLICY).definitions()).isEqualTo(fixture.definitions);
                installed.add(cp);
                return null;
            }).when(session).installCheckpoint(anyString(), any(), any());
        }

        As400NativeInitialization initializer() {
            return new As400NativeInitialization(acquisition, census, journals, identities, tables, reader,
                    new As400OperatorCutReader.Limits(5, 100, 1_000_000), requests, Fixture.POLICY, fixture.history());
        }
    }
}
