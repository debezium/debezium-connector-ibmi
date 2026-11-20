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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Text;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/** Generated scale/fault scenarios use captured header layouts, not new native evidence. */
class As400HandoffWorkTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldFinishLegacyMetadataBeforeHandoffEvenWhenItsOpenTableIsExcluded(boolean metadata) throws Exception {
        var fixture = new Fixture(directory);
        var config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                .with("database.schema", "DCOC1107").with("table.include.list", "DCOC1107\\.A")
                .with("provide.transaction.metadata", metadata).with("skipped.operations", "none").build());
        var order = new As400JournalOrder(List.of(Fixture.RECEIVER), Map.of(Fixture.RECEIVER, fixture.receiverIdentity),
                Map.of(Fixture.RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(29), BigInteger.valueOf(64))));
        var evidence = new As400HandoffReplay.Evidence(fixture.entries, fixture.definitions, order, fixture.journalIdentity,
                "v1:" + "a".repeat(64), Fixture.position(40), Fixture.position(42), Set.of(BigInteger.valueOf(31)), Fixture.POLICY.sourcePartition());
        if (metadata) {
            var pending = assertThrows(As400HandoffReplay.SelectedPrefixPending.class,
                    () -> As400HandoffReplay.prepare(evidence, Fixture.position(42), config));
            assertThat(pending).hasMessageContaining("Open legacy transaction metadata");
            assertThat(pending.cycles()).containsExactly(BigInteger.valueOf(31));
        }
        else {
            assertThat(As400HandoffReplay.prepare(evidence, Fixture.position(42), config).openCycles()).containsExactly(BigInteger.valueOf(31));
        }
        // The terminal is native evidence, not an elapsed timeout or a new BEGIN.
        for (int terminal : List.of(43, 64)) {
            assertThat(As400HandoffReplay.prepare(evidence, Fixture.position(terminal), config).openCycles()).isEmpty();
        }
        assertThat(Files.exists(fixture.file)).isFalse();
    }

    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReconstructMoreThanTwentyThousandRowsAndControlsAcrossManyPagesWithoutPublishing(boolean metadata) throws Exception {
        var source = new Source(directory, 11_000, false, "none", metadata);
        byte[] history = Files.readAllBytes(source.fixture.file);
        List<RawJournalPage.Entry> borrowed;
        As400Checkpoint saved;
        try (var reader = source.reader()) {
            var prepared = reader.resume(source.pending, source.cut(), source.config, source.policy);
            borrowed = prepared.replay().projection().entries();
            assertThat(borrowed).hasSize(33_000);
            assertThat(prepared.controls().entries()).hasSize(22_000);
            assertThat(prepared.controls().inventory().terminals()).hasSize(11_000);
            assertThat(prepared.replay().candidate().openCycles()).isEmpty();
            saved = prepared.replay().candidate().checkpoint();
            assertThat(saved.completedThrough()).isEqualTo(source.cut());
            assertThat(saved.replayFrom()).isEqualTo(source.cut());
            assertThat(saved.replayInclusive()).isFalse();
        }
        assertThrows(IllegalStateException.class, borrowed::size);
        assertThat(source.pages.get()).isGreaterThan(128);
        assertThat(Files.readAllBytes(source.fixture.file)).isEqualTo(history);
        source.assertClean();
        // Scratch is disposable. Fresh native reads rebuild exactly the same F/W.
        try (var replacement = source.reader()) {
            assertThat(replacement.resume(source.pending, source.cut(), source.config, source.policy).replay().candidate().checkpoint()).isEqualTo(saved);
        }
        source.assertClean();
    }

    @ParameterizedTest
    @ValueSource(strings = { "read-error", "repeated", "continuation", "foreign", "missing-control", "late-corrupt-row", "stop", "raw-budget", "index-budget" })
    void shouldRejectIncompleteOrFailedHistoryWithoutChangingTheDurableMigrationState(String fault) throws Exception {
        var source = new Source(directory, 1800, false, fault);
        byte[] history = Files.readAllBytes(source.fixture.file);
        try (var reader = source.reader()) {
            Exception failure = assertThrows(Exception.class, () -> reader.resume(source.pending, source.cut(), source.config, source.policy));
            assertThat(failure).isNotInstanceOf(As400ProspectiveHandoffReplay.PriorEpochPending.class)
                    .isNotInstanceOf(As400HandoffReplay.SelectedPrefixPending.class);
            if (fault.equals("raw-budget")) {
                assertThat(failure).isInstanceOf(As400JournalCapture.CaptureException.class);
            }
            if (fault.equals("index-budget")) {
                assertThat(failure).isInstanceOf(As400SpillStore.SpillException.class);
            }
            switch (fault) {
                case "read-error" -> assertThat(failure).isInstanceOf(java.io.IOException.class).hasMessageContaining("Transport interrupted");
                case "repeated", "foreign" -> assertThat(failure).hasMessageContaining("Repeated, foreign or unordered");
                case "continuation" -> assertThat(failure).hasMessageContaining("Invalid native handoff continuation");
                case "missing-control" -> assertThat(failure).hasMessageContaining("C projections disagree");
                case "late-corrupt-row" -> assertThat(failure).isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
                case "stop" -> assertThat(failure).isInstanceOf(IllegalStateException.class).hasMessageContaining("Stopped");
            }
        }
        assertThat(Files.readAllBytes(source.fixture.file)).isEqualTo(history);
        source.assertClean();
    }

    @ParameterizedTest
    @ValueSource(strings = { "complete", "prior-open", "selected-prefix", "missing-start", "missing-overlap", "changed-identity", "lifecycle-overlap" })
    void shouldPreservePriorSchemaAndInFlightTransactionGuardsWithDiskBackedHistory(String fault) throws Exception {
        var fixture = new Fixture(directory);
        Path scratch = Files.createDirectory(directory.resolve("scratch"));
        var config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                .with("database.schema", "DCOC1107").with("provide.transaction.metadata", false).with("skipped.operations", "none").build());
        var order = new As400JournalOrder(List.of(Fixture.RECEIVER), Map.of(Fixture.RECEIVER, fixture.receiverIdentity),
                Map.of(Fixture.RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(29), BigInteger.valueOf(64))));
        Runnable check = () -> {
        };
        try (var work = new As400HandoffWork(As400HandoffWork.settings(scratch, 8L * 1024 * 1024), As400ContinuousRuntime.Limits.experimental(),
                () -> new As400JournalCapture(scratch, 8L * 1024 * 1024, check, check), check)) {
            var rows = work.capture();
            var controls = work.capture();
            int origin = fault.equals("lifecycle-overlap") ? 37 : 42;
            int cut = fault.equals("prior-open") || fault.equals("changed-identity") ? 42 : fault.equals("selected-prefix") ? 56 : 64;
            for (var entry : fixture.entries) {
                int seq = entry.position().offset().intValueExact();
                if (entry.header().getJournalCode() == 'C' && !(fault.equals("missing-start") && seq == 31)) {
                    controls.append(entry);
                }
                if (seq >= origin && !(fault.equals("missing-overlap") && seq == 43)) {
                    if (fault.equals("changed-identity") && seq == 42) {
                        byte[] raw = entry.data();
                        raw[197] ^= 1;
                        entry = new RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(raw, 0), raw);
                    }
                    rows.append(entry);
                }
            }
            var evidence = new As400HandoffReplay.Evidence(rows.seal(), fixture.definitions, order, fixture.journalIdentity,
                    "v1:" + "a".repeat(64), Fixture.position(origin), Fixture.position(origin), Set.of(BigInteger.valueOf(31)),
                    Fixture.POLICY.sourcePartition(), work);
            var retainedControls = controls.seal();
            if (fault.equals("complete")) {
                var result = As400ProspectiveHandoffReplay.prepare(evidence, retainedControls, Fixture.position(origin), Fixture.position(cut), config);
                assertThat(result.candidate().checkpoint().toOffset()).containsEntry("replay_from.sequence", "46").containsEntry("completed_through.sequence", "59");
                assertThat(result.completedPriorCycles()).containsExactly(BigInteger.valueOf(31));
            }
            else {
                Exception failure = assertThrows(Exception.class,
                        () -> As400ProspectiveHandoffReplay.prepare(evidence, retainedControls, Fixture.position(origin), Fixture.position(cut), config));
                switch (fault) {
                    case "prior-open" -> assertThat(failure).isInstanceOf(As400ProspectiveHandoffReplay.PriorEpochPending.class);
                    case "selected-prefix" -> assertThat(failure).isInstanceOf(As400HandoffReplay.SelectedPrefixPending.class);
                    case "missing-start" -> assertThat(failure).hasMessageContaining("Missing retained original cycle start");
                    case "missing-overlap" -> assertThat(failure).hasMessageContaining("C projections disagree");
                    case "changed-identity" -> assertThat(failure).isInstanceOf(As400SourceNormalizer.NormalizationException.class);
                    case "lifecycle-overlap" -> assertThat(failure).hasMessageContaining("Lifecycle overlaps");
                }
            }
        }
        try (var files = Files.list(scratch)) {
            assertThat(files).isEmpty();
        }
        assertThat(fixture.file).doesNotExist();
    }

    @Test
    void shouldDecodeAndPrepareMoreThanSixtyFourMegabytesOfHistoryWithASixtyFourMegabyteHeap() throws Exception {
        Path log = directory.resolve("large-history.log");
        Process child = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx64m", "-ea", "-cp",
                System.getProperty("java.class.path"), LargeHistory.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertThat(child.waitFor(120, TimeUnit.SECONDS)).as("child completed: %s", log).isTrue();
            assertThat(child.exitValue()).as(Files.readString(log)).isZero();
        }
        finally {
            child.destroyForcibly();
        }
    }

    public static final class LargeHistory {
        public static void main(String[] args) throws Exception {
            var source = new Source(Path.of(args[0]), 2600, true, "none");
            try (var reader = source.reader()) {
                var prepared = reader.resume(source.pending, source.cut(), source.config, source.policy);
                assertThat(prepared.replay().projection().entries()).hasSize(2600);
                assertThat(prepared.replay().candidate().checkpoint().completedThrough()).isEqualTo(source.cut());
                assertThat(prepared.replay().candidate().openCycles()).isEmpty();
                assertThat(source.pages.get()).isGreaterThan(128);
            }
            source.assertClean();
        }
    }

    private static final class Source {
        final Fixture fixture;
        final Path scratch;
        final int transactions;
        final boolean wide;
        final String fault;
        final AtomicInteger pages = new AtomicInteger();
        final AtomicBoolean stopped = new AtomicBoolean();
        final As400ConnectorConfig config;
        final As400CaptureFingerprint.Policy policy;
        final As400BindingHistory.Pending pending;
        final As400OperatorJournalEvidence journals = mock(As400OperatorJournalEvidence.class);
        final As400JdbcNativeIdentityEvidence identities = mock(As400JdbcNativeIdentityEvidence.class);
        final RetrieveJournal transport = mock(RetrieveJournal.class);
        final EntryHeaderDecoder headers = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID);

        Source(Path directory, int transactions, boolean wide, String fault) throws Exception {
            this(directory, transactions, wide, fault, false);
        }

        Source(Path directory, int transactions, boolean wide, String fault, boolean metadata) throws Exception {
            config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test")
                    .with("database.dbname", "DB").with("database.schema", "DCOC1107")
                    .with("provide.transaction.metadata", metadata).with("skipped.operations", "none").build());
            this.transactions = transactions;
            this.wide = wide;
            this.fault = fault;
            fixture = new Fixture(directory);
            scratch = Files.createDirectory(directory.resolve("scratch"));
            if (wide) {
                var sourceFile = fixture.entry(42).header().getFile();
                var definition = fixture.definitions.stream().filter(d -> d.identity().physicalFile().equals(sourceFile)).findFirst().orElseThrow();
                var old = definition.identity();
                var format = old.recordFormat().split(":");
                var identity = new TableIdentity(old.tableId(), old.physicalFile(), old.member(), old.fileJournalId(), old.memberJournalId(),
                        format[0] + ":" + format[1] + ":32768:1:false");
                var table = Table.editor().tableId(old.tableId()).addColumn(Column.editor().name("BYTES").position(1)
                        .jdbcType(Types.BINARY).type("BINARY").length(32768).optional(false).create()).create();
                fixture.definitions.set(fixture.definitions.indexOf(definition),
                        new As400BindingDefinition(identity, table, List.of(new As400BindingDefinition.Codec("BINARY", 32768, 0, 0))));
            }
            var p = Fixture.POLICY;
            policy = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), p.output(), metadata,
                    p.tombstones(), p.sourcePartition());
            pending = fixture.history().recordOnlineCandidate(fixture.journalIdentity, Fixture.position(100), fixture.receiverIdentity,
                    fixture.definitions, policy, new As400BindingHistory.Discovery(Fixture.position(29), fixture.receiverIdentity), Fixture.legacyOffset(100));
            var directorySample = fixture.receiverDirectory(cut().offset().longValueExact());
            when(journals.collectFrom(any())).thenReturn(directorySample);
            when(identities.collect(any())).thenAnswer(i -> fixture.nativeCandidates(i.getArgument(0)));
            when(transport.readRawPage(any(), any(), anyInt())).thenAnswer(i -> page(i.getArgument(0), i.getArgument(1), i.getArgument(2), false));
            when(transport.readControlHistoryPage(any(), any(), anyInt())).thenAnswer(i -> {
                var page = page(i.getArgument(0), i.getArgument(1), i.getArgument(2), true);
                return new RetrieveJournal.ControlPage(page.entries(), page.continuation(), page.bytesReturned());
            });
        }

        JournalPosition cut() {
            return Fixture.position(100 + (wide ? transactions : transactions * 3L));
        }

        private RawJournalPage page(JournalPosition from, JournalPosition through, int requested, boolean controls) throws Exception {
            int page = pages.incrementAndGet();
            if (fault.equals("read-error") && page == 3) {
                throw new java.io.IOException("Transport interrupted");
            }
            if (fault.equals("stop") && page == 3) {
                stopped.set(true);
            }
            var result = new ArrayList<RawJournalPage.Entry>();
            int bytes = 0;
            JournalPosition next = null;
            int limit = Math.min(requested, wide ? 17 : 100);
            for (long seq = Math.max(101, from.offset().longValueExact()); seq <= through.offset().longValueExact(); seq++) {
                if (controls && (wide || (seq - 101) % 3 == 1 || fault.equals("missing-control") && seq == 103)) {
                    continue;
                }
                if (result.size() == limit) {
                    next = Fixture.position(seq);
                    break;
                }
                var entry = entry(seq);
                if (page == 3 && result.isEmpty() && fault.equals("repeated")) {
                    entry = entry(seq - 1);
                }
                if (page == 3 && result.isEmpty() && fault.equals("foreign")) {
                    entry = new RawJournalPage.Entry(new JournalPosition(entry.position().offset(),
                            new io.debezium.ibmi.db2.journal.retrieve.JournalReceiver("OTHER", "LIB")), entry.header(), entry.data());
                }
                result.add(entry);
                bytes += entry.data().length;
            }
            if (page == 3 && next != null && fault.equals("continuation")) {
                next = result.get(result.size() - 1).position();
            }
            return new RawJournalPage(result, Optional.ofNullable(next), bytes);
        }

        private RawJournalPage.Entry entry(long seq) {
            int kind = wide ? 1 : (int) ((seq - 101) % 3);
            byte[] raw = fixture.entry(kind == 0 ? 31 : kind == 1 ? 42 : 43).data();
            ByteBuffer.wrap(raw).putLong(24, seq).putLong(64, wide ? 0 : seq - kind);
            if (kind == 1) {
                ByteBuffer.wrap(raw).putLong(56, seq);
                if (!wide) {
                    byte[] start = fixture.entry(31).data();
                    System.arraycopy(start, 40, raw, 40, 8);
                    System.arraycopy(start, 101, raw, 101, 26);
                }
                if (wide) {
                    int data = ByteBuffer.wrap(raw).getInt(8);
                    raw = Arrays.copyOf(raw, data + 16 + 32768);
                    new AS400Text(5, 37).toBytes("32768", raw, data);
                    Arrays.fill(raw, data + 16, raw.length, (byte) seq);
                }
                if (fault.equals("late-corrupt-row") && seq == cut().offset().longValueExact() - 1) {
                    raw[197] ^= 1;
                }
            }
            return new RawJournalPage.Entry(Fixture.position(seq), headers.decode(raw, 0), raw);
        }

        As400NativeHandoffHistory reader() {
            long rawBudget = fault.equals("raw-budget") ? 65536 : 256L * 1024 * 1024;
            long indexBudget = fault.equals("index-budget") ? 65536 : 256L * 1024 * 1024;
            Runnable check = () -> {
                if (stopped.get()) {
                    throw new IllegalStateException("Stopped");
                }
            };
            var limits = new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000),
                    new As400JournalLifecycle.Limits(1000, 1_000_000, 16, 128, 1000, 1000), 4002);
            var work = new As400HandoffWork(As400HandoffWork.settings(scratch, indexBudget), limits,
                    () -> new As400JournalCapture(scratch, rawBudget, check, () -> {
                    }), check);
            return new As400NativeHandoffHistory(journals, identities, transport, check, work);
        }

        void assertClean() throws Exception {
            try (var files = Files.list(scratch)) {
                assertThat(files).isEmpty();
            }
        }
    }
}
