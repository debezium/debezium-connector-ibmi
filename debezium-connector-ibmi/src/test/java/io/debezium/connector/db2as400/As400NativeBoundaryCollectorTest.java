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
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/** Replays native bytes; only the external acquisitions and pulse execution are controlled. */
class As400NativeBoundaryCollectorTest {
    @org.junit.jupiter.api.io.TempDir
    java.nio.file.Path scratch;

    @ParameterizedTest
    @ValueSource(strings = { "complete", "gap", "repeated", "backward", "empty", "short", "overshoot", "foreign", "failure", "stopped", "close-failure" })
    void shouldPageLargeAuditsWithoutLosingContinuityOrLeakingScratch(String mode) throws Exception {
        var f = new Fixture("");
        f.extraRows = 4096;
        f.pageSize = 400;
        f.pageFault = mode;
        var released = new java.util.concurrent.atomic.AtomicInteger();
        f.captures = () -> new As400JournalCapture(scratch, 64 * 1024 * 1024, () -> {
            if (f.stopped.get()) {
                throw new IllegalStateException("Stopped");
            }
        }, released::incrementAndGet);
        try (var pulses = f.pulses()) {
            if (mode.equals("complete")) {
                try (var candidate = f.collector.collect().orElseThrow()) {
                    assertThat(candidate.audit().entries()).hasSize(4124);
                    assertThat(candidate.origin().offset()).isEqualTo(BigInteger.valueOf(72));
                    assertThat(candidate.historyFloor().offset()).isEqualTo(BigInteger.valueOf(46));
                    assertThat(candidate.audit().entries().get(4123).position().offset()).isEqualTo(BigInteger.valueOf(4180));
                    assertThat(f.events.stream().filter("audit"::equals).count()).isEqualTo(11);
                    try (var files = java.nio.file.Files.list(scratch)) {
                        assertThat(files.count()).isEqualTo(1);
                    }
                }
            }
            else {
                assertThrows(Exception.class, f.collector::collect);
            }
        }
        assertThat(released).hasValue(1);
        try (var files = java.nio.file.Files.list(scratch)) {
            assertThat(files).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "complete", "paged", "prepare", "missing-row-replay", "metadata", "missing-start", "missing-overlap", "empty", "backward-continuation",
            "changed-identity", "resume", "resume-missing-floor", "resume-changed-identity", "resume-missing-start",
            "stopped" })
    void nativeControlReadMustReconstructTheDiscoveredFloorAndMatchIndependentAudit(String fault) throws Exception {
        var f = new Fixture("");
        As400NativeBoundaryCollector.Candidate candidate;
        try (var ignored = f.pulses()) {
            candidate = f.collector.collect().orElseThrow();
        }
        var journals = mock(As400OperatorJournalEvidence.class);
        when(journals.collectFrom(any())).thenReturn(f.directory(84));
        var identities = mock(As400JdbcNativeIdentityEvidence.class);
        var expected = candidate.window().before().identities();
        when(identities.collect(any())).thenReturn(fault.equals("changed-identity")
                ? new As400JdbcNativeIdentityEvidence.Candidates(java.util.Map.of())
                : expected);
        var entries = new ArrayList<RawJournalPage.Entry>();
        try (var stream = getClass().getResourceAsStream("/native-transactions/prospective-boundary.json")) {
            for (var item : new ObjectMapper().readTree(stream).path("older-controls").path("entries")) {
                var position = As400OnlineHandoff.position(item.path("position"));
                if (position.offset().compareTo(candidate.historyFloor().offset()) >= 0 && position.offset().intValueExact() <= 84) {
                    byte[] bytes = Base64.getDecoder().decode(item.path("raw").asText());
                    entries.add(new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(bytes, 0), bytes));
                }
            }
        }
        if (fault.equals("missing-start") || fault.equals("resume-missing-start")) {
            entries.remove(0);
        }
        if (fault.equals("missing-overlap")) {
            entries.remove(entries.size() - 1);
        }
        if (fault.equals("empty")) {
            entries.clear();
        }
        var reader = mock(RetrieveJournal.class);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        when(reader.readControlHistoryPage(any(), any(), anyInt())).thenAnswer(invocation -> {
            if (fault.equals("paged") && calls.incrementAndGet() == 1) {
                return new RetrieveJournal.ControlPage(entries.subList(0, 2), Optional.of(entries.get(2).position()), 1000);
            }
            return new RetrieveJournal.ControlPage(fault.equals("paged") ? entries.subList(2, entries.size()) : entries,
                    fault.equals("backward-continuation") ? Optional.of(candidate.historyFloor()) : Optional.empty(), 4000);
        });
        var history = new As400NativeHandoffHistory(journals, identities, reader, () -> {
            if (fault.equals("stopped")) {
                throw new IllegalStateException("Stopped");
            }
        });
        if (List.of("prepare", "missing-row-replay", "metadata").contains(fault) || fault.startsWith("resume")) {
            var rows = candidate.audit().entries().stream().filter(e -> e.header().getJournalCode() == 'R'
                    && e.position().offset().intValueExact() > 72 && e.position().offset().intValueExact() <= 83).toList();
            when(reader.readRawPage(any(), any(), anyInt())).thenReturn(new RawJournalPage(fault.equals("missing-row-replay") ? rows.subList(1, rows.size()) : rows,
                    Optional.empty(), 10000));
            var config = new As400ConnectorConfig(io.debezium.config.Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                    .with("database.schema", "DCNC0966").with("skipped.operations", "none").with("provide.transaction.metadata", fault.equals("metadata")).build());
            var policy = new As400CaptureFingerprint.Policy(java.util.Map.of(), Set.of(), java.util.Map.of("version", "strict-v1"),
                    java.util.Map.of("version", "native-v1"), java.util.Map.of("version", "test-v1"), false, true, java.util.Map.of("server", "test"));
            var cut = new JournalPosition(BigInteger.valueOf(83), candidate.origin().receiver());
            if (fault.startsWith("resume")) {
                var pending = new As400BindingHistory.Pending(candidate.window().journalToken(), candidate.origin(),
                        fault.equals("resume-changed-identity") ? "changed-source" : candidate.window().receiverTokens().get(candidate.origin().receiver()),
                        "v1:" + "a".repeat(64), candidate.bindings().definitions(),
                        fault.equals("resume-missing-floor") ? Optional.empty()
                                : Optional.of(new As400BindingHistory.Discovery(candidate.historyFloor(),
                                        candidate.window().receiverTokens().get(candidate.historyFloor().receiver()))),
                        Optional.empty());
                if (fault.equals("resume")) {
                    var resumed = history.resume(pending, cut, config, policy);
                    assertThat(resumed.replay().candidate().checkpoint().captureFingerprint()).isEqualTo(pending.fingerprint());
                    assertThat(resumed.replay().candidate().checkpoint().completedThrough()).isEqualTo(cut);
                    assertThat(resumed.replay().candidate().scan()).isEqualTo(cut);
                    assertThat(resumed.controls().inventory().terminals()).containsKey(BigInteger.valueOf(46));
                    verify(reader).readControlHistoryPage(org.mockito.ArgumentMatchers.eq(candidate.historyFloor()),
                            org.mockito.ArgumentMatchers.eq(cut), anyInt());
                }
                else {
                    assertThrows(Exception.class, () -> history.resume(pending, cut, config, policy));
                    if (fault.equals("resume-missing-floor")) {
                        org.mockito.Mockito.verifyNoInteractions(reader, journals, identities);
                    }
                }
            }
            else if (fault.equals("metadata")) {
                assertThrows(IllegalArgumentException.class, () -> history.prepare(candidate, cut, config, policy));
                org.mockito.Mockito.verifyNoInteractions(reader, journals, identities);
            }
            else if (fault.equals("missing-row-replay")) {
                assertThrows(IllegalArgumentException.class, () -> history.prepare(candidate, cut, config, policy));
            }
            else {
                var prepared = history.prepare(candidate, cut, config, policy);
                assertThat(prepared.replay().candidate().checkpoint().completedThrough()).isEqualTo(cut);
                assertThat(prepared.replay().candidate().scan()).isEqualTo(cut);
                assertThat(prepared.replay().candidate().openCycles()).isEmpty();
            }
        }
        else if (fault.equals("complete") || fault.equals("paged")) {
            var observed = history.collect(candidate, candidate.window().through());
            assertThat(observed.inventory().openAtLower()).isEmpty();
            assertThat(observed.inventory().openThrough()).isEmpty();
            assertThat(observed.inventory().terminals()).containsKey(BigInteger.valueOf(46));
            assertThat(observed.entries()).hasSize(entries.size());
        }
        else {
            assertThrows(Exception.class, () -> history.collect(candidate, candidate.window().through()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "writers-outside-markers", "gap-before-opening", "gap-after-closing", "admin-F/OP", "admin-F/CL", "admin-F/MS", "admin-D/DH",
            "admin-D/GT", "admin-R/IL", "admin-F/SS", "admin-D/DW" })
    void nativeBytesBindTheObservationWithoutCreatingAPublicationCheckpoint(String mode) throws Exception {
        var f = new Fixture(mode);
        try (var pulses = f.pulses()) {
            var candidate = f.collector.collect().orElseThrow();
            assertThat(candidate.origin().offset()).isEqualTo(BigInteger.valueOf(72));
            assertThat(candidate.historyFloor().offset()).isLessThan(candidate.origin().offset());
            assertThat(candidate.program().targets()).hasSize(4);
            assertThat(candidate.bindings()).isEqualTo(f.bindings);
            verify(pulses.constructed().get(0)).finish(candidate.program());
            verify(pulses.constructed().get(0)).close();
            assertThat(f.events).containsSubsequence("bindings", "pulse-returned", "bindings", "audit");
        }
        f.connections.forEach(c -> {
            try {
                verify(c).close();
            }
            catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    @Test
    void returnedRefusalDoesNotReadOrAuthorizeAnyHistory() throws Exception {
        var f = new Fixture("refusal");
        try (var pulses = f.pulses()) {
            assertThat(f.collector.collect()).isEmpty();
            verify(pulses.constructed().get(0)).close();
            assertThat(f.events).contains("pulse-returned").doesNotContain("audit");
            org.mockito.Mockito.verifyNoInteractions(f.reader);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "scope-before", "scope-after", "bindings-after", "missing-row", "continuation", "stop-after-pulse", "missing-marker", "foreign-marker",
            "admin-identity", "admin-transaction", "admin-restore" })
    void incompleteObservationCannotBecomeACandidate(String fault) throws Exception {
        var f = new Fixture(fault);
        try (var pulses = f.pulses()) {
            assertThrows(Exception.class, f.collector::collect);
            verify(pulses.constructed().get(0)).close();
            if (List.of("scope-before", "scope-after", "bindings-after", "stop-after-pulse").contains(fault)) {
                assertThat(f.events).doesNotContain("audit");
            }
        }
    }

    private static final class Fixture {
        final As400NativeBoundaryCollector collector;
        final RetrieveJournal reader = mock(RetrieveJournal.class);
        final List<String> events = new ArrayList<>();
        final List<Connection> connections = new ArrayList<>();
        final As400NativeBindingAcquisition.Acquired bindings;
        final JsonNode boundary;
        final As400NativeJournalWindow.Window window;
        final String fault;
        final AtomicBoolean stopped = new AtomicBoolean();
        As400ShortBoundaryProgram program;
        int extraRows;
        int pageSize = 1000;
        String pageFault = "";
        java.util.concurrent.Callable<As400JournalCapture> captures = () -> new As400JournalCapture(
                java.nio.file.Path.of(System.getProperty("java.io.tmpdir")), 64 * 1024 * 1024, () -> {
                }, () -> {
                });

        Fixture(String fault) throws Exception {
            this.fault = fault;
            try (var stream = getClass().getResourceAsStream("/native-transactions/prospective-boundary.json")) {
                boundary = As400OnlineFenceEvidenceTest.withUnverifiedMetadataForReplay(new ObjectMapper().readTree(stream).path("boundary"));
            }
            var metadata = As400OnlineFenceEvidence.validateOffLock(boundary.path("before"));
            bindings = metadata.tables();
            window = As400OnlineNativeEvidence.restore(boundary.path("raw").path("native_identity_evidence")).nativeWindow();
            var journal = window.before().directory().journal();
            var scopes = mock(As400JournalFileScope.class);
            var scope = new As400JournalFileScope.Scope(journal, bindings.observations().stream().map(o -> o.request()).toList(), Set.of(journal.library()));
            var changed = new As400JournalFileScope.Scope(journal, scope.files().subList(1, scope.files().size()), scope.journaledLibraries());
            var scopeCount = new java.util.concurrent.atomic.AtomicInteger();
            when(scopes.collect(any())).thenAnswer(invocation -> {
                int count = scopeCount.incrementAndGet();
                return (fault.equals("scope-before") && count == 2 || fault.equals("scope-after") && count == 3) ? changed : scope;
            });
            var acquisition = mock(As400NativeBindingAcquisition.class);
            var acquisitions = new java.util.concurrent.atomic.AtomicInteger();
            when(acquisition.collect(scope.files())).thenAnswer(invocation -> {
                events.add("bindings");
                return fault.equals("bindings-after") && acquisitions.incrementAndGet() == 2
                        ? new As400NativeBindingAcquisition.Acquired(bindings.observations().subList(1, 4), bindings.definitions().subList(1, 4))
                        : bindings;
            });
            var journals = mock(As400OperatorJournalEvidence.class);
            when(journals.collect()).thenReturn(directory(fault.equals("gap-before-opening") ? 55 : 57), directory(58), directory(60), directory(71), directory(73));
            when(journals.collectFrom(any()))
                    .thenAnswer(i -> directory(extraRows + (fault.equals("gap-after-closing") ? 86 : fault.equals("writers-outside-markers") ? 85 : 84)));
            var identities = mock(As400JdbcNativeIdentityEvidence.class);
            when(identities.collect(any())).thenReturn(window.before().identities());
            var pending = mock(As400PendingReceiverEvidence.class);
            var r = directory(59).receivers().get(0);
            when(pending.collect()).thenReturn(new As400PendingReceiverEvidence.Sample(journal, r.name(), List.of(
                    new As400PendingReceiverEvidence.Receiver(r.name(), r.status(), r.first(), r.last(), r.previous(), r.next(), false))));
            when(reader.readAuditHistoryPage(any(), any(), anyInt())).thenAnswer(invocation -> {
                events.add("audit");
                var entries = new ArrayList<RawJournalPage.Entry>();
                int marker = 0;
                for (var item : boundary.path("raw").path("entries")) {
                    byte[] bytes = Base64.getDecoder().decode(item.path("raw").asText());
                    var header = new EntryHeaderDecoder().decode(bytes, 0);
                    if (header.getJournalCode() == 'U') {
                        String payload = switch (marker++) {
                            case 0 -> "DCGB1 " + program.nonce() + " A";
                            case 1 -> program.payload();
                            default -> "DCGB1 " + program.nonce() + " Z " + program.scopeDigest();
                        };
                        byte[] encoded = payload.getBytes(Charset.forName("Cp037"));
                        System.arraycopy(encoded, 0, bytes, header.getEntrySpecificDataOffset() + 16, encoded.length);
                        if (fault.equals("missing-marker") && marker == 3 || fault.equals("foreign-marker") && marker == 2) {
                            bytes[header.getEntrySpecificDataOffset() + 16] ^= 1;
                        }
                    }
                    var position = As400OnlineHandoff.position(item.path("position"));
                    entries.add(new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(bytes, 0), bytes));
                }
                if (fault.equals("missing-row")) {
                    entries.remove(5);
                }
                if (fault.startsWith("admin-")) {
                    // Synthetic placement of captured administrative bytes in
                    // the A/Z bracket. This is a parser/identity regression,
                    // not evidence that this altered native interval occurred.
                    var original = entries.get(2);
                    var type = fault.substring(6);
                    boolean corrupt = type.equals("identity") || type.equals("transaction") || type.equals("restore");
                    try (var stream = getClass().getResourceAsStream("/native-transactions/"
                            + (type.equals("R/IL") ? "record-limit.json"
                                    : List.of("F/SS", "D/DW").contains(type) ? "save-while-active.json" : "administrative-entries.json"))) {
                        var captured = new ObjectMapper().readTree(stream).path("entries");
                        var selected = java.util.stream.StreamSupport.stream(captured.spliterator(), false)
                                .filter(e -> e.path("type").asText().equals(corrupt ? "F/OP" : type)).findFirst().orElseThrow();
                        byte[] bytes = java.util.HexFormat.of().parseHex(selected.path("hex").asText());
                        var definition = bindings.definitions().get(0).identity();
                        var text = new com.ibm.as400.access.AS400Text(10, 37);
                        text.toBytes(definition.physicalFile(), bytes, 157);
                        text.toBytes(definition.tableId().schema(), bytes, 167);
                        text.toBytes(type.startsWith("D/") ? "" : definition.member(), bytes, 177);
                        byte[] jid = java.util.HexFormat.of().parseHex(type.startsWith("D/") ? definition.fileJournalId() : definition.memberJournalId());
                        System.arraycopy(jid, 0, bytes, 197, jid.length);
                        int receiverOffset = java.nio.ByteBuffer.wrap(bytes).getInt(20);
                        if (receiverOffset != 0) {
                            text.toBytes(original.position().receiver().name(), bytes, receiverOffset);
                            text.toBytes(original.position().receiver().library(), bytes, receiverOffset + 10);
                        }
                        java.nio.ByteBuffer.wrap(bytes).putLong(24, original.position().offset().longValueExact());
                        if (type.equals("D/GT")) {
                            int payload = java.nio.ByteBuffer.wrap(bytes).getInt(8) + 16;
                            text.toBytes(definition.physicalFile(), bytes, payload);
                            text.toBytes(definition.tableId().schema(), bytes, payload + 10);
                        }
                        if (type.equals("identity")) {
                            bytes[197] ^= 1;
                        }
                        if (type.equals("transaction")) {
                            java.nio.ByteBuffer.wrap(bytes).putLong(64, 1);
                        }
                        if (type.equals("restore")) {
                            new com.ibm.as400.access.AS400Text(2, 37).toBytes("MR", bytes, 99);
                        }
                        entries.set(2, new RawJournalPage.Entry(original.position(), new EntryHeaderDecoder().decode(bytes, 0), bytes));
                    }
                }
                if (fault.equals("writers-outside-markers")) {
                    // Deterministic race shaping only: preserve native payloads,
                    // move A after an ordinary row and add a row following Z.
                    var opening = entries.get(1);
                    entries.set(1, at(entries.get(2), 58));
                    entries.set(2, at(opening, 59));
                    entries.add(at(entries.get(3), 85));
                }
                if (fault.equals("gap-before-opening")) {
                    // Reproduce the captured DCNC0989 failure: an omitted F/DE
                    // internal entry lies before A, outside the qualified interval.
                    entries.add(0, at(entries.get(0), 55)); // 56 is outside A..Z.
                }
                if (fault.equals("gap-after-closing")) {
                    entries.add(at(entries.get(3), 86)); // 85 follows Z.
                }
                if (extraRows > 0) {
                    var closing = entries.remove(entries.size() - 1);
                    for (int i = 0; i < extraRows; i++) {
                        var row = at(entries.get(3), 84 + i);
                        byte[] raw = row.data();
                        java.nio.ByteBuffer.wrap(raw).putLong(64, 0);
                        entries.add(new RawJournalPage.Entry(row.position(), new EntryHeaderDecoder().decode(raw, 0), raw));
                    }
                    entries.add(at(closing, 84 + extraRows));
                }
                JournalPosition next = invocation.getArgument(0);
                int requested = Math.min(pageSize, invocation.getArgument(2));
                var remaining = entries.stream().filter(e -> e.position().offset().compareTo(next.offset()) >= 0).toList();
                var page = new ArrayList<>(remaining.subList(0, Math.min(requested, remaining.size())));
                Optional<JournalPosition> continuation = remaining.size() > page.size() ? Optional.of(remaining.get(page.size()).position()) : Optional.empty();
                if (next.offset().compareTo(window.from().offset()) > 0) {
                    switch (pageFault) {
                        case "gap" -> page.remove(4);
                        case "repeated" -> page.add(0, at(page.get(0), next.offset().intValueExact() - 1));
                        case "backward" -> continuation = Optional.of(next);
                        case "overshoot" -> continuation = Optional.of(new JournalPosition(BigInteger.valueOf(9000), next.receiver()));
                        case "foreign" -> continuation = Optional.of(new JournalPosition(BigInteger.valueOf(9000),
                                new io.debezium.ibmi.db2.journal.retrieve.JournalReceiver("FOREIGN", "LIB")));
                        case "empty" -> page.clear();
                        case "short" -> continuation = Optional.empty();
                        case "failure" -> throw new java.sql.SQLException("Later native page failed");
                        case "stopped" -> stopped.set(true);
                    }
                }
                return new RetrieveJournal.AuditPage(page, fault.equals("continuation") ? Optional.of(window.through()) : continuation, 16000);
            });
            collector = new As400NativeBoundaryCollector(journal, () -> {
                var connection = mock(Connection.class);
                connections.add(connection);
                return connection;
            }, scopes, acquisition, journals, identities, pending, reader, () -> {
                if (stopped.get()) {
                    throw new IllegalStateException("Terminal stop");
                }
            }, () -> captures.call());
        }

        org.mockito.MockedConstruction<As400NativeBoundaryPulse> pulses() {
            return mockConstruction(As400NativeBoundaryPulse.class, (pulse, context) -> {
                if (pageFault.equals("close-failure")) {
                    org.mockito.Mockito.doThrow(new java.sql.SQLException("Holder close failed")).when(pulse).close();
                }
                when(pulse.attempt(any())).thenAnswer(invocation -> {
                    events.add("pulse-returned");
                    program = invocation.getArgument(0);
                    if (fault.equals("stop-after-pulse")) {
                        stopped.set(true);
                    }
                    if (fault.equals("refusal")) {
                        return Optional.empty();
                    }
                    var row = boundary.path("pulse").path("result_rows").get(0);
                    return Optional.of(new As400ShortBoundaryAudit.Completion(boundary.path("pulse").path("job").asText(),
                            new BigInteger(boundary.path("interval").path("holder").get(0).path("THREAD_HEX").asText(), 16),
                            java.time.LocalDateTime.parse(row.path("ACQUIRED").asText().replace(' ', 'T')),
                            java.time.LocalDateTime.parse(row.path("RELEASED").asText().replace(' ', 'T'))));
                });
            });
        }

        As400OperatorJournalEvidence.Observation directory(long tail) {
            var d = window.before().directory();
            var r = d.receivers().get(0);
            return new As400OperatorJournalEvidence.Observation(d.journal(), d.attached(), List.of(
                    new As400OperatorJournalEvidence.Receiver(r.name(), r.status(), r.first(), BigInteger.valueOf(tail), r.previous(), r.next(),
                            r.minimizeFiles(), r.job(), r.thread(), r.luw(), r.xid())));
        }

        RawJournalPage.Entry at(RawJournalPage.Entry entry, long sequence) {
            byte[] bytes = entry.data().clone();
            java.nio.ByteBuffer.wrap(bytes).putLong(24, sequence);
            return new RawJournalPage.Entry(new JournalPosition(BigInteger.valueOf(sequence), entry.position().receiver()),
                    new EntryHeaderDecoder().decode(bytes, 0), bytes);
        }
    }
}
