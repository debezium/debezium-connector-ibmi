/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;

import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

/**
 * Owns a bounded online observation attempt. No output, offset, transaction
 * census authority or history activation is created here. Complete C/J recovery,
 * historical bindings and the durable legacy barrier are still separate gates.
 * The caller owns native I/O and terminal admission for the whole attempt.
 */
final class As400NativeBoundaryCollector {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(As400NativeBoundaryCollector.class);

    /** Bounded diagnostic bytes survive rejection; this exception grants no admission. */
    static final class AuditRejected extends IllegalArgumentException {
        final As400NativeJournalWindow.Window window;
        final RetrieveJournal.AuditPage page;

        AuditRejected(Exception cause, As400NativeJournalWindow.Window window, RetrieveJournal.AuditPage page) {
            super("Native boundary audit rejected", cause);
            this.window = window;
            this.page = page;
        }
    }

    record Candidate(As400ShortBoundaryProgram program, As400ShortBoundaryAudit.Completion completion,
            As400JournalFileScope.Scope scope, As400NativeBindingAcquisition.Acquired bindings,
            As400PendingReceiverEvidence.Sample pending, As400NativeJournalWindow.Sample pendingBefore,
            As400NativeJournalWindow.Sample pendingAfter, As400NativeJournalWindow.Window window,
            As400JournalCapture audit, JournalPosition origin, JournalPosition historyFloor) implements AutoCloseable {
        @Override
        public void close() throws java.io.IOException {
            audit.close();
        }
    }

    private final JournalReceiver journal;
    private final As400JdbcConnectionFactory connections;
    private final As400JournalFileScope scopes;
    private final As400NativeBindingAcquisition acquisition;
    private final As400OperatorJournalEvidence journals;
    private final As400JdbcNativeIdentityEvidence identities;
    private final As400PendingReceiverEvidence pending;
    private final RetrieveJournal reader;
    private final Runnable checkRunning;
    private final Callable<As400JournalCapture> captures;

    As400NativeBoundaryCollector(JournalReceiver journal, As400JdbcConnectionFactory connections,
                                 As400NativeBindingAcquisition acquisition, RetrieveJournal reader, Runnable checkRunning) {
        this(journal, connections, acquisition, reader, checkRunning, () -> new As400JournalCapture(
                java.nio.file.Path.of(System.getProperty("java.io.tmpdir")), 10L * 1024 * 1024 * 1024, checkRunning, () -> {
                }));
    }

    As400NativeBoundaryCollector(JournalReceiver journal, As400JdbcConnectionFactory connections,
                                 As400NativeBindingAcquisition acquisition, RetrieveJournal reader, Runnable checkRunning,
                                 Callable<As400JournalCapture> captures) {
        this(journal, connections, new As400JournalFileScope(journal, 5), acquisition,
                As400OperatorJournalEvidence.directory(connections, journal),
                new As400JdbcNativeIdentityEvidence(connections, new As400JdbcNativeIdentityEvidence.Limits(Integer.MAX_VALUE, 5, 1024)),
                new As400PendingReceiverEvidence(connections, journal, Integer.MAX_VALUE - 1, 5), reader, checkRunning, captures);
    }

    As400NativeBoundaryCollector(JournalReceiver journal, As400JdbcConnectionFactory connections, As400JournalFileScope scopes,
                                 As400NativeBindingAcquisition acquisition, As400OperatorJournalEvidence journals,
                                 As400JdbcNativeIdentityEvidence identities, As400PendingReceiverEvidence pending,
                                 RetrieveJournal reader, Runnable checkRunning) {
        this(journal, connections, scopes, acquisition, journals, identities, pending, reader, checkRunning, () -> new As400JournalCapture(
                java.nio.file.Path.of(System.getProperty("java.io.tmpdir")), 10L * 1024 * 1024 * 1024, checkRunning, () -> {
                }));
    }

    As400NativeBoundaryCollector(JournalReceiver journal, As400JdbcConnectionFactory connections, As400JournalFileScope scopes,
                                 As400NativeBindingAcquisition acquisition, As400OperatorJournalEvidence journals,
                                 As400JdbcNativeIdentityEvidence identities, As400PendingReceiverEvidence pending,
                                 RetrieveJournal reader, Runnable checkRunning, Callable<As400JournalCapture> captures) {
        this.journal = journal;
        this.connections = connections;
        this.scopes = scopes;
        this.acquisition = acquisition;
        this.journals = journals;
        this.identities = identities;
        this.pending = pending;
        this.reader = reader;
        this.checkRunning = checkRunning;
        this.captures = captures;
    }

    Optional<Candidate> collect() throws Exception {
        checkRunning.run();
        var anchor = As400NativeJournalWindow.collectDirectory(journals, identities);
        var from = tail(anchor);
        String nonce = UUID.randomUUID().toString().replace("-", "").toUpperCase(java.util.Locale.ROOT);
        As400JournalCapture capture = null;
        try (var pulse = new As400NativeBoundaryPulse(connections, checkRunning, journal, nonce)) {
            // A already exists. Sampling and schema acquisition are entirely
            // outside the server-owned allocation interval.
            var pendingBefore = As400NativeJournalWindow.collectDirectory(journals, identities);
            var sample = pending.collect();
            var pendingAfter = As400NativeJournalWindow.collectDirectory(journals, identities);
            As400NativeJournalWindow.requireStableDirectory(anchor, pendingBefore);
            As400NativeJournalWindow.requireStableDirectory(pendingBefore, pendingAfter);
            sample.verifyWindow(pendingBefore.directory(), pendingAfter.directory());
            var scope = scope();
            var bindings = acquisition.collect(scope.files());
            require(bindings.observations().stream().mapToLong(o -> o.memberObjects().size()).sum() == scope.journaledMembers(),
                    "Native member inventory differs from independent journal counts");
            require(scope.equals(scope()), "Journal membership changed during binding acquisition");
            var before = As400NativeJournalWindow.collectDirectory(journals, identities);
            As400NativeJournalWindow.requireStableDirectory(pendingAfter, before);
            var targets = bindings.observations().stream()
                    .flatMap(o -> o.memberObjects().keySet().stream()
                            .map(member -> new As400ShortBoundaryProgram.Target(o.request().library(), o.request().file(), member)))
                    .toList();
            var program = new As400ShortBoundaryProgram(journal, targets, nonce, digest(bindings.definitions()));
            var completion = pulse.attempt(program);
            if (completion.isEmpty()) {
                return Optional.empty(); // Completed refusal, no candidate or offset.
            }
            var after = As400NativeJournalWindow.collectDirectory(journals, identities);
            require(scope.equals(scope()), "Journal membership changed across allocation");
            require(bindings.equals(acquisition.collect(scope.files())), "Native identity or schema changed across allocation");
            require(scope.equals(scope()), "Journal membership changed during recheck");
            As400NativeJournalWindow.requireStableDirectory(before, after);
            pulse.finish(program);
            var rawBefore = As400NativeJournalWindow.collect(journals, identities, from);
            var through = tail(rawBefore);
            require(from.receiver().equals(through.receiver()), "Receiver rotated during boundary attempt");
            checkRunning.run();
            capture = captures.call();
            try {
                readAudit(from, through, capture);
                var entries = capture.seal();
                var rawAfter = As400NativeJournalWindow.collect(journals, identities, from);
                var readWindow = new As400NativeJournalWindow.Window(from, through, rawBefore, rawAfter);
                try {
                    require(pendingBefore.identities().objects().entrySet().containsAll(rawBefore.identities().objects().entrySet())
                            && pendingAfter.identities().objects().entrySet().containsAll(rawAfter.identities().objects().entrySet()),
                            "Pending discovery and raw audit identities differ");
                    require(from.offset().compareTo(tail(pendingBefore).offset()) <= 0 && tail(pendingAfter).offset().compareTo(through.offset()) <= 0,
                            "Pending observation is outside audit interval");
                    // Writers can append after Z while the tail is being observed. Keep
                    // their raw evidence, but validate the exact A/N/Z prefix as boundary.
                    var prefix = As400ShortBoundaryAudit.attemptEntries(program, readWindow, entries);
                    var end = prefix.get(prefix.size() - 1).position();
                    var window = new As400NativeJournalWindow.Window(prefix.get(0).position(), end, rawBefore, rawAfter);
                    var origin = As400ShortBoundaryAudit.validate(program, completion.get(), window, prefix, bindings.definitions(), tail(before), tail(after));
                    require(tail(pendingAfter).offset().compareTo(end.offset()) < 0, "Pending sample follows closing marker");
                    // Retain all cycle references from the sampling bracket, including
                    // work before A, without decoding those older row layouts.
                    int endIndex = 0;
                    while (endIndex < entries.size() && entries.get(endIndex).position().offset().compareTo(end.offset()) <= 0) {
                        endIndex++;
                    }
                    var floor = sample.floor(entries.subList(0, endIndex));
                    checkRunning.run();
                    LOGGER.info("Validated transactional truncate boundary across {} raw journal entries", entries.size());
                    return Optional.of(new Candidate(program, completion.get(), scope, bindings, sample, pendingBefore, pendingAfter,
                            readWindow, capture, origin, floor));
                }
                catch (Exception rejected) {
                    // Diagnostic bytes remain bounded and grant no completeness
                    // or startup proof. The full owned capture is closed below.
                    var diagnostic = new RetrieveJournal.AuditPage(entries.subList(0, Math.min(32, entries.size())), Optional.empty(), 0);
                    throw new AuditRejected(rejected, readWindow, diagnostic);
                }
            }
            catch (Throwable failure) {
                try {
                    capture.close();
                }
                catch (Throwable cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
        }
        catch (Throwable failure) {
            // A holder close can fail after candidate construction. Ownership
            // transfers only when the entire native attempt has returned.
            if (capture != null) {
                try {
                    capture.close();
                }
                catch (Throwable close) {
                    failure.addSuppressed(close);
                }
            }
            throw failure;
        }
    }

    private void readAudit(JournalPosition from, JournalPosition through, As400JournalCapture capture) throws Exception {
        JournalPosition next = from;
        JournalPosition previous = null;
        while (true) {
            checkRunning.run();
            var page = reader.readAuditHistoryPage(next, through, 1000);
            require(page.entries().size() <= 1000, "Native audit page exceeds request bound");
            for (var entry : page.entries()) {
                checkRunning.run();
                var at = entry.position();
                require(at.receiver().equals(from.receiver()) && at.offset().compareTo(next.offset()) >= 0
                        && at.offset().compareTo(through.offset()) <= 0 && (previous == null || previous.offset().compareTo(at.offset()) < 0),
                        "Unordered or foreign audit page");
                capture.append(entry);
                previous = at;
            }
            if (page.continuation().isEmpty()) {
                require(through.equals(previous), "Incomplete raw audit endpoint");
                return;
            }
            var continuation = page.continuation().orElseThrow();
            require(!page.entries().isEmpty() && continuation.receiver().equals(from.receiver())
                    && continuation.offset().compareTo(previous.offset()) > 0 && continuation.offset().compareTo(through.offset()) <= 0,
                    "Invalid raw audit continuation");
            next = continuation;
        }
    }

    private As400JournalFileScope.Scope scope() throws Exception {
        checkRunning.run();
        try (var connection = connections.open()) {
            return scopes.collect(connection);
        }
    }

    private static JournalPosition tail(As400NativeJournalWindow.Sample sample) {
        var directory = sample.directory();
        var attached = directory.receivers().stream().filter(r -> r.name().equals(directory.attached())).findFirst().orElseThrow();
        attached.requireReadableProfile();
        return new JournalPosition(attached.last(), attached.name());
    }

    static String digest(List<As400BindingDefinition> definitions) throws Exception {
        var digest = MessageDigest.getInstance("SHA-256");
        digest.update("{\"tables\":[".getBytes(StandardCharsets.UTF_8));
        boolean first = true;
        for (var definition : definitions) {
            require(!Thread.currentThread().isInterrupted(), "Boundary digest processing stopped");
            if (!first) {
                digest.update((byte) ',');
            }
            var changes = new TableChanges();
            changes.create(definition.storedTable());
            var table = new JsonTableChangeSerializer().serialize(changes).get(0).asDocument();
            digest.update(DocumentWriter.defaultWriter().write(table).getBytes(StandardCharsets.UTF_8));
            first = false;
        }
        digest.update("]}".getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().withUpperCase().formatHex(digest.digest());
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
