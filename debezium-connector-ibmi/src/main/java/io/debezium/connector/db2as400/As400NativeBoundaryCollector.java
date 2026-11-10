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

import io.debezium.document.Document;
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
            RetrieveJournal.AuditPage audit, JournalPosition origin, JournalPosition historyFloor) {
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

    As400NativeBoundaryCollector(JournalReceiver journal, As400JdbcConnectionFactory connections,
                                 As400NativeBindingAcquisition acquisition, RetrieveJournal reader, Runnable checkRunning) {
        this(journal, connections, new As400JournalFileScope(journal, 5), acquisition,
                new As400OperatorJournalEvidence(connections, journal, new As400OperatorJournalEvidence.Limits(16, 5)),
                new As400JdbcNativeIdentityEvidence(connections, new As400JdbcNativeIdentityEvidence.Limits(17, 5, 1024)),
                new As400PendingReceiverEvidence(connections, journal, 16, 5), reader, checkRunning);
    }

    As400NativeBoundaryCollector(JournalReceiver journal, As400JdbcConnectionFactory connections, As400JournalFileScope scopes,
                                 As400NativeBindingAcquisition acquisition, As400OperatorJournalEvidence journals,
                                 As400JdbcNativeIdentityEvidence identities, As400PendingReceiverEvidence pending,
                                 RetrieveJournal reader, Runnable checkRunning) {
        this.journal = journal;
        this.connections = connections;
        this.scopes = scopes;
        this.acquisition = acquisition;
        this.journals = journals;
        this.identities = identities;
        this.pending = pending;
        this.reader = reader;
        this.checkRunning = checkRunning;
    }

    Optional<Candidate> collect() throws Exception {
        checkRunning.run();
        var anchor = As400NativeJournalWindow.collectDirectory(journals, identities);
        var from = tail(anchor);
        String nonce = UUID.randomUUID().toString().replace("-", "").toUpperCase(java.util.Locale.ROOT);
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
            require(scope.equals(scope()), "Journal membership changed during binding acquisition");
            var before = As400NativeJournalWindow.collectDirectory(journals, identities);
            As400NativeJournalWindow.requireStableDirectory(pendingAfter, before);
            var targets = bindings.observations().stream()
                    .map(o -> new As400ShortBoundaryProgram.Target(o.request().library(), o.request().file(), o.member())).toList();
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
            var page = reader.readAuditHistoryPage(from, through, 2000);
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
                var prefix = As400ShortBoundaryAudit.attemptInterval(program, readWindow, page);
                var end = prefix.entries().get(prefix.entries().size() - 1).position();
                var window = new As400NativeJournalWindow.Window(prefix.entries().get(0).position(), end, rawBefore, rawAfter);
                var origin = As400ShortBoundaryAudit.validate(program, completion.get(), window, prefix, bindings.definitions(), tail(before), tail(after));
                require(tail(pendingAfter).offset().compareTo(end.offset()) < 0, "Pending sample follows closing marker");
                // Retain all cycle references from the sampling bracket, including
                // work before A, without decoding those older row layouts.
                var floor = sample.floor(page.entries().stream().filter(e -> e.position().offset().compareTo(end.offset()) <= 0).toList());
                checkRunning.run();
                return Optional.of(new Candidate(program, completion.get(), scope, bindings, sample, pendingBefore, pendingAfter,
                        readWindow, page, origin, floor));
            }
            catch (Exception rejected) {
                throw new AuditRejected(rejected, readWindow, page);
            }
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
        var changes = new TableChanges();
        definitions.forEach(d -> changes.create(d.storedTable()));
        var document = Document.create();
        document.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
        return HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("SHA-256")
                .digest(DocumentWriter.defaultWriter().write(document).getBytes(StandardCharsets.UTF_8)));
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
