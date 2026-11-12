/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.relational.TableId;

/** Current catalog acquisition bracketed by complete, identity-checked native history. */
final class As400NativeSchemaRefresh {
    private final As400DcgNativeIo io;
    private final JournalReceiver journal;
    private final String journalIdentity;
    private final Runnable check;
    private final String catalog;

    As400NativeSchemaRefresh(As400DcgNativeIo io, JournalReceiver journal, String journalIdentity, String catalog, Runnable check) {
        this.io = io;
        this.journal = journal;
        this.journalIdentity = journalIdentity;
        this.check = check;
        this.catalog = catalog;
    }

    As400SchemaChange collect(As400BindingDefinition before, RawJournalPage.Entry opening) throws Exception {
        check.run();
        var first = sample(opening.position());
        var firstTail = tail(first);
        // Do not issue catalog reads while the observed DDL is unfinished. Such
        // reads can block on the DDL itself. A later interval can retry at F/CH.
        try (var captured = io.capture(check)) {
            read(first, opening.position(), firstTail, captured);
            var afterRead = sample(opening.position());
            var window = new As400NativeJournalWindow.Window(opening.position(), firstTail, first, afterRead);
            require(window.journalToken().equals(journalIdentity), "Schema refresh journal identity changed");
            As400SchemaChange.qualify(before, before, captured.seal(), order(window), firstTail);
        }
        check.run();
        var request = new As400OperatorTableEvidence.Request(journal, opening.header().getLibrary(), opening.header().getFile());
        var acquired = io.acquisition.collect(List.of(request)).definitions().get(0);
        // Native metadata uses physical/SQL names without a JDBC catalog. Keep
        // the same catalog identity the ordinary connector already publishes.
        var identity = acquired.identity();
        var table = new TableId(before == null ? catalog : before.schema().id().catalog(), acquired.schema().id().schema(), acquired.schema().id().table());
        var after = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(table, identity.physicalFile(), identity.member(),
                identity.fileJournalId(), identity.memberJournalId(), identity.recordFormat()), acquired.schema().edit().tableId(table).create(), acquired.codecs());
        var last = sample(opening.position());
        // Verify that the history observed before metadata acquisition still
        // exists. Endpoint equality alone cannot establish a schema epoch.
        new As400NativeJournalWindow.Window(opening.position(), firstTail, first, last);
        try (var captured = io.capture(check)) {
            var through = tail(last);
            read(last, opening.position(), through, captured);
            var rechecked = sample(opening.position());
            var window = new As400NativeJournalWindow.Window(opening.position(), through, last, rechecked);
            require(window.journalToken().equals(journalIdentity), "Schema refresh journal identity changed");
            return As400SchemaChange.qualify(before, after, captured.seal(), order(window), firstTail);
        }
    }

    private As400NativeJournalWindow.Sample sample(JournalPosition from) throws Exception {
        check.run();
        return As400NativeJournalWindow.collect(io.journals, io.identities, from);
    }

    private static JournalPosition tail(As400NativeJournalWindow.Sample sample) {
        var directory = sample.directory();
        var attached = directory.receivers().stream().filter(receiver -> receiver.name().equals(directory.attached())).findFirst().orElseThrow();
        return new JournalPosition(attached.last(), attached.name());
    }

    private static As400JournalOrder order(As400NativeJournalWindow.Window window) {
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        for (var receiver : As400OperatorCutReader.chain(window.before().directory(), window.from().receiver())) {
            receiver.requireReadableProfile();
            ranges.put(receiver.name(), new As400JournalOrder.Range(receiver.first(), receiver.last()));
        }
        return new As400JournalOrder(new ArrayList<>(ranges.keySet()), window.receiverTokens(), ranges);
    }

    private void read(As400NativeJournalWindow.Sample sample, JournalPosition from, JournalPosition through, As400JournalCapture captured) throws Exception {
        var chain = As400OperatorCutReader.chain(sample.directory(), from.receiver());
        for (var receiver : chain) {
            receiver.requireReadableProfile();
            var next = receiver.name().equals(from.receiver()) ? from : new JournalPosition(receiver.first(), receiver.name());
            var end = receiver.name().equals(through.receiver()) ? through : new JournalPosition(receiver.last(), receiver.name());
            while (true) {
                check.run();
                var page = io.reader.readRawPage(next, end, 1000);
                for (var entry : page.entries()) {
                    captured.append(entry);
                }
                if (page.continuation().isEmpty()) {
                    break;
                }
                var continuation = page.continuation().orElseThrow();
                require(continuation.receiver().equals(next.receiver()) && continuation.offset().compareTo(next.offset()) > 0,
                        "Schema evidence read did not progress");
                next = continuation;
            }
            if (receiver.name().equals(through.receiver())) {
                return;
            }
        }
        throw new IllegalArgumentException("Schema evidence did not reach its observed receiver");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
