/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.relational.TableId;

/** Current catalog acquisition bracketed by complete, identity-checked native history. */
final class As400NativeSchemaRefresh {
    private static final Logger LOGGER = LoggerFactory.getLogger(As400NativeSchemaRefresh.class);

    private static final class CatalogMoved extends Exception {
        private final As400NativeEvidenceException failure;

        private CatalogMoved(As400NativeEvidenceException cause) {
            super(cause);
            failure = cause;
        }
    }

    private final As400DcgNativeIo io;
    private final JournalReceiver journal;
    private final String journalIdentity;
    private final Runnable check;
    private final String catalog;

    @FunctionalInterface
    interface Observation {
        As400SchemaChange read() throws Exception;
    }

    As400NativeSchemaRefresh(As400DcgNativeIo io, JournalReceiver journal, String journalIdentity, String catalog, Runnable check) {
        this.io = io;
        this.journal = journal;
        this.journalIdentity = journalIdentity;
        this.check = check;
        this.catalog = catalog;
    }

    As400SchemaChange collect(As400BindingDefinition before, RawJournalPage.Entry opening, As400SourceNormalizer.TableResolver verifiedBindings) throws Exception {
        check.run();
        // Reuse transport only within this observation. Every directory/identity
        // sample still executes fresh queries; no evidence survives this refresh.
        try (var connection = io.connection(false)) {
            return observe(opening.position(), check, () -> collect(connection, before, opening, verifiedBindings));
        }
    }

    /** Retry only a qualified history/catalog race, with a fresh observation and stop admission. */
    static As400SchemaChange observe(JournalPosition opening, Runnable check, Observation observation) throws Exception {
        for (int attempt = 0;; attempt++) {
            check.run();
            try {
                return observation.read();
            }
            catch (CatalogMoved moved) {
                if (attempt == 1) {
                    throw moved.failure;
                }
                LOGGER.info("Re-observing schema change at {} after retained journal evidence explains a missing catalog", opening);
            }
            catch (As400SchemaChange.MetadataMovedException moved) {
                if (attempt == 1) {
                    // Continuous DDL can keep moving the observation. Bound
                    // work per poll, retain F and retry from complete history;
                    // neither a stale schema nor partial output is accepted.
                    throw new As400SchemaTimeline.PendingChangeException();
                }
                LOGGER.info("Re-observing schema change at {} after a qualified transition overlapped catalog acquisition", opening);
            }
        }
    }

    private As400SchemaChange collect(java.sql.Connection connection, As400BindingDefinition before, RawJournalPage.Entry opening,
                                      As400SourceNormalizer.TableResolver verifiedBindings)
            throws Exception {
        var first = sample(connection, opening.position());
        var firstTail = tail(first);
        boolean rename = opening.header().getJournalCode() == 'D' && opening.header().getEntryType().equals("FN");
        String acquiredFile = opening.header().getFile();
        String acquiredLibrary = opening.header().getLibrary();
        // Do not issue catalog reads while the observed DDL is unfinished. Such
        // reads can block on the DDL itself. A later interval can retry at F/CH.
        try (var captured = io.capture(check)) {
            read(first, opening.position(), firstTail, captured);
            var afterRead = sample(connection, opening.position());
            var window = new As400NativeJournalWindow.Window(opening.position(), firstTail, first, afterRead);
            require(window.journalToken().equals(journalIdentity), "Schema refresh journal identity changed");
            var entries = captured.seal();
            if (As400FileRename.moveOpening(opening)) {
                return As400FileRename.move(before, entries, order(window));
            }
            if (opening.header().getJournalCode() == 'D' && opening.header().getEntryType().equals("DT")) {
                return As400FileDeletion.qualify(before, entries, order(window));
            }
            if (As400MemberCreation.opening(before, opening)) {
                // The complete native association adds one member to the retained
                // file format; no current-catalog schema substitution is needed.
                return As400MemberCreation.qualify(before, entries, order(window));
            }
            if (As400MemberCreation.deletionOpening(before, opening)) {
                return As400MemberCreation.deletion(before, entries, order(window));
            }
            if (As400MemberCreation.renameOpening(before, opening)) {
                return As400MemberCreation.rename(before, opening, order(window));
            }
            var deletedAfterChange = As400FileDeletion.afterSchemaChange(before, entries, order(window), verifiedBindings, io.historicalNames);
            if (deletedAfterChange != null) {
                return deletedAfterChange;
            }
            var metadataPrefix = As400SchemaChange.metadataBeforeTransition(before, entries, order(window), verifiedBindings, io.historicalNames);
            if (metadataPrefix != null) {
                return metadataPrefix;
            }
            if (rename) {
                var namedPrefix = As400FileRename.beforeMetadata(before, entries, order(window), verifiedBindings);
                if (namedPrefix != null) {
                    return namedPrefix;
                }
                var deletion = As400FileRename.deleted(before, entries, order(window), verifiedBindings, io.historicalNames);
                if (deletion != null) {
                    return deletion;
                }
                acquiredFile = As400FileRename.observe(before, entries, order(window),
                        verifiedBindings.resolve(before.identity().physicalFile(), before.identity().tableId().schema())).file();
            }
            var following = As400FileRename.following(before, entries, order(window), verifiedBindings);
            if (following != null) {
                acquiredFile = following.file();
                acquiredLibrary = following.library();
                entries = following.prefix();
            }
            As400SchemaChange preliminary;
            try {
                preliminary = rename ? null
                        : before == null
                                ? As400FileCreation.qualify(null, entries, order(window), firstTail, verifiedBindings, io.historicalNames, catalog)
                                : As400SchemaChange.qualify(before, before, entries, order(window), firstTail, verifiedBindings);
            }
            catch (As400SchemaTimeline.PendingChangeException pending) {
                var excluded = As400SchemaChange.excludedCommittedPrefix(before, entries, order(window), verifiedBindings, io.historicalNames);
                if (excluded != null) {
                    return excluded;
                }
                throw pending;
            }
            if (preliminary != null && (preliminary.abortedCreation()
                    || preliminary.before() == null && preliminary.after() != null && preliminary.after().unobservedLayout()
                    || preliminary.rolledBackAlter()
                            && !As400SchemaChange.restoredLayoutStable(before, entries,
                                    preliminary.records().get(preliminary.records().size() - 1).position(), order(window)))) {
                // Complete rollback or an exact excluded identity prefix needs
                // no current row layout. Do not substitute today's renamed or
                // temporary catalog for its proven historical identity.
                return preliminary;
            }
            // An excluded committed prefix can have identity but no observed
            // layout. A complete rollback with a stable suffix now permits
            // acquiring its surviving definition. The post-catalog pass checks
            // that stability again, so a later DDL cannot supply an earlier schema.
            if (before == null) {
                var location = As400FileCreation.acquiredLocation(entries, order(window));
                acquiredFile = location.file();
                acquiredLibrary = location.library();
            }
        }
        check.run();
        var request = new As400OperatorTableEvidence.Request(journal, acquiredLibrary, acquiredFile);
        As400BindingDefinition acquired;
        try {
            acquired = io.acquisition.collectOn(connection, List.of(request)).definitions().get(0);
        }
        catch (As400NativeEvidenceException missing) {
            if (!missing.catalogAbsent()) {
                throw missing;
            }
            // A DROP/rename can land after the first read but before the catalog
            // query. Retry once only when new, completely retained history
            // explains this object's disappearance. Never infer exclusion from
            // a missing catalog or hide authority/unsupported-layout failures.
            var newer = sample(connection, opening.position());
            try (var captured = io.capture(check)) {
                var through = tail(newer);
                read(newer, opening.position(), through, captured);
                var rechecked = sample(connection, opening.position());
                var window = new As400NativeJournalWindow.Window(opening.position(), through, newer, rechecked);
                require(window.journalToken().equals(journalIdentity), "Schema retry journal identity changed");
                String fileJid = before == null ? opening.header().getRecordInfo().objectJournalId() : before.identity().fileJournalId();
                if (disappearedAfter(acquiredFile, acquiredLibrary, fileJid, firstTail, captured.seal(), order(window))) {
                    throw new CatalogMoved(missing);
                }
            }
            throw missing;
        }
        // Native metadata uses physical/SQL names without a JDBC catalog. Keep
        // the same catalog identity the ordinary connector already publishes.
        var identity = before == null ? acquired.identity() : acquired.memberIdentity(before.identity().member());
        var table = new TableId(before == null ? catalog : before.schema().id().catalog(), acquired.schema().id().schema(), acquired.schema().id().table());
        var after = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(table, identity.physicalFile(), identity.member(),
                identity.fileJournalId(), identity.memberJournalId(), identity.recordFormat()), acquired.schema().edit().tableId(table).create(), acquired.codecs(),
                acquired.members());
        var last = sample(connection, opening.position());
        // Verify that the history observed before metadata acquisition still
        // exists. Endpoint equality alone cannot establish a schema epoch.
        new As400NativeJournalWindow.Window(opening.position(), firstTail, first, last);
        try (var captured = io.capture(check)) {
            var through = tail(last);
            read(last, opening.position(), through, captured);
            var rechecked = sample(connection, opening.position());
            var window = new As400NativeJournalWindow.Window(opening.position(), through, last, rechecked);
            require(window.journalToken().equals(journalIdentity), "Schema refresh journal identity changed");
            var entries = captured.seal();
            return before == null ? As400FileCreation.qualify(after, entries, order(window), firstTail, verifiedBindings, io.historicalNames, catalog)
                    : qualifyAcquired(before, after, entries, order(window), firstTail, verifiedBindings, io.historicalNames);
        }
    }

    static boolean disappearedAfter(RawJournalPage.Entry opening, String acquiredFile, String fileJid, JournalPosition observedThrough,
                                    List<RawJournalPage.Entry> entries, As400JournalOrder order)
            throws Exception {
        return disappearedAfter(acquiredFile, opening.header().getLibrary(), fileJid, observedThrough, entries, order);
    }

    static boolean disappearedAfter(String acquiredFile, String acquiredLibrary, String fileJid,
                                    JournalPosition observedThrough, List<RawJournalPage.Entry> entries, As400JournalOrder order)
            throws Exception {
        for (var entry : entries) {
            var h = entry.header();
            if (order.compare(entry.position(), observedThrough) > 0 && h.getJournalCode() == 'D'
                    && (h.getEntryType().equals("DT") || h.getEntryType().equals("FN") || h.getEntryType().equals("FM"))
                    && h.getLibrary().equals(acquiredLibrary) && h.getFile().equals(acquiredFile)
                    && h.getRecordInfo().objectType().equals("*FILE") && h.getRecordInfo().objectNameIndicator() == '1'
                    && h.getRecordInfo().objectJournalId().equals(fileJid)) {
                As400SourceNormalizer.validateSource(h, entry.data(), 0, entry.position());
                return true;
            }
        }
        return false;
    }

    static As400SchemaChange qualifyAcquired(As400BindingDefinition before, As400BindingDefinition after, List<RawJournalPage.Entry> entries,
                                             As400JournalOrder order, JournalPosition metadataStartedAfter,
                                             As400SourceNormalizer.TableResolver bindings)
            throws Exception {
        return qualifyAcquired(before, after, entries, order, metadataStartedAfter, bindings, null);
    }

    static As400SchemaChange qualifyAcquired(As400BindingDefinition before, As400BindingDefinition after, List<RawJournalPage.Entry> entries,
                                             As400JournalOrder order, JournalPosition metadataStartedAfter,
                                             As400SourceNormalizer.TableResolver bindings, As400HistoricalTableNames.Reader names)
            throws Exception {
        // A DROP may arrive after acquisition. The second complete interval
        // needs the same historical-name proof as the pre-catalog observation.
        var deleted = As400FileDeletion.afterSchemaChange(before, entries, order, bindings, names);
        if (deleted != null) {
            return deleted;
        }
        var namedPrefix = As400FileRename.beforeMetadata(before, entries, order, bindings);
        if (namedPrefix != null) {
            return namedPrefix;
        }
        var renamedThenDeleted = As400FileRename.deleted(before, entries, order, bindings, names);
        if (renamedThenDeleted != null) {
            return renamedThenDeleted;
        }
        var metadataPrefix = As400SchemaChange.metadataBeforeTransition(before, entries, order, bindings, names);
        if (metadataPrefix != null) {
            return metadataPrefix;
        }
        var following = As400FileRename.following(before, entries, order, bindings);
        if (following != null) {
            As400SchemaChange.requireMetadataAfter(following.observedThrough(), metadataStartedAfter, order,
                    "Catalog was read before the later physical rename or move completed");
            require(after != null && after.identity().physicalFile().equals(following.file()) && after.schema().id().schema().equals(following.library()),
                    "Acquired definition does not match the later physical rename or move");
            var original = before.identity();
            var acquired = after.identity();
            require(!following.preservesSqlName() || original.tableId().table().equals(acquired.tableId().table()),
                    "File move catalog changed the retained SQL name");
            require(original.fileJournalId().equals(acquired.fileJournalId()) && before.members().equals(after.members())
                    && original.member().equals(acquired.member()) && original.memberJournalId().equals(acquired.memberJournalId()),
                    "Later rename or move changed the altered object's identity");
            // The rename/move proof excludes subsequent layout changes. Its current
            // definition can therefore describe the completed ALTER, but rows
            // before D/FN or D/FM still belong to the retained old SQL name and key.
            // A metadata pair followed by rows could conceal a SQL-only rename.
            boolean metadata = false;
            for (var entry : following.prefix()) {
                var h = entry.header();
                if (h.getFile().equals(original.physicalFile()) && h.getLibrary().equals(original.tableId().schema())) {
                    metadata |= As400SchemaChange.metadataOpening(entry);
                    require(!metadata || h.getJournalCode() != 'R', "Rows require an unavailable SQL name before the physical rename");
                }
            }
            after = new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(original.tableId(), original.physicalFile(), original.member(),
                    acquired.fileJournalId(), acquired.memberJournalId(), acquired.recordFormat()),
                    after.schema().edit().tableId(original.tableId()).create(), after.codecs(), after.members());
            entries = following.prefix();
        }
        return As400SchemaChange.qualify(before, after, entries, order, metadataStartedAfter, bindings);
    }

    private As400NativeJournalWindow.Sample sample(java.sql.Connection connection, JournalPosition from) throws Exception {
        check.run();
        var directory = io.journals.collectFromOn(connection, from.receiver());
        return new As400NativeJournalWindow.Sample(directory, io.identities.collectOn(connection, As400NativeJournalWindow.targets(directory, from)));
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
