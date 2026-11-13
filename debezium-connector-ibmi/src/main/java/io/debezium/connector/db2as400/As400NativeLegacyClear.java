/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.relational.TableId;

/** Read-only, retained-history qualification for a clear held by the legacy prefix. */
final class As400NativeLegacyClear {
    record Target(TableId table, String file, JournalPosition firstClear) {
        String key() {
            return table.schema() + "/" + file;
        }
    }

    static final class Prepared implements AutoCloseable {
        private final As400HandoffWork work;
        private final List<As400SpillStore> stores = new ArrayList<>();
        private final List<As400LegacyClearReplay> replays = new ArrayList<>();
        List<As400BindingDefinition> definitions;
        List<RawJournalPage.Entry> entries;
        As400LegacyClearReplay.Resolution resolution;

        Prepared(As400HandoffWork work) {
            this.work = work;
        }

        @Override
        public void close() throws Exception {
            Exception failure = null;
            var resources = new ArrayList<AutoCloseable>();
            resources.addAll(replays);
            resources.addAll(stores);
            resources.add(work);
            for (var resource : resources) {
                try {
                    resource.close();
                }
                catch (Exception error) {
                    if (failure == null) {
                        failure = error;
                    }
                    else {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private final As400DcgNativeIo io;
    private final As400OperatorJournalEvidence directory;
    private final JournalReceiver journal;
    private final As400ContinuousRuntime.Limits limits;
    private final Runnable check;

    As400NativeLegacyClear(As400DcgNativeIo io, JournalReceiver journal, As400ContinuousRuntime.Limits limits, Runnable check) {
        this.io = io;
        this.directory = As400OperatorJournalEvidence.directory(() -> io.connection(false), journal);
        this.journal = journal;
        this.limits = limits;
        this.check = check;
    }

    Optional<Prepared> prepare(BigInteger cycle, List<Target> targets, JournalPosition through, String journalIdentity) throws Exception {
        check.run();
        var result = new Prepared(io.handoffWork(limits, check));
        boolean returned = false;
        try {
            var requested = new LinkedHashMap<String, JournalPosition>();
            targets.forEach(target -> require(requested.put(target.key(), target.firstClear()) == null, "Repeated deferred-clear table"));
            var starts = result.work.map(As400SpillCodec.INTEGER, As400SpillCodec.POSITIONS);
            starts.put(cycle, requested);
            BigInteger earliest = cycle;
            As400NativeJournalWindow.Sample before;
            As400NativeJournalWindow.Window window;
            As400JournalOrder order;
            // Another held table can be cleared by a cycle that began earlier.
            // Re-read its real SC, never synthesize transaction context.
            while (true) {
                var from = originalStart(directory.collect(), through, earliest);
                before = As400NativeJournalWindow.collect(io.journals, io.identities, from);
                var capture = result.work.capture();
                read(before, from, through, capture);
                result.entries = capture.seal();
                var after = As400NativeJournalWindow.collect(io.journals, io.identities, from);
                window = new As400NativeJournalWindow.Window(from, through, before, after);
                require(window.journalToken().equals(journalIdentity), "Deferred-clear journal identity changed");
                order = order(window);
                var decoder = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID);
                BigInteger expanded = earliest;
                for (var entry : result.entries) {
                    var h = entry.header();
                    String key = h.getLibrary() + "/" + h.getFile();
                    if (requested.containsKey(key) && order.compare(entry.position(), requested.get(key)) >= 0
                            && h.getJournalCode() == 'D' && h.getEntryType().equals("CG")) {
                        require(decoder.decode(h, entry.data(), 0).clearsRows() && h.getCommitCycle().signum() > 0,
                                "Schema change overlaps an unresolved legacy clear");
                        var tables = starts.getOrDefault(h.getCommitCycle(), new LinkedHashMap<>());
                        tables.putIfAbsent(key, entry.position());
                        starts.put(h.getCommitCycle(), tables);
                        expanded = expanded.min(h.getCommitCycle());
                    }
                }
                if (expanded.equals(earliest)) {
                    break;
                }
                earliest = expanded;
            }
            var requests = targets.stream().map(target -> new As400OperatorTableEvidence.Request(journal, target.table().schema(), target.file())).toList();
            var acquired = As400NativeBoundaryCollector.metadata(() -> io.acquisition.collect(requests));
            result.definitions = As400NativeHandoffSession.legacyCatalog(acquired.definitions(), io.server());
            As400NativeTableSchemas.requireLegacyTemporalCompatibility(result.definitions);
            var bindings = new HashMap<String, As400SourceNormalizer.TableBinding>();
            for (var definition : result.definitions) {
                String key = definition.identity().tableId().schema() + "/" + definition.identity().physicalFile();
                require(targets.stream().anyMatch(t -> t.key().equals(key) && t.table().equals(definition.identity().tableId())),
                        "Deferred-clear native SQL identity changed");
                bindings.put(key, definition.binding(1024 * 1024));
            }
            require(acquired.equals(As400NativeBoundaryCollector.metadata(() -> io.acquisition.collect(requests))),
                    "Deferred-clear native metadata changed during qualification");
            var last = As400NativeJournalWindow.collect(io.journals, io.identities, window.from());
            new As400NativeJournalWindow.Window(window.from(), through, before, last);
            var suffix = result.work.capture();
            var tailReceiver = last.directory().receivers().stream().filter(r -> r.name().equals(last.directory().attached())).findFirst().orElseThrow();
            var tail = new JournalPosition(tailReceiver.last(), tailReceiver.name());
            read(last, through, tail, suffix);
            for (var entry : suffix.seal()) {
                var h = entry.header();
                if (entry.position().offset().compareTo(through.offset()) > 0 && requested.containsKey(h.getLibrary() + "/" + h.getFile())
                        && (h.getJournalCode() == 'D' || h.getJournalCode() == 'F')
                        && !preservesDefinition(entry,
                                (file, library) -> bindings.get(library + "/" + file))) {
                    return Optional.empty(); // Read that source work before selecting its schema.
                }
            }
            var finalSample = As400NativeJournalWindow.collect(io.journals, io.identities, window.from());
            new As400NativeJournalWindow.Window(window.from(), tail, last, finalSample);
            var covered = result.work.set(As400SpillCodec.POSITION);
            for (var group : starts.entrySet()) {
                check.run();
                var start = result.entries.stream().filter(e -> e.position().offset().equals(group.getKey())).findFirst()
                        .orElseThrow(() -> new IllegalArgumentException("Missing original deferred-clear SC"));
                var selected = new HashMap<String, As400SourceNormalizer.TableBinding>();
                group.getValue().keySet().forEach(key -> selected.put(key, bindings.get(key)));
                var store = result.work.transactionStore();
                result.stores.add(store);
                var replay = new As400LegacyClearReplay(journalIdentity, group.getKey(), start.position(), group.getValue(), selected, order, limits, store);
                result.replays.add(replay);
                boolean borrowed = false;
                try {
                    for (var entry : result.entries) {
                        if (entry.position().offset().compareTo(group.getKey()) >= 0) {
                            replay.accept(entry);
                        }
                    }
                    if (replay.resolution().isEmpty()) {
                        return Optional.empty();
                    }
                    var resolution = replay.resolution().orElseThrow();
                    for (var entry : result.entries) {
                        var h = entry.header();
                        if (group.getValue().containsKey(h.getLibrary() + "/" + h.getFile())
                                && (h.getJournalCode() == 'D' || h.getJournalCode() == 'F')
                                && entry.position().offset().compareTo(group.getKey()) >= 0
                                && order.compare(entry.position(), resolution.consumedThrough()) <= 0) {
                            covered.add(entry.position());
                        }
                    }
                    if (group.getKey().equals(cycle)) {
                        result.resolution = resolution;
                        borrowed = true;
                    }
                }
                finally {
                    // Only the requested output borrows its image store. Later
                    // lifecycle proofs retain ordinal facts on disk, not one
                    // live cache/store for every transaction in retained history.
                    if (!borrowed) {
                        result.replays.remove(replay);
                        result.stores.remove(store);
                        try (var closingStore = store; var closingReplay = replay) {
                            // Release both resources even if lifecycle cleanup fails.
                        }
                    }
                }
            }
            for (var entry : result.entries) {
                var h = entry.header();
                String key = h.getLibrary() + "/" + h.getFile();
                if (!requested.containsKey(key) || h.getJournalCode() != 'D' && h.getJournalCode() != 'F') {
                    continue;
                }
                if (covered.contains(entry.position())
                        || preservesDefinition(entry,
                                (file, library) -> bindings.get(library + "/" + file))) {
                    continue;
                }
                if (h.getJournalCode() == 'F' && h.getEntryType().equals("CH")) {
                    String subtype = new AS400Text(1, RawJournalPage.METADATA_CCSID)
                            .toObject(As400JournalLifecycle.payload(new As400JournalLifecycle.Entry(h, entry.data(), entry.position()))).toString();
                    if (List.of("3", "8").contains(subtype)) {
                        return Optional.empty(); // An additional clear has begun but not yet supplied its D/CG/terminal.
                    }
                }
                throw new IllegalArgumentException("Unqualified schema or journaling transition during deferred clear recovery");
            }
            check.run();
            returned = true;
            return Optional.of(result);
        }
        catch (As400NativeBoundaryCollector.MetadataUnavailable unavailable) {
            check.run();
            return Optional.empty(); // No outcome has been published; a later prefix retries with fresh read-only handles.
        }
        finally {
            if (!returned) {
                result.close();
            }
        }
    }

    static boolean preservesDefinition(RawJournalPage.Entry entry, As400SourceNormalizer.TableResolver tables) throws Exception {
        if (entry.header().getJournalCode() == 'F' && entry.header().getEntryType().equals("CR")) {
            As400SourceNormalizer.validateMemberClear(entry.header(), entry.data(), 0, entry.position(), tables);
            // This is never dropped as administrative work. The held-table
            // prefix dispatches it in order after the transactional outcome.
            return true;
        }
        return As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(entry.header(), entry.data(), entry.position()), tables);
    }

    /** Walk only the required sequence epoch; unrelated old receivers grant no proof and need not be readable. */
    static JournalPosition originalStart(As400OperatorJournalEvidence.Observation directory, JournalPosition through, BigInteger cycle) {
        require(cycle.signum() > 0 && cycle.compareTo(through.offset()) < 0, "Invalid original deferred-clear cycle");
        var receivers = new HashMap<JournalReceiver, As400OperatorJournalEvidence.Receiver>();
        directory.receivers().forEach(r -> require(receivers.putIfAbsent(r.name(), r) == null, "Duplicate deferred-clear receiver"));
        var current = receivers.get(through.receiver());
        require(current != null && current.first().compareTo(through.offset()) <= 0 && through.offset().compareTo(current.last()) <= 0,
                "Deferred-clear scan receiver is unavailable");
        var seen = new java.util.HashSet<JournalReceiver>();
        while (true) {
            require(seen.add(current.name()), "Cyclic deferred-clear receiver history");
            current.requireReadableProfile();
            if (current.first().compareTo(cycle) <= 0 && cycle.compareTo(current.last()) <= 0) {
                return new JournalPosition(cycle, current.name());
            }
            var previous = current.previous().map(receivers::get).orElse(null);
            require(previous != null && previous.next().equals(Optional.of(current.name()))
                    && previous.last().compareTo(current.first()) < 0 && cycle.compareTo(current.first()) < 0,
                    "Required deferred-clear original start is unavailable or crosses a sequence reset");
            current = previous;
        }
    }

    private static As400JournalOrder order(As400NativeJournalWindow.Window window) {
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        As400OperatorCutReader.chain(window.before().directory(), window.from().receiver())
                .forEach(r -> ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last())));
        return new As400JournalOrder(new ArrayList<>(ranges.keySet()), window.receiverTokens(), ranges);
    }

    private void read(As400NativeJournalWindow.Sample sample, JournalPosition from, JournalPosition through, As400JournalCapture capture) throws Exception {
        for (var receiver : As400OperatorCutReader.chain(sample.directory(), from.receiver())) {
            receiver.requireReadableProfile();
            var next = receiver.name().equals(from.receiver()) ? from : new JournalPosition(receiver.first(), receiver.name());
            var end = receiver.name().equals(through.receiver()) ? through : new JournalPosition(receiver.last(), receiver.name());
            JournalPosition previous = null;
            while (true) {
                check.run();
                var page = io.reader.readRawPage(next, end, 1000);
                require(page.entries().size() <= 1000, "Oversized deferred-clear history page");
                for (var entry : page.entries()) {
                    require(entry.position().receiver().equals(receiver.name()) && entry.position().offset().compareTo(next.offset()) >= 0
                            && entry.position().offset().compareTo(end.offset()) <= 0
                            && (previous == null || entry.position().offset().compareTo(previous.offset()) > 0),
                            "Unordered or foreign deferred-clear history");
                    As400JournalRecordContent.digest(entry.data(), entry.position());
                    capture.append(entry);
                    previous = entry.position();
                }
                if (page.continuation().isEmpty()) {
                    break;
                }
                var continuation = page.continuation().orElseThrow();
                require(!page.entries().isEmpty() && continuation.receiver().equals(next.receiver())
                        && continuation.offset().compareTo(previous.offset()) > 0 && continuation.offset().compareTo(end.offset()) <= 0,
                        "Deferred-clear evidence did not progress");
                next = continuation;
            }
            if (receiver.name().equals(through.receiver())) {
                return;
            }
        }
        throw new IllegalArgumentException("Deferred-clear evidence did not reach its observed receiver");
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
