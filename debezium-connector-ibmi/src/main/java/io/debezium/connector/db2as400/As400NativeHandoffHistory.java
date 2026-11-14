/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

/** Bounded native C/J and prospective row history. No publication or startup authority. */
final class As400NativeHandoffHistory implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(As400NativeHandoffHistory.class);

    record Observed(As400NativeJournalWindow.Window window, As400JournalOrder order,
            List<RawJournalPage.Entry> entries, As400ControlInventory.Inventory inventory) {
        Observed {
            entries = As400JournalCapture.owned(entries);
        }
    }

    record Prepared(Observed controls, As400NativeJournalWindow.Window rows,
            As400ProspectiveHandoffReplay.Prepared replay) {
    }

    private final As400OperatorJournalEvidence journals;
    private final As400JdbcNativeIdentityEvidence identities;
    private final RetrieveJournal reader;
    private final Runnable checkRunning;
    private final As400HandoffWork work;

    As400NativeHandoffHistory(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities,
                              RetrieveJournal reader, Runnable checkRunning) {
        this(journals, identities, reader, checkRunning, null);
    }

    As400NativeHandoffHistory(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities,
                              RetrieveJournal reader, Runnable checkRunning, As400HandoffWork work) {
        this.journals = journals;
        this.identities = identities;
        this.reader = reader;
        this.checkRunning = checkRunning;
        this.work = work;
    }

    Observed collect(As400NativeBoundaryCollector.Candidate candidate, JournalPosition through) throws Exception {
        checkRunning.run();
        var from = candidate.historyFloor();
        var before = As400NativeJournalWindow.collect(journals, identities, from);
        require(before.identities().objects().entrySet().containsAll(candidate.window().before().identities().objects().entrySet()),
                "Candidate journal/receivers changed before control discovery");
        var chain = As400OperatorCutReader.chain(before.directory(), from.receiver());
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        chain.forEach(r -> ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last())));
        var provisional = new As400NativeJournalWindow.Window(from, through, before, before);
        var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), provisional.receiverTokens(), ranges);
        require(order.compare(from, candidate.origin()) <= 0 && order.compare(candidate.origin(), through) <= 0,
                "Legacy cut has not reached the observed origin");
        var entries = read(from, through, chain, order, true);
        checkRunning.run();
        var after = As400NativeJournalWindow.collect(journals, identities, from);
        var window = new As400NativeJournalWindow.Window(from, through, before, after);
        require(after.identities().objects().entrySet().containsAll(candidate.window().after().identities().objects().entrySet()),
                "Candidate journal/receivers changed after control discovery");
        BigInteger overlapFrom = from.offset().max(candidate.window().from().offset());
        BigInteger overlapThrough = through.offset().min(candidate.window().through().offset());
        require(As400HandoffWork.sameProjection(entries, candidate.audit().entries(), overlapFrom, overlapThrough, java.util.Set.of('C')),
                "Native control discovery differs from complete boundary audit");
        var inventory = As400ControlInventory.reconstruct(entries, order, window.journalToken(), from, candidate.origin(), through, work);
        checkRunning.run();
        LOGGER.info("Validated transactional truncate discovery history across {} control entries", entries.size());
        return new Observed(window, order, entries, inventory);
    }

    Prepared prepare(As400NativeBoundaryCollector.Candidate candidate, JournalPosition cut,
                     As400ConnectorConfig config, As400CaptureFingerprint.Policy policy)
            throws Exception {
        // Preserve the selected metadata contract. Replay additionally waits for
        // every legacy metadata cycle to finish before a handoff is admitted.
        require(config.shouldProvideTransactionMetadata() == policy.transactionMetadata(), "Legacy metadata setting differs from capture history");
        checkRunning.run();
        var controls = collect(candidate, cut);
        var from = candidate.origin();
        var before = As400NativeJournalWindow.collect(journals, identities, from);
        require(controls.window().before().identities().objects().entrySet().containsAll(before.identities().objects().entrySet()),
                "Row/control source identities differ before replay");
        var chain = As400OperatorCutReader.chain(before.directory(), from.receiver());
        var provisional = new As400NativeJournalWindow.Window(from, cut, before, before);
        var entries = read(from, cut, chain, controls.order(), false);
        checkRunning.run();
        var after = As400NativeJournalWindow.collect(journals, identities, from);
        var rows = new As400NativeJournalWindow.Window(from, cut, before, after);
        require(provisional.journalToken().equals(controls.window().journalToken())
                && controls.window().after().identities().objects().entrySet().containsAll(after.identities().objects().entrySet()),
                "Row/control source identities differ after replay");
        var overlapThrough = cut.offset().min(candidate.window().through().offset());
        var codes = java.util.Set.of('C', 'D', 'F', 'R');
        require(As400HandoffWork.sameProjection(entries, candidate.audit().entries(), from.offset(), overlapThrough, codes),
                "Native row history differs from complete boundary audit");
        String fingerprint = "v1:" + As400CaptureFingerprint.hash("online-handoff-candidate-v1", java.util.Map.of(
                "bindings", As400NativeBoundaryCollector.digest(candidate.bindings().definitions()),
                "policy", policy.fingerprint(candidate.bindings().definitions().stream().flatMap(d -> d.identities().stream()).toList()),
                "journal", rows.journalToken(), "origin", from.offset().toString(), "receiver", rows.receiverTokens().get(from.receiver())));
        var evidence = new As400HandoffReplay.Evidence(entries, candidate.bindings().definitions(), controls.order(), rows.journalToken(), fingerprint,
                from, from, controls.inventory().openAtLower().keySet(), policy.sourcePartition(), work);
        var prepared = As400ProspectiveHandoffReplay.prepare(evidence, controls.entries(), from, cut, config);
        checkRunning.run();
        return new Prepared(controls, rows, prepared);
    }

    /**
     * Rebuild an interrupted pending migration from its persisted discovery
     * floor. Current pending-transaction flags cannot replace that floor: an old
     * transaction may have committed while this task was down. No current table
     * schema, fixture capture or synthetic output position is used here.
     */
    Prepared resume(As400BindingHistory.Pending pending, JournalPosition cut, As400ConnectorConfig config,
                    As400CaptureFingerprint.Policy policy)
            throws Exception {
        require(config.shouldProvideTransactionMetadata() == policy.transactionMetadata(), "Legacy metadata setting differs from capture history");
        var discovery = pending.discovery().orElseThrow(() -> new IllegalArgumentException("Pending history lacks its validated discovery floor"));
        checkRunning.run();
        var from = discovery.floor();
        var before = As400NativeJournalWindow.collect(journals, identities, from);
        var provisional = new As400NativeJournalWindow.Window(from, cut, before, before);
        require(provisional.journalToken().equals(pending.journal())
                && discovery.receiverIdentity().equals(provisional.receiverTokens().get(from.receiver()))
                && pending.receiverIdentity().equals(provisional.receiverTokens().get(pending.origin().receiver())),
                "Pending discovery source identity changed");
        var chain = As400OperatorCutReader.chain(before.directory(), from.receiver());
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        chain.forEach(r -> ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last())));
        var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), provisional.receiverTokens(), ranges);
        require(order.compare(from, pending.origin()) <= 0 && order.compare(pending.origin(), cut) <= 0,
                "Pending schema origin is outside the retained handoff interval");
        var controls = read(from, cut, chain, order, true);
        var rowChain = As400OperatorCutReader.chain(before.directory(), pending.origin().receiver());
        var entries = read(pending.origin(), cut, rowChain, order, false);
        checkRunning.run();
        var after = As400NativeJournalWindow.collect(journals, identities, from);
        var window = new As400NativeJournalWindow.Window(from, cut, before, after);
        var inventory = As400ControlInventory.reconstruct(controls, order, pending.journal(), from, pending.origin(), cut, work);
        var evidence = new As400HandoffReplay.Evidence(entries, pending.definitions(), order, pending.journal(), pending.fingerprint(),
                pending.origin(), pending.origin(), inventory.openAtLower().keySet(), policy.sourcePartition(), work);
        As400ProspectiveHandoffReplay.Prepared prepared;
        if (entries.isEmpty() && cut.equals(pending.origin())) {
            // This is an empty schema epoch, not a fabricated terminal or an
            // empty-page inference. The native origin must still exist, all
            // C/J history must be complete, and no prior transaction may remain.
            if (!inventory.openAtLower().isEmpty() || !inventory.openThrough().isEmpty()) {
                throw new As400ProspectiveHandoffReplay.PriorEpochPending(inventory.openThrough().keySet());
            }
            var anchor = reader.readAuditHistoryPage(cut, cut, 1);
            require(anchor.entries().size() == 1 && anchor.continuation().isEmpty() && anchor.entries().get(0).position().equals(cut),
                    "Empty online origin marker is absent or ambiguous");
            As400JournalRecordContent.userMarker(anchor.entries().get(0));
            var checked = As400NativeJournalWindow.collect(journals, identities, from);
            new As400NativeJournalWindow.Window(from, cut, before, checked);
            var checkpoint = As400Checkpoint.afterVerifiedBoundary(pending.journal(), pending.fingerprint(), cut, order.incarnation(cut));
            prepared = new As400ProspectiveHandoffReplay.Prepared(evidence, new As400HandoffReplay.Candidate(checkpoint, cut, java.util.Set.of()), java.util.Set.of());
        }
        else {
            prepared = As400ProspectiveHandoffReplay.prepare(evidence, controls, pending.origin(), cut, config);
        }
        checkRunning.run();
        LOGGER.info("Validated transactional truncate replay history across {} source entries and {} controls", entries.size(), controls.size());
        return new Prepared(new Observed(window, order, controls, inventory), window, prepared);
    }

    private List<RawJournalPage.Entry> read(JournalPosition from, JournalPosition through,
                                            List<As400OperatorJournalEvidence.Receiver> chain, As400JournalOrder order, boolean controls)
            throws Exception {
        var capture = work == null ? null : work.capture();
        var entries = capture == null ? new ArrayList<RawJournalPage.Entry>() : null;
        int entryCount = 0;
        int pages = 0;
        long bytes = 0;
        JournalPosition previous = null;
        boolean reached = false;
        for (var receiver : chain) {
            receiver.requireReadableProfile();
            JournalPosition next = receiver.name().equals(from.receiver()) ? from : new JournalPosition(receiver.first(), receiver.name());
            JournalPosition end = receiver.name().equals(through.receiver()) ? through : new JournalPosition(receiver.last(), receiver.name());
            while (true) {
                checkRunning.run();
                require(work != null || ++pages <= 128 && entryCount < 20_000, "Native handoff history exceeds page/entry bounds");
                int limit = work == null ? Math.min(1000, 20_000 - entryCount) : 1000;
                RawJournalPage page;
                if (controls) {
                    var selected = reader.readControlHistoryPage(next, end, limit);
                    page = new RawJournalPage(selected.entries(), selected.continuation(), selected.bytesReturned());
                }
                else {
                    page = reader.readRawPage(next, end, limit);
                }
                require(page.entries().size() <= limit, "Native history page exceeds the requested bound");
                for (var entry : page.entries()) {
                    byte[] raw = entry.data();
                    require(raw.length <= 1024 * 1024 && (work != null || (bytes += raw.length) <= 64 * 1024 * 1024),
                            "Native handoff history exceeds byte bounds");
                    require(entry.position().receiver().equals(receiver.name()) && order.compare(next, entry.position()) <= 0
                            && order.compare(entry.position(), end) <= 0 && (previous == null || order.compare(previous, entry.position()) < 0),
                            "Repeated, foreign or unordered native handoff entry");
                    As400JournalRecordContent.digest(raw, entry.position());
                    if (capture == null) {
                        entries.add(entry);
                    }
                    else {
                        capture.append(entry);
                    }
                    entryCount = Math.incrementExact(entryCount);
                    previous = entry.position();
                }
                if (page.continuation().isEmpty()) {
                    break;
                }
                var continuation = page.continuation().orElseThrow();
                require(!page.entries().isEmpty() && continuation.receiver().equals(receiver.name())
                        && order.compare(previous, continuation) < 0 && order.compare(continuation, end) <= 0,
                        "Invalid native handoff continuation");
                next = continuation;
            }
            if (receiver.name().equals(through.receiver())) {
                reached = true;
                break;
            }
        }
        require(reached, "Native history does not reach requested cut");
        return capture == null ? List.copyOf(entries) : capture.seal();
    }

    @Override
    public void close() throws Exception {
        if (work != null) {
            work.close();
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
