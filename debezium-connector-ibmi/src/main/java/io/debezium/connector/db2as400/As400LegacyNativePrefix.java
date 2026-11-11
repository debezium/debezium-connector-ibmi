/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

/**
 * Incremental legacy publication while an online boundary is being qualified.
 * Uses the existing row handler/dispatcher and native owned pages, without a
 * simulated RPC cursor. A returned progress offset still requires the task's
 * registered Kafka barrier and independent durable acknowledgement.
 */
final class As400LegacyNativePrefix {
    private final As400OperatorJournalEvidence journals;
    private final As400JdbcNativeIdentityEvidence identities;
    private final RetrieveJournal reader;
    private final As400StreamingChangeEventSource source;
    private final As400Partition partition;
    private final As400OffsetContext scan;
    private final Runnable checkRunning;
    private final As400OperatorCutReader.Limits limits;
    private As400NativeJournalWindow.Window previous;
    private boolean initialized;
    private Map<String, ?> completePosition;

    As400LegacyNativePrefix(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities, RetrieveJournal reader,
                            As400StreamingChangeEventSource source, As400Partition partition, As400OffsetContext scan,
                            Runnable checkRunning, As400OperatorCutReader.Limits limits) {
        As400LegacyHandoff.startingPosition(As400LegacyHandoff.strings(scan.getOffset()));
        this.journals = journals;
        this.identities = identities;
        this.reader = reader;
        this.source = source;
        this.partition = partition;
        this.scan = scan;
        this.checkRunning = checkRunning;
        this.limits = limits;
    }

    /** Never advances to a requested tail, continuation cursor or failed record. */
    Optional<Map<String, ?>> publishNext() throws Exception {
        completePosition = null;
        checkRunning.run();
        JournalPosition from = scan.getPosition().asJournalPosition();
        var before = As400NativeJournalWindow.collect(journals, identities, from);
        var chain = As400OperatorCutReader.chain(before.directory(), from.receiver());
        var attached = chain.get(chain.size() - 1);
        var tail = new JournalPosition(attached.last(), attached.name());
        var window = new As400NativeJournalWindow.Window(from, tail, before, before);
        if (previous != null) {
            require(previous.journalToken().equals(window.journalToken())
                    && previous.receiverTokens().get(from.receiver()).equals(window.receiverTokens().get(from.receiver())),
                    "Legacy source identity changed between prefixes");
        }
        checkRunning.run();
        var anchor = reader.readAuditHistoryPage(from, from, 1);
        require(anchor.entries().size() == 1 && anchor.entries().get(0).position().equals(from) && anchor.continuation().isEmpty(),
                "Required legacy history anchor is absent");
        As400JournalRecordContent.digest(anchor.entries().get(0).data(), from);
        var entries = new ArrayList<RawJournalPage.Entry>();
        int pages = 1;
        long bytes = anchor.entries().get(0).data().length;
        require(bytes <= limits.bytes(), "Legacy anchor exceeds byte bound");
        JournalPosition last = from;
        boolean anchorSeen = false;
        outer: for (var receiver : chain) {
            receiver.requireReadableProfile();
            JournalPosition next = receiver.name().equals(from.receiver()) ? from : new JournalPosition(receiver.first(), receiver.name());
            var end = new JournalPosition(receiver.last(), receiver.name());
            while (true) {
                checkRunning.run();
                require(++pages <= limits.pages(), "Legacy prefix exceeds page bound");
                var page = reader.readRawPage(next, end, limits.entries() - entries.size() + 1);
                require(page.entries().size() <= limits.entries() - entries.size() + 1, "Oversized native prefix response");
                for (var entry : page.entries()) {
                    byte[] raw = entry.data();
                    require((bytes += raw.length) <= limits.bytes(), "Legacy prefix exceeds byte bound");
                    require(entry.position().receiver().equals(receiver.name()) && entry.position().offset().compareTo(next.offset()) >= 0
                            && entry.position().offset().compareTo(end.offset()) <= 0, "Legacy prefix contains a foreign coordinate");
                    As400JournalRecordContent.digest(raw, entry.position());
                    if (entry.position().equals(from) && !anchorSeen && entries.isEmpty()) {
                        anchorSeen = true;
                        require(As400JournalRecordContent.digest(raw, from).equals(As400JournalRecordContent.digest(anchor.entries().get(0).data(), from)),
                                "Legacy anchor changed between selections");
                        continue;
                    }
                    require(entry.position().offset().compareTo(last.offset()) > 0, "Legacy prefix is unordered or repeated");
                    if (entries.size() < limits.entries()) {
                        entries.add(entry);
                    }
                    last = entry.position();
                }
                if (page.continuation().isPresent()) {
                    var continuation = page.continuation().orElseThrow();
                    require(!page.entries().isEmpty() && continuation.receiver().equals(receiver.name())
                            && continuation.offset().compareTo(page.entries().get(page.entries().size() - 1).position().offset()) > 0
                            && continuation.offset().compareTo(end.offset()) <= 0, "Invalid legacy native continuation");
                    next = continuation;
                }
                if (entries.size() == limits.entries()) {
                    break outer;
                }
                if (page.continuation().isEmpty()) {
                    break;
                }
            }
        }
        checkRunning.run();
        var after = As400NativeJournalWindow.collect(journals, identities, from);
        previous = new As400NativeJournalWindow.Window(from, tail, before, after);
        // A legacy row offset can point at the already-read UB immediately before
        // its emitted UP. Rebuild that cache from the validated anchor so a fresh
        // task preserves a primary-key change's delete/tombstone/create output.
        boolean replayAnchor = !initialized && !scan.getPosition().processed();
        if (!initialized && (replayAnchor
                || anchor.entries().get(0).header().getJournalEntryType() == io.debezium.ibmi.db2.journal.retrieve.JournalEntryType.BEFORE_IMAGE)) {
            checkRunning.run();
            source.processOwnedEntry(partition, scan, anchor.entries().get(0));
            checkRunning.run();
            scan.setPosition(new JournalProcessedPosition(from, anchor.entries().get(0).header().getTime(), true));
        }
        initialized = true;
        for (var entry : entries) {
            checkRunning.run();
            source.processOwnedEntry(partition, scan, entry);
            checkRunning.run();
            scan.setPosition(new JournalProcessedPosition(entry.position(), entry.header().getTime(), true));
        }
        checkRunning.run();
        if (!source.hasPendingBeforeImage()) {
            completePosition = Map.copyOf(scan.getOffset());
        }
        return entries.isEmpty() && !replayAnchor ? Optional.empty() : Optional.ofNullable(completePosition);
    }

    /**
     * Cross an actual, object-free user marker after a complete CDC read proves
     * there is no intervening source work. This is only a scan candidate; the
     * caller must still acknowledge its normal Kafka barrier. Never use the
     * receiver tail or an unobserved requested position as progress.
     */
    Optional<Map<String, ?>> advanceThroughMarker(JournalPosition marker) throws Exception {
        checkRunning.run();
        boolean complete = completePosition != null;
        completePosition = null;
        var from = scan();
        if (!complete || source.hasPendingBeforeImage() || marker.offset().compareTo(from.offset()) <= 0) {
            return Optional.empty();
        }
        var before = As400NativeJournalWindow.collect(journals, identities, from);
        var window = new As400NativeJournalWindow.Window(from, marker, before, before);
        require(previous != null && previous.journalToken().equals(window.journalToken())
                && previous.receiverTokens().get(from.receiver()).equals(window.receiverTokens().get(from.receiver())),
                "Legacy source identity changed before marker proof");
        var page = reader.readAuditHistoryPage(marker, marker, 1);
        require(page.entries().size() == 1 && page.continuation().isEmpty() && page.entries().get(0).position().equals(marker),
                "Required handoff marker is absent or ambiguous");
        var entry = page.entries().get(0);
        byte[] raw = entry.data();
        long bytes = raw.length;
        var header = As400JournalRecordContent.userMarker(entry);
        int pages = 1;
        for (var receiver : As400OperatorCutReader.chain(before.directory(), from.receiver())) {
            receiver.requireReadableProfile();
            require(++pages <= limits.pages(), "Marker proof exceeds page bound");
            var start = receiver.name().equals(from.receiver()) ? from : new JournalPosition(receiver.first(), receiver.name());
            var end = receiver.name().equals(marker.receiver()) ? marker : new JournalPosition(receiver.last(), receiver.name());
            checkRunning.run();
            var cdc = reader.readRawPage(start, end, 2);
            require(cdc.entries().size() <= 2, "Oversized marker CDC proof");
            boolean anchorSeen = false;
            for (var item : cdc.entries()) {
                byte[] data = item.data();
                require((bytes += data.length) <= limits.bytes(), "Marker proof exceeds byte bound");
                require(item.position().receiver().equals(receiver.name()) && item.position().offset().compareTo(start.offset()) >= 0
                        && item.position().offset().compareTo(end.offset()) <= 0, "Foreign marker CDC coordinate");
                As400JournalRecordContent.digest(data, item.position());
                if (!item.position().equals(from)) {
                    return Optional.empty(); // Let the ordinary dispatcher process this work first.
                }
                require(!anchorSeen, "Repeated legacy anchor in marker proof");
                anchorSeen = true;
            }
            if (cdc.continuation().isPresent()) {
                return Optional.empty();
            }
            if (receiver.name().equals(marker.receiver())) {
                break;
            }
        }
        require(bytes <= limits.bytes(), "Marker proof exceeds byte bound");
        var after = As400NativeJournalWindow.collect(journals, identities, from);
        previous = new As400NativeJournalWindow.Window(from, marker, before, after);
        checkRunning.run();
        require(!source.hasPendingBeforeImage(), "Incomplete legacy before-image at marker");
        scan.setPosition(new JournalProcessedPosition(marker, header.getTime(), true));
        completePosition = Map.copyOf(scan.getOffset());
        return Optional.of(completePosition);
    }

    /** Validated complete output boundary, including an unchanged anchor on an idle restart. Not a durable acknowledgement. */
    Optional<Map<String, ?>> completePosition() {
        checkRunning.run();
        return Optional.ofNullable(completePosition);
    }

    JournalPosition scan() {
        return scan.getPosition().asJournalPosition();
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
