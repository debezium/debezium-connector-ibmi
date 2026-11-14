/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Receiver;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

/**
 * Bounded owned history for the ordinary operator-retention contract. Equality
 * checks are observations, not retention/maintenance leases. Nothing here supplies
 * protected Source/Inspection proof, publishes output or authorizes retention release.
 * Source/decoder/task integration and native qualification remain required.
 */
final class As400OperatorCutReader {
    record Limits(int pages, int entries, long bytes) {
        Limits {
            if (pages < 1 || pages > 100_000 || entries < 1 || entries > 100_000 || bytes < 236 || bytes > 1024L * 1024 * 1024) {
                throw new IllegalArgumentException("Finite ordinary history bounds required");
            }
        }
    }

    record Cut(As400Checkpoint checkpoint, As400JournalOrder order, JournalPosition tail, List<RawJournalPage.Entry> entries,
            List<As400OperatorTableEvidence.Observation> tables, Optional<As400BindingHistory.Recovered> historicalBindings, boolean complete) {
        Cut(As400Checkpoint checkpoint, As400JournalOrder order, JournalPosition tail, List<RawJournalPage.Entry> entries,
            List<As400OperatorTableEvidence.Observation> tables, Optional<As400BindingHistory.Recovered> historicalBindings) {
            this(checkpoint, order, tail, entries, tables, historicalBindings, true);
        }

        Cut {
            entries = List.copyOf(entries);
            tables = List.copyOf(tables);
            Objects.requireNonNull(historicalBindings);
        }
    }

    private final As400OperatorJournalEvidence journals;
    private final As400JdbcNativeIdentityEvidence identities;
    private final As400OperatorTableEvidence tables;
    private final List<As400OperatorTableEvidence.Request> requests;
    private final RetrieveJournal reader;
    private final Limits limits;
    private final List<TableIdentity> expectedTables;
    private final String fingerprint;
    private final As400BindingHistory.Recovered recovered;
    private final As400JdbcConnectionFactory intervalConnections;

    private record EmptyReceiver(Receiver observation, String identity) {
    }

    private final Map<JournalReceiver, EmptyReceiver> emptyReceivers = new HashMap<>();

    As400OperatorCutReader(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities,
                           As400OperatorTableEvidence tables, List<As400OperatorTableEvidence.Request> requests,
                           RetrieveJournal reader, Limits limits, List<TableIdentity> expectedTables, As400CaptureFingerprint.Policy policy) {
        this.journals = Objects.requireNonNull(journals);
        this.identities = Objects.requireNonNull(identities);
        this.tables = Objects.requireNonNull(tables);
        this.requests = List.copyOf(requests);
        this.reader = Objects.requireNonNull(reader);
        this.limits = Objects.requireNonNull(limits);
        this.expectedTables = List.copyOf(expectedTables);
        recovered = null;
        intervalConnections = null;
        fingerprint = policy.fingerprint(expectedTables);
        require(!this.requests.isEmpty() && this.requests.size() == this.expectedTables.size()
                && this.requests.stream().distinct().count() == this.requests.size(), "Distinct matching captured tables required");
    }

    /**
     * Separate fixed-schema ordinary profile. Only a recovered exact history
     * manifest can select this path; no catch-and-ignore of catalog failures.
     * Caller must still run every entry through the identity/lifecycle normalizer.
     */
    As400OperatorCutReader(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities,
                           JournalReceiver journal, As400BindingHistory.Recovered recovered, RetrieveJournal reader, Limits limits) {
        this(journals, identities, journal, recovered, reader, limits, null);
    }

    As400OperatorCutReader(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities,
                           JournalReceiver journal, As400BindingHistory.Recovered recovered, RetrieveJournal reader, Limits limits,
                           As400JdbcConnectionFactory intervalConnections) {
        this.intervalConnections = intervalConnections;
        this.journals = Objects.requireNonNull(journals);
        this.identities = Objects.requireNonNull(identities);
        this.recovered = Objects.requireNonNull(recovered);
        this.reader = Objects.requireNonNull(reader);
        this.limits = Objects.requireNonNull(limits);
        tables = null;
        expectedTables = recovered.definitions().stream().map(As400BindingDefinition::identity).toList();
        requests = expectedTables.stream().map(t -> new As400OperatorTableEvidence.Request(journal, t.tableId().schema(), t.physicalFile())).toList();
        fingerprint = recovered.fingerprint();
    }

    Cut read(Map<String, ?> offset) throws Exception {
        return readInternal(offset, null, false);
    }

    /** Read only the unscanned suffix, retaining independent F/W anchors and exact R bytes. */
    Cut readAfter(Map<String, ?> offset, RawJournalPage.Entry scan) throws Exception {
        require(recovered != null, "Incremental reading requires recovered immutable binding history");
        return readInternal(offset, Objects.requireNonNull(scan), false);
    }

    /** Bounded prefix of a stable native cut; F/W anchors remain independently verified. */
    Cut readPrefix(Map<String, ?> offset, RawJournalPage.Entry scan) throws Exception {
        require(recovered != null && limits.entries() >= 2 && limits.pages() >= 3, "Prefix reading requires history and room for anchors plus progress");
        return readInternal(offset, scan, true);
    }

    private Cut readInternal(Map<String, ?> offset, RawJournalPage.Entry scan, boolean prefix) throws Exception {
        // Refuse implicit first-start/legacy migration before any I/O.
        if (offset == null || !offset.containsKey(As400Checkpoint.VERSION)) {
            throw new IllegalArgumentException("Ordinary resume requires an explicitly initialized F/W checkpoint");
        }
        // Parse only to select exact metadata. The supplied identity is not proof:
        // the combined journal/receiver sample below must match it before reads.
        As400Checkpoint requested = As400Checkpoint.fromOffset(offset, (String) offset.get("journal_identity"), fingerprint);
        // Share only this interval's qualified, non-transactional metadata
        // connection. Both endpoints are queried afresh; no evidence is cached.
        // A close failure prevents the otherwise verified cut from escaping.
        try (Connection connection = intervalConnections == null ? null : Objects.requireNonNull(intervalConnections.open())) {
            return readObserved(offset, scan, prefix, requested, connection);
        }
    }

    private Cut readObserved(Map<String, ?> offset, RawJournalPage.Entry scan, boolean prefix, As400Checkpoint requested, Connection connection) throws Exception {
        JournalReceiver floor = requested.replayFrom().receiver();
        Observation before = observe(connection, prefix, floor);
        require(requests.stream().allMatch(request -> request.journal().equals(before.journal())), "Table and history journals differ");
        Target journalTarget = target(before.journal(), Kind.JOURNAL);
        // Ordinary snapshots retain W inside F's history; never invent a missing older anchor.
        List<Receiver> chain = chain(before, floor);
        List<Target> targets = new ArrayList<>();
        targets.add(journalTarget);
        chain.forEach(r -> targets.add(target(r.name(), Kind.RECEIVER)));
        Candidates nativeBefore = identify(connection, targets);
        String journalIdentity = token(journalTarget, nativeBefore.objects().get(journalTarget));
        require(recovered == null || recovered.journalIdentity().equals(journalIdentity), "Binding journal identity changed");
        As400Checkpoint loaded = As400Checkpoint.fromOffset(offset, journalIdentity, fingerprint);
        List<As400OperatorTableEvidence.Observation> tableBefore = recovered == null ? tables.collect(requests) : List.of();
        require(recovered != null || tableBefore.stream().map(As400OperatorTableEvidence.Observation::tableIdentity).toList().equals(expectedTables),
                "Captured file/member association or row format changed");
        As400ReceiverChain qualified = qualify(journalIdentity, chain, nativeBefore);
        qualified.verify(loaded);
        if (recovered != null) {
            recovered.verify(loaded, qualified.order());
        }
        require(qualified.order().compare(loaded.replayFrom(), loaded.completedThrough()) <= 0,
                "Ordinary retention must include the W anchor");
        Receiver last = chain.get(chain.size() - 1);
        JournalPosition tail = new JournalPosition(last.last(), last.name());
        List<RawJournalPage.Entry> captured = new ArrayList<>();
        int pages = 0;
        long bytes = 0;
        JournalPosition from = scan == null ? loaded.replayFrom() : scan.position();
        require((prefix ? qualified.order().compare(loaded.replayFrom(), from) <= 0 : qualified.order().compare(loaded.completedThrough(), from) <= 0) || scan == null,
                "Scan position precedes durable completed output");
        boolean emptyOnlineOrigin = recovered != null && recovered.emptyOnlineOrigin(loaded);
        int originReserve = emptyOnlineOrigin && from.equals(loaded.replayFrom()) ? 1 : 0;
        if (scan != null || prefix) {
            var anchors = new java.util.LinkedHashSet<>(List.of(loaded.replayFrom(), loaded.completedThrough()));
            anchors.remove(from);
            for (JournalPosition anchor : anchors) {
                require(++pages <= limits.pages(), "Ordinary anchor reads exceed configured bounds");
                RawJournalPage page = reader.readRawPage(anchor, anchor, 1);
                if (emptyOnlineOrigin && anchor.equals(loaded.replayFrom()) && page.entries().isEmpty() && page.continuation().isEmpty()) {
                    require(++pages <= limits.pages(), "Empty online origin exceeds anchor bound");
                    var audit = reader.readAuditHistoryPage(anchor, anchor, 1);
                    page = new RawJournalPage(audit.entries(), audit.continuation(), audit.bytesReturned());
                    require(page.entries().size() == 1, "Empty online origin marker is absent");
                    As400JournalRecordContent.userMarker(page.entries().get(0));
                }
                require(page.entries().size() == 1 && page.entries().get(0).position().equals(anchor) && page.continuation().isEmpty(),
                        "Required durable history anchor is absent");
                bytes += page.entries().get(0).data().length;
                require(bytes <= limits.bytes(), "Ordinary anchors exceed byte bound");
            }
        }
        boolean reached = false;
        boolean complete = true;
        boolean hasReplay = false;
        Map<JournalReceiver, EmptyReceiver> newlyEmpty = new HashMap<>();
        retrieval: for (int index = 0; index < chain.size(); index++) {
            Receiver receiver = chain.get(index);
            boolean first = receiver.name().equals(from.receiver());
            reached |= first;
            if (!reached) {
                continue;
            }
            JournalPosition next = new JournalPosition(first ? from.offset() : receiver.first(), receiver.name());
            JournalPosition through = new JournalPosition(receiver.last(), receiver.name());
            EmptyReceiver observed = new EmptyReceiver(receiver, qualified.order().incarnation(next));
            boolean detached = !receiver.name().equals(before.attached());
            if (!first && detached && observed.equals(emptyReceivers.get(receiver.name()))) {
                continue;
            }
            while (true) {
                if (prefix && (hasReplay && pages >= limits.pages() - originReserve || captured.size() >= limits.entries() - originReserve)) {
                    complete = false;
                    break retrieval;
                }
                ++pages;
                require((prefix || pages <= limits.pages() - originReserve) && captured.size() < limits.entries() - originReserve,
                        "Ordinary replay history exceeds configured bounds");
                RawJournalPage page = reader.readRawPage(next, through, limits.entries() - captured.size() - originReserve);
                if (!first && detached && next.offset().equals(receiver.first()) && page.entries().isEmpty() && page.continuation().isEmpty()) {
                    newlyEmpty.put(receiver.name(), observed);
                }
                for (RawJournalPage.Entry entry : page.entries()) {
                    if (prefix && bytes + entry.data().length > limits.bytes()) {
                        complete = false;
                        break retrieval;
                    }
                    bytes += entry.data().length;
                    require(bytes <= limits.bytes(), "Ordinary owned history exceeds byte bound");
                    captured.add(entry);
                    hasReplay |= qualified.order().compare(entry.position(), from) > 0
                            || entry.position().equals(from) && scan == null && loaded.replayInclusive();
                }
                if (page.continuation().isEmpty()) {
                    break;
                }
                next = page.continuation().orElseThrow();
            }
        }
        if (emptyOnlineOrigin && from.equals(loaded.replayFrom()) && captured.stream().noneMatch(e -> e.position().equals(from))) {
            // Existing online baselines can also be ordinary CDC anchors. Only
            // an absent exclusive origin may use the separately retained U/DN
            // marker; no other missing F/W or scan anchor gets this exception.
            require((prefix || ++pages <= limits.pages()) && captured.size() < limits.entries(), "Empty online origin exceeds anchor bound");
            var page = reader.readAuditHistoryPage(from, from, 1);
            require(page.entries().size() == 1 && page.entries().get(0).position().equals(from) && page.continuation().isEmpty(),
                    "Empty online origin marker is absent");
            As400JournalRecordContent.userMarker(page.entries().get(0));
            captured.add(0, page.entries().get(0));
            bytes += page.entries().get(0).data().length;
            require(bytes <= limits.bytes(), "Empty online origin exceeds byte bound");
        }
        if (scan == null) {
            require(captured.stream().anyMatch(e -> e.position().equals(loaded.replayFrom())), "Replay floor anchor is absent");
            require(prefix || captured.stream().anyMatch(e -> e.position().equals(loaded.completedThrough())), "Completed-output anchor is absent");
        }
        else {
            require(captured.stream().anyMatch(e -> e.position().equals(from) && sameScanAnchor(scan, e)),
                    "Scan anchor is missing or its raw bytes changed");
        }
        // No partially inspected bytes escape on failure. File native tuple changes,
        // including a concurrent legitimate truncate, require a fresh complete read.
        if (recovered == null) {
            require(tableBefore.equals(tables.collect(requests)), "Native table observations changed during retrieval");
        }
        require(nativeBefore.equals(identify(connection, targets)), "Journal or receiver object changed during retrieval");
        Observation after = observe(connection, prefix, floor);
        stableCut(before, after, chain);
        // Only immutable detached ranges from a fully checked interval may be
        // reused. Every poll still verifies identity, range, links, readability,
        // and the actual F/W/R anchors. This cache never manufactures progress.
        Set<JournalReceiver> retained = chain.stream().map(Receiver::name).collect(java.util.stream.Collectors.toSet());
        emptyReceivers.keySet().retainAll(retained);
        emptyReceivers.putAll(newlyEmpty);
        List<RawJournalPage.Entry> replay = captured.stream().filter(entry -> {
            int comparison = qualified.order().compare(entry.position(), from);
            return comparison > 0 || comparison == 0 && scan == null && loaded.replayInclusive();
        }).toList();
        require(complete || !replay.isEmpty(), "Native prefix budget cannot admit progress after anchors");
        return new Cut(loaded, qualified.order(), complete ? tail : captured.get(captured.size() - 1).position(), replay,
                tableBefore, Optional.ofNullable(recovered), complete);
    }

    private Observation observe(Connection connection, boolean prefix, JournalReceiver floor) throws java.sql.SQLException {
        if (connection == null) {
            return prefix ? journals.collectFrom(floor) : journals.collect();
        }
        return prefix ? journals.collectFromOn(connection, floor) : journals.collectOn(connection);
    }

    private Candidates identify(Connection connection, List<Target> targets) throws java.sql.SQLException {
        return connection == null ? identities.collect(targets) : identities.collectOn(connection, targets);
    }

    static boolean sameScanAnchor(RawJournalPage.Entry expected, RawJournalPage.Entry actual) {
        return expected.position().equals(actual.position()) && java.util.Arrays.equals(anchorBytes(expected), anchorBytes(actual));
    }

    private static byte[] anchorBytes(RawJournalPage.Entry entry) {
        byte[] raw = entry.data();
        var header = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(raw, 0);
        require(header.getNextEntryOffset() == 0 && header.getSequenceNumber().equals(entry.position().offset()), "Invalid owned scan anchor");
        if (!header.hasReceiver()) {
            return withoutAlignmentPadding(raw);
        }
        require(header.getReceiver().equals(entry.position().receiver().name())
                && header.getReceiverLibrary().equals(entry.position().receiver().library()), "Scan anchor receiver mismatch");
        var fields = java.nio.ByteBuffer.wrap(raw);
        // QjoRetrieveJournalEntries adds receiver information only to the first
        // entry in a buffer (or when its receiver changes). Explicit single-receiver
        // calls already bind that identity. Remove ONLY this checked 32-byte wrapper,
        // adjusting its displacements, before comparing every remaining source byte.
        // The admitted profile has no XID/LUW and puts receiver information first.
        require(fields.getInt(20) == 236 && fields.getInt(12) == 0 && fields.getInt(16) == 0,
                "Unsupported scan anchor receiver layout");
        // ReceiverNameDecoder exposes only name/library. The remaining wrapper
        // fields are still identity: this profile admits *SYSBAS / ASP 1 only.
        require(fields.getShort(266) == 1
                && java.util.Arrays.equals(java.util.Arrays.copyOfRange(raw, 256, 266),
                        new com.ibm.as400.access.AS400Text(10, 37).toBytes("*SYSBAS")),
                "Unsupported scan anchor receiver ASP identity");
        byte[] canonical = new byte[raw.length - 32];
        System.arraycopy(raw, 0, canonical, 0, 236);
        System.arraycopy(raw, 268, canonical, 236, raw.length - 268);
        var adjusted = java.nio.ByteBuffer.wrap(canonical);
        adjusted.putInt(20, 0);
        for (int index : List.of(4, 8)) {
            int displacement = fields.getInt(index);
            require(displacement == 0 || displacement >= 268, "Scan anchor variable section overlaps receiver information");
            adjusted.putInt(index, displacement == 0 ? 0 : displacement - 32);
        }
        new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(canonical, 0);
        return withoutAlignmentPadding(canonical);
    }

    private static byte[] withoutAlignmentPadding(byte[] raw) {
        var fields = java.nio.ByteBuffer.wrap(raw);
        require(fields.getInt(12) == 0 && fields.getInt(16) == 0 && fields.getInt(20) == 0 && fields.getInt(72) == 0,
                "Unsupported pointer/XID/LUW scan anchor");
        int end = 236;
        int nulls = fields.getInt(4);
        int specific = fields.getInt(8);
        if (nulls != 0) {
            require(nulls >= 236 && (long) nulls + 4 <= raw.length, "Invalid anchor null-map displacement");
            int length = fields.getInt(nulls);
            require(length >= 0 && (long) nulls + 4 + length <= raw.length, "Invalid anchor null-map length");
            end = nulls + 4 + length;
        }
        if (specific != 0) {
            require(specific >= end && (long) specific + 16 <= raw.length, "Overlapping or incomplete anchor payload prefix");
            int length = ((java.math.BigDecimal) new com.ibm.as400.access.AS400ZonedDecimal(5, 0).toObject(raw, specific)).intValueExact();
            require(length >= 0 && (long) specific + 16 + length <= raw.length, "Invalid anchor payload length");
            end = specific + 16 + length;
        }
        // Nonfinal response entries can carry zero fill to the next 16-byte
        // header boundary. Native first/last captures prove the difference; only
        // bytes outside the documented length-delimited sections can be removed.
        require(raw.length == end || raw.length == (end + 15) / 16 * 16, "Unexpected anchor trailing section");
        for (int i = end; i < raw.length; i++) {
            require(raw[i] == 0, "Nonzero anchor alignment fill");
        }
        return java.util.Arrays.copyOf(raw, end);
    }

    /** Tokens identify observed native objects within this explicitly restricted source profile. */
    static String token(Target target, Identity identity) {
        Objects.requireNonNull(identity, "Missing native object");
        return "ordinary-native-v1:" + As400CaptureFingerprint.hash("ordinary-native-object-v1", Map.of(
                "path", target.path(), "type", target.kind().name(), "filesystem", identity.fileSystem().toString(),
                "file", identity.file().toString(), "generation", identity.generation().toString()));
    }

    private static Target target(JournalReceiver name, Kind kind) {
        return new Target("/QSYS.LIB/" + name.library() + ".LIB/" + name.name() + (kind == Kind.JOURNAL ? ".JRN" : ".JRNRCV"), kind);
    }

    static List<Receiver> chain(Observation observation, JournalReceiver floor) {
        Map<JournalReceiver, Receiver> directory = new HashMap<>();
        for (Receiver receiver : observation.receivers()) {
            require(directory.putIfAbsent(receiver.name(), receiver) == null, "Duplicate receiver directory entry");
        }
        Set<JournalReceiver> successors = new HashSet<>();
        for (Receiver receiver : observation.receivers()) {
            require(receiver.next().isEmpty() || successors.add(receiver.next().orElseThrow()), "Forked receiver directory");
        }
        List<Receiver> result = new ArrayList<>();
        Set<JournalReceiver> seen = new HashSet<>();
        Receiver current = directory.get(floor);
        while (current != null) {
            require(seen.add(current.name()), "Cyclic receiver directory");
            current.requireReadableProfile();
            result.add(current);
            if (current.name().equals(observation.attached())) {
                require(current.status().equals("ATTACHED") && current.next().isEmpty(), "Inconsistent attached receiver");
                return List.copyOf(result);
            }
            require(!current.status().equals("ATTACHED") && current.next().isPresent(), "Missing receiver successor");
            Receiver next = directory.get(current.next().orElseThrow());
            require(next != null && next.previous().equals(Optional.of(current.name())) && next.first() != null
                    && next.first().compareTo(current.last()) > 0, "Broken reciprocal linkage or journal sequence reset");
            current = next;
        }
        throw new IllegalArgumentException("Required receiver is missing");
    }

    private static As400ReceiverChain qualify(String journal, List<Receiver> chain, Candidates identities) {
        Map<JournalReceiver, As400ReceiverChain.Identity> names = new HashMap<>();
        for (Receiver receiver : chain) {
            Target target = target(receiver.name(), Kind.RECEIVER);
            names.put(receiver.name(), new As400ReceiverChain.Identity(receiver.name(), token(target, identities.objects().get(target))));
        }
        List<As400ReceiverChain.Receiver> observed = new ArrayList<>();
        for (Receiver receiver : chain) {
            observed.add(new As400ReceiverChain.Receiver(names.get(receiver.name()), journal, receiver.first(), receiver.last(),
                    receiver.status().equals("ATTACHED") ? As400ReceiverChain.Availability.ATTACHED : As400ReceiverChain.Availability.RETAINED,
                    receiver.next().map(names::get), false));
        }
        Receiver last = chain.get(chain.size() - 1);
        return new As400ReceiverChain(journal, observed, new JournalPosition(last.last(), last.name()));
    }

    private static void stableCut(Observation before, Observation after, List<Receiver> required) {
        require(before.journal().equals(after.journal()), "Journal changed during retrieval");
        if (!before.attached().equals(after.attached())) {
            throw new UnstableCutException("Journal attachment changed during retrieval; discard and revalidate the complete interval");
        }
        List<Receiver> observed = chain(after, required.get(0).name());
        require(observed.size() == required.size(), "Receiver chain changed during retrieval");
        for (int i = 0; i < required.size(); i++) {
            Receiver a = required.get(i);
            Receiver b = observed.get(i);
            require(a.name().equals(b.name()) && a.first().equals(b.first()) && a.previous().equals(b.previous()) && a.next().equals(b.next())
                    && As400NativeJournalWindow.sameReadableStatus(a.status(), b.status())
                    && (a.status().equals("ATTACHED") ? b.last().compareTo(a.last()) >= 0 : a.last().equals(b.last())),
                    "Previously inspected receiver range changed");
        }
    }

    /** Only an attachment race before any bytes escape is eligible for bounded rereading. */
    static final class UnstableCutException extends IllegalArgumentException {
        UnstableCutException(String message) {
            super(message);
        }
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
