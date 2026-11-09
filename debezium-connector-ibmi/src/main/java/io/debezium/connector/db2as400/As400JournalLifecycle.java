/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableResolver;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Incremental classifier for the narrowly observed native clear lifecycle profile.
 * Explicit coordinator admission only; not a protected startup proof.
 * Holds component records until every overlapping lifecycle window is complete.
 * The caller supplies verified history, paired identities and immutable layouts.
 * The ordinary profile requires verified receiver order; ambiguous producers fail closed.
 */
final class As400JournalLifecycle implements AutoCloseable {
    record Limits(int entries, long bytes, int cycles, int markers, int clears, int dependencies) {
        Limits {
            require(entries > 0 && bytes >= 236 && cycles > 0 && markers > 0 && clears > 0 && dependencies > 0,
                    "Finite positive lifecycle limits required");
        }
    }

    record Entry(EntryHeader header, byte[] bytes, JournalPosition position) {
        String type() {
            return header.getJournalCode() + "/" + header.getEntryType();
        }
    }

    private record Producer(String system, String job, String user, String number, BigInteger thread) {
    }

    private record Marker(BigInteger sequence, long level, int clears) {
    }

    private record Dependency(JournalPosition start, JournalPosition end) {
    }

    private static final class Cycle {
        final Producer producer;
        final JournalPosition start;
        final Map<BigInteger, Marker> markers = new LinkedHashMap<>();
        final List<TableBinding> clears = new ArrayList<>();

        private final long timestamp;

        Cycle(Producer producer, JournalPosition start, long timestamp) {
            this.timestamp = timestamp;
            this.producer = producer;
            this.start = start;
        }
    }

    private record Step(String type, TableBinding table) {
    }

    private static final class Window {
        final String subtype;
        final Cycle cycle;
        final Marker marker;
        final List<Step> expected;
        int step;

        Window(TableBinding table, String subtype, Cycle cycle, Marker marker, List<String> expected) {
            this.subtype = subtype;
            this.cycle = cycle;
            this.marker = marker;
            this.expected = expected.stream().map(type -> new Step(type, table)).toList();
        }

        Window(Cycle cycle, Marker marker, List<TableBinding> undone) {
            this.subtype = null;
            this.cycle = cycle;
            this.marker = marker;
            List<Step> steps = new ArrayList<>();
            // Native rollback unwinds the clears in reverse operation order. Keep
            // each exact file/member pair; a balanced transition on another table
            // cannot satisfy this window.
            for (int i = undone.size() - 1; i >= 0; i--) {
                for (String type : List.of(marker == null ? "F/EJ" : "F/MD", "D/EF", "D/JF", "F/JM")) {
                    steps.add(new Step(type, undone.get(i)));
                }
            }
            if (marker != null) {
                steps.add(new Step("C/SB", undone.get(0)));
            }
            this.expected = List.copyOf(steps);
        }
    }

    private final Limits limits;
    private final TableResolver tables;
    private final As400JournalOrder order;
    private final As400Checkpoint loaded;
    private final boolean ordinary;
    private final TransactionControlEntryDecoder controls = new TransactionControlEntryDecoder();
    private final EntryHeaderDecoder headers = new EntryHeaderDecoder();

    long[] bufferUsage() {
        return new long[]{ cycles.size(), heldBytes };
    }

    long oldestTransactionTimestamp() {
        return cycles.values().stream().mapToLong(c -> c.timestamp).min().orElse(-1);
    }

    private final Map<BigInteger, Cycle> cycles = new HashMap<>();
    private final Map<Producer, Window> windows = new HashMap<>();
    private final Map<JournalPosition, Dependency> dependencies = new LinkedHashMap<>();
    private final Deque<Entry> held = new ArrayDeque<>();
    private long heldBytes;
    private int markerCount;
    private int clearCount;

    As400JournalLifecycle(Limits limits, TableResolver tables, As400JournalOrder order, As400Checkpoint loaded) {
        this(limits, tables, order, loaded, false);
    }

    As400JournalLifecycle(Limits limits, TableResolver tables, As400JournalOrder order, As400Checkpoint loaded, boolean ordinary) {
        this.ordinary = ordinary;
        this.limits = limits;
        this.tables = tables;
        this.order = order;
        this.loaded = loaded;
        require(ordinary || loaded.replayFrom().receiver().equals(loaded.completedThrough().receiver()), "Lifecycle profile supports one receiver only");
    }

    void accept(EntryHeader header, byte[] data, int offset, JournalPosition position) throws Exception {
        require(ordinary || position.receiver().equals(loaded.replayFrom().receiver()), "Lifecycle profile supports one receiver only");
        // The ordinary profile receives an explicitly validated continuous chain.
        // Never infer order from receiver names or from a sequence-number gap.
        require(order.compare(position, loaded.replayFrom()) >= 0, "Entry precedes the verified replay window");
        int length = header.getEndOffset();
        require(offset >= 0 && length >= 236 && (long) offset + length <= data.length, "Invalid entry bounds");
        require(length <= limits.bytes() - heldBytes && held.size() < limits.entries(), "Lifecycle staging limit exceeded");
        // Re-decode owned bytes, so neither later RPC reuse nor a mismatched supplied header
        // can alter the queued record. The resolved receiver is checked against these bytes.
        byte[] owned = Arrays.copyOfRange(data, offset, offset + length);
        Entry entry = new Entry(headers.decode(owned, 0), owned, position);
        require(entry.header().getEndOffset() == length, "Entry boundary changed while copying");
        As400SourceMetadata.from(entry.header(), position);
        if (classify(entry)) {
            held.addLast(entry);
            heldBytes += length;
        }
    }

    Optional<Entry> poll() {
        if (!windows.isEmpty() || held.isEmpty()) {
            return Optional.empty();
        }
        Entry entry = held.removeFirst();
        heldBytes -= entry.bytes().length;
        return Optional.of(entry);
    }

    boolean hasReady() {
        return windows.isEmpty() && !held.isEmpty();
    }

    void verifyTail() {
        require(windows.isEmpty() && held.isEmpty(), "Journal tail interrupts a lifecycle sequence or queued output");
    }

    /**
     * Compute closure: moving F back across a completed lifecycle can require an
     * even earlier SC. Keep dependencies until the resulting replay interval no
     * longer touches them, even if W already passed their transaction's commit.
     */
    As400Checkpoint retainContext(As400Checkpoint checkpoint) {
        JournalPosition floor = checkpoint.replayFrom();
        boolean inclusive = checkpoint.replayInclusive();
        boolean changed;
        do {
            changed = false;
            for (Dependency dependency : dependencies.values()) {
                int endVsFloor = order.compare(dependency.end(), floor);
                if ((endVsFloor > 0 || (endVsFloor == 0 && inclusive)) && order.compare(dependency.start(), floor) < 0) {
                    floor = dependency.start();
                    inclusive = true;
                    changed = true;
                }
            }
        } while (changed);
        JournalPosition retained = floor;
        boolean includeRetained = inclusive;
        dependencies.values().removeIf(d -> order.compare(d.end(), retained) < 0
                || (order.compare(d.end(), retained) == 0 && !includeRetained));
        return new As400Checkpoint(checkpoint.journalIdentity(), checkpoint.captureFingerprint(), floor, inclusive, checkpoint.completedThrough(),
                order.incarnation(floor), checkpoint.completedReceiverIdentity());
    }

    private boolean classify(Entry entry) throws Exception {
        var h = entry.header();
        Producer producer = producer(h);
        if (ordinary && entry.type().equals("F/CB")) {
            statistics(entry);
            return false;
        }
        Window window = windows.get(producer);
        if (window != null && window.subtype != null && window.step == 0 && entry.type().equals("C/SU")) {
            // IBM i can abandon the optimized attempt before it changes journaling,
            // then either fail the statement or fall back to ordinary row deletes.
            // Only the exact opening marker may cancel it; once F/EJ begins, the
            // entire lifecycle remains mandatory.
            require(h.getCommitCycle().equals(window.cycle.start.offset())
                    && h.getControlInfo().nestedCommitLevel() == window.marker.level()
                    && controls.decode(h, entry.bytes(), 0).savepointSequence().equals(window.marker.sequence()),
                    "Wrong abandoned-clear scope");
            complete(producer, window, entry.position());
            window = null;
        }
        if (window != null) {
            Step expected = window.expected.get(window.step);
            require(entry.type().equals(expected.type()), "Incomplete or mismatched lifecycle sequence");
            if (h.getJournalCode() != 'C' && !entry.type().equals("D/CG")) {
                auxiliary(entry, expected.table(), entry.type().equals("F/MD") ? 0 : 0x10);
            }
            else if (entry.type().equals("D/CG")) {
                require(h.getCommitCycle().equals(window.cycle.start.offset())
                        && h.getControlInfo().nestedCommitLevel() == window.marker.level(), "Clear has wrong transaction/scope");
                var clear = new FileChangeEntryDecoder().decode(h, entry.bytes(), 0);
                require(clear.clearsRows() && String.valueOf(clear.changeType()).equals(window.subtype), "Clear subtype mismatch");
                identity(h, expected.table());
                require(clear.file().equals(h.getFile()) && clear.library().equals(h.getLibrary())
                        && (clear.member().isEmpty() || clear.member().equals(expected.table().identity().member())), "Clear payload identity mismatch");
                require(clearCount < limits.clears(), "Lifecycle clear limit exceeded");
                window.cycle.clears.add(expected.table());
                clearCount++;
            }
            else if (entry.type().equals("C/SQ")) {
                require(h.getCommitCycle().equals(window.cycle.start.offset())
                        && h.getControlInfo().nestedCommitLevel() == window.marker.level(), "Wrong clear completion scope");
                require(controls.decode(h, entry.bytes(), 0).savepointSequence().equals(window.marker.sequence()), "Wrong clear completion marker");
            }
            else if (entry.type().equals("C/SB")) {
                require(h.getCommitCycle().equals(window.cycle.start.offset())
                        && h.getControlInfo().nestedCommitLevel() == window.marker.level(), "Wrong rollback replacement scope");
            }
            if (++window.step == window.expected.size()) {
                complete(producer, window, entry.position());
            }
            if (h.getJournalCode() != 'C' && !entry.type().equals("D/CG")) {
                return false;
            }
        }
        else if (entry.type().equals("F/CH")) {
            List<Cycle> matches = cycles.values().stream().filter(c -> c.producer.equals(producer)).toList();
            require(matches.size() == 1, "Missing or ambiguous lifecycle transaction start");
            Cycle cycle = matches.get(0);
            require(!cycle.markers.isEmpty(), "Legacy clear companion lacks an open scope");
            Marker marker = new ArrayList<>(cycle.markers.values()).get(cycle.markers.size() - 1);
            TableBinding table = tables.resolve(h.getFile(), h.getLibrary());
            auxiliary(entry, table, 0);
            String subtype = new AS400Text(1).toObject(payload(entry)).toString();
            require(subtype.equals("3") || subtype.equals("8"), "Unsupported legacy change");
            windows.put(producer, new Window(table, subtype, cycle, marker, List.of("F/EJ", "D/EF", "D/JF", "F/JM", "D/CG", "C/SQ")));
            return false;
        }
        else {
            require(h.getJournalCode() == 'C' || h.getJournalCode() == 'R'
                    || (ordinary && h.getCommitCycle().signum() == 0 && entry.type().equals("F/CR")), "Unmatched native lifecycle operation: " + entry.type());
        }
        As400SourceNormalizer.validateSource(h, entry.bytes(), 0, entry.position());
        Cycle cycle = cycles.get(h.getCommitCycle());
        // Ordinary completed orphan suffixes remain the coordinator's responsibility.
        // A lifecycle suffix without SC cannot borrow a producer/identity from the future.
        boolean orphan = cycle == null && loaded.replayInclusive() && order.compare(entry.position(), loaded.completedThrough()) <= 0;
        if (h.getJournalCode() == 'C') {
            var control = controls.decode(h, entry.bytes(), 0);
            String type = h.getEntryType();
            if (type.equals("SC")) {
                require(cycles.size() < limits.cycles() && h.getCommitCycle().equals(entry.position().offset()), "Unsupported cycle start");
                require(cycles.putIfAbsent(h.getCommitCycle(), new Cycle(producer, entry.position(), h.getTimestamp().toEpochMilli())) == null, "Repeated cycle");
            }
            else if (!type.equals("BC") && !type.equals("EC") && !orphan) {
                require(cycle != null && cycle.producer.equals(producer), "Missing cycle or changed producer");
                if (type.equals("SB")) {
                    require(markerCount < limits.markers() && h.getControlInfo().nestedCommitLevel() > 0, "Unsupported savepoint scope");
                    require(cycle.markers.values().stream().allMatch(m -> m.level() < h.getControlInfo().nestedCommitLevel()), "Overlapping savepoint scope");
                    cycle.markers.put(h.getSequenceNumber(), new Marker(h.getSequenceNumber(), h.getControlInfo().nestedCommitLevel(), cycle.clears.size()));
                    markerCount++;
                }
                else if (type.equals("SQ") || type.equals("SU")) {
                    Marker marker = cycle.markers.get(control.savepointSequence());
                    require(marker != null && marker.level() == h.getControlInfo().nestedCommitLevel(), "Unknown or mismatched savepoint target");
                    if (type.equals("SU")) {
                        List<TableBinding> undone = new ArrayList<>(cycle.clears.subList(marker.clears(), cycle.clears.size()));
                        if (!undone.isEmpty()) {
                            windows.put(producer, new Window(cycle, marker, undone));
                        }
                        clearCount -= undone.size();
                        cycle.clears.subList(marker.clears(), cycle.clears.size()).clear();
                    }
                    int before = cycle.markers.size();
                    cycle.markers.entrySet().removeIf(e -> e.getKey().compareTo(marker.sequence()) >= 0);
                    markerCount -= before - cycle.markers.size();
                }
                else if (type.equals("CM") || type.equals("RB")) {
                    require(!windows.containsKey(producer), "Terminal control precedes lifecycle completion");
                    if (type.equals("RB")) {
                        if (!cycle.clears.isEmpty()) {
                            windows.put(producer, new Window(cycle, null, cycle.clears));
                        }
                    }
                    cycles.remove(h.getCommitCycle());
                    markerCount -= cycle.markers.size();
                    clearCount -= cycle.clears.size();
                }
                else {
                    require(type.equals("R1"), "Unsupported control");
                }
            }
        }
        else {
            require((ordinary && h.getCommitCycle().signum() == 0) || orphan || (cycle != null && cycle.producer.equals(producer)),
                    "Row/clear lacks matching producer and cycle");
        }
        return true;
    }

    /** IBM object-level subtype C is a statistics request, not a row or schema change. */
    private void statistics(Entry entry) throws Exception {
        var h = entry.header();
        As400SourceNormalizer.validateSource(h, entry.bytes(), 0, entry.position());
        var control = h.getControlInfo();
        require(h.getCommitCycle().signum() == 0 && control.nestedCommitLevel() == 0 && control.flags() == 0
                && control.indicatorFlag() == '0' && h.getRecordInfo().recordNumber().signum() == 0,
                "Unsupported statistics record metadata");
        TableBinding table = tables.resolve(h.getFile(), h.getLibrary());
        identity(h, table);
        byte[] data = payload(entry);
        require(data.length >= 109, "Incomplete statistics request");
        var name = new AS400Text(10);
        require(name.toObject(data, 0).toString().trim().equals(h.getFile())
                && name.toObject(data, 10).toString().trim().equals(h.getLibrary())
                && name.toObject(data, 20).toString().trim().equals(h.getMember())
                && new AS400Text(30).toObject(data, 30).toString().isBlank()
                && new AS400Text(1).toObject(data, 108).toString().equals("C"),
                "Unsupported or mismatched F/CB subtype; only a statistics request is admitted");
        // No output, cycle/window transition, or completed-output advancement.
        // Only the enclosing coordinator's diagnostic scan R can move here.
    }

    private void complete(Producer producer, Window window, JournalPosition end) {
        windows.remove(producer);
        require(dependencies.containsKey(window.cycle.start) || dependencies.size() < limits.dependencies(), "Lifecycle recovery limit exceeded");
        dependencies.put(window.cycle.start, new Dependency(window.cycle.start, end));
    }

    private static void auxiliary(Entry entry, TableBinding table, int flags) {
        var h = entry.header();
        var control = h.getControlInfo();
        require(h.getCommitCycle().signum() == 0 && control.nestedCommitLevel() == 0 && control.flags() == flags
                && !control.hasLogicalUnitOfWork() && !control.hasTransactionIdentifier() && h.getPointerHandle() == 0,
                "Lifecycle record carries unsupported flags or transaction metadata");
        identity(h, table);
        byte[] payload = payload(entry);
        switch (entry.type()) {
            case "F/CH" -> require(payload.length == 1 && control.indicatorFlag() == '0', "Unsupported legacy change payload");
            case "D/JF", "F/JM" -> require(control.indicatorFlag() == '1' && Arrays.equals(payload, new AS400Text(3).toBytes("100")), "Recording profile changed");
            default -> require(payload.length == 0 && control.indicatorFlag() == '0', "Unexpected lifecycle payload");
        }
    }

    private static void identity(EntryHeader h, TableBinding binding) {
        require(binding != null, "Missing paired identity");
        var id = binding.identity();
        boolean file = h.getJournalCode() == 'D';
        require(h.getFile().equals(id.physicalFile()) && h.getLibrary().equals(id.tableId().schema())
                && h.getMember().equals(file ? "" : id.member()) && h.getRecordInfo().objectNameIndicator() == '1'
                && h.getRecordInfo().objectType().equals(file ? "*FILE" : "*QDDS")
                && h.getRecordInfo().objectJournalId().equals(file ? id.fileJournalId() : id.memberJournalId()), "Lifecycle identity mismatch");
    }

    private static Producer producer(EntryHeader h) {
        var r = h.getRecordInfo();
        require(r.threadId().signum() > 0 && r.jobNumber().matches("[0-9]{6}") && !r.jobNumber().equals("000000"), "Missing native producer");
        for (String name : List.of(r.jobName(), r.jobUser(), r.systemName())) {
            require(!name.isBlank() && !name.startsWith("*"), "Missing native producer");
        }
        return new Producer(r.systemName(), r.jobName(), r.jobUser(), r.jobNumber(), r.threadId());
    }

    private static byte[] payload(Entry entry) {
        int prefix = entry.header().getEntrySpecificDataOffset();
        if (prefix == 0) {
            return new byte[0];
        }
        require(prefix >= 236 && (long) prefix + 16 <= entry.header().getEndOffset(), "Invalid lifecycle payload prefix");
        String digits = new AS400Text(5).toObject(entry.bytes(), prefix).toString();
        require(digits.matches("[0-9]{5}"), "Invalid lifecycle payload length");
        int length = Integer.parseInt(digits);
        require((long) prefix + 16 + length <= entry.header().getEndOffset(), "Truncated lifecycle payload");
        return Arrays.copyOfRange(entry.bytes(), prefix + 16, prefix + 16 + length);
    }

    @Override
    public void close() {
        cycles.clear();
        windows.clear();
        dependencies.clear();
        held.clear();
        heldBytes = 0;
        markerCount = 0;
        clearCount = 0;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
