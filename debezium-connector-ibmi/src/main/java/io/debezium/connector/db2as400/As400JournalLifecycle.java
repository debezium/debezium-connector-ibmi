/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableResolver;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
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
    private static final Set<String> OBJECT_METADATA = Set.of("D/GT", "D/RV", "D/AC", "D/DC", "D/TC", "D/TD");
    private static final Set<String> ADMINISTRATIVE_ENTRIES = Set.of("F/OP", "F/CL", "F/MS", "D/DH", "F/SS", "D/DW", "D/GT", "D/RV", "D/AC", "D/DC", "D/TC", "D/TD",
            "D/LF", "R/IL",
            "F/RG", "F/RM",
            "F/JP", "F/EP");

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

    private static final As400SpillStore.Codec<Producer> PRODUCER = As400SpillCodec.codec((out, producer) -> {
        As400SpillCodec.string(out, producer.system());
        As400SpillCodec.string(out, producer.job());
        As400SpillCodec.string(out, producer.user());
        As400SpillCodec.string(out, producer.number());
        As400SpillCodec.integer(out, producer.thread());
    }, in -> new Producer(As400SpillCodec.string(in), As400SpillCodec.string(in), As400SpillCodec.string(in), As400SpillCodec.string(in), As400SpillCodec.integer(in)));
    private static final As400SpillStore.Codec<Marker> MARKER = As400SpillCodec.codec((out, marker) -> {
        As400SpillCodec.integer(out, marker.sequence());
        out.writeLong(marker.level());
        out.writeInt(marker.clears());
    }, in -> new Marker(As400SpillCodec.integer(in), in.readLong(), in.readInt()));

    private static final class Cycle {
        final Producer producer;
        final JournalPosition start;
        final Map<BigInteger, Marker> markers;
        final List<TableBinding> clears;

        private final long timestamp;

        Cycle(Producer producer, JournalPosition start, long timestamp, As400SpillStore spill, As400SpillStore.Codec<TableBinding> bindings) {
            this.timestamp = timestamp;
            this.producer = producer;
            this.start = start;
            String prefix = "lifecycle/" + start.offset();
            markers = spill == null ? new LinkedHashMap<>() : spill.map(prefix + "/markers", As400SpillCodec.INTEGER, MARKER);
            clears = spill == null ? new ArrayList<>() : spill.list(prefix + "/clears", bindings);
        }
    }

    private record Step(String type, TableBinding table) {
    }

    private final class Window {
        final String subtype;
        final Cycle cycle;
        final Marker marker;
        final List<Step> expected;
        int step;

        Window(TableBinding table, String subtype, Cycle cycle, Marker marker, List<String> expected) {
            this.subtype = subtype;
            this.cycle = cycle;
            this.marker = marker;
            this.expected = steps(cycle);
            expected.forEach(type -> this.expected.add(new Step(type, table)));
        }

        Window(Cycle cycle, Marker marker, List<TableBinding> undone) {
            this.subtype = null;
            this.cycle = cycle;
            this.marker = marker;
            List<Step> steps = steps(cycle);
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
            this.expected = steps;
        }

        Window(String subtype, Cycle cycle, Marker marker, int step) {
            this.subtype = subtype;
            this.cycle = cycle;
            this.marker = marker;
            this.step = step;
            this.expected = steps(cycle);
        }
    }

    private List<Step> steps(Cycle cycle) {
        return spill == null ? new ArrayList<>() : spill.list("window/" + cycle.start.offset(), stepCodec);
    }

    private final Limits limits;
    private final TableResolver tables;
    private final As400JournalOrder order;
    private final As400Checkpoint loaded;
    private final JournalPosition from;
    private final boolean ordinary;
    private final TransactionControlEntryDecoder controls = new TransactionControlEntryDecoder();
    private final EntryHeaderDecoder headers = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID);

    long[] bufferUsage() {
        return new long[]{ cycles.size(), heldBytes };
    }

    long oldestTransactionTimestamp() {
        return cycles.values().stream().mapToLong(c -> c.timestamp).min().orElse(-1);
    }

    private final Map<BigInteger, Cycle> cycles;
    private final Map<Producer, Window> windows;
    private final Map<JournalPosition, Dependency> dependencies;
    private final Map<JournalPosition, Entry> held;
    private final As400SpillStore spill;
    private final As400SpillStore.Codec<TableBinding> bindingCodec;
    private final As400SpillStore.Codec<Step> stepCodec;
    private long heldBytes;
    private int markerCount;
    private int clearCount;

    As400JournalLifecycle(Limits limits, TableResolver tables, As400JournalOrder order, As400Checkpoint loaded) {
        this(limits, tables, order, loaded, false);
    }

    As400JournalLifecycle(Limits limits, TableResolver tables, As400JournalOrder order, As400Checkpoint loaded, boolean ordinary) {
        this(limits, tables, order, loaded, ordinary, null);
    }

    As400JournalLifecycle(Limits limits, TableResolver tables, As400JournalOrder order, As400Checkpoint loaded, boolean ordinary, As400SpillStore spill) {
        this(limits, tables, order, loaded.replayFrom(), loaded, ordinary, spill);
    }

    /** Validate a complete original cycle without inventing a completed-output checkpoint. */
    As400JournalLifecycle(Limits limits, TableResolver tables, As400JournalOrder order, JournalPosition from, As400SpillStore spill) {
        this(limits, tables, order, from, null, true, spill);
    }

    private As400JournalLifecycle(Limits limits, TableResolver tables, As400JournalOrder order, JournalPosition from,
                                  As400Checkpoint loaded, boolean ordinary, As400SpillStore spill) {
        this.ordinary = ordinary;
        this.limits = limits;
        this.tables = tables;
        this.order = order;
        this.loaded = loaded;
        this.from = from;
        this.spill = spill;
        require(ordinary || loaded.replayFrom().receiver().equals(loaded.completedThrough().receiver()), "Lifecycle profile supports one receiver only");
        bindingCodec = As400SpillCodec.codec((out, binding) -> As400SpillCodec.table(out, binding.identity()), in -> {
            var identity = As400SpillCodec.table(in);
            try {
                var binding = tables.resolve(identity.physicalFile(), identity.tableId().schema(), identity.member());
                require(binding.identity().equals(identity), "Spilled lifecycle binding no longer matches validated history");
                return binding;
            }
            catch (Exception e) {
                throw new java.io.IOException("Cannot resolve spilled lifecycle binding", e);
            }
        });
        stepCodec = As400SpillCodec.codec((out, step) -> {
            As400SpillCodec.string(out, step.type());
            As400SpillCodec.bytes(out, bindingCodec.encode(step.table()));
        }, in -> new Step(As400SpillCodec.string(in), bindingCodec.decode(As400SpillCodec.bytes(in))));
        As400SpillStore.Codec<Cycle> cycleCodec = As400SpillCodec.codec((out, cycle) -> {
            As400SpillCodec.bytes(out, PRODUCER.encode(cycle.producer));
            As400SpillCodec.position(out, cycle.start);
            out.writeLong(cycle.timestamp);
        }, in -> new Cycle(PRODUCER.decode(As400SpillCodec.bytes(in)), As400SpillCodec.position(in), in.readLong(), spill, bindingCodec));
        cycles = spill == null ? new HashMap<>() : spill.map("lifecycle-cycles", As400SpillCodec.INTEGER, cycleCodec);
        windows = spill == null ? new HashMap<>() : spill.map("lifecycle-windows", PRODUCER, As400SpillCodec.codec((out, window) -> {
            As400SpillCodec.string(out, window.subtype);
            As400SpillCodec.bytes(out, cycleCodec.encode(window.cycle));
            As400SpillCodec.bytes(out, window.marker == null ? null : MARKER.encode(window.marker));
            out.writeInt(window.step);
        }, in -> {
            String subtype = As400SpillCodec.string(in);
            Cycle cycle = cycleCodec.decode(As400SpillCodec.bytes(in));
            byte[] marker = As400SpillCodec.bytes(in);
            return new Window(subtype, cycle, marker == null ? null : MARKER.decode(marker), in.readInt());
        }));
        dependencies = spill == null ? new LinkedHashMap<>()
                : spill.map("lifecycle-dependencies", As400SpillCodec.POSITION,
                        As400SpillCodec.codec((out, dependency) -> {
                            As400SpillCodec.position(out, dependency.start());
                            As400SpillCodec.position(out, dependency.end());
                        }, in -> new Dependency(As400SpillCodec.position(in), As400SpillCodec.position(in))));
        held = spill == null ? new LinkedHashMap<>()
                : spill.map("lifecycle-held", As400SpillCodec.POSITION,
                        As400SpillCodec.codec((out, entry) -> {
                            As400SpillCodec.position(out, entry.position());
                            As400SpillCodec.bytes(out, entry.bytes());
                        }, in -> {
                            var position = As400SpillCodec.position(in);
                            byte[] bytes = As400SpillCodec.bytes(in);
                            return new Entry(headers.decode(bytes, 0), bytes, position);
                        }));
    }

    void accept(EntryHeader header, byte[] data, int offset, JournalPosition position) throws Exception {
        require(ordinary || position.receiver().equals(from.receiver()), "Lifecycle profile supports one receiver only");
        // The ordinary profile receives an explicitly validated continuous chain.
        // Never infer order from receiver names or from a sequence-number gap.
        require(order.compare(position, from) >= 0, "Entry precedes the verified replay window");
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
            held.put(entry.position(), entry);
            heldBytes += length;
        }
    }

    Optional<Entry> poll() {
        if (!windows.isEmpty() || held.isEmpty()) {
            return Optional.empty();
        }
        var first = held.values().iterator();
        Entry entry = first.next();
        first.remove();
        heldBytes -= entry.bytes().length;
        return Optional.of(entry);
    }

    boolean hasReady() {
        return windows.isEmpty() && !held.isEmpty();
    }

    void verifyTail() {
        require(drained(), "Journal tail interrupts a lifecycle sequence or queued output");
    }

    boolean drained() {
        return windows.isEmpty() && held.isEmpty();
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
                order.incarnation(floor), checkpoint.completedReceiverIdentity(), checkpoint.snapshotState(), checkpoint.schemaState());
    }

    private boolean classify(Entry entry) throws Exception {
        var h = entry.header();
        if (ordinary && As400LogicalFileRecords.accept(entry)) {
            return false;
        }
        if (ordinary && tables.creationLifecycle(new RawJournalPage.Entry(entry.position(), h, entry.bytes()))) {
            return false;
        }
        if (ordinary && administrative(entry, tables)) {
            return false;
        }
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
            // A native clear-only savepoint rollback can restore the member via
            // F/EJ instead of F/MD. This is only the opening step of an already
            // matched undo window; the same identities, F/EJ flags and complete
            // D/EF, D/JF, F/JM, C/SB restoration remain mandatory.
            boolean savepointJournalRestart = window.subtype == null && window.marker != null
                    && expected.type().equals("F/MD") && entry.type().equals("F/EJ");
            require(entry.type().equals(expected.type()) || savepointJournalRestart, "Incomplete or mismatched lifecycle sequence");
            if (h.getJournalCode() != 'C' && !entry.type().equals("D/CG")) {
                auxiliary(entry, expected.table(), entry.type().equals("F/MD") ? 0 : 0x10);
            }
            else if (entry.type().equals("D/CG")) {
                require(h.getCommitCycle().equals(window.cycle.start.offset())
                        && h.getControlInfo().nestedCommitLevel() == window.marker.level(), "Clear has wrong transaction/scope");
                var clear = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID).decode(h, entry.bytes(), 0);
                require((clear.clearsRows() || window.subtype.equals("0") && clear.changeType() == '0')
                        && String.valueOf(clear.changeType()).equals(window.subtype), "Clear subtype mismatch");
                identity(h, expected.table());
                require(clear.file().equals(h.getFile()) && clear.library().equals(h.getLibrary())
                        && (clear.member().isEmpty() || clear.member().equals(expected.table().identity().member())), "Clear payload identity mismatch");
                if (clear.clearsRows()) {
                    require(clearCount < limits.clears(), "Lifecycle clear limit exceeded");
                    window.cycle.clears.add(expected.table());
                    clearCount++;
                }
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
            else if (spill != null) {
                windows.put(producer, window);
            }
            if (h.getJournalCode() != 'C' && !entry.type().equals("D/CG")) {
                return false;
            }
        }
        else if (entry.type().equals("F/CH")) {
            List<Cycle> matches = cycles.values().stream().filter(c -> c.producer.equals(producer)).limit(2).toList();
            require(matches.size() == 1, "Missing or ambiguous lifecycle transaction start");
            Cycle cycle = matches.get(0);
            require(!cycle.markers.isEmpty(), "Legacy clear companion lacks an open scope");
            Marker marker = null;
            for (Marker candidate : cycle.markers.values()) {
                marker = candidate;
            }
            TableBinding table = tables.resolve(h.getFile(), h.getLibrary(), h.getMember());
            auxiliary(entry, table, 0);
            String subtype = new AS400Text(1, RawJournalPage.METADATA_CCSID).toObject(payload(entry)).toString();
            require(subtype.equals("3") || subtype.equals("8") || subtype.equals("0") && tables.allowsSchemaChange(entry.position()),
                    "Unsupported legacy change");
            windows.put(producer, new Window(table, subtype, cycle, marker, List.of("F/EJ", "D/EF", "D/JF", "F/JM", "D/CG", "C/SQ")));
            return false;
        }
        else {
            require(h.getJournalCode() == 'C' || h.getJournalCode() == 'R'
                    || (ordinary && h.getCommitCycle().signum() == 0 && entry.type().equals("F/CR")), "Unmatched native lifecycle operation: " + entry.type());
        }
        As400SourceNormalizer.validateSource(h, entry.bytes(), 0, entry.position(), tables);
        Cycle cycle = cycles.get(h.getCommitCycle());
        // Ordinary completed orphan suffixes remain the coordinator's responsibility.
        // A lifecycle suffix without SC cannot borrow a producer/identity from the future.
        boolean orphan = cycle == null && loaded != null && loaded.replayInclusive() && order.compare(entry.position(), loaded.completedThrough()) <= 0;
        if (h.getJournalCode() == 'C') {
            var control = controls.decode(h, entry.bytes(), 0);
            String type = h.getEntryType();
            if (type.equals("SC")) {
                require(cycles.size() < limits.cycles() && h.getCommitCycle().equals(entry.position().offset()), "Unsupported cycle start");
                require(cycles.putIfAbsent(h.getCommitCycle(), new Cycle(producer, entry.position(), h.getTimestamp().toEpochMilli(), spill, bindingCodec)) == null,
                        "Repeated cycle");
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
                        List<TableBinding> undone = cycle.clears.subList(marker.clears(), cycle.clears.size());
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
                    if (spill != null) {
                        cycle.markers.clear();
                        cycle.clears.clear();
                    }
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
        TableBinding table = tables.resolve(h.getFile(), h.getLibrary(), h.getMember());
        identity(h, table);
        byte[] data = payload(entry);
        require(data.length >= 109, "Incomplete statistics request");
        var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
        require(name.toObject(data, 0).toString().trim().equals(h.getFile())
                && name.toObject(data, 10).toString().trim().equals(h.getLibrary())
                && name.toObject(data, 20).toString().trim().equals(h.getMember())
                && new AS400Text(30, RawJournalPage.METADATA_CCSID).toObject(data, 30).toString().isBlank()
                && new AS400Text(1, RawJournalPage.METADATA_CCSID).toObject(data, 108).toString().equals("C"),
                "Unsupported or mismatched F/CB subtype; only a statistics request is admitted");
        // No output, cycle/window transition, or completed-output advancement.
        // Only the enclosing coordinator's diagnostic scan R can move here.
    }

    /**
     * IBM documents these entries as open/close, save history, or authority grants,
     * not row changes. Keep exact header identity and complete payload validation;
     * restore, abnormal-end and journaling transitions remain separate. Classic
     * reorganization is admitted only for logical keys; RRN keys remain rejected.
     * https://www.ibm.com/docs/en/i/7.5.0?topic=information-all-journal-entries-by-code-type
     */
    static boolean administrative(Entry entry, TableResolver tables) throws Exception {
        if (!ADMINISTRATIVE_ENTRIES.contains(entry.type())) {
            return false;
        }
        var h = entry.header();
        As400SourceNormalizer.validateSource(h, entry.bytes(), 0, entry.position());
        var control = h.getControlInfo();
        require(h.getCommitCycle().signum() == 0 && control.nestedCommitLevel() == 0 && control.flags() == 0
                && control.indicatorFlag() == '0' && h.getRecordInfo().recordNumber().signum() == 0,
                "Unsupported administrative record metadata");
        if (entry.type().equals("F/JP") || entry.type().equals("F/EP")) {
            // *QDDSI identifies an access path, not a *QDDS data member. These
            // commands cannot start/end row journaling or establish a row binding.
            var object = h.getRecordInfo();
            require(object.objectType().equals("*QDDSI") && object.objectNameIndicator() == '1'
                    && As400TransactionBuffer.TableIdentity.validJournalId(object.objectJournalId())
                    && h.getFile().matches("[A-Z0-9_#$@]{1,10}") && h.getLibrary().matches("[A-Z0-9_#$@]{1,10}")
                    && h.getMember().matches("[A-Z0-9_#$@]{1,10}") && payload(entry).length == 0,
                    "Unqualified access-path journaling metadata");
            return true;
        }
        TableBinding binding = tables.resolve(h.getFile(), h.getLibrary(), h.getMember());
        identity(h, binding);
        byte[] data = payload(entry);
        int minimum = switch (entry.type()) {
            case "F/RG" -> 0;
            case "F/RM" -> 109;
            case "F/OP" -> 41;
            case "F/CL" -> 30;
            case "D/GT", "D/RV", "D/AC", "D/DC", "D/TC", "D/TD" -> 60;
            case "D/LF" -> 30;
            case "F/SS", "D/DW" -> 64; // Start of save-while-active, not restore.
            case "R/IL" -> 8; // Internal record-limit data; never a row image.
            default -> 160; // Native save history layout, not save-with-storage-freed.
        };
        require(data.length >= minimum, "Incomplete administrative record");
        require(!entry.type().equals("R/IL") || data.length == 8, "Unqualified record-limit layout");
        if (entry.type().equals("D/LF")) {
            var name = new AS400Text(10, RawJournalPage.METADATA_CCSID);
            String jid = java.util.HexFormat.of().formatHex(data, 20, 30);
            require(data.length == 30 && name.toObject(data, 0).toString().trim().matches("[A-Z0-9_#$@]{1,10}")
                    && name.toObject(data, 10).toString().trim().matches("[A-Z0-9_#$@]{1,10}")
                    && As400TransactionBuffer.TableIdentity.validJournalId(jid)
                    && !jid.equals(binding.identity().fileJournalId()) && !binding.members().containsValue(jid),
                    "Invalid logical-file association");
        }
        if (entry.type().equals("F/RG") || entry.type().equals("F/RM")) {
            // Classic reorganization moves physical record numbers, not logical
            // rows. It cannot be ignored when the emitted key is an RRN.
            require(!binding.rrnKey(), "Reorganization cannot preserve relative-record-number keys");
            if (entry.type().equals("F/RG")) {
                require(data.length == 0, "Unqualified reorganization-start payload");
            }
            else {
                var name = new AS400Text(10, 37);
                require(name.toObject(data, 0).toString().trim().equals(h.getFile())
                        && name.toObject(data, 10).toString().trim().equals(h.getLibrary())
                        && name.toObject(data, 20).toString().trim().equals(h.getMember())
                        && new AS400Text(30, 37).toObject(data, 30).toString().isBlank()
                        && new AS400Text(1, 37).toObject(data, 108).equals("E"), "Unqualified reorganization identity or mode");
            }
        }
        if (OBJECT_METADATA.contains(entry.type())) {
            // These records share the object-level name prefix. Authority and
            // trigger changes do not replace row formats or journal bindings;
            // triggered row changes are journaled and handled independently.
            var name = new AS400Text(10, 37);
            require(name.toObject(data, 0).toString().trim().equals(h.getFile())
                    && name.toObject(data, 10).toString().trim().equals(h.getLibrary())
                    && new AS400Text(40, 37).toObject(data, 20).toString().isBlank(), "Authority record object identity mismatch");
            require(!List.of("D/AC", "D/DC").contains(entry.type()) || binding.excluded(),
                    "Captured constraint changes require validation of the output key schema");
        }
        // F/OP and F/CL payload names may identify a logical file. The header's
        // physical member/JID, validated above, is the captured object identity.
        // A native F/MS can have thread zero even with receiver thread recording.
        // These cycle-zero records do not correlate producers or resolve any state;
        // row, clear and transaction records still require a complete producer.
        // No output, pending image removal, transaction resolution, or W advance.
        return true;
    }

    static boolean administrativeType(String type) {
        return ADMINISTRATIVE_ENTRIES.contains(type);
    }

    private void complete(Producer producer, Window window, JournalPosition end) {
        windows.remove(producer);
        if (spill != null) {
            window.expected.clear();
        }
        require(dependencies.containsKey(window.cycle.start) || dependencies.size() < limits.dependencies(), "Lifecycle recovery limit exceeded");
        dependencies.put(window.cycle.start, new Dependency(window.cycle.start, end));
    }

    static void auxiliary(Entry entry, TableBinding table, int flags) {
        var h = entry.header();
        var control = h.getControlInfo();
        require(h.getCommitCycle().signum() == 0 && control.nestedCommitLevel() == 0 && control.flags() == flags
                && !control.hasLogicalUnitOfWork() && !control.hasTransactionIdentifier() && h.getPointerHandle() == 0,
                "Lifecycle record carries unsupported flags or transaction metadata");
        identity(h, table);
        byte[] payload = payload(entry);
        switch (entry.type()) {
            case "F/CH" -> require(payload.length == 1 && control.indicatorFlag() == '0', "Unsupported legacy change payload");
            case "D/JF", "F/JM" -> {
                // IBM's start-journal layout: omit open/close, inherit, remote filter.
                // Both OMTJRNE(*NONE) and *OPNCLO retain complete row history. The
                // image flag must still select *BOTH; inheritance/filtering are not
                // admitted. Open/close records use the identity-checked classifier.
                require(control.indicatorFlag() == '1' && payload.length == 3
                        && new AS400Text(3, RawJournalPage.METADATA_CCSID).toObject(payload).toString().matches("[01]00"),
                        "Unsupported start-journal recording profile");
            }
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

    static byte[] payload(Entry entry) {
        int prefix = entry.header().getEntrySpecificDataOffset();
        if (prefix == 0) {
            return new byte[0];
        }
        require(prefix >= 236 && (long) prefix + 16 <= entry.header().getEndOffset(), "Invalid lifecycle payload prefix");
        String digits = new AS400Text(5, RawJournalPage.METADATA_CCSID).toObject(entry.bytes(), prefix).toString();
        require(digits.matches("[0-9]{5}"), "Invalid lifecycle payload length");
        int length = Integer.parseInt(digits);
        require((long) prefix + 16 + length <= entry.header().getEndOffset(), "Truncated lifecycle payload");
        return Arrays.copyOfRange(entry.bytes(), prefix + 16, prefix + 16 + length);
    }

    @Override
    public void close() {
        if (spill == null) {
            cycles.clear();
            windows.clear();
            dependencies.clear();
            held.clear();
        }
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
