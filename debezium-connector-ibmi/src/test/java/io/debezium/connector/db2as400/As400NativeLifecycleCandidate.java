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

import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/**
 * TEST ONLY: bounded two-pass hypothesis checker for the observed native profile.
 * It returns a component projection only after validating the entire capture.
 * This is not an incremental reader, native startup proof or a production filter.
 * Unknown patterns, real unflagged journal gaps and missing context reject.
 */
final class As400NativeLifecycleCandidate {
    record Input(EntryHeader header, byte[] bytes, JournalPosition position) {
        String type() {
            return header.getJournalCode() + "/" + header.getEntryType();
        }
    }

    /** A rollback's W may precede its tail: replay must reconstruct context from SC. */
    record RecoveryContext(JournalPosition start, JournalPosition rollback, JournalPosition tailEnd) {
        boolean neededAt(JournalPosition completed) {
            require(completed.receiver().equals(rollback.receiver()), "Candidate handles one receiver only");
            return completed.offset().compareTo(rollback.offset()) >= 0 && completed.offset().compareTo(tailEnd.offset()) < 0;
        }
    }

    record Result(List<Input> component, List<RecoveryContext> recovery) {
        /** Experimental checkpoint transformation, not connected to a live offset sink. */
        As400Checkpoint retainContext(As400Checkpoint checkpoint) {
            JournalPosition floor = checkpoint.replayFrom();
            boolean inclusive = checkpoint.replayInclusive();
            for (RecoveryContext context : recovery) {
                if (context.neededAt(checkpoint.completedThrough()) && context.start().offset().compareTo(floor.offset()) < 0) {
                    require(context.start().receiver().equals(floor.receiver()), "Candidate handles one receiver only");
                    floor = context.start();
                    inclusive = true;
                }
            }
            return new As400Checkpoint(checkpoint.journalIdentity(), checkpoint.captureFingerprint(), floor, inclusive, checkpoint.completedThrough(),
                    checkpoint.replayReceiverIdentity(), checkpoint.completedReceiverIdentity());
        }
    }

    private record Producer(String system, String job, String user, String number, BigInteger thread) {
    }

    private record Marker(BigInteger sequence, long level, int clears) {
    }

    private static final class Cycle {
        final Producer producer;
        final JournalPosition start;
        final Map<BigInteger, Marker> markers = new LinkedHashMap<>();
        final List<String> clears = new ArrayList<>();

        Cycle(Producer producer, JournalPosition start) {
            this.producer = producer;
            this.start = start;
        }
    }

    private static final class Window {
        final String table;
        final String subtype;
        final Cycle cycle;
        final Marker marker;
        final JournalPosition rollback;
        final List<String> expected;
        int step;

        Window(String table, String subtype, Cycle cycle, Marker marker, JournalPosition rollback, List<String> expected) {
            this.table = table;
            this.subtype = subtype;
            this.cycle = cycle;
            this.marker = marker;
            this.rollback = rollback;
            this.expected = expected;
        }
    }

    static Result inspect(List<Input> input, Map<String, TableBinding> bindings) throws Exception {
        require(!input.isEmpty() && input.size() <= 4096, "Bounded nonempty capture required");
        Map<BigInteger, Cycle> cycles = new HashMap<>();
        Map<Producer, Window> windows = new HashMap<>();
        List<Input> component = new ArrayList<>();
        List<RecoveryContext> recovery = new ArrayList<>();
        TransactionControlEntryDecoder controls = new TransactionControlEntryDecoder();
        BigInteger previous = BigInteger.ZERO;
        for (Input entry : input) {
            var h = entry.header();
            require(entry.position().receiver().equals(input.get(0).position().receiver()), "Candidate handles one receiver only");
            require(h.getSequenceNumber().compareTo(previous) > 0, "Entry order regressed");
            previous = h.getSequenceNumber();
            require(h.getEndOffset() >= 236 && h.getEndOffset() <= entry.bytes().length, "Invalid bounds");
            As400SourceMetadata.from(h, entry.position());
            Producer producer = producer(h);
            Window window = windows.get(producer);
            if (window != null) {
                require(entry.type().equals(window.expected.get(window.step)), "Incomplete or mismatched lifecycle sequence");
                if (h.getJournalCode() != 'C' && !entry.type().equals("D/CG")) {
                    auxiliary(entry, window.table, bindings, entry.type().equals("F/MD") ? 0 : 0x10);
                }
                else if (entry.type().equals("D/CG")) {
                    require(h.getCommitCycle().equals(window.cycle.start.offset())
                            && h.getControlInfo().nestedCommitLevel() == window.marker.level(), "Clear has wrong transaction/scope");
                    var clear = new FileChangeEntryDecoder().decode(h, entry.bytes(), 0);
                    require(clear.clearsRows() && String.valueOf(clear.changeType()).equals(window.subtype), "Clear subtype mismatch");
                    identity(h, window.table, bindings);
                    require(clear.file().equals(h.getFile()) && clear.library().equals(h.getLibrary())
                            && (clear.member().isEmpty() || clear.member().equals(bindings.get(window.table).identity().member())), "Clear payload identity mismatch");
                    window.cycle.clears.add(window.table);
                }
                else if (entry.type().equals("C/SQ")) {
                    require(controls.decode(h, entry.bytes(), 0).savepointSequence().equals(window.marker.sequence()), "Wrong clear completion marker");
                }
                else if (entry.type().equals("C/SB")) {
                    require(h.getCommitCycle().equals(window.cycle.start.offset())
                            && h.getControlInfo().nestedCommitLevel() == window.marker.level(), "Wrong rollback replacement scope");
                }
                if (++window.step == window.expected.size()) {
                    windows.remove(producer);
                    if (window.rollback != null && window.marker == null) {
                        recovery.add(new RecoveryContext(window.cycle.start, window.rollback, entry.position()));
                    }
                }
                if (h.getJournalCode() != 'C' && !entry.type().equals("D/CG")) {
                    continue;
                }
            }
            else if (entry.type().equals("F/CH")) {
                Cycle cycle = uniqueCycle(cycles, producer);
                require(!cycle.markers.isEmpty(), "Legacy clear companion lacks an open scope");
                Marker marker = new ArrayList<>(cycle.markers.values()).get(cycle.markers.size() - 1);
                auxiliary(entry, h.getFile(), bindings, 0);
                String subtype = new AS400Text(1).toObject(payload(entry)).toString();
                require(subtype.equals("3") || subtype.equals("8"), "Unsupported legacy change");
                windows.put(producer, new Window(h.getFile(), subtype, cycle, marker, null,
                        List.of("F/EJ", "D/EF", "D/JF", "F/JM", "D/CG", "C/SQ")));
                continue;
            }
            else {
                require(h.getJournalCode() == 'C' || h.getJournalCode() == 'R', "Unmatched native lifecycle operation");
            }
            As400SourceNormalizer.validateSource(h, entry.bytes(), 0, entry.position());
            if (h.getJournalCode() == 'C') {
                var control = controls.decode(h, entry.bytes(), 0);
                String type = h.getEntryType();
                if (type.equals("SC")) {
                    require(cycles.size() < 32 && h.getCommitCycle().equals(entry.position().offset()), "Unsupported cycle start");
                    require(cycles.putIfAbsent(h.getCommitCycle(), new Cycle(producer, entry.position())) == null, "Repeated cycle");
                }
                else if (!type.equals("BC") && !type.equals("EC")) {
                    Cycle cycle = cycles.get(h.getCommitCycle());
                    require(cycle != null && cycle.producer.equals(producer), "Missing cycle or changed producer");
                    if (type.equals("SB")) {
                        require(cycle.markers.size() < 128 && h.getControlInfo().nestedCommitLevel() > 0, "Unsupported savepoint scope");
                        require(cycle.markers.values().stream().allMatch(m -> m.level() < h.getControlInfo().nestedCommitLevel()), "Overlapping savepoint scope");
                        cycle.markers.put(h.getSequenceNumber(), new Marker(h.getSequenceNumber(), h.getControlInfo().nestedCommitLevel(), cycle.clears.size()));
                    }
                    else if (type.equals("SQ") || type.equals("SU")) {
                        Marker marker = cycle.markers.get(control.savepointSequence());
                        require(marker != null && marker.level() == h.getControlInfo().nestedCommitLevel(), "Unknown or mismatched savepoint target");
                        if (type.equals("SU")) {
                            List<String> undone = new ArrayList<>(cycle.clears.subList(marker.clears(), cycle.clears.size()));
                            require(undone.size() <= 1, "Multiple clear rollback needs further validation");
                            if (!undone.isEmpty()) {
                                windows.put(producer, new Window(undone.get(0), null, cycle, marker, entry.position(),
                                        List.of("F/MD", "D/EF", "D/JF", "F/JM", "C/SB")));
                            }
                            cycle.clears.subList(marker.clears(), cycle.clears.size()).clear();
                        }
                        // Conservative consumption, not a claim about native marker lifetime.
                        cycle.markers.entrySet().removeIf(e -> e.getKey().compareTo(marker.sequence()) >= 0);
                    }
                    else if (type.equals("CM") || type.equals("RB")) {
                        require(!windows.containsKey(producer), "Terminal control precedes lifecycle completion");
                        if (type.equals("RB")) {
                            require(cycle.clears.size() <= 1, "Multiple clear rollback needs further validation");
                            if (!cycle.clears.isEmpty()) {
                                windows.put(producer, new Window(cycle.clears.get(0), null, cycle, null, entry.position(),
                                        List.of("F/EJ", "D/EF", "D/JF", "F/JM")));
                            }
                        }
                        cycles.remove(h.getCommitCycle());
                    }
                    else {
                        require(type.equals("R1"), "Unsupported control");
                    }
                }
            }
            else {
                Cycle cycle = cycles.get(h.getCommitCycle());
                require(cycle != null && cycle.producer.equals(producer), "Row/clear lacks matching producer and cycle");
            }
            component.add(entry);
        }
        require(windows.isEmpty() && cycles.isEmpty(), "Capture ends before lifecycle/transaction completion");
        return new Result(List.copyOf(component), List.copyOf(recovery));
    }

    private static Cycle uniqueCycle(Map<BigInteger, Cycle> cycles, Producer producer) {
        List<Cycle> matches = cycles.values().stream().filter(c -> c.producer.equals(producer)).toList();
        require(matches.size() == 1, "Ambiguous lifecycle producer");
        return matches.get(0);
    }

    private static void auxiliary(Input entry, String table, Map<String, TableBinding> bindings, int flags) {
        var h = entry.header();
        var control = h.getControlInfo();
        require(h.getCommitCycle().signum() == 0 && control.nestedCommitLevel() == 0 && control.flags() == flags
                && !control.hasLogicalUnitOfWork() && !control.hasTransactionIdentifier() && h.getPointerHandle() == 0,
                "Lifecycle record carries unsupported flags or transaction metadata");
        identity(h, table, bindings);
        byte[] payload = payload(entry);
        switch (entry.type()) {
            case "F/CH" -> require(payload.length == 1 && control.indicatorFlag() == '0', "Unsupported legacy change payload");
            case "D/JF", "F/JM" -> require(control.indicatorFlag() == '1' && Arrays.equals(payload, new AS400Text(3).toBytes("100")),
                    "Recording profile changed");
            default -> require(payload.length == 0 && control.indicatorFlag() == '0', "Unexpected lifecycle payload");
        }
    }

    private static void identity(EntryHeader h, String table, Map<String, TableBinding> bindings) {
        TableBinding binding = bindings.get(table);
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

    private static byte[] payload(Input entry) {
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

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
