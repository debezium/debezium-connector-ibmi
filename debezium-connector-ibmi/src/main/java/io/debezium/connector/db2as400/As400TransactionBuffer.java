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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.debezium.DebeziumException;
import io.debezium.data.Envelope.Operation;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Control;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Kind;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Outcome;
import io.debezium.relational.TableId;

/**
 * Single-threaded, memory-only state machine for normalized local commit cycles.
 * Not connected to streaming until durable replay and validated startup exist.
 * The caller supplies immutable, owned row values and verified object identities
 * in journal order, with receiver names already resolved. Cross-receiver ordering and
 * incarnation validation belong to the reader; this class never orders receiver names.
 * As400RowData copies mutable payloads before admission. Cycle-zero changes belong to the future
 * output coordinator, not this transaction-only component.
 */
final class As400TransactionBuffer implements AutoCloseable {
    private static final BigInteger MAX_SEQUENCE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private final String journalIdentity;
    private final Limits limits;
    private final Map<BigInteger, Cycle> cycles = new LinkedHashMap<>();
    private JournalPosition lastPosition;
    private CommittedBatch pendingBatch;
    private long payloadBytes;
    private int retainedEntries;
    private int savepoints;
    private boolean failed;
    private boolean closed;

    As400TransactionBuffer(String journalIdentity, Limits limits) {
        this.journalIdentity = identifier(journalIdentity, 256);
        this.limits = Objects.requireNonNull(limits);
    }

    Optional<CommittedBatch> acceptControl(As400SourceMetadata source, Control control) {
        return acceptControl(source, control, SavepointRollback.UNRESOLVED);
    }

    /** SU requires an explicit source policy, including conservative target consumption. */
    Optional<CommittedBatch> acceptControl(As400SourceMetadata source, Control control, SavepointRollback rollback) {
        JournalPosition position = acceptInput(source);
        if (control == null || control.kind() == null || control.controlInfo() == null) {
            throw fail("Missing normalized transaction control");
        }
        if (source.journalCode() != 'C' || !source.entryType().equals(control.entryType())
                || !source.commitCycle().equals(control.commitCycle()) || !source.controlInfo().equals(control.controlInfo())) {
            throw fail("Control metadata does not match its source entry");
        }
        var info = control.controlInfo();
        if (info.nestedCommitLevel() < 0 || info.hasTransactionIdentifier() || info.hasLogicalUnitOfWork()
                || (info.flags() & 0x20) != 0) {
            throw fail("Unvalidated transaction identifiers or incomplete control metadata");
        }
        if (control.kind() != Kind.ROLLBACK_SAVEPOINT && rollback != SavepointRollback.UNRESOLVED) {
            throw fail("Savepoint disposition supplied for a different control");
        }
        if (control.kind() == Kind.UNSUPPORTED) {
            throw fail("Unsupported transaction control " + control.entryType());
        }
        if (control.kind() == Kind.BEGIN_COMMITMENT_CONTROL || control.kind() == Kind.END_COMMITMENT_CONTROL) {
            if (cycles.containsKey(control.commitCycle())) {
                throw fail("Commitment-control lifecycle entry cannot resolve an active cycle");
            }
            return Optional.empty();
        }
        if (control.kind() == Kind.START || control.kind() == Kind.COMMIT || control.kind() == Kind.ROLLBACK) {
            if (info.nestedCommitLevel() != 0) {
                throw fail("Expected a top-level transaction boundary");
            }
        }
        if (control.kind() == Kind.START) {
            sequence(control.commitCycle());
            if (cycles.containsKey(control.commitCycle())) {
                throw fail("Duplicate active commit cycle " + control.commitCycle());
            }
            if (cycles.size() >= limits.maxTransactions()) {
                throw fail("Active transaction limit exceeded");
            }
            cycles.put(control.commitCycle(), new Cycle(new TransactionId(journalIdentity, control.commitCycle(), position), source));
            return Optional.empty();
        }
        Cycle cycle = cycle(control.commitCycle());
        switch (control.kind()) {
            case COMMIT -> {
                if (control.outcome() != Outcome.DURABLE_COMMIT || cycle.rollingBack || cycle.compensating) {
                    throw fail("Cannot commit an unresolved or non-durable cycle");
                }
                requirePairedImages(cycle);
                pendingBatch = new CommittedBatch(cycle.id, cycle.startSource, source, cycle.changes);
                return Optional.of(pendingBatch);
            }
            case ROLLBACK -> {
                if (control.outcome() != Outcome.FULL_ROLLBACK) {
                    throw fail("Cannot discard an incomplete or unknown rollback");
                }
                removeCycle(cycle);
            }
            case START_ROLLBACK -> cycle.rollingBack = true;
            case START_SAVEPOINT -> {
                requireForward(cycle);
                requirePairedImages(cycle);
                if (cycle.savepoints.containsKey(position.getOffset())) {
                    throw fail("Ambiguous savepoint sequence within a cycle");
                }
                if (savepoints >= limits.maxSavepoints()) {
                    throw fail("Savepoint limit exceeded");
                }
                cycle.savepoints.put(position.getOffset(), cycle.changes.size());
                savepoints++;
            }
            case RELEASE_SAVEPOINT -> {
                requireForward(cycle);
                requirePairedImages(cycle);
                savepoint(cycle, control.savepointSequence());
                removeSavepoints(cycle, control.savepointSequence(), true);
            }
            case ROLLBACK_SAVEPOINT -> {
                if (cycle.rollingBack || rollback == null || rollback == SavepointRollback.UNRESOLVED) {
                    throw fail("Savepoint rollback disposition is unresolved or a full rollback is active");
                }
                int keep = savepoint(cycle, control.savepointSequence());
                for (Change change : cycle.changes.subList(keep, cycle.changes.size())) {
                    release(cycle, change.bytes(), 1);
                }
                cycle.changes.subList(keep, cycle.changes.size()).clear();
                // SB requires no dangling images, so all pending pairs are after the target.
                for (Image image : cycle.before.values()) {
                    release(cycle, image.row.payloadBytes(), 1);
                }
                cycle.before.clear();
                removeSavepoints(cycle, control.savepointSequence(), rollback == SavepointRollback.RELEASE_TARGET || rollback == SavepointRollback.CONSUME_TARGET);
                cycle.compensating = false;
            }
            default -> throw fail("Unexpected transaction control " + control.entryType());
        }
        return Optional.empty();
    }

    void insert(As400SourceMetadata source, RecordIdentity record, As400RowData after) {
        append(source, record, after, Operation.CREATE);
    }

    void delete(As400SourceMetadata source, RecordIdentity record, As400RowData before) {
        append(source, record, before, Operation.DELETE);
    }

    private void append(As400SourceMetadata source, RecordIdentity record, As400RowData bytes, Operation operation) {
        acceptInput(source);
        requireImage(record, bytes);
        Cycle cycle = forward(source.commitCycle(), record.table());
        if (cycle.before.containsKey(record)) {
            throw fail("Unpaired update precedes another operation on the same record");
        }
        reserve(cycle, bytes.payloadBytes(), 1);
        Image image = new Image(source, bytes);
        cycle.changes.add(new Change(operation, record.table(), record, source,
                operation == Operation.DELETE ? image : null, operation == Operation.CREATE ? image : null));
    }

    void beforeImage(As400SourceMetadata source, RecordIdentity record, As400RowData bytes) {
        acceptInput(source);
        requireImage(record, bytes);
        Cycle cycle = forward(source.commitCycle(), record.table());
        if (cycle.before.containsKey(record)) {
            throw fail("Duplicate before-image for an unresolved update");
        }
        reserve(cycle, bytes.payloadBytes(), 1);
        cycle.before.put(record, new Image(source, bytes));
    }

    void update(As400SourceMetadata source, RecordIdentity record, As400RowData after) {
        acceptInput(source);
        requireImage(record, after);
        Cycle cycle = forward(source.commitCycle(), record.table());
        Image before = cycle.before.get(record);
        if (before == null) {
            throw fail("Update has no matching transaction/record before-image");
        }
        // The before-image's slot and bytes become part of the completed change.
        reserve(cycle, after.payloadBytes(), 0);
        cycle.changes.add(new Change(Operation.UPDATE, record.table(), record, source, before, new Image(source, after)));
        cycle.before.remove(record);
    }

    void truncate(As400SourceMetadata source, TableIdentity table) {
        acceptInput(source);
        Cycle cycle = forward(source.commitCycle(), table);
        var iterator = cycle.before.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getKey().table().equals(table)) {
                release(cycle, entry.getValue().row.payloadBytes(), 1);
                iterator.remove();
            }
        }
        reserve(cycle, 0, 1);
        cycle.changes.add(new Change(Operation.TRUNCATE, table, null, source, null, null));
    }

    /** A validated normalizer calls this for undo records, never insert/update/delete. */
    void compensation(As400SourceMetadata source) {
        acceptInput(source);
        Cycle cycle = cycle(source.commitCycle());
        cycle.compensating = true;
    }

    /**
     * Release memory after successful batch dispatch; this is NOT durable offset acknowledgement.
     * The caller must separately bound its output queue and stop retaining completed batches.
     */
    void completeBatch(CommittedBatch batch) {
        ensureUsable();
        if (batch == null || batch != pendingBatch) {
            throw fail("Completion does not match the pending batch");
        }
        removeCycle(cycle(batch.transactionId().commitCycle()));
        pendingBatch = null;
    }

    Optional<CommittedBatch> pendingBatch() {
        ensureUsable();
        return Optional.ofNullable(pendingBatch);
    }

    /** Includes the pending committed batch until completeBatch is called. */
    Optional<JournalPosition> oldestTransactionPosition() {
        return cycles.values().stream().findFirst().map(cycle -> cycle.id.start());
    }

    long oldestTransactionTimestamp() {
        return cycles.values().stream().mapToLong(c -> c.startSource.timestamp().toEpochMilli()).min().orElse(-1);
    }

    Usage usage() {
        return new Usage(cycles.size(), retainedEntries, savepoints, payloadBytes);
    }

    boolean hasTransaction(BigInteger commitCycle) {
        ensureUsable();
        return cycles.containsKey(commitCycle);
    }

    private JournalPosition acceptInput(As400SourceMetadata source) {
        ensureUsable();
        if (pendingBatch != null) {
            throw fail("A committed batch must finish before accepting more journal entries");
        }
        if (source == null) {
            throw fail("Missing source metadata");
        }
        JournalPosition position = source.position();
        sequence(position.offset());
        if (position.receiver() == null || position.receiver().name() == null || position.receiver().name().isBlank()
                || position.receiver().name().length() > 10 || position.receiver().library() == null
                || position.receiver().library().isBlank() || position.receiver().library().length() > 10) {
            throw fail("Journal receiver must be resolved before buffering");
        }
        if (lastPosition != null && lastPosition.receiver().equals(position.receiver())
                && position.offset().compareTo(lastPosition.offset()) <= 0) {
            throw fail("Journal entries are duplicated or out of order");
        }
        lastPosition = position;
        return position;
    }

    private void ensureUsable() {
        if (failed || closed) {
            throw new BufferException("Transaction buffer is stopped; reconstruct it from a safe checkpoint");
        }
    }

    private Cycle cycle(BigInteger commitCycle) {
        Cycle cycle = cycles.get(commitCycle);
        if (cycle == null) {
            throw fail("Missing start for commit cycle " + commitCycle);
        }
        return cycle;
    }

    private Cycle forward(BigInteger commitCycle, TableIdentity table) {
        Cycle cycle = cycle(commitCycle);
        requireForward(cycle);
        if (table == null) {
            throw fail("Missing normalized table identity");
        }
        for (Cycle active : cycles.values()) {
            TableIdentity known = active.tables.get(table.tableId());
            if (known != null && !known.equals(table)) {
                throw fail("Captured object/member/record format changed while a cycle was pending");
            }
        }
        if (!cycle.tables.containsKey(table.tableId())) {
            reserve(cycle, 0, 1);
            cycle.tables.put(table.tableId(), table);
        }
        return cycle;
    }

    private void requireForward(Cycle cycle) {
        if (cycle.rollingBack || cycle.compensating) {
            throw fail("Forward change arrived before rollback was resolved");
        }
    }

    private void requirePairedImages(Cycle cycle) {
        if (!cycle.before.isEmpty()) {
            throw fail("Unpaired update crosses a commit/savepoint boundary");
        }
    }

    private void requireImage(RecordIdentity record, As400RowData bytes) {
        if (record == null || bytes == null) {
            throw fail("Missing normalized row identity or image");
        }
    }

    private int savepoint(Cycle cycle, BigInteger target) {
        Integer index = cycle.savepoints.get(target);
        if (index == null) {
            throw fail("Savepoint target is absent from this cycle");
        }
        return index;
    }

    private void removeSavepoints(Cycle cycle, BigInteger target, boolean includeTarget) {
        boolean afterTarget = false;
        var iterator = cycle.savepoints.keySet().iterator();
        while (iterator.hasNext()) {
            BigInteger key = iterator.next();
            boolean isTarget = key.equals(target);
            if (afterTarget || (isTarget && includeTarget)) {
                iterator.remove();
                savepoints--;
            }
            afterTarget |= isTarget;
        }
    }

    private void reserve(Cycle cycle, long bytes, int entries) {
        if (bytes > limits.maxPayloadBytes() - payloadBytes || entries > limits.maxRetainedEntries() - retainedEntries) {
            throw fail("Transaction buffer capacity exceeded; no pending work may be evicted");
        }
        payloadBytes += bytes;
        retainedEntries += entries;
        cycle.bytes += bytes;
        cycle.entries += entries;
    }

    private void release(Cycle cycle, long bytes, int entries) {
        payloadBytes -= bytes;
        retainedEntries -= entries;
        cycle.bytes -= bytes;
        cycle.entries -= entries;
    }

    private void removeCycle(Cycle cycle) {
        release(cycle, cycle.bytes, cycle.entries);
        savepoints -= cycle.savepoints.size();
        cycles.remove(cycle.id.commitCycle());
    }

    /** Latch normalization failures too, so a caller cannot accidentally commit a partial cycle. */
    void abort() {
        failed = true;
    }

    private BufferException fail(String message) {
        failed = true;
        return new BufferException(message);
    }

    private void sequence(BigInteger sequence) {
        if (sequence == null || sequence.signum() <= 0 || sequence.compareTo(MAX_SEQUENCE) > 0) {
            throw fail("Invalid unsigned journal sequence or commit-cycle identifier");
        }
    }

    @Override
    public void close() {
        closed = true;
        cycles.clear();
        pendingBatch = null;
        payloadBytes = 0;
        retainedEntries = 0;
        savepoints = 0;
    }

    /** Entries count changes, unpaired images, and per-cycle table identities; bytes count owned row payloads. */
    record Limits(int maxTransactions, int maxRetainedEntries, int maxSavepoints, long maxPayloadBytes) {
        Limits {
            if (maxTransactions <= 0 || maxRetainedEntries <= 0 || maxSavepoints < 0 || maxPayloadBytes < 0) {
                throw new IllegalArgumentException("Transaction limits must be finite and non-negative, with positive transaction/entry limits");
            }
        }
    }

    record Usage(int transactions, int retainedEntries, int savepoints, long payloadBytes) {
    }

    enum SavepointRollback {
        UNRESOLVED,
        RETAIN_TARGET,
        RELEASE_TARGET,
        /**
         * Undo to a known marker and consume it locally without claiming that IBM i
         * released it. A later reference to that marker fails closed; only a new SB
         * can establish a new usable target. This may reject a valid source history,
         * but cannot publish changes by guessing that an unknown target still exists.
         */
        CONSUME_TARGET
    }

    /**
     * One pinned file/member association. JIDs identify different native objects:
     * D/CG uses *FILE; row images use the member data space (*QDDS). Construction
     * validates the representation, not the association's native provenance/history.
     */
    record TableIdentity(TableId tableId, String physicalFile, String member,
            String fileJournalId, String memberJournalId, String recordFormat) {
        TableIdentity {
            Objects.requireNonNull(tableId);
            identifier(tableId.toString(), 512);
            physicalFile = identifier(physicalFile, 10);
            member = identifier(member, 10);
            if (!validJournalId(fileJournalId) || !validJournalId(memberJournalId) || fileJournalId.equals(memberJournalId)) {
                throw new IllegalArgumentException("Distinct, complete file and member journal identifiers are required");
            }
            recordFormat = identifier(recordFormat, 128);
        }

        static boolean validJournalId(String value) {
            return value != null && value.matches("[0-9a-f]{20}")
                    && !value.equals("00000000000000000000") && !value.equals("40404040404040404040");
        }
    }

    /** Producer identifies the source job/thread; recordNumber is the physical relative record number. */
    record RecordIdentity(TableIdentity table, String producer, BigInteger recordNumber) {
        RecordIdentity {
            Objects.requireNonNull(table);
            producer = identifier(producer, 128);
            if (recordNumber == null || recordNumber.signum() <= 0 || recordNumber.compareTo(MAX_SEQUENCE) > 0) {
                throw new IllegalArgumentException("Invalid relative record number");
            }
        }
    }

    record TransactionId(String journalIdentity, BigInteger commitCycle, JournalPosition start) {
    }

    record CommittedBatch(TransactionId transactionId, As400SourceMetadata startSource, As400SourceMetadata commitSource, List<Change> changes) {
        CommittedBatch {
            changes = List.copyOf(changes);
        }

        JournalPosition commitPosition() {
            return commitSource.position();
        }
    }

    record Change(Operation operation, TableIdentity table, RecordIdentity record, As400SourceMetadata source, Image before, Image after) {
        long bytes() {
            return (before == null ? 0L : before.row.payloadBytes()) + (after == null ? 0L : after.row.payloadBytes());
        }

        JournalPosition position() {
            return source.position();
        }
    }

    static final class Image {
        private final As400SourceMetadata source;
        private final As400RowData row;

        Image(As400SourceMetadata source, As400RowData row) {
            this.source = source;
            this.row = row;
        }

        As400SourceMetadata source() {
            return source;
        }

        JournalPosition position() {
            return source.position();
        }

        Object[] values() {
            return row.values();
        }
    }

    static final class BufferException extends DebeziumException {
        BufferException(String message) {
            super(message);
        }
    }

    private static String identifier(String value, int maximumLength) {
        if (value == null || value.isBlank() || value.length() > maximumLength) {
            throw new IllegalArgumentException("Missing or oversized normalized identifier");
        }
        return value;
    }

    private static final class Cycle {
        private final TransactionId id;
        private final List<Change> changes = new ArrayList<>();
        private final Map<RecordIdentity, Image> before = new LinkedHashMap<>();
        private final Map<TableId, TableIdentity> tables = new LinkedHashMap<>();
        private final Map<BigInteger, Integer> savepoints = new LinkedHashMap<>();
        private long bytes;
        private int entries;
        private boolean rollingBack;
        private boolean compensating;

        private final As400SourceMetadata startSource;

        private Cycle(TransactionId id, As400SourceMetadata startSource) {
            this.id = id;
            this.startSource = startSource;
        }
    }
}
