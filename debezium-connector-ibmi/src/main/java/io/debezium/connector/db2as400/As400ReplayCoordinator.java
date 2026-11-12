/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.DebeziumException;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Control;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Kind;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Outcome;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/**
 * Single-threaded replay and serial output coordinator used by the admitted source.
 * Restarts from an offset supplied by the durable offset store; enqueue success is
 * NOT durability. The sink must preserve source-partition order and acknowledge a
 * prefix of that order before persisting offsets. Scheduled heartbeats must be
 * marshalled onto the same thread/queue. Scratch storage is never restart authority.
 */
final class As400ReplayCoordinator implements AutoCloseable {
    private final As400Checkpoint loaded;
    private final As400JournalOrder order;
    private final As400TransactionBuffer buffer;
    private final As400SourceNormalizer normalizer;
    private final As400SourceNormalizer.TableResolver tables;
    private final As400AutocommitBuffer autocommit;
    private final As400JournalLifecycle journalLifecycle;
    private final Limits limits;
    private final int maximumOutputRecords;
    private final Map<String, ?> sourcePartition;
    private final RecordSink sink;
    private final TransactionControlEntryDecoder controls = new TransactionControlEntryDecoder();
    private final Set<BigInteger> orphans;
    private final As400SpillStore spill;
    private final As400SpillStore.Codec<SourceRecord> outputCodec;
    private As400Checkpoint checkpoint;
    private JournalPosition completed;
    private JournalPosition scan;
    private CommittedBatch pending;
    private boolean replaying;
    private boolean busy;
    private boolean stopped;

    long[] bufferUsage() {
        var usage = buffer.usage();
        var lifecycle = journalLifecycle == null ? new long[]{ 0, 0 } : journalLifecycle.bufferUsage();
        return new long[]{ Math.max(usage.transactions(), lifecycle[0]),
                usage.payloadBytes() + lifecycle[1] + (autocommit == null ? 0 : autocommit.payloadBytes()) };
    }

    long oldestTransactionTimestamp() {
        long a = buffer.oldestTransactionTimestamp();
        long b = journalLifecycle == null ? -1 : journalLifecycle.oldestTransactionTimestamp();
        return a < 0 ? b : b < 0 ? a : Math.min(a, b);
    }

    As400ReplayCoordinator(Map<String, ?> persistedOffset, String journalIdentity, String fingerprint,
                           As400JournalOrder order, Limits limits, long maximumRowBytes, int maximumOutputRecords,
                           As400SourceNormalizer.TableResolver tables, As400SourceNormalizer.SavepointPolicy savepoints,
                           Map<String, ?> sourcePartition, RecordSink sink) {
        this(persistedOffset, journalIdentity, fingerprint, order, limits, maximumRowBytes, maximumOutputRecords,
                tables, savepoints, sourcePartition, sink, null);
    }

    /**
     * Explicit staged native profile with conservative savepoint consumption.
     * The supplied savepoint policy applies only when lifecycleLimits is null.
     * The admitted recovery source selects the separately qualified ordinary profile.
     */
    As400ReplayCoordinator(Map<String, ?> persistedOffset, String journalIdentity, String fingerprint,
                           As400JournalOrder order, Limits limits, long maximumRowBytes, int maximumOutputRecords,
                           As400SourceNormalizer.TableResolver tables, As400SourceNormalizer.SavepointPolicy savepoints,
                           Map<String, ?> sourcePartition, RecordSink sink, As400JournalLifecycle.Limits lifecycleLimits) {
        this(persistedOffset, journalIdentity, fingerprint, order, limits, maximumRowBytes, maximumOutputRecords,
                tables, savepoints, sourcePartition, sink, lifecycleLimits, false);
    }

    /** Operator-retention profile; native lifecycle validation and qualified task admission remain mandatory. */
    As400ReplayCoordinator(Map<String, ?> persistedOffset, String journalIdentity, String fingerprint,
                           As400JournalOrder order, Limits limits, long maximumRowBytes, int maximumOutputRecords,
                           As400SourceNormalizer.TableResolver tables, As400SourceNormalizer.SavepointPolicy savepoints,
                           Map<String, ?> sourcePartition, RecordSink sink, As400JournalLifecycle.Limits lifecycleLimits, boolean ordinary) {
        this(persistedOffset, journalIdentity, fingerprint, order, limits, maximumRowBytes, maximumOutputRecords,
                tables, savepoints, sourcePartition, sink, lifecycleLimits, ordinary, null);
    }

    As400ReplayCoordinator(Map<String, ?> persistedOffset, String journalIdentity, String fingerprint,
                           As400JournalOrder order, Limits limits, long maximumRowBytes, int maximumOutputRecords,
                           As400SourceNormalizer.TableResolver tables, As400SourceNormalizer.SavepointPolicy savepoints,
                           Map<String, ?> sourcePartition, RecordSink sink, As400JournalLifecycle.Limits lifecycleLimits, boolean ordinary,
                           As400SpillStore spill) {
        if (ordinary && lifecycleLimits == null) {
            throw new IllegalArgumentException("Ordinary CDC requires the native lifecycle validator");
        }
        loaded = As400Checkpoint.fromOffset(persistedOffset, journalIdentity, fingerprint);
        this.order = Objects.requireNonNull(order);
        this.limits = Objects.requireNonNull(limits);
        this.sink = Objects.requireNonNull(sink);
        this.sourcePartition = Map.copyOf(sourcePartition);
        if (maximumOutputRecords <= 0 || this.sourcePartition.isEmpty()) {
            throw new IllegalArgumentException("Bounded output and a source partition are required");
        }
        this.maximumOutputRecords = maximumOutputRecords;
        this.spill = spill;
        this.tables = tables;
        outputCodec = spill == null ? null : As400SourceRecordCodec.stored(spill);
        orphans = spill == null ? new HashSet<>() : spill.set("orphan-cycles", As400SpillCodec.INTEGER);
        int floorVsCompleted = order.compare(loaded.replayFrom(), loaded.completedThrough());
        if (!order.incarnation(loaded.replayFrom()).equals(loaded.replayReceiverIdentity())
                || !order.incarnation(loaded.completedThrough()).equals(loaded.completedReceiverIdentity())) {
            throw new IllegalArgumentException("Checkpoint receiver was replaced");
        }
        replaying = loaded.replayInclusive() && floorVsCompleted <= 0;
        checkpoint = loaded;
        completed = loaded.completedThrough();
        buffer = new As400TransactionBuffer(journalIdentity, limits, spill);
        autocommit = ordinary ? new As400AutocommitBuffer(journalIdentity, limits, buffer::usage, spill) : null;
        normalizer = new As400SourceNormalizer(buffer, tables, maximumRowBytes,
                lifecycleLimits == null ? savepoints : As400SourceNormalizer.SavepointPolicy.consumingTargets(), autocommit);
        journalLifecycle = lifecycleLimits == null ? null : new As400JournalLifecycle(lifecycleLimits, tables, order, loaded, ordinary, spill);
    }

    /** R is diagnostic/read state only. It is deliberately absent from serialized offsets. */
    Optional<JournalPosition> scanPosition() {
        return Optional.ofNullable(scan);
    }

    /** A safe immutable snapshot, including while a batch is pending or the coordinator has failed. */
    As400Checkpoint checkpoint() {
        return checkpoint;
    }

    boolean replaying() {
        return replaying;
    }

    /** Call only at a verified journal tail, not at an empty RPC page or filter boundary. */
    void verifyEndOfAvailableJournal() {
        try {
            ensureRunning();
            if (journalLifecycle != null) {
                journalLifecycle.verifyTail();
            }
            if (replaying || (scan == null && loaded.replayInclusive())) {
                throw new IllegalArgumentException("Replay floor or completion fence is missing from available journal data");
            }
        }
        catch (Exception e) {
            throw stop("Journal ended before recovery was complete", e);
        }
    }

    /** A verified bounded prefix can end before W; no output or checkpoint advance is allowed during that replay. */
    void verifyOrdinaryInterval(boolean complete) {
        if (complete || !replaying) {
            verifyOrdinaryCut();
            return;
        }
        ensureRunning();
        if (autocommit == null || scan == null || !checkpoint.equals(loaded)) {
            throw stop("Unsafe partial ordinary reconstruction", new IllegalArgumentException("Partial replay must preserve durable F/W"));
        }
    }

    /** A live cut may end inside an open lifecycle; retain F and wait for the next validated cut. */
    void verifyOrdinaryCut() {
        try {
            ensureRunning();
            if (autocommit == null || replaying || (scan == null && loaded.replayInclusive())) {
                throw new IllegalArgumentException("Ordinary recovery boundary has not been reconstructed");
            }
        }
        catch (Exception e) {
            throw stop("Incomplete ordinary recovery cut", e);
        }
    }

    Optional<CommittedBatch> accept(EntryHeader header, byte[] data, int offset, JournalPosition position) {
        try {
            ensureRunning();
            if (pending != null || busy) {
                throw new IllegalStateException("Pending output must finish before scanning another entry");
            }
            checkScanPosition(position);
            if (journalLifecycle == null) {
                acceptComponent(header, data, offset, position);
            }
            else {
                journalLifecycle.accept(header, data, offset, position);
                drainValidatedEntries();
            }
            scan = position;
            return Optional.ofNullable(pending);
        }
        catch (Exception e) {
            throw stop("Cannot accept journal entry at " + position, e);
        }
    }

    private void acceptComponent(EntryHeader header, byte[] data, int offset, JournalPosition position) throws Exception {
        if (replaying && order.compare(position, completed) > 0) {
            throw new IllegalArgumentException("Completed boundary was not found during replay");
        }
        As400SourceMetadata source = As400SourceNormalizer.validateSource(header, data, offset, position, tables);
        Control control = header.getJournalCode() == 'C' ? controls.decode(header, data, offset) : null;
        BigInteger cycle = source.commitCycle();
        boolean start = control != null && control.kind() == Kind.START;
        boolean lifecycle = control != null && (control.kind() == Kind.BEGIN_COMMITMENT_CONTROL || control.kind() == Kind.END_COMMITMENT_CONTROL);
        boolean known = buffer.hasTransaction(cycle);
        if (start) {
            if (orphans.contains(cycle)) {
                throw new IllegalArgumentException("A start follows an orphan suffix for the same cycle");
            }
            if (!known) {
                requireCycleCapacity();
            }
        }
        if (lifecycle && orphans.contains(cycle)) {
            throw new IllegalArgumentException("Lifecycle control cannot resolve an orphan cycle");
        }
        if (!start && !lifecycle && !known && cycle.signum() > 0 && replaying) {
            if (!orphans.contains(cycle)) {
                requireCycleCapacity();
                orphans.add(cycle);
            }
            acceptOrphan(header, data, offset, control);
            if (terminal(control)) {
                orphans.remove(cycle);
            }
        }
        else {
            Optional<CommittedBatch> result = normalizer.accept(header, data, offset, position, replaying && order.compare(position, completed) <= 0);
            if (autocommit != null) {
                autocommit.checkCapacity();
            }
            if (result.isPresent()) {
                CommittedBatch batch = result.orElseThrow();
                if (order.compare(position, completed) <= 0 || batch.changes().isEmpty()) {
                    completeBatch(batch);
                    if (!replaying && order.compare(position, completed) > 0) {
                        advance(position);
                    }
                }
                else {
                    pending = batch;
                }
            }
            else if (control != null && control.kind() == Kind.ROLLBACK && !replaying) {
                advance(position);
            }
        }
        if (replaying && order.compare(position, completed) == 0) {
            boolean completedAutocommit = autocommit != null && cycle.signum() == 0
                    && ((header.getJournalCode() == 'R' && Set.of("PT", "PX", "UP", "DL").contains(header.getEntryType()))
                            || (header.getJournalCode() == 'F' && header.getEntryType().equals("CR")));
            if ((!terminal(control) && !completedAutocommit) || !orphans.isEmpty()) {
                throw new IllegalArgumentException("Recovery fence has unresolved orphan cycles or is not a completion boundary");
            }
            replaying = false;
        }
        // Until W is reached, the complete set of starts/orphans is not yet known.
        if (!replaying) {
            checkpoint = snapshot(completed);
        }
    }

    private void drainValidatedEntries() throws Exception {
        while (pending == null && journalLifecycle.hasReady()) {
            var entry = journalLifecycle.poll().orElseThrow();
            acceptComponent(entry.header(), entry.bytes(), 0, entry.position());
        }
    }

    /**
     * Prepare every required SourceRecord before the first enqueue, including BEGIN/END,
     * tombstones, key-change subrecords and dispatcher-generated heartbeats. The coordinator
     * replaces every offset with the same frozen (F, previous W) and checks the partition.
     * A validated lifecycle window can release several commits: drain them serially,
     * freezing a new checkpoint for each batch, before accepting another raw entry.
     */
    void deliver(RecordFactory factory) {
        try {
            ensureRunning();
            if (pending == null || busy) {
                throw new IllegalStateException("No deliverable batch or reentrant output");
            }
            busy = true;
            do {
                CommittedBatch batch = pending;
                Map<String, String> frozen = checkpoint.toOffset();
                List<SourceRecord> prepared = spill == null ? new ArrayList<>() : spill.list("prepared-output", outputCodec);
                if (!prepared.isEmpty()) {
                    throw new IllegalStateException("Output from another batch is still retained");
                }
                boolean[] collecting = { true };
                try {
                    factory.prepare(batch, record -> {
                        try {
                            ensureRunning();
                            if (!collecting[0] || prepared.size() >= maximumOutputRecords) {
                                throw new IllegalArgumentException("Late or oversized prepared output");
                            }
                            prepared.add(stamp(record, frozen));
                        }
                        catch (RuntimeException e) {
                            // A factory must not swallow a rejected required record and publish the rest.
                            throw stop("Required record preparation failed", e);
                        }
                    });
                }
                finally {
                    collecting[0] = false;
                }
                ensureRunning();
                for (SourceRecord record : prepared) {
                    enqueue(record);
                }
                prepared.clear();
                completeBatch(batch);
                pending = null;
                advance(batch.commitPosition(), factory.snapshotState().orElse(checkpoint.snapshotState()));
                if (journalLifecycle != null) {
                    drainValidatedEntries();
                }
            } while (pending != null);
        }
        catch (Exception e) {
            throw stop("Batch delivery failed; replay the whole batch from a durable checkpoint", e);
        }
        finally {
            busy = false;
        }
    }

    /** Snapshot signals use the same all-prepared, ordered publication boundary as row batches. */
    void deliverSignals(RecordFactory factory) {
        try {
            ensureRunning();
            if (pending != null || busy) {
                throw new IllegalStateException("Signals cannot overlap committed batch publication");
            }
            busy = true;
            var frozen = checkpoint.toOffset();
            List<SourceRecord> prepared = spill == null ? new ArrayList<>() : spill.list("prepared-signals", outputCodec);
            if (!prepared.isEmpty()) {
                throw new IllegalStateException("Previous signal output is still retained");
            }
            boolean[] collecting = { true };
            try {
                factory.prepareSignals(record -> {
                    ensureRunning();
                    if (!collecting[0] || prepared.size() >= maximumOutputRecords) {
                        throw stop("Required signal output rejected", new IllegalArgumentException("Late or oversized snapshot signal output"));
                    }
                    prepared.add(stamp(record, frozen));
                });
            }
            finally {
                collecting[0] = false;
            }
            ensureRunning();
            for (var record : prepared) {
                enqueue(record);
            }
            prepared.clear();
            checkpoint = checkpoint.withSnapshotState(factory.snapshotState().orElse(checkpoint.snapshotState()));
        }
        catch (Exception e) {
            throw stop("Signal delivery failed; restore the durable snapshot checkpoint", e);
        }
        finally {
            busy = false;
        }
    }

    /** Ordered heartbeat publication; the returned record is an enqueue receipt, not a durable acknowledgement. */
    SourceRecord publishCheckpoint(SourceRecord heartbeat) {
        try {
            ensureRunning();
            if (busy) {
                throw new IllegalStateException("Reentrant heartbeat; prepare internal heartbeats with the batch instead");
            }
            busy = true;
            SourceRecord stamped = stamp(heartbeat, checkpoint.toOffset());
            enqueue(stamped);
            return stamped;
        }
        catch (Exception e) {
            throw stop("Checkpoint publication failed", e);
        }
        finally {
            busy = false;
        }
    }

    private void checkScanPosition(JournalPosition position) {
        if (scan == null) {
            int comparison = order.compare(position, loaded.replayFrom());
            if (loaded.replayInclusive() ? comparison != 0 : comparison <= 0) {
                throw new IllegalArgumentException("Reader did not resume at the checkpoint replay boundary");
            }
        }
        else if (order.compare(position, scan) <= 0) {
            throw new IllegalArgumentException("Journal order regressed or repeated");
        }
    }

    private void acceptOrphan(EntryHeader header, byte[] data, int offset, Control control) throws Exception {
        if (control != null) {
            if (control.kind() == Kind.UNSUPPORTED || control.kind() == Kind.START) {
                throw new IllegalArgumentException("Unsupported orphan control");
            }
            if (terminal(control) && (control.controlInfo().nestedCommitLevel() != 0
                    || (control.kind() == Kind.COMMIT && control.outcome() != Outcome.DURABLE_COMMIT)
                    || (control.kind() == Kind.ROLLBACK && control.outcome() != Outcome.FULL_ROLLBACK))) {
                throw new IllegalArgumentException("Orphan completion is not durable or complete");
            }
            // SQ/SU targets were decoded and bounded, but may refer to markers before F.
            return;
        }
        if (header.getJournalCode() == 'R' && Set.of("PT", "PX", "UB", "UP", "DL", "BR", "DR", "UR").contains(header.getEntryType())) {
            // No value/image normalization: UB, SC and savepoints can predate F.
            return;
        }
        if (header.getJournalCode() == 'D' && header.getEntryType().equals("CG")
                && new FileChangeEntryDecoder().decode(header, data, offset).clearsRows()) {
            return;
        }
        throw new IllegalArgumentException("Unsupported orphan record");
    }

    private static boolean terminal(Control control) {
        return control != null && (control.kind() == Kind.COMMIT || control.kind() == Kind.ROLLBACK);
    }

    private void requireCycleCapacity() {
        if (orphans.size() + buffer.usage().transactions() >= limits.maxTransactions()) {
            throw new IllegalArgumentException("Replay cycle limit exceeded; pending state cannot be evicted");
        }
    }

    private void advance(JournalPosition boundary) {
        advance(boundary, checkpoint.snapshotState());
    }

    private void advance(JournalPosition boundary, Map<String, String> snapshotState) {
        if (order.compare(boundary, completed) <= 0) {
            throw new IllegalArgumentException("Completed boundary must advance");
        }
        As400Checkpoint next = snapshot(boundary).withSnapshotState(snapshotState);
        completed = boundary;
        checkpoint = next;
    }

    private As400Checkpoint snapshot(JournalPosition boundary) {
        Optional<JournalPosition> oldest = buffer.oldestTransactionPosition();
        if (autocommit != null) {
            for (JournalPosition position : autocommit.retainedStarts()) {
                if (oldest.isEmpty() || order.compare(position, oldest.orElseThrow()) < 0) {
                    oldest = Optional.of(position);
                }
            }
        }
        if (autocommit != null && oldest.isPresent() && order.compare(oldest.orElseThrow(), boundary) > 0) {
            // Ordinary retention includes the durable W anchor. Starting exclusively
            // after W reconstructs every later start/UB, without requiring operators
            // to retain a separate older W receiver outside the advertised F floor.
            oldest = Optional.empty();
        }
        As400Checkpoint snapshot = new As400Checkpoint(loaded.journalIdentity(), loaded.captureFingerprint(), oldest.orElse(boundary), oldest.isPresent(), boundary,
                order.incarnation(oldest.orElse(boundary)), order.incarnation(boundary), checkpoint.snapshotState(), checkpoint.schemaState());
        return tables.schemaCheckpoint(journalLifecycle == null ? snapshot : journalLifecycle.retainContext(snapshot));
    }

    private void completeBatch(CommittedBatch batch) {
        if (batch.transactionId().commitCycle().signum() == 0) {
            if (autocommit == null) {
                throw new IllegalStateException("Autocommit output is not enabled");
            }
            autocommit.complete(batch);
        }
        else {
            buffer.completeBatch(batch);
        }
    }

    private SourceRecord stamp(SourceRecord record, Map<String, String> frozen) {
        Objects.requireNonNull(record, "Missing prepared record");
        if (!sourcePartition.equals(record.sourcePartition())) {
            throw new IllegalArgumentException("Prepared record belongs to another source partition");
        }
        return new SourceRecord(sourcePartition, frozen, record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), record.value(), record.timestamp(), record.headers().duplicate());
    }

    private void enqueue(SourceRecord record) throws Exception {
        ensureRunning();
        if (!sink.enqueue(record)) {
            throw new IllegalStateException("Required SourceRecord was not enqueued");
        }
        ensureRunning();
    }

    private void ensureRunning() {
        if (stopped) {
            throw new IllegalStateException("Replay coordinator is stopped; recreate it from a durable offset");
        }
    }

    private RecoveryException stop(String message, Exception cause) {
        stopped = true;
        buffer.abort();
        if (autocommit != null) {
            autocommit.close();
        }
        if (journalLifecycle != null) {
            journalLifecycle.close();
        }
        if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        return new RecoveryException(message, cause);
    }

    @Override
    public void close() {
        stopped = true;
        buffer.close();
        if (autocommit != null) {
            autocommit.close();
        }
        if (journalLifecycle != null) {
            journalLifecycle.close();
        }
        if (spill == null) {
            orphans.clear();
        }
        pending = null;
    }

    interface RecordFactory {
        /** Synchronously prepare complete immutable record values; do not retain the collector. */
        void prepare(CommittedBatch batch, Consumer<SourceRecord> records) throws Exception;

        default boolean needsValues(io.debezium.relational.TableId table) {
            return true;
        }

        default void prepareSignals(Consumer<SourceRecord> records) throws Exception {
        }

        default boolean hasSignalWork() {
            return false;
        }

        default Optional<Map<String, String>> snapshotState() {
            return Optional.empty();
        }
    }

    interface RecordSink {
        boolean enqueue(SourceRecord record) throws Exception;
    }

    static final class RecoveryException extends DebeziumException {
        RecoveryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
