/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.config.CommonConnectorConfig.EventConvertingFailureHandlingMode;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.QueueProvider;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.TransactionId;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * Prepares committed output on one thread through the real Debezium dispatcher.
 * Used by the admitted recovery source. Records must pass through the replay coordinator;
 * their event-position offsets here are NOT recovery checkpoints. No source
 * startup, ownership, delivery acknowledgement or scheduled executor is installed.
 */
final class As400BatchDispatcher implements As400ReplayCoordinator.RecordFactory, AutoCloseable {
    private final Thread owner = Thread.currentThread();
    private final As400ConnectorConfig config;
    private final As400Partition partition;
    private final Clock clock;
    private final boolean ordinary;
    private final StagingQueue staging;
    private final EventDispatcher<As400Partition, TableId> dispatcher;
    private final HeartbeatImpl heartbeat;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final As400BufferedSignals signals;
    private io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext<TableId> snapshotContext;
    private As400BufferedIncrementalSnapshot incrementalSnapshot;
    private volatile boolean stopped;
    private boolean active;
    private As400OffsetContext source;

    As400BatchDispatcher(As400ConnectorConfig config, As400Partition partition, DatabaseSchema<TableId> schema,
                         TopicNamingStrategy<TableId> topics, DebeziumHeaderProducer headers, Clock clock, int maximumRecords) {
        this(config, partition, schema, topics, headers, clock, maximumRecords, false);
    }

    As400BatchDispatcher(As400ConnectorConfig config, As400Partition partition, DatabaseSchema<TableId> schema,
                         TopicNamingStrategy<TableId> topics, DebeziumHeaderProducer headers, Clock clock, int maximumRecords, boolean ordinary) {
        this(config, partition, schema, topics, headers, clock, maximumRecords, ordinary, null);
    }

    As400BatchDispatcher(As400ConnectorConfig config, As400Partition partition, DatabaseSchema<TableId> schema,
                         TopicNamingStrategy<TableId> topics, DebeziumHeaderProducer headers, Clock clock, int maximumRecords, boolean ordinary,
                         As400BufferedSignals signals) {
        this.signals = signals;
        this.ordinary = ordinary;
        this.config = Objects.requireNonNull(config);
        this.partition = Objects.requireNonNull(partition);
        this.clock = Objects.requireNonNull(clock);
        if (maximumRecords <= 0 || maximumRecords == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("A bounded staging capacity is required");
        }
        requireConfiguration(config);
        staging = new StagingQueue(maximumRecords);
        // Reject at the provider's smaller bound instead of blocking this single
        // producer while it is preparing the complete batch before any polling.
        queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxQueueSize(maximumRecords + 1).maxBatchSize(maximumRecords)
                .pollInterval(Duration.ofMillis(1)).queueProvider(staging).build();
        boolean disabledHeartbeat = config.getHeartbeatInterval().isZero();
        heartbeat = new HeartbeatImpl(config.getHeartbeatInterval(), disabledHeartbeat ? "__debezium-truncate." + config.getLogicalName() : topics.heartbeatTopic(),
                config.getLogicalName(), config.schemaNameAdjuster(), queue);
        dispatcher = new As400EventDispatcher(config, topics, schema, queue, id -> true, DataChangeEvent::new,
                new As400EventMetadataProvider(), disabledHeartbeat ? Heartbeat.ScheduledHeartbeat.NOOP_HEARTBEAT : heartbeat, config.schemaNameAdjuster(), signals,
                headers);
    }

    void incrementalSnapshot(As400BufferedIncrementalSnapshot snapshot, As400Checkpoint checkpoint) {
        if (signals == null || incrementalSnapshot != null) {
            throw new IllegalStateException("Incremental snapshot requires one admitted signal processor");
        }
        incrementalSnapshot = snapshot;
        snapshotContext = As400IncrementalSnapshotState.restore(checkpoint.snapshotState());
        source = new As400OffsetContext(config, new JournalProcessedPosition(checkpoint.completedThrough().offset(),
                checkpoint.completedThrough().receiver(), clock.currentTimeAsInstant(), true), true, snapshotContext);
        signals.setContext(io.debezium.pipeline.spi.Offsets.of(partition, source));
        dispatcher.setIncrementalSnapshotChangeEventSource(java.util.Optional.of(snapshot));
    }

    private void processSignals() throws InterruptedException {
        if (incrementalSnapshot != null) {
            incrementalSnapshot.start(partition, source);
            signals.executePending();
        }
    }

    void schemaChanged(As400BindingDefinition definition) {
        if (incrementalSnapshot != null) {
            if (snapshotContext.snapshotRunning() && snapshotContext.currentDataCollectionId() != null
                    && definition.schema().id().equals(snapshotContext.currentDataCollectionId().getId())) {
                throw new IllegalArgumentException("Schema refresh during an active incremental snapshot is not supported by this connector");
            }
            incrementalSnapshot.schemaChanged(definition);
        }
    }

    @Override
    public boolean hasSignalWork() {
        return incrementalSnapshot != null;
    }

    @Override
    public java.util.Optional<Map<String, String>> snapshotState() {
        return snapshotContext == null ? java.util.Optional.empty() : java.util.Optional.of(As400IncrementalSnapshotState.capture(snapshotContext));
    }

    @Override
    public void prepareSignals(Consumer<SourceRecord> records) throws Exception {
        try {
            enter();
            processSignals();
            drain(records);
        }
        catch (Exception failure) {
            stopped = true;
            throw failure;
        }
        finally {
            leave();
        }
    }

    /** Admission must run this before first-start side effects; construction also defends the boundary. */
    static void requireConfiguration(As400ConnectorConfig config) {
        boolean online = As400DcgConfig.transactionalTruncateEnabled(config.getConfig());
        if (config.getSkippedOperations().contains(io.debezium.data.Envelope.Operation.TRUNCATE)
                || !online && (config.getEventConvertingFailureHandlingMode() != EventConvertingFailureHandlingMode.FAIL
                        || config.getEventProcessingFailureHandlingMode() != EventProcessingFailureHandlingMode.FAIL)
                || config.getHeartbeatInterval().isNegative()) {
            throw new IllegalArgumentException("Buffered dispatch requires truncate capture, "
                    + "event.processing.failure.handling.mode=fail, event.converting.failure.handling.mode=fail and heartbeat.interval.ms >= 0");
        }
    }

    @Override
    public boolean needsValues(io.debezium.relational.TableId table) {
        return config.getTableFilters().dataCollectionFilter().isIncluded(table) || config.isSignalDataCollection(table);
    }

    @Override
    public void prepare(CommittedBatch batch, Consumer<SourceRecord> records) throws Exception {
        try {
            enter();
            Objects.requireNonNull(records);
            boolean transactional = batch.transactionId().commitCycle().signum() > 0;
            if (batch.transactionId().commitCycle().signum() < 0 || (!transactional && (!ordinary || batch.changes().size() != 1))) {
                throw new IllegalArgumentException("Invalid transaction or autocommit batch");
            }
            var selected = batch.changes().stream().filter(c -> needsValues(c.table().tableId())).iterator();
            if (!selected.hasNext()) {
                return; // No successful transaction metadata for an empty/filtered batch.
            }
            source = context(batch.startSource());
            processSignals();
            drain(records);
            if (transactional) {
                String id = As400DcgConfig.transactionalTruncateEnabled(config.getConfig())
                        ? batch.transactionId().commitCycle().toString()
                        : transactionId(batch.transactionId());
                dispatcher.dispatchTransactionStartedEvent(partition, id, source, batch.startSource().timestamp());
                drain(records);
            }
            while (selected.hasNext()) {
                As400TransactionBuffer.Change change = selected.next();
                position(change.source());
                var emitter = new As400ChangeRecordEmitter(partition, source, change.operation(),
                        change.before() == null ? null : change.before().values(),
                        change.after() == null ? null : change.after().values(), clock, config);
                if (incrementalSnapshot != null && change.operation() == io.debezium.data.Envelope.Operation.TRUNCATE) {
                    incrementalSnapshot.truncate(change.table().tableId());
                }
                Map<String, Object> transactionBefore = null;
                if (config.getEventProcessingFailureHandlingMode() != EventProcessingFailureHandlingMode.FAIL) {
                    transactionBefore = new java.util.HashMap<>();
                    source.getTransactionContext().store(transactionBefore);
                }
                try {
                    if (!dispatcher.dispatchDataChangeEvent(partition, change.table().tableId(), emitter)) {
                        if (change.operation() == io.debezium.data.Envelope.Operation.TRUNCATE
                                || config.isSignalDataCollection(change.table().tableId())
                                || config.getEventProcessingFailureHandlingMode() == EventProcessingFailureHandlingMode.FAIL) {
                            throw new IllegalStateException("Required buffered dispatch was rejected for " + change.table().tableId());
                        }
                        // Explicit WARN/SKIP has the same lossy row semantics as
                        // ordinary dispatch. Do not publish a partial key change.
                        staging.records.clear();
                        source.setTransaction(TransactionContext.load(transactionBefore));
                    }
                }
                finally {
                    emitter.rethrowDispatchInterruption();
                }
                processSignals();
                // The replay coordinator prepares the entire batch before any
                // publication. Drain each logical change into its bounded store
                // instead of retaining a second complete batch in this deque.
                drain(records);
            }
            position(batch.commitSource());
            if (transactional) {
                dispatcher.dispatchTransactionCommittedEvent(partition, source, batch.commitSource().timestamp());
            }
            drain(records);
        }
        catch (Exception e) {
            stopped = true;
            if (e instanceof InterruptedException || e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
        finally {
            leave();
        }
    }

    private void drain(Consumer<SourceRecord> records) {
        ensureActive();
        while (!staging.records.isEmpty()) {
            records.accept(staging.records.removeFirst().getRecord());
            ensureActive();
        }
    }

    /**
     * Invoke from the same source loop after delivery or while idle. The real
     * heartbeat timer is checked here; no executor may publish around a batch.
     * The coordinator stamps current safe F/W, even if the scan is further ahead.
     */
    boolean publishCheckpoint(As400ReplayCoordinator replay, boolean force) throws InterruptedException {
        try {
            enter();
            if (!force && (config.getHeartbeatInterval().isZero() || !heartbeat.expired())) {
                return false;
            }
            var checkpoint = replay.checkpoint();
            var position = new JournalProcessedPosition(checkpoint.completedThrough().offset(),
                    checkpoint.completedThrough().receiver(), clock.currentTimeAsInstant(), true);
            if (snapshotContext == null) {
                source = new As400OffsetContext(config, position);
            }
            else {
                source.setPosition(position);
            }
            source.setTransaction(new TransactionContext());
            heartbeat.emit(partition.getSourcePartition(), source);
            heartbeat.reset();
            ensureActive();
            if (staging.records.size() != 1) {
                throw new IllegalStateException("A checkpoint heartbeat must produce exactly one record");
            }
            replay.publishCheckpoint(staging.records.removeFirst().getRecord());
            return true;
        }
        catch (RuntimeException | InterruptedException e) {
            stopped = true;
            if (e instanceof InterruptedException || e.getCause() instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
        finally {
            leave();
        }
    }

    private As400OffsetContext context(As400SourceMetadata metadata) {
        var position = new JournalProcessedPosition(metadata.position().offset(), metadata.position().receiver(), metadata.timestamp(), true);
        var context = snapshotContext == null ? new As400OffsetContext(config, position) : source;
        context.setPosition(position);
        // Never restore metadata counters: recovery replays whole committed batches.
        context.setTransaction(new TransactionContext());
        context.updateSourceInfo(metadata);
        return context;
    }

    private void position(As400SourceMetadata metadata) {
        source.setPosition(new JournalProcessedPosition(metadata.position().offset(), metadata.position().receiver(), metadata.timestamp(), true));
        source.updateSourceInfo(metadata);
    }

    static String transactionId(TransactionId id) {
        return "dcg-v1:" + field(id.journalIdentity()) + field(id.commitCycle().toString())
                + field(id.start().receiver().library()) + field(id.start().receiver().name()) + field(id.start().offset().toString());
    }

    private static String field(String value) {
        return value.length() + ":" + value;
    }

    private void enter() {
        if (Thread.currentThread() != owner || stopped || active || !staging.records.isEmpty()) {
            stopped = true;
            throw new IllegalStateException("Buffered dispatcher is stopped, reentrant or used from another thread");
        }
        active = true;
        staging.prepared = 0;
    }

    private void ensureActive() {
        if (Thread.currentThread() != owner || stopped || !active) {
            stopped = true;
            throw new IllegalStateException("Output outside the synchronous preparation scope");
        }
    }

    private void leave() {
        if (Thread.currentThread() == owner) {
            staging.records.clear();
            active = false;
        }
    }

    /** The enclosing lifecycle may close this dispatcher; publication still requires an active staging scope. */
    EventDispatcher<As400Partition, TableId> eventDispatcher() {
        return dispatcher;
    }

    @Override
    public void close() {
        stopped = true;
        if (Thread.currentThread() == owner) {
            staging.records.clear();
        }
        dispatcher.close();
    }

    private final class StagingQueue implements QueueProvider<DataChangeEvent> {
        private final Deque<DataChangeEvent> records = new ArrayDeque<>();
        private final int maximum;
        private int prepared;

        private StagingQueue(int maximum) {
            this.maximum = maximum;
        }

        @Override
        public String name() {
            return "ibmi-committed-batch-staging";
        }

        @Override
        public void configure(Map<String, ?> properties) {
        }

        @Override
        public void enqueue(DataChangeEvent event) {
            ensureActive();
            if (prepared >= maximum) {
                stopped = true;
                throw new IllegalStateException("Committed batch exceeds output staging bound");
            }
            prepared++;
            records.addLast(event);
        }

        @Override
        public DataChangeEvent poll() {
            throw new UnsupportedOperationException("Only the batch adapter may drain prepared records");
        }

        @Override
        public int size() {
            return records.size();
        }
    }
}
