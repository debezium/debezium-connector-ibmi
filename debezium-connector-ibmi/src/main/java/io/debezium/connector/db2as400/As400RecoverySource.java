/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/** Real coordinator source/factory; recovery or explicitly admitted first initialization, never a legacy reader. */
final class As400RecoverySource implements StreamingChangeEventSource<As400Partition, As400OffsetContext>,
        ChangeEventSourceFactory<As400Partition, As400OffsetContext>, DatabaseSchema<TableId> {
    private final As400TaskRecovery input;
    private final As400ConnectorConfig config;
    private volatile As400RecoveryOffsetContext offset;
    private final As400TaskInitialization initialization;
    private final Object initializationState = new Object();
    private volatile String initializationEpoch;
    private volatile boolean handedOff;
    private As400Checkpoint initialCheckpoint;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final ErrorHandler errors;
    private final TopicNamingStrategy<TableId> topics;
    private final DebeziumHeaderProducer headers;
    private final Supplier<Map<String, ?>> durable;
    private final JournalReadLifecycle admission = new JournalReadLifecycle();
    private final As400RecoveryAcknowledgements acknowledgements = new As400RecoveryAcknowledgements();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean cancelRequested = new AtomicBoolean();
    private final CompletableFuture<Void> cleanup = new CompletableFuture<>();
    private final CompletableFuture<Void> cancellation = new CompletableFuture<>();
    private volatile Throwable cancellationFailure;
    private volatile As400ContinuousRuntime runtime;
    private volatile As400NativeTableSchemas schemas;
    private volatile As400BindingHistory.Recovered history;
    private StreamingChangeEventSourceMetrics<As400Partition> metrics;
    private SourceRecord lastEnqueued;
    private int streamingChunkRecords;

    As400RecoverySource(As400TaskRecovery input, As400TaskInitialization initialization, As400ConnectorConfig config, As400RecoveryOffsetContext offset,
                        ChangeEventQueue<DataChangeEvent> queue, ErrorHandler errors, TopicNamingStrategy<TableId> topics,
                        DebeziumHeaderProducer headers, Supplier<Map<String, ?>> durable) {
        this.input = input;
        this.initialization = initialization;
        this.config = config;
        this.offset = offset;
        this.queue = queue;
        this.errors = errors;
        this.topics = topics;
        this.headers = headers;
        this.durable = durable;
    }

    void metrics(StreamingChangeEventSourceMetrics<As400Partition> metrics) {
        this.metrics = metrics;
    }

    private boolean captured(TableId table) {
        return config.getTableFilters().dataCollectionFilter().isIncluded(table);
    }

    private void check() throws Exception {
        admission.checkOpen();
        input.checkQualified(config.getConfig());
        synchronized (initializationState) {
            if (initializationEpoch != null && !handedOff && !initializationEpoch.equals(initialization.checkHeld())) {
                throw new IllegalStateException("Initialization session changed");
            }
        }
    }

    private void prepare() throws Exception {
        try (var lease = admission.enter()) {
            check();
            if (!offset.getOffset().equals(durable.get())) {
                throw new IllegalStateException("Durable F/W changed before history recovery");
            }
            var recovered = input.recover(offset.getOffset());
            check();
            if (!recovered.journalIdentity().equals(input.journalIdentity()) || !recovered.fingerprint().equals(input.fingerprint())
                    || recovered.initialization().isEmpty() || !offset.getOffset().equals(durable.get())) {
                throw new IllegalStateException("Recovery requires matching initialized history and durable F/W");
            }
            schemas = new As400NativeTableSchemas(recovered.definitions(), config, topics);
            history = recovered;
            if (metrics instanceof As400StreamingChangeEventSourceMetrics journalMetrics) {
                journalMetrics.setDurablePositions(offset.getOffset());
                journalMetrics.setJournalBehind(java.math.BigInteger.valueOf(-1));
            }
        }
    }

    private void initialize(SnapshotProgressListener<As400Partition> listener) throws Exception {
        try (var lease = admission.enter()) {
            check();
            if (offset != null || durable.get() != null) {
                throw new IllegalStateException("First initialization cannot replace existing offsets");
            }
            if (initialization.chunkLimits().isPresent() && !initialization.supportsChunks()) {
                throw new IllegalStateException("Initialization provider has no qualified chunk source");
            }
            initializationEpoch = initialization.enroll();
            new As400BindingHistory.Initialization(initializationEpoch, "pending");
            check();
            List<SourceRecord> baseline = new ArrayList<>();
            List<TableId> baselineTables = new ArrayList<>();
            AtomicBoolean installed = new AtomicBoolean();
            var checkpoint = initialization.initializer().initialize(new As400NativeInitialization.Session() {
                private As400SnapshotChunks chunks;
                private As400TaskInitialization.Row finalRow;
                private As400NativeTableSchemas nativeSchemas;
                private final Map<TableId, Long> scanned = new HashMap<>();
                private Map<String, ?> eventOffset;
                private final As400OffsetContext eventContext = new As400OffsetContext(config) {
                    @Override
                    public Map<String, ?> getOffset() {
                        return eventOffset;
                    }
                };

                private void scanned(As400TaskInitialization.Row row) {
                    listener.rowsScanned(input.partition(), row.table(), scanned.merge(row.table(), 1L, Long::sum));
                }

                private void snapshotEvent(TableId table, SourceRecord record) {
                    if (listener instanceof DataChangeEventListener<?> changes) {
                        @SuppressWarnings("unchecked")
                        var events = (DataChangeEventListener<As400Partition>) changes;
                        eventOffset = record.sourceOffset();
                        events.onEvent(input.partition(), table, eventContext, record.key(),
                                (org.apache.kafka.connect.data.Struct) record.value(),
                                io.debezium.data.Envelope.Operation.READ);
                    }
                }

                @Override
                public String checkHeld(io.debezium.ibmi.db2.journal.retrieve.JournalReceiver journal,
                                        List<As400OperatorTableEvidence.Request> captured)
                        throws Exception {
                    check();
                    if (!journal.equals(initialization.journal()) || !captured.equals(initialization.captured())) {
                        throw new IllegalStateException("Initialization source scope changed");
                    }
                    return initializationEpoch;
                }

                @Override
                public void prepareBaseline(String epoch, List<As400BindingDefinition> definitions) throws Exception {
                    checkEpoch(epoch);
                    nativeSchemas = new As400NativeTableSchemas(definitions, config, topics);
                    var selected = definitions.stream().filter(d -> captured(d.schema().id())).toList();
                    listener.monitoredDataCollectionsDetermined(input.partition(), selected.stream().map(d -> d.schema().id()).toList());
                    if (initialization.chunkLimits().isPresent()) {
                        chunks = new As400SnapshotChunks(epoch, initialization.chunkLimits().orElseThrow(), definitions, (rows, dataOffset, barrierOffset) -> {
                            checkEpoch(epoch);
                            List<SourceRecord> records = new ArrayList<>();
                            List<As400TaskInitialization.Row> selectedRows = new ArrayList<>();
                            // Validate the entire chunk before exposing any of it.
                            // Hold one owned row, never an acknowledged SourceRecord.
                            // A chunk receipt remains a non-resumable pending offset;
                            // BASELINE covers the final row as well as every chunk.
                            for (var row : rows) {
                                checkForPoll();
                                var record = snapshotRecord(row, nativeSchemas, dataOffset, SnapshotRecord.TRUE);
                                if (captured(row.table())) {
                                    records.add(record);
                                    selectedRows.add(row);
                                }
                            }
                            checkEpoch(epoch);
                            SourceRecord previous = null;
                            TableId previousTable = finalRow == null ? null : finalRow.table();
                            if (finalRow != null && !selectedRows.isEmpty()) {
                                previous = snapshotRecord(finalRow, nativeSchemas, dataOffset, SnapshotRecord.TRUE);
                            }
                            selectedRows.forEach(this::scanned);
                            if (previous != null) {
                                enqueueChunk(previous);
                            }
                            for (int i = 0; i < selectedRows.size() - 1; i++) {
                                enqueueChunk(records.get(i));
                            }
                            if (!selectedRows.isEmpty()) {
                                finalRow = selectedRows.get(selectedRows.size() - 1);
                            }
                            SourceRecord barrier = marker(barrierOffset, "CHUNK");
                            enqueueChunk(barrier);
                            // Keep metric formatting out of the queue-fill loop:
                            // a poll draining a partially filled batch otherwise
                            // waits for the poll timeout at every chunk.
                            if (previous != null) {
                                snapshotEvent(previousTable, previous);
                            }
                            for (int i = 0; i < selectedRows.size() - 1; i++) {
                                snapshotEvent(selectedRows.get(i).table(), records.get(i));
                            }
                            checkEpoch(epoch);
                            return awaitDurable(barrier);
                        });
                        var consumer = chunks.consumer();
                        initialization.scanBaseline(epoch, definitions, new As400SnapshotChunks.Consumer() {
                            @Override
                            public void chunk(TableId table, List<As400TaskInitialization.Row> rows) throws Exception {
                                consumer.chunk(table, rows);
                            }

                            @Override
                            public void complete(TableId table) throws Exception {
                                consumer.complete(table);
                                if (captured(table)) {
                                    listener.dataCollectionSnapshotCompleted(input.partition(), table, scanned.getOrDefault(table, 0L));
                                }
                            }
                        });
                        checkEpoch(epoch);
                        chunks.finish();
                        return;
                    }
                    var limits = initialization.snapshotLimits();
                    List<As400TaskInitialization.Row> allRows = List.copyOf(initialization.prepareBaseline(epoch, definitions));
                    List<As400TaskInitialization.Row> rows = allRows.stream().filter(r -> captured(r.table())).toList();
                    checkEpoch(epoch);
                    if (allRows.size() > limits.rows()) {
                        throw new IllegalArgumentException("Initialization snapshot exceeds row bound");
                    }
                    long bytes = 0;
                    for (var row : allRows) {
                        // These values are already owned and materialized. No
                        // source read or publication occurs while constructing
                        // envelopes; retain local stop/window checks per row and
                        // verify native exclusion again before exposing the list.
                        checkForPoll();
                        bytes += row.values().payloadBytes();
                        if (bytes > limits.bytes()) {
                            throw new IllegalArgumentException("Initialization snapshot exceeds byte bound");
                        }
                        var record = snapshotRecord(row, nativeSchemas, pending("snapshot"),
                                baseline.size() == rows.size() - 1 ? SnapshotRecord.LAST : SnapshotRecord.TRUE);
                        if (captured(row.table())) {
                            scanned(row);
                            baselineTables.add(row.table());
                            baseline.add(record);
                        }
                    }
                    for (var definition : selected) {
                        listener.dataCollectionSnapshotCompleted(input.partition(), definition.schema().id(),
                                scanned.getOrDefault(definition.schema().id(), 0L));
                    }
                    checkEpoch(epoch);
                }

                @Override
                public String acknowledgeBaseline(String epoch, As400NativeInitialization.Boundary boundary,
                                                  List<As400BindingDefinition> definitions)
                        throws Exception {
                    checkEpoch(epoch);
                    for (int i = 0; i < baseline.size(); i++) {
                        enqueue(baseline.get(i));
                        snapshotEvent(baselineTables.get(i), baseline.get(i));
                    }
                    baselineTables.clear();
                    baseline.clear();
                    var completion = chunks == null ? pending("baseline-complete") : chunks.baseline(boundary);
                    if (finalRow != null) {
                        var record = snapshotRecord(finalRow, nativeSchemas, completion, SnapshotRecord.LAST);
                        enqueueChunk(record);
                        snapshotEvent(finalRow.table(), record);
                        finalRow = null;
                    }
                    SourceRecord barrier = marker(completion, "BASELINE");
                    enqueue(barrier);
                    return awaitDurable(barrier);
                }

                @Override
                public void installCheckpoint(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered recovered) throws Exception {
                    checkEpoch(epoch);
                    if (!checkpoint.journalIdentity().equals(input.journalIdentity())
                            || !recovered.fingerprint().equals(checkpoint.captureFingerprint())
                            || !recovered.initialization().orElseThrow().epoch().equals(epoch)) {
                        throw new IllegalStateException("Initial checkpoint and recovered history differ");
                    }
                    SourceRecord barrier = marker(checkpoint.toOffset(), "INITIAL");
                    enqueue(barrier);
                    awaitDurable(barrier);
                    initialization.checkpointAccepted(epoch, checkpoint, recovered);
                    checkEpoch(epoch);
                    installed.set(true);
                }
            });
            check();
            if (!installed.get() || !checkpoint.toOffset().equals(durable.get())) {
                throw new IllegalStateException("Initial F/W was not durably installed");
            }
            initialCheckpoint = checkpoint;
            offset = new As400RecoveryOffsetContext(config, checkpoint.toOffset(), input.journalIdentity(), input.fingerprint());
        }
    }

    private void checkEpoch(String epoch) throws Exception {
        check();
        if (!epoch.equals(initializationEpoch)) {
            throw new IllegalStateException("Initialization epoch differs");
        }
    }

    private SourceRecord snapshotRecord(As400TaskInitialization.Row row, As400NativeTableSchemas nativeSchemas, Map<String, ?> pendingOffset, SnapshotRecord snapshot) {
        var schema = nativeSchemas.schemaFor(row.table());
        var source = new As400OffsetContext(config);
        source.markSnapshotRecord(snapshot);
        source.event(row.table(), row.timestamp());
        Object[] owned = row.values().values();
        return new SourceRecord(input.partition().getSourcePartition(), pendingOffset, topics.dataChangeTopic(row.table()),
                schema.keySchema(), schema.keyFromColumnData(owned), schema.getEnvelopeSchema().schema(),
                schema.getEnvelopeSchema().read(schema.valueFromColumnData(owned), source.getSourceInfo(), row.timestamp()));
    }

    private Map<String, String> pending(String phase) {
        return Map.of("initialization.version", "test-only-1", "initialization.epoch", initializationEpoch, "initialization.phase", phase);
    }

    private SourceRecord marker(Map<String, ?> checkpoint, String kind) {
        return new SourceRecord(input.partition().getSourcePartition(), checkpoint, topics.heartbeatTopic() + ".initialization",
                SchemaBuilder.string().name("io.debezium.connector.db2as400.InitializationBarrier").build(), kind);
    }

    @Override
    public void execute(ChangeEventSourceContext context, As400Partition partition, As400OffsetContext previous) throws InterruptedException {
        try (var lease = admission.enter()) {
            check();
            if (previous != offset || !partition.equals(input.partition()) || history == null) {
                throw new IllegalStateException("Streaming requires the coordinator's recovered context");
            }
            try (var output = new As400BatchDispatcher(config, partition, this, topics, headers, Clock.system(), input.runtimeLimits().outputRecords(), true);
                    var active = new As400ContinuousRuntime(offset.getOffset(), history, input, output, this::enqueue,
                            partition.getSourcePartition(), () -> {
                            }, input.runtimeLimits())) {
                output.eventDispatcher().setEventListener(metrics);
                runtime = active;
                if (initialization != null) {
                    synchronized (initializationState) {
                        check();
                        // A concurrent Connect callback cannot observe released
                        // exclusion before the task records successful handoff.
                        initialization.handoff(initializationEpoch, initialCheckpoint, history);
                        handedOff = true;
                    }
                    check();
                }
                while (context.isRunning() && !admission.isStopped()) {
                    check();
                    var saved = durable.get();
                    var progress = active.poll(saved);
                    check();
                    if (metrics instanceof As400StreamingChangeEventSourceMetrics journalMetrics) {
                        progress.scan().ifPresent(journalMetrics::setScanPosition);
                        journalMetrics.setBufferUsage(active.bufferUsage(), active.oldestTransactionTimestamp());
                        if (progress.entries() > 0) {
                            journalMetrics.setLastProcessedMs(System.currentTimeMillis());
                        }
                    }
                    if (!progress.deferred() && active.publishCheckpoint(output, progress.entries() > 0 || !progress.checkpoint().equals(saved))) {
                        SourceRecord barrier = lastEnqueued;
                        if (!barrier.sourceOffset().equals(progress.checkpoint())) {
                            throw new IllegalStateException("Checkpoint barrier differs from completed output");
                        }
                        awaitDurable(barrier);
                        streamingChunkRecords = 0;
                        offset.acknowledged(barrier.sourceOffset());
                        if (metrics instanceof As400StreamingChangeEventSourceMetrics journalMetrics) {
                            journalMetrics.setDurablePositions(barrier.sourceOffset());
                        }
                    }
                    Thread.sleep(Math.max(1, config.getPollInterval().toMillis()));
                }
            }
        }
        catch (InterruptedException e) {
            if (!admission.isStopped() && context.isRunning()) {
                errors.setProducerThrowable(new IllegalStateException("Recovery interrupted outside terminal stop", e));
            }
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (Exception e) {
            if (!admission.isStopped()) {
                errors.setProducerThrowable(e);
            }
        }
        finally {
            runtime = null;
            close();
        }
    }

    private String awaitDurable(SourceRecord barrier) throws Exception {
        long timeout = initialization != null && !handedOff ? Math.min(120_000, initialization.remainingMillis()) : 120_000;
        String receipt = acknowledgements.await(barrier, Duration.ofMillis(timeout), this::checkForPoll);
        check();
        if (!barrier.sourceOffset().equals(durable.get())) {
            throw new IllegalStateException("Commit callback has no matching durable source offset");
        }
        acknowledgements.retireDurableThrough(barrier);
        return receipt;
    }

    private boolean enqueue(SourceRecord record) throws Exception {
        check();
        if (!record.sourcePartition().equals(input.partition().getSourcePartition())) {
            throw new IllegalStateException("Unexpected source partition");
        }
        // Bound retained acknowledgements even inside a large committed source
        // transaction. Its records still carry the previous completed W, so a
        // crash here replays the whole batch. Do not retire without offset readback.
        if (runtime != null && streamingChunkRecords >= 256) {
            awaitDurable(lastEnqueued);
            offset.acknowledged(lastEnqueued.sourceOffset());
            if (metrics instanceof As400StreamingChangeEventSourceMetrics journalMetrics) {
                journalMetrics.setDurablePositions(lastEnqueued.sourceOffset());
            }
            streamingChunkRecords = 0;
        }
        acknowledgements.register(record);
        queue.enqueue(new DataChangeEvent(record));
        check();
        lastEnqueued = record;
        if (runtime != null) {
            streamingChunkRecords++;
        }
        return true;
    }

    private void enqueueChunk(SourceRecord record) throws Exception {
        checkForPoll();
        if (!record.sourcePartition().equals(input.partition().getSourcePartition())) {
            throw new IllegalStateException("Unexpected snapshot source partition");
        }
        acknowledgements.register(record, true);
        queue.enqueue(new DataChangeEvent(record));
        checkForPoll();
        lastEnqueued = record;
    }

    void record(SourceRecord record, RecordMetadata metadata) {
        try {
            synchronized (initializationState) {
                if (acknowledgements.snapshotChunk(record)) {
                    // Only exact records registered by the chunk delivery path
                    // qualify. Receipts are staged until the native-checked commit.
                    checkForPoll();
                }
                else {
                    check();
                }
                acknowledgements.record(record, metadata);
            }
        }
        catch (Exception e) {
            requestStop();
            errors.setProducerThrowable(e);
            throw new IllegalStateException("Recovery record acknowledgement rejected", e);
        }
    }

    void committed() {
        try {
            if (!acknowledgements.hasRecorded()) {
                // EOS/poll calls commit even for empty polls. Do not promote
                // anything here, including a callback racing this observation.
                // A later commit must perform the full native check before it
                // can advance acknowledgement state.
                // Preserve the existing handoff/callback serialization even
                // though this callback cannot advance an acknowledgement.
                synchronized (initializationState) {
                    checkForPoll();
                }
                return;
            }
            synchronized (initializationState) {
                check();
                acknowledgements.committed();
            }
        }
        catch (Exception e) {
            requestStop();
            errors.setProducerThrowable(e);
            throw new IllegalStateException("Recovery commit acknowledgement rejected", e);
        }
    }

    void requestStop() {
        admission.requestStop();
        acknowledgements.stop();
        var active = runtime;
        if (active != null) {
            active.requestStop();
        }
        if (cancelRequested.compareAndSet(false, true)) {
            try {
                input.requestStop();
            }
            catch (Throwable failure) {
                cancellationFailure = failure;
                errors.setProducerThrowable(failure);
            }
            finally {
                cancellation.complete(null);
            }
        }
    }

    boolean stopped() {
        return admission.isStopped();
    }

    /** Check local admission even while the coordinator waits for acknowledgement. */
    void checkForPoll() {
        checkAdmission(false);
    }

    /** Revalidate native exclusion immediately before queued records leave the task. */
    void checkForPublication() {
        checkAdmission(true);
    }

    private void checkAdmission(boolean publication) {
        try {
            if (publication) {
                check();
            }
            else {
                admission.checkOpen();
                input.checkQualified(config.getConfig());
            }
        }
        catch (Exception error) {
            requestStop();
            errors.setProducerThrowable(error);
            throw new IllegalStateException("Recovery publication admission lost", error);
        }
    }

    boolean drained() {
        return cleanup.isDone() && !cleanup.isCompletedExceptionally();
    }

    @Override
    public void close() {
        requestStop();
        admission.awaitDrained();
        cancellation.join();
        if (closed.compareAndSet(false, true)) {
            try {
                input.close();
                if (cancellationFailure != null) {
                    throw new IllegalStateException("Source cancellation failed", cancellationFailure);
                }
                cleanup.complete(null);
            }
            catch (Throwable e) {
                errors.setProducerThrowable(e);
                cleanup.completeExceptionally(e);
                throw new IllegalStateException("Recovery source cleanup failed", e);
            }
        }
        cleanup.join();
    }

    @Override
    public As400OffsetContext getOffsetContext() {
        return offset;
    }

    @Override
    public SnapshotChangeEventSource<As400Partition, As400OffsetContext> getSnapshotChangeEventSource(
                                                                                                      SnapshotProgressListener<As400Partition> listener,
                                                                                                      NotificationService<As400Partition, As400OffsetContext> notifications) {
        return new SnapshotChangeEventSource<>() {
            @Override
            public SnapshottingTask getSnapshottingTask(As400Partition partition, As400OffsetContext previous) {
                if (previous != offset) {
                    throw new IllegalStateException("Coordinator context differs from admitted startup mode");
                }
                return new SnapshottingTask(false, initialization != null, List.of(), Map.of(), false);
            }

            @Override
            public SnapshotResult<As400OffsetContext> execute(ChangeEventSourceContext context, As400Partition partition,
                                                              As400OffsetContext previous, SnapshottingTask task)
                    throws InterruptedException {
                try {
                    if (!context.isRunning()) {
                        throw new InterruptedException("Recovery coordinator stopped");
                    }
                    if (initialization != null) {
                        listener.snapshotStarted(partition);
                        initialize(listener);
                    }
                    prepare();
                    if (initialization != null) {
                        listener.snapshotCompleted(partition);
                        return SnapshotResult.completed(offset);
                    }
                    listener.snapshotSkipped(partition);
                    return SnapshotResult.skipped(offset);
                }
                catch (Throwable e) {
                    if (initialization != null) {
                        listener.snapshotAborted(partition);
                    }
                    close();
                    throw new IllegalStateException("Could not establish initialized bindings", e);
                }
            }

            @Override
            public SnapshottingTask getBlockingSnapshottingTask(As400Partition partition, As400OffsetContext previous, SnapshotConfiguration configuration) {
                throw new IllegalStateException("Recovery admission does not permit new snapshots");
            }
        };
    }

    @Override
    public StreamingChangeEventSource<As400Partition, As400OffsetContext> getStreamingChangeEventSource() {
        return this;
    }

    @Override
    public Optional<IncrementalSnapshotChangeEventSource<As400Partition, ? extends DataCollectionId>> getIncrementalSnapshotChangeEventSource(
                                                                                                                                              As400OffsetContext offset,
                                                                                                                                              SnapshotProgressListener<As400Partition> progress,
                                                                                                                                              DataChangeEventListener<As400Partition> changes,
                                                                                                                                              NotificationService<As400Partition, As400OffsetContext> notifications) {
        return Optional.empty();
    }

    @Override
    public TableSchema schemaFor(TableId id) {
        if (schemas == null) {
            throw new IllegalStateException("Native schemas have not been recovered");
        }
        return schemas.schemaFor(id);
    }

    @Override
    public Collection<TableId> dataCollectionIds() {
        var bindings = history;
        return bindings == null ? List.of() : bindings.definitions().stream().map(d -> d.schema().id()).filter(this::captured).toList();
    }

    @Override
    public boolean tableInformationComplete() {
        return schemas != null;
    }

    @Override
    public boolean isHistorized() {
        return false;
    }
}
