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

/** Real coordinator source/factory for recovery, acknowledged snapshot initialization or explicitly admitted legacy handoff. */
final class As400RecoverySource implements StreamingChangeEventSource<As400Partition, As400OffsetContext>,
        ChangeEventSourceFactory<As400Partition, As400OffsetContext>, DatabaseSchema<TableId> {
    private final As400TaskRecovery input;
    private final As400ConnectorConfig config;
    private volatile As400RecoveryOffsetContext offset;
    private volatile As400OffsetContext snapshotOnlyContext;
    private final As400TaskInitialization initialization;
    private final As400OffsetContext legacy;
    private final Map<String, ?> legacyOffset;
    private final As400OffsetContext startupContext;
    private boolean ordinarySnapshot;
    private volatile boolean handoffInProgress;
    private volatile SourceRecord handoffCommitBoundary;
    private final Object initializationState = new Object();
    private volatile String initializationEpoch;
    private volatile boolean handedOff;
    private As400Checkpoint initialCheckpoint;
    private final ChangeEventQueue<DataChangeEvent> queue;
    private final ErrorHandler errors;
    private final TopicNamingStrategy<TableId> topics;
    private final DebeziumHeaderProducer headers;
    private final Supplier<Map<String, ?>> durable;
    private final Supplier<Map<String, ?>> deliveryOffset;
    private final JournalReadLifecycle admission = new JournalReadLifecycle();
    private final As400RecoveryAcknowledgements acknowledgements;
    private final AtomicBoolean cancelRequested = new AtomicBoolean();
    private final As400AsyncCleanup cleanup = new As400AsyncCleanup("ibmi-source-close");
    private final CompletableFuture<Void> cancellation = new CompletableFuture<>();
    private volatile Throwable cancellationFailure;
    private volatile As400ContinuousRuntime runtime;
    private volatile As400NativeTableSchemas schemas;
    private volatile As400BindingHistory.Recovered history;
    private StreamingChangeEventSourceMetrics<As400Partition> metrics;
    private SourceRecord lastEnqueued;
    private int streamingChunkRecords;
    private List<io.debezium.pipeline.notification.channels.NotificationChannel> notificationChannels = List.of();
    private As400BufferedSignals signals;
    private io.debezium.connector.common.CdcSourceTaskContext<As400ConnectorConfig> taskContext;
    private volatile As400BatchDispatcher preparedOutput;
    private NotificationService<As400Partition, As400OffsetContext> incrementalNotifications;
    private java.util.function.Consumer<io.debezium.pipeline.EventDispatcher<As400Partition, TableId>> signalRegistration;

    void signalRegistration(java.util.function.Consumer<io.debezium.pipeline.EventDispatcher<As400Partition, TableId>> register) {
        signalRegistration = register;
    }

    void signals(As400BufferedSignals processor, io.debezium.connector.common.CdcSourceTaskContext<As400ConnectorConfig> context) {
        signals = processor;
        taskContext = context;
    }

    void notifications(List<io.debezium.pipeline.notification.channels.NotificationChannel> channels) {
        notificationChannels = List.copyOf(channels);
    }

    As400RecoverySource(As400TaskRecovery input, As400TaskInitialization initialization, As400ConnectorConfig config, As400RecoveryOffsetContext offset,
                        ChangeEventQueue<DataChangeEvent> queue, ErrorHandler errors, TopicNamingStrategy<TableId> topics,
                        DebeziumHeaderProducer headers, Supplier<Map<String, ?>> durable) {
        this(input, initialization, config, offset, queue, errors, topics, headers, durable, null, null);
    }

    As400RecoverySource(As400TaskRecovery input, As400TaskInitialization initialization, As400ConnectorConfig config, As400RecoveryOffsetContext offset,
                        ChangeEventQueue<DataChangeEvent> queue, ErrorHandler errors, TopicNamingStrategy<TableId> topics,
                        DebeziumHeaderProducer headers, Supplier<Map<String, ?>> durable, As400OffsetContext legacy, Map<String, ?> legacyOffset) {
        if (legacy != null && (initialization != null || offset != null || !(input instanceof As400TaskHandoff) || !input.allowLegacyHandoffRecovery())) {
            throw new IllegalArgumentException("Conflicting startup origins");
        }
        this.legacy = legacy;
        this.legacyOffset = legacy == null ? null : Map.copyOf(legacyOffset);
        this.startupContext = legacy == null ? offset : legacy;
        this.ordinarySnapshot = input instanceof As400TaskHandoff handoff && handoff.snapshotRequired();
        if (ordinarySnapshot && (initialization != null || offset != null)) {
            throw new IllegalArgumentException("Ordinary first start cannot replace native initialization or recovery");
        }
        this.handoffInProgress = legacy != null || ordinarySnapshot;
        this.input = input;
        this.initialization = initialization;
        this.config = config;
        this.offset = offset;
        this.queue = queue;
        this.errors = errors;
        this.topics = topics;
        this.headers = headers;
        this.deliveryOffset = durable;
        this.durable = () -> As400DeliveryOffset.strip(durable.get());
        var routing = As400TopicRouting.from(config.getConfig().asMap());
        this.acknowledgements = input.sourceOffsetAcknowledgements()
                ? new As400RecoveryAcknowledgements(durable.get(), routing)
                : new As400RecoveryAcknowledgements(routing);
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

    boolean shouldCommitPoll(List<SourceRecord> records) {
        return !handoffInProgress || records.stream().anyMatch(record -> acknowledgements.isDeliveryOf(record, handoffCommitBoundary));
    }

    private void initializeHandoff(ChangeEventSourceContext context, SnapshotProgressListener<As400Partition> listener) throws Exception {
        try (var lease = admission.enter()) {
            check();
            // Preserve the exact serialized legacy barrier, including handoff
            // fields which the ordinary legacy OffsetContext intentionally omits.
            Map<String, ?> saved = durable.get();
            if (!java.util.Objects.equals(legacyOffset, saved)) {
                throw new IllegalStateException("Legacy handoff lost its starting offset");
            }
            var output = new As400TaskHandoff.Output() {
                @Override
                public void emit(SourceRecord record) throws Exception {
                    enqueue(record);
                }

                @Override
                public void barrier(SourceRecord record) throws Exception {
                    check();
                    if (handoffCommitBoundary != null) {
                        throw new IllegalStateException("Previous handoff boundary is unacknowledged");
                    }
                    handoffCommitBoundary = record;
                    enqueue(record);
                }

                @Override
                public String acknowledge(SourceRecord barrier) throws Exception {
                    check();
                    if (barrier != handoffCommitBoundary) {
                        throw new IllegalStateException("Unregistered handoff boundary");
                    }
                    String receipt = awaitDurable(barrier);
                    handoffCommitBoundary = null;
                    return receipt;
                }
            };
            var handoff = (As400TaskHandoff) input;
            if (ordinarySnapshot) {
                saved = handoff.snapshot(context, legacy, listener, notificationChannels, output);
                check();
                if (!saved.equals(durable.get())) {
                    throw new IllegalStateException("Ordinary snapshot must finish durably before online handoff");
                }
            }
            if (snapshotOnly()) {
                snapshotOnlyContext = new As400OffsetContext.Loader(config).load(saved);
                return; // Snapshot-only mode cannot acquire or publish a migration boundary.
            }
            @SuppressWarnings("unchecked")
            var changes = listener instanceof DataChangeEventListener<?> data ? (DataChangeEventListener<As400Partition>) data
                    : DataChangeEventListener.<As400Partition> NO_OP();
            var handoffSignals = signals == null ? null : new As400TaskHandoff.Signals(signals, signalRegistration, listener, changes, notificationChannels);
            var initial = handoff.handoff(saved, output, handoffSignals);
            check();
            if (!initial.toOffset().equals(durable.get())) {
                throw new IllegalStateException("Handoff did not install its exact durable F/W");
            }
            offset = new As400RecoveryOffsetContext(config, initial.toOffset(), input.journalIdentity(), input.fingerprint());
            handoffInProgress = false;
        }
    }

    private void prepare() throws Exception {
        try (var lease = admission.enter()) {
            check();
            if (!offset.getOffset().equals(As400FullSnapshotState.base(durable.get()))) {
                throw new IllegalStateException("Durable F/W changed before history recovery");
            }
            var recovered = input.recover(offset.getOffset());
            check();
            if (!recovered.journalIdentity().equals(input.journalIdentity()) || !recovered.fingerprint().equals(input.fingerprint())
                    || !offset.getOffset().equals(As400FullSnapshotState.base(durable.get()))) {
                throw new IllegalStateException("Recovery requires matching initialized history and durable F/W");
            }
            recovered.requireStartupOrigin(offset.getOffset(), input.allowLegacyHandoffRecovery());
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
            try (var output = preparedOutput == null
                    ? new As400BatchDispatcher(config, partition, this, topics, headers, Clock.system(), input.runtimeLimits().outputRecords(), true)
                    : preparedOutput;
                    var active = new As400ContinuousRuntime(offset.getOffset(), history, input, output, this::enqueue,
                            partition.getSourcePartition(), () -> {
                            }, input.runtimeLimits(), definition -> {
                                output.schemaChanged(definition);
                                schemas.refresh(definition);
                            })) {
                output.eventDispatcher().setEventListener(metrics);
                runtime = active;
                boolean asynchronous = input.sourceOffsetAcknowledgements();
                if (asynchronous) {
                    acknowledgements.startStreaming();
                }
                Map<String, ?> published = offset.getOffset();
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
                    var saved = asynchronous ? acknowledgements.committedOffset() : durable.get();
                    var progress = asynchronous ? active.poll(published, saved) : active.poll(saved);
                    check();
                    if (metrics instanceof As400StreamingChangeEventSourceMetrics journalMetrics) {
                        progress.scan().ifPresent(journalMetrics::setScanPosition);
                        journalMetrics.setBufferUsage(active.bufferUsage(), active.oldestTransactionTimestamp());
                        if (progress.entries() > 0) {
                            journalMetrics.setLastProcessedMs(System.currentTimeMillis());
                        }
                    }
                    if (!progress.deferred() && active.publishCheckpoint(output, progress.entries() > 0 || !progress.checkpoint().equals(published))) {
                        SourceRecord barrier = lastEnqueued;
                        if (!barrier.sourceOffset().equals(progress.checkpoint())) {
                            throw new IllegalStateException("Checkpoint barrier differs from completed output");
                        }
                        if (!asynchronous) {
                            awaitDurable(barrier);
                            streamingChunkRecords = 0;
                            offset.acknowledged(barrier.sourceOffset());
                            if (metrics instanceof As400StreamingChangeEventSourceMetrics journalMetrics) {
                                journalMetrics.setDurablePositions(barrier.sourceOffset());
                            }
                        }
                    }
                    published = progress.checkpoint();
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
        String receipt = initialization != null && !handedOff
                ? acknowledgements.await(barrier, Duration.ofMillis(Math.min(120_000, initialization.remainingMillis())), this::checkForPoll)
                : acknowledgements.awaitUntilStopped(barrier, this::checkForPoll);
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
        // Bound retained records even during a large source transaction. In the
        // source-offset contract delivery releases memory, never the durable F/W.
        // A crash before offset flush replays from the saved checkpoint.
        if (runtime != null && streamingChunkRecords >= 256) {
            if (input.sourceOffsetAcknowledgements()) {
                acknowledgements.awaitDeliveredAndRetire(lastEnqueued, this::checkForPoll);
            }
            else {
                awaitDurable(lastEnqueued);
                offset.acknowledged(lastEnqueued.sourceOffset());
                if (metrics instanceof As400StreamingChangeEventSourceMetrics journalMetrics) {
                    journalMetrics.setDurablePositions(lastEnqueued.sourceOffset());
                }
            }
            streamingChunkRecords = 0;
        }
        queue.enqueue(new DataChangeEvent(acknowledgements.register(record)));
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
        queue.enqueue(new DataChangeEvent(acknowledgements.register(record, true)));
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
                acknowledgements.committed(deliveryOffset.get());
                if (runtime != null && input.sourceOffsetAcknowledgements()) {
                    var saved = acknowledgements.committedOffset();
                    offset.acknowledged(saved);
                    if (metrics instanceof As400StreamingChangeEventSourceMetrics journalMetrics) {
                        journalMetrics.setDurablePositions(saved);
                    }
                }
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

    int pendingAcknowledgementCount() {
        return acknowledgements.pendingCount();
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
        return cleanup.completed();
    }

    @Override
    public void close() {
        closeWithin(As400AsyncCleanup.STOP_TIMEOUT);
    }

    void closeWithin(Duration timeout) {
        requestStop();
        cleanup.start(() -> {
            try {
                admission.awaitDrained();
                cancellation.join();
                input.close();
                if (preparedOutput != null) {
                    preparedOutput.close();
                }
                if (incrementalNotifications != null) {
                    incrementalNotifications.stop();
                }
                if (cancellationFailure != null) {
                    throw new IllegalStateException("Source cancellation failed", cancellationFailure);
                }
            }
            catch (Throwable failure) {
                errors.setProducerThrowable(failure);
                throw failure;
            }
        });
        cleanup.await(timeout);
    }

    @Override
    public As400OffsetContext getOffsetContext() {
        return snapshotOnlyContext == null ? offset : snapshotOnlyContext;
    }

    private boolean snapshotOnly() {
        return !config.getServiceRegistry().getService(io.debezium.snapshot.SnapshotterService.class).getSnapshotter().shouldStream();
    }

    private void resnapshot(ChangeEventSourceContext context, SnapshotProgressListener<As400Partition> listener) throws Exception {
        try (var lease = admission.enter()) {
            check();
            var completed = input.fullSnapshot(As400Checkpoint.fromOffset(offset.getOffset(), input.journalIdentity(), input.fingerprint()),
                    history, context, listener, notificationChannels, new As400TaskHandoff.Output() {
                        @Override
                        public void emit(SourceRecord record) throws Exception {
                            enqueue(record);
                        }

                        @Override
                        public void barrier(SourceRecord record) throws Exception {
                            enqueue(record);
                        }

                        @Override
                        public String acknowledge(SourceRecord record) throws Exception {
                            return awaitDurable(record);
                        }
                    }, As400FullSnapshotState.pending(durable.get()));
            if (!completed.toOffset().equals(durable.get())) {
                throw new IllegalStateException("Requested snapshot has no exact durable completion checkpoint");
            }
            offset.acknowledged(completed.toOffset());
            prepare();
        }
    }

    @Override
    public SnapshotChangeEventSource<As400Partition, As400OffsetContext> getSnapshotChangeEventSource(
                                                                                                      SnapshotProgressListener<As400Partition> listener,
                                                                                                      NotificationService<As400Partition, As400OffsetContext> notifications) {
        return new SnapshotChangeEventSource<>() {
            @Override
            public SnapshottingTask getSnapshottingTask(As400Partition partition, As400OffsetContext previous) {
                if (previous != startupContext) {
                    throw new IllegalStateException("Coordinator context differs from admitted startup mode");
                }
                var snapshotter = config.getServiceRegistry().getService(io.debezium.snapshot.SnapshotterService.class).getSnapshotter();
                boolean pending = As400FullSnapshotState.pending(durable.get());
                boolean requested = input.allowLegacyHandoffRecovery() && snapshotter.shouldSnapshotData(previous != null,
                        pending || previous != null && !(previous instanceof As400RecoveryOffsetContext) && !previous.isSnapshotComplete());
                if (pending && !requested) {
                    throw new IllegalArgumentException("Cannot skip an interrupted requested snapshot");
                }
                ordinarySnapshot |= legacy != null && requested;
                return new SnapshottingTask(ordinarySnapshot || requested, initialization != null || requested,
                        List.of(), Map.of(), false);
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
                    if (legacy != null || ordinarySnapshot) {
                        initializeHandoff(context, listener);
                    }
                    if (snapshotOnlyContext != null) {
                        if (ordinarySnapshot) {
                            return SnapshotResult.completed(snapshotOnlyContext);
                        }
                        listener.snapshotSkipped(partition);
                        return SnapshotResult.skipped(snapshotOnlyContext);
                    }
                    prepare();
                    if (startupContext instanceof As400RecoveryOffsetContext && task.snapshotData() && input.allowLegacyHandoffRecovery()) {
                        resnapshot(context, listener);
                        return SnapshotResult.completed(offset);
                    }
                    if (initialization != null) {
                        listener.snapshotCompleted(partition);
                        return SnapshotResult.completed(offset);
                    }
                    if (ordinarySnapshot) {
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
                    throw new IllegalStateException("Could not establish acknowledged startup bindings", e);
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
        if (config.getSignalingDataCollectionIds().isEmpty()) {
            if (!As400IncrementalSnapshotState.fields(offset.getOffset()).isEmpty()) {
                throw new IllegalArgumentException("An unfinished incremental snapshot requires its signaling configuration");
            }
            return Optional.empty();
        }
        try {
            check();
            if (signals == null || preparedOutput != null || history == null) {
                throw new IllegalStateException("Incremental snapshot requires recovered bindings and one signal processor");
            }
            requireSignalingBindings(config, history.definitions());
            preparedOutput = new As400BatchDispatcher(config, input.partition(), this, topics, headers, Clock.system(),
                    input.runtimeLimits().outputRecords(), true, signals);
            var connection = input.incrementalSnapshotConnection(config);
            var snapshotSchemas = new As400DatabaseSchema(config, connection, topics, config.schemaNameAdjuster(),
                    As400NativeTableSchemas.converterRegistry(config), taskContext);
            history.definitions().stream().filter(definition -> !definition.excluded()).forEach(definition -> snapshotSchemas.forwardSchema(definition.outputTable()));
            incrementalNotifications = new NotificationService<>(notificationChannels, config, io.debezium.schema.SchemaFactory.get(),
                    preparedOutput.eventDispatcher()::enqueueNotification);
            var snapshot = new As400BufferedIncrementalSnapshot(config, connection, preparedOutput.eventDispatcher(), snapshotSchemas,
                    Clock.system(), progress, changes, incrementalNotifications);
            preparedOutput.incrementalSnapshot(snapshot, As400Checkpoint.fromOffset(offset.getOffset(), input.journalIdentity(), input.fingerprint()));
            return Optional.of(snapshot);
        }
        catch (Exception failure) {
            close();
            throw new IllegalStateException("Could not initialize buffered incremental snapshot", failure);
        }
    }

    static void requireSignalingBindings(As400ConnectorConfig config, List<As400BindingDefinition> definitions) {
        for (String signal : config.getSignalingDataCollectionIds()) {
            if (definitions.stream().noneMatch(definition -> !definition.excluded() && definition.schema().id().toString().equals(signal))) {
                throw new IllegalArgumentException("Incremental snapshot signal table has no recovered journal binding: " + signal);
            }
        }
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
