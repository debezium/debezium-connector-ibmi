/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.QueueProviderService;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.metrics.As400ChangeEventSourceMetricsFactory;
import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.document.DocumentReader;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.Strings;

public class As400ConnectorTask extends BaseSourceTask<As400Partition, As400OffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(As400ConnectorTask.class);
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private static final String CONTEXT_NAME = "db2as400-server-connector-task";
    private As400DatabaseSchema schema;
    private ErrorHandler errorHandler;
    private As400RpcConnection rpcConnection;
    private As400JdbcConnection jdbcConnection;
    private As400ConnectorConfig connectorConfig;
    private CdcSourceTaskContext<As400ConnectorConfig> taskContext;
    private volatile As400RecoverySource recoverySource;
    private org.apache.kafka.connect.source.TransactionContext configuredTransactions;

    // Typed lifecycle seams also let local transports exercise the real task.
    final void initializeOutput(ChangeEventQueue<DataChangeEvent> queue, ErrorHandler errorHandler) {
        this.queue = java.util.Objects.requireNonNull(queue);
        this.errorHandler = java.util.Objects.requireNonNull(errorHandler);
    }

    final void initializeConnections(As400RpcConnection rpc, As400JdbcConnection jdbc) {
        this.rpcConnection = rpc;
        this.jdbcConnection = jdbc;
    }

    final void configureTransactions(org.apache.kafka.connect.source.TransactionContext transactions) {
        this.configuredTransactions = transactions;
    }

    final int pendingAcknowledgementCount() {
        var recovery = recoverySource;
        return recovery == null ? 0 : recovery.pendingAcknowledgementCount();
    }

    /** No configuration flag selects this path. Only an in-package test task supplies admission. */
    As400TaskRecovery recoveryForTesting(Configuration config) {
        return null;
    }

    /** Explicit first-start authority is separate from missing-offset recovery. */
    As400TaskInitialization initializationForTesting(Configuration config) {
        return null;
    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {

        connectorConfig = new As400ConnectorConfig(config);
        taskContext = new CdcSourceTaskContext<>(config, connectorConfig, connectorConfig.getCustomMetricTags());

        return taskContext;
    }

    @Override
    protected ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> start(Configuration config) {
        LOGGER.info("starting connector task {}", version());
        if (recoverySource != null && !recoverySource.drained()) {
            throw new org.apache.kafka.connect.errors.ConnectException("Previous recovery session has not drained; replacement admission is unsafe");
        }
        configuredTransactions = null;
        if (As400DcgConfig.configured(config)) {
            try {
                var admission = As400DcgConfig.transactionalTruncateEnabled(config)
                        ? new As400DcgSession.Admission(As400NativeHandoffSession.open(connectorConfig, context, taskContext), null)
                        : As400DcgSession.open(config, context);
                configureTransactions(context.transactionContext());
                return startRecovery(config, admission.recovery(), admission.initialization());
            }
            catch (Exception error) {
                throw new org.apache.kafka.connect.errors.ConnectException("Cannot admit configured native D/CG task", error);
            }
        }
        As400TaskInitialization initialization = initializationForTesting(config);
        if (initialization != null) {
            return startRecovery(config, initialization.recovery(), initialization);
        }
        As400TaskRecovery recovery = recoveryForTesting(config);
        if (recovery != null) {
            return startRecovery(config, recovery, null);
        }
        // TODO resolve schema FIELD_NAME_ADJUSTMENT_MODE to be SchemaNameAdjuster.AVRO_FIELD_NAMER

        @SuppressWarnings("unchecked")
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig
                .getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);

        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        final MainConnectionProvidingConnectionFactory<As400JdbcConnection> jdbcConnectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new As400JdbcConnection(connectorConfig.getJdbcConfig()));
        jdbcConnection = jdbcConnectionFactory.mainConnection();
        registerServiceProviders(connectorConfig.getServiceRegistry());

        CustomConverterRegistry customConverterRegistry = connectorConfig.getServiceRegistry().tryGetService(CustomConverterRegistry.class);

        this.schema = new As400DatabaseSchema(connectorConfig, jdbcConnection, topicNamingStrategy, schemaNameAdjuster, customConverterRegistry, taskContext);

        Offsets<As400Partition, As400OffsetContext> previousOffsetPartition = getPreviousOffsets(
                new As400Partition.Provider(connectorConfig), saved -> {
                    if (saved.containsKey(As400LegacyHandoff.VERSION)) {
                        throw new IllegalArgumentException(
                                "A durable truncate handoff requires transactional truncate recovery; disabling it cannot resume legacy streaming");
                    }
                    return new As400OffsetContext.Loader(connectorConfig).load(saved);
                });
        As400OffsetContext previousOffset = previousOffsetPartition.getTheOnlyOffset();

        // Manual Bean Registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.JDBC_CONNECTION, jdbcConnection);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsetPartition);

        // Service providers

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .queueProvider(connectorConfig.getServiceRegistry().tryGetService(QueueProviderService.class).getQueueProvider())
                .pollInterval(connectorConfig.getPollInterval())
                .pollDispatchInterval(connectorConfig.getPollDispatchInterval())
                .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME)).build();

        errorHandler = new As400ErrorHandler(connectorConfig, queue);

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        final As400EventMetadataProvider metadataProvider = new As400EventMetadataProvider();

        final As400TaskContext taskContext = new As400TaskContext(config,
                connectorConfig,
                connectorConfig.getCustomMetricTags());
        final As400ConnectorConfig newConfig = taskContext.getConfig();

        final As400StreamingChangeEventSourceMetrics streamingMetrics = new As400StreamingChangeEventSourceMetrics(
                taskContext, queue, metadataProvider, schema::tableIds);

        String configuredIncludes = newConfig.tableIncludeList();
        String signalDataCollection = config.getString(RelationalDatabaseConnectorConfig.SIGNAL_DATA_COLLECTION);
        if (!Strings.isNullOrBlank(signalDataCollection)) {
            configuredIncludes = configuredIncludes.length() > 0 ? String.format("%s,%s", configuredIncludes, signalDataCollection) : "";
        }

        final List<FileFilter> shortIncludes = jdbcConnection.shortIncludes(schema.getSchemaName(),
                configuredIncludes);

        final long cacheWait = JournalInfoRetrieval.getJournalCacheDurationInMilliseconds(jdbcConnection);

        rpcConnection = new As400RpcConnection(connectorConfig, streamingMetrics,
                shortIncludes, cacheWait);

        validateSchemaHistory(connectorConfig, rpcConnection::validateLogPosition, previousOffsetPartition, schema,
                snapshotterService.getSnapshotter());

        As400ConnectorConfig snapshotConnectorConfig = connectorConfig;

        final SignalProcessor<As400Partition, As400OffsetContext> signalProcessor = new SignalProcessor<>(
                As400RpcConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsetPartition);

        final EventDispatcher<As400Partition, TableId> dispatcher = new As400EventDispatcher(connectorConfig, // CommonConnectorConfig
                topicNamingStrategy, // TopicSelector
                schema, // DatabaseSchema
                queue, // ChangeEventQueue
                newConfig.getTableFilters().dataCollectionFilter(), // DataCollectionFilter
                DataChangeEvent::new, // ! ChangeEventCreator
                metadataProvider,
                schemaNameAdjuster,
                signalProcessor,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        final Clock clock = Clock.system();

        final As400ChangeEventSourceFactory changeFactory = new As400ChangeEventSourceFactory(newConfig, snapshotConnectorConfig, rpcConnection,
                jdbcConnectionFactory, errorHandler, dispatcher, clock, schema, snapshotterService);

        final NotificationService<As400Partition, As400OffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        final ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsetPartition, errorHandler, As400RpcConnector.class, newConfig, changeFactory,
                new As400ChangeEventSourceMetricsFactory(streamingMetrics), dispatcher, schema,
                signalProcessor, notificationService, snapshotterService);
        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    private ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> startRecovery(Configuration raw, As400TaskRecovery input,
                                                                                           As400TaskInitialization initialization) {
        recoverySource = null;
        try {
            if (configuredTransactions == null && !input.sourceOffsetAcknowledgements()
                    && input.allowLegacyHandoffRecovery() && "connector".equals(raw.getString("transaction.boundary"))) {
                As400RecoveryAcknowledgements.requireConnectorProfile(raw.asMap());
                configuredTransactions = java.util.Objects.requireNonNull(context.transactionContext(), "Connect transaction context required");
            }
            As400DcgConfig.requireSnapshotMode(raw, input.allowLegacyHandoffRecovery());
            As400BatchDispatcher.requireConfiguration(connectorConfig);
            if (input.sourceOffsetAcknowledgements()) {
                As400RecoveryAcknowledgements.requireSourceOffsetProfile(raw.asMap());
                if (configuredTransactions != null) {
                    throw new IllegalArgumentException("Conflicting recovery acknowledgement contracts");
                }
            }
            else if (configuredTransactions == null) {
                As400RecoveryAcknowledgements.requireProfile(raw.asMap());
            }
            input.checkQualified(raw);
            if (!input.allowLegacyHandoffRecovery()
                    && (!connectorConfig.getSignalingDataCollectionIds().isEmpty() || !raw.getString("signal.enabled.channels", "").isEmpty())) {
                throw new IllegalArgumentException("Recovery-only admission does not permit snapshot signals");
            }
            var legacyStart = new java.util.concurrent.atomic.AtomicReference<Map<String, ?>>();
            Offsets<As400Partition, As400OffsetContext> offsets = getPreviousOffsets(() -> Set.of(input.partition()),
                    saved -> {
                        saved = As400DeliveryOffset.strip(saved);
                        saved = As400FullSnapshotState.base(saved);
                        if (initialization != null) {
                            throw new IllegalArgumentException("Explicit first start requires absent offsets; pending initialization cannot resume");
                        }
                        if (input instanceof As400TaskHandoff && input.allowLegacyHandoffRecovery() && !saved.containsKey(As400Checkpoint.VERSION)) {
                            if (configuredTransactions == null && !input.sourceOffsetAcknowledgements()) {
                                throw new IllegalArgumentException("Legacy handoff requires an admitted durable acknowledgement contract");
                            }
                            if (!((As400TaskHandoff) input).snapshotRequired()) {
                                As400LegacyHandoff.startingPosition(As400LegacyHandoff.strings(saved));
                            }
                            legacyStart.set(Map.copyOf(saved));
                            return new As400OffsetContext.Loader(connectorConfig).load(saved);
                        }
                        return new As400RecoveryOffsetContext(connectorConfig, saved, input.journalIdentity(), input.fingerprint());
                    });
            As400OffsetContext previous = offsets.getTheOnlyOffset();
            As400RecoveryOffsetContext recovered = previous instanceof As400RecoveryOffsetContext saved ? saved : null;
            if (previous == null && initialization == null && !(input instanceof As400TaskHandoff handoff && handoff.snapshotRequired())) {
                throw new IllegalArgumentException("Missing durable F/W: explicit initialization is required");
            }
            registerServiceProviders(connectorConfig.getServiceRegistry());
            connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, offsets);
            queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .maxBatchSize(connectorConfig.getMaxBatchSize()).maxQueueSize(connectorConfig.getMaxQueueSize())
                    .queueProvider(connectorConfig.getServiceRegistry().tryGetService(QueueProviderService.class).getQueueProvider())
                    .pollInterval(connectorConfig.getPollInterval()).pollDispatchInterval(connectorConfig.getPollDispatchInterval())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME)).build();
            errorHandler = new As400RecoveryErrorHandler(connectorConfig, queue, errorHandler, input.supportsTaskRestart());
            TopicNamingStrategy<TableId> topics = connectorConfig.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
            var headers = connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class);
            var source = new As400RecoverySource(input, initialization, connectorConfig, recovered, queue, errorHandler, topics, headers,
                    () -> context.offsetStorageReader().offset(input.partition().getSourcePartition()), recovered == null ? previous : null, legacyStart.get());
            recoverySource = source;
            source.notifications(getNotificationChannels());
            var signals = connectorConfig.getSignalingDataCollectionIds().isEmpty() ? null
                    : new As400BufferedSignals(connectorConfig, getAvailableSignalChannels(), offsets);
            source.signals(signals, taskContext);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, source);
            var metadata = new As400EventMetadataProvider();
            var metricContext = new As400TaskContext(raw, connectorConfig, connectorConfig.getCustomMetricTags());
            var streamingMetrics = new As400StreamingChangeEventSourceMetrics(metricContext, queue, metadata, source::dataCollectionIds);
            source.metrics(streamingMetrics);
            var dispatcher = new EventDispatcher<As400Partition, TableId>(connectorConfig, topics, source, queue, id -> true,
                    DataChangeEvent::new, metadata, connectorConfig.schemaNameAdjuster(), signals, headers);
            var snapshotter = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);
            var coordinator = new ChangeEventSourceCoordinator<As400Partition, As400OffsetContext>(offsets, errorHandler,
                    As400RpcConnector.class, connectorConfig, source, new As400ChangeEventSourceMetricsFactory(streamingMetrics),
                    dispatcher, source, signals, null, snapshotter) {
                {
                    source.signalRegistration(selected -> registerSignalActionsAndStartProcessor(signals, selected, this, connectorConfig));
                }

                @Override
                protected void streamEvents(io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext context,
                                            As400Partition partition, As400OffsetContext offset)
                        throws InterruptedException {
                    if (!snapshotter.getSnapshotter().shouldStream()) {
                        // Do not initialize snapshot watermarks or the native
                        // reader for an explicitly snapshot-only task.
                        source.close();
                        return;
                    }
                    super.streamEvents(context, partition, offset);
                }

                @Override
                public synchronized void stop() throws InterruptedException {
                    // BaseSourceTask stops its coordinator before doStop. Close
                    // admission/cancel now, before waiting for its executor.
                    source.requestStop();
                    try {
                        super.stop();
                    }
                    finally {
                        source.close();
                    }
                }
            };
            coordinator.start(metricContext, queue, metadata);
            return coordinator;
        }
        catch (RuntimeException | Error failure) {
            try {
                if (recoverySource != null) {
                    recoverySource.close();
                }
                else {
                    try {
                        input.requestStop();
                    }
                    finally {
                        input.close();
                    }
                }
            }
            catch (Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            try {
                connectorConfig.getServiceRegistry().close();
            }
            catch (Throwable cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        var recovery = recoverySource;
        if (recovery != null) {
            recovery.record(record, metadata);
        }
        super.commitRecord(record, metadata);
    }

    @Override
    public void commit() throws InterruptedException {
        super.commit();
        var recovery = recoverySource;
        if (recovery != null) {
            recovery.committed();
        }
    }

    boolean recoveryDrained() {
        return recoverySource != null && recoverySource.drained();
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        var recovery = recoverySource;
        // Even a stopped source must surface its producer failure through poll.
        if (recovery != null && !recovery.stopped()) {
            recovery.checkForPoll();
        }
        final List<DataChangeEvent> records = queue.poll();

        if (recovery != null && recovery.stopped()) {
            return List.of();
        }

        if (recovery != null) {
            if (records.isEmpty()) {
                recovery.checkForPoll();
            }
            else {
                checkRecoveryPublication();
            }
        }

        final List<SourceRecord> sourceRecords = records.stream().map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        if (configuredTransactions != null && !sourceRecords.isEmpty() && (recovery == null || recovery.shouldCommitPoll(sourceRecords))) {
            // Public Connect transaction request: commit this poll's records and
            // their conservative offsets together. This is not source exactly once.
            configuredTransactions.commitTransaction();
        }
        return sourceRecords;
    }

    // Also used when the native test adapter releases a deliberately held poll.
    final void checkRecoveryPublication() {
        var recovery = recoverySource;
        if (recovery != null) {
            recovery.checkForPublication();
        }
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    protected void doStop() {
        // Request terminal admission before closing JDBC. Closing must start
        // before the RPC lease wait because that lease can cover JDBC metadata.
        if (rpcConnection != null) {
            rpcConnection.requestStop();
        }
        Thread jdbcCloser = startJdbcClose();
        long deadline = System.nanoTime() + java.time.Duration.ofSeconds(30).toNanos();
        try {
            if (recoverySource != null) {
                recoverySource.close();
            }
            if (rpcConnection != null) {
                rpcConnection.stop();
            }
        }
        finally {
            awaitJdbcClose(jdbcCloser, deadline);
            // An admitted connect may have completed after the first close.
            // Once the RPC/JDBC retry lease drains, close that handle too.
            if (jdbcCloser != null && !jdbcCloser.isAlive()) {
                awaitJdbcClose(startJdbcClose(), deadline);
            }
        }
    }

    private Thread startJdbcClose() {
        if (jdbcConnection == null) {
            return null;
        }
        var closing = jdbcConnection;
        Thread thread = new Thread(() -> {
            try {
                closing.close();
            }
            catch (java.sql.SQLException e) {
                LOGGER.warn("Failed to close task JDBC connection", e);
            }
        }, "ibmi-jdbc-stop");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private void awaitJdbcClose(Thread closing, long deadline) {
        if (closing == null) {
            return;
        }
        try {
            long remaining = deadline - System.nanoTime();
            if (remaining > 0) {
                closing.join(Math.max(1, remaining / 1_000_000));
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (closing.isAlive()) {
            LOGGER.warn("Task JDBC close is still pending after the stop deadline; cleanup is not verified");
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return As400ConnectorConfig.ALL_FIELDS;
    }
}
