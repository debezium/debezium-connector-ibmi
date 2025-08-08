/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.DefaultQueueProvider;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.common.DebeziumHeaderProducerProvider;
import io.debezium.connector.db2as400.metrics.As400ChangeEventSourceMetricsFactory;
import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.converters.custom.CustomConverterServiceProvider;
import io.debezium.document.DocumentReader;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.processors.PostProcessorRegistryServiceProvider;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.service.spi.ServiceRegistry;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

public class As400ConnectorTask extends BaseSourceTask<As400Partition, As400OffsetContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(As400ConnectorTask.class);
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private static final String CONTEXT_NAME = "db2as400-server-connector-task";
    private As400DatabaseSchema schema;
    private ErrorHandler errorHandler;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    protected ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> start(Configuration config) {
        LOGGER.info("starting connector task {}", version());
        // TODO resolve schema FIELD_NAME_ADJUSTMENT_MODE to be SchemaNameAdjuster.AVRO_FIELD_NAMER
        final As400ConnectorConfig connectorConfig = new As400ConnectorConfig(config);
        @SuppressWarnings("unchecked")
        final TopicNamingStrategy<TableId> topicNamingStrategy = connectorConfig
                .getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);

        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        final MainConnectionProvidingConnectionFactory<As400JdbcConnection> jdbcConnectionFactory = new DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new As400JdbcConnection(connectorConfig.getJdbcConfig()));
        final As400JdbcConnection jdbcConnection = jdbcConnectionFactory.mainConnection();
        registerServiceProviders(connectorConfig.getServiceRegistry());

        CustomConverterRegistry customConverterRegistry = connectorConfig.getServiceRegistry().tryGetService(CustomConverterRegistry.class);

        this.schema = new As400DatabaseSchema(connectorConfig, jdbcConnection, topicNamingStrategy, schemaNameAdjuster, customConverterRegistry);

        final CdcSourceTaskContext ctx = new CdcSourceTaskContext(connectorConfig, connectorConfig.getCustomMetricTags(), schema::tableIds);

        // Manual Bean Registration
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, config);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
        connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, ctx);

        // Service providers

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .queueProvider(new DefaultQueueProvider<>(connectorConfig.getMaxQueueSize()))
                .loggingContextSupplier(() -> ctx.configureLoggingContext(CONTEXT_NAME)).build();

        errorHandler = new ErrorHandler(As400RpcConnector.class, connectorConfig, queue, null);

        Offsets<As400Partition, As400OffsetContext> previousOffsetPartition = getPreviousOffsets(
                new As400Partition.Provider(connectorConfig), new As400OffsetContext.Loader(connectorConfig));
        As400OffsetContext previousOffset = previousOffsetPartition.getTheOnlyOffset();
        if (previousOffset == null) {
            LOGGER.info("previous offsets not found creating from config");
            previousOffset = new As400OffsetContext(connectorConfig);
            previousOffsetPartition = Offsets.of(new As400Partition(connectorConfig.getLogicalName()),
                    previousOffset);
        }

        final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

        final As400EventMetadataProvider metadataProvider = new As400EventMetadataProvider();

        final As400TaskContext taskContext = new As400TaskContext(connectorConfig, schema,
                connectorConfig.getCustomMetricTags());
        final As400ConnectorConfig newConfig = taskContext.getConfig();

        final As400StreamingChangeEventSourceMetrics streamingMetrics = new As400StreamingChangeEventSourceMetrics(
                taskContext, queue, metadataProvider);

        final List<FileFilter> shortIncludes = jdbcConnection.shortIncludes(schema.getSchemaName(),
                newConfig.tableIncludeList());

        final As400RpcConnection rpcConnection = new As400RpcConnection(connectorConfig, streamingMetrics,
                shortIncludes);

        As400ConnectorConfig snapshotConnectorConfig = connectorConfig;
        final Set<String> additionalTables = additionalTablesInConfigTables(connectorConfig, previousOffset, newConfig);
        if (!additionalTables.isEmpty()) {
            final String newIncludes = String.join(",", additionalTables);
            LOGGER.info("found new tables to stream {}", newIncludes);

            snapshotConnectorConfig = new As400ConnectorConfig(config, newIncludes);
            previousOffset.hasNewTables(true);
        }
        else {
            LOGGER.info("no new tables to stream");
        }

        final EventDispatcher<As400Partition, TableId> dispatcher = new EventDispatcher<>(connectorConfig, // CommonConnectorConfig
                topicNamingStrategy, // TopicSelector
                schema, // DatabaseSchema
                queue, // ChangeEventQueue
                newConfig.getTableFilters().dataCollectionFilter(), // DataCollectionFilter
                DataChangeEvent::new, // ! ChangeEventCreator
                metadataProvider,
                schemaNameAdjuster,
                connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

        final Clock clock = Clock.system();

        final As400ChangeEventSourceFactory changeFactory = new As400ChangeEventSourceFactory(newConfig, snapshotConnectorConfig, rpcConnection,
                jdbcConnectionFactory, errorHandler, dispatcher, clock, schema, snapshotterService);

        final SignalProcessor<As400Partition, As400OffsetContext> signalProcessor = new SignalProcessor<>(
                As400RpcConnector.class, connectorConfig, Map.of(),
                getAvailableSignalChannels(),
                DocumentReader.defaultReader(),
                previousOffsetPartition);

        final NotificationService<As400Partition, As400OffsetContext> notificationService = new NotificationService<>(getNotificationChannels(),
                connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

        final ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                previousOffsetPartition, errorHandler, As400RpcConnector.class, newConfig, changeFactory,
                new As400ChangeEventSourceMetricsFactory(streamingMetrics), dispatcher, schema,
                signalProcessor, notificationService, snapshotterService);
        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    private Set<String> additionalTablesInConfigTables(final As400ConnectorConfig connectorConfig,
                                                       As400OffsetContext previousOffset, As400ConnectorConfig newConfig) {
        final String newInclude = newConfig.tableIncludeList();
        final String oldInclude = previousOffset.getIncludeTables();
        LOGGER.info("previous includes {} , new includes {}", oldInclude, newInclude);

        if (oldInclude != null && newInclude != null) {
            final Set<String> newset = Stream.of(newInclude.split(",", -1))
                    .collect(Collectors.toCollection(HashSet::new));
            newset.removeAll(Set.of(oldInclude.split(",")));
            return newset;
        }
        return Collections.emptySet();
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream().map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.of(errorHandler);
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return As400ConnectorConfig.ALL_FIELDS;
    }

    // TODO remove when DBZ-7700 is implemented
    @Override
    protected void registerServiceProviders(ServiceRegistry serviceRegistry) {

        serviceRegistry.registerServiceProvider(new PostProcessorRegistryServiceProvider());
        serviceRegistry.registerServiceProvider(new DebeziumHeaderProducerProvider());
        serviceRegistry.registerServiceProvider(new CustomConverterServiceProvider());
    }
}
