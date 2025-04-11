/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2as400.As400OffsetContext.Loader;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.SchemaChangeEvent.SchemaChangeEventType;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.util.Clock;

public class As400SnapshotChangeEventSource
        extends RelationalSnapshotChangeEventSource<As400Partition, As400OffsetContext> {
    private static final Logger log = LoggerFactory.getLogger(As400SnapshotChangeEventSource.class);

    private final As400ConnectorConfig connectorConfig;
    private final As400JdbcConnection jdbcConnection;
    private final As400RpcConnection rpcConnection;
    private final As400DatabaseSchema schema;
    protected final SnapshotterService snapshotterService;

    public As400SnapshotChangeEventSource(As400ConnectorConfig connectorConfig, As400RpcConnection rpcConnection,
                                          MainConnectionProvidingConnectionFactory<As400JdbcConnection> jdbcConnectionFactory,
                                          As400DatabaseSchema schema, EventDispatcher<As400Partition, TableId> dispatcher, Clock clock,
                                          SnapshotProgressListener<As400Partition> snapshotProgressListener,
                                          NotificationService<As400Partition, As400OffsetContext> notificationService,
                                          SnapshotterService snapshotterService) {

        super(connectorConfig, jdbcConnectionFactory, schema, dispatcher, clock, snapshotProgressListener,
                notificationService, snapshotterService);

        this.connectorConfig = connectorConfig;
        this.rpcConnection = rpcConnection;
        this.jdbcConnection = jdbcConnectionFactory.mainConnection();
        this.schema = schema;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotResult<As400OffsetContext> execute(ChangeEventSourceContext context, As400Partition partition,
                                                      As400OffsetContext previousOffset, SnapshottingTask snapshottingTask)
            throws InterruptedException {

        return super.execute(context, partition, previousOffset, snapshottingTask);
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext)
            throws Exception {
        final Set<TableId> tables = jdbcConnection.readTableNames(jdbcConnection.getRealDatabaseName(),
                connectorConfig.getSchema(), null, new String[]{ "TABLE" });
        return tables;
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext)
            throws Exception {
        final Duration lockTimeout = connectorConfig.snapshotLockTimeout();
        final Set<String> capturedTablesNames = snapshotContext.capturedTables.stream().map(As400ConnectorConfig.tableToString::toString).collect(Collectors.toSet());

        List<String> tableLockStatements = capturedTablesNames.stream()
                .map(tableId -> snapshotterService.getSnapshotLock().tableLockingStatement(lockTimeout, tableId))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        if (!tableLockStatements.isEmpty()) {

            String lineSeparator = System.lineSeparator();
            StringBuilder statements = new StringBuilder();
            statements.append("SET lock_timeout = ").append(lockTimeout.toMillis()).append(";").append(lineSeparator);
            // we're locking in ACCESS SHARE MODE to avoid concurrent schema changes while we're taking the snapshot
            // this does not prevent writes to the table, but prevents changes to the table's schema....
            // DBZ-298 Quoting name in case it has been quoted originally; it doesn't do harm if it hasn't been quoted
            tableLockStatements.forEach(tableStatement -> statements.append(tableStatement).append(lineSeparator));

            log.info("Waiting a maximum of '{}' seconds for each table lock", lockTimeout.getSeconds());
            jdbcConnection.executeWithoutCommitting(statements.toString());
        }
    }

    @Override
    protected void determineSnapshotOffset(
                                           RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext,
                                           As400OffsetContext previousOffset)
            throws Exception {
        if (previousOffset != null && previousOffset.isPositionSet() && !snapshotterService.getSnapshotter().shouldStreamEventsStartingFromSnapshot()) {
            snapshotContext.offset = previousOffset;
        }
        else {
            final Instant now = Instant.now();
            final JournalPosition position = rpcConnection.getCurrentPosition();
            // set last entry to processed, so we don't process it again
            final JournalProcessedPosition processedPos = new JournalProcessedPosition(position, now, true);
            snapshotContext.offset = new As400OffsetContext(connectorConfig, processedPos);
        }
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext,
                                      RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext,
                                      As400OffsetContext offsetContext, SnapshottingTask snapshottingTask)
            throws Exception {
        final Set<String> schemas = snapshotContext.capturedTables.stream().map(TableId::schema)
                .collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of
        // captured tables;
        // while the passed table name filter alone would skip all non-included tables,
        // reading the schema
        // would take much longer that way
        for (final String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }

            log.info("Reading structure of schema '{}'", schema);

            jdbcConnection.readSchema(snapshotContext.tables, // use snapshotContext.capturedSchemaTables?
                    jdbcConnection.getRealDatabaseName(), schema,
                    connectorConfig.getTableFilters().eligibleDataCollectionFilter(), null, false);

            try {
                jdbcConnection.getAllSystemNames(schema);
            }
            catch (final Exception e) {
                log.warn("failure fetching table names", e);
            }
        }

        for (final TableId id : snapshotContext.capturedTables) {
            final Table table = snapshotContext.tables.forTable(id);
            if (table == null) {
                log.error("table schema not found for {}", id, new Exception("missing table definition"));
            }
            else {
                schema.addSchema(table);
            }
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(
                                              RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext)
            throws Exception {
        // TODO unlock tables
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(
                                                    RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext, Table table) {
        return SchemaChangeEvent.of(SchemaChangeEventType.CREATE, snapshotContext.partition, snapshotContext.offset,
                snapshotContext.catalogName, table.id().schema(), null, table, true);
    }

    @Override
    protected Optional<String> getSnapshotSelect(
                                                 RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext, TableId tableId,
                                                 List<String> columns) {
        String fullTableName = String.format("%s.%s", tableId.schema(), tableId.table());
        return snapshotterService.getSnapshotQuery().snapshotQuery(fullTableName, columns);
    }

    @Override
    public SnapshottingTask getSnapshottingTask(As400Partition partition, As400OffsetContext previousOffset) {
        final Snapshotter snapshotter = snapshotterService.getSnapshotter();
        final List<String> dataCollectionsToBeSnapshotted = connectorConfig.getDataCollectionsToBeSnapshotted();
        final Map<DataCollectionId, String> snapshotSelectOverridesByTable = connectorConfig.getSnapshotSelectOverridesByTable();

        boolean offsetExists = previousOffset != null;
        boolean snapshotInProgress = false;

        if (offsetExists) {
            snapshotInProgress = !previousOffset.isSnapshotComplete();
        }

        if (offsetExists && previousOffset.isSnapshotComplete()) {
            // when control tables in place
            if (!previousOffset.hasNewTables()) {
                log.info(
                        "A previous offset indicating a completed snapshot has been found. Neither schema nor data will be snapshotted.");
                return new SnapshottingTask(true, false, dataCollectionsToBeSnapshotted,
                        snapshotSelectOverridesByTable, false);
            }
            log.info("A previous offset indicating a completed snapshot has been found.");
        }

        boolean shouldSnapshotSchema = snapshotter.shouldSnapshotSchema(offsetExists, snapshotInProgress);
        boolean shouldSnapshotData = snapshotter.shouldSnapshotData(offsetExists, snapshotInProgress);

        if (shouldSnapshotData && shouldSnapshotSchema) {
            log.info("According to the connector configuration both schema and data will be snapshot.");
        }
        else {
            log.info("According to the connector configuration only schema will be snapshot (this should always be done).");  
        }

        return new SnapshottingTask(true,
                shouldSnapshotData, dataCollectionsToBeSnapshotted,
                snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<As400Partition, As400OffsetContext> prepare(As400Partition partition, boolean onDemand) throws Exception {
        return new RelationalSnapshotContext<>(partition, jdbcConnection.getRealDatabaseName(), onDemand);
    }

    @Override
    protected As400OffsetContext copyOffset(
                                            RelationalSnapshotContext<As400Partition, As400OffsetContext> snapshotContext) {
        return new Loader(connectorConfig).load(snapshotContext.offset.getOffset());
    }
}
