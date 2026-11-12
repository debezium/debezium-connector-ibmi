/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/** Standard chunk algorithm with ordered startup and whole-table clear deduplication. */
final class As400BufferedIncrementalSnapshot extends SignalBasedIncrementalSnapshotChangeEventSource<As400Partition, TableId> {
    private final As400DatabaseSchema snapshotSchemas;
    private final As400ConnectorConfig config;
    private final Clock clock;
    private boolean started;

    As400BufferedIncrementalSnapshot(As400ConnectorConfig config, As400JdbcConnection connection,
                                     EventDispatcher<As400Partition, TableId> dispatcher, As400DatabaseSchema schemas,
                                     Clock clock, SnapshotProgressListener<As400Partition> progress, DataChangeEventListener<As400Partition> listener,
                                     NotificationService<As400Partition, As400OffsetContext> notifications) {
        super(config, connection, dispatcher, schemas, clock, progress, listener, notifications);
        this.snapshotSchemas = schemas;
        this.config = config;
        this.clock = clock;
    }

    @Override
    public void init(As400Partition partition, OffsetContext offset) {
        // The coordinator invokes init before the output preparation scope.
        // Read/restart the first chunk only once the source owns that scope.
    }

    void start(As400Partition partition, As400OffsetContext offset) {
        if (!started) {
            started = true;
            super.init(partition, offset);
        }
    }

    @Override
    protected void readChunk(As400Partition partition, OffsetContext offset) throws InterruptedException {
        // The core helper can fetch an unknown table from today's catalog. A
        // recovered transactional snapshot must only use the definitions whose
        // history also validates its streamed deduplication records. Check every
        // queued target before the core can write a watermark or query data.
        for (var collection : context.getDataCollections()) {
            var table = collection.getId();
            if (snapshotSchemas.tableFor(table) == null || !config.getTableFilters().dataCollectionFilter().isIncluded(table)) {
                throw new IllegalArgumentException("Incremental snapshot target has no recovered capture schema: " + table);
            }
        }
        super.readChunk(partition, offset);
    }

    void truncate(TableId table) {
        if (context != null && context.deduplicationNeeded() && context.currentDataCollectionId() != null
                && context.currentDataCollectionId().getId().equals(table)) {
            // Core removes one key for a row change. A committed clear removes
            // every pre-clear row from the active snapshot window.
            window.clear();
        }
    }

    void schemaChanged(As400BindingDefinition definition) {
        if (!definition.excluded()) {
            snapshotSchemas.forwardSchema(definition.outputTable());
        }
    }

    @Override
    protected ChangeRecordEmitter<As400Partition> getChangeRecordEmitter(As400Partition partition, TableId table, OffsetContext offset, Object[] row) {
        // SELECT values already use ordinary JDBC representations. Native row
        // validators apply to journal images, not to the JDBC snapshot values.
        return new io.debezium.relational.SnapshotChangeRecordEmitter<>(partition, offset, row, clock, config) {
            @Override
            public void emitChangeRecords(DataCollectionSchema ignored, Receiver<As400Partition> receiver) throws InterruptedException {
                super.emitChangeRecords(snapshotSchemas.schemaFor(table), receiver);
            }
        };
    }

    @Override
    public void close() {
        try {
            jdbcConnection.close();
        }
        catch (java.sql.SQLException failure) {
            throw new IllegalStateException("Could not close incremental snapshot connection", failure);
        }
        finally {
            snapshotSchemas.close();
        }
    }
}
