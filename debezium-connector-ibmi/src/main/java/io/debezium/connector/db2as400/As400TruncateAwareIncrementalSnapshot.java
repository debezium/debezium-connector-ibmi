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
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/** The ordinary chunk algorithm, with whole-table rather than single-key deduplication for a committed clear. */
class As400TruncateAwareIncrementalSnapshot extends SignalBasedIncrementalSnapshotChangeEventSource<As400Partition, TableId> {
    As400TruncateAwareIncrementalSnapshot(As400ConnectorConfig config, As400JdbcConnection connection,
                                          EventDispatcher<As400Partition, TableId> dispatcher, As400DatabaseSchema schemas,
                                          Clock clock, SnapshotProgressListener<As400Partition> progress, DataChangeEventListener<As400Partition> listener,
                                          NotificationService<As400Partition, As400OffsetContext> notifications) {
        super(config, connection, dispatcher, schemas, clock, progress, listener, notifications);
    }

    void truncate(TableId table) {
        if (context != null && context.deduplicationNeeded() && context.currentDataCollectionId() != null
                && context.currentDataCollectionId().getId().equals(table)) {
            // Core removes one key for a row change. A committed clear removes
            // every pre-clear row from the active snapshot window.
            window.clear();
        }
    }

}
