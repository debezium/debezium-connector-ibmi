/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.util.Clock;

/** Autocommit row offsets; transaction-buffer and migration emission remain separate. */
final class As400LegacyChangeRecordEmitter extends As400ChangeRecordEmitter {
    private final OffsetContext replay;
    private final OffsetContext completed;
    private final boolean tombstones;
    private boolean emitted;

    As400LegacyChangeRecordEmitter(As400Partition partition, OffsetContext replay, OffsetContext completed,
                                   Operation operation, Object[] before, Object[] after, Clock clock, As400ConnectorConfig config) {
        super(partition, replay, operation, before, after, clock, config);
        this.replay = replay;
        this.completed = completed;
        this.tombstones = config.isEmitTombstoneOnDelete();
    }

    @Override
    public OffsetContext getOffset() {
        // EventDispatcher's trailing heartbeat must not regress behind a completed row.
        return emitted ? completed : replay;
    }

    @Override
    public void emitChangeRecords(DataCollectionSchema schema, Receiver<As400Partition> receiver) throws InterruptedException {
        super.emitChangeRecords(schema, new Receiver<>() {
            @Override
            public void changeRecord(As400Partition partition, DataCollectionSchema schema, Operation operation,
                                     Object key, Struct value, OffsetContext ignored, ConnectHeaders headers)
                    throws InterruptedException {
                // A key change's delete/tombstone prefix must replay its before-image if
                // the following create fails. A plain delete with a tombstone also keeps
                // its entry replayable until the post-dispatch heartbeat acknowledges it.
                boolean prefix = operation == Operation.DELETE && (getOperation() == Operation.UPDATE || tombstones);
                receiver.changeRecord(partition, schema, operation, key, value, prefix ? replay : completed, headers);
            }

            @Override
            public void unchangedEventSkipped(As400Partition partition) {
                receiver.unchangedEventSkipped(partition);
            }
        });
        emitted = true;
    }
}
