/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;

public class As400ChangeRecordEmitter extends RelationalChangeRecordEmitter<As400Partition> {

    private final Operation operation;
    private final Object[] data;
    private final Object[] dataNext;

    public As400ChangeRecordEmitter(As400Partition partition, OffsetContext offset, Operation operation, Object[] data, Object[] dataNext, Clock clock,
                                    RelationalDatabaseConnectorConfig connectorConfig) {
        super(partition, offset, clock, connectorConfig);

        this.operation = operation;
        this.data = data;
        this.dataNext = dataNext;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    protected Object[] getOldColumnValues() {
        return data;
    }

    @Override
    protected Object[] getNewColumnValues() {
        return dataNext;
    }

    @Override
    protected void emitTruncateRecord(Receiver<As400Partition> receiver, TableSchema tableSchema) throws InterruptedException {
        Struct envelope = tableSchema.getEnvelopeSchema().truncate(getOffset().getSourceInfo(), getClock().currentTimeAsInstant());
        receiver.changeRecord(getPartition(), tableSchema, Operation.TRUNCATE, null, envelope, getOffset(), null);
    }

}
