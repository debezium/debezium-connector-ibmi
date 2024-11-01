/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.relational.TableId;

public class As400ChangeRecord {
    As400Partition partition;
    TableId tableId;
    As400ChangeRecordEmitter emitter;

    public As400ChangeRecord(As400Partition partition, TableId tableId, As400ChangeRecordEmitter emitter) {
        this.partition = partition;
        this.tableId = tableId;
        this.emitter = emitter;
    }

    public As400Partition getPartition() {
        return partition;
    }

    public void setPartition(As400Partition partition) {
        this.partition = partition;
    }

    public TableId getTableId() {
        return tableId;
    }

    public void setTableId(TableId tableId) {
        this.tableId = tableId;
    }

    public As400ChangeRecordEmitter getEmitter() {
        return emitter;
    }

    public void setEmitter(As400ChangeRecordEmitter emitter) {
        this.emitter = emitter;
    }

    @Override
    public String toString() {
        return "As400ChangeRecord{" +
                "partition=" + partition +
                ", tableId=" + tableId +
                ", emitter=" + emitter +
                '}';
    }
}
