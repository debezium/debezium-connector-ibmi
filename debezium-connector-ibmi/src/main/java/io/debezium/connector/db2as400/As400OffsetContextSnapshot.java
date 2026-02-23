/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Instant;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.SnapshotRecord;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * A read-only snapshot of an {@link As400OffsetContext} taken at a point in time.
 * Used when buffering change records during transaction management so that each
 * buffered emitter preserves the offset state from when the journal entry was read,
 * rather than sharing a mutable reference that gets overwritten by later entries.
 */
public class As400OffsetContextSnapshot implements OffsetContext {

    private final Map<String, ?> offset;
    private final Schema sourceInfoSchema;
    private final Struct sourceInfo;
    private final TransactionContext transactionContext;
    private final IncrementalSnapshotContext<?> incrementalSnapshotContext;

    public As400OffsetContextSnapshot(As400OffsetContext context) {
        this.offset = context.getOffset();
        this.sourceInfoSchema = context.getSourceInfoSchema();
        this.sourceInfo = context.getSourceInfo();
        this.transactionContext = context.getTransactionContext();
        this.incrementalSnapshotContext = context.getIncrementalSnapshotContext();
    }

    @Override
    public Map<String, ?> getOffset() {
        return offset;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfoSchema;
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo;
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }

    @Override
    public boolean isInitialSnapshotRunning() {
        return false;
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        // no-op: snapshot is read-only
    }

    @Override
    public void preSnapshotStart(boolean onDemand) {
        // no-op: snapshot is read-only
    }

    @Override
    public void preSnapshotCompletion() {
        // no-op: snapshot is read-only
    }

    @Override
    public void postSnapshotCompletion() {
        // no-op: snapshot is read-only
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        // no-op: snapshot is read-only
    }
}