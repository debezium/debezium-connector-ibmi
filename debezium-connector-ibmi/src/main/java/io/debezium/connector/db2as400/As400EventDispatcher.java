/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.heartbeat.Heartbeat.ScheduledHeartbeat;
import io.debezium.heartbeat.HeartbeatFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.ChangeEventCreator;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionFilters.DataCollectionFilter;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Preserves heartbeat interruption in every failure-handling mode. Core may consume it,
 * but cancellation must never authorize acknowledgement of an unfinished record.
 */
class As400EventDispatcher extends EventDispatcher<As400Partition, TableId> {
    private final ThreadLocal<HeartbeatDispatch> activeDispatch = new ThreadLocal<>();

    As400EventDispatcher(As400ConnectorConfig config, TopicNamingStrategy<TableId> topicNamingStrategy,
                         DatabaseSchema<TableId> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<TableId> filter,
                         ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider, SchemaNameAdjuster schemaNameAdjuster,
                         SignalProcessor<As400Partition, ?> signalProcessor, DebeziumHeaderProducer headers) {
        this(config, topicNamingStrategy, schema, queue, filter, changeEventCreator, metadataProvider,
                new HeartbeatFactory<>().getScheduledHeartbeat(config, null, null, queue), schemaNameAdjuster, signalProcessor, headers);
    }

    As400EventDispatcher(As400ConnectorConfig config, TopicNamingStrategy<TableId> topicNamingStrategy,
                         DatabaseSchema<TableId> schema, ChangeEventQueue<DataChangeEvent> queue, DataCollectionFilter<TableId> filter,
                         ChangeEventCreator changeEventCreator, EventMetadataProvider metadataProvider, ScheduledHeartbeat heartbeat,
                         SchemaNameAdjuster schemaNameAdjuster, SignalProcessor<As400Partition, ?> signalProcessor, DebeziumHeaderProducer headers) {
        super(config, topicNamingStrategy, schema, queue, filter, changeEventCreator, metadataProvider, heartbeat,
                schemaNameAdjuster, signalProcessor, headers);
    }

    @Override
    public boolean dispatchDataChangeEvent(As400Partition partition, TableId tableId, ChangeRecordEmitter<As400Partition> emitter)
            throws InterruptedException {
        HeartbeatDispatch previous = activeDispatch.get();
        HeartbeatDispatch current = new HeartbeatDispatch();
        activeDispatch.set(current);
        try {
            return super.dispatchDataChangeEvent(partition, tableId, emitter);
        }
        finally {
            if (previous == null) {
                activeDispatch.remove();
            }
            else {
                activeDispatch.set(previous);
            }
            if (current.interrupted != null) {
                throw current.interrupted;
            }
        }
    }

    @Override
    public void dispatchHeartbeatEvent(As400Partition partition, OffsetContext offset) throws InterruptedException {
        try {
            super.dispatchHeartbeatEvent(partition, offset);
        }
        catch (InterruptedException e) {
            HeartbeatDispatch current = activeDispatch.get();
            if (current != null) {
                current.interrupted = e;
            }
            throw e;
        }
    }

    private static class HeartbeatDispatch {
        private InterruptedException interrupted;
    }
}
