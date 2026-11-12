/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * Explicitly qualified online-start provider. Native admission independently
 * validates its runtime contract. A separate empty-history check admits an
 * ordinary first-start snapshot; missing offsets alone never admit recovery.
 * The recovery input owns cancellation/drain for all handoff work as well.
 */
interface As400TaskHandoff extends As400TaskRecovery {
    interface Output {
        void emit(SourceRecord record) throws Exception;

        /** Enqueue the commit boundary for the preceding registered prefix. */
        void barrier(SourceRecord record) throws Exception;

        /** Wait for the registered boundary and exact durable offset readback. */
        String acknowledge(SourceRecord barrier) throws Exception;
    }

    /**
     * Validate native history, persist pending bindings, acknowledge the exact
     * legacy barrier, verify the selected history, then acknowledge F/W. No snapshot or
     * synthetic completed output is permitted. Return only that durable F/W.
     */
    As400Checkpoint handoff(Map<String, ?> legacy, Output output) throws Exception;

    record Signals(As400BufferedSignals processor,
            java.util.function.Consumer<io.debezium.pipeline.EventDispatcher<As400Partition, io.debezium.relational.TableId>> register,
            io.debezium.pipeline.source.spi.SnapshotProgressListener<As400Partition> progress,
            io.debezium.pipeline.source.spi.DataChangeEventListener<As400Partition> changes,
            java.util.List<io.debezium.pipeline.notification.channels.NotificationChannel> notifications) {
    }

    default As400Checkpoint handoff(Map<String, ?> legacy, Output output, Signals signals) throws Exception {
        return handoff(legacy, output);
    }

    /** Ordinary first-start snapshot, distinct from native recovery and operator-window initialization. */
    default boolean snapshotRequired() {
        return false;
    }

    default Map<String, ?> snapshot(io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext context,
                                    As400OffsetContext previous,
                                    io.debezium.pipeline.source.spi.SnapshotProgressListener<As400Partition> listener,
                                    java.util.List<io.debezium.pipeline.notification.channels.NotificationChannel> notifications, Output output)
            throws Exception {
        throw new IllegalStateException("Provider does not admit ordinary first-start snapshots");
    }
}
