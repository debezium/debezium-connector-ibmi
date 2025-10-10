/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.db2as400.As400Partition;
import io.debezium.connector.db2as400.As400TaskContext;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.metrics.SnapshotChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class As400ChangeEventSourceMetricsFactory extends DefaultChangeEventSourceMetricsFactory<As400Partition> {

    final As400StreamingChangeEventSourceMetrics streamingMetrics;

    public As400ChangeEventSourceMetricsFactory(As400StreamingChangeEventSourceMetrics streamingMetrics) {
        this.streamingMetrics = streamingMetrics;
    }

    @Override
    public <T extends CdcSourceTaskContext> SnapshotChangeEventSourceMetrics<As400Partition> getSnapshotMetrics(T taskContext,
                                                                                                                ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                EventMetadataProvider eventMetadataProvider) {
        return super.getSnapshotMetrics((As400TaskContext) taskContext, changeEventQueueMetrics, eventMetadataProvider);
    }

    @Override
    public <T extends CdcSourceTaskContext> StreamingChangeEventSourceMetrics<As400Partition> getStreamingMetrics(T taskContext,
                                                                                                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                                                  EventMetadataProvider eventMetadataProvider,
                                                                                                                  CapturedTablesSupplier capturedTablesSupplier) {
        return streamingMetrics;
    }

    @Override
    public boolean connectionMetricHandledByCoordinator() {
        return false;
    }
}
