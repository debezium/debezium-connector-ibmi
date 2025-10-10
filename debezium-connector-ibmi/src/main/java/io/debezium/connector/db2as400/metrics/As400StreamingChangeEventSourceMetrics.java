/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.metrics;

import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.db2as400.As400Partition;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.metrics.spi.CapturedTablesSupplier;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class As400StreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<As400Partition> implements As400ChangeEventSourceMetricsMXBean {
    private final AtomicLong journalBehind = new AtomicLong();
    private final AtomicLong journalOffset = new AtomicLong();
    private final AtomicLong lastProcessedMs = new AtomicLong();

    public <T extends CdcSourceTaskContext> As400StreamingChangeEventSourceMetrics(T taskContext, ChangeEventQueueMetrics changeEventQueueMetrics,
                                                                                   EventMetadataProvider metadataProvider,
                                                                                   CapturedTablesSupplier capturedTablesSupplier) {
        super(taskContext, changeEventQueueMetrics, metadataProvider, capturedTablesSupplier);
    }

    @Override
    public long getJournalBehind() {
        return journalBehind.get();
    }

    public void setJournalBehind(BigInteger behind) {
        this.journalBehind.lazySet(behind.longValue());
    }

    @Override
    public long getJournalOffset() {
        return journalOffset.get();
    }

    public void setJournalOffset(BigInteger offset) {
        this.journalOffset.lazySet(offset.longValue());
    }

    @Override
    public long getLastProcessedMs() {
        return lastProcessedMs.get();
    }

    public void setLastProcessedMs(long lastProccessedMs) {
        this.lastProcessedMs.lazySet(lastProccessedMs);
    }
}
