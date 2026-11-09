/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.metrics;

import java.math.BigInteger;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.db2as400.As400Partition;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.pipeline.metrics.CapturedTablesSupplier;
import io.debezium.pipeline.metrics.DefaultStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.source.spi.EventMetadataProvider;

public class As400StreamingChangeEventSourceMetrics extends DefaultStreamingChangeEventSourceMetrics<As400Partition> implements As400ChangeEventSourceMetricsMXBean {
    private final AtomicLong journalBehind = new AtomicLong();
    private final AtomicLong journalOffset = new AtomicLong();
    private final AtomicLong lastProcessedMs = new AtomicLong();
    private volatile String scanPosition = "";
    private volatile String replayFloor = "";
    private volatile String completedPosition = "";
    private volatile long bufferedTransactions;
    private volatile long bufferedBytes;
    private volatile long oldestTransactionTimestamp = -1;

    public void setScanPosition(JournalPosition position) {
        scanPosition = position.receiver().library() + "/" + position.receiver().name() + ":" + position.offset();
        journalOffset.set(position.offset().bitLength() < 64 ? position.offset().longValueExact() : -1);
    }

    public void setDurablePositions(Map<String, ?> offset) {
        replayFloor = position(offset, "replay_from");
        completedPosition = position(offset, "completed_through");
    }

    private static String position(Map<String, ?> offset, String prefix) {
        return offset.get(prefix + ".receiver_library") + "/" + offset.get(prefix + ".receiver") + ":" + offset.get(prefix + ".sequence");
    }

    public void setBufferUsage(long[] usage, long oldestTimestamp) {
        bufferedTransactions = usage[0];
        bufferedBytes = usage[1];
        oldestTransactionTimestamp = oldestTimestamp;
    }

    @Override
    public String getScanPosition() {
        return scanPosition;
    }

    @Override
    public String getDurableReplayFloor() {
        return replayFloor;
    }

    @Override
    public String getDurableCompletedPosition() {
        return completedPosition;
    }

    @Override
    public long getBufferedTransactions() {
        return bufferedTransactions;
    }

    @Override
    public long getBufferedBytes() {
        return bufferedBytes;
    }

    @Override
    public long getOldestTransactionAgeInMilliseconds() {
        long timestamp = oldestTransactionTimestamp;
        return timestamp < 0 ? 0 : Math.max(0, System.currentTimeMillis() - timestamp);
    }

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
