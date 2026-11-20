/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.metrics;

import io.debezium.pipeline.metrics.StreamingChangeEventSourceMetricsMXBean;

public interface As400ChangeEventSourceMetricsMXBean extends StreamingChangeEventSourceMetricsMXBean {
    long getJournalBehind();

    long getJournalOffset();

    long getLastProcessedMs();

    String getScanPosition();

    String getDurableReplayFloor();

    String getDurableCompletedPosition();

    long getBufferedTransactions();

    long getBufferedBytes();

    long getOldestTransactionAgeInMilliseconds();
}
