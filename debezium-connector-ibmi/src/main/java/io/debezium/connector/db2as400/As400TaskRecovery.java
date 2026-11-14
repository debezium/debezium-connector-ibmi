/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;

import io.debezium.config.Configuration;

/**
 * Internal admission for durable F/W recovery. The configured native session
 * selects its acknowledgement contract from the actual source task context;
 * tests retain their stricter controlled EOS/poll profiles. Properties alone
 * cannot prove worker fencing.
 * Implementations must close source admission on requestStop, including during recover.
 * Recovery must use retained binding history, never today's catalog or a new baseline.
 */
interface As400TaskRecovery extends As400ContinuousRuntime.Intervals, AutoCloseable {
    /** The task factory must create a fresh provider on retry, never reuse this stopped instance. */
    default boolean supportsTaskRestart() {
        return false;
    }

    /** True for ordered at-least-once delivery with exact offset-store acknowledgement. */
    default boolean sourceOffsetAcknowledgements() {
        return false;
    }

    default As400ContinuousRuntime.Limits runtimeLimits() {
        return As400ContinuousRuntime.Limits.experimental();
    }

    /** Only an admitted handoff provider may resume its retained online binding history. */
    default boolean allowLegacyHandoffRecovery() {
        return false;
    }

    As400Partition partition();

    String journalIdentity();

    String fingerprint();

    void checkQualified(Configuration configuration);

    As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception;

    /** Uses the session's existing terminal admission and cancellation boundary. */
    default As400JdbcConnection incrementalSnapshotConnection(As400ConnectorConfig config) throws Exception {
        throw new IllegalStateException("Recovery provider has no qualified incremental snapshot connection");
    }

    /** Observed journal tail for a requested ordinary snapshot, never a fabricated completion checkpoint. */
    default io.debezium.ibmi.db2.journal.retrieve.JournalPosition snapshotPosition() throws Exception {
        throw new IllegalStateException("Recovery provider has no qualified snapshot boundary observation");
    }

    default As400Checkpoint fullSnapshot(As400Checkpoint original, As400BindingHistory.Recovered history,
                                         io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext context,
                                         io.debezium.pipeline.source.spi.SnapshotProgressListener<As400Partition> listener,
                                         java.util.List<io.debezium.pipeline.notification.channels.NotificationChannel> channels,
                                         As400TaskHandoff.Output output, boolean interrupted)
            throws Exception {
        throw new IllegalStateException("Recovery provider has no qualified full snapshot delivery path");
    }

    /** Nonblocking cancellation/admission closure, valid before recover starts. */
    void requestStop();

    /** Called after all admitted source work returns; must release owned handles. */
    @Override
    void close() throws Exception;
}
