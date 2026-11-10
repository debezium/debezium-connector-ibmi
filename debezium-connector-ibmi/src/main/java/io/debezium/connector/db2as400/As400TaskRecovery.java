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
 * validates the public Connect transaction capability; tests retain their stricter
 * controlled EOS/poll profiles. Properties alone cannot prove worker fencing.
 * Implementations must close source admission on requestStop, including during recover.
 * Recovery must use retained binding history, never today's catalog or a new baseline.
 */
interface As400TaskRecovery extends As400ContinuousRuntime.Intervals, AutoCloseable {
    default As400ContinuousRuntime.Limits runtimeLimits() {
        return As400ContinuousRuntime.Limits.experimental();
    }

    /** Public native admission remains disabled until online handoff qualification is complete. */
    default boolean allowLegacyHandoffRecovery() {
        return false;
    }

    As400Partition partition();

    String journalIdentity();

    String fingerprint();

    void checkQualified(Configuration configuration);

    As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception;

    /** Nonblocking cancellation/admission closure, valid before recover starts. */
    void requestStop();

    /** Called after all admitted source work returns; must release owned handles. */
    @Override
    void close() throws Exception;
}
