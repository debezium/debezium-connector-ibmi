/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * Explicitly qualified online-start provider. Public native admission does not
 * implement this interface. Missing offsets never select this startup mode.
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
     * legacy barrier, persist activation, then acknowledge F/W. No snapshot or
     * synthetic completed output is permitted. Return only that durable F/W.
     */
    As400Checkpoint handoff(Map<String, ?> legacy, Output output) throws Exception;
}
