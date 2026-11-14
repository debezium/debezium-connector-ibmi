/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import io.debezium.connector.db2as400.As400OperatorTableEvidence.Request;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.relational.TableId;

/**
 * Explicit first-start admission supplied by the configured, qualified native
 * session or test fixtures. Missing offsets alone never establish this authority.
 * The recovery input owns cancellation and cleanup for ALL initialization handles.
 */
interface As400TaskInitialization {
    As400TaskRecovery recovery();

    As400NativeInitialization initializer();

    JournalReceiver journal();

    List<Request> captured();

    /**
     * Independently checked complete file scope under operator/source exclusion.
     * A provider must also reject unsupported non-file journal resources. Captured
     * tables alone never establish this scope.
     */
    default List<String> controlledTables() throws Exception {
        throw new IllegalStateException("Provider has no qualified planned-window scope");
    }

    /** Explicit bounded materialization; defaults preserve the original admission. */
    default SnapshotLimits snapshotLimits() {
        return new SnapshotLimits(1000, 1_000_000);
    }

    record SnapshotLimits(int rows, long bytes) {
        public SnapshotLimits {
            // Leave room for barriers under the acknowledgement tracker's 10,000
            // record ceiling. This bounds owned payload, not total JVM overhead.
            if (rows < 1 || rows > 9000 || bytes < 1 || bytes > 16_000_000) {
                throw new IllegalArgumentException("Unsupported initialization snapshot limits");
            }
        }
    }

    /**
     * Verify a fresh, dedicated output/history generation and consume a durable,
     * single-use enrollment before source preparation. An interrupted enrollment
     * remains quarantined. Verify journal-wide writer/DDL/maintenance exclusion,
     * required census authority and retention; return the held session's epoch.
     */
    String enroll() throws Exception;

    /** Verify the same scoped source session is still held; never a quietness flag. */
    String checkHeld() throws Exception;

    /** Materialize a bounded, complete snapshot under the held session before B is selected. */
    List<Row> prepareBaseline(String epoch, List<As400BindingDefinition> definitions) throws Exception;

    /** Separate, explicitly admitted protocol; never reinterpret the list limits. */
    default Optional<As400SnapshotChunks.Limits> chunkLimits() {
        return Optional.empty();
    }

    default boolean supportsChunks() {
        return false;
    }

    default long remainingMillis() {
        return Long.MAX_VALUE;
    }

    /**
     * Synchronously deliver owned chunks, then report each table complete only
     * after genuine EOF and successful cursor/statement close. All source reads
     * finish before returning and before B. The consumer waits for durability.
     */
    default void scanBaseline(String epoch, List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
        throw new UnsupportedOperationException("Chunked snapshot source is not qualified");
    }

    /** Bind the recovery input to acknowledged F/W/history; retain source exclusion. */
    void checkpointAccepted(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception;

    /**
     * Recheck admission and install the reader continuation before explicitly ending
     * initialization exclusion. Called only after durable F/W and recovered history.
     * Failure/stop must not authorize a new initialization or automatically release it.
     */
    void handoff(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception;

    record Row(TableId table, As400RowData values, Instant timestamp) {
        public Row {
            Objects.requireNonNull(table);
            Objects.requireNonNull(values);
            Objects.requireNonNull(timestamp);
        }
    }
}
