/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;

/** Coordinator context for immutable durable F/W. Legacy Loader deliberately stays unchanged. */
final class As400RecoveryOffsetContext extends As400OffsetContext {
    private volatile Map<String, String> durable;
    private final String journal;
    private final String fingerprint;

    As400RecoveryOffsetContext(As400ConnectorConfig config, Map<String, ?> offset, String journal, String fingerprint) {
        super(config);
        this.journal = journal;
        this.fingerprint = fingerprint;
        acknowledged(offset);
    }

    void acknowledged(Map<String, ?> offset) {
        durable = As400Checkpoint.fromOffset(offset, journal, fingerprint).toOffset();
    }

    @Override
    public Map<String, ?> getOffset() {
        return durable;
    }

    @Override
    public boolean isInitialSnapshotRunning() {
        return false;
    }
}
