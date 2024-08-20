/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;

import io.debezium.connector.common.CdcSourceTaskContext;

public class As400TaskContext extends CdcSourceTaskContext {
    private final As400ConnectorConfig config;

    public As400TaskContext(As400ConnectorConfig config, As400DatabaseSchema schema,
                            Map<String, String> customMetricTags) {
        super(config, customMetricTags, schema::tableIds);
        this.config = config;
    }

    public As400ConnectorConfig getConfig() {
        return config;
    }
}
