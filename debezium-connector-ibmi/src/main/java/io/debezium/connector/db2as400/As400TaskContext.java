/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Map;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

public class As400TaskContext extends CdcSourceTaskContext<As400ConnectorConfig> {

    public As400TaskContext(Configuration rawConfig,
                            As400ConnectorConfig config,
                            Map<String, String> customMetricTags) {
        super(rawConfig, config, customMetricTags);
    }
}
