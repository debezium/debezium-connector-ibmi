/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;

public class As400RpcConnector extends SourceConnector {
    private static final Logger log = LoggerFactory.getLogger(As400RpcConnector.class);

    private Map<String, String> props;

    public As400RpcConnector() {
    }

    public As400RpcConnector(Configuration config) {

    }

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return As400ConnectorTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> l = new ArrayList<>();
        l.add(props);
        return l;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return As400ConnectorConfig.configDef();
    }

}
