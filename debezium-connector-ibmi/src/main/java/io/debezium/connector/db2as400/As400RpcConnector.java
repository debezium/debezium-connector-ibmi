/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;

public class As400RpcConnector extends RelationalBaseSourceConnector {
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

    @Override
    protected void validateConnection(Map<String, ConfigValue> configValues, Configuration config) {
        As400ConnectorConfig aconfig = new As400ConnectorConfig(config);
        JdbcConfiguration jdbcConfig = aconfig.getJdbcConfig();
        As400JdbcConnection jdbcConnection = new As400JdbcConnection(jdbcConfig);
        final ConfigValue hostnameValue = configValues.get(RelationalDatabaseConnectorConfig.HOSTNAME.name());
        try (JdbcConnection connection = jdbcConnection.connect()) {
            connection.isValid();
        }
        catch (SQLException e) {
            log.error("Failed testing connection for {} with user '{}'", jdbcConnection.connectionString(),
                    jdbcConnection.username(), e);
            hostnameValue.addErrorMessage("Error while validating connector config: " + e.getMessage());
        }
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(As400ConnectorConfig.ALL_FIELDS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<TableId> getMatchingCollections(Configuration config) {
        As400ConnectorConfig aconfig = new As400ConnectorConfig(config);
        JdbcConfiguration jdbcConfig = aconfig.getJdbcConfig();
        As400JdbcConnection jdbcConnection = new As400JdbcConnection(jdbcConfig);
        try (JdbcConnection connection = jdbcConnection.connect()) {
            return connection.readTableNames(jdbcConnection.getRealDatabaseName(), null, null, new String[]{ "TABLE" }).stream()
                    .filter(tableId -> aconfig.getTableFilters().dataCollectionFilter().isIncluded(tableId))
                    .collect(Collectors.toList());
        }
        catch (SQLException e) {
            throw new DebeziumException(e);
        }
    }
}
