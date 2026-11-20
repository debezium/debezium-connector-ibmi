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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.common.RelationalBaseSourceConnector;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

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
    public org.apache.kafka.connect.source.ConnectorTransactionBoundaries canDefineTransactionBoundaries(Map<String, String> props) {
        return As400DcgConfig.configured(Configuration.from(props))
                ? org.apache.kafka.connect.source.ConnectorTransactionBoundaries.SUPPORTED
                : org.apache.kafka.connect.source.ConnectorTransactionBoundaries.UNSUPPORTED;
    }

    @Override
    public org.apache.kafka.connect.source.ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        // Whole-batch replay may repeat an already committed prefix.
        return org.apache.kafka.connect.source.ExactlyOnceSupport.UNSUPPORTED;
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
        if (As400DcgConfig.configured(config)) {
            // Use the same bounded, read-only, terminally admitted connection
            // profile as the configured task, not the legacy JDBC defaults.
            try (var io = new As400DcgNativeIo(As400DcgConfig.configuredProfile(config)); var connection = io.connection(false)) {
                connection.setReadOnly(true);
                if (!connection.isValid(2)) {
                    throw new SQLException("Native connection is not valid");
                }
            }
            catch (Exception error) {
                configValues.computeIfAbsent(RelationalDatabaseConnectorConfig.HOSTNAME.name(), ConfigValue::new)
                        .addErrorMessage("Cannot validate configured D/CG connection: " + error.getMessage());
            }
            return;
        }
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
        var values = config.validate(As400ConnectorConfig.ALL_FIELDS);
        try {
            if (As400DcgConfig.configured(config)) {
                values.putAll(config.validate(io.debezium.config.Field.setOf(RelationalDatabaseConnectorConfig.HOSTNAME)));
                As400DcgConfig.configuredProfile(config);
            }
        }
        catch (IllegalArgumentException error) {
            String field = config.getString("truncate.transactional.enabled", "false").equals("false")
                    ? As400ConnectorConfig.DCG_MODE.name()
                    : As400ConnectorConfig.TRANSACTIONAL_TRUNCATE.name();
            values.get(field).addErrorMessage(error.getMessage());
        }
        return values;
    }

}
