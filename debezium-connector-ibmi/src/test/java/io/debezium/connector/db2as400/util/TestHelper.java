/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.util;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.ConfigurationNames;
import io.debezium.connector.db2as400.As400ConnectorConfig;
import io.debezium.connector.db2as400.As400JdbcConnection;
import io.debezium.jdbc.JdbcConfiguration;

public class TestHelper {

    public static String schemaName() {
        String schema = System.getProperty("test.database.schema", "DTEST");
        if (!schema.matches("[A-Z][A-Z0-9_]{0,9}")) {
            throw new IllegalArgumentException("test.database.schema must be an uppercase IBM i library name");
        }
        return schema;
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(ConfigurationNames.DATABASE_CONFIG_PREFIX))
                .withDefault(JdbcConfiguration.PORT, "")
                .withDefault(JdbcConfiguration.USER, "debezium")
                .withDefault(JdbcConfiguration.DATABASE, schemaName())
                .withDefault("secure", "false")
                .build();
    }

    public static Configuration defaultConfig(String... tableNames) {
        assert tableNames != null && tableNames.length > 0 : "At least one table name must be provided";

        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(ConfigurationNames.DATABASE_CONFIG_PREFIX + field, value));

        final var tableNamesStr = Stream.of(tableNames)
                .map(x -> schemaName() + "." + x)
                .collect(Collectors.joining(","));

        return builder.with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
                .with(As400ConnectorConfig.SCHEMA, schemaName())
                .with(As400ConnectorConfig.TABLE_INCLUDE_LIST, tableNamesStr)
                .withDefault("secure", "false")
                .build();
    }

    public static As400JdbcConnection testConnection() {
        return new As400JdbcConnection(defaultJdbcConfig());
    }
}
