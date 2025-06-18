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
import io.debezium.connector.db2as400.As400ConnectorConfig;
import io.debezium.connector.db2as400.As400JdbcConnection;
import io.debezium.jdbc.JdbcConfiguration;

public class TestHelper {

    private static final String DATABASE_NAME = "DTEST";

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(CommonConnectorConfig.DATABASE_CONFIG_PREFIX))
                .withDefault(JdbcConfiguration.PORT, "")
                .withDefault(JdbcConfiguration.USER, "debezium")
                .withDefault(JdbcConfiguration.DATABASE, DATABASE_NAME)
                .withDefault("secure", "false")
                .build();
    }

    public static Configuration defaultConfig(String... tableNames) {
        assert tableNames != null && tableNames.length > 0 : "At least one table name must be provided";

        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();

        jdbcConfiguration.forEach(
                (field, value) -> builder.with(CommonConnectorConfig.DATABASE_CONFIG_PREFIX + field, value));

        final var tableNamesStr = Stream.of(tableNames)
                .map(x -> DATABASE_NAME + "." + x)
                .collect(Collectors.joining(","));

        return builder.with(CommonConnectorConfig.TOPIC_PREFIX, "server1")
                .with(As400ConnectorConfig.SCHEMA, DATABASE_NAME)
                .with(As400ConnectorConfig.TABLE_INCLUDE_LIST, tableNamesStr)
                .withDefault("secure", "false")
                .build();
    }

    public static As400JdbcConnection testConnection() {
        return new As400JdbcConnection(defaultJdbcConfig());
    }
}
