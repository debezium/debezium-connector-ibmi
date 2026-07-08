/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

public class As400ConnectorConfigCaptureSchemasTest {

    private As400ConnectorConfig configWith(String schema, String includeList) {
        Configuration.Builder builder = Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .with(As400ConnectorConfig.DATABASE_NAME, "serverX")
                .with(As400ConnectorConfig.SCHEMA, schema);
        if (includeList != null) {
            builder = builder.with(RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST, includeList);
        }
        return new As400ConnectorConfig(builder.build());
    }

    @Test
    public void unionsLibrariesFromQualifiedIncludeList() {
        // Given an include list spanning two libraries
        final As400ConnectorConfig config = configWith("LIB1", "LIB1.T1,LIB2.T2");

        // When deriving the capture schemas
        // Then both libraries are returned
        assertThat(config.getCaptureSchemas()).containsExactlyInAnyOrder("LIB1", "LIB2");
    }

    @Test
    public void fallsBackToDefaultSchemaForUnqualifiedEntries() {
        // Given an include list with no library prefixes
        final As400ConnectorConfig config = configWith("MYLIB", "T1,T2");

        // Then only the default schema is captured
        assertThat(config.getCaptureSchemas()).containsExactly("MYLIB");
    }

    @Test
    public void normalizesCaseAndDeduplicates() {
        // Given a mix of cases, a duplicate, and an unqualified entry
        final As400ConnectorConfig config = configWith("LIB1", "lib2.t1,LIB2.T2,T3");

        // Then libraries are uppercased and deduplicated, with the default schema included
        assertThat(config.getCaptureSchemas()).containsExactlyInAnyOrder("LIB1", "LIB2");
    }

    @Test
    public void handlesDatabaseSchemaTableForm() {
        // Given a fully-qualified database.schema.table entry
        final As400ConnectorConfig config = configWith("LIB1", "MYDB.LIB3.T1");

        // Then only the schema segment is taken as the library
        assertThat(config.getCaptureSchemas()).containsExactlyInAnyOrder("LIB1", "LIB3");
    }

    @Test
    public void returnsOnlyDefaultSchemaWhenIncludeListAbsent() {
        // Given no include list
        final As400ConnectorConfig config = configWith("LIB1", null);

        // Then only the default schema is captured
        assertThat(config.getCaptureSchemas()).containsExactly("LIB1");
    }
}
