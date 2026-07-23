/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.List;
import java.util.Map;

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

    @Test
    public void capturesTablesPerLibraryWithFullTableName() {
        // Given qualified entries across two libraries
        final As400ConnectorConfig config = configWith("LIB1", "LIB1.T1,LIB2.T2");

        // Then each library keeps its full (untruncated) table name
        assertThat(config.getCaptured()).containsOnly(
                entry("LIB1", List.of("T1")),
                entry("LIB2", List.of("T2")));
    }

    @Test
    public void capturesUnqualifiedTablesUnderDefaultSchema() {
        // Given an include list with no library prefixes
        final As400ConnectorConfig config = configWith("MYLIB", "T1,T2");

        // Then both tables are captured under the default schema
        assertThat(config.getCaptured()).containsOnly(entry("MYLIB", List.of("T1", "T2")));
    }

    @Test
    public void mixedQualifiedAndUnqualifiedUnderDefaultSchemaAreKept() {
        // Given a qualified and an unqualified table that both resolve to the default schema
        final As400ConnectorConfig config = configWith("LIB1", "LIB1.T1,T2");

        // Then neither entry clobbers the other
        assertThat(config.getCaptured()).containsOnly(entry("LIB1", List.of("T1", "T2")));
    }

    @Test
    public void databaseSchemaTableFormKeepsTableName() {
        // Given a fully-qualified database.schema.table entry
        final As400ConnectorConfig config = configWith("LIB1", "MYDB.LIB3.T1");

        // Then the library is the schema segment and the table name is preserved
        assertThat(config.getCaptured()).containsOnly(
                entry("LIB1", List.of()),
                entry("LIB3", List.of("T1")));
    }

    @Test
    public void uppercasesLibraryButPreservesRawTableCasing() {
        // Given a lowercase library and a mixed-case table name
        final As400ConnectorConfig config = configWith("LIB1", "lib2.myTable");

        // Then the library is uppercased (for dedup) but the table name is looked up verbatim
        assertThat(config.getCaptured()).containsOnly(
                entry("LIB1", List.of()),
                entry("LIB2", List.of("myTable")));
    }

    @Test
    public void addIncludesFoldsInSignalDataCollection() {
        // Given a captured map and a separately-configured signal data collection
        final As400ConnectorConfig config = configWith("LIB1", "LIB1.T1");
        final Map<String, List<String>> captured = config.getCaptured();

        // When the signal table is folded in
        config.addIncludes(captured, "LIB1.SIGNALS");

        // Then it is journalled alongside the captured tables
        assertThat(captured).containsOnly(entry("LIB1", List.of("T1", "SIGNALS")));
    }
}
