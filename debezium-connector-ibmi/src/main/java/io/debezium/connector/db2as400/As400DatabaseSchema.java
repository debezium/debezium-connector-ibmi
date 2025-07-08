/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2as400.conversion.As400DefaultValueConverter;
import io.debezium.connector.db2as400.conversion.SchemaInfoConversion;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

public class As400DatabaseSchema extends RelationalDatabaseSchema implements SchemaCacheIF {

    private static final Logger log = LoggerFactory.getLogger(As400DatabaseSchema.class);
    private final As400ConnectorConfig config;
    private final Map<String, TableInfo> map = new HashMap<>();
    private final As400JdbcConnection jdbcConnection;
    private final SchemaInfoConversion schemaInfoConversion;
    private final JdbcFileDecoder fileDecoder;

    public As400DatabaseSchema(As400ConnectorConfig config, As400JdbcConnection jdbcConnection,
                               TopicNamingStrategy<TableId> topicSelector, SchemaNameAdjuster schemaNameAdjuster, CustomConverterRegistry customConverterRegistry) {
        super(config, topicSelector, config.getTableFilters().dataCollectionFilter(), config.getColumnFilter(),
                new TableSchemaBuilder(new As400ValueConverters(config.getDecimalMode()),
                        new As400DefaultValueConverter(), schemaNameAdjuster,
                        customConverterRegistry, config.getSourceInfoStructMaker().schema(),
                        config.getFieldNamer(), false),
                false, config.getKeyMapper());

        this.config = config;
        this.jdbcConnection = jdbcConnection;
        fileDecoder = new JdbcFileDecoder(jdbcConnection, jdbcConnection.getRealDatabaseName(), this,
                config.getFromCcsid(),
                config.getToCcsid());

        schemaInfoConversion = new SchemaInfoConversion(fileDecoder);
    }

    public JdbcFileDecoder getFileDecoder() {
        return fileDecoder;
    }

    public void clearCache(String systemTableName, String schema) {
        fileDecoder.clearCache(systemTableName, schema);
    }

    public Optional<TableInfo> getRecordFormat(String systemTableName, String schema) {
        final Optional<TableInfo> oti = fileDecoder.getRecordFormat(systemTableName, schema);
        oti.stream().forEach(ti -> {
            store(jdbcConnection.getRealDatabaseName(), schema, systemTableName, ti);
        });
        return oti;
    }

    // assume always long name - only called from snapshotting
    public void addSchema(Table table) {
        final TableId id = table.id();

        // save for decoding
        final Optional<String> systemTableNameOpt = jdbcConnection.getSystemName(id.schema(), id.table());
        systemTableNameOpt.map(systemTableName -> {
            final TableInfo tableInfo = schemaInfoConversion.table2TableInfo(table);
            return map.put(toKey(id.catalog(), id.schema(), systemTableName), tableInfo);
        });

        forwardSchema(table);
    }

    public void forwardSchema(Table table) {
        tables().overwriteTable(table);
        this.buildAndRegisterSchema(table);
    }

    public String getSchemaName() {
        return config.getSchema();
    }

    @Override
    // implements SchemaCacheIF.store - system name tables/column names
    // assume always short name - only called from the journal
    public void store(String database, String schema, String tableName, TableInfo tableInfo) {
        map.put(toKey(database, schema, tableName), tableInfo);

        final Table table = SchemaInfoConversion.tableInfo2Table(database, schema, tableName, tableInfo);
        forwardSchema(table);
    }

    @Override
    // assume always short name - only called from the journal
    public TableInfo retrieve(String database, String schema, String tableName) {
        return map.get(toKey(database, schema, tableName));
    }

    @Override
    // assume always short name - only called from the journal
    public void clearCache(String database, String schema, String tableName) {
        map.remove(toKey(database, schema, tableName));
    }

    private String toKey(String database, String schema, String tableName) {
        return String.format("%s.%s.%s", database, schema, tableName);
    }
}
