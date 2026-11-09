/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.DatabaseSchema;
import io.debezium.spi.topic.TopicNamingStrategy;

/** Fixed native schemas shared by snapshot and buffered output; no current-catalog lookup or partial projection. */
final class As400NativeTableSchemas implements DatabaseSchema<TableId> {
    private final Map<TableId, TableSchema> schemas;

    As400NativeTableSchemas(List<As400BindingDefinition> definitions, As400ConnectorConfig config, TopicNamingStrategy<TableId> topics) {
        if (config.getDecimalMode() != JdbcValueConverters.DecimalMode.PRECISE
                || config.binaryHandlingMode() != io.debezium.config.CommonConnectorConfig.BinaryHandlingMode.BYTES
                || config.getConfig().asMap().entrySet().stream().anyMatch(e -> e.getValue() != null && !e.getValue().isBlank()
                        && (e.getKey().equals("converters") || e.getKey().equals("message.key.columns") || e.getKey().startsWith("column.")))) {
            throw new IllegalArgumentException("Native output requires precise decimals, owned bytes and complete unmodified native columns/keys");
        }
        var converters = new JdbcValueConverters(config.getDecimalMode(), config.getTemporalPrecisionMode(), ZoneOffset.UTC, null, null, null) {
            @Override
            protected int getTimePrecision(io.debezium.relational.Column column) {
                // Native LENGTH is storage/buffer size; DATETIME_PRECISION is
                // pinned separately as the acquired column scale.
                return column.scale().orElseThrow(() -> new IllegalArgumentException("Native temporal precision missing"));
            }
        };
        var builder = new TableSchemaBuilder(converters, config.schemaNameAdjuster(), new CustomConverterRegistry(List.of()),
                config.getSourceInfoStructMaker().schema(), io.debezium.pipeline.txmetadata.TransactionMonitor.TRANSACTION_BLOCK_SCHEMA,
                config.getFieldNamer(), false, config.getEventConvertingFailureHandlingMode());
        var result = new HashMap<TableId, TableSchema>();
        for (var definition : definitions) {
            var table = definition.outputTable();
            if (table.primaryKeyColumnNames().isEmpty() || table.primaryKeyColumnNames().stream().anyMatch(k -> table.columnWithName(k).isOptional())) {
                throw new IllegalArgumentException("Native output requires a complete non-null primary key");
            }
            TableSchema built = builder.create(topics, table, null, null, null);
            if (built.keySchema() == null || built.valueSchema().fields().size() != table.columns().size()
                    || !built.keySchema().fields().stream().map(f -> f.name()).toList().equals(table.primaryKeyColumnNames())
                    || !built.valueSchema().fields().stream().map(f -> f.name()).toList().equals(table.columns().stream().map(c -> c.name()).toList())) {
                throw new IllegalArgumentException("Native output schema lost or remapped acquired fields/keys");
            }
            var temporal = definition.codecs().stream().map(c -> c.temporal()
                    ? (io.debezium.ibmi.db2.journal.data.types.AS400Temporal) c.decoder()
                    : null).toList();
            TableSchema checked = new TableSchema(table.id(), built.keySchema(), data -> built.keyFromColumnData(owned(definition, data, temporal)),
                    built.getEnvelopeSchema(), built.valueSchema(), data -> built.valueFromColumnData(owned(definition, data, temporal)));
            if (result.put(table.id(), checked) != null) {
                throw new IllegalArgumentException("Duplicate acquired table schema");
            }
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("Native output requires acquired schemas");
        }
        schemas = Map.copyOf(result);
    }

    private static Object[] owned(As400BindingDefinition definition, Object[] data,
                                  List<io.debezium.ibmi.db2.journal.data.types.AS400Temporal> temporal) {
        if (data.length != definition.codecs().size() + (definition.usesRrn() ? 1 : 0)) {
            throw new IllegalArgumentException("Incomplete native row projection");
        }
        if (definition.usesRrn() && (!(data[data.length - 1] instanceof Long rrn) || rrn <= 0)) {
            throw new IllegalArgumentException("Invalid relative record key in native output");
        }
        for (int i = 0; i < definition.codecs().size(); i++) {
            Object value = data[i];
            var codec = definition.codecs().get(i);
            boolean valid = value == null ? definition.schema().columns().get(i).isOptional() : switch (codec.kind()) {
                case "BIN2" -> value instanceof Short || value instanceof Integer n && n >= Short.MIN_VALUE && n <= Short.MAX_VALUE;
                case "BIN4" -> value instanceof Integer;
                case "BIN8" -> value instanceof Long;
                case "FLOAT4" -> value instanceof Float n && Float.isFinite(n);
                case "FLOAT8" -> value instanceof Double n && Double.isFinite(n);
                case "BOOLEAN" -> value instanceof Boolean;
                case "PACKED", "ZONED" -> value instanceof BigDecimal n && n.scale() == codec.scale() && n.precision() <= codec.size();
                case "TEXT", "VARCHAR" -> value instanceof String;
                case "BINARY" -> value instanceof byte[] bytes && bytes.length == codec.size();
                case "VARBINARY" -> value instanceof byte[] bytes && bytes.length <= codec.size();
                case "DATE_ISO", "TIME_ISO", "TIMESTAMP_ISO" -> {
                    temporal.get(i).validate(value);
                    if (codec.kind().equals("TIMESTAMP_ISO") && codec.scale() > 6) {
                        // The shared converter may impose a narrower boundary
                        // than signed epoch nanos. Check outside its nullable
                        // fallback so an unsupported value cannot become null.
                        io.debezium.time.NanoTimestamp.toEpochNanos(value, null);
                    }
                    yield true;
                }
                default -> false;
            };
            if (!valid) {
                throw new IllegalArgumentException("Unsupported/null native value for " + definition.schema().columns().get(i).name());
            }
        }
        return As400RowData.copyOf(data, 65536).values();
    }

    @Override
    public TableSchema schemaFor(TableId id) {
        TableSchema schema = schemas.get(id);
        if (schema == null) {
            throw new IllegalArgumentException("Table is absent from recovered native schema history: " + id);
        }
        return schema;
    }

    @Override
    public boolean tableInformationComplete() {
        return true;
    }

    @Override
    public boolean isHistorized() {
        return false; // Fixed schemas recovered externally; no live DDL/schema-change handler.
    }

    @Override
    public void close() {
    }
}
