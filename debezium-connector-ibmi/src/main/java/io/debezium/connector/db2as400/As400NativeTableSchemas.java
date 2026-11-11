/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigDecimal;
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
    private static final java.time.format.DateTimeFormatter LEGACY_TIMESTAMP = java.time.format.DateTimeFormatter.ofPattern(
            "uuuu-MM-dd-HH.mm.ss.SSSSSS", java.util.Locale.ROOT);
    private final Map<TableId, TableSchema> schemas;

    As400NativeTableSchemas(List<As400BindingDefinition> definitions, As400ConnectorConfig config, TopicNamingStrategy<TableId> topics) {
        if (config.getDecimalMode() != JdbcValueConverters.DecimalMode.PRECISE
                || config.binaryHandlingMode() != io.debezium.config.CommonConnectorConfig.BinaryHandlingMode.BYTES
                || config.getConfig().asMap().entrySet().stream().anyMatch(e -> e.getValue() != null && !e.getValue().isBlank()
                        && (e.getKey().equals("converters") || e.getKey().equals("message.key.columns") || e.getKey().startsWith("column.")))) {
            throw new IllegalArgumentException("Native output requires precise decimals, owned bytes and complete unmodified native columns/keys");
        }
        // Snapshot, legacy streaming and buffered streaming must share string
        // cleanup and trim semantics, especially for already-published keys.
        var converters = new As400ValueConverters(config.getDecimalMode(), config) {
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
        boolean legacyTemporal = As400DcgConfig.transactionalTruncateEnabled(config.getConfig());
        if (legacyTemporal) {
            requireLegacyTemporalCompatibility(definitions);
        }
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
            var compatibility = definition.codecs().stream().map(c -> legacyTemporal ? legacyTemporal(c) : null).toList();
            TableSchema checked = new TableSchema(table.id(), built.keySchema(), data -> built.keyFromColumnData(owned(definition, data, temporal, compatibility)),
                    built.getEnvelopeSchema(), built.valueSchema(), data -> built.valueFromColumnData(owned(definition, data, temporal, compatibility)));
            if (result.put(table.id(), checked) != null) {
                throw new IllegalArgumentException("Duplicate acquired table schema");
            }
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("Native output requires acquired schemas");
        }
        schemas = Map.copyOf(result);
    }

    private static java.util.function.Function<Object, Object> legacyTemporal(As400BindingDefinition.Codec codec) {
        com.ibm.as400.access.AS400DataType legacy = switch (codec.kind()) {
            case "DATE_ISO" -> new com.ibm.as400.access.AS400Date();
            case "TIME_ISO" -> new com.ibm.as400.access.AS400Time();
            case "TIMESTAMP_ISO" -> new com.ibm.as400.access.AS400Timestamp();
            default -> null;
        };
        if (legacy == null) {
            return null;
        }
        var text = new com.ibm.as400.access.AS400Text(legacy.getByteLength(), codec.ccsid());
        return value -> {
            String encoded;
            if (value instanceof java.time.Duration time) {
                long seconds = time.getSeconds();
                encoded = twoDigits(seconds / 3600) + "." + twoDigits(seconds / 60 % 60) + "." + twoDigits(seconds % 60);
            }
            else if (value instanceof java.time.LocalDateTime timestamp) {
                encoded = LEGACY_TIMESTAMP.format(timestamp);
            }
            else {
                encoded = ((java.time.LocalDate) value).toString();
            }
            return legacy.toObject(text.toBytes(encoded));
        };
    }

    private static String twoDigits(long value) {
        return value < 10 ? "0" + value : Long.toString(value);
    }

    static void requireLegacyTemporalCompatibility(List<As400BindingDefinition> definitions) {
        for (var definition : definitions) {
            for (var codec : definition.codecs()) {
                if (codec.kind().equals("TIMESTAMP_ISO") && codec.scale() != 6) {
                    throw new IllegalArgumentException("Online migration cannot preserve the legacy fixed-width timestamp decoder at precision " + codec.scale());
                }
            }
        }
    }

    private static Object[] owned(As400BindingDefinition definition, Object[] data,
                                  List<io.debezium.ibmi.db2.journal.data.types.AS400Temporal> temporal,
                                  List<java.util.function.Function<Object, Object>> compatibility) {
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
        Object[] result = As400RowData.copyOf(data, 65536).values();
        for (int i = 0; i < result.length && i < compatibility.size(); i++) {
            if (result[i] != null && compatibility.get(i) != null) {
                // Only the output boundary uses the legacy representation.
                // Buffered/replayed values remain owned and timezone-free.
                result[i] = compatibility.get(i).apply(result[i]);
            }
        }
        return result;
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
