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
    private final Map<String, TableId> fileNames = new HashMap<>();
    private final As400ConnectorConfig config;
    private final TopicNamingStrategy<TableId> topics;

    As400NativeTableSchemas(List<As400BindingDefinition> definitions, As400ConnectorConfig config, TopicNamingStrategy<TableId> topics) {
        this.config = config;
        this.topics = topics;
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
        var builder = new TableSchemaBuilder(converters, config.schemaNameAdjuster(), converterRegistry(config),
                config.getSourceInfoStructMaker().schema(), io.debezium.pipeline.txmetadata.TransactionMonitor.TRANSACTION_BLOCK_SCHEMA,
                config.getFieldNamer(), false, config.getEventConvertingFailureHandlingMode());
        var result = new HashMap<TableId, TableSchema>();
        boolean legacyTemporal = As400DcgConfig.transactionalTruncateEnabled(config.getConfig());
        if (legacyTemporal) {
            requireLegacyTemporalCompatibility(definitions.stream().filter(d -> selected(d.identity().tableId(), config)).toList());
        }
        for (var definition : definitions) {
            if (!selected(definition.identity().tableId(), config) && (legacyTemporal || definition.excluded())) {
                continue;
            }
            if (definition.excluded()) {
                throw new IllegalArgumentException("Captured table has only excluded identity history; a validated capture transition is required");
            }
            var table = definition.outputTable();
            TableSchema built = builder.create(topics, table, config.getColumnFilter(),
                    io.debezium.relational.mapping.ColumnMappers.create(config), config.getKeyMapper());
            var expectedKeys = new io.debezium.relational.Key.Builder(table).customKeyMapper(config.getKeyMapper()).build().keyColumns().stream()
                    .map(config.getFieldNamer()::fieldNameFor).toList();
            var expectedValues = table.columns().stream()
                    .filter(column -> config.getColumnFilter().matches(table.id().catalog(), table.id().schema(), table.id().table(), column.name()))
                    .map(config.getFieldNamer()::fieldNameFor).toList();
            var keyFields = built.keySchema() == null ? List.<String> of() : built.keySchema().fields().stream().map(f -> f.name()).toList();
            if ((built.keySchema() == null) != expectedKeys.isEmpty() || !keyFields.equals(expectedKeys)
                    || !built.valueSchema().fields().stream().map(f -> f.name()).toList().equals(expectedValues)) {
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
            fileNames.put(definition.identity().fileJournalId(), definition.schema().id());
        }
        schemas = new java.util.concurrent.ConcurrentHashMap<>(result);
    }

    private static boolean selected(TableId table, As400ConnectorConfig config) {
        return config.getTableFilters().dataCollectionFilter().isIncluded(table) || config.isSignalDataCollection(table);
    }

    void refresh(As400BindingDefinition definition) {
        TableSchema rebuilt = null;
        if (selected(definition.identity().tableId(), config) || !As400DcgConfig.transactionalTruncateEnabled(config.getConfig()) && !definition.excluded()) {
            var replacement = new As400NativeTableSchemas(List.of(definition), config, topics);
            rebuilt = replacement.schemaFor(definition.schema().id());
        }
        var previous = fileNames.remove(definition.identity().fileJournalId());
        if (previous != null && (rebuilt == null || !previous.equals(definition.schema().id()))) {
            schemas.remove(previous);
        }
        if (rebuilt != null) {
            schemas.put(definition.schema().id(), rebuilt);
            fileNames.put(definition.identity().fileJournalId(), definition.schema().id());
        }
    }

    static CustomConverterRegistry converterRegistry(As400ConnectorConfig config) {
        var services = config.getServiceRegistry();
        // Normal tasks register this provider before schema construction. Native
        // preflight also builds schemas before task startup; use the same provider
        // there, preserving any service contributed by the hosting runtime.
        if (services.tryGetService(CustomConverterRegistry.class) == null) {
            services.registerServiceProvider(new io.debezium.converters.custom.CustomConverterServiceProvider());
        }
        return services.getService(CustomConverterRegistry.class);
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
                case "TEXT", "VARCHAR", "VARGRAPHIC" -> value instanceof String;
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
