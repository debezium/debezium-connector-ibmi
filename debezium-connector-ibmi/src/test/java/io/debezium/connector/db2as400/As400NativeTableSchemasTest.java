/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;

class As400NativeTableSchemasTest {
    static Map<String, String> converterProperties() {
        return Map.of("converters", "label", "label.type", LabelConverter.class.getName(), "label.prefix", "custom:");
    }

    public static class LabelConverter
            implements io.debezium.spi.converter.CustomConverter<org.apache.kafka.connect.data.SchemaBuilder, io.debezium.spi.converter.RelationalColumn> {
        private String prefix;
        private String rejected;

        @Override
        public void configure(Properties properties) {
            prefix = properties.getProperty("prefix");
            rejected = properties.getProperty("reject");
        }

        @Override
        public void converterFor(io.debezium.spi.converter.RelationalColumn column,
                                 ConverterRegistration<org.apache.kafka.connect.data.SchemaBuilder> registration) {
            if (column.name().equals("ID")) {
                registration.register(org.apache.kafka.connect.data.SchemaBuilder.string(), value -> {
                    if (value != null && value.toString().equals(rejected)) {
                        throw new IllegalArgumentException("Configured conversion failure");
                    }
                    return value == null ? null : prefix + value;
                });
            }
        }
    }

    @Test
    void shouldUseTheOrdinaryConverterRegistryForKeysAndValuesAfterBindingHistoryReload() throws Exception {
        var definition = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions().get(0);
        var raw = new java.util.HashMap<>(converterProperties());
        raw.put("topic.prefix", "test");
        raw.put("database.dbname", "TEST");
        var config = new As400ConnectorConfig(Configuration.from(raw));
        var topics = config.<TableId> getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        var ordinary = legacySchema(definition, config, topics);
        for (var restored : List.of(definition, As400BindingDefinition.restore(definition.storedTable()))) {
            var nativeSchema = new As400NativeTableSchemas(List.of(restored), config, topics).schemaFor(restored.identity().tableId());
            assertThat(nativeSchema.keyFromColumnData(row())).isEqualTo(ordinary.keyFromColumnData(row()));
            assertThat(nativeSchema.valueFromColumnData(row())).isEqualTo(ordinary.valueFromColumnData(row()));
            assertThat(nativeSchema.keyFromColumnData(row()).getString("ID")).isEqualTo("custom:7");
            assertThat(nativeSchema.valueFromColumnData(row()).getString("ID")).isEqualTo("custom:7");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "warn", "skip", "fail" })
    void shouldPreserveOrdinaryColumnConversionFailurePolicy(String mode) throws Exception {
        var definition = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions().get(0);
        var raw = new java.util.HashMap<>(converterProperties());
        raw.put("label.reject", "7");
        raw.put("topic.prefix", "test");
        raw.put("database.dbname", "TEST");
        raw.put("event.converting.failure.handling.mode", mode);
        var config = new As400ConnectorConfig(Configuration.from(raw));
        var topics = config.<TableId> getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        var ordinary = legacySchema(definition, config, topics);
        var nativeSchema = new As400NativeTableSchemas(List.of(definition), config, topics).schemaFor(definition.identity().tableId());
        if (mode.equals("fail")) {
            assertThrows(io.debezium.DebeziumException.class, () -> ordinary.valueFromColumnData(row()));
            assertThrows(io.debezium.DebeziumException.class, () -> nativeSchema.valueFromColumnData(row()));
        }
        else {
            assertThat(nativeSchema.valueFromColumnData(row())).isEqualTo(ordinary.valueFromColumnData(row()));
            assertThat(nativeSchema.valueFromColumnData(row()).get("ID")).isNull();
        }
    }

    @Test
    void shouldRejectPublishingFromIdentityOnlyHistoryEvenAfterAFilterChange() throws Exception {
        var definition = As400BindingDefinition.excluded(new As400NativeBindingAcquisitionTest.Fixture().observation.tableIdentity());
        assertThrows(IllegalArgumentException.class, () -> schemas(definition));
        assertThrows(IllegalStateException.class, () -> definition.binding(1024).values(null, null, 0));
    }

    @Test
    void shouldPreserveLegacyNullKeysAndValuesForTablesWithoutAPrimaryKey() throws Exception {
        var fixture = new As400NativeBindingAcquisitionTest.Fixture();
        fixture.keys.clear();
        var definition = As400BindingDefinition.restore(fixture.acquire().definitions().get(0).storedTable());
        var config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("database.dbname", "TEST").build());
        var topics = config.<TableId> getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        var nativeSchema = new As400NativeTableSchemas(List.of(definition), config, topics).schemaFor(definition.schema().id());
        var legacy = legacySchema(definition, config, topics);
        assertThat(nativeSchema.keySchema()).isNull();
        assertThat(legacy.keySchema()).isNull();
        assertThat(nativeSchema.keyFromColumnData(row())).isNull();
        assertThat(nativeSchema.valueFromColumnData(row())).isEqualTo(legacy.valueFromColumnData(row()));
        Object[] invalid = row();
        invalid[0] = null;
        assertThrows(IllegalArgumentException.class, () -> nativeSchema.valueFromColumnData(invalid));
    }

    @ParameterizedTest
    @CsvSource({ "default,both", "none,none", "leading,leading", "trailing,trailing", "both,both" })
    void shouldPreserveLegacyCharacterKeysAndValuesWhenUsingNativeSchemas(String configured, String expectedMode) throws Exception {
        var acquired = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions().get(0);
        // The character column participates in the key so trim/cleanup drift
        // cannot pass by comparing only non-key payloads.
        var definition = new As400BindingDefinition(acquired.identity(), acquired.schema().edit()
                .addColumn(acquired.schema().columnWithName("C").edit().optional(false).create()).setPrimaryKeyNames("ID", "C").create(), acquired.codecs());
        var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "TEST");
        if (!configured.equals("default")) {
            raw.with("trim.non.xml.charsequence.field.mode", configured);
        }
        var config = new As400ConnectorConfig(raw.build());
        assertThat(config.getCharSequenceTrimMode().getValue()).isEqualTo(expectedMode);
        var topics = config.<TableId> getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        var nativeSchema = new As400NativeTableSchemas(List.of(definition), config, topics).schemaFor(definition.schema().id());
        var legacy = legacySchema(definition, config, topics);
        for (String text : List.of("  A   ", " \u0000A\u200B\uE000 ", " Ä[]😃 ")) {
            Object[] values = row();
            values[4] = text;
            assertThat(nativeSchema.keyFromColumnData(values)).as("key %s", configured).isEqualTo(legacy.keyFromColumnData(values));
            assertThat(nativeSchema.valueFromColumnData(values)).as("value %s", configured).isEqualTo(legacy.valueFromColumnData(values));
        }
    }

    @ParameterizedTest
    @CsvSource({ "adaptive,UTC", "adaptive,America/Chicago", "adaptive,Asia/Tokyo", "adaptive_time_microseconds,UTC",
            "adaptive_time_microseconds,America/Chicago", "adaptive_time_microseconds,Asia/Tokyo", "connect,UTC", "connect,America/Chicago" })
    void shouldPreserveLegacyTemporalSchemasAndValuesAcrossWorkerTimeZones(String mode, String zone) throws Exception {
        var previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone(zone));
            var definition = As400TemporalTest.definition();
            var config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("database.dbname", "TEST")
                    .with("time.precision.mode", mode).build());
            var topics = config.<TableId> getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
            var nativeSchema = new As400NativeTableSchemas(List.of(definition), config, topics).schemaFor(definition.schema().id());
            var legacy = legacySchema(definition, config, topics);
            byte[] bytes = java.util.HexFormat.of().parseHex(As400TemporalTest.entries().get(0).get("hex"));
            var header = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(bytes, 0);
            Object[] values = definition.binding(1024).values(header, bytes, 0);
            assertThat(nativeSchema.keyFromColumnData(values)).isEqualTo(legacy.keyFromColumnData(values));
            assertThat(nativeSchema.valueFromColumnData(values)).isEqualTo(legacy.valueFromColumnData(values));
            // Exercise the actual legacy byte codecs, not merely two schema
            // builders receiving the strict decoder's already-normalized values.
            // The legacy timestamp codec is fixed at precision six. Keep the
            // captured columns at other precisions out of this compatibility claim.
            var decoder = new io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder(() -> {
                throw new AssertionError("Temporal codec comparison must not query a database");
            }, "TEST", new io.debezium.ibmi.db2.journal.retrieve.SchemaCacheHash(), -1, -1);
            Object[] legacyValues = values.clone();
            int position = (int) header.getEntrySpecificDataOffset() + 16;
            for (int i = 0; i < definition.codecs().size(); i++) {
                if (i == 1 || i == 2 || i == 5) {
                    var column = definition.schema().columns().get(i);
                    // SYSCOLUMNS uses TIMESTMP; DatabaseMetaData uses TIMESTAMP.
                    String jdbcTypeName = column.jdbcType() == java.sql.Types.TIMESTAMP ? "TIMESTAMP" : column.typeName();
                    var codec = decoder.toDataType("LIB", "M", column.name(), jdbcTypeName, column.length(), column.scale().orElse(0));
                    legacyValues[i] = decoder.decodeEntry(new com.ibm.as400.access.AS400Structure(new com.ibm.as400.access.AS400DataType[]{ codec }),
                            bytes, position)[0];
                }
                position += definition.codecs().get(i).decoder().getByteLength();
            }
            if (zone.equals("UTC")) {
                assertThat(legacy.valueFromColumnData(legacyValues)).isEqualTo(nativeSchema.valueFromColumnData(values));
            }
            else {
                // Characterize the pre-existing legacy timezone dependence.
                assertThat(legacy.valueFromColumnData(legacyValues)).isNotEqualTo(nativeSchema.valueFromColumnData(values));
            }
            var online = new As400ConnectorConfig(config.getConfig().edit().with("truncate.transactional.enabled", true).build());
            // The old timestamp decoder cannot read the other physical widths;
            // migration refuses them before installing a history/checkpoint.
            assertThrows(IllegalArgumentException.class, () -> new As400NativeTableSchemas(List.of(definition), online, topics));
            var identity = definition.identity();
            var format = identity.recordFormat().split(":");
            var projectedIdentity = new As400TransactionBuffer.TableIdentity(identity.tableId(), identity.physicalFile(), identity.member(),
                    identity.fileJournalId(), identity.memberJournalId(), format[0] + ":" + format[1] + ":48:4:false");
            var projection = new As400BindingDefinition(projectedIdentity, definition.schema().edit()
                    .removeColumn("TS0").removeColumn("TS3").removeColumn("TS9").create(),
                    List.of(definition.codecs().get(0), definition.codecs().get(1), definition.codecs().get(2), definition.codecs().get(5)));
            var onlineSchema = new As400NativeTableSchemas(List.of(projection), online, topics).schemaFor(projection.schema().id());
            var legacyProjection = legacySchema(projection, config, topics);
            Object[] strictProjection = { values[0], values[1], values[2], values[5] };
            Object[] decodedProjection = { legacyValues[0], legacyValues[1], legacyValues[2], legacyValues[5] };
            assertThat(onlineSchema.keyFromColumnData(strictProjection)).isEqualTo(legacyProjection.keyFromColumnData(decodedProjection));
            assertThat(onlineSchema.valueFromColumnData(strictProjection)).isEqualTo(legacyProjection.valueFromColumnData(decodedProjection));
            // Dates around the calendar cutover, leap years and DST gaps/folds,
            // plus IBM i's end-of-day TIME spelling. Compare real byte decoders.
            var temporalKeys = new As400BindingDefinition(projection.identity(), projection.schema().edit()
                    .addColumn(projection.schema().columnWithName("D").edit().optional(false).create())
                    .addColumn(projection.schema().columnWithName("T").edit().optional(false).create())
                    .addColumn(projection.schema().columnWithName("TS6").edit().optional(false).create())
                    .setPrimaryKeyNames("ID", "D", "T", "TS6").create(), projection.codecs());
            var nativeKeys = new As400NativeTableSchemas(List.of(temporalKeys), online, topics).schemaFor(projection.schema().id());
            var legacyKeys = legacySchema(temporalKeys, config, topics);
            for (String timestamp : List.of("0001-01-01-00.00.00.000001", "1500-02-28-23.59.59.999999", "1582-10-10-12.00.00.000000",
                    "1969-12-31-23.59.59.999999", "2000-02-29-12.34.56.123456", "2024-03-10-02.30.00.000000",
                    "2024-11-03-01.30.00.000000", "9999-12-31-23.59.59.999999")) {
                Object[] strict = { 7, null, null, null };
                Object[] old = strict.clone();
                String[] encoded = { "", timestamp.substring(0, 10), timestamp.substring(11, 19), timestamp };
                for (int i = 1; i < 4; i++) {
                    var column = projection.schema().columns().get(i);
                    byte[] data = new com.ibm.as400.access.AS400Text(encoded[i].length(), 37).toBytes(encoded[i]);
                    strict[i] = projection.codecs().get(i).decoder().toObject(data);
                    String type = column.jdbcType() == java.sql.Types.TIMESTAMP ? "TIMESTAMP" : column.typeName();
                    old[i] = decoder.toDataType("LIB", "M", column.name(), type, column.length(), column.scale().orElse(0)).toObject(data);
                }
                if (zone.equals("Asia/Tokyo") && timestamp.startsWith("1500-02-28")) {
                    // The existing JT400/calendar conversion creates Julian
                    // February 29, which the shared Gregorian converter rejects.
                    // Migration must preserve that failure, never coerce a key.
                    assertThrows(java.time.DateTimeException.class, () -> legacyKeys.keyFromColumnData(old));
                    assertThrows(java.time.DateTimeException.class, () -> nativeKeys.keyFromColumnData(strict));
                    continue;
                }
                assertThat(nativeKeys.keyFromColumnData(strict)).as("temporal key %s in %s", timestamp, zone)
                        .isEqualTo(legacyKeys.keyFromColumnData(old));
                assertThat(onlineSchema.valueFromColumnData(strict)).as("temporal value %s in %s", timestamp, zone)
                        .isEqualTo(legacyProjection.valueFromColumnData(old));
            }
            Object[] nulls = { 7, null, null, null };
            assertThat(onlineSchema.valueFromColumnData(nulls)).isEqualTo(legacyProjection.valueFromColumnData(nulls));
            byte[] endOfDay = new com.ibm.as400.access.AS400Text(8, 37).toBytes("24.00.00");
            strictProjection[2] = projection.codecs().get(2).decoder().toObject(endOfDay);
            decodedProjection[2] = decoder.toDataType("LIB", "M", "T", "TIME", 8, 0).toObject(endOfDay);
            assertThat(onlineSchema.valueFromColumnData(strictProjection)).isEqualTo(legacyProjection.valueFromColumnData(decodedProjection));
        }
        finally {
            TimeZone.setDefault(previous);
        }
    }

    private static TableSchema legacySchema(As400BindingDefinition definition, As400ConnectorConfig config, TopicNamingStrategy<TableId> topics) {
        var jdbc = mock(As400JdbcConnection.class);
        when(jdbc.getRealDatabaseName()).thenReturn("TEST");
        var context = new io.debezium.connector.common.CdcSourceTaskContext<>(config.getConfig(), config, config.getCustomMetricTags());
        try (var legacy = new As400DatabaseSchema(config, jdbc, topics, config.schemaNameAdjuster(),
                As400NativeTableSchemas.converterRegistry(config), context)) {
            legacy.forwardSchema(definition.outputTable());
            return legacy.schemaFor(definition.schema().id());
        }
    }

    @Test
    void acquiredMixedColumnsUseRealDebeziumConversionWithoutLosingKeysNullsScaleOrOwnedBytes() throws Exception {
        var definition = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions().get(0);
        var schema = schemas(definition).schemaFor(definition.schema().id());
        Object[] row = row();
        var value = schema.valueFromColumnData(row);
        assertThat(schema.keyFromColumnData(row).getInt32("ID")).isEqualTo(7);
        assertThat(value.getInt16("S")).isEqualTo((short) 12);
        assertThat(value.get("N")).isEqualTo(new BigDecimal("123.45"));
        assertThat(value.get("D")).isEqualTo(new BigDecimal("1234.567"));
        assertThat(value.getString("C")).isEqualTo("A?");
        assertThat(value.getString("V")).isEqualTo("Ä![]");
        assertThat(value.getInt64("L")).isEqualTo(9007199254740993L);
        assertThat(value.getFloat32("F")).isEqualTo(1.25f);
        assertThat(value.getFloat64("G")).isEqualTo(-2.5d);
        Arrays.fill((byte[]) row[6], (byte) 99);
        assertThat(value.get("B") instanceof byte[] ? (byte[]) value.get("B") : ((ByteBuffer) value.get("B")).array()).containsExactly(0, 1, 2, 3);
        row[5] = null;
        assertThat(schema.valueFromColumnData(row).get("V")).isNull();
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-column", "required-null", "wrong-type", "decimal-scale", "binary-length", "nonfinite" })
    void incompleteOrCoercedValuesCannotBecomeApparentlyValidRecords(String fault) throws Exception {
        var definition = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions().get(0);
        var schema = schemas(definition).schemaFor(definition.schema().id());
        Object[] row = row();
        switch (fault) {
            case "missing-column" -> row = new Object[]{ 7 };
            case "required-null" -> row[0] = null;
            case "wrong-type" -> row[0] = "7";
            case "decimal-scale" -> row[3] = new BigDecimal("1234.56");
            case "binary-length" -> row[6] = new byte[]{ 1 };
            case "nonfinite" -> row[9] = Float.NaN;
            default -> throw new AssertionError(fault);
        }
        Object[] bad = row;
        assertThrows(IllegalArgumentException.class, () -> schema.valueFromColumnData(bad));
        assertThrows(IllegalArgumentException.class, () -> schema.keyFromColumnData(bad));
    }

    @ParameterizedTest
    @CsvSource({ "decimal.handling.mode,double", "decimal.handling.mode,string", "binary.handling.mode,hex", "binary.handling.mode,base64",
            "column.exclude.list,LIB.M.C", "column.include.list,LIB.M.ID", "column.mask.with.4.chars,LIB.M.C", "column.truncate.to.2.chars,LIB.M.V",
            "message.key.columns,LIB.M:C", "field.name.adjustment.mode,avro", "field.name.adjustment.mode,avro_unicode",
            "schema.name.adjustment.mode,avro", "schema.name.adjustment.mode,avro_unicode", "datatype.propagate.source.type,.*", "column.propagate.source.type,LIB.M.C" })
    void shouldPreserveLegacyKeyAndValueSchemasWithStandardOutputSettings(String key, String value) throws Exception {
        var definition = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions().get(0);
        if (key.equals("field.name.adjustment.mode")) {
            definition = new As400BindingDefinition(definition.identity(), definition.schema().edit().renameColumn("C", "customer-code").create(), definition.codecs());
        }
        var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "TEST").with(key, value).build();
        var config = new As400ConnectorConfig(raw);
        var topics = config.<TableId> getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        var legacy = legacySchema(definition, config, topics);
        var nativeSchema = new As400NativeTableSchemas(List.of(definition), config, topics).schemaFor(definition.schema().id());
        assertThat(nativeSchema.keySchema()).isEqualTo(legacy.keySchema());
        assertThat(nativeSchema.valueSchema()).isEqualTo(legacy.valueSchema());
        if (key.contains("propagate.source.type")) {
            assertThat(nativeSchema.valueSchema().field("C").schema().parameters()).containsKey("__debezium.source.column.type");
        }
        assertThat(nativeSchema.keyFromColumnData(row())).isEqualTo(legacy.keyFromColumnData(row()));
        assertThat(nativeSchema.valueFromColumnData(row())).isEqualTo(legacy.valueFromColumnData(row()));
        Object[] nullable = row();
        nullable[4] = null;
        assertThat(nativeSchema.keyFromColumnData(nullable)).isEqualTo(legacy.keyFromColumnData(nullable));
        assertThat(nativeSchema.valueFromColumnData(nullable)).isEqualTo(legacy.valueFromColumnData(nullable));
    }

    @Test
    void shouldRejectAMissingCustomConverterImplementationBeforeNativeOutput() throws Exception {
        var definition = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions().get(0);
        var config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("converters", "missing")
                .with("missing.type", "example.MissingConverter").build());
        assertThrows(RuntimeException.class, () -> new As400NativeTableSchemas(List.of(definition), config, null));
    }

    @Test
    void rrnProjectionIsTheOnlyKeyAndPreservesEveryPhysicalColumn() throws Exception {
        var f = new As400NativeBindingAcquisitionTest.Fixture();
        f.keys.clear();
        var definition = f.acquire(true).definitions().get(0);
        var restored = As400BindingDefinition.restore(definition.storedTable());
        var schema = schemas(restored).schemaFor(definition.schema().id());
        Object[] row = As400BindingDefinition.withRrn(row(), java.math.BigInteger.valueOf(9007199254740993L));
        assertThat(schema.keyFromColumnData(row).schema().fields()).hasSize(1);
        assertThat(schema.keyFromColumnData(row).getInt64(As400BindingDefinition.RRN_COLUMN)).isEqualTo(9007199254740993L);
        assertThat(schema.valueFromColumnData(row).getInt32("ID")).isEqualTo(7);
        assertThat(schema.valueFromColumnData(row).getInt64(As400BindingDefinition.RRN_COLUMN)).isEqualTo(9007199254740993L);
        assertThrows(IllegalArgumentException.class, () -> schema.valueFromColumnData(row()));
        row[row.length - 1] = 0L;
        assertThrows(IllegalArgumentException.class, () -> schema.keyFromColumnData(row));
        row[row.length - 1] = "1";
        assertThrows(IllegalArgumentException.class, () -> schema.keyFromColumnData(row));
    }

    private static As400NativeTableSchemas schemas(As400BindingDefinition definition) {
        var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "TEST").build();
        var config = new As400ConnectorConfig(raw);
        return new As400NativeTableSchemas(List.of(definition), config, new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
            @Override
            public String dataChangeTopic(TableId id) {
                return "test." + id.table();
            }
        });
    }

    private static Object[] row() {
        return new Object[]{ 7, (short) 12, new BigDecimal("123.45"), new BigDecimal("1234.567"), "A\u0000   ", "Ä![]",
                new byte[]{ 0, 1, 2, 3 }, new byte[]{ 0, (byte) 255 }, 9007199254740993L, 1.25f, -2.5d };
    }
}
