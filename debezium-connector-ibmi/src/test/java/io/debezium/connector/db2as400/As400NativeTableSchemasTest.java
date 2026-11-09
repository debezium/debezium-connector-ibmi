/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.schema.AbstractTopicNamingStrategy;

class As400NativeTableSchemasTest {
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
        assertThat(value.getString("C")).isEqualTo("A\u0000   ");
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
    @ValueSource(strings = { "decimal.handling.mode", "binary.handling.mode", "converters", "message.key.columns", "column.exclude.list", "column.mask.with.4.chars" })
    void unqualifiedOutputTransformationsCannotSilentlyChangeTheFixedNativeProfile(String key) throws Exception {
        var definition = new As400NativeBindingAcquisitionTest.Fixture().acquire().definitions().get(0);
        String value = key.equals("decimal.handling.mode") ? "double"
                : key.equals("binary.handling.mode") ? "hex" : key.equals("message.key.columns") ? ".*:ID" : "unqualified";
        var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "AEQA").with(key, value).build();
        var config = new As400ConnectorConfig(raw);
        assertThrows(IllegalArgumentException.class, () -> new As400NativeTableSchemas(List.of(definition), config, null));
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
        var raw = Configuration.create().with("topic.prefix", "test").with("database.dbname", "AEQA").build();
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
