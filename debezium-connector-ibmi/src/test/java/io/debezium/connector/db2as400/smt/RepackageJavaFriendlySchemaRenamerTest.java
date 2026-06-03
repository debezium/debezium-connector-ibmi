/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.smt;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTests")
public class RepackageJavaFriendlySchemaRenamerTest {
    RepackageJavaFriendlySchemaRenamer<SourceRecord> rr = new RepackageJavaFriendlySchemaRenamer<>();

    @BeforeEach
    public void setUp() throws Exception {

    }

    @Test
    public void testApplyWithStruct() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(RepackageJavaFriendlySchemaRenamer.PACKAGE_CONFIG, "com.fnz.debezium");
        rr.configure(config);

        SourceRecord cr = structRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);

        assertThat(newSchemaRecord.keySchema().name()).isEqualTo("com.fnz.debezium.TableKey");
        assertThat(newSchemaRecord.valueSchema().name()).isEqualTo("com.fnz.debezium.TableValue");
    }

    @Test
    public void testApplyWithNullKeySchema() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(RepackageJavaFriendlySchemaRenamer.PACKAGE_CONFIG, "com.fnz.debezium");
        rr.configure(config);

        SourceRecord cr = nullSchemaKeyRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);

        assertThat(newSchemaRecord.keySchema()).isNull();
        assertThat(newSchemaRecord.valueSchema().name()).isEqualTo("com.fnz.debezium.TableValue");
    }

    @Test
    public void testApplyWithSimpleKeySchema() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(RepackageJavaFriendlySchemaRenamer.PACKAGE_CONFIG, "com.fnz.debezium");
        rr.configure(config);

        SourceRecord cr = simpleScheamKeyRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);

        assertThat(newSchemaRecord.keySchema()).isEqualTo(Schema.INT64_SCHEMA);
        assertThat(newSchemaRecord.valueSchema().name()).isEqualTo("com.fnz.debezium.TableValue");
    }

    private SourceRecord nullSchemaKeyRecord() {
        Schema keySchema = null;
        Long key = Long.valueOf(0);
        Schema valueSchema = SchemaBuilder.struct().name("database.com.schema.TABLE.Value").field("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema);
        value.put("name", "name value");

        Map<String, Integer> partition = new HashMap<>();
        Map<String, Integer> offset = new HashMap<>();

        return new SourceRecord(partition, offset, "my.topic", 0, keySchema, key, valueSchema, value, Long.valueOf(123456l));
    }

    private SourceRecord simpleScheamKeyRecord() {
        Schema keySchema = SchemaBuilder.INT64_SCHEMA;
        Long key = Long.valueOf(0);
        Schema valueSchema = SchemaBuilder.struct().name("database.com.schema.TABLE.Value").field("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema);
        value.put("name", "name value");

        Map<String, Integer> partition = new HashMap<>();
        Map<String, Integer> offset = new HashMap<>();

        return new SourceRecord(partition, offset, "my.topic", 0, keySchema, key, valueSchema, value, Long.valueOf(123456l));
    }

    private SourceRecord structRecord() {
        Schema keySchema = SchemaBuilder.struct().name("database.com.schema.TABLE.Key").field("id", SchemaBuilder.INT64_SCHEMA).build();
        Struct key = new Struct(keySchema);
        key.put("id", Long.valueOf(0));
        Schema valueSchema = SchemaBuilder.struct().name("database.com.schema.TABLE.Value").field("name", SchemaBuilder.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema);
        value.put("name", "name value");

        Map<String, Integer> partition = new HashMap<>();
        Map<String, Integer> offset = new HashMap<>();

        return new SourceRecord(partition, offset, "my.topic", 0, keySchema, key, valueSchema, value, Long.valueOf(123456l));
    }

    @Test
    public void testToCamelCaseNoUnderscores() {
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase("abcdef")).isEqualTo("Abcdef");
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase("ABCDEF")).isEqualTo("Abcdef");
    }

    @Test
    public void testToCamelCaseUnderscores() {
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase("abcdef_")).isEqualTo("Abcdef");
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase("ABCDEF_GHI")).isEqualTo("AbcdefGhi");
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase("_abc")).isEqualTo("Abc");
    }

    @Test
    public void testToCamelCaseUnderscoresSingleChar() {
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase("a")).isEqualTo("A");
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase("_")).isEqualTo("_");
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase(null)).isNull();
    }
}
