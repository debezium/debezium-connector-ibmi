/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.smt;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.hamcrest.core.IsNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class RepackageJavaFriendlySchemaRenamerTest {
    RepackageJavaFriendlySchemaRenamer<SourceRecord> rr = new RepackageJavaFriendlySchemaRenamer<>();

    @BeforeEach
    public void setUp() {
        rr.configure(Map.of(RepackageJavaFriendlySchemaRenamer.PACKAGE_CONFIG, "com.fnz.debezium"));
    }

    @Test
    public void shouldRenameKeyAndValueSchemasWhenRecordContainsStructs() throws Exception {
        SourceRecord cr = structRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);

        assertThat(newSchemaRecord.keySchema().name(), is("com.fnz.debezium.TableKey"));
        assertThat(newSchemaRecord.valueSchema().name(), is("com.fnz.debezium.TableValue"));
    }

    @Test
    public void shouldPreserveAbsentKeySchemaWhenRenamingValue() throws Exception {
        SourceRecord cr = nullSchemaKeyRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);

        assertThat(newSchemaRecord.keySchema(), IsNull.nullValue());
        assertThat(newSchemaRecord.valueSchema().name(), is("com.fnz.debezium.TableValue"));
    }

    @Test
    public void shouldPreservePrimitiveKeySchemaWhenRenamingValue() throws Exception {
        SourceRecord cr = simpleSchemaKeyRecord();
        SourceRecord newSchemaRecord = rr.apply(cr);

        assertThat(newSchemaRecord.keySchema(), is(Schema.INT64_SCHEMA));
        assertThat(newSchemaRecord.valueSchema().name(), is("com.fnz.debezium.TableValue"));
    }

    private SourceRecord nullSchemaKeyRecord() {
        return record(null, 0L);
    }

    private SourceRecord simpleSchemaKeyRecord() {
        return record(Schema.INT64_SCHEMA, 0L);
    }

    private SourceRecord structRecord() {
        var schema = SchemaBuilder.struct().name("database.com.schema.TABLE.Key").field("id", Schema.INT64_SCHEMA).build();
        return record(schema, new Struct(schema).put("id", 0L));
    }

    private SourceRecord record(Schema keySchema, Object key) {
        var schema = SchemaBuilder.struct().name("database.com.schema.TABLE.Value").field("name", Schema.OPTIONAL_STRING_SCHEMA).build();
        return new SourceRecord(Map.of(), Map.of(), "my.topic", 0, keySchema, key, schema, new Struct(schema).put("name", "name value"), 123456L);
    }

    @ParameterizedTest
    @CsvSource({ "abcdef,Abcdef", "ABCDEF,Abcdef", "abcdef_,Abcdef", "ABCDEF_GHI,AbcdefGhi", "_abc,Abc", "a,A", "_,_" })
    void shouldNormalizeSchemaWordsWhenConvertingToCamelCase(String input, String expected) {
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase(input), is(expected));
    }

    @Test
    void shouldPreserveNullWhenSchemaNameIsAbsent() {
        assertThat(RepackageJavaFriendlySchemaRenamer.toCamelCase(null), is(nullValue()));
    }
}
