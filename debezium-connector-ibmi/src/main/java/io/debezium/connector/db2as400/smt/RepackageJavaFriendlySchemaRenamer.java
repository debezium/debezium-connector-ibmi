/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.smt;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2as400.Module;

public class RepackageJavaFriendlySchemaRenamer<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
    private static final Logger log = LoggerFactory.getLogger(RepackageJavaFriendlySchemaRenamer.class);

    public static final String OVERVIEW_DOC = "<p/>regex schema renamer</p>";

    public static final String PACKAGE_CONFIG = "package";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(PACKAGE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.Validator() {
                @Override
                public void ensureValid(String name, Object valueObject) {
                    String value = (String) valueObject;
                    if (value == null || value.isEmpty()) {
                        throw new ConfigException("Must specify replacement e.g. 'com.company.schema'");
                    }
                }

                @Override
                public String toString() {
                    return "Replacement string";
                }
            },
                    ConfigDef.Importance.HIGH, "package to replace the current prefix with");

    private static final String PURPOSE = "rename schemas";

    private String packageName;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        packageName = config.getString(PACKAGE_CONFIG) + ".";
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (!hasSchema(record)) {
            return record;
        }

        return applyWithSchema(record);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    boolean hasSchema(R record) {
        Schema key = record.keySchema();
        Schema value = record.valueSchema();
        return ((key != null && Type.STRUCT.equals(key.type())) || (value != null && Type.STRUCT.equals(value.type())));
    }

    private R applyWithSchema(R record) {
        RenamedSchema renamedKeySchema = renameSchema(record.keySchema());
        RenamedSchema renamedValueSchema = renameSchema(record.valueSchema());
        if (renamedKeySchema.isRenamed || renamedValueSchema.isRenamed) {
            Object key = updateSchemaIn(record.key(), renamedKeySchema.schema);
            Object value = updateSchemaIn(record.value(), renamedValueSchema.schema);
            R renamedRecord = record.newRecord(record.topic(), record.kafkaPartition(), renamedKeySchema.schema, key, renamedValueSchema.schema, value,
                    record.timestamp());
            return renamedRecord;
        }
        return record;
    }

    static Object updateSchemaIn(Object keyOrValue, Schema updatedSchema) {
        if (keyOrValue instanceof Struct) {
            Struct origStruct = (Struct) keyOrValue;
            Struct newStruct = new Struct(updatedSchema);
            for (Field field : updatedSchema.fields()) {
                newStruct.put(field, origStruct.get(field));
            }
            return newStruct;
        }
        return keyOrValue;
    }

    RenamedSchema renameSchema(Schema schema) {
        String newName = newSchemaName(schema);
        if (newName != null) {
            Schema renamed = schemaUpdateCache.get(schema);
            if (renamed == null) {
                renamed = renameSchema(schema, newName);
                schemaUpdateCache.put(schema, renamed);
            }

            return new RenamedSchema(true, renamed);
        }
        return new RenamedSchema(false, schema);
    }

    private static final Pattern regex = Pattern.compile(".*\\.([^.]*)\\.(Value|Key)");

    String newSchemaName(Schema schema) {
        if (schema == null) {
            return null;
        }
        String name = schema.name();
        if (name == null) {
            return null;
        }
        Matcher m = regex.matcher(name);
        StringBuilder sb = new StringBuilder(packageName);
        if (m.find()) {
            String table = name.substring(m.start(1), m.end(1));
            sb.append(toCamelCase(table));
            sb.append(m.group(2));
            return sb.toString();
        }
        return name;
    }

    public static String toCamelCase(String x) {
        if (x == null) {
            return null;
        }
        int len = x.length();
        if (len == 1) {
            return x.toUpperCase();
        }

        StringBuilder sb = new StringBuilder();
        loop: for (int offset = 0; offset < len;) {
            int nextOffset = x.indexOf('_', offset);
            switch (nextOffset) {
                case 0: // drop first character
                    offset = nextOffset + 1;
                    break;
                case -1: // no more found exit
                    sb.append(x.substring(offset, offset + 1).toUpperCase());
                    sb.append(x.substring(offset + 1, len).toLowerCase());
                    break loop;
                default:
                    sb.append(x.substring(offset, offset + 1).toUpperCase());
                    sb.append(x.substring(offset + 1, nextOffset).toLowerCase());
                    offset = nextOffset + 1;
                    break;
            }
        }
        return sb.toString();
    }

    Schema renameSchema(Schema schema, String newName) {
        if (schema == null) {
            return schema;
        }
        SchemaBuilder builder = new SchemaBuilder(schema.type());
        builder.name(newName);
        builder.version(schema.version());
        builder.doc(schema.doc());
        for (Field f : schema.fields()) {
            builder.field(f.name(), f.schema());
        }

        final Map<String, String> params = schema.parameters();
        if (params != null) {
            builder.parameters(params);
        }

        return builder.build();
    }

    public class RenamedSchema {
        public final Boolean isRenamed;
        public final Schema schema;

        public RenamedSchema(Boolean isRenamed, Schema schema) {
            this.isRenamed = isRenamed;
            this.schema = schema;
        }
    }

    public static class SimpleConfig extends AbstractConfig {

        public SimpleConfig(ConfigDef configDef, Map<?, ?> originals) {
            super(configDef, originals, false);
        }

    }

    public String capitalizeFirstLowerRest(String original) {
        if (original == null || original.length() == 0) {
            return original;
        }
        return original.substring(0, 1).toUpperCase() + original.substring(1).toLowerCase();
    }

    @Override
    public String version() {
        return Module.version();
    }
}
