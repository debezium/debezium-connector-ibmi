/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.document.Array;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.document.Value;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;

/**
 * Connector-owned binding encoding v1. Field names and default representations
 * are frozen here, independent of core's table-change serializer. New manifests
 * store sorted JSON in their position; legacy manifests use the original field
 * order for their existing fingerprint. Neither path reads today's catalog.
 */
final class As400BindingEncoding {
    static final String VERSION = "1";
    private static final ObjectMapper JSON = new ObjectMapper();

    private As400BindingEncoding() {
    }

    static String encode(List<As400BindingDefinition> definitions, boolean legacy) throws Exception {
        Document document = Document.create();
        document.setArray("tables", tables(definitions));
        return legacy ? DocumentWriter.defaultWriter().write(document) : JSON.writeValueAsString(sorted(Value.create(document)));
    }

    static List<As400BindingDefinition> decode(String encoded) throws Exception {
        Document document = DocumentReader.defaultReader().read(encoded);
        var definitions = decode(document.getArray("tables"));
        require(encode(definitions, false).equals(encoded), "Noncanonical connector binding encoding");
        return definitions;
    }

    static List<As400BindingDefinition> decode(Array changes) {
        require(changes != null && !changes.isEmpty(), "Incomplete binding list");
        var definitions = new ArrayList<As400BindingDefinition>();
        for (var entry : changes) {
            Document change = entry.getValue().asDocument();
            require("CREATE".equals(change.getString("type")) && !change.has("previousId"), "Binding history is immutable");
            Document table = change.getDocument("table");
            var editor = Table.editor().tableId(TableId.parse(change.getString("id"), false))
                    .setDefaultCharsetName(table.getString("defaultCharsetName"));
            if (table.getString("comment") != null) {
                editor.setComment(table.getString("comment"));
            }
            for (var value : table.getArray("columns")) {
                Document c = value.getValue().asDocument();
                var column = Column.editor().name(c.getString("name")).jdbcType(c.getInteger("jdbcType"))
                        .type(c.getString("typeName"), c.getString("typeExpression")).charsetName(c.getString("charsetName"))
                        .position(c.getInteger("position")).optional(c.getBoolean("optional"))
                        .autoIncremented(c.getBoolean("autoIncremented")).generated(c.getBoolean("generated"));
                if (c.has("nativeType")) {
                    column.nativeType(c.getInteger("nativeType"));
                }
                if (c.has("length")) {
                    column.length(c.getInteger("length"));
                }
                if (c.has("scale")) {
                    column.scale(c.getInteger("scale"));
                }
                if (c.getString("comment") != null) {
                    column.comment(c.getString("comment"));
                }
                if (c.getString("defaultValueExpression") != null || Boolean.TRUE.equals(c.getBoolean("hasDefaultValue"))) {
                    column.defaultValueExpression(c.getString("defaultValueExpression"));
                }
                if (c.getArray("enumValues") != null) {
                    column.enumValues(c.getArray("enumValues").streamValues().map(Value::asString).toList());
                }
                editor.addColumn(column.create());
            }
            editor.setPrimaryKeyNames(table.getArray("primaryKeyColumnNames").streamValues().map(Value::asString).toList());
            for (var value : table.getArray("attributes")) {
                Document a = value.getValue().asDocument();
                editor.addAttribute(Attribute.editor().name(a.getString("name")).value(a.getString("value")).create());
            }
            definitions.add(As400BindingDefinition.restore(editor.create()));
        }
        // Includes unknown fields, null-vs-absent distinctions, duplicate or
        // reordered descriptors and lossy coercions. No silently dropped fields.
        require(tables(definitions).equals(changes), "Incomplete or unsupported binding fields");
        return List.copyOf(definitions);
    }

    static Array tables(List<As400BindingDefinition> definitions) {
        return Array.create(definitions.stream().map(d -> {
            Table table = d.storedTable();
            Document change = Document.create();
            change.setString("type", "CREATE");
            change.setString("id", table.id().toDoubleQuotedString());
            Document value = Document.create();
            value.set("defaultCharsetName", table.defaultCharsetName());
            value.setString("comment", table.comment());
            value.set("primaryKeyColumnNames", Array.create(table.primaryKeyColumnNames()));
            value.setArray("columns", Array.create(table.columns().stream().map(As400BindingEncoding::column).toList()));
            value.setArray("attributes", Array.create(table.attributes().stream().map(a -> {
                Document attribute = Document.create();
                attribute.setString("name", a.name());
                attribute.setString("value", a.value());
                return attribute;
            }).toList()));
            change.setDocument("table", value);
            return change;
        }).toList());
    }

    private static Document column(Column column) {
        Document value = Document.create();
        value.setString("name", column.name());
        value.setNumber("jdbcType", column.jdbcType());
        if (column.nativeType() != Column.UNSET_INT_VALUE) {
            value.setNumber("nativeType", column.nativeType());
        }
        value.setString("typeName", column.typeName());
        value.setString("typeExpression", column.typeExpression());
        value.setString("charsetName", column.charsetName());
        if (column.length() != Column.UNSET_INT_VALUE) {
            value.setNumber("length", column.length());
        }
        column.scale().ifPresent(s -> value.setNumber("scale", s));
        value.setNumber("position", column.position());
        value.setBoolean("optional", column.isOptional());
        value.setBoolean("autoIncremented", column.isAutoIncremented());
        value.setBoolean("generated", column.isGenerated());
        value.setString("comment", column.comment());
        value.setBoolean("hasDefaultValue", column.hasDefaultValue());
        column.defaultValueExpression().ifPresent(d -> value.setString("defaultValueExpression", d));
        if (column.enumValues() != null) {
            value.setArray("enumValues", column.enumValues().toArray());
        }
        return value;
    }

    private static Object sorted(Value value) {
        if (value.isDocument()) {
            Map<String, Object> result = new TreeMap<>();
            value.asDocument().forEach(field -> result.put(field.getName().toString(), sorted(field.getValue())));
            return result;
        }
        if (value.isArray()) {
            return value.asArray().streamValues().map(As400BindingEncoding::sorted).toList();
        }
        return value.asObject();
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
