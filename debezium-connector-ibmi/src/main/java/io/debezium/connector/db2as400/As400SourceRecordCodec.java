/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400SpillCodec.string;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

/** Owned Connect output in scratch storage, including tombstones, schemas and ordered headers. */
final class As400SourceRecordCodec {
    private static final int MAX_DEPTH = 64;
    private static final int CACHE_BYTES = 256 * 1024;
    private static final As400SpillStore.Codec<Schema> SCHEMA = As400SpillCodec.codec((out, schema) -> schema(out, schema, 0), in -> schema(in, 0));
    static final As400SpillStore.Codec<SourceRecord> RECORD = new As400SourceRecordCodec(null).codec();
    private final Map<String, byte[]> schemas;
    private final Map<Schema, String> schemaIds = new LinkedHashMap<>();
    private final Map<String, CachedSchema> cache = new LinkedHashMap<>(16, 0.75f, true);
    private int cachedBytes;

    static As400SpillStore.Codec<SourceRecord> stored(As400SpillStore store) {
        return new As400SourceRecordCodec(store.map("output-schemas", As400SpillCodec.STRING, As400SpillCodec.BYTES)).codec();
    }

    private As400SourceRecordCodec(Map<String, byte[]> schemas) {
        this.schemas = schemas;
    }

    private As400SpillStore.Codec<SourceRecord> codec() {
        return As400SpillCodec.codec((out, record) -> {
            value(out, null, record.sourcePartition(), 0);
            value(out, null, record.sourceOffset(), 0);
            string(out, record.topic());
            As400SpillCodec.scalar(out, record.kafkaPartition());
            reference(out, record.keySchema());
            value(out, record.keySchema(), record.key(), 0);
            reference(out, record.valueSchema());
            value(out, record.valueSchema(), record.value(), 0);
            As400SpillCodec.scalar(out, record.timestamp());
            out.writeInt(record.headers().size());
            for (var header : record.headers()) {
                string(out, header.key());
                reference(out, header.schema());
                value(out, header.schema(), header.value(), 0);
            }
        }, in -> {
            @SuppressWarnings("unchecked")
            Map<String, ?> partition = (Map<String, ?>) value(in, null, 0);
            @SuppressWarnings("unchecked")
            Map<String, ?> offset = (Map<String, ?>) value(in, null, 0);
            String topic = string(in);
            Integer kafkaPartition = (Integer) As400SpillCodec.scalar(in);
            Schema keySchema = reference(in);
            Object key = value(in, keySchema, 0);
            Schema valueSchema = reference(in);
            Object value = value(in, valueSchema, 0);
            Long timestamp = (Long) As400SpillCodec.scalar(in);
            var headers = new ConnectHeaders();
            int count = count(in);
            for (int i = 0; i < count; i++) {
                String name = string(in);
                Schema headerSchema = reference(in);
                headers.add(name, value(in, headerSchema, 0), headerSchema);
            }
            return new SourceRecord(partition == null ? null : Map.copyOf(partition), offset == null ? null : Map.copyOf(offset),
                    topic, kafkaPartition, keySchema, key, valueSchema, value, timestamp, headers);
        });
    }

    private record CachedSchema(Schema schema, int bytes) {
    }

    private void reference(DataOutputStream out, Schema schema) throws IOException {
        if (schemas == null) {
            schema(out, schema, 0);
            return;
        }
        if (schema == null) {
            string(out, null);
            return;
        }
        String id = schemaIds.get(schema);
        if (id == null) {
            byte[] encoded = SCHEMA.encode(schema);
            id = hash(encoded);
            byte[] known = schemas.get(id);
            if (known == null) {
                schemas.put(id, encoded);
            }
            else if (!java.util.Arrays.equals(known, encoded)) {
                throw new IOException("Conflicting spill schema identity");
            }
            // Own mutable defaults as well as record values. Retaining the
            // caller's schema here could change a prepared record after spill.
            remember(id, SCHEMA.decode(encoded), encoded.length);
        }
        string(out, id);
    }

    private Schema reference(DataInputStream in) throws IOException {
        if (schemas == null) {
            return schema(in, 0);
        }
        String id = string(in);
        if (id == null) {
            return null;
        }
        CachedSchema known = cache.get(id);
        if (known != null) {
            return known.schema();
        }
        byte[] encoded = schemas.get(id);
        if (encoded == null || !hash(encoded).equals(id)) {
            throw new IOException("Missing or changed spill schema");
        }
        Schema schema = SCHEMA.decode(encoded);
        remember(id, schema, encoded.length);
        return schema;
    }

    private void remember(String id, Schema schema, int bytes) {
        if (bytes > CACHE_BYTES || cache.containsKey(id)) {
            return;
        }
        while (!cache.isEmpty() && (cachedBytes + bytes > CACHE_BYTES || cache.size() >= 64)) {
            var iterator = cache.entrySet().iterator();
            var oldest = iterator.next();
            cachedBytes -= oldest.getValue().bytes();
            schemaIds.remove(oldest.getValue().schema());
            iterator.remove();
        }
        cache.put(id, new CachedSchema(schema, bytes));
        schemaIds.put(schema, id);
        cachedBytes += bytes;
    }

    private static String hash(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void depth(int depth) throws IOException {
        if (depth > MAX_DEPTH) {
            throw new IOException("Recursive or oversized Connect schema/value in spill storage");
        }
    }

    private static int count(DataInputStream in) throws IOException {
        int count = in.readInt();
        if (count < 0 || count > in.available()) {
            throw new IOException("Invalid Connect collection length in spill storage");
        }
        return count;
    }

    private static void schema(DataOutputStream out, Schema schema, int depth) throws IOException {
        depth(depth);
        out.writeBoolean(schema != null);
        if (schema == null) {
            return;
        }
        string(out, schema.type().name());
        string(out, schema.name());
        string(out, schema.doc());
        As400SpillCodec.scalar(out, schema.version());
        out.writeBoolean(schema.isOptional());
        value(out, null, schema.parameters(), depth + 1);
        switch (schema.type()) {
            case ARRAY -> schema(out, schema.valueSchema(), depth + 1);
            case MAP -> {
                schema(out, schema.keySchema(), depth + 1);
                schema(out, schema.valueSchema(), depth + 1);
            }
            case STRUCT -> {
                out.writeInt(schema.fields().size());
                for (var field : schema.fields()) {
                    string(out, field.name());
                    schema(out, field.schema(), depth + 1);
                }
            }
            default -> {
            }
        }
        value(out, schema, schema.defaultValue(), depth + 1);
    }

    private static Schema schema(DataInputStream in, int depth) throws IOException {
        depth(depth);
        if (!in.readBoolean()) {
            return null;
        }
        Schema.Type type = Schema.Type.valueOf(string(in));
        String name = string(in);
        String doc = string(in);
        Integer version = (Integer) As400SpillCodec.scalar(in);
        boolean optional = in.readBoolean();
        @SuppressWarnings("unchecked")
        Map<String, String> parameters = (Map<String, String>) value(in, null, depth + 1);
        SchemaBuilder builder = switch (type) {
            case ARRAY -> SchemaBuilder.array(schema(in, depth + 1));
            case MAP -> SchemaBuilder.map(schema(in, depth + 1), schema(in, depth + 1));
            default -> new SchemaBuilder(type);
        };
        if (type == Schema.Type.STRUCT) {
            int count = count(in);
            for (int i = 0; i < count; i++) {
                builder.field(string(in), schema(in, depth + 1));
            }
        }
        if (name != null) {
            builder.name(name);
        }
        if (doc != null) {
            builder.doc(doc);
        }
        if (version != null) {
            builder.version(version);
        }
        if (optional) {
            builder.optional();
        }
        if (parameters != null) {
            builder.parameters(parameters);
        }
        Object defaultValue = value(in, builder.build(), depth + 1);
        if (defaultValue != null) {
            builder.defaultValue(defaultValue);
        }
        return builder.build();
    }

    private static void value(DataOutputStream out, Schema schema, Object value, int depth) throws IOException {
        depth(depth);
        if (value instanceof Struct struct) {
            out.writeByte(1);
            if (schema == null || schema.type() != Schema.Type.STRUCT || !schema.equals(struct.schema())) {
                throw new IOException("Spilled Struct does not match its schema");
            }
            for (var field : schema.fields()) {
                value(out, field.schema(), struct.getWithoutDefault(field.name()), depth + 1);
            }
        }
        else if (value instanceof List<?> list) {
            out.writeByte(2);
            out.writeInt(list.size());
            for (Object element : list) {
                value(out, schema == null ? null : schema.valueSchema(), element, depth + 1);
            }
        }
        else if (value instanceof Map<?, ?> map) {
            out.writeByte(3);
            out.writeInt(map.size());
            for (var entry : map.entrySet()) {
                value(out, schema == null ? null : schema.keySchema(), entry.getKey(), depth + 1);
                value(out, schema == null ? null : schema.valueSchema(), entry.getValue(), depth + 1);
            }
        }
        else if (value instanceof ByteBuffer bytes) {
            out.writeByte(4);
            var copy = bytes.duplicate();
            byte[] owned = new byte[copy.remaining()];
            copy.get(owned);
            As400SpillCodec.bytes(out, owned);
        }
        else {
            out.writeByte(0);
            As400SpillCodec.scalar(out, value);
        }
    }

    private static Object value(DataInputStream in, Schema schema, int depth) throws IOException {
        depth(depth);
        return switch (in.readUnsignedByte()) {
            case 0 -> As400SpillCodec.scalar(in);
            case 1 -> {
                var struct = new Struct(schema);
                for (var field : schema.fields()) {
                    Object value = value(in, field.schema(), depth + 1);
                    // Unset required fields with defaults are represented as null
                    // in Struct, although put(field, null) rejects them.
                    if (value != null) {
                        struct.put(field, value);
                    }
                }
                yield struct;
            }
            case 2 -> {
                int count = count(in);
                var list = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    list.add(value(in, schema == null ? null : schema.valueSchema(), depth + 1));
                }
                yield list;
            }
            case 3 -> {
                int count = count(in);
                var map = new LinkedHashMap<>();
                for (int i = 0; i < count; i++) {
                    Object key = value(in, schema == null ? null : schema.keySchema(), depth + 1);
                    Object value = value(in, schema == null ? null : schema.valueSchema(), depth + 1);
                    if (map.containsKey(key)) {
                        throw new IOException("Duplicate Connect map key in spill storage");
                    }
                    map.put(key, value);
                }
                yield map;
            }
            case 4 -> ByteBuffer.wrap(As400SpillCodec.bytes(in));
            default -> throw new IOException("Unsupported Connect value in spill storage");
        };
    }
}
