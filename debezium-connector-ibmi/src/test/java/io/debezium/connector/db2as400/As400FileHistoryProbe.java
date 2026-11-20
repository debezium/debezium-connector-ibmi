/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetStorageReaderImpl;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.config.EmbeddedWorkerConfig;
import io.debezium.embedded.Connect;
import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;

/** Local file consumer for the shipping connector, not a test task or native provider. Explicit owned fixtures only. */
public final class As400FileHistoryProbe {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        if (args.length != 3 || !java.util.Set.of("run", "offset").contains(args[0])) {
            throw new IllegalArgumentException("run|offset CONFIGURATION_FILE OUTPUT_DIRECTORY");
        }
        Properties properties = new Properties();
        try (var reader = Files.newBufferedReader(Path.of(args[1]))) {
            properties.load(reader);
        }
        String library = properties.getProperty("database.schema", "");
        if (!library.matches("DCNC[0-9]{4}") || !properties.getProperty("dcg.journal", "").equals(library + "/QSQJRN")
                || !properties.getProperty("connector.class", "").equals(As400RpcConnector.class.getName())
                || !properties.getProperty("offset.storage", "").equals(FileOffsetBackingStore.class.getName())
                || !properties.getProperty("schema.history.internal", "").equals("io.debezium.storage.file.history.FileSchemaHistory")) {
            throw new IllegalArgumentException("Explicit isolated fixture and ordinary connector/file configuration required");
        }
        Path directory = Path.of(args[2]);
        try (var key = converter(true); var value = converter(false)) {
            if (args[0].equals("offset")) {
                var store = new FileOffsetBackingStore(key);
                store.configure(new EmbeddedWorkerConfig(new HashMap<>(Configuration.from(properties).asMap())));
                store.start();
                try (var reader = new OffsetStorageReaderImpl(store, properties.getProperty("name"), key, value)) {
                    var saved = reader.offset(Map.of("server", properties.getProperty("topic.prefix")));
                    JSON.writeValue(directory.resolve("offset-read.json").toFile(), saved == null ? Map.of() : saved);
                }
                finally {
                    store.stop();
                }
                return;
            }
            Files.createDirectory(directory);
            var finished = new CompletableFuture<Boolean>();
            try (var records = FileChannel.open(directory.resolve("records.jsonl"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                DebeziumEngine<ChangeEvent<SourceRecord, SourceRecord>> engine = DebeziumEngine.create(Connect.class).using(properties)
                        .using((success, message, failure) -> {
                            if (failure != null) {
                                failure.printStackTrace(); // Controller redacts retained process output.
                            }
                            finished.complete(success);
                        }).notifying((events, committer) -> {
                            for (var event : events) {
                                SourceRecord record = event.value();
                                var saved = new HashMap<String, Object>();
                                saved.put("topic", record.topic());
                                saved.put("partition", record.sourcePartition());
                                saved.put("offset", record.sourceOffset());
                                saved.put("key", decoded(key.fromConnectData(record.topic(), record.keySchema(), record.key())));
                                saved.put("value", decoded(value.fromConnectData(record.topic(), record.valueSchema(), record.value())));
                                try {
                                    byte[] json = JSON.writeValueAsBytes(saved);
                                    ByteBuffer bytes = ByteBuffer.allocate(json.length + 1).put(json).put((byte) '\n').flip();
                                    while (bytes.hasRemaining()) {
                                        records.write(bytes);
                                    }
                                    records.force(true);
                                }
                                catch (java.io.IOException e) {
                                    throw new IllegalStateException("Local consumer failed before acknowledgement", e);
                                }
                                committer.markProcessed(event);
                            }
                            committer.markBatchFinished();
                        }).build();
                var runner = CompletableFuture.runAsync(engine);
                boolean closeRequested = false;
                try {
                    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(12);
                    while (!finished.isDone() && !Files.exists(directory.resolve("stop")) && System.nanoTime() < deadline) {
                        Thread.sleep(100);
                    }
                    if (!finished.isDone()) {
                        closeRequested = true;
                        engine.close();
                    }
                    runner.get(45, TimeUnit.SECONDS);
                    JSON.writeValue(directory.resolve("result.json").toFile(), Map.of("success", finished.get(5, TimeUnit.SECONDS)));
                    if (!finished.get()) {
                        throw new IllegalStateException("Native engine failed; inspect redacted process log");
                    }
                }
                finally {
                    if (!closeRequested && !finished.isDone()) {
                        engine.close();
                    }
                }
            }
        }
    }

    private static Object decoded(byte[] bytes) {
        try {
            return bytes == null ? null : JSON.readTree(bytes);
        }
        catch (java.io.IOException e) {
            throw new IllegalStateException("Invalid local consumer JSON", e);
        }
    }

    private static JsonConverter converter(boolean key) {
        var converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", false), key);
        return converter;
    }
}
