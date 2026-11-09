/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.Converter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;

class As400OutputContractTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = { "unknown-table", "missing-eof", "invalid-row" })
    void filteringCannotHideInvalidOrIncompleteControlledSnapshotInput(String fault) throws Exception {
        var fixture = new As400ChunkedInitializationFixture(directory, 1) {
            @Override
            public void scanBaseline(String epoch, List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
                var table = fault.equals("unknown-table") ? new io.debezium.relational.TableId(null, "DCOC1107", "UNKNOWN") : definitions.get(0).schema().id();
                Object value = fault.equals("invalid-row") ? "wrong" : 1;
                consumer.chunk(table, List.of(new Row(table, As400RowData.copyOf(new Object[]{ value }, 65536), java.time.Instant.EPOCH)));
                consumer.complete(definitions.get(1).schema().id()); // Missing EOF of the excluded first table.
            }
        };
        try (var run = new As400InitializationTaskTest.Running(fixture, null, input -> input)) {
            var properties = As400RecoveryTaskTest.properties();
            properties.put("table.include.list", "DCOC1107\\.B");
            run.started = true;
            run.task.start(properties);
            if (fault.equals("missing-eof")) {
                var chunk = run.through("CHUNK");
                assertThat(chunk).noneMatch(r -> r.value() instanceof Struct);
                run.ack(chunk);
            }
            assertThrows(Exception.class, run::next);
            assertThat(run.durable.get() == null || !run.durable.get().containsKey("offset.version")).isTrue();
            assertThat(fixture.handoffs).hasValue(0);
        }
    }

    @Test
    void tableSelectionFiltersSnapshotAndCommittedOutputWithoutFilteringRecovery() throws Exception {
        try (var run = new As400InitializationTaskTest.Running(directory, null)) {
            var properties = As400RecoveryTaskTest.properties();
            properties.put("table.include.list", "DCOC1107\\.B");
            run.started = true;
            run.task.start(properties);
            var baseline = run.through("BASELINE");
            var rows = baseline.stream().filter(r -> r.value() instanceof Struct).toList();
            assertThat(rows).hasSize(3).allMatch(r -> r.topic().endsWith(".B"));
            assertThat(((Struct) rows.get(2).value()).getStruct("source").getString("snapshot")).isEqualTo("last");
            run.ack(baseline);
            var initial = run.through("INITIAL");
            var recovered = run.input.fixture.history().recover(initial.get(0).sourceOffset(), run.input.journalIdentity(), As400BindingHistoryTest.Fixture.POLICY);
            assertThat(recovered.definitions()).hasSize(2); // Both bindings and the full journal remain recoverable.
            run.ack(initial);
            var stream = run.throughCheckpoint("42");
            assertThat(As400BindingHistoryTest.data(stream)).isEmpty(); // A's commit still advances W.
            run.ack(stream);
            assertThat(run.durable.get().get("completed_through.sequence")).isEqualTo("42");
            assertThat(run.input.handoffs).hasValue(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "json-schema", "json", "avro" })
    void actualTaskSnapshotAndStreamingRecordsUseStandardConverters(String encoding) throws Exception {
        Converter key = converter(encoding, true);
        Converter value = converter(encoding, false);
        try (var run = new As400InitializationTaskTest.Running(directory, null)) {
            run.start();
            var baseline = run.through("BASELINE");
            roundTrip(baseline, key, value);
            run.ack(baseline);
            var initial = run.through("INITIAL");
            roundTrip(initial, key, value);
            run.ack(initial);
            var stream = run.throughCheckpoint("42");
            roundTrip(stream, key, value);
            run.ack(stream);
            assertThat(run.durable.get().get("completed_through.sequence")).isEqualTo("42");
        }
    }

    static Converter converter(String encoding, boolean key) {
        if (encoding.equals("avro")) {
            var converter = new AvroConverter(new MockSchemaRegistryClient());
            converter.configure(Map.of("schema.registry.url", "mock://dcg-contract", "connect.meta.data", true), key);
            return converter;
        }
        var converter = new JsonConverter();
        converter.configure(Map.of("schemas.enable", encoding.equals("json-schema")), key);
        return converter;
    }

    static void roundTrip(List<SourceRecord> records, Converter key, Converter value) {
        for (var record : records) {
            var keyBytes = key.fromConnectData(record.topic(), record.keySchema(), record.key());
            var valueBytes = value.fromConnectData(record.topic(), record.valueSchema(), record.value());
            var decodedKey = key.toConnectData(record.topic(), keyBytes).value();
            var decodedValue = value.toConnectData(record.topic(), valueBytes).value();
            if (record.value() instanceof Struct original && original.schema().field("op") != null) {
                assertThat(field(decodedValue, "op")).isEqualTo(original.getString("op"));
                String snapshot = original.getStruct("source").getString("snapshot");
                assertThat(field(field(decodedValue, "source"), "snapshot")).isEqualTo(snapshot == null ? "false" : snapshot);
                if (record.key() == null) {
                    assertThat(decodedKey).isNull();
                }
                else {
                    assertThat(((Number) field(decodedKey, "ID")).longValue()).isEqualTo(((Struct) record.key()).getInt32("ID").longValue());
                }
            }
            else if (record.value() == null) {
                assertThat(decodedValue).isNull();
                assertThat(decodedKey).isNotNull();
            }
            else {
                assertThat(decodedValue).isNotNull();
            }
        }
    }

    private static Object field(Object value, String name) {
        return value instanceof Struct struct ? struct.get(name) : ((Map<?, ?>) value).get(name);
    }
}
