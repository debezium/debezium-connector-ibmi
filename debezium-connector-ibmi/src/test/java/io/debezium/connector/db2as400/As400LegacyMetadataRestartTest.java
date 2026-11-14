/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import io.debezium.connector.db2as400.As400LegacyRowFailureTest.Fixture;
import io.debezium.connector.db2as400.As400StreamingChangeEventSource.RowProcessingException;
import io.debezium.pipeline.DataChangeEvent;

/**
 * Restart characterization using actual legacy record offsets and fresh dispatchers.
 */
class As400LegacyMetadataRestartTest {
    private static final Map<String, String> METADATA = Map.of("provide.transaction.metadata", "true");

    @Test
    void shouldResumeOpenTransactionMetadataFromAnActualRowOffset() throws Exception {
        Map<String, ?> saved;
        try (var original = new Fixture(METADATA, false)) {
            send(original, 41, "C/SC", 41, 0);
            send(original, 42, "R/PT", 41, 1);
            var row = data(records(original)).get(0);
            saved = roundTrip(row.sourceOffset());
            assertThat(saved.get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("41");
            assertThat(transaction(row).getInt64("total_order")).isEqualTo(1);
        }
        try (var replacement = new Fixture(METADATA, false, saved)) {
            send(replacement, 42, "R/PT", 41, 1);
            send(replacement, 43, "C/CM", 41, 0);
            var output = records(replacement);
            assertThat(markers(output)).extracting(r -> value(r).getString("status")).containsExactly("BEGIN", "END");
            assertThat(transaction(data(output).get(0)).getString("id")).isEqualTo("41");
            assertThat(value(end(output, "41")).getInt64("event_count")).isEqualTo(1L);
            assertThat(replacement.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(43));
            verifyNoInteractions(replacement.errors);
        }
    }

    @Test
    void currentCommitOffsetAloneCannotReproduceAnEndMarker() throws Exception {
        Map<String, ?> saved;
        try (var original = new Fixture(METADATA, false)) {
            send(original, 41, "C/SC", 41, 0);
            send(original, 42, "R/PT", 41, 1);
            send(original, 43, "C/CM", 41, 0);
            var end = end(records(original), "41");
            saved = roundTrip(end.sourceOffset());
            assertThat(saved.get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
            assertThat(value(end).getInt64("event_count")).isEqualTo(1);
        }
        try (var replacement = new Fixture(METADATA, false, saved)) {
            send(replacement, 43, "C/CM", 41, 0);
            // Characterizes the limitation; this is not an accepted complete recovery.
            assertThat(records(replacement)).isEmpty();
            assertThat(replacement.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(43));
            verifyNoInteractions(replacement.errors);
        }
    }

    @Test
    void actualInterruptedKeyChangeKeepsThePositionAndReplaysFromBeforeItsImage() throws Exception {
        Map<String, ?> candidate;
        SourceRecord escaped;
        try (var original = new Fixture(METADATA, true)) {
            send(original, 41, "C/SC", 41, 0);
            candidate = roundTrip(original.offset.getOffset());
            records(original);
            send(original, 42, "R/UB", 41, 1);
            var failure = org.junit.jupiter.api.Assertions.assertThrows(RowProcessingException.class, () -> send(original, 43, "R/UP", 41, 2));
            assertThat(failure).hasRootCauseMessage("controlled dispatch failure after first subrecord");
            verifyNoInteractions(original.errors);
            assertThat(original.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            var prefix = records(original);
            assertThat(prefix).hasSize(1);
            escaped = prefix.get(0);
            assertThat(value(escaped).getString("op")).isEqualTo("d");
            assertThat(escaped.sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
        }
        try (var replacement = new Fixture(METADATA, false, candidate)) {
            send(replacement, 42, "R/UB", 41, 1);
            send(replacement, 43, "R/UP", 41, 2);
            send(replacement, 44, "C/CM", 41, 0);
            var output = records(replacement);
            assertThat(output).hasSize(5);
            assertThat(markers(output)).extracting(r -> value(r).getString("status")).containsExactly("BEGIN", "END");
            var rows = output.stream().filter(r -> r.value() == null || r.value() instanceof Struct v && v.schema().field("op") != null).toList();
            assertThat(wire(List.of(rows.get(0)))).isEqualTo(wire(List.of(escaped)));
            assertThat(rows.get(1).value()).isNull();
            assertThat(value(rows.get(2)).getString("op")).isEqualTo("c");
            assertThat(value(end(output, "41")).getInt64("event_count")).isEqualTo(2);
            verifyNoInteractions(replacement.errors);
        }
    }

    private static void send(Fixture fixture, long sequence, String operation, int cycle, int key) throws Exception {
        when(fixture.journal.decode(fixture.decoder)).thenReturn(new Object[]{ key });
        fixture.run(As400ConnectProbeConnector.event(sequence, operation, cycle, key));
    }

    private static List<SourceRecord> records(Fixture fixture) throws Exception {
        return fixture.queue.poll().stream().map(DataChangeEvent::getRecord).toList();
    }

    private static Struct value(SourceRecord record) {
        return (Struct) record.value();
    }

    private static Struct transaction(SourceRecord record) {
        return value(record).getStruct("transaction");
    }

    private static List<SourceRecord> data(List<SourceRecord> records) {
        return records.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("op") != null).toList();
    }

    private static List<SourceRecord> markers(List<SourceRecord> records) {
        return records.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("status") != null).toList();
    }

    private static SourceRecord end(List<SourceRecord> records, String cycle) {
        return markers(records).stream().filter(r -> "END".equals(value(r).getString("status")) && cycle.equals(value(r).getString("id")))
                .findFirst().orElseThrow();
    }

    private static List<List<Object>> wire(List<SourceRecord> records) {
        return records.stream().map(r -> {
            var headers = new ArrayList<List<Object>>();
            r.headers().forEach(header -> {
                if (header.key().equals("__debezium.context.runId")) {
                    assertThat(header.value()).isInstanceOf(String.class).asString().isNotBlank();
                }
                else {
                    headers.add(java.util.Arrays.asList(header.key(), header.schema(), header.value()));
                }
            });
            return java.util.Arrays.asList(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), r.valueSchema(), stableValue(r), headers);
        }).toList();
    }

    private static Object stableValue(SourceRecord record) {
        if (record.value() == null || value(record).schema().field("op") == null) {
            return record.value();
        }
        var fields = new LinkedHashMap<String, Object>();
        value(record).schema().fields().forEach(field -> {
            // Processing-clock envelope timestamps change on replay. Keep source
            // timestamps, source identity, row images and every transaction field.
            if (!List.of("ts_ms", "ts_us", "ts_ns").contains(field.name())) {
                fields.put(field.name(), value(record).get(field));
            }
        });
        return fields;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> roundTrip(Map<String, ?> sourceOffset) {
        try (var converter = new JsonConverter()) {
            converter.configure(Map.of("schemas.enable", false), false);
            return (Map<String, ?>) converter.toConnectData("offsets", converter.fromConnectData("offsets", null, sourceOffset)).value();
        }
    }
}
