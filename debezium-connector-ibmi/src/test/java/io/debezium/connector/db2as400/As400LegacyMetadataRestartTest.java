/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.OffsetUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import io.debezium.connector.db2as400.As400LegacyRowFailureTest.Fixture;
import io.debezium.connector.db2as400.As400StreamingChangeEventSource.RowProcessingException;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.txmetadata.TransactionContext;

/**
 * Restart characterization using actual legacy record offsets and fresh dispatchers.
 * Context injection below is conditional test evidence, not an enabled recovery path.
 */
class As400LegacyMetadataRestartTest {
    private static final Map<String, String> METADATA = Map.of("provide.transaction.metadata", "true");
    private static final String TEST_CONTEXT_PREFIX = "test.only.context.";

    @Test
    void actualRowOffsetCannotRecoverItsOpenTransaction() throws Exception {
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
            var failure = ArgumentCaptor.forClass(RowProcessingException.class);
            verify(replacement.errors).setProducerThrowable(failure.capture());
            assertThat(failure.getValue()).hasRootCauseMessage("Missing transaction start for commit cycle 41");
            assertThat(replacement.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(41));
            verify(replacement.journal, never()).getPosition();
            assertThat(records(replacement)).isEmpty();
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
    void storingCurrentCountersWithALaggingOffsetDoubleCountsTheReplayedRow() throws Exception {
        Map<String, ?> naive;
        try (var original = new Fixture(METADATA, false)) {
            send(original, 41, "C/SC", 41, 0);
            send(original, 42, "R/PT", 41, 1);
            var row = data(records(original)).get(0);
            naive = withContexts(row.sourceOffset(), contexts(original));
            assertThat(transaction(row).getInt64("total_order")).isEqualTo(1);
        }
        try (var replacement = restoreForExperiment(naive)) {
            send(replacement, 42, "R/PT", 41, 1);
            send(replacement, 43, "C/CM", 41, 0);
            var output = records(replacement);
            assertThat(transaction(data(output).get(0)).getInt64("total_order")).isEqualTo(2);
            assertThat(value(end(output, "41")).getInt64("event_count")).isEqualTo(2);
            verifyNoInteractions(replacement.errors);
        }
    }

    @Test
    void preEntryContextsReplayTheSameRowOrderWithoutDuplicateBegin() throws Exception {
        Map<String, ?> candidate;
        SourceRecord originalRow;
        try (var original = new Fixture(METADATA, false)) {
            send(original, 41, "C/SC", 41, 0);
            var beforeRow = contexts(original);
            send(original, 42, "R/PT", 41, 1);
            originalRow = data(records(original)).get(0);
            candidate = withContexts(originalRow.sourceOffset(), beforeRow);
        }
        try (var replacement = restoreForExperiment(candidate)) {
            send(replacement, 42, "R/PT", 41, 1);
            send(replacement, 43, "C/CM", 41, 0);
            var output = records(replacement);
            assertThat(wire(data(output))).isEqualTo(wire(List.of(originalRow)));
            assertThat(data(output).get(0).headers().lastWithName("__debezium.context.runId").value())
                    .isNotEqualTo(originalRow.headers().lastWithName("__debezium.context.runId").value());
            assertThat(markers(output)).extracting(r -> value(r).getString("status")).containsExactly("END");
            assertThat(value(end(output, "41")).getInt64("event_count")).isEqualTo(1);
            verifyNoInteractions(replacement.errors);
        }
    }

    @Test
    void replayNeedsAllInterleavedContextsIncludingAnEmptyCycle() throws Exception {
        Map<String, ?> complete;
        Map<String, ?> selectedOnly;
        List<SourceRecord> expected;
        try (var original = new Fixture(METADATA, false)) {
            send(original, 41, "C/SC", 41, 0);
            send(original, 42, "C/SC", 42, 0);
            send(original, 43, "R/PT", 41, 1);
            var beforeSecondCycle = contexts(original);
            records(original);
            send(original, 44, "R/PT", 42, 2);
            var row = records(original);
            complete = withContexts(row.get(0).sourceOffset(), beforeSecondCycle);
            selectedOnly = withContexts(row.get(0).sourceOffset(), Map.of("42", beforeSecondCycle.get("42")));
            send(original, 45, "C/CM", 41, 0);
            send(original, 46, "C/CM", 42, 0);
            expected = new ArrayList<>(row);
            expected.addAll(records(original));
        }
        try (var replacement = restoreForExperiment(complete)) {
            send(replacement, 44, "R/PT", 42, 2);
            send(replacement, 45, "C/CM", 41, 0);
            send(replacement, 46, "C/CM", 42, 0);
            assertThat(wire(records(replacement))).isEqualTo(wire(expected));
            verifyNoInteractions(replacement.errors);
        }
        try (var incomplete = restoreForExperiment(selectedOnly)) {
            send(incomplete, 44, "R/PT", 42, 2);
            send(incomplete, 45, "C/CM", 41, 0);
            send(incomplete, 46, "C/CM", 42, 0);
            assertThat(markers(records(incomplete))).extracting(r -> value(r).getString("id")).containsExactly("42");
        }
    }

    @Test
    void endMarkerRequiresContextFromBeforeItsDispatchNotAfterCommit() throws Exception {
        Map<String, ?> candidate;
        SourceRecord expected;
        try (var original = new Fixture(METADATA, false)) {
            send(original, 41, "C/SC", 41, 0);
            send(original, 42, "R/PT", 41, 1);
            var beforeCommit = contexts(original);
            send(original, 43, "C/CM", 41, 0);
            expected = end(records(original), "41");
            assertThat(contexts(original)).isEmpty();
            candidate = withContexts(expected.sourceOffset(), beforeCommit);
        }
        try (var replacement = restoreForExperiment(candidate)) {
            send(replacement, 43, "C/CM", 41, 0);
            assertThat(wire(records(replacement))).isEqualTo(wire(List.of(expected)));
            verifyNoInteractions(replacement.errors);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 2, 3 })
    void keyChangePrefixesRequireBeforeImageReplayAsWellAsCounters(int acceptedPrefix) throws Exception {
        Map<String, ?> countersOnly;
        Map<String, ?> replayBeforeImage;
        List<SourceRecord> expected;
        try (var original = new Fixture(METADATA, false)) {
            send(original, 41, "C/SC", 41, 0);
            send(original, 42, "R/PT", 41, 1);
            records(original);
            var beforeImageOffset = roundTrip(original.offset.getOffset());
            var beforeImageContexts = contexts(original);
            send(original, 43, "R/UB", 41, 1);
            var beforeUpdate = contexts(original);
            send(original, 44, "R/UP", 41, 2);
            expected = records(original);
            assertThat(expected).hasSize(3);
            assertThat(expected.get(1).value()).isNull();
            assertThat(expected).allSatisfy(r -> assertThat(r.sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("43"));
            // The saved offset can follow delete, tombstone or create. These prefixes
            // are possible ordinary delivery cuts, not asserted Kafka commits here.
            countersOnly = withContexts(expected.get(acceptedPrefix - 1).sourceOffset(), beforeUpdate);
            replayBeforeImage = withContexts(beforeImageOffset, beforeImageContexts);
        }
        try (var insufficient = restoreForExperiment(countersOnly)) {
            send(insufficient, 44, "R/UP", 41, 2);
            var wrongShape = records(insufficient);
            assertThat(wrongShape).hasSize(1);
            assertThat(value(wrongShape.get(0)).getString("op")).isEqualTo("u");
            assertThat(wire(wrongShape)).isNotEqualTo(wire(expected));
        }
        try (var replacement = restoreForExperiment(replayBeforeImage)) {
            assertThat(replacement.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            send(replacement, 43, "R/UB", 41, 1);
            send(replacement, 44, "R/UP", 41, 2);
            var replayed = records(replacement);
            assertThat(wire(replayed)).isEqualTo(wire(expected));
            assertThat(data(replayed)).extracting(r -> transaction(r).getInt64("total_order")).containsExactly(2L, 3L);
            send(replacement, 45, "C/CM", 41, 0);
            assertThat(value(end(records(replacement), "41")).getInt64("event_count")).isEqualTo(3);
            verifyNoInteractions(replacement.errors);
            // Replaying the full group preserves identities/orders but duplicates any
            // prefix already delivered. It is not exactly-once recovery by itself.
            var delivered = new ArrayList<>(expected.subList(0, acceptedPrefix));
            delivered.addAll(replayed);
            assertThat(delivered).hasSize(acceptedPrefix + 3);
        }
    }

    @Test
    void actualInterruptedKeyChangeKeepsThePositionAndReplaysFromBeforeItsImage() throws Exception {
        Map<String, ?> candidate;
        SourceRecord escaped;
        try (var original = new Fixture(METADATA, true)) {
            send(original, 41, "C/SC", 41, 0);
            candidate = withContexts(original.offset.getOffset(), contexts(original));
            records(original);
            send(original, 42, "R/UB", 41, 1);
            send(original, 43, "R/UP", 41, 2);
            var failure = ArgumentCaptor.forClass(RowProcessingException.class);
            verify(original.errors).setProducerThrowable(failure.capture());
            assertThat(failure.getValue()).hasRootCauseMessage("controlled dispatch failure after first subrecord");
            assertThat(original.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            var prefix = records(original);
            assertThat(prefix).hasSize(1);
            escaped = prefix.get(0);
            assertThat(value(escaped).getString("op")).isEqualTo("d");
            assertThat(escaped.sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
        }
        try (var replacement = restoreForExperiment(candidate)) {
            send(replacement, 42, "R/UB", 41, 1);
            send(replacement, 43, "R/UP", 41, 2);
            send(replacement, 44, "C/CM", 41, 0);
            var output = records(replacement);
            assertThat(output).hasSize(4);
            assertThat(wire(List.of(output.get(0)))).isEqualTo(wire(List.of(escaped)));
            assertThat(output.get(1).value()).isNull();
            assertThat(value(output.get(2)).getString("op")).isEqualTo("c");
            assertThat(value(end(output, "41")).getInt64("event_count")).isEqualTo(2);
            verifyNoInteractions(replacement.errors);
        }
    }

    @Test
    void autocommitPublicationCannotPersistJustTheSelectedInactiveContext() throws Exception {
        Map<String, ?> candidate;
        try (var original = new Fixture(METADATA, false)) {
            send(original, 41, "C/SC", 41, 0);
            send(original, 42, "R/PT", 41, 1);
            var beforeAutocommit = contexts(original);
            records(original);
            send(original, 43, "R/PT", 0, 2);
            var autocommit = records(original).get(0);
            assertThat(original.offset.getTransactionContext().store(new HashMap<>())).isEmpty();
            assertThat(beforeAutocommit).containsOnlyKeys("41");
            candidate = withContexts(autocommit.sourceOffset(), beforeAutocommit);
        }
        try (var replacement = restoreForExperiment(candidate)) {
            send(replacement, 43, "R/PT", 0, 2);
            send(replacement, 44, "C/CM", 41, 0);
            var output = records(replacement);
            assertThat(value(data(output).get(0)).get("transaction")).isNull();
            assertThat(value(end(output, "41")).getInt64("event_count")).isEqualTo(1);
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
    private static Map<String, TransactionContext> transactionMap(Fixture fixture) throws Exception {
        var field = As400StreamingChangeEventSource.class.getDeclaredField("txMap");
        field.setAccessible(true);
        return (Map<String, TransactionContext>) field.get(fixture.source);
    }

    private static Map<String, Map<String, Object>> contexts(Fixture fixture) throws Exception {
        var saved = new LinkedHashMap<String, Map<String, Object>>();
        transactionMap(fixture).forEach((id, context) -> saved.put(id, Map.copyOf(context.store(new HashMap<>()))));
        return Map.copyOf(saved);
    }

    private static Map<String, ?> withContexts(Map<String, ?> sourceOffset, Map<String, Map<String, Object>> contexts) {
        var candidate = new LinkedHashMap<String, Object>(sourceOffset);
        contexts.forEach((id, values) -> values.forEach((key, value) -> candidate.put(TEST_CONTEXT_PREFIX + id + "." + key, value)));
        OffsetUtils.validateFormat(candidate);
        return roundTrip(candidate);
    }

    private static Fixture restoreForExperiment(Map<String, ?> candidate) throws Exception {
        var fixture = new Fixture(METADATA, false, candidate);
        try {
            var saved = new HashMap<String, Map<String, Object>>();
            candidate.forEach((key, value) -> {
                if (key.startsWith(TEST_CONTEXT_PREFIX)) {
                    String suffix = key.substring(TEST_CONTEXT_PREFIX.length());
                    int separator = suffix.indexOf('.');
                    saved.computeIfAbsent(suffix.substring(0, separator), ignored -> new HashMap<>()).put(suffix.substring(separator + 1), value);
                }
            });
            var transactions = transactionMap(fixture);
            saved.forEach((id, values) -> transactions.put(id, TransactionContext.load(values)));
            return fixture;
        }
        catch (Exception e) {
            fixture.close();
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, ?> roundTrip(Map<String, ?> sourceOffset) {
        try (var converter = new JsonConverter()) {
            converter.configure(Map.of("schemas.enable", false), false);
            return (Map<String, ?>) converter.toConnectData("offsets", converter.fromConnectData("offsets", null, sourceOffset)).value();
        }
    }
}
