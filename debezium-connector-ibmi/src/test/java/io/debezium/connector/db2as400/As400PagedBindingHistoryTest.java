/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.storage.file.history.FileSchemaHistory;

class As400PagedBindingHistoryTest {
    @TempDir
    Path directory;

    private Fixture fixture() throws Exception {
        var fixture = new Fixture(directory);
        fixture.config = Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName())
                .with(FileSchemaHistory.FILE_PATH, fixture.file.toString()).build();
        fixture.factory = As400BindingHistory.storage(fixture.config);
        return fixture;
    }

    private As400BindingHistory.Pending propose(Fixture f, long origin) throws Exception {
        return propose(f, origin, Fixture.POLICY);
    }

    private As400BindingHistory.Pending propose(Fixture f, long origin, As400CaptureFingerprint.Policy policy) throws Exception {
        return f.history().recordOnlineCandidate(f.journalIdentity, Fixture.position(origin), f.receiverIdentity, f.definitions,
                policy, new As400BindingHistory.Discovery(Fixture.position(29), f.receiverIdentity), Fixture.legacyOffset(28));
    }

    private As400BindingHistory.Recovered recover(Fixture f, As400BindingHistory.Pending selected) throws Exception {
        return recover(f, selected, Fixture.POLICY);
    }

    private As400BindingHistory.Recovered recover(Fixture f, As400BindingHistory.Pending selected, As400CaptureFingerprint.Policy policy) throws Exception {
        var saved = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity, selected.fingerprint(), selected.origin(), f.receiverIdentity).toOffset();
        return f.history().recoverOnlineCandidate(saved, f.journalIdentity, policy);
    }

    @ParameterizedTest
    @ValueSource(strings = { "metadata", "tombstones", "operations", "conversion", "routing", "columns", "keys", "format" })
    void shouldRecoverTheSameSourceCheckpointAfterAnOrdinaryOutputConfigurationChange(String setting) throws Exception {
        var f = fixture();
        var selected = propose(f, 29);
        byte[] history = Files.readAllBytes(f.file);
        var p = Fixture.POLICY;
        var output = new java.util.HashMap<>(p.output());
        switch (setting) {
            case "conversion" -> output.putAll(As400NativeTableSchemasTest.converterProperties());
            case "routing" -> output.putAll(Map.of("transforms", "route", "transforms.route.type", "org.apache.kafka.connect.transforms.RegexRouter",
                    "transforms.route.regex", "(.*)", "transforms.route.replacement", "changed-$1"));
            case "columns" -> output.put("column.mask.with.4.chars", "LIB.A.ID");
            case "keys" -> output.put("message.key.columns", "LIB.A:ID");
            case "format" -> output.putAll(Map.of("key.converter", "org.apache.kafka.connect.json.JsonConverter", "key.converter.schemas.enable", "false",
                    "value.converter", "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable", "false"));
        }
        var changed = new As400CaptureFingerprint.Policy(p.filters(), setting.equals("operations") ? Set.of("d") : p.skippedOperations(),
                p.decoder(), p.schema(), output, setting.equals("metadata") ? !p.transactionMetadata() : p.transactionMetadata(),
                setting.equals("tombstones") ? !p.tombstones() : p.tombstones(), p.sourcePartition());
        var restored = recover(fixture(), selected, changed);
        assertThat(restored.definitions()).containsExactlyElementsOf(f.definitions);
        assertThat(restored.fingerprint()).isEqualTo(selected.fingerprint());
        assertThat(Files.readAllBytes(f.file)).isEqualTo(history);
        if (setting.equals("conversion")) {
            var config = new As400ConnectorConfig(Configuration.from(As400NativeTableSchemasTest.converterProperties()).edit()
                    .with("topic.prefix", "test").with("database.schema", "LIB").build());
            var schemas = new As400NativeTableSchemas(restored.definitions(), config,
                    config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true));
            var table = schemas.schemaFor(restored.definitions().get(0).identity().tableId());
            assertThat(table.keyFromColumnData(new Object[]{ 8 }).getString("ID")).isEqualTo("custom:8");
            assertThat(table.valueFromColumnData(new Object[]{ 8 }).getString("ID")).isEqualTo("custom:8");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "decoder", "schema", "history.topic", "temporal.worker.zone", "output.version", "unknown", "alias-collision" })
    void shouldKeepSourceAndHistoryInvariantsPinnedWhenOutputCanChange(String change) throws Exception {
        var f = fixture();
        var selected = propose(f, 29);
        var p = Fixture.POLICY;
        var output = new java.util.HashMap<>(p.output());
        var decoder = new java.util.HashMap<>(p.decoder());
        var schema = new java.util.HashMap<>(p.schema());
        switch (change) {
            case "decoder" -> decoder.put("from.ccsid", "37");
            case "schema" -> schema.put("version", "different");
            case "alias-collision" -> output.putAll(Map.of("converters", "temporal", "temporal.type", As400NativeTableSchemasTest.LabelConverter.class.getName(),
                    "temporal.worker.zone", "another-zone"));
            case "output.version" -> output.put("version", "different");
            default -> output.put(change, "different");
        }
        var changed = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), decoder, schema, output,
                p.transactionMetadata(), p.tombstones(), p.sourcePartition());
        byte[] before = Files.readAllBytes(f.file);
        assertThatThrownBy(() -> recover(fixture(), selected, changed)).isInstanceOf(RuntimeException.class);
        assertThat(Files.readAllBytes(f.file)).isEqualTo(before);
    }

    @ParameterizedTest
    @ValueSource(strings = { "table.include.list", "table.exclude.list", "schema.include.list", "schema.exclude.list" })
    void shouldChangeSelectionWithoutChangingRetainedSchemasOrTheDurableCheckpoint(String setting) throws Exception {
        var f = fixture();
        var selected = propose(f, 29);
        var p = Fixture.POLICY;
        var decoder = new java.util.HashMap<>(p.decoder());
        decoder.put(setting, "LIB.A");
        var changed = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), decoder, p.schema(), p.output(),
                p.transactionMetadata(), p.tombstones(), p.sourcePartition());
        byte[] before = Files.readAllBytes(f.file);
        var restored = recover(fixture(), selected, changed);
        assertThat(restored.definitions()).containsExactlyElementsOf(f.definitions);
        assertThat(restored.fingerprint()).isEqualTo(selected.fingerprint());
        assertThat(Files.readAllBytes(f.file)).isEqualTo(before);
    }

    @Test
    void shouldRetainPinnedSelectionForPreviouslyWrittenVersionSevenHistory() throws Exception {
        var f = fixture();
        try (var input = getClass().getResourceAsStream("/binding-history/online-v7.jsonl")) {
            Files.copy(input, f.file);
        }
        var saved = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity,
                "v2:04cab1532ae53cc5ed0ea915bc808bff9fad7797d8e55fd99a66713e485353dd", Fixture.position(29), f.receiverIdentity).toOffset();
        byte[] before = Files.readAllBytes(f.file);
        var p = Fixture.POLICY;
        assertThat(f.history().recoverOnlineCandidate(saved, f.journalIdentity, p).definitions()).containsExactlyElementsOf(f.definitions);
        var decoder = new java.util.HashMap<>(p.decoder());
        decoder.put("table.include.list", "LIB.A");
        var changed = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), decoder, p.schema(), p.output(),
                p.transactionMetadata(), p.tombstones(), p.sourcePartition());
        assertThatThrownBy(() -> f.history().recoverOnlineCandidate(saved, f.journalIdentity, changed)).isInstanceOf(RuntimeException.class);
        assertThat(Files.readAllBytes(f.file)).isEqualTo(before);
    }

    @Test
    void shouldRetainStrictOutputCompatibilityForPreviouslyWrittenVersionSixHistory() throws Exception {
        var f = fixture();
        try (var input = getClass().getResourceAsStream("/binding-history/online-v6.jsonl")) {
            Files.copy(input, f.file);
        }
        var saved = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity,
                "v2:dcb24ab8414c9a4d4cd6ec771c50422389b830410814ed7c969dacac2e2f3508", Fixture.position(29), f.receiverIdentity).toOffset();
        byte[] before = Files.readAllBytes(f.file);
        assertThat(f.history().recoverOnlineCandidate(saved, f.journalIdentity, Fixture.POLICY).definitions()).containsExactlyElementsOf(f.definitions);
        var p = Fixture.POLICY;
        var changed = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), p.output(),
                !p.transactionMetadata(), p.tombstones(), p.sourcePartition());
        assertThatThrownBy(() -> f.history().recoverOnlineCandidate(saved, f.journalIdentity, changed)).isInstanceOf(RuntimeException.class);
        assertThat(Files.readAllBytes(f.file)).isEqualTo(before);
        var newer = propose(f, 31);
        assertThat(recover(fixture(), newer, changed).definitions()).containsExactlyElementsOf(f.definitions);
        assertThat(f.history().recoverOnlineCandidate(saved, f.journalIdentity, Fixture.POLICY).definitions()).containsExactlyElementsOf(f.definitions);
    }

    @Test
    void shouldRetainExistingV1OffsetAndDigestCompatibilityWhenPagedProposalsAreAppended() throws Exception {
        var f = fixture();
        // Captured from the preceding production implementation using the
        // synthetic Fixture catalog, not regenerated by the new encoder.
        try (var input = getClass().getResourceAsStream("/binding-history/online-v5.jsonl")) {
            Files.copy(input, f.file);
        }
        String legacyRecord = Files.readString(f.file);
        var saved = As400Checkpoint.afterVerifiedBoundary(f.journalIdentity,
                "v1:c8ad20312825b6cfb5c65057b54df6aee3255b7f64763da4f53dec3c44586fcd", Fixture.position(29), f.receiverIdentity).toOffset();
        assertThat(f.history().recoverOnlineCandidate(saved, f.journalIdentity, Fixture.POLICY).definitions()).containsExactlyElementsOf(f.definitions);
        var historicalBarrier = new java.util.HashMap<>(As400LegacyHandoff.bind(Fixture.legacyOffset(30),
                As400Checkpoint.fromOffset(saved, f.journalIdentity, (String) saved.get("capture_fingerprint"))));
        historicalBarrier.remove(As400LegacyHandoff.FINGERPRINT);
        assertThat(f.history().pendingOnlineCandidate(historicalBarrier, Fixture.POLICY).orElseThrow().fingerprint())
                .isEqualTo(saved.get("capture_fingerprint"));
        var selected = propose(f, 31);
        assertThat(recover(fixture(), selected).definitions()).containsExactlyElementsOf(f.definitions);
        var p = Fixture.POLICY;
        var changed = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), p.output(),
                !p.transactionMetadata(), p.tombstones(), p.sourcePartition());
        assertThat(recover(fixture(), selected, changed).definitions()).containsExactlyElementsOf(f.definitions);
        assertThat(f.history().recoverOnlineCandidate(saved, f.journalIdentity, Fixture.POLICY).definitions()).containsExactlyElementsOf(f.definitions);
        assertThat(As400NativeBoundaryCollector.digest(f.definitions)).isEqualTo("FFD8A8AA59B2AD3E4C5ADCA1B36BA4C6427B00DC919C89C1E3F29B6D4F7D1C34");
        assertThat(As400CaptureFingerprint.hash("domain", java.util.Map.of("a", "b:c", "d", "é")))
                .isEqualTo("aede6dc28075c6287c5efc7e322bb2a5df50ecd7d8e676426b7890a0eb3ce4a8");
        // Preserve v1's physical eligible-prefix rule even when a late writer
        // appended unselectable or damaged bytes beyond that original cut.
        Files.writeString(f.file, legacyRecord.repeat(As400BindingHistory.MAX_HISTORY_RECORDS) + "invalid tail\n");
        assertThat(f.history().recoverOnlineCandidate(saved, f.journalIdentity, Fixture.POLICY).definitions()).containsExactlyElementsOf(f.definitions);
    }

    @Test
    void shouldRecoverCatalogBeyondPreviousTableAndHistoryBoundsWithFreshInstances() throws Exception {
        var f = fixture();
        var template = f.definitions.get(0);
        f.definitions.clear();
        for (int i = 0; i < 5000; i++) {
            String name = String.format(Locale.ROOT, "F%05d", i);
            var id = new TableId(null, "LIB", name);
            var identity = new TableIdentity(id, name, name, String.format(Locale.ROOT, "%020x", 100L + 2 * i),
                    String.format(Locale.ROOT, "%020x", 101L + 2 * i), template.identity().recordFormat());
            f.definitions.add(new As400BindingDefinition(identity, template.schema().edit().tableId(id).setComment("x".repeat(3000)).create(), template.codecs()));
        }
        var selected = propose(f, 29);
        assertThat(Files.size(f.file)).isGreaterThan(As400BindingHistory.MAX_HISTORY_BYTES);
        var reopened = fixture();
        var recovered = recover(reopened, selected);
        assertThat(recovered.definitions()).containsExactlyElementsOf(f.definitions);
        assertThat(recovered.resolver(1024).resolve("F04999", "LIB").identity()).isEqualTo(f.definitions.get(4999).identity());
        // Selecting an already complete candidate is idempotent, even beyond
        // the old lifetime cap; it must not append a duplicate catalog.
        long bytes = Files.size(f.file);
        assertThat(propose(f, 29).fingerprint()).isEqualTo(selected.fingerprint());
        assertThat(Files.size(f.file)).isEqualTo(bytes);
    }

    @Test
    void shouldKeepLegacySelectionBoundedByActualStoredBytesDuringPagedDiscovery() throws Exception {
        var f = fixture();
        propose(f, 29);
        String page = Files.readAllLines(f.file).get(0);
        // Legal JSON whitespace counts against v1's physical history prefix,
        // even though parsing and reserialization remove it. These pages have
        // no completed descriptor, so none supplies a selectable generation.
        String padded = page + " ".repeat(3 * 1024 * 1024 - page.length()) + "\n";
        Files.writeString(f.file, padded.repeat(6));
        try (var input = getClass().getResourceAsStream("/binding-history/online-v5.jsonl")) {
            Files.write(f.file, input.readAllBytes(), java.nio.file.StandardOpenOption.APPEND);
        }
        assertThat(f.history().pendingOnlineCandidate(Fixture.legacyOffset(28), Fixture.POLICY)).isEmpty();
    }

    @Test
    void shouldGrantNoAuthorityToPartialPagesAndRecoverAnInterleavedRetry() throws Exception {
        var f = fixture();
        var first = propose(f, 29);
        var firstLines = Files.readAllLines(f.file);
        var second = propose(f, 31);
        var secondLines = Files.readAllLines(f.file).subList(firstLines.size(), firstLines.size() * 2);
        Files.write(f.file, firstLines.subList(0, 1)); // interruption before descriptor
        assertThat(f.history().pendingOnlineCandidate(Fixture.legacyOffset(28), Fixture.POLICY)).isEmpty();
        assertThatThrownBy(() -> recover(f, first)).isInstanceOf(RuntimeException.class);
        var interleaved = new ArrayList<String>();
        interleaved.add(firstLines.get(0));
        interleaved.add(secondLines.get(0));
        interleaved.addAll(firstLines); // exact retry repeats the first page
        interleaved.addAll(secondLines);
        Files.write(f.file, interleaved);
        assertThat(recover(fixture(), first).definitions()).containsExactlyElementsOf(f.definitions);
        assertThat(recover(fixture(), second).definitions()).containsExactlyElementsOf(f.definitions);
        assertThat(f.history().pendingOnlineCandidate(Fixture.legacyOffset(28), Fixture.POLICY).orElseThrow().fingerprint()).isEqualTo(second.fingerprint());
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-page", "reordered", "changed-page", "duplicate-conflict", "count", "digest", "descriptor-first", "foreign", "unknown-tail",
            "mixed-page-version" })
    void shouldRejectIncompleteOrChangedSelectedHistoryBeforeReturningACheckpoint(String damage) throws Exception {
        var f = fixture();
        var selected = propose(f, 29);
        var lines = new ArrayList<>(Files.readAllLines(f.file));
        switch (damage) {
            case "missing-page" -> lines.remove(0);
            case "reordered" -> Collections.swap(lines, 0, 1);
            case "changed-page" -> lines.set(0, lines.get(0).replace("29087D5938DDA", "39087D5938DDA"));
            case "duplicate-conflict" -> lines.add(1, lines.get(0).replace("29087D5938DDA", "39087D5938DDA"));
            case "count", "digest" -> {
                var descriptor = DocumentReader.defaultReader().read(lines.get(2));
                descriptor.getDocument("position").setString(damage.equals("count") ? "bindings.count" : "bindings.hash",
                        damage.equals("count") ? "3" : "0".repeat(64));
                lines.set(2, DocumentWriter.defaultWriter().write(descriptor));
            }
            case "descriptor-first" -> Collections.rotate(lines, 1);
            case "foreign" -> lines.set(0, lines.get(0).replace("\"server\":\"test\"", "\"server\":\"foreign\""));
            case "mixed-page-version" -> lines.set(0, lines.get(0).replace("8.table", "6.table"));
            case "unknown-tail" -> {
                for (int i = 0; i < As400BindingHistory.MAX_HISTORY_RECORDS; i++) {
                    lines.add(lines.get(0));
                }
                lines.add(lines.get(2).replace("\"version\":\"8\"", "\"version\":\"unknown\""));
            }
        }
        Files.write(f.file, lines);
        byte[] before = Files.readAllBytes(f.file);
        assertThatThrownBy(() -> recover(fixture(), selected)).isInstanceOf(RuntimeException.class);
        assertThat(Files.readAllBytes(f.file)).isEqualTo(before);
    }
}
