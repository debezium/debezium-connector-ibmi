/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;

public class As400DcgConfigTest {
    @Test
    void shouldInheritOrdinaryWorkerDefaultsWithoutChangingTheSingleTaskContract() {
        var properties = onlineProperties();
        for (String key : java.util.List.of("key.converter", "value.converter", "key.converter.schemas.enable", "value.converter.schemas.enable",
                "tasks.max", "errors.tolerance", "exactly.once.support", "transaction.boundary")) {
            properties.remove(key);
        }
        var admission = As400DcgConfig.onlineCandidate(Configuration.from(properties));
        admission.requireRuntime(mock(SourceTaskContext.class));
        assertThat(admission.policy.output()).containsEntry("key.converter", "worker-default").containsEntry("value.converter", "worker-default");
        properties.put("tasks.max", "8");
        var connector = new As400RpcConnector();
        connector.start(properties);
        assertThat(connector.taskConfigs(8)).containsExactly(properties);
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(properties)).policy).isEqualTo(admission.policy);
        properties.put("errors.tolerance", "all");
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.onlineCandidate(Configuration.from(properties)));
        assertThrows(IllegalArgumentException.class, () -> As400RecoveryAcknowledgements.requireSourceOffsetProfile(properties));
    }

    @Test
    void shouldAllowOrdinaryProducerInstrumentationWithoutOpeningRecoveryStorageOverrides() {
        var properties = onlineProperties();
        var original = As400DcgConfig.onlineCandidate(Configuration.from(properties)).policy;
        properties.put("producer.override.interceptor.classes", "example.TracingInterceptor");
        properties.put("producer.override.partitioner.class", "example.Partitioner");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(properties)).policy).isEqualTo(original);
        properties.put("schema.history.internal.consumer.interceptor.classes", "example.ChangeHistory");
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.onlineCandidate(Configuration.from(properties)));
    }

    @Test
    void shouldRetainOrdinaryDottedTopicPrefixesWithinKafkaTopicLength() {
        var properties = onlineProperties();
        properties.put("topic.prefix", "application.cdc");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(properties)).controlTopic()).isEqualTo("__debezium-truncate.application.cdc");
        properties.put("topic.prefix", "a".repeat(229));
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(properties)).controlTopic().length()).isLessThanOrEqualTo(249);
        properties.put("topic.prefix", "a".repeat(250));
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.onlineCandidate(Configuration.from(properties)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "label", "custom", "source" })
    void shouldPinCustomConversionSettingsAndPreserveUnconfiguredHistory(String alias) {
        var p = onlineProperties();
        var unchanged = As400DcgConfig.onlineCandidate(Configuration.from(p)).policy;
        var custom = new HashMap<String, String>();
        As400NativeTableSchemasTest.converterProperties()
                .forEach((key, value) -> custom.put(key.replace("label.", alias + "."), key.equals("converters") ? alias : value));
        p.putAll(custom);
        var selected = As400DcgConfig.onlineCandidate(Configuration.from(p)).policy;
        assertThat(selected.output()).containsAllEntriesOf(custom);
        assertThat(selected).isNotEqualTo(unchanged);
        p.put(alias + ".prefix", "changed:");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isNotEqualTo(selected);
        p.put(alias + ".password", "must-not-enter-history");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> As400DcgConfig.onlineCandidate(Configuration.from(p)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("authentication")
                .hasMessageNotContaining("must-not-enter-history");
    }

    @Test
    void shouldAdmitAndPinExplicitHeaderSerializationWithoutChangingTheInheritedProfile() {
        var p = onlineProperties();
        var original = As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy();
        p.put("header.converter", "org.apache.kafka.connect.json.JsonConverter");
        p.put("header.converter.schemas.enable", "true");
        var explicit = As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy();
        assertThat(explicit.output()).containsEntry("header.converter", "org.apache.kafka.connect.json.JsonConverter")
                .containsEntry("header.converter.schemas.enable", "true");
        assertThat(explicit).isNotEqualTo(original);
        p.put("header.converter.schemas.enable", "false");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy()).isNotEqualTo(explicit);
        p.remove("header.converter");
        p.remove("header.converter.schemas.enable");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy()).isEqualTo(original);
    }

    @ParameterizedTest
    @CsvSource({ "-1,-1", "37,-1", "-1,500", "37,37", "37,500", "1200,13488" })
    void shouldPinEffectiveCcsidOverridesWithoutChangingDisabledGenerations(int from, int to) {
        var p = onlineProperties();
        var original = As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy();
        p.put("from.ccsid", Integer.toString(from));
        p.put("to.ccsid", Integer.toString(to));
        var changed = As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy();
        if (from == -1 || to == -1 || from == to) {
            assertThat(changed).isEqualTo(original);
        }
        else {
            assertThat(changed).isNotEqualTo(original);
            assertThat(changed.decoder()).containsEntry("from.ccsid", Integer.toString(from)).containsEntry("to.ccsid", Integer.toString(to));
            p.put("to.ccsid", "1140");
            assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy()).isNotEqualTo(changed);
        }
    }

    @ParameterizedTest
    @CsvSource({ "-2,37", "37,65536", "37,0", "0,37", "37,65535" })
    void shouldRejectAmbiguousCcsidSubstitutionBeforeNativeAcquisition(int from, int to) {
        var p = onlineProperties();
        p.put("from.ccsid", Integer.toString(from));
        p.put("to.ccsid", Integer.toString(to));
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.onlineCandidate(Configuration.from(p)));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = { "relative", "absent", "file", "budget" })
    void shouldValidateAuditScratchStorageForMemoryTransactions(String fault, @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory)
            throws Exception {
        var properties = onlineProperties();
        properties.put("truncate.buffer.storage", "memory");
        properties.put("truncate.buffer.directory", directory.toString());
        switch (fault) {
            case "relative" -> properties.put("truncate.buffer.directory", "relative");
            case "absent" -> properties.put("truncate.buffer.directory", directory.resolve("missing").toString());
            case "file" -> properties.put("truncate.buffer.directory", java.nio.file.Files.createFile(directory.resolve("file")).toString());
            case "budget" -> properties.put("truncate.buffer.disk.max.bytes", "65535");
        }
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.configuredProfile(Configuration.from(properties)));
    }

    @Test
    void shouldUseStandardFileHistorySettingsWithoutKafkaClientsOrChangingExistingKafkaFingerprints(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) {
        var properties = onlineProperties();
        var kafka = As400DcgConfig.onlineCandidate(Configuration.from(properties));
        properties.put("schema.history.internal", io.debezium.storage.kafka.history.KafkaSchemaHistory.class.getName());
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(properties)).onlinePolicy()).isEqualTo(kafka.onlinePolicy());
        properties.put("schema.history.internal", io.debezium.storage.file.history.FileSchemaHistory.class.getName());
        properties.put("schema.history.internal.file.filename", directory.resolve("history.jsonl").toString());
        properties.keySet().removeIf(k -> k.startsWith("schema.history.internal.kafka.") || k.startsWith("producer.override.") || k.startsWith("admin.override."));
        properties.remove("transaction.boundary");
        properties.remove("exactly.once.support");
        var file = As400DcgConfig.onlineCandidate(Configuration.from(properties));
        file.requireRuntime(mock(SourceTaskContext.class));
        assertThat(file.onlinePolicy().output()).containsEntry("history.storage", "file-v1").doesNotContainKey("history.topic");
        assertThat(file.historyTopic).isNull();
        properties.put("schema.history.internal.file.filename", directory.resolve("moved.jsonl").toString());
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(properties)).onlinePolicy()).isEqualTo(file.onlinePolicy());
        assertThat(directory.toFile().list()).isEmpty(); // Configuration never initializes storage.
        var transactional = mock(SourceTaskContext.class);
        when(transactional.transactionContext()).thenReturn(mock(org.apache.kafka.connect.source.TransactionContext.class));
        assertThrows(IllegalStateException.class, () -> file.requireRuntime(transactional));
        properties.put("offset.storage.file.filename", properties.get("schema.history.internal.file.filename"));
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.onlineCandidate(Configuration.from(properties)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "unknown-store", "missing-file", "operator-window" })
    void shouldRejectUnqualifiedHistoryBeforeCreatingNativeHandles(String fault) {
        var properties = fault.equals("operator-window") ? properties() : onlineProperties();
        properties.put("schema.history.internal", fault.equals("unknown-store") ? "unqualified.History"
                : io.debezium.storage.file.history.FileSchemaHistory.class.getName());
        try (var handles = org.mockito.Mockito.mockConstruction(As400DcgNativeIo.class)) {
            assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.configuredProfile(Configuration.from(properties)
                    .edit().with("truncate.transactional.enabled", !fault.equals("operator-window")).build()));
            assertThat(handles.constructed()).isEmpty();
        }
    }

    @Test
    void shouldSelectOrdinaryOnlineAcknowledgementWithoutRequiringKafkaTransactions() {
        var properties = onlineProperties();
        var originalPolicy = As400DcgConfig.onlineCandidate(Configuration.from(properties)).onlinePolicy();
        properties.remove("exactly.once.support");
        properties.remove("transaction.boundary");
        var config = As400DcgConfig.onlineCandidate(Configuration.from(properties));
        var context = mock(SourceTaskContext.class);
        config.requireRuntime(context);
        assertThat(config.onlinePolicy()).isEqualTo(originalPolicy);
        assertThrows(IllegalStateException.class, () -> config.requireRuntime(null));
        // An actual EOS context still requires the existing connector-managed contract.
        when(context.transactionContext()).thenReturn(mock(org.apache.kafka.connect.source.TransactionContext.class));
        assertThrows(IllegalArgumentException.class, () -> config.requireRuntime(context));
        properties.put("exactly.once.support", "requested");
        properties.put("transaction.boundary", "connector");
        As400DcgConfig.onlineCandidate(Configuration.from(properties)).requireRuntime(context);
    }

    @ParameterizedTest
    @ValueSource(strings = { "unordered", "required-eos", "invalid-boundary" })
    void shouldRejectUnsupportedOnlineDeliveryBeforeOpeningNativeHandles(String fault) {
        var properties = onlineProperties();
        switch (fault) {
            case "unordered" -> properties.put("record.processing.order", "UNORDERED");
            case "required-eos" -> properties.put("exactly.once.support", "required");
            case "invalid-boundary" -> properties.put("transaction.boundary", "unsupported");
        }
        var context = mock(SourceTaskContext.class);
        try (var handles = org.mockito.Mockito.mockConstruction(As400DcgNativeIo.class)) {
            assertThrows(IllegalArgumentException.class,
                    () -> As400DcgConfig.onlineCandidate(Configuration.from(properties)).requireRuntime(context));
            assertThat(handles.constructed()).isEmpty();
        }
    }

    @Test
    void shouldPermitDiskSizedTransactionsWithoutChangingCaptureOrRequiringSharedStorage(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) {
        var properties = onlineProperties();
        var baseline = As400DcgConfig.onlineCandidate(Configuration.from(properties));
        properties.put("truncate.buffer.storage", "disk");
        properties.put("truncate.buffer.directory", directory.toString());
        properties.put("dcg.buffer.max.transactions", "50000");
        properties.put("dcg.buffer.max.entries", "1000000");
        properties.put("dcg.buffer.max.savepoints", "1000000");
        properties.put("dcg.buffer.max.bytes", "5368709120");
        var disk = As400DcgConfig.onlineCandidate(Configuration.from(properties));
        assertThat(disk.onlinePolicy()).isEqualTo(baseline.onlinePolicy());
        assertThat(disk.runtimeLimits.transactions()).isEqualTo(new As400TransactionBuffer.Limits(50000, 1000000, 1000000, 5368709120L));
        assertThat(disk.runtimeLimits.storage().directory()).isEqualTo(directory);
        assertThat(disk.runtimeLimits.outputRecords()).isEqualTo(4000002);
        properties.put("truncate.buffer.directory", directory.resolve("missing").toString());
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.onlineCandidate(Configuration.from(properties)));
    }

    @Test
    void shouldUseDiskBudgetDefaultsWithoutSilentlyChangingTheMemoryProfile() {
        var properties = onlineProperties();
        var memory = As400DcgConfig.onlineCandidate(Configuration.from(properties));
        properties.put("truncate.buffer.storage", "disk");
        var disk = As400DcgConfig.onlineCandidate(Configuration.from(properties));
        assertThat(memory.runtimeLimits.storage()).isNull();
        assertThat(memory.runtimeLimits.transactions().maxPayloadBytes()).isEqualTo(16 * 1024 * 1024);
        assertThat(disk.runtimeLimits.transactions().maxPayloadBytes()).isEqualTo(disk.runtimeLimits.storage().maximumBytes());
        assertThat(disk.runtimeLimits.transactions().maxTransactions()).isGreaterThan(1024);
        assertThat(disk.runtimeLimits.transactions().maxRetainedEntries()).isGreaterThan(100000);
    }

    @Test
    void shouldBindOnlineTemporalCompatibilityToTheWorkersZoneAndRules() {
        var previous = java.util.TimeZone.getDefault();
        try {
            var config = Configuration.from(onlineProperties());
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"));
            var utc = As400DcgConfig.onlineCandidate(config).onlinePolicy();
            assertThat(utc.output()).containsEntry("temporal.compatibility", "legacy-jt400-v1")
                    .containsEntry("temporal.worker.zone", "Z").containsEntry("temporal.worker.rules", "fixed");
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("Etc/UTC"));
            assertThat(As400DcgConfig.onlineCandidate(config).onlinePolicy()).isEqualTo(utc);
            java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("America/Chicago"));
            assertThat(As400DcgConfig.onlineCandidate(config).onlinePolicy()).isNotEqualTo(utc);
        }
        finally {
            java.util.TimeZone.setDefault(previous);
        }
    }

    @Test
    void shouldKeepExistingOutputDefaultsWhenAdmittingOnlineTruncate() {
        var p = onlineProperties();
        for (String key : java.util.List.of("topic.naming.strategy", "time.precision.mode", "trim.non.xml.charsequence.field.mode")) {
            p.remove(key);
        }
        var defaults = As400DcgConfig.onlineCandidate(Configuration.from(p));
        assertThat(defaults.policy.output()).containsEntry("topic.naming.strategy", HostnamePrefixNamingScheme.class.getName())
                .containsEntry("time.precision.mode", "adaptive").containsEntry("trim.non.xml.charsequence.field.mode", "both");
        p.put("topic.naming.strategy", HostnamePrefixNamingScheme.class.getName());
        p.put("time.precision.mode", "adaptive");
        p.put("trim.non.xml.charsequence.field.mode", "both");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isEqualTo(defaults.policy);
        p.put("trim.non.xml.charsequence.field.mode", "none");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isNotEqualTo(defaults.policy);
    }

    @Test
    void shouldPinOrderedRoutingRulesWithoutChangingUntransformedHistoryFingerprints() {
        var p = onlineProperties();
        var original = As400DcgConfig.onlineCandidate(Configuration.from(p)).policy;
        p.put("transforms", "");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isEqualTo(original);
        p.putAll(As400TopicRoutingTest.properties());
        var routed = As400DcgConfig.onlineCandidate(Configuration.from(p)).policy;
        assertThat(routed.output()).containsAllEntriesOf(As400TopicRoutingTest.properties());
        assertThat(routed).isNotEqualTo(original);
        p.put("transforms.route.regex", " ^(.*)$ ");
        p.put("transforms.route.replacement", " routed.$1 ");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isEqualTo(routed);
        p.put("transforms.route.regex", "^(.*)$");
        p.put("transforms.route.replacement", "other.$1");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isNotEqualTo(routed);
        p.put("transforms.route.replacement", "routed.$1");
        p.put("transforms", "route,second");
        p.put("transforms.second.type", "org.apache.kafka.connect.transforms.RegexRouter");
        p.put("transforms.second.regex", "^routed\\.(.*)$");
        p.put("transforms.second.replacement", "final.$1");
        var ordered = As400DcgConfig.onlineCandidate(Configuration.from(p)).policy;
        assertThat(As400TopicRouting.from(p).topic("data")).isEqualTo("final.data");
        p.put("transforms", "second,route");
        assertThat(As400TopicRouting.from(p).topic("data")).isEqualTo("routed.data");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isNotEqualTo(ordered);
    }

    @Test
    void internalOnlineCandidateUsesTheSharedOutputContractWithoutAnOperatorInventory() {
        var p = onlineProperties();
        var admission = As400DcgConfig.onlineCandidate(Configuration.from(p));
        assertThat(admission.captured).isEmpty();
        assertThat(admission.controlled).isEmpty();
        assertThat(admission.policy.transactionMetadata()).isFalse();
        assertThat(admission.policy.output()).containsEntry("binding.catalog", "verified-native-server")
                .containsEntry("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        assertThat(admission.policy.filters()).containsEntry("controlled", "native-discovered");
        assertThat(As400DcgConfig.enabled(Configuration.from(p))).isFalse();
        p.put("table.include.list", "APPDATA\\.(ORDERS|ITEMS)");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isNotEqualTo(admission.policy);
        p.remove("table.include.list");
        p.put("value.converter.schemas.enable", "false");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isNotEqualTo(admission.policy);
    }

    @ParameterizedTest
    @ValueSource(strings = { "dcg.mode", "dcg.tables", "dcg.controlled.tables", "dcg.initialization.window", "dcg.journaled.libraries",
            "dcg.key.mode", "transforms", "event.processing.failure.handling.mode" })
    void onlineCandidateCannotBypassExistingGatesOrInventScope(String key) {
        var p = onlineProperties();
        p.put(key, switch (key) {
            case "dcg.mode" -> "initialize";
            case "dcg.key.mode" -> "rrn";
            case "event.processing.failure.handling.mode" -> "warn";
            default -> "not-authority";
        });
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.onlineCandidate(Configuration.from(p)));
    }

    static Map<String, String> onlineProperties() {
        var p = properties();
        p.remove("dcg.mode");
        p.remove("dcg.tables");
        p.remove("dcg.controlled.tables");
        p.put("provide.transaction.metadata", "false");
        return p;
    }

    @ParameterizedTest
    @ValueSource(strings = { "none", "c", "u", "d", "c,u,d", "d, c, d" })
    void shouldPinOrdinaryOperationFiltersWhileRequiringTruncateCapture(String skipped) {
        var p = onlineProperties();
        var previous = As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy();
        p.put("skipped.operations", skipped);
        var selected = As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy();
        var expected = skipped.equals("none") ? java.util.Set.<String> of()
                : java.util.Arrays.stream(skipped.split(",")).map(String::trim).collect(java.util.stream.Collectors.toSet());
        assertThat(selected.skippedOperations()).isEqualTo(expected);
        if (expected.isEmpty()) {
            assertThat(selected).isEqualTo(previous);
        }
        else {
            assertThat(selected).isNotEqualTo(previous);
        }
        p.put("skipped.operations", skipped.equals("none") ? "t" : skipped + ",t");
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.onlineCandidate(Configuration.from(p)));
    }

    @Test
    void shouldPinOnlineTransactionMetadataAndPreserveTheLegacyCycleIdFormat() {
        var p = onlineProperties();
        var disabled = As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy();
        p.put("provide.transaction.metadata", "true");
        var enabled = As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy();
        assertThat(enabled.transactionMetadata()).isTrue();
        assertThat(enabled.output()).containsEntry("transaction.id.format", "legacy-cycle-v1");
        assertThat(enabled).isNotEqualTo(disabled);
    }

    static Map<String, String> properties() {
        Map<String, String> p = new HashMap<>(Map.of("name", "configured", "dcg.mode", "resume", "database.schema", "APPDATA", "database.dbname", "TESTDB",
                "dcg.journal", "JOURNALS/APPJRN", "dcg.tables", "APPDATA.ORDERS,APPDATA.ITEMS,APPDATA.OTHER", "dcg.controlled.tables",
                "APPDATA.ORDERS,APPDATA.ITEMS,APPDATA.OTHER",
                "topic.prefix", "configured", "schema.history.internal.kafka.bootstrap.servers", "broker:9092", "schema.history.internal.kafka.topic",
                "configured-history"));
        p.putAll(Map.of("producer.override.bootstrap.servers", "broker:9092", "admin.override.bootstrap.servers", "broker:9092", "dcg.retention", "operator-managed",
                "tasks.max", "1", "exactly.once.support", "requested", "transaction.boundary", "connector", "errors.tolerance", "none",
                "event.converting.failure.handling.mode", "fail", "skipped.operations", "none", "provide.transaction.metadata", "true"));
        p.putAll(Map.of("tombstones.on.delete", "true", "topic.naming.strategy", "io.debezium.schema.SchemaTopicNamingStrategy",
                "decimal.handling.mode", "precise", "binary.handling.mode", "bytes", "time.precision.mode", "adaptive_time_microseconds",
                "schema.name.adjustment.mode", "none", "field.name.adjustment.mode", "none", "trim.non.xml.charsequence.field.mode", "none",
                "key.converter", "org.apache.kafka.connect.json.JsonConverter", "value.converter", "org.apache.kafka.connect.json.JsonConverter"));
        p.putAll(Map.of("key.converter.schemas.enable", "true", "value.converter.schemas.enable", "true", "database.hostname", "unreachable.invalid",
                "database.user", "not-used", "database.password", "not-used"));
        p.put("heartbeat.interval.ms", "1000");
        return p;
    }

    @Test
    void publicOnlineAdmissionIsDefaultOffAndPinsItsControlTopicWithoutInventingScope() {
        assertThat(As400DcgConfig.configured(Configuration.empty())).isFalse();
        assertThat(As400ConnectorConfig.configDef().defaultValues()).containsEntry("truncate.transactional.enabled", false);
        var p = onlineProperties();
        p.put("truncate.transactional.enabled", "true");
        var profile = As400DcgConfig.configuredProfile(Configuration.from(p));
        assertThat(profile.captured).isEmpty();
        assertThat(profile.controlled).isEmpty();
        assertThat(profile.onlinePolicy().output()).containsEntry("control.topic", "__debezium-truncate.configured");
        var connector = new As400RpcConnector();
        assertThat(connector.canDefineTransactionBoundaries(p))
                .isEqualTo(org.apache.kafka.connect.source.ConnectorTransactionBoundaries.SUPPORTED);
        assertThat(connector.exactlyOnceSupport(p)).isEqualTo(org.apache.kafka.connect.source.ExactlyOnceSupport.UNSUPPORTED);
        assertThat(connector.validateAllFields(Configuration.from(p)).values()).allMatch(v -> v.errorMessages().isEmpty());
        p.put("dcg.mode", "initialize");
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.configuredProfile(Configuration.from(p)));
        assertThat(connector.validateAllFields(Configuration.from(p)).get("truncate.transactional.enabled").errorMessages()).isNotEmpty();
    }

    @Test
    void publicSessionRetainsTheTaskServiceRegistryForLegacySchemaConstruction() throws Exception {
        var p = onlineProperties();
        p.put("truncate.transactional.enabled", "true");
        var config = new As400ConnectorConfig(Configuration.from(p));
        var context = mock(SourceTaskContext.class);
        when(context.transactionContext()).thenReturn(mock(org.apache.kafka.connect.source.TransactionContext.class));
        try (var sessions = org.mockito.Mockito.mockConstruction(As400NativeHandoffSession.class, (session, construction) -> {
            assertThat(construction.arguments().get(0)).isSameAs(config);
            assertThat(((As400ConnectorConfig) construction.arguments().get(0)).getServiceRegistry()).isSameAs(config.getServiceRegistry());
        })) {
            assertThat(As400NativeHandoffSession.open(config, context, null)).isSameAs(sessions.constructed().get(0));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "yes", "1", "TRUE", "" })
    void malformedOnlineEnableCannotSilentlyStartTheLegacyPath(String value) {
        var p = onlineProperties();
        p.put("truncate.transactional.enabled", value);
        assertThrows(IllegalArgumentException.class, () -> As400DcgConfig.configured(Configuration.from(p)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "fail", "warn", "skip", "default" })
    void shouldAcceptOrdinaryRowErrorPoliciesWithoutToleratingDeliveryErrors(String mode) {
        var raw = onlineProperties();
        raw.put("truncate.transactional.enabled", "true");
        if (mode.equals("default")) {
            raw.remove("event.converting.failure.handling.mode");
        }
        else {
            raw.put("event.converting.failure.handling.mode", mode);
            raw.put("event.processing.failure.handling.mode", mode);
        }
        As400DcgConfig.configuredProfile(Configuration.from(raw));
        As400BatchDispatcher.requireConfiguration(new As400ConnectorConfig(Configuration.from(raw)));
        As400RecoveryAcknowledgements.requireConnectorProfile(raw);
        As400RecoveryAcknowledgements.requireSourceOffsetProfile(raw);
        raw.put("errors.tolerance", "all");
        assertThrows(IllegalArgumentException.class, () -> As400RecoveryAcknowledgements.requireConnectorProfile(raw));
        assertThrows(IllegalArgumentException.class, () -> As400RecoveryAcknowledgements.requireSourceOffsetProfile(raw));
    }

    public static class CustomTopics extends io.debezium.schema.SchemaTopicNamingStrategy {
        public CustomTopics(java.util.Properties properties) {
            super(properties);
        }

        @Override
        public String dataChangeTopic(io.debezium.spi.schema.DataCollectionId id) {
            return "custom." + super.dataChangeTopic(id);
        }
    }

    @Test
    void shouldLoadTheOrdinaryTopicNamingSpiBeforeAdmittingOnlineCapture() {
        var raw = onlineProperties();
        raw.put("truncate.transactional.enabled", "true");
        raw.put("topic.naming.strategy", CustomTopics.class.getName());
        var profile = As400DcgConfig.configuredProfile(Configuration.from(raw));
        var config = new As400ConnectorConfig(Configuration.from(raw));
        assertThat(config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true)
                .dataChangeTopic(new io.debezium.relational.TableId(null, "APPDATA", "ORDERS"))).isEqualTo("custom.configured.APPDATA.ORDERS");
        assertThat(profile.policy.output()).containsEntry("topic.naming.strategy", CustomTopics.class.getName());
        raw.put("topic.naming.strategy", "unavailable.CustomTopics");
        assertThrows(RuntimeException.class, () -> As400DcgConfig.configuredProfile(Configuration.from(raw)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "disabled", "partial-offset", "operator" })
    void publicOnlineAdmissionRejectsBeforeNativeHandleConstruction(String fault) {
        var p = onlineProperties();
        p.put("truncate.transactional.enabled", fault.equals("disabled") ? "false" : "true");
        if (fault.equals("operator")) {
            p.put("dcg.mode", "initialize");
        }
        var context = mock(SourceTaskContext.class);
        when(context.transactionContext()).thenReturn(mock(org.apache.kafka.connect.source.TransactionContext.class));
        var offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(org.mockito.ArgumentMatchers.anyMap())).thenReturn(null);
        when(context.offsetStorageReader()).thenReturn(offsets);
        if (fault.equals("partial-offset")) {
            when(offsets.offset(Map.of("server", "configured"))).thenReturn(Map.of("offset.event_sequence", "42"));
        }
        try (var handles = org.mockito.Mockito.mockConstruction(As400DcgNativeIo.class)) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> As400NativeHandoffSession.open(new As400ConnectorConfig(Configuration.from(p)), context, null))
                    .isInstanceOfAny(IllegalArgumentException.class, IllegalStateException.class);
            assertThat(handles.constructed()).isEmpty();
            if (!fault.equals("disabled")) {
                var raw = Configuration.from(p);
                var task = new As400ConnectorTask();
                task.initialize(context);
                task.preStart(raw);
                try (var jdbc = org.mockito.Mockito.mockConstruction(As400JdbcConnection.class)) {
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> task.start(raw)).isInstanceOf(RuntimeException.class);
                    assertThat(jdbc.constructed()).isEmpty();
                    assertThat(handles.constructed()).isEmpty();
                }
            }
        }
    }

    @Test
    void defaultRemainsDisabledAndExplicitConfigurationHasNoFixtureNames() {
        assertThat(As400DcgConfig.enabled(Configuration.empty())).isFalse();
        var config = new As400DcgConfig(Configuration.from(properties()));
        assertThat(config.captured).containsExactly("APPDATA.ITEMS", "APPDATA.ORDERS", "APPDATA.OTHER");
        assertThat(config.requests()).allMatch(r -> r.journal().name().equals("APPJRN"));
        assertThat(config.topics()).contains("configured.APPDATA.ITEMS", "configured.APPDATA.ORDERS", "configured-history");
        assertThat(new As400RpcConnector().exactlyOnceSupport(properties())).isEqualTo(org.apache.kafka.connect.source.ExactlyOnceSupport.UNSUPPORTED);
        assertThat(new As400RpcConnector().canDefineTransactionBoundaries(properties()))
                .isEqualTo(org.apache.kafka.connect.source.ConnectorTransactionBoundaries.SUPPORTED);
        assertThat(new As400RpcConnector().canDefineTransactionBoundaries(Map.of()))
                .isEqualTo(org.apache.kafka.connect.source.ConnectorTransactionBoundaries.UNSUPPORTED);
    }

    @ParameterizedTest
    @ValueSource(strings = { "dcg.mode", "dcg.tables", "dcg.controlled.tables", "dcg.journal", "dcg.retention", "tasks.max", "exactly.once.support",
            "transaction.boundary", "errors.tolerance", "event.converting.failure.handling.mode", "skipped.operations", "provide.transaction.metadata",
            "tombstones.on.delete", "topic.naming.strategy", "schema.name.adjustment.mode", "key.converter", "value.converter.schemas.enable",
            "producer.override.bootstrap.servers", "admin.override.bootstrap.servers", "decimal.handling.mode", "binary.handling.mode", "dcg.journaled.libraries" })
    void rejectsUnsupportedSourceAndRuntimeProfile(String key) {
        var p = properties();
        p.put(key, "unsupported");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "transforms", "converters", "signal.enabled.channels", "producer.override.interceptor.classes",
            "producer.override.key.serializer", "producer.override.value.serializer", "producer.override.partitioner.class", "producer.override.transactional.id",
            "schema.history.internal.consumer.interceptor.classes", "schema.history.internal.producer.interceptor.classes",
            "from.ccsid", "to.ccsid", "source.struct.version" })
    void rejectsOverridesThatWouldInvalidateRecovery(String key) {
        var p = properties();
        p.put(key, "some-value");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "dcg.buffer.max.transactions", "dcg.buffer.max.entries", "dcg.buffer.max.savepoints", "dcg.buffer.max.bytes" })
    void bufferLimitsAreBoundedAndCanIncreaseWithoutChangingCapture(String key) {
        var p = properties();
        var baseline = new As400DcgConfig(Configuration.from(p));
        p.put(key, "0");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
        p.put(key, "2147483647");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
        p.put(key, key.endsWith("bytes") ? "33554432" : key.endsWith("transactions") ? "128" : "20000");
        assertThat(new As400DcgConfig(Configuration.from(p)).policy).isEqualTo(baseline.policy);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void connectorValidationUsesTheBoundedReadOnlyNativeProfile(boolean valid) throws Exception {
        var connection = mock(java.sql.Connection.class);
        when(connection.isValid(2)).thenReturn(valid);
        try (var io = org.mockito.Mockito.mockConstruction(As400DcgNativeIo.class,
                (instance, context) -> when(instance.connection(false)).thenReturn(connection));
                var legacy = org.mockito.Mockito.mockConstruction(As400JdbcConnection.class)) {
            var connector = new As400RpcConnector();
            var config = Configuration.from(properties());
            var values = connector.validateAllFields(config);
            connector.validateConnection(values, config);
            assertThat(legacy.constructed()).isEmpty();
            assertThat(io.constructed()).hasSize(1);
            org.mockito.Mockito.verify(io.constructed().get(0)).close();
            org.mockito.Mockito.verify(connection).close();
            assertThat(values.get("database.hostname").errorMessages().isEmpty()).isEqualTo(valid);
        }
    }

    @Test
    void keyModeIsExplicitAndChangingItInvalidatesTheCaptureFingerprint() {
        var p = properties();
        var original = new As400DcgConfig(Configuration.from(p));
        p.put("dcg.key.mode", "primary-key");
        assertThat(new As400DcgConfig(Configuration.from(p)).policy).isEqualTo(original.policy);
        p.put("dcg.key.mode", "rrn");
        var rrn = new As400DcgConfig(Configuration.from(p));
        assertThat(rrn.rrnKeys).isTrue();
        assertThat(rrn.policy).isNotEqualTo(original.policy);
        p.put("dcg.key.mode", "fallback");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial_only", "no_data", "always", "when_needed", "configuration_based", "custom" })
    void unsupportedSnapshotModesRejectBeforeNativeOrKafkaIo(String mode) {
        var p = properties();
        p.put("snapshot.mode", mode);
        for (String admission : java.util.List.of("initialize", "resume")) {
            p.put("dcg.mode", admission);
            p.put("dcg.initialization.window", "/not-opened");
            var context = mock(SourceTaskContext.class);
            assertThrows(IllegalArgumentException.class, () -> As400DcgSession.open(Configuration.from(p), context));
            verifyNoInteractions(context);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial", "initial_only", "no_data", "when_needed", "always", "configuration_based", "custom" })
    void shouldAcceptOrdinarySnapshotPoliciesWhileRejectingMalformedLegacyOffsets(String mode) {
        var p = onlineProperties();
        p.put("truncate.transactional.enabled", "true");
        p.put("snapshot.mode", mode);
        var originalPolicy = As400DcgConfig.configuredProfile(Configuration.from(p)).onlinePolicy();
        p.put("snapshot.select.statement.overrides", "APPDATA.ORDERS");
        p.put("snapshot.select.statement.overrides.APPDATA.ORDERS", "SELECT * FROM APPDATA.ORDERS WHERE ID > 100");
        p.put("snapshot.query.mode", "select_all");
        p.put("snapshot.locking.mode", "none");
        var profile = As400DcgConfig.configuredProfile(Configuration.from(p));
        assertThat(profile.mode).isEqualTo("online-candidate");
        assertThat(profile.onlinePolicy()).isEqualTo(originalPolicy);
        var context = mock(SourceTaskContext.class);
        when(context.transactionContext()).thenReturn(mock(org.apache.kafka.connect.source.TransactionContext.class));
        var offsets = mock(OffsetStorageReader.class);
        when(offsets.offset(org.mockito.ArgumentMatchers.anyMap())).thenReturn(Map.of("offset.event_sequence", "42"));
        when(context.offsetStorageReader()).thenReturn(offsets);
        try (var handles = org.mockito.Mockito.mockConstruction(As400DcgNativeIo.class)) {
            var config = new As400ConnectorConfig(Configuration.from(p));
            assertThat(assertThrows(IllegalArgumentException.class, () -> As400NativeHandoffSession.open(config, context, null)))
                    .hasMessageContaining("Completed legacy stream required");
            assertThat(handles.constructed()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "snapshot.select.statement.overrides", "snapshot.select.statement.overrides.APPDATA.ORDERS", "snapshot.query.mode",
            "snapshot.locking.mode" })
    void shouldRejectSnapshotOverridesForTheOperatorWindowBaseline(String key) {
        var p = properties();
        p.put(key, "customized");
        for (String mode : java.util.List.of("initialize", "resume")) {
            p.put("dcg.mode", mode);
            p.put("dcg.initialization.window", "/not-opened");
            var context = mock(SourceTaskContext.class);
            assertThrows(IllegalArgumentException.class, () -> As400DcgSession.open(Configuration.from(p), context));
            verifyNoInteractions(context);
        }
    }

    @Test
    void shouldPinStandardOutputChoicesAndPreserveTheExistingDefaultFingerprint() {
        var p = onlineProperties();
        var original = As400DcgConfig.onlineCandidate(Configuration.from(p));
        p.put("tombstones.on.delete", "false");
        var noTombstones = As400DcgConfig.onlineCandidate(Configuration.from(p));
        assertThat(noTombstones.policy.tombstones()).isFalse();
        assertThat(noTombstones.policy).isNotEqualTo(original.policy);
        p.put("tombstones.on.delete", "true");
        p.put("column.exclude.list", "APPDATA\\.ITEMS\\.SECRET");
        var filtered = As400DcgConfig.onlineCandidate(Configuration.from(p));
        assertThat(filtered.policy.output()).containsEntry("column.exclude.list", p.get("column.exclude.list"));
        assertThat(filtered.policy).isNotEqualTo(original.policy);
        p.remove("column.exclude.list");
        p.put("skip.messages.without.change", "true");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy.output()).containsEntry("skip.messages.without.change", "true");
        p.put("skip.messages.without.change", "false");
        for (String key : java.util.List.of("tombstones.on.delete", "decimal.handling.mode", "binary.handling.mode", "schema.name.adjustment.mode",
                "field.name.adjustment.mode")) {
            p.remove(key);
        }
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).policy).isEqualTo(original.policy);
    }

    @ParameterizedTest
    @ValueSource(strings = { "io.debezium.schema.SchemaTopicNamingStrategy", "io.debezium.connector.db2as400.HostnamePrefixNamingScheme" })
    void shouldPreserveBuiltInTopicNamesAndPinOnlyNonDefaultNaming(String strategy) {
        var p = properties();
        p.put("topic.naming.strategy", strategy);
        var original = new As400DcgConfig(Configuration.from(p));
        p.putAll(Map.of("topic.delimiter", ".", "topic.heartbeat.prefix", "__debezium-heartbeat", "topic.transaction", "transaction"));
        assertThat(new As400DcgConfig(Configuration.from(p)).policy).isEqualTo(original.policy);
        p.putAll(Map.of("topic.delimiter", "-", "topic.heartbeat.prefix", "beats", "topic.transaction", "transactions"));
        var named = new As400DcgConfig(Configuration.from(p));
        assertThat(named.policy.output()).containsEntry("topic.delimiter", "-").containsEntry("topic.heartbeat.prefix", "beats")
                .containsEntry("topic.transaction", "transactions");
        assertThat(named.topics()).contains("beats-configured", "configured-transactions").doesNotContain("__debezium-heartbeat.configured");
        assertThat(named.topics()).contains("configured-APPDATA-ORDERS");
        var collision = new java.util.HashMap<>(p);
        collision.put("schema.history.internal.kafka.topic", "beats-configured");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(collision)).topics());
        p.remove("dcg.mode");
        p.remove("dcg.tables");
        p.remove("dcg.controlled.tables");
        p.put("provide.transaction.metadata", "false");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy().output())
                .containsEntry("topic.delimiter", "-").containsEntry("topic.heartbeat.prefix", "beats").containsEntry("topic.transaction", "transactions");
        p.put("datatype.propagate.source.type", ".*\\.VARCHAR");
        assertThat(As400DcgConfig.onlineCandidate(Configuration.from(p)).onlinePolicy().output())
                .containsEntry("datatype.propagate.source.type", ".*\\.VARCHAR");
    }

    @Test
    void standardSelectionChangesOutputTopicsAndFingerprintButKeepsAllBindings() {
        var p = properties();
        var original = new As400DcgConfig(Configuration.from(p));
        p.put("table.include.list", "APPDATA\\.ITEMS");
        var selected = new As400DcgConfig(Configuration.from(p));
        assertThat(selected.requests()).isEqualTo(original.requests());
        assertThat(selected.topics()).contains("configured.APPDATA.ITEMS").doesNotContain("configured.APPDATA.ORDERS");
        assertThat(selected.policy).isNotEqualTo(original.policy);
        p.put("schema.include.list", "APPDATA");
        var schemaSelected = new As400DcgConfig(Configuration.from(p));
        assertThat(schemaSelected.requests()).isEqualTo(original.requests());
        assertThat(schemaSelected.topics()).isEqualTo(selected.topics());
        assertThat(schemaSelected.policy).isNotEqualTo(selected.policy);
        p.put("schema.exclude.list", "OTHER");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
        p.remove("schema.exclude.list");
        p.put("schema.include.list", "OTHER");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
        p.put("schema.include.list", "APPDATA");
        p.put("table.exclude.list", "APPDATA\\.ORDERS");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
        p.remove("table.exclude.list");
        p.put("table.include.list", "MISSING\\..*");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
    }

    @Test
    void shouldRequireTheWorkersTransactionCapabilityInsteadOfGuessingItFromAVersionString() {
        var config = new As400DcgConfig(Configuration.from(properties()));
        var context = mock(SourceTaskContext.class);
        assertThrows(IllegalStateException.class, () -> config.requireRuntime(null));
        assertThrows(IllegalStateException.class, () -> config.requireRuntime(context));
        when(context.transactionContext()).thenReturn(mock(org.apache.kafka.connect.source.TransactionContext.class));
        config.requireRuntime(context);
    }

    @Test
    void converterSettingsAreExplicitAndPinnedWithoutJsonOnlyRestriction() {
        var p = properties();
        var original = new As400DcgConfig(Configuration.from(p));
        p.put("value.converter.schemas.enable", "false");
        assertThat(new As400DcgConfig(Configuration.from(p)).policy).isNotEqualTo(original.policy);
        p.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        p.put("value.converter.schema.registry.url", "mock://local");
        var avro = new As400DcgConfig(Configuration.from(p));
        assertThat(avro.policy).isNotEqualTo(original.policy);
        p.put("value.converter.schema.registry.url", "mock://different");
        assertThat(new As400DcgConfig(Configuration.from(p)).policy).isNotEqualTo(avro.policy);
    }

    @Test
    void workerWithoutTransactionContextRejectsBeforeEvenReadingOffsets() {
        var context = mock(SourceTaskContext.class);
        var offsets = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsets);
        assertThrows(IllegalStateException.class, () -> As400DcgSession.open(Configuration.from(properties()), context));
        verifyNoInteractions(offsets);
    }

    @ParameterizedTest
    @ValueSource(strings = { "heartbeat.interval.ms=-1",
            "event.processing.failure.handling.mode=warn", "event.processing.failure.handling.mode=skip" })
    void streamingPrerequisitesRejectBeforeOffsetPlanOrNativeAccess(String setting) throws Exception {
        var p = properties();
        String[] parts = setting.split("=");
        if (parts[1].equals("missing")) {
            p.remove(parts[0]);
        }
        else {
            p.put(parts[0], parts[1]);
        }
        try (var nativeIo = org.mockito.Mockito.mockConstruction(As400DcgNativeIo.class);
                var windows = org.mockito.Mockito.mockConstruction(As400PlannedStartWindow.class)) {
            for (String mode : java.util.List.of("initialize", "resume")) {
                p.put("dcg.mode", mode);
                p.put("dcg.initialization.window", "/must-not-be-opened");
                var context = mock(SourceTaskContext.class);
                assertThat(assertThrows(IllegalArgumentException.class, () -> As400DcgSession.open(Configuration.from(p), context)))
                        .hasMessageContaining("Buffered dispatch requires");
                verifyNoInteractions(context);
                var values = new As400RpcConnector().validateAllFields(Configuration.from(p));
                assertThat(values.get("dcg.mode").errorMessages()).anyMatch(message -> message.contains("Buffered dispatch requires"));
            }
            assertThat(nativeIo.constructed()).isEmpty();
            assertThat(windows.constructed()).isEmpty();
        }
    }

    @Test
    void policyPinsScopeAndOutputButNotCredentialsOrFirstStartMode() {
        var p = properties();
        var a = new As400DcgConfig(Configuration.from(p));
        p.put("database.password", "replacement-secret");
        p.put("dcg.mode", "initialize");
        p.put("dcg.initialization.window", "/operator/window");
        var b = new As400DcgConfig(Configuration.from(p));
        assertThat(a.policy).isEqualTo(b.policy);
        p.put("dcg.tables", "APPDATA.ITEMS");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
        p.put("dcg.controlled.tables", "APPDATA.ITEMS");
        assertThat(new As400DcgConfig(Configuration.from(p)).policy).isNotEqualTo(a.policy);
    }
}
