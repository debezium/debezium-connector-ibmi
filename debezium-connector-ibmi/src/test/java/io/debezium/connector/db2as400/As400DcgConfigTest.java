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
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;

class As400DcgConfigTest {
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
            "dcg.key.mode", "provide.transaction.metadata", "transforms", "event.processing.failure.handling.mode" })
    void onlineCandidateCannotBypassExistingGatesOrInventScope(String key) {
        var p = onlineProperties();
        p.put(key, switch (key) {
            case "dcg.mode" -> "initialize";
            case "dcg.key.mode" -> "rrn";
            case "provide.transaction.metadata" -> "true";
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
    @ValueSource(strings = { "disabled", "missing-offset", "partial-offset", "operator", "warn", "no-transactions" })
    void publicOnlineAdmissionRejectsBeforeNativeHandleConstruction(String fault) {
        var p = onlineProperties();
        p.put("truncate.transactional.enabled", fault.equals("disabled") ? "false" : "true");
        if (fault.equals("operator")) {
            p.put("dcg.mode", "initialize");
        }
        if (fault.equals("warn")) {
            p.put("event.processing.failure.handling.mode", "warn");
        }
        var context = mock(SourceTaskContext.class);
        when(context.transactionContext()).thenReturn(fault.equals("no-transactions") ? null : mock(org.apache.kafka.connect.source.TransactionContext.class));
        var offsets = mock(OffsetStorageReader.class);
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
    @ValueSource(strings = { "transforms", "column.include.list", "converters", "signal.enabled.channels", "producer.override.interceptor.classes",
            "producer.override.key.serializer", "producer.override.value.serializer", "producer.override.partitioner.class", "producer.override.transactional.id",
            "schema.history.internal.consumer.interceptor.classes", "schema.history.internal.producer.interceptor.classes",
            "from.ccsid", "topic.delimiter", "message.key.columns", "source.struct.version" })
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

    @Test
    void standardSelectionChangesOutputTopicsAndFingerprintButKeepsAllBindings() {
        var p = properties();
        var original = new As400DcgConfig(Configuration.from(p));
        p.put("table.include.list", "APPDATA\\.ITEMS");
        var selected = new As400DcgConfig(Configuration.from(p));
        assertThat(selected.requests()).isEqualTo(original.requests());
        assertThat(selected.topics()).contains("configured.APPDATA.ITEMS").doesNotContain("configured.APPDATA.ORDERS");
        assertThat(selected.policy).isNotEqualTo(original.policy);
        p.put("table.exclude.list", "APPDATA\\.ORDERS");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
        p.remove("table.exclude.list");
        p.put("table.include.list", "MISSING\\..*");
        assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(p)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "4.2.0", "4.2.2", "4.3.0", "4.3.1", "4.3.2" })
    void runtimeLineUsesCapabilitiesInsteadOfAnExactPatch(String version) {
        As400DcgConfig.requireRuntimeVersion(version);
    }

    @ParameterizedTest
    @ValueSource(strings = { "3.9.1", "4.1.0", "4.4.0", "4.3.1-SNAPSHOT", "unknown" })
    void unqualifiedRuntimeLinesStillReject(String version) {
        assertThrows(IllegalStateException.class, () -> As400DcgConfig.requireRuntimeVersion(version));
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
    @ValueSource(strings = { "heartbeat.interval.ms=missing", "heartbeat.interval.ms=0", "heartbeat.interval.ms=-1",
            "event.processing.failure.handling.mode=warn", "event.processing.failure.handling.mode=skip", "skip.messages.without.change=true" })
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
