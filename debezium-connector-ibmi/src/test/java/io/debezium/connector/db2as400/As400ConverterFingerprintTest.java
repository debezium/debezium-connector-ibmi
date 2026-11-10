/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;

class As400ConverterFingerprintTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(strings = { "basic.auth.user.info", "schema.registry.basic.auth.user.info", "bearer.auth.token", "bearer.auth.client.id",
            "bearer.auth.client.secret", "sasl.jaas.config", "schema.registry.ssl.key.password", "schema.registry.ssl.keystore.password",
            "schema.registry.ssl.truststore.password", "schema.registry.ssl.keystore.key", "schema.registry.ssl.keystore.certificate.chain",
            "schema.registry.ssl.truststore.certificates" })
    void credentialRotationAndRemovalRecoverTheSameInitializedHistory(String property) throws Exception {
        for (String prefix : List.of("key.converter", "value.converter")) {
            var f = new Fixture(Files.createDirectory(directory.resolve(prefix)));
            var properties = legacyProperties(false);
            properties.put(prefix, "io.confluent.connect.avro.AvroConverter");
            String key = prefix + "." + property;
            properties.put(key, "synthetic-before");
            var original = new As400DcgConfig(Configuration.from(properties));
            var history = new As400BindingHistory(f.factory, Configuration.empty(), original.policy.sourcePartition());
            var saved = history.recordInitializedBaseline(f.journalIdentity, Fixture.position(29), f.receiverIdentity, f.definitions, original.policy,
                    new As400BindingHistory.Initialization("credential-rotation", "kafka:baseline:0:6")).toOffset();
            var unchanged = Map.copyOf(saved);
            byte[] stored = Files.readAllBytes(f.file);
            for (boolean remove : List.of(false, true)) {
                if (remove) {
                    properties.remove(key);
                }
                else {
                    properties.put(key, "synthetic-after");
                }
                var current = new As400DcgConfig(Configuration.from(properties));
                assertThat(current.policy.output()).doesNotContainKey(key);
                assertThat(history.recover(saved, f.journalIdentity, current.policy).fingerprint()).isEqualTo(saved.get("capture_fingerprint"));
                assertThat(Files.readAllBytes(f.file)).isEqualTo(stored);
                assertThat(saved).isEqualTo(unchanged);
            }
        }
    }

    @Test
    void preFixCredentialFreeCheckpointRemainsCompatibleWhenAuthenticationIsAddedOrRotated() throws Exception {
        var fixture = legacy(false);
        var properties = legacyProperties(false);
        Path file = directory.resolve("history.jsonl");
        Files.writeString(file, fixture.get("history").asText());
        var saved = strings(fixture.get("offset"));
        for (String credential : List.of("", "synthetic-before", "synthetic-after")) {
            if (!credential.isEmpty()) {
                properties.put("value.converter.basic.auth.user.info", credential);
                properties.put("value.converter.schema.registry.ssl.truststore.password", credential);
            }
            var config = new As400DcgConfig(Configuration.from(properties));
            var history = history(file, config);
            assertThat(history.recover(saved, fixture.get("journal").asText(), config.policy).fingerprint()).isEqualTo(saved.get("capture_fingerprint"));
        }
        assertThat(Files.readString(file)).isEqualTo(fixture.get("history").asText());
        assertThat(saved).isEqualTo(strings(fixture.get("offset")));
    }

    @Test
    void preFixCredentialBoundCheckpointRejectsWithoutRewritingOffsetsOrHistory() throws Exception {
        var fixture = legacy(true);
        var properties = legacyProperties(true);
        Path file = directory.resolve("history.jsonl");
        Files.writeString(file, fixture.get("history").asText());
        var saved = strings(fixture.get("offset"));
        for (String credential : List.of("synthetic-before", "synthetic-after", "")) {
            for (String key : List.of("value.converter.basic.auth.user.info", "value.converter.schema.registry.ssl.truststore.password")) {
                if (credential.isEmpty()) {
                    properties.remove(key);
                }
                else {
                    properties.put(key, credential);
                }
            }
            var config = new As400DcgConfig(Configuration.from(properties));
            var failure = assertThrows(Exception.class, () -> history(file, config).recover(saved, fixture.get("journal").asText(), config.policy));
            assertThat(failure).hasStackTraceContaining("cannot be migrated automatically");
            assertThat(failure).hasStackTraceContaining("new empty generation");
            assertThat(failure).hasStackTraceContaining("preserve this history and offsets");
            assertThat(Files.readString(file)).isEqualTo(fixture.get("history").asText());
            assertThat(saved).isEqualTo(strings(fixture.get("offset")));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "value.converter.schema.registry.url=https://different.invalid", "value.converter=example.OtherConverter",
            "value.converter.value.subject.name.strategy=io.confluent.kafka.serializers.subject.RecordNameStrategy",
            "value.converter.auto.register.schemas=false", "value.converter.use.latest.version=true", "value.converter.connect.meta.data=false" })
    void authenticationExclusionCannotBypassChangedSerializationPolicy(String setting) throws Exception {
        var fixture = legacy(false);
        var properties = legacyProperties(false);
        properties.put("value.converter.basic.auth.user.info", "synthetic-after");
        if (setting.startsWith("value.converter=")) {
            properties.remove("value.converter.basic.auth.user.info");
        }
        String[] parts = setting.split("=", 2);
        properties.put(parts[0], parts[1]);
        var config = new As400DcgConfig(Configuration.from(properties));
        Path file = directory.resolve("history.jsonl");
        Files.writeString(file, fixture.get("history").asText());
        var saved = strings(fixture.get("offset"));
        assertThrows(Exception.class, () -> history(file, config).recover(saved, fixture.get("journal").asText(), config.policy));
        assertThat(Files.readString(file)).isEqualTo(fixture.get("history").asText());
        assertThat(saved).isEqualTo(strings(fixture.get("offset")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "key.converter", "value.converter" })
    void registryCredentialsRejectWithNonDnsHostnamesAndMultipleEndpoints(String prefix) throws Exception {
        var properties = legacyProperties(false);
        properties.put(prefix, "io.confluent.connect.avro.AvroConverter");
        String key = prefix + ".schema.registry.url";
        properties.put(key, "http://schema_registry:8081");
        assertThat(new As400DcgConfig(Configuration.from(properties)).policy.output()).containsEntry(key, "http://schema_registry:8081");
        String secret = "do-not-print-this-value";
        for (String endpoint : List.of("http://user:" + secret + "@schema_registry:8081",
                "https://user:" + secret + "@schema_registry:8081", "https://registry.invalid,http://user:" + secret + "@schema_registry:8081")) {
            properties.put(key, endpoint);
            var failure = assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(properties)));
            assertThat(failure.getMessage()).contains("Use separate converter authentication properties").doesNotContain(secret);
            assertThat(failure.getCause()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "custom-converter", "unclassified-secret", "bare-password", "embedded-url", "malformed-url" })
    void ambiguousCredentialsRejectWithoutPrintingTheirValues(String fault) throws Exception {
        var properties = legacyProperties(false);
        String secret = "do-not-print-this-value";
        switch (fault) {
            case "custom-converter" -> {
                properties.put("value.converter", "example.CustomConverter");
                properties.put("value.converter.basic.auth.user.info", secret);
            }
            case "unclassified-secret" -> properties.put("value.converter.custom.secret", secret);
            case "bare-password" -> properties.put("value.converter.password", secret);
            case "embedded-url" -> properties.put("value.converter.schema.registry.url", "https://user:" + secret + "@registry.invalid");
            case "malformed-url" -> properties.put("value.converter.schema.registry.url", "https://user:" + secret + "@registry invalid");
            default -> throw new AssertionError(fault);
        }
        var failure = assertThrows(IllegalArgumentException.class, () -> new As400DcgConfig(Configuration.from(properties)));
        assertThat(failure.getMessage()).doesNotContain(secret);
        assertThat(failure.getCause()).isNull();
    }

    private static As400BindingHistory history(Path file, As400DcgConfig config) {
        return new As400BindingHistory(() -> new As400RecoveryTestSupport.FileHistory(file), Configuration.empty(), config.policy.sourcePartition());
    }

    private static Map<String, String> legacyProperties(boolean credentials) throws Exception {
        return new HashMap<>(strings(legacy(credentials).get("properties")));
    }

    private static JsonNode legacy(boolean credentials) throws Exception {
        try (var input = As400ConverterFingerprintTest.class.getResourceAsStream("/converter-fingerprints/legacy.json")) {
            return JSON.readTree(input).get(credentials ? 1 : 0);
        }
    }

    private static Map<String, String> strings(JsonNode node) {
        return JSON.convertValue(node, new TypeReference<>() {
        });
    }
}
