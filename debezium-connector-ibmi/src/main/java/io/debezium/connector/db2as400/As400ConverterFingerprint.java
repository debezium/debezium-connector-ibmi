/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.net.URI;
import java.util.Set;

import io.debezium.config.Configuration;

/** Classify documented converter credentials without weakening the serialization fingerprint. */
final class As400ConverterFingerprint {
    private static final Set<String> REGISTRY_CONVERTERS = Set.of("io.confluent.connect.avro.AvroConverter",
            "io.confluent.connect.json.JsonSchemaConverter", "io.confluent.connect.protobuf.ProtobufConverter");
    // Schema Registry's basic/bearer/SASL credentials and Kafka SSL credential fields.
    // The optional schema.registry. namespace is normalized only for classification.
    // Provider/source selection, registry URL, subject naming and schema policy remain pinned.
    private static final Set<String> CREDENTIALS = Set.of("basic.auth.user.info", "bearer.auth.token", "bearer.auth.client.id", "bearer.auth.client.secret",
            "sasl.jaas.config", "ssl.key.password", "ssl.keystore.password", "ssl.truststore.password", "ssl.keystore.key",
            "ssl.keystore.certificate.chain", "ssl.truststore.certificates");

    private As400ConverterFingerprint() {
    }

    static boolean include(Configuration config, String key, String value) {
        String prefix = key.startsWith("key.converter.") ? "key.converter" : "value.converter";
        String property = key.substring(prefix.length() + 1);
        String clientProperty = property.startsWith("schema.registry.") ? property.substring("schema.registry.".length()) : property;
        if (CREDENTIALS.contains(clientProperty)) {
            if (!REGISTRY_CONVERTERS.contains(config.getString(prefix))) {
                throw new IllegalArgumentException("Unclassified converter authentication property: " + key);
            }
            return false;
        }
        if (Set.of("password", "secret", "token").contains(clientProperty)
                || clientProperty.endsWith(".password") || clientProperty.endsWith(".secret") || clientProperty.endsWith(".token")
                || clientProperty.endsWith(".user.info") || clientProperty.endsWith(".jaas.config") || clientProperty.endsWith(".keystore.key")) {
            // Unknown providers need an explicit classification, not a secret hash
            // or an assumption that their settings cannot change serialized output.
            throw new IllegalArgumentException("Unclassified converter authentication property: " + key);
        }
        if (property.equals("schema.registry.url")) {
            for (String endpoint : value.split(",", -1)) {
                boolean credentialFree;
                try {
                    // URI leaves userInfo null for registry-style authorities
                    // such as user:password@schema_registry. The registry client's
                    // URL parser still recognizes those credentials, so inspect
                    // the complete authority instead of assuming a DNS hostname.
                    String authority = URI.create(endpoint.trim()).getRawAuthority();
                    credentialFree = authority == null || !authority.contains("@");
                }
                catch (IllegalArgumentException invalid) {
                    // Do not include a possibly credential-bearing URL in the diagnostic.
                    throw new IllegalArgumentException("Invalid converter registry endpoint in " + key);
                }
                if (!credentialFree) {
                    throw new IllegalArgumentException("Use separate converter authentication properties instead of URL credentials in " + key);
                }
            }
        }
        return true;
    }
}
