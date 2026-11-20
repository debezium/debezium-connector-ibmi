/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class As400TopicRoutingTest {
    static Map<String, String> properties() {
        return Map.of("transforms", "route", "transforms.route.type", RegexRouter.class.getName(),
                "transforms.route.regex", "^(.*)$", "transforms.route.replacement", "routed.$1");
    }

    @ParameterizedTest
    @CsvSource(value = { "^(.*)$,routed.$1,data,routed.data", "data,routed,db.data,db.data", "^(.*)$,$1,data,data",
            "' ^(.*)$ ',routed.$1,data,routed.data", "^(.*)$,' routed.$1 ',data,routed.data",
            "' ^(.*)$ ',' routed.$1 ',data,routed.data" }, ignoreLeadingAndTrailingWhitespace = false)
    void shouldPredictTheActualKafkaTransformationWithoutChangingPayloadOrSourceOffsets(String regex, String replacement, String topic, String expected) {
        var config = new HashMap<>(properties());
        config.put("transforms.route.regex", regex);
        config.put("transforms.route.replacement", replacement);
        var record = new SourceRecord(Map.of("server", "test"), Map.of("W", "9"), topic, 2, Schema.INT32_SCHEMA, 7, Schema.STRING_SCHEMA, "value");
        record.headers().addString("delivery", "retained");
        try (var actual = new RegexRouter<SourceRecord>()) {
            actual.configure(Map.of("regex", regex, "replacement", replacement));
            var transformed = actual.apply(record);
            assertThat(As400TopicRouting.from(config).topic(topic)).isEqualTo(expected).isEqualTo(transformed.topic());
            assertThat(transformed.sourceOffset()).isEqualTo(record.sourceOffset());
            assertThat(transformed.key()).isEqualTo(record.key());
            assertThat(transformed.value()).isEqualTo(record.value());
            assertThat(transformed.kafkaPartition()).isEqualTo(2);
            assertThat(transformed.headers()).isEqualTo(record.headers());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-type", "secret-option", "orphan", "missing-regex", "duplicate", "empty-alias", "bad-regex" })
    void shouldRejectUnqualifiedOrIncompleteRoutingBeforeDelivery(String fault) {
        var config = new HashMap<>(properties());
        switch (fault) {
            case "missing-type" -> config.remove("transforms.route.type");
            case "secret-option" -> config.put("transforms.route.password", "not-for-history");
            case "orphan" -> config.put("transforms.unused.type", RegexRouter.class.getName());
            case "missing-regex" -> config.remove("transforms.route.regex");
            case "duplicate" -> config.put("transforms", "route,route");
            case "empty-alias" -> config.put("transforms", "route,");
            case "bad-regex" -> config.put("transforms.route.regex", "[");
            default -> throw new AssertionError(fault);
        }
        assertThrows(IllegalArgumentException.class, () -> As400TopicRouting.from(config));
    }
}
