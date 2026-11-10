/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class As400ShortBoundaryEvidenceTest {
    private ObjectNode fixture() throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/prospective-boundary.json")) {
            return (ObjectNode) new ObjectMapper().readTree(stream);
        }
    }

    @Test
    void actualNativeScopePulseAuditAndHandoffJoinWithoutCallerSuppliedOriginAuthority() throws Exception {
        var fixture = fixture();
        var observed = As400ShortBoundaryEvidence.validate(fixture.get("boundary"));
        assertThat(observed.origin().offset().intValueExact()).isEqualTo(72);
        assertThat(observed.auditedThrough().offset().intValueExact()).isEqualTo(84);
        var result = As400ProspectiveReplay.verify(fixture);
        assertThat(result).containsEntry("status", "verified-offline").containsEntry("startup_proof", false);
        var recovery = new ObjectMapper().valueToTree(result.get("recovery"));
        assertThat(recovery.path("cases")).hasSize(23).allSatisfy(c -> assertThat(c.path("status").asText()).isEqualTo("passed"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-boundary", "profile-downgrade", "missing-target", "different-schema", "program", "lost-response", "held-lock",
            "long-hold", "overlap", "missing-page", "filtered-capture", "missing-entry", "marker-bytes", "foreign-thread", "origin" })
    void incompleteOrMismatchedBoundaryNeverReachesHandoff(String fault) throws Exception {
        var fixture = fixture();
        var boundary = (ObjectNode) fixture.get("boundary");
        var pulse = (ObjectNode) boundary.get("pulse");
        var capture = (ObjectNode) boundary.get("raw");
        switch (fault) {
            case "missing-boundary" -> fixture.remove("boundary");
            case "profile-downgrade" -> fixture.put("profile", "prospective-native-fixture-v1");
            case "missing-target" -> ((ArrayNode) boundary.path("request").path("targets")).remove(3);
            case "different-schema" -> ((ArrayNode) boundary.path("after").path("queries")).remove(10);
            case "program" -> boundary.put("statement", boundary.path("statement").asText().replace("FORCE(*NO)", "FORCE(*YES)"));
            case "lost-response" -> pulse.put("response", "unknown");
            case "held-lock" -> pulse.put("holder_drained", false);
            case "long-hold" -> ((ObjectNode) pulse.path("result_rows").get(0)).put("RELEASED",
                    java.time.LocalDateTime.parse(pulse.path("result_rows").get(0).path("ACQUIRED").asText().replace(' ', 'T')).plusSeconds(2).toString());
            case "overlap" -> ((ObjectNode) boundary.path("after").path("started")).put("monotonic_ns", pulse.path("started").path("monotonic_ns").asLong());
            case "missing-page" -> ((ArrayNode) capture.get("pages")).removeAll();
            case "filtered-capture" -> capture.put("mode", "capture");
            case "missing-entry" -> ((ArrayNode) capture.get("entries")).remove(1);
            case "foreign-thread" -> ((ObjectNode) boundary.path("interval").path("holder").get(0)).put("THREAD_HEX", "0000000000000001");
            case "origin" -> ((ObjectNode) fixture.get("origin")).put("sequence", 58);
            case "marker-bytes" -> {
                var item = (ObjectNode) java.util.stream.StreamSupport.stream(capture.path("entries").spliterator(), false)
                        .filter(e -> e.path("type").asText().equals("U/DN")).skip(1).findFirst().orElseThrow();
                byte[] raw = Base64.getDecoder().decode(item.path("raw").asText());
                raw[item.path("entry_specific_data_offset").asInt() + 16] ^= 1;
                item.put("raw", Base64.getEncoder().encodeToString(raw)).put("sha256", As400OnlineHandoff.hash(raw));
            }
            default -> throw new AssertionError(fault);
        }
        assertThatThrownBy(() -> As400ProspectiveReplay.verify(fixture)).isInstanceOf(Exception.class);
    }
}
