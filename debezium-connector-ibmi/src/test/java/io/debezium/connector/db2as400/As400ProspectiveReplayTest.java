/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

class As400ProspectiveReplayTest {
    private ObjectNode fixture() throws Exception {
        try (var input = getClass().getResourceAsStream("/native-transactions/prospective-native.json")) {
            return (ObjectNode) new ObjectMapper().readTree(input);
        }
    }

    @Test
    void nativeProspectiveCaptureSurvivesTheExistingRestartAndInterruptedDeliveryMatrix() throws Exception {
        var result = As400ProspectiveReplay.verify(fixture());
        assertThat(result).containsEntry("status", "verified-offline").containsEntry("startup_proof", false);
        var saved = new ObjectMapper().valueToTree(result.get("candidate_offset"));
        assertThat(saved.path("replay_from.sequence").asText()).isEqualTo("66");
        assertThat(saved.path("completed_through.sequence").asText()).isEqualTo("66");
        var recovery = new ObjectMapper().valueToTree(result.get("recovery"));
        assertThat(recovery.path("reference_records").asInt()).isEqualTo(3);
        assertThat(recovery.path("cases")).hasSize(23);
        assertThat(recovery.path("cases")).allSatisfy(c -> assertThat(c.path("status").asText()).isEqualTo("passed"));
    }

    @ParameterizedTest
    @ValueSource(strings = { "proof", "origin", "cut", "identity", "bytes", "missing-control" })
    void changedOrIncompleteNativeEvidenceCannotAuthorizeTheProspectiveCheckpoint(String fault) throws Exception {
        var fixture = fixture();
        switch (fault) {
            case "proof" -> fixture.put("startup_proof", true);
            case "origin" -> ((ObjectNode) fixture.get("origin")).put("sequence", "58");
            case "cut" -> ((ObjectNode) fixture.get("cut")).put("sequence", "59");
            case "identity" -> ((ObjectNode) fixture.path("older-controls").path("native_identity_evidence").path("before")
                    .path("objects").get(0)).put("file", "1");
            case "bytes" -> ((ObjectNode) fixture.path("prospective-rows").path("entries").get(0)).put("raw", "AAAA");
            case "missing-control" -> ((com.fasterxml.jackson.databind.node.ArrayNode) fixture.path("older-controls").path("entries")).remove(9);
            default -> throw new AssertionError(fault);
        }
        assertThatThrownBy(() -> As400ProspectiveReplay.verify(fixture)).isInstanceOf(Exception.class);
    }
}
