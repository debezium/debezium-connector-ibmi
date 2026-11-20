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

class As400BoundaryTailReplayTest {
    private ObjectNode fixture() throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/late-membership.json")) {
            return As400OnlineFenceEvidenceTest.withUnverifiedMetadataForReplay((ObjectNode) new ObjectMapper().readTree(stream));
        }
    }

    @Test
    void lateNativeMembershipStopsRealRecoveryBeforeUnknownOutputAndSurvivesRestart() throws Exception {
        var result = As400BoundaryTailReplay.verify(fixture());
        assertThat(result).containsEntry("status", "verified-offline").containsEntry("startup_proof", false);
        assertThat((java.util.List<?>) result.get("cases")).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "promotion", "raw-bytes", "baseline-marker", "missing-native-identity" })
    void diagnosticEvidenceCannotBePromotedOrSubstituted(String fault) throws Exception {
        var input = fixture();
        switch (fault) {
            case "promotion" -> input.put("startup_proof", true);
            case "raw-bytes" -> ((ObjectNode) input.path("autocommit-rows").path("entries").get(0)).put("raw", "AAAA");
            case "baseline-marker" -> ((ObjectNode) input.path("baseline-row").path("entries").get(0)).put("type", "U/DN");
            case "missing-native-identity" -> ((ObjectNode) input.path("autocommit-rows")).remove("native_identity_evidence");
            default -> throw new AssertionError(fault);
        }
        assertThatThrownBy(() -> As400BoundaryTailReplay.verify(input)).isInstanceOf(Exception.class);
    }
}
