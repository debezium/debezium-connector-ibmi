/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.util.HashSet;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;

class As400OnlineCensusRecoveryTest {
    @ParameterizedTest
    @ValueSource(strings = { "multiple_clear_commit", "two_table_clear_savepoint", "multiple_clear_rollback" })
    void capturedFramesUseFreshSchemasCoordinatorAndDispatcherAtModeledCrashBoundaries(String scenario) throws Exception {
        var entries = As400OnlineCensusReplayTest.entries(As400OnlineCensusReplayTest.scenario(scenario));
        var first = entries.get(0).position();
        // Controlled historical fixture precondition; not online boundary discovery.
        var baseline = new JournalPosition(first.offset().subtract(BigInteger.ONE), first.receiver());
        var report = As400OnlineCensusRecovery.verify(entries, As400OnlineCensusReplayTest.definitions(), baseline);
        JsonNode result = new ObjectMapper().valueToTree(report);
        assertEquals("verified-offline", result.get("status").asText());
        assertFalse(result.get("startup_proof").asBoolean());
        assertEquals(scenario.equals("multiple_clear_rollback") ? 0 : scenario.equals("multiple_clear_commit") ? 7 : 4,
                result.get("reference_records").asInt());
        var kinds = new HashSet<String>();
        for (JsonNode item : result.get("cases")) {
            assertEquals("passed", item.get("status").asText());
            kinds.add(item.get("kind").asText());
            if (item.get("kind").asText().equals("restart")) {
                var offset = item.get("acknowledged");
                assertFalse(offset.has("R") || offset.has("scan"));
                assertTrue(new BigInteger(offset.get("completed_through.sequence").asText())
                        .compareTo(new BigInteger(item.get("R").get("sequence").asText())) <= 0);
            }
        }
        assertTrue(kinds.containsAll(java.util.Set.of("restart", "missing-offset", "changed-fingerprint", "changed-receiver")));
        if (!scenario.equals("multiple_clear_rollback")) {
            assertTrue(kinds.containsAll(java.util.Set.of("enqueue-false", "enqueue-exception", "missing-F", "missing-W")));
        }
        As400OnlineCensusReplayTest.saveRecovery(scenario, report);
    }
}
