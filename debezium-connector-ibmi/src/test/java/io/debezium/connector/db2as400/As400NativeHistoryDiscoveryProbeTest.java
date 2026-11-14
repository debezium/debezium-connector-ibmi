/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class As400NativeHistoryDiscoveryProbeTest {
    @ParameterizedTest
    @ValueSource(strings = { "busy", "candidate" })
    void actualNativePendingAndTerminalHistoryDetermineFloorWithoutFixtureStart(String name) throws Exception {
        var fixture = fixture(name);
        assertThat(As400NativeHistoryDiscoveryProbe.derive(fixture.path("pending"), fixture.path("audit")))
                .isEqualTo(As400OnlineHandoff.position(fixture.path("expected").path("floor")));
    }

    @ParameterizedTest
    @ValueSource(strings = { "after-bracket", "before-bracket", "sample-outside-window", "missing-receiver", "recreated", "missing-record",
            "reordered", "continuation", "page-bound", "missing-flag", "unknown-flag", "raw-changed", "failed-observation" })
    void partialObservationCannotAuthorizeHistoryFloor(String fault) throws Exception {
        var fixture = fixture("candidate");
        var pending = (ObjectNode) fixture.path("pending");
        var audit = (ObjectNode) fixture.path("audit");
        switch (fault) {
            case "after-bracket", "before-bracket" -> {
                String sequence = fault.equals("after-bracket") ? "9999" : "4";
                for (String sample : java.util.List.of("directory_evidence", "directory_after", "pending_receivers")) {
                    ((ObjectNode) pending.path(sample).path("receivers").get(1)).put("last", sequence);
                }
            }
            case "sample-outside-window" -> ((ObjectNode) pending.path("pending_receivers").path("receivers").get(1)).put("last", "9999");
            case "missing-receiver" -> ((ArrayNode) pending.path("pending_receivers").path("receivers")).remove(0);
            case "recreated" -> ((ObjectNode) pending.path("directory_after").path("objects").get(1)).put("generation", "1");
            case "missing-record" -> ((ArrayNode) audit.path("entries")).remove(1);
            case "reordered" -> {
                var rows = (ArrayNode) audit.path("entries");
                var first = rows.get(0);
                rows.set(0, rows.get(1));
                rows.set(1, first);
            }
            case "continuation" -> ((ObjectNode) audit.path("pages").get(0)).set("continuation", audit.path("through").deepCopy());
            case "page-bound" -> ((ObjectNode) audit.path("pages").get(0).path("from")).put("sequence", "1");
            case "missing-flag" -> ((ObjectNode) pending.path("pending_receivers").path("receivers").get(1)).remove("pending");
            case "unknown-flag" -> ((ObjectNode) pending.path("pending_receivers").path("receivers").get(1)).put("pending", "NO");
            case "raw-changed" -> ((ObjectNode) audit.path("entries").get(0)).put("sha256", "0".repeat(64));
            case "failed-observation" -> pending.put("status", "rejected");
            default -> throw new AssertionError(fault);
        }
        assertThrows(Exception.class, () -> As400NativeHistoryDiscoveryProbe.derive(pending, audit));
    }

    private ObjectNode fixture(String name) throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/pending-receiver-boundary.json")) {
            return (ObjectNode) new ObjectMapper().readTree(stream).path(name);
        }
    }
}
