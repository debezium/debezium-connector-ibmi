/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400OnlineControlInventoryTest {
    private record Capture(String name, List<Entry> entries, As400OnlineNativeEvidence.Window window,
            JournalPosition lower, Set<BigInteger> atLower, Set<BigInteger> through) {
        @Override
        public String toString() {
            return name;
        }

        As400OnlineControlInventory.Inventory reconstruct() throws Exception {
            return As400OnlineControlInventory.reconstruct(entries, window, lower);
        }
    }

    @ParameterizedTest
    @MethodSource("captures")
    void reconstructsOriginalNativeControlHistoryAndIdentities(Capture capture) throws Exception {
        var inventory = capture.reconstruct();
        assertThat(inventory.openAtLower().keySet()).isEqualTo(capture.atLower());
        assertThat(inventory.openThrough().keySet()).isEqualTo(capture.through());
        for (var start : inventory.openThrough().entrySet()) {
            assertThat(start.getValue().source().position().offset()).isEqualTo(start.getKey());
            assertThat(start.getValue().producer().thread()).isPositive();
            assertThat(start.getValue().producer().system()).isEqualTo("AEQA");
        }
        // These old completed tails are explicitly excluded, never treated as
        // recovered row context, active cycles or completed-output checkpoints.
        assertThat(inventory.completedBeforeHistory().keySet()).isEqualTo(
                Set.of("clear-only-open", "cycle-reuse").contains(capture.name()) ? Set.of(BigInteger.valueOf(100)) : Set.of());
        long maintenance = capture.entries().stream().filter(e -> type(e).startsWith("J/")).count();
        assertThat(inventory.maintenance()).hasSize((int) maintenance);
        if (capture.name().equals("silent-in-detached-receiver")) {
            assertThat(inventory.maintenance()).extracting(As400OnlineControlInventory.Maintenance::type).contains("XP", "PR", "NR");
            assertThat(inventory.openThrough().get(BigInteger.valueOf(100)).source().position().receiver())
                    .isNotEqualTo(capture.lower().receiver());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-start", "changed-thread", "missing-thread", "unknown-control", "non-durable", "incomplete-rollback",
            "nested-terminal", "wrong-start-cycle", "repeated-entry", "unrequested-code", "active-EC", "unknown-lifecycle-cycle", "savepoint-target" })
    void rejectsDamagedControlHistoryBeforeReturningAnInventory(String fault) throws Exception {
        Capture capture = named(fault.equals("savepoint-target") ? "cycle-reuse" : "silent-commit-during-observation");
        var entries = new ArrayList<>(capture.entries());
        int index = index(entries, switch (fault) {
            case "missing-start", "wrong-start-cycle", "missing-thread" -> "C/SC";
            case "incomplete-rollback" -> "C/RB";
            case "unrequested-code", "unknown-lifecycle-cycle" -> "C/BC";
            case "savepoint-target" -> "C/SQ";
            default -> "C/CM";
        });
        Entry original = entries.get(index);
        byte[] bytes = original.bytes().clone();
        var binary = ByteBuffer.wrap(bytes);
        switch (fault) {
            case "missing-start" -> entries.remove(index);
            case "changed-thread" -> binary.putLong(40, binary.getLong(40) + 1);
            case "missing-thread" -> binary.putLong(40, 0);
            case "unknown-control" -> text(bytes, 99, "ZZ");
            case "non-durable" -> text(bytes, 216, "3");
            case "incomplete-rollback" -> text(bytes, 216, "1");
            case "nested-terminal" -> binary.putInt(232, 1);
            case "wrong-start-cycle" -> binary.putLong(64, original.position().offset().longValueExact() + 1);
            case "repeated-entry" -> entries.add(index, original);
            case "unrequested-code" -> text(bytes, 98, "R");
            case "active-EC" -> text(bytes, 99, "EC");
            case "unknown-lifecycle-cycle" -> binary.putLong(64, 1);
            case "savepoint-target" -> text(bytes, binary.getInt(8) + 16, "00000000000000000001");
            default -> throw new AssertionError(fault);
        }
        if (!Set.of("missing-start", "repeated-entry").contains(fault)) {
            entries.set(index, new Entry(original.position(), bytes));
        }
        assertThatThrownBy(() -> As400OnlineControlInventory.reconstruct(entries, capture.window(), capture.lower()))
                .isInstanceOf(Exception.class);
        // Fault injection never changes the retained native capture or permits a
        // partially constructed inventory to escape after failure.
        assertThat(capture.reconstruct().openThrough().keySet()).isEqualTo(capture.through());
    }

    @Test
    void aTerminalInsideObservationCannotExcuseItsMissingOriginalStart() throws Exception {
        Capture capture = named("clear-only-open");
        Entry terminal = capture.entries().get(index(capture.entries(), "C/CM"));
        assertThatThrownBy(() -> As400OnlineControlInventory.reconstruct(capture.entries(), capture.window(), terminal.position()))
                .hasMessageContaining("Missing retained original cycle start");
    }

    @Test
    void maintenanceInsideObservationRejectsRatherThanCertifyingVisibility() throws Exception {
        Capture capture = named("silent-in-detached-receiver");
        assertThatThrownBy(() -> As400OnlineControlInventory.reconstruct(capture.entries(), capture.window(), capture.window().from()))
                .hasMessageContaining("maintenance overlaps");
    }

    @Test
    void rejectsOversizedHistoryBeforeAllocatingTransactionState() throws Exception {
        Capture capture = named("silent-in-detached-receiver");
        assertThatThrownBy(() -> As400OnlineControlInventory.reconstruct(Collections.nCopies(20_001, capture.entries().get(0)), capture.window(), capture.lower()))
                .hasMessageContaining("Bounded C/J history");
    }

    @Test
    void joinsControlProjectionUsingNativeIdentityAndOriginalBytes() throws Exception {
        Capture capture = named("silent-in-detached-receiver");
        var result = As400OnlineControlInventory.load(discovery(capture), capture.entries(), capture.window(), capture.lower(), capture.window().through());
        assertThat(result.openAtLower().keySet()).isEqualTo(capture.atLower());
        assertThat(result.maintenance()).hasSize(7);
    }

    @ParameterizedTest
    @ValueSource(strings = { "omitted-control", "changed-bytes", "missing-native-evidence", "changed-bracket", "promoted-proof", "changed-directory" })
    void cannotJoinIncompleteOrIncompatibleDiscoveryToRowHistory(String fault) throws Exception {
        Capture capture = named("silent-in-detached-receiver");
        var discovery = discovery(capture);
        var window = capture.window();
        switch (fault) {
            case "omitted-control" -> ((ArrayNode) discovery.get("entries")).remove(index(capture.entries(), "C/SC"));
            case "changed-bytes" -> ((ObjectNode) discovery.get("entries").get(0)).put("sha256", "0".repeat(64));
            case "missing-native-evidence" -> window = null;
            case "changed-bracket" -> discovery.set("lower", new ObjectMapper().valueToTree(position(capture.window().from())));
            case "promoted-proof" -> discovery.put("startup_proof", true);
            case "changed-directory" -> ((ArrayNode) discovery.get("directory_after").get("objects")).removeAll();
            default -> throw new AssertionError(fault);
        }
        var completeWindow = window;
        assertThatThrownBy(() -> As400OnlineControlInventory.load(discovery, capture.entries(), completeWindow, capture.lower(), capture.window().through()))
                .isInstanceOf(Exception.class);
    }

    private static ObjectNode discovery(Capture capture) throws Exception {
        var json = new ObjectMapper();
        var entries = new ArrayList<Map<String, Object>>();
        for (var entry : capture.entries()) {
            entries.add(Map.of("position", position(entry.position()), "raw", Base64.getEncoder().encodeToString(entry.bytes()),
                    "sha256", As400OnlineHandoff.hash(entry.bytes())));
        }
        // This test covers the native identity join; pending-flag acquisition is
        // separately exercised by the receiver collector and native controller.
        return json.valueToTree(Map.of("profile", "receiver-history-candidate-v1", "startup_proof", false,
                "lower", position(capture.lower()), "entries", entries, "native_identity_evidence", capture.window().report(),
                "directory_before", As400OnlineNativeEvidence.sample(capture.window().before()),
                "directory_after", As400OnlineNativeEvidence.sample(capture.window().after())));
    }

    private static Map<String, String> position(JournalPosition position) {
        return Map.of("library", position.receiver().library(), "receiver", position.receiver().name(), "sequence", position.offset().toString());
    }

    private static Capture named(String name) throws Exception {
        return captures().filter(c -> c.name().equals(name)).findFirst().orElseThrow();
    }

    private static Stream<Capture> captures() throws Exception {
        JsonNode fixture;
        try (var input = As400OnlineControlInventoryTest.class.getResourceAsStream("/native-transactions/receiver-control-history.json")) {
            fixture = new ObjectMapper().readTree(input);
        }
        assertThat(fixture.path("scope").asText()).isEqualTo("test-only-native-control-inventory-v1");
        assertThat(fixture.path("startup_proof").asBoolean(true)).isFalse();
        var captures = new ArrayList<Capture>();
        for (var scenario : fixture.path("cases")) {
            var entries = new ArrayList<Entry>();
            for (var reference : scenario.path("entries")) {
                var entry = fixture.path("entries").path(reference.asText());
                byte[] bytes = Base64.getDecoder().decode(entry.path("base64").asText());
                assertThat(As400OnlineHandoff.hash(bytes)).isEqualTo(reference.asText());
                var position = As400OnlineHandoff.position(entry.path("position"));
                As400JournalCaptureContent.digest(bytes, position);
                entries.add(new Entry(position, bytes));
            }
            captures.add(new Capture(scenario.path("name").asText(), List.copyOf(entries),
                    As400OnlineNativeEvidence.restore(scenario.path("native_identity_evidence")),
                    As400OnlineHandoff.position(scenario.path("lower")), ids(scenario.path("open_at_lower")), ids(scenario.path("open_through"))));
        }
        return captures.stream();
    }

    private static Set<BigInteger> ids(JsonNode values) {
        return StreamSupport.stream(values.spliterator(), false).map(n -> new BigInteger(n.asText())).collect(Collectors.toUnmodifiableSet());
    }

    private static String type(Entry entry) {
        var header = new EntryHeaderDecoder().decode(entry.bytes(), 0);
        return header.getJournalCode() + "/" + header.getEntryType();
    }

    private static int index(List<Entry> entries, String type) {
        for (int i = 0; i < entries.size(); i++) {
            if (type(entries.get(i)).equals(type)) {
                return i;
            }
        }
        throw new AssertionError("Missing retained control " + type);
    }

    private static void text(byte[] bytes, int offset, String value) {
        new AS400Text(value.length(), 37).toBytes(value, bytes, offset);
    }
}
