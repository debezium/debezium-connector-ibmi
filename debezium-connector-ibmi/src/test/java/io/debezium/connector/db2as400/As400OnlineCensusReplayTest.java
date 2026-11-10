/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

class As400OnlineCensusReplayTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir
    Path directory;

    @Test
    void controlDiscoveryIsNeverAcceptedAsAFullPublicationCapture() throws Exception {
        Files.createDirectory(directory.resolve("oracle"));
        Files.writeString(directory.resolve("oracle/capture.json"), "{\"status\":\"controls-captured\",\"mode\":\"controls\"}");
        assertThrows(IllegalArgumentException.class, () -> As400OnlineCensusReplay.main(new String[]{ directory.toString() }));
        assertEquals("rejected", JSON.readTree(directory.resolve("replay.json").toFile()).path("status").asText());
    }

    @Test
    void retainedOrdinaryCaptureIncludesAutocommitProgressAndReceiverRotation() throws Exception {
        JsonNode fixture;
        try (var input = getClass().getResourceAsStream("/native-transactions/ordinary-cdc.json")) {
            fixture = JSON.readTree(input);
        }
        var definitions = new ArrayList<As400BindingDefinition>();
        for (JsonNode binding : fixture.get("tables")) {
            String file = binding.get("file").asText();
            var table = new TableId("AEQA", binding.get("library").asText(), file);
            definitions.add(new As400BindingDefinition(new TableIdentity(table, file, binding.get("member").asText(), binding.get("file_jid").asText(),
                    binding.get("member_jid").asText(), binding.get("format").asText()),
                    Table.editor().tableId(table).addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER")
                            .optional(false).create()).setPrimaryKeyNames("ID").create(),
                    List.of(new As400BindingDefinition.Codec("BIN4", 4, 0, 0))));
        }
        var entries = entries(fixture);
        assertTrue(entries.stream().map(e -> e.position().receiver()).distinct().count() > 1);
        JsonNode replay = JSON.valueToTree(As400OnlineCensusReplay.replay(entries.subList(1, entries.size()), definitions, entries.get(0).position()));
        assertEquals("replayed", replay.get("status").asText());
        var cycles = new java.util.HashSet<String>();
        replay.get("batches").forEach(batch -> cycles.add(batch.get("cycle").asText()));
        assertTrue(cycles.contains("0") && cycles.size() > 1);
        var recovery = As400OnlineCensusRecovery.verify(entries.subList(1, entries.size()), definitions, entries.get(0).position());
        assertEquals("verified-offline", recovery.get("status"));
        saveRecovery("ordinary-cdc", recovery);
    }

    @ParameterizedTest
    @ValueSource(strings = { "multiple_clear_commit", "multiple_clear_rollback", "multiple_clear_savepoint", "nested_clear_savepoint",
            "non_top_clear_savepoint", "two_table_clear_rollback", "two_table_clear_savepoint" })
    void retainedNativeCapturesReplayToTheIndependentlyObservedFinalRows(String name) throws Exception {
        JsonNode fixture = scenario(name);
        JsonNode replay = JSON.valueToTree(replay(entries(fixture)));
        assertEquals("replayed", replay.get("status").asText());
        assertFalse(replay.get("startup_proof").asBoolean());
        var rows = new TreeMap<String, TreeSet<Integer>>();
        fixture.get("tables").forEach(table -> rows.put(table.asText(), new TreeSet<>(List.of(1, 2, 3))));
        for (JsonNode batch : replay.get("batches")) {
            for (JsonNode change : batch.get("changes")) {
                var table = rows.get(change.get("file").asText());
                switch (change.get("operation").asText()) {
                    case "c" -> assertTrue(table.add(change.get("id").asInt()));
                    case "t" -> table.clear();
                    case "d" -> assertTrue(table.remove(change.get("id").asInt()));
                    default -> throw new AssertionError("Unexpected fixture operation");
                }
            }
        }
        for (var table : rows.entrySet()) {
            assertEquals(JSON.valueToTree(table.getValue()), fixture.get("source_rows").get(table.getKey()));
        }
        // Optional local artifact for exercising the Python ledger checker with
        // these actual decoder results; never a new native capture or credential.
        String evidence = System.getProperty("ibmi.census.replay.evidence");
        if (evidence != null) {
            Files.createDirectories(Path.of(evidence));
            JSON.writerWithDefaultPrettyPrinter().writeValue(Path.of(evidence, name + ".json").toFile(), replay);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "start", "terminal", "row-jid", "thread", "minimized", "order", "savepoint-target" })
    void incompleteOrAmbiguousNativeCopiesFailClosed(String fault) throws Exception {
        var entries = new ArrayList<>(entries(scenario(fault.equals("savepoint-target") ? "nested_clear_savepoint" : "multiple_clear_commit")));
        if (fault.equals("start") || fault.equals("terminal")) {
            String type = fault.equals("start") ? "SC" : "CM";
            entries.removeIf(e -> new EntryHeaderDecoder().decode(e.bytes(), 0).getEntryType().equals(type));
        }
        else if (fault.equals("order")) {
            java.util.Collections.swap(entries, 2, 3);
        }
        else {
            Entry entry = entries.stream().filter(e -> {
                var header = new EntryHeaderDecoder().decode(e.bytes(), 0);
                return fault.equals("savepoint-target") ? header.getEntryType().equals("SB") : header.getJournalCode() == 'R';
            }).findFirst().orElseThrow();
            switch (fault) {
                case "row-jid" -> entry.bytes()[197] ^= 1;
                case "thread" -> ByteBuffer.wrap(entry.bytes()).putLong(40, 0);
                case "minimized" -> entry.bytes()[218] |= 0x20;
                case "savepoint-target" -> entries.remove(entry);
                default -> throw new AssertionError(fault);
            }
        }
        assertThrows(Exception.class, () -> replay(entries));
    }

    @ParameterizedTest
    @ValueSource(strings = { "sha", "type", "cycle", "position", "path" })
    void rawEvidenceAndSummariesMustAgreeBeforeReplay(String fault) throws Exception {
        Entry entry = entries(scenario("multiple_clear_commit")).get(2);
        var header = new EntryHeaderDecoder().decode(entry.bytes(), 0);
        Files.write(directory.resolve("entry-0.bin"), entry.bytes());
        var descriptor = JSON.createObjectNode();
        descriptor.put("file", "entry-0.bin");
        descriptor.put("sha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(entry.bytes())));
        descriptor.put("type", header.getJournalCode() + "/" + header.getEntryType());
        descriptor.put("cycle", header.getCommitCycle().toString());
        descriptor.putObject("position").put("sequence", entry.position().offset().toString())
                .put("receiver", entry.position().receiver().name()).put("library", entry.position().receiver().library());
        assertEquals(entry.position(), As400OnlineCensusReplay.load(directory, descriptor).position());
        switch (fault) {
            case "sha" -> descriptor.put("sha256", "00");
            case "type" -> descriptor.put("type", "C/CM");
            case "cycle" -> descriptor.put("cycle", "99999");
            case "position" -> descriptor.withObject("position").put("sequence", "99999");
            case "path" -> descriptor.put("file", "../entry-0.bin");
            default -> throw new AssertionError(fault);
        }
        assertThrows(Exception.class, () -> As400OnlineCensusReplay.load(directory, descriptor));
    }

    @ParameterizedTest
    @ValueSource(strings = { "complete", "anchor", "binding", "missing-start" })
    void offlineEntryPointUsesStoredBindingsAndExactRetainedAnchors(String fault) throws Exception {
        var entries = new ArrayList<>(entries(scenario("multiple_clear_commit")));
        writeCapture(directory.resolve("baseline"), List.of(entries.get(0)));
        writeCapture(directory.resolve("final"), List.of(entries.get(entries.size() - 1)));
        if (fault.equals("missing-start")) {
            entries.removeIf(e -> new EntryHeaderDecoder().decode(e.bytes(), 0).getEntryType().equals("SC"));
        }
        writeCapture(directory.resolve("oracle"), entries);
        var changes = new TableChanges();
        definitions().forEach(definition -> changes.create(definition.storedTable()));
        Document document = Document.create();
        document.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
        Files.createDirectory(directory.resolve("native-bindings"));
        String serialized = DocumentWriter.defaultWriter().write(document);
        if (fault.equals("binding")) {
            serialized = serialized.replace("00000000000000000002", "000000000000000000ff");
        }
        Files.writeString(directory.resolve("native-bindings/bindings.json"), serialized);
        if (fault.equals("anchor")) {
            Files.write(directory.resolve("baseline/entry-0.bin"), new byte[236]);
        }
        if (fault.equals("complete")) {
            As400OnlineCensusReplay.main(new String[]{ directory.toString() });
            JsonNode report = JSON.readTree(directory.resolve("replay.json").toFile());
            assertEquals("replayed", report.get("status").asText());
            assertEquals("verified-offline", report.get("recovery").get("status").asText());
            byte[] result = Files.readAllBytes(directory.resolve("replay.json"));
            assertThrows(Exception.class, () -> As400OnlineCensusReplay.main(new String[]{ directory.toString() }));
            assertTrue(java.util.Arrays.equals(result, Files.readAllBytes(directory.resolve("replay.json"))));
        }
        else {
            assertThrows(Exception.class, () -> As400OnlineCensusReplay.main(new String[]{ directory.toString() }));
            assertEquals("rejected", JSON.readTree(directory.resolve("replay.json").toFile()).get("status").asText());
        }
    }

    private static void writeCapture(Path directory, List<Entry> entries) throws Exception {
        Files.createDirectory(directory);
        var capture = JSON.createObjectNode().put("status", "captured").put("library", "DCCV1107");
        capture.set("from", position(entries.get(0).position()));
        capture.set("through", position(entries.get(entries.size() - 1).position()));
        var records = capture.putArray("entries");
        for (Entry entry : entries) {
            String name = "entry-" + records.size() + ".bin";
            Files.write(directory.resolve(name), entry.bytes());
            var header = new EntryHeaderDecoder().decode(entry.bytes(), 0);
            var record = records.addObject().put("file", name).put("sha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(entry.bytes())))
                    .put("type", header.getJournalCode() + "/" + header.getEntryType()).put("cycle", header.getCommitCycle().toString());
            record.set("position", position(entry.position()));
        }
        JSON.writeValue(directory.resolve("capture.json").toFile(), capture);
    }

    private static JsonNode position(JournalPosition position) {
        return JSON.createObjectNode().put("sequence", position.offset().toString()).put("receiver", position.receiver().name()).put("library",
                position.receiver().library());
    }

    static JsonNode scenario(String name) throws Exception {
        try (var input = As400OnlineCensusReplayTest.class.getResourceAsStream("/native-transactions/multiple-clears.json")) {
            for (JsonNode scenario : JSON.readTree(input).get("cases")) {
                if (scenario.get("name").asText().equals(name)) {
                    return scenario;
                }
            }
        }
        throw new IllegalArgumentException(name);
    }

    private static Map<String, Object> replay(List<Entry> entries) throws Exception {
        // This older controlled fixture starts with no open writer transactions;
        // the fabricated preceding coordinate is a test precondition only.
        var first = entries.get(0).position();
        return As400OnlineCensusReplay.replay(entries, definitions(), new JournalPosition(first.offset().subtract(BigInteger.ONE), first.receiver()));
    }

    static List<Entry> entries(JsonNode scenario) {
        var entries = new ArrayList<Entry>();
        for (JsonNode entry : scenario.get("entries")) {
            entries.add(new Entry(new JournalPosition(new BigInteger(entry.get("sequence").asText()),
                    new JournalReceiver(entry.get("receiver").asText(),
                            entry.has("receiver_library") ? entry.get("receiver_library").asText() : scenario.get("library").asText())),
                    HexFormat.of().parseHex(entry.get("hex").asText())));
        }
        return entries;
    }

    static List<As400BindingDefinition> definitions() throws Exception {
        var result = new ArrayList<As400BindingDefinition>();
        try (var input = As400OnlineCensusReplayTest.class.getResourceAsStream("/native-transactions/multiple-clears-bindings.json")) {
            for (JsonNode binding : JSON.readTree(input).get("tables")) {
                String file = binding.get("file").asText();
                var table = new TableId("AEQA", binding.get("library").asText(), file);
                var identity = new TableIdentity(table, file, binding.get("member").asText(), binding.get("fileJournalId").asText(),
                        binding.get("memberJournalId").asText(), "TEST:0000000000000:4:1:false");
                // Controlled fixture layout; it does not assert native format/history provenance.
                result.add(new As400BindingDefinition(identity,
                        Table.editor().tableId(table).addColumn(Column.editor().name("ID").position(1).jdbcType(Types.INTEGER).type("INTEGER")
                                .optional(false).create()).setPrimaryKeyNames("ID").create(),
                        List.of(new As400BindingDefinition.Codec("BIN4", 4, 0, 0))));
            }
        }
        return result;
    }

    static void saveRecovery(String name, Map<String, Object> report) throws Exception {
        String evidence = System.getProperty("ibmi.census.recovery.evidence");
        if (evidence != null) {
            Files.createDirectories(Path.of(evidence));
            JSON.writerWithDefaultPrettyPrinter().writeValue(Path.of(evidence, name + ".json").toFile(), report);
        }
    }
}
