/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.FixtureContext;

/** Offline mixed-column recovery with native bindings/identities; fixture origin remains an explicit assumption. */
public final class As400OnlineFenceReplay {
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        require(args.length == 1, "Existing isolated automatic fence capture required");
        Path root = Path.of(args[0]);
        Path output = root.resolve("fence-replay.json");
        require(!Files.exists(output), "Replay output already exists");
        var result = new LinkedHashMap<String, Object>();
        result.put("startup_proof", false);
        result.put("origin", "controller-established fixture anchor; not the online fence");
        try {
            JsonNode report = json(root.resolve("report.json"));
            require(report.path("status").asText().equals("captured-unqualified")
                    && report.path("fixture_cleanup").path("status").asText().equals("passed"), "Complete cleaned fixture required");
            String candidatePath = report.path("candidate").asText();
            require(candidatePath.matches("candidate-[1-5]/capture.json"), "Exact bounded candidate path required");
            var candidate = As400OnlineFenceEvidence.validate(json(root.resolve(candidatePath)));
            JsonNode capture = json(root.resolve("oracle/capture.json"));
            require(capture.path("status").asText().equals("captured"), "Complete raw native capture required");
            var window = As400OnlineNativeEvidence.restore(capture.path("native_identity_evidence"));
            require(window.before().identities().objects().entrySet().containsAll(candidate.identities().objects().entrySet()),
                    "Metadata and raw capture have different native object identities");
            var entries = new ArrayList<Entry>();
            require(capture.path("entries").isArray() && capture.path("entries").size() <= 20_000, "Bounded raw entries required");
            long bytes = 0;
            for (JsonNode entry : capture.get("entries")) {
                var loaded = As400OnlineCensusReplay.load(root.resolve("oracle"), entry);
                require((bytes += loaded.bytes().length) <= 64 * 1024 * 1024, "Raw capture exceeds byte bound");
                entries.add(loaded);
            }
            require(entries.size() > 1 && entries.get(0).position().equals(window.from())
                    && entries.get(entries.size() - 1).position().equals(window.through())
                    && entries.stream().anyMatch(e -> e.position().equals(candidate.boundary())), "Missing exact native anchor");
            for (var anchor : Map.of("baseline", entries.get(0), "final", entries.get(entries.size() - 1)).entrySet()) {
                JsonNode saved = json(root.resolve(anchor.getKey() + "/capture.json"));
                require(saved.path("status").asText().equals("captured") && saved.path("entries").size() == 1, "Missing independent fixture anchor");
                var actual = As400OnlineCensusReplay.load(root.resolve(anchor.getKey()), saved.get("entries").get(0));
                require(actual.position().equals(anchor.getValue().position())
                        && As400JournalCaptureContent.digest(actual.bytes(), actual.position()).equals(
                                As400JournalCaptureContent.digest(anchor.getValue().bytes(), anchor.getValue().position())),
                        "Changed fixture anchor");
            }
            var ranges = new LinkedHashMap<io.debezium.ibmi.db2.journal.retrieve.JournalReceiver, As400JournalOrder.Range>();
            As400OperatorCutReader.chain(window.before().directory(), window.from().receiver()).forEach(r -> {
                r.requireReadableProfile();
                ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last()));
            });
            var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), window.receiverTokens(), ranges);
            String fingerprint = "v1:" + As400OnlineHandoff.hash(Files.readAllBytes(root.resolve(candidatePath)));
            var baseline = As400Checkpoint.afterVerifiedBoundary(window.journalToken(), fingerprint, window.from(), order.incarnation(window.from()));
            result.put("recovery", As400OnlineCensusRecovery.verify(entries.subList(1, entries.size()), candidate.tables().definitions(),
                    new FixtureContext(order, baseline), records -> {
                        result.put("reference_output", records.stream().map(r -> Map.of("topic", r.topic(), "key", String.valueOf(r.key()),
                                "value", String.valueOf(r.value()))).toList());
                        validateOutput(records, report);
                    }));
            result.put("native_identity", "actual journal and receiver objects, no stand-in incarnations");
            result.put("status", "verified-offline");
        }
        catch (Exception e) {
            result.put("status", "rejected");
            result.put("error", e.toString());
            throw e;
        }
        finally {
            Files.writeString(output, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result), StandardOpenOption.CREATE_NEW);
        }
    }

    private static void validateOutput(List<SourceRecord> records, JsonNode report) {
        var expectedA = new ArrayList<Integer>();
        for (JsonNode call : report.path("writer_calls")) {
            require(call.path("status").asText().equals("completed"), "Writer did not complete");
            String sql = call.path("sql").asText();
            String prefix = "INSERT INTO " + report.path("library").asText() + ".A VALUES (";
            require(sql.startsWith(prefix) && sql.endsWith(")"), "Unknown controlled writer statement");
            expectedA.add(Integer.parseInt(sql.substring(prefix.length(), sql.length() - 1)));
        }
        var actualA = new ArrayList<Integer>();
        int mixed = 0, updates = 0, clears = 0, transactions = 0, transactionRows = 0;
        String active = null;
        for (SourceRecord record : records) {
            if (record.value() == null) {
                require(record.topic().equals("offline." + report.path("library").asText() + ".B") && active != null
                        && updates == 1 && ((Struct) record.key()).getInt32("ID") == 1, "Orphan or mismatched primary-key-change tombstone");
                updates++;
                continue;
            }
            require(record.value() instanceof Struct, "Unexpected tombstone or schema-free fixture output");
            var value = (Struct) record.value();
            if (record.topic().equals("offline.transaction")) {
                if (value.getString("status").equals("BEGIN")) {
                    require(active == null, "Overlapping output transaction markers");
                    active = value.getString("id");
                    transactionRows = 0;
                }
                else {
                    require(value.getString("status").equals("END") && value.getString("id").equals(active)
                            && value.getInt64("event_count") == transactionRows, "Incomplete output transaction");
                    active = null;
                    transactions++;
                }
                continue;
            }
            String topicPrefix = "offline." + report.path("library").asText() + ".";
            require(record.topic().startsWith(topicPrefix), "Fixture output belongs to another source");
            String table = record.topic().substring(topicPrefix.length());
            String operation = value.getString("op");
            if (active != null) {
                require(value.getStruct("transaction").getString("id").equals(active), "Data belongs to another output transaction");
                transactionRows++;
            }
            if (table.equals("A")) {
                require(operation.equals("c") && ((Struct) record.key()).getInt32("ID").equals(value.getStruct("after").getInt32("ID")),
                        "Unexpected independent writer operation/key");
                actualA.add(value.getStruct("after").getInt32("ID"));
            }
            else if (table.equals("B")) {
                require(active != null && (updates == 0 && operation.equals("d") && value.getStruct("before").getInt32("ID") == 1
                        && value.getStruct("after") == null && ((Struct) record.key()).getInt32("ID") == 1
                        || updates == 2 && operation.equals("c") && value.getStruct("after").getInt32("ID") == 12
                                && value.getStruct("before") == null && ((Struct) record.key()).getInt32("ID") == 12),
                        "Rolled back B clear/update escaped or committed primary-key change changed");
                updates++;
            }
            else if (table.equals("M") && operation.equals("t")) {
                require(record.key() == null && mixed == 0, "Clear must be keyless and precede its reload");
                clears++;
            }
            else if (table.equals("M")) {
                var after = value.getStruct("after");
                require(operation.equals("c") && ((Struct) record.key()).getInt32("ID") == 101
                        && after.getInt32("ID") == 101 && after.getString("TXT").equals("fence")
                        && after.get("AMOUNT").equals(new BigDecimal("123.45")) && after.getInt16("FLAG") == 1,
                        "Native mixed values/key changed in normalization or dispatch");
                mixed++;
            }
            else {
                throw new IllegalArgumentException("Unexpected fixture output table");
            }
        }
        require(!expectedA.isEmpty() && actualA.equals(expectedA) && mixed == 1 && updates == 3
                && clears == (report.path("mixed_clear_committed").asBoolean(false) ? 1 : 0)
                && active == null && transactions == 1 + clears, "Fixture output differs from completed writer operations");
    }

    private static JsonNode json(Path path) throws Exception {
        require(Files.size(path) <= 16 * 1024 * 1024, "Evidence file exceeds bound");
        return JSON.readTree(path.toFile());
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
