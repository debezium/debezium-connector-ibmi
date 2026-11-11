/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.FixtureContext;
import io.debezium.connector.db2as400.As400OnlineHandoff.Evidence;
import io.debezium.connector.db2as400.As400OnlineHandoff.Visibility;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/** TEST ONLY. Native bytes and identities; qualified origin and exact legacy acknowledgement remain premises. */
public final class As400ProspectiveReplay {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PROFILE = "prospective-native-fixture-v1";
    private static final String BOUNDARY_PROFILE = "prospective-native-boundary-v1";

    public static void main(String[] args) throws Exception {
        require(args.length == 2 || args.length == 3 && args[2].equals("boundary"), "Cleaned capture and new output bundle path required [boundary]");
        Path root = Path.of(args[0]);
        var report = json(root.resolve("report.json"));
        require(report.path("status").asText().equals("captured-unqualified")
                && report.path("fixture_cleanup").path("status").asText().equals("passed"), "Cleaned successful capture required");
        ObjectNode bundle = JSON.createObjectNode();
        bundle.put("profile", PROFILE).put("startup_proof", false);
        bundle.set("metadata", json(root.resolve("candidate/before/capture.json")));
        bundle.set("origin", report.path("origin"));
        bundle.set("cut", report.path("cut"));
        if (args.length == 3) {
            var boundary = bundle.putObject("boundary");
            boundary.put("profile", "native-short-boundary-observation-v1").put("startup_proof", false);
            for (String label : List.of("before", "after")) {
                boundary.set(label, json(root.resolve("candidate/" + label + "/capture.json")));
            }
            for (String label : List.of("request", "interval")) {
                boundary.set(label, json(root.resolve("candidate/" + label + ".json")));
            }
            boundary.set("pulse", json(root.resolve("candidate/pulse/pulse.json")));
            boundary.put("statement", Files.readString(root.resolve("candidate/pulse/statement.sql")).stripTrailing());
            var raw = json(root.resolve("candidate/raw/capture.json"));
            for (var item : raw.path("entries")) {
                var loaded = As400OnlineCensusReplay.load(root.resolve("candidate/raw"), item);
                ((ObjectNode) item).put("raw", Base64.getEncoder().encodeToString(loaded.bytes()));
            }
            boundary.set("raw", raw);
            bundle.put("profile", BOUNDARY_PROFILE);
        }
        for (String label : List.of("older-controls", "prospective-rows")) {
            JsonNode capture = json(root.resolve(label + "/capture.json"));
            for (JsonNode item : capture.path("entries")) {
                var loaded = As400OnlineCensusReplay.load(root.resolve(label), item);
                ((ObjectNode) item).put("raw", Base64.getEncoder().encodeToString(loaded.bytes()));
            }
            bundle.set(label, capture);
        }
        var result = verify(bundle);
        Files.writeString(Path.of(args[1]), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(bundle), StandardOpenOption.CREATE_NEW);
        Path resultFile = args.length == 3 ? Path.of(args[1] + ".replay.json") : root.resolve("prospective-replay.json");
        Files.writeString(resultFile, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result), StandardOpenOption.CREATE_NEW);
    }

    static NativeInput prepare(JsonNode bundle, As400ConnectorConfig config) throws Exception {
        return prepare(bundle, config, As400OnlineFenceEvidence::validateOffLock);
    }

    static NativeInput prepare(JsonNode bundle, As400ConnectorConfig config, As400OnlineFenceEvidence.MetadataValidator validator) throws Exception {
        boolean withBoundary = bundle.path("profile").asText().equals(BOUNDARY_PROFILE);
        require((withBoundary || bundle.path("profile").asText().equals(PROFILE)) && withBoundary == bundle.has("boundary") && bundle.path("startup_proof").isBoolean()
                && !bundle.path("startup_proof").asBoolean(), "Explicit unqualified prospective fixture required");
        var metadata = validator.validate(bundle.path("metadata"));
        var future = entries(bundle.path("prospective-rows"), "captured");
        var controls = entries(bundle.path("older-controls"), "controls-captured");
        var window = As400OnlineNativeEvidence.restore(bundle.path("prospective-rows").path("native_identity_evidence"));
        var older = As400OnlineNativeEvidence.restore(bundle.path("older-controls").path("native_identity_evidence"));
        if (bundle.has("history_discovery")) {
            require(withBoundary, "Native history discovery requires the complete short-boundary bracket");
            var discovery = bundle.get("history_discovery");
            var discoveredFloor = As400NativeHistoryDiscoveryProbe.derive(discovery, bundle.path("boundary").path("raw"));
            require(discoveredFloor.equals(older.from()), "Older controls do not begin at the natively discovered history floor");
            var directoryBefore = As400OnlineNativeEvidence.restoreSample(discovery.path("directory_evidence"));
            var directoryAfter = As400OnlineNativeEvidence.restoreSample(discovery.path("directory_after"));
            require(directoryBefore.identities().objects().entrySet().containsAll(older.before().identities().objects().entrySet())
                    && directoryAfter.identities().objects().entrySet().containsAll(older.after().identities().objects().entrySet()),
                    "Control back-read identities differ from the pending-receiver observation");
        }
        require(window.journalToken().equals(older.journalToken()) && window.receiverTokens().equals(older.receiverTokens())
                && window.through().equals(older.through())
                && window.before().identities().objects().entrySet().containsAll(metadata.identities().objects().entrySet()),
                "Native controls, rows and bindings have different journal/receiver identities or cuts");
        var origin = As400OnlineHandoff.position(bundle.path("origin"));
        var cut = As400OnlineHandoff.position(bundle.path("cut"));
        require(window.from().equals(origin), "Raw prospective interval must begin at the candidate origin");
        if (withBoundary) {
            var boundary = As400ShortBoundaryEvidence.validate(bundle.path("boundary"), validator);
            require(boundary.origin().equals(origin) && boundary.metadata().equals(metadata)
                    && boundary.window().journalToken().equals(window.journalToken())
                    && boundary.window().receiverTokens().equals(window.receiverTokens())
                    && boundary.auditedThrough().receiver().equals(cut.receiver())
                    && boundary.auditedThrough().offset().compareTo(cut.offset()) <= 0, "Handoff does not match observed native boundary");
        }
        var inventory = As400OnlineControlInventory.reconstruct(controls, older, origin);
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        As400OperatorCutReader.chain(older.before().directory(), older.from().receiver()).forEach(r -> {
            r.requireReadableProfile();
            ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last()));
        });
        var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), older.receiverTokens(), ranges);
        var definitions = metadata.tables().definitions();
        String digest = As400OnlineHandoff.digest(future);
        var evidence = new Evidence(future, definitions, order, window.journalToken(),
                As400OnlineHandoff.fingerprint(definitions, config, digest), origin, origin,
                inventory.openAtLower().keySet(), digest, Visibility.CONTROLLED_FIXTURE_ASSUMPTION);
        var prepared = As400ProspectiveHandoff.prepare(evidence, controls, origin, cut, config);
        var checkpoint = prepared.candidate().checkpoint();
        require(order.compare(checkpoint.replayFrom(), origin) >= 0 && checkpoint.completedThrough().equals(cut),
                "Migration requires old row schemas or invented completed output");
        // First native fixture profile uses a successful retry after all old
        // cycles completed. The more general old-cycle model is not admitted here.
        require(inventory.openAtLower().isEmpty(), "Native fixture still has prior-schema cycles");
        var runtime = new Evidence(future, definitions, order, window.journalToken(), evidence.fingerprint(),
                future.get(0).position(), future.get(0).position(), java.util.Set.of(), digest, Visibility.CONTROLLED_FIXTURE_ASSUMPTION);
        var candidate = As400OnlineHandoff.prepare(runtime, cut, config);
        require(candidate.checkpoint().replayFrom().equals(checkpoint.replayFrom())
                && candidate.checkpoint().completedThrough().equals(checkpoint.completedThrough()), "Prospective and complete replay cuts differ");
        return new NativeInput(runtime, candidate, origin, cut);
    }

    record NativeInput(Evidence evidence, As400OnlineHandoff.Candidate candidate,
            io.debezium.ibmi.db2.journal.retrieve.JournalPosition origin, io.debezium.ibmi.db2.journal.retrieve.JournalPosition cut) {
    }

    static Map<String, Object> verify(JsonNode bundle) throws Exception {
        return verify(bundle, As400OnlineFenceEvidence::validateOffLock);
    }

    static Map<String, Object> verify(JsonNode bundle, As400OnlineFenceEvidence.MetadataValidator validator) throws Exception {
        var definitions = validator.validate(bundle.path("metadata")).tables().definitions();
        var config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                .with("database.schema", definitions.get(0).identity().tableId().schema()).with("skipped.operations", "none")
                .with("provide.transaction.metadata", false).build());
        var prepared = prepare(bundle, config, validator);
        var checkpoint = prepared.candidate().checkpoint();
        var future = prepared.evidence().entries();
        var order = prepared.evidence().order();
        // Reuse the actual task/dispatcher and the existing restart, false enqueue,
        // throwing enqueue, missing-offset/anchor and changed-identity matrix.
        var retained = future.stream().filter(e -> {
            int compared = order.compare(e.position(), checkpoint.replayFrom());
            return checkpoint.replayInclusive() ? compared >= 0 : compared > 0;
        }).toList();
        var recovery = As400OnlineCensusRecovery.verify(retained, definitions, new FixtureContext(order, checkpoint), false, records -> {
            require(records.size() == 3, "Prefix replayed or post-cut output missing");
            var unrelated = (Struct) records.get(0).value();
            var clear = (Struct) records.get(1).value();
            var reload = (Struct) records.get(2).value();
            require(records.get(0).topic().endsWith(".A") && unrelated.getString("op").equals("c")
                    && unrelated.getStruct("after").getInt32("ID") == 14, "Unrelated row did not progress before clear commit");
            require(records.get(1).topic().endsWith(".B") && records.get(1).key() == null && clear.getString("op").equals("t"),
                    "Missing ordered keyless truncate");
            require(records.get(2).topic().endsWith(".B") && reload.getString("op").equals("c")
                    && reload.getStruct("after").getInt32("ID") == 101, "Reload did not follow clear");
        });
        return Map.of("status", "verified-offline", "startup_proof", false, "candidate_offset", checkpoint.toOffset(),
                "schema_origin", bundle.path("origin"), "recovery", recovery,
                "qualification", bundle.has("boundary")
                        ? "native boundary observations joined; ordinary fixed-schema and retained-history prerequisites remain; legacy acknowledgement modeled"
                        : "native origin remains candidate; legacy cut acknowledgement modeled, no Kafka");
    }

    private static List<Entry> entries(JsonNode capture, String status) throws Exception {
        require(capture.path("status").asText().equals(status) && capture.path("entries").isArray()
                && !capture.path("entries").isEmpty() && capture.path("entries").size() <= 100, "Bounded complete native capture required");
        var result = new ArrayList<Entry>();
        long bytes = 0;
        for (var item : capture.get("entries")) {
            String raw = item.path("raw").asText();
            require(raw.length() <= 1_398_104, "Excessive base64 capture");
            byte[] data = Base64.getDecoder().decode(raw);
            require((bytes += data.length) <= 16 * 1024 * 1024, "Excessive capture bytes");
            result.add(As400OnlineCensusReplay.load(data, item));
        }
        return List.copyOf(result);
    }

    private static JsonNode json(Path file) throws Exception {
        require(Files.size(file) <= 16 * 1024 * 1024, "Excessive capture document");
        return JSON.readTree(file.toFile());
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
