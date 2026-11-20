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
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.connector.db2as400.As400OnlineHandoff.Evidence;
import io.debezium.connector.db2as400.As400OnlineHandoff.Visibility;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * TEST ONLY. Verifies the last unknown-binding guard using an explicitly incomplete
 * diagnostic projection. Never authorizes dropping maintenance or starting from N/Z.
 * The complete native trace must remain separately rejected by the strict reader.
 */
public final class As400BoundaryTailReplay {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String PROFILE = "late-membership-diagnostic-projection-v1";

    public static void main(String[] args) throws Exception {
        require(args.length == 2, "Existing cleaned capture and new bundle path required");
        Path root = Path.of(args[0]);
        JsonNode report = json(root.resolve("report.json"));
        require(report.path("status").asText().equals("captured-unqualified")
                && report.path("fixture_cleanup").path("status").asText().equals("passed")
                && report.path("complete_tail").asText().equals("strict-pointer-rejection-preserved"),
                "Complete failed-maintenance evidence and cleanup required");
        ObjectNode bundle = JSON.createObjectNode();
        bundle.put("profile", PROFILE).put("startup_proof", false);
        bundle.set("metadata", json(root.resolve("candidate/before/capture.json")));
        bundle.set("report", report);
        for (String label : List.of("baseline-row", "autocommit-rows", "transaction-rows")) {
            var capture = json(root.resolve(label + "/capture.json"));
            require(capture.path("status").asText().equals("captured"), "Complete diagnostic raw projection required");
            for (var item : capture.path("entries")) {
                var entry = As400OnlineCensusReplay.load(root.resolve(label), item);
                ((ObjectNode) item).put("raw", Base64.getEncoder().encodeToString(entry.bytes()));
            }
            bundle.set(label, capture);
        }
        var results = verify(bundle);
        Files.writeString(Path.of(args[1]), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(bundle), StandardOpenOption.CREATE_NEW);
        Files.writeString(root.resolve("tail-replay.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(results), StandardOpenOption.CREATE_NEW);
    }

    static Map<String, Object> verify(JsonNode bundle) throws Exception {
        require(bundle.path("profile").asText().equals(PROFILE) && bundle.path("startup_proof").isBoolean()
                && !bundle.path("startup_proof").asBoolean(), "Explicit diagnostic-only projection required");
        var metadata = As400OnlineFenceEvidence.validateOffLock(bundle.path("metadata"));
        var definitions = metadata.tables().definitions();
        require(definitions.stream().noneMatch(d -> d.identity().physicalFile().equals("LATE")), "Late table already enrolled");
        Entry originalBaseline = entries(bundle.path("baseline-row")).get(0);
        require(entries(bundle.path("baseline-row")).size() == 1 && header(originalBaseline).getFile().equals("A")
                && type(originalBaseline).equals("R/PT") && header(originalBaseline).getCommitCycle().signum() == 0,
                "Completed controlled A insert required as baseline, never a boundary marker");
        var cases = new ArrayList<Map<String, Object>>();
        for (String label : List.of("autocommit-rows", "transaction-rows")) {
            var captured = bundle.path(label);
            var window = As400OnlineNativeEvidence.restore(captured.path("native_identity_evidence"));
            require(window.before().identities().objects().entrySet().containsAll(metadata.identities().objects().entrySet()),
                    "Metadata and raw projection native identity differ");
            var input = entries(captured);
            Entry baseline = label.equals("autocommit-rows") ? originalBaseline : input.get(0);
            require(type(baseline).equals("R/PT") && header(baseline).getFile().equals("A")
                    && header(baseline).getCommitCycle().signum() == 0, "Controlled completed baseline changed");
            var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
            As400OperatorCutReader.chain(window.before().directory(), baseline.position().receiver()).forEach(r -> {
                r.requireReadableProfile();
                ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last()));
            });
            var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), window.receiverTokens(), ranges);
            String fingerprint = "v1:" + As400OnlineHandoff.hash(JSON.writeValueAsBytes(bundle.path("metadata")));
            var checkpoint = As400Checkpoint.afterVerifiedBoundary(window.journalToken(), fingerprint, baseline.position(), order.incarnation(baseline.position()));
            var evidence = new Evidence(input, definitions, order, window.journalToken(), fingerprint, baseline.position(),
                    window.through(), Set.of(), As400OnlineHandoff.digest(input), Visibility.CONTROLLED_FIXTURE_ASSUMPTION);
            var output = new ArrayList<SourceRecord>();
            Entry rejected = null;
            Map<String, ?> saved;
            try (var dispatcher = new As400OnlineCensusRecovery.Output(definitions);
                    var replay = replay(evidence, checkpoint.toOffset(), output::add)) {
                for (Entry entry : input) {
                    if (order.compare(entry.position(), baseline.position()) <= 0) {
                        continue;
                    }
                    var beforeFailure = replay.checkpoint().toOffset();
                    int beforeOutput = output.size();
                    try {
                        if (replay.accept(header(entry), entry.bytes(), 0, entry.position()).isPresent()) {
                            replay.deliver(dispatcher.dispatcher);
                        }
                    }
                    catch (As400ReplayCoordinator.RecoveryException failure) {
                        if (!header(entry).getFile().equals("LATE") || !causedByMissingBinding(failure)) {
                            throw new IllegalArgumentException("Projection failed before reaching the actual unknown-binding guard", failure);
                        }
                        require(replay.checkpoint().toOffset().equals(beforeFailure) && output.size() == beforeOutput,
                                "Rejected input changed F/W or emitted output");
                        rejected = entry;
                        break;
                    }
                }
                require(rejected != null, "Unknown journaled object escaped strict normalization");
                saved = replay.checkpoint().toOffset();
                require(order.compare(replay.checkpoint().completedThrough(), rejected.position()) < 0, "W advanced over unknown object");
                int count = output.size();
                Entry last = input.get(input.size() - 1);
                expectRejected(() -> replay.accept(header(last), last.bytes(), 0, last.position()));
                require(output.size() == count && replay.checkpoint().toOffset().equals(saved), "Stopped coordinator published or advanced F/W");
            }
            List<Integer> rows = output.stream().filter(r -> r.value() instanceof Struct s && s.schema().field("op") != null)
                    .map(r -> ((Struct) r.value()).getStruct("after").getInt32("ID")).toList();
            require(rows.equals(label.equals("autocommit-rows") ? List.of(11) : List.of()), "Unknown object released buffered or subsequent rows");
            // JSON round trip and fresh buffers, no retained live state or rewritten offset.
            Map<String, ?> restored = JSON.readValue(JSON.writeValueAsBytes(saved), new TypeReference<Map<String, Object>>() {
            });
            var restarted = new ArrayList<SourceRecord>();
            try (var replay = replay(evidence, restored, restarted::add)) {
                Entry unknown = rejected;
                // Feed from the actual durable F, including the transaction start.
                expectRejected(() -> {
                    for (Entry entry : input) {
                        int compare = order.compare(entry.position(), replay.checkpoint().replayFrom());
                        if ((replay.checkpoint().replayInclusive() ? compare >= 0 : compare > 0)
                                && order.compare(entry.position(), unknown.position()) <= 0) {
                            replay.accept(header(entry), entry.bytes(), 0, entry.position());
                        }
                    }
                });
                require(restarted.isEmpty() && replay.checkpoint().toOffset().equals(saved), "Restart changed rejected output or F/W");
            }
            cases.add(Map.of("case", label, "published_known_rows", rows, "rejected_sequence", rejected.position().offset().toString(),
                    "retained_offset", saved, "fresh_restart", "rejected with unchanged F/W", "status", "passed"));
        }
        return Map.of("status", "verified-offline", "startup_proof", false, "acknowledgements", "modeled locally; no Kafka",
                "coverage", "deliberately incomplete diagnostic projections, no migration authority", "cases", cases);
    }

    private static As400ReplayCoordinator replay(Evidence evidence, Map<String, ?> offset, As400ReplayCoordinator.RecordSink sink) {
        var bindings = new LinkedHashMap<String, As400SourceNormalizer.TableBinding>();
        evidence.definitions().forEach(d -> require(bindings.put(d.identity().tableId().schema() + "/" + d.identity().physicalFile(),
                d.binding(1024 * 1024)) == null, "Duplicate native binding"));
        return new As400ReplayCoordinator(offset, evidence.journal(), evidence.fingerprint(), evidence.order(),
                new As400TransactionBuffer.Limits(128, 40_000, 2048, 64 * 1024 * 1024), 1024 * 1024, 20_000,
                (file, library) -> bindings.get(library + "/" + file), As400SourceNormalizer.SavepointPolicy.consumingTargets(),
                Map.of("server", "offline"), sink, new As400JournalLifecycle.Limits(20_000, 64 * 1024 * 1024, 128, 2048, 1024, 4096), true);
    }

    private static boolean causedByMissingBinding(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause()) {
            if (String.valueOf(t.getMessage()).contains("Missing verified table binding")) {
                return true;
            }
        }
        return false;
    }

    private static void expectRejected(Runnable action) {
        try {
            action.run();
        }
        catch (As400ReplayCoordinator.RecoveryException expected) {
            return;
        }
        throw new IllegalArgumentException("Expected strict coordinator rejection");
    }

    private static List<Entry> entries(JsonNode captured) throws Exception {
        require(captured.path("status").asText().equals("captured") && captured.path("entries").isArray()
                && !captured.path("entries").isEmpty() && captured.path("entries").size() <= 100, "Bounded raw projection required");
        var result = new ArrayList<Entry>();
        for (var item : captured.get("entries")) {
            byte[] bytes = Base64.getDecoder().decode(item.path("raw").asText());
            require(bytes.length <= 1048576 && As400OnlineHandoff.hash(bytes).equals(item.path("sha256").asText()), "Changed raw projection bytes");
            var entry = new Entry(As400OnlineHandoff.position(item.path("position")), bytes);
            As400SourceMetadata.from(header(entry), entry.position());
            require(type(entry).equals(item.path("type").asText()), "Raw type differs from summary");
            result.add(entry);
        }
        return List.copyOf(result);
    }

    private static io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader header(Entry entry) {
        return new EntryHeaderDecoder().decode(entry.bytes(), 0);
    }

    private static String type(Entry entry) {
        var header = header(entry);
        return header.getJournalCode() + "/" + header.getEntryType();
    }

    private static JsonNode json(Path path) throws Exception {
        require(Files.size(path) <= 16 * 1024 * 1024, "Evidence exceeds bound");
        return JSON.readTree(path.toFile());
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
