/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/** Explicit native discovery experiment; serialization grants no startup or publication authority. */
public final class As400NativeHistoryDiscoveryProbe {
    static Map<String, Object> pending(As400PendingReceiverEvidence.Sample sample) {
        return Map.of("journal", sample.journal(), "attached", sample.attached(), "receivers", sample.receivers().stream()
                .map(r -> Map.of("name", r.name(), "status", r.status(), "first", r.first().toString(), "last", r.last().toString(),
                        "previous", r.previous().map(n -> List.of(n.library(), n.name())).orElse(List.of()),
                        "next", r.next().map(n -> List.of(n.library(), n.name())).orElse(List.of()), "pending", r.pending()))
                .toList());
    }

    static As400PendingReceiverEvidence.Sample restore(JsonNode node) {
        var receivers = new ArrayList<As400PendingReceiverEvidence.Receiver>();
        require(node.path("receivers").isArray() && node.path("receivers").size() <= 128, "Bounded pending receiver sample required");
        for (var row : node.path("receivers")) {
            require(row.path("pending").isBoolean(), "Unknown pending receiver flag");
            receivers.add(new As400PendingReceiverEvidence.Receiver(name(row.path("name")), row.path("status").asText(),
                    new BigInteger(row.path("first").asText()), new BigInteger(row.path("last").asText()),
                    link(row.path("previous")), link(row.path("next")), row.path("pending").asBoolean()));
        }
        return new As400PendingReceiverEvidence.Sample(name(node.path("journal")), name(node.path("attached")), receivers);
    }

    static JournalPosition derive(JsonNode pending, JsonNode audit) throws Exception {
        require(pending.path("mode").asText().equals("pending") && pending.path("status").asText().equals("captured")
                && pending.path("transport_disconnected").asBoolean(), "Completed native pending observation required");
        var sample = restore(pending.path("pending_receivers"));
        var before = As400OnlineNativeEvidence.restoreSample(pending.path("directory_evidence"));
        var after = As400OnlineNativeEvidence.restoreSample(pending.path("directory_after"));
        As400OnlineNativeEvidence.requireStableDirectory(before, after);
        sample.verifyWindow(before.directory(), after.directory());
        require(audit.path("mode").asText().equals("audit") && audit.path("status").asText().equals("audit-captured-unqualified")
                && audit.path("transport_disconnected").asBoolean() && audit.path("pages").isArray() && audit.path("pages").size() == 1,
                "Single complete all-code audit required");
        var window = As400OnlineNativeEvidence.restore(audit.path("native_identity_evidence"));
        require(before.identities().objects().entrySet().containsAll(window.before().identities().objects().entrySet())
                && after.identities().objects().entrySet().containsAll(window.after().identities().objects().entrySet()),
                "Raw bracket and pending directory identities differ");
        require(window.from().equals(As400OnlineHandoff.position(audit.path("from")))
                && window.through().equals(As400OnlineHandoff.position(audit.path("through")))
                && window.from().receiver().equals(window.through().receiver()), "Pending bracket endpoints differ");
        var lower = before.directory().receivers().stream().filter(r -> r.name().equals(sample.attached())).findFirst().orElseThrow();
        var upper = after.directory().receivers().stream().filter(r -> r.name().equals(sample.attached())).findFirst().orElseThrow();
        require(sample.attached().equals(window.from().receiver()) && window.from().offset().compareTo(lower.last()) <= 0
                && upper.last().compareTo(window.through().offset()) <= 0, "Pending observation is outside the complete raw bracket");
        var page = audit.path("pages").get(0);
        require(window.from().equals(As400OnlineHandoff.position(page.path("from")))
                && window.through().equals(As400OnlineHandoff.position(page.path("through")))
                && page.path("continuation").isObject() && page.path("continuation").isEmpty()
                && page.path("count").asInt() == audit.path("entries").size(), "Incomplete pending bracket page");
        var bracket = new ArrayList<RawJournalPage.Entry>();
        BigInteger expected = window.from().offset();
        require(audit.path("entries").isArray() && audit.path("entries").size() <= 2000, "Bounded all-code bracket required");
        for (var item : audit.path("entries")) {
            require(item.path("raw").asText().length() <= 1_398_104, "Excessive raw pending bracket entry");
            byte[] data = Base64.getDecoder().decode(item.path("raw").asText());
            var entry = As400OnlineCensusReplay.load(data, item);
            require(entry.position().receiver().equals(window.from().receiver()) && entry.position().offset().equals(expected),
                    "Pending bracket omits, repeats or reorders raw source records");
            bracket.add(new RawJournalPage.Entry(entry.position(), new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(data, 0), data));
            expected = expected.add(BigInteger.ONE);
        }
        require(expected.equals(window.through().offset().add(BigInteger.ONE)), "Incomplete pending bracket");
        return sample.floor(bracket);
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 3, "pending/capture.json audit-directory new-floor.json required");
        var json = new ObjectMapper();
        Path source = Path.of(args[0]), auditRoot = Path.of(args[1]);
        require(Files.size(source) <= 1024 * 1024 && Files.size(auditRoot.resolve("capture.json")) <= 16 * 1024 * 1024, "Bounded native evidence required");
        var pending = json.readTree(source.toFile());
        var audit = json.readTree(auditRoot.resolve("capture.json").toFile());
        for (var item : audit.path("entries")) {
            var entry = As400OnlineCensusReplay.load(auditRoot, item);
            ((ObjectNode) item).put("raw", Base64.getEncoder().encodeToString(entry.bytes()));
        }
        var floor = derive(pending, audit);
        var result = Map.of("status", "native-history-floor-discovered", "startup_proof", false,
                "floor", Map.of("library", floor.receiver().library(), "receiver", floor.receiver().name(), "sequence", floor.offset().toString()));
        Files.writeString(Path.of(args[2]), json.writerWithDefaultPrettyPrinter().writeValueAsString(result), StandardOpenOption.CREATE_NEW);
    }

    private static JournalReceiver name(JsonNode node) {
        String library = node.path("library").asText(), name = node.path("name").asText();
        require(library.matches("[A-Z#$@][A-Z0-9_#$@]{0,9}") && name.matches("[A-Z#$@][A-Z0-9_#$@]{0,9}"), "Exact native receiver required");
        return new JournalReceiver(name, library);
    }

    private static Optional<JournalReceiver> link(JsonNode node) {
        require(node.isArray() && (node.size() == 0 || node.size() == 2), "Complete native receiver link required");
        return node.isEmpty() ? Optional.empty() : Optional.of(new JournalReceiver(node.get(1).asText(), node.get(0).asText()));
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
