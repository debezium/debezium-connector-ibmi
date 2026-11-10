/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

/**
 * TEST ONLY: candidate bootstrap from nonempty transaction history. No closed
 * anchor, native connection, durable acknowledgement or production admission.
 * Complete inventory/terminal visibility and immutable historical bindings are
 * explicit controlled-fixture assumptions, NOT proved by a capture checksum.
 */
final class As400OnlineHandoff {
    enum Visibility {
        CONTROLLED_FIXTURE_ASSUMPTION
    }

    static final Map<String, String> PARTITION = Map.of("server", "test");
    static final int MAX_BYTES = 64 * 1024 * 1024;

    record Evidence(List<Entry> entries, List<As400BindingDefinition> definitions, As400JournalOrder order,
            String journal, String fingerprint, JournalPosition lower, JournalPosition upper,
            Set<BigInteger> inventory, String historyDigest, Visibility visibility) {
        Evidence {
            entries = entries.stream().map(e -> new Entry(e.position(), e.bytes().clone())).toList();
            definitions = List.copyOf(definitions);
            inventory = Set.copyOf(inventory);
        }
    }

    record Candidate(As400Checkpoint checkpoint, JournalPosition scan, Set<BigInteger> openCycles) {
        Candidate {
            openCycles = Set.copyOf(openCycles);
        }
    }

    /** Only this validated, temporary condition may defer admission. Other failures remain fatal. */
    static final class SelectedPrefixPending extends IllegalArgumentException {
        private final Set<BigInteger> cycles;

        SelectedPrefixPending(Set<BigInteger> cycles) {
            super("Open cycle has a selected legacy prefix; decline migration without repairing published rows");
            this.cycles = Set.copyOf(cycles);
        }

        Set<BigInteger> cycles() {
            return cycles;
        }
    }

    static Candidate prepare(Evidence evidence, JournalPosition cut, As400ConnectorConfig config) throws Exception {
        require(evidence.visibility() == Visibility.CONTROLLED_FIXTURE_ASSUMPTION, "Explicit unqualified fixture visibility assumption required");
        require(evidence.fingerprint().equals(fingerprint(evidence.definitions(), config, evidence.historyDigest())), "Capture policy or bindings changed");
        require(evidence.historyDigest().equals(digest(evidence.entries())), "Retained history differs from independently pinned capture");
        try {
            var candidate = As400HandoffReplay.prepare(owned(evidence), cut, config);
            return new Candidate(candidate.checkpoint(), candidate.scan(), candidate.openCycles());
        }
        catch (As400HandoffReplay.SelectedPrefixPending pending) {
            throw new SelectedPrefixPending(pending.cycles());
        }
    }

    static As400HandoffReplay.Evidence owned(Evidence evidence) {
        var entries = evidence.entries().stream().map(e -> new io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry(
                e.position(), new EntryHeaderDecoder().decode(e.bytes(), 0), e.bytes())).toList();
        return new As400HandoffReplay.Evidence(entries, evidence.definitions(), evidence.order(), evidence.journal(), evidence.fingerprint(),
                evidence.lower(), evidence.upper(), evidence.inventory(), PARTITION);
    }

    static As400ReplayCoordinator coordinator(Evidence evidence, Map<String, ?> offset, As400ReplayCoordinator.RecordSink sink) {
        return As400HandoffReplay.coordinator(owned(evidence), offset, sink);
    }

    static Evidence load(Path bundle, String expectedDigest, As400ConnectorConfig config) throws Exception {
        require(Files.isRegularFile(bundle, java.nio.file.LinkOption.NOFOLLOW_LINKS) && Files.size(bundle) <= MAX_BYTES, "Bounded owned capture bundle required");
        byte[] raw = Files.readAllBytes(bundle);
        require(hash(raw).equals(expectedDigest), "Capture bundle changed");
        var json = new ObjectMapper().readTree(raw);
        require(json.path("scope").asText().equals("controlled-capture-assumption-v1") && !json.path("startup_proof").asBoolean(true), "Fixture-only evidence required");
        var entries = new ArrayList<Entry>();
        long bytes = 0;
        require(json.path("entries").isArray() && json.path("entries").size() <= 20_000, "Bounded entry array required");
        for (JsonNode value : json.path("entries")) {
            byte[] data = Base64.getDecoder().decode(value.path("raw").asText());
            bytes += data.length;
            require(bytes <= MAX_BYTES && hash(data).equals(value.path("sha256").asText()), "Changed or excessive raw evidence");
            var position = position(value.get("position"));
            As400JournalCaptureContent.digest(data, position); // Validate original transport sections, never rewrite source fields.
            entries.add(new Entry(position, data));
        }
        var definitions = new ArrayList<As400BindingDefinition>();
        var document = DocumentReader.defaultReader().read(json.get("bindings").toString());
        for (var change : new JsonTableChangeSerializer().deserialize(document.getArray("tables"), false)) {
            definitions.add(As400BindingDefinition.restore(change.getTable()));
        }
        require(!definitions.isEmpty() && definitions.size() <= 16, "Bounded acquired bindings required");
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        JournalPosition prior = null;
        for (Entry entry : entries) {
            require(prior == null || entry.position().offset().compareTo(prior.offset()) > 0, "Captured SEQOPT(*CONT) ordering required");
            var range = ranges.get(entry.position().receiver());
            require(range == null || prior.receiver().equals(entry.position().receiver()), "Receiver chain revisits an old receiver");
            ranges.put(entry.position().receiver(), new As400JournalOrder.Range(range == null ? entry.position().offset() : range.first(), entry.position().offset()));
            prior = entry.position();
        }
        var tokens = new HashMap<JournalReceiver, String>();
        As400OnlineNativeEvidence.Window nativeWindow = null;
        String journal;
        if (json.has("native_identity_evidence") || json.has("binding_provenance") || json.has("bindings_raw") || json.has("bindings_sha256")) {
            // Actual native observations replace fixture incarnation tokens only.
            // Inventory completeness and historical bindings remain unproved.
            nativeWindow = As400OnlineNativeEvidence.restore(json.path("native_identity_evidence"));
            require(nativeWindow.from().equals(entries.get(0).position())
                    && nativeWindow.through().equals(entries.get(entries.size() - 1).position()), "Native identity window differs from captured history");
            require(nativeWindow.before().directory().journal().equals(new JournalReceiver("QSQJRN", json.path("library").asText())),
                    "Native journal differs from fixture capture");
            journal = nativeWindow.journalToken();
            var observedTokens = nativeWindow.receiverTokens();
            for (JournalReceiver receiver : ranges.keySet()) {
                require(observedTokens.containsKey(receiver), "Captured receiver lacks native identity evidence");
                tokens.put(receiver, observedTokens.get(receiver));
            }
            var provenance = json.path("binding_provenance");
            require("current-metadata-only-v1".equals(provenance.path("profile").asText())
                    && provenance.path("historical_schema_proof").isBoolean() && !provenance.path("historical_schema_proof").asBoolean()
                    && journal.equals(provenance.path("journal_identity").asText()), "Native binding acquisition provenance is missing or mismatched");
            require(provenance.path("bindings_sha256").asText().equals(json.path("bindings_sha256").asText())
                    && provenance.path("bindings_sha256").asText().matches("[0-9a-f]{64}"), "Native bindings differ from acquisition evidence");
            byte[] acquiredBindings = Base64.getDecoder().decode(json.path("bindings_raw").asText());
            require(hash(acquiredBindings).equals(provenance.path("bindings_sha256").asText())
                    && new ObjectMapper().readTree(acquiredBindings).equals(json.get("bindings")), "Acquired schema bytes differ from the supplied definitions");
            var acquiredThrough = position(provenance.path("through"));
            var acquiredFrom = position(provenance.path("from"));
            require(acquiredFrom.offset().signum() > 0 && acquiredFrom.receiver().equals(acquiredThrough.receiver())
                    && acquiredFrom.offset().compareTo(acquiredThrough.offset()) <= 0
                    && acquiredThrough.receiver().equals(entries.get(0).position().receiver())
                    && acquiredThrough.offset().compareTo(entries.get(0).position().offset()) <= 0,
                    "Current metadata cannot establish history before its acquisition");
        }
        else {
            // Capture-scoped TEST identities. This census capture did not establish native
            // journal/receiver incarnations; do not disguise these hashes as native proof.
            ranges.keySet()
                    .forEach(r -> tokens.put(r,
                            "fixture-" + As400CaptureFingerprint.hash("capture-receiver", Map.of("bundle", expectedDigest, "receiver", r.toString()))));
            journal = "fixture-" + expectedDigest;
        }
        var inventory = new HashSet<BigInteger>();
        require(json.path("inventory").isArray() && json.path("inventory").size() <= 256, "Bounded inventory required");
        for (JsonNode cycle : json.path("inventory")) {
            require(inventory.add(new BigInteger(cycle.asText())), "Duplicate inventory cycle");
        }
        if (json.has("control_history")) {
            var discovered = As400OnlineControlInventory.load(json.path("control_history"), entries, nativeWindow,
                    position(json.get("lower")), position(json.get("upper")));
            require(discovered.openAtLower().keySet().equals(inventory), "Exported inventory differs from native controls");
        }
        String digest = digest(entries);
        return new Evidence(entries, definitions, new As400JournalOrder(new ArrayList<>(ranges.keySet()), tokens, ranges), journal,
                fingerprint(definitions, config, digest), position(json.get("lower")), position(json.get("upper")), inventory, digest,
                Visibility.CONTROLLED_FIXTURE_ASSUMPTION);
    }

    static String fingerprint(List<As400BindingDefinition> definitions, As400ConnectorConfig config, String historyDigest) throws Exception {
        var filters = new TreeMap<String, String>();
        for (String key : List.of("table.include.list", "table.exclude.list", "schema.include.list", "schema.exclude.list")) {
            filters.put(key, "configured:" + config.getConfig().getString(key, ""));
        }
        var output = new TreeMap<String, String>(Map.of("version", "test-handoff-v1", "history", historyDigest));
        for (String key : List.of("topic.prefix", "topic.naming.strategy", "time.precision.mode", "decimal.handling.mode", "binary.handling.mode",
                "tombstones.on.delete", "key.converter", "value.converter", "key.converter.schemas.enable", "value.converter.schemas.enable")) {
            output.put(key, "configured:" + config.getConfig().getString(key, ""));
        }
        var policy = new As400CaptureFingerprint.Policy(filters, Set.of(),
                Map.of("version", "strict-native-v1"), Map.of("version", "owned-acquired-v1"),
                output, false, true, PARTITION);
        var fields = new TreeMap<String, String>();
        fields.put("policy", policy.fingerprint(definitions.stream().map(As400BindingDefinition::identity).toList()));
        var changes = new TableChanges();
        definitions.stream().sorted(java.util.Comparator.comparing(d -> d.identity().tableId().toString())).forEach(d -> changes.create(d.storedTable()));
        var document = Document.create();
        document.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
        fields.put("bindings", DocumentWriter.defaultWriter().write(document));
        return "v1:" + As400CaptureFingerprint.hash("test-handoff-bindings-v1", fields);
    }

    static String digest(List<Entry> entries) throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            for (Entry entry : entries) {
                out.writeUTF(entry.position().toString());
                out.writeInt(entry.bytes().length);
                out.write(entry.bytes());
                require(bytes.size() <= MAX_BYTES, "History exceeds bounded fixture");
            }
        }
        return hash(bytes.toByteArray());
    }

    static String hash(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    static JournalPosition position(JsonNode value) {
        return new JournalPosition(new BigInteger(value.path("sequence").asText()), new JournalReceiver(value.path("receiver").asText(), value.path("library").asText()));
    }

    static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
