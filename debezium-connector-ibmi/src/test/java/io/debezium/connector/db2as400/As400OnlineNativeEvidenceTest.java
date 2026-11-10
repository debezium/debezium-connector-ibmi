/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400OnlineNativeEvidence.Sample;
import io.debezium.connector.db2as400.As400OnlineNativeEvidence.Window;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Receiver;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

/** Synthetic faults using retained native bytes; these tests are not live native identity evidence. */
class As400OnlineNativeEvidenceTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    @TempDir
    Path directory;

    private Sample sample(Fixture f, long last) {
        return new Sample(new Observation(Fixture.JOURNAL, Fixture.RECEIVER, List.of(new Receiver(Fixture.RECEIVER, "ATTACHED", BigInteger.ONE,
                BigInteger.valueOf(last), Optional.empty(), Optional.empty(), "NO", "YES", "YES", "NO", "NO"))), new Candidates(f.nativeObjects));
    }

    @Test
    void ordinaryNativeIdentitiesRoundTripAndTailGrowthDoesNotBecomeStartupProof() throws Exception {
        var f = new Fixture(directory);
        var window = new Window(Fixture.position(31), Fixture.position(64), sample(f, 64), sample(f, 80));
        var restored = As400OnlineNativeEvidence.restore(JSON.valueToTree(window.report()));
        assertThat(restored.journalToken()).isEqualTo(f.journalIdentity).startsWith("ordinary-native-v1:");
        assertThat(restored.receiverTokens()).containsEntry(Fixture.RECEIVER, f.receiverIdentity);
        assertThat(restored.report()).containsEntry("startup_proof", false).containsEntry("visibility_proof", false).containsEntry("historical_schema_proof", false);
    }

    @Test
    void fullDirectoryObservationIncludesOldReceiversEvenOutsideTheCurrentRawBracket() throws Exception {
        var f = new Fixture(directory);
        var old = new JournalReceiver("OLD", Fixture.RECEIVER.library());
        var rows = List.of(new Receiver(old, "ONLINE", BigInteger.ONE, BigInteger.TEN, Optional.empty(), Optional.of(Fixture.RECEIVER),
                "NO", "YES", "NO", "NO", "NO"),
                new Receiver(Fixture.RECEIVER, "ATTACHED", BigInteger.valueOf(11), BigInteger.valueOf(80), Optional.of(old), Optional.empty(),
                        "NO", "YES", "YES", "NO", "NO"));
        var objects = new HashMap<>(f.nativeObjects);
        var oldTarget = As400OnlineNativeEvidence.target(old, As400JdbcNativeIdentityEvidence.Kind.RECEIVER);
        objects.put(oldTarget, new As400JdbcNativeIdentityEvidence.Identity(BigInteger.ONE, BigInteger.valueOf(123), BigInteger.ONE));
        var before = new Sample(new Observation(Fixture.JOURNAL, Fixture.RECEIVER, rows), new Candidates(objects));
        var restored = As400OnlineNativeEvidence.restoreSample(JSON.valueToTree(As400OnlineNativeEvidence.sample(before)));
        As400OnlineNativeEvidence.requireStableDirectory(before, restored);
        // Merely observing an older recording profile does not qualify it for
        // strict row reads; that reader's original profile check still rejects.
        assertThatThrownBy(rows.get(0)::requireReadableProfile).isInstanceOf(IllegalArgumentException.class);
        objects.remove(oldTarget);
        assertThatThrownBy(() -> As400OnlineNativeEvidence.requireStableDirectory(before, new Sample(before.directory(), new Candidates(objects))))
                .hasMessageContaining("identity");
        objects.put(oldTarget, new As400JdbcNativeIdentityEvidence.Identity(BigInteger.ONE, BigInteger.valueOf(123), BigInteger.TWO));
        assertThatThrownBy(() -> As400OnlineNativeEvidence.requireStableDirectory(before, new Sample(before.directory(), new Candidates(objects))))
                .hasMessageContaining("identity");
    }

    @ParameterizedTest
    @ValueSource(strings = { "identity", "missing-object", "duplicate-object", "attachment", "range", "thread", "link", "outside", "proof" })
    void rejectsIncompleteChangedOrPromotedObservations(String fault) throws Exception {
        var f = new Fixture(directory);
        ObjectNode node = JSON.valueToTree(new Window(Fixture.position(31), Fixture.position(64), sample(f, 64), sample(f, 64)).report());
        ObjectNode after = (ObjectNode) node.get("after");
        ObjectNode receiver = (ObjectNode) after.get("receivers").get(0);
        ArrayNode objects = (ArrayNode) after.get("objects");
        switch (fault) {
            case "identity" -> ((ObjectNode) objects.get(0)).put("generation", "999");
            case "missing-object" -> objects.remove(0);
            case "duplicate-object" -> objects.add(objects.get(0).deepCopy());
            case "attachment" -> ((ObjectNode) after.get("attached")).put("name", "OTHER");
            case "range" -> receiver.put("last", "63");
            case "thread" -> ((ArrayNode) receiver.get("profile")).set(2, JSON.getNodeFactory().textNode("NO"));
            case "link" -> receiver.set("previous", after.get("attached").deepCopy());
            case "outside" -> ((ObjectNode) node.get("through")).put("sequence", "65");
            case "proof" -> node.put("startup_proof", true);
            default -> throw new AssertionError(fault);
        }
        assertThatThrownBy(() -> As400OnlineNativeEvidence.restore(node)).isInstanceOf(Exception.class);
    }

    @Test
    void handoffUsesObservedNativeTokensWhileCompletenessRemainsAnExplicitAssumption() throws Exception {
        var f = new Fixture(directory);
        var evidence = load(bundle(f));
        var candidate = As400OnlineHandoff.prepare(evidence, Fixture.position(42), config());
        assertThat(candidate.checkpoint().toOffset()).containsEntry("journal_identity", f.journalIdentity)
                .containsEntry("replay_from.receiver_identity", f.receiverIdentity);
        assertThat(evidence.visibility()).isEqualTo(As400OnlineHandoff.Visibility.CONTROLLED_FIXTURE_ASSUMPTION);
        assertThat(Files.exists(directory.resolve("history.jsonl"))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "late-schema", "schema-window", "schema-bytes", "schema-document", "schema-digest", "schema-journal", "schema-proof", "window",
            "missing-window" })
    void nativeObservationsNeverWaiveHistoricalBindingOrExactIntervalChecks(String fault) throws Exception {
        var f = new Fixture(directory);
        ObjectNode bundle = bundle(f);
        ObjectNode provenance = (ObjectNode) bundle.get("binding_provenance");
        switch (fault) {
            case "late-schema" -> ((ObjectNode) provenance.get("through")).put("sequence", "64");
            case "schema-window" -> ((ObjectNode) provenance.get("from")).put("sequence", "30");
            case "schema-bytes" -> bundle.put("bindings_raw", Base64.getEncoder().encodeToString("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
            case "schema-document" -> bundle.set("bindings", JSON.createObjectNode());
            case "schema-digest" -> provenance.put("bindings_sha256", "0".repeat(64));
            case "schema-journal" -> provenance.put("journal_identity", "fixture");
            case "schema-proof" -> provenance.put("historical_schema_proof", true);
            case "window" -> ((ObjectNode) bundle.get("native_identity_evidence").get("from")).put("sequence", "30");
            case "missing-window" -> bundle.remove("native_identity_evidence");
            default -> throw new AssertionError(fault);
        }
        assertThatThrownBy(() -> load(bundle)).isInstanceOf(Exception.class);
    }

    private As400OnlineHandoff.Evidence load(ObjectNode bundle) throws Exception {
        byte[] raw = JSON.writeValueAsBytes(bundle);
        Path path = directory.resolve("bundle.json");
        Files.write(path, raw);
        return As400OnlineHandoff.load(path, As400OnlineHandoff.hash(raw), config());
    }

    private As400ConnectorConfig config() {
        return new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                .with("database.schema", "DCOC1107").with("table.include.list", "DCOC1107\\.A").with("provide.transaction.metadata", false)
                .with("skipped.operations", "none").build());
    }

    private ObjectNode bundle(Fixture f) throws Exception {
        var changes = new TableChanges();
        f.definitions.forEach(d -> changes.create(d.storedTable()));
        var doc = Document.create();
        doc.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
        byte[] bindings = DocumentWriter.defaultWriter().write(doc).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        ObjectNode result = JSON.createObjectNode();
        result.put("scope", "controlled-capture-assumption-v1").put("startup_proof", false).put("library", Fixture.JOURNAL.library());
        result.set("bindings", JSON.readTree(bindings));
        result.put("bindings_raw", Base64.getEncoder().encodeToString(bindings)).put("bindings_sha256", As400OnlineHandoff.hash(bindings));
        result.set("lower", position(40));
        result.set("upper", position(42));
        result.set("inventory", JSON.valueToTree(List.of("31")));
        ArrayNode entries = result.putArray("entries");
        for (var e : f.entries) {
            entries.add(JSON.valueToTree(Map.of("position", position(e.position().offset().longValueExact()), "raw", Base64.getEncoder().encodeToString(e.data()),
                    "sha256", As400OnlineHandoff.hash(e.data()))));
        }
        result.set("native_identity_evidence", JSON.valueToTree(new Window(f.entries.get(0).position(), f.entries.get(f.entries.size() - 1).position(),
                sample(f, 64), sample(f, 64)).report()));
        result.set("binding_provenance", JSON.valueToTree(Map.of("profile", "current-metadata-only-v1", "historical_schema_proof", false,
                "from", position(29), "through", position(29), "journal_identity", f.journalIdentity, "bindings_sha256", As400OnlineHandoff.hash(bindings))));
        return result;
    }

    private static ObjectNode position(long sequence) {
        return JSON.valueToTree(Map.of("sequence", Long.toString(sequence), "receiver", Fixture.RECEIVER.name(), "library", Fixture.RECEIVER.library()));
    }
}
