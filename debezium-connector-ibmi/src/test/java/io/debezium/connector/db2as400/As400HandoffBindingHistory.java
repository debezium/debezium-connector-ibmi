/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400OnlineHandoff.Evidence;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/** Test-only migration adapter to the existing ordinary Kafka binding history. */
final class As400HandoffBindingHistory {
    private static final Map<String, String> PARTITION = Map.of("server", "test");
    private final As400BindingHistory history;
    private final As400CaptureFingerprint.Policy policy;
    private final JournalPosition origin;

    As400HandoffBindingHistory(Configuration raw, JournalPosition origin) {
        this(raw, origin, As400KafkaBindingHistory::new);
    }

    As400HandoffBindingHistory(Configuration raw, JournalPosition origin, java.util.function.Supplier<SchemaHistory> factory) {
        this.origin = origin;
        String name = raw.getString("name");
        As400OnlineHandoff.require(name != null && name.matches("handoff[a-z]+")
                && Set.of("no-live-ibmi.invalid", "stdin-native-handoff.invalid").contains(raw.getString("database.hostname")),
                "Explicit isolated handoff history only");
        var historyConfiguration = Configuration.create().with(KafkaSchemaHistory.BOOTSTRAP_SERVERS, "broker:9092")
                .with(KafkaSchemaHistory.TOPIC, "bindings-" + name).with(SchemaHistory.NAME, name)
                .with(SchemaHistory.INTERNAL_CONNECTOR_CLASS, As400RpcConnector.class.getName()).with(SchemaHistory.INTERNAL_CONNECTOR_ID, name).build();
        history = new As400BindingHistory(factory, historyConfiguration, PARTITION);
        var output = new TreeMap<String, String>();
        output.put("version", "test-online-handoff-v1");
        for (String key : Set.of("probe.topic", "topic.prefix", "topic.naming.strategy", "time.precision.mode", "decimal.handling.mode",
                "binary.handling.mode", "tombstones.on.delete", "key.converter", "value.converter", "key.converter.schemas.enable", "value.converter.schemas.enable")) {
            output.put(key, "configured:" + raw.getString(key, ""));
        }
        var filters = new TreeMap<String, String>();
        for (String key : Set.of("table.include.list", "table.exclude.list", "schema.include.list", "schema.exclude.list")) {
            filters.put(key, "configured:" + raw.getString(key, ""));
        }
        policy = new As400CaptureFingerprint.Policy(filters, Set.of(),
                Map.of("version", "strict-native-v1"), Map.of("version", "native-short-boundary-v1"), output, false, true, PARTITION);
    }

    record Stored(As400Checkpoint checkpoint, Evidence runtime, As400BindingHistory.Recovered bindings) {
    }

    /** Persist before the legacy barrier; the returned candidate MUST NOT be published until that barrier is acknowledged. */
    Stored initialize(Evidence observed, As400Checkpoint candidate) throws Exception {
        As400OnlineHandoff.require(observed.order().compare(origin, candidate.replayFrom()) <= 0, "Binding epoch would start after F");
        var stored = history.recordBaseline(observed.journal(), origin, observed.order().incarnation(origin), observed.definitions(), policy);
        var checkpoint = new As400Checkpoint(candidate.journalIdentity(), stored.captureFingerprint(), candidate.replayFrom(), candidate.replayInclusive(),
                candidate.completedThrough(), candidate.replayReceiverIdentity(), candidate.completedReceiverIdentity());
        return recover(observed, checkpoint.toOffset());
    }

    /** Pending bindings are source evidence, never a completed-output offset. */
    record Pending(String journal, JournalPosition origin, String receiverIdentity, String fingerprint, List<As400BindingDefinition> definitions) {
    }

    Optional<Pending> pending(Map<String, ?> legacy) throws Exception {
        return history.pending(legacy, policy).map(value -> new Pending(value.journal(), value.origin(), value.receiverIdentity(),
                value.fingerprint(), value.definitions()));
    }

    Stored resumeCandidate(Evidence observed, As400Checkpoint candidate, Pending pending) throws Exception {
        As400OnlineHandoff.require(pending.origin().equals(origin) && pending.journal().equals(observed.journal())
                && pending.receiverIdentity().equals(observed.order().incarnation(origin)) && pending.definitions().equals(observed.definitions()),
                "Pending history differs from native handoff evidence");
        var checkpoint = new As400Checkpoint(candidate.journalIdentity(), pending.fingerprint(), candidate.replayFrom(), candidate.replayInclusive(),
                candidate.completedThrough(), candidate.replayReceiverIdentity(), candidate.completedReceiverIdentity());
        return recover(observed, checkpoint.toOffset());
    }

    Stored activate(Evidence evidence, Stored pending, Map<String, ?> barrier, java.util.function.Supplier<Map<String, ?>> durable) throws Exception {
        var activation = new As400LegacyHandoff(pending.checkpoint(), As400LegacyHandoff.strings(barrier),
                evidence.order().incarnation(As400LegacyHandoff.position(As400LegacyHandoff.strings(barrier))));
        history.recordLegacyHandoff(activation, policy, evidence.order(), durable);
        return recover(evidence, pending.checkpoint().toOffset());
    }

    /** Fresh backend recovery is mandatory; missing history never falls back to fixture/current metadata. */
    Stored recover(Evidence transport, Map<String, ?> offset) throws Exception {
        var recovered = recoverSource(transport.journal(), offset);
        var checkpoint = As400Checkpoint.fromOffset(offset, recovered.journalIdentity(), recovered.fingerprint());
        recovered.verify(checkpoint, transport.order());
        var runtime = new Evidence(transport.entries(), recovered.definitions(), transport.order(), recovered.journalIdentity(), recovered.fingerprint(),
                transport.lower(), transport.upper(), transport.inventory(), transport.historyDigest(), transport.visibility());
        return new Stored(checkpoint, runtime, recovered);
    }

    /** Native continuation validates current receiver ranges through its actual reader before publication. */
    As400BindingHistory.Recovered recoverSource(String journal, Map<String, ?> offset) throws Exception {
        return history.recover(offset, journal, policy);
    }
}
