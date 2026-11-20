/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Receiver;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/**
 * TEST ONLY. Bind a bounded captured interval to actual ordinary native identity
 * observations. No census completeness, historical schema, maintenance exclusion
 * or automatic migration proof is created by matching observations.
 */
final class As400OnlineNativeEvidence {
    static final String PROFILE = "online-native-observation-v1";

    record Sample(Observation directory, Candidates identities) {
        As400NativeJournalWindow.Sample nativeSample() {
            return new As400NativeJournalWindow.Sample(directory, identities);
        }
    }

    record Window(JournalPosition from, JournalPosition through, Sample before, Sample after) {
        Window {
            new As400NativeJournalWindow.Window(from, through, before.nativeSample(), after.nativeSample());
        }

        As400NativeJournalWindow.Window nativeWindow() {
            return new As400NativeJournalWindow.Window(from, through, before.nativeSample(), after.nativeSample());
        }

        String journalToken() {
            return nativeWindow().journalToken();
        }

        Map<JournalReceiver, String> receiverTokens() {
            return nativeWindow().receiverTokens();
        }

        Map<String, Object> report() {
            return Map.of("profile", PROFILE, "startup_proof", false, "visibility_proof", false, "historical_schema_proof", false,
                    "from", position(from), "through", position(through), "before", sample(before), "after", sample(after));
        }
    }

    static Sample collect(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities, JournalPosition from) throws Exception {
        var sample = As400NativeJournalWindow.collect(journals, identities, from);
        return new Sample(sample.directory(), sample.identities());
    }

    static Sample collectDirectory(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities) throws Exception {
        var sample = As400NativeJournalWindow.collectDirectory(journals, identities);
        return new Sample(sample.directory(), sample.identities());
    }

    static void requireStableDirectory(Sample before, Sample after) {
        As400NativeJournalWindow.requireStableDirectory(before.nativeSample(), after.nativeSample());
    }

    static List<Target> targets(Observation directory, JournalPosition from) {
        return As400NativeJournalWindow.targets(directory, from);
    }

    static Target target(JournalReceiver name, Kind kind) {
        return As400NativeJournalWindow.target(name, kind);
    }

    static Window restore(JsonNode node) {
        require(PROFILE.equals(node.path("profile").asText()) && node.path("startup_proof").isBoolean()
                && !node.path("startup_proof").asBoolean() && node.path("visibility_proof").isBoolean() && !node.path("visibility_proof").asBoolean()
                && node.path("historical_schema_proof").isBoolean() && !node.path("historical_schema_proof").asBoolean(), "Unqualified native observation required");
        return new Window(As400OnlineHandoff.position(node.path("from")), As400OnlineHandoff.position(node.path("through")),
                restoreSample(node.path("before")), restoreSample(node.path("after")));
    }

    static Map<String, Object> sample(Sample sample) {
        return Map.of("journal", name(sample.directory().journal()), "attached", name(sample.directory().attached()),
                "receivers", sample.directory().receivers().stream().map(r -> Map.of("name", name(r.name()), "status", r.status(),
                        "first", r.first().toString(), "last", r.last().toString(), "previous", r.previous().map(As400OnlineNativeEvidence::name).orElse(Map.of()),
                        "next", r.next().map(As400OnlineNativeEvidence::name).orElse(Map.of()),
                        "profile", List.of(r.minimizeFiles(), r.job(), r.thread(), r.luw(), r.xid()))).toList(),
                "objects", sample.identities().objects().entrySet().stream().sorted(java.util.Comparator.comparing(e -> e.getKey().path()))
                        .map(e -> Map.of("path", e.getKey().path(), "kind", e.getKey().kind().name(), "filesystem", e.getValue().fileSystem().toString(),
                                "file", e.getValue().file().toString(), "generation", e.getValue().generation().toString()))
                        .toList());
    }

    static Sample restoreSample(JsonNode node) {
        require(node.path("receivers").isArray() && node.path("receivers").size() > 0 && node.path("receivers").size() <= 16
                && node.path("objects").isArray() && node.path("objects").size() <= 17, "Bounded native sample required");
        var receivers = new ArrayList<Receiver>();
        for (JsonNode r : node.path("receivers")) {
            JsonNode p = r.path("profile");
            require(p.isArray() && p.size() == 5, "Complete receiver recording profile required");
            receivers.add(new Receiver(restoreName(r.path("name")), r.path("status").asText(), new BigInteger(r.path("first").asText()),
                    new BigInteger(r.path("last").asText()), optionalName(r.path("previous")), optionalName(r.path("next")),
                    p.get(0).asText(), p.get(1).asText(), p.get(2).asText(), p.get(3).asText(), p.get(4).asText()));
        }
        var objects = new LinkedHashMap<Target, Identity>();
        for (JsonNode o : node.path("objects")) {
            var target = new Target(o.path("path").asText(), Kind.valueOf(o.path("kind").asText()));
            require(objects.putIfAbsent(target, new Identity(new BigInteger(o.path("filesystem").asText()), new BigInteger(o.path("file").asText()),
                    new BigInteger(o.path("generation").asText()))) == null, "Duplicate native identity");
        }
        return new Sample(new Observation(restoreName(node.path("journal")), restoreName(node.path("attached")), receivers), new Candidates(objects));
    }

    private static Optional<JournalReceiver> optionalName(JsonNode node) {
        require(node.isObject(), "Missing receiver link observation");
        return node.isEmpty() ? Optional.empty() : Optional.of(restoreName(node));
    }

    private static JournalReceiver restoreName(JsonNode node) {
        var name = new JournalReceiver(node.path("name").asText(), node.path("library").asText());
        target(name, Kind.RECEIVER); // Same canonical basic system-name restriction.
        return name;
    }

    private static Map<String, String> name(JournalReceiver name) {
        return Map.of("name", name.name(), "library", name.library());
    }

    private static Map<String, String> position(JournalPosition position) {
        return Map.of("sequence", position.offset().toString(), "receiver", position.receiver().name(), "library", position.receiver().library());
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
