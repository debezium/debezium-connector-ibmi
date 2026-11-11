/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.Map;
import java.util.TreeMap;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/**
 * A distinct recovery origin for an acknowledged legacy publication barrier.
 * Construction is not admission: the caller must independently qualify native
 * history and verify the exact barrier in Connect's durable offset store.
 */
record As400LegacyHandoff(As400Checkpoint initial, Map<String, String> barrier, String barrierReceiverIdentity) {

    static final String VERSION = "handoff.version";
    static final String CHECKPOINT = "handoff.checkpoint";
    static final String FINGERPRINT = "handoff.capture_fingerprint";

    As400LegacyHandoff {
        barrier = strings(barrier);
        require("1".equals(barrier.get(VERSION)) && digest(initial).equals(barrier.get(CHECKPOINT)), "Foreign or incomplete handoff barrier");
        require(!barrier.containsKey(FINGERPRINT) || initial.captureFingerprint().equals(barrier.get(FINGERPRINT)), "Foreign handoff binding selection");
        JournalPosition cut = position(barrier);
        // The ordinary history profile forbids sequence resets. Native receiver
        // order/incarnations are additionally checked at activation and replay.
        require(initial.replayFrom().offset().compareTo(initial.completedThrough().offset()) <= 0
                && initial.completedThrough().offset().compareTo(cut.offset()) <= 0, "Handoff invents completed output");
        As400Checkpoint.afterVerifiedBoundary(initial.journalIdentity(), initial.captureFingerprint(), cut, barrierReceiverIdentity);
    }

    static Map<String, String> bind(Map<String, ?> legacy, As400Checkpoint initial) {
        var fields = new TreeMap<>(strings(legacy));
        require(!fields.containsKey(VERSION) && !fields.containsKey(CHECKPOINT), "Legacy offset already contains a handoff");
        position(fields);
        fields.put(VERSION, "1");
        fields.put(CHECKPOINT, digest(initial));
        fields.put(FINGERPRINT, initial.captureFingerprint());
        return Map.copyOf(fields);
    }

    void verifyOrder(As400JournalOrder order) {
        require(order.compare(initial.replayFrom(), initial.completedThrough()) <= 0
                && order.compare(initial.completedThrough(), position(barrier)) <= 0
                && order.incarnation(position(barrier)).equals(barrierReceiverIdentity)
                && order.incarnation(initial.replayFrom()).equals(initial.replayReceiverIdentity())
                && order.incarnation(initial.completedThrough()).equals(initial.completedReceiverIdentity()), "Handoff receiver evidence differs");
    }

    void verifyResume(As400Checkpoint saved) {
        require(saved.journalIdentity().equals(initial.journalIdentity()) && saved.captureFingerprint().equals(initial.captureFingerprint()),
                "Handoff journal or capture policy differs");
        require(saved.replayFrom().offset().compareTo(initial.replayFrom().offset()) >= 0
                && saved.completedThrough().offset().compareTo(initial.completedThrough().offset()) >= 0,
                "Checkpoint predates activated handoff");
        sameCoordinate(saved.replayFrom(), saved.replayReceiverIdentity(), initial.replayFrom(), initial.replayReceiverIdentity());
        sameCoordinate(saved.completedThrough(), saved.completedReceiverIdentity(), initial.completedThrough(), initial.completedReceiverIdentity());
        require(!saved.replayFrom().equals(initial.replayFrom()) || !saved.replayInclusive() || initial.replayInclusive(),
                "Checkpoint replays an excluded handoff boundary");
    }

    Map<String, String> fields() {
        var fields = new TreeMap<String, String>();
        fields.put("version", "3");
        fields.put("kind", "legacy-handoff");
        fields.put("barrier.receiver.identity", barrierReceiverIdentity);
        initial.toOffset().forEach((key, value) -> fields.put("checkpoint." + key, value));
        barrier.forEach((key, value) -> fields.put("legacy." + key, value));
        fields.put("digest", As400CaptureFingerprint.hash("legacy-handoff-activation-v1", fields));
        return Map.copyOf(fields);
    }

    static As400LegacyHandoff restore(Map<String, String> fields) {
        var checkpoint = new TreeMap<String, String>();
        var barrier = new TreeMap<String, String>();
        fields.forEach((key, value) -> {
            if (key.startsWith("checkpoint.")) {
                checkpoint.put(key.substring(11), value);
            }
            if (key.startsWith("legacy.")) {
                barrier.put(key.substring(7), value);
            }
        });
        var restored = new As400LegacyHandoff(As400Checkpoint.fromOffset(checkpoint, checkpoint.get("journal_identity"), checkpoint.get("capture_fingerprint")),
                barrier, fields.get("barrier.receiver.identity"));
        require(restored.fields().equals(fields), "Noncanonical handoff history");
        return restored;
    }

    static Map<String, String> strings(Map<String, ?> input) {
        require(input != null && !input.isEmpty() && input.size() <= 64, "Bounded nonempty offset required");
        var fields = new TreeMap<String, String>();
        input.forEach((key, value) -> {
            require(key != null && key.length() <= 128 && value instanceof String && ((String) value).length() <= 512, "Malformed legacy offset field");
            fields.put(key, (String) value);
        });
        return Map.copyOf(fields);
    }

    static JournalPosition position(Map<String, String> legacy) {
        require("true".equals(legacy.get("offset.processed")), "Completed processed legacy stream required");
        return startingPosition(legacy);
    }

    /** An ordinary offset may require replay of its anchor; it cannot authorize a handoff barrier. */
    static JournalPosition startingPosition(Map<String, String> legacy) {
        require(!legacy.containsKey(As400Checkpoint.VERSION) && java.util.Set.of("true", "false").contains(legacy.getOrDefault("offset.processed", ""))
                && "true".equals(legacy.get("snapshot_completed")) && !legacy.containsKey("snapshot"), "Completed legacy stream required");
        require(!legacy.containsKey(VERSION) || "true".equals(legacy.get("offset.processed")), "Handoff barrier must be fully processed");
        require(!legacy.containsKey(FINGERPRINT) || legacy.containsKey(VERSION)
                && legacy.get(FINGERPRINT).matches("v1:[0-9a-f]{64}"), "Incomplete handoff binding selection");
        require(legacy.getOrDefault("offset.event_sequence", "").matches("[1-9][0-9]{0,19}"), "Malformed legacy sequence");
        var position = new JournalPosition(new BigInteger(legacy.get("offset.event_sequence")),
                new JournalReceiver(legacy.get("offset.receiver"), legacy.get("offset.receiver_library")));
        As400Checkpoint.validatePosition(position);
        return position;
    }

    private static String digest(As400Checkpoint checkpoint) {
        return As400CaptureFingerprint.hash("legacy-handoff-checkpoint-v1", checkpoint.toOffset());
    }

    private static void sameCoordinate(JournalPosition saved, String identity, JournalPosition initial, String initialIdentity) {
        require(!saved.offset().equals(initial.offset()) || saved.equals(initial) && identity.equals(initialIdentity), "Ambiguous handoff coordinate");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
