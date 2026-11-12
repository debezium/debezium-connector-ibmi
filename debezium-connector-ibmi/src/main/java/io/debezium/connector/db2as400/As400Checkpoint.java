/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/** Immutable offset-store representation. It contains F and W, never the volatile scan cursor R. */
record As400Checkpoint(String journalIdentity, String captureFingerprint, JournalPosition replayFrom,
        boolean replayInclusive, JournalPosition completedThrough, String replayReceiverIdentity, String completedReceiverIdentity,
        Map<String, String> snapshotState, As400SchemaState schemaState) {

    static final String VERSION = "offset.version";
    private static final String FLOOR = "replay_from";
    private static final String COMPLETED = "completed_through";
    private static final Set<String> KEYS = Set.of(VERSION, "journal_identity", "capture_fingerprint",
            FLOOR + ".sequence", FLOOR + ".receiver", FLOOR + ".receiver_library", FLOOR + ".receiver_identity", FLOOR + ".inclusive",
            COMPLETED + ".sequence", COMPLETED + ".receiver", COMPLETED + ".receiver_library", COMPLETED + ".receiver_identity");
    private static final BigInteger MAX_SEQUENCE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);

    As400Checkpoint(String journalIdentity, String captureFingerprint, JournalPosition replayFrom, boolean replayInclusive,
                    JournalPosition completedThrough, String replayReceiverIdentity, String completedReceiverIdentity) {
        this(journalIdentity, captureFingerprint, replayFrom, replayInclusive, completedThrough, replayReceiverIdentity, completedReceiverIdentity, Map.of());
    }

    As400Checkpoint(String journalIdentity, String captureFingerprint, JournalPosition replayFrom, boolean replayInclusive,
                    JournalPosition completedThrough, String replayReceiverIdentity, String completedReceiverIdentity, Map<String, String> snapshotState) {
        this(journalIdentity, captureFingerprint, replayFrom, replayInclusive, completedThrough, replayReceiverIdentity, completedReceiverIdentity,
                snapshotState, As400SchemaState.EMPTY);
    }

    As400Checkpoint {
        Objects.requireNonNull(schemaState);
        snapshotState = Map.copyOf(snapshotState);
        if (!As400IncrementalSnapshotState.fields(snapshotState).equals(snapshotState)) {
            throw new IllegalArgumentException("Unexpected incremental snapshot checkpoint fields");
        }
        identifier(journalIdentity, 256);
        if (captureFingerprint == null || !captureFingerprint.matches("v[12]:[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Expected a versioned SHA-256 capture fingerprint");
        }
        validatePosition(replayFrom);
        validatePosition(completedThrough);
        identifier(replayReceiverIdentity, 256);
        identifier(completedReceiverIdentity, 256);
        if (replayFrom.receiver().equals(completedThrough.receiver()) && !replayReceiverIdentity.equals(completedReceiverIdentity)) {
            throw new IllegalArgumentException("Conflicting incarnations for the same receiver");
        }
        if (!replayInclusive && !replayFrom.equals(completedThrough)) {
            throw new IllegalArgumentException("Exclusive replay must start immediately after W");
        }
    }

    /** Only controlled startup may establish a boundary known to have no open cycles. */
    static As400Checkpoint afterVerifiedBoundary(String journalIdentity, String fingerprint, JournalPosition boundary, String receiverIdentity) {
        return new As400Checkpoint(journalIdentity, fingerprint, boundary, false, boundary, receiverIdentity, receiverIdentity);
    }

    Map<String, String> toOffset() {
        Map<String, String> result = new HashMap<>();
        result.put(VERSION, !schemaState.equals(As400SchemaState.EMPTY) ? "4" : snapshotState.isEmpty() ? "2" : "3");
        result.put("journal_identity", journalIdentity);
        result.put("capture_fingerprint", captureFingerprint);
        putPosition(result, FLOOR, replayFrom);
        result.put(FLOOR + ".inclusive", Boolean.toString(replayInclusive));
        putPosition(result, COMPLETED, completedThrough);
        result.put(FLOOR + ".receiver_identity", replayReceiverIdentity);
        result.put(COMPLETED + ".receiver_identity", completedReceiverIdentity);
        result.putAll(snapshotState);
        result.putAll(schemaState.fields());
        return Map.copyOf(result);
    }

    As400Checkpoint withSnapshotState(Map<String, String> state) {
        return new As400Checkpoint(journalIdentity, captureFingerprint, replayFrom, replayInclusive, completedThrough,
                replayReceiverIdentity, completedReceiverIdentity, state, schemaState);
    }

    As400Checkpoint withSchemaState(As400SchemaState state) {
        return new As400Checkpoint(journalIdentity, captureFingerprint, replayFrom, replayInclusive, completedThrough,
                replayReceiverIdentity, completedReceiverIdentity, snapshotState, state);
    }

    static As400Checkpoint fromOffset(Map<String, ?> offset, String expectedJournal, String expectedFingerprint) {
        if (offset == null || !(offset.get(VERSION) instanceof String version) || !Set.of("2", "3", "4").contains(version)) {
            throw new IllegalArgumentException("Missing or unsupported buffered offset version; legacy migration is not automatic");
        }
        var state = As400IncrementalSnapshotState.fields(offset);
        var schemas = As400SchemaState.from(offset);
        var keys = new java.util.HashSet<>(KEYS);
        keys.addAll(state.keySet());
        keys.addAll(schemas.fields().keySet());
        if (!offset.keySet().equals(keys) || ("4".equals(offset.get(VERSION)) ? schemas.equals(As400SchemaState.EMPTY)
                : !schemas.equals(As400SchemaState.EMPTY) || "3".equals(offset.get(VERSION)) == state.isEmpty())) {
            throw new IllegalArgumentException("Incomplete or mixed checkpoint representation");
        }
        String identity = string(offset, "journal_identity");
        String fingerprint = string(offset, "capture_fingerprint");
        if (!identity.equals(expectedJournal) || !fingerprint.equals(expectedFingerprint)) {
            throw new IllegalArgumentException("Journal incarnation or capture configuration changed");
        }
        String inclusive = string(offset, FLOOR + ".inclusive");
        if (!inclusive.equals("true") && !inclusive.equals("false")) {
            throw new IllegalArgumentException("Malformed replay inclusivity");
        }
        return new As400Checkpoint(identity, fingerprint, position(offset, FLOOR), inclusive.equals("true"), position(offset, COMPLETED),
                string(offset, FLOOR + ".receiver_identity"), string(offset, COMPLETED + ".receiver_identity"), state, schemas);
    }

    static void validatePosition(JournalPosition position) {
        Objects.requireNonNull(position, "Missing journal position");
        BigInteger sequence = position.offset();
        if (sequence == null || sequence.signum() <= 0 || sequence.compareTo(MAX_SEQUENCE) > 0) {
            throw new IllegalArgumentException("Invalid unsigned journal sequence");
        }
        Objects.requireNonNull(position.receiver(), "Missing journal receiver");
        identifier(position.receiver().name(), 10);
        identifier(position.receiver().library(), 10);
    }

    private static JournalPosition position(Map<String, ?> offset, String prefix) {
        String value = string(offset, prefix + ".sequence");
        if (!value.matches("[1-9][0-9]{0,19}")) {
            throw new IllegalArgumentException("Malformed journal sequence");
        }
        return new JournalPosition(new BigInteger(value), new JournalReceiver(string(offset, prefix + ".receiver"),
                string(offset, prefix + ".receiver_library")));
    }

    private static void putPosition(Map<String, String> target, String prefix, JournalPosition position) {
        target.put(prefix + ".sequence", position.offset().toString());
        target.put(prefix + ".receiver", position.receiver().name());
        target.put(prefix + ".receiver_library", position.receiver().library());
    }

    private static String string(Map<String, ?> offset, String key) {
        if (!(offset.get(key) instanceof String value)) {
            throw new IllegalArgumentException("Missing or malformed checkpoint field " + key);
        }
        return value;
    }

    private static void identifier(String value, int maximumLength) {
        if (value == null || value.isBlank() || value.length() > maximumLength || value.startsWith("*")) {
            throw new IllegalArgumentException("Missing or malformed checkpoint identity");
        }
    }
}
