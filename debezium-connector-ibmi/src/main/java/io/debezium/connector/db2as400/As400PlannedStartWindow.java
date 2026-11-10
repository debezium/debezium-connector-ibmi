/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.StringReader;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * Operator-owned first-start window, not native exclusion proof or a publication
 * ownership store. A separately qualified source session must hold the declared
 * scope. The configured initialization path requires this plan. Storage must survive
 * task replacement; interrupted generations require explicit operator recovery.
 */
final class As400PlannedStartWindow {
    private static final Set<String> KEYS = Set.of("version", "window.id", "operator", "journal", "captured", "controlled", "assurance", "retention",
            "opened.ms", "expires.ms", "snapshot.rows", "snapshot.bytes");
    private final Path directory;
    private final byte[] original;
    private final String digest;
    private final LongSupplier millis;
    private final LongSupplier nanos;
    private final long startedNanos;
    private final long remainingNanos;
    private final long opened;
    private final long expires;
    final String id;
    final String journal;
    final List<String> captured;
    final List<String> controlled;
    final As400TaskInitialization.SnapshotLimits limits;
    final Optional<As400SnapshotChunks.Limits> chunks;
    private boolean claimed;
    private volatile boolean completed;

    As400PlannedStartWindow(Path directory) throws Exception {
        this(directory, System::currentTimeMillis, System::nanoTime);
    }

    As400PlannedStartWindow(Path directory, LongSupplier millis, LongSupplier nanos) throws Exception {
        this.directory = directory;
        this.millis = millis;
        this.nanos = nanos;
        if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Persistent operator window directory required");
        }
        original = readPlan();
        digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(original));
        Properties plan = new Properties();
        plan.load(new StringReader(new String(original, java.nio.charset.StandardCharsets.UTF_8)));
        boolean chunked = "2".equals(plan.getProperty("version"));
        var keys = new HashSet<>(KEYS);
        if (chunked) {
            keys.addAll(Set.of("chunk.rows", "chunk.bytes"));
        }
        if (!plan.stringPropertyNames().equals(keys) || !(chunked || "1".equals(plan.getProperty("version")))
                || !"all-journal-writers-ddl-maintenance-paused".equals(plan.getProperty("assurance"))
                || !"operator-managed".equals(plan.getProperty("retention"))
                || !plan.getProperty("operator").matches("[A-Za-z0-9@._-]{1,100}")) {
            throw new IllegalArgumentException("Complete explicit operator window and retention contract required");
        }
        id = plan.getProperty("window.id");
        journal = plan.getProperty("journal");
        if (!id.matches("[A-Za-z0-9-]{1,100}") || !journal.matches("[A-Z][A-Z0-9_]{0,9}/[A-Z][A-Z0-9_]{0,9}")) {
            throw new IllegalArgumentException("Invalid window/journal identity");
        }
        captured = scope(plan.getProperty("captured"));
        controlled = scope(plan.getProperty("controlled"));
        if (!controlled.containsAll(captured)) {
            throw new IllegalArgumentException("Controlled scope must include every captured table");
        }
        if (chunked) {
            chunks = Optional.of(new As400SnapshotChunks.Limits(Integer.parseInt(plan.getProperty("chunk.rows")), Long.parseLong(plan.getProperty("chunk.bytes")),
                    Long.parseLong(plan.getProperty("snapshot.rows")), Long.parseLong(plan.getProperty("snapshot.bytes"))));
            // The old list interface must not silently use chunk budgets.
            limits = null;
        }
        else {
            chunks = Optional.empty();
            limits = new As400TaskInitialization.SnapshotLimits(Integer.parseInt(plan.getProperty("snapshot.rows")), Long.parseLong(plan.getProperty("snapshot.bytes")));
        }
        opened = Long.parseLong(plan.getProperty("opened.ms"));
        expires = Long.parseLong(plan.getProperty("expires.ms"));
        long now = millis.getAsLong();
        if (opened < 0 || expires <= opened || expires - opened > 1_800_000 || opened > now || expires <= now) {
            throw new IllegalArgumentException("Operator window must be open and at most 30 minutes");
        }
        startedNanos = nanos.getAsLong();
        remainingNanos = (expires - now) * 1_000_000;
        requireActive();
    }

    private static List<String> scope(String value) {
        var entries = Arrays.asList(value.split(",", -1));
        if (entries.isEmpty() || entries.size() > 100 || Set.copyOf(entries).size() != entries.size()
                || entries.stream().anyMatch(v -> !v.matches("[A-Z][A-Z0-9_]{0,9}\\.[A-Z][A-Z0-9_]{0,9}"))) {
            throw new IllegalArgumentException("Exact nonempty unique table scope required");
        }
        return entries.stream().sorted().toList();
    }

    private byte[] readPlan() throws Exception {
        Path path = directory.resolve("plan.properties");
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.size(path) > 16384) {
            throw new IllegalStateException("Bounded immutable operator plan required");
        }
        return Files.readAllBytes(path);
    }

    void requireActive() {
        if (completed) {
            return;
        }
        try {
            if (millis.getAsLong() >= expires || nanos.getAsLong() - startedNanos >= remainingNanos) {
                throw new IllegalStateException("Operator initialization window expired");
            }
            if (Files.exists(directory.resolve("revoked"), LinkOption.NOFOLLOW_LINKS) || !Arrays.equals(original, readPlan())) {
                throw new IllegalStateException("Operator initialization window revoked or changed");
            }
        }
        catch (Exception error) {
            throw new IllegalStateException("Operator window is not active", error);
        }
    }

    long remainingMillis() {
        requireActive();
        if (completed) {
            return Long.MAX_VALUE;
        }
        return Math.max(1, Math.min(expires - millis.getAsLong(), (remainingNanos - (nanos.getAsLong() - startedNanos)) / 1_000_000));
    }

    synchronized void claim(String actualJournal, List<String> actualCaptured) throws Exception {
        requireActive();
        if (claimed || !journal.equals(actualJournal) || !captured.equals(actualCaptured.stream().sorted().toList())) {
            throw new IllegalStateException("Operator window scope differs or was already consumed");
        }
        // CREATE_NEW survives failure before native preparation. Never remove it
        // on stop, timeout, loss of offsets, or interrupted acknowledgement.
        Path enrollment = directory.resolve("enrolled");
        Files.writeString(enrollment, id + "\n" + digest + "\n", StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        force(enrollment);
        claimed = true;
        phase("ENROLLED");
    }

    synchronized void phase(String phase) throws Exception {
        requireActive();
        if (!claimed) {
            throw new IllegalStateException("Operator window has not been enrolled");
        }
        status(phase);
    }

    synchronized void complete() throws Exception {
        requireActive();
        if (!claimed) {
            throw new IllegalStateException("Operator window has not been enrolled");
        }
        status("HANDED_OFF");
        completed = true;
    }

    synchronized void quarantine() throws Exception {
        if (claimed && !completed) {
            status("QUARANTINED");
        }
    }

    private void status(String phase) throws Exception {
        Path temporary = directory.resolve("status.tmp");
        long now = millis.getAsLong();
        String content = "window.id=" + id + "\nplan.sha256=" + digest + "\nphase=" + phase + "\nopened.ms=" + opened
                + "\nobserved.ms=" + now + "\nelapsed.ms=" + Math.max(0, now - opened) + "\n";
        Files.writeString(temporary, content, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        force(temporary);
        Files.move(temporary, directory.resolve("status.properties"), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        force(directory);
        Files.writeString(directory.resolve("phases.log"), content + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        force(directory.resolve("phases.log"));
    }

    private void force(Path path) throws Exception {
        try (var channel = FileChannel.open(path, Files.isDirectory(path) ? StandardOpenOption.READ : StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        if (!path.equals(directory)) {
            force(directory);
        }
    }
}
