/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class As400PlannedStartWindowTest {
    @TempDir
    Path directory;
    final AtomicLong millis = new AtomicLong(1000);
    final AtomicLong nanos = new AtomicLong();

    Path plan() throws Exception {
        Path window = directory.resolve("window");
        Files.createDirectory(window);
        Files.writeString(window.resolve("plan.properties"), "version=1\nwindow.id=window-1\noperator=test-operator\njournal=DCOC1107/QSQJRN\n"
                + "captured=DCOC1107.A,DCOC1107.B\ncontrolled=DCOC1107.A,DCOC1107.B\nassurance=all-journal-writers-ddl-maintenance-paused\n"
                + "retention=operator-managed\nopened.ms=1000\nexpires.ms=11000\nsnapshot.rows=2000\nsnapshot.bytes=2000000\n");
        return window;
    }

    As400PlannedStartWindow window(Path path) throws Exception {
        return new As400PlannedStartWindow(path, millis::get, nanos::get);
    }

    void change(Path path, String key, String value) throws Exception {
        var properties = new Properties();
        try (var in = Files.newInputStream(path.resolve("plan.properties"))) {
            properties.load(in);
        }
        properties.setProperty(key, value);
        try (var out = Files.newOutputStream(path.resolve("plan.properties"))) {
            properties.store(out, "");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "version", "window.id", "operator", "journal", "captured", "controlled", "assurance", "retention", "opened.ms", "expires.ms",
            "snapshot.rows", "snapshot.bytes", "unknown" })
    void invalidOrIncompletePlanCannotEnroll(String key) throws Exception {
        Path path = plan();
        change(path, key, "invalid!");
        assertThrows(Exception.class, () -> window(path));
        assertThat(Files.exists(path.resolve("enrolled"))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "expiry", "clock-backward", "revoke", "change", "missing" })
    void activeWindowCannotSurviveExpiryOrLossOfAuthority(String fault) throws Exception {
        Path path = plan();
        var window = window(path);
        window.claim(window.journal, window.captured);
        switch (fault) {
            case "expiry" -> millis.set(11000);
            case "clock-backward" -> {
                millis.set(0);
                nanos.set(10_000_000_000L);
            }
            case "revoke" -> Files.createFile(path.resolve("revoked"));
            case "change" -> change(path, "operator", "replacement");
            default -> Files.delete(path.resolve("plan.properties"));
        }
        assertThrows(IllegalStateException.class, window::requireActive);
        window.quarantine();
        assertThat(Files.readString(path.resolve("status.properties"))).contains("phase=QUARANTINED");
        assertThat(Files.exists(path.resolve("enrolled"))).isTrue();
    }

    @Test
    void exactScopeAndSingleUseEnrollmentSurviveFreshInstance() throws Exception {
        Path path = plan();
        var first = window(path);
        assertThrows(Exception.class, () -> first.claim("OTHER/QSQJRN", first.captured));
        assertThrows(Exception.class, () -> first.claim(first.journal, List.of("DCOC1107.A")));
        first.claim(first.journal, first.captured);
        var replacement = window(path);
        assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> replacement.claim(replacement.journal, replacement.captured));
        assertThat(Files.readString(path.resolve("status.properties"))).contains("phase=ENROLLED");
    }

    @Test
    void chunkPlanSeparatesAttemptBudgetsAndRequiresAnExplicitProvider() throws Exception {
        Path path = plan();
        change(path, "version", "2");
        change(path, "snapshot.rows", "30000");
        change(path, "snapshot.bytes", "20000000");
        change(path, "chunk.rows", "256");
        change(path, "chunk.bytes", "1000000");
        var window = window(path);
        assertThat(window.limits).isNull();
        assertThat(window.chunks.orElseThrow().totalRows()).isEqualTo(30000);
        assertThrows(Exception.class, () -> new As400PlannedTaskInitialization(new As400InitializationTaskFixture(directory), window));
        var mismatched = new As400ChunkedInitializationFixture(directory, 30000);
        assertThrows(Exception.class, () -> new As400PlannedTaskInitialization(mismatched, window));
        assertThat(mismatched.heldChecks.get()).isZero();
        assertThat(Files.exists(mismatched.enrollment)).isFalse();
        mismatched.limits = window.chunks.orElseThrow();
        assertThat(new As400PlannedTaskInitialization(mismatched, window).chunkLimits()).isEqualTo(window.chunks);
        assertThat(Files.exists(path.resolve("enrolled"))).isFalse();
        window.claim(window.journal, window.captured);
        change(path, "chunk.rows", "128");
        assertThrows(Exception.class, window::requireActive);
    }

    @Test
    void runtimeAfterSuccessfulHandoffDoesNotRequireAnotherPause() throws Exception {
        Path path = plan();
        var window = window(path);
        window.claim(window.journal, window.captured);
        millis.set(5000);
        window.complete();
        Files.delete(path.resolve("plan.properties"));
        millis.set(20000);
        window.requireActive();
        window.quarantine();
        assertThat(Files.readString(path.resolve("status.properties"))).contains("phase=HANDED_OFF", "elapsed.ms=4000");
    }

    @ParameterizedTest
    @ValueSource(strings = { "expiry", "revocation" })
    void actualTaskRejectsQueuedSnapshotAfterWindowEndsWithoutAcknowledgement(String fault) throws Exception {
        Path path = plan();
        var window = window(path);
        try (var run = new As400InitializationTaskTest.Running(directory, null, input -> new As400PlannedTaskInitialization(input, window))) {
            run.start();
            run.next(); // No producer/commit acknowledgement: coordinator may be waiting or enqueuing.
            if (fault.equals("expiry")) {
                millis.set(11000);
            }
            else {
                Files.createFile(path.resolve("revoked"));
            }
            assertThrows(Exception.class, run::next);
            assertThat(run.durable.get()).isNull();
            assertThat(run.input.reads).hasValue(0);
            assertThat(run.input.handoffs).hasValue(0);
        }
        assertThat(Files.readString(path.resolve("status.properties"))).contains("phase=QUARANTINED");
    }

    @Test
    void actualTaskCanCommitLargerBoundedSnapshotThenRecoverWithoutWindow() throws Exception {
        Path path = plan();
        var window = window(path);
        Map<String, ?> saved;
        try (var run = new As400InitializationTaskTest.Running(directory, null, input -> new As400PlannedTaskInitialization(input, window))) {
            run.input.rowFault = "size";
            run.start();
            var baseline = run.through("BASELINE");
            assertThat(baseline).hasSize(1002);
            run.ack(baseline);
            run.ack(run.through("INITIAL"));
            run.ack(run.throughCheckpoint("42"));
            saved = run.durable.get();
        }
        Files.delete(path.resolve("plan.properties"));
        var baseline = saved.entrySet().stream().collect(java.util.stream.Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()));
        try (var replacement = new As400RecoveryTaskTest.Running(directory, baseline, saved)) {
            replacement.input.tail = 64;
            replacement.start();
            var records = replacement.throughCheckpoint("59");
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("t:DCOC1107.B", "c:DCOC1107.B:100", "c:DCOC1107.A:9");
            replacement.ack(records);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "rows", "bytes", "scope" })
    void unsupportedSnapshotOrIncompleteControlFailsBeforePublication(String fault) throws Exception {
        Path path = plan();
        if (fault.equals("scope")) {
            change(path, "controlled", "DCOC1107.A,DCOC1107.B,DCOC1107.U");
        }
        else {
            change(path, "snapshot." + fault, "1");
        }
        var window = window(path);
        try (var run = new As400InitializationTaskTest.Running(directory, null, input -> new As400PlannedTaskInitialization(input, window))) {
            run.start();
            assertThrows(Exception.class, run::next);
            assertThat(run.durable.get()).isNull();
            assertThat(run.input.reads).hasValue(0);
        }
        assertThat(Files.readString(path.resolve("status.properties"))).contains("phase=QUARANTINED");
    }
}
