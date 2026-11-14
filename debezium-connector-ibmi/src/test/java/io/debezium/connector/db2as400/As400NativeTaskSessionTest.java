/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Identity;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

class As400NativeTaskSessionTest {
    @TempDir
    Path directory;
    final AtomicInteger constructions = new AtomicInteger();

    Configuration config() throws Exception {
        Files.writeString(directory.resolve("worker.properties"), "bootstrap.servers=broker:9092\nexactly.once.source.support=enabled\n");
        Files.writeString(directory.resolve("native.owned-fixture"), "DCNC7201");
        Files.writeString(directory.resolve("native.qualified"), "native-DCNC7201");
        Files.writeString(directory.resolve("native.initialize-request"), "");
        return Configuration.create().with("name", "native").with("probe.library", "DCNC7201").with("probe.topic", "events-native")
                .with("database.hostname", "stdin-native-credentials.invalid").with("database.schema", "DCNC7201").with("topic.prefix", "test")
                .with("provide.transaction.metadata", true).with("skipped.operations", "none").with("tasks.max", 1)
                .with("exactly.once.support", "requested").with("transaction.boundary", "poll").with("errors.tolerance", "none")
                .with("event.converting.failure.handling.mode", "fail").build();
    }

    As400NativeTaskSession session(Configuration configuration, Map<String, ?> loaded, As400NativeTaskSession.Factory factory) throws Exception {
        return new As400NativeTaskSession(configuration, directory, directory.resolve("worker.properties"), "native-w1-" + UUID.randomUUID(), loaded,
                evidence -> {
                    constructions.incrementAndGet();
                    return factory.create(evidence);
                }) {
            @Override
            void requireEmptyGeneration() {
                // Only Kafka's external read is controlled; all admission ordering
                // and the durable single-use enrollment remain real.
            }
        };
    }

    @ParameterizedTest
    @ValueSource(strings = { "database.hostname", "database.schema", "topic.prefix", "provide.transaction.metadata", "skipped.operations",
            "tasks.max", "exactly.once.support", "transaction.boundary", "errors.tolerance", "event.converting.failure.handling.mode" })
    void unsupportedProfileRejectsBeforeNativeConstruction(String key) throws Exception {
        var values = new HashMap<>(config().asMap());
        values.put(key, "unqualified");
        var config = Configuration.from(values);
        try (var session = session(config, null, p -> {
            throw new AssertionError("Must not open source");
        })) {
            assertThrows(Exception.class, session::enroll);
            assertThat(constructions).hasValue(0);
            assertThat(Files.exists(directory.resolve("native.enrolled"))).isFalse();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "worker", "owner", "token", "enrollment" })
    void missingExternalAdmissionDoesNotOpenNativeSource(String missing) throws Exception {
        var config = config();
        Path path = directory.resolve(switch (missing) {
            case "worker" -> "worker.properties";
            case "owner" -> "native.owned-fixture";
            case "token" -> "native.qualified";
            default -> "native.initialize-request";
        });
        Files.delete(path);
        try (var session = session(config, null, p -> {
            throw new AssertionError("Must not open source");
        })) {
            assertThrows(Exception.class, session::enroll);
            assertThat(constructions).hasValue(0);
            assertThat(Files.exists(directory.resolve("native.enrolled"))).isFalse();
        }
    }

    @Test
    void terminalStopBeforeAdmissionPreventsEverySourceEntry() throws Exception {
        var config = config();
        try (var session = session(config, null, p -> {
            throw new AssertionError("Must not open source");
        })) {
            session.requestStop();
            assertThrows(Exception.class, session::enroll);
            assertThrows(Exception.class, () -> session.recover(Map.of()));
            assertThrows(Exception.class, () -> session.read(Map.of(), Optional.empty()));
            assertThat(constructions).hasValue(0);
        }
    }

    @Test
    void interruptedEnrollmentCannotReinitializeEvenBeforeAnySnapshotOutput() throws Exception {
        var config = config();
        try (var first = session(config, null, p -> {
            throw new java.io.IOException("Native construction failed");
        })) {
            assertThrows(java.io.IOException.class, first::enroll);
            assertThat(Files.readString(directory.resolve("native.enrolled"))).isEqualTo("native-DCNC7201");
        }
        try (var second = session(config, null, p -> {
            throw new AssertionError("Must not reopen source");
        })) {
            assertThrows(java.nio.file.FileAlreadyExistsException.class, second::enroll);
            assertThat(constructions).hasValue(1);
        }
    }

    @Test
    void nativeHandlesConstructedAfterStopAreCancelledWithoutConnecting() throws Exception {
        var config = config();
        var nativeSource = nativeSource();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var session = session(config, null, p -> {
            entered.countDown();
            assertThat(release.await(10, TimeUnit.SECONDS)).isTrue();
            return nativeSource;
        })) {
            var attempt = CompletableFuture.runAsync(() -> assertThrows(IllegalStateException.class, session::enroll));
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
                session.requestStop();
            }
            finally {
                release.countDown();
            }
            attempt.get(10, TimeUnit.SECONDS);
            verify(nativeSource, never()).connection(false);
            verify(nativeSource, never()).connection(true);
            assertThat(nativeSource.lifecycle().isStopped()).isTrue();
            verify(nativeSource.guardedReader()).cancelJob();
        }
        verify(nativeSource).close();
    }

    @Test
    void cleanupClosesLockedConnectionBeforeWaitingForNativeDrainWithoutClaimingHandoff() throws Exception {
        var config = config();
        var nativeSource = nativeSource();
        var locked = mock(Connection.class);
        when(locked.isValid(2)).thenReturn(true);
        when(nativeSource.connection(true)).thenReturn(locked);
        String generation;
        try (var session = session(config, null, p -> nativeSource)) {
            generation = session.generation;
            assertThat(session.enroll()).isEqualTo("native-DCNC7201");
            session.requestStop();
            assertThrows(Exception.class, session::checkHeld);
        }
        var order = inOrder(locked, nativeSource);
        order.verify(locked).rollback();
        order.verify(locked).close();
        order.verify(nativeSource).close();
        assertThat(Files.exists(directory.resolve(generation + ".session-released.json"))).isFalse();
        assertThat(Files.exists(directory.resolve(generation + ".drained.json"))).isTrue();
        assertThat(Files.exists(directory.resolve("native.enrolled"))).isTrue();
    }

    @Test
    void failedSnapshotCancellationCannotClaimCompletedDrain() throws Exception {
        var config = config();
        var session = session(config, null, p -> {
            throw new AssertionError("No source should open");
        });
        var statement = mock(java.sql.Statement.class);
        org.mockito.Mockito.doThrow(new java.sql.SQLException("cancel failed")).when(statement).cancel();
        session.trackSnapshotStatement(statement);
        session.requestStop();
        verify(statement).cancel();
        assertThrows(IllegalStateException.class, session::close);
        assertThat(Files.exists(directory.resolve(session.generation + ".drained.json"))).isFalse();
        assertThat(Files.exists(directory.resolve(session.generation + ".session-released.json"))).isFalse();
    }

    @Test
    void currentNativeIdentityMustMatchDurableJournalBeforeHistoryOrOutput() throws Exception {
        var config = config();
        var fixture = new As400RecoveryTestSupport.Fixture(directory);
        var saved = As400RecoveryTaskFixture.initialize(fixture);
        var nativeSource = nativeSource();
        try (var session = session(config, saved, p -> nativeSource)) {
            var error = assertThrows(IllegalArgumentException.class, () -> session.recover(saved));
            assertThat(error).hasMessageContaining("native journal identity differs");
            verify(nativeSource, never()).connection(true);
        }
    }

    private As400ContinuousConnectSource nativeSource() throws Exception {
        var result = mock(As400ContinuousConnectSource.class);
        when(result.lifecycle()).thenReturn(new JournalReadLifecycle());
        when(result.guardedReader()).thenReturn(mock(RetrieveJournal.class));
        var identities = mock(As400JdbcNativeIdentityEvidence.class);
        var target = new Target("/QSYS.LIB/DCNC7201.LIB/QSQJRN.JRN", Kind.JOURNAL);
        when(identities.collect(any())).thenReturn(new Candidates(Map.of(target, new Identity(BigInteger.ONE, BigInteger.TWO, BigInteger.TEN))));
        when(result.identities()).thenReturn(identities);
        when(result.connection(false)).thenReturn(mock(Connection.class));
        return result;
    }

}
