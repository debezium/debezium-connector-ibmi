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
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class As400StartupPublicationTest {
    @TempDir
    Path directory;

    @Test
    void emptyCommitsDoNotProbeOrAdvanceWhileNativePreparationIsHeld() throws Exception {
        try (var run = new As400InitializationTaskTest.Running(directory, null)) {
            run.input.hold = "prepare";
            run.start();
            assertThat(run.input.held.await(10, TimeUnit.SECONDS)).isTrue();
            int checks = run.input.heldChecks.get();
            for (int i = 0; i < 20; i++) {
                run.task.commit();
            }
            assertThat(run.input.heldChecks).hasValue(checks);
            assertThat(run.durable.get()).isNull();
            assertThat(run.input.reads).hasValue(0);
            assertThat(run.input.handoffs).hasValue(0);
            // Local qualification still applies even though nothing can advance.
            run.input.qualified.set(false);
            assertThrows(Exception.class, run.task::commit);
            assertThat(run.input.stopped).isTrue();
        }
    }

    @Test
    void unacknowledgedSnapshotCannotBecomeDurableThroughEmptyCommits() throws Exception {
        try (var run = new As400InitializationTaskTest.Running(directory, null)) {
            run.start();
            var records = run.through("BASELINE");
            for (int i = 0; i < 20; i++) {
                run.task.commit();
            }
            assertThat(run.durable.get()).isNull();
            assertThat(Files.exists(run.input.fixture.file)).isFalse();
            assertThat(run.input.handoffs).hasValue(0);
            run.producerAcks(records);
            // A commit that CAN advance must still verify native exclusion.
            run.input.heldSession.set(false);
            assertThrows(Exception.class, run.task::commit);
            assertThat(run.durable.get()).isNull();
            assertThat(Files.exists(run.input.fixture.file)).isFalse();
        }
    }

    @Test
    void lostSessionRejectsRemainingQueuedRowsBeforePublication() throws Exception {
        try (var run = new As400InitializationTaskTest.Running(directory, null)) {
            run.start();
            assertThat(run.next()).hasSize(1);
            run.input.heldSession.set(false);
            assertThrows(Exception.class, run::next);
            assertThat(run.durable.get()).isNull();
            assertThat(run.input.handoffs).hasValue(0);
            assertThat(run.input.reads).hasValue(0);
        }
    }

    @Test
    void retainedPollCannotBeReleasedAfterSessionLoss() throws Exception {
        try (var run = new As400InitializationTaskTest.Running(directory, null)) {
            run.start();
            run.next();
            run.input.heldSession.set(false);
            assertThrows(Exception.class, run.task::checkRecoveryPublication);
            assertThat(run.durable.get()).isNull();
            assertThat(run.input.stopped).isTrue();
        }
    }

    @Test
    void sessionLostAfterMaterializationCannotExposeConstructedSnapshot() throws Exception {
        try (var run = new As400InitializationTaskTest.Running(directory, null)) {
            run.input.loseSessionAfterMaterialization = 3;
            run.start();
            assertThrows(Exception.class, run::next);
            assertThat(run.input.checksAfterMaterialization.get()).isGreaterThanOrEqualTo(3);
            assertThat(run.durable.get()).isNull();
            assertThat(run.input.handoffs).hasValue(0);
            assertThat(Files.exists(run.input.fixture.file)).isFalse();
        }
    }
}
