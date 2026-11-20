/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/** Controlled source session only. No native connection, public setting or implicit first-start permission. */
class As400InitializationTaskFixture extends As400RecoveryTaskFixture implements As400TaskInitialization {
    final As400NativeInitializationTest.Setup setup;
    final Path enrollment;
    final AtomicBoolean heldSession = new AtomicBoolean(true);
    final AtomicInteger handoffs = new AtomicInteger();
    final AtomicInteger heldChecks = new AtomicInteger();
    final AtomicInteger checksAfterMaterialization = new AtomicInteger();
    volatile int loseSessionAfterMaterialization = Integer.MAX_VALUE;
    volatile boolean materialized;
    volatile String epoch = "controlled-epoch";
    volatile String acceptedFingerprint;
    volatile String rowFault = "";

    As400InitializationTaskFixture(Path directory) throws Exception {
        super(directory, null);
        setup = new As400NativeInitializationTest.Setup(directory);
        enrollment = directory.resolve("enrolled");
    }

    @Override
    public As400TaskRecovery recovery() {
        return this;
    }

    @Override
    public As400NativeInitialization initializer() {
        return setup.initializer();
    }

    @Override
    public JournalReceiver journal() {
        return Fixture.JOURNAL;
    }

    @Override
    public List<As400OperatorTableEvidence.Request> captured() {
        return setup.requests;
    }

    @Override
    public List<String> controlledTables() {
        checkHeld();
        return setup.requests.stream().map(r -> r.library() + "." + r.file()).sorted().toList();
    }

    @Override
    public String enroll() throws Exception {
        checkHeld();
        if (Files.exists(fixture.file)) {
            throw new IllegalStateException("Existing history is not a fresh initialization");
        }
        consumeEnrollment(enrollment, epoch);
        gate("enroll");
        return checkHeld();
    }

    static void consumeEnrollment(Path path, String epoch) throws Exception {
        Files.writeString(path, epoch, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        try (var channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
        try (var channel = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    @Override
    public String checkHeld() {
        heldChecks.incrementAndGet();
        if (!heldSession.get() || stopped.get()) {
            throw new IllegalStateException("Controlled initialization session ended");
        }
        return epoch;
    }

    @Override
    public void checkQualified(io.debezium.config.Configuration configuration) {
        super.checkQualified(configuration);
        if (materialized && checksAfterMaterialization.incrementAndGet() >= loseSessionAfterMaterialization) {
            heldSession.set(false);
        }
    }

    @Override
    public List<Row> prepareBaseline(String epoch, List<As400BindingDefinition> definitions) throws Exception {
        gate("prepare");
        setup.session.prepareBaseline(epoch, definitions); // Existing fixture asserts preparation precedes native boundary selection.
        var result = new ArrayList<Row>();
        for (var definition : definitions) {
            for (int id = 1; id <= 3; id++) {
                Object value = rowFault.equals("type") ? "wrong" : id;
                Object[] values = rowFault.equals("projection") ? new Object[]{ value, 2 } : new Object[]{ value };
                if (rowFault.equals("null-key")) {
                    values[0] = null;
                }
                result.add(new Row(definition.schema().id(), As400RowData.copyOf(values, 65536), Instant.EPOCH));
            }
        }
        materialized = true;
        if (rowFault.equals("size")) {
            return java.util.Collections.nCopies(1001, result.get(0));
        }
        if (rowFault.equals("empty")) {
            return List.of();
        }
        return result;
    }

    @Override
    public void checkpointAccepted(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
        gate("checkpoint");
        acceptedFingerprint = history.fingerprint();
    }

    @Override
    public String fingerprint() {
        return acceptedFingerprint;
    }

    @Override
    public void handoff(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
        gate("handoff");
        if (!epoch.equals(checkHeld()) || !checkpoint.captureFingerprint().equals(history.fingerprint())) {
            throw new IllegalStateException("Initialization handoff differs");
        }
        handoffs.incrementAndGet();
        heldSession.set(false);
        gate("released");
    }

    @Override
    public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
        return super.recover(durable);
    }
}
