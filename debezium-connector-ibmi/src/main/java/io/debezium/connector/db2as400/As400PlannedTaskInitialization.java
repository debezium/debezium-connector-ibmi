/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/** Planned-window policy around a separately qualified native session. */
final class As400PlannedTaskInitialization implements As400TaskInitialization, As400TaskRecovery {
    private final As400TaskInitialization source;
    private final As400TaskRecovery recovery;
    private final As400PlannedStartWindow window;

    As400PlannedTaskInitialization(As400TaskInitialization source, As400PlannedStartWindow window) {
        this.source = source;
        this.recovery = source.recovery();
        this.window = window;
        if (window.chunks.isPresent() && (!source.supportsChunks() || !source.chunkLimits().equals(window.chunks))) {
            throw new IllegalArgumentException("Provider does not support the planned chunk budgets");
        }
    }

    @Override
    public As400TaskRecovery recovery() {
        return this;
    }

    @Override
    public As400NativeInitialization initializer() {
        return source.initializer();
    }

    @Override
    public JournalReceiver journal() {
        return source.journal();
    }

    @Override
    public List<As400OperatorTableEvidence.Request> captured() {
        return source.captured();
    }

    @Override
    public SnapshotLimits snapshotLimits() {
        if (window.chunks.isPresent()) {
            throw new IllegalStateException("Chunk budgets cannot be used as materialization limits");
        }
        return window.limits;
    }

    @Override
    public Optional<As400SnapshotChunks.Limits> chunkLimits() {
        return window.chunks;
    }

    @Override
    public boolean supportsChunks() {
        return source.supportsChunks();
    }

    @Override
    public long remainingMillis() {
        return Math.min(window.remainingMillis(), source.remainingMillis());
    }

    @Override
    public void scanBaseline(String epoch, List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
        requireScope();
        if (!source.chunkLimits().equals(window.chunks)) {
            throw new IllegalStateException("Native chunk bounds differ from operator plan");
        }
        window.phase("SNAPSHOT_READING");
        source.scanBaseline(epoch, definitions, consumer);
        window.phase("SNAPSHOT_SCANNED");
    }

    @Override
    public String enroll() throws Exception {
        var journal = journal();
        window.claim(journal.library() + "/" + journal.name(), captured().stream().map(r -> r.library() + "." + r.file()).toList());
        String epoch = source.enroll();
        requireScope();
        window.phase("SOURCE_HELD");
        return epoch;
    }

    @Override
    public String checkHeld() throws Exception {
        window.requireActive();
        return source.checkHeld();
    }

    @Override
    public List<Row> prepareBaseline(String epoch, List<As400BindingDefinition> definitions) throws Exception {
        requireScope();
        window.phase("SNAPSHOT_READING");
        var rows = source.prepareBaseline(epoch, definitions);
        window.phase("SNAPSHOT_MATERIALIZED");
        return rows;
    }

    @Override
    public void checkpointAccepted(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
        requireScope();
        window.phase("CHECKPOINT_ACKNOWLEDGED");
        source.checkpointAccepted(epoch, checkpoint, history);
        window.requireActive();
    }

    @Override
    public void handoff(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
        requireScope();
        window.phase("HANDOFF");
        source.handoff(epoch, checkpoint, history);
        window.complete();
    }

    private void requireScope() throws Exception {
        window.requireActive();
        if (!source.controlledTables().stream().sorted().toList().equals(window.controlled)) {
            throw new IllegalStateException("Native controlled journal scope differs from operator plan");
        }
    }

    @Override
    public As400Partition partition() {
        return recovery.partition();
    }

    @Override
    public String journalIdentity() {
        return recovery.journalIdentity();
    }

    @Override
    public String fingerprint() {
        return recovery.fingerprint();
    }

    @Override
    public As400ContinuousRuntime.Limits runtimeLimits() {
        return recovery.runtimeLimits();
    }

    @Override
    public void checkQualified(Configuration configuration) {
        window.requireActive();
        recovery.checkQualified(configuration);
    }

    @Override
    public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
        window.requireActive();
        return recovery.recover(durable);
    }

    @Override
    public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
        return recovery.read(durable, scan);
    }

    @Override
    public void requestStop() {
        recovery.requestStop();
    }

    @Override
    public void close() throws Exception {
        try {
            recovery.close();
        }
        finally {
            window.quarantine();
        }
    }
}
