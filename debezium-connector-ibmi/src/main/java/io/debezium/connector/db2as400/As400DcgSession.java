/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/** Configured operator-managed initialization and ordinary F/W recovery. No test fixture authority. */
final class As400DcgSession implements As400TaskInitialization, As400TaskRecovery {
    private static final Logger LOGGER = LoggerFactory.getLogger(As400DcgSession.class);

    record Admission(As400TaskRecovery recovery, As400TaskInitialization initialization) {
    }

    static Admission open(Configuration raw, SourceTaskContext context) throws Exception {
        var config = new As400DcgConfig(raw);
        config.requireRuntime(context);
        Map<String, ?> loaded = context.offsetStorageReader().offset(new As400Partition(config.prefix).getSourcePartition());
        if (loaded == null && !config.mode.equals("initialize")) {
            throw new IllegalArgumentException("Missing durable F/W; explicit first initialization is required");
        }
        if (loaded != null) {
            // Reject legacy, malformed and pending initialization offsets before native work.
            As400Checkpoint.fromOffset(loaded, (String) loaded.get("journal_identity"), (String) loaded.get("capture_fingerprint"));
        }
        var window = loaded == null ? new As400PlannedStartWindow(config.window) : null;
        if (window != null && (window.chunks.isEmpty() || !window.controlled.equals(config.controlled)
                || !window.captured.equals(config.captured) || !window.journal.equals(config.journal.library() + "/" + config.journal.name()))) {
            throw new IllegalArgumentException("Version-2 initialization plan must match the complete configured journal scope");
        }
        var session = new As400DcgSession(config, context, loaded, window);
        if (window != null) {
            var planned = new As400PlannedTaskInitialization(session, window);
            return new Admission(planned, planned);
        }
        return new Admission(session, null);
    }

    private final As400DcgConfig config;
    private final SourceTaskContext context;
    private final As400PlannedStartWindow window;
    private final As400DcgNativeIo io;
    private final As400DcgScope scope;
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final As400AsyncCleanup cancellation = new As400AsyncCleanup("ibmi-snapshot-cancel");
    private final As400AsyncCleanup cleanup = new As400AsyncCleanup("ibmi-session-close");
    private final Object snapshotIo = new Object();
    private volatile Connection locked;
    private volatile java.sql.Statement snapshotStatement;
    private volatile boolean released;
    private String identity;
    private String fingerprint;
    private As400OperatorCutReader reader;
    private As400BindingHistory.Recovered recovered;

    private As400DcgSession(As400DcgConfig config, SourceTaskContext context, Map<String, ?> loaded, As400PlannedStartWindow window) throws Exception {
        this.config = config;
        this.context = context;
        this.window = window;
        this.scope = new As400DcgScope(config);
        // Construction does not open native connections; terminal stop owns these handles from the start.
        this.io = new As400DcgNativeIo(config);
        if (loaded != null) {
            identity = (String) loaded.get("journal_identity");
            fingerprint = (String) loaded.get("capture_fingerprint");
        }
    }

    @Override
    public As400ContinuousRuntime.Limits runtimeLimits() {
        return config.runtimeLimits;
    }

    @Override
    public As400TaskRecovery recovery() {
        return this;
    }

    @Override
    public As400Partition partition() {
        return new As400Partition(config.prefix);
    }

    @Override
    public String journalIdentity() {
        return identity;
    }

    @Override
    public String fingerprint() {
        return fingerprint;
    }

    @Override
    public JournalReceiver journal() {
        return config.journal;
    }

    @Override
    public List<As400OperatorTableEvidence.Request> captured() {
        return config.requests();
    }

    @Override
    public void checkQualified(Configuration ignored) {
        checkRunning();
        config.requireRuntime(context);
    }

    private void checkRunning() {
        if (stopped.get() || Thread.currentThread().isInterrupted()) {
            throw new IllegalStateException("Native D/CG session stopped; completed drain is separate");
        }
        if (window != null && !released) {
            window.requireActive();
        }
    }

    private void identity() throws Exception {
        checkRunning();
        var target = new Target("/QSYS.LIB/" + journal().library() + ".LIB/" + journal().name() + ".JRN", Kind.JOURNAL);
        var objects = io.identities.collect(List.of(target));
        String current = As400OperatorCutReader.token(target, objects.objects().get(target));
        if (identity != null && !identity.equals(current)) {
            throw new IllegalArgumentException("Durable journal identity differs from current native object");
        }
        identity = current;
        checkRunning();
    }

    @Override
    public String enroll() throws Exception {
        checkRunning();
        if (window == null) {
            throw new IllegalStateException("Explicit operator initialization window required");
        }
        requireGeneration(true);
        identity();
        locked = io.connection(true);
        scope.validate(locked);
        scope.lock(locked);
        scope.validate(locked);
        LOGGER.info("D/CG operator window {} holds the configured native file scope", window.id);
        return checkHeld();
    }

    @Override
    public String checkHeld() throws Exception {
        synchronized (snapshotIo) {
            checkRunning();
            if (window == null || released || locked == null || locked.isClosed() || locked.getAutoCommit() || !locked.isValid(2)) {
                throw new IllegalStateException("Native initialization session or locks no longer held");
            }
            return window.id;
        }
    }

    @Override
    public List<String> controlledTables() throws Exception {
        synchronized (snapshotIo) {
            checkHeld();
            scope.validate(locked);
            return config.controlled;
        }
    }

    @Override
    public As400NativeInitialization initializer() {
        return new As400NativeInitialization(io.acquisition, io.census, io.journals, io.identities, io.tables, io.reader,
                io.cutLimits, config.requests(), config.policy, io.history);
    }

    @Override
    public boolean supportsChunks() {
        return true;
    }

    @Override
    public Optional<As400SnapshotChunks.Limits> chunkLimits() {
        return window == null ? Optional.empty() : window.chunks;
    }

    @Override
    public long remainingMillis() {
        checkRunning();
        return window == null || released ? Long.MAX_VALUE : window.remainingMillis();
    }

    @Override
    public List<Row> prepareBaseline(String epoch, List<As400BindingDefinition> definitions) {
        throw new UnsupportedOperationException("Configured native first start requires bounded chunks");
    }

    @Override
    public void scanBaseline(String epoch, List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
        requireEpoch(epoch);
        var actual = definitions.stream().map(d -> d.schema().id().schema() + "." + d.identity().physicalFile()).sorted().toList();
        if (!actual.equals(config.captured)) {
            throw new IllegalArgumentException("Acquired native snapshot scope differs from configured capture");
        }
        new As400NativeSnapshotScanner(locked, snapshotIo, () -> requireEpoch(epoch), this::checkRunning,
                statement -> snapshotStatement = statement, chunkLimits().orElseThrow()).scan(definitions, consumer);
    }

    private void requireEpoch(String epoch) throws Exception {
        if (!checkHeld().equals(epoch)) {
            throw new IllegalArgumentException("Native initialization epoch changed");
        }
    }

    @Override
    public void checkpointAccepted(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
        requireEpoch(epoch);
        if (!checkpoint.journalIdentity().equals(identity) || !history.initialization().orElseThrow().epoch().equals(epoch)) {
            throw new IllegalArgumentException("Native initialization checkpoint/history mismatch");
        }
        fingerprint = history.fingerprint();
        scope.verifyLocks(locked);
        var observation = io.journals.collect();
        var attached = observation.receivers().stream().filter(r -> r.name().equals(observation.attached())).findFirst().orElseThrow();
        if (!checkpoint.completedThrough().receiver().equals(attached.name()) || !checkpoint.completedThrough().offset().equals(attached.last())) {
            throw new IllegalStateException("Native initial boundary changed before handoff");
        }
    }

    @Override
    public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
        requireGeneration(false);
        identity();
        var history = io.history.recover(durable, identity, config.policy);
        if (history.initialization().isEmpty() || !history.fingerprint().equals(fingerprint)) {
            throw new IllegalArgumentException("Recovered initialized binding history differs");
        }
        reader = new As400OperatorCutReader(io.journals, io.identities, journal(), history, io.reader, io.cutLimits, () -> io.connection(false));
        recovered = history;
        LOGGER.info("D/CG recovered initialized binding history for {} captured files", history.definitions().size());
        return history;
    }

    @Override
    public void handoff(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
        requireEpoch(epoch);
        if (reader == null || history != recovered || !checkpoint.captureFingerprint().equals(history.fingerprint())) {
            throw new IllegalStateException("Native reader handoff lacks a validated continuation");
        }
        scope.verifyLocks(locked);
        locked.rollback();
        locked.close();
        locked = null;
        released = true;
        LOGGER.info("D/CG initialization {} handed off to continuous journal reading", epoch);
    }

    @Override
    public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
        checkRunning();
        if (reader == null) {
            throw new IllegalStateException("Native reader continuation missing");
        }
        Thread.sleep(500);
        checkRunning();
        var cut = reader.readPrefix(durable, scan.orElse(null));
        checkRunning();
        return cut;
    }

    private void requireGeneration(boolean empty) {
        var ordered = new java.util.HashSet<>(java.util.Set.of(config.historyTopic));
        if (new As400ConnectorConfig(config.raw).getHeartbeatInterval().isZero()) {
            ordered.add(config.controlTopic());
        }
        As400KafkaTopicGate.require(config.raw, config.topics(), ordered, empty, this::checkRunning);
    }

    @Override
    public void requestStop() {
        if (stopped.compareAndSet(false, true)) {
            io.requestStop();
            cancellation.start(() -> {
                var statement = snapshotStatement;
                if (statement != null) {
                    statement.cancel();
                }
            });
            LOGGER.info("D/CG native stop requested; drain is pending");
        }
    }

    @Override
    public void close() throws Exception {
        requestStop();
        cleanup.start(() -> {
            try {
                if (locked != null) {
                    try {
                        locked.rollback();
                    }
                    finally {
                        locked.close();
                        locked = null;
                    }
                }
            }
            finally {
                io.close();
            }
            cancellation.awaitInCleanupWorker();
            LOGGER.info("D/CG native handles drained");
        });
        cleanup.await(As400AsyncCleanup.STOP_TIMEOUT);
    }
}
