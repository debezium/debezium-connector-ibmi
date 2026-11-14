/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * Task-owned composition of online native discovery, durable legacy handoff and
 * continuous F/W recovery. Public admission uses the same qualified source,
 * retention, output and acknowledgement contract as recovery. Native acquisition remains lazy.
 */
class As400NativeHandoffSession implements As400TaskHandoff {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(As400NativeHandoffSession.class);
    static final Schema CONTROL_SCHEMA = org.apache.kafka.connect.data.SchemaBuilder.string()
            .name("io.debezium.connector.db2as400.AutomaticHandoffControl").build();
    private final As400ConnectorConfig config;
    private final SourceTaskContext context;
    private final CdcSourceTaskContext<As400ConnectorConfig> taskContext;
    private final As400DcgNativeIo foreground;
    private final As400DcgNativeIo background;
    private final As400CaptureFingerprint.Policy policy;
    private final JournalReceiver journal;
    private final As400AsyncCleanup cleanup = new As400AsyncCleanup("ibmi-handoff-close");
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ExecutorService collector = Executors.newSingleThreadExecutor(r -> {
        var thread = new Thread(r, "ibmi-native-boundary");
        thread.setDaemon(true);
        return thread;
    });
    private final String controlTopic;
    private final As400ContinuousRuntime.Limits limits;
    private final Configuration nativeRaw;
    private final String historyTopic;
    private final boolean sourceOffsets;
    private final boolean snapshotRequired;
    private String identity;
    private String fingerprint;
    private As400OperatorCutReader continuation;
    private int steps;
    private int attempts;

    As400NativeHandoffSession(As400ConnectorConfig config, SourceTaskContext context,
                              CdcSourceTaskContext<As400ConnectorConfig> taskContext, Configuration nativeRaw,
                              JournalReceiver journal, As400CaptureFingerprint.Policy policy, String historyTopic, String controlTopic,
                              As400ContinuousRuntime.Limits limits)
            throws Exception {
        this(config, context, taskContext, nativeRaw, journal, policy, historyTopic, controlTopic, limits, false);
    }

    /** The acknowledgement contract follows the admitted runtime's actual transaction capability. */
    As400NativeHandoffSession(As400ConnectorConfig config, SourceTaskContext context,
                              CdcSourceTaskContext<As400ConnectorConfig> taskContext, Configuration nativeRaw,
                              JournalReceiver journal, As400CaptureFingerprint.Policy policy, String historyTopic, String controlTopic,
                              As400ContinuousRuntime.Limits limits, boolean sourceOffsets)
            throws Exception {
        this.config = config;
        this.context = context;
        this.taskContext = taskContext;
        this.journal = journal;
        this.policy = policy;
        this.controlTopic = java.util.Objects.requireNonNull(controlTopic);
        this.limits = java.util.Objects.requireNonNull(limits);
        this.nativeRaw = nativeRaw;
        this.historyTopic = historyTopic;
        this.sourceOffsets = sourceOffsets;
        requireDeliveryProfile();
        require(policy.transactionMetadata() == config.shouldProvideTransactionMetadata()
                && policy.skippedOperations().equals(As400DcgConfig.skippedOperations(config.getConfig()))
                && policy.tombstones() == config.isEmitTombstoneOnDelete()
                && policy.sourcePartition().equals(partition().getSourcePartition()), "Handoff policy differs from effective publication configuration");
        var loaded = As400FullSnapshotState.base(As400DeliveryOffset.strip(context.offsetStorageReader().offset(partition().getSourcePartition())));
        snapshotRequired = As400DcgConfig.transactionalTruncateEnabled(nativeRaw) && snapshotRequired(loaded);
        if (snapshotRequired) {
            // Read the history store before creating IBM i handles. Even a
            // partial proposal means this is not a new empty generation.
            As400DcgNativeIo.bindingHistory(nativeRaw, policy, historyTopic, config.getLogicalName()).requireEmptyForSnapshot();
        }
        else if (loaded == null) {
            throw new IllegalArgumentException("Missing offsets cannot select automatic migration");
        }
        else if (loaded.containsKey(As400Checkpoint.VERSION)) {
            identity = (String) loaded.get("journal_identity");
            fingerprint = (String) loaded.get("capture_fingerprint");
            As400Checkpoint.fromOffset(loaded, identity, fingerprint);
        }
        else {
            As400LegacyHandoff.startingPosition(As400LegacyHandoff.strings(loaded));
        }
        foreground = new As400DcgNativeIo(nativeRaw, config.getConfig().getString("database.dbname"), journal, false, policy, historyTopic,
                config.getLogicalName());
        background = new As400DcgNativeIo(nativeRaw, config.getConfig().getString("database.dbname"), journal, false, policy, historyTopic,
                config.getLogicalName());
    }

    static boolean snapshotRequired(Map<String, ?> loaded) {
        if (loaded == null) {
            return true;
        }
        if (loaded.containsKey(As400Checkpoint.VERSION) || !"false".equals(loaded.get("snapshot_completed"))) {
            return false;
        }
        var partial = As400LegacyHandoff.strings(loaded);
        require(!partial.containsKey(As400LegacyHandoff.VERSION) && !partial.containsKey(As400LegacyHandoff.FINGERPRINT)
                && !partial.containsKey(As400LegacyHandoff.CHECKPOINT) && !partial.containsKey("snapshot")
                && "true".equals(partial.get("offset.processed")), "Invalid incomplete ordinary snapshot offset");
        As400Checkpoint.validatePosition(new JournalPosition(new java.math.BigInteger(partial.get("offset.event_sequence")),
                new JournalReceiver(partial.get("offset.receiver"), partial.get("offset.receiver_library"))));
        return true;
    }

    @Override
    public boolean snapshotRequired() {
        return snapshotRequired;
    }

    @Override
    public Map<String, ?> snapshot(io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext context,
                                   As400OffsetContext previous,
                                   io.debezium.pipeline.source.spi.SnapshotProgressListener<As400Partition> listener,
                                   java.util.List<io.debezium.pipeline.notification.channels.NotificationChannel> notifications, Output output)
            throws Exception {
        check();
        require(snapshotRequired || config.getServiceRegistry().getService(io.debezium.snapshot.SnapshotterService.class)
                .getSnapshotter().shouldSnapshotData(previous != null, previous != null && !previous.isSnapshotComplete()),
                "This task was not admitted for an ordinary snapshot");
        foreground.history.requireEmptyForSnapshot();
        foreground.requireRetention();
        requireTopics(List.of());
        var connections = new io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory<>(
                () -> new As400JdbcConnection(config.getJdbcConfig(), ignored -> foreground.connection(false)));
        var saved = As400LegacySnapshotBootstrap.execute(config, taskContext, connections, this::snapshotPosition,
                context, previous, listener, notifications, output, offset -> marker(offset, "SNAPSHOT"));
        check();
        require(saved.equals(durable()), "Ordinary snapshot has no matching durable completion offset");
        foreground.history.requireEmptyForSnapshot();
        observe("snapshot-complete", () -> saved);
        return saved;
    }

    @Override
    public JournalPosition snapshotPosition() throws Exception {
        check();
        var directory = foreground.journals.collect();
        var attached = directory.receivers().stream().filter(r -> r.name().equals(directory.attached())).findFirst().orElseThrow();
        attached.requireReadableProfile();
        return new JournalPosition(attached.last(), attached.name());
    }

    @Override
    public As400Checkpoint fullSnapshot(As400Checkpoint original, As400BindingHistory.Recovered history,
                                        io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext context,
                                        io.debezium.pipeline.source.spi.SnapshotProgressListener<As400Partition> listener,
                                        java.util.List<io.debezium.pipeline.notification.channels.NotificationChannel> channels,
                                        Output output, boolean interrupted)
            throws Exception {
        return As400NativeResnapshot.execute(this, config, taskContext, original, history, context, listener, channels, output,
                offset -> marker(offset, "SNAPSHOT"), interrupted);
    }

    static As400NativeHandoffSession open(As400ConnectorConfig config, SourceTaskContext context,
                                          CdcSourceTaskContext<As400ConnectorConfig> taskContext)
            throws Exception {
        Configuration raw = config.getConfig();
        if (!As400DcgConfig.transactionalTruncateEnabled(raw)) {
            throw new IllegalArgumentException("Explicit transactional truncate admission required");
        }
        var profile = As400DcgConfig.configuredProfile(raw);
        profile.requireRuntime(context);
        // Keep the task's service registry: startRecovery registers the converter
        // and queue services on this exact configuration before coordinator work.
        return new As400NativeHandoffSession(config, context, taskContext, raw,
                profile.journal, profile.onlinePolicy(), profile.historyTopic, profile.controlTopic(), profile.runtimeLimits,
                context.transactionContext() == null);
    }

    @Override
    public As400ContinuousRuntime.Limits runtimeLimits() {
        return limits;
    }

    @Override
    public boolean allowLegacyHandoffRecovery() {
        return true;
    }

    @Override
    public boolean sourceOffsetAcknowledgements() {
        return sourceOffsets;
    }

    @Override
    public As400Partition partition() {
        return new As400Partition(config.getLogicalName());
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
    public void checkQualified(Configuration raw) {
        check();
        // Connector settings are fixed for this task instance and were validated
        // before constructing either native handle. Reconfiguration creates a
        // new task. Do not rebuild SMT rules and configuration maps per record;
        // keep the live stop and runtime-capability checks on every publication.
        require(context != null && (context.transactionContext() == null) == sourceOffsets,
                "Native handoff runtime acknowledgement capability changed");
    }

    private void requireDeliveryProfile() {
        require(!As400BindingHistory.fileStorage(nativeRaw) || sourceOffsets,
                "File binding history requires ordinary source-offset acknowledgement");
        if (!sourceOffsets) {
            requireProfile(config, context);
            return;
        }
        As400BatchDispatcher.requireConfiguration(config);
        As400RecoveryAcknowledgements.requireSourceOffsetProfile(config.getConfig().asMap());
        require(context != null && context.transactionContext() == null,
                "Qualified ordered source-offset handoff without transaction context required");
    }

    static void requireProfile(As400ConnectorConfig config, SourceTaskContext context) {
        As400BatchDispatcher.requireConfiguration(config);
        As400RecoveryAcknowledgements.requireConnectorProfile(config.getConfig().asMap());
        require(context != null && context.transactionContext() != null,
                "Qualified native handoff with Connect source transactions required");
    }

    @Override
    public boolean supportsTaskRestart() {
        return true;
    }

    private void check() {
        require(!stopped.get() && !Thread.currentThread().isInterrupted(), "Native handoff stopped; completed drain is separate");
        checkBudget();
    }

    /** A qualified caller may impose a shorter lifetime; never grants native or publication authority. */
    void checkBudget() {
    }

    /** Observability only. A listener may reject work, but cannot approve a boundary or checkpoint. */
    void observe(String name, java.util.function.Supplier<Object> value) throws Exception {
        if (java.util.Set.of("pending", "handoff-complete", "recovered", "drained").contains(name)) {
            LOGGER.info("Transactional truncate {}: {}", config.getLogicalName(), name);
        }
    }

    /** Test replacement may delay activation while the ordinary prefix keeps progressing. */
    boolean activationDeferred() {
        return false;
    }

    private As400NativeHandoffHistory historyReader() {
        return new As400NativeHandoffHistory(background.journals, background.identities, background.reader, this::check,
                background.handoffWork(runtimeLimits(), this::check));
    }

    /** Immutable discovery facts only; the collector closes every raw capture before returning. */
    private record Discovered(String journal, JournalPosition origin, String receiver,
            List<As400BindingDefinition> definitions, As400BindingHistory.Discovery discovery) {
        private Discovered {
            definitions = List.copyOf(definitions);
        }
    }

    private Future<Optional<Discovered>> collect() {
        check();
        int attempt = ++attempts;
        return collector.submit(() -> {
            if (attempt > 1) {
                long delay = Math.min(30_000L, 1000L << Math.min(attempt - 2, 5));
                LOGGER.info("Transactional truncate boundary attempt {} deferred for {} ms; legacy capture continues", attempt, delay);
                Thread.sleep(delay);
                check();
            }
            var boundary = attemptBoundary(attempt);
            if (boundary.isEmpty()) {
                return Optional.empty();
            }
            // Complete potentially large control-history discovery on this same
            // background thread. Legacy publication does not borrow these handles
            // or captures. Only immutable facts cross back to the source loop.
            try (var c = boundary.orElseThrow(); var history = historyReader()) {
                var nativeHistory = history.collect(c, c.window().through());
                var definitions = legacyCatalog(c.bindings().definitions(), background.server());
                As400NativeTableSchemas.requireLegacyTemporalCompatibility(definitions.stream()
                        .filter(d -> config.getTableFilters().dataCollectionFilter().isIncluded(d.identity().tableId())
                                || config.isSignalDataCollection(d.identity().tableId()))
                        .toList());
                observe("native-boundary", () -> Map.of("origin", c.origin().toString(), "floor", c.historyFloor().toString(),
                        "acquired", c.completion().acquired().toString(), "released", c.completion().released().toString(),
                        "job", c.completion().job(), "thread", c.completion().thread().toString(),
                        "raw", raw(c.audit().entries()), "controls", raw(nativeHistory.entries()),
                        "native_before", nativeSample(c.window().before()), "native_after", nativeSample(c.window().after())));
                check();
                return Optional.of(new Discovered(c.window().journalToken(), c.origin(), c.window().receiverTokens().get(c.origin().receiver()),
                        definitions, new As400BindingHistory.Discovery(c.historyFloor(), nativeHistory.order().incarnation(c.historyFloor()))));
            }
            catch (IllegalArgumentException | As400OperatorJournalEvidence.IncompleteObservationException rejected) {
                check();
                if (hasSuppressedCleanup(rejected)) {
                    throw rejected;
                }
                LOGGER.warn("Unpublished boundary history changed; acquiring a new boundary while legacy capture continues: {}", rejected.getMessage());
                return Optional.empty();
            }
        });
    }

    Optional<As400NativeBoundaryCollector.Candidate> attemptBoundary(int attempt) throws Exception {
        check();
        try {
            background.requireRetention(); // Preflight before any marker or allocation.
            Optional<As400NativeBoundaryCollector.Candidate> candidate;
            try {
                candidate = new As400NativeBoundaryCollector(journal, () -> background.connection(false),
                        background.acquisition, background.reader, this::check, () -> background.capture(this::check)).collect();
            }
            catch (Exception failure) {
                if (As400NativeBoundaryPulse.hasUnverifiedCleanup(failure)) {
                    background.failedCleanup(failure);
                }
                throw failure;
            }
            if (candidate.isEmpty()) {
                observe("busy-" + attempt, () -> Map.of("refused", true));
            }
            else {
                var observed = candidate.orElseThrow();
                try {
                    requireImages(background, observed.bindings().definitions());
                }
                catch (Exception failure) {
                    try {
                        observed.close();
                    }
                    catch (Exception cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                    throw failure;
                }
            }
            return candidate;
        }
        catch (As400NativeBoundaryCollector.MetadataUnavailable unavailable) {
            check();
            // The wrapper only covers read-only work outside pulse.attempt().
            // A later holder-close failure is suppressed on the wrapper itself
            // and must not be converted to an ordinary admission retry.
            if (unavailable.getSuppressed().length != 0 || As400NativeBoundaryPulse.hasUnverifiedCleanup(unavailable)) {
                throw unavailable;
            }
            LOGGER.info("Transactional truncate metadata is busy; retrying admission with backoff while legacy capture continues");
            return Optional.empty();
        }
        catch (As400NativeBoundaryCollector.AuditRejected rejected) {
            observe("rejected-boundary-" + attempt, () -> Map.of("from", rejected.window.from().toString(), "through", rejected.window.through().toString(),
                    "reason", rejected.getCause().toString(), "raw", raw(rejected.page.entries()),
                    "native_before", nativeSample(rejected.window.before()), "native_after", nativeSample(rejected.window.after())));
            check();
            // No candidate has escaped and the pulse has completed release.
            // Keep legacy publication independent from an unqualified audit.
            // A suppressed native close/drain failure is never retryable.
            if (hasSuppressedCleanup(rejected)) {
                throw rejected;
            }
            LOGGER.warn("Transactional truncate boundary rejected; legacy capture continues: {}",
                    rejected.getCause() == null ? rejected.getMessage() : rejected.getCause().getMessage());
            return Optional.empty();
        }
        catch (IllegalArgumentException | As400NativeEvidenceException | As400OperatorJournalEvidence.IncompleteObservationException rejected) {
            check();
            if (hasSuppressedCleanup(rejected)) {
                throw rejected;
            }
            LOGGER.warn("Transactional truncate admission deferred; legacy capture continues: {}", rejected.getMessage());
            return Optional.empty();
        }
    }

    private static boolean hasSuppressedCleanup(Throwable failure) {
        var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        for (Throwable current = failure; current != null && seen.add(current); current = current.getCause()) {
            if (current.getSuppressed().length != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public As400Checkpoint handoff(Map<String, ?> loaded, Output output) throws Exception {
        return handoff(loaded, output, null);
    }

    @Override
    public As400Checkpoint handoff(Map<String, ?> loaded, Output output, Signals signals) throws Exception {
        requireTopics(List.of());
        var pending = foreground.history.pendingOnlineCandidate(loaded, policy);
        if (pending.isPresent()) {
            requireTopics(pending.orElseThrow().definitions());
        }
        if (loaded.containsKey(As400LegacyHandoff.VERSION)) {
            var facts = pending.orElseThrow();
            try (var history = historyReader()) {
                var prepared = history.resume(facts, As400LegacyHandoff.position(As400LegacyHandoff.strings(loaded)), config, policy);
                return activate(facts, prepared, loaded, output);
            }
        }
        Future<Optional<Discovered>> candidate = pending.isEmpty() ? collect() : null;
        // The prefix keeps unrelated tables moving while a transactional clear
        // waits for its verified outcome. Publication offsets remain pinned.
        var legacyConfig = config;
        var scan = new As400OffsetContext.Loader(legacyConfig).load(loaded);
        @SuppressWarnings("unchecked")
        TopicNamingStrategy<TableId> topics = config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        // The real legacy JDBC decoder acquires its own metadata. It must
        // never decode older rows using the later boundary's acquired schemas.
        try (var jdbc = new As400JdbcConnection(legacyConfig.getJdbcConfig(), ignored -> foreground.connection(false));
                var legacyWork = foreground.handoffWork(limits, this::check);
                var legacyStore = legacyWork.transactionStore()) {
            var schemas = new As400DatabaseSchema(legacyConfig, jdbc, topics, config.schemaNameAdjuster(),
                    config.getServiceRegistry().tryGetService(CustomConverterRegistry.class), taskContext);
            var legacyNotifications = new java.util.concurrent.atomic.AtomicReference<io.debezium.pipeline.notification.NotificationService<As400Partition, As400OffsetContext>>();
            var staging = new As400LegacySnapshotBootstrap.Delivery(output, offset -> marker(offset, "LEGACY-PREFIX"));
            var delivery = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(4096).maxQueueSize(4096).pollInterval(Duration.ZERO)
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext("automatic-legacy"))
                    .queueProvider(staging).build();
            try (var dispatcher = new As400EventDispatcher(legacyConfig, topics, schemas, delivery,
                    table -> legacyConfig.getTableFilters().dataCollectionFilter().isIncluded(table) || legacyConfig.isSignalDataCollection(table),
                    DataChangeEvent::new, new As400EventMetadataProvider(),
                    config.schemaNameAdjuster(), signals == null ? null : signals.processor(), new DebeziumHeaderProducer(taskContext)) {
                @Override
                public boolean dispatchDataChangeEvent(As400Partition partition, TableId table, io.debezium.pipeline.spi.ChangeRecordEmitter<As400Partition> emitter)
                        throws InterruptedException {
                    boolean handled = super.dispatchDataChangeEvent(partition, table, emitter);
                    if (signals != null) {
                        signals.processor().executePending();
                    }
                    return handled;
                }
            }) {
                if (signals != null) {
                    var tables = new io.debezium.relational.Tables();
                    jdbc.readSchema(tables, jdbc.getRealDatabaseName(), config.getSchema(), config.getTableFilters().eligibleDataCollectionFilter(), null, false);
                    tables.tableIds().forEach(id -> schemas.forwardSchema(tables.forTable(id)));
                    var notifications = new io.debezium.pipeline.notification.NotificationService<As400Partition, As400OffsetContext>(
                            signals.notifications(), config, io.debezium.schema.SchemaFactory.get(), dispatcher::enqueueNotification);
                    legacyNotifications.set(notifications);
                    var incremental = new As400TruncateAwareIncrementalSnapshot(
                            config, jdbc, dispatcher, schemas, Clock.system(), signals.progress(), signals.changes(), notifications);
                    dispatcher.setIncrementalSnapshotChangeEventSource(Optional.of(incremental));
                    signals.processor().setContext(io.debezium.pipeline.spi.Offsets.of(partition(), scan));
                    signals.register().accept(dispatcher);
                    incremental.init(partition(), scan);
                }
                var source = new As400StreamingChangeEventSource(legacyConfig, null, jdbc, dispatcher,
                        new ErrorHandler(As400RpcConnector.class, legacyConfig, delivery, null), Clock.system(), schemas);
                var clearEvidence = new As400NativeLegacyClear(foreground, journal, limits, this::check);
                var clears = new As400LegacyClearBuffer(partition(), scan, source, staging, clearEvidence::prepare,
                        limits.transactions().maxTransactions(), legacyStore);
                var prefix = new As400LegacyNativePrefix(foreground.journals, foreground.identities, foreground.reader, source,
                        partition(), scan, this::check, new As400OperatorCutReader.Limits(32, 500, 16 * 1024 * 1024), clears);
                int incompleteObservations = 0;
                while (true) {
                    check();
                    if (signals != null) {
                        signals.processor().executePending();
                    }
                    Optional<Map<String, ?>> progress;
                    try {
                        progress = prefix.publishNext();
                        if (progress.isEmpty() && pending.isPresent() && prefix.scan().offset().compareTo(pending.orElseThrow().origin().offset()) < 0) {
                            progress = prefix.advanceThroughMarker(pending.orElseThrow().origin());
                        }
                        incompleteObservations = 0;
                    }
                    catch (As400OperatorJournalEvidence.IncompleteObservationException incomplete) {
                        if (hasSuppressedCleanup(incomplete)) {
                            throw incomplete;
                        }
                        check();
                        incompleteObservations = As400OperatorJournalEvidence.retryIncomplete(incompleteObservations, incomplete);
                        Thread.sleep(1000);
                        continue;
                    }
                    catch (As400OperatorCutReader.UnstableCutException race) {
                        LOGGER.info("Receiver changed during legacy prefix qualification; retrying from the last observed scan");
                        Thread.sleep(250);
                        continue;
                    }
                    if (progress.isEmpty() && staging.pending() && prefix.completePosition().isPresent()) {
                        progress = prefix.completePosition();
                    }
                    if (progress.isPresent()) {
                        var completedPrefix = progress.orElseThrow();
                        staging.flush(completedPrefix);
                        observe("progress-" + (++steps), () -> Map.of("offset", completedPrefix));
                    }
                    if (pending.isEmpty() && candidate != null && candidate.isDone()) {
                        var collected = candidate.get();
                        if (collected.isPresent()) {
                            var c = collected.orElseThrow();
                            requireTopics(c.definitions());
                            As400BindingHistory.Pending selected;
                            try {
                                selected = foreground.history.recordOnlineCandidate(c.journal(), c.origin(), c.receiver(), c.definitions(),
                                        policy, c.discovery(), durable());
                            }
                            catch (As400BindingHistory.CandidateHistoryFullException exhausted) {
                                check();
                                if (hasSuppressedCleanup(exhausted)) {
                                    throw exhausted;
                                }
                                LOGGER.warn("Transactional truncate activation deferred: {}. No further boundary attempts in this task", exhausted.getMessage());
                                candidate = null;
                                continue;
                            }
                            pending = Optional.of(selected);
                            observe("pending", () -> Map.of("origin", selected.origin().offset().toString(),
                                    "floor", selected.discovery().orElseThrow().floor().offset().toString()));
                        }
                        else {
                            observe("retry-" + steps, () -> Map.of("candidate_accepted", false));
                            candidate = collect();
                        }
                    }
                    // Deterministic test fault: retain history while ordinary
                    // row publication progresses, then kill this task before activation.
                    var complete = prefix.completePosition();
                    if (pending.isPresent() && complete.isPresent() && !prefix.hasDeferredWork() && !activationDeferred()
                            && !scan.getIncrementalSnapshotContext().snapshotRunning()
                            && prefix.scan().offset().compareTo(pending.orElseThrow().origin().offset()) >= 0) {
                        try (var history = historyReader()) {
                            As400NativeHandoffHistory.Prepared prepared = null;
                            try {
                                // Check before offering HANDOFF: a current clear
                                // may temporarily end journaling and must leave
                                // this candidate replaceable while legacy CDC runs.
                                requireImages(foreground, pending.orElseThrow().definitions());
                                prepared = history.resume(pending.orElseThrow(), prefix.scan(), config, policy);
                            }
                            catch (As400ProspectiveHandoffReplay.PriorEpochPending | As400HandoffReplay.SelectedPrefixPending deferred) {
                                observe("deferred-" + steps, () -> Map.of("reason", deferred.getMessage()));
                            }
                            catch (IllegalArgumentException | As400NativeEvidenceException | As400OperatorJournalEvidence.IncompleteObservationException rejected) {
                                check();
                                require(!durable().containsKey(As400LegacyHandoff.VERSION), "Cannot replace an acknowledged handoff");
                                if (hasSuppressedCleanup(rejected)) {
                                    throw rejected;
                                }
                                LOGGER.warn("Unpublished truncate boundary is no longer usable; acquiring a new boundary while legacy capture continues: {}",
                                        rejected.getMessage());
                                pending = Optional.empty();
                                candidate = collect();
                            }
                            if (prepared != null) {
                                var ready = prepared;
                                observe("prepared", () -> Map.of("raw", raw(ready.replay().projection().entries()), "controls", raw(ready.controls().entries()),
                                        "candidate", ready.replay().candidate().checkpoint().toOffset(), "publication_acknowledged", false));
                                var barrier = marker(As400LegacyHandoff.bind(complete.orElseThrow(), prepared.replay().candidate().checkpoint()), "HANDOFF");
                                // Once this record is offered, even an uncertain
                                // acknowledgement must fail, never supersede it.
                                output.barrier(barrier);
                                output.acknowledge(barrier);
                                observe("handoff-acknowledged", () -> Map.of("legacy", barrier.sourceOffset()));
                                return activate(pending.orElseThrow(), prepared, barrier.sourceOffset(), output);
                            }
                        }
                    }
                    if (progress.isEmpty()) {
                        Thread.sleep(250);
                    }
                }
            }
            finally {
                try {
                    if (legacyNotifications.get() != null) {
                        legacyNotifications.get().stop();
                    }
                }
                finally {
                    schemas.close();
                }
            }
        }
    }

    private Map<String, ?> durable() {
        check();
        return As400DeliveryOffset.strip(context.offsetStorageReader().offset(partition().getSourcePartition()));
    }

    private As400Checkpoint activate(As400BindingHistory.Pending pending, As400NativeHandoffHistory.Prepared prepared,
                                     Map<String, ?> barrier, Output output)
            throws Exception {
        check();
        var checkpoint = prepared.replay().candidate().checkpoint();
        var activation = new As400LegacyHandoff(checkpoint, As400LegacyHandoff.strings(barrier),
                prepared.controls().order().incarnation(As400LegacyHandoff.position(As400LegacyHandoff.strings(barrier))));
        activation.verifyOrder(prepared.controls().order());
        require(activation.barrier().equals(As400LegacyHandoff.strings(durable())), "Legacy barrier is not durably acknowledged");
        var selected = foreground.history.recoverOnlineCandidate(checkpoint.toOffset(), pending.journal(), policy);
        selected.verify(checkpoint, prepared.controls().order());
        identity = pending.journal();
        fingerprint = pending.fingerprint();
        observe("migration-ready", () -> Map.of("checkpoint", checkpoint.toOffset(), "legacy", barrier,
                "publication_acknowledged", false, "history_activation_written", false));
        var migration = marker(checkpoint.toOffset(), "MIGRATION");
        output.barrier(migration);
        output.acknowledge(migration);
        observe("handoff-complete", () -> Map.of("checkpoint", checkpoint.toOffset()));
        return checkpoint;
    }

    private SourceRecord marker(Map<String, ?> offset, String kind) {
        return new SourceRecord(partition().getSourcePartition(), offset, controlTopic, 0, Schema.STRING_SCHEMA, kind, CONTROL_SCHEMA, kind);
    }

    @Override
    public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
        check();
        var history = foreground.history.recoverOnlineCandidate(durable, identity, policy);
        history.requireStartupOrigin(durable, true);
        // Replay validates the retained journaling lifecycle. A transactional
        // clear can temporarily end journaling, so today's OBJECT_STATISTICS
        // cannot qualify the historical stream on an ordinary restart.
        requireTopics(history.definitions());
        continuation = new As400OperatorCutReader(foreground.journals, foreground.identities, journal, history, foreground.reader, foreground.cutLimits,
                () -> foreground.connection(false));
        observe("recovered", () -> Map.of("checkpoint", durable));
        return history;
    }

    @Override
    public As400JdbcConnection incrementalSnapshotConnection(As400ConnectorConfig config) {
        check();
        return new As400JdbcConnection(config.getJdbcConfig(), ignored -> foreground.connection(false));
    }

    @Override
    public As400SchemaChange schemaChange(As400BindingDefinition before, RawJournalPage.Entry opening,
                                          As400SourceNormalizer.TableResolver verifiedBindings)
            throws Exception {
        check();
        var change = new As400NativeSchemaRefresh(foreground, journal, identity, foreground.server(), this::check)
                .collect(before, opening, verifiedBindings);
        requireTopics(change.after() == null ? List.of() : List.of(change.after()));
        return change;
    }

    private void requireImages(As400DcgNativeIo io, List<As400BindingDefinition> definitions) throws Exception {
        var selected = definitions.stream().filter(definition -> config.getTableFilters().dataCollectionFilter().isIncluded(definition.schema().id())
                || config.isSignalDataCollection(definition.schema().id())).toList();
        int incompleteObservations = 0;
        while (!selected.isEmpty()) {
            check();
            try (var connection = io.connection(false)) {
                As400DcgScope.requireImages(connection, journal, selected);
                break;
            }
            catch (As400OperatorJournalEvidence.IncompleteObservationException incomplete) {
                if (hasSuppressedCleanup(incomplete)) {
                    throw incomplete;
                }
                if (++incompleteObservations > As400OperatorJournalEvidence.MAX_INCOMPLETE_OBSERVATIONS) {
                    throw new IllegalStateException("Captured-file catalog remained incomplete; correct the SQL warning before restarting", incomplete);
                }
                LOGGER.warn("Captured-file catalog is incomplete; recollecting before publication: {}", incomplete.getCause().getMessage());
                Thread.sleep(1000);
            }
        }
        check();
    }

    private void requireTopics(List<As400BindingDefinition> definitions) {
        @SuppressWarnings("unchecked")
        TopicNamingStrategy<TableId> topics = config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        var names = new java.util.ArrayList<String>();
        for (var definition : definitions) {
            if (config.getTableFilters().dataCollectionFilter().isIncluded(definition.schema().id())) {
                names.add(topics.dataChangeTopic(definition.schema().id()));
            }
        }
        if (!config.getHeartbeatInterval().isZero()) {
            names.add(topics.heartbeatTopic());
        }
        if (config.shouldProvideTransactionMetadata()) {
            names.add(topics.transactionTopic());
        }
        names.add(controlTopic);
        if (As400BindingHistory.fileStorage(nativeRaw)) {
            // Engine/Server destinations are logical names, not Kafka topics.
            // Successful ordered delivery and durable offsets acknowledge them;
            // do not open a Kafka client to infer a sink this task does not own.
            var routing = As400TopicRouting.from(nativeRaw.asMap());
            if (routing.predictable()) {
                require(names.stream().map(routing::topic).distinct().count() == names.size(), "Data and control destinations must be distinct");
            }
            check();
            return;
        }
        names.add(historyTopic);
        As400KafkaTopicGate.require(nativeRaw, names, java.util.Set.of(controlTopic, historyTopic), false, this::check);
    }

    @Override
    public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
        check();
        var cut = continuation.readPrefix(durable, scan.orElse(null));
        if (cut.entries().isEmpty()) {
            Thread.sleep(250);
        }
        check();
        return cut;
    }

    @Override
    public void requestStop() {
        stopped.set(true);
        foreground.requestStop();
        background.requestStop();
        collector.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        requestStop();
        cleanup.start(() -> {
            while (!collector.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                // The bounded caller can return while a late admitted call drains.
            }
            try {
                foreground.close();
            }
            finally {
                background.close();
            }
            observe("drained", () -> Map.of("completed", true));
        });
        cleanup.await(As400AsyncCleanup.STOP_TIMEOUT);
    }

    private static Object raw(List<RawJournalPage.Entry> entries) {
        return entries.stream().map(e -> Map.of("sequence", e.position().offset().toString(), "receiver", e.position().receiver().name(),
                "library", e.position().receiver().library(), "raw", java.util.Base64.getEncoder().encodeToString(e.data()))).toList();
    }

    private static Object nativeSample(As400NativeJournalWindow.Sample sample) {
        return Map.of("journal", sample.directory().journal().toString(), "attached", sample.directory().attached().toString(),
                "identities", sample.identities().objects().entrySet().stream().map(e -> Map.of("path", e.getKey().path(),
                        "kind", e.getKey().kind().name(), "fileSystem", e.getValue().fileSystem().toString(),
                        "file", e.getValue().file().toString(), "generation", e.getValue().generation().toString())).toList(),
                "receivers", sample.directory().receivers().stream().map(r -> Map.of("receiver", r.name().toString(),
                        "first", r.first().toString(), "last", r.last().toString(), "previous", r.previous().map(Object::toString).orElse(""),
                        "next", r.next().map(Object::toString).orElse(""), "status", r.status(),
                        "profile", List.of(r.minimizeFiles(), r.job(), r.thread(), r.luw(), r.xid()))).toList());
    }

    /**
     * NativeIo verifies CURRENT_SERVER before returning each acquisition connection.
     * The physical identities/codecs stay intact; add that server to the output
     * identity just as the actual legacy JDBC schema does. Persist the result so
     * replacement tasks use the same topics and schema names without catalog reads.
     */
    static List<As400BindingDefinition> legacyCatalog(List<As400BindingDefinition> definitions, String server) {
        require(server != null && !server.isBlank(), "Verified native server required");
        return definitions.stream().map(definition -> {
            var identity = definition.identity();
            require(identity.tableId().catalog() == null, "Acquired binding already has an unqualified catalog association");
            var id = new TableId(server, identity.tableId().schema(), identity.tableId().table());
            return new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(id, identity.physicalFile(), identity.member(),
                    identity.fileJournalId(), identity.memberJournalId(), identity.recordFormat()), definition.schema().edit().tableId(id).create(), definition.codecs(),
                    definition.members());
        }).toList();
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
