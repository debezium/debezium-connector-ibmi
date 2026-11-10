/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.ArrayDeque;
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
import io.debezium.connector.base.QueueProvider;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * Task-owned composition of online native discovery, durable legacy handoff and
 * continuous F/W recovery. Public admission uses the same qualified source,
 * retention, output and Kafka contract as recovery. Native acquisition remains lazy.
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
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final ExecutorService collector = Executors.newSingleThreadExecutor(r -> new Thread(r, "ibmi-native-boundary"));
    private final String controlTopic;
    private final As400ContinuousRuntime.Limits limits;
    private final Configuration nativeRaw;
    private final String historyTopic;
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
        this.config = config;
        this.context = context;
        this.taskContext = taskContext;
        this.journal = journal;
        this.policy = policy;
        this.controlTopic = java.util.Objects.requireNonNull(controlTopic);
        this.limits = java.util.Objects.requireNonNull(limits);
        this.nativeRaw = nativeRaw;
        this.historyTopic = historyTopic;
        requireProfile(config, context);
        require(!policy.transactionMetadata() && policy.skippedOperations().isEmpty()
                && policy.tombstones() == config.isEmitTombstoneOnDelete()
                && policy.sourcePartition().equals(partition().getSourcePartition()), "Handoff policy differs from effective publication configuration");
        var loaded = context.offsetStorageReader().offset(partition().getSourcePartition());
        require(loaded != null, "Missing offsets cannot select automatic migration");
        if (loaded.containsKey(As400Checkpoint.VERSION)) {
            identity = (String) loaded.get("journal_identity");
            fingerprint = (String) loaded.get("capture_fingerprint");
            As400Checkpoint.fromOffset(loaded, identity, fingerprint);
        }
        else {
            As400LegacyHandoff.position(As400LegacyHandoff.strings(loaded));
        }
        foreground = new As400DcgNativeIo(nativeRaw, config.getConfig().getString("database.dbname"), journal, false, policy, historyTopic,
                config.getLogicalName());
        background = new As400DcgNativeIo(nativeRaw, config.getConfig().getString("database.dbname"), journal, false, policy, historyTopic,
                config.getLogicalName());
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
                profile.journal, profile.onlinePolicy(), profile.historyTopic, profile.controlTopic(), profile.runtimeLimits);
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
        requireProfile(config, context);
    }

    static void requireProfile(As400ConnectorConfig config, SourceTaskContext context) {
        As400BatchDispatcher.requireConfiguration(config);
        As400RecoveryAcknowledgements.requireConnectorProfile(config.getConfig().asMap());
        require(!config.shouldProvideTransactionMetadata() && "none".equals(config.getConfig().getString("skipped.operations"))
                && context != null && context.transactionContext() != null,
                "Qualified no-metadata native handoff with Connect source transactions required");
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
        return new As400NativeHandoffHistory(background.journals, background.identities, background.reader, this::check);
    }

    private Future<Optional<As400NativeBoundaryCollector.Candidate>> collect() {
        check();
        int attempt = ++attempts;
        return collector.submit(() -> attemptBoundary(attempt));
    }

    Optional<As400NativeBoundaryCollector.Candidate> attemptBoundary(int attempt) throws Exception {
        check();
        try {
            var candidate = new As400NativeBoundaryCollector(journal, () -> background.connection(false),
                    background.acquisition, background.reader, this::check).collect();
            if (candidate.isEmpty()) {
                observe("busy-" + attempt, () -> Map.of("refused", true));
            }
            return candidate;
        }
        catch (As400NativeBoundaryCollector.AuditRejected rejected) {
            observe("rejected-boundary-" + attempt, () -> Map.of("from", rejected.window.from().toString(), "through", rejected.window.through().toString(),
                    "reason", rejected.getCause().toString(), "raw", raw(rejected.page.entries()),
                    "native_before", nativeSample(rejected.window.before()), "native_after", nativeSample(rejected.window.after())));
            check();
            // No candidate has escaped and the pulse has completed release.
            // Keep legacy publication independent from an unqualified audit.
            // A suppressed native close/drain failure is never retryable.
            if (rejected.getSuppressed().length != 0) {
                throw rejected;
            }
            return Optional.empty();
        }
    }

    @Override
    public As400Checkpoint handoff(Map<String, ?> loaded, Output output) throws Exception {
        requireTopics(List.of());
        var pending = foreground.history.pendingOnlineCandidate(loaded, policy);
        if (pending.isPresent()) {
            requireTopics(pending.orElseThrow().definitions());
        }
        if (loaded.containsKey(As400LegacyHandoff.VERSION)) {
            var facts = pending.orElseThrow();
            var prepared = historyReader().resume(facts, As400LegacyHandoff.position(As400LegacyHandoff.strings(loaded)), config, policy);
            return activate(facts, prepared, loaded, output);
        }
        Future<Optional<As400NativeBoundaryCollector.Candidate>> candidate = pending.isEmpty() ? collect() : null;
        var legacyConfig = new As400ConnectorConfig(config.getConfig().edit().with("skipped.operations", "t").build());
        var scan = new As400OffsetContext.Loader(legacyConfig).load(loaded);
        @SuppressWarnings("unchecked")
        TopicNamingStrategy<TableId> topics = config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        // The real legacy JDBC decoder acquires its own metadata. It must
        // never decode older rows using the later boundary's acquired schemas.
        try (var jdbc = new As400JdbcConnection(legacyConfig.getJdbcConfig(), ignored -> foreground.connection(false))) {
            var schemas = new As400DatabaseSchema(legacyConfig, jdbc, topics, config.schemaNameAdjuster(),
                    config.getServiceRegistry().tryGetService(CustomConverterRegistry.class), taskContext);
            var staging = new Staging();
            var delivery = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(4096).maxQueueSize(4096).pollInterval(Duration.ZERO)
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext("automatic-legacy"))
                    .queueProvider(staging).build();
            try (var dispatcher = new EventDispatcher<As400Partition, TableId>(legacyConfig, topics, schemas, delivery,
                    legacyConfig.getTableFilters().dataCollectionFilter(), DataChangeEvent::new, new As400EventMetadataProvider(),
                    config.schemaNameAdjuster(), null, new DebeziumHeaderProducer(taskContext))) {
                var source = new As400StreamingChangeEventSource(legacyConfig, null, jdbc, dispatcher,
                        new ErrorHandler(As400RpcConnector.class, legacyConfig, delivery, null), Clock.system(), schemas);
                var prefix = new As400LegacyNativePrefix(foreground.journals, foreground.identities, foreground.reader, source,
                        partition(), scan, this::check, new As400OperatorCutReader.Limits(32, 500, 16 * 1024 * 1024));
                while (true) {
                    check();
                    var progress = prefix.publishNext();
                    if (progress.isEmpty() && pending.isPresent() && prefix.scan().offset().compareTo(pending.orElseThrow().origin().offset()) < 0) {
                        progress = prefix.advanceThroughMarker(pending.orElseThrow().origin());
                    }
                    while (!staging.records.isEmpty()) {
                        output.emit(staging.records.removeFirst().getRecord());
                    }
                    if (progress.isPresent()) {
                        var barrier = marker(progress.orElseThrow(), "LEGACY");
                        output.barrier(barrier);
                        output.acknowledge(barrier);
                        observe("progress-" + (++steps), () -> Map.of("offset", barrier.sourceOffset()));
                    }
                    if (pending.isEmpty() && candidate.isDone()) {
                        var collected = candidate.get();
                        if (collected.isPresent()) {
                            var c = collected.orElseThrow();
                            var nativeHistory = historyReader().collect(c, c.window().through());
                            observe("native-boundary", () -> Map.of("origin", c.origin().toString(), "floor", c.historyFloor().toString(),
                                    "acquired", c.completion().acquired().toString(), "released", c.completion().released().toString(),
                                    "job", c.completion().job(), "thread", c.completion().thread().toString(),
                                    "raw", raw(c.audit().entries()), "controls", raw(nativeHistory.entries()),
                                    "native_before", nativeSample(c.window().before()), "native_after", nativeSample(c.window().after())));
                            var definitions = legacyCatalog(c.bindings().definitions(), config.getConfig().getString("database.dbname"));
                            requireTopics(definitions);
                            var selected = foreground.history.recordOnlineCandidate(c.window().journalToken(), c.origin(),
                                    c.window().receiverTokens().get(c.origin().receiver()), definitions,
                                    policy,
                                    new As400BindingHistory.Discovery(c.historyFloor(), nativeHistory.order().incarnation(c.historyFloor())), durable());
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
                    if (pending.isPresent() && complete.isPresent() && !activationDeferred()
                            && prefix.scan().offset().compareTo(pending.orElseThrow().origin().offset()) >= 0) {
                        try {
                            var prepared = historyReader().resume(pending.orElseThrow(), prefix.scan(), config, policy);
                            observe("prepared", () -> Map.of("raw", raw(prepared.replay().projection().entries()), "controls", raw(prepared.controls().entries()),
                                    "candidate", prepared.replay().candidate().checkpoint().toOffset(), "publication_acknowledged", false));
                            var barrier = marker(As400LegacyHandoff.bind(complete.orElseThrow(), prepared.replay().candidate().checkpoint()), "HANDOFF");
                            output.barrier(barrier);
                            output.acknowledge(barrier);
                            observe("handoff-acknowledged", () -> Map.of("legacy", barrier.sourceOffset()));
                            return activate(pending.orElseThrow(), prepared, barrier.sourceOffset(), output);
                        }
                        catch (As400ProspectiveHandoffReplay.PriorEpochPending | As400HandoffReplay.SelectedPrefixPending deferred) {
                            observe("deferred-" + steps, () -> Map.of("reason", deferred.getMessage()));
                        }
                    }
                    Thread.sleep(250);
                }
            }
            finally {
                schemas.close();
            }
        }
    }

    private Map<String, ?> durable() {
        check();
        return context.offsetStorageReader().offset(partition().getSourcePartition());
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
        requireTopics(history.definitions());
        continuation = new As400OperatorCutReader(foreground.journals, foreground.identities, journal, history, foreground.reader, foreground.cutLimits);
        observe("recovered", () -> Map.of("checkpoint", durable));
        return history;
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
        require(definitions.isEmpty() || !names.isEmpty(), "Table filters select no native binding");
        names.add(topics.heartbeatTopic());
        names.add(controlTopic);
        names.add(historyTopic);
        As400KafkaTopicGate.require(nativeRaw, names, false, this::check);
    }

    @Override
    public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
        check();
        Thread.sleep(250);
        var cut = continuation.readPrefix(durable, scan.orElse(null));
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
        boolean interrupted = false;
        try {
            while (!collector.isTerminated()) {
                try {
                    collector.awaitTermination(100, TimeUnit.MILLISECONDS);
                }
                catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }
            try {
                foreground.close();
            }
            finally {
                background.close();
            }
            observe("drained", () -> Map.of("completed", true));
        }
        finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
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
        require(server != null && server.matches("[A-Z0-9_#$@]{1,10}"), "Verified native server required");
        return definitions.stream().map(definition -> {
            var identity = definition.identity();
            require(identity.tableId().catalog() == null, "Acquired binding already has an unqualified catalog association");
            var id = new TableId(server, identity.tableId().schema(), identity.tableId().table());
            return new As400BindingDefinition(new As400TransactionBuffer.TableIdentity(id, identity.physicalFile(), identity.member(),
                    identity.fileJournalId(), identity.memberJournalId(), identity.recordFormat()), definition.schema().edit().tableId(id).create(), definition.codecs());
        }).toList();
    }

    private static final class Staging implements QueueProvider<DataChangeEvent> {
        final ArrayDeque<DataChangeEvent> records = new ArrayDeque<>();

        @Override
        public void configure(Map<String, ?> config) {
        }

        @Override
        public String name() {
            return "isolated-legacy-staging";
        }

        @Override
        public void enqueue(DataChangeEvent event) {
            require(records.size() < 4096, "Bounded legacy staging exceeded");
            records.addLast(event);
        }

        @Override
        public DataChangeEvent poll() {
            return records.pollFirst();
        }

        @Override
        public int size() {
            return records.size();
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
