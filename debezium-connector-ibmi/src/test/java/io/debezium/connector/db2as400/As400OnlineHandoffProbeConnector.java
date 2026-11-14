/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Clock;

/**
 * LOCAL TEST ONLY. Captured source bytes/bindings; controlled RetrieveJournal
 * responses through the actual RPC/legacy handler, task and dispatcher. Standard
 * Connect offsets/transactions/fencing; no custom owner or receipt authority.
 * The read-only bundle is fixture history, not native startup proof or a new store.
 */
public class As400OnlineHandoffProbeConnector extends As400LegacyBarrierProbeConnector {
    static final String COMMIT = "probe-handoff-boundary";
    static final Set<String> LEGACY_TYPES = Set.of("PT", "PX", "UP", "UB", "DL", "DR", "CT", "CG", "CR", "SC", "CM");

    @Override
    public Class<? extends Task> taskClass() {
        return HandoffTask.class;
    }

    public static final class Topics extends AbstractTopicNamingStrategy<TableId> {
        private String topic;

        public Topics(java.util.Properties properties) {
            super(properties);
        }

        @Override
        public void configure(java.util.Properties properties) {
            super.configure(properties);
            topic = properties.getProperty("probe.topic");
        }

        @Override
        public String dataChangeTopic(TableId table) {
            return topic;
        }

        @Override
        public String heartbeatTopic() {
            return topic + "-control";
        }
    }

    /** Fault injection only: hold source-offset enqueue after a truncate send, leaving worker bootstrap offsets readable. */
    public static final class HoldTruncateOffset implements org.apache.kafka.clients.producer.ProducerInterceptor<Object, Object> {
        private boolean truncateSent;

        @Override
        public org.apache.kafka.clients.producer.ProducerRecord<Object, Object> onSend(
                                                                                       org.apache.kafka.clients.producer.ProducerRecord<Object, Object> record) {
            if (truncateSent && record.topic().startsWith("offsets-handoff")) {
                hold();
                truncateSent = false;
            }
            if (record.value() instanceof byte[] bytes) {
                try {
                    var node = new ObjectMapper().readTree(bytes);
                    truncateSent |= "t".equals((node.has("payload") ? node.path("payload") : node).path("op").asText());
                }
                catch (java.io.IOException ignored) {
                    // Internal offset/control values are not data envelopes.
                }
            }
            return record;
        }

        private void hold() {
            try (var files = Files.list(CONTROL)) {
                var hold = files.filter(p -> p.getFileName().toString().startsWith(System.getenv("PROBE_WORKER") + "-")
                        && p.getFileName().toString().endsWith(".hold-first-truncate")).findFirst();
                if (hold.isEmpty()) {
                    return;
                }
                String generation = hold.orElseThrow().getFileName().toString().replace(".hold-first-truncate", "");
                write(generation + ".truncate-offset-held.json", Map.of("source_offset_enqueued", false));
                long end = System.nanoTime() + Duration.ofMinutes(4).toNanos();
                while (!Files.exists(CONTROL.resolve(generation + ".release-truncate"))) {
                    if (System.nanoTime() > end) {
                        throw new IllegalStateException("Bounded offset-send fault gate expired");
                    }
                    Thread.sleep(20);
                }
            }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            catch (java.io.IOException error) {
                throw new IllegalStateException(error);
            }
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        }

        @Override
        public void configure(Map<String, ?> properties) {
            if (System.getenv("PROBE_WORKER") == null) {
                throw new IllegalStateException("Local fault worker required");
            }
        }

        @Override
        public void close() {
        }
    }

    public static class HandoffTask extends As400ConnectorTask {
        interface NativeRuntime extends As400ContinuousRuntime.Intervals, AutoCloseable {
            void requestStop();

            @Override
            void close() throws Exception;
        }

        boolean liveSource() {
            return false;
        }

        void checkSource(Configuration raw) {
            As400OnlineHandoff.require("no-live-ibmi.invalid".equals(raw.getString("database.hostname")), "Captured-only task cannot open IBM i");
        }

        List<Entry> readLegacyEntries(As400OnlineHandoff.Evidence captured, JournalPosition from, JournalPosition through, List<Entry> expected) throws Exception {
            return expected;
        }

        NativeRuntime nativeRuntime(Configuration raw, String generation, As400BindingHistory.Recovered history) throws Exception {
            As400OnlineHandoff.require(!liveSource(), "Live continuation is unavailable");
            return null;
        }

        void stopNativeSource() {
        }

        void closeNativeSource() throws Exception {
        }

        private CdcSourceTaskContext<? extends CommonConnectorConfig> taskContext;
        private ChangeEventQueue<DataChangeEvent> queue;
        private ErrorHandler errors;
        private String generation;
        private String topic;
        private String bundleHash;
        private As400OnlineHandoff.Evidence evidence;
        private As400OnlineHandoff.Evidence runtimeEvidence;
        private As400HandoffBindingHistory bindingHistory;
        private As400HandoffBindingHistory.Stored storedBindings;
        private boolean normalRequested;
        private NormalRecovery normalRecovery;
        private As400OnlineHandoff.Candidate candidate;
        private As400ConnectorConfig config;
        private As400ConnectorConfig legacyConfig;
        private JournalPosition cut;
        private List<JournalPosition> deferrals = List.of();
        private java.util.function.Supplier<As400BatchDispatcher> batchFactory;
        private volatile boolean stopped;
        private volatile boolean barrierAcknowledged;
        private volatile boolean migrationAcknowledged;
        private volatile BigInteger progressAcknowledged = BigInteger.ZERO;
        private int polled;
        private boolean truncatePolled;
        private final long deadline = System.nanoTime() + Duration.ofMinutes(12).toNanos();

        @Override
        public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration raw) {
            taskContext = super.preStart(raw);
            return taskContext;
        }

        @Override
        protected ChangeEventSourceCoordinator<As400Partition, As400OffsetContext> start(Configuration raw) {
            config = (As400ConnectorConfig) taskContext.getConfig();
            legacyConfig = new As400ConnectorConfig(raw.edit().with("skipped.operations", "t").build());
            generation = System.getenv("PROBE_WORKER") + "-" + UUID.randomUUID();
            topic = raw.getString("probe.topic");
            bundleHash = raw.getString("probe.bundle.sha256");
            normalRequested = "true".equals(raw.getString("probe.normal.recovery"));
            Map<String, ?> loaded = context.offsetStorageReader().offset(SOURCE_PARTITION);
            write(generation + ".start.json", Map.of("generation", generation, "loaded", loaded == null ? Map.of() : loaded));
            As400OnlineHandoff.require(loaded != null, "Missing offset: no implicit migration or initialization");
            checkSource(raw);
            As400OnlineHandoff.require("connector".equals(raw.getString("transaction.boundary")) && "requested".equals(raw.getString("exactly.once.support"))
                    && "none".equals(raw.getString("errors.tolerance")) && "true".equals(raw.getString("probe.fixture.visibility.assumed"))
                    && Integer.valueOf(1).equals(raw.getInteger("max.batch.size"))
                    && !config.shouldProvideTransactionMetadata() && raw.getString("table.exclude.list") == null
                    && raw.getString("transforms") == null, "Fixed local fixture profile required");
            As400BatchDispatcher.requireConfiguration(config);
            try {
                Path bundle = Path.of("/probe/online-handoff.json");
                if ("true".equals(raw.getString("probe.prospective"))) {
                    As400OnlineHandoff.require(Files.size(bundle) <= 16 * 1024 * 1024, "Excessive prospective fixture");
                    byte[] bytes = Files.readAllBytes(bundle);
                    As400OnlineHandoff.require(As400OnlineHandoff.hash(bytes).equals(bundleHash), "Changed prospective fixture");
                    var bundleContent = new ObjectMapper().readTree(bytes);
                    As400OnlineHandoff.require(!"true".equals(raw.getString("probe.native.history.discovery")) || bundleContent.has("history_discovery"),
                            "Required native initial history discovery is missing");
                    var input = As400ProspectiveReplay.prepare(bundleContent, config);
                    As400OnlineHandoff.require(input.cut().offset().toString().equals(raw.getString("probe.cut")), "Changed prospective cut");
                    evidence = input.evidence();
                    if ("true".equals(raw.getString("probe.standard.history"))) {
                        As400OnlineHandoff.require(bundleContent.path("profile").asText().equals("prospective-native-boundary-v1"),
                                "Native short-boundary evidence required before history experiment");
                        bindingHistory = new As400HandoffBindingHistory(raw, input.origin());
                    }
                }
                else {
                    evidence = As400OnlineHandoff.load(bundle, bundleHash, config);
                }
                As400OnlineHandoff.require(!"true".equals(raw.getString("probe.standard.history")) || bindingHistory != null,
                        "History experiment cannot fall back to fixture-only admission");
                As400OnlineHandoff.require(!normalRequested || bindingHistory != null, "Normal recovery requires durable native binding history");
                runtimeEvidence = evidence;
                cut = evidence.entries().stream().map(Entry::position).filter(p -> p.offset().toString().equals(raw.getString("probe.cut"))).findFirst().orElseThrow();
                String requested = raw.getString("probe.defer.cuts");
                if (requested != null) {
                    var numbers = java.util.Arrays.stream(requested.split(",", -1)).map(BigInteger::new).toList();
                    As400OnlineHandoff.require(!numbers.isEmpty() && numbers.size() <= 16 && numbers.stream().distinct().count() == numbers.size()
                            && numbers.equals(numbers.stream().sorted().toList()), "Ordered bounded deferral cuts required");
                    deferrals = numbers.stream().map(n -> evidence.entries().stream().map(Entry::position).filter(p -> p.offset().equals(n)).findFirst().orElseThrow())
                            .toList();
                    As400OnlineHandoff.require(deferrals.stream().allMatch(p -> evidence.order().compare(evidence.upper(), p) <= 0
                            && evidence.order().compare(p, cut) < 0), "Deferrals must precede the final cut");
                }
                if (deferrals.isEmpty() || loaded.containsKey(As400Checkpoint.VERSION) || "native-capture-output-v1".equals(loaded.get(BARRIER))) {
                    candidate = As400OnlineHandoff.prepare(evidence, cut, config);
                }
                if (bindingHistory != null && !loaded.containsKey(As400Checkpoint.VERSION)) {
                    var pending = bindingHistory.pending(loaded);
                    if (pending.isPresent()) {
                        storedBindings = bindingHistory.resumeCandidate(evidence, candidate.checkpoint(), pending.orElseThrow());
                    }
                    else {
                        As400OnlineHandoff.require(!"native-capture-output-v1".equals(loaded.get(BARRIER)),
                                "Acknowledged migration barrier lost its required binding history");
                    }
                }
                if (loaded.containsKey(As400Checkpoint.VERSION)) {
                    if (bindingHistory != null) {
                        if (liveSource()) {
                            var recovered = bindingHistory.recoverSource(evidence.journal(), loaded);
                            runtimeEvidence = new As400OnlineHandoff.Evidence(evidence.entries(), recovered.definitions(), evidence.order(),
                                    recovered.journalIdentity(), recovered.fingerprint(), evidence.lower(), evidence.upper(), evidence.inventory(),
                                    evidence.historyDigest(), evidence.visibility());
                        }
                        else {
                            runtimeEvidence = bindingHistory.recover(evidence, loaded).runtime();
                        }
                        write(generation + ".history-recovered.json", Map.of("offset", loaded));
                    }
                    var saved = checkpoint(loaded);
                    As400OnlineHandoff.require((liveSource() ? saved.completedThrough().offset().compareTo(candidate.checkpoint().completedThrough().offset())
                            : evidence.order().compare(saved.completedThrough(), candidate.checkpoint().completedThrough())) >= 0,
                            "Buffered offset predates migration");
                }
                else if (isProgress(loaded)) {
                    validateProgress(loaded);
                }
                else if (loaded.containsKey(BARRIER)) {
                    validateBarrier(loaded);
                }
                else {
                    As400OnlineHandoff.require(loaded.equals(legacy(evidence.entries().get(0).position())),
                            "Unexpected ordinary legacy seed; a lagging offset is not a barrier");
                }
                if (normalRequested) {
                    normalRecovery = new NormalRecovery();
                    var coordinator = super.start(raw);
                    errors = getErrorHandler().orElseThrow();
                    write(generation + ".normal-task.json", Map.of("offset", loaded));
                    return coordinator;
                }
                registerServiceProviders(config.getServiceRegistry());
                config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
                config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
                config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
                queue = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(1).maxQueueSize(64).pollInterval(Duration.ofMillis(10))
                        .loggingContextSupplier(() -> taskContext.configureLoggingContext("online-handoff-probe")).build();
                errors = new ErrorHandler(As400RpcConnector.class, config, queue, null);
                initializeOutput(queue, errors);
                var topics = new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
                    @Override
                    public String dataChangeTopic(TableId id) {
                        return topic;
                    }
                };
                var schemas = new As400NativeTableSchemas(runtimeEvidence.definitions(), config, topics);
                var headers = new DebeziumHeaderProducer(taskContext);
                batchFactory = () -> new As400BatchDispatcher(config, new As400Partition("test"), schemas, topics, headers, System::currentTimeMillis, 20_000, true);
                var dispatcher = new EventDispatcher<As400Partition, TableId>(config, topics, schemas, queue,
                        id -> config.getTableFilters().dataCollectionFilter().isIncluded(id), DataChangeEvent::new, null, new As400EventMetadataProvider(),
                        new io.debezium.heartbeat.HeartbeatImpl(config.getHeartbeatInterval(), topic + "-control", config.getLogicalName(), config.schemaNameAdjuster(),
                                queue),
                        config.schemaNameAdjuster(), null, headers);
                var coordinator = new As400TaskStopProbeConnector.FixtureCoordinator(config, errors, dispatcher, schemas,
                        running -> produce(running, dispatcher, loaded), generation);
                coordinator.start(taskContext, queue, new As400EventMetadataProvider());
                return coordinator;
            }
            catch (Exception error) {
                throw new IllegalStateException("Rejected test-only handoff admission", error);
            }
        }

        private void produce(ChangeEventSourceContext running, EventDispatcher<As400Partition, TableId> dispatcher, Map<String, ?> loaded) {
            try {
                Map<String, ?> resume = loaded;
                if (!loaded.containsKey(As400Checkpoint.VERSION)) {
                    if (!loaded.containsKey(BARRIER) || isProgress(loaded)) {
                        write(generation + ".before-legacy.json", Map.of());
                        await("release-legacy", running);
                        var barrier = new HashMap<String, Object>(publishLegacy(dispatcher, running, loaded));
                        barrier.put(BARRIER, "native-capture-output-v1");
                        barrier.put("probe.generation", generation);
                        barrier.put("probe.bundle", bundleHash);
                        barrier.put("probe.fingerprint", evidence.fingerprint());
                        stageHistory(running);
                        enqueue(marker(barrier, "barrier"));
                        write(generation + ".barrier-enqueued.json", Map.of("offset", barrier));
                        while (!barrierAcknowledged) {
                            tick(running);
                            Thread.sleep(10);
                        }
                    }
                    else {
                        write(generation + ".resumed-barrier.json", Map.of("offset", loaded));
                        stageHistory(running);
                    }
                    await("release-migration", running);
                    if (bindingHistory != null) {
                        runtimeEvidence = storedBindings.runtime();
                        resume = storedBindings.checkpoint().toOffset();
                        tick(running);
                    }
                    else {
                        resume = candidate.checkpoint().toOffset();
                    }
                    enqueue(marker(resume, "migration"));
                    write(generation + ".migration-enqueued.json", Map.of("offset", resume, "R", cut.toString(), "open", candidate.openCycles().toString()));
                    while (!migrationAcknowledged) {
                        tick(running);
                        Thread.sleep(10);
                    }
                }
                else {
                    write(generation + ".resumed-buffered.json", Map.of("offset", loaded));
                }
                await("release-stream", running);
                stream(resume, running);
            }
            catch (Exception error) {
                if (!stopped && running.isRunning()) {
                    errors.setProducerThrowable(error);
                    write(generation + ".producer-failure.json", Map.of("error", error.toString()));
                }
            }
        }

        private void stageHistory(ChangeEventSourceContext running) throws Exception {
            if (bindingHistory != null) {
                tick(running);
                if (storedBindings == null) {
                    storedBindings = bindingHistory.initialize(evidence, candidate.checkpoint());
                }
                write(generation + ".history-durable.json", Map.of("unpublished_candidate", storedBindings.checkpoint().toOffset()));
                if (Files.exists(CONTROL.resolve(generation + ".hold-history"))) {
                    await("release-history", running);
                }
                tick(running);
            }
        }

        private Map<String, ?> publishLegacy(EventDispatcher<As400Partition, TableId> dispatcher, ChangeEventSourceContext running, Map<String, ?> loaded)
                throws Exception {
            JournalPosition from = isProgress(loaded) ? validateProgress(loaded) : evidence.entries().get(0).position();
            var boundaries = new java.util.ArrayList<>(deferrals.stream().filter(p -> evidence.order().compare(p, from) > 0).toList());
            boundaries.add(cut);
            var through = new AtomicReference<>(boundaries.get(0));
            var expected = evidence.entries().stream().filter(e -> evidence.order().compare(e.position(), from) > 0
                    && evidence.order().compare(e.position(), cut) <= 0 && LEGACY_TYPES.contains(new EntryHeaderDecoder().decode(e.bytes(), 0).getEntryType())).toList();
            var selected = readLegacyEntries(evidence, from, cut, expected);
            var at = new AtomicInteger(-1);
            var journal = mock(RetrieveJournal.class);
            when(journal.retrieveJournal(any())).thenReturn(RetrievalState.Success);
            when(journal.nextEntry()).thenAnswer(i -> {
                int next = at.get() + 1;
                if (next >= selected.size() || evidence.order().compare(selected.get(next).position(), through.get()) > 0) {
                    return false;
                }
                at.incrementAndGet();
                return true;
            });
            when(journal.getEntryHeader()).thenAnswer(i -> new EntryHeaderDecoder().decode(selected.get(at.get()).bytes(), 0));
            when(journal.getPosition()).thenAnswer(i -> {
                var entry = selected.get(Math.min(at.get(), selected.size() - 1));
                return new JournalProcessedPosition(entry.position(), new EntryHeaderDecoder().decode(entry.bytes(), 0).getTime(), true);
            });
            when(journal.decode(any())).thenAnswer(i -> {
                var entry = selected.get(at.get());
                return ((io.debezium.ibmi.db2.journal.retrieve.JournalEntryDeocder<?>) i.getArgument(0))
                        .decode(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0);
            });
            var lifecycle = new JournalReadLifecycle();
            var rpc = org.mockito.Mockito.spy(new As400RpcConnection(legacyConfig, null, null, null, lifecycle, ignored -> journal));
            org.mockito.Mockito.doNothing().when(rpc).logOffsets(any(), any());
            var jdbc = mock(As400JdbcConnection.class);
            when(jdbc.getRealDatabaseName()).thenReturn(null);
            when(jdbc.getLongName(anyString(), anyString())).thenAnswer(i -> i.getArgument(1));
            var metadata = mock(As400DatabaseSchema.class);
            var cache = mock(io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.class);
            when(cache.retrieve(any(), anyString(), anyString())).thenAnswer(i -> {
                var definition = evidence.definitions().stream().filter(d -> d.identity().physicalFile().equals(i.getArgument(2))
                        && d.identity().tableId().schema().equals(i.getArgument(1))).findFirst().orElseThrow();
                return new io.debezium.ibmi.db2.journal.retrieve.SchemaCacheIF.TableInfo(List.of(), definition.schema().primaryKeyColumnNames(),
                        new com.ibm.as400.access.AS400Structure(definition.codecs().stream().map(As400BindingDefinition.Codec::decoder)
                                .toArray(com.ibm.as400.access.AS400DataType[]::new)));
            });
            // Exercise the real LEGACY decoder, including its R/DR behavior. The
            // buffered strict decoder intentionally accepts forward images only.
            var rowDecoder = org.mockito.Mockito.spy(new io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder(() -> {
                throw new java.sql.SQLException("Live JDBC forbidden in captured fixture");
            }, null, cache, 37, 37));
            for (var definition : evidence.definitions()) {
                org.mockito.Mockito.doReturn(java.util.Optional.of(definition.identity().physicalFile())).when(rowDecoder)
                        .getLongName(definition.identity().tableId().schema(), definition.identity().physicalFile());
            }
            when(metadata.getFileDecoder()).thenReturn(rowDecoder);
            var scan = new As400OffsetContext.Loader(legacyConfig).load(isProgress(loaded) ? exactLegacy(from) : legacy(from));
            var source = new As400StreamingChangeEventSource(legacyConfig, rpc, jdbc, dispatcher, errors, Clock.system(), metadata);
            source.init(scan);
            var consumer = source.processJournalEntries(new As400Partition("test"), scan);
            try {
                for (JournalPosition boundary : boundaries) {
                    through.set(boundary);
                    rpc.getJournalEntries(running, scan, (sequence, reader, header) -> {
                        tick(running);
                        consumer.accept(sequence, reader, header);
                        append(generation, "legacy-scan", Map.of("source", sequence.toString(), "serialized", scan.getOffset()));
                    }, mock(WatchDog.class));
                    As400OnlineHandoff.require(scan.getPosition().asJournalPosition().equals(boundary), "Legacy reader missed processed boundary");
                    if (!boundary.equals(cut)) {
                        Set<BigInteger> pending;
                        try {
                            As400OnlineHandoff.prepare(evidence, boundary, config);
                            throw new IllegalStateException("Fixture deferral boundary unexpectedly qualifies");
                        }
                        catch (As400OnlineHandoff.SelectedPrefixPending busy) {
                            pending = busy.cycles();
                        }
                        // A legacy progress marker must not skip cached UB state
                        // on replacement. This fixture supports insert/delete/clear
                        // prefixes; unresolved updates reject rather than disappear.
                        As400OnlineHandoff.require(!source.hasPendingBeforeImage(), "Legacy progress crosses a cached before-image");
                        var progress = new HashMap<String, Object>(scan.getOffset());
                        progress.put(BARRIER, "native-capture-progress-v1");
                        progress.put("probe.generation", generation);
                        progress.put("probe.bundle", bundleHash);
                        progress.put("probe.fingerprint", evidence.fingerprint());
                        enqueue(marker(progress, "legacy-progress"));
                        write(generation + ".progress-" + boundary.offset() + ".json", Map.of("offset", progress, "pending", pending.toString()));
                        while (progressAcknowledged.compareTo(boundary.offset()) < 0) {
                            tick(running);
                            Thread.sleep(10);
                        }
                        await("release-progress-" + boundary.offset(), running);
                    }
                }
                As400OnlineHandoff.require(scan.getPosition().asJournalPosition().equals(cut), "Legacy reader did not reach the exact processed cut");
                if (candidate == null) {
                    candidate = As400OnlineHandoff.prepare(evidence, cut, config);
                }
                return Map.copyOf(scan.getOffset());
            }
            finally {
                lifecycle.requestStop();
            }
        }

        private void stream(Map<String, ?> saved, ChangeEventSourceContext running) throws Exception {
            var loaded = checkpoint(saved);
            try (var replay = As400OnlineHandoff.coordinator(runtimeEvidence, saved, this::enqueue);
                    var batches = batchFactory.get()) {
                for (Entry entry : evidence.entries()) {
                    tick(running);
                    int c = evidence.order().compare(entry.position(), loaded.replayFrom());
                    if (c < 0 || c == 0 && !loaded.replayInclusive()) {
                        continue;
                    }
                    if (replay.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position()).isPresent()) {
                        replay.deliver(batches);
                        replay.publishCheckpoint(marker(Map.of(), "batch"));
                    }
                }
                replay.verifyEndOfAvailableJournal();
                replay.publishCheckpoint(marker(Map.of(), "final"));
                write(generation + ".stream-enqueued.json", Map.of("offset", replay.checkpoint().toOffset(), "R", replay.scanPosition().orElseThrow().toString()));
            }
        }

        private As400Checkpoint checkpoint(Map<String, ?> offset) {
            var checkpoint = As400Checkpoint.fromOffset(offset, runtimeEvidence.journal(), runtimeEvidence.fingerprint());
            if (liveSource()) {
                // Current native identity/receiver/history proof is mandatory in
                // the live reader before any output. Captured ranges cannot
                // validate a later durable F/W after real source progress.
                return checkpoint;
            }
            for (var position : List.of(checkpoint.replayFrom(), checkpoint.completedThrough())) {
                As400OnlineHandoff.require(evidence.entries().stream().anyMatch(e -> e.position().equals(position)), "Required F/W history anchor missing");
                As400OnlineHandoff.require(evidence.order().incarnation(position).equals(position.equals(checkpoint.replayFrom())
                        ? checkpoint.replayReceiverIdentity()
                        : checkpoint.completedReceiverIdentity()), "Receiver evidence changed");
            }
            return checkpoint;
        }

        private Map<String, ?> legacy(JournalPosition position) {
            return new As400OffsetContext(config, new JournalProcessedPosition(position, Instant.EPOCH, true), true,
                    new io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext<>(true)).getOffset();
        }

        private void validateBarrier(Map<String, ?> offset) {
            validateBoundary(offset, "native-capture-output-v1", cut);
        }

        private boolean isProgress(Map<String, ?> offset) {
            return "native-capture-progress-v1".equals(offset.get(BARRIER));
        }

        private JournalPosition validateProgress(Map<String, ?> offset) {
            var position = deferrals.stream().filter(p -> p.offset().toString().equals(offset.get("offset.event_sequence"))).findFirst().orElseThrow();
            validateBoundary(offset, "native-capture-progress-v1", position);
            return position;
        }

        private void validateBoundary(Map<String, ?> offset, String kind, JournalPosition boundary) {
            var original = new HashMap<String, Object>(offset);
            if (normalRequested && kind.equals("native-capture-output-v1")) {
                new As400LegacyHandoff(storedBindings.checkpoint(), As400LegacyHandoff.strings(offset), evidence.order().incarnation(boundary))
                        .verifyOrder(evidence.order());
                original.remove(As400LegacyHandoff.VERSION);
                original.remove(As400LegacyHandoff.CHECKPOINT);
                original.remove(As400LegacyHandoff.FINGERPRINT);
            }
            As400OnlineHandoff.require(kind.equals(original.remove(BARRIER))
                    && bundleHash.equals(original.remove("probe.bundle")) && evidence.fingerprint().equals(original.remove("probe.fingerprint"))
                    && original.remove("probe.generation") instanceof String, "Incomplete or foreign publication barrier");
            As400OnlineHandoff.require(original.equals(exactLegacy(boundary)), "Publication cut differs from pinned native capture");
        }

        private Map<String, ?> exactLegacy(JournalPosition position) {
            var entry = evidence.entries().stream().filter(e -> e.position().equals(position)).findFirst().orElseThrow();
            var scan = new As400OffsetContext(config, new JournalProcessedPosition(position, new EntryHeaderDecoder().decode(entry.bytes(), 0).getTime(), true), true,
                    new io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext<>(true));
            return Map.copyOf(scan.getOffset());
        }

        private SourceRecord marker(Map<String, ?> offset, String kind) {
            var record = new SourceRecord(SOURCE_PARTITION, offset, topic + "-control", 0, Schema.STRING_SCHEMA, kind, CONTROL_SCHEMA, kind);
            record.headers().addString(COMMIT, kind);
            return record;
        }

        private boolean enqueue(SourceRecord record) throws InterruptedException {
            if (stopped || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Stopped handoff task");
            }
            if (normalRecovery != null && normalRecovery.legacyOutput != null) {
                try {
                    if (record.headers().lastWithName(COMMIT) != null) {
                        normalRecovery.legacyOutput.barrier(record);
                        normalRecovery.legacyOutput.acknowledge(record);
                    }
                    else {
                        normalRecovery.legacyOutput.emit(record);
                    }
                }
                catch (InterruptedException error) {
                    throw error;
                }
                catch (Exception error) {
                    throw new IllegalStateException(error);
                }
            }
            else {
                queue.enqueue(new DataChangeEvent(record));
            }
            return true;
        }

        private void tick(ChangeEventSourceContext running) throws InterruptedException {
            if (stopped || !running.isRunning() || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Stopped handoff task");
            }
            As400OnlineHandoff.require(System.nanoTime() < deadline, "Bounded handoff experiment timed out");
        }

        private void await(String suffix, ChangeEventSourceContext running) throws Exception {
            while (!Files.exists(CONTROL.resolve(generation + "." + suffix))) {
                tick(running);
                Thread.sleep(20);
            }
            tick(running);
        }

        @Override
        protected List<SourceRecord> doPoll() throws InterruptedException {
            if (errors.getProducerThrowable() != null) {
                return super.doPoll();
            }
            try {
                var permit = CONTROL.resolve(generation + ".permit");
                boolean held = truncatePolled && Files.exists(CONTROL.resolve(generation + ".hold-first-truncate"))
                        && !Files.exists(CONTROL.resolve(generation + ".release-truncate"));
                if (!stopped && !held && Files.exists(permit) && Integer.parseInt(Files.readString(permit).trim()) > polled) {
                    var records = super.doPoll();
                    for (var record : records) {
                        record.headers().addString("probe-generation", generation);
                        record.headers().addString("probe-index", Integer.toString(polled++));
                        if (normalRecovery == null && record.headers().lastWithName(COMMIT) != null) {
                            context.transactionContext().commitTransaction(record);
                        }
                        append(generation, "poll", Map.of("index", polled, "key", String.valueOf(record.key()), "offset", record.sourceOffset()));
                        // Fault injection follows actual output, not an estimated number of
                        // checkpoint markers: one lifecycle release can drain several batches.
                        if (Files.exists(CONTROL.resolve(generation + ".hold-first-truncate")) && record.value() instanceof Struct value
                                && value.schema().field("op") != null && "t".equals(value.getString("op"))) {
                            truncatePolled = true;
                            write(generation + ".truncate-polled.json", Map.of("index", polled, "offset", record.sourceOffset()));
                        }
                    }
                    return records;
                }
                Thread.sleep(20);
                return List.of();
            }
            catch (java.io.IOException error) {
                throw new IllegalStateException(error);
            }
        }

        @Override
        public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
            String kind = record.headers().lastWithName(COMMIT) == null ? "data" : record.headers().lastWithName(COMMIT).value().toString();
            if (normalRecovery != null && record.topic().equals(topic + "-control")
                    && evidence.entries().get(evidence.entries().size() - 1).position().offset().toString()
                            .equals(record.sourceOffset().get("completed_through.sequence"))) {
                kind = "final";
            }
            if (Files.exists(CONTROL.resolve(generation + ".hold-" + kind + "-callback"))) {
                write(generation + "." + kind + "-callback-held.json", Map.of("offset", record.sourceOffset()));
                while (!stopped && !Files.exists(CONTROL.resolve(generation + ".release-callback"))) {
                    Thread.sleep(20);
                }
            }
            super.commitRecord(record, metadata);
            append(generation, "commitRecord", Map.of("kind", kind, "offset", record.sourceOffset()));
            if (normalRecovery != null && kind.equals("final")) {
                write(generation + ".stream-enqueued.json", Map.of("offset", record.sourceOffset(), "normal_task", true));
            }
            if (kind.equals("barrier")) {
                barrierAcknowledged = true;
            }
            if (kind.equals("migration")) {
                migrationAcknowledged = true;
            }
            if (kind.equals("legacy-progress")) {
                progressAcknowledged = new BigInteger(record.sourceOffset().get("offset.event_sequence").toString());
            }
        }

        @Override
        protected void doStop() {
            stopped = true;
            super.doStop();
            write(generation + ".stopped.json", Map.of());
        }

        @Override
        As400TaskRecovery recoveryForTesting(Configuration raw) {
            return normalRecovery;
        }

        /** Only captured transport is substituted; normal startup/runtime/acknowledgements/stop are real. */
        private final class NormalRecovery implements As400TaskHandoff {
            private final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean();
            private As400BindingHistory.Recovered history;
            private As400TaskHandoff.Output legacyOutput;
            private volatile NativeRuntime nativeReader;

            NormalRecovery() throws java.io.IOException {
                var worker = new java.util.Properties();
                try (var input = Files.newInputStream(Path.of("/worker.properties"))) {
                    worker.load(input);
                }
                As400OnlineHandoff.require("enabled".equals(worker.getProperty("exactly.once.source.support"))
                        && "broker:9092".equals(worker.getProperty("bootstrap.servers")), "Pinned local EOS worker required");
            }

            @Override
            public boolean allowLegacyHandoffRecovery() {
                return true;
            }

            @Override
            public As400Partition partition() {
                return new As400Partition("test");
            }

            @Override
            public String journalIdentity() {
                return runtimeEvidence.journal();
            }

            @Override
            public String fingerprint() {
                return runtimeEvidence.fingerprint();
            }

            @Override
            public void checkQualified(Configuration raw) {
                As400RecoveryAcknowledgements.requireConnectorProfile(raw.asMap());
                As400OnlineHandoff.require(!closed.get() && System.getenv("PROBE_WORKER") != null, "Local handoff admission ended");
                checkSource(raw);

            }

            @Override
            public As400Checkpoint handoff(Map<String, ?> loaded, As400TaskHandoff.Output output) throws Exception {
                legacyOutput = output;
                ChangeEventSourceContext running = mock(ChangeEventSourceContext.class);
                when(running.isRunning()).thenAnswer(i -> !closed.get());
                try {
                    if (!loaded.containsKey(BARRIER) || isProgress(loaded)) {
                        write(generation + ".before-legacy.json", Map.of("normal_task", true));
                        await("release-legacy", running);
                        // Only the queue adapter is controlled: real legacy RPC
                        // parsing/dispatch routes every output through the normal
                        // source's registered acknowledgement and queue lifecycle.
                        @SuppressWarnings("unchecked")
                        ChangeEventQueue<DataChangeEvent> delivery = mock(ChangeEventQueue.class);
                        org.mockito.Mockito.doAnswer(invocation -> {
                            output.emit(((DataChangeEvent) invocation.getArgument(0)).getRecord());
                            return null;
                        }).when(delivery).enqueue(any());
                        var topics = new Topics(config.getConfig().asProperties());
                        var schemas = new As400NativeTableSchemas(evidence.definitions(), config, topics);
                        var headers = new DebeziumHeaderProducer(taskContext);
                        try (var dispatcher = new EventDispatcher<As400Partition, TableId>(config, topics, schemas, delivery,
                                id -> config.getTableFilters().dataCollectionFilter().isIncluded(id), DataChangeEvent::new,
                                new As400EventMetadataProvider(), config.schemaNameAdjuster(), null, headers)) {
                            var barrier = new HashMap<String, Object>(publishLegacy(dispatcher, running, loaded));
                            barrier.put(BARRIER, "native-capture-output-v1");
                            barrier.put("probe.generation", generation);
                            barrier.put("probe.bundle", bundleHash);
                            barrier.put("probe.fingerprint", evidence.fingerprint());
                            stageHistory(running);
                            var record = marker(As400LegacyHandoff.bind(barrier, storedBindings.checkpoint()), "barrier");
                            output.barrier(record);
                            write(generation + ".barrier-enqueued.json", Map.of("offset", record.sourceOffset()));
                            output.acknowledge(record);
                        }
                    }
                    else {
                        stageHistory(running);
                        write(generation + ".resumed-barrier.json", Map.of("offset", loaded));
                    }
                    var durable = context.offsetStorageReader().offset(SOURCE_PARTITION);
                    validateBarrier(durable);
                    storedBindings = bindingHistory.activate(evidence, storedBindings, durable,
                            () -> context.offsetStorageReader().offset(SOURCE_PARTITION));
                    write(generation + ".activation-durable.json", Map.of("offset", storedBindings.checkpoint().toOffset()));
                    if (Files.exists(CONTROL.resolve(generation + ".hold-activation"))) {
                        await("release-activation", running);
                    }
                    await("release-migration", running);
                    runtimeEvidence = storedBindings.runtime();
                    var migration = marker(storedBindings.checkpoint().toOffset(), "migration");
                    output.barrier(migration);
                    write(generation + ".migration-enqueued.json", Map.of("offset", migration.sourceOffset()));
                    output.acknowledge(migration);
                    write(generation + ".handoff-complete.json", Map.of("offset", migration.sourceOffset()));
                    return storedBindings.checkpoint();
                }
                finally {
                    legacyOutput = null;
                }
            }

            @Override
            public As400BindingHistory.Recovered recover(Map<String, ?> durable) throws Exception {
                history = liveSource() ? bindingHistory.recoverSource(evidence.journal(), durable) : bindingHistory.recover(evidence, durable).bindings();
                nativeReader = nativeRuntime(config.getConfig(), generation, history);
                if (closed.get() && nativeReader != null) {
                    nativeReader.requestStop();
                    throw new IllegalStateException("Stopped during native continuation setup");
                }
                write(generation + ".history-recovered.json", Map.of("offset", durable, "normal_task", true));
                return history;
            }

            @Override
            public As400OperatorCutReader.Cut read(Map<String, ?> durable, java.util.Optional<io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry> scan)
                    throws Exception {
                while (!Files.exists(CONTROL.resolve(generation + ".release-stream"))) {
                    checkQualified(config.getConfig());
                    As400OnlineHandoff.require(System.nanoTime() < deadline, "Bounded normal reader gate timed out");
                    Thread.sleep(20);
                }
                if (nativeReader != null) {
                    checkQualified(config.getConfig());
                    return nativeReader.read(durable, scan);
                }
                var checkpoint = checkpoint(durable);
                if (scan.isPresent()) {
                    var anchor = scan.orElseThrow();
                    As400OnlineHandoff.require(evidence.entries().stream().anyMatch(e -> e.position().equals(anchor.position())
                            && java.util.Arrays.equals(e.bytes(), anchor.data())), "Changed controlled scan anchor");
                }
                var from = scan.map(io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry::position).orElse(checkpoint.replayFrom());
                var selected = evidence.entries().stream().filter(e -> {
                    int c = evidence.order().compare(e.position(), from);
                    return c > 0 || c == 0 && scan.isEmpty() && checkpoint.replayInclusive();
                }).map(e -> new io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry(e.position(),
                        new EntryHeaderDecoder().decode(e.bytes(), 0), e.bytes())).toList();
                checkQualified(config.getConfig());
                return new As400OperatorCutReader.Cut(checkpoint, evidence.order(), evidence.entries().get(evidence.entries().size() - 1).position(),
                        selected, List.of(), java.util.Optional.of(history), true);
            }

            @Override
            public void requestStop() {
                closed.set(true);
                stopNativeSource();
                if (nativeReader != null) {
                    nativeReader.requestStop();
                }
                write(generation + ".stop-requested.json", Map.of());
            }

            @Override
            public void close() throws Exception {
                closed.set(true);
                closeNativeSource();
                write(generation + ".drained.json", Map.of());
            }
        }

    }
}
