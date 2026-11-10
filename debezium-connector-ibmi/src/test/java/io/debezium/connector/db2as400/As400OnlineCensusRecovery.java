/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.Consumer;

import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.FixtureContext;
import io.debezium.connector.db2as400.As400ReplayCoordinator.RecoveryException;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.schema.AbstractTopicNamingStrategy;

/** Bounded offline replay experiment. Acknowledgements are modeled; no Kafka or IBM i access. */
final class As400OnlineCensusRecovery {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final As400Partition PARTITION = new As400Partition("offline");
    private static final int MAX_RECORDS = 40_000;
    private static final int MAX_SAMPLES = 12;
    private final List<Entry> entries;
    private final FixtureContext context;
    private final String storedBindings;
    private final boolean transactionMetadata;
    private final long deadline = System.nanoTime() + 60_000_000_000L;

    private record Delivered(SourceRecord record, JournalPosition commit) {
    }

    private record HeaderValue(String key, Object schema, Object value) {
    }

    private record Content(String topic, Integer kafkaPartition, Map<String, ?> partition, Object keySchema, Object key,
            Object valueSchema, Object value, Long timestamp, List<HeaderValue> headers) {
    }

    private As400OnlineCensusRecovery(List<Entry> entries, List<As400BindingDefinition> definitions, JournalPosition baseline, FixtureContext suppliedContext,
                                      boolean transactionMetadata)
            throws Exception {
        require(!entries.isEmpty() && entries.size() <= As400OnlineCensusCapture.MAX_CAPTURE_ENTRIES, "Bounded nonempty recovery fixture required");
        this.entries = List.copyOf(entries);
        this.transactionMetadata = transactionMetadata;
        context = suppliedContext == null ? As400OnlineCensusReplay.fixtureContext(entries, baseline) : suppliedContext;
        require(context.baseline().completedThrough().equals(baseline), "Recovery baseline differs from supplied native context");
        var changes = new TableChanges();
        definitions.forEach(definition -> changes.create(definition.storedTable()));
        Document document = Document.create();
        document.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
        storedBindings = DocumentWriter.defaultWriter().write(document);
    }

    static Map<String, Object> verify(List<Entry> entries, List<As400BindingDefinition> definitions, JournalPosition baseline) throws Exception {
        return new As400OnlineCensusRecovery(entries, definitions, baseline, null, true).verify(records -> {
        });
    }

    static Map<String, Object> verify(List<Entry> entries, List<As400BindingDefinition> definitions, FixtureContext context,
                                      Consumer<List<SourceRecord>> referenceCheck)
            throws Exception {
        return verify(entries, definitions, context, true, referenceCheck);
    }

    static Map<String, Object> verify(List<Entry> entries, List<As400BindingDefinition> definitions, FixtureContext context,
                                      boolean transactionMetadata, Consumer<List<SourceRecord>> referenceCheck)
            throws Exception {
        return new As400OnlineCensusRecovery(entries, definitions, context.baseline().completedThrough(), context, transactionMetadata).verify(referenceCheck);
    }

    private Map<String, Object> verify(Consumer<List<SourceRecord>> referenceCheck) throws Exception {
        List<Delivered> expected;
        Map<String, ?> finalOffset;
        try (var reference = session(initial(), -1, false)) {
            reference.feed(entries);
            reference.coordinator.verifyEndOfAvailableJournal();
            expected = List.copyOf(reference.delivered);
            finalOffset = reference.checkpointReceipt();
        }
        referenceCheck.accept(expected.stream().map(Delivered::record).toList());
        var cases = new ArrayList<Map<String, Object>>();
        for (int cut : samples(entries.size() + 1)) {
            tick();
            List<Delivered> prefix;
            Map<String, ?> acknowledged;
            Map<String, ?> dataOffset;
            JournalPosition scan;
            try (var first = session(initial(), -1, false)) {
                first.feed(entries.subList(0, cut));
                prefix = List.copyOf(first.delivered);
                dataOffset = prefix.isEmpty() ? initial() : roundTrip(prefix.get(prefix.size() - 1).record().sourceOffset());
                scan = first.coordinator.scanPosition().orElse(context.baseline().completedThrough());
                acknowledged = first.checkpointReceipt();
            }
            try (var resumed = session(acknowledged, -1, false)) {
                resumed.resume(entries);
                var combined = new ArrayList<>(prefix);
                combined.addAll(resumed.delivered);
                require(contents(combined).equals(contents(expected)), "Acknowledged restart lost, duplicated or reordered output at cut " + cut);
            }
            try (var resumed = session(dataOffset, -1, false)) {
                resumed.resume(entries);
                require(contents(resumed.delivered).equals(contents(after(expected, checkpoint(dataOffset).completedThrough()))),
                        "Unacknowledged checkpoint did not replay complete batches at cut " + cut);
            }
            cases.add(Map.of("kind", "restart", "raw_entries_before_stop", cut, "R", coordinate(scan), "acknowledged", acknowledged,
                    "last_data_offset", dataOffset, "acknowledgements", "modeled", "status", "passed"));
        }
        for (int cut : samples(expected.size())) {
            for (boolean throwing : List.of(false, true)) {
                tick();
                Map<String, ?> saved;
                try (var interrupted = session(initial(), cut, throwing)) {
                    boolean rejected = false;
                    try {
                        interrupted.feed(entries);
                    }
                    catch (RecoveryException expectedFailure) {
                        rejected = true;
                    }
                    require(rejected && interrupted.injected, "Delivery fault did not reach the actual coordinator sink");
                    require(interrupted.delivered.size() == cut, "Delivery fault escaped an unexpected record count");
                    require(interrupted.coordinator.checkpoint().toOffset().equals(interrupted.batchOffset), "Interrupted output changed F/W");
                    require(context.order().compare(interrupted.coordinator.checkpoint().completedThrough(), expected.get(cut).commit()) < 0,
                            "Interrupted output advanced completed W");
                    saved = interrupted.delivered.isEmpty() ? initial()
                            : roundTrip(interrupted.delivered.get(interrupted.delivered.size() - 1).record().sourceOffset());
                }
                try (var resumed = session(saved, -1, false)) {
                    resumed.resume(entries);
                    require(contents(resumed.delivered).equals(contents(after(expected, checkpoint(saved).completedThrough()))),
                            "Interrupted delivery did not replay complete ordered batches");
                }
                cases.add(Map.of("kind", throwing ? "enqueue-exception" : "enqueue-false", "records_before_failure", cut,
                        "saved", saved, "failed_batch_commit", coordinate(expected.get(cut).commit()), "status", "passed"));
            }
        }
        requireRejected(null, entries, "missing-offset", cases);
        var altered = new HashMap<String, Object>(finalOffset);
        altered.put("capture_fingerprint", "v1:" + "a".repeat(64));
        requireRejected(altered, entries, "changed-fingerprint", cases);
        altered = new HashMap<>(finalOffset);
        altered.put("replay_from.receiver_identity", "replaced");
        altered.put("completed_through.receiver_identity", "replaced");
        requireRejected(altered, entries, "changed-receiver", cases);
        var completed = checkpoint(finalOffset);
        for (String boundary : List.of("F", "W")) {
            JournalPosition missing = boundary.equals("F") ? completed.replayFrom() : completed.completedThrough();
            if (!missing.equals(context.baseline().completedThrough())) {
                requireRejected(finalOffset, entries.stream().filter(e -> !e.position().equals(missing)).toList(), "missing-" + boundary, cases);
            }
        }
        tick();
        return Map.of("status", "verified-offline", "startup_proof", false, "acknowledgements", "modeled; no broker or durable store",
                "reference_records", expected.size(), "raw_entries", entries.size(), "maximum_samples_per_category", MAX_SAMPLES,
                "sampling", "evenly spaced; exhaustive only when candidates <= maximum",
                "comparison_exception", "connector-instance __debezium.context.runId header value; UUID shape checked", "cases", cases);
    }

    private void requireRejected(Map<String, ?> offset, List<Entry> available, String fault, List<Map<String, Object>> cases) throws Exception {
        tick();
        String reason = null;
        var emitted = new ArrayList<Delivered>();
        try (var candidate = new Session(offset, -1, false, emitted)) {
            candidate.resume(available);
        }
        catch (IllegalArgumentException | RecoveryException expected) {
            reason = expected.getMessage();
        }
        String expectedReason = switch (fault) {
            case "missing-offset" -> "Missing or unsupported buffered offset version; legacy migration is not automatic";
            case "changed-fingerprint" -> "Journal incarnation or capture configuration changed";
            case "changed-receiver" -> "Checkpoint receiver was replaced";
            case "missing-F", "missing-W" -> "Required F/W anchor is missing from retained fixture";
            default -> throw new IllegalArgumentException("Unknown offline recovery fault");
        };
        require(expectedReason.equals(reason) && emitted.isEmpty(), "Recovery did not reject for the expected reason with zero output: " + fault);
        tick();
        cases.add(Map.of("kind", fault, "status", "passed", "output_records", emitted.size(), "reason", reason));
    }

    private List<Delivered> after(List<Delivered> records, JournalPosition completed) {
        return records.stream().filter(record -> context.order().compare(record.commit(), completed) > 0).toList();
    }

    private static List<Content> contents(List<Delivered> records) {
        return records.stream().map(delivered -> {
            SourceRecord record = delivered.record();
            var headers = new ArrayList<HeaderValue>();
            record.headers().forEach(header -> {
                Object value = header.value();
                if (header.key().equals("__debezium.context.runId")) {
                    require(value instanceof String, "Missing instance run ID");
                    java.util.UUID.fromString((String) value);
                    value = "<connector-instance>"; // A fresh task intentionally gets a different run ID.
                }
                headers.add(new HeaderValue(header.key(), header.schema(), value));
            });
            return new Content(record.topic(), record.kafkaPartition(), record.sourcePartition(), record.keySchema(), record.key(),
                    record.valueSchema(), record.value(), record.timestamp(), headers);
        }).toList();
    }

    static List<Integer> samples(int count) {
        var selected = new TreeSet<Integer>();
        if (count <= MAX_SAMPLES) {
            for (int i = 0; i < count; i++) {
                selected.add(i);
            }
        }
        else {
            for (int i = 0; i < MAX_SAMPLES; i++) {
                selected.add((int) ((long) i * (count - 1) / (MAX_SAMPLES - 1)));
            }
        }
        return List.copyOf(selected);
    }

    private Session session(Map<String, ?> offset, int failAt, boolean throwing) throws Exception {
        return new Session(offset, failAt, throwing, new ArrayList<>());
    }

    private Map<String, ?> initial() {
        return context.baseline().toOffset();
    }

    private As400Checkpoint checkpoint(Map<String, ?> offset) {
        return As400Checkpoint.fromOffset(offset, context.baseline().journalIdentity(), context.baseline().captureFingerprint());
    }

    private final class Session implements AutoCloseable {
        private final As400ReplayCoordinator coordinator;
        private final Output output;
        private final List<Delivered> delivered;
        private JournalPosition currentCommit;
        private Map<String, ?> batchOffset;
        private boolean publishingCheckpoint;
        private SourceRecord receipt;
        private boolean injected;

        Session(Map<String, ?> offset, int failAt, boolean throwing, List<Delivered> delivered) throws Exception {
            tick();
            checkpoint(offset); // Reject absent/foreign offsets before constructing output services.
            this.delivered = delivered;
            var definitions = new ArrayList<As400BindingDefinition>();
            var document = DocumentReader.defaultReader().read(storedBindings);
            for (var change : new JsonTableChangeSerializer().deserialize(document.getArray("tables"), false)) {
                definitions.add(As400BindingDefinition.restore(change.getTable()));
            }
            var bindings = new HashMap<String, TableBinding>();
            definitions.forEach(
                    definition -> bindings.put(definition.identity().tableId().schema() + "/" + definition.identity().physicalFile(), definition.binding(1024 * 1024)));
            output = new Output(definitions, transactionMetadata);
            try {
                coordinator = new As400ReplayCoordinator(roundTrip(offset), context.baseline().journalIdentity(), context.baseline().captureFingerprint(),
                        context.order(), new As400TransactionBuffer.Limits(128, 40_000, 2048, 64 * 1024 * 1024), 1024 * 1024, 20_000,
                        (file, library) -> bindings.get(library + "/" + file), As400SourceNormalizer.SavepointPolicy.consumingTargets(), PARTITION.getSourcePartition(),
                        record -> {
                            tick();
                            if (publishingCheckpoint) {
                                require(receipt == null, "Multiple checkpoint receipts");
                                receipt = record;
                                return true;
                            }
                            require(delivered.size() < MAX_RECORDS && currentCommit != null, "Recovery output budget or batch attribution failed");
                            require(record.sourceOffset().equals(batchOffset), "Batch record did not retain its frozen F/W offset");
                            if (delivered.size() == failAt) {
                                injected = true;
                                if (throwing) {
                                    throw new IllegalStateException("Injected offline enqueue failure");
                                }
                                return false;
                            }
                            delivered.add(new Delivered(record, currentCommit));
                            return true;
                        }, new As400JournalLifecycle.Limits(20_000, 64 * 1024 * 1024, 128, 2048, 1024, 4096), true);
            }
            catch (Exception error) {
                output.close();
                throw error;
            }
        }

        void feed(List<Entry> source) throws Exception {
            for (Entry entry : source) {
                tick();
                var pending = coordinator.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position());
                if (pending.isPresent()) {
                    coordinator.deliver((batch, records) -> {
                        currentCommit = batch.commitPosition();
                        batchOffset = coordinator.checkpoint().toOffset();
                        output.dispatcher.prepare(batch, records);
                    });
                }
            }
        }

        void resume(List<Entry> available) throws Exception {
            var loaded = coordinator.checkpoint();
            for (JournalPosition anchor : List.of(loaded.replayFrom(), loaded.completedThrough())) {
                require(anchor.equals(context.baseline().completedThrough()) || available.stream().anyMatch(e -> e.position().equals(anchor)),
                        "Required F/W anchor is missing from retained fixture");
            }
            feed(available.stream().filter(entry -> {
                int compared = context.order().compare(entry.position(), loaded.replayFrom());
                return loaded.replayInclusive() ? compared >= 0 : compared > 0;
            }).toList());
            coordinator.verifyEndOfAvailableJournal();
        }

        Map<String, ?> checkpointReceipt() throws Exception {
            receipt = null;
            publishingCheckpoint = true;
            try {
                require(output.dispatcher.publishCheckpoint(coordinator, true) && receipt != null, "Missing checkpoint heartbeat");
                Map<String, ?> offset = roundTrip(receipt.sourceOffset());
                require(offset.equals(coordinator.checkpoint().toOffset()), "Checkpoint receipt differs from F/W");
                return offset; // Caller models acknowledgement only after the complete preceding prefix.
            }
            finally {
                publishingCheckpoint = false;
            }
        }

        @Override
        public void close() {
            coordinator.close();
            output.close();
        }
    }

    static final class Output extends As400ConnectorTask implements AutoCloseable {
        final As400BatchDispatcher dispatcher;

        Output(List<As400BindingDefinition> definitions) {
            this(definitions, true);
        }

        Output(List<As400BindingDefinition> definitions, boolean transactionMetadata) {
            var raw = Configuration.create().with("topic.prefix", "offline").with("database.dbname", "OFFLINE")
                    .with("database.schema", definitions.get(0).schema().id().schema()).with("skipped.operations", "none")
                    .with("heartbeat.interval.ms", "3600000").with("provide.transaction.metadata", transactionMetadata)
                    .with("event.converting.failure.handling.mode", "fail").build();
            var taskContext = preStart(raw);
            var config = (As400ConnectorConfig) taskContext.getConfig();
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);
            var topics = new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
                @Override
                public String dataChangeTopic(TableId id) {
                    return "offline." + id.schema() + "." + id.table();
                }
            };
            var schemas = new As400NativeTableSchemas(definitions, config, topics);
            dispatcher = new As400BatchDispatcher(config, PARTITION, schemas, topics, new DebeziumHeaderProducer(taskContext), () -> 42_000L, 20_000, true);
        }

        @Override
        public void close() {
            dispatcher.close();
        }
    }

    private static Map<String, ?> roundTrip(Map<String, ?> offset) throws Exception {
        return JSON.readValue(JSON.writeValueAsBytes(offset), new TypeReference<Map<String, Object>>() {
        });
    }

    private static Map<String, String> coordinate(JournalPosition position) {
        return Map.of("sequence", position.offset().toString(), "receiver", position.receiver().name(), "library", position.receiver().library());
    }

    private void tick() {
        require(System.nanoTime() < deadline, "Offline recovery validation exceeded its 60-second budget");
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
