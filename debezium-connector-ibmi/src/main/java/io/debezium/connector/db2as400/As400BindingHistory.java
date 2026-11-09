/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/**
 * One immutable schema epoch per dedicated standard history store, for the ordinary
 * operator-retention profile. No DDL evolution, implicit bootstrap, current catalog
 * fallback or protected-startup proof. Recovered by the configured task before journal replay.
 */
final class As400BindingHistory {
    private static final int MAX_TABLES = 1024;
    private static final int MAX_MANIFEST_CHARS = 4 * 1024 * 1024;
    private final Supplier<SchemaHistory> factory;
    private final Configuration config;
    private final Map<String, String> source;

    As400BindingHistory(Supplier<SchemaHistory> factory, Configuration config, Map<String, String> source) {
        this.factory = Objects.requireNonNull(factory);
        this.config = Objects.requireNonNull(config).edit()
                .with(SchemaHistory.SKIP_UNPARSEABLE_DDL_STATEMENTS, false).with(SchemaHistory.INTERNAL_PREFER_DDL, false).build();
        this.source = Map.copyOf(source);
        require(!source.isEmpty(), "Dedicated source partition required");
    }

    /**
     * Explicit initialization only. Caller must establish a consistent baseline and
     * verified native bindings first. A returned checkpoint depends on successful
     * storage AND a fresh backend recovery, never an in-memory cache or scan R.
     */
    As400Checkpoint recordBaseline(String journal, JournalPosition baseline, String receiverIdentity,
                                   List<As400BindingDefinition> definitions, As400CaptureFingerprint.Policy policy)
            throws Exception {
        return recordBaseline(journal, baseline, receiverIdentity, definitions, policy, Optional.empty());
    }

    /** Acknowledged external baseline, stored in the same record as its native binding epoch. */
    record Initialization(String epoch, String baselineReceipt) {
        Initialization {
            for (String value : List.of(epoch, baselineReceipt)) {
                require(value.matches("[A-Za-z0-9_.:-]{1,256}"), "Bounded initialization identifiers required");
            }
        }
    }

    As400Checkpoint recordInitializedBaseline(String journal, JournalPosition baseline, String receiverIdentity,
                                              List<As400BindingDefinition> definitions, As400CaptureFingerprint.Policy policy,
                                              Initialization initialization)
            throws Exception {
        return recordBaseline(journal, baseline, receiverIdentity, definitions, policy, Optional.of(initialization));
    }

    private As400Checkpoint recordBaseline(String journal, JournalPosition baseline, String receiverIdentity,
                                           List<As400BindingDefinition> definitions, As400CaptureFingerprint.Policy policy,
                                           Optional<Initialization> initialization)
            throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        var manifest = manifest(journal, baseline, receiverIdentity, definitions, policy, initialization);
        SchemaHistory writer = factory.get();
        requireQualified(writer);
        try {
            writer.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            if (!writer.storageExists()) {
                writer.initializeStorage();
            }
            writer.start();
            if (!writer.exists()) {
                // No snapshot batch is opened: KafkaSchemaHistory flushes and awaits
                // acknowledgement of this complete record before returning.
                writer.record(source, manifest.position, null, null, null, changes(manifest.definitions), null);
            }
        }
        finally {
            writer.stop();
        }
        var initial = As400Checkpoint.afterVerifiedBoundary(journal, manifest.fingerprint(), baseline, receiverIdentity);
        recover(initial.toOffset(), journal, policy);
        return initial;
    }

    Recovered recover(Map<String, ?> offset, String journal, As400CaptureFingerprint.Policy policy) throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        require(offset != null && "2".equals(offset.get(As400Checkpoint.VERSION)), "Explicit durable F/W required before history recovery");
        Object expected = offset.get("capture_fingerprint");
        // Parse the full checkpoint before opening history, even when the manifest is absent.
        require(expected instanceof String, "Missing binding fingerprint");
        var checkpoint = As400Checkpoint.fromOffset(offset, journal, (String) expected);
        var listener = new Validation(policy, checkpoint);
        SchemaHistory reader = factory.get();
        requireQualified(reader);
        try {
            reader.configure(config, HistoryRecordComparator.INSTANCE, listener, false);
            require(reader.storageExists() && reader.exists(), "Required binding history is absent");
            reader.start();
            // Listener validates EVERY record, including ones the default comparator
            // would skip. No stale/future/foreign record can disappear by filtering.
            reader.recover(source, Map.of(), new Tables(), null);
        }
        finally {
            reader.stop();
        }
        require(listener.manifest != null, "Required binding history is empty");
        return new Recovered(listener.manifest);
    }

    final class Recovered {
        private final Manifest manifest;

        private Recovered(Manifest manifest) {
            this.manifest = manifest;
        }

        String fingerprint() {
            return manifest.fingerprint();
        }

        String journalIdentity() {
            return manifest.position.get("journal");
        }

        List<As400BindingDefinition> definitions() {
            return manifest.definitions;
        }

        Optional<Initialization> initialization() {
            return initializationFrom(manifest.position);
        }

        As400SourceNormalizer.TableResolver resolver(int maximumRowBytes) {
            Map<String, TableBinding> bindings = new TreeMap<>();
            for (var definition : manifest.definitions) {
                var id = definition.identity();
                bindings.put(id.tableId().schema() + "/" + id.physicalFile(), definition.binding(maximumRowBytes));
            }
            return (file, library) -> Objects.requireNonNull(bindings.get(library + "/" + file), "Missing recovered binding");
        }

        void verify(As400Checkpoint checkpoint, As400JournalOrder order) {
            manifest.verify(checkpoint);
            require(order.compare(checkpoint.replayFrom(), checkpoint.completedThrough()) <= 0, "Ordinary F must not follow W");
            require(order.incarnation(checkpoint.replayFrom()).equals(checkpoint.replayReceiverIdentity())
                    && order.incarnation(checkpoint.completedThrough()).equals(checkpoint.completedReceiverIdentity()), "Receiver identity changed");
        }
    }

    private record Manifest(Map<String, String> position, List<As400BindingDefinition> definitions, String encodedTables) {
        String fingerprint() {
            return position.get("fingerprint");
        }

        void verify(As400Checkpoint checkpoint) {
            require(checkpoint.journalIdentity().equals(position.get("journal")) && checkpoint.captureFingerprint().equals(fingerprint()),
                    "History does not match durable checkpoint");
            var baseline = new JournalPosition(new BigInteger(position.get("sequence")), new JournalReceiver(position.get("receiver"), position.get("library")));
            int c = baseline.offset().compareTo(checkpoint.replayFrom().offset());
            // This ordinary profile forbids journal sequence resets. The historical
            // baseline receiver need not be retained once F has moved beyond it.
            require(c <= 0 && (c != 0 || baseline.receiver().equals(checkpoint.replayFrom().receiver())), "Binding epoch starts after F or at an ambiguous receiver");
            if (baseline.receiver().equals(checkpoint.replayFrom().receiver())) {
                require(position.get("receiver.identity").equals(checkpoint.replayReceiverIdentity()), "Binding baseline receiver changed");
            }
        }
    }

    private static Manifest manifest(String journal, JournalPosition baseline, String receiverIdentity, List<As400BindingDefinition> input,
                                     As400CaptureFingerprint.Policy policy, Optional<Initialization> initialization)
            throws Exception {
        // Reuse checkpoint identifier/position validation without inventing a cursor.
        As400Checkpoint.afterVerifiedBoundary(journal, "v1:" + "0".repeat(64), baseline, receiverIdentity);
        require(!input.isEmpty() && input.size() <= MAX_TABLES, "Bounded complete captured table set required");
        var definitions = input.stream().sorted(Comparator.comparing(d -> d.identity().tableId().toDoubleQuotedString())).toList();
        Set<Object> ids = new HashSet<>();
        Set<String> physical = new HashSet<>();
        Set<String> journalIds = new HashSet<>();
        for (var definition : definitions) {
            require(ids.add(definition.identity().tableId())
                    && physical.add(definition.identity().tableId().schema() + "/" + definition.identity().physicalFile()), "Duplicate binding");
            require(journalIds.add(definition.identity().fileJournalId()) && journalIds.add(definition.identity().memberJournalId()),
                    "Captured native journal identities must be distinct");
        }
        var document = Document.create();
        document.setArray("tables", new JsonTableChangeSerializer().serialize(changes(definitions)));
        String encoded = DocumentWriter.defaultWriter().write(document);
        require(encoded.length() <= MAX_MANIFEST_CHARS, "Binding manifest exceeds bound");
        Map<String, String> fields = new TreeMap<>(Map.of("version", "1", "journal", journal, "sequence", baseline.offset().toString(),
                "library", baseline.receiver().library(), "receiver", baseline.receiver().name(), "receiver.identity", receiverIdentity));
        initialization.ifPresent(value -> {
            fields.put("version", "2");
            fields.put("initialization.epoch", value.epoch());
            fields.put("initialization.baseline", value.baselineReceipt());
        });
        Map<String, String> fingerprint = new TreeMap<>(fields);
        fingerprint.put("tables", encoded);
        fingerprint.put("policy", policy.fingerprint(definitions.stream().map(As400BindingDefinition::identity).toList()));
        fields.put("fingerprint", "v1:" + As400CaptureFingerprint.hash("ordinary-binding-history-v1", fingerprint));
        return new Manifest(Map.copyOf(fields), definitions, encoded);
    }

    private static TableChanges changes(List<As400BindingDefinition> definitions) {
        var changes = new TableChanges();
        definitions.forEach(d -> changes.create(d.storedTable()));
        return changes;
    }

    private final class Validation implements SchemaHistoryListener {
        private final As400CaptureFingerprint.Policy policy;
        private final As400Checkpoint checkpoint;
        private Manifest manifest;
        private int records;

        Validation(As400CaptureFingerprint.Policy policy, As400Checkpoint checkpoint) {
            this.policy = policy;
            this.checkpoint = checkpoint;
        }

        @Override
        public void onChangeFromHistory(HistoryRecord record) {
            try {
                require(++records <= 1024, "Binding history exceeds record bound");
                Document doc = record.document();
                require(strings(doc.getDocument("source")).equals(source) && !doc.has("ddl"), "Foreign or DDL history record");
                Map<String, String> position = strings(doc.getDocument("position"));
                Optional<Initialization> initialization = initializationFrom(position);
                var array = doc.getArray("tableChanges");
                require(array != null && array.size() > 0 && array.size() <= MAX_TABLES, "Incomplete binding history record");
                require(DocumentWriter.defaultWriter().write(doc).length() <= MAX_MANIFEST_CHARS + 4096, "Oversized binding history record");
                List<As400BindingDefinition> definitions = new ArrayList<>();
                for (var change : new JsonTableChangeSerializer().deserialize(array, false)) {
                    require(change.getType() == TableChanges.TableChangeType.CREATE && change.getPreviousId() == null, "Binding history is immutable");
                    definitions.add(As400BindingDefinition.restore(change.getTable()));
                }
                require(new JsonTableChangeSerializer().serialize(changes(definitions)).equals(array), "Noncanonical or lossy table history");
                var candidate = manifest(position.get("journal"),
                        new JournalPosition(new BigInteger(position.get("sequence")), new JournalReceiver(position.get("receiver"), position.get("library"))),
                        position.get("receiver.identity"), definitions, policy, initialization);
                require(candidate.position.equals(position), "Changed, incomplete or noncanonical binding history. "
                        + "Pre-fix configured D/CG fingerprints that included converter credentials cannot be migrated automatically; "
                        + "preserve this history and offsets and initialize a new empty generation under a new operator window");
                candidate.verify(checkpoint);
                require(manifest == null || manifest.position.equals(candidate.position) && manifest.encodedTables.equals(candidate.encodedTables),
                        "Conflicting binding epochs");
                manifest = candidate;
            }
            catch (Exception e) {
                throw new IllegalArgumentException("Cannot recover exact binding history", e);
            }
        }

        @Override
        public void started() {
        }

        @Override
        public void stopped() {
        }

        @Override
        public void recoveryStarted() {
        }

        @Override
        public void recoveryStopped() {
        }

        @Override
        public void onChangeApplied(HistoryRecord record) {
        }
    }

    private static Map<String, String> strings(Document document) {
        require(document != null, "Missing history document");
        Map<String, String> result = new TreeMap<>();
        for (var field : document) {
            require(field.getValue().isString(), "Non-string binding identity");
            result.put(field.getName().toString(), field.getValue().asString());
        }
        return result;
    }

    private static Optional<Initialization> initializationFrom(Map<String, String> position) {
        require(Set.of("1", "2").contains(position.get("version")), "Unsupported binding history version");
        return "1".equals(position.get("version")) ? Optional.empty()
                : Optional.of(new Initialization(position.get("initialization.epoch"), position.get("initialization.baseline")));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private static void requireQualified(SchemaHistory history) {
        require(history != null && (!(history instanceof KafkaSchemaHistory) || history instanceof As400KafkaBindingHistory),
                "Kafka binding history requires strict recovery without skipped records");
    }
}
