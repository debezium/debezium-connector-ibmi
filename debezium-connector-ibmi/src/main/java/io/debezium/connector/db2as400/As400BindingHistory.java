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
import java.util.function.Consumer;
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
        return recordBaseline(journal, baseline, receiverIdentity, definitions, policy, Optional.empty(), Optional.empty());
    }

    /** Retain the complete discovery floor across a crash before activation. This is not output F/W. */
    record Discovery(JournalPosition floor, String receiverIdentity) {
    }

    As400Checkpoint recordPendingBaseline(String journal, JournalPosition baseline, String receiverIdentity,
                                          List<As400BindingDefinition> definitions, As400CaptureFingerprint.Policy policy, Discovery discovery)
            throws Exception {
        return recordBaseline(journal, baseline, receiverIdentity, definitions, policy, Optional.empty(), Optional.of(discovery));
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
        return recordBaseline(journal, baseline, receiverIdentity, definitions, policy, Optional.of(initialization), Optional.empty());
    }

    private As400Checkpoint recordBaseline(String journal, JournalPosition baseline, String receiverIdentity,
                                           List<As400BindingDefinition> definitions, As400CaptureFingerprint.Policy policy,
                                           Optional<Initialization> initialization, Optional<Discovery> discovery)
            throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        var manifest = manifest(journal, baseline, receiverIdentity, definitions, policy, initialization, discovery);
        storeBaselineIfAbsent(manifest);
        var initial = As400Checkpoint.afterVerifiedBoundary(journal, manifest.fingerprint(), baseline, receiverIdentity);
        recover(initial.toOffset(), journal, policy);
        return initial;
    }

    private void storeBaselineIfAbsent(Manifest manifest) throws Exception {
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
    }

    /** Immutable candidate facts only. Connect's acknowledged main offset selects their authority. */
    Pending recordOnlineCandidate(String journal, JournalPosition origin, String receiverIdentity, List<As400BindingDefinition> definitions,
                                  As400CaptureFingerprint.Policy policy, Discovery discovery, Map<String, ?> legacy)
            throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        As400LegacyHandoff.position(As400LegacyHandoff.strings(legacy));
        require(!legacy.containsKey(As400LegacyHandoff.VERSION) && !legacy.containsKey(As400LegacyHandoff.CHECKPOINT),
                "Cannot propose new history after the handoff barrier");
        storeBaselineIfAbsent(manifest(journal, origin, receiverIdentity, definitions, policy, Optional.empty(), Optional.of(discovery), true));
        return pendingOnlineCandidate(legacy, policy).orElseThrow();
    }

    Optional<Pending> pendingOnlineCandidate(Map<String, ?> legacy, As400CaptureFingerprint.Policy policy) throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        var durable = As400LegacyHandoff.strings(legacy);
        As400LegacyHandoff.position(durable);
        boolean barrier = durable.containsKey(As400LegacyHandoff.VERSION);
        require(barrier == durable.containsKey(As400LegacyHandoff.CHECKPOINT)
                && (!barrier || "1".equals(durable.get(As400LegacyHandoff.VERSION))), "Incomplete legacy activation barrier");
        var recovered = read(new Validation(policy, candidate -> {
        }, true, null), true);
        require(!barrier || recovered.isPresent(), "An acknowledged legacy barrier requires its binding history");
        return recovered.map(result -> new Pending(result.journalIdentity(), result.manifest.baseline(),
                result.manifest.position.get("receiver.identity"), result.fingerprint(), result.definitions(),
                discoveryFrom(result.manifest.position), Optional.empty()));
    }

    Recovered recoverOnlineCandidate(Map<String, ?> offset, String journal, As400CaptureFingerprint.Policy policy) throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        require(offset != null && offset.get("capture_fingerprint") instanceof String, "Durable online F/W required");
        var checkpoint = As400Checkpoint.fromOffset(offset, journal, (String) offset.get("capture_fingerprint"));
        return read(new Validation(policy, candidate -> candidate.verify(checkpoint), true, checkpoint.captureFingerprint()), false).orElseThrow();
    }

    /** Append activation only after independent durable-offset readback of the exact legacy barrier. */
    Recovered recordLegacyHandoff(As400LegacyHandoff handoff, As400CaptureFingerprint.Policy policy, As400JournalOrder order,
                                  Supplier<Map<String, ?>> durable)
            throws Exception {
        require(handoff.barrier().equals(As400LegacyHandoff.strings(durable.get())), "Legacy barrier is not durably acknowledged");
        handoff.verifyOrder(order);
        var recovered = recover(handoff.initial().toOffset(), handoff.initial().journalIdentity(), policy);
        require(recovered.initialization().isEmpty(), "An initialized snapshot cannot be replaced by a handoff");
        recovered.verify(handoff.initial(), order);
        if (recovered.handoff().isPresent()) {
            require(recovered.handoff().orElseThrow().equals(handoff), "Conflicting legacy activation");
        }
        else {
            SchemaHistory writer = factory.get();
            requireQualified(writer);
            try {
                writer.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
                require(writer.storageExists() && writer.exists(), "Pending binding history disappeared before activation");
                writer.start();
                require(handoff.barrier().equals(As400LegacyHandoff.strings(durable.get())), "Legacy barrier changed before activation");
                writer.record(source, handoff.fields(), null, null, null, new TableChanges(), null);
            }
            finally {
                writer.stop();
            }
        }
        var activated = recover(handoff.initial().toOffset(), handoff.initial().journalIdentity(), policy);
        require(activated.handoff().orElseThrow().equals(handoff)
                && handoff.barrier().equals(As400LegacyHandoff.strings(durable.get())), "Uncertain handoff activation acknowledgement");
        return activated;
    }

    Recovered recover(Map<String, ?> offset, String journal, As400CaptureFingerprint.Policy policy) throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        require(offset != null && "2".equals(offset.get(As400Checkpoint.VERSION)), "Explicit durable F/W required before history recovery");
        Object expected = offset.get("capture_fingerprint");
        // Parse the full checkpoint before opening history, even when the manifest is absent.
        require(expected instanceof String, "Missing binding fingerprint");
        var checkpoint = As400Checkpoint.fromOffset(offset, journal, (String) expected);
        return read(new Validation(policy, candidate -> candidate.verify(checkpoint)), false).orElseThrow();
    }

    /**
     * Source facts left by an interrupted handoff, not permission to publish or a
     * completed-output checkpoint. The caller must revalidate native history and
     * independently acknowledge the barrier before activating these bindings.
     */
    record Pending(String journal, JournalPosition origin, String receiverIdentity, String fingerprint,
            List<As400BindingDefinition> definitions, Optional<Discovery> discovery, Optional<As400LegacyHandoff> activation) {
        Pending {
            definitions = List.copyOf(definitions);
        }
    }

    Optional<Pending> pending(Map<String, ?> legacy, As400CaptureFingerprint.Policy policy) throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        var durable = As400LegacyHandoff.strings(legacy);
        As400LegacyHandoff.position(durable);
        boolean barrier = durable.containsKey(As400LegacyHandoff.VERSION);
        require(barrier == durable.containsKey(As400LegacyHandoff.CHECKPOINT)
                && (!barrier || "1".equals(durable.get(As400LegacyHandoff.VERSION))), "Incomplete legacy activation barrier");
        var recovered = read(new Validation(policy, candidate -> require(initializationFrom(candidate.position).isEmpty(),
                "A snapshot baseline cannot be resumed as pending legacy bindings")), true);
        if (recovered.isEmpty()) {
            require(!barrier, "An acknowledged legacy barrier requires its binding history");
            return Optional.empty();
        }
        var result = recovered.orElseThrow();
        result.handoff.ifPresent(value -> require(value.barrier().equals(durable), "Activated history requires its exact durable legacy barrier"));
        // A crash may leave a committed barrier without the activation record.
        // Its digest must be checked against the reconstructed candidate before
        // recordLegacyHandoff can append activation; these facts alone admit nothing.
        var fields = result.manifest.position;
        return Optional.of(new Pending(result.journalIdentity(), result.manifest.baseline(), fields.get("receiver.identity"),
                result.fingerprint(), result.definitions(), discoveryFrom(fields), result.handoff));
    }

    private Optional<Recovered> read(Validation listener, boolean allowAbsent) throws Exception {
        SchemaHistory reader = factory.get();
        requireQualified(reader);
        try {
            reader.configure(config, HistoryRecordComparator.INSTANCE, listener, false);
            if (listener.online && reader instanceof As400KafkaBindingHistory kafka) {
                kafka.allowCandidateAppends();
            }
            if (!reader.storageExists() || !reader.exists()) {
                require(allowAbsent, "Required binding history is absent");
                return Optional.empty();
            }
            reader.start();
            // Validate every observed record, including ones the base comparator
            // would skip. Online selection can choose among canonical immutable
            // proposals; it cannot hide malformed, foreign-source or mixed records.
            reader.recover(source, Map.of(), new Tables(), null);
        }
        finally {
            reader.stop();
        }
        require(listener.manifest != null, "Required binding history is empty");
        return Optional.of(new Recovered(listener.manifest, Optional.ofNullable(listener.handoff)));
    }

    final class Recovered {
        private final Manifest manifest;
        private final Optional<As400LegacyHandoff> handoff;

        private Recovered(Manifest manifest, Optional<As400LegacyHandoff> handoff) {
            this.manifest = manifest;
            this.handoff = handoff;
        }

        Optional<As400LegacyHandoff> handoff() {
            return handoff;
        }

        boolean emptyOnlineOrigin(As400Checkpoint checkpoint) {
            manifest.verify(checkpoint);
            return "5".equals(manifest.position.get("version")) && !checkpoint.replayInclusive()
                    && checkpoint.replayFrom().equals(checkpoint.completedThrough()) && checkpoint.replayFrom().equals(manifest.baseline());
        }

        void requireStartupOrigin(Map<String, ?> durable, boolean allowHandoff) {
            if ("5".equals(manifest.position.get("version"))) {
                require(allowHandoff && handoff.isEmpty(), "Online candidate recovery requires explicit Connect-offset admission");
                manifest.verify(As400Checkpoint.fromOffset(durable, journalIdentity(), fingerprint()));
                return;
            }
            if (initialization().isPresent()) {
                require(handoff.isEmpty(), "Conflicting snapshot and handoff origins");
                return;
            }
            require(allowHandoff && handoff.isPresent(), "Recovery requires acknowledged initialization or explicitly admitted legacy handoff");
            handoff.orElseThrow().verifyResume(As400Checkpoint.fromOffset(durable, journalIdentity(), fingerprint()));
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

        JournalPosition baseline() {
            return new JournalPosition(new BigInteger(position.get("sequence")), new JournalReceiver(position.get("receiver"), position.get("library")));
        }

        void verify(As400Checkpoint checkpoint) {
            require(checkpoint.journalIdentity().equals(position.get("journal")) && checkpoint.captureFingerprint().equals(fingerprint()),
                    "History does not match durable checkpoint");
            var baseline = baseline();
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
                                     As400CaptureFingerprint.Policy policy, Optional<Initialization> initialization, Optional<Discovery> discovery)
            throws Exception {
        return manifest(journal, baseline, receiverIdentity, input, policy, initialization, discovery, false);
    }

    private static Manifest manifest(String journal, JournalPosition baseline, String receiverIdentity, List<As400BindingDefinition> input,
                                     As400CaptureFingerprint.Policy policy, Optional<Initialization> initialization, Optional<Discovery> discovery,
                                     boolean online)
            throws Exception {
        require(!online || initialization.isEmpty() && discovery.isPresent(), "Online candidate requires its native discovery origin");
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
        discovery.ifPresent(value -> {
            require(initialization.isEmpty(), "Snapshot and pending handoff origins are distinct");
            As400Checkpoint.afterVerifiedBoundary(journal, "v1:" + "0".repeat(64), value.floor(), value.receiverIdentity());
            require(value.floor().offset().compareTo(baseline.offset()) <= 0
                    && (!value.floor().offset().equals(baseline.offset()) || value.floor().equals(baseline)), "Discovery floor follows or aliases the schema origin");
            require(!value.floor().receiver().equals(baseline.receiver()) || value.receiverIdentity().equals(receiverIdentity),
                    "Discovery and schema origin receiver identities differ");
            fields.put("version", online ? "5" : "4");
            fields.put("discovery.sequence", value.floor().offset().toString());
            fields.put("discovery.receiver", value.floor().receiver().name());
            fields.put("discovery.library", value.floor().receiver().library());
            fields.put("discovery.receiver.identity", value.receiverIdentity());
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
        private final Consumer<Manifest> validateOrigin;
        private final boolean online;
        private final String selectedFingerprint;
        private Manifest manifest;
        private As400LegacyHandoff handoff;
        private int records;

        Validation(As400CaptureFingerprint.Policy policy, Consumer<Manifest> validateOrigin) {
            this(policy, validateOrigin, false, null);
        }

        Validation(As400CaptureFingerprint.Policy policy, Consumer<Manifest> validateOrigin, boolean online, String selectedFingerprint) {
            this.policy = policy;
            this.validateOrigin = validateOrigin;
            this.online = online;
            this.selectedFingerprint = selectedFingerprint;
        }

        @Override
        public void onChangeFromHistory(HistoryRecord record) {
            try {
                require(++records <= 1024, "Binding history exceeds record bound");
                Document doc = record.document();
                require(strings(doc.getDocument("source")).equals(source) && !doc.has("ddl"), "Foreign or DDL history record");
                require(DocumentWriter.defaultWriter().write(doc).length() <= MAX_MANIFEST_CHARS + 4096, "Oversized binding history record");
                Map<String, String> position = strings(doc.getDocument("position"));
                require(online == "5".equals(position.get("version")), "Online candidate history and strict activation history cannot be mixed");
                var array = doc.getArray("tableChanges");
                if ("3".equals(position.get("version"))) {
                    require(manifest != null && initializationFrom(manifest.position).isEmpty()
                            && array != null && array.size() == 0, "Handoff requires preceding immutable pending bindings");
                    var activation = As400LegacyHandoff.restore(position);
                    manifest.verify(activation.initial());
                    require(handoff == null || handoff.equals(activation), "Conflicting legacy activations");
                    handoff = activation;
                    return;
                }
                require(handoff == null, "Binding record follows activation");
                Optional<Initialization> initialization = initializationFrom(position);
                require(array != null && array.size() > 0 && array.size() <= MAX_TABLES, "Incomplete binding history record");
                List<As400BindingDefinition> definitions = new ArrayList<>();
                for (var change : new JsonTableChangeSerializer().deserialize(array, false)) {
                    require(change.getType() == TableChanges.TableChangeType.CREATE && change.getPreviousId() == null, "Binding history is immutable");
                    definitions.add(As400BindingDefinition.restore(change.getTable()));
                }
                require(new JsonTableChangeSerializer().serialize(changes(definitions)).equals(array), "Noncanonical or lossy table history");
                var candidate = manifest(position.get("journal"),
                        new JournalPosition(new BigInteger(position.get("sequence")), new JournalReceiver(position.get("receiver"), position.get("library"))),
                        position.get("receiver.identity"), definitions, policy, initialization, discoveryFrom(position), online);
                require(candidate.position.equals(position), online ? "Changed, incomplete or noncanonical online candidate history"
                        : "Changed, incomplete or noncanonical binding history. "
                                + "Pre-fix configured D/CG fingerprints that included converter credentials cannot be migrated automatically; "
                                + "preserve this history and offsets and initialize a new empty generation under a new operator window");
                if (online) {
                    // Every observed record remains canonical and policy-bound.
                    // Before publication choose the first immutable candidate;
                    // after publication only the fenced Connect offset selects it.
                    if (selectedFingerprint == null && manifest != null
                            || selectedFingerprint != null && !selectedFingerprint.equals(candidate.fingerprint())) {
                        return;
                    }
                }
                validateOrigin.accept(candidate);
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
        require(Set.of("1", "2", "4", "5").contains(position.get("version")), "Unsupported binding history version");
        return "2".equals(position.get("version"))
                ? Optional.of(new Initialization(position.get("initialization.epoch"), position.get("initialization.baseline")))
                : Optional.empty();
    }

    private static Optional<Discovery> discoveryFrom(Map<String, String> position) {
        return Set.of("4", "5").contains(position.get("version")) ? Optional.of(new Discovery(
                new JournalPosition(new BigInteger(position.get("discovery.sequence")),
                        new JournalReceiver(position.get("discovery.receiver"), position.get("discovery.library"))),
                position.get("discovery.receiver.identity"))) : Optional.empty();
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
