/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HexFormat;
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
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.storage.file.history.FileSchemaHistory;
import io.debezium.storage.kafka.history.KafkaSchemaHistory;

/**
 * One immutable schema epoch per dedicated standard history store, for the ordinary
 * operator-retention profile. No DDL evolution, implicit bootstrap, current catalog
 * fallback or protected-startup proof. Recovered by the configured task before journal replay.
 */
final class As400BindingHistory {
    static final int MAX_HISTORY_RECORDS = 1024;
    static final int MAX_HISTORY_BYTES = 16 * 1024 * 1024;
    static final int MAX_RECORD_BYTES = 4 * 1024 * 1024;
    private static final int MAX_TABLES = 4096; // Frozen legacy single-manifest format only.
    private static final int MAX_MANIFEST_CHARS = 4 * 1024 * 1024;
    private final Supplier<SchemaHistory> factory;
    private final Configuration config;
    private final Map<String, String> source;

    /** Standard configuration selects storage; binding recovery always uses its strict adapter. */
    static boolean fileStorage(Configuration config) {
        String name = config.getString("schema.history.internal", KafkaSchemaHistory.class.getName());
        require(Set.of(KafkaSchemaHistory.class.getName(), FileSchemaHistory.class.getName()).contains(name),
                "Binding history requires the supported KafkaSchemaHistory or FileSchemaHistory store");
        return name.equals(FileSchemaHistory.class.getName());
    }

    static Supplier<SchemaHistory> storage(Configuration config) {
        return fileStorage(config) ? As400FileBindingHistory::new : As400KafkaBindingHistory::new;
    }

    /** Missing offsets may start a snapshot only in an actually empty generation. */
    void requireEmptyForSnapshot() throws Exception {
        SchemaHistory reader = factory.get();
        requireQualified(reader);
        try {
            reader.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
            if (reader instanceof As400KafkaBindingHistory kafka) {
                kafka.requireEmptyForSnapshot();
            }
            else {
                require(!reader.storageExists() || !reader.exists(),
                        "Missing offsets cannot select automatic migration or a new snapshot with retained binding history");
            }
        }
        finally {
            reader.stop();
        }
    }

    interface BoundedRecovery {
        void allowCandidateAppends();

        void allowPagedCandidates();

        HistoryBudget recoveryBudget();

        default boolean eligibleLegacyRecord() {
            return true;
        }

        default void appendBatch(Runnable records) {
            records.run();
        }
    }

    static final class CandidateHistoryFullException extends IllegalStateException {
        CandidateHistoryFullException() {
            super("Online binding proposal capacity exhausted; preserve history and offsets and continue legacy capture");
        }
    }

    /** Legacy immutable-prefix budget, or per-record bound for streamed paged history. */
    static final class HistoryBudget {
        private final boolean streaming;
        private int records;
        private long bytes;
        private boolean exhausted;

        HistoryBudget() {
            this(false);
        }

        HistoryBudget(boolean streaming) {
            this.streaming = streaming;
        }

        boolean accept(int size, boolean online) {
            if (streaming) {
                require(size > 0 && size <= MAX_RECORD_BYTES, "Oversized binding history record");
                return true;
            }
            if (online && (exhausted || records == MAX_HISTORY_RECORDS)) {
                exhausted = true;
                return false;
            }
            require(size > 0 && size <= MAX_RECORD_BYTES, "Oversized binding history record");
            if (records >= MAX_HISTORY_RECORDS || bytes + size > MAX_HISTORY_BYTES) {
                require(online, "Binding history exceeds recovery bounds");
                exhausted = true;
                return false;
            }
            records++;
            bytes += size;
            exhausted = records == MAX_HISTORY_RECORDS || bytes == MAX_HISTORY_BYTES;
            return true;
        }

        void requireAppend(int size) {
            if (streaming) {
                require(size > 0 && size <= MAX_RECORD_BYTES, "Oversized binding history record");
                return;
            }
            if (exhausted || records >= MAX_HISTORY_RECORDS || size <= 0 || size > MAX_RECORD_BYTES || bytes + size > MAX_HISTORY_BYTES) {
                throw new CandidateHistoryFullException();
            }
        }

        void include(HistoryBudget wire) {
            records = Math.max(records, wire.records);
            bytes = Math.max(bytes, wire.bytes);
            exhausted |= wire.exhausted;
        }

        boolean full() {
            return exhausted;
        }
    }

    private static int recordBytes(HistoryRecord record) throws Exception {
        return DocumentWriter.defaultWriter().write(record.document()).getBytes(StandardCharsets.UTF_8).length;
    }

    private HistoryRecord record(Manifest manifest) {
        return new HistoryRecord(source, manifest.position, null, null, null, new TableChanges(), null);
    }

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
        new HistoryBudget().accept(recordBytes(record(manifest)), false);
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
                writer.record(source, manifest.position, null, null, null, new TableChanges(), null);
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
        As400LegacyHandoff.startingPosition(As400LegacyHandoff.strings(legacy));
        require(!legacy.containsKey(As400LegacyHandoff.VERSION) && !legacy.containsKey(As400LegacyHandoff.CHECKPOINT),
                "Cannot propose new history after the handoff barrier");
        var proposal = pagedManifest(journal, origin, receiverIdentity, definitions, policy, discovery, "8");
        var observed = new Validation(policy, candidate -> {
        }, true, null);
        var current = read(observed, true, false);
        if (current.isEmpty() || !current.orElseThrow().fingerprint().equals(proposal.fingerprint())) {
            SchemaHistory writer = factory.get();
            requireQualified(writer);
            try {
                writer.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
                if (writer instanceof BoundedRecovery bounded) {
                    bounded.allowPagedCandidates();
                }
                if (!writer.storageExists()) {
                    writer.initializeStorage();
                }
                writer.start();
                // A proposal grants no publication authority. An acknowledged
                // source offset selects it; later stale appends cannot win.
                Runnable append = () -> {
                    for (int ordinal = 0; ordinal < proposal.definitions.size(); ordinal++) {
                        checkInterrupted();
                        try {
                            var fields = Map.of("version", proposal.position.get("version") + ".table", "fingerprint", proposal.fingerprint(), "ordinal",
                                    Integer.toString(ordinal),
                                    "bindings", As400BindingEncoding.encode(List.of(proposal.definitions.get(ordinal)), false));
                            requireRecordSize(fields);
                            writer.record(source, fields, null, null, null, new TableChanges(), null);
                        }
                        catch (Exception failure) {
                            throw new IllegalArgumentException("Cannot append binding page", failure);
                        }
                    }
                    // This final descriptor is the only selectable record. A
                    // partial page sequence grants no publication authority.
                    writer.record(source, proposal.position, null, null, null, new TableChanges(), null);
                };
                if (writer instanceof BoundedRecovery bounded) {
                    bounded.appendBatch(append);
                }
                else {
                    append.run();
                }
            }
            finally {
                writer.stop();
            }
        }
        read(new Validation(policy, candidate -> {
        }, true, proposal.fingerprint()), false).orElseThrow();
        // The complete readback above verified every canonical table and the
        // exact descriptor. Reuse the caller's immutable definitions instead of
        // retaining a second copy of the same catalog after initialization.
        return pending(new Recovered(proposal, Optional.empty()));
    }

    Optional<Pending> pendingOnlineCandidate(Map<String, ?> legacy, As400CaptureFingerprint.Policy policy) throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        var durable = As400LegacyHandoff.strings(legacy);
        As400LegacyHandoff.startingPosition(durable);
        boolean barrier = durable.containsKey(As400LegacyHandoff.VERSION);
        require(barrier == durable.containsKey(As400LegacyHandoff.CHECKPOINT)
                && (!barrier || "1".equals(durable.get(As400LegacyHandoff.VERSION))), "Incomplete legacy activation barrier");
        var validation = new Validation(policy, candidate -> {
        }, true, barrier ? durable.getOrDefault(As400LegacyHandoff.FINGERPRINT, "") : null);
        validation.schemaRecords = fields -> {
            throw new IllegalArgumentException("Observed schema history requires its durable native checkpoint; a legacy offset cannot replace it");
        };
        var recovered = read(validation, true);
        require(!barrier || recovered.isPresent(), "An acknowledged legacy barrier requires its binding history");
        return recovered.map(this::pending);
    }

    private Pending pending(Recovered result) {
        return new Pending(result.journalIdentity(), result.manifest.baseline(), result.manifest.position.get("receiver.identity"),
                result.fingerprint(), result.definitions(), discoveryFrom(result.manifest.position), Optional.empty());
    }

    Recovered recoverOnlineCandidate(Map<String, ?> offset, String journal, As400CaptureFingerprint.Policy policy) throws Exception {
        require(source.equals(policy.sourcePartition()), "History and output partitions differ");
        require(offset != null && offset.get("capture_fingerprint") instanceof String, "Durable online F/W required");
        var checkpoint = As400Checkpoint.fromOffset(offset, journal, (String) offset.get("capture_fingerprint"));
        var recovered = read(new Validation(policy, candidate -> candidate.verify(checkpoint), true, checkpoint.captureFingerprint()), false).orElseThrow();
        recovered.recoverSchemas(checkpoint, policy);
        return recovered;
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
        require(offset != null && offset.containsKey(As400Checkpoint.VERSION), "Explicit durable F/W required before history recovery");
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
        return read(listener, allowAbsent, true);
    }

    private Optional<Recovered> read(Validation listener, boolean allowAbsent, boolean materialize) throws Exception {
        SchemaHistory reader = factory.get();
        requireQualified(reader);
        try {
            reader.configure(config, HistoryRecordComparator.INSTANCE, listener, false);
            if (reader instanceof BoundedRecovery bounded) {
                listener.eligibleLegacyRecord = bounded::eligibleLegacyRecord;
            }
            if (listener.online && reader instanceof BoundedRecovery bounded) {
                if (listener.paged) {
                    bounded.allowPagedCandidates();
                }
                else {
                    bounded.allowCandidateAppends();
                }
            }
            if (!reader.storageExists() || !reader.exists()) {
                require(allowAbsent, "Required binding history is absent");
                return Optional.empty();
            }
            reader.start();
            // Validate every eligible record, including ones the base comparator
            // would skip. Online selection cannot hide malformed, foreign-source
            // or mixed records within its bounded immutable proposal prefix.
            reader.recover(source, Map.of(), new Tables(), null);
            if (reader instanceof BoundedRecovery bounded) {
                listener.budget.include(bounded.recoveryBudget());
            }
        }
        finally {
            reader.stop();
        }
        if (listener.online && !listener.paged && listener.manifest == null && listener.budget.exhausted) {
            throw new CandidateHistoryFullException();
        }
        if (listener.paged && listener.manifest == null && allowAbsent) {
            return Optional.empty(); // Only incomplete, non-authoritative page sequences remain.
        }
        require(listener.manifest != null, "Required binding history is empty");
        if (materialize && Set.of("6", "7", "8").contains(listener.manifest.position.get("version")) && !listener.collectPages) {
            // Select a completed descriptor in one bounded-memory pass, then
            // recover only its pages. Concurrent proposals cannot replace the
            // selected fingerprint between the two immutable-log reads.
            return read(new Validation(listener.policy, listener.validateOrigin, true, listener.manifest.fingerprint(), true), false);
        }
        var recovered = new Recovered(listener.manifest, Optional.ofNullable(listener.handoff));
        recovered.recordedSchemas = listener.recordedSchemas;
        return Optional.of(recovered);
    }

    final class Recovered {
        private final Manifest manifest;
        private final Optional<As400LegacyHandoff> handoff;
        private List<As400BindingDefinition> recoveredDefinitions;
        private As400SchemaHistory schemaHistory;
        private boolean recordedSchemas;
        private boolean schemaFloorVerified;

        private Recovered(Manifest manifest, Optional<As400LegacyHandoff> handoff) {
            this.manifest = manifest;
            this.handoff = handoff;
            recoveredDefinitions = manifest.definitions;
        }

        Optional<As400LegacyHandoff> handoff() {
            return handoff;
        }

        boolean emptyOnlineOrigin(As400Checkpoint checkpoint) {
            manifest.verify(checkpoint);
            return Set.of("5", "6", "7", "8").contains(manifest.position.get("version")) && !checkpoint.replayInclusive()
                    && checkpoint.replayFrom().equals(checkpoint.completedThrough()) && checkpoint.replayFrom().equals(manifest.baseline());
        }

        void requireStartupOrigin(Map<String, ?> durable, boolean allowHandoff) {
            if (Set.of("5", "6", "7", "8").contains(manifest.position.get("version"))) {
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
            return recoveredDefinitions;
        }

        private void recoverSchemas(As400Checkpoint checkpoint, As400CaptureFingerprint.Policy policy) throws Exception {
            if (!Set.of("7", "8").contains(manifest.position.get("version"))) {
                require(checkpoint.schemaState().equals(As400SchemaState.EMPTY), "Older binding history cannot select schema transitions");
                return;
            }
            schemaHistory = new As400SchemaHistory(fingerprint(), manifest.definitions, new As400SchemaHistory.Store() {
                @Override
                public void read(Consumer<Map<String, String>> records) throws Exception {
                    var validation = new Validation(policy, candidate -> candidate.verify(checkpoint), true, fingerprint());
                    validation.schemaRecords = records;
                    As400BindingHistory.this.read(validation, false, false);
                }

                @Override
                public void append(Map<String, String> fields) throws Exception {
                    requireRecordSize(fields);
                    SchemaHistory writer = factory.get();
                    requireQualified(writer);
                    try {
                        writer.configure(config, HistoryRecordComparator.INSTANCE, SchemaHistoryListener.NOOP, false);
                        require(writer.storageExists() && writer.exists(), "Schema append requires retained binding history");
                        if (writer instanceof BoundedRecovery bounded) {
                            bounded.allowPagedCandidates();
                        }
                        writer.start();
                        writer.record(source, fields, null, null, null, new TableChanges(), null);
                    }
                    finally {
                        writer.stop();
                    }
                }
            });
            if (recordedSchemas || !checkpoint.schemaState().equals(As400SchemaState.EMPTY)) {
                recoveredDefinitions = schemaHistory.recover(checkpoint.schemaState());
            }
        }

        As400SchemaHistory schemaHistory() {
            return schemaHistory;
        }

        Optional<Initialization> initialization() {
            return initializationFrom(manifest.position);
        }

        As400SourceNormalizer.TableResolver resolver(int maximumRowBytes) {
            Map<String, TableBinding> bindings = new TreeMap<>();
            for (var definition : recoveredDefinitions) {
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
            if (recordedSchemas && !schemaFloorVerified) {
                try {
                    schemaHistory.verifyFloor(checkpoint, order);
                    schemaFloorVerified = true;
                }
                catch (Exception failure) {
                    throw new IllegalArgumentException("Replay floor has inconsistent schema history", failure);
                }
            }
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
        return manifest(journal, baseline, receiverIdentity, input, policy, initialization, discovery, online, false);
    }

    private static Manifest manifest(String journal, JournalPosition baseline, String receiverIdentity, List<As400BindingDefinition> input,
                                     As400CaptureFingerprint.Policy policy, Optional<Initialization> initialization, Optional<Discovery> discovery,
                                     boolean online, boolean legacyEncoding)
            throws Exception {
        require(!online || initialization.isEmpty() && discovery.isPresent(), "Online candidate requires its native discovery origin");
        // Reuse checkpoint identifier/position validation without inventing a cursor.
        As400Checkpoint.afterVerifiedBoundary(journal, "v1:" + "0".repeat(64), baseline, receiverIdentity);
        require(!input.isEmpty() && input.size() <= MAX_TABLES, "Bounded complete captured table set required");
        var definitions = orderedDefinitions(input);
        String encoded = As400BindingEncoding.encode(definitions, legacyEncoding);
        require(encoded.length() <= MAX_MANIFEST_CHARS, "Binding manifest exceeds bound");
        Map<String, String> fields = manifestFields(journal, baseline, receiverIdentity, initialization, discovery, online);
        if (!legacyEncoding) {
            fields.put("binding.encoding", As400BindingEncoding.VERSION);
            fields.put("bindings", encoded);
        }
        Map<String, String> fingerprint = new TreeMap<>(fields);
        fingerprint.put("tables", encoded);
        fingerprint.put("policy", policy.fingerprint(definitions.stream().map(As400BindingDefinition::identity).toList()));
        fields.put("fingerprint", "v1:" + As400CaptureFingerprint.hash("ordinary-binding-history-v1", fingerprint));
        return new Manifest(Map.copyOf(fields), definitions, encoded);
    }

    private static List<As400BindingDefinition> orderedDefinitions(List<As400BindingDefinition> input) {
        require(!input.isEmpty(), "Complete captured table set required");
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
        return definitions;
    }

    private static Map<String, String> manifestFields(String journal, JournalPosition baseline, String receiverIdentity,
                                                      Optional<Initialization> initialization, Optional<Discovery> discovery, boolean online) {
        require(!online || initialization.isEmpty() && discovery.isPresent(), "Online candidate requires its native discovery origin");
        As400Checkpoint.afterVerifiedBoundary(journal, "v1:" + "0".repeat(64), baseline, receiverIdentity);
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
        return fields;
    }

    private Manifest pagedManifest(String journal, JournalPosition baseline, String receiverIdentity, List<As400BindingDefinition> input,
                                   As400CaptureFingerprint.Policy policy, Discovery discovery, String version)
            throws Exception {
        require(Set.of("6", "7", "8").contains(version), "Unsupported paged binding version");
        var definitions = orderedDefinitions(input);
        var digest = MessageDigest.getInstance("SHA-256");
        for (int i = 0; i < definitions.size(); i++) {
            checkInterrupted();
            String encoded = As400BindingEncoding.encode(List.of(definitions.get(i)), false);
            // Bound each actual wire record, including its JSON escaping. No
            // serialization of the complete catalog is retained in memory.
            requireRecordSize(Map.of("version", version + ".table", "fingerprint", "v2:" + "0".repeat(64), "ordinal", Integer.toString(i), "bindings", encoded));
            byte[] bytes = encoded.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(8).putInt(i).putInt(bytes.length).array());
            digest.update(bytes);
        }
        String tables = HexFormat.of().formatHex(digest.digest());
        var fields = manifestFields(journal, baseline, receiverIdentity, Optional.empty(), Optional.of(discovery), true);
        fields.put("version", version);
        fields.put("binding.encoding", "2");
        fields.put("bindings.count", Integer.toString(definitions.size()));
        fields.put("bindings.hash", tables);
        var identities = definitions.stream().map(As400BindingDefinition::identity).toList();
        fields.put("policy.hash", version.equals("8") ? policy.selectableFingerprint(identities)
                : version.equals("7") ? policy.recoveryFingerprint(identities) : policy.fingerprint(identities));
        fields.put("fingerprint", "v2:" + As400CaptureFingerprint.hash("ordinary-binding-history-v2", fields));
        requireRecordSize(fields);
        return new Manifest(Map.copyOf(fields), definitions, tables);
    }

    private void requireRecordSize(Map<String, String> position) throws Exception {
        new HistoryBudget(true).requireAppend(recordBytes(new HistoryRecord(source, position, null, null, null, new TableChanges(), null)));
    }

    private static void checkInterrupted() {
        require(!Thread.currentThread().isInterrupted(), "Binding history processing stopped");
    }

    private final class Validation implements SchemaHistoryListener {
        private final As400CaptureFingerprint.Policy policy;
        private final Consumer<Manifest> validateOrigin;
        private final boolean online;
        private final String selectedFingerprint;
        private final boolean paged;
        private final boolean collectPages;
        private final List<As400BindingDefinition> pages = new ArrayList<>();
        private Consumer<Map<String, String>> schemaRecords = fields -> {
        };
        private boolean recordedSchemas;
        private String pageVersion;
        private java.util.function.BooleanSupplier eligibleLegacyRecord = () -> true;
        private Manifest manifest;
        private As400LegacyHandoff handoff;
        private final HistoryBudget budget = new HistoryBudget();

        Validation(As400CaptureFingerprint.Policy policy, Consumer<Manifest> validateOrigin) {
            this(policy, validateOrigin, false, null);
        }

        Validation(As400CaptureFingerprint.Policy policy, Consumer<Manifest> validateOrigin, boolean online, String selectedFingerprint) {
            this(policy, validateOrigin, online, selectedFingerprint, false);
        }

        Validation(As400CaptureFingerprint.Policy policy, Consumer<Manifest> validateOrigin, boolean online, String selectedFingerprint, boolean collectPages) {
            this.policy = policy;
            this.validateOrigin = validateOrigin;
            this.online = online;
            this.selectedFingerprint = selectedFingerprint;
            this.paged = online && (selectedFingerprint == null || selectedFingerprint.startsWith("v2:"));
            this.collectPages = collectPages;
        }

        @Override
        public void onChangeFromHistory(HistoryRecord record) {
            try {
                checkInterrupted();
                Document doc = record.document();
                Map<String, String> position = strings(doc.getDocument("position"));
                if (As400SchemaHistory.VERSION.equals(position.get("version"))) {
                    require(online && paged && strings(doc.getDocument("source")).equals(source) && !doc.has("ddl")
                            && doc.getArray("tableChanges") != null && doc.getArray("tableChanges").isEmpty(), "Foreign schema transition history");
                    requireRecordSize(position);
                    As400SchemaHistory.Record.restore(position);
                    recordedSchemas |= selectedFingerprint == null || selectedFingerprint.equals(position.get("fingerprint"));
                    schemaRecords.accept(position);
                    return;
                }
                if (Set.of("6", "6.table", "7", "7.table", "8", "8.table").contains(position.get("version"))) {
                    require(online, "Online candidate history and strict activation history cannot be mixed");
                    boolean legacyEligible = budget.accept(recordBytes(record), true);
                    if (paged || legacyEligible) {
                        acceptPage(record, position);
                    }
                    return;
                }
                if (paged) {
                    require("5".equals(position.get("version")), "Foreign or unsupported online history record");
                    require(strings(doc.getDocument("source")).equals(source) && !doc.has("ddl"), "Foreign or DDL history record");
                }
                if (!budget.accept(recordBytes(record), online) || online && !eligibleLegacyRecord.getAsBoolean()) {
                    // A late unfenced proposal outside the eligible prefix must
                    // neither grant authority nor poison a previously selected epoch.
                    return;
                }
                require(strings(doc.getDocument("source")).equals(source) && !doc.has("ddl"), "Foreign or DDL history record");
                require(DocumentWriter.defaultWriter().write(doc).length() <= MAX_MANIFEST_CHARS + 4096, "Oversized binding history record");
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
                boolean legacyEncoding = !position.containsKey("binding.encoding");
                List<As400BindingDefinition> definitions;
                if (legacyEncoding) {
                    require(!position.containsKey("bindings"), "Incomplete binding encoding version");
                    definitions = As400BindingEncoding.decode(array);
                }
                else {
                    require(As400BindingEncoding.VERSION.equals(position.get("binding.encoding")) && array != null && array.isEmpty(),
                            "Unsupported or mixed connector binding encoding");
                    definitions = As400BindingEncoding.decode(position.get("bindings"));
                }
                if (online && selectedFingerprint != null && !selectedFingerprint.isEmpty()) {
                    require(position.get("fingerprint") != null && position.get("fingerprint").matches("v1:[0-9a-f]{64}"), "Invalid legacy binding selection");
                    if (!selectedFingerprint.equals(position.get("fingerprint"))) {
                        // A different, unselected old generation cannot impose its
                        // output settings on the selected new recovery contract.
                        // Its shape and binding encoding were still validated above.
                        return;
                    }
                }
                var candidate = manifest(position.get("journal"),
                        new JournalPosition(new BigInteger(position.get("sequence")), new JournalReceiver(position.get("receiver"), position.get("library"))),
                        position.get("receiver.identity"), definitions, policy, initialization, discoveryFrom(position), online, legacyEncoding);
                require(candidate.position.equals(position), online ? "Changed, incomplete or noncanonical online candidate history"
                        : "Changed, incomplete or noncanonical binding history. "
                                + "Earlier configured D/CG fingerprints with different output conversion or converter credentials cannot be migrated automatically; "
                                + "preserve this history and offsets and initialize a new empty generation under a new operator window");
                if (online) {
                    // Ordinary legacy offsets may adopt a newer proposal. A
                    // durable barrier/F/W selects one exact fingerprint, even if
                    // an old task later appends another proposal. Historical v1
                    // barriers lacked that field and retain their first proposal.
                    if ("".equals(selectedFingerprint) && manifest != null
                            || selectedFingerprint != null && !selectedFingerprint.isEmpty() && !selectedFingerprint.equals(candidate.fingerprint())) {
                        return;
                    }
                }
                validateOrigin.accept(candidate);
                require(manifest == null || online && selectedFingerprint == null
                        || manifest.position.equals(candidate.position) && manifest.encodedTables.equals(candidate.encodedTables),
                        "Conflicting binding epochs");
                manifest = candidate;
            }
            catch (Exception e) {
                throw new IllegalArgumentException("Cannot recover exact binding history", e);
            }
        }

        private void acceptPage(HistoryRecord record, Map<String, String> fields) throws Exception {
            var doc = record.document();
            require(strings(doc.getDocument("source")).equals(source) && !doc.has("ddl")
                    && doc.getArray("tableChanges") != null && doc.getArray("tableChanges").isEmpty(), "Foreign or mixed paged binding history");
            requireRecordSize(fields);
            String fingerprint = fields.get("fingerprint");
            require(fingerprint != null && fingerprint.matches("v2:[0-9a-f]{64}"), "Invalid paged binding selection");
            if (Set.of("6.table", "7.table", "8.table").contains(fields.get("version"))) {
                require(fields.keySet().equals(Set.of("version", "fingerprint", "ordinal", "bindings")), "Incomplete binding page");
                int ordinal = Integer.parseInt(fields.get("ordinal"));
                require(ordinal >= 0 && fields.get("ordinal").equals(Integer.toString(ordinal)), "Invalid binding ordinal");
                var definitions = As400BindingEncoding.decode(fields.get("bindings"));
                require(definitions.size() == 1, "A binding page must describe exactly one table");
                if (collectPages && fingerprint.equals(selectedFingerprint)) {
                    String version = fields.get("version").substring(0, 1);
                    require(pageVersion == null || pageVersion.equals(version), "Mixed selected binding page versions");
                    pageVersion = version;
                    require(ordinal <= pages.size(), "Missing or reordered selected binding page");
                    if (ordinal < pages.size()) {
                        // A retry may repeat an interrupted proposal. Repeated
                        // pages must be byte-for-byte canonical equivalents.
                        require(fields.get("bindings").equals(As400BindingEncoding.encode(List.of(pages.get(ordinal)), false)),
                                "Conflicting selected binding page");
                    }
                    else {
                        require(manifest == null, "Additional page follows completed binding descriptor");
                        pages.add(definitions.get(0));
                    }
                }
                return;
            }
            require(fields.keySet().equals(Set.of("version", "journal", "sequence", "library", "receiver", "receiver.identity",
                    "discovery.sequence", "discovery.receiver", "discovery.library", "discovery.receiver.identity",
                    "binding.encoding", "bindings.count", "bindings.hash", "policy.hash", "fingerprint"))
                    && "2".equals(fields.get("binding.encoding")) && fields.get("bindings.hash").matches("[0-9a-f]{64}")
                    && fields.get("policy.hash").matches("v1:[0-9a-f]{64}"),
                    "Incomplete paged binding descriptor");
            var descriptor = new TreeMap<>(fields);
            descriptor.remove("fingerprint");
            require(fingerprint.equals("v2:" + As400CaptureFingerprint.hash("ordinary-binding-history-v2", descriptor)), "Changed binding descriptor");
            int count = Integer.parseInt(fields.get("bindings.count"));
            require(count > 0 && fields.get("bindings.count").equals(Integer.toString(count)), "Invalid paged binding count");
            if (selectedFingerprint != null && !fingerprint.equals(selectedFingerprint)) {
                return;
            }
            if (!collectPages) {
                // The second pass must verify every selected page and recompute
                // this descriptor before any checkpoint can be returned.
                require(selectedFingerprint == null || manifest == null || manifest.position.equals(fields), "Conflicting selected descriptor");
                manifest = new Manifest(Map.copyOf(fields), List.of(), fields.get("bindings.hash"));
                return;
            }
            require(pages.size() == count, "Incomplete selected binding pages");
            require(fields.get("version").equals(pageVersion), "Binding descriptor and page versions differ");
            require(pages.equals(orderedDefinitions(pages)), "Reordered selected binding pages");
            var candidate = pagedManifest(fields.get("journal"),
                    new JournalPosition(new BigInteger(fields.get("sequence")), new JournalReceiver(fields.get("receiver"), fields.get("library"))),
                    fields.get("receiver.identity"), pages, policy, discoveryFrom(fields).orElseThrow(), fields.get("version"));
            require(candidate.position.equals(fields), "Changed, incomplete or noncanonical paged binding history");
            validateOrigin.accept(candidate);
            require(manifest == null || manifest.position.equals(candidate.position), "Conflicting selected binding epoch");
            manifest = candidate;
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
        require(Set.of("1", "2", "4", "5", "6", "7", "8").contains(position.get("version")), "Unsupported binding history version");
        return "2".equals(position.get("version"))
                ? Optional.of(new Initialization(position.get("initialization.epoch"), position.get("initialization.baseline")))
                : Optional.empty();
    }

    private static Optional<Discovery> discoveryFrom(Map<String, String> position) {
        return Set.of("4", "5", "6", "7", "8").contains(position.get("version")) ? Optional.of(new Discovery(
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
        require(!(history instanceof FileSchemaHistory) || history instanceof As400FileBindingHistory,
                "File binding history requires strict recovery and forced writes");
    }
}
