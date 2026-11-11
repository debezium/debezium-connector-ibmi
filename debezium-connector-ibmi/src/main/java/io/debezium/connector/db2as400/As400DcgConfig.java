/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.connect.source.SourceTaskContext;

import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaTopicNamingStrategy;
import io.debezium.spi.topic.TopicNamingStrategy;

/** Explicit, fail-closed admission for the first configured native D/CG profile. */
final class As400DcgConfig {
    static final String MODE = "dcg.mode";
    final Configuration raw;
    final String mode;
    final String library;
    final JournalReceiver journal;
    final List<String> captured;
    final List<String> controlled;
    final List<String> journaledLibraries;
    final String prefix;
    final String server;
    final String historyTopic;
    final String bootstrap;
    final Path window;
    final As400CaptureFingerprint.Policy policy;
    final As400ContinuousRuntime.Limits runtimeLimits;
    final boolean rrnKeys;

    static boolean enabled(Configuration config) {
        String mode = config.getString(MODE, "disabled");
        if (!Set.of("disabled", "initialize", "resume").contains(mode)) {
            throw new IllegalArgumentException("dcg.mode must be disabled, initialize or resume");
        }
        return !"disabled".equals(mode);
    }

    static boolean transactionalTruncateEnabled(Configuration config) {
        String value = config.getString("truncate.transactional.enabled", "false");
        if (!Set.of("true", "false").contains(value)) {
            throw new IllegalArgumentException("truncate.transactional.enabled must be true or false");
        }
        return Boolean.parseBoolean(value);
    }

    static boolean configured(Configuration config) {
        // Evaluate both fields, including invalid/conflicting admissions.
        boolean online = transactionalTruncateEnabled(config);
        boolean operator = enabled(config);
        if (online && operator) {
            throw new IllegalArgumentException("Transactional truncate migration cannot select dcg.mode initialization or resume");
        }
        return online || operator;
    }

    static As400DcgConfig configuredProfile(Configuration config) {
        configured(config);
        return transactionalTruncateEnabled(config) ? onlineCandidate(config) : new As400DcgConfig(config);
    }

    String controlTopic() {
        return "__debezium-truncate." + prefix;
    }

    As400CaptureFingerprint.Policy onlinePolicy() {
        var output = new TreeMap<>(policy.output());
        output.put("control.topic", controlTopic());
        // Preserve already-published legacy temporal values during migration.
        // Its JT400 codecs depend on the worker zone; replacements must not
        // silently reinterpret keys or replayed values in another zone/ruleset.
        var zone = java.util.TimeZone.getDefault().toZoneId().normalized();
        output.put("temporal.compatibility", "legacy-jt400-v1");
        output.put("temporal.worker.zone", zone.getId());
        output.put("temporal.worker.rules", zone.getRules().isFixedOffset() ? "fixed"
                : java.time.zone.ZoneRulesProvider.getVersions(zone.getId()).lastKey());
        return new As400CaptureFingerprint.Policy(policy.filters(), policy.skippedOperations(), policy.decoder(), policy.schema(), output,
                policy.transactionMetadata(), policy.tombstones(), policy.sourcePartition());
    }

    As400DcgConfig(Configuration config) {
        this(config, false);
    }

    /** Shared online contract; the normal task additionally requires explicit transactional truncate admission. */
    static As400DcgConfig onlineCandidate(Configuration config) {
        return new As400DcgConfig(config, true);
    }

    private As400DcgConfig(Configuration config, boolean online) {
        raw = config;
        if (!online && (!enabled(config) || transactionalTruncateEnabled(config))) {
            throw new IllegalArgumentException("Explicit D/CG admission required");
        }
        if (online && (enabled(config) || config.getString("dcg.tables") != null || config.getString("dcg.controlled.tables") != null
                || config.getString("dcg.initialization.window") != null || config.getString("dcg.journaled.libraries") != null)) {
            throw new IllegalArgumentException("Online candidate must discover its complete native scope and cannot select an operator initialization");
        }
        mode = online ? "online-candidate" : required(config, MODE);
        requireSnapshotMode(config);
        String keyMode = config.getString("dcg.key.mode", "primary-key");
        if (!Set.of("primary-key", "rrn").contains(keyMode)) {
            throw new IllegalArgumentException("dcg.key.mode must be primary-key or rrn");
        }
        rrnKeys = keyMode.equals("rrn");
        if (online && rrnKeys) {
            throw new IllegalArgumentException("Online key migration requires separate legacy RRN compatibility validation");
        }
        int transactions = bounded(config, "dcg.buffer.max.transactions", 64, 1, 1024);
        int entries = bounded(config, "dcg.buffer.max.entries", 10000, 1, 100000);
        int savepoints = bounded(config, "dcg.buffer.max.savepoints", 1024, 1, 100000);
        int bytes = bounded(config, "dcg.buffer.max.bytes", 16 * 1024 * 1024, 65536, 256 * 1024 * 1024);
        runtimeLimits = new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(transactions, entries, savepoints, bytes),
                // Each key change can emit delete, tombstone, create and a heartbeat;
                // BEGIN/END add at most two records to the committed batch.
                new As400JournalLifecycle.Limits(entries, bytes, transactions, savepoints, entries, entries), 4 * entries + 2, true);
        library = name(required(config, "database.schema"));
        server = name(required(config, "database.dbname"));
        String[] j = required(config, "dcg.journal").split("/", -1);
        if (j.length != 2) {
            throw new IllegalArgumentException("dcg.journal must be LIBRARY/JOURNAL");
        }
        journal = new JournalReceiver(name(j[1]), name(j[0]));
        captured = online ? List.of() : tables(required(config, "dcg.tables"));
        controlled = online ? List.of() : tables(required(config, "dcg.controlled.tables"));
        if (!controlled.equals(captured) || controlled.stream().anyMatch(t -> !t.startsWith(library + "."))) {
            throw new IllegalArgumentException("First configured D/CG profile requires one library and capture of every controlled journal file");
        }
        String libraries = config.getString("dcg.journaled.libraries", "");
        journaledLibraries = libraries.isEmpty() ? List.of() : List.of(name(libraries));
        if (!journaledLibraries.isEmpty() && !journaledLibraries.equals(List.of(library))) {
            throw new IllegalArgumentException("Only the controlled library may itself be journaled");
        }
        prefix = required(config, "topic.prefix");
        if (!prefix.matches("[A-Za-z0-9_-]{1,100}")) {
            throw new IllegalArgumentException("Explicit simple unique topic.prefix required");
        }
        bootstrap = required(config, "schema.history.internal.kafka.bootstrap.servers");
        historyTopic = required(config, "schema.history.internal.kafka.topic");
        if (!historyTopic.matches("[A-Za-z0-9_.-]{1,200}")) {
            throw new IllegalArgumentException("Explicit dedicated history topic required");
        }
        require(config, "producer.override.bootstrap.servers", bootstrap);
        require(config, "admin.override.bootstrap.servers", bootstrap);
        require(config, "dcg.retention", "operator-managed");
        require(config, "tasks.max", "1");
        require(config, "exactly.once.support", "requested");
        require(config, "transaction.boundary", "connector");
        require(config, "errors.tolerance", "none");
        require(config, "event.converting.failure.handling.mode", "fail");
        require(config, "skipped.operations", "none");
        require(config, "provide.transaction.metadata", online ? "false" : "true");
        require(config, "tombstones.on.delete", "true");
        String naming = config.getString(As400ConnectorConfig.TOPIC_NAMING_STRATEGY);
        if (!Set.of(SchemaTopicNamingStrategy.class.getName(), HostnamePrefixNamingScheme.class.getName()).contains(naming)) {
            throw new IllegalArgumentException("D/CG requires a built-in IBM i or schema topic naming strategy");
        }
        require(config, "decimal.handling.mode", "precise");
        require(config, "binary.handling.mode", "bytes");
        String temporal = config.getString("time.precision.mode", "adaptive");
        if (!Set.of("adaptive", "adaptive_time_microseconds").contains(temporal)) {
            throw new IllegalArgumentException("D/CG requires adaptive or adaptive_time_microseconds time precision");
        }
        require(config, "schema.name.adjustment.mode", "none");
        require(config, "field.name.adjustment.mode", "none");
        var trim = As400ConnectorConfig.CharSequenceTrimMode.parse(config.getString(As400ConnectorConfig.TRIM_NON_XML_CHARSEQUENCE_FIELD_MODE));
        for (String key : List.of("key.converter", "value.converter")) {
            // Serialization is Connect's Converter contract. No source record or
            // offset is acknowledged after a converter error (errors.tolerance=none).
            // Explicit classes pin restart semantics instead of inheriting a worker
            // default which can change during reassignment.
            if (!required(config, key).matches("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+")) {
                throw new IllegalArgumentException("Explicit Converter class required for " + key);
            }
            if ("org.apache.kafka.connect.json.JsonConverter".equals(config.getString(key))) {
                String schemas = config.getString(key + ".schemas.enable", "true");
                if (!Set.of("true", "false").contains(schemas)) {
                    throw new IllegalArgumentException("Invalid JSON schemas.enable");
                }
            }
        }
        for (String key : config.asMap().keySet()) {
            if ((key.startsWith("column.") || (key.startsWith("table.") && !Set.of("table.include.list", "table.exclude.list").contains(key))
                    || key.startsWith("schema.include") || key.startsWith("schema.exclude")
                    || key.startsWith("message.key.") || key.startsWith("converters") || key.startsWith("transforms") || key.startsWith("predicates")
                    || key.startsWith("signal.") || key.startsWith("snapshot.select") || key.startsWith("custom.") || key.startsWith("source."))
                    && !config.getString(key, "").isEmpty()) {
                throw new IllegalArgumentException("Unsupported D/CG capture/output override: " + key);
            }
        }
        for (String key : List.of("producer.override.interceptor.classes", "producer.override.key.serializer", "producer.override.value.serializer",
                "producer.override.partitioner.class", "producer.override.transactional.id", "schema.history.internal.consumer.interceptor.classes",
                "schema.history.internal.producer.interceptor.classes", "header.converter", "from.ccsid", "to.ccsid", "snapshot.query.mode", "snapshot.locking.mode")) {
            if (config.getString(key) != null) {
                throw new IllegalArgumentException("Unsupported D/CG override: " + key);
            }
        }
        String path = config.getString("dcg.initialization.window");
        window = path == null ? null : Path.of(path);
        if ((window != null && !window.isAbsolute()) || (mode.equals("initialize") && window == null)) {
            throw new IllegalArgumentException("initialize requires an absolute persistent dcg.initialization.window directory");
        }
        Map<String, String> output = new TreeMap<>();
        // Previous native generations bypassed legacy string cleanup. Do not
        // silently reinterpret their offsets with changed key/value semantics.
        output.put("version", online ? "native-online-candidate-v3" : "native-configured-v2");
        if (online) {
            output.put("binding.catalog", "verified-native-server");
        }
        output.put("history.topic", historyTopic);
        if (rrnKeys) {
            output.put("key.mode", "rrn-v1");
        }
        for (String key : List.of("topic.prefix", "topic.naming.strategy", "decimal.handling.mode", "binary.handling.mode", "time.precision.mode",
                "schema.name.adjustment.mode", "field.name.adjustment.mode", "trim.non.xml.charsequence.field.mode", "key.converter", "value.converter",
                "key.converter.schemas.enable", "value.converter.schemas.enable")) {
            output.put(key, config.getString(key, key.endsWith(".schemas.enable") ? "true" : ""));
        }
        output.put("topic.naming.strategy", naming);
        output.put("time.precision.mode", temporal);
        output.put("trim.non.xml.charsequence.field.mode", trim.getValue());
        config.asMap().forEach((key, value) -> {
            if ((key.startsWith("key.converter.") || key.startsWith("value.converter.")) && value != null
                    && As400ConverterFingerprint.include(config, key, value)) {
                output.put(key, value);
            }
        });
        for (String key : List.of("topic.delimiter", "topic.heartbeat.prefix", "topic.transaction", "datatype.propagate.source.type")) {
            if (config.getString(key) != null) {
                throw new IllegalArgumentException("Unsupported D/CG naming/type override: " + key);
            }
        }
        Map<String, String> selection = new TreeMap<>();
        for (String key : List.of("table.include.list", "table.exclude.list")) {
            if (config.getString(key) != null) {
                selection.put(key, config.getString(key));
            }
        }
        if (selection.size() > 1) {
            throw new IllegalArgumentException("table.include.list and table.exclude.list are mutually exclusive");
        }
        var connector = new As400ConnectorConfig(config);
        As400BatchDispatcher.requireConfiguration(connector);
        var filter = connector.getTableFilters().dataCollectionFilter();
        if (!online && captured.stream().noneMatch(t -> filter.isIncluded(new TableId(null, t.split("\\.")[0], t.split("\\.")[1])))) {
            throw new IllegalArgumentException("Table filters select no file in the controlled journal inventory");
        }
        // Preserve the existing no-filter fingerprint; filtered generations pin
        // their selection and still acquire/replay every controlled binding.
        Map<String, String> decoderPolicy = new TreeMap<>(selection);
        decoderPolicy.put("version", "strict-row-v1");
        decoderPolicy.put("journal.metadata.ccsid", Integer.toString(RawJournalPage.METADATA_CCSID));
        policy = new As400CaptureFingerprint.Policy(Map.of("captured", online ? "native-discovered" : String.join(",", captured),
                "controlled", online ? "native-discovered" : String.join(",", controlled),
                "journal", config.getString("dcg.journal"), "server", server, "journaled.libraries", libraries.isEmpty() ? "none" : libraries),
                Set.of(), decoderPolicy, Map.of("version", "fixed-v1"), output, !online, true, Map.of("server", prefix));
    }

    void requireRuntime(SourceTaskContext context) {
        // Public framework capability, not an operator-supplied worker config file.
        if (context == null || context.transactionContext() == null) {
            throw new IllegalStateException("D/CG requires distributed Kafka Connect source transactions and connector transaction boundaries");
        }
        // Do not strand a durable generation on a newer compatible worker solely
        // because its version string is outside the local validation matrix.
        // The artifact's Debezium/Kafka compatibility and source transaction API
        // remain requirements; tested worker versions are reported separately.
    }

    static void requireSnapshotMode(Configuration config) {
        // The admitted native session establishes a baseline once and thereafter
        // resumes F/W. Other snapshot policies need their own exclusion/handoff
        // protocol; never silently turn no_data or initial_only into initial.
        if (!"initial".equals(config.getString("snapshot.mode", "initial"))) {
            throw new IllegalArgumentException("D/CG supports snapshot.mode=initial only; other modes cannot establish or replace its native baseline");
        }
    }

    List<As400OperatorTableEvidence.Request> requests() {
        return captured.stream().map(t -> new As400OperatorTableEvidence.Request(journal, t.split("\\.")[0], t.split("\\.")[1])).toList();
    }

    List<String> topics() {
        As400ConnectorConfig c = new As400ConnectorConfig(raw);
        TopicNamingStrategy<TableId> strategy = c.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        var topics = new java.util.ArrayList<String>();
        for (String table : captured) {
            String[] parts = table.split("\\.");
            var id = new TableId(null, parts[0], parts[1]);
            if (c.getTableFilters().dataCollectionFilter().isIncluded(id)) {
                topics.add(strategy.dataChangeTopic(id));
            }
        }
        topics.add(strategy.heartbeatTopic());
        topics.add(strategy.heartbeatTopic() + ".initialization");
        topics.add(strategy.transactionTopic());
        topics.add(historyTopic);
        if (topics.stream().distinct().count() != topics.size()) {
            throw new IllegalArgumentException("D/CG output, barrier and history topics must be distinct");
        }
        return List.copyOf(topics);
    }

    private static int bounded(Configuration config, String key, int fallback, int minimum, int maximum) {
        int value = Integer.parseInt(config.getString(key, Integer.toString(fallback)));
        if (value < minimum || value > maximum) {
            throw new IllegalArgumentException(key + " must be between " + minimum + " and " + maximum);
        }
        return value;
    }

    static String name(String value) {
        if (!value.matches("[A-Z][A-Z0-9_]{0,9}")) {
            throw new IllegalArgumentException("Exact basic IBM i system name required");
        }
        return value;
    }

    static List<String> tables(String value) {
        var list = Arrays.asList(value.split(",", -1));
        if (list.isEmpty() || list.size() > 100 || list.stream().distinct().count() != list.size()
                || list.stream().anyMatch(v -> !v.matches("[A-Z][A-Z0-9_]{0,9}\\.[A-Z][A-Z0-9_]{0,9}"))) {
            throw new IllegalArgumentException("Exact distinct bounded LIBRARY.FILE list required");
        }
        return list.stream().sorted().toList();
    }

    static String required(Configuration config, String key) {
        String value = config.getString(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing " + key);
        }
        return value;
    }

    static void require(Configuration config, String key, String expected) {
        if (!expected.equals(config.getString(key))) {
            throw new IllegalArgumentException("D/CG requires " + key + "=" + expected);
        }
    }
}
