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
    final boolean fileHistory;
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
        requireSnapshotMode(config, online);
        var ccsids = ccsidMapping(config);
        String keyMode = config.getString("dcg.key.mode", "primary-key");
        if (!Set.of("primary-key", "rrn").contains(keyMode)) {
            throw new IllegalArgumentException("dcg.key.mode must be primary-key or rrn");
        }
        rrnKeys = keyMode.equals("rrn");
        if (online && rrnKeys) {
            throw new IllegalArgumentException("Online key migration requires separate legacy RRN compatibility validation");
        }
        String storage = config.getString("truncate.buffer.storage", "memory");
        if (!Set.of("memory", "disk").contains(storage)) {
            throw new IllegalArgumentException("truncate.buffer.storage must be memory or disk");
        }
        boolean disk = storage.equals("disk");
        // Boundary audits can spill even when transaction payloads use memory.
        // Validate their scratch location before creating any native handles.
        Path scratch = Path.of(config.getString("truncate.buffer.directory", System.getProperty("java.io.tmpdir")));
        long diskBytes = boundedLong(config, "truncate.buffer.disk.max.bytes", 10L * 1024 * 1024 * 1024, 65536, Long.MAX_VALUE);
        if (!scratch.isAbsolute() || !java.nio.file.Files.isDirectory(scratch) || !java.nio.file.Files.isWritable(scratch)) {
            throw new IllegalArgumentException("truncate.buffer.directory must be an existing writable absolute local directory");
        }
        As400SpillStore.Settings spill = disk ? new As400SpillStore.Settings(scratch,
                bounded(config, "truncate.buffer.cache.mb", 8, 1, 1024),
                bounded(config, "truncate.buffer.write.kb", 1024, 16, 65536),
                diskBytes) : null;
        int maximumEntries = disk ? (Integer.MAX_VALUE - 3) / 4 : 100000;
        int transactions = bounded(config, "dcg.buffer.max.transactions", disk ? Integer.MAX_VALUE - 2 : 64, 1, disk ? Integer.MAX_VALUE - 2 : 1024);
        int entries = bounded(config, "dcg.buffer.max.entries", disk ? maximumEntries : 10000, 1, maximumEntries);
        int savepoints = bounded(config, "dcg.buffer.max.savepoints", disk ? Integer.MAX_VALUE - 2 : 1024, 1, disk ? Integer.MAX_VALUE - 2 : 100000);
        long bytes = boundedLong(config, "dcg.buffer.max.bytes", disk ? spill.maximumBytes() : 16 * 1024 * 1024, 65536,
                disk ? Long.MAX_VALUE : 256 * 1024 * 1024);
        runtimeLimits = new As400ContinuousRuntime.Limits(new As400TransactionBuffer.Limits(transactions, entries, savepoints, bytes),
                // Each key change can emit delete, tombstone, create and a heartbeat;
                // BEGIN/END add at most two records to the committed batch.
                new As400JournalLifecycle.Limits(entries, bytes, transactions, savepoints, entries, entries), 4 * entries + 2, true, spill);
        library = name(required(config, "database.schema"));
        String database = config.getString("database.dbname", "");
        server = online ? (database.isBlank() ? null : database.trim()) : name(required(config, "database.dbname"));
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
        if (!prefix.matches(online ? "[A-Za-z0-9_.-]{1,229}" : "[A-Za-z0-9_-]{1,100}")) {
            throw new IllegalArgumentException("Valid topic.prefix and room for the internal control-topic suffix required");
        }
        fileHistory = As400BindingHistory.fileStorage(config);
        if (fileHistory) {
            if (!online) {
                throw new IllegalArgumentException("File binding history is qualified for online handoff/recovery, not operator-window initialization");
            }
            Path history = As400FileBindingHistory.configuredPath(config);
            String offsets = config.getString("offset.storage.file.filename");
            if (offsets != null && history.equals(Path.of(offsets).toAbsolutePath().normalize())) {
                throw new IllegalArgumentException("Source offsets and binding history require distinct files");
            }
            bootstrap = null;
            historyTopic = null;
        }
        else {
            bootstrap = required(config, "schema.history.internal.kafka.bootstrap.servers");
            historyTopic = required(config, "schema.history.internal.kafka.topic");
            if (!historyTopic.matches("[A-Za-z0-9_.-]{1,200}")) {
                throw new IllegalArgumentException("Explicit dedicated history topic required");
            }
            require(config, "producer.override.bootstrap.servers", bootstrap);
            require(config, "admin.override.bootstrap.servers", bootstrap);
        }
        require(config, "dcg.retention", "operator-managed");
        if (online) {
            // Like ordinary startup, the connector returns exactly one task
            // even when the worker permits more. The ceiling is not task count.
            bounded(config, "tasks.max", 1, 1, Integer.MAX_VALUE);
        }
        else {
            require(config, "tasks.max", "1");
        }
        if (online) {
            choice(config, "exactly.once.support", "requested", Set.of("requested"));
            choice(config, "transaction.boundary", "poll", Set.of("poll", "connector"));
        }
        else {
            require(config, "exactly.once.support", "requested");
            require(config, "transaction.boundary", "connector");
        }
        if (online) {
            choice(config, "errors.tolerance", "none", Set.of("none"));
        }
        else {
            require(config, "errors.tolerance", "none");
        }
        if (online) {
            choice(config, "event.converting.failure.handling.mode", "warn", Set.of("fail", "warn", "skip"));
        }
        else {
            require(config, "event.converting.failure.handling.mode", "fail");
        }
        Set<String> skipped = skippedOperations(config);
        if (skipped.contains("t") || !online && !skipped.isEmpty()) {
            throw new IllegalArgumentException(online ? "Transactional truncate requires t to be absent from skipped.operations"
                    : "Operator initialization requires skipped.operations=none");
        }
        boolean transactionMetadata = booleanSetting(config, "provide.transaction.metadata", !online);
        if (!online && !transactionMetadata) {
            throw new IllegalArgumentException("Operator initialization requires transaction metadata");
        }
        boolean tombstones = booleanSetting(config, "tombstones.on.delete", true);
        String naming = config.getString(As400ConnectorConfig.TOPIC_NAMING_STRATEGY);
        if (online) {
            // Use the same SPI and constructor validation as ordinary startup.
            // Actual records retain their destinations through acknowledgement;
            // the naming implementation is not a recovery identity.
            new As400ConnectorConfig(config).getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        }
        else if (!Set.of(SchemaTopicNamingStrategy.class.getName(), HostnamePrefixNamingScheme.class.getName()).contains(naming)) {
            throw new IllegalArgumentException("D/CG requires a built-in IBM i or schema topic naming strategy");
        }
        choice(config, "decimal.handling.mode", "precise", Set.of("precise", "double", "string"));
        choice(config, "binary.handling.mode", "bytes", Set.of("bytes", "base64", "base64-url-safe", "hex"));
        String temporal = config.getString("time.precision.mode", "adaptive");
        if (!Set.of("adaptive", "adaptive_time_microseconds", "connect").contains(temporal)) {
            throw new IllegalArgumentException("Unsupported time precision mode");
        }
        choice(config, "schema.name.adjustment.mode", "none", Set.of("none", "avro", "avro_unicode"));
        choice(config, "field.name.adjustment.mode", "none", Set.of("none", "avro", "avro_unicode"));
        var trim = As400ConnectorConfig.CharSequenceTrimMode.parse(config.getString(As400ConnectorConfig.TRIM_NON_XML_CHARSEQUENCE_FIELD_MODE));
        for (String key : List.of("key.converter", "value.converter", "header.converter")) {
            if ((online || key.equals("header.converter")) && config.getString(key) == null) {
                continue;
            }
            // Serialization is Connect's Converter contract. No source record or
            // offset is acknowledged after a converter error (errors.tolerance=none).
            // Online delivery can inherit the worker's standard converters.
            // Serialization is an output choice, not a native replay identity.
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
        var customConversion = customConverters(config);
        for (String key : config.asMap().keySet()) {
            if (((key.startsWith("table.") && !Set.of("table.include.list", "table.exclude.list").contains(key))
                    || ((key.startsWith("schema.include") || key.startsWith("schema.exclude"))
                            && !Set.of("schema.include.list", "schema.exclude.list").contains(key))
                    || (key.startsWith("message.key.") && !key.equals("message.key.columns"))
                    || (!online && key.startsWith("signal.")) || (!online && key.startsWith("snapshot.select")) || key.startsWith("custom.") || key.startsWith("source."))
                    && !config.getString(key, "").isEmpty() && !customConversion.containsKey(key)) {
                throw new IllegalArgumentException("Unsupported D/CG capture/output override: " + key);
            }
        }
        for (String key : List.of("producer.override.interceptor.classes", "producer.override.key.serializer", "producer.override.value.serializer",
                "producer.override.partitioner.class", "producer.override.transactional.id", "schema.history.internal.consumer.interceptor.classes",
                "schema.history.internal.producer.interceptor.classes")) {
            if (config.getString(key) != null && !(online && Set.of("producer.override.interceptor.classes", "producer.override.partitioner.class").contains(key))) {
                throw new IllegalArgumentException("Unsupported D/CG override: " + key);
            }
        }
        // Fresh online startup uses the ordinary snapshotter. Snapshot-only
        // options do not change streaming recovery identity. The operator-window
        // profile still owns a full baseline and cannot honor these overrides.
        if (!online && (config.getString("snapshot.query.mode") != null || config.getString("snapshot.locking.mode") != null)) {
            throw new IllegalArgumentException("Operator initialization cannot override its snapshot query or locking protocol");
        }
        String path = config.getString("dcg.initialization.window");
        window = path == null ? null : Path.of(path);
        if ((window != null && !window.isAbsolute()) || (mode.equals("initialize") && window == null)) {
            throw new IllegalArgumentException("initialize requires an absolute persistent dcg.initialization.window directory");
        }
        Map<String, String> output = new TreeMap<>();
        output.putAll(As400TopicRouting.from(config.asMap()).settings());
        output.putAll(customConversion);
        // Previous native generations bypassed legacy string cleanup. Do not
        // silently reinterpret their offsets with changed key/value semantics.
        output.put("version", online ? "native-online-candidate-v3" : "native-configured-v2");
        if (online && transactionMetadata) {
            output.put("transaction.id.format", "legacy-cycle-v1");
        }
        if (online) {
            output.put("binding.catalog", "verified-native-server");
        }
        // Preserve Kafka generations' exact fingerprint. A file path may move
        // with its durable contents, but switching storage contracts is explicit.
        output.put(fileHistory ? "history.storage" : "history.topic", fileHistory ? "file-v1" : historyTopic);
        if (rrnKeys) {
            output.put("key.mode", "rrn-v1");
        }
        for (String key : List.of("topic.prefix", "topic.naming.strategy", "decimal.handling.mode", "binary.handling.mode", "time.precision.mode",
                "schema.name.adjustment.mode", "field.name.adjustment.mode", "trim.non.xml.charsequence.field.mode", "key.converter", "value.converter",
                "key.converter.schemas.enable", "value.converter.schemas.enable")) {
            output.put(key, config.getString(key, key.endsWith(".schemas.enable") ? "true" : ""));
        }
        if (online) {
            for (String key : List.of("key.converter", "value.converter")) {
                if (config.getString(key) == null) {
                    output.put(key, "worker-default");
                    if (config.getString(key + ".schemas.enable") == null) {
                        output.put(key + ".schemas.enable", "worker-default");
                    }
                }
            }
        }
        output.put("topic.naming.strategy", naming);
        output.put("decimal.handling.mode", config.getString("decimal.handling.mode", "precise"));
        output.put("binary.handling.mode", config.getString("binary.handling.mode", "bytes"));
        output.put("schema.name.adjustment.mode", config.getString("schema.name.adjustment.mode", "none"));
        output.put("field.name.adjustment.mode", config.getString("field.name.adjustment.mode", "none"));
        output.put("time.precision.mode", temporal);
        output.put("trim.non.xml.charsequence.field.mode", trim.getValue());
        if (config.getString("header.converter") != null) {
            output.put("header.converter", config.getString("header.converter"));
        }
        if (booleanSetting(config, "skip.messages.without.change", false)) {
            output.put("skip.messages.without.change", "true");
        }
        config.asMap().forEach((key, value) -> {
            if ((key.startsWith("column.") || key.equals("message.key.columns")) && value != null && !value.isEmpty()) {
                output.put(key, value);
            }
            if ((key.startsWith("key.converter.") || key.startsWith("value.converter.") || key.startsWith("header.converter.")) && value != null
                    && As400ConverterFingerprint.include(config, key, value)) {
                output.put(key, value);
            }
        });
        // Both publication paths use the same built-in topic strategies. Pin
        // non-default naming instead of rejecting a legacy stream's names, and
        // preserve existing default fingerprints (including explicit defaults).
        Map.of("topic.delimiter", ".", "topic.heartbeat.prefix", "__debezium-heartbeat", "topic.transaction", "transaction")
                .forEach((key, fallback) -> {
                    String value = config.getString(key, fallback);
                    if (!value.equals(fallback)) {
                        output.put(key, value);
                    }
                });
        for (String key : List.of("topic.heartbeat.name", "datatype.propagate.source.type")) {
            String value = config.getString(key);
            if (value != null && !value.isEmpty()) {
                output.put(key, value);
            }
        }
        Map<String, String> selection = new TreeMap<>();
        for (String key : List.of("table.include.list", "table.exclude.list", "schema.include.list", "schema.exclude.list")) {
            if (config.getString(key) != null) {
                selection.put(key, config.getString(key));
            }
        }
        if (selection.containsKey("table.include.list") && selection.containsKey("table.exclude.list")
                || selection.containsKey("schema.include.list") && selection.containsKey("schema.exclude.list")) {
            throw new IllegalArgumentException("Include and exclude lists for the same scope are mutually exclusive");
        }
        var connector = new As400ConnectorConfig(config);
        // Use the same SPI/service registry as ordinary streaming. Resolve the
        // configured classes before any native handles or initialization work.
        As400NativeTableSchemas.converterRegistry(connector);
        // Validate built-in naming configuration before any native acquisition.
        connector.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
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
        if (!connector.getSignalingDataCollectionIds().isEmpty()) {
            decoderPolicy.put("signal.data.collection", String.join(",", connector.getSignalingDataCollectionIds()));
        }
        // Disabled and identity substitutions preserve existing generations.
        // Effective substitutions must survive restart without changing decoded keys.
        if (ccsids.active()) {
            decoderPolicy.put("from.ccsid", Integer.toString(ccsids.from()));
            decoderPolicy.put("to.ccsid", Integer.toString(ccsids.to()));
        }
        policy = new As400CaptureFingerprint.Policy(Map.of("captured", online ? "native-discovered" : String.join(",", captured),
                "controlled", online ? "native-discovered" : String.join(",", controlled),
                "journal", config.getString("dcg.journal"), "server", server == null ? "native-discovered" : server, "journaled.libraries",
                libraries.isEmpty() ? "none" : libraries),
                skipped, decoderPolicy, Map.of("version", "fixed-v1"), output, transactionMetadata, tombstones, Map.of("server", prefix));
    }

    static Set<String> skippedOperations(Configuration config) {
        String value = config.getString("skipped.operations", "t").trim();
        if (value.equalsIgnoreCase("none")) {
            return Set.of();
        }
        var operations = java.util.Arrays.stream(value.split(",", -1)).map(String::trim).collect(java.util.stream.Collectors.toSet());
        if (!Set.of("c", "u", "d", "t").containsAll(operations)) {
            throw new IllegalArgumentException("Invalid skipped.operations");
        }
        return Set.copyOf(operations);
    }

    private static Map<String, String> customConverters(Configuration config) {
        String configured = config.getString("converters", "").trim();
        if (configured.isEmpty()) {
            return Map.of();
        }
        var aliases = Arrays.stream(configured.split(",", -1)).map(String::trim).toList();
        if (aliases.stream().anyMatch(String::isEmpty) || aliases.stream().distinct().count() != aliases.size()) {
            throw new IllegalArgumentException("Distinct nonempty custom converter aliases required");
        }
        var settings = new TreeMap<String, String>();
        settings.put("converters", String.join(",", aliases));
        for (String alias : aliases) {
            required(config, alias + ".type");
            config.asMap().forEach((key, value) -> {
                if (key.startsWith(alias + ".") && value != null) {
                    As400ConverterFingerprint.requireNonSecretCustomSetting(key);
                    settings.put(key, value);
                }
            });
        }
        return Map.copyOf(settings);
    }

    static As400NativeBindingAcquisition.CcsidMapping ccsidMapping(Configuration config) {
        return new As400NativeBindingAcquisition.CcsidMapping(Integer.parseInt(config.getString("from.ccsid", "-1")),
                Integer.parseInt(config.getString("to.ccsid", "-1")));
    }

    void requireRuntime(SourceTaskContext context) {
        // Public framework capability, not an operator-supplied worker config file.
        if (context == null) {
            throw new IllegalStateException("Source task context required");
        }
        if (fileHistory && context.transactionContext() != null) {
            throw new IllegalStateException("File binding history requires ordinary source-offset acknowledgement");
        }
        if (context.transactionContext() == null && mode.equals("online-candidate")) {
            As400RecoveryAcknowledgements.requireSourceOffsetProfile(raw.asMap());
            return;
        }
        if (context.transactionContext() == null) {
            throw new IllegalStateException("D/CG requires distributed Kafka Connect source transactions and connector transaction boundaries");
        }
        As400RecoveryAcknowledgements.requireConnectorProfile(raw.asMap());
        // Do not strand a durable generation on a newer compatible worker solely
        // because its version string is outside the local validation matrix.
        // The artifact's Debezium/Kafka compatibility and source transaction API
        // remain requirements; tested worker versions are reported separately.
    }

    static void requireSnapshotMode(Configuration config) {
        requireSnapshotMode(config, transactionalTruncateEnabled(config));
    }

    static void requireSnapshotMode(Configuration config, boolean existingStream) {
        String mode = config.getString("snapshot.mode", "initial");
        if (existingStream && Set.of("initial", "initial_only", "no_data", "when_needed", "always", "configuration_based", "custom").contains(mode)) {
            // The ordinary snapshotter chooses data collection. Requested full
            // snapshots retain existing F/W until their whole delivery completes.
            return;
        }
        // The separate operator-window profile establishes its baseline once.
        if (!"initial".equals(mode)) {
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
        if (!c.getHeartbeatInterval().isZero()) {
            topics.add(strategy.heartbeatTopic());
        }
        else {
            topics.add(controlTopic());
        }
        topics.add(strategy.heartbeatTopic() + ".initialization");
        topics.add(strategy.transactionTopic());
        if (!fileHistory) {
            topics.add(historyTopic);
        }
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

    private static long boundedLong(Configuration config, String key, long fallback, long minimum, long maximum) {
        long value = Long.parseLong(config.getString(key, Long.toString(fallback)));
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

    private static void choice(Configuration config, String key, String fallback, Set<String> values) {
        if (!values.contains(config.getString(key, fallback))) {
            throw new IllegalArgumentException("Unsupported " + key);
        }
    }

    private static boolean booleanSetting(Configuration config, String key, boolean fallback) {
        choice(config, key, Boolean.toString(fallback), Set.of("true", "false"));
        return Boolean.parseBoolean(config.getString(key, Boolean.toString(fallback)));
    }
}
