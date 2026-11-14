/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Configuration;

/** Operator window exercised through the actual task; owned native fixture only. */
public class As400PlannedNativeProbeConnector extends As400NativeNormalTaskProbeConnector {
    @Override
    public Class<? extends Task> taskClass() {
        return PlannedTask.class;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 4 && List.of("read_committed", "read_uncommitted").contains(args[2])) {
            readLarge(args);
            return;
        }
        As400NativeNormalTaskProbeConnector.main(args);
    }

    private static void readLarge(String[] args) throws Exception {
        var properties = new java.util.Properties();
        properties.setProperty("bootstrap.servers", args[0]);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("isolation.level", args[2]);
        properties.setProperty("default.api.timeout.ms", "10000");
        List<Map<String, Object>> records = new ArrayList<>();
        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(properties)) {
            var partition = new org.apache.kafka.common.TopicPartition(args[1], 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.nanoTime() + java.time.Duration.ofSeconds(20).toNanos();
            while (consumer.position(partition) < end) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("Large broker evidence timed out");
                }
                for (var record : consumer.poll(java.time.Duration.ofMillis(200))) {
                    if (records.size() >= 20000) {
                        throw new IllegalStateException("Large broker evidence exceeded bound");
                    }
                    var row = new java.util.HashMap<String, Object>();
                    row.put("offset", record.offset());
                    row.put("key", record.key());
                    row.put("value", record.value());
                    row.put("timestamp", record.timestamp());
                    records.add(row);
                }
            }
        }
        As400NativeInitializationProbe.JSON.writeValue(Path.of(args[3]).toFile(), records);
    }

    public static final class PlannedTask extends NativeTask {
        private As400PlannedStartWindow window;
        private final Profile profile = new Profile();

        @Override
        As400NativeTaskSession nativeSession(Configuration config, String generation, Map<String, ?> loaded, As400NativeTaskSession.Factory factory) throws Exception {
            profile.path = CONTROL.resolve(generation + ".startup-profile.json");
            // Ordinary recovery deliberately does not read an operator plan or
            // request another write pause. It still uses every native/F/W guard.
            if (loaded != null) {
                return super.nativeSession(config, generation, loaded, factory);
            }
            window = new As400PlannedStartWindow(CONTROL.resolve("native.window"));
            return new PlannedNativeSession(config, generation, factory, window, profile);
        }

        @Override
        protected List<org.apache.kafka.connect.source.SourceRecord> doPoll() throws InterruptedException {
            long start = System.nanoTime();
            try {
                var records = super.doPoll();
                if (!records.isEmpty()) {
                    profile.event("first-returned-record");
                    profile.poll(records.size());
                }
                return records;
            }
            finally {
                profile.add("task.poll", start);
                profile.flush(false);
            }
        }

        @Override
        public void commitRecord(org.apache.kafka.connect.source.SourceRecord record, org.apache.kafka.clients.producer.RecordMetadata metadata)
                throws InterruptedException {
            long start = System.nanoTime();
            try {
                super.commitRecord(record, metadata);
                profile.event("first-acknowledged-record");
                if (record.value() instanceof String marker) {
                    profile.event("acknowledged-" + marker);
                }
            }
            finally {
                profile.add("task.commitRecord", start);
            }
        }

        @Override
        public void commit() throws InterruptedException {
            long start = System.nanoTime();
            try {
                super.commit();
            }
            finally {
                profile.add("task.commit", start);
            }
        }

        @Override
        As400TaskInitialization initialization(As400NativeTaskSession source) {
            return new As400PlannedTaskInitialization(source, window);
        }
    }

    static final class PlannedNativeSession extends As400NativeTaskSession {
        private final As400PlannedStartWindow window;
        private final Profile profile;

        PlannedNativeSession(Configuration config, String generation, Factory factory, As400PlannedStartWindow window, Profile profile) throws Exception {
            super(config, CONTROL, Path.of("/worker.properties"), generation, null, factory);
            this.window = window;
            this.profile = profile;
        }

        @Override
        public String checkHeld() throws Exception {
            String caller = StackWalker.getInstance().walk(frames -> frames
                    .filter(f -> f.getClassName().equals(As400RecoverySource.class.getName()))
                    .map(StackWalker.StackFrame::getMethodName).filter(n -> !n.equals("check")).findFirst().orElse("native"));
            return profile.call("session.checkHeld." + caller, super::checkHeld);
        }

        @Override
        boolean validateConnection(java.sql.Connection connection) throws Exception {
            return profile.call("session.isValid", () -> super.validateConnection(connection));
        }

        @Override
        public String enroll() throws Exception {
            return profile.call("stage.enroll", super::enroll);
        }

        @Override
        public List<Row> prepareBaseline(String epoch, List<As400BindingDefinition> definitions) throws Exception {
            profile.event("native-snapshot-start");
            var rows = profile.call("stage.nativeSnapshot", () -> super.prepareBaseline(epoch, definitions));
            profile.event("native-snapshot-materialized");
            return rows;
        }

        @Override
        public void checkpointAccepted(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
            profile.call("stage.checkpointAccepted", () -> {
                super.checkpointAccepted(epoch, checkpoint, history);
                return null;
            });
        }

        @Override
        public void handoff(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered history) throws Exception {
            profile.call("stage.handoff", () -> {
                super.handoff(epoch, checkpoint, history);
                return null;
            });
            profile.event("native-handoff");
            profile.flush(true);
        }

        @Override
        public void close() throws Exception {
            try {
                super.close();
                profile.event("native-drained");
            }
            finally {
                profile.flush(true);
            }
        }

        @Override
        public SnapshotLimits snapshotLimits() {
            if (window.chunks.isPresent()) {
                throw new IllegalStateException("Chunk budgets cannot materialize a baseline");
            }
            return window.limits;
        }

        @Override
        public boolean supportsChunks() {
            return true;
        }

        @Override
        public java.util.Optional<As400SnapshotChunks.Limits> chunkLimits() {
            return window.chunks;
        }

        @Override
        public long remainingMillis() {
            return Math.min(super.remainingMillis(), window.remainingMillis());
        }

        @Override
        public void scanBaseline(String epoch, List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
            profile.event("native-snapshot-start");
            profile.call("stage.nativeSnapshotChunks", () -> {
                super.scanBaseline(epoch, definitions, new As400SnapshotChunks.Consumer() {
                    @Override
                    public void chunk(io.debezium.relational.TableId table, List<Row> rows) throws Exception {
                        profile.peak("chunk.rows", rows.size());
                        profile.peak("chunk.owned_bytes", rows.stream().mapToLong(r -> r.values().payloadBytes()).sum());
                        profile.peak("sampled.heap_used_bytes", java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed());
                        profile.event("native-first-chunk");
                        consumer.chunk(table, rows);
                        profile.event("native-first-chunk-retired");
                    }

                    @Override
                    public void complete(io.debezium.relational.TableId table) throws Exception {
                        consumer.complete(table);
                    }
                });
                return null;
            });
            profile.event("native-snapshot-scanned");
        }

        @Override
        void checkRunning() {
            super.checkRunning();
            window.requireActive();
        }

        @Override
        int tableRowLimit() {
            return window.limits.rows();
        }

        @Override
        public List<String> controlledTables() throws Exception {
            checkHeld();
            var source = nativeSource();
            List<String> controlled = new ArrayList<>();
            try (var connection = source.connection(false)) {
                try (var statement = connection.prepareStatement("SELECT JOURNALED_FILES, JOURNALED_MEMBERS, JOURNALED_DATA_AREAS, JOURNALED_DATA_QUEUES,"
                        + " JOURNALED_IFS_OBJECTS, JOURNALED_ACCESS_PATHS, JOURNALED_LIBRARIES, NUMBER_REMOTE_JOURNALS FROM QSYS2.JOURNAL_INFO"
                        + " WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ?")) {
                    statement.setQueryTimeout(5);
                    statement.setMaxRows(2);
                    statement.setString(1, library);
                    statement.setString(2, "QSQJRN");
                    try (var rows = statement.executeQuery()) {
                        if (!rows.next()) {
                            throw new IllegalStateException("Native journal scope unavailable");
                        }
                        for (int column = 1; column <= 8; column++) {
                            int expected = column <= 2 ? 4 : column == 7 ? 1 : 0;
                            if (rows.getInt(column) != expected || rows.wasNull()) {
                                throw new IllegalStateException("Unsupported or incomplete planned native journal scope");
                            }
                        }
                        if (rows.next() || rows.getWarnings() != null || statement.getWarnings() != null) {
                            throw new IllegalStateException("Ambiguous native journal scope");
                        }
                    }
                }
                try (var statement = connection.prepareStatement("SELECT OBJNAME, JOURNAL_IMAGES FROM TABLE(QSYS2.OBJECT_STATISTICS(?, '*FILE')) X"
                        + " WHERE JOURNAL_LIBRARY = ? AND JOURNAL_NAME = ? ORDER BY OBJNAME")) {
                    statement.setQueryTimeout(5);
                    statement.setMaxRows(5);
                    statement.setString(1, library);
                    statement.setString(2, library);
                    statement.setString(3, "QSQJRN");
                    try (var rows = statement.executeQuery()) {
                        while (rows.next()) {
                            if (!"*BOTH".equals(rows.getString(2))) {
                                throw new IllegalStateException("Native journal images changed");
                            }
                            controlled.add(library + "." + rows.getString(1).trim());
                        }
                        if (rows.getWarnings() != null || statement.getWarnings() != null) {
                            throw new IllegalStateException("Native scope warning");
                        }
                    }
                }
            }
            var expected = List.of("A", "B", "M", "U").stream().map(t -> library + "." + t).toList();
            if (!controlled.equals(expected)) {
                throw new IllegalStateException("Native files differ from the controlled isolated fixture");
            }
            source.save("planned-native-scope.json", Map.of("controlled", controlled, "operatorWindow", window.id,
                    "authority", "controller owns every writer, DDL and maintenance operation for this isolated fixture"));
            return List.copyOf(controlled);
        }
    }

    /** Test-only inclusive timing counters. Concurrent/nested totals must not be added as wall time. */
    static final class Profile {
        interface Work<T> {
            T run() throws Exception;
        }

        private final Map<String, long[]> stats = new java.util.TreeMap<>();
        private final Map<String, Long> events = new java.util.TreeMap<>();
        private final Map<String, Long> peaks = new java.util.TreeMap<>();
        private final Map<Integer, Long> pollSizes = new java.util.TreeMap<>();
        private long lastFlush;
        private Path path;

        synchronized void add(String name, long started) {
            long elapsed = System.nanoTime() - started;
            var values = stats.computeIfAbsent(name, key -> new long[3]);
            values[0]++;
            values[1] += elapsed;
            values[2] = Math.max(values[2], elapsed);
        }

        <T> T call(String name, Work<T> work) throws Exception {
            long started = System.nanoTime();
            try {
                return work.run();
            }
            finally {
                add(name, started);
            }
        }

        synchronized void event(String name) {
            events.putIfAbsent(name, System.currentTimeMillis());
        }

        synchronized void peak(String name, long value) {
            peaks.merge(name, value, Math::max);
        }

        synchronized void poll(int size) {
            pollSizes.merge(size, 1L, Long::sum);
        }

        synchronized void flush(boolean force) {
            long now = System.nanoTime();
            if (path == null || (!force && now - lastFlush < 1_000_000_000L)) {
                return;
            }
            var values = new java.util.TreeMap<String, Object>();
            stats.forEach((key, value) -> values.put(key, Map.of("calls", value[0], "total_ns", value[1], "max_ns", value[2])));
            try {
                Path temporary = path.resolveSibling(path.getFileName() + ".tmp");
                As400NativeInitializationProbe.JSON.writeValue(temporary.toFile(), Map.of("inclusive_timings", values, "events_ms", events,
                        "peaks", peaks, "poll_sizes", pollSizes));
                java.nio.file.Files.move(temporary, path, java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                lastFlush = now;
            }
            catch (Exception failure) {
                throw new IllegalStateException("Startup profiling evidence could not be retained", failure);
            }
        }
    }

}
