/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.relational.TableId;

/**
 * MANUAL LOCAL EXPERIMENT ONLY. Synthetic input, no source connections or startup
 * proof. Compiled in test scope, never included in the distributed connector.
 */
public class As400ConnectProbeConnector extends SourceConnector {
    private static final ObjectMapper JSON = new ObjectMapper();
    static final Path CONTROL = Path.of("/control");
    static final String IDENTITY = "synthetic-journal";
    static final String FINGERPRINT = "v1:" + "a".repeat(64);
    static final String JID = "0102030405060708090a";
    static final String MEMBER_JID = "0b0c0d0e0f1011121314";
    static final JournalReceiver RECEIVER = new JournalReceiver("RCV", "LIB");
    static final Map<String, String> PARTITION = Map.of("server", "synthetic");
    static final As400JournalOrder ORDER = new As400JournalOrder(List.of(RECEIVER), Map.of(RECEIVER, "synthetic-receiver"),
            Map.of(RECEIVER, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(9))));
    private Map<String, String> props;

    @Override
    public String version() {
        return "local-feasibility-1";
    }

    @Override
    public void start(Map<String, String> props) {
        this.props = Map.copyOf(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ProbeTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return List.of(props);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef().define("probe.topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Synthetic output topic");
    }

    @Override
    public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> props) {
        // Whole-batch replay deliberately permits duplicates. Never advertise EOS.
        return ExactlyOnceSupport.UNSUPPORTED;
    }

    /** Uses the real coordinator and source normalizer, with bounded synthetic journal bytes. */
    public static class ProbeTask extends SourceTask {
        private final List<SourceRecord> output = new ArrayList<>();
        private String generation;
        private String topic;
        private int next;
        private volatile boolean stopped;

        @Override
        public String version() {
            return "local-feasibility-1";
        }

        @Override
        public void start(Map<String, String> props) {
            topic = props.get("probe.topic");
            generation = System.getenv("PROBE_WORKER") + "-" + UUID.randomUUID();
            Map<String, Object> persisted = context.offsetStorageReader().offset(PARTITION);
            write(generation + ".start.json", Map.of("generation", generation, "loaded", persisted == null ? Map.of() : persisted));
            // A fixture baseline only. This is NOT a production missing-offset policy.
            Map<String, ?> initial = persisted == null
                    ? As400Checkpoint.afterVerifiedBoundary(IDENTITY, FINGERPRINT, pos(1), "synthetic-receiver").toOffset()
                    : persisted;
            CompleteRowEntryDecoder decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
            TableIdentity table = new TableIdentity(new TableId("DB", "LIB", "TEST"), "TEST", "TEST", JID, As400ConnectProbeConnector.MEMBER_JID, "FORMAT_V1");
            try (As400ReplayCoordinator coordinator = new As400ReplayCoordinator(initial, IDENTITY, FINGERPRINT, ORDER,
                    new Limits(4, 20, 10, 4096), 1024, 20,
                    (file, library) -> new As400SourceNormalizer.TableBinding(table, decoder),
                    (source, control) -> SavepointRollback.UNRESOLVED, PARTITION, record -> {
                        Map<String, Object> value = new HashMap<>((Map<String, ?>) record.value());
                        value.put("checkpoint", record.sourceOffset());
                        value.put("index", output.size());
                        output.add(new SourceRecord(record.sourcePartition(), record.sourceOffset(), record.topic(), 0,
                                record.keySchema(), record.key(), null, value, record.timestamp()));
                        return true;
                    })) {
                As400Checkpoint resume = coordinator.checkpoint();
                for (Event event : events()) {
                    int comparison = ORDER.compare(event.position(), resume.replayFrom());
                    if (comparison < 0 || (comparison == 0 && !resume.replayInclusive())) {
                        continue;
                    }
                    if (coordinator.accept(event.header(), event.data(), 0, event.position()).isPresent()) {
                        coordinator.deliver(this::prepare);
                        coordinator.publishCheckpoint(record("HB", event.position().offset().longValue()));
                    }
                }
                coordinator.verifyEndOfAvailableJournal();
                write(generation + ".prepared.json", Map.of("count", output.size(), "checkpoint", coordinator.checkpoint().toOffset(),
                        "scan", coordinator.scanPosition().map(p -> p.offset().toString()).orElse("none")));
            }
        }

        private void prepare(CommittedBatch batch, Consumer<SourceRecord> records) {
            records.accept(record("BEGIN", batch.startSource().position().offset().longValue()));
            batch.changes().forEach(change -> records.accept(record(switch (change.operation()) {
                case TRUNCATE -> "t";
                case CREATE -> "c:" + change.after().values()[0];
                default -> throw new IllegalArgumentException("Unexpected fixture operation");
            }, change.source().position().offset().longValue())));
            records.accept(record("END", batch.commitPosition().offset().longValue()));
        }

        private SourceRecord record(String operation, long sequence) {
            // Unsafe supplied offset must be replaced by the real coordinator.
            return new SourceRecord(PARTITION, Map.of("scan", "9999"), topic, 0, null, null, null,
                    Map.of("op", operation, "generation", generation, "sequence", sequence), sequence * 1000);
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            while (!stopped) {
                Path permit = CONTROL.resolve(generation + ".permit");
                try {
                    if (next < output.size() && Files.exists(permit) && Integer.parseInt(Files.readString(permit).trim()) > next) {
                        SourceRecord record = output.get(next++);
                        append(generation, "poll", Map.of("value", record.value(), "offset", record.sourceOffset()));
                        return List.of(record); // Deliberately split a source batch across Kafka transactions.
                    }
                }
                catch (java.io.IOException e) {
                    throw new IllegalStateException(e);
                }
                Thread.sleep(25);
            }
            return null;
        }

        @Override
        public void commitRecord(SourceRecord record, RecordMetadata metadata) {
            append(generation, "commitRecord", Map.of("value", record.value(), "offset", record.sourceOffset()));
        }

        @Override
        public void commit() {
            append(generation, "commit", Map.of());
        }

        @Override
        public void stop() {
            stopped = true;
            append(generation, "stop", Map.of());
        }
    }

    /** Adversarial test hooks inside the REAL producer, not a replacement producer or offset store. */
    public static class BlockingInterceptor implements ProducerInterceptor<byte[], byte[]> {
        private final ConcurrentLinkedQueue<Map<String, Object>> sent = new ConcurrentLinkedQueue<>();

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
            if (record.topic().startsWith("events-")) {
                try {
                    byte[] diagnostics = record.headers().lastHeader("probe") == null ? record.value() : record.headers().lastHeader("probe").value();
                    Map<String, Object> value = JSON.readValue(diagnostics, new TypeReference<Map<String, Object>>() {
                    });
                    sent.add(value);
                    gate(value, "send");
                }
                catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            if (metadata != null && metadata.topic().startsWith("events-")) {
                Map<String, Object> value = sent.poll();
                if (value != null) {
                    String generation = value.get("generation").toString();
                    append(generation, "producerAck", Map.of("value", value, "error", exception == null ? "" : exception.getClass().getName()));
                    gate(value, "ack");
                }
            }
        }

        private void gate(Map<String, Object> value, String kind) {
            String generation = value.get("generation").toString();
            Path block = CONTROL.resolve(generation + "." + kind + "-block");
            try {
                if (!Files.exists(block) || !Files.readString(block).trim().equals(value.get("index").toString())) {
                    return;
                }
                write(generation + "." + kind + "-blocked.json", value);
                long deadline = System.nanoTime() + Duration.ofMinutes(4).toNanos();
                while (!Files.exists(CONTROL.resolve(generation + "." + kind + "-release"))) {
                    if (System.nanoTime() > deadline) {
                        write(generation + ".hook-timeout.json", value);
                        throw new IllegalStateException("Fault injection gate timed out");
                    }
                    try {
                        Thread.sleep(25);
                    }
                    catch (InterruptedException e) {
                        // Deliberately emulate an uncooperative old task during the bounded experiment.
                    }
                }
                write(generation + "." + kind + "-released.json", value);
            }
            catch (java.io.IOException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public void close() {
        }
    }

    static JournalPosition pos(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), RECEIVER);
    }

    static List<Event> events() {
        return List.of(event(2, "C/SC", 10, 0), event(3, "D/CG", 10, 8), event(4, "R/PT", 10, 1), event(5, "R/PT", 10, 2),
                event(6, "C/CM", 10, 0), event(7, "C/SC", 20, 0), event(8, "R/PT", 20, 3), event(9, "C/CM", 20, 0));
    }

    static Event event(long sequence, String operation, int cycle, int value) {
        byte[] payload = new byte[0];
        if (operation.startsWith("R/")) {
            payload = ByteBuffer.allocate(4).putInt(value).array();
        }
        else if (operation.equals("D/CG")) {
            payload = new byte[109];
            new AS400Text(10, 37).toBytes("TEST", payload, 0);
            new AS400Text(10, 37).toBytes("LIB", payload, 10);
            new AS400Text(10, 37).toBytes("", payload, 20);
            new AS400Text(1, 37).toBytes(Integer.toString(value), payload, 108);
        }
        byte[] data = new byte[256 + payload.length];
        new AS400Text(5, 37).toBytes(String.format("%05d", payload.length), data, 240);
        System.arraycopy(payload, 0, data, 256, payload.length);
        EntryHeader header = new EntryHeader(data.length, 0, payload.length == 0 ? 0 : 240, BigInteger.valueOf(sequence), BigInteger.ONE,
                Instant.ofEpochSecond(sequence), operation.charAt(0), operation.substring(2),
                "TEST      LIB       " + (operation.equals("D/CG") ? "          " : "TEST      "), BigInteger.valueOf(cycle),
                data.length, 0, RECEIVER.name(), RECEIVER.library(),
                new EntryHeader.ControlInfo(operation.equals("R/UB") || operation.equals("R/DL") ? '1' : '0', 0, false, false, (byte) 0),
                new EntryHeader.RecordInfo(BigInteger.ONE, BigInteger.ONE, "JOB", "USER", "123456",
                        operation.startsWith("R/") ? As400ConnectProbeConnector.MEMBER_JID : JID, '1', operation.startsWith("R/") ? "*QDDS" : "*FILE", "SYS", "PGM",
                        "LIB", "USER"));
        return new Event(header, data, pos(sequence));
    }

    record Event(EntryHeader header, byte[] data, JournalPosition position) {
    }

    static synchronized void append(String generation, String event, Map<String, ?> fields) {
        try {
            Files.writeString(CONTROL.resolve(generation + ".jsonl"),
                    JSON.writeValueAsString(Map.of("event", event, "at", Instant.now().toString(), "fields", fields)) + "\n",
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
        catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    static void write(String name, Object value) {
        try {
            Path temporary = CONTROL.resolve(name + ".tmp");
            Files.writeString(temporary, JSON.writeValueAsString(value));
            Files.move(temporary, CONTROL.resolve(name), java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        }
        catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Bounded snapshot from the actual broker, including the actual Connect offset topic. */
    public static void main(String[] args) throws Exception {
        if (args.length == 3 && (args[0].equals("rest") || args[0].equals("rest-put"))) {
            {
                HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
                HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:8083" + args[1]))
                        .timeout(Duration.ofSeconds(10)).header("Content-Type", "application/json");
                if (!args[2].equals("GET")) {
                    request.method(args[0].equals("rest-put") ? "PUT" : "POST", HttpRequest.BodyPublishers.ofString(args[2]));
                }
                HttpResponse<String> response = client.send(request.build(), HttpResponse.BodyHandlers.ofString());
                System.out.println(JSON.writeValueAsString(Map.of("status", response.statusCode(), "body", response.body())));
            }
            return;
        }
        if ((args.length != 4 && args.length != 5) || !List.of("read_committed", "read_uncommitted").contains(args[2])
                || args.length == 5 && !args[4].equals("latest-per-key")) {
            throw new IllegalArgumentException("bootstrap topic isolation output-json [latest-per-key]");
        }
        Map<String, Object> config = Map.of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args[0], ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, args[2], ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        SnapshotRows records = new SnapshotRows(args.length == 5);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            TopicPartition partition = new TopicPartition(args[1], 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (consumer.position(partition) < end) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("Snapshot bound exceeded");
                }
                consumer.poll(Duration.ofMillis(200)).forEach(record -> records.accept(record, end));
            }
        }
        JSON.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[3]).toFile(), records.rows());
    }

    /** Latest offsets need bounded retained state, not every obsolete offset value. */
    static final class SnapshotRows {
        private final boolean latestPerKey;
        private final Map<Object, Map<String, Object>> records = new LinkedHashMap<>();
        private long retainedCharacters;
        private int scanned;

        SnapshotRows(boolean latestPerKey) {
            this.latestPerKey = latestPerKey;
        }

        void accept(ConsumerRecord<String, String> record, long end) {
            if (record.offset() >= end) {
                return;
            }
            if (++scanned > 100_000 || latestPerKey && record.key() == null) {
                throw new IllegalStateException("Snapshot scan bound or offset key violated");
            }
            Object identity = latestPerKey ? record.key() : record.offset();
            Map<String, Object> previous = records.get(identity);
            String probe = record.headers().lastHeader("probe") == null ? null
                    : new String(record.headers().lastHeader("probe").value(), java.nio.charset.StandardCharsets.UTF_8);
            long characters = retainedCharacters - characters(previous) + length(record.key()) + length(record.value()) + length(probe);
            if (previous == null && records.size() >= 1000 || characters > 8 * 1024 * 1024) {
                throw new IllegalStateException("Snapshot retained-state bound exceeded");
            }
            Map<String, Object> row = new HashMap<>();
            row.put("offset", record.offset());
            row.put("key", record.key());
            row.put("value", record.value());
            row.put("timestamp", record.timestamp());
            if (probe != null) {
                row.put("probe", probe);
            }
            records.put(identity, row);
            retainedCharacters = characters;
        }

        List<Map<String, Object>> rows() {
            return new ArrayList<>(records.values());
        }

        private static long characters(Map<String, Object> row) {
            return row == null ? 0 : length((String) row.get("key")) + length((String) row.get("value")) + length((String) row.get("probe"));
        }

        private static long length(String value) {
            return value == null ? 0 : value.length();
        }
    }
}
