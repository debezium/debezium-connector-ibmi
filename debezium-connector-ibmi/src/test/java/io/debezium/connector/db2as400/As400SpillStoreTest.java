/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.connector.db2as400.As400TransactionBuffer.Image;
import io.debezium.connector.db2as400.As400TransactionBuffer.RecordIdentity;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Control;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Kind;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Outcome;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader.ControlInfo;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader.RecordInfo;
import io.debezium.relational.TableId;

class As400SpillStoreTest {
    @TempDir
    Path directory;

    @Test
    void shouldPreserveOwnedRowTypesAndExactSourceMetadataAfterSpill() {
        Timestamp timestamp = Timestamp.valueOf("2026-01-02 03:04:05.123456789");
        Object[] values = { null, new byte[]{ 1, 2, 3 }, "text\u0000\ud800\ud83d\ude80", new BigDecimal("123456789.00100"), BigInteger.ONE.shiftLeft(63),
                timestamp, LocalDate.of(2026, 1, 2), LocalDateTime.of(2026, 1, 2, 3, 4, 5, 987654321), Duration.ofHours(24),
                (byte) 1, (short) 2, 3, 4L, -0.0f, -0.0d, true, new java.sql.Date(12345), new java.sql.Time(67890), new java.util.Date(123456789) };
        var metadata = new As400SourceMetadata(position(42), Instant.ofEpochSecond(12345, 6789), BigInteger.ONE.shiftLeft(63), BigInteger.TEN, 'R', "UB",
                new RecordInfo(BigInteger.TWO, BigInteger.valueOf(3), "JOB", "USER", "123456", "0102030405060708090a", '1', "*QDDS", "SYS", "PGM", "LIB", "PROFILE"),
                new ControlInfo('1', 2, false, false, (byte) 0x10));
        try (var store = store(8 * 1024 * 1024)) {
            List<Image> images = store.list("images", As400SpillCodec.IMAGE);
            images.add(new Image(metadata, As400RowData.copyOf(values, 65536)));
            images.add(new Image(metadata, As400RowData.excluded()));
            store.flush();
            Image read = images.get(0);
            assertThat(read.source()).isEqualTo(metadata);
            assertThat(read.values()).containsExactly(values);
            ((byte[]) read.values()[1])[0] = 99;
            timestamp.setNanos(0);
            assertThat((byte[]) images.get(0).values()[1]).containsExactly(1, 2, 3);
            assertThat(((Timestamp) images.get(0).values()[5]).getNanos()).isEqualTo(123456789);
            assertThat(images.get(1).row().included()).isFalse();
            assertThatThrownBy(() -> images.get(1).values()).isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void shouldSpillTheNativeColumnLimitIncludingAnExplicitRrnKey() {
        Object[] physical = new Object[8000];
        for (int i = 0; i < physical.length; i++) {
            physical[i] = (short) i;
        }
        Object[] output = As400BindingDefinition.withRrn(physical, BigInteger.TEN);
        try (var store = store(8 * 1024 * 1024)) {
            var images = store.list("wide-rows", As400SpillCodec.IMAGE);
            images.add(new Image(source(42, "PT"), As400RowData.copyOf(output, 8001 * 8)));
            store.flush();
            assertThat(images.get(0).values()).containsExactly(output);
            assertThatThrownBy(() -> As400RowData.copyOf(output, 8001 * 8 - 1)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> As400RowData.copyOf(new Object[8002], 65536)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void shouldPreserveConnectSchemasDefaultsHeadersAndTombstones() {
        byte[] defaultBytes = { 7, 8 };
        Schema schema = SchemaBuilder.struct().name("record").doc("schema documentation").version(2).parameter("custom", "value")
                .field("amount", Decimal.schema(3))
                .field("defaulted", SchemaBuilder.string().defaultValue("fallback").build())
                .field("optional", Schema.OPTIONAL_STRING_SCHEMA)
                .field("defaultBytes", SchemaBuilder.bytes().defaultValue(defaultBytes).build())
                .field("array", SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).build())
                .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.OPTIONAL_INT64_SCHEMA).build())
                .field("bytes", Schema.BYTES_SCHEMA).build();
        Struct value = new Struct(schema).put("amount", new BigDecimal("123.450")).put("array", java.util.Arrays.asList(1, null, 2))
                .put("map", java.util.Collections.singletonMap("nullable", null)).put("bytes", ByteBuffer.wrap(new byte[]{ 1, 2, 3 }));
        SourceRecord record = new SourceRecord(Map.of("server", "test"), Map.of("position", "1"), "topic", 2,
                Schema.INT32_SCHEMA, 7, schema, value, 42L);
        record.headers().addString("duplicate", "first").addString("duplicate", "second").add("struct", value, schema);
        var restored = As400SourceRecordCodec.RECORD.decode(As400SourceRecordCodec.RECORD.encode(record));
        assertThat(restored).isEqualTo(record);
        assertThat(((Struct) restored.value()).getWithoutDefault("defaulted")).isNull();
        assertThat(((Struct) restored.value()).get("defaulted")).isEqualTo("fallback");
        var tombstone = new SourceRecord(record.sourcePartition(), record.sourceOffset(), "topic", Schema.INT32_SCHEMA, 7, null, null);
        assertThat(As400SourceRecordCodec.RECORD.decode(As400SourceRecordCodec.RECORD.encode(tombstone))).isEqualTo(tombstone);
        try (var store = store(8 * 1024 * 1024)) {
            var codec = As400SourceRecordCodec.stored(store);
            var records = store.list("output", codec);
            records.add(record);
            records.add(tombstone);
            store.flush();
            assertThat(records).containsExactly(record, tombstone);
            defaultBytes[0] = 99;
            assertThat((byte[]) records.get(0).valueSchema().field("defaultBytes").schema().defaultValue()).containsExactly(7, 8);
        }
    }

    @Test
    void shouldKeepNamespacesAndInsertionOrderWhileUpdatingAndRemovingAcrossFlushes() throws Exception {
        Path sibling = Files.writeString(directory.resolve("unrelated"), "preserve");
        Path owned;
        try (var store = store(8 * 1024 * 1024); var replacement = store(8 * 1024 * 1024)) {
            owned = store.directory();
            var map = store.map("same", As400SpillCodec.STRING, As400SpillCodec.INT);
            var other = replacement.map("same", As400SpillCodec.STRING, As400SpillCodec.INT);
            map.put("b", 1);
            map.put("a", 2);
            other.put("a", 99);
            store.flush();
            map.put("b", 3);
            map.put("c", 4);
            var iterator = map.entrySet().iterator();
            assertThat(iterator.next()).isEqualTo(Map.entry("b", 3));
            iterator.remove();
            store.flush();
            assertThat(iterator.next()).isEqualTo(Map.entry("a", 2));
            assertThat(iterator.next()).isEqualTo(Map.entry("c", 4));
            assertThat(iterator.hasNext()).isFalse();
            assertThat(other).containsExactlyEntriesOf(Map.of("a", 99));
        }
        assertThat(owned).doesNotExist();
        assertThat(sibling).hasContent("preserve");
    }

    @Test
    void shouldStopWithoutEvictionWhenDiskBudgetIsExhaustedAndRemoveOnlyOwnedFiles() throws Exception {
        Path sibling = Files.writeString(directory.resolve("keep"), "keep");
        Path owned;
        try (var store = store(128 * 1024)) {
            owned = store.directory();
            var values = store.list("data", As400SpillCodec.BYTES);
            byte[] random = new byte[32768];
            new Random(7).nextBytes(random);
            assertThatThrownBy(() -> {
                for (int i = 0; i < 20; i++) {
                    values.add(random);
                }
            }).isInstanceOf(As400SpillStore.SpillException.class).hasMessageContaining("replay from the durable offset");
            assertThatThrownBy(() -> values.get(0)).isInstanceOf(As400SpillStore.SpillException.class);
        }
        assertThat(owned).doesNotExist();
        assertThat(sibling).hasContent("keep");
    }

    @Test
    void shouldRejectMalformedAndUnsupportedSpillValues() {
        byte[] truncated = ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array();
        assertThatThrownBy(() -> As400SpillCodec.STRING.decode(truncated)).isInstanceOf(As400SpillStore.SpillException.class);
        var unsupported = new SourceRecord(Map.of("server", "test"), Map.of(), "topic", null, new Object());
        assertThatThrownBy(() -> As400SourceRecordCodec.RECORD.encode(unsupported)).isInstanceOf(As400SpillStore.SpillException.class);
    }

    @Test
    void shouldPrepareTransactionLargerThanHeapWithoutKeepingItsRowsOrOutputInMemory() throws Exception {
        Path log = directory.resolve("small-heap.log");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx64m", "-ea", "-cp",
                System.getProperty("java.class.path"), LargeTransaction.class.getName(), directory.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertThat(process.waitFor(60, TimeUnit.SECONDS)).as("small-heap child finished: %s", log).isTrue();
            assertThat(process.exitValue()).as(Files.readString(log)).isZero();
            assertThat(log).content().contains("rows=4096", "payloadBytes=134217728", "heapBytes=67108864", "complete");
            try (var files = Files.list(directory)) {
                assertThat(files.map(p -> p.getFileName().toString()).toList()).containsExactly("small-heap.log");
            }
        }
        finally {
            if (process.isAlive()) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        }
    }

    private As400SpillStore store(long budget) {
        return new As400SpillStore(new As400SpillStore.Settings(directory, 1, 16, budget));
    }

    private static JournalPosition position(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), new JournalReceiver("RCV", "LIB"));
    }

    private static As400SourceMetadata source(long sequence, String type) {
        return new As400SourceMetadata(position(sequence), Instant.EPOCH, BigInteger.ONE, BigInteger.TEN,
                type.equals("PT") ? 'R' : 'C', type, RecordInfo.UNKNOWN, new ControlInfo('0', 0, false, false, (byte) 0));
    }

    public static final class LargeTransaction {
        public static void main(String[] args) {
            long started = System.nanoTime();
            boolean disk = args.length < 2 || !args[1].equals("memory");
            int rows = args.length < 3 ? 4096 : Integer.parseInt(args[2]);
            int rowBytes = args.length < 4 ? 32768 : Integer.parseInt(args[3]);
            var settings = new As400SpillStore.Settings(Path.of(args[0]), 2, 256, 1024L * 1024 * 1024);
            try (var store = disk ? new As400SpillStore(settings) : null;
                    var buffer = new As400TransactionBuffer("journal", new As400TransactionBuffer.Limits(30000, 30000, 30000, 512L * 1024 * 1024), store)) {
                var start = source(1, "SC");
                buffer.acceptControl(start, new Control("SC", Kind.START, BigInteger.TEN, start.controlInfo(), Outcome.NONE, null));
                var table = new TableIdentity(new TableId("DB", "LIB", "TEST"), "TEST", "TEST", "0102030405060708090a", "1112131415161718191a", "FORMAT");
                var random = new Random(1);
                byte[] bytes = new byte[rowBytes];
                for (int i = 0; i < rows; i++) {
                    random.nextBytes(bytes);
                    ByteBuffer.wrap(bytes).putInt(i);
                    buffer.insert(source(i + 2, "PT"), new RecordIdentity(table, "job/thread", BigInteger.valueOf(i + 1)),
                            As400RowData.copyOf(new Object[]{ bytes }, 65536));
                }
                long buffered = System.nanoTime();
                var commit = source(rows + 2, "CM");
                var batch = buffer.acceptControl(commit, new Control("CM", Kind.COMMIT, BigInteger.TEN, commit.controlInfo(), Outcome.DURABLE_COMMIT, null))
                        .orElseThrow();
                List<SourceRecord> output = store == null ? new ArrayList<>() : store.list("prepared", As400SourceRecordCodec.stored(store));
                for (var change : batch.changes()) {
                    output.add(new SourceRecord(Map.of("server", "test"), Map.of("completed_through.sequence", "0"), "topic", Schema.BYTES_SCHEMA,
                            change.after().values()[0]));
                }
                if (store != null) {
                    store.flush();
                }
                long prepared = System.nanoTime();
                int index = 0;
                for (var record : output) {
                    if (ByteBuffer.wrap((byte[]) record.value()).getInt() != index++) {
                        throw new AssertionError("Spill changed source operation order");
                    }
                }
                if (index != rows || buffer.usage().payloadBytes() != (long) rows * rowBytes
                        || args.length == 1 && Runtime.getRuntime().maxMemory() > 80L * 1024 * 1024) {
                    throw new AssertionError("Incorrect constrained-heap test size");
                }
                System.out.println("mode=" + (disk ? "disk" : "memory") + " rows=" + index + " payloadBytes=" + buffer.usage().payloadBytes()
                        + " heapBytes=" + Runtime.getRuntime().maxMemory() + " store=" + (store == null ? "none" : store.usage())
                        + " bufferMillis=" + (buffered - started) / 1_000_000 + " prepareMillis=" + (prepared - buffered) / 1_000_000
                        + " readMillis=" + (System.nanoTime() - prepared) / 1_000_000);
                output.clear();
                buffer.completeBatch(batch);
                if (buffer.usage().transactions() != 0 || buffer.oldestTransactionPosition().isPresent()) {
                    throw new AssertionError("Completed transaction remains retained");
                }
            }
            System.out.println("complete");
        }
    }
}
