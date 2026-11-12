/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400JournalCaptureTest {
    private static final RawJournalPage.Entry TEMPLATE = template();
    @TempDir
    Path scratch;

    @ParameterizedTest
    @ValueSource(ints = { 2, 40 })
    void shouldPreserveExactOwnedBytesAcrossTheBackgroundToForegroundHandoff(int count) throws Exception {
        var released = new AtomicInteger();
        try (var capture = new As400JournalCapture(scratch, 8 * 1024 * 1024, () -> {
        }, released::incrementAndGet)) {
            for (int i = 0; i < count; i++) {
                capture.append(entry(i, 32768));
            }
            var entries = capture.seal();
            var executor = java.util.concurrent.Executors.newSingleThreadExecutor();
            try {
                executor.submit(() -> {
                    for (int i = count - 1; i >= 0; i--) {
                        var expected = entry(i, 32768);
                        assertThat(entries.get(i).position()).isEqualTo(expected.position());
                        assertThat(entries.get(i).data()).isEqualTo(expected.data());
                    }
                    return null;
                }).get(10, TimeUnit.SECONDS);
            }
            finally {
                executor.shutdownNow();
            }
            assertThrows(IllegalStateException.class, () -> capture.append(entry(50, 32768)));
            assertThrows(UnsupportedOperationException.class, entries::clear);
            try (var children = Files.list(scratch)) {
                assertThat(children.count()).isEqualTo(count == 2 ? 0 : 1);
            }
        }
        assertThat(released).hasValue(1);
        assertEmpty(scratch);
    }

    @ParameterizedTest
    @ValueSource(strings = { "content", "index", "truncated" })
    void shouldRejectCorruptOrTruncatedScratchInsteadOfReturningDifferentEvidence(String fault) throws Exception {
        try (var capture = new As400JournalCapture(scratch, 8 * 1024 * 1024, () -> {
        }, () -> {
        })) {
            for (int i = 0; i < 40; i++) {
                capture.append(entry(i, 32768));
            }
            var entries = capture.seal();
            Path owned;
            try (var directories = Files.list(scratch)) {
                owned = directories.findFirst().orElseThrow();
            }
            try (var file = new RandomAccessFile(owned.resolve(fault.equals("index") ? "index" : "records").toFile(), "rw")) {
                if (fault.equals("truncated")) {
                    file.setLength(8);
                }
                else if (fault.equals("index")) {
                    file.seek(8);
                    long second = file.readLong();
                    file.seek(0);
                    file.writeLong(second);
                }
                else {
                    file.seek(16);
                    file.writeByte(255);
                }
            }
            assertThrows(UncheckedIOException.class, () -> entries.get(0));
            assertThrows(IllegalStateException.class, () -> entries.get(1));
        }
        assertEmpty(scratch);
    }

    @Test
    void shouldRejectDiskExhaustionAndStopWithoutExposingPartialCaptures() throws Exception {
        var stopped = new AtomicBoolean();
        try (var capture = new As400JournalCapture(scratch, 2 * 1024 * 1024, () -> {
            if (stopped.get()) {
                throw new IllegalStateException("Stopped");
            }
        }, () -> {
        })) {
            assertThrows(IllegalStateException.class, capture::entries);
            assertThrows(As400JournalCapture.CaptureException.class, () -> {
                for (int i = 0; i < 100; i++) {
                    capture.append(entry(i, 32768));
                }
            });
            assertThrows(IllegalStateException.class, capture::seal);
        }
        try (var capture = new As400JournalCapture(scratch, 2 * 1024 * 1024, () -> {
            if (stopped.get()) {
                throw new IllegalStateException("Stopped");
            }
        }, () -> {
        })) {
            capture.append(entry(0, 32768));
            var entries = capture.seal();
            stopped.set(true);
            assertThrows(IllegalStateException.class, () -> entries.get(0));
        }
        assertEmpty(scratch);
    }

    @Test
    void shouldReplayAnAuditLargerThanTheHeapWithBoundedWorkingMemory() throws Exception {
        Path log = scratch.resolve("capture.log");
        Process process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-Xmx64m", "-ea", "-cp",
                System.getProperty("java.class.path"), LargeCapture.class.getName(), scratch.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertThat(process.waitFor(90, TimeUnit.SECONDS)).as("child completed: %s", log).isTrue();
            assertThat(process.exitValue()).as(Files.readString(log)).isZero();
        }
        finally {
            process.destroyForcibly();
        }
        Files.delete(log);
        assertEmpty(scratch);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReleaseUnconsumedBackgroundCapturesWhenNativeSessionCloses(boolean failedDrain) throws Exception {
        var config = io.debezium.config.Configuration.from(As400DcgConfigTest.properties()).edit()
                .with("truncate.buffer.directory", scratch.toString()).build();
        try (var readers = org.mockito.Mockito.mockConstruction(io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal.class)) {
            var io = new As400DcgNativeIo(new As400DcgConfig(config));
            var capture = io.capture(() -> {
            });
            for (int i = 0; i < 40; i++) {
                capture.append(entry(i, 32768));
            }
            var entries = capture.seal();
            try (var children = Files.list(scratch)) {
                assertThat(children.count()).isEqualTo(1);
            }
            if (failedDrain) {
                io.failedCleanup(new IOException("Native cleanup failed"));
                assertThrows(IllegalStateException.class, io::close);
            }
            else {
                io.close();
            }
            assertThrows(IllegalStateException.class, () -> entries.get(0));
            assertThrows(IOException.class, () -> io.capture(() -> {
            }));
            assertEmpty(scratch);
        }
    }

    public static final class LargeCapture {
        public static void main(String[] args) throws Exception {
            try (var capture = new As400JournalCapture(Path.of(args[0]), 256L * 1024 * 1024, () -> {
            }, () -> {
            })) {
                for (int i = 0; i < 4096; i++) {
                    capture.append(entry(i, 32768));
                }
                var entries = capture.seal();
                for (int i = 0; i < entries.size(); i++) {
                    var raw = entries.get(i);
                    if (raw.position().offset().intValueExact() != i + 1 || raw.data()[32767] != (byte) i) {
                        throw new AssertionError("Capture changed");
                    }
                }
            }
        }
    }

    private static RawJournalPage.Entry entry(int i, int length) {
        // Grow captured bytes only to exercise opaque storage. These synthetic
        // payloads are not claimed to be valid native row images or boundaries.
        byte[] raw = Arrays.copyOf(TEMPLATE.data(), length);
        ByteBuffer.wrap(raw).putLong(24, i + 1L);
        raw[length - 1] = (byte) i;
        var position = new JournalPosition(BigInteger.valueOf(i + 1L), TEMPLATE.position().receiver());
        return new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(raw, 0), raw);
    }

    private static RawJournalPage.Entry template() {
        try (var stream = As400JournalCaptureTest.class.getResourceAsStream("/native-transactions/prospective-boundary.json")) {
            var node = new ObjectMapper().readTree(stream).path("boundary").path("raw").path("entries").get(0);
            byte[] raw = Base64.getDecoder().decode(node.path("raw").asText());
            var position = As400OnlineHandoff.position(node.path("position"));
            return new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(raw, 0), raw);
        }
        catch (Exception error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    private static void assertEmpty(Path directory) throws IOException {
        try (var children = Files.list(directory)) {
            assertThat(children).isEmpty();
        }
    }
}
