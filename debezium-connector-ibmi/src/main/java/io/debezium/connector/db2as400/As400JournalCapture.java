/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Owned immutable raw evidence after sealing. Large captures spill to private
 * scratch files, never reused as recovery authority. Positional reads permit a
 * completed background capture to be consumed by the foreground task.
 */
final class As400JournalCapture implements AutoCloseable {
    /** Owned immutable view whose owner must outlive every consumer. */
    abstract static class Entries extends AbstractList<RawJournalPage.Entry> {
    }

    static List<RawJournalPage.Entry> owned(List<RawJournalPage.Entry> entries) {
        return entries instanceof Entries ? entries : List.copyOf(entries);
    }

    static final class CaptureException extends UncheckedIOException {
        CaptureException(String message, IOException cause) {
            super(message, cause);
        }
    }

    static final int MEMORY_BYTES = 1024 * 1024;
    private static final int MAX_FRAME = MEMORY_BYTES + 128;
    private static final int DIGEST_BYTES = 32;
    private final Path parent;
    private final long maximumBytes;
    private final Runnable checkRunning;
    private final Runnable released;
    private final List<RawJournalPage.Entry> memory = new ArrayList<>();
    private final MessageDigest digest = sha256();
    private Path directory;
    private FileChannel data;
    private FileChannel index;
    private long bytes;
    private long dataBytes;
    private int size;
    private boolean sealed;
    private boolean closed;
    private boolean failed;
    private final List<RawJournalPage.Entry> entries = new Entries() {
        @Override
        public RawJournalPage.Entry get(int at) {
            return read(at);
        }

        @Override
        public int size() {
            synchronized (As400JournalCapture.this) {
                check();
                return size;
            }
        }
    };

    As400JournalCapture(Path parent, long maximumBytes, Runnable checkRunning, Runnable released) {
        this.parent = Objects.requireNonNull(parent);
        this.maximumBytes = maximumBytes;
        this.checkRunning = Objects.requireNonNull(checkRunning);
        this.released = Objects.requireNonNull(released);
        if (!parent.isAbsolute() || maximumBytes < 65536) {
            throw new IllegalArgumentException("Capture requires an absolute scratch directory and at least 64 KiB of storage");
        }
    }

    synchronized void append(RawJournalPage.Entry entry) throws IOException {
        check();
        if (sealed || size == Integer.MAX_VALUE) {
            throw new IllegalStateException("Capture is sealed or its index is exhausted");
        }
        byte[] raw = entry.data();
        if (raw.length > MEMORY_BYTES) {
            throw new IllegalArgumentException("Raw journal entry exceeds the native read buffer");
        }
        byte[] frame = encode(size, entry.position(), raw);
        long added = Integer.BYTES + frame.length + DIGEST_BYTES + Long.BYTES;
        if (added > maximumBytes - bytes) {
            failed = true;
            throw new CaptureException("Native journal capture exceeds its scratch disk budget", new IOException("Scratch budget exhausted"));
        }
        try {
            if (directory == null && bytes + added > MEMORY_BYTES) {
                spill();
            }
            if (directory == null) {
                memory.add(entry);
            }
            else {
                write(size, frame);
            }
            bytes += added;
            size++;
            check();
        }
        catch (IOException failure) {
            failed = true;
            throw new CaptureException("Cannot store native journal capture", failure);
        }
        catch (RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    synchronized List<RawJournalPage.Entry> seal() throws IOException {
        check();
        if (data != null) {
            // Seal only after all bytes have reached the file. This is scratch
            // durability, never acknowledgement of source records or an offset.
            try {
                data.force(false);
                index.force(false);
            }
            catch (IOException failure) {
                failed = true;
                throw new CaptureException("Cannot seal native journal capture", failure);
            }
        }
        sealed = true;
        return entries;
    }

    synchronized List<RawJournalPage.Entry> entries() {
        check();
        if (!sealed) {
            throw new IllegalStateException("Unsealed raw capture cannot escape");
        }
        return entries;
    }

    private void spill() throws IOException {
        directory = Files.getFileStore(parent).supportsFileAttributeView("posix")
                ? Files.createTempDirectory(parent, "ibmi-journal-", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")))
                : Files.createTempDirectory(parent, "ibmi-journal-");
        data = FileChannel.open(directory.resolve("records"), StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        index = FileChannel.open(directory.resolve("index"), StandardOpenOption.CREATE_NEW, StandardOpenOption.READ, StandardOpenOption.WRITE);
        for (int i = 0; i < memory.size(); i++) {
            check();
            var entry = memory.get(i);
            write(i, encode(i, entry.position(), entry.data()));
        }
        memory.clear();
    }

    private void write(int ordinal, byte[] frame) throws IOException {
        writeFully(index, ByteBuffer.allocate(Long.BYTES).putLong(dataBytes).flip(), (long) ordinal * Long.BYTES);
        var record = ByteBuffer.allocate(Integer.BYTES + frame.length + DIGEST_BYTES).putInt(frame.length).put(frame).put(digest.digest(frame)).flip();
        int length = record.remaining();
        writeFully(data, record, dataBytes);
        dataBytes += length;
    }

    private synchronized RawJournalPage.Entry read(int ordinal) {
        check();
        if (!sealed) {
            throw new IllegalStateException("Unsealed capture read");
        }
        Objects.checkIndex(ordinal, size);
        if (data == null) {
            return memory.get(ordinal);
        }
        try {
            long offset = readFully(index, Long.BYTES, (long) ordinal * Long.BYTES).getLong();
            if (offset < 0 || offset > dataBytes - Integer.BYTES - DIGEST_BYTES) {
                throw new IOException("Invalid capture index");
            }
            int length = readFully(data, Integer.BYTES, offset).getInt();
            if (length <= 0 || length > MAX_FRAME || offset + Integer.BYTES + length + DIGEST_BYTES > dataBytes) {
                throw new IOException("Invalid capture frame");
            }
            var record = readFully(data, length + DIGEST_BYTES, offset + Integer.BYTES);
            byte[] frame = new byte[length];
            byte[] expected = new byte[DIGEST_BYTES];
            record.get(frame).get(expected);
            if (!MessageDigest.isEqual(digest.digest(frame), expected)) {
                throw new IOException("Corrupt raw journal capture");
            }
            try (var input = new DataInputStream(new ByteArrayInputStream(frame))) {
                if (input.readInt() != ordinal) {
                    throw new IOException("Reordered raw journal capture");
                }
                var position = new JournalPosition(new BigInteger(input.readUTF()), new JournalReceiver(input.readUTF(), input.readUTF()));
                int count = input.readInt();
                if (count <= 0 || count > MEMORY_BYTES || count != input.available()) {
                    throw new IOException("Invalid raw capture length");
                }
                byte[] raw = input.readNBytes(count);
                var result = new RawJournalPage.Entry(position, new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(raw, 0), raw);
                check();
                return result;
            }
        }
        catch (IOException failure) {
            failed = true;
            throw new CaptureException("Cannot read retained native capture", failure);
        }
        catch (RuntimeException failure) {
            failed = true;
            throw failure;
        }
    }

    private static byte[] encode(int ordinal, JournalPosition position, byte[] raw) throws IOException {
        var bytes = new ByteArrayOutputStream();
        try (var output = new DataOutputStream(bytes)) {
            output.writeInt(ordinal);
            output.writeUTF(position.offset().toString());
            output.writeUTF(position.receiver().name());
            output.writeUTF(position.receiver().library());
            output.writeInt(raw.length);
            output.write(raw);
        }
        return bytes.toByteArray();
    }

    private void writeFully(FileChannel channel, ByteBuffer buffer, long offset) throws IOException {
        while (buffer.hasRemaining()) {
            check();
            int count = channel.write(buffer, offset);
            if (count <= 0) {
                throw new IOException("Incomplete capture write");
            }
            offset += count;
        }
    }

    private ByteBuffer readFully(FileChannel channel, int count, long offset) throws IOException {
        var buffer = ByteBuffer.allocate(count);
        while (buffer.hasRemaining()) {
            check();
            int read = channel.read(buffer, offset);
            if (read <= 0) {
                throw new IOException("Truncated raw journal capture");
            }
            offset += read;
        }
        return buffer.flip();
    }

    private void check() {
        checkRunning.run();
        if (closed || failed) {
            throw new IllegalStateException("Raw journal capture is closed or failed");
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        memory.clear();
        IOException failure = null;
        for (var channel : Arrays.asList(data, index)) {
            if (channel != null) {
                try {
                    channel.close();
                }
                catch (IOException error) {
                    if (failure == null) {
                        failure = error;
                    }
                    else {
                        failure.addSuppressed(error);
                    }
                }
            }
        }
        try {
            if (directory != null) {
                Files.deleteIfExists(directory.resolve("records"));
                Files.deleteIfExists(directory.resolve("index"));
                Files.delete(directory);
            }
        }
        catch (IOException error) {
            if (failure == null) {
                failure = error;
            }
            else {
                failure.addSuppressed(error);
            }
        }
        finally {
            released.run();
        }
        if (failure != null) {
            throw failure;
        }
    }
}
