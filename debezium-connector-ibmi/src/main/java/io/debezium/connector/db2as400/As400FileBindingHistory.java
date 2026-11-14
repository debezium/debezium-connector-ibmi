/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.function.Consumer;

import io.debezium.config.Configuration;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.storage.file.history.FileSchemaHistory;

/**
 * Standard file history format/configuration with bounded, non-skipping recovery
 * and forced writes. Locks serialize file operations, not connector ownership.
 * The file is durable recovery history, never disposable task-local spill state.
 */
final class As400FileBindingHistory extends FileSchemaHistory implements As400BindingHistory.BoundedRecovery {
    private Path path;
    private boolean starting;
    private boolean candidateAppends;
    private boolean pagedCandidates;
    private FileChannel batchChannel;
    private boolean eligibleLegacyRecord;
    private As400BindingHistory.HistoryBudget budget = new As400BindingHistory.HistoryBudget();

    static Path configuredPath(Configuration config) {
        String value = config.getString(FILE_PATH);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("File binding history requires schema.history.internal.file.filename");
        }
        Path result = Path.of(value).toAbsolutePath().normalize();
        if (result.getParent() == null) {
            throw new IllegalArgumentException("Binding history must name a file");
        }
        return result;
    }

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator, SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        path = configuredPath(config);
        candidateAppends = false;
        pagedCandidates = false;
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
    }

    @Override
    public void allowCandidateAppends() {
        candidateAppends = true;
    }

    @Override
    public void allowPagedCandidates() {
        candidateAppends = true;
        pagedCandidates = true;
    }

    @Override
    public void appendBatch(Runnable records) {
        require(running.get() && pagedCandidates && batchChannel == null, "Active paged history writer required");
        var before = attributes();
        require(before != null, "Binding history disappeared before append");
        try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                var held = acquire(channel, false)) {
            // Validate the existing file once, then append bounded records under
            // one lock. No O(n squared) rescans or force per table are needed.
            read(channel, ignored -> {
            }, true);
            unchanged(before, channel);
            channel.position(channel.size());
            batchChannel = channel;
            try {
                records.run();
                channel.force(true);
                forceDirectory(path.getParent());
            }
            finally {
                batchChannel = null;
            }
        }
        catch (IOException failure) {
            throw new SchemaHistoryException("Cannot durably append binding pages", failure);
        }
    }

    @Override
    public As400BindingHistory.HistoryBudget recoveryBudget() {
        return budget;
    }

    @Override
    public boolean eligibleLegacyRecord() {
        return eligibleLegacyRecord;
    }

    @Override
    public synchronized void start() {
        starting = true;
        try {
            super.start();
        }
        catch (RuntimeException failure) {
            running.set(false);
            throw failure;
        }
        finally {
            starting = false;
        }
    }

    @Override
    protected void doStart() {
        // Do not preload the base class's unbounded record list. Every recovery
        // reads a fresh, locked file cut, with strict UTF-8/JSON and size checks.
        require(storageExists(), "Required binding history disappeared");
    }

    @Override
    public boolean storageExists() {
        return attributes() != null;
    }

    @Override
    public boolean exists() {
        var attributes = attributes();
        return attributes != null && attributes.size() > 0;
    }

    private BasicFileAttributes attributes() {
        try {
            var attributes = Files.readAttributes(path, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            require(attributes.isRegularFile(), "Binding history must be a regular file, not a symlink or directory");
            return attributes;
        }
        catch (NoSuchFileException absent) {
            return null;
        }
        catch (IOException failure) {
            throw new SchemaHistoryException("Cannot inspect binding history", failure);
        }
    }

    @Override
    public void initializeStorage() {
        require(!starting, "Required binding history disappeared before start; recovery cannot recreate it");
        try {
            // Creating storage is explicit; a missing parent is never mistaken for
            // an empty recovered generation. Force each new directory entry too.
            createDirectory(path.getParent());
            try (var channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
                channel.force(true);
            }
            catch (FileAlreadyExistsException exists) {
                require(storageExists(), "Binding history creation raced removal");
            }
            forceDirectory(path.getParent());
        }
        catch (IOException failure) {
            throw new SchemaHistoryException("Cannot durably initialize binding history", failure);
        }
    }

    private static void createDirectory(Path directory) throws IOException {
        if (Files.isDirectory(directory)) {
            return;
        }
        if (directory.getParent() != null) {
            createDirectory(directory.getParent());
        }
        try {
            Files.createDirectory(directory);
        }
        catch (FileAlreadyExistsException exists) {
            require(Files.isDirectory(directory), "Binding history parent is not a directory");
        }
        forceDirectory(directory);
        if (directory.getParent() != null) {
            forceDirectory(directory.getParent());
        }
    }

    private static void forceDirectory(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    @Override
    protected void doStoreRecord(HistoryRecord record) {
        try {
            byte[] encoded = documentWriter.write(record.document()).getBytes(StandardCharsets.UTF_8);
            if (batchChannel != null) {
                new As400BindingHistory.HistoryBudget(true).requireAppend(encoded.length);
                append(batchChannel, encoded);
                return;
            }
            var before = attributes();
            require(before != null, "Binding history disappeared before append");
            // No create/truncate here: uncertainty must retain the previous bytes.
            try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                    var held = acquire(channel, false)) {
                var observed = read(channel, ignored -> {
                }, false);
                observed.requireAppend(encoded.length);
                unchanged(before, channel);
                channel.position(channel.size());
                append(channel, encoded);
                channel.force(true);
                forceDirectory(path.getParent());
            }
        }
        catch (IOException failure) {
            throw new SchemaHistoryException("Cannot durably append binding history", failure);
        }
    }

    private static void append(FileChannel channel, byte[] encoded) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(encoded.length + 1).put(encoded).put((byte) '\n').flip();
        while (bytes.hasRemaining()) {
            checkInterrupted();
            channel.write(bytes);
        }
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> records) {
        require(running.get(), "Binding history is stopped");
        var before = attributes();
        require(before != null, "Required binding history disappeared");
        try (var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                var held = acquire(channel, true)) {
            budget = read(channel, records, candidateAppends);
            unchanged(before, channel);
            // A killed writer may have left a complete line before its force.
            // Readback must make that cut durable before it can select F/W.
            channel.force(true);
            forceDirectory(path.getParent());
        }
        catch (IOException failure) {
            throw new SchemaHistoryException("Cannot recover binding history", failure);
        }
    }

    private static FileLock acquire(FileChannel channel, boolean shared) throws IOException {
        checkInterrupted();
        FileLock held;
        try {
            held = channel.tryLock(0, Long.MAX_VALUE, shared);
        }
        catch (OverlappingFileLockException busy) {
            throw new SchemaHistoryException("Binding history is busy; no checkpoint may advance", busy);
        }
        require(held != null, "Binding history is busy; no checkpoint may advance");
        return held;
    }

    private As400BindingHistory.HistoryBudget read(FileChannel channel, Consumer<HistoryRecord> records, boolean online) throws IOException {
        var observed = new As400BindingHistory.HistoryBudget(pagedCandidates);
        var legacyBudget = new As400BindingHistory.HistoryBudget();
        long end = channel.size();
        channel.position(0);
        ByteBuffer buffer = ByteBuffer.allocate(8192);
        var line = new ByteArrayOutputStream();
        while (channel.position() < end) {
            checkInterrupted();
            buffer.clear().limit((int) Math.min(buffer.capacity(), end - channel.position()));
            require(channel.read(buffer) > 0, "Binding history was truncated during recovery");
            buffer.flip();
            while (buffer.hasRemaining()) {
                if (online && observed.full()) {
                    return observed;
                }
                byte next = buffer.get();
                if (next == '\n') {
                    byte[] encoded = line.toByteArray();
                    if (!observed.accept(encoded.length, online)) {
                        return observed;
                    }
                    String value = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(encoded)).toString();
                    HistoryRecord record = new HistoryRecord(documentReader.read(value));
                    require(record.isValid(), "Incomplete binding history record");
                    eligibleLegacyRecord = !pagedCandidates || legacyBudget.accept(encoded.length, true);
                    records.accept(record);
                    line.reset();
                }
                else {
                    require(line.size() < As400BindingHistory.MAX_RECORD_BYTES, "Oversized binding history record");
                    line.write(next);
                }
            }
        }
        require(line.size() == 0, "Incomplete final binding history record");
        return observed;
    }

    private void unchanged(BasicFileAttributes before, FileChannel channel) throws IOException {
        var after = attributes();
        require(after != null && Objects.equals(before.fileKey(), after.fileKey()) && before.size() == after.size()
                && before.lastModifiedTime().equals(after.lastModifiedTime()) && channel.size() == after.size(),
                "Binding history changed during recovery");
    }

    private static void checkInterrupted() throws IOException {
        if (Thread.currentThread().isInterrupted()) {
            throw new IOException("Stopped during binding history I/O");
        }
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new SchemaHistoryException(message);
        }
    }
}
