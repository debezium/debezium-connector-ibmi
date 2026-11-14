/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.h2.mvstore.type.ByteArrayDataType;
import org.h2.mvstore.type.LongDataType;
import org.h2.mvstore.type.StringDataType;

import io.debezium.DebeziumException;

/**
 * Single-owner, disposable working storage. Never reopen this file for recovery:
 * the acknowledged offset and retained journal are the recovery authority.
 * A fixed number of MVMaps bounds map metadata independently of transaction count.
 */
final class As400SpillStore implements AutoCloseable {
    interface Codec<T> {
        byte[] encode(T value);

        T decode(byte[] value);
    }

    record Settings(Path directory, int cacheMegabytes, int writeBufferKilobytes, long maximumBytes) {
        Settings {
            Objects.requireNonNull(directory);
            if (!directory.isAbsolute() || cacheMegabytes < 1 || writeBufferKilobytes < 16
                    || maximumBytes < 1024L * writeBufferKilobytes * 4) {
                throw new IllegalArgumentException("Spill storage requires an absolute directory and positive cache, write buffer and disk budget");
            }
        }
    }

    record Usage(long encodedBytes, long fileBytes, int cacheMegabytes, int unsavedBytes) {
    }

    private final Thread owner = Thread.currentThread();
    private final Settings settings;
    private final Path directory;
    private final Path file;
    private final MVStore store;
    private final MVMap<String, byte[]> data;
    private final MVMap<String, String> insertionOrder;
    private final MVMap<String, Long> counters;
    private long encodedBytes;
    private boolean failed;
    private boolean closed;

    As400SpillStore(Settings settings) {
        this.settings = Objects.requireNonNull(settings);
        Path created = null;
        MVStore opened = null;
        try {
            if (Files.getFileStore(settings.directory()).supportsFileAttributeView("posix")) {
                created = Files.createTempDirectory(settings.directory(), "ibmi-truncate-",
                        PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            }
            else {
                created = Files.createTempDirectory(settings.directory(), "ibmi-truncate-");
            }
            directory = created;
            file = directory.resolve("working.mv");
            opened = new MVStore.Builder().fileName(file.toString()).cacheSize(settings.cacheMegabytes()).compress().autoCommitDisabled().open();
            store = opened;
            // No snapshot versions are exposed. Iterators seek against the current
            // tree instead of pinning an old tree while another namespace changes.
            store.setVersionsToKeep(0);
            store.setRetentionTime(0);
            data = store.openMap("data", new MVMap.Builder<String, byte[]>().keyType(StringDataType.INSTANCE).valueType(ByteArrayDataType.INSTANCE));
            insertionOrder = store.openMap("order", new MVMap.Builder<String, String>().keyType(StringDataType.INSTANCE).valueType(StringDataType.INSTANCE));
            counters = store.openMap("counters", new MVMap.Builder<String, Long>().keyType(StringDataType.INSTANCE).valueType(LongDataType.INSTANCE));
        }
        catch (Exception e) {
            if (opened != null) {
                opened.closeImmediately();
            }
            if (created != null) {
                try {
                    Files.deleteIfExists(created.resolve("working.mv"));
                    Files.delete(created);
                }
                catch (IOException cleanup) {
                    e.addSuppressed(cleanup);
                }
            }
            throw new SpillException("Cannot create private transaction spill storage", e);
        }
    }

    <K, V> Map<K, V> map(String namespace, Codec<K> keys, Codec<V> values) {
        check();
        return new DiskMap<>(prefix(namespace), keys, values);
    }

    <V> List<V> list(String namespace, Codec<V> values) {
        check();
        return new DiskList<>(prefix(namespace), values);
    }

    <K> Set<K> set(String namespace, Codec<K> keys) {
        Map<K, Integer> entries = map(namespace, keys, As400SpillCodec.INT);
        return new AbstractSet<>() {
            @Override
            public boolean add(K value) {
                return entries.put(value, 1) == null;
            }

            @Override
            public boolean contains(Object value) {
                return entries.containsKey(value);
            }

            @Override
            public boolean remove(Object value) {
                return entries.remove(value) != null;
            }

            @Override
            public Iterator<K> iterator() {
                return entries.keySet().iterator();
            }

            @Override
            public int size() {
                return entries.size();
            }

            @Override
            public void clear() {
                entries.clear();
            }
        };
    }

    /** Marker type avoids materializing a committed disk list with List.copyOf. */
    static final class FrozenList<V> extends AbstractList<V> {
        private final List<V> delegate;
        private final int size;
        private boolean valid = true;

        FrozenList(List<V> delegate) {
            this.delegate = delegate;
            size = delegate.size();
        }

        @Override
        public V get(int index) {
            Objects.checkIndex(index, size);
            if (!valid || delegate.size() != size) {
                throw new IllegalStateException("Committed spill batch is no longer retained");
            }
            return delegate.get(index);
        }

        @Override
        public int size() {
            return size;
        }

        void invalidate() {
            valid = false;
        }
    }

    Usage usage() {
        return access(() -> new Usage(encodedBytes, store.getFileStore().size(), store.getCacheSizeUsed(), store.getUnsavedMemory()));
    }

    Path directory() {
        return directory;
    }

    /** Flush scratch pages to bound dirty memory, not to acknowledge source output. */
    void flush() {
        check();
        try {
            store.commit();
            if (store.getFileStore().size() > settings.maximumBytes()) {
                // Obsolete MVStore chunks can temporarily exhaust the physical
                // budget even when the live dataset fits. Reclaim with a bounded
                // effort before deciding; never increase or bypass the budget.
                store.compactFile(1000);
            }
            if (store.getFileStore().size() > settings.maximumBytes()) {
                throw new IllegalStateException("Transaction spill file exceeded its disk budget after reclamation: file="
                        + store.getFileStore().size() + ", live encoded data=" + encodedBytes + ", budget=" + settings.maximumBytes());
            }
        }
        catch (RuntimeException e) {
            throw fail(e);
        }
    }

    private void changed() {
        if (store.getUnsavedMemory() >= settings.writeBufferKilobytes() * 1024L) {
            flush();
        }
    }

    private byte[] get(String key) {
        check();
        try {
            return data.get(key);
        }
        catch (RuntimeException e) {
            throw fail(e);
        }
    }

    private void put(String key, byte[] value) {
        check();
        try {
            byte[] previous = data.get(key);
            long difference = value.length - (previous == null ? 0L : previous.length);
            if (previous == null) {
                difference += 2L * key.length();
            }
            if (difference > settings.maximumBytes() - encodedBytes) {
                throw new IllegalStateException("Transaction spill data exceeded its disk budget; pending work cannot be evicted");
            }
            data.put(key, value);
            encodedBytes += difference;
        }
        catch (RuntimeException e) {
            throw fail(e);
        }
    }

    private byte[] remove(String key) {
        check();
        try {
            byte[] removed = data.remove(key);
            if (removed != null) {
                encodedBytes -= removed.length + 2L * key.length();
            }
            return removed;
        }
        catch (RuntimeException e) {
            throw fail(e);
        }
    }

    private long count(String key) {
        return access(() -> counters.getOrDefault(key, 0L));
    }

    private <T> T access(Supplier<T> operation) {
        check();
        try {
            return operation.get();
        }
        catch (RuntimeException e) {
            throw fail(e);
        }
    }

    private void check() {
        if (Thread.currentThread() != owner || closed || failed || store.getPanicException() != null) {
            throw new SpillException("Spill storage is stopped or called by another owner; replay from the durable offset", store.getPanicException());
        }
    }

    private SpillException fail(RuntimeException cause) {
        failed = true;
        return new SpillException("Transaction spill storage failed; replay from the durable offset", cause);
    }

    private static String prefix(String namespace) {
        if (namespace == null || namespace.isEmpty() || namespace.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Invalid spill namespace");
        }
        return namespace + '\0';
    }

    private static String ordinal(long index) {
        if (index < 0) {
            throw new IllegalStateException("Spill index overflow");
        }
        String hex = Long.toHexString(index);
        return "0".repeat(16 - hex.length()) + hex;
    }

    @Override
    public void close() {
        if (Thread.currentThread() != owner) {
            throw new IllegalStateException("Only the drained spill owner may close storage");
        }
        if (closed) {
            return;
        }
        closed = true;
        try {
            // Scratch is not a checkpoint. Do not perform a final large commit,
            // especially after an I/O failure or during task shutdown.
            store.closeImmediately();
        }
        finally {
            try {
                Files.deleteIfExists(file);
                Files.delete(directory);
            }
            catch (IOException e) {
                throw new SpillException("Cannot remove this task's transaction spill storage", e);
            }
        }
    }

    private final class DiskMap<K, V> extends AbstractMap<K, V> {
        private final String prefix;
        private final Codec<K> keys;
        private final Codec<V> values;

        private DiskMap(String prefix, Codec<K> keys, Codec<V> values) {
            this.prefix = prefix;
            this.keys = Objects.requireNonNull(keys);
            this.values = Objects.requireNonNull(values);
        }

        private String key(Object key) {
            @SuppressWarnings("unchecked")
            K typed = (K) Objects.requireNonNull(key);
            return prefix + Base64.getUrlEncoder().withoutPadding().encodeToString(keys.encode(typed));
        }

        private V value(byte[] stored) {
            return stored == null ? null : values.decode(java.util.Arrays.copyOfRange(stored, Long.BYTES, stored.length));
        }

        @Override
        public V get(Object key) {
            return value(As400SpillStore.this.get(key(key)));
        }

        @Override
        public boolean containsKey(Object key) {
            return As400SpillStore.this.get(key(key)) != null;
        }

        @Override
        public V put(K key, V value) {
            String encodedKey = key(key);
            byte[] previous = As400SpillStore.this.get(encodedKey);
            long index = previous == null ? count(prefix + "next") : ByteBuffer.wrap(previous).getLong();
            byte[] encoded = values.encode(Objects.requireNonNull(value));
            As400SpillStore.this.put(encodedKey, ByteBuffer.allocate(Long.BYTES + encoded.length).putLong(index).put(encoded).array());
            if (previous == null) {
                access(() -> insertionOrder.put(prefix + ordinal(index), encodedKey));
                access(() -> counters.put(prefix + "next", Math.addExact(index, 1)));
                access(() -> counters.put(prefix + "size", Math.addExact(count(prefix + "size"), 1)));
            }
            changed();
            return value(previous);
        }

        @Override
        public V remove(Object key) {
            byte[] previous = As400SpillStore.this.remove(key(key));
            if (previous != null) {
                access(() -> insertionOrder.remove(prefix + ordinal(ByteBuffer.wrap(previous).getLong())));
                access(() -> counters.put(prefix + "size", count(prefix + "size") - 1));
                changed();
            }
            return value(previous);
        }

        @Override
        public int size() {
            return Math.toIntExact(count(prefix + "size"));
        }

        @Override
        public void clear() {
            String cursor = prefix;
            while (true) {
                String previous = cursor;
                String next = access(() -> insertionOrder.higherKey(previous));
                if (next == null || !next.startsWith(prefix)) {
                    break;
                }
                String key = access(() -> insertionOrder.remove(next));
                As400SpillStore.this.remove(key);
                cursor = next;
                changed();
            }
            access(() -> counters.remove(prefix + "size"));
            access(() -> counters.remove(prefix + "next"));
            changed();
        }

        @Override
        public Set<Map.Entry<K, V>> entrySet() {
            return new AbstractSet<>() {
                @Override
                public int size() {
                    return DiskMap.this.size();
                }

                @Override
                public Iterator<Map.Entry<K, V>> iterator() {
                    return new Iterator<>() {
                        private String cursor = prefix;
                        private K removable;

                        private String nextKey() {
                            String next = access(() -> insertionOrder.higherKey(cursor));
                            return next != null && next.startsWith(prefix) ? next : null;
                        }

                        @Override
                        public boolean hasNext() {
                            return nextKey() != null;
                        }

                        @Override
                        public Map.Entry<K, V> next() {
                            String next = nextKey();
                            if (next == null) {
                                throw new NoSuchElementException();
                            }
                            cursor = next;
                            String encodedKey = access(() -> insertionOrder.get(next));
                            K key = keys.decode(Base64.getUrlDecoder().decode(encodedKey.substring(prefix.length())));
                            removable = key;
                            return new SimpleImmutableEntry<>(key, value(As400SpillStore.this.get(encodedKey)));
                        }

                        @Override
                        public void remove() {
                            if (removable == null) {
                                throw new IllegalStateException("Iterator has no removable entry");
                            }
                            DiskMap.this.remove(removable);
                            removable = null;
                        }
                    };
                }
            };
        }
    }

    /** Append/truncate only: source journal order never inserts into the middle. */
    private final class DiskList<V> extends AbstractList<V> {
        private final String prefix;
        private final Codec<V> values;

        private DiskList(String prefix, Codec<V> values) {
            this.prefix = prefix;
            this.values = Objects.requireNonNull(values);
        }

        @Override
        public int size() {
            return Math.toIntExact(count(prefix + "size"));
        }

        @Override
        public V get(int index) {
            Objects.checkIndex(index, size());
            return values.decode(As400SpillStore.this.get(prefix + ordinal(index)));
        }

        @Override
        public boolean add(V value) {
            int size = size();
            As400SpillStore.this.put(prefix + ordinal(size), values.encode(Objects.requireNonNull(value)));
            access(() -> counters.put(prefix + "size", Math.addExact((long) size, 1)));
            modCount++;
            changed();
            return true;
        }

        @Override
        protected void removeRange(int from, int to) {
            int size = size();
            Objects.checkFromToIndex(from, to, size);
            if (to != size) {
                throw new UnsupportedOperationException("Only a spill list suffix can be removed");
            }
            for (int i = from; i < to; i++) {
                As400SpillStore.this.remove(prefix + ordinal(i));
                changed();
            }
            if (from == 0) {
                access(() -> counters.remove(prefix + "size"));
            }
            else {
                access(() -> counters.put(prefix + "size", (long) from));
            }
            modCount++;
            changed();
        }
    }

    static final class SpillException extends DebeziumException {
        SpillException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
