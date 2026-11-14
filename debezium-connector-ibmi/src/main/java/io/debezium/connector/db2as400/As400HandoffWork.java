/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * One foreground handoff attempt's disposable history and indexes. All borrowed
 * views expire on close. Neither these files nor their indexes authorize output;
 * a replacement repeats native validation from the persisted discovery floor.
 */
final class As400HandoffWork implements AutoCloseable {
    static As400SpillStore.Settings settings(Path directory, long maximumBytes) {
        // Bound dirty state without committing the index for every few controls.
        return new As400SpillStore.Settings(directory, 2, (int) Math.min(256, maximumBytes / 4096), maximumBytes);
    }

    private final As400SpillStore.Settings settings;
    private final As400ContinuousRuntime.Limits limits;
    private final Callable<As400JournalCapture> captures;
    private final Runnable checkRunning;
    private final List<As400JournalCapture> owned = new ArrayList<>();
    private As400SpillStore index;
    private int namespace;
    private boolean closed;

    As400HandoffWork(As400SpillStore.Settings settings, As400ContinuousRuntime.Limits limits,
                     Callable<As400JournalCapture> captures, Runnable checkRunning) {
        this.settings = settings;
        this.limits = limits;
        this.captures = captures;
        this.checkRunning = checkRunning;
    }

    void check() {
        checkRunning.run();
        if (closed) {
            throw new IllegalStateException("Handoff working history is closed");
        }
    }

    As400ContinuousRuntime.Limits limits() {
        check();
        return limits;
    }

    As400JournalCapture capture() throws Exception {
        check();
        var capture = captures.call();
        owned.add(capture);
        return capture;
    }

    /** A separate scope prevents successive state-machine passes sharing state. */
    As400SpillStore transactionStore() {
        check();
        return new As400SpillStore(settings);
    }

    private As400SpillStore index() {
        check();
        if (index == null) {
            index = new As400SpillStore(settings);
        }
        return index;
    }

    <K, V> Map<K, V> map(As400SpillStore.Codec<K> keys, As400SpillStore.Codec<V> values) {
        return index().map("handoff-" + namespace++, keys, values);
    }

    <K> Set<K> set(As400SpillStore.Codec<K> keys) {
        return index().set("handoff-" + namespace++, keys);
    }

    <V> List<V> list(As400SpillStore.Codec<V> values) {
        return index().list("handoff-" + namespace++, values);
    }

    <K, V> Map<K, V> snapshot(Map<K, V> source, As400SpillStore.Codec<K> keys, As400SpillStore.Codec<V> values) {
        var copy = map(keys, values);
        for (var entry : source.entrySet()) {
            check();
            copy.put(entry.getKey(), entry.getValue());
        }
        return Collections.unmodifiableMap(copy);
    }

    /** Retain ordinal references, never another heap copy of the raw payloads. */
    List<RawJournalPage.Entry> select(List<RawJournalPage.Entry> source, Predicate<RawJournalPage.Entry> included) {
        var positions = list(As400SpillCodec.INT);
        for (int i = 0; i < source.size(); i++) {
            check();
            if (included.test(source.get(i))) {
                positions.add(i);
            }
        }
        int size = positions.size();
        return new As400JournalCapture.Entries() {
            @Override
            public RawJournalPage.Entry get(int i) {
                check();
                java.util.Objects.checkIndex(i, size);
                return source.get(positions.get(i));
            }

            @Override
            public int size() {
                check();
                return size;
            }
        };
    }

    static boolean sameProjection(List<RawJournalPage.Entry> left, List<RawJournalPage.Entry> right,
                                  BigInteger from, BigInteger through, Set<Character> codes)
            throws Exception {
        var headers = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID);
        Predicate<RawJournalPage.Entry> selected = e -> e.position().offset().compareTo(from) >= 0
                && e.position().offset().compareTo(through) <= 0 && codes.contains(headers.decode(e.data(), 0).getJournalCode());
        var a = left.stream().filter(selected).iterator();
        var b = right.stream().filter(selected).iterator();
        while (a.hasNext() && b.hasNext()) {
            var x = a.next();
            var y = b.next();
            if (!As400JournalRecordContent.digest(x.data(), x.position()).equals(As400JournalRecordContent.digest(y.data(), y.position()))) {
                return false;
            }
        }
        return !a.hasNext() && !b.hasNext();
    }

    /** Failure diagnostics must not materialize an unbounded set after storage closes. */
    static Set<BigInteger> diagnosticCycles(Set<BigInteger> cycles) {
        return Set.copyOf(cycles.stream().limit(128).toList());
    }

    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        Exception failure = null;
        var resources = new ArrayList<AutoCloseable>(owned);
        if (index != null) {
            resources.add(index);
        }
        for (var resource : resources) {
            try {
                resource.close();
            }
            catch (Exception error) {
                if (failure == null) {
                    failure = error;
                }
                else {
                    failure.addSuppressed(error);
                }
            }
        }
        owned.clear();
        if (failure != null) {
            throw failure;
        }
    }
}
