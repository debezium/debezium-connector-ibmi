/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import io.debezium.connector.db2as400.As400SchemaState.Cursor;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;

/** Source-thread schema selection; old definitions are replayed from history, never reacquired. */
final class As400SchemaTimeline implements As400SourceNormalizer.TableResolver {
    static final class PendingChangeException extends RuntimeException {
        PendingChangeException() {
            super("Schema transaction has not completed; metadata refresh is deferred");
        }
    }

    interface Refresh {
        As400SchemaChange collect(As400BindingDefinition before, RawJournalPage.Entry opening, As400SourceNormalizer.TableResolver verifiedBindings) throws Exception;
    }

    private static final class Pending {
        final As400SchemaChange change;
        boolean applied;
        boolean committed;

        Pending(As400SchemaChange change) {
            this.change = change;
        }
    }

    private record Applied(JournalPosition position, Cursor cursor) {
    }

    private final As400SchemaHistory history;
    private final As400Checkpoint loaded;
    private final As400JournalOrder order;
    private final Refresh refresh;
    private final Consumer<As400BindingDefinition> update;
    private final Map<String, As400BindingDefinition> definitions = new HashMap<>();
    private final Map<String, As400SourceNormalizer.TableBinding> bindings = new HashMap<>();
    private final Map<String, Pending> pending = new HashMap<>();
    private final Map<JournalPosition, JournalPosition> creationDependencies = new HashMap<>();
    private final Map<Long, Applied> applied;
    private Cursor current;
    private Cursor floor;
    private JournalPosition creationRecord;

    As400SchemaTimeline(As400SchemaHistory history, List<As400BindingDefinition> recovered, As400Checkpoint loaded,
                        As400JournalOrder order, Refresh refresh, Consumer<As400BindingDefinition> update, As400SpillStore spill) {
        this.history = Objects.requireNonNull(history);
        this.loaded = loaded;
        this.order = order;
        this.refresh = refresh;
        this.update = update;
        applied = spill == null ? new HashMap<>()
                : spill.map("schema-positions", As400SpillCodec.codec((out, key) -> out.writeLong(key), in -> in.readLong()),
                        As400SpillCodec.codec((out, value) -> {
                            As400SpillCodec.position(out, value.position());
                            out.writeLong(value.cursor().ordinal());
                            As400SpillCodec.string(out, value.cursor().hash());
                        }, in -> new Applied(As400SpillCodec.position(in), new Cursor(in.readLong(), As400SpillCodec.string(in)))));
        current = floor = loaded.schemaState().floor();
        recovered.forEach(this::install);
    }

    private void install(As400BindingDefinition definition) {
        String key = As400SchemaHistory.key(definition);
        definitions.put(key, definition);
        bindings.put(key, definition.binding(65536));
    }

    void before(RawJournalPage.Entry entry) throws Exception {
        creationRecord = null;
        String key = entry.header().getLibrary() + "/" + entry.header().getFile();
        boolean addingMember = !pending.containsKey(key) && As400MemberCreation.opening(definitions.get(key), entry);
        if (As400SchemaChange.isOpening(entry) || addingMember) {
            require(!pending.containsKey(key), "Overlapping schema changes for one table");
            var before = definitions.get(key);
            boolean creation = entry.header().getJournalCode() == 'D';
            require(creation == (before == null), "Creation cannot replace a retained file; ALTER requires an existing definition");
            var saved = history.find(entry.position());
            As400SchemaChange change;
            if (saved == null) {
                require(order.compare(entry.position(), loaded.completedThrough()) > 0,
                        "Schema history is missing from an already completed prefix");
                change = refresh.collect(before, entry, this);
            }
            else {
                change = saved.change();
            }
            require(Objects.equals(change.before(), before) && change.start().equals(entry.position()), "Schema refresh does not extend the active definition");
            change.verifyRecord(entry, order);
            pending.put(key, new Pending(change));
            if (change.creationLifecycle()) {
                creationDependencies.put(change.start(), change.commit());
            }
        }
        var iterator = pending.values().iterator();
        while (iterator.hasNext()) {
            var value = iterator.next();
            if (value.change.records().stream().anyMatch(mark -> mark.position().equals(entry.position()))) {
                value.change.verifyRecord(entry, order);
                if (value.change.creationLifecycle()) {
                    creationRecord = entry.position();
                }
            }
            if (value.change.creationLifecycle() && value.change.change().equals(entry.position())) {
                apply(value, entry.position());
            }
            if (value.change.commit().equals(entry.position())) {
                value.committed = true;
            }
            if (value.applied && value.committed) {
                iterator.remove();
            }
        }
    }

    @Override
    public As400SourceNormalizer.TableBinding resolve(String physicalFile, String library) {
        return Objects.requireNonNull(bindings.get(library + "/" + physicalFile), "Missing recovered schema binding");
    }

    @Override
    public boolean allowsSchemaChange(JournalPosition opening) {
        return pending.values().stream().anyMatch(value -> value.change.start().equals(opening));
    }

    @Override
    public boolean creationLifecycle(RawJournalPage.Entry entry) {
        return entry.position().equals(creationRecord);
    }

    @Override
    public void schemaChange(RawJournalPage.Entry entry) throws Exception {
        String key = entry.header().getLibrary() + "/" + entry.header().getFile();
        var value = Objects.requireNonNull(pending.get(key), "Unqualified schema change");
        require(!value.applied && value.change.change().equals(entry.position()), "Unexpected or repeated schema-change record");
        value.change.verifyRecord(entry, order);
        apply(value, entry.position());
        if (value.committed) {
            pending.remove(key);
        }
    }

    private void apply(Pending value, JournalPosition position) throws Exception {
        require(!value.applied, "Repeated schema transition");
        var retained = history.append(current, value.change);
        // Rebuild output only after durable history; nothing from this epoch has
        // reached the queue. Failed conversion/setup leaves the old checkpoint.
        update.accept(value.change.after());
        install(value.change.after());
        current = retained.cursor();
        applied.put(current.ordinal(), new Applied(position, current));
        value.applied = true;
    }

    @Override
    public As400Checkpoint schemaCheckpoint(As400Checkpoint checkpoint) {
        JournalPosition replayFrom = checkpoint.replayFrom();
        boolean inclusive = checkpoint.replayInclusive();
        var dependencies = creationDependencies.entrySet().iterator();
        while (dependencies.hasNext()) {
            var dependency = dependencies.next();
            if (order.compare(checkpoint.replayFrom(), dependency.getValue()) > 0) {
                dependencies.remove();
            }
            else if (order.compare(replayFrom, dependency.getKey()) >= 0) {
                replayFrom = dependency.getKey();
                inclusive = true;
            }
        }
        if (!replayFrom.equals(checkpoint.replayFrom()) || inclusive != checkpoint.replayInclusive()) {
            checkpoint = new As400Checkpoint(checkpoint.journalIdentity(), checkpoint.captureFingerprint(), replayFrom, inclusive,
                    checkpoint.completedThrough(), order.incarnation(replayFrom), checkpoint.completedReceiverIdentity(),
                    checkpoint.snapshotState(), checkpoint.schemaState());
        }
        Cursor atFloor = through(checkpoint.replayFrom(), !checkpoint.replayInclusive());
        Cursor atCompleted = through(checkpoint.completedThrough(), true);
        var state = new As400SchemaState(atFloor, atCompleted);
        if (order.compare(checkpoint.completedThrough(), loaded.completedThrough()) == 0) {
            require(state.completed().equals(loaded.schemaState().completed()), "Completed prefix did not reconstruct its saved schema history");
        }
        for (long retired = floor.ordinal() + 1; retired <= atFloor.ordinal(); retired++) {
            applied.remove(retired);
        }
        floor = atFloor;
        return checkpoint.withSchemaState(state);
    }

    private Cursor through(JournalPosition position, boolean inclusive) {
        long low = floor.ordinal() + 1, high = current.ordinal();
        Cursor found = floor;
        while (low <= high) {
            long middle = low + (high - low) / 2;
            var candidate = Objects.requireNonNull(applied.get(middle), "Missing retained schema position");
            int compared = order.compare(candidate.position(), position);
            if (compared < 0 || compared == 0 && inclusive) {
                found = candidate.cursor();
                low = middle + 1;
            }
            else {
                high = middle - 1;
            }
        }
        return found;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
