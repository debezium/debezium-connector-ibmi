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
import java.util.function.Function;

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
        final String key;
        final As400SchemaChange change;
        final JournalPosition unobservedLayoutThrough;
        final boolean rolledBack;
        boolean applied;
        boolean committed;
        java.math.BigInteger rollbackCycle;
        String rollbackProducer;
        String rollbackMember;
        String rollbackMemberId;
        boolean rollbackEnded;

        Pending(String key, As400SchemaChange change) {
            this.key = key;
            this.change = change;
            rolledBack = change.rolledBackAlter() || change.abortedCreation()
                    && change.records().stream().anyMatch(mark -> mark.type().equals("C/RB"));
            unobservedLayoutThrough = change.sharedAlterDeletion() ? change.records().stream().filter(mark -> mark.type().equals("D/DT"))
                    .findFirst().orElseThrow().position()
                    : change.before() == null && change.after() != null ? change.records().stream()
                            .filter(mark -> mark.type().equals("D/CG")).reduce((first, last) -> last).map(As400SchemaChange.Mark::position).orElse(null) : null;
        }
    }

    private record Applied(JournalPosition position, Cursor cursor) {
    }

    private record Waiting(java.math.BigInteger cycle, As400BindingDefinition before, RawJournalPage.Entry opening, As400SchemaHistory.Record saved,
            As400SchemaChange.Miscellaneous miscellaneous, As400FileCreation.Progress creation) {
    }

    private final As400SchemaHistory history;
    private final As400Checkpoint loaded;
    private final As400JournalOrder order;
    private final Refresh refresh;
    private final Consumer<As400BindingDefinition> update;
    private final Map<String, As400BindingDefinition> definitions = new HashMap<>();
    private final Map<String, As400SourceNormalizer.TableBinding> bindings = new HashMap<>();
    private final Map<String, Pending> pending = new HashMap<>();
    private final Map<String, Waiting> waiting = new java.util.LinkedHashMap<>();
    private final java.util.Set<java.math.BigInteger> deferredCycles = new java.util.HashSet<>();
    private final java.util.Set<java.math.BigInteger> deferredCycleView = java.util.Collections.unmodifiableSet(deferredCycles);
    private final Map<JournalPosition, JournalPosition> creationDependencies = new HashMap<>();
    private final Map<Long, Applied> applied;
    private Cursor current;
    private Cursor floor;
    private JournalPosition creationRecord;
    private JournalPosition creationClear;
    private JournalPosition admittedOpening;
    private Function<RawJournalPage.Entry, java.math.BigInteger> cycleLookup;
    private Consumer<As400TransactionBuffer.TableIdentity> idleTable;

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

    void bindReplay(Function<RawJournalPage.Entry, java.math.BigInteger> cycleLookup, Consumer<As400TransactionBuffer.TableIdentity> idleTable) {
        this.cycleLookup = Objects.requireNonNull(cycleLookup);
        this.idleTable = Objects.requireNonNull(idleTable);
    }

    private void install(As400BindingDefinition definition) {
        String key = As400SchemaHistory.key(definition);
        definitions.put(key, definition);
        bindings.put(key, definition.binding(65536));
    }

    @Override
    public boolean allowsExcludedHistoricalLayout(io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader header, JournalPosition position) {
        var value = pending.get(header.getLibrary() + "/" + header.getFile());
        return value != null && value.applied && value.unobservedLayoutThrough != null
                && order.compare(position, value.change.start()) > 0 && order.compare(position, value.unobservedLayoutThrough) < 0;
    }

    void before(RawJournalPage.Entry entry) throws Exception {
        creationRecord = null;
        creationClear = null;
        admittedOpening = null;
        String key = entry.header().getLibrary() + "/" + entry.header().getFile();
        if (entry.header().getJournalCode() == 'C') {
            for (var value : waiting.values()) {
                if (value.creation() != null && value.cycle().equals(entry.header().getCommitCycle())) {
                    value.creation().control(entry);
                }
            }
        }
        if (entry.header().getJournalCode() == 'C' && List.of("CM", "RB").contains(entry.header().getEntryType())
                && deferredSchema(entry.header().getCommitCycle())) {
            qualifyDeferred(entry);
            return; // Replay its owned entries before consuming this terminal control.
        }
        var delayed = waiting.get(key);
        if (delayed != null && delayed.creation() != null) {
            if (delayed.creation().deferred(entry)) {
                creationClear = entry.position();
            }
            return;
        }
        if (delayed != null && delayed.miscellaneous() != null && (delayed.miscellaneous().pending() || As400SchemaChange.metadataOpening(entry))) {
            require(entry.header().getCommitCycle().signum() == 0 ? cycleLookup.apply(entry).equals(delayed.cycle())
                    : entry.header().getCommitCycle().equals(delayed.cycle()), "Metadata overlaps another unfinished schema transaction");
            delayed.miscellaneous().accept(entry);
            // Validate the pair now; the complete schema history will retain it
            // with the ALTER when its transaction outcome is observed.
            creationRecord = entry.position();
            return;
        }
        if (delayed != null && entry.header().getJournalCode() == 'R') {
            require(entry.header().getCommitCycle().equals(delayed.cycle()), "Row from another cycle overlaps an unfinished schema change");
        }
        boolean memberChange = !pending.containsKey(key) && (As400MemberCreation.opening(definitions.get(key), entry)
                || As400MemberCreation.deletionOpening(definitions.get(key), entry) || As400MemberCreation.renameOpening(definitions.get(key), entry));
        boolean opening = As400SchemaChange.isOpening(entry);
        boolean retainedOpening = (opening || memberChange) && pending.values().stream().anyMatch(value -> value.change.records().stream()
                .anyMatch(mark -> mark.position().equals(entry.position())));
        boolean transactionalDrop = entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("DT")
                && entry.header().getCommitCycle().signum() > 0;
        boolean transactionalCreate = entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("CT")
                && entry.header().getCommitCycle().signum() > 0;
        if (!retainedOpening && opening && cycleLookup != null && (transactionalDrop || transactionalCreate
                || entry.header().getJournalCode() == 'F' && !As400SchemaChange.metadataOpening(entry))) {
            var saved = history.find(entry.position());
            if (saved == null || saved.change().deferred()) {
                var cycle = cycleLookup.apply(entry);
                require(!pending.containsKey(key), "Overlapping schema changes for one table");
                require(delayed == null || delayed.cycle().equals(cycle), "Another transaction changed an unfinished schema");
                if (transactionalDrop) {
                    As400FileDeletion.opening(definitions.get(key), entry);
                }
                if (delayed == null) {
                    var before = definitions.get(key);
                    require(transactionalCreate == (before == null), "Creation cannot replace a retained file; ALTER/DROP require an existing definition");
                    var creation = transactionalCreate ? new As400FileCreation.Progress(entry) : null;
                    if (creation != null) {
                        creation.deferred(entry);
                    }
                    waiting.put(key, new Waiting(cycle, before, entry, saved, transactionalDrop || transactionalCreate ? null
                            : new As400SchemaChange.Miscellaneous(
                                    before.identity().physicalFile(), before.identity().tableId().schema(), before.identity().fileJournalId(), before.members()),
                            creation));
                    deferredCycles.add(cycle);
                }
                admittedOpening = entry.position();
                return;
            }
        }
        if (!retainedOpening && (opening || memberChange)) {
            require(!pending.containsKey(key), "Overlapping schema changes for one table");
            var before = definitions.get(key);
            boolean creation = entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("CT");
            require(creation == (before == null), "Creation cannot replace a retained file; ALTER requires an existing definition");
            if (entry.header().getJournalCode() == 'D' && (entry.header().getEntryType().equals("FN") || entry.header().getEntryType().equals("FM"))) {
                require(!waiting.containsKey(key), "Rename overlaps an unfinished schema definition");
                if (idleTable != null) {
                    idleTable.accept(before.identity());
                }
            }
            var saved = history.find(entry.position());
            As400SchemaChange change;
            if (saved == null) {
                change = refresh.collect(before, entry, this);
                require(change.abortedCreation() || order.compare(entry.position(), loaded.completedThrough()) > 0,
                        "Schema history is missing from an already completed prefix");
            }
            else {
                change = restoredChange(saved, before, entry);
            }
            require(Objects.equals(change.before(), before) && change.start().equals(entry.position()), "Schema refresh does not extend the active definition");
            var completion = change.records().get(change.records().size() - 1);
            if (!order.covers(completion.position(), completion.receiverIdentity())) {
                // Native schema acquisition samples independently. A commit may
                // arrive after the streaming cut was verified; retry that entry
                // after the reader extends its evidence, without mutating state.
                throw new PendingChangeException();
            }
            change.verifyRecord(entry, order);
            // A proven row-free rename/drop frees the old native name. Another
            // object may be created there before the renamed file's D/DF. Keep
            // the retired identity's remaining marks separate from that name.
            String pendingKey = change.retiredByRename() ? key + "/" + change.before().identity().fileJournalId() : key;
            require(!pending.containsKey(pendingKey), "Repeated retiring file identity");
            pending.put(pendingKey, new Pending(key, change));
            creationDependencies.put(change.start(), change.commit());
        }
        process(entry);
    }

    private void qualifyDeferred(RawJournalPage.Entry terminal) throws Exception {
        var qualified = new java.util.LinkedHashMap<String, As400SchemaChange>();
        for (var item : waiting.entrySet()) {
            var value = item.getValue();
            if (!value.cycle().equals(terminal.header().getCommitCycle())) {
                continue;
            }
            var change = value.saved() == null ? refresh.collect(value.before(), value.opening(), this).withDeferredVisibility()
                    : restoredChange(value.saved(), value.before(), value.opening());
            require(change.deferred() && Objects.equals(change.before(), definitions.get(item.getKey()))
                    && change.start().equals(value.opening().position()) && change.visibility().equals(terminal.position()),
                    "Deferred schema outcome does not extend its original transaction and definition");
            require(value.saved() != null || change.abortedCreation() || order.compare(change.visibility(), loaded.completedThrough()) > 0,
                    "Schema history is missing from an already completed prefix");
            var completion = change.records().get(change.records().size() - 1);
            if (!order.covers(completion.position(), completion.receiverIdentity())) {
                throw new PendingChangeException();
            }
            change.verifyRecord(value.opening(), order);
            change.verifyRecord(terminal, order);
            qualified.put(item.getKey(), change);
        }
        // A failed observation of any table must leave the whole cycle waiting.
        qualified.forEach((key, change) -> {
            pending.put(key, new Pending(key, change));
            creationDependencies.put(change.start(), change.commit());
        });
    }

    private As400SchemaChange restoredChange(As400SchemaHistory.Record saved, As400BindingDefinition before, RawJournalPage.Entry opening) throws Exception {
        var change = saved.change();
        if (change.sharedAlterDeletion()) {
            // History retains native marks and the original binding, not an
            // invented intermediate layout. Selection may change on restart;
            // re-establish the excluded-name proof from the retained journal.
            var observed = refresh.collect(before, opening, this);
            if (change.deferred()) {
                observed = observed.withDeferredVisibility();
            }
            require(observed.equals(change), "Shared ALTER/DROP history changed during exclusion revalidation");
        }
        return change;
    }

    private void process(RawJournalPage.Entry entry) throws Exception {
        creationRecord = null;
        String key = entry.header().getLibrary() + "/" + entry.header().getFile();
        var iterator = pending.values().iterator();
        while (iterator.hasNext()) {
            var value = iterator.next();
            if (value.change.records().stream().anyMatch(mark -> mark.position().equals(entry.position()))) {
                value.change.verifyRecord(entry, order);
                retainRollbackContext(value, entry);
                if (entry.header().getJournalCode() != 'C') {
                    creationRecord = entry.position();
                }
            }
            boolean undoneClear = value.change.abortedCreation() && entry.header().getJournalCode() == 'D' && entry.header().getEntryType().equals("CG");
            if (value.change.excludedCreation() != null && entry.header().getJournalCode() == 'R'
                    && !As400JournalLifecycle.administrativeType("R/" + entry.header().getEntryType()) && value == pending.get(key)) {
                value.change.excludedCreation().validate(entry, order);
                creationRecord = entry.position();
            }
            if (value.rollbackCycle != null && !value.rollbackEnded && (entry.header().getJournalCode() == 'R' || undoneClear)
                    && value == pending.get(key)) {
                // The verified full rollback makes these rows unobservable. Keep
                // only its outcome in schema history, not a mark per discarded row.
                var header = entry.header();
                if (value.change.abortedCreation() && header.getJournalCode() == 'R') {
                    As400SourceNormalizer.validateUnobservedRow(header, entry.data(), 0, entry.position());
                }
                else {
                    As400SourceNormalizer.validateSource(header, entry.data(), 0, entry.position());
                }
                require(header.getCommitCycle().equals(value.rollbackCycle)
                        && As400SchemaChange.producer(entry).equals(value.rollbackProducer),
                        "Rolled-back schema row belongs to another transaction or member");
                if (undoneClear) {
                    lifecycleIdentity(header.getFile(), header.getLibrary(), header.getMember()).validate(header);
                    require(new io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID)
                            .decode(header, entry.data(), 0).clearsRows(), "Rolled-back creation has an unqualified change");
                }
                else {
                    require(header.getMember().equals(value.rollbackMember) && header.getRecordInfo().objectType().equals("*QDDS")
                            && header.getRecordInfo().objectJournalId().equals(value.rollbackMemberId),
                            "Rolled-back schema row belongs to another transaction or member");
                }
                creationRecord = entry.position();
            }
            if (value.change.change().equals(entry.position())) {
                apply(value, entry.position());
            }
            if (value.change.commit().equals(entry.position())) {
                value.committed = true;
                if (value.change.after() == null && value.change.before() != null && !value.change.retiredByRename()) {
                    bindings.remove(As400SchemaHistory.key(value.change.before()));
                }
            }
            if (value.applied && value.committed && !awaitingDeferredReplay(value)) {
                iterator.remove();
            }
        }
    }

    private static void retainRollbackContext(Pending value, RawJournalPage.Entry entry) {
        if (value.change.excludedCreation() != null || !value.rolledBack) {
            return;
        }
        var header = entry.header();
        String type = header.getJournalCode() + "/" + header.getEntryType();
        if (type.equals("D/CT") || type.equals("D/CG")) {
            value.rollbackCycle = header.getCommitCycle();
            value.rollbackProducer = As400SchemaChange.producer(entry);
            if (value.change.before() != null) {
                value.rollbackMember = value.change.before().identity().member();
                value.rollbackMemberId = value.change.before().identity().memberJournalId();
            }
        }
        if (type.equals("F/JM") && value.change.abortedCreation()) {
            value.rollbackMember = header.getMember();
            value.rollbackMemberId = header.getRecordInfo().objectJournalId();
        }
        if (type.equals("C/RB")) {
            value.rollbackEnded = true;
        }
    }

    @Override
    public As400SourceNormalizer.TableBinding resolve(String physicalFile, String library) {
        return Objects.requireNonNull(bindings.get(library + "/" + physicalFile), "Missing recovered schema binding");
    }

    @Override
    public As400JournalLifecycle.FileIdentity lifecycleIdentity(String file, String library, String member) throws Exception {
        var delayed = waiting.get(library + "/" + file);
        if (delayed != null && delayed.creation() != null && delayed.creation().complete()) {
            var identity = delayed.creation().identity();
            require(member.isEmpty() || member.equals(identity.member()), "Uncommitted clear changed its created member");
            return As400JournalLifecycle.FileIdentity.of(identity);
        }
        return As400SourceNormalizer.TableResolver.super.lifecycleIdentity(file, library, member);
    }

    @Override
    public boolean allowsSchemaChange(JournalPosition opening) {
        return opening.equals(admittedOpening) || pending.values().stream().anyMatch(value -> value.change.records().stream()
                .anyMatch(mark -> mark.type().equals("F/CH") && mark.position().equals(opening)));
    }

    @Override
    public java.util.Set<java.math.BigInteger> deferredSchemaCycles() {
        return deferredCycleView;
    }

    @Override
    public java.math.BigInteger deferredObjectCycle(io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader header, JournalPosition position) {
        if (waiting.isEmpty() || position.equals(creationClear)) {
            return null;
        }
        var value = waiting.get(header.getLibrary() + "/" + header.getFile());
        return value != null && (value.creation() != null && order.compare(position, value.opening().position()) >= 0
                || value.miscellaneous() == null && value.opening().position().equals(position)
                || position.equals(admittedOpening) && header.getJournalCode() == 'D' && header.getEntryType().equals("DT")) ? value.cycle() : null;
    }

    @Override
    public boolean replaySchemaEntry(RawJournalPage.Entry entry) throws Exception {
        var objectCycle = deferredObjectCycle(entry.header(), entry.position());
        require(deferredSchema(objectCycle == null ? entry.header().getCommitCycle() : objectCycle), "Entry has no deferred schema cycle");
        process(entry);
        return creationLifecycle(entry);
    }

    @Override
    public boolean schemaChangeRequiresIdle(RawJournalPage.Entry entry) {
        var value = pending.get(entry.header().getLibrary() + "/" + entry.header().getFile());
        return value != null && !value.applied && value.change.change().equals(entry.position())
                && value.change.before() != null && value.change.after() != null
                && !Objects.equals(value.change.before(), value.change.after());
    }

    @Override
    public void finishDeferredSchema(RawJournalPage.Entry terminal) throws Exception {
        require(deferredSchema(terminal.header().getCommitCycle()), "Terminal has no deferred schema cycle");
        process(terminal);
        waiting.values().removeIf(value -> value.cycle().equals(terminal.header().getCommitCycle()));
        deferredCycles.remove(terminal.header().getCommitCycle());
        pending.values().removeIf(value -> value.applied && value.committed && !awaitingDeferredReplay(value));
    }

    private boolean awaitingDeferredReplay(Pending value) {
        // A clear's rollback restoration can hold the terminal in the lifecycle
        // queue after this schema's physical restoration has already been read.
        // Its original row-discard context must survive until the owned entries drain.
        return value.change.deferred() && waiting.containsKey(value.key);
    }

    @Override
    public boolean creationLifecycle(RawJournalPage.Entry entry) {
        return entry.position().equals(creationRecord);
    }

    @Override
    public void schemaChange(RawJournalPage.Entry entry) throws Exception {
        String key = entry.header().getLibrary() + "/" + entry.header().getFile();
        var value = Objects.requireNonNull(pending.get(key), "Unqualified schema change");
        value.change.verifyRecord(entry, order);
        if (value.applied) {
            require(order.compare(value.change.change(), entry.position()) < 0, "Unexpected or repeated schema-change record");
            return; // The observed final definition is already installed; no captured rows existed between these changes.
        }
        require(value.change.change().equals(entry.position()), "Unexpected schema-change record");
        apply(value, entry.position());
        if (value.committed && !awaitingDeferredReplay(value)) {
            pending.remove(key);
        }
    }

    private void apply(Pending value, JournalPosition position) throws Exception {
        require(!value.applied, "Repeated schema transition");
        if (value.change.abortedCreation()) {
            // No definition ever became visible. Its complete raw lifecycle is
            // retained by F until physical deletion, including across restart.
            value.applied = true;
            return;
        }
        boolean renamed = value.change.before() != null && value.change.after() != null
                && !As400SchemaHistory.key(value.change.before()).equals(As400SchemaHistory.key(value.change.after()));
        require(!renamed || !definitions.containsKey(As400SchemaHistory.key(value.change.after())), "Rename collides with another retained file");
        require(value.change.before() != null || !definitions.containsKey(As400SchemaHistory.key(value.change.after())),
                "Creation collides with another retained file");
        if (idleTable != null && value.change.before() != null && value.change.after() != null
                && !value.change.before().schema().id().equals(value.change.after().schema().id())) {
            idleTable.accept(value.change.before().identity());
        }
        var retained = history.append(current, value.change);
        // Rebuild output only after durable history; nothing from this epoch has
        // reached the queue. Failed conversion/setup leaves the old checkpoint.
        if (value.change.after() == null) {
            String key = As400SchemaHistory.key(value.change.before());
            definitions.remove(key);
            if (value.change.retiredByRename()) {
                // No pending or intervening rows can need this alias. Do not
                // later remove a replacement binding installed under its name.
                bindings.remove(key);
            }
            // A transactional DROP's commit can still release older buffered
            // rows for this identity. Keep its decoder until the qualified D/DF;
            // F pins the opening, so a restart reconstructs this same interval.
        }
        else {
            if (renamed) {
                definitions.remove(As400SchemaHistory.key(value.change.before()));
                bindings.remove(As400SchemaHistory.key(value.change.before()));
            }
            update.accept(value.change.after());
            install(value.change.after());
        }
        current = retained.cursor();
        applied.put(current.ordinal(), new Applied(value.change.visibility(), current));
        value.applied = true;
    }

    @Override
    public As400Checkpoint retainSchemaContext(As400Checkpoint checkpoint) {
        JournalPosition replayFrom = checkpoint.replayFrom();
        boolean inclusive = checkpoint.replayInclusive();
        boolean changed;
        do {
            changed = false;
            for (var dependency : creationDependencies.entrySet()) {
                if (order.compare(replayFrom, dependency.getValue()) <= 0
                        && (order.compare(replayFrom, dependency.getKey()) > 0 || replayFrom.equals(dependency.getKey()) && !inclusive)) {
                    replayFrom = dependency.getKey();
                    inclusive = true;
                    changed = true;
                }
            }
        } while (changed);
        if (!replayFrom.equals(checkpoint.replayFrom()) || inclusive != checkpoint.replayInclusive()) {
            checkpoint = new As400Checkpoint(checkpoint.journalIdentity(), checkpoint.captureFingerprint(), replayFrom, inclusive,
                    checkpoint.completedThrough(), order.incarnation(replayFrom), checkpoint.completedReceiverIdentity(),
                    checkpoint.snapshotState(), checkpoint.schemaState());
        }
        return checkpoint;
    }

    @Override
    public As400Checkpoint schemaCheckpoint(As400Checkpoint checkpoint) {
        checkpoint = retainSchemaContext(checkpoint);
        JournalPosition retained = checkpoint.replayFrom();
        creationDependencies.values().removeIf(end -> order.compare(retained, end) > 0);
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
