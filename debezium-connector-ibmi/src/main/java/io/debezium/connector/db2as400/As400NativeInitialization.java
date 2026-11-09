/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.List;
import java.util.Objects;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Request;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;

/**
 * Explicit controlled initialization, never a missing-offset recovery fallback.
 * Native acquisition/census/boundary checks are implemented here. A deployment
 * must supply and qualify the journal-wide fence and acknowledged snapshot/output
 * handoff. The configured provider requires an explicit planned first-start window.
 */
final class As400NativeInitialization {
    /**
     * Integration contract, NOT implemented by catalog observations or table locks.
     * Covers all journal producers (including API and uncaptured-table cycles), DDL
     * and maintenance, retention through handoff, and exclusive output ownership.
     * Failure/crash MUST leave the enrollment quarantined; never auto-release it.
     * The caller retains this session until a reader and its offset are durable.
     */
    interface Session {
        String checkHeld(JournalReceiver journal, List<Request> captured) throws Exception;

        /**
         * Finish all snapshot reads and close cursors before choosing B. The
         * separately admitted chunk protocol can deliver durably acknowledged
         * pending chunks here; only acknowledgeBaseline establishes completion.
         * Cursor open/close can append journal entries. No source reads or writes
         * may be deferred to final baseline acknowledgement.
         */
        void prepareBaseline(String epoch, List<As400BindingDefinition> definitions) throws Exception;

        /**
         * Deliver the consistent snapshot/new downstream generation at B. Return
         * only after durable source output acknowledgement through Kafka Connect.
         * A queued record or a completed SELECT is not an acknowledgement.
         */
        String acknowledgeBaseline(String epoch, Boundary boundary, List<As400BindingDefinition> definitions) throws Exception;

        /**
         * Atomically verify ownership/fence admission, install the exact checkpoint
         * and reader continuation, and await their durable acknowledgement. Never
         * release the fence here; an uncertain outcome requires explicit recovery.
         */
        void installCheckpoint(String epoch, As400Checkpoint checkpoint, As400BindingHistory.Recovered bindings) throws Exception;
    }

    record Boundary(String journalIdentity, JournalPosition position, String receiverIdentity) {
    }

    private final As400NativeBindingAcquisition acquisition;
    private final As400NativeTransactionCensus census;
    private final As400OperatorJournalEvidence journals;
    private final As400JdbcNativeIdentityEvidence identities;
    private final As400OperatorTableEvidence tables;
    private final RetrieveJournal reader;
    private final As400OperatorCutReader.Limits limits;
    private final List<Request> requests;
    private final As400CaptureFingerprint.Policy policy;
    private final As400BindingHistory history;

    As400NativeInitialization(As400NativeBindingAcquisition acquisition, As400NativeTransactionCensus census,
                              As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities,
                              As400OperatorTableEvidence tables, RetrieveJournal reader, As400OperatorCutReader.Limits limits,
                              List<Request> requests, As400CaptureFingerprint.Policy policy, As400BindingHistory history) {
        this.acquisition = Objects.requireNonNull(acquisition);
        this.census = Objects.requireNonNull(census);
        this.journals = Objects.requireNonNull(journals);
        this.identities = Objects.requireNonNull(identities);
        this.tables = Objects.requireNonNull(tables);
        this.reader = Objects.requireNonNull(reader);
        this.limits = Objects.requireNonNull(limits);
        this.requests = List.copyOf(requests);
        this.policy = Objects.requireNonNull(policy);
        this.history = Objects.requireNonNull(history);
        require(!requests.isEmpty() && requests.stream().map(Request::journal).distinct().count() == 1, "One captured journal required");
    }

    /** Returns only after history readback and the session's durable initial-offset handoff. */
    As400Checkpoint initialize(Session session) throws Exception {
        Objects.requireNonNull(session, "Qualified initialization session required");
        require(!Thread.currentThread().isInterrupted(), "Initialization stopped");
        JournalReceiver journal = requests.get(0).journal();
        String epoch = session.checkHeld(journal, requests);
        new As400BindingHistory.Initialization(epoch, "pending");
        census.requireEmpty(journal);
        held(session, epoch);
        var acquired = acquisition.collect(requests);
        held(session, epoch);
        session.prepareBaseline(epoch, acquired.definitions());
        held(session, epoch);
        var boundary = boundary(acquired.definitions());
        held(session, epoch);
        census.requireEmpty(journal);
        held(session, epoch);
        String receipt = session.acknowledgeBaseline(epoch, boundary, acquired.definitions());
        held(session, epoch);
        var initialization = new As400BindingHistory.Initialization(epoch, receipt);
        recheck(session, epoch, acquired, boundary);
        // History includes the acknowledged baseline identity in its fingerprint.
        // A crash before/after this write cannot turn missing offsets into resume.
        var checkpoint = history.recordInitializedBaseline(boundary.journalIdentity(), boundary.position(), boundary.receiverIdentity(),
                acquired.definitions(), policy, initialization);
        held(session, epoch);
        var recovered = history.recover(checkpoint.toOffset(), boundary.journalIdentity(), policy);
        require(recovered.initialization().orElseThrow().equals(initialization), "Baseline history readback differs");
        recheck(session, epoch, acquired, boundary);
        session.installCheckpoint(epoch, checkpoint, recovered);
        held(session, epoch);
        return checkpoint;
    }

    private void recheck(Session session, String epoch, As400NativeBindingAcquisition.Acquired acquired, Boundary boundary) throws Exception {
        held(session, epoch);
        census.requireEmpty(requests.get(0).journal());
        held(session, epoch);
        require(acquired.equals(acquisition.collect(requests)), "Native schema/key/identity changed during initialization");
        held(session, epoch);
        require(boundary.equals(boundary(acquired.definitions())), "Starting boundary changed during initialization");
        held(session, epoch);
    }

    private void held(Session session, String epoch) throws Exception {
        require(!Thread.currentThread().isInterrupted() && epoch.equals(session.checkHeld(requests.get(0).journal(), requests)),
                "Initialization stopped or fence epoch changed");
    }

    private Boundary boundary(List<As400BindingDefinition> definitions) throws Exception {
        var observation = journals.collect();
        require(observation.journal().equals(requests.get(0).journal()), "Initialization journal differs");
        var attached = observation.receivers().stream().filter(r -> r.name().equals(observation.attached())).toList();
        require(attached.size() == 1, "Ambiguous boundary receiver");
        attached.get(0).requireReadableProfile();
        var journalTarget = target(observation.journal(), Kind.JOURNAL);
        var receiverTarget = target(observation.attached(), Kind.RECEIVER);
        var objects = identities.collect(List.of(journalTarget, receiverTarget));
        String journalIdentity = As400OperatorCutReader.token(journalTarget, objects.objects().get(journalTarget));
        String receiverIdentity = As400OperatorCutReader.token(receiverTarget, objects.objects().get(receiverTarget));
        var position = new JournalPosition(attached.get(0).last(), observation.attached());
        // This cursor exists ONLY to probe the exact native boundary with the strict
        // reader. It is never exposed as a checkpoint or used to initialize history.
        var probe = As400Checkpoint.afterVerifiedBoundary(journalIdentity,
                policy.fingerprint(definitions.stream().map(As400BindingDefinition::identity).toList()), position, receiverIdentity);
        var cut = new As400OperatorCutReader(journals, identities, tables, requests, reader, limits,
                definitions.stream().map(As400BindingDefinition::identity).toList(), policy).read(probe.toOffset());
        require(cut.tail().equals(position) && cut.entries().isEmpty(), "Boundary advanced while writers were fenced");
        return new Boundary(journalIdentity, position, receiverIdentity);
    }

    private static Target target(JournalReceiver name, Kind kind) {
        return new Target("/QSYS.LIB/" + name.library() + ".LIB/" + name.name() + (kind == Kind.JOURNAL ? ".JRN" : ".JRNRCV"), kind);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
