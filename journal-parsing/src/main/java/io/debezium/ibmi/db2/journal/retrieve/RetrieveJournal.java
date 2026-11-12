/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.Job;
import com.ibm.as400.access.MessageFile;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.SecureAS400;
import com.ibm.as400.access.ServiceProgramCall;

import io.debezium.ibmi.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalCriteria.JournalEntryType;
import io.debezium.ibmi.db2.journal.retrieve.exception.FatalException;
import io.debezium.ibmi.db2.journal.retrieve.exception.InvalidJournalFilterException;
import io.debezium.ibmi.db2.journal.retrieve.exception.InvalidPositionException;
import io.debezium.ibmi.db2.journal.retrieve.exception.LostJournalException;
import io.debezium.ibmi.db2.journal.retrieve.exception.RetrieveJournalException;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.FirstHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.FirstHeaderDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.OffsetStatus;

/**
 * based on the work of Stanley Vong see
 * https://www.ibm.com/docs/en/i/7.5?topic=ssw_ibm_i_75/apis/QJORJRNE.html
 */
public class RetrieveJournal {
    private static final Logger log = LoggerFactory.getLogger(RetrieveJournal.class);

    private static final JournalCode[] REQUIRED_JOURNAL_CODES = new JournalCode[]{ JournalCode.D, JournalCode.R,
            JournalCode.C, JournalCode.F };
    private static final JournalCode[] HISTORY_CONTROL_CODES = new JournalCode[]{ JournalCode.C, JournalCode.J };
    private static final JournalEntryType[] REQURED_ENTRY_TYPES = new JournalEntryType[]{ JournalEntryType.PT,
            JournalEntryType.PX, JournalEntryType.UP, JournalEntryType.UB, JournalEntryType.DL, JournalEntryType.DR,
            JournalEntryType.CT, JournalEntryType.CG, JournalEntryType.CR, JournalEntryType.SC, JournalEntryType.CM };
    private final FirstHeaderDecoder firstHeaderDecoder = new FirstHeaderDecoder();
    private final EntryHeaderDecoder entryHeaderDecoder = new EntryHeaderDecoder();
    private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyMMdd-hhmm");
    private final ReceiverPagination journalReceivers;
    private final ParameterListBuilder builder = new ParameterListBuilder();

    RetrieveConfig config;
    private byte[] outputData = null;
    private FirstHeader header = null;
    private EntryHeader entryHeader = null;
    private int offset = -1;
    private JournalProcessedPosition position;
    private long totalTransferred = 0;
    // One reader, with asynchronous cancellation. Never hold this monitor while
    // obtaining the RPC connection: close() acquires its connection lock first.
    private final Object activeReadLock = new Object();
    private ActiveRead activeRead;
    private final JournalReadLifecycle lifecycle;
    private RawJournalPage rawPage;

    private static final class ActiveRead {
        private final AS400 connection;
        private final Job job;
        private boolean cancellationRequested;

        private ActiveRead(AS400 connection, Job job) {
            this.connection = connection;
            this.job = job;
        }
    }

    public RetrieveJournal(RetrieveConfig config, JournalInfoRetrieval journalRetrieval) {
        this(config, journalRetrieval, new JournalReadLifecycle());
    }

    public RetrieveJournal(RetrieveConfig config, JournalInfoRetrieval journalRetrieval, JournalReadLifecycle lifecycle) {
        this.config = config;
        this.lifecycle = lifecycle;
        journalReceivers = new ReceiverPagination(journalRetrieval, config.maxServerSideEntries(), config.journalInfo(), lifecycle);

        builder.withJournal(config.journalInfo().journalName(), config.journalInfo().journalLibrary());
    }

    /**
     * retrieves a block of journal data
     *
     * @param previousPosition
     * @return true if the journal was read successfully false if there was some
     *         problem reading the journal
     * @throws Exception
     *
     *                   CURAVLCHN - returns only available journals CURCHAIN will
     *                   work though journals that have happened but may no longer
     *                   be available if the journal is no longer available we need
     *                   to capture this and log an error as we may have missed data
     */
    public RetrievalState retrieveJournal(JournalProcessedPosition previousPosition) throws Exception {

        try (var ignored = lifecycle.enter()) {
            final AS400 connection = config.as400().connection();
            try {
                final Optional<PositionRange> rangeOpt = journalReceivers.findRange(connection, previousPosition);
                lifecycle.checkOpen();
                return rangeOpt.map(range -> {
                    try {
                        return retrieveJournal(previousPosition, range);
                    }
                    catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }).orElse(RetrievalState.NotCalled);
            }
            finally {
                disconnectIfStopped(connection);
            }
        }
    }

    /** Diagnostic snapshot only: it does not grant admission or certify drain. */
    public record ReadState(boolean registered, boolean cancellationRequested, String job) {
    }

    public ReadState readState() {
        synchronized (activeReadLock) {
            return activeRead == null ? new ReadState(false, false, "")
                    : new ReadState(true, activeRead.cancellationRequested, activeRead.job.getQualifiedJobName());
        }
    }

    public void cancelJob() {
        synchronized (activeReadLock) {
            ActiveRead read = activeRead;
            if (read == null || read.cancellationRequested) {
                return;
            }
            read.cancellationRequested = true;
            AS400 cancellationConnection = null;
            try {
                // Use the read's captured connection. Calling config.as400().connection()
                // here would re-enter close/reconnect when that transport is down.
                cancellationConnection = read.connection instanceof SecureAS400
                        ? new SecureAS400(read.connection)
                        : new AS400(read.connection);
                Job job = read.job;
                Job killer = new Job(cancellationConnection, job.getName(), job.getUser(), job.getNumber());
                log.info("Killing job {}/{}/{}", job.getName(), job.getUser(), job.getNumber());
                killer.end(0);
            }
            catch (Exception e) {
                log.error("Failed to cancel active journal read", e);
            }
            finally {
                if (cancellationConnection != null) {
                    try {
                        cancellationConnection.disconnectAllServices();
                    }
                    catch (Exception e) {
                        log.warn("Failed to disconnect journal cancellation connection", e);
                    }
                }
            }
            // Keep the read registered until its finally block. A repeated stop
            // cannot retry an old job handle, and completion/new registration cannot
            // race cancellation's use of that handle or its temporary connection.
        }
    }

    public RetrievalState retrieveJournal(JournalProcessedPosition previousPosition, final PositionRange range)
            throws Exception {
        try (var ignored = lifecycle.enter()) {
            return retrieveAdmitted(previousPosition, range);
        }
    }

    /**
     * Explicit single-receiver read for the operator-managed recovery path. Selects
     * every C/D/F/R type without object/file selection; never uses receiver
     * pagination caches, file filters, suffix reset, empty-response advancement or
     * oversized-entry skipping. The caller supplies and verifies receiver identity.
     */
    public RawJournalPage readRawPage(JournalPosition from, JournalPosition through, int maximumEntries) throws Exception {
        return readRawPage(from, through, maximumEntries, RawSelection.CDC);
    }

    /**
     * Bounded discovery of commitment and journal-maintenance controls only.
     * This selection cannot establish row/lifecycle completeness or a publication
     * checkpoint. It has a distinct result type and never advances the legacy
     * position. All C/J types are returned without file, job or entry-count filters.
     */
    public ControlPage readControlHistoryPage(JournalPosition from, JournalPosition through, int maximumEntries) throws Exception {
        RawJournalPage page = readRawPage(from, through, maximumEntries, RawSelection.CONTROLS);
        return new ControlPage(page.entries(), page.continuation(), page.bytesReturned());
    }

    /**
     * All-code audit for an explicitly bounded native startup interval. Unlike
     * CDC and control discovery this omits code selection entirely, including
     * codes not represented by the local enum. It preserves the same owned
     * response, structured error, pointer, continuation and cancellation checks.
     * Completion is not an incarnation, schema or transaction-history proof.
     */
    public AuditPage readAuditHistoryPage(JournalPosition from, JournalPosition through, int maximumEntries) throws Exception {
        RawJournalPage page = readRawPage(from, through, maximumEntries, RawSelection.AUDIT);
        return new AuditPage(page.entries(), page.continuation(), page.bytesReturned());
    }

    public record AuditPage(List<RawJournalPage.Entry> entries, Optional<JournalPosition> continuation, int bytesReturned) {
        public AuditPage {
            entries = List.copyOf(entries);
        }
    }

    private enum RawSelection {
        CDC,
        CONTROLS,
        AUDIT
    }

    public record ControlPage(List<RawJournalPage.Entry> entries, Optional<JournalPosition> continuation, int bytesReturned) {
        public ControlPage {
            entries = List.copyOf(entries);
        }
    }

    private RawJournalPage readRawPage(JournalPosition from, JournalPosition through, int maximumEntries, RawSelection selection) throws Exception {
        RawJournalPage.validateRange(from, through, maximumEntries);
        if (config.journalBufferSize() < 512 || config.journalBufferSize() > 16 * 1024 * 1024) {
            throw new IllegalArgumentException("Bounded raw response buffer required");
        }
        try (var ignored = lifecycle.enter()) {
            rawPage = null;
            JournalProcessedPosition start = new JournalProcessedPosition(from, Instant.EPOCH, false);
            AS400 connection = config.as400().connection();
            try {
                callAdmitted(start, new PositionRange(false, start, through),
                        new JournalProcessedPosition(through, Instant.EPOCH, true), connection, maximumEntries, selection);
                lifecycle.checkOpen();
                return rawPage;
            }
            finally {
                rawPage = null;
                disconnectIfStopped(connection);
            }
        }
    }

    private RetrievalState retrieveAdmitted(JournalProcessedPosition previousPosition, PositionRange range) throws Exception {
        this.offset = -1;
        this.entryHeader = null;
        this.position = new JournalProcessedPosition(previousPosition);

        // will return data for both first entry and last entry
        // but call fails if start == end
        if (range.startEqualsEnd()) {
            this.header = new FirstHeader(0, 0, 0, OffsetStatus.NOT_CALLED,
                    new JournalProcessedPosition(range.end(), Instant.EPOCH, true));

            log.debug("start equals end - range {}", range);
            return RetrievalState.NotCalled;
        }

        // TODO end could be optional for filtering or use same mechanism as non
        // filtering?
        final JournalProcessedPosition end = new JournalProcessedPosition(range.end(), Instant.EPOCH, true);

        final AS400 connection = config.as400().connection();
        try {
            return callAdmitted(previousPosition, range, end, connection);
        }
        finally {
            disconnectIfStopped(connection);
        }
    }

    private void disconnectIfStopped(AS400 connection) {
        if (lifecycle.isStopped()) {
            try {
                connection.disconnectAllServices();
            }
            catch (Exception e) {
                log.warn("Failed to disconnect stopped journal operation", e);
            }
        }
    }

    private RetrievalState callAdmitted(JournalProcessedPosition previousPosition, PositionRange range,
                                        JournalProcessedPosition end, AS400 connection)
            throws Exception {
        return callAdmitted(previousPosition, range, end, connection, 0, RawSelection.CDC);
    }

    private RetrievalState callAdmitted(JournalProcessedPosition previousPosition, PositionRange range,
                                        JournalProcessedPosition end, AS400 connection, int rawEntryLimit, RawSelection selection)
            throws Exception {
        lifecycle.checkOpen();
        final ServiceProgramCall spc = new ServiceProgramCall(connection);
        lifecycle.checkOpen();
        final Job job = spc.getServerJob();
        lifecycle.checkOpen();
        if (rawEntryLimit > 0) {
            // The owned native profile must not inherit the worker locale.
            // Qualify the actual API job before encoding names or retrieving bytes.
            // Each call observes the job again, including after reconnect.
            if (job.getCodedCharacterSetID() != RawJournalPage.METADATA_CCSID) {
                throw new IllegalArgumentException("Native journal retrieval requires an API job with CCSID " + RawJournalPage.METADATA_CCSID);
            }
            lifecycle.checkOpen();
        }
        else {
            job.setLoggingLevel(0);
        }
        final ParameterListBuilder builder = rawEntryLimit == 0 ? this.builder
                : new ParameterListBuilder(RawJournalPage.METADATA_CCSID)
                        .withJournal(config.journalInfo().journalName(), config.journalInfo().journalLibrary());
        builder.init();
        builder.withBufferLenth(config.journalBufferSize());
        // An allowlist must not hide rollback/savepoint or unknown C controls from
        // a transaction-aware consumer. Legacy consumers retain their existing filter.
        if (rawEntryLimit == 0) {
            builder.filterJournalEntryType(config.transactionControls() ? new JournalEntryType[]{ JournalEntryType.ALL } : REQURED_ENTRY_TYPES);
            builder.filterJournalCodes(REQUIRED_JOURNAL_CODES, config.transactionControls() && config.filtering() && !config.includeFiles().isEmpty());
            if (config.filtering() && !config.includeFiles().isEmpty()) {
                builder.withFileFilters(config.includeFiles());
            }
        }
        else {
            // NBRENT is deliberately omitted: IBM i treats the count limit as a
            // completed selection and may return continuation=0 before the cut ends.
            builder.withVariableNullIndicators();
            if (selection != RawSelection.AUDIT) {
                builder.filterJournalCodes(selection == RawSelection.CONTROLS ? HISTORY_CONTROL_CODES : REQUIRED_JOURNAL_CODES);
            }
        }
        builder.withRange(range);
        final ProgramParameter[] parameters = builder.build();
        if (rawEntryLimit > 0) {
            // With escape-message errors JT400 withholds the output buffer. An
            // explicit by-reference QUS error structure preserves it for strict
            // classification; successful transport does not imply API success.
            byte[] error = new byte[RawJournalPage.ERROR_BYTES];
            java.nio.ByteBuffer.wrap(error).putInt(error.length);
            parameters[5] = new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, error, error.length);
        }

        lifecycle.checkOpen();
        spc.setProgram(JournalInfoRetrieval.JOURNAL_SERVICE_LIB, parameters);
        spc.setProcedureName("QjoRetrieveJournalEntries");
        spc.setAlignOn16Bytes(true);
        spc.setReturnValueFormat(ServiceProgramCall.RETURN_INTEGER);
        final ActiveRead read = new ActiveRead(connection, job);
        synchronized (activeReadLock) {
            lifecycle.checkOpen();
            if (activeRead != null) {
                throw new IllegalStateException("A journal read is already active");
            }
            activeRead = read;
        }
        final boolean success;
        try (var ignored = lifecycle.enter()) {
            // Registration precedes admission to run. Stop either rejects this
            // lease, or cancels and drains it before reporting completion.
            success = spc.run();
            if (rawEntryLimit > 0) {
                if (!success || spc.getMessageList().length != 0) {
                    throw new RetrieveJournalException("Bounded raw journal retrieval failed; no entries or continuation accepted: "
                            + Arrays.stream(spc.getMessageList()).map(AS400Message::getID).toList());
                }
                rawPage = RawJournalPage.decodeResult(parameters[0].getOutputData(), parameters[5].getOutputData(),
                        range.start().asJournalPosition(), range.end(), rawEntryLimit, handle -> releasePointer(spc, job, handle));
                if (selection == RawSelection.CONTROLS
                        && rawPage.entries().stream().anyMatch(e -> e.header().getJournalCode() != 'C' && e.header().getJournalCode() != 'J')) {
                    throw new RetrieveJournalException("Control-history response contains an unrequested journal code");
                }
            }
        }
        catch (Exception e) {
            if (rawEntryLimit > 0) {
                // A partial/unsupported response may have allocated native pointer
                // handles. End only this captured read job; never reuse its handles
                // on another connection or advance past the failed response.
                cancelJob();
            }
            throw e;
        }
        finally {
            synchronized (activeReadLock) {
                if (activeRead == read) {
                    activeRead = null;
                }
            }
        }
        lifecycle.checkOpen();
        if (success) {
            outputData = parameters[0].getOutputData();
            if (rawEntryLimit > 0) {
                return rawPage.continuation().isPresent() ? RetrievalState.MoreDataAvailable : RetrievalState.Success;
            }
            header = firstHeaderDecoder.decode(outputData, end);
            totalTransferred += header.totalBytes();
            log.debug("retrieve from {} to {} header {}", range.start(), range.end(), header);
            offset = -1;
            if (header.status() == OffsetStatus.MORE_DATA_NEW_OFFSET && header.offset() == 0) {
                log.error("buffer too small need to skip this entry {}", previousPosition);
                this.position.setPosition(header.nextPosition());
            }
            if (!hasData()) {
                this.position.setPosition(end);
            }
        }
        else {
            log.debug("retrieve from {} to {} status {}", range.start(), range.end(), success);
            return reThrowIfFatal(previousPosition, spc, end, builder);
        }
        if (futureDataAvailable()) {
            return RetrievalState.MoreDataAvailable;
        }
        return RetrievalState.Success;
    }

    private void releasePointer(ServiceProgramCall call, Job expectedJob, long handle) {
        try {
            lifecycle.checkOpen();
            Job current = call.getServerJob();
            if (!current.getName().equals(expectedJob.getName()) || !current.getUser().equals(expectedJob.getUser())
                    || !current.getNumber().equals(expectedJob.getNumber())) {
                throw new IllegalStateException("Pointer cleanup cannot move to another API job");
            }
            byte[] error = new byte[RawJournalPage.ERROR_BYTES];
            java.nio.ByteBuffer.wrap(error).putInt(error.length);
            var parameters = new ProgramParameter[]{
                    new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, new com.ibm.as400.access.AS400UnsignedBin4().toBytes(handle)),
                    new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, error, error.length)
            };
            call.setProgram(JournalInfoRetrieval.JOURNAL_SERVICE_LIB, parameters);
            call.setProcedureName("QjoDeletePointerHandle");
            call.setReturnValueFormat(ServiceProgramCall.NO_RETURN_VALUE);
            lifecycle.checkOpen();
            if (!call.run() || call.getMessageList().length != 0
                    || java.nio.ByteBuffer.wrap(parameters[1].getOutputData()).getInt(4) != 0) {
                throw new IllegalStateException("Creation metadata pointer cleanup failed");
            }
            lifecycle.checkOpen();
        }
        catch (Exception failure) {
            // The caller ends this captured API job on every failed response;
            // incomplete cleanup never permits publication or continuation.
            throw new IllegalStateException("Cannot release native creation pointer", failure);
        }
    }

    private RetrievalState reThrowIfFatal(JournalProcessedPosition retrievePosition, final ServiceProgramCall spc,
                                          JournalProcessedPosition latestJournalPosition, final ParameterListBuilder builder)
            throws LostJournalException, FatalException {
        for (final AS400Message id : spc.getMessageList()) {
            final String idt = id.getID();
            if (idt == null) {
                log.error("Call failed position {} parameters {} no Id, message: {}", retrievePosition, builder, id.getText());
                continue;
            }
            switch (idt) {
                case "CPF7053": { // sequence number does not exist or break in receivers
                    throw new LostJournalException(String.format("Call failed %s position %s parameters %s failed to find sequence or break in receivers: %s",
                            idt, retrievePosition, builder, getFullAS400MessageText(id)));
                }
                case "CPF9801": { // specify invalid receiver
                    throw new LostJournalException(String.format("Call failed %s position %s parameters %s failed to find sequence or break in receivers: %s",
                            idt, retrievePosition, builder, getFullAS400MessageText(id)));
                }
                case "CPF7054": { // e.g. last < first or using offset that doesn't belong to journal
                    throw new InvalidPositionException(
                            String.format("Call failed position %s parameters %s failed to find offset or invalid offsets: %s",
                                    retrievePosition, builder, id.getText()));
                }
                case "CPF7060": { // object in filter doesn't exist, or was not journaled
                    throw new InvalidJournalFilterException(
                            String.format("Call failed position %s parameters %s object not found or not journaled: %s", retrievePosition, builder,
                                    getFullAS400MessageText(id)));
                }
                case "CPF7062": {
                    log.debug("Normal when filtering, call failed position {} parameters {} no data received: {}", retrievePosition, builder,
                            id.getText());
                    // if we're filtering we get no continuation offset just an error
                    header = new FirstHeader(0, 0, 0, OffsetStatus.NO_DATA, latestJournalPosition);
                    this.position.setPosition(latestJournalPosition);
                    return RetrievalState.Success;
                }
                default:
                    log.error("Call failed position {} parameters {} with error code {} message {}", retrievePosition, idt,
                            builder, getFullAS400MessageText(id));
            }
        }
        throw new RetrieveJournalException(String.format("Call failed position %s", retrievePosition));
    }

    boolean shouldLimitRange() {
        return config.filtering();
    }

    private String getFullAS400MessageText(AS400Message message) {
        try {
            message.load(MessageFile.RETURN_FORMATTING_CHARACTERS);
            return String.format("%s %s", message.getText(), message.getHelp());
        }
        catch (final Exception e) {
            return message.getText();
        }
    }

    /**
     * @return the current position or the next offset for fetching data when the
     *         end of data is reached
     */
    public JournalProcessedPosition getPosition() {
        return position;
    }

    public void setOutputData(byte[] b, FirstHeader header, JournalProcessedPosition position) {
        outputData = b;
        this.offset = -1;
        this.entryHeader = null;
        this.header = header;
        this.position = position;
    }

    // test without moving on
    public boolean hasData() {
        if (header.status() == OffsetStatus.NO_DATA) {
            return false;
        }
        if (offset < 0 && header.size() > 0) {
            return true;
        }
        return (offset > 0 && entryHeader.getNextEntryOffset() > 0);
    }

    public boolean futureDataAvailable() {
        return (header.hasFutureDataAvailable());
    }

    public boolean nextEntry() {
        if (header == null) {
            return false;
        }
        if (offset < 0) {
            if (header.size() > 0) {
                offset = header.offset();
                entryHeader = entryHeaderDecoder.decode(outputData, offset);
                if (alreadyProcessed(position, entryHeader)) {
                    log.debug("skipping already seen entry {} {}", position, entryHeader);
                    return nextEntry();
                }
                updatePosition(position, entryHeader);
                return true;
            }
            else {
                return false;
            }
        }
        else {
            final long nextOffset = entryHeader.getNextEntryOffset();
            if (nextOffset > 0) {
                offset += (int) nextOffset;
                entryHeader = entryHeaderDecoder.decode(outputData, offset);
                updatePosition(position, entryHeader);
                return true;
            }

            updateOffsetFromContinuation();
            return false;
        }
    }

    private void updateOffsetFromContinuation() {
        // after we hit the end use the continuation header for the next offset
        final JournalProcessedPosition nextOffset = header.nextPosition();
        log.debug("Setting continuation offset {}", nextOffset);
        position.setPosition(nextOffset);
    }

    static boolean alreadyProcessed(JournalProcessedPosition position, EntryHeader entryHeader) {
        return position.processed() && position.getOffset().equals(entryHeader.getSequenceNumber()) && (!entryHeader.hasReceiver() ||
                (entryHeader.getReceiverLibrary().equals(position.getReceiver().library()) && entryHeader.getReceiver().equals(position.getReceiver().name())));

    }

    private static void updatePosition(JournalProcessedPosition p, EntryHeader entryHeader) {
        if (entryHeader.hasReceiver()) {
            log.debug("offset with receiver {}", entryHeader.getReceiver());
            p.setJournalReceiver(entryHeader.getSequenceNumber(), entryHeader.getReceiver(),
                    entryHeader.getReceiverLibrary(), entryHeader.getTime(), true);
        }
        else {
            // this happens a lot
            log.debug("offset no receiver {}", entryHeader);
            p.setOffset(entryHeader.getSequenceNumber(), entryHeader.getTime(), true);
        }
    }

    public EntryHeader getEntryHeader() {
        return entryHeader;
    }

    public void dumpEntry() {
        final int start = offset + entryHeader.getEntrySpecificDataOffset();
        final long end = entryHeader.getNextEntryOffset();
        log.debug("total offset {} entry specific offset {} next offset {}", start, entryHeader.getEntrySpecificDataOffset(), end);
    }

    public int getOffset() {
        return offset;
    }

    public <T> T decode(JournalEntryDeocder<T> decoder) throws Exception {
        // Diagnostics.dump(outputData, start);
        try {
            final T t = decoder.decode(entryHeader, outputData, offset);
            return t;
        }
        catch (final Exception e) {
            dumpEntryToFile(config.dumpFolder());
            throw e;
        }
    }

    public void dumpEntryToFile(File path) {
        File dumpFile = null;
        if (path != null) {
            boolean created = false;
            for (int i = 0; !created && i < 100; i++) {

                final String formattedDate = dateFormatter.format(new Date());
                final File f = new File(path, String.format("%s-%s", formattedDate, Integer.toString(i)));
                try {
                    created = f.createNewFile();
                    if (created) {
                        dumpFile = f;
                    }
                }
                catch (final IOException e) {
                    log.error("unable to dump to file", e);
                }
            }
            if (dumpFile != null) {
                try {
                    final int start = offset;
                    final int end = outputData.length;

                    final byte[] bdata = Arrays.copyOfRange(outputData, start, end);
                    Files.write(dumpFile.toPath(), bdata);

                    final File entryInfo = new File(dumpFile.getPath() + ".txt");

                    try (FileWriter fw = new FileWriter(entryInfo, true);
                            BufferedWriter bw = new BufferedWriter(fw);
                            PrintWriter out = new PrintWriter(bw)) {
                        out.println(entryHeader.toString());
                        out.print("dumped: ");
                        out.println(end - start);
                        out.print("total length: ");
                        out.println(outputData.length);
                    }
                }
                catch (final IOException e) {
                    log.error("failed to dump problematic data", e);
                }
            }
            else {
                log.error("failed to create a dump file");
            }
        }
    }

    public FirstHeader getFirstHeader() {
        return header;
    }

    public long getTotalTransferred() {
        return totalTransferred;
    }
}
