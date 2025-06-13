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
import java.util.concurrent.atomic.AtomicReference;

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
import io.debezium.ibmi.db2.journal.retrieve.exception.InvalidJournalFilterException;
import io.debezium.ibmi.db2.journal.retrieve.exception.InvalidPositionException;
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
            JournalCode.C };
    private static final JournalEntryType[] REQURED_ENTRY_TYPES = new JournalEntryType[]{ JournalEntryType.PT,
            JournalEntryType.PX, JournalEntryType.UP, JournalEntryType.UB, JournalEntryType.DL, JournalEntryType.DR,
            JournalEntryType.CT, JournalEntryType.CG, JournalEntryType.SC, JournalEntryType.CM };
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
    private AtomicReference<Job> ibmiJob = new AtomicReference<>();

    public RetrieveJournal(RetrieveConfig config, JournalInfoRetrieval journalRetrieval) {
        this.config = config;
        journalReceivers = new ReceiverPagination(journalRetrieval, config.maxServerSideEntries(), config.journalInfo());

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
    public boolean retrieveJournal(JournalProcessedPosition previousPosition) throws Exception {

        final PositionRange range = journalReceivers.findRange(config.as400().connection(), previousPosition);
        return retrieveJournal(previousPosition, range);
    }

    public void cancelJob() {
        Job job = ibmiJob.get();
        if (job == null) {
            log.debug("No job to cancel");
            return;
        }
        try {
            AS400 as400 = config.as400().connection();
            if (job != null) {
                Job killer; // create a new instance with a new connection as current connection is tied up with this job
                if (as400 instanceof SecureAS400) {
                    killer = new Job(new SecureAS400(as400), job.getName(), job.getUser(), job.getNumber());
                }
                else {
                    killer = new Job(new AS400(as400), job.getName(), job.getUser(), job.getNumber());
                }
                log.info("Killing job {}/{}/{}", job.getName(), job.getUser(), job.getNumber());
                killer.end(0);
            }
        }
        catch (Exception e) {
            log.error("Failed to cancel job name {} user {} number {}", job.getName(), job.getUser(), job.getNumber(), e);
        }
    }

    public boolean retrieveJournal(JournalProcessedPosition previousPosition, final PositionRange range)
            throws Exception {
        this.offset = -1;
        this.entryHeader = null;
        this.position = new JournalProcessedPosition(previousPosition);

        // will return data for both first entry and last entry
        // but call fails if start == end
        if (range.startEqualsEnd()) {
            this.header = new FirstHeader(0, 0, 0, OffsetStatus.NOT_CALLED,
                    new JournalProcessedPosition(range.end(), Instant.EPOCH, true));

            log.debug("start equals end - range {}", range);
            return true;
        }

        // TODO end could be optional for filtering or use same mechanism as non
        // filtering?
        final JournalProcessedPosition end = new JournalProcessedPosition(range.end(), Instant.EPOCH, true);

        final ServiceProgramCall spc = new ServiceProgramCall(config.as400().connection());
        spc.getServerJob().setLoggingLevel(0);
        builder.init();
        builder.withBufferLenth(config.journalBufferSize());
        builder.filterJournalEntryType(REQURED_ENTRY_TYPES);
        builder.filterJournalCodes(REQUIRED_JOURNAL_CODES);
        if (config.filtering() && !config.includeFiles().isEmpty()) {
            builder.withFileFilters(config.includeFiles());
        }
        builder.withRange(range);
        final ProgramParameter[] parameters = builder.build();

        spc.setProgram(JournalInfoRetrieval.JOURNAL_SERVICE_LIB, parameters);
        spc.setProcedureName("QjoRetrieveJournalEntries");
        spc.setAlignOn16Bytes(true);
        spc.setReturnValueFormat(ServiceProgramCall.RETURN_INTEGER);
        ibmiJob.set(spc.getServerJob()); // capture so we can asynchronously cancel it
        final boolean success = spc.run();
        ibmiJob.set(null); // job finished
        if (success) {
            outputData = parameters[0].getOutputData();
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
        return success;
    }

    private boolean reThrowIfFatal(JournalProcessedPosition retrievePosition, final ServiceProgramCall spc,
                                   JournalProcessedPosition latestJournalPosition, final ParameterListBuilder builder)
            throws InvalidPositionException, InvalidJournalFilterException, RetrieveJournalException {
        for (final AS400Message id : spc.getMessageList()) {
            final String idt = id.getID();
            if (idt == null) {
                log.error("Call failed position {} parameters {} no Id, message: {}", retrievePosition, builder, id.getText());
                continue;
            }
            switch (idt) {
                case "CPF7053": { // sequence number does not exist or break in receivers
                    throw new InvalidPositionException(
                            String.format("Call failed position %s parameters %s failed to find sequence or break in receivers: %s",
                                    retrievePosition, builder, getFullAS400MessageText(id)));
                }
                case "CPF9801": { // specify invalid receiver
                    throw new InvalidPositionException(String.format("Call failed position %s parameters %s failed to find receiver: %s",
                            retrievePosition, builder, getFullAS400MessageText(id)));
                }
                case "CPF7054": { // e.g. last < first
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
                    return true;
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

    public static class RetrieveJournalException extends Exception {
        private static final long serialVersionUID = 1L;

        public RetrieveJournalException(String message) {
            super(message);
        }
    }

}
