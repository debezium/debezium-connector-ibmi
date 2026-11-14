/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.CommandCall;
import com.ibm.as400.access.Job;
import com.ibm.as400.access.ProgramCall;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.QSYSObjectPathName;
import com.ibm.as400.access.SecureAS400;

import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.ParameterListBuilder;
import io.debezium.ibmi.db2.journal.retrieve.PositionRange;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Lazy, private API job for QDBRPLAY's names-only mode. It compiles only CL in
 * QTEMP, never reconstructs a table, and never invokes database replay mode.
 * The pointer and its handle remain in the retrieving job until released.
 */
final class As400NativeHistoricalNames implements As400HistoricalTableNames.Reader, AutoCloseable {
    private final AS400 transport;
    private final JournalReceiver journal;
    private final JournalReadLifecycle lifecycle;
    private volatile Job job;
    private boolean prepared;
    private boolean ended;

    As400NativeHistoricalNames(AS400 source, JournalReceiver journal, JournalReadLifecycle lifecycle) throws java.beans.PropertyVetoException {
        transport = source instanceof SecureAS400 ? new SecureAS400(source) : new AS400(source);
        transport.setThreadUsed(false);
        this.journal = journal;
        this.lifecycle = lifecycle;
    }

    @Override
    public As400HistoricalTableNames read(RawJournalPage.Entry expected) throws Exception {
        try (var lease = lifecycle.enter()) {
            // Validate evidence and fixed API selection before admitting native work.
            As400JournalRecordContent.digest(expected.data(), expected.position());
            require(expected.header().getJournalCode() == 'D' && List.of("CT", "CG").contains(expected.header().getEntryType()),
                    "Historical names require an exact D/CT or D/CG");
            prepare();
            var at = expected.position();
            var criteria = new ParameterListBuilder(RawJournalPage.METADATA_CCSID).withJournal(journal.name(), journal.library());
            criteria.init();
            criteria.withVariableNullIndicators();
            criteria.withRange(new PositionRange(false, new JournalProcessedPosition(at, Instant.EPOCH, false), at));
            byte[] selection = criteria.build()[4].getInputData();
            require(selection.length <= 32767, "Excessive native-name selection");
            ProgramParameter output = new ProgramParameter(32767);
            call("QTEMP", "DCNAMES", text(String.format("%-10s%-10s", journal.name(), journal.library()), 20),
                    new ProgramParameter(Arrays.copyOf(selection, 32767)), output);
            byte[] result = output.getOutputData();
            require(result != null && result.length == 32767, "Incomplete native-name response");
            var fields = ByteBuffer.wrap(result);
            require(fields.getInt(0) == 0, "Native historical-name inspection or pointer release failed");
            int length = fields.getInt(4), offset = fields.getInt(8);
            require(length >= 64 && length <= 1024 * 1024 && offset >= 64 && offset <= length - 236,
                    "Invalid historical-name journal response");
            var raw = new ProgramParameter(length);
            call("QSYS", "QUSRTVUS", text("DCNMBUF   QTEMP", 20), number(1), number(length), raw);
            byte[] response = raw.getOutputData();
            require(response != null && response.length == length && ByteBuffer.wrap(response).getInt(0) == length
                    && ByteBuffer.wrap(response).getInt(4) == offset && ByteBuffer.wrap(response).getInt(8) == 1,
                    "Historical-name response changed during retrieval");
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(response, offset);
            require(header.getNextEntryOffset() == 0 && header.getEndOffset() >= 236 && (long) offset + header.getEndOffset() <= length,
                    "Invalid historical-name entry bounds");
            byte[] owned = Arrays.copyOfRange(response, offset, offset + header.getEndOffset());
            ByteBuffer.wrap(owned).putInt(0, 0);
            require(As400JournalRecordContent.digest(expected.data(), at).equals(As400JournalRecordContent.digest(owned, at)),
                    "Historical-name retrieval differs from retained journal content");
            lifecycle.checkOpen();
            return As400HistoricalTableNames.decode(expected, Arrays.copyOfRange(result, 256, 256 + 8708));
        }
    }

    private void prepare() throws Exception {
        lifecycle.checkOpen();
        if (prepared) {
            return;
        }
        transport.connectService(AS400.COMMAND);
        Job[] jobs = transport.getJobs(AS400.COMMAND);
        require(jobs.length == 1, "Historical-name inspection requires one owned API job");
        job = jobs[0];
        lifecycle.checkOpen();
        command("CHGJOB CCSID(37)");
        source("capture.clp", "DCEXSRC");
        command("CRTCLPGM PGM(QTEMP/DCNAMEEXIT) SRCFILE(QTEMP/DCEXSRC) SRCMBR(DCEXSRC) USRPRF(*USER) AUT(*EXCLUDE) OUTPUT(*NONE)");
        source("inspect.clle", "DCNMSRC");
        command("CRTCLMOD MODULE(QTEMP/DCNAMES) SRCFILE(QTEMP/DCNMSRC) SRCMBR(DCNMSRC) OUTPUT(*NONE)");
        command("CRTPGM PGM(QTEMP/DCNAMES) MODULE(QTEMP/DCNAMES) BNDSRVPGM(QSYS/QJOURNAL) BNDDIR(QSYS/QC2LE) ACTGRP(*NEW) USRPRF(*USER) AUT(*EXCLUDE)");
        space("DCNMBUF", 1024 * 1024);
        space("DCNMFLAT", 16773120);
        prepared = true;
    }

    static String resource(String name) throws IOException {
        try (var input = Objects.requireNonNull(As400NativeHistoricalNames.class.getResourceAsStream("/native-names/" + name))) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void source(String resource, String name) throws Exception {
        command("CRTSRCPF FILE(QTEMP/" + name + ") RCDLEN(240) MBR(*FILE) CCSID(37) AUT(*EXCLUDE)");
        var lines = resource(resource).lines().toList();
        for (int at = 0; at < lines.size(); at += 10) {
            var values = new ArrayList<String>();
            for (int i = at; i < Math.min(at + 10, lines.size()); i++) {
                require(lines.get(i).length() <= 228, "Native source line exceeds its record");
                values.add("(" + (i + 1) + ",0,'" + lines.get(i).replace("'", "''") + "')");
            }
            String sql = "INSERT INTO QTEMP." + name + " VALUES " + String.join(",", values);
            command("RUNSQL SQL('" + sql.replace("'", "''") + "') COMMIT(*NONE) NAMING(*SQL)");
        }
    }

    private void space(String name, int bytes) throws Exception {
        byte[] initial = new byte[512];
        ByteBuffer.wrap(initial).putInt(initial.length);
        var error = new ProgramParameter(initial, initial.length);
        call("QSYS", "QUSCRTUS", text(String.format("%-10sQTEMP", name), 20), text("NAMES", 10), number(bytes),
                new ProgramParameter(new byte[1]), text("*EXCLUDE", 10), text("Connector historical-name evidence", 50), text("*NO", 10), error, text("*USER", 10));
        require(error.getOutputData() != null && ByteBuffer.wrap(error.getOutputData()).getInt(4) == 0,
                "Cannot create bounded historical-name working space");
    }

    private void command(String command) throws Exception {
        lifecycle.checkOpen();
        var call = new CommandCall(transport);
        require(call.run(command), "Historical-name setup failed: " + messages(call.getMessageList()));
        lifecycle.checkOpen();
    }

    private void call(String library, String name, ProgramParameter... parameters) throws Exception {
        lifecycle.checkOpen();
        var call = new ProgramCall(transport, new QSYSObjectPathName(library, name, "PGM").getPath(), parameters);
        require(call.run(), "Historical-name API failed: " + messages(call.getMessageList()));
        lifecycle.checkOpen();
    }

    private static String messages(AS400Message[] messages) {
        return Arrays.toString(Arrays.stream(messages).map(AS400Message::getID).toArray());
    }

    private static ProgramParameter text(String value, int bytes) {
        return new ProgramParameter(new AS400Text(bytes, RawJournalPage.METADATA_CCSID).toBytes(value));
    }

    private static ProgramParameter number(int value) {
        return new ProgramParameter(new AS400Bin4().toBytes(value));
    }

    /** Ends only this helper's captured job; never a shared journal reader or application job. */
    synchronized void cancel() throws Exception {
        if (job == null || ended) {
            return;
        }
        AS400 cancellation = transport instanceof SecureAS400 ? new SecureAS400(transport) : new AS400(transport);
        try {
            new Job(cancellation, job.getName(), job.getUser(), job.getNumber()).end(0);
            ended = true;
        }
        finally {
            cancellation.disconnectAllServices();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            cancel(); // Ending the private job releases all QTEMP objects, including partial setup.
        }
        finally {
            transport.disconnectAllServices();
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
