/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.ProgramParameter;

import io.debezium.ibmi.db2.journal.retrieve.RetrievalCriteria.JournalCode;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalCriteria.JournalEntryType;

// mostly a wrapper for RetrievalCriteria so we can capture parameters for diagnostics
public class ParameterListBuilder {
    private static final Logger log = LoggerFactory.getLogger(ParameterListBuilder.class);

    public static final int DEFAULT_JOURNAL_BUFFER_SIZE = 65536 * 2;
    public static final int ERROR_CODE = 0;
    private static final byte[] errorCodeData = new AS400Bin4().toBytes(ERROR_CODE);
    public static final String FORMAT_NAME = "RJNE0200";
    private final byte[] formatNameData;
    private final int ccsid;

    private int bufferLength = DEFAULT_JOURNAL_BUFFER_SIZE;
    private byte[] bufferLengthData = new AS400Bin4().toBytes(bufferLength);

    private String receiver = "";
    private String receiverLibrary = "";
    private final RetrievalCriteria criteria;
    private byte[] journalData;

    // for diagnostics
    private String startReceiver;
    private String startLibrary;
    private String endReceiver;
    private String endLibrary;
    private List<FileFilter> tableFilters;
    private String startOffset;
    private String endOffset;
    private RetrievalCriteria.JournalEntryType[] journalEntryTypes;
    private JournalCode[] journalCode;

    public ParameterListBuilder() {
        this(65535);
    }

    public ParameterListBuilder(int ccsid) {
        this.ccsid = ccsid;
        formatNameData = new AS400Text(8, ccsid).toBytes(FORMAT_NAME);
        criteria = new RetrievalCriteria(ccsid);
        criteria.withLenNullPointerIndicatorVarLength();
    }

    public ParameterListBuilder withBufferLenth(int bufferLength) {
        this.bufferLength = bufferLength;
        this.bufferLengthData = new AS400Bin4().toBytes(bufferLength);
        return this;
    }

    public ParameterListBuilder withRange(PositionRange range) {
        if (range.fromBeginning()) {
            log.warn("starting from beginning");
            withFromBeginningToEnd();
        }
        else {
            withReceivers(range);
        }
        return this;
    }

    public ParameterListBuilder withJournal(String receiver, String receiverLibrary) {
        if (!this.receiver.equals(receiver) && !this.receiverLibrary.equals(receiverLibrary)) {
            this.receiver = receiver;
            this.receiverLibrary = receiverLibrary;

            final String jrnLib = StringHelpers.padRight(receiver, 10)
                    + StringHelpers.padRight(receiverLibrary, 10);
            journalData = new AS400Text(20, ccsid).toBytes(jrnLib);
        }
        return this;
    }

    public void init() {
        criteria.reset();
    }

    public ParameterListBuilder withVariableNullIndicators() {
        criteria.withLenNullPointerIndicatorVarLength();
        return this;
    }

    public ParameterListBuilder withJournalEntryType(JournalEntryType type) {
        criteria.withEntTyp(new JournalEntryType[]{ type });
        return this;
    }

    public ParameterListBuilder withReceivers(PositionRange range) {
        this.startReceiver = range.start().getReceiver().name();
        this.startLibrary = range.start().getReceiver().library();
        withStartingSequence(range.start().getOffset());
        this.endReceiver = range.end().getReceiver().name();
        this.endLibrary = range.end().receiver().library();
        withEnd(range.end().getOffset());
        criteria.withReceiverRange(startReceiver, startLibrary, endReceiver, endLibrary);
        return this;
    }

    private ParameterListBuilder withStartingSequence(BigInteger start) {
        startOffset = start.toString();
        criteria.withFromEnt(start);
        return this;
    }

    private ParameterListBuilder withEnd() {
        endOffset = "*LAST";
        criteria.withEnd();
        return this;
    }

    private ParameterListBuilder withEnd(BigInteger end) {
        endOffset = end.toString();
        criteria.withEnd(end);
        return this;
    }

    public ParameterListBuilder withStartReceiversToCurrentEnd(BigInteger start, String startReceiver, String startLibrary) {
        withStartingSequence(start);
        this.startReceiver = startReceiver;
        this.startLibrary = startLibrary;
        this.endReceiver = "*CURRENT";
        this.endLibrary = "";
        criteria.withReceiverRange(startReceiver, startLibrary, endReceiver, endLibrary);
        this.withEnd();
        return this;
    }

    public ParameterListBuilder withFromBeginningToEnd() {
        this.startReceiver = "*CURCHAIN";
        this.startLibrary = "";
        this.endReceiver = "";
        this.endLibrary = "";
        criteria.withReceiverRange(startReceiver, startLibrary, endReceiver, endLibrary);
        this.startOffset = "*FIRST";
        criteria.withStart();
        this.withEnd();
        return this;
    }

    public ParameterListBuilder filterJournalCodes(JournalCode[] journalCode) {
        return filterJournalCodes(journalCode, false);
    }

    public ParameterListBuilder filterJournalCodes(JournalCode[] journalCode, boolean transactionControls) {
        this.journalCode = journalCode;
        criteria.withJrnCde(journalCode, transactionControls);
        return this;
    }

    public ParameterListBuilder withFileFilters(List<FileFilter> tableFilters) {
        this.tableFilters = tableFilters;
        criteria.withFILE(tableFilters);
        return this;
    }

    public ParameterListBuilder filterJournalEntryType(RetrievalCriteria.JournalEntryType[] journalEntryTypes) {
        this.journalEntryTypes = journalEntryTypes;
        criteria.withEntTyp(journalEntryTypes);
        return this;
    }

    public ProgramParameter[] build() {
        final byte[] criteriaData = new AS400Structure(criteria.getStructure()).toBytes(criteria.getObject());
        return new ProgramParameter[]{ new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, bufferLength), // 1
                // Receiver
                // variable
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, bufferLengthData), // 2 Length of receiver
                // variable
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, journalData), // 3 Qualified journal name
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, formatNameData), // 4 Format name
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, criteriaData), // 5 Journal entries to
                // retrieve
                new ProgramParameter(ProgramParameter.PASS_BY_REFERENCE, errorCodeData) }; // 6 Error code
    }

    @Override
    public String toString() {
        return String.format(
                "ParameterListBuilder [receiver=%s, receiverLibrary=%s, startReceiver=%s, startLibrary=%s, endReceiver=%s, endLibrary=%s, startOffset=%s, endOffset=%s, journalEntryTypes=%s, journalCode=%s, tableFilters=%s]",
                receiver, receiverLibrary, startReceiver, startLibrary,
                endReceiver, endLibrary, startOffset, endOffset, Arrays.toString(journalEntryTypes),
                Arrays.toString(journalCode), filtersToShortString(tableFilters));
    }

    public String filtersToShortString(List<FileFilter> tableFilters) {
        if (tableFilters == null) {
            return "null";
        }
        return tableFilters.stream().map(FileFilter::toShortString).collect(Collectors.joining("-", "{", "}"));
    }
}
