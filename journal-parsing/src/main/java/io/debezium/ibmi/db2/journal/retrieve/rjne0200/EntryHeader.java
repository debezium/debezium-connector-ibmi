/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rjne0200;

import java.math.BigInteger;
import java.time.Instant;

import io.debezium.ibmi.db2.journal.retrieve.JournalEntryType;
import io.debezium.ibmi.db2.journal.retrieve.StringHelpers;

public class EntryHeader {
    private final int nextEntryOffset;
    private final int nullValueOffest;
    private final int entrySpecificDataOffset;
    // private final int transactionDataOffset;
    // private final int logicalWorkOffset;
    // private final int receiverOffset;
    private final BigInteger sequenceNumber;
    private final BigInteger systemSequenceNumber;
    private final Instant timestamp;
    private final char journalCode;
    private final String entryType;
    private final String objectName;
    private final BigInteger commitCycle;
    private final int endOffset;
    private final long pointerHandle;
    private final String receiver;
    private final String receiverLibrary;
    private final ControlInfo controlInfo;
    private final RecordInfo recordInfo;

    public EntryHeader(int nextEntryOffset, int nullValueOffest, long entrySpecificDataOffset, BigInteger sequenceNumber, BigInteger systemSequenceNumber,
                       Instant timestamp, char journalCode, String entryType, String objectName, BigInteger commitCycle, int endOffset, long pointerHandle,
                       String receiver, String receiverLibrary) {
        this(nextEntryOffset, nullValueOffest, entrySpecificDataOffset, sequenceNumber, systemSequenceNumber,
                timestamp, journalCode, entryType, objectName, commitCycle, endOffset, pointerHandle, receiver, receiverLibrary,
                ControlInfo.UNKNOWN);
    }

    public EntryHeader(int nextEntryOffset, int nullValueOffest, long entrySpecificDataOffset, BigInteger sequenceNumber, BigInteger systemSequenceNumber,
                       Instant timestamp, char journalCode, String entryType, String objectName, BigInteger commitCycle, int endOffset, long pointerHandle,
                       String receiver, String receiverLibrary, ControlInfo controlInfo) {
        this(nextEntryOffset, nullValueOffest, entrySpecificDataOffset, sequenceNumber, systemSequenceNumber, timestamp,
                journalCode, entryType, objectName, commitCycle, endOffset, pointerHandle, receiver, receiverLibrary,
                controlInfo, RecordInfo.UNKNOWN);
    }

    public EntryHeader(int nextEntryOffset, int nullValueOffest, long entrySpecificDataOffset, BigInteger sequenceNumber, BigInteger systemSequenceNumber,
                       Instant timestamp, char journalCode, String entryType, String objectName, BigInteger commitCycle, int endOffset, long pointerHandle,
                       String receiver, String receiverLibrary, ControlInfo controlInfo, RecordInfo recordInfo) {
        this.nextEntryOffset = nextEntryOffset;
        this.nullValueOffest = nullValueOffest;
        this.entrySpecificDataOffset = (int) entrySpecificDataOffset;
        this.sequenceNumber = sequenceNumber;
        this.systemSequenceNumber = systemSequenceNumber;
        this.timestamp = timestamp;
        this.journalCode = journalCode;
        this.entryType = entryType;
        this.objectName = objectName;
        this.commitCycle = commitCycle;
        this.endOffset = endOffset;
        this.pointerHandle = pointerHandle;
        this.receiver = StringHelpers.safeTrim(receiver);
        this.receiverLibrary = StringHelpers.safeTrim(receiverLibrary);
        this.controlInfo = controlInfo;
        this.recordInfo = recordInfo;
    }

    public RecordInfo getRecordInfo() {
        return recordInfo;
    }

    public BigInteger getRelativeRecordNumber() {
        return recordInfo.recordNumber();
    }

    /** The opaque ten-byte object JID is hex encoded without text conversion or trimming. */
    public record RecordInfo(BigInteger recordNumber, BigInteger threadId, String jobName, String jobUser, String jobNumber,
            String objectJournalId, char objectNameIndicator, String objectType, String systemName,
            String programName, String programLibrary, String userProfile) {
        public static final RecordInfo UNKNOWN = new RecordInfo(null, null, "", "", "", "", '\0', "", "", "", "", "");
    }

    public long getPointerHandle() {
        return pointerHandle;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public ControlInfo getControlInfo() {
        return controlInfo;
    }

    /**
     * RJNE0200 fixed-header attributes. Indicator flag (JOFLAG) is distinct from
     * the bit flags. Identifier presence does not decode or resolve an XA/LUW outcome.
     */
    public record ControlInfo(char indicatorFlag, long nestedCommitLevel, boolean hasTransactionIdentifier,
            boolean hasLogicalUnitOfWork, byte flags) {

        public static final ControlInfo UNKNOWN = new ControlInfo('\0', -1, false, false, (byte) 0);

        /** RJNE0200 offset 218, bit 5 (MSB first): this entry describes a logical file. */
        public boolean logicalFile() {
            return (flags & 0x04) != 0;
        }
    }

    @Override
    public String toString() {
        return String.format(
                "EntryHeader [nextEntryOffset=%s, nullValueOffest=%s, entrySpecificDataOffset=%s, sequenceNumber=%s, systemSequenceNumber=%s, timestamp=%s, journalCode=%s, entryType=%s, objectName=%s, commitCycle=%s, endOffset=%s, pointerHandle=%s, receiver=%s, receiverLibrary=%s]",
                nextEntryOffset, nullValueOffest, entrySpecificDataOffset, sequenceNumber, systemSequenceNumber,
                timestamp, journalCode, entryType, objectName, commitCycle, endOffset, pointerHandle, receiver,
                receiverLibrary);
    }

    public int getLength() {
        return getEndOffset() - getEntrySpecificDataOffset();
    }

    public int getNextEntryOffset() {
        return nextEntryOffset;
    }

    public int getEntrySpecificDataOffset() {
        return entrySpecificDataOffset;
    }

    public BigInteger getSequenceNumber() {
        return sequenceNumber;
    }

    public BigInteger getSystemSequenceNumber() {
        return systemSequenceNumber;
    }

    public Instant getTime() {
        if (timestamp == null) {
            return Instant.ofEpochSecond(0);
        }
        return timestamp;
    }

    public char getJournalCode() {
        return journalCode;
    }

    public String getEntryType() {
        return entryType;
    }

    public String getObjectName() {
        return objectName;
    }

    public JournalEntryType getJournalEntryType() {
        String entryType = String.format("%s.%s", getJournalCode(), getEntryType());
        return JournalEntryType.toValue(entryType);
    }

    /**
     * @return table name
     */
    public String getFile() {
        return StringHelpers.safeTrim(objectName.substring(0, 10));
    }

    /**
     * @return schema
     */
    public String getLibrary() {
        return StringHelpers.safeTrim(objectName.substring(10, 20));
    }

    /**
     * @return magic stuff within a file
     */
    public String getMember() {
        return StringHelpers.safeTrim(objectName.substring(20, 30));
    }

    public BigInteger getCommitCycle() {
        return commitCycle;
    }

    public int getEndOffset() {
        return endOffset;
    }

    public int getNullValueOffest() {
        return nullValueOffest;
    }

    public boolean hasReceiver() {
        return !receiver.isEmpty();
    }

    public String getReceiver() {
        return receiver;
    }

    public String getReceiverLibrary() {
        return receiverLibrary;
    }
}
