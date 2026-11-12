/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Field;
import io.debezium.connector.SnapshotRecord;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Strings;

public class As400OffsetContext extends CommonOffsetContext<SourceInfo> {
    private static final Logger log = LoggerFactory.getLogger(As400OffsetContext.class);
    // TODO note believe there is a per journal offset
    private static final String SERVER_PARTITION_KEY = "server";
    public static final String EVENT_SEQUENCE = "offset.event_sequence";
    public static final String EVENT_TIME = "offset.time";
    public static final String RECEIVER_LIBRARY = "offset.receiver_library";
    public static final String PROCESSED = "offset.processed";
    public static final String RECEIVER = "offset.receiver";
    private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

    public static final Field EVENT_SEQUENCE_FIELD = Field.create(EVENT_SEQUENCE);
    public static final Field RECEIVER_LIBRARY_FIELD = Field.create(RECEIVER_LIBRARY);
    public static final Field RECEIVER_FIELD = Field.create(RECEIVER);
    public static final Field PROCESSED_FIELD = Field.create(PROCESSED);

    private final Map<String, String> partition;
    private final TransactionContext nonTransactionalContext = new TransactionContext();
    private TransactionContext transactionContext = nonTransactionalContext;

    private final As400ConnectorConfig connectorConfig;
    private final SourceInfo sourceInfo;
    private final JournalProcessedPosition position;
    private JournalProcessedPosition legacyReplayFloor;
    private boolean hasNewTables = false;
    private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;

    public As400OffsetContext(As400ConnectorConfig connectorConfig) {
        super(new SourceInfo(connectorConfig), false);
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.position = connectorConfig.getOffset();
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
        this.incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>(true);
    }

    public As400OffsetContext(As400ConnectorConfig connectorConfig, JournalProcessedPosition position) {
        super(new SourceInfo(connectorConfig), false);
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.position = position;
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
        this.incrementalSnapshotContext = new SignalBasedIncrementalSnapshotContext<>(true);
    }

    public As400OffsetContext(As400ConnectorConfig connectorConfig, JournalProcessedPosition position,
                              boolean snapshotComplete, IncrementalSnapshotContext<TableId> incrementalSnapshotContext) {
        super(new SourceInfo(connectorConfig), snapshotComplete);
        partition = Collections.singletonMap(SERVER_PARTITION_KEY, connectorConfig.getLogicalName());
        this.position = position;
        this.connectorConfig = connectorConfig;
        sourceInfo = new SourceInfo(connectorConfig);
        this.incrementalSnapshotContext = incrementalSnapshotContext;
    }

    public void setPosition(JournalProcessedPosition newPosition) {
        this.position.setPosition(newPosition);
    }

    public boolean isSnapshotComplete() {
        return snapshotCompleted;
    }

    public JournalProcessedPosition getPosition() {
        return position;
    }

    public boolean isPositionSet() {
        return position != null && position.isOffsetSet();
    }

    public void setTransaction(TransactionContext transactionContext) {
        if (transactionContext == null) {
            // Reuse an inactive context without clearing another interleaved cycle.
            nonTransactionalContext.endTransaction();
            this.transactionContext = nonTransactionalContext;
        }
        else {
            this.transactionContext = transactionContext;
        }
    }

    public void endTransaction() {
        setTransaction(null);
    }

    @Override
    public Map<String, ?> getOffset() {
        return offsetAt(legacyReplayFloor == null ? position : legacyReplayFloor);
    }

    private Map<String, ?> offsetAt(JournalProcessedPosition position) {
        if (sourceInfo.isSnapshot()) {
            log.debug("new snapshot offset {}", position);
        }
        else {
            log.debug("new offset {}", position);

        }
        final BigInteger seq = position.getOffset();
        String offsetStr = "null";
        if (null != seq) {
            offsetStr = seq.toString();
        }
        Instant last = position.getTimeOfLastProcessed();
        String time = last == null ? "" : Long.toString(last.getEpochSecond());
        final Map<String, Object> offset = new HashMap<>(Map.of(As400OffsetContext.EVENT_SEQUENCE, offsetStr,
                As400OffsetContext.EVENT_TIME, time,
                As400OffsetContext.RECEIVER, position.getReceiver().name(),
                As400OffsetContext.PROCESSED, Boolean.toString(position.processed()),
                As400OffsetContext.RECEIVER_LIBRARY, position.getReceiver().library(),
                As400OffsetContext.SNAPSHOT_COMPLETED_KEY, Boolean.toString(snapshotCompleted)));
        incrementalSnapshotContext.store(offset);
        if (offset.get("incremental_snapshot_correlation_id") == null) {
            offset.remove("incremental_snapshot_correlation_id");
        }
        return offset;
    }

    /** Protect an unpaired autocommit before-image without changing the RPC scan cursor. */
    void legacyReplayFloor(JournalProcessedPosition floor) {
        legacyReplayFloor = floor == null ? null : new JournalProcessedPosition(floor);
    }

    JournalProcessedPosition entryPosition(EntryHeader entry) {
        JournalReceiver receiver = entry.hasReceiver() ? new JournalReceiver(entry.getReceiver(), entry.getReceiverLibrary()) : position.getReceiver();
        return new JournalProcessedPosition(entry.getSequenceNumber(), receiver, entry.getTime(), true);
    }

    /** A record owns its coordinates; later reads cannot mutate an enqueued SourceRecord's offset. */
    As400OffsetContext recordContext(EntryHeader entry, JournalProcessedPosition checkpoint) {
        var result = new As400OffsetContext(connectorConfig, new JournalProcessedPosition(checkpoint), snapshotCompleted, incrementalSnapshotContext);
        var source = entryPosition(entry);
        result.updateSourceInfo(source.getOffset(), source.getReceiver().name(), source.getReceiver().library(), entry.getTime(),
                rowNumber(entry.getJournalCode(), entry.getRelativeRecordNumber()));
        result.setTransaction(transactionContext);
        return result;
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    /**
     * Stamps the source info block from the journal entry being dispatched. The read position
     * lags by one scanned entry at dispatch time, so the entry itself is the only correct source
     * for {@code source.sequence} and, across a receiver rollover, for the receiver name and
     * library. Entries that do not name a receiver, which is the common case, fall back to the
     * position's receiver.
     */
    public void updateSourceInfo(EntryHeader entryHeader) {
        if (entryHeader.hasReceiver()) {
            updateSourceInfo(entryHeader.getSequenceNumber(), entryHeader.getReceiver(), entryHeader.getReceiverLibrary(), entryHeader.getTime(),
                    rowNumber(entryHeader.getJournalCode(), entryHeader.getRelativeRecordNumber()));
        }
        else {
            updateSourceInfo(entryHeader.getSequenceNumber(), position.getReceiver().name(), position.getReceiver().library(), entryHeader.getTime(),
                    rowNumber(entryHeader.getJournalCode(), entryHeader.getRelativeRecordNumber()));
        }
    }

    void updateSourceInfo(As400SourceMetadata metadata) {
        updateSourceInfo(metadata.position().offset(), metadata.position().receiver().name(), metadata.position().receiver().library(), metadata.timestamp(),
                rowNumber(metadata.journalCode(), metadata.recordInfo().recordNumber()));
    }

    private static BigInteger rowNumber(char journalCode, BigInteger number) {
        // Non-row records reuse the header field for other purposes. A clear
        // must never inherit the last row's physical number.
        return journalCode == 'R' && number != null && number.signum() > 0 ? number : null;
    }

    private void updateSourceInfo(BigInteger sequence, String receiver, String receiverLibrary, Instant timestamp, BigInteger rrn) {
        sourceInfo.setSourceTime(timestamp);
        sourceInfo.setReceiver(receiver);
        sourceInfo.setReceiverLib(receiverLibrary);
        sourceInfo.setSequence(sequence.toString());
        sourceInfo.setRrn(rrn == null ? null : rrn.toString());
    }

    @Override
    public Struct getSourceInfo() {
        return sourceInfo.struct();
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        // snapshot rows are read via SELECT and have no journal entry of their own,
        // so the processed position is the correct source here
        updateSourceInfo(position.getOffset(), position.getReceiver().name(), position.getReceiver().library(), timestamp, null);
        // sourceInfo.tableEvent((TableId) collectionId);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public static class Loader implements OffsetContext.Loader<As400OffsetContext> {

        private final As400ConnectorConfig connectorConfig;

        public Loader(As400ConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public As400OffsetContext load(Map<String, ?> map) {
            if (map.containsKey(As400Checkpoint.VERSION)) {
                throw new IllegalArgumentException("Versioned transaction offsets require buffered recovery; legacy streaming cannot load them");
            }
            final String offsetStr = (String) map.get(As400OffsetContext.EVENT_SEQUENCE);
            final String timeStr = (String) map.get(As400OffsetContext.EVENT_TIME);
            final String complete = (String) map.get(As400OffsetContext.SNAPSHOT_COMPLETED_KEY);
            boolean snapshotComplete = false;
            if (null != complete) {
                snapshotComplete = Boolean.valueOf(complete);
            }
            final String receiver = (String) map.get(As400OffsetContext.RECEIVER);
            final boolean processed = Boolean.valueOf((String) map.get(As400OffsetContext.PROCESSED));
            final String receiverLibrary = (String) map.get(As400OffsetContext.RECEIVER_LIBRARY);
            JournalProcessedPosition position = new JournalProcessedPosition();
            if ("null".equals(offsetStr)) {
                log.warn("setting offsets to zero");
            }
            else {
                final BigInteger offset = new BigInteger(offsetStr);
                Instant time = (Strings.isNullOrBlank(timeStr)) ? Instant.ofEpochSecond(0) : Instant.ofEpochSecond(Long.parseLong(timeStr));
                position = new JournalProcessedPosition(offset, new JournalReceiver(receiver, receiverLibrary), time, processed);
            }
            return new As400OffsetContext(connectorConfig, position, snapshotComplete, SignalBasedIncrementalSnapshotContext.load(map, true));
        }
    }

    public boolean hasNewTables() {
        return hasNewTables;
    }

    public void hasNewTables(boolean hasNewTables) {
        this.hasNewTables = hasNewTables;
    }

    @Override
    public String toString() {
        return "As400OffsetContext [position=" + position + "]";
    }

    @Override
    public void markSnapshotRecord(SnapshotRecord record) {
        sourceInfo.setSnapshot(record);
    }

    @Override
    public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
        return incrementalSnapshotContext;
    }
}
