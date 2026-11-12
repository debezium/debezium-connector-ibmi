/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig.EventProcessingFailureHandlingMode;
import io.debezium.connector.db2as400.As400RpcConnection.BlockingReceiverConsumer;
import io.debezium.data.Envelope.Operation;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder.FileChange;
import io.debezium.ibmi.db2.journal.retrieve.JournalEntryDeocder;
import io.debezium.ibmi.db2.journal.retrieve.JournalEntryType;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.exception.FatalException;
import io.debezium.ibmi.db2.journal.retrieve.exception.InvalidPositionException;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.OffsetActivityMonitorService;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * <p>
 * A {@link StreamingChangeEventSource} A main loop polls using a RPC call for
 * new journal entries and turns them into change events.
 * </p>
 */
public class As400StreamingChangeEventSource implements StreamingChangeEventSource<As400Partition, As400OffsetContext> {
    private static final String NO_TRANSACTION_ID = "00000000000000000000";
    private long connectionTime = -1;
    private final long MIN_DISCONNECT_TIME_MS = 30000;

    private static final Logger log = LoggerFactory.getLogger(As400StreamingChangeEventSource.class);

    private final HashMap<String, Object[]> beforeMap = new HashMap<>();
    private final Map<TableId, EntryHeader> migrationBeforeHeaders = new HashMap<>();
    private final Set<TableId> legacyBeforeTables = new HashSet<>();
    private JournalProcessedPosition legacyBeforeFloor;
    private final FileChangeEntryDecoder fileChangeDecoder = new FileChangeEntryDecoder();
    private final FileChangeEntryDecoder nativeFileChangeDecoder = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID);
    private static Set<Character> alwaysProcess = Stream.of('J', 'C').collect(Collectors.toCollection(HashSet::new));

    /**
     * Connection used for reading CDC tables.
     */
    private final As400RpcConnection dataConnection;
    private final As400JdbcConnection jdbcConnection;

    /**
     * A separate connection for retrieving timestamps; without it, adaptive
     * buffering will not work.
     */
    private final EventDispatcher<As400Partition, TableId> dispatcher;
    private final ErrorHandler errorHandler;
    private final Clock clock;
    private final As400DatabaseSchema schema;
    private final Duration pollInterval;
    private final As400ConnectorConfig connectorConfig;
    private final boolean captureTruncates;
    private final Map<String, TransactionContext> txMap = new HashMap<>();
    private final String database;
    private As400OffsetContext offsetContext;
    private volatile ChangeEventSourceContext streamingContext;
    private volatile boolean streamingFinished;
    private final OffsetActivityMonitorService offsetActivityMonitorService;
    private OffsetActivityMonitor<As400Partition, As400OffsetContext> offsetActivityMonitor;

    public As400StreamingChangeEventSource(As400ConnectorConfig connectorConfig, As400RpcConnection dataConnection,
                                           As400JdbcConnection jdbcConnection, EventDispatcher<As400Partition, TableId> dispatcher,
                                           ErrorHandler errorHandler, Clock clock, As400DatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.captureTruncates = !connectorConfig.getSkippedOperations().contains(Operation.TRUNCATE);
        this.dataConnection = dataConnection;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.pollInterval = connectorConfig.getPollInterval();
        this.database = jdbcConnection.getRealDatabaseName();
        this.offsetActivityMonitorService = OffsetActivityMonitorService.lookup(connectorConfig.getServiceRegistry());
    }

    @Override
    public void init(As400OffsetContext offsetContext) {
        if (offsetContext == null) {
            this.offsetContext = new As400OffsetContext(connectorConfig);
        }
        else {
            this.offsetContext = offsetContext;
        }
    }

    private void cacheBefore(TableId tableId, Object[] dataBefore) {
        final String key = String.format("%s-%s", tableId.schema(), tableId.table());
        beforeMap.put(key, dataBefore);
    }

    private Object[] getBefore(TableId tableId) {
        final String key = String.format("%s-%s", tableId.schema(), tableId.table());
        final Object[] dataBefore = beforeMap.remove(key);
        if (dataBefore == null) {
            log.debug("before image not found for {}", key);
        }
        else {
            log.debug("found before image for {}", key);
        }
        return dataBefore;
    }

    @Override
    public void execute(ChangeEventSourceContext context, As400Partition partition, As400OffsetContext offsetContext)
            throws InterruptedException {
        streamingContext = context;
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        int retries = 0;
        final WatchDog watchDog = new WatchDog(Thread.currentThread(), connectorConfig.getMaxRetrievalTimeout());
        watchDog.start();
        try {
            while (context.isRunning()) {
                offsetActivityMonitorService.pulse(partition, offsetContext);

                try {
                    try {
                        switch (dataConnection.getJournalEntries(context, offsetContext,
                                processJournalEntries(partition, offsetContext), watchDog)) {
                            case Success:
                                break;
                            case MoreDataAvailable:
                                break;
                            case NotCalled:
                                metronome.pause();
                                if (context.isRunning()) {
                                    dispatcher.dispatchHeartbeatEventAlsoToIncrementalSnapshot(partition, offsetContext);
                                }
                                break;
                            default:
                                metronome.pause();
                                break;
                        }
                        retries = 0;
                    }
                    catch (final InvalidPositionException e) {
                        throw new DebeziumException("Invalid journal receiver/sequence are we using the wrong offset for the receiver: " + offsetContext.getPosition(),
                                e);
                    }
                    catch (final FatalException e) {
                        throw new DebeziumException("Unable to process offset " + offsetContext.getPosition(), e);
                    }
                    catch (TruncateException | RowProcessingException | TransactionMetadataException e) {
                        // Stop before the RPC loop can acknowledge a failed record.
                        throw e;
                    }
                    catch (final InterruptedException e) {
                        if (context.isRunning()) {
                            log.error("Interrupted processing offset {} retry {}", offsetContext.getPosition(), retries);
                            closeAndReconnect();
                            retries++;
                            metronome.pause();
                        }
                    }
                    catch (IOException | SQLNonTransientConnectionException e) { // SQLNonTransientConnectionException
                        // A late connection failure must not reopen either transport
                        // after the coordinator has stopped this streaming context.
                        if (!context.isRunning()) {
                            return;
                        }
                        log.error("Connection failed offset {} retry {}", offsetContext.getPosition(), retries, e);
                        closeAndReconnect();

                        retries++;
                        metronome.pause(); // throws interruptedException
                    }
                    catch (final Exception e) {
                        if (!context.isRunning()) {
                            return;
                        }
                        log.error("Failed to process offset {} retry {}", offsetContext.getPosition(), retries, e);

                        retries++;
                        metronome.pause();
                    }
                }
                catch (final InterruptedException e) { // handle InterruptedException during the exception handling
                    if (context.isRunning()) {
                        log.debug("Interrupted", e);
                    }
                }
            }
        }
        finally {
            streamingFinished = true;
            watchDog.stop();
        }
    }

    public void rateLimittedClose() {
        if (System.currentTimeMillis() - connectionTime > MIN_DISCONNECT_TIME_MS) {
            closeAndReconnect();
        }
        else {
            log.debug("Only connected since {} ignoring disconnect", new Date(connectionTime));
        }
    }

    private boolean retryAllowed() {
        ChangeEventSourceContext context = streamingContext;
        return !streamingFinished && (context == null || context.isRunning());
    }

    public void closeAndReconnect() {
        if (!retryAllowed()) {
            return;
        }
        try (var ignored = dataConnection.admitRetry()) {
            if (!retryAllowed()) {
                return;
            }
            try {
                dataConnection.close();
                if (!retryAllowed()) {
                    return;
                }
                dataConnection.connection();
            }
            catch (final Exception e) {
                log.error("Failure reconnecting command", e);
            }
            if (!retryAllowed()) {
                return;
            }
            try {
                jdbcConnection.close();
                if (!retryAllowed()) {
                    return;
                }
                jdbcConnection.connect();
            }
            catch (final Exception e) {
                log.error("Failure reconnecting sql", e);
            }
            connectionTime = System.currentTimeMillis();
        }
        catch (IOException e) {
            log.debug("Reader stopped before retry admission", e);
        }
    }

    // TODO tidy up exception handling
    BlockingReceiverConsumer processJournalEntries(As400Partition partition, As400OffsetContext offsetContext)
            throws IOException, SQLNonTransientConnectionException, InterruptedException {
        return (nextOffset, reader, header) -> processJournalEntry(partition, offsetContext, nextOffset, reader::decode, header, false);
    }

    @FunctionalInterface
    private interface EntryValues {
        <T> T decode(JournalEntryDeocder<T> decoder) throws Exception;
    }

    /** Same legacy dispatcher for bounded owned native pages; no RPC cursor is acknowledged here. */
    void processOwnedEntry(As400Partition partition, As400OffsetContext offset, RawJournalPage.Entry entry)
            throws IOException, SQLNonTransientConnectionException, InterruptedException {
        if (connectorConfig.getEventProcessingFailureHandlingMode() != EventProcessingFailureHandlingMode.FAIL) {
            throw new IllegalArgumentException("A migration prefix requires fail-on-error legacy dispatch");
        }
        byte[] bytes = entry.data();
        var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(bytes, 0);
        As400SourceMetadata.from(header, entry.position());
        processJournalEntry(partition, offset, header.getSequenceNumber(), new EntryValues() {
            @Override
            public <T> T decode(JournalEntryDeocder<T> decoder) throws Exception {
                return decoder.decode(header, bytes, 0);
            }
        }, header, true);
    }

    /** A progress barrier cannot discard an update's cached before-image on restart. */
    boolean hasPendingBeforeImage() {
        return !beforeMap.isEmpty();
    }

    private void processJournalEntry(As400Partition partition, As400OffsetContext offsetContext, BigInteger nextOffset,
                                     EntryValues r, EntryHeader eheader, boolean migration)
            throws IOException, SQLNonTransientConnectionException, InterruptedException {
        final JournalEntryType journalEntryType = eheader.getJournalEntryType();
        TableId resolvedTable = null;
        try {

            if (journalEntryType == null || ignore(journalEntryType)) {
                log.debug("excluding table {} entry type {}", eheader.getFile(), eheader.getEntryType());
                return;
            }

            if (journalEntryType == JournalEntryType.MEMBER_CLEARED && !captureTruncates) {
                return;
            }
            FileChange fileChange = null;
            if (journalEntryType == JournalEntryType.FILE_CHANGE && captureTruncates) {
                try {
                    fileChange = r.decode(migration ? nativeFileChangeDecoder : fileChangeDecoder);
                }
                catch (Exception e) {
                    throw new TruncateException("Cannot classify D/CG journal entry " + eheader.getSequenceNumber(), e);
                }
            }
            final String file = fileChange == null ? eheader.getFile() : fileChange.file();
            final String library = fileChange == null ? eheader.getLibrary() : fileChange.library();
            if (file.isEmpty() || library.isEmpty()) {
                if (journalEntryType == JournalEntryType.MEMBER_CLEARED) {
                    // Preserve the reviewed F/CR policy when capture selection is unknowable.
                    log.warn("Skipping clear with missing file or library at journal entry {}", eheader.getSequenceNumber());
                    return;
                }
                if (fileChange != null && fileChange.clearsRows()) {
                    throw new TruncateException("Missing table identity for clear at journal entry " + eheader.getSequenceNumber());
                }
            }
            String longName = file;
            try {
                longName = jdbcConnection.getLongName(library, file);
            }
            catch (final IllegalStateException e) {
                if (migration) {
                    throw e;
                }
                log.error("failed to look up long name", e);
            }
            final TableId tableId = new TableId(database, library, longName);
            resolvedTable = tableId;

            final boolean includeTable = connectorConfig.getTableFilters().dataCollectionFilter()
                    .isIncluded(tableId) || connectorConfig.isSignalDataCollection(tableId);

            if (!alwaysProcess.contains(eheader.getJournalCode()) && !includeTable) { // always process journal J
                // and transaction C
                // messages
                log.debug("excluding table {} journal code {}", tableId, eheader.getJournalCode());
                return;
            }

            log.debug("next event: {} - {} type: {} table: {}", eheader.getTime(), eheader.getSequenceNumber(),
                    eheader.getEntryType(), tableId.table());
            switch (journalEntryType) {
                case START_COMMIT: {
                    // start commit
                    final String txId = eheader.getCommitCycle().toString();
                    log.debug("begin transaction: {}", txId);
                    final TransactionContext txc = txMap.computeIfAbsent(txId, ignored -> new TransactionContext());
                    offsetContext.setTransaction(txc);
                    // The monitor owns BEGIN emission and counters when enabled.
                    if (!connectorConfig.shouldProvideTransactionMetadata() && !txc.isTransactionInProgress()) {
                        txc.beginTransaction(txId);
                    }
                    log.debug("start transaction id {} tx {} table {}", nextOffset, txId, tableId);
                    dispatcher.dispatchTransactionStartedEvent(partition, txId, offsetContext,
                            eheader.getTime());
                }
                    break;
                case END_COMMIT: {
                    // end commit
                    final String txId = eheader.getCommitCycle().toString();
                    final TransactionContext txc = txMap.get(txId);
                    log.debug("commit transaction id {} tx {} table {}", nextOffset, txId, tableId);
                    if (txc != null) {
                        offsetContext.setTransaction(txc);
                        dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext,
                                eheader.getTime());
                        txc.endTransaction();
                        txMap.remove(txId);
                        offsetContext.endTransaction();
                    }
                }
                    break;
                case FILE_CHANGE, FILE_CREATED: {
                    if (fileChange != null && fileChange.clearsRows()) {
                        dispatchTruncate(partition, offsetContext, tableId, eheader, file, library, fileChange.member());
                        break;
                    }
                    // table has changed - reload schema
                    schema.clearCache(tableId.table(), tableId.schema());
                    schema.getRecordFormat(tableId.table(), tableId.schema());
                }
                    break;
                case MEMBER_CLEARED: {
                    if (eheader.getMember().isEmpty()) {
                        throw new TruncateException("Missing member in F/CR journal entry " + eheader.getSequenceNumber());
                    }
                    dispatchTruncate(partition, offsetContext, tableId, eheader, file, library, eheader.getMember());
                }
                    break;
                case BEFORE_IMAGE: {
                    // before image
                    tableId.schema();
                    final Object[] dataBefore = r.decode(schema.getFileDecoder());
                    if (migration && migrationBeforeHeaders.putIfAbsent(tableId, eheader) != null) {
                        throw new IllegalArgumentException("Overlapping migration before-images require explicit recovery");
                    }
                    cacheBefore(tableId, dataBefore);
                    if (!migration && eheader.getCommitCycle().signum() == 0) {
                        if (legacyBeforeTables.isEmpty()) {
                            legacyBeforeFloor = offsetContext.entryPosition(eheader).setProcessed(false);
                        }
                        legacyBeforeTables.add(tableId);
                        offsetContext.legacyReplayFloor(legacyFloor(null));
                    }
                }
                    break;
                case AFTER_IMAGE: {
                    // after image
                    // before image is meant to have been immediately before
                    final Object[] dataBefore = migration ? getBefore(tableId)
                            : beforeMap.get(String.format("%s-%s", tableId.schema(), tableId.table()));
                    if (migration && dataBefore == null) {
                        throw new IllegalArgumentException("Migration update lacks its before-image at journal entry " + eheader.getSequenceNumber());
                    }
                    if (migration) {
                        requireMigrationPair(migrationBeforeHeaders.remove(tableId), eheader);
                    }
                    final Object[] dataNext = r.decode(schema.getFileDecoder());

                    offsetContext.updateSourceInfo(eheader);

                    selectRowTransaction(partition, offsetContext, eheader);
                    final String txId = eheader.getCommitCycle().toString();

                    log.debug("update event id {} tx {} table {}", nextOffset, txId, tableId);

                    dispatchRow(partition, tableId, eheader, rowEmitter(partition, offsetContext, tableId, eheader,
                            Operation.UPDATE, dataBefore, dataNext, migration));
                    if (!migration) {
                        releaseLegacyBefore(offsetContext, tableId);
                    }
                }
                    break;
                case ADD_ROW1, ADD_ROW2: {
                    // record added
                    final Object[] dataNext = r.decode(schema.getFileDecoder());
                    offsetContext.updateSourceInfo(eheader);

                    selectRowTransaction(partition, offsetContext, eheader);
                    final String txId = eheader.getCommitCycle().toString();

                    log.debug("insert event id {} tx {} table {}", offsetContext.getPosition(), txId,
                            tableId);
                    dispatchRow(partition, tableId, eheader, rowEmitter(partition, offsetContext, tableId, eheader,
                            Operation.CREATE, null, dataNext, migration));
                }
                    break;
                case DELETE_ROW, ROLLBACK_DELETE_ROW: {
                    // record deleted
                    final Object[] dataBefore = r.decode(schema.getFileDecoder());

                    offsetContext.updateSourceInfo(eheader);

                    selectRowTransaction(partition, offsetContext, eheader);
                    final String txId = eheader.getCommitCycle().toString();

                    log.debug("delete event id {} tx {} table {}", offsetContext.getPosition(), txId,
                            tableId);
                    dispatchRow(partition, tableId, eheader, rowEmitter(partition, offsetContext, tableId, eheader,
                            Operation.DELETE, dataBefore, null, migration));
                }
                    break;
                default:
                    break;
            }
        }
        catch (TruncateException | RowProcessingException | InterruptedException | SQLNonTransientConnectionException e) {
            throw e;
        }
        catch (final Exception e) {
            for (Throwable cause = e; cause != null; cause = cause.getCause()) {
                if (cause instanceof InterruptedException interrupted) {
                    throw interrupted;
                }
            }
            boolean row = journalEntryType != null && switch (journalEntryType) {
                case BEFORE_IMAGE, AFTER_IMAGE, ADD_ROW1, ADD_ROW2, DELETE_ROW, ROLLBACK_DELETE_ROW -> true;
                default -> false;
            };
            // A row decoder can use IOException for malformed in-memory data.
            // FAIL must report it; preserve legacy retries elsewhere and in lossy modes.
            if (e instanceof IOException io && (!row || connectorConfig.getEventProcessingFailureHandlingMode() != EventProcessingFailureHandlingMode.FAIL)) {
                throw io;
            }
            boolean metadata = connectorConfig.shouldProvideTransactionMetadata()
                    && (journalEntryType == JournalEntryType.START_COMMIT || journalEntryType == JournalEntryType.END_COMMIT);
            if (row || metadata) {
                String kind = row ? "row" : "transaction metadata";
                switch (connectorConfig.getEventProcessingFailureHandlingMode()) {
                    case FAIL:
                        String message = "Failed to process " + kind + " at journal entry " + eheader.getSequenceNumber();
                        if (row) {
                            throw new RowProcessingException(message, e);
                        }
                        throw new TransactionMetadataException(message, e);
                    case WARN:
                        log.warn("Failed to process {} at journal entry {}", kind, eheader.getSequenceNumber(), e);
                        break;
                    case SKIP:
                        log.debug("Failed to process {} at journal entry {}", kind, eheader.getSequenceNumber(), e);
                        break;
                }
                if (!migration && journalEntryType == JournalEntryType.AFTER_IMAGE && resolvedTable != null) {
                    releaseLegacyBefore(offsetContext, resolvedTable);
                }
            }
            else {
                if (migration) {
                    throw new DebeziumException("Failed to process migration prefix at journal entry " + eheader.getSequenceNumber(), e);
                }
                log.error("Failed to process record", e);
            }
        }
    }

    private static void requireMigrationPair(EntryHeader before, EntryHeader after) {
        var row = after.getRecordInfo();
        if (before == null || !before.getObjectName().equals(after.getObjectName())
                || !before.getCommitCycle().equals(after.getCommitCycle())
                || !before.getRecordInfo().recordNumber().equals(row.recordNumber())
                || !before.getRecordInfo().objectJournalId().equals(row.objectJournalId())
                || !before.getRecordInfo().objectType().equals(row.objectType())
                || !As400ControlInventory.Producer.from(before).equals(As400ControlInventory.Producer.from(after))) {
            throw new IllegalArgumentException("Migration update before-image belongs to a different source row or producer");
        }
    }

    private void selectRowTransaction(As400Partition partition, As400OffsetContext offset, EntryHeader entry) throws InterruptedException {
        String id = entry.getCommitCycle().toString();
        TransactionContext transaction = txMap.get(id);
        if (transaction == null && entry.getCommitCycle().signum() != 0) {
            transaction = new TransactionContext();
            txMap.put(id, transaction);
            offset.setTransaction(transaction);
            // A snapshot or restart may begin after C/SC. Metadata describes the
            // observed suffix; it cannot reconstruct counters preceding this offset.
            if (connectorConfig.shouldProvideTransactionMetadata()) {
                dispatcher.dispatchTransactionStartedEvent(partition, id, offset, entry.getTime());
            }
            else {
                transaction.beginTransaction(id);
            }
        }
        offset.setTransaction(transaction);
    }

    private JournalProcessedPosition legacyFloor(TableId completed) {
        // A/UB, B/UB, A/UP must retain A/UB until B finishes: restarting at
        // B/UB would replay A/UP without its before-image or old primary key.
        return legacyBeforeTables.isEmpty() || (legacyBeforeTables.size() == 1 && legacyBeforeTables.contains(completed))
                ? null
                : legacyBeforeFloor;
    }

    private void releaseLegacyBefore(As400OffsetContext offset, TableId table) {
        beforeMap.remove(String.format("%s-%s", table.schema(), table.table()));
        legacyBeforeTables.remove(table);
        if (legacyBeforeTables.isEmpty()) {
            legacyBeforeFloor = null;
        }
        offset.legacyReplayFloor(legacyFloor(null));
    }

    private As400ChangeRecordEmitter rowEmitter(As400Partition partition, As400OffsetContext offset, TableId table,
                                                EntryHeader entry, Operation operation, Object[] before, Object[] after, boolean migration) {
        if (migration || entry.getCommitCycle().signum() != 0) {
            return new As400ChangeRecordEmitter(partition, offset, operation, before, after, clock, connectorConfig);
        }
        var position = offset.entryPosition(entry);
        var replay = legacyFloor(null);
        var completed = legacyFloor(operation == Operation.UPDATE ? table : null);
        return new As400LegacyChangeRecordEmitter(partition,
                offset.recordContext(entry, replay == null ? new JournalProcessedPosition(position).setProcessed(false) : replay),
                offset.recordContext(entry, completed == null ? position : completed), operation, before, after, clock, connectorConfig);
    }

    private void dispatchRow(As400Partition partition, TableId tableId, EntryHeader entry, As400ChangeRecordEmitter emitter)
            throws InterruptedException {
        // Table filtering already ran above. Skipped operations and unchanged
        // updates return true; a rejected dispatch must not advance FAIL mode.
        // WARN/SKIP deliberately retain Debezium's configured lossy behavior.
        try {
            if (!dispatcher.dispatchDataChangeEvent(partition, tableId, emitter)
                    && connectorConfig.getEventProcessingFailureHandlingMode() == EventProcessingFailureHandlingMode.FAIL) {
                throw new RowProcessingException("Failed to dispatch row for " + tableId + " at journal entry " + entry.getSequenceNumber(), null);
            }
        }
        finally {
            emitter.rethrowDispatchInterruption();
        }
    }

    static class RowProcessingException extends DebeziumException {
        RowProcessingException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    static class TransactionMetadataException extends DebeziumException {
        TransactionMetadataException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private void dispatchTruncate(As400Partition partition, As400OffsetContext offsetContext, TableId tableId,
                                  EntryHeader entry, String file, String library, String member)
            throws InterruptedException {
        // The connector does not buffer transactions. A truncate cannot be undone by
        // replaying row-level rollback images, so do not emit an uncommitted clear.
        if (entry.getCommitCycle().signum() != 0) {
            throw new TruncateException("Cannot capture a clear under commitment control for " + tableId
                    + "; transactional truncate capture requires transaction buffering");
        }
        try {
            if (!jdbcConnection.isOnlyMember(library, file, member)) {
                throw new TruncateException("Cannot represent a clear as a table truncate for " + tableId
                        + ": expected one matching file member");
            }
            // Clears have no row image to decode, so ensure the table schema is registered explicitly.
            if (schema.getRecordFormat(file, library).isEmpty()) {
                throw new TruncateException("Missing table schema for " + tableId + " at journal entry " + entry.getSequenceNumber());
            }
            beforeMap.remove(String.format("%s-%s", tableId.schema(), tableId.table()));
            offsetContext.updateSourceInfo(entry);
            offsetContext.setTransaction(null);
            As400ChangeRecordEmitter emitter = new As400ChangeRecordEmitter(partition,
                    offsetContext, Operation.TRUNCATE, null, null, clock, connectorConfig);
            boolean dispatched;
            try {
                dispatched = dispatcher.dispatchDataChangeEvent(partition, tableId, emitter);
            }
            finally {
                // Recover the original interruption even when core logged it and returned false.
                emitter.rethrowDispatchInterruption();
            }
            if (!dispatched) {
                throw new TruncateException("Failed to dispatch clear for " + tableId + " at journal entry " + entry.getSequenceNumber());
            }
            releaseLegacyBefore(offsetContext, tableId);
        }
        catch (TruncateException | InterruptedException e) {
            throw e;
        }
        catch (Exception e) {
            // EventDispatcher can wrap an interrupted queue write in ConnectException.
            if (e.getCause() instanceof InterruptedException interrupted) {
                throw interrupted;
            }
            throw new TruncateException("Failed to capture clear for " + tableId + " at journal entry " + entry.getSequenceNumber(), e);
        }
    }

    static class TruncateException extends DebeziumException {
        TruncateException(String message) {
            super(message);
        }

        TruncateException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private boolean ignore(JournalEntryType journalCode) {
        return journalCode == JournalEntryType.OPEN || journalCode == JournalEntryType.CLOSE;
    }

    @Override
    public As400OffsetContext getOffsetContext() {
        return offsetContext;
    }

    @Override
    public Optional<OffsetActivityMonitor<As400Partition, As400OffsetContext>> getOffsetActivityMonitor() {
        if (offsetActivityMonitor == null) {
            offsetActivityMonitor = new As400OffsetActivityMonitor(connectorConfig.getOffsetActivityMonitorInterval());
        }
        return Optional.of(offsetActivityMonitor);
    }
}
