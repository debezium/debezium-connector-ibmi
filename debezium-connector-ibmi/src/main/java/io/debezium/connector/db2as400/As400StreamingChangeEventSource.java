/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.IOException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.db2as400.As400RpcConnection.BlockingReceiverConsumer;
import io.debezium.data.Envelope.Operation;
import io.debezium.ibmi.db2.journal.retrieve.JournalEntryType;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.exception.FatalException;
import io.debezium.ibmi.db2.journal.retrieve.exception.InvalidPositionException;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
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
    private final Map<String, TransactionContext> txMap = new HashMap<>();
    private final String database;
    private As400OffsetContext offsetContext;

    public As400StreamingChangeEventSource(As400ConnectorConfig connectorConfig, As400RpcConnection dataConnection,
                                           As400JdbcConnection jdbcConnection, EventDispatcher<As400Partition, TableId> dispatcher,
                                           ErrorHandler errorHandler, Clock clock, As400DatabaseSchema schema) {
        this.connectorConfig = connectorConfig;
        this.dataConnection = dataConnection;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.errorHandler = errorHandler;
        this.clock = clock;
        this.schema = schema;
        this.pollInterval = connectorConfig.getPollInterval();
        this.database = jdbcConnection.getRealDatabaseName();
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
        final Metronome metronome = Metronome.sleeper(pollInterval, clock);
        int retries = 0;
        final WatchDog watchDog = new WatchDog(Thread.currentThread(), connectorConfig.getMaxRetrievalTimeout());
        watchDog.start();
        try {
            while (context.isRunning()) {
                try {
                    try {
                        final JournalProcessedPosition before = new JournalProcessedPosition(offsetContext.getPosition());
                        if (!dataConnection.getJournalEntries(context, offsetContext,
                                processJournalEntries(partition, offsetContext), watchDog)) {
                            metronome.pause();
                        }
                        if (!offsetContext.getPosition().equals(before)) {
                            dispatcher.dispatchHeartbeatEventAlsoToIncrementalSnapshot(partition, offsetContext);
                        }
                        retries = 0;
                    }
                    catch (final FatalException e) {
                        throw new DebeziumException("Unable to process offset " + offsetContext.getPosition(), e);
                    }
                    catch (final InvalidPositionException e) {
                        throw new DebeziumException("Invalid journal receiver/sequence has the receiver been deleted? position was: " + offsetContext.getPosition(), e);
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
                        // thrown by jt400 jdbc driver when
                        // connection errors
                        log.error("Connection failed offset {} retry {}", offsetContext.getPosition(), retries, e);
                        closeAndReconnect();

                        retries++;
                        metronome.pause(); // throws interruptedException
                    }
                    catch (final Exception e) {
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

    public void closeAndReconnect() {
        try {
            dataConnection.close();
            dataConnection.connection();
        }
        catch (final Exception e) {
            log.error("Failure reconnecting command", e);
        }
        try {
            jdbcConnection.close();
            jdbcConnection.connect();
        }
        catch (final Exception e) {
            log.error("Failure reconnecting sql", e);
        }
        connectionTime = System.currentTimeMillis();
    }

    // TODO tidy up exception handling
    private BlockingReceiverConsumer processJournalEntries(As400Partition partition, As400OffsetContext offsetContext)
            throws IOException, SQLNonTransientConnectionException {
        return (nextOffset, r, eheader) -> {
            try {
                final JournalEntryType journalEntryType = eheader.getJournalEntryType();

                if (journalEntryType == null || ignore(journalEntryType)) {
                    log.debug("excluding table {} entry type {}", eheader.getFile(), eheader.getEntryType());
                    return;
                }

                String longName = eheader.getFile();
                try {
                    longName = jdbcConnection.getLongName(eheader.getLibrary(), eheader.getFile());
                }
                catch (final IllegalStateException e) {
                    log.error("failed to look up long name", e);
                }
                final TableId tableId = new TableId(database, eheader.getLibrary(), longName);

                final boolean includeTable = connectorConfig.getTableFilters().dataCollectionFilter()
                        .isIncluded(tableId);

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
                        final TransactionContext txc = new TransactionContext();
                        txc.beginTransaction(txId);
                        txMap.put(txId, txc);
                        log.debug("start transaction id {} tx {} table {}", nextOffset, txId, tableId);
                        dispatcher.dispatchTransactionStartedEvent(partition, txId, offsetContext,
                                eheader.getTime());
                    }
                        break;
                    case END_COMMIT: {
                        // end commit
                        // TOOD transaction must be provided by the OffsetContext
                        final String txId = eheader.getCommitCycle().toString();
                        final TransactionContext txc = txMap.remove(txId);
                        log.debug("commit transaction id {} tx {} table {}", nextOffset, txId, tableId);
                        if (txc != null) {
                            txc.endTransaction();
                            dispatcher.dispatchTransactionCommittedEvent(partition, offsetContext,
                                    eheader.getTime());
                        }
                    }
                        break;
                    case FILE_CHANGE, FILE_CREATED: {
                        // table has changed - reload schema
                        schema.clearCache(tableId.table(), tableId.schema());
                        schema.getRecordFormat(tableId.table(), tableId.schema());
                    }
                        break;
                    case BEFORE_IMAGE: {
                        // before image
                        tableId.schema();
                        final Object[] dataBefore = r.decode(schema.getFileDecoder());

                        cacheBefore(tableId, dataBefore);
                    }
                        break;
                    case AFTER_IMAGE: {
                        // after image
                        // before image is meant to have been immediately before
                        final Object[] dataBefore = getBefore(tableId);
                        final Object[] dataNext = r.decode(schema.getFileDecoder());

                        offsetContext.updateSourceInfo(eheader.getTime());

                        final String txId = eheader.getCommitCycle().toString();
                        final TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);

                        log.debug("update event id {} tx {} table {}", nextOffset, txId, tableId);

                        dispatcher.dispatchDataChangeEvent(partition, tableId, new As400ChangeRecordEmitter(partition,
                                offsetContext, Operation.UPDATE, dataBefore, dataNext, clock, connectorConfig));
                    }
                        break;
                    case ADD_ROW1, ADD_ROW2: {
                        // record added
                        final Object[] dataNext = r.decode(schema.getFileDecoder());
                        offsetContext.updateSourceInfo(eheader.getTime());

                        final String txId = eheader.getCommitCycle().toString();
                        final TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);
                        if (txc != null) {
                            txc.event(tableId);
                        }

                        log.debug("insert event id {} tx {} table {}", offsetContext.getPosition(), txId,
                                tableId);
                        dispatcher.dispatchDataChangeEvent(partition, tableId, new As400ChangeRecordEmitter(partition,
                                offsetContext, Operation.CREATE, null, dataNext, clock, connectorConfig));
                    }
                        break;
                    case DELETE_ROW, ROLLBACK_DELETE_ROW: {
                        // record deleted
                        final Object[] dataBefore = r.decode(schema.getFileDecoder());

                        offsetContext.updateSourceInfo(eheader.getTime());

                        final String txId = eheader.getCommitCycle().toString();
                        final TransactionContext txc = txMap.get(txId);
                        offsetContext.setTransaction(txc);
                        if (txc != null) {
                            txc.event(tableId);
                        }

                        log.debug("delete event id {} tx {} table {}", offsetContext.getPosition(), txId,
                                tableId);
                        dispatcher.dispatchDataChangeEvent(partition, tableId, new As400ChangeRecordEmitter(partition,
                                offsetContext, Operation.DELETE, dataBefore, null, clock, connectorConfig));
                    }
                        break;
                    default:
                        break;
                }
            }
            catch (IOException | SQLNonTransientConnectionException e) {
                throw e;
            }
            catch (final Exception e) {
                log.error("Failed to process record", e);
            }
        };
    }

    private boolean ignore(JournalEntryType journalCode) {
        return journalCode == JournalEntryType.OPEN || journalCode == JournalEntryType.CLOSE;
    }

    @Override
    public As400OffsetContext getOffsetContext() {
        return offsetContext;
    }
}
