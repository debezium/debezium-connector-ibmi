/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLNonTransientConnectionException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.SecureAS400;
import com.ibm.as400.access.SocketProperties;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfig;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.exception.JournalReceiverNotFoundException;
import io.debezium.ibmi.db2.journal.retrieve.exception.LostJournalException;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.Partition;

public class As400RpcConnection implements AutoCloseable, Connect<AS400, IOException> {
    private static Logger log = LoggerFactory.getLogger(As400RpcConnection.class);
    private final As400StreamingChangeEventSourceMetrics streamingMetrics;

    private final As400ConnectorConfig config;
    private JournalInfo journalInfo;
    private RetrieveJournal retrieveJournal;
    private AS400 as400;
    private static SocketProperties socketProperties = new SocketProperties();
    private final LogLimmiting periodic = new LogLimmiting(5 * 60 * 1000l);
    private final JournalInfoRetrieval journalInfoRetrieval;

    private final boolean isSecure;

    public As400RpcConnection(As400ConnectorConfig config, As400StreamingChangeEventSourceMetrics streamingMetrics, List<FileFilter> includes, long cacheWait) {
        super();
        this.config = config;
        this.isSecure = config.getJdbcConfig().getBoolean("secure", config.isSecure());
        this.streamingMetrics = streamingMetrics;
        this.journalInfoRetrieval = new JournalInfoRetrieval(cacheWait, config.cacheAdditionalDelay(), config.getPollInterval().toMillis());
        try {
            System.setProperty("com.ibm.as400.access.AS400.guiAvailable", "False");
            journalInfo = journalInfoRetrieval.getJournal(connection(), config.getSchema(), includes);

            boolean transactionMgt = config.isTransactionMgmtEnabled();

            final RetrieveConfig rconfig = new RetrieveConfigBuilder().withAs400(this)
                    .withJournalBufferSize(config.getJournalBufferSize())
                    .withJournalInfo(journalInfo)
                    .withMaxServerSideEntries(config.getMaxServerSideEntries())
                    .withServerFiltering(!transactionMgt)
                    .withIncludeFiles(includes).withDumpFolder(config.diagnosticsFolder())
                    .build();
            retrieveJournal = new RetrieveJournal(rconfig, journalInfoRetrieval);
        }
        catch (final IOException e) {
            log.error("Failed to fetch library", e);
        }
    }

    @Override
    public void close() {
        try {
            if (as400 != null) {
                log.info("Disconnecting");
                if (retrieveJournal != null) {
                    retrieveJournal.cancelJob();
                }
                this.as400.disconnectAllServices();
            }
        }
        catch (final Exception e) {
            log.debug("Problem closing connection", e);
        }

        this.as400 = null;
    }

    public boolean isValid() {
        return (as400 != null && as400.isConnectionAlive(AS400.COMMAND));

    }

    /**
     * Availability of a stored journal position on the server.
     */
    public enum PositionAvailability {
        /** The receiver referenced by the stored position still exists. */
        AVAILABLE,
        /** No position is stored yet (fresh start / blank offset); streaming begins at the earliest receiver. */
        NOT_SET,
        /** The receiver has been pruned/rotated off the server; the position can never be resolved again. */
        PRUNED
    }

    /**
     * Classify a stored offset without collapsing every failure mode into a single boolean.
     * A pruned receiver ({@link PositionAvailability#PRUNED}) is a permanent condition, whereas a
     * transient RPC/connection failure is rethrown so the caller retries instead of wrongly treating
     * the position as lost.
     *
     * @throws DebeziumException if the position could not be validated for a transient reason
     */
    public PositionAvailability checkLogPosition(OffsetContext offsetContext) {
        return checkLogPosition(journalInfoRetrieval, as400, offsetContext);
    }

    static PositionAvailability checkLogPosition(JournalInfoRetrieval journalInfoRetrieval, AS400 as400, OffsetContext offsetContext) {
        if (!(offsetContext instanceof As400OffsetContext offset) || !offset.isPositionSet()) {
            return PositionAvailability.NOT_SET;
        }
        try {
            JournalReceiverInfo receiver = new JournalReceiverInfo(offset.getPosition().getReceiver(), null, null, Optional.empty());
            DetailedJournalReceiver dr = journalInfoRetrieval.getReceiverDetails(as400, receiver);
            return dr != null ? PositionAvailability.AVAILABLE : PositionAvailability.PRUNED;
        }
        catch (JournalReceiverNotFoundException e) {
            // Non-transient: the receiver has been pruned on the source and will never come back.
            log.warn("stored journal position {} points at a receiver that no longer exists on the server ({})", offsetContext, e.getMessageId());
            return PositionAvailability.PRUNED;
        }
        catch (Exception e) {
            // Transient (connection/RPC): do not misreport as lost, let the caller retry.
            throw new DebeziumException("transient failure while validating stored journal position " + offsetContext, e);
        }
    }

    public boolean validateLogPosition(Partition partition, OffsetContext offsetContext, CommonConnectorConfig config) {
        // AVAILABLE and NOT_SET are both valid starting points; only a pruned receiver is unavailable.
        return checkLogPosition(offsetContext) != PositionAvailability.PRUNED;
    }

    @Override
    public AS400 connection() throws IOException {
        if (as400 == null || !as400.isConnectionAlive(AS400.COMMAND)) {
            log.info("create new as400 connection");
            try {
                // need to both create a new object and connect
                close();
                if (isSecure) {
                    this.as400 = new SecureAS400(config.getHostname(), config.getUser(),
                            config.getPassword().toCharArray());
                }
                else {
                    this.as400 = new AS400(config.getHostname(), config.getUser(), config.getPassword().toCharArray());
                }
                socketProperties.setSoTimeout(config.getSocketTimeout());
                as400.setSocketProperties(socketProperties);
                as400.connectService(AS400.COMMAND);
            }
            catch (final Exception e) {
                log.error("Failed to reconnect", e);
                throw new IOException("Failed to reconnect", e);
            }
        }
        return as400;
    }

    public JournalPosition getCurrentPosition() throws RpcException {
        try {
            final JournalPosition position = journalInfoRetrieval.getCurrentPosition(connection(), journalInfo);

            return new JournalPosition(position);
        }
        catch (final Exception e) {
            throw new RpcException("Failed to find offset", e);
        }
    }

    public RetrievalState getJournalEntries(ChangeEventSourceContext context, As400OffsetContext offsetCtx, BlockingReceiverConsumer consumer, WatchDog watchDog)
            throws Exception {
        final JournalProcessedPosition position = offsetCtx.getPosition();
        try {

            RetrievalState state = retrieveJournal.retrieveJournal(position);

            logOffsets(position, state);

            watchDog.alive();

            if (state.hasData()) {
                while (retrieveJournal.nextEntry() && context.isRunning()) {
                    watchDog.alive();
                    final EntryHeader eheader = retrieveJournal.getEntryHeader();
                    final BigInteger processingOffset = eheader.getSequenceNumber();

                    consumer.accept(processingOffset, retrieveJournal, eheader);
                    // while processing journal entries getPosistion is the current position
                    position.setPosition(retrieveJournal.getPosition());
                }

                // note that getPosition returns the current position or the next continuation offset after the current block
                offsetCtx.setPosition(retrieveJournal.getPosition());

            }
            return state;
        }
        catch (LostJournalException e) {
            // this is bad, we've probably lost data
            final List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(connection(), journalInfo);
            log.error("Failed to fetch journal entries '{}', resetting journal to blank",
                    Map.of("position", position,
                            "receivers", receivers));
            offsetCtx.setPosition(new JournalProcessedPosition());
        }

        return RetrievalState.NotCalled;
    }

    private void logOffsets(JournalProcessedPosition position, RetrievalState state) throws IOException, Exception {
        if (periodic.shouldLogRateLimted("offsets")) {
            final JournalPosition currentReceiver = getCurrentPosition();
            final BigInteger behind = currentReceiver.getOffset().subtract(position.getOffset());
            streamingMetrics.setJournalOffset(currentReceiver.getOffset());
            streamingMetrics.setJournalBehind(behind);
            streamingMetrics.setLastProcessedMs(position.getTimeOfLastProcessed().toEpochMilli());
            log.info("Current position diagnostics last call {}, header {}, behind {}, currentReceiver", state, retrieveJournal.getFirstHeader(), behind,
                    currentReceiver);
        }
    }

    public interface BlockingReceiverConsumer {
        void accept(BigInteger offset, RetrieveJournal r, EntryHeader eheader) throws RpcException, InterruptedException, IOException, SQLNonTransientConnectionException;
    }

    public interface BlockingNoDataConsumer {
        void accept() throws InterruptedException;
    }

    public static class RpcException extends Exception {
        public RpcException(String message, Throwable cause) {
            super(message, cause);
        }

        public RpcException(String message) {
            super(message);
        }
    }

    private static class LogLimmiting {
        private final Map<String, Long> lastLogged = new HashMap<>();
        private final long rate;

        LogLimmiting(long rate) {
            this.rate = rate;
        }

        public boolean shouldLogRateLimted(String type) {
            if (lastLogged.containsKey(type)) {
                if (System.currentTimeMillis() > rate + lastLogged.get(type)) {
                    lastLogged.put(type, System.currentTimeMillis());
                    return true;
                }
            }
            else {
                lastLogged.put(type, System.currentTimeMillis());
                return true;
            }
            return false;
        }
    }
}
