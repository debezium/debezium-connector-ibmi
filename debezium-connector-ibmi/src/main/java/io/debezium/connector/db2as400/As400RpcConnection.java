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

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfig;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
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
    private final JournalReadLifecycle lifecycle = new JournalReadLifecycle();
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

            final RetrieveConfig rconfig = new RetrieveConfigBuilder().withAs400(this)
                    .withJournalBufferSize(config.getJournalBufferSize())
                    .withJournalInfo(journalInfo)
                    .withMaxServerSideEntries(config.getMaxServerSideEntries())
                    .withServerFiltering(true)
                    .withIncludeFiles(includes).withDumpFolder(config.diagnosticsFolder())
                    .build();
            retrieveJournal = new RetrieveJournal(rconfig, journalInfoRetrieval, lifecycle);
        }
        catch (final IOException e) {
            log.error("Failed to fetch library", e);
        }
    }

    /**
     * Permanently reject new work, cancel the captured read, then drain admitted
     * work. Unlike close(), this instance can never reconnect after stop.
     * Must be called outside a reader operation. No driver deadline is assumed.
     */
    public void stop() {
        lifecycle.requestStop();
        final AS400 closing;
        synchronized (this) {
            closing = as400;
        }
        try {
            // Also cancel when an ordinary close has already detached as400.
            if (retrieveJournal != null) {
                retrieveJournal.cancelJob();
            }
            close();
        }
        finally {
            lifecycle.awaitDrained();
            // An admitted driver operation may have reconnected its captured
            // transport while stop was draining. Release that same object again.
            if (closing != null) {
                try {
                    closing.disconnectAllServices();
                }
                catch (Exception e) {
                    log.debug("Problem closing drained connection", e);
                }
            }
        }
    }

    // Streaming retries include JDBC work. They must drain with this reader even
    // when RPC reconnect fails or stop arrives between the two transports.
    JournalReadLifecycle.Lease admitRetry() throws IOException {
        return lifecycle.enter();
    }

    @Override
    public synchronized void close() {
        final AS400 closing = as400;
        as400 = null;
        if (closing == null) {
            return;
        }
        try {
            log.info("Disconnecting");
            if (retrieveJournal != null) {
                retrieveJournal.cancelJob();
            }
        }
        finally {
            try {
                closing.disconnectAllServices();
            }
            catch (final Exception e) {
                log.debug("Problem closing connection", e);
            }
        }
    }

    public synchronized boolean isValid() {
        return (as400 != null && as400.isConnectionAlive(AS400.COMMAND));

    }

    public boolean validateLogPosition(Partition partition, OffsetContext offsetContext, CommonConnectorConfig config) {
        try (var ignored = lifecycle.enter()) {
            if (offsetContext instanceof As400OffsetContext offset) {
                if (offset.isPositionSet()) {
                    JournalReceiverInfo receiver = new JournalReceiverInfo(offset.getPosition().getReceiver(), null, null, Optional.empty());

                    final AS400 connection = connection();
                    try {
                        DetailedJournalReceiver dr = journalInfoRetrieval.getReceiverDetails(connection, receiver);
                        lifecycle.checkOpen();
                        return dr != null;
                    }
                    finally {
                        disconnectIfStopped(connection);
                    }
                }
            }
        }
        catch (Exception e) {
            log.warn("unable to find journal poisition {}", offsetContext, e);
        }
        return false;
    }

    @Override
    public synchronized AS400 connection() throws IOException {
        try (var ignored = lifecycle.enter()) {
            return connect();
        }
    }

    private AS400 connect() throws IOException {
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
        try (var ignored = lifecycle.enter()) {
            final AS400 connection = connection();
            try {
                final JournalPosition position = journalInfoRetrieval.getCurrentPosition(connection, journalInfo);
                lifecycle.checkOpen();
                return new JournalPosition(position);
            }
            finally {
                disconnectIfStopped(connection);
            }
        }
        catch (final Exception e) {
            throw new RpcException("Failed to find offset", e);
        }
    }

    private void disconnectIfStopped(AS400 connection) {
        if (lifecycle.isStopped()) {
            try {
                connection.disconnectAllServices();
            }
            catch (Exception e) {
                log.warn("Failed to disconnect stopped metadata operation", e);
            }
        }
    }

    public RetrievalState getJournalEntries(ChangeEventSourceContext context, As400OffsetContext offsetCtx, BlockingReceiverConsumer consumer, WatchDog watchDog)
            throws Exception {
        try (var ignored = lifecycle.enter()) {
            return readEntries(context, offsetCtx, consumer, watchDog);
        }
    }

    private RetrievalState readEntries(ChangeEventSourceContext context, As400OffsetContext offsetCtx, BlockingReceiverConsumer consumer, WatchDog watchDog)
            throws Exception {
        final JournalProcessedPosition position = offsetCtx.getPosition();
        try {
            lifecycle.checkOpen();
            RetrievalState state = retrieveJournal.retrieveJournal(position);

            lifecycle.checkOpen();
            logOffsets(position, state);

            watchDog.alive();

            if (state.hasData()) {
                while (!lifecycle.isStopped() && retrieveJournal.nextEntry() && context.isRunning()) {
                    watchDog.alive();
                    final EntryHeader eheader = retrieveJournal.getEntryHeader();
                    final BigInteger processingOffset = eheader.getSequenceNumber();

                    consumer.accept(processingOffset, retrieveJournal, eheader);
                    // while processing journal entries getPosistion is the current position
                    position.setPosition(retrieveJournal.getPosition());
                }

                // note that getPosition returns the current position or the next continuation offset after the current block
                lifecycle.checkOpen();
                offsetCtx.setPosition(retrieveJournal.getPosition());

            }
            return state;
        }
        catch (LostJournalException e) {
            lifecycle.checkOpen();
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
