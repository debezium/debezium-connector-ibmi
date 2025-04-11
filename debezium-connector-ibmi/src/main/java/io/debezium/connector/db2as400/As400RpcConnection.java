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
import io.debezium.function.LogPositionValidator;
import io.debezium.ibmi.db2.journal.retrieve.Connect;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfig;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
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
    private final JournalInfoRetrieval journalInfoRetrieval = new JournalInfoRetrieval();

    private final boolean isSecure;

    public As400RpcConnection(As400ConnectorConfig config, As400StreamingChangeEventSourceMetrics streamingMetrics, List<FileFilter> includes) {
        super();
        this.config = config;
        this.isSecure = config.getJdbcConfig().getBoolean("secure", config.isSecure());
        this.streamingMetrics = streamingMetrics;
        try {
            System.setProperty("com.ibm.as400.access.AS400.guiAvailable", "False");
            final var JournalInfoRetrieval = new JournalInfoRetrieval();
            journalInfo = JournalInfoRetrieval.getJournal(connection(), config.getSchema(), includes);
            final RetrieveConfig rconfig = new RetrieveConfigBuilder().withAs400(this)
                    .withJournalBufferSize(config.getJournalBufferSize())
                    .withJournalInfo(journalInfo)
                    .withMaxServerSideEntries(config.getMaxServerSideEntries())
                    .withServerFiltering(true)
                    .withIncludeFiles(includes).withDumpFolder(config.diagnosticsFolder()).build();
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
                retrieveJournal.cancelJob();
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
    
    public boolean validateLogPosition(Partition partition, OffsetContext offsetContext, CommonConnectorConfig config) {
        try {
            if (offsetContext instanceof As400OffsetContext offset) {
                JournalReceiverInfo receiver = new JournalReceiverInfo(offset.getPosition().getReceiver(), null, null, Optional.empty()); 
                
                DetailedJournalReceiver dr = new JournalInfoRetrieval().getReceiverDetails(as400, receiver);
                return dr != null;
            }
        } catch (Exception e) {
            log.warn("unable to find journal poisition {}", offsetContext, e);
        }
        return false;
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

    public boolean getJournalEntries(ChangeEventSourceContext context, As400OffsetContext offsetCtx, BlockingReceiverConsumer consumer, WatchDog watchDog)
            throws Exception {
        boolean success = false;
        final JournalProcessedPosition position = offsetCtx.getPosition();
        success = retrieveJournal.retrieveJournal(position);

        logOffsets(position, success);

        watchDog.alive();

        if (success) {
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
        else {
            // this is bad, we've probably lost data
            final List<DetailedJournalReceiver> receivers = journalInfoRetrieval.getReceivers(connection(), journalInfo);
            log.error("Failed to fetch journal entries '{}', resetting journal to blank",
                    Map.of("position", position,
                            "receivers", receivers));
            offsetCtx.setPosition(new JournalProcessedPosition());
        }

        return success && retrieveJournal.futureDataAvailable();
    }

    private void logOffsets(JournalProcessedPosition position, boolean success) throws IOException, Exception {
        if (periodic.shouldLogRateLimted("offsets")) {
            final JournalPosition currentReceiver = getCurrentPosition();
            final BigInteger behind = currentReceiver.getOffset().subtract(position.getOffset());
            streamingMetrics.setJournalOffset(currentReceiver.getOffset());
            streamingMetrics.setJournalBehind(behind);
            streamingMetrics.setLastProcessedMs(position.getTimeOfLastProcessed().toEpochMilli());
            log.info("Current position diagnostics '{}'",
                    Map.of("header", retrieveJournal.getFirstHeader(),
                            "behind", behind,
                            "position", position,
                            "currentReceiver", currentReceiver,
                            "success", success));

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
