/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/** A transport restart constructs a new admitted session and recovers durable F/W. */
final class As400RecoveryErrorHandler extends As400ErrorHandler {
    private final boolean freshSession;

    As400RecoveryErrorHandler(As400ConnectorConfig config, ChangeEventQueue<?> queue, ErrorHandler previous, boolean freshSession) {
        super(config, queue, previous);
        this.freshSession = freshSession;
    }

    @Override
    protected boolean isRetriable(Throwable failure) {
        if (!freshSession) {
            return false;
        }
        boolean transport = false;
        var seen = java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<Throwable, Boolean>());
        for (Throwable cause = failure; cause != null && seen.add(cause); cause = cause.getCause()) {
            // A decoder/history error is not a broken connection, even if its
            // cause happens to be I/O. Cleanup uncertainty cannot authorize reuse.
            if (cause.getSuppressed().length != 0 || cause instanceof IllegalArgumentException
                    || cause instanceof As400SourceNormalizer.NormalizationException
                    || cause instanceof As400ReplayCoordinator.RecoveryException
                    || cause instanceof As400TransactionBuffer.BufferException
                    || cause instanceof As400NativeEvidenceException
                    || cause instanceof As400SpillStore.SpillException
                    || cause instanceof As400JournalCapture.CaptureException
                    || cause instanceof As400StreamingChangeEventSource.RowProcessingException
                    || cause instanceof As400StreamingChangeEventSource.TruncateException
                    || cause instanceof As400StreamingChangeEventSource.TransactionMetadataException) {
                return false;
            }
            transport |= cause instanceof SocketException || cause instanceof SocketTimeoutException
                    || cause instanceof SQLException sql && sql.getSQLState() != null && sql.getSQLState().startsWith("08");
        }
        return transport;
    }

    @Override
    protected boolean isCustomRetriable(Throwable failure) {
        // A message regex cannot override recovery or cleanup evidence.
        return false;
    }
}
