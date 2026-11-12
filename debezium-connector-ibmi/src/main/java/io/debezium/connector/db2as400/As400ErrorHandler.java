/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;

/** Data failures cannot be repaired by reconnecting to the same journal entry. */
class As400ErrorHandler extends ErrorHandler {
    As400ErrorHandler(As400ConnectorConfig config, ChangeEventQueue<?> queue) {
        super(As400RpcConnector.class, config, queue, null);
    }

    As400ErrorHandler(As400ConnectorConfig config, ChangeEventQueue<?> queue, ErrorHandler previous) {
        super(As400RpcConnector.class, config, queue, previous);
    }

    @Override
    protected boolean isRetriable(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof As400StreamingChangeEventSource.RowProcessingException
                    || cause instanceof As400StreamingChangeEventSource.TruncateException
                    || cause instanceof As400StreamingChangeEventSource.TransactionMetadataException
                    || cause instanceof As400SpillStore.SpillException
                    || cause instanceof As400JournalCapture.CaptureException) {
                return false;
            }
        }
        return super.isRetriable(failure);
    }
}
