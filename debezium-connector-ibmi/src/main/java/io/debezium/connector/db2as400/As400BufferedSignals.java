/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.List;
import java.util.Map;

import io.debezium.document.DocumentReader;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.signal.actions.SignalAction;
import io.debezium.pipeline.signal.channels.SignalChannelReader;
import io.debezium.pipeline.spi.Offsets;

/** Channel readers may run concurrently; every action executes inside source-thread publication. */
final class As400BufferedSignals extends SignalProcessor<As400Partition, As400OffsetContext> {
    private Throwable actionFailure;
    private final Map<String, SignalAction<As400Partition>> actions = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicBoolean started = new java.util.concurrent.atomic.AtomicBoolean();

    As400BufferedSignals(As400ConnectorConfig config, List<SignalChannelReader> channels, Offsets<As400Partition, As400OffsetContext> offsets) {
        super(As400RpcConnector.class, config, Map.of(), channels, DocumentReader.defaultReader(), offsets);
    }

    @Override
    public synchronized void registerSignalAction(String id, SignalAction<As400Partition> action) {
        if (actions.put(id, action) != null) {
            return;
        }
        super.registerSignalAction(id, new SignalAction<>() {
            @Override
            public boolean isSynchronous() {
                return true;
            }

            @Override
            public boolean arrived(SignalPayload<As400Partition> payload) throws InterruptedException {
                if (actionFailure != null) {
                    throw new IllegalStateException("A previous buffered signal action failed", actionFailure);
                }
                try {
                    // A queued request survives the legacy/native dispatcher
                    // transition and executes against its current implementation.
                    return actions.get(id).arrived(payload);
                }
                catch (RuntimeException | InterruptedException failure) {
                    actionFailure = failure;
                    throw failure;
                }
            }
        });
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            super.start();
        }
    }

    @Override
    public synchronized void setContext(Offsets<As400Partition, As400OffsetContext> offsets) {
        super.setContext(offsets);
    }

    @Override
    public synchronized void process() {
        super.process();
    }

    @Override
    public synchronized void processSourceSignal(As400Partition partition) {
        super.processSourceSignal(partition);
    }

    void executePending() throws InterruptedException {
        if (actionFailure == null) {
            processSynchronousSignals();
        }
        // Core logs action errors. A partially emitted buffered snapshot must
        // instead retain its old F/W and chunk state for restart.
        if (actionFailure instanceof InterruptedException interrupted) {
            throw interrupted;
        }
        if (actionFailure != null) {
            throw new IllegalStateException("Buffered signal action failed before checkpoint completion", actionFailure);
        }
    }
}
