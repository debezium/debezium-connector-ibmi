/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The processed journal position, the combination of the journal receiver and sequence number,
 * is compared against the value captured when the monitor was last consulted, and when the
 * position has not moved, a warning is logged. The journal records entries for all journaled
 * tables along with commit-control entries, so a stationary position means no journal entries
 * of any kind have been processed during the check interval.
 *
 * @author Chris Cranford
 */
public class As400OffsetActivityMonitor implements OffsetActivityMonitor<As400Partition, As400OffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(As400OffsetActivityMonitor.class);

    private final Duration checkInterval;

    private JournalProcessedPosition previousPosition;

    public As400OffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void checkForStaleOffsets(As400Partition partition, As400OffsetContext offsetContext) {
        final JournalProcessedPosition position = offsetContext.getPosition();

        // Check for stale state
        if (position != null && Objects.equals(previousPosition, position)) {
            LOGGER.warn("Offset journal position {} has not changed in at least {} milliseconds. " +
                    "This may indicate the database is idle, there are no changes for the captured tables, " +
                    "or that the connector is no longer receiving journal entries from the server.",
                    position, checkInterval.toMillis());
        }

        // Update tracked stats; copied because the offset context mutates its position in place
        previousPosition = position != null ? new JournalProcessedPosition(position) : null;
    }

}