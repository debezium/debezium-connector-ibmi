/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.common.BaseSourceInfo;

/**
 * Coordinates from the database log to establish the relation between the change streamed and the source log position.
 * Maps to {@code source} field in {@code Envelope}.
 *
 * @author Jiri Pechanec
 *
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    public static final String SNAPSHOT_KEY = "snapshot";
    public static final String JOURNAL_KEY = "journal";
    public static final String RECEIVER_KEY = "receiver";
    public static final String RECEIVER_LIBRARY_KEY = "receiver_library";
    private Instant sourceTime;
    private String databaseName;
    private String receiver;
    private String receiverLib;
    private String sequence;

    protected SourceInfo(As400ConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.databaseName = "db name"; // connectorConfig.getDatabaseName();
    }

    public void setSourceTime(Instant instant) {
        sourceTime = instant;
    }

    @Override
    protected Instant timestamp() {
        return sourceTime;
    }

    @Override
    protected String database() {
        return databaseName;
    }

    public String getReceiver() {
        return receiver;
    }

    public void setReceiver(String receiver) {
        this.receiver = receiver;
    }

    public String getReceiverLib() {
        return receiverLib;
    }

    public void setReceiverLib(String receiverLib) {
        this.receiverLib = receiverLib;
    }

    @Override
    protected String sequence() {
        return sequence;
    }

    public void setSequence(String sequence) {
        this.sequence = sequence;
    }

}
