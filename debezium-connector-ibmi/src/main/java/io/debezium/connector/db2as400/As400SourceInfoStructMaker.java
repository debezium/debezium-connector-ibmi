/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.SourceInfo.EVENT_TIME_KEY;
import static io.debezium.connector.db2as400.SourceInfo.RECEIVER_KEY;
import static io.debezium.connector.db2as400.SourceInfo.RECEIVER_LIBRARY_KEY;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

public class As400SourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private final Schema schema;

    public As400SourceInfoStructMaker(String connector, String version, CommonConnectorConfig connectorConfig) {
        init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name("io.debezium.connector.db2as400.Source")
                // TODO add in table info
                // .field(SourceInfo.SCHEMA_NAME_KEY, Schema.STRING_SCHEMA)
                // .field(SourceInfo.TABLE_NAME_KEY, Schema.STRING_SCHEMA)
                .field(RECEIVER_KEY, Schema.STRING_SCHEMA)
                .field(RECEIVER_LIBRARY_KEY, Schema.STRING_SCHEMA)
                .field(EVENT_TIME_KEY, Schema.STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        final Struct ret = super.commonStruct(sourceInfo);
        // .put(SourceInfo.SCHEMA_NAME_KEY, sourceInfo.getTableId().schema())
        // .put(SourceInfo.TABLE_NAME_KEY, sourceInfo.getTableId().table());
        ret.put(RECEIVER_KEY, sourceInfo.getReceiver() == null ? "null" : sourceInfo.getReceiver());
        ret.put(RECEIVER_LIBRARY_KEY, sourceInfo.getReceiverLib() == null ? "null" : sourceInfo.getReceiverLib());
        ret.put(EVENT_TIME_KEY, sourceInfo.getEventTime() == null ? "null" : sourceInfo.getEventTime());
        return ret;
    }
}
