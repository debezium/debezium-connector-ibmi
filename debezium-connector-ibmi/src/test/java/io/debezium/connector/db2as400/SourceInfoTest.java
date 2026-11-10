/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.SourceInfo.RECEIVER_KEY;
import static io.debezium.connector.db2as400.SourceInfo.RECEIVER_LIBRARY_KEY;
import static io.debezium.connector.db2as400.SourceInfo.SEQUENCE_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.VerifyRecord;
import io.debezium.schema.SchemaFactory;
import io.debezium.time.Conversions;

/**
 * @author Pierre-Yves Péton
 */
public class SourceInfoTest {

    private SourceInfo source;

    @BeforeEach
    public void beforeEach() {
        source = new SourceInfo(new As400ConnectorConfig(
                Configuration.create()
                        .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                        .with(As400ConnectorConfig.DATABASE_NAME, "serverX")
                        .build()));
        source.setSourceTime(Conversions.toInstantFromMicros(123_456_789L));
        source.setReceiver("RECV008");
        source.setReceiverLib("RCV_LIB");
        source.setSequence("82");
    }

    @Test
    public void shouldIncludeConnectorAndJournalCoordinatesWhenBuildingSourceRecord() {
        var record = source.struct();
        assertThat(record.getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
        assertThat(record.getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
        assertThat(record.getString(RECEIVER_KEY)).isEqualTo("RECV008");
        assertThat(record.getString(RECEIVER_LIBRARY_KEY)).isEqualTo("RCV_LIB");
        assertThat(record.getString(SEQUENCE_KEY)).isEqualTo("82");
        assertThat(record.getInt64("ts_ms")).isEqualTo(123_456L);
    }

    @Test
    public void shouldPreserveThePublishedSourceSchema() {
        final Schema schema = SchemaBuilder.struct()
                .name("io.debezium.connector.db2as400.Source")
                .version(SchemaFactory.SOURCE_INFO_DEFAULT_SCHEMA_VERSION)
                .field("version", Schema.STRING_SCHEMA)
                .field("connector", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA)
                .field("snapshot", SchemaFactory.get().snapshotRecordSchema())
                .field("db", Schema.STRING_SCHEMA)
                .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
                .field(RECEIVER_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(RECEIVER_LIBRARY_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        VerifyRecord.assertConnectSchemasAreEqual(null, source.struct().schema(), schema);
    }
}
