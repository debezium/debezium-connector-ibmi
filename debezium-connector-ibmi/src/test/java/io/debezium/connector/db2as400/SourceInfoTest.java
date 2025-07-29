package io.debezium.connector.db2as400;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.data.VerifyRecord;
import io.debezium.schema.SchemaFactory;
import io.debezium.time.Conversions;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Before;
import org.junit.Test;

import static io.debezium.connector.db2as400.SourceInfo.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Pierre-Yves Péton
 */
public class SourceInfoTest {

    private SourceInfo source;

    @Before
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
        source.setEventTime(Long.toString(Conversions.toInstantFromMicros(123_456_000L).toEpochMilli()));
    }

    @Test
    public void versionIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_VERSION_KEY)).isEqualTo(Module.version());
    }

    @Test
    public void connectorIsPresent() {
        assertThat(source.struct().getString(SourceInfo.DEBEZIUM_CONNECTOR_KEY)).isEqualTo(Module.name());
    }

    @Test
    public void shouldHaveReceiver() {
        assertThat(source.struct().getString(RECEIVER_KEY)).isEqualTo("RECV008");
    }

    @Test
    public void shouldHaveReceiverLib() {
        assertThat(source.struct().getString(RECEIVER_LIBRARY_KEY)).isEqualTo("RCV_LIB");
    }

    @Test
    public void shouldHaveSequence() {
        assertThat(source.struct().getString(SEQUENCE_KEY)).isEqualTo("82");
    }

    @Test
    public void shouldHaveEventTime() {
        assertThat(source.struct().getString(EVENT_TIME_KEY)).isEqualTo("123456");
    }

    @Test
    public void shouldHaveTimestamp() {
        assertThat(source.struct().getInt64("ts_ms")).isEqualTo(123_456L);
    }

    @Test
    public void schemaIsCorrect() {
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
                .field(RECEIVER_KEY, Schema.STRING_SCHEMA)
                .field(RECEIVER_LIBRARY_KEY, Schema.STRING_SCHEMA)
                .field(EVENT_TIME_KEY, Schema.STRING_SCHEMA)
                .build();

        VerifyRecord.assertConnectSchemasAreEqual(null, source.struct().schema(), schema);
    }
}
