/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Instant;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfig;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnFilterMode;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalTableFilters;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.TableFilter;

//TODO  can we deliver HistorizedRelationalDatabaseConnectorConfig or should it be RelationalDatabaseConnectorConfig
public class As400ConnectorConfig extends RelationalDatabaseConnectorConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(As400ConnectorTask.class);
    
    private static TableIdToStringMapper tableToString = x -> {
        if (x.table() != null) {
            if (x.schema() != null) {
                final StringBuilder sb = new StringBuilder(x.schema());
                sb.append(".").append(x.table());
                return sb.toString();
            }
            LOGGER.error("missing schema name {}, did the function expect the database.schema.table?", x);
        
            return x.table();
        }
        LOGGER.error("missing table name {}", x);
        return "";
    };

    private final SnapshotMode snapshotMode;
    private final Configuration config;
    private String incrementalTables = "";

    // used by the snapshot to limit the additional tables for a change in configuration
    private RelationalTableFilters tableFilters;

    /**
     * A field for the password to connect to the AS400. This field has no default
     * value.
     */
    public static final Field SCHEMA = Field.create(DATABASE_CONFIG_PREFIX + "schema", "schema holding tables to capture");

    /**
     * A field for the size of buffer for fetching journal entries default 65535 (should not be smaller)
     */
    public static final Field BUFFER_SIZE = Field.create("buffer.size", "journal buffer size",
            "size of buffer for fetching journal entries default 131072 (should not be smaller)", "131072");

    /**
     * keep alive flag, should the driver use a secure connection defaults to false
     */
    public static final Field SECURE = Field.create("secure", "secure", "use secure connection", true);

    /**
     * The timeout to use for sockets
     */
    public static final Field SOCKET_TIMEOUT = Field.create("socket.timeout", "socket timeout in milliseconds",
            "socket timeout", 0);

    /**
     * If the ccsid is wrong on your tables and that is the least of your problems - just correct the CCSID before using this or as a last resort...
     * This applies to all tables - everything
     * mapping from.ccsid and to.ccsid must *both* be specified
     */
    public static final Field FROM_CCSID = Field.create("from.ccsid", "from ccsid",
            "when the table indicates this from_ccsid translate to the to_ccsid setting", -1);

    public static final Field TO_CCSID = Field.create("to.ccsid", "to ccsid",
            "when the table indicates the from_ccsid translate to this to_ccsid setting", -1);

    public static final Field DIAGNOSTICS_FOLDER = Field.create("diagnostics.folder",
            "folder to dump failed decodings to", "used when there is a decoding failure to aid diagnostics");

    /**
     * Maximum number of journal entries to process server side
     */
    public static final Field MAX_SERVER_SIDE_ENTRIES = Field.create("max.entries", "max server side entries",
            "Maximum number of journal entries to process server side when filtering", RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES);

    public static final long DEFAULT_MAX_JOURNAL_TIMEOUT = 60000;
    /**
     * Maximum number of journal entries to process server side
     */
    public static final Field MAX_RETRIEVAL_TIMEOUT = Field.create("max.journal.timeout", "max time to fetch the journal entries",
            "Maximum time to fetch the journal entries in ms", DEFAULT_MAX_JOURNAL_TIMEOUT);

    public static final Field TOPIC_NAMING_STRATEGY = Field.create("topic.naming.strategy")
            .withDisplayName("Topic naming strategy class")
            .withType(Type.CLASS)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.MEDIUM)
            .withDescription("The name of the TopicNamingStrategy class that should be used to determine the topic name " +
                    "for data change, schema change, transaction, heartbeat event etc.")
            .withDefault(HostnamePrefixNamingScheme.class.getName());

    public As400ConnectorConfig(Configuration config) {
        super(config, new SystemTablesPredicate(),
                tableToString, 1, ColumnFilterMode.SCHEMA, false);
        this.config = config;
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.tableFilters = new As400NormalRelationalTableFilters(config, new SystemTablesPredicate(), tableToString);
    }

    // used by the snapshot to limit the additional tables for a change in configuration
    public As400ConnectorConfig(Configuration config, String incrementalTableFilters) {
        this(config);
        this.incrementalTables = incrementalTableFilters;
        this.tableFilters = new As400AdditionalRelationalTableFilters(
                config, new SystemTablesPredicate(), tableToString, incrementalTableFilters);
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<? extends EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    public String getHostname() {
        return config.getString(HOSTNAME);
    }

    public String getUser() {
        return config.getString(USER);
    }

    public String getPassword() {
        return config.getString(PASSWORD);
    }

    public String getSchema() {
        return config.getString(SCHEMA).toUpperCase();
    }

    public Integer getJournalBufferSize() {
        return config.getInteger(BUFFER_SIZE);
    }

    public Integer getSocketTimeout() {
        return config.getInteger(SOCKET_TIMEOUT);
    }

    public Integer getMaxServerSideEntries() {
        return config.getInteger(MAX_SERVER_SIDE_ENTRIES);
    }

    public Integer getMaxRetrievalTimeout() {
        return config.getInteger(MAX_RETRIEVAL_TIMEOUT);
    }

    public Integer getFromCcsid() {
        return config.getInteger(FROM_CCSID);
    }

    public Integer getToCcsid() {
        return config.getInteger(TO_CCSID);
    }

    public boolean isSecure() {
        return config.getBoolean(SECURE);
    }

    public String diagnosticsFolder() {
        return config.getString(DIAGNOSTICS_FOLDER);
    }

    public JournalProcessedPosition getOffset() {
        final String receiver = config.getString(As400OffsetContext.RECEIVER);
        final String lib = config.getString(As400OffsetContext.RECEIVER_LIBRARY);
        final String offset = config.getString(As400OffsetContext.EVENT_SEQUENCE);
        final Boolean processed = config.getBoolean(As400OffsetContext.PROCESSED);
        final Long configTime = config.getLong(As400OffsetContext.EVENT_TIME);
        final Instant time = (configTime == null) ? Instant.ofEpochSecond(0) : Instant.ofEpochSecond(configTime);
        return new JournalProcessedPosition(offset, receiver, lib, time, (processed == null) ? false : processed);
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return !(t.schema().toLowerCase().equals("QSYS2") || t.schema().toLowerCase().equals("QSYS")); // TODO
        }
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new As400SourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    public static Field.Set ALL_FIELDS = Field.setOf(JdbcConfiguration.HOSTNAME, USER, PASSWORD, SCHEMA, BUFFER_SIZE,
            RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE, SOCKET_TIMEOUT,
            MAX_SERVER_SIDE_ENTRIES, TOPIC_NAMING_STRATEGY, FROM_CCSID, TO_CCSID, SECURE,
            DIAGNOSTICS_FOLDER);

    public static ConfigDef configDef() {
        final ConfigDef c = RelationalDatabaseConnectorConfig.CONFIG_DEFINITION.edit()
                .name("ibmi")
                .type(
                        HOSTNAME, USER, PASSWORD, SCHEMA, BUFFER_SIZE,
                        SOCKET_TIMEOUT, FROM_CCSID, TO_CCSID, SECURE,
                        DIAGNOSTICS_FOLDER)
                .connector()
                .events(
                        As400OffsetContext.EVENT_SEQUENCE_FIELD,
                        As400OffsetContext.RECEIVER_FIELD,
                        As400OffsetContext.RECEIVER_LIBRARY_FIELD,
                        As400OffsetContext.PROCESSED_FIELD)
                .create().configDef();
        return c;
    }

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withGroup(Field.createGroupEntry(Field.Group.CONNECTOR_SNAPSHOT, 0))
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL)
            .withWidth(Width.SHORT)
            .withImportance(Importance.MEDIUM)
            .withDescription("The criteria for running a snapshot upon startup of the connector. "
                    + "Select one of the following snapshot options: "
                    + "'always': The connector runs a snapshot every time that it starts. After the snapshot completes, the connector begins to stream changes from the transaction log.; "
                    + "'initial' (default): If the connector does not detect any offsets for the logical server name, it runs a snapshot that captures the current full state of the configured tables. After the snapshot completes, the connector begins to stream changes from the transaction log. "
                    + "'initial_only': The connector performs a snapshot as it does for the 'initial' option, but after the connector completes the snapshot, it stops, and does not stream changes from the transaction log.; "
                    + "'never': The connector does not run a snapshot. Upon first startup, the connector immediately begins reading from the beginning of the transaction log. "
                    + "'exported': This option is deprecated; use 'initial' instead.; "
                    + "'custom': The connector loads a custom class  to specify how the connector performs snapshots. For more information, see Custom snapshotter SPI in the PostgreSQL connector documentation.");

    /**
     * The set of predefined Snapshotter options or aliases.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /**
         * Always perform a snapshot when starting.
         */
        ALWAYS("always"),

        /**
         * Perform a snapshot only upon initial startup of a connector.
         */
        INITIAL("initial"),

        /**
         * Never perform a snapshot and only receive logical changes.
         */
        NO_DATA("no_data"),

        /**
         * Perform a snapshot and then stop before attempting to receive any logical changes.
         */
        INITIAL_ONLY("initial_only"),

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed"),

        /**
         * Allows control over snapshots by setting connectors properties prefixed with 'snapshot.mode.configuration.based'.
         */
        CONFIGURATION_BASED("configuration_based"),

        /**
         * Inject a custom snapshotter, which allows for more control over snapshots.
         */
        CUSTOM("custom");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;

        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    @Override
    // used by the snapshot to limit the additional tables for a change in configuration
    public RelationalTableFilters getTableFilters() {
        if (tableFilters == null) {
            return super.getTableFilters();
        }
        return tableFilters;
    }

    @Override
    public JdbcConfiguration getJdbcConfig() {
        JdbcConfiguration dbConfig = super.getJdbcConfig();
        JdbcConfiguration.Builder dbFromConfig = JdbcConfiguration.create();
        dbFromConfig.with("secure", Boolean.toString(isSecure()));
        int fromCcsid = getFromCcsid();
        if (fromCcsid != -1) {
            dbFromConfig.with("from.ccsid", Integer.toString(fromCcsid));
        }
        int toCcsid = getToCcsid();
        if (toCcsid != -1) {
            dbFromConfig.with("to.ccsid", Integer.toString(toCcsid));
        }

        Configuration driverConfig = this.config.subset(CommonConnectorConfig.DRIVER_CONFIG_PREFIX, true);
        return JdbcConfiguration.adapt(dbConfig.merge(dbFromConfig.build()).merge(driverConfig));
    }
}
