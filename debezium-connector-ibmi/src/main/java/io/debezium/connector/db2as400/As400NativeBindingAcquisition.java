/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import io.debezium.connector.db2as400.As400BindingDefinition.Codec;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorTableEvidence.Request;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Bounded native physical-layout acquisition. No fixture schema, implicit CCSID,
 * JDBC type-name guess or current-catalog fallback during recovery. Callers must
 * hold the initialization writer/DDL fence; equal observations are not that fence.
 */
final class As400NativeBindingAcquisition {
    static final String CONTROLS_SQL = """
            SELECT TABLE_SCHEMA, TABLE_NAME, SYSTEM_TABLE_SCHEMA, SYSTEM_TABLE_NAME, CONTROL
            FROM QSYS2.SYSTABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            """;
    static final String COLUMNS_SQL = """
            SELECT TABLE_SCHEMA, TABLE_NAME, SYSTEM_TABLE_SCHEMA, SYSTEM_TABLE_NAME,
                   COLUMN_NAME, SYSTEM_COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, LENGTH,
                   NUMERIC_SCALE, CCSID, IS_NULLABLE, IS_IDENTITY, HIDDEN, HAS_FLDPROC,
                   BUFFER_POSITION, BUFFER_LENGTH, STORAGE, CHARACTER_OCTET_LENGTH,
                   DATETIME_PRECISION, DATE_FORMAT, DATE_SEPARATOR, TIME_FORMAT, TIME_SEPARATOR
            FROM QSYS2.SYSCOLUMNS2 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION
            """;
    static final String KEYS_SQL = """
            SELECT K.COLUMN_NAME, K.ORDINAL_POSITION, K.CONSTRAINT_SCHEMA, K.CONSTRAINT_NAME
            FROM QSYS2.SYSKEYCST K JOIN QSYS2.SYSCST C
              ON K.CONSTRAINT_SCHEMA = C.CONSTRAINT_SCHEMA AND K.CONSTRAINT_NAME = C.CONSTRAINT_NAME
            WHERE C.TABLE_SCHEMA = ? AND C.TABLE_NAME = ? AND C.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY K.ORDINAL_POSITION
            """;
    // Match the ordinary reader's physical-file key fallback, not an arbitrary
    // SQL index. The catalog's ordinal and physical column mapping are checked
    // before these names can become durable output keys.
    static final String DDS_KEYS_SQL = """
            SELECT C.COLUMN_NAME, K.DBKPOS AS ORDINAL_POSITION
            FROM QSYS.QADBKATR K JOIN QSYS2.SYSCOLUMNS C
              ON C.SYSTEM_TABLE_SCHEMA=K.DBKLIB AND C.SYSTEM_TABLE_NAME=K.DBKFIL AND C.SYSTEM_COLUMN_NAME=K.DBKFLD
            WHERE K.DBKLIB=? AND K.DBKFIL=? ORDER BY K.DBKPOS
            """;

    record Acquired(List<Observation> observations, List<As400BindingDefinition> definitions) {
        Acquired {
            observations = List.copyOf(observations);
            definitions = List.copyOf(definitions);
        }
    }

    /** Same explicit text substitution as the legacy CcsidCache, never a storage-layout change. */
    record CcsidMapping(int from, int to) {
        CcsidMapping {
            if (from < -1 || from > 65535 || to < -1 || to > 65535
                    || from != -1 && to != -1 && from != to && (from == 0 || to == 0 || to == 65535)) {
                throw new IllegalArgumentException("CCSID substitution requires explicit text encodings or disabled (-1) values");
            }
        }

        boolean active() {
            return from != -1 && to != -1 && from != to;
        }

        Codec apply(Codec physical) {
            // Binary and temporal codecs do not consult CcsidCache in the ordinary
            // reader. Preserve that behavior, including CHAR FOR BIT DATA.
            return active() && physical.ccsid() == from && List.of("TEXT", "VARCHAR", "VARGRAPHIC").contains(physical.kind())
                    ? new Codec(physical.kind(), physical.size(), physical.scale(), to)
                    : physical;
        }
    }

    private final As400JdbcConnectionFactory connections;
    private final As400OperatorTableEvidence tables;
    private final As400OperatorTableEvidence.Limits limits;
    private final boolean rrnKeys;
    private final java.util.function.Predicate<io.debezium.relational.TableId> captured;
    private final CcsidMapping ccsids;
    private final boolean retainExcludedSchemas;

    As400NativeBindingAcquisition(As400JdbcConnectionFactory connections, As400OperatorTableEvidence tables,
                                  As400OperatorTableEvidence.Limits limits) {
        this(connections, tables, limits, false);
    }

    As400NativeBindingAcquisition(As400JdbcConnectionFactory connections, As400OperatorTableEvidence tables,
                                  As400OperatorTableEvidence.Limits limits, boolean rrnKeys) {
        this(connections, tables, limits, rrnKeys, table -> true);
    }

    As400NativeBindingAcquisition(As400JdbcConnectionFactory connections, As400OperatorTableEvidence tables,
                                  As400OperatorTableEvidence.Limits limits, boolean rrnKeys,
                                  java.util.function.Predicate<io.debezium.relational.TableId> captured) {
        this(connections, tables, limits, rrnKeys, captured, new CcsidMapping(-1, -1));
    }

    As400NativeBindingAcquisition(As400JdbcConnectionFactory connections, As400OperatorTableEvidence tables,
                                  As400OperatorTableEvidence.Limits limits, boolean rrnKeys,
                                  java.util.function.Predicate<io.debezium.relational.TableId> captured, CcsidMapping ccsids) {
        this(connections, tables, limits, rrnKeys, captured, ccsids, false);
    }

    As400NativeBindingAcquisition(As400JdbcConnectionFactory connections, As400OperatorTableEvidence tables,
                                  As400OperatorTableEvidence.Limits limits, boolean rrnKeys,
                                  java.util.function.Predicate<io.debezium.relational.TableId> captured, CcsidMapping ccsids, boolean retainExcludedSchemas) {
        this.connections = Objects.requireNonNull(connections);
        this.tables = Objects.requireNonNull(tables);
        this.limits = Objects.requireNonNull(limits);
        this.rrnKeys = rrnKeys;
        this.captured = Objects.requireNonNull(captured);
        this.ccsids = Objects.requireNonNull(ccsids);
        this.retainExcludedSchemas = retainExcludedSchemas;
    }

    Acquired collect(List<Request> requested) throws SQLException {
        List<Request> requests = List.copyOf(requested);
        require(!requests.isEmpty() && requests.size() <= limits.maximumTables() && requests.stream().distinct().count() == requests.size(),
                "Distinct bounded native binding requests required");
        var before = tables.collect(requests);
        List<As400BindingDefinition> definitions = new ArrayList<>();
        require(!Thread.currentThread().isInterrupted(), "Native acquisition stopped");
        try (Connection connection = Objects.requireNonNull(connections.open())) {
            connection.setReadOnly(true);
            check(connection);
            for (Observation observation : before) {
                var definition = acquire(connection, observation);
                definitions.add(new As400BindingDefinition(definition.identity(), definition.schema(), definition.codecs(), observation.memberJournalIds()));
            }
            check(connection);
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new As400NativeEvidenceException("Unsupported native physical binding", e);
        }
        require(before.equals(tables.collect(requests)), "Native object/member/format changed during acquisition");
        return new Acquired(before, definitions);
    }

    private As400BindingDefinition acquire(Connection connection, Observation observation) throws SQLException {
        if (captured.test(observation.table())) {
            require(!observation.format().lob(), "LOB value capture is unsupported");
            return read(connection, observation);
        }
        if (observation.format().lob()) {
            // Preserve ordinary table filtering. Only fixed source identity and
            // transaction effects are needed; no LOB address or value is read.
            return As400BindingDefinition.excluded(observation.tableIdentity(), true);
        }
        if (retainExcludedSchemas) {
            try {
                return read(connection, observation);
            }
            catch (SQLException unavailable) {
                if (!(unavailable instanceof As400NativeEvidenceException) && !"42501".equals(unavailable.getSQLState())) {
                    throw unavailable;
                }
                check(connection);
            }
            catch (IllegalArgumentException | ArithmeticException unavailable) {
                // An unsupported excluded layout must not narrow capture of other
                // tables. Keep its identity only; later inclusion still requires
                // a complete historical schema, never a current-catalog guess.
                check(connection);
            }
        }
        return As400BindingDefinition.excluded(observation.tableIdentity());
    }

    private As400BindingDefinition read(Connection connection, Observation observation) throws SQLException {
        // Snapshot SQL obeys RCAC while journal images do not. A masked or
        // filtered baseline cannot be followed by unfiltered native row images.
        // SYSTABLES.CONTROL: blank = none, R/C/B = active enforcement.
        // https://www.ibm.com/docs/en/i/7.6.0?topic=views-systables
        try (PreparedStatement statement = prepare(connection, CONTROLS_SQL, observation, 2);
                ResultSet rows = statement.executeQuery()) {
            check(statement, rows);
            require(rows.next(), "Missing table access-control evidence");
            for (String field : List.of("TABLE_SCHEMA", "SYSTEM_TABLE_SCHEMA")) {
                require(text(rows, field).equals(observation.request().library()), "Ambiguous access-control schema identity");
            }
            require(text(rows, "TABLE_NAME").equals(observation.table().table())
                    && text(rows, "SYSTEM_TABLE_NAME").equals(observation.request().file()), "Ambiguous access-control table identity");
            require(" ".equals(rows.getString("CONTROL")), "Native capture does not support active or unknown row/column access controls");
            require(!rows.next(), "Ambiguous table access-control evidence");
            check(statement, rows);
        }
        var schema = Table.editor().tableId(observation.table());
        List<Codec> codecs = new ArrayList<>();
        var names = new HashSet<String>();
        var systemNames = new HashSet<String>();
        int position = 1;
        try (PreparedStatement statement = prepare(connection, COLUMNS_SQL, observation, observation.format().fields() + 1);
                ResultSet rows = statement.executeQuery()) {
            check(statement, rows);
            while (rows.next()) {
                check(connection);
                check(statement, rows);
                require(codecs.size() < observation.format().fields(), "Extra physical columns");
                for (String field : List.of("TABLE_SCHEMA", "SYSTEM_TABLE_SCHEMA")) {
                    require(text(rows, field).equals(observation.request().library()), "Ambiguous native schema mapping");
                }
                require(text(rows, "TABLE_NAME").equals(observation.table().table())
                        && text(rows, "SYSTEM_TABLE_NAME").equals(observation.request().file()), "Ambiguous native table mapping");
                String name = text(rows, "COLUMN_NAME");
                String system = text(rows, "SYSTEM_COLUMN_NAME");
                require(!name.isBlank() && name.length() <= 128 && names.add(name)
                        && system.matches("[A-Z0-9_#$@]{1,10}") && systemNames.add(system), "Duplicate or invalid physical column name");
                require(integer(rows, "ORDINAL_POSITION") == codecs.size() + 1 && integer(rows, "BUFFER_POSITION") == position,
                        "Missing, reordered or overlapping physical fields");
                require(text(rows, "HIDDEN").equals("N") && text(rows, "HAS_FLDPROC").equals("N"), "Hidden or transformed physical field unsupported");
                String nullable = text(rows, "IS_NULLABLE");
                String identity = text(rows, "IS_IDENTITY");
                require(List.of("Y", "N").contains(nullable) && List.of("YES", "NO").contains(identity), "Unknown column flags");
                String type = text(rows, "DATA_TYPE");
                int length = integer(rows, "LENGTH");
                Integer scale = optionalInteger(rows, "NUMERIC_SCALE");
                Integer ccsid = optionalInteger(rows, "CCSID");
                Integer octets = optionalInteger(rows, "CHARACTER_OCTET_LENGTH");
                boolean temporal = List.of("DATE", "TIME", "TIMESTMP").contains(type);
                Codec codec = temporal ? temporal(rows, type, length, scale, ccsid, octets) : codec(type, length, scale, ccsid, octets);
                int bytes = codec.decoder().getByteLength();
                require(integer(rows, "BUFFER_LENGTH") == bytes && integer(rows, "STORAGE") == (temporal ? length : bytes),
                        "Native buffer and decoder lengths disagree");
                int jdbcType = (type.equals("CHAR") || type.equals("VARCHAR")) && Integer.valueOf(65535).equals(ccsid)
                        ? (type.equals("CHAR") ? Types.BINARY : Types.VARBINARY)
                        : jdbcType(type, length);
                var column = Column.editor().name(name).position(codecs.size() + 1).jdbcType(jdbcType).type(type)
                        .length(temporal ? bytes : length).optional(nullable.equals("Y")).autoIncremented(identity.equals("YES"));
                if (temporal || scale != null) {
                    column.scale(temporal ? codec.scale() : scale);
                }
                schema.addColumn(column.create());
                codecs.add(ccsids.apply(codec));
                position = Math.addExact(position, bytes);
            }
            check(statement, rows);
        }
        require(codecs.size() == observation.format().fields() && position - 1 == observation.format().recordBytes(), "Incomplete native row layout");
        List<String> keys = new ArrayList<>();
        String constraint = null;
        try (PreparedStatement statement = prepare(connection, KEYS_SQL, observation, observation.format().fields() + 1);
                ResultSet rows = statement.executeQuery()) {
            check(statement, rows);
            while (rows.next()) {
                check(connection);
                check(statement, rows);
                require(keys.size() < codecs.size() && integer(rows, "ORDINAL_POSITION") == keys.size() + 1, "Incomplete or excessive primary key");
                String name = text(rows, "COLUMN_NAME");
                String current = text(rows, "CONSTRAINT_SCHEMA") + "/" + text(rows, "CONSTRAINT_NAME");
                require(names.contains(name) && !keys.contains(name) && (constraint == null || constraint.equals(current)), "Ambiguous native primary key");
                constraint = current;
                keys.add(name);
            }
            check(statement, rows);
        }
        if (keys.isEmpty()) {
            try (var statement = prepare(connection, DDS_KEYS_SQL, observation.request().library(), observation.request().file(), observation.format().fields() + 1);
                    var rows = statement.executeQuery()) {
                check(statement, rows);
                while (rows.next()) {
                    check(connection);
                    check(statement, rows);
                    String name = text(rows, "COLUMN_NAME");
                    require(keys.size() < codecs.size() && integer(rows, "ORDINAL_POSITION") == keys.size() + 1
                            && names.contains(name) && !keys.contains(name), "Incomplete or ambiguous physical-file keys");
                    keys.add(name);
                }
                check(statement, rows);
            }
        }
        // Empty, completely read SQL and physical key catalogs prove a keyless table. Preserve
        // Debezium's null-key contract; never invent an RRN key during migration.
        schema.setPrimaryKeyNames(keys);
        if (rrnKeys) {
            schema.addAttribute(io.debezium.relational.Attribute.editor().name(As400BindingDefinition.KEY_MODE).value("rrn").create());
        }
        Table table = outputSchema(connection, schema.create());
        require(constraint == null || keys.stream().allMatch(k -> !table.columnWithName(k).isOptional()), "Nullable SQL primary key");
        return new As400BindingDefinition(observation.tableIdentity(), table, codecs);
    }

    /**
     * Physical storage length/type names are not JDBC output precision/type names.
     * Obtain the same descriptors as the ordinary reader, on the qualified session
     * and inside the caller's schema fence. Recovery uses this persisted table;
     * it never reinterprets an old generation using current catalog metadata.
     */
    private Table outputSchema(Connection connection, Table physical) throws SQLException {
        check(connection);
        var metadata = connection.getMetaData();
        String escape = metadata.getSearchStringEscape();
        require(escape != null && escape.length() == 1, "Exact JDBC metadata search escape required");
        var table = physical.edit();
        int index = 0;
        // getColumns has no Statement/query-timeout argument. The native session
        // supplies the socket deadline and terminal-close guard; exact patterns
        // and the independently proven field count bound the returned inventory.
        try (var rows = metadata.getColumns(null, pattern(physical.id().schema(), escape), pattern(physical.id().table(), escape), null)) {
            warning(rows.getWarnings());
            while (rows.next()) {
                check(connection);
                warning(rows.getWarnings());
                require(index < physical.columns().size(), "Excess JDBC output columns");
                var expected = physical.columns().get(index++);
                require(physical.id().schema().equals(text(rows, "TABLE_SCHEM")) && physical.id().table().equals(text(rows, "TABLE_NAME")),
                        "JDBC output table identity mismatch");
                require(expected.name().equals(rows.getString("COLUMN_NAME")) && integer(rows, "ORDINAL_POSITION") == expected.position(),
                        "Incomplete or reordered JDBC output columns");
                int nullable = integer(rows, "NULLABLE");
                String identity = text(rows, "IS_AUTOINCREMENT");
                String generated = text(rows, "IS_GENERATEDCOLUMN");
                require((nullable == 0 || nullable == 1) && List.of("YES", "NO").contains(identity) && List.of("YES", "NO").contains(generated),
                        "Unknown JDBC output column flags");
                int jdbcType = integer(rows, "DATA_TYPE");
                require(jdbcType == expected.jdbcType() && (nullable == 1) == expected.isOptional()
                        && identity.equals("YES") == expected.isAutoIncremented(), "JDBC output and physical column disagree");
                String type = text(rows, "TYPE_NAME");
                int length = integer(rows, "COLUMN_SIZE");
                require(!type.isBlank() && type.length() <= 128 && length > 0 && length <= 65535, "Incomplete JDBC output type descriptor");
                // Use the ordinary reader's descriptor conversion, without its
                // mutable cache or extra physical-codec catalog queries. The
                // complete native-layout checks above still gate this mapping.
                var descriptor = JdbcFileDecoder.readColumnMetadata(rows);
                require(descriptor.getPrecision() >= 0 && descriptor.getPrecision() <= length, "Invalid JDBC output scale");
                var column = Column.editor().name(descriptor.getName()).position(descriptor.getPosition()).jdbcType(descriptor.getJdcbType())
                        .type(descriptor.getType()).length(descriptor.getLength()).scale(descriptor.getPrecision())
                        .optional(descriptor.isOptional()).autoIncremented(descriptor.isAutoinc()).generated(generated.equals("YES"));
                // As400JdbcConnection deliberately omits defaults. Preserve that
                // contract, including explicit zero scales on character columns.
                table.addColumn(column.create());
            }
            require(index == physical.columns().size(), "Missing JDBC output columns");
            check(connection);
            warning(rows.getWarnings());
        }
        return table.create();
    }

    private static String pattern(String name, String escape) {
        return name.replace(escape, escape + escape).replace("%", escape + "%").replace("_", escape + "_");
    }

    static Codec codec(String type, int length, Integer scale, Integer ccsid, Integer octets) throws SQLException {
        boolean graphic = type.equals("GRAPHIC") || type.equals("VARG");
        boolean string = graphic || List.of("CHAR", "VARCHAR", "BINARY", "VARBIN").contains(type);
        // Catalog LENGTH is in double-byte units for graphic strings and bytes
        // for character/binary strings. BUFFER_LENGTH independently proves the
        // physical span (including the varying prefix) before a binding is saved.
        // https://www.ibm.com/docs/en/i/7.5.0?topic=views-syscolumns2
        require(string ? octets != null && octets == (long) length * (graphic ? 2 : 1) && ccsid != null && scale == null
                : octets == null && ccsid == null,
                "Unsupported native byte, precision or CCSID attributes");
        if (!type.equals("DECIMAL") && !type.equals("NUMERIC")) {
            require(scale == null || scale == 0, "Unexpected native numeric scale");
        }
        return switch (type) {
            case "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "BOOLEAN" -> {
                String kind = switch (type) {
                    case "SMALLINT" -> "BIN2";
                    case "INTEGER" -> "BIN4";
                    case "BIGINT" -> "BIN8";
                    case "BOOLEAN" -> "BOOLEAN";
                    default -> length == 4 ? "FLOAT4" : "FLOAT8";
                };
                yield new Codec(kind, length, 0, 0);
            }
            case "DECIMAL", "NUMERIC" -> {
                require(scale != null, "Native decimal scale missing");
                yield new Codec(type.equals("DECIMAL") ? "PACKED" : "ZONED", length, scale, 0);
            }
            case "CHAR", "VARCHAR" -> ccsid == 65535
                    ? new Codec(type.equals("CHAR") ? "BINARY" : "VARBINARY", length, 0, 0)
                    : new Codec(type.equals("CHAR") ? "TEXT" : "VARCHAR", length, 0, ccsid);
            case "GRAPHIC" -> new Codec("TEXT", octets, 0, ccsid);
            case "VARG" -> new Codec("VARGRAPHIC", length, 0, ccsid);
            case "BINARY", "VARBIN" -> {
                require(ccsid == 65535, "Binary field has unexpected CCSID");
                yield new Codec(type.equals("BINARY") ? "BINARY" : "VARBINARY", length, 0, 0);
            }
            default -> throw new As400NativeEvidenceException("Unsupported native column type " + type);
        };
    }

    private static Codec temporal(ResultSet rows, String type, int length, Integer scale, Integer ccsid, Integer octets) throws SQLException {
        int precision = integer(rows, "DATETIME_PRECISION");
        require(scale == null && octets == null && ccsid != null, "Incomplete native temporal attributes");
        String dateFormat = rows.getString("DATE_FORMAT");
        String timeFormat = rows.getString("TIME_FORMAT");
        String dateSeparator = rows.getString("DATE_SEPARATOR");
        String timeSeparator = rows.getString("TIME_SEPARATOR");
        // SQL ISO columns report no override separator. Admit only the exact
        // native formats captured and verified against complete journal rows.
        require(dateSeparator == null && timeSeparator == null, "Temporal separator override unsupported");
        return switch (type) {
            case "DATE" -> {
                require(length == 4 && precision == 0 && "ISO".equals(dateFormat) && timeFormat == null, "Unsupported native date format");
                yield new Codec("DATE_ISO", 10, 0, ccsid);
            }
            case "TIME" -> {
                require(length == 3 && precision == 0 && "ISO".equals(timeFormat) && dateFormat == null, "Unsupported native time format");
                yield new Codec("TIME_ISO", 8, 0, ccsid);
            }
            default -> {
                require(precision >= 0 && precision <= 9 && length == (precision + 1) / 2 + 7 && dateFormat == null && timeFormat == null,
                        "Unsupported native timestamp precision/layout");
                yield new Codec("TIMESTAMP_ISO", precision == 0 ? 19 : 20 + precision, precision, ccsid);
            }
        };
    }

    private static int jdbcType(String type, int length) throws SQLException {
        return switch (type) {
            case "SMALLINT" -> Types.SMALLINT;
            case "INTEGER" -> Types.INTEGER;
            case "BIGINT" -> Types.BIGINT;
            case "FLOAT" -> length == 4 ? Types.REAL : Types.DOUBLE;
            case "BOOLEAN" -> Types.BOOLEAN;
            case "DECIMAL" -> Types.DECIMAL;
            case "NUMERIC" -> Types.NUMERIC;
            case "CHAR" -> Types.CHAR;
            case "VARCHAR" -> Types.VARCHAR;
            case "GRAPHIC" -> Types.NCHAR;
            case "VARG" -> Types.NVARCHAR;
            case "BINARY" -> Types.BINARY;
            case "VARBIN" -> Types.VARBINARY;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "TIMESTMP" -> Types.TIMESTAMP;
            default -> throw new As400NativeEvidenceException("Unsupported native column type " + type);
        };
    }

    private PreparedStatement prepare(Connection connection, String sql, Observation table, int maximum) throws SQLException {
        return prepare(connection, sql, table.table().schema(), table.table().table(), maximum);
    }

    private PreparedStatement prepare(Connection connection, String sql, String library, String table, int maximum) throws SQLException {
        check(connection);
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout(limits.queryTimeoutSeconds());
            statement.setMaxRows(maximum);
            statement.setString(1, library);
            statement.setString(2, table);
            warning(statement.getWarnings());
            return statement;
        }
        catch (Exception e) {
            statement.close();
            throw e;
        }
    }

    private static int integer(ResultSet rows, String field) throws SQLException {
        Integer value = optionalInteger(rows, field);
        require(value != null, "Missing native numeric field " + field);
        return value;
    }

    private static Integer optionalInteger(ResultSet rows, String field) throws SQLException {
        BigDecimal value = rows.getBigDecimal(field);
        return value == null ? null : value.intValueExact();
    }

    private static String text(ResultSet rows, String field) throws SQLException {
        String value = rows.getString(field);
        require(value != null, "Missing native field " + field);
        return value.stripTrailing();
    }

    private static void check(Connection connection) throws SQLException {
        require(!Thread.currentThread().isInterrupted() && !connection.isClosed() && connection.isReadOnly(), "Native acquisition stopped or connection not read-only");
        warning(connection.getWarnings());
    }

    private static void check(PreparedStatement statement, ResultSet rows) throws SQLException {
        warning(statement.getWarnings());
        warning(rows.getWarnings());
    }

    private static void warning(SQLWarning warning) throws SQLException {
        if (warning != null) {
            throw new As400NativeEvidenceException("Partial native binding evidence rejected", warning);
        }
    }

    private static void require(boolean condition, String message) throws SQLException {
        if (!condition) {
            throw new As400NativeEvidenceException(message);
        }
    }
}
