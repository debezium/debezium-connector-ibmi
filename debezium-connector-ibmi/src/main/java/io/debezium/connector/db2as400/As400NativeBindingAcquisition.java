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
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Bounded native physical-layout acquisition. No fixture schema, implicit CCSID,
 * JDBC type-name guess or current-catalog fallback during recovery. Callers must
 * hold the initialization writer/DDL fence; equal observations are not that fence.
 */
final class As400NativeBindingAcquisition {
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

    record Acquired(List<Observation> observations, List<As400BindingDefinition> definitions) {
        Acquired {
            observations = List.copyOf(observations);
            definitions = List.copyOf(definitions);
        }
    }

    private final As400JdbcConnectionFactory connections;
    private final As400OperatorTableEvidence tables;
    private final As400OperatorTableEvidence.Limits limits;
    private final boolean rrnKeys;

    As400NativeBindingAcquisition(As400JdbcConnectionFactory connections, As400OperatorTableEvidence tables,
                                  As400OperatorTableEvidence.Limits limits) {
        this(connections, tables, limits, false);
    }

    As400NativeBindingAcquisition(As400JdbcConnectionFactory connections, As400OperatorTableEvidence tables,
                                  As400OperatorTableEvidence.Limits limits, boolean rrnKeys) {
        this.connections = Objects.requireNonNull(connections);
        this.tables = Objects.requireNonNull(tables);
        this.limits = Objects.requireNonNull(limits);
        this.rrnKeys = rrnKeys;
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
                definitions.add(read(connection, observation));
            }
            check(connection);
        }
        catch (IllegalArgumentException | ArithmeticException e) {
            throw new SQLException("Unsupported native physical binding", e);
        }
        require(before.equals(tables.collect(requests)), "Native object/member/format changed during acquisition");
        return new Acquired(before, definitions);
    }

    private As400BindingDefinition read(Connection connection, Observation observation) throws SQLException {
        var schema = Table.editor().tableId(observation.table());
        List<Codec> codecs = new ArrayList<>();
        var names = new HashSet<String>();
        var systemNames = new HashSet<String>();
        int position = 1;
        try (PreparedStatement statement = prepare(connection, COLUMNS_SQL, observation, 4097);
                ResultSet rows = statement.executeQuery()) {
            check(statement, rows);
            while (rows.next()) {
                check(connection);
                check(statement, rows);
                require(codecs.size() < observation.format().fields(), "Extra physical columns");
                for (String field : List.of("TABLE_SCHEMA", "SYSTEM_TABLE_SCHEMA")) {
                    require(text(rows, field).equals(observation.request().library()), "Ambiguous native schema mapping");
                }
                for (String field : List.of("TABLE_NAME", "SYSTEM_TABLE_NAME")) {
                    require(text(rows, field).equals(observation.request().file()), "Ambiguous native table mapping");
                }
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
                int jdbcType = jdbcType(type, length);
                var column = Column.editor().name(name).position(codecs.size() + 1).jdbcType(jdbcType).type(type)
                        .length(temporal ? bytes : length).optional(nullable.equals("Y")).autoIncremented(identity.equals("YES"));
                if (temporal || scale != null) {
                    column.scale(temporal ? codec.scale() : scale);
                }
                schema.addColumn(column.create());
                codecs.add(codec);
                position = Math.addExact(position, bytes);
            }
            check(statement, rows);
        }
        require(codecs.size() == observation.format().fields() && position - 1 == observation.format().recordBytes(), "Incomplete native row layout");
        List<String> keys = new ArrayList<>();
        String constraint = null;
        try (PreparedStatement statement = prepare(connection, KEYS_SQL, observation, 4097);
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
        require(rrnKeys || !keys.isEmpty(), "Native initialization requires a primary key or explicit rrn key mode");
        schema.setPrimaryKeyNames(keys);
        if (rrnKeys) {
            schema.addAttribute(io.debezium.relational.Attribute.editor().name(As400BindingDefinition.KEY_MODE).value("rrn").create());
        }
        Table table = schema.create();
        require(keys.stream().allMatch(k -> !table.columnWithName(k).isOptional()), "Nullable primary key");
        return new As400BindingDefinition(observation.tableIdentity(), table, codecs);
    }

    static Codec codec(String type, int length, Integer scale, Integer ccsid, Integer octets) throws SQLException {
        boolean string = List.of("CHAR", "VARCHAR", "BINARY", "VARBIN").contains(type);
        require(string ? octets != null && octets == length && ccsid != null && scale == null : octets == null && ccsid == null,
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
            case "CHAR", "VARCHAR" -> new Codec(type.equals("CHAR") ? "TEXT" : "VARCHAR", length, 0, ccsid);
            case "BINARY", "VARBIN" -> {
                require(ccsid == 65535, "Binary field has unexpected CCSID");
                yield new Codec(type.equals("BINARY") ? "BINARY" : "VARBINARY", length, 0, 0);
            }
            default -> throw new SQLException("Unsupported native column type " + type);
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
            case "BINARY" -> Types.BINARY;
            case "VARBIN" -> Types.VARBINARY;
            case "DATE" -> Types.DATE;
            case "TIME" -> Types.TIME;
            case "TIMESTMP" -> Types.TIMESTAMP;
            default -> throw new SQLException("Unsupported native column type " + type);
        };
    }

    private PreparedStatement prepare(Connection connection, String sql, Observation table, int maximum) throws SQLException {
        check(connection);
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout(limits.queryTimeoutSeconds());
            statement.setMaxRows(maximum);
            statement.setString(1, table.request().library());
            statement.setString(2, table.request().file());
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
        require(!Thread.currentThread().isInterrupted() && connection.isReadOnly(), "Native acquisition stopped or connection not read-only");
        warning(connection.getWarnings());
    }

    private static void check(PreparedStatement statement, ResultSet rows) throws SQLException {
        warning(statement.getWarnings());
        warning(rows.getWarnings());
    }

    private static void warning(SQLWarning warning) throws SQLException {
        if (warning != null) {
            throw new SQLException("Partial native binding evidence rejected", warning);
        }
    }

    private static void require(boolean condition, String message) throws SQLException {
        if (!condition) {
            throw new SQLException(message);
        }
    }
}
