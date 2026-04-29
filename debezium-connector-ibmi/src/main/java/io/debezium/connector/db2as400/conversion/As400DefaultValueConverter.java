/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.db2as400.conversion;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.Immutable;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.util.Collect;

/* @See MySqlDefaultValueConverter
 *
 * based on work by @author Jiri Pechanec
 */
@Immutable
public class As400DefaultValueConverter implements DefaultValueConverter {

    private static final Logger log = LoggerFactory.getLogger(As400DefaultValueConverter.class);

    @Immutable
    private static final Set<Integer> TRIM_DATA_TYPES = Collect.unmodifiableSet(Types.TINYINT, Types.INTEGER,
            Types.DATE, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE, Types.TIME, Types.BOOLEAN, Types.BIT,
            Types.NUMERIC, Types.DECIMAL, Types.FLOAT, Types.DOUBLE, Types.REAL);

    public As400DefaultValueConverter() {
    }

    /**
     * This interface is used by a DDL parser to convert the string default value to
     * a Java type recognized by value converters for a subset of types.
     *
     * @param column                 the column definition describing the
     *                               {@code data} value; never null
     * @param defaultValueExpression the default value literal; may be null
     * @return value converted to a Java type; optional
     */
    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValueExpression) {
        try {
            Object logicalDefaultValue = convert(column, defaultValueExpression);
            if (logicalDefaultValue == null) {
                return Optional.empty();
            }
            return Optional.of(logicalDefaultValue);
        }
        catch (Exception e) {
            log.error("default conversion failed, please report", e);
            return Optional.empty();
        }
    }

    /**
     * Converts a default value from the expected format to a logical object
     * acceptable by the main JDBC converter.
     *
     * @param column column definition
     * @param value  string formatted default value
     * @return value converted to a Java type
     */
    public Object convert(Column column, String value) {
        if (value == null || "NULL".equals(value)) {
            return null;
        }

        // trim non varchar data types before converting
        if (TRIM_DATA_TYPES.contains(column.jdbcType())) {
            value = value.trim();
        }

        // boolean is also INT(1) or TINYINT(1)
        if ("TINYINT".equals(column.typeName()) || "INT".equals(column.typeName())) {
            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                return convertToBoolean(value);
            }
        }
        switch (column.jdbcType()) {
            case Types.DATE: {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                try {
                    return (LocalDate.parse(stripQuotes(value), formatter).toEpochDay());
                }
                catch (DateTimeParseException e) {
                    log.debug("Failed to parse date default value: {}", value);
                    return null;
                }
            }
            case Types.TIMESTAMP: {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH.mm.ss.SSSSSS");
                try {
                    return (LocalDateTime.parse(stripQuotes(value), formatter));
                }
                catch (DateTimeParseException e) {
                    log.debug("Failed to parse timestamp default value: {}", value);
                    return null;
                }
            }
            case Types.TIME:
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH.mm.ss");
                    return LocalTime.parse(stripQuotes(value), formatter);
                }
                catch (DateTimeParseException e) {
                    log.debug("Failed to parse time default value: {}", value);
                    return null;
                }
            case Types.BOOLEAN:
                return convertToBoolean(value);
            case Types.BIT:
                return convertToBits(column, value);

            case Types.BIGINT:
            case Types.NUMERIC:
            case Types.DECIMAL:
                try {
                    return convertToDecimal(column, value);
                }
                catch (NumberFormatException e) {
                    log.debug("Failed to parse decimal default value: {}", value);
                    return null;
                }

            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.REAL:
                try {
                    return convertToDouble(value);
                }
                catch (NumberFormatException e) {
                    log.debug("Failed to parse double default value: {}", value);
                    return null;
                }
            case Types.VARCHAR:
            case Types.CHAR:
            case Types.NVARCHAR:
            case Types.NCHAR:
                // no quotes implies this is a special register
                if (!value.contains("'")) {
                    return null;
                }
                return stripQuotes(value);
            case Types.INTEGER:
            case Types.SMALLINT:
                try {
                    return Integer.parseInt(value);
                }
                catch (NumberFormatException e) {
                    log.debug("Failed to parse integer default value: {}", value);
                    return null;
                }
        }
        return null;
    }

    private String stripQuotes(String value) {
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#DOUBLE}.
     *
     * @param value the string object to be converted into a {@link Types#DOUBLE}
     *              type;
     * @return the converted value;
     */
    private Object convertToDouble(String value) {
        return Double.parseDouble(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#DECIMAL}.
     *
     * @param column the column definition describing the {@code data} value; never
     *               null
     * @param value  the string object to be converted into a {@link Types#DECIMAL}
     *               type;
     * @return the converted value;
     */
    private Object convertToDecimal(Column column, String value) {
        return column.scale().isPresent() ? new BigDecimal(value).setScale(column.scale().get(), RoundingMode.HALF_UP)
                : new BigDecimal(value);
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#BIT}.
     *
     * @param column the column definition describing the {@code data} value; never
     *               null
     * @param value  the string object to be converted into a {@link Types#BIT}
     *               type;
     * @return the converted value;
     */
    private Object convertToBits(Column column, String value) {
        if (column.length() > 1) {
            return convertToBits(value);
        }
        return convertToBit(value);
    }

    private Object convertToBit(String value) {
        try {
            return Short.parseShort(value) != 0;
        }
        catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
    }

    private Object convertToBits(String value) {
        int nums = value.length() / Byte.SIZE + (value.length() % Byte.SIZE == 0 ? 0 : 1);
        byte[] bytes = new byte[nums];
        for (int i = 0; i < nums; i++) {
            int s = value.length() - Byte.SIZE < 0 ? 0 : value.length() - Byte.SIZE;
            int e = value.length();
            bytes[nums - i - 1] = (byte) Integer.parseInt(value.substring(s, e), 2);
            value = value.substring(0, s);
        }
        return bytes;
    }

    /**
     * Converts a string object for an expected JDBC type of {@link Types#BOOLEAN}.
     *
     * @param value the string object to be converted into a {@link Types#BOOLEAN}
     *              type;
     *
     * @return the converted value;
     */
    private Object convertToBoolean(String value) {
        try {
            return Integer.parseInt(value) != 0;
        }
        catch (NumberFormatException ignore) {
            return Boolean.parseBoolean(value);
        }
    }

}
