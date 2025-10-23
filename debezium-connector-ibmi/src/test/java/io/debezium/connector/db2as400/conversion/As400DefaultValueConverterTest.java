/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.conversion;

import static org.fest.assertions.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import io.debezium.relational.Column;

public class As400DefaultValueConverterTest {

    private As400DefaultValueConverter converter;

    @Before
    public void setUp() {
        converter = new As400DefaultValueConverter();
    }

    @Test
    public void testConvertDate() {
        Column column = Column.editor()
                .name("date_col")
                .type("DATE")
                .jdbcType(Types.DATE)
                .create();

        Object result = converter.convert(column, "'2023-10-15'");
        assertThat(result).isEqualTo(LocalDate.of(2023, 10, 15).toEpochDay());
    }

    @Test
    public void testConvertDateWithoutQuotes() {
        Column column = Column.editor()
                .name("date_col")
                .type("DATE")
                .jdbcType(Types.DATE)
                .create();

        Object result = converter.convert(column, "2023-10-15");
        assertThat(result).isEqualTo(LocalDate.of(2023, 10, 15).toEpochDay());
    }

    @Test
    public void testConvertInvalidDate() {
        Column column = Column.editor()
                .name("date_col")
                .type("DATE")
                .jdbcType(Types.DATE)
                .create();

        Object result = converter.convert(column, "invalid-date");
        assertThat(result).isNull();
    }

    @Test
    public void testConvertTimestamp() {
        Column column = Column.editor()
                .name("timestamp_col")
                .type("TIMESTAMP")
                .jdbcType(Types.TIMESTAMP)
                .create();

        Object result = converter.convert(column, "'2023-10-15-14.30.45.123456'");
        LocalDateTime expected = LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123456000);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testConvertInvalidTimestamp() {
        Column column = Column.editor()
                .name("timestamp_col")
                .type("TIMESTAMP")
                .jdbcType(Types.TIMESTAMP)
                .create();

        Object result = converter.convert(column, "invalid-timestamp");
        assertThat(result).isNull();
    }

    @Test
    public void testConvertTime() {
        Column column = Column.editor()
                .name("time_col")
                .type("TIME")
                .jdbcType(Types.TIME)
                .create();

        Object result = converter.convert(column, "'14.30.45'");
        LocalTime expected = LocalTime.of(14, 30, 45);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testConvertTimeWithoutQuotes() {
        Column column = Column.editor()
                .name("time_col")
                .type("TIME")
                .jdbcType(Types.TIME)
                .create();

        Object result = converter.convert(column, "14.30.45");
        LocalTime expected = LocalTime.of(14, 30, 45);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void testConvertInvalidTime() {
        Column column = Column.editor()
                .name("time_col")
                .type("TIME")
                .jdbcType(Types.TIME)
                .create();

        Object result = converter.convert(column, "invalid-time");
        assertThat(result).isNull();
    }

    @Test
    public void testConvertBoolean() {
        Column column = Column.editor()
                .name("bool_col")
                .type("BOOLEAN")
                .jdbcType(Types.BOOLEAN)
                .create();

        assertThat(converter.convert(column, "1")).isEqualTo(true);
        assertThat(converter.convert(column, "0")).isEqualTo(false);
        assertThat(converter.convert(column, "true")).isEqualTo(true);
        assertThat(converter.convert(column, "false")).isEqualTo(false);
    }

    @Test
    public void testConvertInteger() {
        Column column = Column.editor()
                .name("int_col")
                .type("INTEGER")
                .jdbcType(Types.INTEGER)
                .create();

        Object result = converter.convert(column, "42");
        assertThat(result).isEqualTo(42);
    }

    @Test
    public void testConvertInvalidInteger() {
        Column column = Column.editor()
                .name("int_col")
                .type("INTEGER")
                .jdbcType(Types.INTEGER)
                .create();

        Object result = converter.convert(column, "not-a-number");
        assertThat(result).isNull();
    }

    @Test
    public void testConvertSmallInt() {
        Column column = Column.editor()
                .name("smallint_col")
                .type("SMALLINT")
                .jdbcType(Types.SMALLINT)
                .create();

        Object result = converter.convert(column, "123");
        assertThat(result).isEqualTo(123);
    }

    @Test
    public void testConvertDecimal() {
        Column column = Column.editor()
                .name("decimal_col")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .scale(2)
                .create();

        Object result = converter.convert(column, "123.456");
        assertThat(result).isEqualTo(new BigDecimal("123.46"));
    }

    @Test
    public void testConvertDecimalWithoutScale() {
        Column column = Column.editor()
                .name("decimal_col")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .create();

        Object result = converter.convert(column, "123.456");
        assertThat(result).isEqualTo(new BigDecimal("123.456"));
    }

    @Test
    public void testConvertInvalidDecimal() {
        Column column = Column.editor()
                .name("decimal_col")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .create();

        Object result = converter.convert(column, "not-a-decimal");
        assertThat(result).isNull();
    }

    @Test
    public void testConvertDouble() {
        Column column = Column.editor()
                .name("double_col")
                .type("DOUBLE")
                .jdbcType(Types.DOUBLE)
                .create();

        Object result = converter.convert(column, "123.456");
        assertThat(result).isEqualTo(123.456);
    }

    @Test
    public void testConvertInvalidDouble() {
        Column column = Column.editor()
                .name("double_col")
                .type("DOUBLE")
                .jdbcType(Types.DOUBLE)
                .create();

        Object result = converter.convert(column, "not-a-double");
        assertThat(result).isNull();
    }

    @Test
    public void testConvertVarchar() {
        Column column = Column.editor()
                .name("varchar_col")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .create();

        Object result = converter.convert(column, "'test value'");
        assertThat(result).isEqualTo("test value");
    }

    @Test
    public void testConvertVarcharWithoutQuotes() {
        Column column = Column.editor()
                .name("varchar_col")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .create();

        // No quotes implies special register - should return null
        Object result = converter.convert(column, "CURRENT_USER");
        assertThat(result).isNull();
    }

    @Test
    public void testConvertChar() {
        Column column = Column.editor()
                .name("char_col")
                .type("CHAR")
                .jdbcType(Types.CHAR)
                .create();

        Object result = converter.convert(column, "'A'");
        assertThat(result).isEqualTo("A");
    }

    @Test
    public void testConvertBitSingleBit() {
        Column column = Column.editor()
                .name("bit_col")
                .type("BIT")
                .jdbcType(Types.BIT)
                .length(1)
                .create();

        assertThat(converter.convert(column, "1")).isEqualTo(true);
        assertThat(converter.convert(column, "0")).isEqualTo(false);
    }

    @Test
    public void testConvertBitMultipleBits() {
        Column column = Column.editor()
                .name("bit_col")
                .type("BIT")
                .jdbcType(Types.BIT)
                .length(8)
                .create();

        Object result = converter.convert(column, "10101010");
        assertThat(result).isInstanceOf(byte[].class);
        byte[] bytes = (byte[]) result;
        assertThat(bytes).hasSize(1);
        assertThat(bytes[0]).isEqualTo((byte) 0xAA);
    }

    @Test
    public void testConvertNull() {
        Column column = Column.editor()
                .name("test_col")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .create();

        assertThat(converter.convert(column, null)).isNull();
        assertThat(converter.convert(column, "NULL")).isNull();
    }

    @Test
    public void testParseDefaultValueReturnsEmpty() {
        Column column = Column.editor()
                .name("test_col")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .create();

        Optional<Object> result = converter.parseDefaultValue(column, null);
        assertThat(result).isEqualTo(Optional.empty());
    }

    @Test
    public void testParseDefaultValueReturnsValue() {
        Column column = Column.editor()
                .name("int_col")
                .type("INTEGER")
                .jdbcType(Types.INTEGER)
                .create();

        Optional<Object> result = converter.parseDefaultValue(column, "42");
        assertThat(result.isPresent()).isTrue();
        assertThat(result.get()).isEqualTo(42);
    }

    @Test
    public void testTinyIntAsBoolean() {
        Column column = Column.editor()
                .name("bool_col")
                .type("TINYINT")
                .jdbcType(Types.TINYINT)
                .create();

        assertThat(converter.convert(column, "true")).isEqualTo(true);
        assertThat(converter.convert(column, "false")).isEqualTo(false);
    }

    @Test
    public void testIntAsBoolean() {
        Column column = Column.editor()
                .name("bool_col")
                .type("INT")
                .jdbcType(Types.INTEGER)
                .create();

        assertThat(converter.convert(column, "true")).isEqualTo(true);
        assertThat(converter.convert(column, "false")).isEqualTo(false);
    }

    @Test
    public void testTrimDataTypes() {
        Column column = Column.editor()
                .name("int_col")
                .type("INTEGER")
                .jdbcType(Types.INTEGER)
                .create();

        // Should trim whitespace for integer types
        Object result = converter.convert(column, "  42  ");
        assertThat(result).isEqualTo(42);
    }

    @Test
    public void testUnsupportedTypeReturnsNull() {
        Column column = Column.editor()
                .name("test_col")
                .type("BLOB")
                .jdbcType(Types.BLOB)
                .create();

        Object result = converter.convert(column, "some value");
        assertThat(result).isNull();
    }
}
