/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.conversion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

import java.math.BigDecimal;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Optional;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.db2as400.As400ConnectorConfig.CharSequenceTrimMode;
import io.debezium.connector.db2as400.As400ValueConverters;
import io.debezium.jdbc.JdbcValueConverters.DecimalMode;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;

@Tag("UnitTests")
public class As400DefaultValueConverterTest {

    private As400DefaultValueConverter converter;

    @BeforeEach
    public void setUp() {
        converter = new As400DefaultValueConverter(valueConverters(DecimalMode.PRECISE, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS));
    }

    private static As400ValueConverters valueConverters(DecimalMode decimalMode, TemporalPrecisionMode temporalMode) {
        return new As400ValueConverters(decimalMode, temporalMode, CharSequenceTrimMode.NONE);
    }

    @Test
    public void testParseDefaultTimestampMillisPrecision() {
        Column column = Column.editor().name("ts").type("TIMESTAMP").jdbcType(Types.TIMESTAMP).length(26).scale(3).create();

        Optional<Object> result = converter.parseDefaultValue(column, "'2023-10-15-14.30.45.123000'");
        long expected = LocalDateTime.of(2023, 10, 15, 14, 30, 45, 123_000_000).toInstant(ZoneOffset.UTC).toEpochMilli();
        assertThatObject(result.get()).isEqualTo(expected);
    }

    @Test
    public void testParseDefaultTimestampConnectMode() {
        As400DefaultValueConverter connectConverter = new As400DefaultValueConverter(valueConverters(DecimalMode.PRECISE, TemporalPrecisionMode.CONNECT));
        Column column = Column.editor().name("ts").type("TIMESTAMP").jdbcType(Types.TIMESTAMP).length(26).scale(6).create();

        Optional<Object> result = connectConverter.parseDefaultValue(column, "'2023-10-15-14.30.45.123000'");
        assertThatObject(result.get()).isInstanceOf(java.util.Date.class);
    }

    @Test
    public void testParseDefaultDateConnectMode() {
        As400DefaultValueConverter connectConverter = new As400DefaultValueConverter(valueConverters(DecimalMode.PRECISE, TemporalPrecisionMode.CONNECT));
        Column column = Column.editor().name("d").type("DATE").jdbcType(Types.DATE).create();

        Optional<Object> result = connectConverter.parseDefaultValue(column, "'2023-10-15'");
        assertThatObject(result.get()).isInstanceOf(java.util.Date.class);
    }

    @Test
    public void testConvertDate() {
        Column column = Column.editor()
                .name("date_col")
                .type("DATE")
                .jdbcType(Types.DATE)
                .create();

        Object result = converter.convert(column, "'2023-10-15'");

        assertThatObject(result).isEqualTo(java.sql.Date.valueOf(LocalDate.of(2023, 10, 15)));
    }

    @Test
    public void testConvertDateWithoutQuotes() {
        Column column = Column.editor()
                .name("date_col")
                .type("DATE")
                .jdbcType(Types.DATE)
                .create();

        Object result = converter.convert(column, "2023-10-15");
        assertThatObject(result).isEqualTo(java.sql.Date.valueOf(LocalDate.of(2023, 10, 15)));
    }

    @Test
    public void testConvertCurrentDate() {
        Column column = Column.editor()
                .name("date_col")
                .type("DATE")
                .jdbcType(Types.DATE)
                .create();

        Object result = converter.convert(column, "CURRENT_DATE");
        assertThatObject(result).isEqualTo(java.sql.Date.valueOf(LocalDate.EPOCH));
    }

    @Test
    public void testConvertInvalidDate() {
        Column column = Column.editor()
                .name("date_col")
                .type("DATE")
                .jdbcType(Types.DATE)
                .create();

        Object result = converter.convert(column, "invalid-date");
        assertThatObject(result).isNull();
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
        assertThatObject(result).isEqualTo(java.sql.Timestamp.valueOf(expected));
    }

    @Test
    public void testConvertCurrentTimestamp() {
        Column column = Column.editor()
                .name("timestamp_col")
                .type("TIMESTAMP")
                .jdbcType(Types.TIMESTAMP)
                .create();

        Object result = converter.convert(column, "CURRENT_TIMESTAMP");

        assertThatObject(result).isEqualTo(java.sql.Timestamp.valueOf(LocalDateTime.of(LocalDate.EPOCH, LocalTime.MIDNIGHT)));
    }

    @Test
    public void testConvertInvalidTimestamp() {
        Column column = Column.editor()
                .name("timestamp_col")
                .type("TIMESTAMP")
                .jdbcType(Types.TIMESTAMP)
                .create();

        Object result = converter.convert(column, "invalid-timestamp");
        assertThatObject(result).isNull();
    }

    @Test
    public void testConvertTime() {
        Column column = Column.editor()
                .name("time_col")
                .type("TIME")
                .jdbcType(Types.TIME)
                .create();

        Object result = converter.convert(column, "'14.30.45'");
        assertThatObject(result).isEqualTo(java.sql.Time.valueOf(LocalTime.of(14, 30, 45)));
    }

    @Test
    public void testConvertTimeWithoutQuotes() {
        Column column = Column.editor()
                .name("time_col")
                .type("TIME")
                .jdbcType(Types.TIME)
                .create();

        Object result = converter.convert(column, "14.30.45");
        assertThatObject(result).isEqualTo(java.sql.Time.valueOf(LocalTime.of(14, 30, 45)));
    }

    @Test
    public void testConvertCurrentTime() {
        Column column = Column.editor()
                .name("time_col")
                .type("TIME")
                .jdbcType(Types.TIME)
                .create();

        Object result = converter.convert(column, "CURRENT_TIME");
        assertThatObject(result).isEqualTo(java.sql.Time.valueOf(LocalTime.MIDNIGHT));
    }

    @Test
    public void testConvertInvalidTime() {
        Column column = Column.editor()
                .name("time_col")
                .type("TIME")
                .jdbcType(Types.TIME)
                .create();

        Object result = converter.convert(column, "invalid-time");
        assertThatObject(result).isNull();
    }

    @Test
    public void testConvertBoolean() {
        Column column = Column.editor()
                .name("bool_col")
                .type("BOOLEAN")
                .jdbcType(Types.BOOLEAN)
                .create();

        assertThatObject(converter.convert(column, "1")).isEqualTo(true);
        assertThatObject(converter.convert(column, "0")).isEqualTo(false);
        assertThatObject(converter.convert(column, "true")).isEqualTo(true);
        assertThatObject(converter.convert(column, "false")).isEqualTo(false);
    }

    @Test
    public void testConvertInteger() {
        Column column = Column.editor()
                .name("int_col")
                .type("INTEGER")
                .jdbcType(Types.INTEGER)
                .create();

        Object result = converter.convert(column, "42");
        assertThatObject(result).isEqualTo(42);
    }

    @Test
    public void testConvertInvalidInteger() {
        Column column = Column.editor()
                .name("int_col")
                .type("INTEGER")
                .jdbcType(Types.INTEGER)
                .create();

        Object result = converter.convert(column, "not-a-number");
        assertThatObject(result).isNull();
    }

    @Test
    public void testConvertSmallInt() {
        Column column = Column.editor()
                .name("smallint_col")
                .type("SMALLINT")
                .jdbcType(Types.SMALLINT)
                .create();

        Object result = converter.convert(column, "123");
        assertThatObject(result).isEqualTo((short) 123);
    }

    @Test
    public void testConvertBigInt() {
        Column column = Column.editor()
                .name("bigint_col")
                .type("BIGINT")
                .jdbcType(Types.BIGINT)
                .create();

        Object result = converter.convert(column, "9223372036");
        assertThatObject(result).isEqualTo(9223372036L);
    }

    @Test
    public void testConvertInvalidBigInt() {
        Column column = Column.editor()
                .name("bigint_col")
                .type("BIGINT")
                .jdbcType(Types.BIGINT)
                .create();

        Object result = converter.convert(column, "not-a-bigint");
        assertThatObject(result).isNull();
    }

    @Test
    public void testConvertFloat() {
        Column column = Column.editor()
                .name("float_col")
                .type("FLOAT")
                .jdbcType(Types.FLOAT)
                .create();

        Object result = converter.convert(column, "1.5");
        assertThatObject(result).isEqualTo(1.5f);
    }

    @Test
    public void testConvertReal() {
        Column column = Column.editor()
                .name("real_col")
                .type("REAL")
                .jdbcType(Types.REAL)
                .create();

        Object result = converter.convert(column, "2.5");
        assertThatObject(result).isEqualTo(2.5f);
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
        assertThatObject(result).isEqualTo(new BigDecimal("123.46"));
    }

    @Test
    public void testConvertDecimalWithoutScale() {
        Column column = Column.editor()
                .name("decimal_col")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .create();

        Object result = converter.convert(column, "123.456");
        assertThatObject(result).isEqualTo(new BigDecimal("123.456"));
    }

    @Test
    public void testConvertDecimalDefaultForStringMode() {
        Column column = Column.editor()
                .name("decimal_col")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .length(10)
                .scale(2)
                .create();

        Object result = new As400DefaultValueConverter(valueConverters(DecimalMode.STRING, TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS))
                .parseDefaultValue(column, "0.00").get();

        assertThatObject(result).isEqualTo("0.00");
        SchemaBuilder.string().defaultValue(result);
    }

    @Test
    public void testConvertInvalidDecimal() {
        Column column = Column.editor()
                .name("decimal_col")
                .type("DECIMAL")
                .jdbcType(Types.DECIMAL)
                .create();

        Object result = converter.convert(column, "not-a-decimal");
        assertThatObject(result).isNull();
    }

    @Test
    public void testConvertDouble() {
        Column column = Column.editor()
                .name("double_col")
                .type("DOUBLE")
                .jdbcType(Types.DOUBLE)
                .create();

        Object result = converter.convert(column, "123.456");
        assertThatObject(result).isEqualTo(123.456);
    }

    @Test
    public void testConvertInvalidDouble() {
        Column column = Column.editor()
                .name("double_col")
                .type("DOUBLE")
                .jdbcType(Types.DOUBLE)
                .create();

        Object result = converter.convert(column, "not-a-double");
        assertThatObject(result).isNull();
    }

    @Test
    public void testConvertVarchar() {
        Column column = Column.editor()
                .name("varchar_col")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .create();

        Object result = converter.convert(column, "'test value'");
        assertThatObject(result).isEqualTo("test value");
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
        assertThatObject(result).isNull();
    }

    @Test
    public void testConvertChar() {
        Column column = Column.editor()
                .name("char_col")
                .type("CHAR")
                .jdbcType(Types.CHAR)
                .create();

        Object result = converter.convert(column, "'A'");
        assertThatObject(result).isEqualTo("A");
    }

    @Test
    public void testConvertBitSingleBit() {
        Column column = Column.editor()
                .name("bit_col")
                .type("BIT")
                .jdbcType(Types.BIT)
                .length(1)
                .create();

        assertThatObject(converter.convert(column, "1")).isEqualTo(true);
        assertThatObject(converter.convert(column, "0")).isEqualTo(false);
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
        assertThatObject(result).isInstanceOf(byte[].class);
        byte[] bytes = (byte[]) result;
        assertThat(bytes).hasSize(1);
        assertThat(bytes[0]).isEqualTo((byte) 0xAA);
    }

    @Test
    public void testConvertBinary() {
        Column column = Column.editor()
                .name("binary_col")
                .type("BINARY")
                .jdbcType(Types.BINARY)
                .length(1)
                .create();

        Object result = converter.convert(column, "BX'01'");
        assertThatObject(result).isInstanceOf(byte[].class);
        assertThat((byte[]) result).containsExactly((byte) 0x01);
    }

    @Test
    public void testConvertVarbinary() {
        Column column = Column.editor()
                .name("varbinary_col")
                .type("VARBINARY")
                .jdbcType(Types.VARBINARY)
                .length(8)
                .create();

        Object result = converter.convert(column, "BX'01020304'");
        assertThatObject(result).isInstanceOf(byte[].class);
        assertThat((byte[]) result).containsExactly((byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04);
    }

    @Test
    public void testConvertInvalidBinary() {
        Column column = Column.editor()
                .name("binary_col")
                .type("BINARY")
                .jdbcType(Types.BINARY)
                .length(1)
                .create();

        Object result = converter.convert(column, "BX'0G'");
        assertThatObject(result).isNull();
    }

    @Test
    public void testConvertNull() {
        Column column = Column.editor()
                .name("test_col")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .create();

        assertThatObject(converter.convert(column, null)).isNull();
        assertThatObject(converter.convert(column, "NULL")).isNull();
    }

    @Test
    public void testParseDefaultValueReturnsEmpty() {
        Column column = Column.editor()
                .name("test_col")
                .type("VARCHAR")
                .jdbcType(Types.VARCHAR)
                .create();

        Optional<Object> result = converter.parseDefaultValue(column, null);
        assertThatObject(result).isEqualTo(Optional.empty());
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
        assertThatObject(result.get()).isEqualTo(42);
    }

    @Test
    public void testTinyIntAsBoolean() {
        Column column = Column.editor()
                .name("bool_col")
                .type("TINYINT")
                .jdbcType(Types.TINYINT)
                .create();

        assertThatObject(converter.convert(column, "true")).isEqualTo(true);
        assertThatObject(converter.convert(column, "false")).isEqualTo(false);
    }

    @Test
    public void testIntAsBoolean() {
        Column column = Column.editor()
                .name("bool_col")
                .type("INT")
                .jdbcType(Types.INTEGER)
                .create();

        assertThatObject(converter.convert(column, "true")).isEqualTo(true);
        assertThatObject(converter.convert(column, "false")).isEqualTo(false);
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
        assertThatObject(result).isEqualTo(42);
    }

    private long toEpochMicros(LocalDateTime value) {
        return value.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000 + value.getNano() / 1_000;
    }

    @Test
    public void testUnsupportedTypeReturnsNull() {
        Column column = Column.editor()
                .name("test_col")
                .type("BLOB")
                .jdbcType(Types.BLOB)
                .create();

        Object result = converter.convert(column, "some value");
        assertThatObject(result).isNull();
    }
}
