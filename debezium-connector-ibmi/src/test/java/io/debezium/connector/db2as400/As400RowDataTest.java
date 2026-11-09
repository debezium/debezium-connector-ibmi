/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;

import org.junit.jupiter.api.Test;

class As400RowDataTest {
    @Test
    void ownsArraysDatesAndNanosecondTimestampsOnBothSidesOfTheBoundary() {
        Timestamp timestamp = Timestamp.valueOf("2026-11-06 12:34:56.123456789");
        Timestamp expected = (Timestamp) timestamp.clone();
        java.sql.Date date = java.sql.Date.valueOf("2026-11-06");
        java.sql.Time time = java.sql.Time.valueOf("12:34:56");
        byte[] bytes = { 1, 2 };
        Object[] original = { bytes, timestamp, date, time, "text", null, new BigDecimal("12.3400"), BigInteger.TEN, true };
        As400RowData row = As400RowData.copyOf(original, 1024);
        bytes[0] = 9;
        timestamp.setNanos(0);
        date.setTime(0);
        time.setTime(0);
        original[4] = "changed";
        Object[] first = row.values();
        assertThat((byte[]) first[0]).containsExactly(1, 2);
        assertThat(first[1]).isEqualTo(expected);
        assertThat(first[2]).isEqualTo(java.sql.Date.valueOf("2026-11-06"));
        assertThat(first[3]).isEqualTo(java.sql.Time.valueOf("12:34:56"));
        assertThat(first[4]).isEqualTo("text");
        assertThat(first[6]).isEqualTo(new BigDecimal("12.3400"));
        ((byte[]) first[0])[0] = 7;
        ((Timestamp) first[1]).setNanos(1);
        assertThat((byte[]) row.values()[0]).containsExactly(1, 2);
        assertThat(row.values()[1]).isEqualTo(expected);
    }

    @Test
    void rejectsLocatorsStreamsAndUnknownMutableValues() {
        for (Object value : List.of(new Object(), new Object[]{ 1 }, new StringBuilder("text"), new ByteArrayInputStream(new byte[]{ 1 }))) {
            assertThrows(IllegalArgumentException.class, () -> As400RowData.copyOf(new Object[]{ value }, 1024));
        }
    }

    @Test
    void boundsPayloadAndColumnCountIncludingAllNullRows() {
        assertThrows(IllegalArgumentException.class, () -> As400RowData.copyOf(new Object[]{ "123" }, 5));
        assertThrows(IllegalArgumentException.class, () -> As400RowData.copyOf(new Object[]{ new byte[6] }, 5));
        assertThrows(IllegalArgumentException.class, () -> As400RowData.copyOf(new Object[As400RowData.MAX_COLUMNS + 1], 1024));
        assertThat(As400RowData.copyOf(new Object[]{ "123" }, 6).payloadBytes()).isEqualTo(6);
    }
}
