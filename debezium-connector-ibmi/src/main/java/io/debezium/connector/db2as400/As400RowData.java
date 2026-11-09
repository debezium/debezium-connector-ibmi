/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;

/** Owns complete column values; never retains driver locators, streams, or arbitrary mutable objects. */
final class As400RowData {
    static final int MAX_COLUMNS = 4096;
    private final Object[] values;
    private final long payloadBytes;

    private As400RowData(Object[] values, long payloadBytes) {
        this.values = values;
        this.payloadBytes = payloadBytes;
    }

    static As400RowData copyOf(Object[] values, long maximumBytes) {
        if (values == null || values.length == 0 || values.length > MAX_COLUMNS || maximumBytes < 0) {
            throw new IllegalArgumentException("Missing or oversized row layout");
        }
        long bytes = 0;
        for (Object value : values) {
            long size = size(value);
            if (size > maximumBytes - bytes) {
                throw new IllegalArgumentException("Materialized row exceeds payload limit");
            }
            bytes += size;
        }
        return new As400RowData(copy(values), bytes);
    }

    Object[] values() {
        return copy(values);
    }

    long payloadBytes() {
        return payloadBytes;
    }

    private static long size(Object value) {
        if (value == null) {
            return 0;
        }
        Class<?> type = value.getClass();
        if (type == byte[].class) {
            return ((byte[]) value).length;
        }
        if (type == String.class) {
            return 2L * ((String) value).length();
        }
        if (type == BigDecimal.class) {
            return 4L + (((BigDecimal) value).unscaledValue().bitLength() + 8L) / 8;
        }
        if (type == BigInteger.class) {
            return (((BigInteger) value).bitLength() + 8L) / 8;
        }
        if (type == Timestamp.class) {
            return 12;
        }
        if (type == java.time.LocalDate.class || type == java.time.LocalDateTime.class || type == java.time.Duration.class) {
            return 16; // Immutable timezone-free values, including TIME 24:00:00.
        }
        if (type == Byte.class || type == Short.class || type == Integer.class || type == Long.class
                || type == Float.class || type == Double.class || type == Boolean.class
                || type == java.sql.Date.class || type == java.sql.Time.class || type == java.util.Date.class) {
            return 8;
        }
        throw new IllegalArgumentException("Unsupported materialized column type " + type.getName());
    }

    private static Object[] copy(Object[] values) {
        Object[] owned = values.clone();
        for (int i = 0; i < owned.length; i++) {
            if (owned[i] instanceof byte[] bytes) {
                owned[i] = bytes.clone();
            }
            else if (owned[i] instanceof java.util.Date date) {
                // Timestamp.clone preserves nanoseconds, unlike new Date(getTime()).
                owned[i] = date.clone();
            }
        }
        return owned;
    }
}
