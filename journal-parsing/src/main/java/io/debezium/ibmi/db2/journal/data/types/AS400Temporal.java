/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.data.types;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.regex.Pattern;

import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;

/**
 * Strict, timezone-free ISO journal temporal values. Native storage length is
 * distinct from the character buffer length. TIME retains IBM i's 24:00:00;
 * timestamp precision and the Debezium epoch representation must be lossless.
 */
public final class AS400Temporal implements AS400DataType {
    private static final long serialVersionUID = 1L;

    public enum Kind {
        DATE,
        TIME,
        TIMESTAMP
    }

    private final Kind kind;
    private final int precision;
    private final int ccsid;
    private final Pattern syntax;
    private final AS400Text text;
    private final int nanosQuantum;
    private final long units;

    public AS400Temporal(Kind kind, int precision, int ccsid) {
        this.kind = Objects.requireNonNull(kind);
        if (precision < 0 || precision > 9 || kind != Kind.TIMESTAMP && precision != 0
                || ccsid != 37 && ccsid != 500 && ccsid != 1140) {
            throw new IllegalArgumentException("Unsupported temporal precision or CCSID");
        }
        this.precision = precision;
        this.ccsid = ccsid;
        nanosQuantum = (int) Math.pow(10, 9 - precision);
        units = precision <= 3 ? 1_000 : precision <= 6 ? 1_000_000 : 1_000_000_000;
        text = new AS400Text(getByteLength(), ccsid);
        syntax = Pattern.compile(switch (kind) {
            case DATE -> "[0-9]{4}-[0-9]{2}-[0-9]{2}";
            case TIME -> "[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}";
            case TIMESTAMP -> "[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}"
                    + (precision == 0 ? "" : "\\.[0-9]{" + precision + "}");
        });
    }

    /** Also used for explicitly ISO-projected snapshot columns; no JDBC timezone conversion. */
    public Object parse(String value) {
        if (value == null || value.length() != getByteLength() || !syntax.matcher(value).matches()) {
            throw new IllegalArgumentException("Noncanonical temporal value");
        }
        if (kind == Kind.TIME) {
            return time(value);
        }
        LocalDate date = LocalDate.parse(value.substring(0, 10));
        if (date.getYear() < 1 || date.getYear() > 9999) {
            throw new IllegalArgumentException("IBM i date outside supported range");
        }
        if (kind == Kind.DATE) {
            return date;
        }
        Duration time = time(value.substring(11, 19));
        int nanos = precision == 0 ? 0 : Integer.parseInt(value.substring(20) + "0".repeat(9 - precision));
        if (time.equals(Duration.ofDays(1))) {
            throw new IllegalArgumentException("End-of-day timestamp normalization is not qualified");
        }
        LocalDateTime timestamp = date.atStartOfDay().plusSeconds(time.getSeconds()).plusNanos(nanos);
        validate(timestamp);
        return timestamp;
    }

    /** Reject type, precision or range coercion before the shared output converter. */
    public void validate(Object value) {
        if (kind == Kind.DATE && value instanceof LocalDate date && date.getYear() >= 1 && date.getYear() <= 9999) {
            return;
        }
        if (kind == Kind.TIME && value instanceof Duration time && !time.isNegative()
                && time.compareTo(Duration.ofDays(1)) <= 0 && time.getNano() == 0) {
            return;
        }
        if (kind != Kind.TIMESTAMP || !(value instanceof LocalDateTime timestamp)
                || timestamp.getYear() < 1 || timestamp.getYear() > 9999
                || timestamp.getNano() % nanosQuantum != 0) {
            throw new IllegalArgumentException("Temporal value differs from its pinned type/precision/range");
        }
        // Shared Debezium converters use millis/micros/nanos according to scale.
        // Check before conversion so an out-of-range nanos value cannot wrap.
        long seconds = timestamp.toEpochSecond(ZoneOffset.UTC);
        long fraction = timestamp.getNano() / (1_000_000_000 / units);
        // For negative seconds, multiply the next second before subtracting the
        // remaining fraction: the minimum signed nanos value is representable
        // even though its floor-second product alone would underflow.
        Math.addExact(Math.multiplyExact(seconds < 0 ? seconds + 1 : seconds, units), seconds < 0 ? fraction - units : fraction);
    }

    private static Duration time(String value) {
        int hours = Integer.parseInt(value.substring(0, 2));
        int minutes = Integer.parseInt(value.substring(3, 5));
        int seconds = Integer.parseInt(value.substring(6, 8));
        if (hours > 24 || minutes > 59 || seconds > 59 || hours == 24 && (minutes != 0 || seconds != 0)) {
            throw new IllegalArgumentException("Invalid IBM i time");
        }
        return Duration.ofSeconds(hours * 3600L + minutes * 60L + seconds);
    }

    @Override
    public int getByteLength() {
        return switch (kind) {
            case DATE -> 10;
            case TIME -> 8;
            case TIMESTAMP -> precision == 0 ? 19 : 20 + precision;
        };
    }

    @Override
    public Object toObject(byte[] value, int offset) {
        Objects.checkFromIndexSize(offset, getByteLength(), value.length);
        return parse((String) text.toObject(value, offset));
    }

    @Override
    public Object toObject(byte[] value) {
        return toObject(value, 0);
    }

    @Override
    public Object getDefaultValue() {
        throw new UnsupportedOperationException("Defaulted journal fields are not admitted");
    }

    @Override
    public int getInstanceType() {
        return -1;
    }

    @Override
    public Class<?> getJavaType() {
        return switch (kind) {
            case DATE -> LocalDate.class;
            case TIME -> Duration.class;
            case TIMESTAMP -> LocalDateTime.class;
        };
    }

    @Override
    public Object clone() {
        return new AS400Temporal(kind, precision, ccsid);
    }

    @Override
    public byte[] toBytes(Object value) {
        throw new UnsupportedOperationException("Read-only journal codec");
    }

    @Override
    public int toBytes(Object value, byte[] target) {
        throw new UnsupportedOperationException("Read-only journal codec");
    }

    @Override
    public int toBytes(Object value, byte[] target, int offset) {
        throw new UnsupportedOperationException("Read-only journal codec");
    }
}
