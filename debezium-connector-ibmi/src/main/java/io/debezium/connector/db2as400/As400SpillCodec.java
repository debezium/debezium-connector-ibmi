/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import io.debezium.connector.db2as400.As400SpillStore.Codec;
import io.debezium.connector.db2as400.As400TransactionBuffer.Change;
import io.debezium.connector.db2as400.As400TransactionBuffer.Image;
import io.debezium.connector.db2as400.As400TransactionBuffer.RecordIdentity;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.data.Envelope.Operation;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader.ControlInfo;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader.RecordInfo;
import io.debezium.relational.TableId;

/** Explicit scratch encoding. Never loads classes or deserializes arbitrary Java objects. */
final class As400SpillCodec {
    static final int MAX_VALUE_BYTES = 4 * 1024 * 1024;
    static final Codec<BigInteger> INTEGER = codec(As400SpillCodec::integer, As400SpillCodec::integer);
    static final Codec<String> STRING = codec(As400SpillCodec::string, As400SpillCodec::string);
    static final Codec<Integer> INT = codec(DataOutputStream::writeInt, DataInputStream::readInt);
    static final Codec<byte[]> BYTES = codec(As400SpillCodec::bytes, As400SpillCodec::bytes);
    static final Codec<JournalPosition> POSITION = codec(As400SpillCodec::position, As400SpillCodec::position);
    static final Codec<TableId> TABLE_ID = codec(As400SpillCodec::tableId, As400SpillCodec::tableId);
    static final Codec<TableIdentity> TABLE = codec(As400SpillCodec::table, As400SpillCodec::table);
    static final Codec<RecordIdentity> RECORD = codec(As400SpillCodec::record, As400SpillCodec::record);
    static final Codec<Image> IMAGE = codec(As400SpillCodec::image, As400SpillCodec::image);
    static final Codec<Change> CHANGE = codec((out, value) -> {
        string(out, value.operation().code());
        table(out, value.table());
        record(out, value.record());
        source(out, value.source());
        image(out, value.before());
        image(out, value.after());
    }, in -> new Change(Operation.forCode(string(in)), table(in), record(in), source(in), image(in), image(in)));

    private As400SpillCodec() {
    }

    interface Writer<T> {
        void write(DataOutputStream out, T value) throws IOException;
    }

    interface Reader<T> {
        T read(DataInputStream in) throws IOException;
    }

    static <T> Codec<T> codec(Writer<T> writer, Reader<T> reader) {
        return new Codec<>() {
            @Override
            public byte[] encode(T value) {
                try {
                    var bytes = new ByteArrayOutputStream() {
                        private void capacity(int added) {
                            if (added < 0 || added > MAX_VALUE_BYTES - count) {
                                throw new IllegalArgumentException("Oversized spill value");
                            }
                        }

                        @Override
                        public void write(int value) {
                            capacity(1);
                            super.write(value);
                        }

                        @Override
                        public void write(byte[] value, int offset, int length) {
                            capacity(length);
                            super.write(value, offset, length);
                        }
                    };
                    try (var out = new DataOutputStream(bytes)) {
                        writer.write(out, value);
                    }
                    if (bytes.size() > MAX_VALUE_BYTES) {
                        throw new IOException("Oversized spill value");
                    }
                    return bytes.toByteArray();
                }
                catch (IOException e) {
                    throw new As400SpillStore.SpillException("Cannot encode transaction working data", e);
                }
            }

            @Override
            public T decode(byte[] value) {
                if (value == null || value.length > MAX_VALUE_BYTES) {
                    throw new IllegalArgumentException("Missing or oversized spill value");
                }
                try (var in = new DataInputStream(new ByteArrayInputStream(value))) {
                    T decoded = reader.read(in);
                    if (in.available() != 0) {
                        throw new IOException("Trailing spill value data");
                    }
                    return decoded;
                }
                catch (IOException | RuntimeException e) {
                    throw new As400SpillStore.SpillException("Cannot decode transaction working data", e);
                }
            }
        };
    }

    static void bytes(DataOutputStream out, byte[] value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        }
        else {
            if (value.length > MAX_VALUE_BYTES) {
                throw new IOException("Oversized spill field");
            }
            out.writeInt(value.length);
            out.write(value);
        }
    }

    static byte[] bytes(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > MAX_VALUE_BYTES || length > in.available()) {
            throw new IOException("Invalid spill field length");
        }
        return in.readNBytes(length);
    }

    static void string(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        if (value.length() > MAX_VALUE_BYTES / 2) {
            throw new IOException("Oversized spill string");
        }
        // Preserve Java UTF-16 code units, including an unpaired surrogate. A
        // UTF-8 replacement character must never silently change an existing key.
        out.writeInt(value.length());
        out.writeChars(value);
    }

    static String string(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > MAX_VALUE_BYTES / 2 || length > in.available() / 2) {
            throw new IOException("Invalid spill string length");
        }
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = in.readChar();
        }
        return new String(chars);
    }

    static void integer(DataOutputStream out, BigInteger value) throws IOException {
        bytes(out, value == null ? null : value.toByteArray());
    }

    static BigInteger integer(DataInputStream in) throws IOException {
        byte[] value = bytes(in);
        return value == null ? null : new BigInteger(value);
    }

    static void position(DataOutputStream out, JournalPosition value) throws IOException {
        integer(out, value.offset());
        string(out, value.receiver().name());
        string(out, value.receiver().library());
    }

    static JournalPosition position(DataInputStream in) throws IOException {
        return new JournalPosition(integer(in), new JournalReceiver(string(in), string(in)));
    }

    static void tableId(DataOutputStream out, TableId value) throws IOException {
        string(out, value.catalog());
        string(out, value.schema());
        string(out, value.table());
    }

    static TableId tableId(DataInputStream in) throws IOException {
        return new TableId(string(in), string(in), string(in));
    }

    static void table(DataOutputStream out, TableIdentity value) throws IOException {
        tableId(out, value.tableId());
        string(out, value.physicalFile());
        string(out, value.member());
        string(out, value.fileJournalId());
        string(out, value.memberJournalId());
        string(out, value.recordFormat());
    }

    static TableIdentity table(DataInputStream in) throws IOException {
        return new TableIdentity(tableId(in), string(in), string(in), string(in), string(in), string(in));
    }

    static void record(DataOutputStream out, RecordIdentity value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            table(out, value.table());
            string(out, value.producer());
            integer(out, value.recordNumber());
        }
    }

    static RecordIdentity record(DataInputStream in) throws IOException {
        return in.readBoolean() ? new RecordIdentity(table(in), string(in), integer(in)) : null;
    }

    static void source(DataOutputStream out, As400SourceMetadata value) throws IOException {
        position(out, value.position());
        out.writeLong(value.timestamp().getEpochSecond());
        out.writeInt(value.timestamp().getNano());
        integer(out, value.systemSequence());
        integer(out, value.commitCycle());
        out.writeChar(value.journalCode());
        string(out, value.entryType());
        var record = value.recordInfo();
        integer(out, record.recordNumber());
        integer(out, record.threadId());
        string(out, record.jobName());
        string(out, record.jobUser());
        string(out, record.jobNumber());
        string(out, record.objectJournalId());
        out.writeChar(record.objectNameIndicator());
        string(out, record.objectType());
        string(out, record.systemName());
        string(out, record.programName());
        string(out, record.programLibrary());
        string(out, record.userProfile());
        var control = value.controlInfo();
        out.writeChar(control.indicatorFlag());
        out.writeLong(control.nestedCommitLevel());
        out.writeBoolean(control.hasTransactionIdentifier());
        out.writeBoolean(control.hasLogicalUnitOfWork());
        out.writeByte(control.flags());
    }

    static As400SourceMetadata source(DataInputStream in) throws IOException {
        return new As400SourceMetadata(position(in), Instant.ofEpochSecond(in.readLong(), in.readInt()), integer(in), integer(in), in.readChar(), string(in),
                new RecordInfo(integer(in), integer(in), string(in), string(in), string(in), string(in), in.readChar(), string(in), string(in), string(in), string(in),
                        string(in)),
                new ControlInfo(in.readChar(), in.readLong(), in.readBoolean(), in.readBoolean(), in.readByte()));
    }

    static void image(DataOutputStream out, Image value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            source(out, value.source());
            out.writeBoolean(value.row().included());
            if (value.row().included()) {
                Object[] columns = value.values();
                out.writeInt(columns.length);
                for (Object column : columns) {
                    scalar(out, column);
                }
            }
        }
    }

    static Image image(DataInputStream in) throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        As400SourceMetadata source = source(in);
        if (!in.readBoolean()) {
            return new Image(source, As400RowData.excluded());
        }
        int count = in.readInt();
        if (count < 1 || count > As400RowData.MAX_COLUMNS || count > in.available()) {
            throw new IOException("Invalid spill row column count");
        }
        Object[] values = new Object[count];
        for (int i = 0; i < count; i++) {
            values[i] = scalar(in);
        }
        return new Image(source, As400RowData.copyOf(values, MAX_VALUE_BYTES));
    }

    static void scalar(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            out.writeByte(0);
        }
        else if (value instanceof String string) {
            out.writeByte(1);
            string(out, string);
        }
        else if (value instanceof byte[] bytes) {
            out.writeByte(2);
            bytes(out, bytes);
        }
        else if (value instanceof BigInteger integer) {
            out.writeByte(3);
            integer(out, integer);
        }
        else if (value instanceof BigDecimal decimal) {
            out.writeByte(4);
            out.writeInt(decimal.scale());
            integer(out, decimal.unscaledValue());
        }
        else if (value instanceof Timestamp timestamp) {
            out.writeByte(5);
            out.writeLong(timestamp.getTime());
            out.writeInt(timestamp.getNanos());
        }
        else if (value instanceof java.sql.Date date) {
            out.writeByte(6);
            out.writeLong(date.getTime());
        }
        else if (value instanceof java.sql.Time time) {
            out.writeByte(7);
            out.writeLong(time.getTime());
        }
        else if (value instanceof java.util.Date date) {
            out.writeByte(8);
            out.writeLong(date.getTime());
        }
        else if (value instanceof LocalDate date) {
            out.writeByte(9);
            out.writeLong(date.toEpochDay());
        }
        else if (value instanceof LocalDateTime time) {
            out.writeByte(10);
            out.writeLong(time.toLocalDate().toEpochDay());
            out.writeLong(time.toLocalTime().toNanoOfDay());
        }
        else if (value instanceof Duration time) {
            out.writeByte(11);
            out.writeLong(time.getSeconds());
            out.writeInt(time.getNano());
        }
        else if (value instanceof Byte number) {
            out.writeByte(12);
            out.writeByte(number);
        }
        else if (value instanceof Short number) {
            out.writeByte(13);
            out.writeShort(number);
        }
        else if (value instanceof Integer number) {
            out.writeByte(14);
            out.writeInt(number);
        }
        else if (value instanceof Long number) {
            out.writeByte(15);
            out.writeLong(number);
        }
        else if (value instanceof Float number) {
            out.writeByte(16);
            out.writeInt(Float.floatToRawIntBits(number));
        }
        else if (value instanceof Double number) {
            out.writeByte(17);
            out.writeLong(Double.doubleToRawLongBits(number));
        }
        else if (value instanceof Boolean bool) {
            out.writeByte(18);
            out.writeBoolean(bool);
        }
        else {
            throw new IOException("Unsupported spill scalar type " + value.getClass().getName());
        }
    }

    static Object scalar(DataInputStream in) throws IOException {
        return switch (in.readUnsignedByte()) {
            case 0 -> null;
            case 1 -> string(in);
            case 2 -> bytes(in);
            case 3 -> integer(in);
            case 4 -> {
                int scale = in.readInt();
                yield new BigDecimal(integer(in), scale);
            }
            case 5 -> {
                var timestamp = new Timestamp(in.readLong());
                timestamp.setNanos(in.readInt());
                yield timestamp;
            }
            case 6 -> new java.sql.Date(in.readLong());
            case 7 -> new java.sql.Time(in.readLong());
            case 8 -> new java.util.Date(in.readLong());
            case 9 -> LocalDate.ofEpochDay(in.readLong());
            case 10 -> LocalDateTime.of(LocalDate.ofEpochDay(in.readLong()), LocalTime.ofNanoOfDay(in.readLong()));
            case 11 -> Duration.ofSeconds(in.readLong(), in.readInt());
            case 12 -> in.readByte();
            case 13 -> in.readShort();
            case 14 -> in.readInt();
            case 15 -> in.readLong();
            case 16 -> Float.intBitsToFloat(in.readInt());
            case 17 -> Double.longBitsToDouble(in.readLong());
            case 18 -> in.readBoolean();
            default -> throw new IOException("Unknown spill scalar type");
        };
    }
}
