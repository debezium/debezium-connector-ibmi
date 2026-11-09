/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400ByteArray;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400SourceNormalizer.NormalizationException;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.Change;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.data.Envelope.Operation;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;

class As400SourceNormalizerTest {
    private static final int OFFSET = 13;
    private static final Instant TIME = Instant.parse("2026-11-06T12:34:00Z");
    private static final JournalReceiver RECEIVER = new JournalReceiver("RCV", "LIB");
    private static final String JID = "0102030405060708090a";
    private final TableIdentity table = new TableIdentity(new TableId("DB", "LIB", "LONG_TABLE"), "TEST", "TEST", JID, As400ConnectProbeConnector.MEMBER_JID,
            "FORMAT_V1");
    private final CompleteRowEntryDecoder decoder = new CompleteRowEntryDecoder(
            new AS400DataType[]{ new AS400Bin4(), new AS400ByteArray(2) }, new boolean[]{ false, false }, 64);
    private final As400TransactionBuffer buffer = new As400TransactionBuffer("LIB/JRN/incarnation", new Limits(10, 100, 20, 4096));
    private As400SourceNormalizer normalizer = new As400SourceNormalizer(buffer, (file, library) -> new TableBinding(table, decoder), 1024);
    private long sequence;

    @ParameterizedTest
    @ValueSource(strings = { "3", "8" })
    void realWireDecodersPreserveIdentityMetadataAndOwnedValuesThroughCommit(String subtype) throws Exception {
        Entry start = entry('C', "SC", 10, new byte[0]);
        accept(start);
        Entry first = row("PT", 10, 1);
        accept(first);
        Entry clear = clear(subtype.charAt(0));
        accept(clear);
        Entry reload = row("PX", 10, 2);
        accept(reload);
        Arrays.fill(first.bytes(), (byte) 0);
        Arrays.fill(reload.bytes(), (byte) 0);
        Entry commit = entry('C', "CM", 10, new byte[0]);
        CommittedBatch batch = accept(commit).orElseThrow();
        assertThat(batch.changes()).extracting(Change::operation).containsExactly(Operation.CREATE, Operation.TRUNCATE, Operation.CREATE);
        assertThat(batch.changes().get(0).after().values()[0]).isEqualTo(1);
        assertThat(batch.changes().get(2).after().values()[0]).isEqualTo(2);
        As400SourceMetadata source = batch.changes().get(2).source();
        assertThat(source.timestamp()).isEqualTo(TIME.plusSeconds(reload.sequence()));
        assertThat(source.position()).isEqualTo(position(reload));
        assertThat(source.systemSequence()).isEqualTo(BigInteger.ONE.shiftLeft(63).add(BigInteger.ONE));
        assertThat(source.recordInfo().threadId()).isEqualTo(BigInteger.ONE.shiftLeft(63).add(BigInteger.TWO));
        assertThat(source.recordInfo().recordNumber()).isEqualTo(BigInteger.ONE.shiftLeft(63).add(BigInteger.valueOf(3)));
        assertThat(source.recordInfo().jobName()).isEqualTo("JOB");
        assertThat(source.recordInfo().jobUser()).isEqualTo("USER");
        assertThat(source.recordInfo().jobNumber()).isEqualTo("123456");
        assertThat(source.recordInfo().objectJournalId()).isEqualTo(table.memberJournalId());
        assertThat(source.recordInfo().programName()).isEqualTo("PROGRAM");
        assertThat(source.recordInfo().programLibrary()).isEqualTo("PGMLIB");
        assertThat(source.recordInfo().userProfile()).isEqualTo("PROFILE");
        assertThat(source.entryType()).isEqualTo("PX");
        assertThat(batch.changes().get(1).source().entryType()).isEqualTo("CG");
        assertThat(batch.startSource().position()).isEqualTo(position(start));
        assertThat(batch.commitSource().timestamp()).isEqualTo(TIME.plusSeconds(commit.sequence()));
        assertThat(batch.commitPosition()).isEqualTo(position(commit));
        assertThat(batch.changes().get(2).record().table()).isEqualTo(table);
        buffer.completeBatch(batch);
        assertThat(buffer.usage().transactions()).isZero();
    }

    @Test
    void explicitRrnProjectionUsesJournalIdentityAcrossClearUpdateAndDelete() throws Exception {
        normalizer = new As400SourceNormalizer(buffer, (file, library) -> new TableBinding(table, decoder, true), 1024);
        accept(entry('C', "SC", 10, new byte[0]));
        for (String type : new String[]{ "PT", "UB", "UP", "DL" }) {
            Entry row = row(type, 10, type.equals("UP") ? 2 : 1);
            number(row.bytes(), 56, 42);
            accept(row);
        }
        accept(clear('8'));
        Entry reload = row("PT", 10, 3);
        number(reload.bytes(), 56, 42);
        accept(reload);
        var batch = accept(entry('C', "CM", 10, new byte[0])).orElseThrow();
        assertThat(batch.changes()).extracting(Change::operation).containsExactly(Operation.CREATE, Operation.UPDATE, Operation.DELETE, Operation.TRUNCATE,
                Operation.CREATE);
        assertThat(batch.changes().get(0).after().values()).containsExactly(1, new byte[]{ 1, 9 }, 42L);
        assertThat(batch.changes().get(1).before().values()[2]).isEqualTo(42L);
        assertThat(batch.changes().get(1).after().values()[2]).isEqualTo(42L);
        assertThat(batch.changes().get(2).before().values()[2]).isEqualTo(42L);
        assertThat(batch.changes().get(4).after().values()[2]).isEqualTo(42L);
    }

    @Test
    void rrnProjectionRejectsUnrepresentableNativeRecordNumbers() throws Exception {
        normalizer = new As400SourceNormalizer(buffer, (file, library) -> new TableBinding(table, decoder, true), 1024);
        accept(entry('C', "SC", 10, new byte[0]));
        fails(row("PT", 10, 1));
    }

    @Test
    void preservesBothUpdateImageSourcesAndSeparatesInterleavedCycles() throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        accept(entry('C', "SC", 20, new byte[0]));
        Entry beforeA = row("UB", 10, 1);
        accept(beforeA);
        accept(row("UB", 20, 2));
        Entry afterA = row("UP", 10, 3);
        accept(afterA);
        accept(row("UP", 20, 4));
        CommittedBatch b = accept(entry('C', "CM", 20, new byte[0])).orElseThrow();
        assertThat(b.changes().get(0).before().values()[0]).isEqualTo(2);
        assertThat(b.changes().get(0).after().values()[0]).isEqualTo(4);
        buffer.completeBatch(b);
        Change a = accept(entry('C', "CM", 10, new byte[0])).orElseThrow().changes().get(0);
        assertThat(a.before().source().timestamp()).isEqualTo(TIME.plusSeconds(beforeA.sequence()));
        assertThat(a.source().position()).isEqualTo(position(afterA));
        assertThat(a.before().values()[0]).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "thread", "job", "rrn" })
    void mismatchedUpdateProducerOrRecordCannotBorrowAnImage(String field) throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        accept(row("UB", 10, 1));
        Entry update = row("UP", 10, 2);
        switch (field) {
            case "thread" -> number(update.bytes(), 40, 5);
            case "rrn" -> number(update.bytes(), 56, 6);
            default -> text(update.bytes(), 101, 10, "OTHERJOB");
        }
        fails(update);
    }

    @Test
    void decodedSavepointRollbackDropsClearAndReload() throws Exception {
        normalizer = new As400SourceNormalizer(buffer, (file, library) -> new TableBinding(table, decoder), 1024,
                (source, control) -> SavepointRollback.RETAIN_TARGET);
        accept(entry('C', "SC", 10, new byte[0]));
        accept(row("PT", 10, 1));
        Entry savepoint = entry('C', "SB", 10, new byte[0]);
        accept(savepoint);
        accept(clear('8'));
        accept(row("PT", 10, 2));
        accept(row("DR", 10, 2));
        accept(savepointRollback(savepoint));
        accept(row("PT", 10, 3));
        CommittedBatch batch = accept(entry('C', "CM", 10, new byte[0])).orElseThrow();
        assertThat(batch.changes()).extracting(Change::operation).containsExactly(Operation.CREATE, Operation.CREATE);
        assertThat(batch.changes().get(1).after().values()[0]).isEqualTo(3);
    }

    @Test
    void unresolvedSavepointMappingStopsBeforeCommit() throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        Entry savepoint = entry('C', "SB", 10, new byte[0]);
        accept(savepoint);
        accept(clear('3'));
        fails(savepointRollback(savepoint));
    }

    @ParameterizedTest
    @ValueSource(strings = { "BR", "DR", "UR" })
    void knownCompensationNeverBecomesForwardOutput(String type) throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        accept(clear('8'));
        accept(row(type, 10, 7));
        accept(entry('C', "RB", 10, new byte[0]));
        assertThat(buffer.pendingBatch()).isEmpty();
        assertThat(buffer.usage().transactions()).isZero();
    }

    @Test
    void forwardLookingUndoAfterRollbackStartIsAmbiguousAndStops() throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        accept(entry('C', "R1", 10, new byte[0]));
        fails(row("PT", 10, 7));
    }

    @ParameterizedTest
    @ValueSource(strings = { "rrn", "thread", "job", "jobUser", "jobNumber", "system", "jid", "member", "objectType", "historicalName" })
    void absentOrAmbiguousIdentityIsFatal(String field) throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        Entry row = row("PT", 10, 1);
        switch (field) {
            case "rrn" -> number(row.bytes(), 56, 0);
            case "thread" -> number(row.bytes(), 40, 0);
            case "job" -> text(row.bytes(), 101, 10, "*OMITTED");
            case "jobUser" -> text(row.bytes(), 111, 10, "");
            case "jobNumber" -> text(row.bytes(), 121, 6, "000000");
            case "system" -> text(row.bytes(), 208, 8, "");
            case "jid" -> Arrays.fill(row.bytes(), OFFSET + 197, OFFSET + 207, (byte) 0);
            case "member" -> text(row.bytes(), 177, 10, "OTHER");
            case "objectType" -> text(row.bytes(), 219, 10, "*STMF");
            default -> text(row.bytes(), 217, 1, "0");
        }
        fails(row);
    }

    @ParameterizedTest
    @ValueSource(strings = { "pointer", "incomplete", "minimized", "fieldMinimized", "beforeMissing", "length", "xmlLayout", "unmapped", "rowLimit" })
    void incompleteMaterializationAbortsBufferAndCannotBeFollowedByCommit(String reason) throws Exception {
        Entry start = entry('C', "SC", 10, new byte[0]);
        accept(start);
        accept(row("PT", 10, 1));
        Entry row = row("UB", 10, 2);
        switch (reason) {
            case "pointer" -> ByteBuffer.wrap(row.bytes()).putInt(OFFSET + 72, 1);
            case "incomplete" -> row.bytes()[OFFSET + 218] = 0x20;
            case "minimized" -> row.bytes()[OFFSET + 218] = 0x08;
            case "fieldMinimized" -> row.bytes()[OFFSET + 218] = 0x02;
            case "beforeMissing" -> text(row.bytes(), 216, 1, "0");
            case "length" -> text(row.bytes(), 240, 5, "00005");
            case "xmlLayout" -> normalizer = new As400SourceNormalizer(buffer, (file, library) -> {
                throw new IllegalArgumentException("XML placeholder layout is unsupported");
            }, 1024);
            case "unmapped" -> normalizer = new As400SourceNormalizer(buffer, (file, library) -> null, 1024);
            default -> normalizer = new As400SourceNormalizer(buffer, (file, library) -> new TableBinding(table, decoder), 1);
        }
        fails(row);
        assertThat(buffer.oldestTransactionPosition()).contains(position(start));
        assertThat(buffer.usage().transactions()).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "R/ZZ", "R/IL", "D/ZZ", "F/CR", "C/PC", "C/CN", "C/ZZ", "J/NR" })
    void unsupportedEntriesCannotDisappearBehindAnEnumFilter(String operation) throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        fails(entry(operation.charAt(0), operation.substring(2), 10, new byte[0]));
    }

    @ParameterizedTest
    @ValueSource(strings = { "subtype", "file", "library", "member" })
    void rejectsUnsupportedOrConflictingClearIdentity(String field) throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        Entry clear = clear(field.equals("subtype") ? '0' : '8');
        if (!field.equals("subtype")) {
            int index = field.equals("file") ? 256 : field.equals("library") ? 266 : 276;
            text(clear.bytes(), index, 10, "OTHER");
        }
        fails(clear);
    }

    @Test
    void resolvedCursorMustBeCurrentEntry() throws Exception {
        Entry start = entry('C', "SC", 10, new byte[0]);
        accept(start);
        Entry row = row("PT", 10, 1);
        EntryHeader header = new EntryHeaderDecoder().decode(row.bytes(), OFFSET);
        assertThrows(NormalizationException.class, () -> normalizer.accept(header, row.bytes(), OFFSET,
                new JournalPosition(BigInteger.valueOf(row.sequence() + 1), RECEIVER)));
        assertThat(buffer.oldestTransactionPosition()).contains(position(start));
        assertThrows(As400TransactionBuffer.BufferException.class, buffer::pendingBatch);
    }

    @Test
    void resolvedReceiverCannotOverrideConflictingHeaderReceiver() throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        Entry row = row("PT", 10, 1);
        int receiverOffset = row.bytes().length - OFFSET;
        byte[] extended = Arrays.copyOf(row.bytes(), row.bytes().length + 32);
        ByteBuffer.wrap(extended).putInt(OFFSET, receiverOffset + 32).putInt(OFFSET + 20, receiverOffset);
        text(extended, receiverOffset, 10, "OTHER_RCV");
        text(extended, receiverOffset + 10, 10, "LIB");
        text(extended, receiverOffset + 20, 10, "");
        fails(new Entry(row.sequence(), extended));
    }

    @Test
    void deleteRetainsItsCompleteBeforeImage() throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        Entry deleted = row("DL", 10, 42);
        accept(deleted);
        Change change = accept(entry('C', "CM", 10, new byte[0])).orElseThrow().changes().get(0);
        assertThat(change.operation()).isEqualTo(Operation.DELETE);
        assertThat(change.before().values()[0]).isEqualTo(42);
        assertThat(change.after()).isNull();
        assertThat(change.source().position()).isEqualTo(position(deleted));
    }

    @Test
    void rejectsCycleZeroWithoutAStartInsteadOfBypassingOrdering() throws Exception {
        fails(row("PT", 0, 1));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "0102", "00000000000000000000", "40404040404040404040", "zz02030405060708090a", "0102030405060708090A", "0102030405060708090a00" })
    void bothJournalIdentitiesMustBeCompleteAndUnambiguous(String invalid) {
        assertThrows(IllegalArgumentException.class, () -> new TableIdentity(table.tableId(), table.physicalFile(), table.member(),
                invalid, table.memberJournalId(), table.recordFormat()));
        assertThrows(IllegalArgumentException.class, () -> new TableIdentity(table.tableId(), table.physicalFile(), table.member(),
                table.fileJournalId(), invalid, table.recordFormat()));
    }

    @Test
    void singleJidCannotServeAsBothObjectsAndBindingsRequireQualifiedNames() {
        assertThrows(IllegalArgumentException.class, () -> new TableIdentity(table.tableId(), table.physicalFile(), table.member(),
                JID, JID, table.recordFormat()));
        var unqualified = new TableIdentity(new TableId("DB", null, "TEST"), "TEST", "TEST", table.fileJournalId(), table.memberJournalId(), "FORMAT_V1");
        assertThrows(IllegalArgumentException.class, () -> new TableBinding(unqualified, decoder));
        assertThrows(NullPointerException.class, () -> new TableBinding(table, null));
    }

    @ParameterizedTest
    @ValueSource(strings = { "row-file-type", "row-file-jid", "clear-member-type", "clear-member-jid", "missing-binding" })
    void objectKindsAndJournalIdentifiersAreNotInterchangeable(String failure) throws Exception {
        accept(entry('C', "SC", 10, new byte[0]));
        Entry input = failure.startsWith("clear") ? clear('8') : row("PT", 10, 1);
        switch (failure) {
            case "row-file-type" -> text(input.bytes(), 219, 10, "*FILE");
            case "row-file-jid" -> System.arraycopy(HexFormat.of().parseHex(table.fileJournalId()), 0, input.bytes(), OFFSET + 197, 10);
            case "clear-member-type" -> text(input.bytes(), 219, 10, "*QDDS");
            case "clear-member-jid" -> System.arraycopy(HexFormat.of().parseHex(table.memberJournalId()), 0, input.bytes(), OFFSET + 197, 10);
            default -> normalizer = new As400SourceNormalizer(buffer, (file, library) -> null, 1024);
        }
        fails(input);
    }

    private void fails(Entry entry) throws Exception {
        assertThrows(NormalizationException.class, () -> accept(entry));
        assertThrows(As400TransactionBuffer.BufferException.class, buffer::pendingBatch);
        assertThrows(NormalizationException.class, () -> accept(entry('C', "CM", 10, new byte[0])));
    }

    private Optional<CommittedBatch> accept(Entry entry) {
        EntryHeader header = new EntryHeaderDecoder().decode(entry.bytes(), OFFSET);
        return normalizer.accept(header, entry.bytes(), OFFSET, position(entry));
    }

    private JournalPosition position(Entry entry) {
        return new JournalPosition(BigInteger.valueOf(entry.sequence()), RECEIVER);
    }

    private Entry savepointRollback(Entry target) throws Exception {
        return entry('C', "SU", 10, new AS400Text(20, 37).toBytes(String.format("%020d", target.sequence())));
    }

    private Entry row(String type, long cycle, int value) throws Exception {
        byte[] payload = ByteBuffer.allocate(6).putInt(value).put((byte) value).put((byte) 9).array();
        return entry('R', type, cycle, payload);
    }

    private Entry clear(char subtype) throws Exception {
        byte[] payload = new byte[109];
        new AS400Text(10, 37).toBytes("TEST", payload, 0);
        new AS400Text(10, 37).toBytes("LIB", payload, 10);
        new AS400Text(10, 37).toBytes("", payload, 20);
        new AS400Text(1, 37).toBytes(Character.toString(subtype), payload, 108);
        Entry entry = entry('D', "CG", 10, payload);
        text(entry.bytes(), 177, 10, "");
        return entry;
    }

    /** Synthetic RJNE0200 record; all tests traverse the real fixed-header and payload decoders. */
    private Entry entry(char code, String type, long cycle, byte[] payload) throws Exception {
        long seq = ++sequence;
        int length = 256 + payload.length;
        byte[] bytes = new byte[OFFSET + length];
        ByteBuffer.wrap(bytes).putInt(OFFSET, length).putInt(OFFSET + 8, payload.length == 0 ? 0 : 240);
        number(bytes, 24, seq);
        number(bytes, 40, Long.MIN_VALUE + 2);
        number(bytes, 48, Long.MIN_VALUE + 1);
        number(bytes, 56, Long.MIN_VALUE + 3);
        number(bytes, 64, cycle);
        // RJNE0200 DTS: microseconds since 2000-01-01, shifted 12 bits, biased by 2^63.
        // JTOpen's toBytes remains a character timestamp encoder even in its DTS read mode.
        BigInteger micros = BigInteger.valueOf(TIME.plusSeconds(seq).toEpochMilli()).multiply(BigInteger.valueOf(1000));
        BigInteger dts = micros.subtract(new BigInteger("946684800000000")).shiftLeft(12).add(BigInteger.ONE.shiftLeft(63));
        number(bytes, 32, dts.longValue());
        text(bytes, 98, 1, Character.toString(code));
        text(bytes, 99, 2, type);
        text(bytes, 101, 10, "JOB");
        text(bytes, 111, 10, "USER");
        text(bytes, 121, 6, "123456");
        text(bytes, 127, 10, "PROGRAM");
        text(bytes, 137, 10, "PGMLIB");
        text(bytes, 147, 10, "");
        text(bytes, 157, 10, "TEST");
        text(bytes, 167, 10, "LIB");
        text(bytes, 177, 10, "TEST");
        text(bytes, 187, 10, "PROFILE");
        System.arraycopy(HexFormat.of().parseHex(code == 'R' ? table.memberJournalId() : table.fileJournalId()), 0, bytes, OFFSET + 197, 10);
        text(bytes, 208, 8, "SYSTEM");
        text(bytes, 216, 1, type.equals("UB") || type.equals("DL") ? "1" : "0");
        text(bytes, 217, 1, "1");
        text(bytes, 219, 10, code == 'R' ? "*QDDS" : "*FILE");
        text(bytes, 240, 5, String.format("%05d", payload.length));
        System.arraycopy(payload, 0, bytes, OFFSET + 256, payload.length);
        return new Entry(seq, bytes);
    }

    private static void number(byte[] data, int offset, long value) {
        ByteBuffer.wrap(data).putLong(OFFSET + offset, value);
    }

    private static void text(byte[] data, int offset, int length, String value) {
        new AS400Text(length, 37).toBytes(value, data, OFFSET + offset);
    }

    private record Entry(long sequence, byte[] bytes) {
    }
}
