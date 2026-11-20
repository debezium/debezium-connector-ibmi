/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Kind;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Outcome;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.FirstHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.OffsetStatus;

class TransactionControlEntryDecoderTest {
    private static final int OFFSET = 23;
    private final EntryHeaderDecoder headerDecoder = new EntryHeaderDecoder();
    private final TransactionControlEntryDecoder decoder = new TransactionControlEntryDecoder();

    @ParameterizedTest
    @CsvSource({ "CM,0,DURABLE_COMMIT", "CM,2,DURABLE_COMMIT", "CM,3,NON_DURABLE_COMMIT", "CM,4,NON_DURABLE_COMMIT",
            "CM,1,UNKNOWN", "CM,9,UNKNOWN", "CM,X,UNKNOWN", "RB,0,FULL_ROLLBACK", "RB,2,FULL_ROLLBACK",
            "RB,1,INCOMPLETE_ROLLBACK", "RB,3,INCOMPLETE_ROLLBACK", "RB,4,UNKNOWN", "RB,X,UNKNOWN" })
    void decodesOutcomeFromIndicatorRatherThanBitFlags(String type, char flag, Outcome expected) throws Exception {
        byte[] bytes = fixture("commit");
        text(bytes, 99, type);
        text(bytes, 216, Character.toString(flag));
        EntryHeader header = headerDecoder.decode(bytes, 0);
        assertEquals(flag, header.getControlInfo().indicatorFlag());
        assertEquals((byte) 0x80, header.getControlInfo().flags());
        var control = decoder.decode(header, bytes, 0);
        assertEquals(expected, control.outcome());
        assertEquals(BigInteger.TEN, control.commitCycle());
        assertEquals(header.getControlInfo(), control.controlInfo());
        assertNull(control.savepointSequence());
    }

    @Test
    void decodesRollbackFixture() throws Exception {
        byte[] bytes = fixture("rollback");
        assertEquals(Outcome.FULL_ROLLBACK, decoder.decode(headerDecoder.decode(bytes, 0), bytes, 0).outcome());
    }

    @ParameterizedTest
    @CsvSource({ "SC,START", "R1,START_ROLLBACK", "SB,START_SAVEPOINT", "BC,BEGIN_COMMITMENT_CONTROL", "EC,END_COMMITMENT_CONTROL",
            "PC,UNSUPPORTED", "CN,UNSUPPORTED", "LW,UNSUPPORTED", "ZZ,UNSUPPORTED" })
    void retainsNonterminalAndUnsupportedControls(String type, Kind expected) throws Exception {
        byte[] bytes = fixture("commit");
        text(bytes, 99, type);
        var control = decoder.decode(headerDecoder.decode(bytes, 0), bytes, 0);
        assertEquals(type, control.entryType());
        assertEquals(expected, control.kind());
        assertEquals(expected == Kind.UNSUPPORTED ? Outcome.UNKNOWN : Outcome.NONE, control.outcome());
    }

    @ParameterizedTest
    @CsvSource({ "SQ,RELEASE_SAVEPOINT", "SU,ROLLBACK_SAVEPOINT" })
    void decodesUnsignedSavepointTargetAtNonzeroBufferOffset(String type, Kind expected) throws Exception {
        byte[] entry = fixture("savepoint-release");
        text(entry, 99, type);
        byte[] bytes = new byte[OFFSET + entry.length];
        System.arraycopy(entry, 0, bytes, OFFSET, entry.length);
        EntryHeader header = headerDecoder.decode(bytes, OFFSET);
        assertEquals(new BigInteger("18446744073709551614"), header.getSequenceNumber());
        assertEquals(2, header.getControlInfo().nestedCommitLevel());
        var control = decoder.decode(header, bytes, OFFSET);
        assertEquals(expected, control.kind());
        assertEquals(new BigInteger("9223372036854775825"), control.savepointSequence());
        assertEquals(Outcome.NONE, control.outcome());
    }

    @Test
    void preservesUnsignedNestedLevelAndIdentifierPresence() throws Exception {
        byte[] bytes = Arrays.copyOf(fixture("commit"), 400);
        ByteBuffer.wrap(bytes).putInt(232, -1).putInt(12, 240).putInt(16, 280);
        var info = headerDecoder.decode(bytes, 0).getControlInfo();
        assertEquals(4294967295L, info.nestedCommitLevel());
        assertTrue(info.hasTransactionIdentifier());
        assertTrue(info.hasLogicalUnitOfWork());
        var empty = headerDecoder.decode(fixture("commit"), 0).getControlInfo();
        assertFalse(empty.hasTransactionIdentifier());
        assertFalse(empty.hasLogicalUnitOfWork());
    }

    @ParameterizedTest
    @ValueSource(strings = { "00019", "00021", "     ", "-0001", "99999" })
    void rejectsInvalidOrOutOfBoundsPayloadLengths(String declaredLength) throws Exception {
        byte[] bytes = fixture("savepoint-release");
        text(bytes, 240, declaredLength);
        assertThrows(IOException.class, () -> decoder.decode(headerDecoder.decode(bytes, 0), bytes, 0));
    }

    @ParameterizedTest
    @ValueSource(strings = { "00000000000000000000", "18446744073709551616", "-0000000000000000001", "+0000000000000000001",
            "                    ", "00000000X00000000001" })
    void rejectsInvalidTargetSequences(String target) throws Exception {
        byte[] bytes = fixture("savepoint-release");
        text(bytes, 256, target);
        assertThrows(IOException.class, () -> decoder.decode(headerDecoder.decode(bytes, 0), bytes, 0));
    }

    @Test
    void acceptsMaximumUnsignedTargetAndSpacePadding() throws Exception {
        byte[] bytes = fixture("savepoint-release");
        for (String value : new String[]{ "18446744073709551615", "                  17" }) {
            text(bytes, 256, value);
            assertEquals(new BigInteger(value.trim()), decoder.decode(headerDecoder.decode(bytes, 0), bytes, 0).savepointSequence());
        }
    }

    @Test
    void rejectsMissingPrefixAndTruncatedPayloadWithoutReadingTheNextEntry() throws Exception {
        byte[] entry = fixture("savepoint-release");
        EntryHeader header = headerDecoder.decode(entry, 0);
        assertThrows(IOException.class, () -> decoder.decode(header, Arrays.copyOf(entry, entry.length - 1), 0));
        assertThrows(IOException.class, () -> decoder.decode(header, Arrays.copyOf(entry, 255), 0));
        assertThrows(IOException.class, () -> decoder.decode(header, entry, -1));
        byte[] withNextEntry = Arrays.copyOf(entry, entry.length + 100);
        ByteBuffer.wrap(withNextEntry).putInt(0, entry.length - 1);
        assertThrows(IOException.class, () -> decoder.decode(headerDecoder.decode(withNextEntry, 0), withNextEntry, 0));
        ByteBuffer.wrap(entry).putInt(8, 0);
        assertThrows(IOException.class, () -> decoder.decode(headerDecoder.decode(entry, 0), entry, 0));
    }

    @Test
    void rejectsNonControlAndDoesNotInferAnOutcomeForLegacyHeaders() throws Exception {
        byte[] bytes = fixture("commit");
        text(bytes, 98, "D");
        assertThrows(IOException.class, () -> decoder.decode(headerDecoder.decode(bytes, 0), bytes, 0));
        EntryHeader legacy = new EntryHeader(0, 0, 0, BigInteger.ONE, BigInteger.ONE, Instant.EPOCH,
                'C', "CM", " ".repeat(30), BigInteger.TEN, 236, 0, "", "");
        assertEquals(Outcome.UNKNOWN, decoder.decode(legacy, bytes, 0).outcome());
        assertEquals(-1, legacy.getControlInfo().nestedCommitLevel());
    }

    @ParameterizedTest
    @CsvSource({ "0,235", "0,99999", "8,1", "8,99999", "8,-1", "12,235", "16,250", "20,230" })
    void rejectsInvalidHeaderDisplacementsAndEntryBoundaries(int field, int value) throws Exception {
        byte[] bytes = fixture("savepoint-release");
        ByteBuffer.wrap(bytes).putInt(field, value);
        assertThrows(IllegalArgumentException.class, () -> headerDecoder.decode(bytes, 0));
    }

    @Test
    void rejectsShortFixedHeader() throws Exception {
        byte[] bytes = fixture("commit");
        assertThrows(IllegalArgumentException.class, () -> headerDecoder.decode(Arrays.copyOf(bytes, 235), 0));
        assertThrows(IllegalArgumentException.class, () -> headerDecoder.decode(bytes, -1));
        assertThrows(IllegalArgumentException.class, () -> headerDecoder.decode(bytes, Integer.MAX_VALUE));
    }

    @Test
    void retrievesConcatenatedControlsThroughTheRealEntryLoop() throws Exception {
        byte[] start = fixture("commit");
        text(start, 99, "SC");
        byte[] release = fixture("savepoint-release");
        byte[] commit = fixture("commit");
        List<byte[]> entries = List.of(start, release, commit);
        byte[] bytes = new byte[64 + start.length + release.length + commit.length];
        int at = 64;
        for (int i = 0; i < entries.size(); i++) {
            byte[] entry = entries.get(i);
            ByteBuffer.wrap(entry).putInt(0, i == entries.size() - 1 ? 0 : entry.length).putLong(24, 10 + i);
            if (i == 1) {
                text(entry, 256, "00000000000000000010");
            }
            System.arraycopy(entry, 0, bytes, at, entry.length);
            at += entry.length;
        }
        RetrieveConfig config = new RetrieveConfigBuilder().withJournalInfo(new JournalInfo("JRN", "LIB", false))
                .withTransactionControls(true).build();
        RetrieveJournal journal = new RetrieveJournal(config, mock(JournalInfoRetrieval.class));
        JournalProcessedPosition position = new JournalProcessedPosition(BigInteger.ONE, new JournalReceiver("RCV", "LIB"), Instant.EPOCH, false);
        JournalProcessedPosition continuation = position.withOffset(BigInteger.valueOf(13), false);
        journal.setOutputData(bytes, new FirstHeader(bytes.length, 64, 3, OffsetStatus.DATA, continuation), position);
        for (Kind expected : List.of(Kind.START, Kind.RELEASE_SAVEPOINT, Kind.COMMIT)) {
            assertTrue(journal.nextEntry());
            assertEquals(expected, journal.decode(decoder).kind());
        }
        assertFalse(journal.nextEntry());
        assertEquals(continuation, journal.getPosition());
    }

    private static byte[] fixture(String name) throws IOException {
        try (var stream = TransactionControlEntryDecoderTest.class.getResourceAsStream("/transaction-controls/" + name + ".hex")) {
            return HexFormat.of().parseHex(new String(stream.readAllBytes(), StandardCharsets.US_ASCII).replaceAll("\\s", ""));
        }
    }

    private static void text(byte[] bytes, int at, String value) {
        new AS400Text(value.length()).toBytes(value, bytes, at);
    }
}
