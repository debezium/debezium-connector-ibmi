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
import java.util.HexFormat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400JournalAnchorTest {
    @Test
    void nativeFirstEntryReceiverWrapperDoesNotChangeTheSameJournalAnchor() throws Exception {
        var pair = pair();
        assertThat(pair[0].data()).hasSize(256);
        assertThat(pair[1].data()).hasSize(288);
        assertThat(As400OperatorCutReader.sameScanAnchor(pair[0], pair[1])).isTrue();
        assertThat(As400OperatorCutReader.sameScanAnchor(pair[1], pair[0])).isTrue();
    }

    @Test
    void nativeNonfinalEntryHasOnlyZeroAlignmentFillAfterItsLengthDelimitedData() throws Exception {
        var pair = pair("incremental-anchor-padding.json");
        assertThat(pair[0].data()).hasSize(292);
        assertThat(pair[1].data()).hasSize(304);
        assertThat(As400OperatorCutReader.sameScanAnchor(pair[0], pair[1])).isTrue();
        assertThat(As400OperatorCutReader.sameScanAnchor(pair[1], pair[0])).isTrue();
        byte[] changed = pair[1].data();
        changed[303] = 1;
        assertThrows(IllegalArgumentException.class, () -> As400OperatorCutReader.sameScanAnchor(pair[0], entry(pair[1].position(), changed)));
        byte[] longer = java.util.Arrays.copyOf(pair[1].data(), 320);
        assertThrows(IllegalArgumentException.class, () -> As400OperatorCutReader.sameScanAnchor(pair[0], entry(pair[1].position(), longer)));
        byte[] changedValue = pair[1].data();
        changedValue[291] ^= 1;
        assertThat(As400OperatorCutReader.sameScanAnchor(pair[0], entry(pair[1].position(), changedValue))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = { 32, 40, 48, 56, 64, 101, 197, 216, 218, 232, 287 })
    void normalizationStillRejectsChangedSourceMetadataFlagsNullMapAndPayload(int offset) throws Exception {
        var pair = pair();
        byte[] changed = pair[1].data();
        changed[offset] ^= 1;
        assertThat(As400OperatorCutReader.sameScanAnchor(pair[0], entry(pair[1].position(), changed))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(ints = { 256, 257, 258, 259, 260, 261, 262, 263, 264, 265 })
    void receiverWrapperRequiresExactSystemAspDeviceIncludingPadding(int offset) throws Exception {
        var pair = pair();
        byte[] changed = pair[1].data();
        changed[offset] ^= 1;
        assertUnsupportedAsp(pair, changed);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 2, 32, 255, -1 })
    void receiverWrapperRequiresSystemAspNumber(int number) throws Exception {
        var pair = pair();
        byte[] changed = pair[1].data();
        ByteBuffer.wrap(changed).putShort(266, (short) number);
        assertUnsupportedAsp(pair, changed);
    }

    private static void assertUnsupportedAsp(RawJournalPage.Entry[] pair, byte[] changed) {
        var invalid = entry(pair[1].position(), changed);
        assertThrows(IllegalArgumentException.class, () -> As400OperatorCutReader.sameScanAnchor(pair[0], invalid));
        assertThrows(IllegalArgumentException.class, () -> As400OperatorCutReader.sameScanAnchor(invalid, pair[0]));
        assertThrows(IllegalArgumentException.class, () -> As400OperatorCutReader.sameScanAnchor(invalid, invalid));
    }

    @ParameterizedTest
    @ValueSource(strings = { "receiver", "sequence", "overlap", "layout", "xid", "truncated", "null-length", "payload-length" })
    void malformedOrAmbiguousAnchorCannotUseWrapperNormalization(String fault) throws Exception {
        var pair = pair();
        byte[] changed = pair[1].data();
        switch (fault) {
            case "receiver" -> changed[236] = (byte) 0xc1;
            case "sequence" -> changed[31]++;
            case "overlap" -> ByteBuffer.wrap(changed).putInt(4, 240);
            case "layout" -> ByteBuffer.wrap(changed).putInt(20, 240);
            case "xid" -> ByteBuffer.wrap(changed).putInt(12, 236);
            case "truncated" -> changed = java.util.Arrays.copyOf(changed, 260);
            case "null-length" -> ByteBuffer.wrap(changed).putInt(268, 99);
            case "payload-length" -> changed[276] = (byte) 0xf9;
            default -> throw new AssertionError(fault);
        }
        byte[] invalid = changed;
        assertThrows(IllegalArgumentException.class, () -> As400OperatorCutReader.sameScanAnchor(pair[0], entry(pair[1].position(), invalid)));
    }

    private RawJournalPage.Entry[] pair() throws Exception {
        return pair("incremental-anchor.json");
    }

    private RawJournalPage.Entry[] pair(String name) throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/" + name)) {
            var fixture = new ObjectMapper().readTree(stream);
            var position = new JournalPosition(new BigInteger(fixture.get("sequence").asText()),
                    new JournalReceiver(fixture.get("receiver").asText(), fixture.get("library").asText()));
            return new RawJournalPage.Entry[]{ entry(position, HexFormat.of().parseHex(fixture.get("subsequent").asText())),
                    entry(position, HexFormat.of().parseHex(fixture.get("first").asText())) };
        }
    }

    private static RawJournalPage.Entry entry(JournalPosition position, byte[] bytes) {
        return new RawJournalPage.Entry(position, new EntryHeaderDecoder().decode(bytes, 0), bytes);
    }
}
