/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HexFormat;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Content comparison for the bounded SYSBAS, inline, *VARLEN profile.
 * Raw copies/checksums stay unchanged. Compare documented RJNE0200 sections;
 * receiver information is inherited within an API response, and offsets/padding
 * describe transport placement rather than a changed journal entry.
 * https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm
 */
final class As400JournalRecordContent {
    private record Span(int start, int end) {
    }

    /** An observed user entry can anchor an empty epoch, but never represents row output. */
    static EntryHeader userMarker(RawJournalPage.Entry entry) throws Exception {
        byte[] raw = entry.data();
        digest(raw, entry.position());
        var header = new EntryHeaderDecoder().decode(raw, 0);
        var info = As400SourceNormalizer.validateSource(header, raw, 0, entry.position()).recordInfo();
        require(header.getJournalCode() == 'U' && header.getEntryType().equals("DN") && header.getCommitCycle().signum() == 0
                && header.getFile().isEmpty() && header.getLibrary().isEmpty() && header.getMember().isEmpty()
                && info.objectType().isEmpty() && info.objectNameIndicator() == '0', "Handoff marker is not an object-free user entry");
        return header;
    }

    static String digest(byte[] raw, JournalPosition position) throws Exception {
        require(raw.length >= 236 && raw.length <= 1024 * 1024, "Bounded RJNE0200 copy required");
        var header = new EntryHeaderDecoder().decode(raw, 0);
        var input = ByteBuffer.wrap(raw);
        require(input.getInt(0) == 0 && input.getInt(12) == 0 && input.getInt(16) == 0 && header.getPointerHandle() == 0,
                "Owned inline copy without XA/LUW required");
        require(header.getSequenceNumber().equals(position.offset()), "Entry position differs from native sequence");
        var spans = new ArrayList<Span>();
        spans.add(new Span(0, 236));
        int receiver = input.getInt(20);
        if (receiver != 0) {
            span(spans, receiver, 32, raw.length);
            var text = new AS400Text(10, 37);
            require(text.toObject(raw, receiver).toString().trim().equals(position.receiver().name())
                    && text.toObject(raw, receiver + 10).toString().trim().equals(position.receiver().library())
                    && text.toObject(raw, receiver + 20).toString().trim().equals("*SYSBAS")
                    && Short.toUnsignedInt(input.getShort(receiver + 30)) == 1, "Different or unsupported receiver context");
        }
        byte[] nulls = null;
        int nullOffset = input.getInt(4);
        if (nullOffset != 0) {
            require(nullOffset >= 236 && nullOffset <= raw.length - 4, "Invalid null section");
            int length = span(spans, nullOffset, 4L + Integer.toUnsignedLong(input.getInt(nullOffset)), raw.length);
            nulls = Arrays.copyOfRange(raw, nullOffset, nullOffset + length);
        }
        byte[] specific = null;
        int dataOffset = input.getInt(8);
        if (dataOffset != 0) {
            require(dataOffset >= 236 && dataOffset <= raw.length - 16, "Invalid entry-specific section");
            int count = 0;
            for (int i = 0; i < 5; i++) {
                int digit = Byte.toUnsignedInt(raw[dataOffset + i]) - 0xf0;
                require(digit >= 0 && digit <= 9, "Invalid entry-specific length");
                count = count * 10 + digit;
            }
            int length = span(spans, dataOffset, 16L + count, raw.length);
            specific = Arrays.copyOfRange(raw, dataOffset, dataOffset + length);
        }
        spans.sort(Comparator.comparingInt(Span::start));
        int end = 0;
        for (Span part : spans) {
            require(part.start() >= end && part.start() - end < 16, "Overlapping sections or unexplained gap");
            zeroes(raw, end, part.start());
            end = part.end();
        }
        require(raw.length == end || raw.length == ((end + 15) / 16) * 16, "Unexplained trailing bytes");
        zeroes(raw, end, raw.length);
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            // Preserve the established comparison format used by retained evidence.
            out.writeUTF("fixture-rjne0200-content-v1");
            out.writeUTF(position.receiver().library());
            out.writeUTF(position.receiver().name());
            // Preserve every fixed source field, including reserved bytes, flags,
            // producer identity, timestamps, JIDs and nested level.
            out.write(raw, 24, 236 - 24);
            section(out, nulls);
            section(out, specific);
        }
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    }

    private static int span(ArrayList<Span> spans, int start, long length, int maximum) {
        require(start >= 236 && length >= 0 && (long) start + length <= maximum, "Unbounded RJNE0200 section");
        spans.add(new Span(start, start + (int) length));
        return (int) length;
    }

    private static void section(DataOutputStream out, byte[] section) throws Exception {
        out.writeInt(section == null ? -1 : section.length);
        if (section != null) {
            out.write(section);
        }
    }

    private static void zeroes(byte[] bytes, int from, int through) {
        for (int i = from; i < through; i++) {
            require(bytes[i] == 0, "Nonzero bytes outside documented sections");
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
