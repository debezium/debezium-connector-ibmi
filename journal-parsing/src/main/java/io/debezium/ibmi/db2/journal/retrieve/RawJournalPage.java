/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.exception.RetrieveJournalException;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Owned, structurally validated RJNE0200 response for one explicitly named receiver.
 * A successful API response establishes completion of its fixed selection, not
 * source incarnation, retention, historical schema or transaction completeness.
 * Numeric gaps are legal; consumers must separately validate receiver continuity.
 */
public record RawJournalPage(List<Entry> entries, Optional<JournalPosition> continuation, int bytesReturned) {

    static final int ERROR_BYTES = 256;

    public RawJournalPage {
        entries = List.copyOf(entries);
    }

    public record Entry(JournalPosition position, EntryHeader header, byte[] data) {
        public Entry {
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }

    /**
     * QUS error-code output is separate from transport success. Only CPF7062 with
     * a complete empty RJNE0200 header denotes an empty fixed selection. Never
     * infer this from a failed call (JT400 may withhold its output), another error,
     * partial data, or a continuation flag on an errored response. The caller must
     * still verify source identity/ranges and any required F/W anchors.
     */
    static RawJournalPage decodeResult(byte[] response, byte[] error, JournalPosition from, JournalPosition through, int maximumEntries) {
        require(error != null && error.length == ERROR_BYTES, "Missing or truncated journal API error output");
        ByteBuffer fields = ByteBuffer.wrap(error);
        require(fields.getInt(0) == ERROR_BYTES, "Unexpected journal API error buffer size");
        int available = fields.getInt(4);
        require(available == 0 || available >= 16 && available <= error.length, "Invalid or truncated journal API error details");
        if (available == 0) {
            for (int i = 8; i < 16; i++) {
                require(error[i] == 0, "Inconsistent successful journal API error output");
            }
            return decode(response, from, through, maximumEntries);
        }
        String id = text(error, 8, 7);
        if (!id.equals("CPF7062")) {
            throw new RetrieveJournalException("Bounded raw journal API error; no entries accepted: " + id);
        }
        RawJournalPage empty = decode(response, from, through, maximumEntries);
        require(empty.bytesReturned() == 64 && empty.entries().isEmpty() && empty.continuation().isEmpty(),
                "No-match journal error conflicts with nonempty or incomplete response");
        return empty;
    }

    static RawJournalPage decode(byte[] response, JournalPosition from, JournalPosition through, int maximumEntries) {
        validateRange(from, through, maximumEntries);
        require(response != null && response.length >= 64, "Truncated RJNE0200 response");
        ByteBuffer prefix = ByteBuffer.wrap(response);
        int returned = prefix.getInt(0);
        int offset = prefix.getInt(4);
        int count = prefix.getInt(8);
        require(returned >= 64 && returned <= response.length && count >= 0 && count <= maximumEntries,
                "Invalid or excessive RJNE0200 response size");
        String more = text(response, 12, 1);
        require(more.equals("0") || more.equals("1"), "Unknown journal continuation indicator");
        require(count == 0 ? offset == 0 : offset >= 64 && (long) offset + 236 <= returned, "Invalid first entry boundary");
        require(count > 0 || more.equals("0"), "Buffer cannot hold the next complete entry; skipping is forbidden");
        byte[] bounded = Arrays.copyOf(response, returned);
        var decoder = new EntryHeaderDecoder();
        List<Entry> entries = new ArrayList<>();
        BigInteger previous = null;
        for (int i = 0; i < count; i++) {
            require(offset >= 64 && (long) offset + 236 <= returned, "Truncated journal entry header");
            EntryHeader header = decoder.decode(bounded, offset);
            BigInteger sequence = header.getSequenceNumber();
            require(sequence.compareTo(from.offset()) >= 0 && sequence.compareTo(through.offset()) <= 0
                    && (previous == null || sequence.compareTo(previous) > 0), "Journal entry outside range or out of order");
            require(!header.hasReceiver() || (header.getReceiver().equals(from.receiver().name())
                    && header.getReceiverLibrary().equals(from.receiver().library())), "Journal entry came from a different receiver");
            int next = header.getNextEntryOffset();
            require(i == count - 1 ? next == 0 : next >= 236 && (long) offset + next + 236 <= returned,
                    "Journal entry count and links disagree");
            require(header.getPointerHandle() == 0, "Pointer-backed journal data requires a different retrieval profile");
            byte[] owned = Arrays.copyOfRange(bounded, offset, offset + header.getEndOffset());
            // The entry no longer links into the reusable response buffer.
            ByteBuffer.wrap(owned).putInt(0, 0);
            EntryHeader copied = decoder.decode(owned, 0);
            entries.add(new Entry(new JournalPosition(sequence, from.receiver()), copied, owned));
            previous = sequence;
            offset += next;
        }
        Optional<JournalPosition> continuation = Optional.empty();
        if (more.equals("1")) {
            String receiver = text(response, 13, 10).trim();
            String library = text(response, 23, 10).trim();
            String digits = text(response, 33, 20).trim();
            require(receiver.equals(from.receiver().name()) && library.equals(from.receiver().library()) && digits.matches("[0-9]{1,20}"),
                    "Invalid single-receiver continuation");
            BigInteger next = new BigInteger(digits);
            require(previous != null && next.compareTo(previous) > 0 && next.compareTo(through.offset()) <= 0,
                    "Journal continuation regresses or escapes the bounded cut");
            continuation = Optional.of(new JournalPosition(next, from.receiver()));
        }
        return new RawJournalPage(entries, continuation, returned);
    }

    static void validateRange(JournalPosition from, JournalPosition through, int maximumEntries) {
        require(from != null && through != null && from.receiver() != null && from.receiver().equals(through.receiver())
                && from.offset() != null && through.offset() != null && from.offset().signum() > 0
                && from.offset().compareTo(through.offset()) <= 0 && through.offset().bitLength() <= 64
                && maximumEntries > 0 && maximumEntries <= 100_000, "Explicit bounded single-receiver interval required");
        for (String name : List.of(from.receiver().name(), from.receiver().library())) {
            require(name != null && name.matches("[A-Z0-9_#$@]{1,10}"), "Exact basic receiver names required");
        }
    }

    private static String text(byte[] bytes, int offset, int length) {
        return (String) new AS400Text(length).toObject(bytes, offset);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
