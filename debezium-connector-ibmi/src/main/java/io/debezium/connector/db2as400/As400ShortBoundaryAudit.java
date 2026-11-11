/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/** Raw A/N/Z validation shared by native collection and retained-fixture replay. */
final class As400ShortBoundaryAudit {
    private static final Charset CCSID37 = Charset.forName("Cp037");

    record Completion(String job, BigInteger thread, LocalDateTime acquired, LocalDateTime released) {
        Completion {
            require(job != null && job.matches("[0-9]{6}/[A-Z0-9_#$@]{1,10}/[A-Z0-9_#$@]{1,10}")
                    && thread != null && thread.signum() > 0 && thread.bitLength() <= 64, "Complete native holder identity required");
            require(acquired != null && released != null && !released.isBefore(acquired)
                    && Duration.between(acquired, released).compareTo(Duration.ofSeconds(1)) <= 0, "Pulse exceeded the qualified hold bound");
        }
    }

    static RetrieveJournal.AuditPage attemptInterval(As400ShortBoundaryProgram program, As400NativeJournalWindow.Window window,
                                                     RetrieveJournal.AuditPage page)
            throws java.io.IOException {
        require(page.continuation().isEmpty() && !page.entries().isEmpty() && page.entries().size() <= 2000,
                "Complete bounded raw audit required before locating Z");
        byte[] closing = ("DCGB1 " + program.nonce() + " Z " + program.scopeDigest()).getBytes(CCSID37);
        byte[] opening = ("DCGB1 " + program.nonce() + " A").getBytes(CCSID37);
        BigInteger previous = null;
        int start = -1, end = -1;
        long bytes = 0;
        for (int i = 0; i < page.entries().size(); i++) {
            var entry = page.entries().get(i);
            byte[] data = entry.data();
            require(data.length <= 1024 * 1024 && (bytes += data.length) <= 16 * 1024 * 1024
                    && entry.position().receiver().equals(window.from().receiver())
                    && entry.position().offset().compareTo(window.from().offset()) >= 0
                    && entry.position().offset().compareTo(window.through().offset()) <= 0
                    && (previous == null || entry.position().offset().compareTo(previous) > 0),
                    "Unordered, foreign or excessive raw audit suffix at " + entry.position());
            previous = entry.position().offset();
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(data, 0);
            As400SourceNormalizer.validateSource(header, data, 0, entry.position());
            if (i > 0 && header.getJournalCode() == 'U' && header.getEntryType().equals("DN")
                    && Arrays.equals(payload(data, header.getEntrySpecificDataOffset()), closing)) {
                require(end == -1, "Repeated closing marker");
                end = i + 1;
            }
            if (i > 0 && header.getJournalCode() == 'U' && header.getEntryType().equals("DN")
                    && Arrays.equals(payload(data, header.getEntrySpecificDataOffset()), opening)) {
                require(start == -1, "Repeated opening marker");
                start = i - 1; // Existing inclusive anchor immediately before A.
            }
        }
        require(start >= 0 && end > start + 3 && page.entries().get(0).position().equals(window.from())
                && page.entries().get(page.entries().size() - 1).position().equals(window.through()), "Missing markers or incomplete raw endpoint");
        // QjoRetrieveJournalEntries omits some internal entries. A gap before A
        // or after Z does not weaken this observed boundary. Keep the stronger
        // no-gap requirement over the inclusive anchor and A..Z itself; unknown
        // omissions there still reject, even if a different API could explain them.
        BigInteger expected = page.entries().get(start).position().offset();
        for (var entry : page.entries().subList(start, end)) {
            require(entry.position().offset().equals(expected), "Incomplete raw A/Z audit interval");
            expected = expected.add(BigInteger.ONE);
        }
        return new RetrieveJournal.AuditPage(page.entries().subList(start, end), java.util.Optional.empty(), page.bytesReturned());
    }

    static JournalPosition validate(As400ShortBoundaryProgram program, Completion completion,
                                    As400NativeJournalWindow.Window window, RetrieveJournal.AuditPage page,
                                    List<As400BindingDefinition> definitions, JournalPosition metadataBefore, JournalPosition metadataAfter)
            throws Exception {
        var from = window.from();
        var through = window.through();
        require(program.journal().equals(window.before().directory().journal()) && from.receiver().equals(through.receiver()),
                "Foreign journal or receiver rotation during short boundary");
        require(page.continuation().isEmpty() && page.entries().size() >= 4 && page.entries().size() <= 2000,
                "Single complete bounded all-code audit required");
        var targets = definitions.stream().map(d -> new As400ShortBoundaryProgram.Target(d.identity().tableId().schema(),
                d.identity().physicalFile(), d.identity().member())).toList();
        require(targets.equals(program.targets()), "Program scope differs from acquired native bindings");
        var payloads = List.of("DCGB1 " + program.nonce() + " A", program.payload(), "DCGB1 " + program.nonce() + " Z " + program.scopeDigest());
        var markers = new ArrayList<JournalPosition>();
        BigInteger expected = from.offset();
        long totalBytes = 0;
        for (var entry : page.entries()) {
            byte[] bytes = entry.data();
            require(bytes.length <= 1024 * 1024 && (totalBytes += bytes.length) <= 16 * 1024 * 1024, "Excessive raw audit bytes");
            require(entry.position().receiver().equals(from.receiver()) && entry.position().offset().equals(expected), "Missing, reordered or foreign audit entry");
            expected = expected.add(BigInteger.ONE);
            if (entry.position().equals(from)) {
                continue; // The already-existing inclusive API anchor is outside (from,through].
            }
            var header = new EntryHeaderDecoder(RawJournalPage.METADATA_CCSID).decode(bytes, 0);
            var source = As400SourceNormalizer.validateSource(header, bytes, 0, entry.position());
            if (As400JournalLifecycle.administrative(new As400JournalLifecycle.Entry(header, bytes, entry.position()),
                    (file, library) -> definitions.stream()
                            .filter(d -> d.identity().physicalFile().equals(file) && d.identity().tableId().schema().equals(library))
                            .findFirst().map(d -> d.binding(1024 * 1024)).orElse(null))) {
                require(!markers.isEmpty() && markers.size() < 3, "Administrative work is outside A/Z audit bracket");
                continue;
            }
            if (header.getJournalCode() == 'R' || header.getJournalCode() == 'C') {
                require(markers.size() > 0 && markers.size() < 3, "Concurrent work is outside A/Z audit bracket");
                var info = source.recordInfo();
                require(info.threadId().signum() > 0 && info.jobNumber().matches("[0-9]{6}") && !info.jobNumber().equals("000000")
                        && !info.jobUser().isBlank() && !info.jobName().isBlank(), "Missing concurrent producer");
                if (header.getJournalCode() == 'C') {
                    // Full control lifetime and retained history are checked by
                    // the existing prospective adapter, never inferred from N.
                    new TransactionControlEntryDecoder().decode(header, bytes, 0);
                }
                else {
                    require(Set.of("PT", "PX", "UB", "UP", "DL").contains(header.getEntryType()), "Unsupported concurrent row effect");
                    var definition = definitions.stream()
                            .filter(d -> d.identity().physicalFile().equals(header.getFile()) && d.identity().tableId().schema().equals(header.getLibrary()))
                            .findFirst().orElseThrow(() -> new IllegalArgumentException("Concurrent row has unknown native binding"));
                    require(info.objectType().equals("*QDDS") && info.objectNameIndicator() == '1'
                            && info.objectJournalId().equals(definition.identity().memberJournalId()) && header.getMember().equals(definition.identity().member())
                            && info.recordNumber().signum() > 0, "Concurrent row has different member identity");
                    definition.binding(1024 * 1024).values(header, bytes, 0);
                }
                continue;
            }
            require(header.getJournalCode() == 'U' && header.getEntryType().equals("DN") && markers.size() < 3,
                    "Audit contains maintenance or foreign markers; retry boundary");
            var info = source.recordInfo();
            require(header.getCommitCycle().signum() == 0 && header.getFile().isEmpty() && header.getLibrary().isEmpty() && header.getMember().isEmpty()
                    && info.objectType().isEmpty() && info.objectNameIndicator() == '0'
                    && (info.jobNumber() + "/" + info.jobUser() + "/" + info.jobName()).equals(completion.job())
                    && info.threadId().equals(completion.thread()), "Foreign marker producer or object");
            require(Arrays.equals(payload(bytes, header.getEntrySpecificDataOffset()), payloads.get(markers.size()).getBytes(CCSID37)),
                    "Raw marker payload differs from attempted allocation");
            if (markers.size() == 1) {
                require(!header.getTime().isBefore(completion.acquired().toInstant(java.time.ZoneOffset.UTC))
                        && !header.getTime().isAfter(completion.released().toInstant(java.time.ZoneOffset.UTC)), "N is outside native held interval");
            }
            markers.add(entry.position());
        }
        require(markers.size() == 3 && markers.get(2).equals(through) && expected.equals(through.offset().add(BigInteger.ONE)), "Incomplete audit endpoints");
        require(metadataBefore.offset().compareTo(markers.get(0).offset()) >= 0
                && metadataBefore.offset().compareTo(markers.get(1).offset()) < 0
                && metadataAfter.offset().compareTo(markers.get(1).offset()) >= 0
                && metadataAfter.offset().compareTo(markers.get(2).offset()) < 0
                && metadataBefore.receiver().equals(from.receiver()) && metadataAfter.receiver().equals(from.receiver()), "Metadata is outside A/N/Z bracket");
        return markers.get(1);
    }

    private static byte[] payload(byte[] bytes, int offset) {
        require(offset >= 236 && offset + 16 <= bytes.length, "Missing marker prefix");
        int length = 0;
        for (int i = 0; i < 5; i++) {
            int digit = Byte.toUnsignedInt(bytes[offset + i]);
            require(digit >= 0xf0 && digit <= 0xf9, "Malformed marker length");
            length = length * 10 + digit - 0xf0;
        }
        require(length > 0 && length <= 512 && offset + 16 + length <= bytes.length
                && bytes.length <= offset + 16 + length + 15, "Invalid marker bounds");
        for (int i = offset + 5; i < offset + 16; i++) {
            require(bytes[i] == 0, "Nonzero reserved marker bytes");
        }
        for (int i = offset + 16 + length; i < bytes.length; i++) {
            require(bytes[i] == 0, "Nonzero marker alignment");
        }
        return Arrays.copyOfRange(bytes, offset + 16, offset + 16 + length);
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
