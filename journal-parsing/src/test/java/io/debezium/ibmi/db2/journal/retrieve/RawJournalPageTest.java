/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.Job;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.ServiceProgramCall;

import io.debezium.ibmi.db2.journal.retrieve.exception.RetrieveJournalException;

class RawJournalPageTest {
    private static final JournalReceiver RECEIVER = new JournalReceiver("RCV", "LIB");

    private static JournalPosition pos(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), RECEIVER);
    }

    @ParameterizedTest
    @ValueSource(strings = { "en-US", "de-DE", "da-DK", "ja-JP" })
    void shouldPreserveNativeNamesAndRequestsIndependentlyOfWorkerLocale(String language) throws Exception {
        var previous = java.util.Locale.getDefault();
        try {
            java.util.Locale.setDefault(java.util.Locale.forLanguageTag(language));
            byte[] data = page();
            new AS400Text(30, 37).toBytes("FILE#$@_  LIB#$@_   MEMBER@   ", data, 64 + 157);
            var first = RawJournalPage.decode(data, pos(75), pos(100), 1).entries().get(0);
            assertEquals("FILE#$@_", first.header().getFile());
            assertEquals("LIB#$@_", first.header().getLibrary());
            assertEquals("MEMBER@", first.header().getMember());
            var from = new JournalPosition(BigInteger.ONE, new JournalReceiver("RCV#@$", "LIB#@$"));
            var start = new JournalProcessedPosition(from, java.time.Instant.EPOCH, false);
            var parameters = new ParameterListBuilder(RawJournalPage.METADATA_CCSID).withJournal("JRN#@$", "LIB#@$")
                    .withRange(new PositionRange(false, start, from)).build();
            assertEquals("JRN#@$    LIB#@$    ", new AS400Text(20, 37).toObject(parameters[2].getInputData()));
            byte[] criteria = parameters[4].getInputData();
            String decoded = new AS400Text(criteria.length, 37).toObject(criteria).toString();
            assertTrue(decoded.contains("RCV#@$    LIB#@$    RCV#@$    LIB#@$    "));
            var decoder = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder(RawJournalPage.METADATA_CCSID);
            assertEquals(first.header().getFile(), decoder.decode(first.data(), 0).getFile());
        }
        finally {
            java.util.Locale.setDefault(previous);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 273, 500, 65535 })
    void shouldRejectUnqualifiedApiJobEncodingBeforeStartingTheNativeCall(int ccsid) throws Exception {
        var transport = mock(AS400.class);
        var reader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> transport)
                .withJournalInfo(new JournalInfo("JRN", "LIB", false)).build(), mock(JournalInfoRetrieval.class));
        var job = mock(Job.class);
        when(job.getCodedCharacterSetID()).thenReturn(ccsid);
        try (var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> when(call.getServerJob()).thenReturn(job))) {
            var failure = assertThrows(IllegalArgumentException.class, () -> reader.readRawPage(pos(75), pos(100), 100));
            assertTrue(failure.getMessage().contains("CCSID 37"));
            verify(calls.constructed().get(0), org.mockito.Mockito.never()).run();
            assertTrue(!reader.readState().registered());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 1, 10, 14, 16, 17 })
    void shouldPaginateAnOversizedNativeResponseAtTheFirstUnreturnedEntry(int budget) throws Exception {
        var complete = RawJournalPage.decode(page(), pos(75), pos(100), 100);
        var first = RawJournalPage.decode(page(), pos(75), pos(100), budget);
        assertEquals(budget, first.entries().size());
        var collected = new ArrayList<>(first.entries());
        if (budget < complete.entries().size()) {
            var next = complete.entries().get(budget).position();
            assertEquals(next, first.continuation().orElseThrow());
            var remaining = complete.entries().subList(budget, complete.entries().size());
            var second = RawJournalPage.decode(page(remaining.stream().map(RawJournalPage.Entry::data).toList()), next, pos(100), 100);
            collected.addAll(second.entries());
            assertTrue(second.continuation().isEmpty());
        }
        else {
            assertTrue(first.continuation().isEmpty());
        }
        assertEquals(complete.entries().stream().map(RawJournalPage.Entry::position).toList(), collected.stream().map(RawJournalPage.Entry::position).toList());
        for (int i = 0; i < collected.size(); i++) {
            assertArrayEquals(complete.entries().get(i).data(), collected.get(i).data());
        }
    }

    @Test
    void shouldRejectMalformedDeferredEntriesBeforeReturningAnyPrefix() throws Exception {
        byte[] bytes = page();
        int next = 64;
        for (int i = 0; i < 12; i++) {
            next += ByteBuffer.wrap(bytes).getInt(next);
        }
        ByteBuffer.wrap(bytes).putInt(next + 72, 1);
        assertThrows(IllegalArgumentException.class, () -> RawJournalPage.decode(bytes, pos(75), pos(100), 10));
    }

    @Test
    void ownsAllEntriesAndUsesNativeContinuationRatherThanArithmeticOrNumericGapRules() throws Exception {
        byte[] bytes = page();
        var decoded = RawJournalPage.decode(bytes, pos(75), pos(100), 100);
        assertEquals(17, decoded.entries().size());
        assertTrue(decoded.continuation().isEmpty());
        assertTrue(decoded.entries().get(14).position().offset().compareTo(decoded.entries().get(13).position().offset().add(BigInteger.ONE)) > 0);
        byte[] saved = decoded.entries().get(0).data();
        Arrays.fill(bytes, (byte) 0);
        Arrays.fill(decoded.entries().get(0).data(), (byte) 0);
        assertArrayEquals(saved, decoded.entries().get(0).data());
        decoded.entries().forEach(e -> assertEquals(e.data().length, e.header().getEndOffset()));
        bytes = page();
        text(bytes, 12, 1, "1");
        text(bytes, 13, 10, "RCV");
        text(bytes, 23, 10, "LIB");
        text(bytes, 33, 20, "97");
        assertEquals(pos(97), RawJournalPage.decode(bytes, pos(75), pos(100), 100).continuation().orElseThrow());
    }

    @ParameterizedTest
    @ValueSource(strings = { "returned-short", "returned-long", "count-negative", "count-excess", "count-missing", "count-extra", "offset",
            "indicator", "continuation-old", "continuation-future", "continuation-receiver", "continuation-malformed", "link",
            "sequence", "pointer", "zero-count-continuation", "truncated" })
    void rejectsMalformedPartialAndOversizedResponsesBeforeReturningEntries(String fault) throws Exception {
        byte[] bytes = page();
        int first = 64;
        switch (fault) {
            case "returned-short" -> ByteBuffer.wrap(bytes).putInt(0, 63);
            case "returned-long" -> ByteBuffer.wrap(bytes).putInt(0, bytes.length + 1);
            case "count-negative" -> ByteBuffer.wrap(bytes).putInt(8, -1);
            case "count-excess" -> ByteBuffer.wrap(bytes).putInt(8, 101);
            case "count-missing" -> ByteBuffer.wrap(bytes).putInt(8, 16);
            case "count-extra" -> ByteBuffer.wrap(bytes).putInt(8, 18);
            case "offset" -> ByteBuffer.wrap(bytes).putInt(4, 4);
            case "indicator" -> text(bytes, 12, 1, "2");
            case "link" -> ByteBuffer.wrap(bytes).putInt(first, Integer.MAX_VALUE);
            case "sequence" -> ByteBuffer.wrap(bytes).putLong(first + 24, 101);
            case "pointer" -> ByteBuffer.wrap(bytes).putInt(first + 72, 1);
            case "zero-count-continuation" -> {
                ByteBuffer.wrap(bytes).putInt(4, 0).putInt(8, 0);
                text(bytes, 12, 1, "1");
            }
            case "truncated" -> bytes = Arrays.copyOf(bytes, 63);
            default -> {
                text(bytes, 12, 1, "1");
                text(bytes, 13, 10, fault.equals("continuation-receiver") ? "OTHER" : "RCV");
                text(bytes, 23, 10, "LIB");
                text(bytes, 33, 20, switch (fault) {
                    case "continuation-old" -> "93";
                    case "continuation-future" -> "101";
                    case "continuation-malformed" -> "*FIRST";
                    default -> "97";
                });
            }
        }
        byte[] malformed = bytes;
        assertThrows(IllegalArgumentException.class, () -> RawJournalPage.decode(malformed, pos(75), pos(100), 100));
    }

    @Test
    void actualTransportRequestHasAllCdcTypesNoFileFilterAndPreservesVariableNullMaps() throws Exception {
        AS400 as400 = mock(AS400.class);
        var lifecycle = new JournalReadLifecycle();
        var reader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> as400)
                .withJournalInfo(new JournalInfo("JRN", "LIB", false)).withServerFiltering(true)
                .withIncludeFiles(List.of(new FileFilter("LIB", "TEST"))).build(), mock(JournalInfoRetrieval.class), lifecycle);
        var data = page();
        List<ProgramParameter[]> requests = new ArrayList<>();
        try (var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> {
            Job job = mock(Job.class);
            when(job.getCodedCharacterSetID()).thenReturn(37);
            when(call.getServerJob()).thenReturn(job);
            when(call.run()).thenReturn(true);
            when(call.getMessageList()).thenReturn(new AS400Message[0]);
            doAnswer(invocation -> {
                ProgramParameter[] p = invocation.getArgument(1);
                p[0] = mock(ProgramParameter.class);
                when(p[0].getOutputData()).thenReturn(data);
                p[5] = mock(ProgramParameter.class);
                when(p[5].getOutputData()).thenReturn(noError());
                requests.add(p);
                return null;
            }).when(call).setProgram(anyString(), org.mockito.ArgumentMatchers.any(ProgramParameter[].class));
        })) {
            assertEquals(17, reader.readRawPage(pos(75), pos(100), 100).entries().size());
            byte[] criteria = requests.get(0)[4].getInputData();
            int count = ByteBuffer.wrap(criteria).getInt(0);
            List<Integer> keys = new ArrayList<>();
            for (int i = 0, offset = 4; i < count; i++) {
                int length = ByteBuffer.wrap(criteria).getInt(offset);
                int key = ByteBuffer.wrap(criteria).getInt(offset + 4);
                keys.add(key);
                if (key == 7) {
                    assertEquals(4, ByteBuffer.wrap(criteria).getInt(offset + 12));
                    Set<String> codes = new java.util.HashSet<>();
                    for (int codeIndex = 0; codeIndex < 4; codeIndex++) {
                        codes.add(((String) new AS400Text(10).toObject(criteria, offset + 16 + codeIndex * 20)).trim());
                    }
                    assertEquals(Set.of("C", "D", "F", "R"), codes);
                }
                if (key == 15) {
                    assertEquals("*VARLEN", ((String) new AS400Text(10).toObject(criteria, offset + 12)).trim());
                }
                offset += length;
            }
            assertEquals(Set.of(1, 2, 4, 7, 15), Set.copyOf(keys));
            assertNull(reader.getPosition()); // No legacy scan/skip advancement.
            lifecycle.requestStop();
            assertThrows(Exception.class, () -> reader.readRawPage(pos(75), pos(100), 100));
            assertEquals(1, calls.constructed().size());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "CPF7053", "CPF9801", "CPF7062", "CPF9872" })
    void everyApiFailureRejectsWithoutAcceptingPartialDataOrSuffix(String code) throws Exception {
        AS400 as400 = mock(AS400.class);
        var reader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> as400)
                .withJournalInfo(new JournalInfo("JRN", "LIB", false)).build(), mock(JournalInfoRetrieval.class));
        try (var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> {
            Job job = mock(Job.class);
            when(job.getCodedCharacterSetID()).thenReturn(37);
            when(call.getServerJob()).thenReturn(job);
            AS400Message message = mock(AS400Message.class);
            when(message.getID()).thenReturn(code);
            when(call.getMessageList()).thenReturn(new AS400Message[]{ message });
        })) {
            assertThrows(RetrieveJournalException.class, () -> reader.readRawPage(pos(75), pos(100), 100));
            assertNull(reader.getPosition());
        }
    }

    @ParameterizedTest
    @CsvSource({ "false,controls", "false,maintenance", "false,empty", "false,unrequested", "false,pointer", "false,api-error",
            "true,controls", "true,maintenance", "true,empty", "true,unrequested", "true,pointer", "true,api-error", "true,unknown" })
    void historySelectionsCannotBypassRawFailuresOrTerminalStop(boolean audit, String scenario) throws Exception {
        AS400 as400 = mock(AS400.class);
        var lifecycle = new JournalReadLifecycle();
        var reader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> as400)
                .withJournalInfo(new JournalInfo("JRN", "LIB", false)).withServerFiltering(true)
                .withIncludeFiles(List.of(new FileFilter("LIB", "TEST"))).build(), mock(JournalInfoRetrieval.class), lifecycle);
        byte[] response = scenario.equals("empty") ? nativeHex("empty-response")
                : scenario.equals("unrequested") ? page()
                        : page(RawJournalPage.decode(page(), pos(75), pos(100), 100).entries().stream()
                                .filter(e -> e.header().getJournalCode() == 'C').map(RawJournalPage.Entry::data).toList());
        if (scenario.equals("maintenance")) {
            text(response, 64 + 98, 1, "J"); // Synthetic unexpected-maintenance discovery, never a data row.
            text(response, 64 + 99, 2, "RR");
        }
        if (scenario.equals("unknown")) {
            text(response, 64 + 98, 1, "Y"); // Code absent from JournalCode enum must remain visible to the audit.
            text(response, 64 + 99, 2, "ZZ");
        }
        if (scenario.equals("pointer")) {
            ByteBuffer.wrap(response).putInt(64 + 72, 13);
        }
        List<ProgramParameter[]> requests = new ArrayList<>();
        try (var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> {
            Job job = mock(Job.class);
            when(job.getCodedCharacterSetID()).thenReturn(37);
            when(call.getServerJob()).thenReturn(job);
            when(call.run()).thenReturn(true);
            when(call.getMessageList()).thenReturn(new AS400Message[0]);
            doAnswer(invocation -> {
                ProgramParameter[] p = invocation.getArgument(1);
                p[0] = mock(ProgramParameter.class);
                when(p[0].getOutputData()).thenReturn(response);
                p[5] = mock(ProgramParameter.class);
                byte[] error = scenario.equals("empty") || scenario.equals("api-error") ? nativeHex("empty-error") : noError();
                when(p[5].getOutputData()).thenReturn(error);
                requests.add(p);
                return null;
            }).when(call).setProgram(anyString(), org.mockito.ArgumentMatchers.any(ProgramParameter[].class));
        })) {
            if (Set.of("pointer", "api-error").contains(scenario) || !audit && scenario.equals("unrequested")) {
                assertThrows(Exception.class, () -> {
                    if (audit) {
                        reader.readAuditHistoryPage(pos(75), pos(100), 100);
                    }
                    else {
                        reader.readControlHistoryPage(pos(75), pos(100), 100);
                    }
                });
            }
            else {
                var entries = audit ? reader.readAuditHistoryPage(pos(75), pos(100), 100).entries()
                        : reader.readControlHistoryPage(pos(75), pos(100), 100).entries();
                assertEquals(scenario.equals("empty"), entries.isEmpty());
                if (!audit) {
                    assertTrue(entries.stream().allMatch(e -> Set.of('C', 'J').contains(e.header().getJournalCode())));
                }
                if (scenario.equals("unknown")) {
                    assertEquals('Y', entries.get(0).header().getJournalCode());
                }
                if (scenario.equals("maintenance")) {
                    assertEquals("RR", entries.get(0).header().getEntryType());
                }
            }
            assertNull(reader.getPosition());
            byte[] criteria = requests.get(0)[4].getInputData();
            Set<Integer> keys = new java.util.HashSet<>();
            for (int i = 0, offset = 4; i < ByteBuffer.wrap(criteria).getInt(0); i++) {
                int key = ByteBuffer.wrap(criteria).getInt(offset + 4);
                keys.add(key);
                if (key == 7) {
                    assertEquals(2, ByteBuffer.wrap(criteria).getInt(offset + 12));
                    assertEquals("C", ((String) new AS400Text(10).toObject(criteria, offset + 16)).trim());
                    assertEquals("J", ((String) new AS400Text(10).toObject(criteria, offset + 36)).trim());
                }
                offset += ByteBuffer.wrap(criteria).getInt(offset);
            }
            assertEquals(audit ? Set.of(1, 2, 4, 15) : Set.of(1, 2, 4, 7, 15), keys); // No entry count/type, file, job, program or user filters.
            lifecycle.requestStop();
            assertThrows(Exception.class, () -> {
                if (audit) {
                    reader.readAuditHistoryPage(pos(75), pos(100), 100);
                }
                else {
                    reader.readControlHistoryPage(pos(75), pos(100), 100);
                }
            });
            assertEquals(1, calls.constructed().size());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failedOrPointerBackedRawResponseCancelsOnlyItsCapturedJobBeforeUnregistering(boolean pointer) throws Exception {
        AS400 transport = mock(AS400.class);
        Job captured = mock(Job.class);
        when(captured.getCodedCharacterSetID()).thenReturn(37);
        when(captured.getName()).thenReturn("QZRCSRVS");
        when(captured.getUser()).thenReturn("FIXTURE");
        when(captured.getNumber()).thenReturn("123456");
        var reader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> transport)
                .withJournalInfo(new JournalInfo("JRN", "LIB", false)).build(), mock(JournalInfoRetrieval.class));
        byte[] data = page();
        ByteBuffer.wrap(data).putInt(64 + 72, 13);
        List<List<?>> jobArguments = new ArrayList<>();
        List<List<?>> connectionArguments = new ArrayList<>();
        try (var connections = mockConstruction(AS400.class, (connection, context) -> connectionArguments.add(context.arguments()));
                var jobs = mockConstruction(Job.class, (job, context) -> jobArguments.add(context.arguments()));
                var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> {
                    when(call.getServerJob()).thenReturn(captured);
                    when(call.run()).thenReturn(pointer);
                    AS400Message error = mock(AS400Message.class);
                    when(error.getID()).thenReturn("CPF7062");
                    when(call.getMessageList()).thenReturn(pointer ? new AS400Message[0] : new AS400Message[]{ error });
                    doAnswer(invocation -> {
                        ProgramParameter[] p = invocation.getArgument(1);
                        p[0] = mock(ProgramParameter.class);
                        when(p[0].getOutputData()).thenReturn(data);
                        p[5] = mock(ProgramParameter.class);
                        when(p[5].getOutputData()).thenReturn(noError());
                        return null;
                    }).when(call).setProgram(anyString(), org.mockito.ArgumentMatchers.any(ProgramParameter[].class));
                })) {
            assertThrows(Exception.class, () -> reader.readRawPage(pos(75), pos(100), 100));
            assertEquals(List.of(List.of(transport)), connectionArguments);
            assertEquals(List.of(List.of(connections.constructed().get(0), "QZRCSRVS", "FIXTURE", "123456")), jobArguments);
            verify(jobs.constructed().get(0)).end(0);
            verify(connections.constructed().get(0)).disconnectAllServices();
            reader.cancelJob(); // Cleared registration cannot cancel an unrelated/replacement job.
            assertEquals(1, jobs.constructed().size());
            assertNull(reader.getPosition());
        }
    }

    private static byte[] noError() {
        byte[] error = new byte[256];
        ByteBuffer.wrap(error).putInt(256);
        return error;
    }

    private static byte[] creation(long sequence) throws Exception {
        return pointer("file-create-pointer", sequence);
    }

    private static byte[] pointer(String name, long sequence) throws Exception {
        try (var stream = RawJournalPageTest.class.getResourceAsStream("/" + name + ".hex")) {
            var entry = HexFormat.of().parseHex(new String(stream.readAllBytes(), StandardCharsets.UTF_8).lines()
                    .filter(line -> !line.startsWith("#")).reduce("", String::concat));
            ByteBuffer.wrap(entry).putLong(24, sequence);
            return entry;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "file-create-pointer", "excluded-lob-pointer" })
    void shouldReleaseEveryHandleIncludingEntriesDeferredByThePageBudget(String kind) throws Exception {
        var first = pointer(kind, 75);
        var second = pointer(kind, 76);
        ByteBuffer.wrap(second).putInt(72, 14);
        var released = new ArrayList<Long>();
        var result = RawJournalPage.decode(page(List.of(first, second)), pos(75), pos(100), 1, released::add);
        assertEquals(List.of(13L, 14L), released);
        assertEquals(1, result.entries().size());
        assertEquals(pos(76), result.continuation().orElseThrow());
        assertEquals(kind.equals("file-create-pointer") ? "CT" : "PT", result.entries().get(0).header().getEntryType());
    }

    @Test
    void shouldReleaseLogicalCreationHandlesOnlyWhenTheNativeFlagAndAttributeAgree() throws Exception {
        var entry = creation(75);
        var header = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder(37).decode(entry, 0);
        int attribute = header.getEntrySpecificDataOffset() + 16 + 124;
        entry[218] |= 0x04;
        var released = new ArrayList<Long>();
        assertThrows(RuntimeException.class, () -> RawJournalPage.decode(page(List.of(entry)), pos(75), pos(100), 1, released::add));
        assertTrue(released.isEmpty());
        text(entry, attribute, 10, "LF");
        var result = RawJournalPage.decode(page(List.of(entry)), pos(75), pos(100), 1, released::add);
        assertEquals(List.of(13L), released);
        assertTrue(result.entries().get(0).header().getControlInfo().logicalFile());
    }

    @ParameterizedTest
    @ValueSource(strings = { "row", "member", "flags", "subtype", "identity", "not-logged", "truncated", "release" })
    void shouldRejectUnqualifiedCreationPointersBeforeReturningAResponse(String fault) throws Exception {
        var entry = creation(75);
        var header = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder(37).decode(entry, 0);
        int payload = header.getEntrySpecificDataOffset() + 16;
        switch (fault) {
            case "row" -> {
                text(entry, 98, 1, "R");
                text(entry, 99, 2, "PT");
            }
            case "member" -> {
                text(entry, 98, 1, "F");
                text(entry, 99, 2, "MC");
            }
            case "flags" -> entry[218] ^= 1;
            case "subtype" -> text(entry, payload + 108, 1, "7");
            case "identity" -> text(entry, payload, 10, "OTHER");
            case "not-logged" -> text(entry, payload + 142, 1, "1");
            case "truncated" -> text(entry, payload - 16, 5, "00143");
            default -> {
            }
        }
        var released = new ArrayList<Long>();
        assertThrows(RuntimeException.class, () -> RawJournalPage.decode(page(List.of(entry)), pos(75), pos(100), 10, handle -> {
            if (fault.equals("release")) {
                throw new IllegalStateException("cleanup failed");
            }
            released.add(handle);
        }));
        assertTrue(released.isEmpty());
    }

    @ParameterizedTest
    @CsvSource({ "file-create-pointer,success", "file-create-pointer,release-failed", "file-create-pointer,error-structure",
            "file-create-pointer,different-job", "file-create-pointer,stop-during-release-setup",
            "excluded-lob-pointer,success", "excluded-lob-pointer,release-failed", "excluded-lob-pointer,error-structure",
            "excluded-lob-pointer,different-job", "excluded-lob-pointer,stop-during-release-setup" })
    void shouldReleasePointersOnTheRetrievingJobOrCancelWithoutProgress(String kind, String scenario) throws Exception {
        AS400 transport = mock(AS400.class);
        Job captured = mock(Job.class);
        when(captured.getCodedCharacterSetID()).thenReturn(37);
        when(captured.getName()).thenReturn("QZRCSRVS");
        when(captured.getUser()).thenReturn("FIXTURE");
        when(captured.getNumber()).thenReturn("123456");
        Job other = mock(Job.class);
        when(other.getName()).thenReturn("DIFFERENT");
        var lifecycle = new JournalReadLifecycle();
        var reader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> transport)
                .withJournalInfo(new JournalInfo("JRN", "LIB", false)).build(), mock(JournalInfoRetrieval.class), lifecycle);
        var response = page(List.of(pointer(kind, 75)));
        var requests = new ArrayList<ProgramParameter[]>();
        try (var connections = mockConstruction(AS400.class);
                var jobs = mockConstruction(Job.class);
                var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> {
                    when(call.getServerJob()).thenReturn(captured, scenario.equals("different-job") ? other : captured);
                    when(call.run()).thenReturn(true, !scenario.equals("release-failed"));
                    when(call.getMessageList()).thenReturn(new AS400Message[0]);
                    doAnswer(invocation -> {
                        if (scenario.equals("stop-during-release-setup") && invocation.getArgument(0).equals("QjoDeletePointerHandle")) {
                            lifecycle.requestStop();
                        }
                        return null;
                    }).when(call).setProcedureName(anyString());
                    doAnswer(invocation -> {
                        ProgramParameter[] p = invocation.getArgument(1);
                        requests.add(p);
                        if (p.length == 6) {
                            p[0] = mock(ProgramParameter.class);
                            when(p[0].getOutputData()).thenReturn(response);
                            p[5] = mock(ProgramParameter.class);
                            when(p[5].getOutputData()).thenReturn(noError());
                        }
                        else {
                            assertEquals(2, p.length);
                            assertEquals(13, ByteBuffer.wrap(p[0].getInputData()).getInt());
                            p[1] = mock(ProgramParameter.class);
                            byte[] error = noError();
                            if (scenario.equals("error-structure")) {
                                ByteBuffer.wrap(error).putInt(4, 32);
                            }
                            when(p[1].getOutputData()).thenReturn(error);
                        }
                        return null;
                    }).when(call).setProgram(anyString(), org.mockito.ArgumentMatchers.any(ProgramParameter[].class));
                })) {
            if (scenario.equals("success")) {
                assertEquals(1, reader.readRawPage(pos(75), pos(100), 1).entries().size());
                assertTrue(jobs.constructed().isEmpty());
            }
            else {
                assertThrows(IllegalStateException.class, () -> reader.readRawPage(pos(75), pos(100), 1));
                assertEquals(1, jobs.constructed().size());
                verify(jobs.constructed().get(0)).end(0);
            }
            assertEquals(scenario.equals("different-job") ? 1 : 2, requests.size());
            verify(calls.constructed().get(0), org.mockito.Mockito.times(
                    scenario.equals("different-job") || scenario.equals("stop-during-release-setup") ? 1 : 2)).run();
            if (!scenario.equals("different-job")) {
                verify(calls.constructed().get(0)).setProcedureName("QjoDeletePointerHandle");
            }
            assertNull(reader.getPosition());
            assertTrue(!reader.readState().registered());
        }
    }

    private static byte[] nativeHex(String name) throws Exception {
        try (var input = RawJournalPageTest.class.getResourceAsStream("/raw-journal/" + name + ".hex")) {
            String hex = new String(input.readAllBytes(), StandardCharsets.UTF_8).lines().filter(line -> !line.startsWith("#")).reduce("", String::concat);
            return HexFormat.of().parseHex(hex);
        }
    }

    @Test
    void nativeStructuredNoMatchIsACompleteEmptySelectionNotAFailedTransportOrMissingReceiver() throws Exception {
        var empty = RawJournalPage.decodeResult(nativeHex("empty-response"), nativeHex("empty-error"), pos(46), pos(46), 100);
        assertEquals(64, empty.bytesReturned());
        assertTrue(empty.entries().isEmpty());
        assertTrue(empty.continuation().isEmpty());
        assertEquals(17, RawJournalPage.decodeResult(page(), noError(), pos(75), pos(100), 100).entries().size());
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-error", "short-error", "wrong-size", "truncated-details", "negative-details", "short-details",
            "stale-success", "missing-receiver", "invalid-range", "other-error", "partial-data", "continuation", "short-response", "missing-response",
            "extra-returned-bytes" })
    void structuredErrorAndDataMustAgreeBeforeAcceptingEmptySelection(String fault) throws Exception {
        byte[] error = nativeHex("empty-error");
        byte[] response = nativeHex("empty-response");
        switch (fault) {
            case "missing-error" -> error = null;
            case "short-error" -> error = Arrays.copyOf(error, 15);
            case "wrong-size" -> ByteBuffer.wrap(error).putInt(0, 255);
            case "truncated-details" -> ByteBuffer.wrap(error).putInt(4, 257);
            case "negative-details" -> ByteBuffer.wrap(error).putInt(4, -1);
            case "short-details" -> ByteBuffer.wrap(error).putInt(4, 15);
            case "stale-success" -> ByteBuffer.wrap(error).putInt(4, 0);
            case "missing-receiver" -> text(error, 8, 7, "CPF9801");
            case "invalid-range" -> text(error, 8, 7, "CPF7054");
            case "other-error" -> text(error, 8, 7, "CPF9872");
            case "partial-data" -> response = page();
            case "continuation" -> text(response, 12, 1, "1");
            case "short-response" -> response = Arrays.copyOf(response, 63);
            case "missing-response" -> response = null;
            case "extra-returned-bytes" -> {
                response = Arrays.copyOf(response, 65);
                ByteBuffer.wrap(response).putInt(0, 65);
            }
            default -> throw new AssertionError(fault);
        }
        byte[] rejectedError = error;
        byte[] rejectedResponse = response;
        assertThrows(Exception.class, () -> RawJournalPage.decodeResult(rejectedResponse, rejectedError, pos(1), pos(100), 100));
    }

    private static byte[] page() throws Exception {
        List<byte[]> entries = new ArrayList<>();
        try (var input = RawJournalPageTest.class.getResourceAsStream("/raw-journal/native-entries.hex")) {
            new String(input.readAllBytes(), StandardCharsets.UTF_8).lines().filter(line -> !line.startsWith("#") && !line.isBlank())
                    .forEach(line -> entries.add(HexFormat.of().parseHex(line)));
        }
        return page(entries);
    }

    private static byte[] page(List<byte[]> entries) {
        byte[] page = new byte[64 + entries.stream().mapToInt(e -> e.length).sum()];
        ByteBuffer.wrap(page).putInt(0, page.length).putInt(4, 64).putInt(8, entries.size());
        text(page, 12, 1, "0");
        int offset = 64;
        for (int i = 0; i < entries.size(); i++) {
            byte[] e = entries.get(i);
            System.arraycopy(e, 0, page, offset, e.length);
            ByteBuffer.wrap(page).putInt(offset, i == entries.size() - 1 ? 0 : e.length);
            offset += e.length;
        }
        return page;
    }

    private static void text(byte[] target, int offset, int length, String value) {
        System.arraycopy(new AS400Text(length).toBytes(value), 0, target, offset, length);
    }
}
