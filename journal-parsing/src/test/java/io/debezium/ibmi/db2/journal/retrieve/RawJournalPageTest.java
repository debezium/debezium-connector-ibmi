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
            when(call.getServerJob()).thenReturn(mock(Job.class));
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
            when(call.getServerJob()).thenReturn(mock(Job.class));
            AS400Message message = mock(AS400Message.class);
            when(message.getID()).thenReturn(code);
            when(call.getMessageList()).thenReturn(new AS400Message[]{ message });
        })) {
            assertThrows(RetrieveJournalException.class, () -> reader.readRawPage(pos(75), pos(100), 100));
            assertNull(reader.getPosition());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failedOrPointerBackedRawResponseCancelsOnlyItsCapturedJobBeforeUnregistering(boolean pointer) throws Exception {
        AS400 transport = mock(AS400.class);
        Job captured = mock(Job.class);
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
