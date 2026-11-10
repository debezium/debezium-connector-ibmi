/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Message;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.Job;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.ServiceProgramCall;

import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400OnlineCensusCaptureTest {
    private static final JournalReceiver RECEIVER = new JournalReceiver("QSQJRN0002", "DCNC7103");

    @ParameterizedTest
    @CsvSource({ "4096,8,20000", "1048576,501,20000", "4096,8,7" })
    void bufferControlsNativeRequestWhileEntryBudgetOnlyRejectsOversizedResponses(int bufferBytes, int count, int budget) throws Exception {
        AS400 transport = mock(AS400.class);
        var reader = As400OnlineCensusCapture.reader(transport, new JournalInfo("QSQJRN", RECEIVER.library(), false),
                mock(JournalInfoRetrieval.class), bufferBytes);
        long continuation = 100 + count + 9; // Legal gap; follow the native coordinate.
        List<byte[]> responses = List.of(page(100, count, continuation), page(continuation, 1, 0));
        List<ProgramParameter[]> requests = new ArrayList<>();
        try (var connections = mockConstruction(AS400.class);
                var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> {
                    when(call.getServerJob()).thenReturn(mock(Job.class));
                    when(call.run()).thenReturn(true);
                    when(call.getMessageList()).thenReturn(new AS400Message[0]);
                    doAnswer(invocation -> {
                        ProgramParameter[] parameters = invocation.getArgument(1);
                        byte[] response = responses.get(requests.size());
                        assertTrue(response.length <= bufferBytes);
                        assertEquals(bufferBytes, ByteBuffer.wrap(parameters[1].getInputData()).getInt());
                        assertFalse(keys(parameters[4].getInputData()).contains(6), "NBRENT must not truncate this selection");
                        parameters[0] = mock(ProgramParameter.class);
                        when(parameters[0].getOutputData()).thenReturn(response);
                        byte[] error = new byte[256];
                        ByteBuffer.wrap(error).putInt(error.length);
                        parameters[5] = mock(ProgramParameter.class);
                        when(parameters[5].getOutputData()).thenReturn(error);
                        requests.add(parameters);
                        return null;
                    }).when(call).setProgram(anyString(), any(ProgramParameter[].class));
                })) {
            if (budget < count) {
                assertThrows(IllegalArgumentException.class, () -> As400OnlineCensusCapture.readPage(reader, pos(100), pos(continuation), budget));
                assertEquals(1, requests.size());
            }
            else {
                var first = As400OnlineCensusCapture.readPage(reader, pos(100), pos(continuation), budget);
                assertEquals(count, first.entries().size());
                assertEquals(pos(continuation), first.continuation().orElseThrow());
                var last = As400OnlineCensusCapture.readPage(reader, first.continuation().orElseThrow(), pos(continuation), budget - count);
                assertEquals(pos(continuation), last.entries().get(0).position());
                assertTrue(last.continuation().isEmpty());
                assertEquals(2, requests.size());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 7, 500, 4095, 1048577 })
    void rejectsEntryCountsMistakenForResponseBufferSizes(int bytes) {
        assertThrows(IllegalArgumentException.class, () -> As400OnlineCensusCapture.reader(mock(AS400.class),
                new JournalInfo("QSQJRN", RECEIVER.library(), false), mock(JournalInfoRetrieval.class), bytes));
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void auditOnlyOmitsSelectionAndPreservesTheRealRawReader(boolean audit) throws Exception {
        AS400 transport = mock(AS400.class);
        var journal = new JournalInfo("QSQJRN", RECEIVER.library(), false);
        var info = mock(JournalInfoRetrieval.class);
        var reader = As400OnlineCensusCapture.reader(transport, journal, info, 4096);
        try (var calls = mockConstruction(ServiceProgramCall.class, (call, context) -> {
            when(call.getServerJob()).thenReturn(mock(Job.class));
            when(call.run()).thenReturn(true);
            when(call.getMessageList()).thenReturn(new AS400Message[0]);
            doAnswer(invocation -> {
                ProgramParameter[] parameters = invocation.getArgument(1);
                var keys = keys(parameters[4].getInputData());
                assertEquals(!audit, keys.contains(7), "Only the explicit audit omits the code selection");
                assertFalse(keys.contains(6), "No count-limited selection");
                assertFalse(keys.contains(8), "No entry-type selection");
                assertFalse(keys.contains(16), "No file selection");
                assertFalse(keys.contains(17), "No object selection");
                parameters[0] = mock(ProgramParameter.class);
                when(parameters[0].getOutputData()).thenReturn(page(100, 1, 0));
                byte[] error = new byte[256];
                ByteBuffer.wrap(error).putInt(error.length);
                parameters[5] = mock(ProgramParameter.class);
                when(parameters[5].getOutputData()).thenReturn(error);
                return null;
            }).when(call).setProgram(anyString(), any(ProgramParameter[].class));
        })) {
            assertEquals(1, audit ? reader.readAuditHistoryPage(pos(100), pos(100), 10).entries().size()
                    : reader.readRawPage(pos(100), pos(100), 10).entries().size());
            assertEquals(1, calls.constructed().size());
        }
    }

    private static JournalPosition pos(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), RECEIVER);
    }

    private static List<Integer> keys(byte[] criteria) {
        ByteBuffer buffer = ByteBuffer.wrap(criteria);
        List<Integer> keys = new ArrayList<>();
        for (int index = 0, offset = 4; index < buffer.getInt(0); index++) {
            keys.add(buffer.getInt(offset + 4));
            offset += buffer.getInt(offset);
        }
        return keys;
    }

    /** Synthetic transport response using a retained header; not new native evidence. */
    private static byte[] page(long sequence, int count, long continuation) throws Exception {
        byte[] entry;
        try (var input = As400OnlineCensusCaptureTest.class.getResourceAsStream("/native-transactions/incremental-anchor.json")) {
            entry = HexFormat.of().parseHex(new ObjectMapper().readTree(input).get("subsequent").asText());
        }
        byte[] response = new byte[64 + count * entry.length];
        ByteBuffer buffer = ByteBuffer.wrap(response);
        buffer.putInt(0, response.length).putInt(4, 64).putInt(8, count);
        text(response, 12, 1, continuation == 0 ? "0" : "1");
        if (continuation != 0) {
            text(response, 13, 10, RECEIVER.name());
            text(response, 23, 10, RECEIVER.library());
            text(response, 33, 20, Long.toString(continuation));
        }
        for (int i = 0, offset = 64; i < count; i++, offset += entry.length) {
            System.arraycopy(entry, 0, response, offset, entry.length);
            buffer.putInt(offset, i == count - 1 ? 0 : entry.length).putLong(offset + 24, sequence + i);
        }
        return response;
    }

    private static void text(byte[] bytes, int offset, int length, String value) {
        System.arraycopy(new AS400Text(length).toBytes(value), 0, bytes, offset, length);
    }
}
