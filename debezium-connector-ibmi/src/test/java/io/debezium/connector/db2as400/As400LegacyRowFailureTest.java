/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.connector.db2as400.As400StreamingChangeEventSource.RowProcessingException;
import io.debezium.heartbeat.HeartbeatImpl;
import io.debezium.ibmi.db2.journal.retrieve.JdbcFileDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.relational.TableId;
import io.debezium.schema.AbstractTopicNamingStrategy;
import io.debezium.util.Clock;

/** Real legacy handler, dispatcher and RPC advancement boundary; no IBM i connection. */
class As400LegacyRowFailureTest {
    private static final As400Partition PARTITION = new As400Partition("test");

    @ParameterizedTest
    @ValueSource(strings = { "matched", "missing", "thread", "record", "member-id", "cycle", "overlap" })
    void migrationCannotUseAMissingOrForeignBeforeImageForTheOldKey(String mode) throws Exception {
        var entry = nativeRow("UP");
        byte[] bytes = entry.data();
        var header = entry.header();
        var receiver = entry.position().receiver();
        try (var f = new Fixture(Map.of(), false)) {
            when(f.decoder.decode(any(), any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(new Object[]{ 2 });
            if (!mode.equals("missing")) {
                byte[] before = bytes.clone();
                new com.ibm.as400.access.AS400Text(2, 37).toBytes("UB", before, 99);
                new com.ibm.as400.access.AS400UnsignedBin8().toBytes(BigInteger.valueOf(41), before, 24);
                var beforeHeader = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(before, 0);
                when(f.decoder.decode(any(), any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(new Object[]{ 1 }, new Object[]{ 2 });
                f.source.processOwnedEntry(PARTITION, f.offset, new io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry(
                        new io.debezium.ibmi.db2.journal.retrieve.JournalPosition(BigInteger.valueOf(41), receiver), beforeHeader, before));
                if (mode.equals("overlap")) {
                    assertThatThrownBy(() -> f.source.processOwnedEntry(PARTITION, f.offset,
                            new io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry(
                                    new io.debezium.ibmi.db2.journal.retrieve.JournalPosition(BigInteger.valueOf(41), receiver), beforeHeader, before)))
                            .isInstanceOf(RowProcessingException.class).hasRootCauseMessage("Overlapping migration before-images require explicit recovery");
                    assertThat(f.queue.poll()).isEmpty();
                    assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(40));
                    return;
                }
                switch (mode) {
                    case "thread" -> new com.ibm.as400.access.AS400UnsignedBin8().toBytes(header.getRecordInfo().threadId().add(BigInteger.ONE), bytes, 40);
                    case "record" -> new com.ibm.as400.access.AS400UnsignedBin8().toBytes(header.getRecordInfo().recordNumber().add(BigInteger.ONE), bytes, 56);
                    case "cycle" -> new com.ibm.as400.access.AS400UnsignedBin8().toBytes(header.getCommitCycle().add(BigInteger.ONE), bytes, 64);
                    case "member-id" -> bytes[197] ^= 1;
                    default -> {
                    }
                }
                var next = new io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry(entry.position(),
                        new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(bytes, 0), bytes);
                if (!mode.equals("matched")) {
                    assertThatThrownBy(() -> f.source.processOwnedEntry(PARTITION, f.offset, next))
                            .isInstanceOf(RowProcessingException.class)
                            .hasRootCauseMessage("Migration update before-image belongs to a different source row or producer");
                    assertThat(f.queue.poll()).isEmpty();
                    assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(40));
                    return;
                }
                f.source.processOwnedEntry(PARTITION, f.offset, next);
                var records = f.queue.poll();
                assertThat(records).hasSize(3);
                assertThat(((Struct) records.get(0).getRecord().value()).getString("op")).isEqualTo("d");
                assertThat(((Struct) records.get(0).getRecord().key()).getInt32("ID")).isEqualTo(1);
                assertThat(records.get(1).getRecord().value()).isNull();
                assertThat(((Struct) records.get(2).getRecord().value()).getString("op")).isEqualTo("c");
                assertThat(((Struct) records.get(2).getRecord().key()).getInt32("ID")).isEqualTo(2);
            }
            else {
                assertThatThrownBy(() -> f.source.processOwnedEntry(PARTITION, f.offset, entry))
                        .isInstanceOf(RowProcessingException.class).hasRootCauseMessage("Migration update lacks its before-image at journal entry 42");
                assertThat(f.queue.poll()).isEmpty();
            }
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(40));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "success", "decode", "false", "conversion", "lookup", "warn", "skip" })
    void ownedNativeEntryUsesTheSameRealDispatcherAndCannotAuthorizeFailedOrLossyProgress(String mode) throws Exception {
        var entry = nativeRow("PT");
        try (var f = new Fixture(java.util.List.of("warn", "skip").contains(mode)
                ? Map.of("event.processing.failure.handling.mode", mode)
                : Map.of(), false)) {
            when(f.decoder.decode(any(), any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(new Object[]{ 8 });
            if (mode.equals("decode")) {
                when(f.decoder.decode(any(), any(), org.mockito.ArgumentMatchers.anyInt())).thenThrow(new IOException("owned decode rejected"));
            }
            if (mode.equals("false")) {
                doReturn(false).when(f.dispatcher).dispatchDataChangeEvent(any(), any(), any());
            }
            if (mode.equals("conversion")) {
                when(f.decoder.decode(any(), any(), org.mockito.ArgumentMatchers.anyInt())).thenReturn(new Object[]{ "invalid integer" });
            }
            if (mode.equals("lookup")) {
                when(f.jdbc.getLongName(anyString(), anyString())).thenThrow(new IllegalStateException("missing table identity"));
            }
            if (mode.equals("success")) {
                f.source.processOwnedEntry(PARTITION, f.offset, entry);
                var record = f.queue.poll().get(0).getRecord();
                assertThat(((Struct) record.value()).getStruct("after").getInt32("ID")).isEqualTo(8);
                assertThat(((Struct) record.value()).getStruct("source").getString("sequence")).isEqualTo("42");
            }
            else {
                assertThatThrownBy(() -> f.source.processOwnedEntry(PARTITION, f.offset, entry)).isInstanceOf(RuntimeException.class);
                assertThat(f.queue.poll()).isEmpty();
            }
            // Only the caller may advance after successful dispatch; no RPC cursor
            // or producer acknowledgement is inferred from this owned copy.
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(40));
            verify(f.journal, never()).getPosition();
        }
    }

    @ParameterizedTest
    @CsvSource({ "PT,decode", "UB,decode", "PT,checked-decode", "PT,io-decode", "PT,dispatch", "PX,dispatch", "UP,dispatch", "DL,dispatch", "DR,dispatch",
            "PT,false", "UP,false", "DL,false", "UP,partial" })
    void failModeReachesErrorHandlerBeforeRpcPositionCanAdvance(String type, String failure) throws Exception {
        try (var f = new Fixture(Map.of(), failure.equals("partial"))) {
            switch (failure) {
                case "decode" -> when(f.journal.decode(f.decoder)).thenThrow(new IllegalArgumentException("malformed row"));
                case "checked-decode" -> when(f.journal.decode(f.decoder)).thenThrow(new SQLException("unsupported row layout"));
                case "io-decode" -> when(f.journal.decode(f.decoder)).thenThrow(new IOException("malformed row bytes"));
                case "dispatch" -> when(f.journal.decode(f.decoder)).thenReturn(new Object[]{ "wrong integer type" });
                case "false" -> doReturn(false).when(f.dispatcher).dispatchDataChangeEvent(any(), any(), any());
                case "partial" -> f.beforeImage(1, 2);
                default -> throw new AssertionError(failure);
            }
            f.run(type);
            var stopped = f.assertStopped();
            switch (failure) {
                case "decode" -> assertThat(stopped).hasRootCauseMessage("malformed row");
                case "checked-decode" -> assertThat(stopped).hasRootCauseMessage("unsupported row layout");
                case "io-decode" -> assertThat(stopped).hasRootCauseInstanceOf(IOException.class).hasRootCauseMessage("malformed row bytes");
                case "dispatch" -> assertThat(stopped).hasRootCauseInstanceOf(org.apache.kafka.connect.errors.DataException.class);
                case "partial" -> assertThat(stopped).hasRootCauseMessage("controlled dispatch failure after first subrecord");
                case "false" -> assertThat(stopped).hasMessageContaining("Failed to dispatch row").hasNoCause();
                default -> throw new AssertionError(failure);
            }
            var queued = f.queue.poll();
            if (failure.equals("partial")) {
                assertThat(queued).hasSize(1);
                SourceRecord first = queued.get(0).getRecord();
                assertThat(((Struct) first.value()).getString("op")).isEqualTo("d");
                assertThat(first.sourceOffset().get("offset.event_sequence")).isEqualTo("40");
                assertThat(first.sourceOffset().get(As400OffsetContext.PROCESSED)).isEqualTo("false");
                // Enqueue is not broker acknowledgement; the Kafka barrier tests
                // prove this prefix stays uncommitted when the producer fails.
            }
            else {
                assertThat(queued).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @CsvSource({ "warn,decode,PT", "skip,decode,PT", "warn,dispatch,PT", "skip,dispatch,PT", "warn,false,PT", "skip,false,PT",
            "warn,decode,UP", "skip,decode,UP", "warn,dispatch,UP", "skip,dispatch,UP", "warn,false,UP", "skip,false,UP" })
    void explicitLossyModesRetainTheirContinueAndAdvanceBehavior(String mode, String failure, String type) throws Exception {
        try (var f = new Fixture(Map.of("event.processing.failure.handling.mode", mode), false)) {
            if (type.equals("UP")) {
                f.beforeImage(1, 2);
            }
            switch (failure) {
                case "decode" -> when(f.journal.decode(f.decoder)).thenThrow(new IllegalArgumentException("malformed row"));
                case "dispatch" -> when(f.journal.decode(f.decoder)).thenReturn(new Object[]{ "wrong integer type" });
                case "false" -> doReturn(false).when(f.dispatcher).dispatchDataChangeEvent(any(), any(), any());
                default -> throw new AssertionError(failure);
            }
            f.run(type);
            verifyNoInteractions(f.errors);
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            assertThat(f.offset.getOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
            assertThat(f.source.hasPendingBeforeImage()).isFalse();
            assertThat(f.queue.poll()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "excluded-table", "skipped-create", "unchanged-update" })
    void intentionalSkipsAreNotFailures(String kind) throws Exception {
        Map<String, String> config = switch (kind) {
            case "excluded-table" -> Map.of("table.include.list", "LIB.OTHER");
            case "skipped-create" -> Map.of("skipped.operations", "c");
            default -> Map.of("skip.messages.without.change", "true");
        };
        try (var f = new Fixture(config, false)) {
            if (kind.equals("unchanged-update")) {
                f.beforeImage(1, 1);
            }
            f.run(kind.equals("unchanged-update") ? "UP" : "PT");
            verifyNoInteractions(f.errors);
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            assertThat(f.queue.poll()).isEmpty();
            if (kind.equals("excluded-table")) {
                verify(f.journal, never()).decode(any());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "io", "sql" })
    void connectionFailuresStillPropagateToTheExistingRetryPathWithoutAdvancing(String kind) throws Exception {
        try (var f = new Fixture(Map.of(), false)) {
            Exception failure = kind.equals("io") ? new IOException("transport") : new SQLNonTransientConnectionException("transport");
            if (kind.equals("io")) {
                when(f.journal.retrieveJournal(any())).thenThrow(failure);
            }
            else {
                when(f.journal.decode(f.decoder)).thenThrow(failure);
            }
            assertThatThrownBy(() -> f.rpc.getJournalEntries(f.running, f.offset,
                    f.source.processJournalEntries(PARTITION, f.offset), mock(WatchDog.class))).isSameAs(failure);
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(40));
            verify(f.journal, never()).getPosition();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "warn", "skip" })
    void lossyModesKeepTheirExistingDecoderIoRetryClassification(String mode) throws Exception {
        try (var f = new Fixture(Map.of("event.processing.failure.handling.mode", mode), false)) {
            var failure = new IOException("decoder IO");
            when(f.journal.decode(f.decoder)).thenThrow(failure);
            assertThatThrownBy(() -> f.rpc.getJournalEntries(f.running, f.offset,
                    f.source.processJournalEntries(PARTITION, f.offset), mock(WatchDog.class))).isSameAs(failure);
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(40));
            verify(f.journal, never()).getPosition();
        }
    }

    @ParameterizedTest
    @CsvSource({ "event.processing.failure.handling.mode,warn", "event.processing.failure.handling.mode,skip",
            "event.converting.failure.handling.mode,warn" })
    void testBarrierRejectsLossySettingsBeforeOffsetLookup(String property, String value) {
        var task = new As400LegacyBarrierProbeConnector.BarrierTask();
        var context = mock(SourceTaskContext.class);
        task.initialize(context);
        Configuration raw = configuration(Map.of(property, value));
        task.preStart(raw);
        assertThatThrownBy(() -> task.start(raw)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("failure.handling.mode=fail");
        verify(context, never()).offsetStorageReader();
    }

    @ParameterizedTest
    @ValueSource(strings = { "PT", "PX", "UP", "key-change", "rollover" })
    void shouldCheckpointTheCompletedRowWithoutChangingPreviouslyQueuedOffsets(String kind) throws Exception {
        try (var f = new Fixture(Map.of("heartbeat.interval.ms", "1"), false)) {
            if (kind.equals("UP") || kind.equals("key-change")) {
                f.beforeImage(1, kind.equals("UP") ? 1 : 2);
            }
            var event = As400ConnectProbeConnector.event(42, "R/" + (kind.equals("key-change") ? "UP" : kind.equals("rollover") ? "PT" : kind), 0, 1);
            if (kind.equals("rollover")) {
                var h = event.header();
                var header = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader(-1, -1, -1L, h.getSequenceNumber(), BigInteger.ZERO,
                        h.getTime(), 'R', "PT", "TEST      LIB       TEST      ", BigInteger.ZERO, -1, -1, "NEXT", "NEWLIB");
                event = new As400ConnectProbeConnector.Event(header, event.data(), new io.debezium.ibmi.db2.journal.retrieve.JournalPosition(BigInteger.valueOf(42),
                        new JournalReceiver("NEXT", "NEWLIB")));
            }
            f.run(event);
            verifyNoInteractions(f.errors);
            var records = f.queue.poll();
            var rows = records.stream().map(DataChangeEvent::getRecord).filter(r -> r.topic().equals("events")).toList();
            assertThat(rows).hasSize(kind.equals("key-change") ? 3 : 1);
            var record = rows.get(rows.size() - 1);
            assertThat(((Struct) record.value()).getStruct("source").getString("sequence")).isEqualTo("42");
            assertThat(record.sourceOffset().get("offset.event_sequence")).isEqualTo("42");
            assertThat(record.sourceOffset().get(As400OffsetContext.PROCESSED)).isEqualTo("true");
            if (kind.equals("key-change")) {
                assertThat(rows.subList(0, 2)).allSatisfy(r -> {
                    assertThat(r.sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("40");
                    assertThat(r.sourceOffset().get(As400OffsetContext.PROCESSED)).isEqualTo("false");
                });
            }
            for (var queued : records) {
                if (queued.getRecord().topic().equals("heartbeat")) {
                    assertThat(queued.getRecord().sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
                }
            }
            var restored = new As400OffsetContext.Loader(f.config).load(record.sourceOffset());
            assertThat(restored.getPosition().processed()).isTrue();
            assertThat(restored.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            if (kind.equals("rollover")) {
                assertThat(restored.getPosition().getReceiver()).isEqualTo(new JournalReceiver("NEXT", "NEWLIB"));
            }
            assertThat(f.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            f.run(As400ConnectProbeConnector.event(43, "R/PT", 0, 3));
            assertThat(record.sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
        }
    }

    @Test
    void shouldKeepAnUnpairedBeforeImageReplayableAcrossAnotherRowAndFreshRestart() throws Exception {
        Map<String, ?> saved;
        try (var original = new Fixture(Map.of(), false)) {
            original.beforeImage(1, 1);
            original.run("PT");
            saved = original.queue.poll().get(0).getRecord().sourceOffset();
            assertThat(saved.get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("40");
            assertThat(saved.get(As400OffsetContext.PROCESSED)).isEqualTo("false");
            assertThat(original.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(42));
            assertThat(original.offset.getOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("40");
        }
        try (var replacement = new Fixture(Map.of(), false, saved)) {
            assertThat(replacement.offset.getPosition().processed()).isFalse();
            replacement.beforeImage(1, 1);
            replacement.run("UP");
            var row = replacement.queue.poll().get(0).getRecord();
            assertThat(((Struct) row.value()).getStruct("before").getInt32("ID")).isEqualTo(1);
            assertThat(row.sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
            assertThat(replacement.offset.getOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
        }
    }

    @Test
    void shouldKeepDeleteReplayableIfTombstoneCreationFails() throws Exception {
        try (var f = new Fixture(Map.of(), true)) {
            f.run("DL");
            f.assertStopped();
            var prefix = f.queue.poll().get(0).getRecord();
            assertThat(((Struct) prefix.value()).getString("op")).isEqualTo("d");
            assertThat(prefix.sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
            assertThat(prefix.sourceOffset().get(As400OffsetContext.PROCESSED)).isEqualTo("false");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldReconstructCompletedUpdatesWhenAnotherTableStillHasAnOpenPair(boolean keyChange) throws Exception {
        Map<String, ?> saved;
        try (var original = new Fixture(Map.of(), false)) {
            original.row(40, "TEST", "UB", 1);
            original.row(41, "OTHER", "UB", 7);
            original.row(42, "TEST", "UP", keyChange ? 2 : 1);
            var rows = original.queue.poll();
            saved = rows.get(rows.size() - 1).getRecord().sourceOffset();
            assertThat(saved.get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("40");
            assertThat(saved.get(As400OffsetContext.PROCESSED)).isEqualTo("false");
            // Completing A cannot discard A's reconstruction history while B
            // keeps a restart before A's already-published after-image possible.
            original.row(43, "TEST", "PT", 3);
            assertThat(original.queue.poll().get(0).getRecord().sourceOffset()).isEqualTo(saved);
            assertThat(original.offset.getOffset()).isEqualTo(saved);
            assertThat(original.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(43));
        }
        try (var replacement = new Fixture(Map.of(), false, saved)) {
            assertThat(replacement.offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(40));
            assertThat(replacement.offset.getPosition().processed()).isFalse();
            replacement.row(40, "TEST", "UB", 1);
            replacement.row(41, "OTHER", "UB", 7);
            replacement.row(42, "TEST", "UP", keyChange ? 2 : 1);
            var rows = replacement.queue.poll().stream().map(DataChangeEvent::getRecord).toList();
            assertThat(rows).hasSize(keyChange ? 3 : 1);
            assertThat(((Struct) rows.get(0).value()).getStruct("before").getInt32("ID")).isEqualTo(1);
            if (keyChange) {
                assertThat(((Struct) rows.get(0).value()).getString("op")).isEqualTo("d");
                assertThat(((Struct) rows.get(0).key()).getInt32("ID")).isEqualTo(1);
                assertThat(rows.get(1).value()).isNull();
                assertThat(((Struct) rows.get(2).value()).getString("op")).isEqualTo("c");
                assertThat(((Struct) rows.get(2).key()).getInt32("ID")).isEqualTo(2);
            }
            else {
                assertThat(((Struct) rows.get(0).value()).getString("op")).isEqualTo("u");
            }
            assertThat(rows).allSatisfy(row -> assertThat(row.sourceOffset()).isEqualTo(saved));
            replacement.row(43, "TEST", "PT", 3);
            replacement.queue.poll();
            replacement.row(44, "OTHER", "UP", 7);
            var completed = replacement.queue.poll().get(0).getRecord();
            assertThat(((Struct) completed.value()).getStruct("before").getInt32("ID")).isEqualTo(7);
            assertThat(completed.sourceOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("44");
            assertThat(completed.sourceOffset().get(As400OffsetContext.PROCESSED)).isEqualTo("true");
            assertThat(replacement.source.hasPendingBeforeImage()).isFalse();
            assertThat(replacement.offset.getOffset()).isEqualTo(completed.sourceOffset());
            verifyNoInteractions(replacement.errors);
        }
    }

    @Test
    void shouldAdvanceRpcPositionOnlyAfterTheCurrentRowCallbackReturns() throws Exception {
        try (var f = new Fixture(Map.of(), false)) {
            var callbackOffsets = new ArrayList<Map<String, ?>>();
            f.rpc.getJournalEntries(f.running, f.offset, (sequence, reader, header) -> {
                f.offset.updateSourceInfo(header);
                assertThat(f.offset.getSourceInfo().getString("sequence")).isEqualTo("42");
                callbackOffsets.add(Map.copyOf(f.offset.getOffset()));
            }, mock(WatchDog.class));
            assertThat(callbackOffsets).hasSize(1);
            assertThat(callbackOffsets.get(0).get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("40");
            assertThat(f.offset.getOffset().get(As400OffsetContext.EVENT_SEQUENCE)).isEqualTo("42");
        }
    }

    /** Fresh owned bytes for each test; only routing and entry type are synthetic. */
    private static RawJournalPage.Entry nativeRow(String type) throws IOException {
        byte[] bytes;
        try (var input = As400LegacyRowFailureTest.class.getResourceAsStream("/native-transactions/open-clear-progress.json")) {
            var json = new com.fasterxml.jackson.databind.ObjectMapper().readTree(input);
            var row = java.util.stream.StreamSupport.stream(json.path("entries").spliterator(), false)
                    .filter(e -> e.path("sequence").asText().equals("42")).findFirst().orElseThrow();
            bytes = java.util.HexFormat.of().parseHex(row.path("hex").asText());
        }
        new com.ibm.as400.access.AS400Text(30, 37).toBytes("TEST      LIB       TEST      ", bytes, 157);
        new com.ibm.as400.access.AS400Text(2, 37).toBytes(type, bytes, 99);
        var header = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder().decode(bytes, 0);
        var receiver = header.hasReceiver() ? new JournalReceiver(header.getReceiver(), header.getReceiverLibrary())
                : As400RecoveryTestSupport.Fixture.RECEIVER;
        return new RawJournalPage.Entry(new io.debezium.ibmi.db2.journal.retrieve.JournalPosition(header.getSequenceNumber(), receiver), header, bytes);
    }

    private static Configuration configuration(Map<String, String> overrides) {
        return Configuration.create().with("topic.prefix", "test").with("database.hostname", "no-live-ibmi.invalid")
                .with("database.user", "fixture").with("database.password", "fixture").with("database.dbname", "DB").with("database.schema", "LIB")
                .with("skipped.operations", "none").with("heartbeat.interval.ms", 3600000).with("poll.interval.ms", 1)
                .with("event.converting.failure.handling.mode", "fail").with("probe.topic", "events").with("probe.policy", "fixture")
                .with("transaction.boundary", "connector").build().edit().with(Configuration.from(overrides)).build();
    }

    static final class Fixture extends As400ConnectorTask implements AutoCloseable {
        final As400ConnectorConfig config;
        final As400OffsetContext offset;
        final ChangeEventQueue<DataChangeEvent> queue;
        final ErrorHandler errors = mock(ErrorHandler.class);
        final RetrieveJournal journal = mock(RetrieveJournal.class);
        final JdbcFileDecoder decoder = mock(JdbcFileDecoder.class);
        final As400JdbcConnection jdbc = mock(As400JdbcConnection.class);
        final As400RpcConnection rpc = mock(As400RpcConnection.class, CALLS_REAL_METHODS);
        final ChangeEventSourceContext running = mock(ChangeEventSourceContext.class);
        final EventDispatcher<As400Partition, TableId> dispatcher;
        final As400StreamingChangeEventSource source;

        Fixture(Map<String, String> overrides, boolean failAfterFirstRecord) throws Exception {
            this(overrides, failAfterFirstRecord, null);
        }

        Fixture(Map<String, String> overrides, boolean failAfterFirstRecord, Map<String, ?> savedOffset) throws Exception {
            Configuration raw = configuration(overrides);
            var context = preStart(raw);
            config = (As400ConnectorConfig) context.getConfig();
            registerServiceProviders(config.getServiceRegistry());
            config.getBeanRegistry().add(StandardBeanNames.CONFIGURATION, raw);
            config.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, config);
            config.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, context);
            offset = savedOffset == null
                    ? new As400OffsetContext(config, new JournalProcessedPosition(BigInteger.valueOf(40), new JournalReceiver("RCV", "LIB"), Instant.EPOCH, true))
                    : new As400OffsetContext.Loader(config).load(savedOffset);
            queue = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(10).maxQueueSize(64).pollInterval(Duration.ofMillis(1))
                    .loggingContextSupplier(() -> context.configureLoggingContext("row-failure-test")).build();
            var topics = new AbstractTopicNamingStrategy<TableId>(raw.asProperties()) {
                @Override
                public String dataChangeTopic(TableId id) {
                    return "events";
                }
            };
            var count = new AtomicInteger();
            dispatcher = spy(new EventDispatcher<As400Partition, TableId>(config, topics,
                    new As400TaskStopProbeConnector.FixtureSchema(offset, new TableId("DB", "LIB", "TEST"), new TableId("DB", "LIB", "OTHER")), queue,
                    config.getTableFilters().dataCollectionFilter(), record -> {
                        if (failAfterFirstRecord && count.incrementAndGet() == 2) {
                            throw new IllegalStateException("controlled dispatch failure after first subrecord");
                        }
                        return new DataChangeEvent(record);
                    }, null, new As400EventMetadataProvider(), new HeartbeatImpl(config.getHeartbeatInterval(), "heartbeat", "test", config.schemaNameAdjuster(), queue),
                    config.schemaNameAdjuster(), null, new DebeziumHeaderProducer(context)));
            when(jdbc.getRealDatabaseName()).thenReturn("DB");
            when(jdbc.getLongName(anyString(), anyString())).thenAnswer(i -> i.getArgument(1));
            var schema = mock(As400DatabaseSchema.class);
            when(schema.getFileDecoder()).thenReturn(decoder);
            when(journal.decode(decoder)).thenReturn(new Object[]{ 1 });
            source = new As400StreamingChangeEventSource(config, rpc, jdbc, dispatcher, errors, Clock.system(), schema);
            source.init(offset);
            set("lifecycle", new JournalReadLifecycle());
            set("retrieveJournal", journal);
            var periodic = As400RpcConnection.class.getDeclaredField("periodic");
            set("periodic", mock(periodic.getType()));
            when(journal.retrieveJournal(any())).thenReturn(RetrievalState.Success);
            when(journal.nextEntry()).thenReturn(true, false);
            when(journal.getEntryHeader()).thenReturn(As400ConnectProbeConnector.event(42, "R/PT", 0, 1).header());
            when(journal.getPosition()).thenReturn(new JournalProcessedPosition(BigInteger.valueOf(42), new JournalReceiver("RCV", "LIB"), Instant.EPOCH, true));
            when(running.isRunning()).thenReturn(true, true, false);
        }

        void beforeImage(int before, int after) throws Exception {
            when(journal.decode(decoder)).thenReturn(new Object[]{ before }, new Object[]{ after });
            source.processJournalEntries(PARTITION, offset).accept(BigInteger.valueOf(40), journal, As400ConnectProbeConnector.event(40, "R/UB", 0, before).header());
        }

        void run(String type) throws Exception {
            run(As400ConnectProbeConnector.event(42, "R/" + type, 0, 1));
        }

        void row(long sequence, String table, String type, int value) throws Exception {
            var event = As400ConnectProbeConnector.event(sequence, "R/" + type, 0, value);
            var h = event.header();
            var header = new io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader(-1, -1, -1L, h.getSequenceNumber(), BigInteger.ZERO,
                    h.getTime(), 'R', type, String.format("%-10s%-10s%-10s", table, "LIB", table), BigInteger.ZERO, -1, -1, h.getReceiver(), h.getReceiverLibrary());
            when(journal.decode(decoder)).thenReturn(new Object[]{ value });
            run(new As400ConnectProbeConnector.Event(header, event.data(), event.position()));
        }

        void run(As400ConnectProbeConnector.Event event) throws Exception {
            when(running.isRunning()).thenReturn(true, true, false);
            when(journal.nextEntry()).thenReturn(true, false);
            when(journal.getEntryHeader()).thenReturn(event.header());
            when(journal.getPosition()).thenReturn(new JournalProcessedPosition(event.position(), event.header().getTime(), true));
            source.execute(running, PARTITION, offset);
        }

        RowProcessingException assertStopped() throws Exception {
            var failure = ArgumentCaptor.forClass(RowProcessingException.class);
            verify(errors).setProducerThrowable(failure.capture());
            assertThat(failure.getValue()).hasMessageContaining("42");
            assertThat(offset.getPosition().getOffset()).isEqualTo(BigInteger.valueOf(40));
            verify(journal, never()).getPosition();
            verify(journal).nextEntry();
            verify(rpc).getJournalEntries(any(), any(), any(), any());
            return failure.getValue();
        }

        private void set(String name, Object value) throws Exception {
            var field = As400RpcConnection.class.getDeclaredField(name);
            field.setAccessible(true);
            field.set(rpc, value);
        }

        @Override
        public void close() {
            dispatcher.close();
            config.getServiceRegistry().close();
        }
    }
}
