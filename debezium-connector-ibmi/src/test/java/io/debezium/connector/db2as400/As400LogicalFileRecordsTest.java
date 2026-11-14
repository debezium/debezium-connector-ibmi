/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Text;

import io.debezium.document.DocumentReader;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400LogicalFileRecordsTest {
    private static final JournalReceiver RECEIVER = new JournalReceiver("QSQJRN0002", "DCNC0003");
    private static final String FINGERPRINT = "v2:" + "a".repeat(64);
    private final As400BindingDefinition table;
    private final List<RawJournalPage.Entry> entries = new ArrayList<>();

    As400LogicalFileRecordsTest() throws Exception {
        try (var input = getClass().getResourceAsStream("/native-transactions/logical-file.json")) {
            var json = new ObjectMapper().readTree(input);
            assertThat(json.path("startup_proof").asBoolean()).isFalse();
            table = As400BindingEncoding.decode(DocumentReader.defaultReader().read(json.toString()).getArray("tables")).get(0);
            for (var item : json.path("entries")) {
                entries.add(entry(Base64.getDecoder().decode(item.path("raw").asText())));
            }
        }
    }

    private static JournalPosition position(long sequence) {
        return new JournalPosition(BigInteger.valueOf(sequence), RECEIVER);
    }

    private static RawJournalPage.Entry entry(byte[] raw) {
        var header = new EntryHeaderDecoder(37).decode(raw, 0);
        return new RawJournalPage.Entry(new JournalPosition(header.getSequenceNumber(), RECEIVER), header, raw);
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid", "physical-jid", "missing-jid", "incomplete" })
    void shouldValidateThePhysicalFileAndSeparateLogicalIdentityInAssociationRecords(String fault) throws Exception {
        // Documented D/LF payload with controlled identities, using a captured
        // fixed header. The association is attached to its physical file.
        byte[] raw = java.util.Arrays.copyOf(entries.get(0).data(), 286);
        var bytes = java.nio.ByteBuffer.wrap(raw);
        bytes.putInt(0, raw.length).putInt(4, 0).putInt(8, 240).putInt(12, 0).putInt(16, 0).putInt(20, 0).putInt(72, 0);
        raw[218] = 0;
        System.arraycopy(new AS400Text(3, 37).toBytes("DLF"), 0, raw, 98, 3);
        System.arraycopy(new AS400Text(10, 37).toBytes("A"), 0, raw, 157, 10);
        System.arraycopy(java.util.HexFormat.of().parseHex(table.identity().fileJournalId()), 0, raw, 197, 10);
        System.arraycopy(new AS400Text(5, 37).toBytes(fault.equals("incomplete") ? "00029" : "00030"), 0, raw, 240, 5);
        System.arraycopy(new AS400Text(10, 37).toBytes("IXA"), 0, raw, 256, 10);
        System.arraycopy(new AS400Text(10, 37).toBytes("DCNC0003"), 0, raw, 266, 10);
        String jid = fault.equals("physical-jid") ? table.identity().fileJournalId()
                : fault.equals("missing-jid") ? "0".repeat(20) : "00000000000000000003";
        System.arraycopy(java.util.HexFormat.of().parseHex(jid), 0, raw, 276, 10);
        var decoded = entry(raw);
        var association = new As400JournalLifecycle.Entry(decoded.header(), raw, decoded.position());
        if (fault.equals("valid")) {
            assertThat(As400JournalLifecycle.administrative(association, (file, library) -> table.binding(65536))).isTrue();
        }
        else {
            assertThatThrownBy(() -> As400JournalLifecycle.administrative(association, (file, library) -> table.binding(65536)))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "valid", "data-member", "missing-jid", "payload" })
    void shouldRecognizeAccessPathMetadataWithoutAcceptingDataMemberJournalingChanges(String fault) throws Exception {
        try (var input = getClass().getResourceAsStream("/native-transactions/access-path.json")) {
            for (var item : new ObjectMapper().readTree(input).path("entries")) {
                byte[] raw = Base64.getDecoder().decode(item.path("raw").asText());
                if (fault.equals("data-member")) {
                    System.arraycopy(new AS400Text(10, 37).toBytes("*QDDS"), 0, raw, 219, 10);
                }
                else if (fault.equals("missing-jid")) {
                    java.util.Arrays.fill(raw, 197, 207, (byte) 0);
                }
                else if (fault.equals("payload")) {
                    new AS400Text(5, 37).toBytes("00001", raw, 240);
                }
                var parsed = entry(raw);
                var record = new As400JournalLifecycle.Entry(parsed.header(), raw, parsed.position());
                As400SourceNormalizer.TableResolver noBinding = (file, library) -> {
                    throw new AssertionError("An access path must not be promoted to a data-member binding");
                };
                if (fault.equals("valid")) {
                    assertThat(As400JournalLifecycle.administrative(record, noBinding)).isTrue();
                }
                else {
                    assertThatThrownBy(() -> As400JournalLifecycle.administrative(record, noBinding)).isInstanceOf(IllegalArgumentException.class);
                }
            }
        }
    }

    @Test
    void shouldContinuePhysicalRowsAcrossIndexCreationDeletionAndRestartWithoutCatalogLookups() throws Exception {
        var order = new As400JournalOrder(List.of(RECEIVER), Map.of(RECEIVER, "fixture-receiver"),
                Map.of(RECEIVER, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(100))));
        var saved = As400Checkpoint.afterVerifiedBoundary("fixture-journal", FINGERPRINT, position(46), "fixture-receiver");
        for (int restart = 0; restart < 3; restart++) {
            var records = new ArrayList<SourceRecord>();
            var loaded = saved;
            try (var output = new As400OnlineCensusRecovery.Output(List.of(table), false);
                    var replay = new As400ReplayCoordinator(loaded.toOffset(), "fixture-journal", FINGERPRINT, order,
                            new As400TransactionBuffer.Limits(16, 1000, 128, 1_000_000), 65536, 1000,
                            (file, library) -> {
                                assertThat(file).isEqualTo("A");
                                return table.binding(65536);
                            }, As400SourceNormalizer.SavepointPolicy.consumingTargets(), Map.of("server", "offline"), records::add,
                            new As400JournalLifecycle.Limits(100, 65536, 16, 128, 100, 64), true)) {
                long through = restart == 0 ? 51 : 55;
                for (var entry : entries) {
                    if (entry.position().offset().longValueExact() <= through && order.compare(entry.position(), loaded.replayFrom()) > 0) {
                        assertThat(As400SchemaChange.isOpening(entry)).isFalse();
                        if (replay.accept(entry.header(), entry.data(), 0, entry.position()).isPresent()) {
                            replay.deliver(output.dispatcher);
                        }
                    }
                }
                if (restart < 2) {
                    replay.verifyOrdinaryCut();
                }
                saved = replay.checkpoint();
            }
            var data = records.stream().filter(record -> record.value() instanceof Struct value && value.schema().field("op") != null).toList();
            assertThat(data).hasSize(restart < 2 ? 1 : 0);
            if (restart < 2) {
                assertThat(((Struct) data.get(0).value()).getStruct("after").getInt32("ID")).isEqualTo(800 + restart);
                assertThat(saved.replayFrom()).isEqualTo(position(restart == 0 ? 51 : 55));
                assertThat(saved.completedThrough()).isEqualTo(saved.replayFrom());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "row", "object-type", "jid", "minimized", "file-attribute", "payload", "pointer", "physical-clear" })
    void shouldRejectInconsistentLogicalClassificationWithoutRelaxingPhysicalRecords(String fault) {
        var original = entries.get(fault.equals("pointer") ? 1 : 0);
        byte[] raw = original.data();
        switch (fault) {
            case "row" -> {
                System.arraycopy(new AS400Text(3, 37).toBytes("RPT"), 0, raw, 98, 3);
            }
            case "object-type" -> System.arraycopy(new AS400Text(10, 37).toBytes("*QDDS"), 0, raw, 219, 10);
            case "jid" -> java.util.Arrays.fill(raw, 197, 207, (byte) 0);
            case "minimized" -> raw[218] |= 0x08;
            case "file-attribute" -> System.arraycopy(new AS400Text(10, 37).toBytes("PF"), 0, raw,
                    original.header().getEntrySpecificDataOffset() + 16 + 124, 10);
            case "physical-clear" -> {
                System.arraycopy(new AS400Text(2, 37).toBytes("CG"), 0, raw, 99, 2);
                raw[218] = 0x04;
                java.nio.ByteBuffer.wrap(raw).putInt(72, 0);
                System.arraycopy(new AS400Text(10, 37).toBytes("PF"), 0, raw,
                        original.header().getEntrySpecificDataOffset() + 16 + 124, 10);
            }
            case "payload" -> raw[original.header().getEntrySpecificDataOffset()] = 0;
            case "pointer" -> java.nio.ByteBuffer.wrap(raw).putInt(72, 7);
            default -> throw new AssertionError(fault);
        }
        var changed = entry(raw);
        assertThatThrownBy(() -> As400LogicalFileRecords.accept(new As400JournalLifecycle.Entry(changed.header(), raw, changed.position())))
                .isInstanceOf(IllegalArgumentException.class);
        var physical = entries.stream().filter(e -> e.header().getJournalCode() == 'R').findFirst().orElseThrow();
        assertThat(As400LogicalFileRecords.accept(new As400JournalLifecycle.Entry(physical.header(), physical.data(), physical.position()))).isFalse();
    }
}
