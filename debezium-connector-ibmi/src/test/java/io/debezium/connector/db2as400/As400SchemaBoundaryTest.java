/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/** Actual native ALTER and reversal: identical endpoint bindings cannot replace a complete journal. */
class As400SchemaBoundaryTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private JsonNode fixture() throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/equal-width-alter.json")) {
            var fixture = JSON.readTree(stream);
            assertThat(fixture.path("startup_proof").asBoolean()).isFalse();
            assertThat(fixture.path("cleanup").path("status").asText()).isEqualTo("passed");
            return fixture;
        }
    }

    private List<Entry> entries(JsonNode fixture) throws Exception {
        var result = new ArrayList<Entry>();
        for (var item : fixture.path("capture").path("entries")) {
            result.add(As400OnlineCensusReplay.load(Base64.getDecoder().decode(item.path("raw").asText()), item));
        }
        return result;
    }

    @Test
    void equalWidthAlterAndReversalPreserveJidsAndRestoreFormatButCannotValidateIntermediateRows() throws Exception {
        var fixture = fixture();
        var before = As400OnlineFenceEvidence.validateOffLock(fixture.path("before")).tables().definitions();
        var changed = As400OnlineFenceEvidence.validateOffLock(fixture.path("changed")).tables().definitions();
        var restored = As400OnlineFenceEvidence.validateOffLock(fixture.path("restored")).tables().definitions();
        assertThat(restored).isEqualTo(before);
        var oldB = before.stream().filter(d -> d.identity().physicalFile().equals("B")).findFirst().orElseThrow();
        var newB = changed.stream().filter(d -> d.identity().physicalFile().equals("B")).findFirst().orElseThrow();
        assertThat(newB.identity().fileJournalId()).isEqualTo(oldB.identity().fileJournalId());
        assertThat(newB.identity().memberJournalId()).isEqualTo(oldB.identity().memberJournalId());
        assertThat(newB.identity().recordFormat()).isNotEqualTo(oldB.identity().recordFormat());
        Entry row = entries(fixture).stream().filter(e -> e.position().offset().intValueExact() == 61).findFirst().orElseThrow();
        var header = new EntryHeaderDecoder().decode(row.bytes(), 0);
        // Deliberate diagnostic omission of maintenance, NEVER an admitted replay path.
        assertThat(newB.binding(1024).values(header, row.bytes(), 0)).containsExactly(101.0f);
        assertThat(oldB.binding(1024).values(header, row.bytes(), 0)).containsExactly(1120534528);
    }

    @Test
    void completeNativeTraceRejectsAlterBeforePublicationAndRetainsCheckpointAcrossFreshRestart() throws Exception {
        var fixture = fixture();
        var definitions = As400OnlineFenceEvidence.validateOffLock(fixture.path("before")).tables().definitions();
        var input = entries(fixture);
        var nativeWindow = As400OnlineNativeEvidence.restore(fixture.path("capture").path("native_identity_evidence"));
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        As400OperatorCutReader.chain(nativeWindow.before().directory(), input.get(0).position().receiver()).forEach(r -> {
            r.requireReadableProfile();
            ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last()));
        });
        var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), nativeWindow.receiverTokens(), ranges);
        var baseline = input.get(0).position();
        assertThat(baseline.offset().intValueExact()).isEqualTo(48);
        var checkpoint = As400Checkpoint.afterVerifiedBoundary(nativeWindow.journalToken(), "v1:" + "a".repeat(64), baseline, order.incarnation(baseline));
        Map<String, ?> offset = checkpoint.toOffset();
        for (int restart = 0; restart < 2; restart++) {
            var output = new ArrayList<SourceRecord>();
            var bindings = new LinkedHashMap<String, As400SourceNormalizer.TableBinding>();
            definitions.forEach(d -> bindings.put(d.identity().tableId().schema() + "/" + d.identity().physicalFile(), d.binding(1024)));
            try (var dispatcher = new As400OnlineCensusRecovery.Output(definitions, false);
                    var replay = new As400ReplayCoordinator(offset, nativeWindow.journalToken(), "v1:" + "a".repeat(64), order,
                            new As400TransactionBuffer.Limits(128, 40_000, 2048, 64 * 1024 * 1024), 1024, 20_000,
                            (file, library) -> bindings.get(library + "/" + file), As400SourceNormalizer.SavepointPolicy.consumingTargets(),
                            Map.of("server", "offline"), output::add, new As400JournalLifecycle.Limits(20_000, 64 * 1024 * 1024, 128, 2048, 1024, 4096), true)) {
                for (Entry entry : input) {
                    int sequence = entry.position().offset().intValueExact();
                    if (sequence <= 48) {
                        continue;
                    }
                    if (sequence == 52) {
                        assertThatThrownBy(() -> replay.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position()))
                                .isInstanceOf(As400ReplayCoordinator.RecoveryException.class).hasRootCauseMessage("Unsupported legacy change");
                        break;
                    }
                    if (replay.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position()).isPresent()) {
                        replay.deliver(dispatcher.dispatcher);
                    }
                }
                assertThat(output).isEmpty();
                assertThat(replay.checkpoint().toOffset()).isEqualTo(offset);
                var laterRow = input.get(input.size() - 1);
                assertThatThrownBy(() -> replay.accept(new EntryHeaderDecoder().decode(laterRow.bytes(), 0), laterRow.bytes(), 0, laterRow.position()))
                        .isInstanceOf(As400ReplayCoordinator.RecoveryException.class);
                assertThat(output).isEmpty();
                assertThat(replay.checkpoint().toOffset()).isEqualTo(offset);
                offset = JSON.readValue(JSON.writeValueAsBytes(offset), new TypeReference<Map<String, Object>>() {
                });
            }
        }
    }
}
