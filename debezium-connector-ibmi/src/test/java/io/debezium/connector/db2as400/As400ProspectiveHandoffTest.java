/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.connector.db2as400.As400OnlineHandoff.Evidence;
import io.debezium.connector.db2as400.As400OnlineHandoff.Visibility;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400ProspectiveHandoffTest {
    @TempDir
    Path directory;
    private final EntryHeaderDecoder headers = new EntryHeaderDecoder();

    private As400ConnectorConfig config() {
        return new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                .with("database.schema", "DCOC1107").with("table.include.list", "DCOC1107\\.(A|B)")
                .with("provide.transaction.metadata", false).with("skipped.operations", "none").build());
    }

    private List<Entry> captured() throws Exception {
        return new Fixture(directory).entries.stream().map(e -> new Entry(e.position(), e.data())).toList();
    }

    private List<Entry> controls(List<Entry> entries) {
        return entries.stream().filter(e -> headers.decode(e.bytes(), 0).getJournalCode() == 'C').toList();
    }

    private Evidence future(List<Entry> entries) throws Exception {
        var f = new Fixture(directory);
        String digest = As400OnlineHandoff.digest(entries);
        return new Evidence(entries, f.definitions, new As400JournalOrder(List.of(Fixture.RECEIVER), Map.of(Fixture.RECEIVER, f.receiverIdentity),
                Map.of(Fixture.RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(29), BigInteger.valueOf(64)))), f.journalIdentity,
                As400OnlineHandoff.fingerprint(f.definitions, config(), digest), Fixture.position(42), Fixture.position(42),
                Set.of(BigInteger.valueOf(31)), digest, Visibility.CONTROLLED_FIXTURE_ASSUMPTION);
    }

    private List<Entry> from42(List<Entry> all) {
        return all.stream().filter(e -> e.position().offset().intValueExact() >= 42).toList();
    }

    @Test
    void completedPriorCycleNeedsOlderControlsButNoOlderRowSchemaOrNewCheckpointFormat() throws Exception {
        var all = captured();
        var result = As400ProspectiveHandoff.prepare(future(from42(all)), controls(all), Fixture.position(42), Fixture.position(64), config());
        assertThat(result.completedPriorCycles()).containsExactly(BigInteger.valueOf(31));
        assertThat(result.projection().entries()).allSatisfy(e -> assertThat(e.position().offset()).isGreaterThanOrEqualTo(BigInteger.valueOf(42)));
        assertThat(result.candidate().checkpoint().toOffset()).containsEntry("replay_from.sequence", "46")
                .containsEntry("completed_through.sequence", "59").containsEntry("offset.version", "2");
        assertThat(result.candidate().scan()).isEqualTo(Fixture.position(64));
        assertThat(Files.exists(directory.resolve("history.jsonl"))).isFalse();
        var json = new ObjectMapper();
        Map<String, ?> restored = json.readValue(json.writeValueAsBytes(result.candidate().checkpoint().toOffset()), new TypeReference<Map<String, Object>>() {
        });
        List<SourceRecord> output = new ArrayList<>();
        try (var fresh = As400OnlineHandoff.coordinator(result.projection(), restored, output::add)) {
            for (Entry entry : result.projection().entries()) {
                if (entry.position().offset().compareTo(BigInteger.valueOf(46)) >= 0) {
                    assertThat(fresh.accept(headers.decode(entry.bytes(), 0), entry.bytes(), 0, entry.position())).isEmpty();
                }
            }
            fresh.verifyOrdinaryCut();
            assertThat(fresh.checkpoint().toOffset()).isEqualTo(restored);
            assertThat(output).isEmpty();
        }
    }

    @Test
    void priorCycleDefersUntilItsRealTerminalWhileNewSelectedPrefixesStillUseExistingRefusal() throws Exception {
        var all = captured();
        var evidence = future(from42(all));
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(evidence, controls(all), Fixture.position(42), Fixture.position(42), config()))
                .isInstanceOf(As400ProspectiveHandoff.PriorEpochPending.class)
                .satisfies(error -> assertThat(((As400ProspectiveHandoff.PriorEpochPending) error).cycles()).containsExactly(BigInteger.valueOf(31)));
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(evidence, controls(all), Fixture.position(42), Fixture.position(56), config()))
                .isInstanceOf(As400OnlineHandoff.SelectedPrefixPending.class);
    }

    @Test
    void missingOldStartAndMissingProspectiveControlAreNotDeferrable() throws Exception {
        var all = captured();
        var missingStart = controls(all).stream().filter(e -> e.position().offset().intValueExact() != 31).toList();
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(future(from42(all)), missingStart, Fixture.position(42), Fixture.position(64), config()))
                .isInstanceOf(IllegalArgumentException.class).isNotInstanceOf(As400ProspectiveHandoff.PriorEpochPending.class);
        var missingTerminal = from42(all).stream().filter(e -> e.position().offset().intValueExact() != 43).toList();
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(future(missingTerminal), controls(all), Fixture.position(42), Fixture.position(64), config()))
                .hasMessageContaining("C projections disagree");
    }

    @Test
    void corruptNewRowsCannotHideBehindAnOldOpenCycle() throws Exception {
        var all = captured();
        var future = new ArrayList<>(from42(all));
        Entry first = future.get(0);
        byte[] changed = first.bytes().clone();
        changed[197] ^= 1;
        future.set(0, new Entry(first.position(), changed));
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(future(future), controls(all), Fixture.position(42), Fixture.position(42), config()))
                .isInstanceOf(As400SourceNormalizer.NormalizationException.class);
    }

    @Test
    void completedOldCycleStillRejectsWhenTheActualReplayFloorOverlapsItsTail() throws Exception {
        // Explicit synthetic ordering derived from captured headers; not native evidence.
        var all = captured();
        var revised = new ArrayList<>(all.stream().filter(e -> e.position().offset().intValueExact() < 42
                || Set.of(42, 45, 46, 56).contains(e.position().offset().intValueExact())).toList());
        for (int[] moved : List.of(new int[]{ 43, 57 }, new int[]{ 56, 64 })) {
            var source = all.stream().filter(e -> e.position().offset().intValueExact() == moved[0]).findFirst().orElseThrow();
            byte[] changed = source.bytes().clone();
            java.nio.ByteBuffer.wrap(changed).putLong(24, moved[1]);
            revised.add(new Entry(Fixture.position(moved[1]), changed));
        }
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(future(from42(revised)), controls(revised), Fixture.position(42), Fixture.position(64), config()))
                .hasMessageContaining("Replay floor still overlaps prior-schema transaction history");
    }

    @Test
    void oldCycleRowsAfterItsTerminalCannotBeSilentlyDiscarded() throws Exception {
        var all = captured();
        var future = new ArrayList<>(from42(all));
        for (int i = 0; i < future.size(); i++) {
            Entry entry = future.get(i);
            if (entry.position().offset().intValueExact() == 56) {
                byte[] changed = entry.bytes().clone();
                java.nio.ByteBuffer.wrap(changed).putLong(64, 31);
                future.set(i, new Entry(entry.position(), changed));
            }
        }
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(future(future), controls(all), Fixture.position(42), Fixture.position(64), config()))
                .hasMessageContaining("Work follows a completed prior-schema cycle");
    }

    @Test
    void olderRowsAndLifecycleOverlapCannotBeDiscardedToMakeTheProofPass() throws Exception {
        var all = captured();
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(future(all), controls(all), Fixture.position(42), Fixture.position(64), config()))
                .hasMessageContaining("must not decode rows before");
        var earlier = all.stream().filter(e -> e.position().offset().intValueExact() >= 37).toList();
        assertThatThrownBy(() -> As400ProspectiveHandoff.prepare(future(earlier), controls(all), Fixture.position(37), Fixture.position(64), config()))
                .hasMessageContaining("Lifecycle overlaps");
    }
}
