/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.connector.db2as400.As400OnlineHandoff.Evidence;
import io.debezium.connector.db2as400.As400OnlineHandoff.Visibility;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

class As400OnlineHandoffTest {
    @TempDir
    Path directory;

    private As400ConnectorConfig config(String include) {
        return new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").with("database.dbname", "DB")
                .with("database.schema", "DCOC1107").with("table.include.list", include).with("provide.transaction.metadata", false)
                .with("skipped.operations", "none").build());
    }

    private Evidence evidence(As400ConnectorConfig config) throws Exception {
        var f = new Fixture(directory);
        List<Entry> entries = f.entries.stream().map(e -> new Entry(e.position(), e.data())).toList();
        String digest = As400OnlineHandoff.digest(entries);
        return new Evidence(entries, f.definitions, new As400JournalOrder(List.of(Fixture.RECEIVER), Map.of(Fixture.RECEIVER, f.receiverIdentity),
                Map.of(Fixture.RECEIVER, new As400JournalOrder.Range(BigInteger.valueOf(29), BigInteger.valueOf(64)))), f.journalIdentity,
                As400OnlineHandoff.fingerprint(f.definitions, config, digest), Fixture.position(40), Fixture.position(42), Set.of(BigInteger.valueOf(31)), digest,
                Visibility.CONTROLLED_FIXTURE_ASSUMPTION);
    }

    @Test
    void autocommitOnlyHistoryUsesRealRecordsWithoutInventingATransactionStart() throws Exception {
        var config = config("DCOC1107\\.A");
        var original = evidence(config);
        var rows = original.entries().stream().filter(e -> {
            var h = new EntryHeaderDecoder().decode(e.bytes(), 0);
            return h.getJournalCode() == 'R' && h.getEntryType().equals("PT") && h.getCommitCycle().signum() == 0;
        }).toList();
        assertThat(rows).isNotEmpty();
        String digest = As400OnlineHandoff.digest(rows);
        var from = rows.get(0).position();
        var through = rows.get(rows.size() - 1).position();
        var input = new Evidence(rows, original.definitions(), original.order(), original.journal(),
                As400OnlineHandoff.fingerprint(original.definitions(), config, digest), from, from, Set.of(), digest, original.visibility());
        var candidate = As400OnlineHandoff.prepare(input, through, config);
        var prospective = As400ProspectiveHandoff.prepare(input, List.of(), from, through, config);
        assertThat(prospective.candidate().checkpoint()).isEqualTo(candidate.checkpoint());
        assertThat(candidate.openCycles()).isEmpty();
        assertThat(candidate.checkpoint().completedThrough()).isEqualTo(through);
        try (var fresh = As400OnlineHandoff.coordinator(input, candidate.checkpoint().toOffset(), record -> {
            throw new AssertionError("Previously published autocommit prefix repeated");
        })) {
            for (var entry : rows) {
                int comparison = input.order().compare(entry.position(), candidate.checkpoint().replayFrom());
                if (comparison > 0 || comparison == 0 && candidate.checkpoint().replayInclusive()) {
                    assertThat(fresh.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position())).isEmpty();
                }
            }
            fresh.verifyOrdinaryCut();
        }
        var missing = new Evidence(rows, original.definitions(), original.order(), original.journal(), input.fingerprint(),
                from, from, Set.of(BigInteger.ONE), digest, original.visibility());
        assertThatThrownBy(() -> As400OnlineHandoff.prepare(missing, through, config)).hasMessageContaining("original start");
    }

    @Test
    void reconstructsOpenContextWithoutClosedAnchorAndFreshInstanceSuppressesPublishedPrefix() throws Exception {
        var config = config("DCOC1107\\.A");
        var evidence = evidence(config);
        var candidate = As400OnlineHandoff.prepare(evidence, Fixture.position(42), config);
        assertThat(candidate.checkpoint().toOffset()).containsEntry("replay_from.sequence", "31").containsEntry("completed_through.sequence", "42");
        assertThat(candidate.openCycles()).containsExactly(BigInteger.valueOf(31));
        assertThat(java.nio.file.Files.exists(directory.resolve("history.jsonl"))).isFalse(); // No baseline or fabricated closed anchor.
        List<SourceRecord> output = new ArrayList<>();
        try (var replay = As400OnlineHandoff.coordinator(evidence(config), candidate.checkpoint().toOffset(), output::add);
                var dispatcher = new As400IncrementalLifecycleTest.Output(evidence.definitions().stream()
                        .collect(Collectors.toMap(d -> d.identity().physicalFile(), d -> d.binding(1024))), true,
                        Map.of("table.include.list", "DCOC1107\\.A")).dispatcher) {
            for (var entry : evidence.entries()) {
                if (entry.position().offset().compareTo(BigInteger.valueOf(31)) >= 0) {
                    if (replay.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position()).isPresent()) {
                        replay.deliver(dispatcher);
                    }
                }
            }
            replay.verifyEndOfAvailableJournal();
            assertThat(As400RecoveryTestSupport.data(output)).containsExactly("c:A:9");
        }
    }

    @Test
    void exactPublicationCutIsNotRelabeledAsCompletedOutput() throws Exception {
        var config = config("DCOC1107\\.A");
        var input = evidence(config);
        var candidate = As400OnlineHandoff.prepare(input, Fixture.position(46), config);
        assertThat(candidate.scan()).isEqualTo(Fixture.position(46));
        assertThat(candidate.checkpoint().completedThrough()).isEqualTo(Fixture.position(43));
        assertThat(candidate.checkpoint().toOffset()).doesNotContainKey("scan.sequence");
    }

    @ParameterizedTest
    @ValueSource(longs = { 42, 55, 56 })
    void selectedUnfinishedPrefixesRejectEvenWithAnExactCut(long cut) throws Exception {
        var config = config("DCOC1107\\.(A|B)");
        var input = evidence(config);
        assertThatThrownBy(() -> As400OnlineHandoff.prepare(input, Fixture.position(cut), config)).hasMessageContaining("selected legacy prefix");
    }

    @ParameterizedTest
    @ValueSource(longs = { 43, 64 })
    void completedHistoryCanQualifyWithNoOpenCycle(long cut) throws Exception {
        var config = config("DCOC1107\\.(A|B)");
        var candidate = As400OnlineHandoff.prepare(evidence(config), Fixture.position(cut), config);
        assertThat(candidate.openCycles()).isEmpty();
        assertThat(candidate.checkpoint().completedThrough()).isEqualTo(Fixture.position(cut == 64 ? 59 : 43));
        assertThat(candidate.scan()).isEqualTo(Fixture.position(cut));
    }

    @Test
    void temporaryPrefixDeferralDoesNotAuthorizeMigrationOrHideCorruptedRows() throws Exception {
        var config = config("DCOC1107\\.(A|B)");
        var input = evidence(config);
        assertThatThrownBy(() -> As400OnlineHandoff.prepare(input, Fixture.position(42), config))
                .isInstanceOf(As400OnlineHandoff.SelectedPrefixPending.class)
                .satisfies(error -> assertThat(((As400OnlineHandoff.SelectedPrefixPending) error).cycles()).containsExactly(BigInteger.valueOf(31)));
        // The same unchanged history is safe after the actual terminal, not after an elapsed wait.
        assertThat(As400OnlineHandoff.prepare(input, Fixture.position(43), config).openCycles()).isEmpty();
        var damaged = new ArrayList<>(input.entries());
        int index = 0;
        while (damaged.get(index).position().offset().intValue() != 42) {
            index++;
        }
        byte[] row = damaged.get(index).bytes().clone();
        row[197] ^= 1;
        damaged.set(index, new Entry(damaged.get(index).position(), row));
        String digest = As400OnlineHandoff.digest(damaged);
        var invalid = new Evidence(damaged, input.definitions(), input.order(), input.journal(),
                As400OnlineHandoff.fingerprint(input.definitions(), config, digest), input.lower(), input.upper(), input.inventory(), digest, input.visibility());
        assertThatThrownBy(() -> As400OnlineHandoff.prepare(invalid, Fixture.position(42), config))
                .isNotInstanceOf(As400OnlineHandoff.SelectedPrefixPending.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-start", "whole-cycle", "reordered", "missing-cut", "missing-inventory-start", "visibility", "selection", "member-jid" })
    void insufficientOrChangedEvidenceNeverCreatesACandidate(String fault) throws Exception {
        var config = config("DCOC1107\\.A");
        var input = evidence(config);
        List<Entry> entries = new ArrayList<>(input.entries());
        Set<BigInteger> cycles = input.inventory();
        var visibility = input.visibility();
        String digest = input.historyDigest();
        switch (fault) {
            case "missing-start" -> entries.removeIf(e -> e.position().offset().intValue() == 31);
            case "whole-cycle" -> entries.removeIf(e -> new EntryHeaderDecoder().decode(e.bytes(), 0).getCommitCycle().intValue() == 31);
            case "reordered" -> java.util.Collections.swap(entries, 0, 1);
            case "missing-cut" -> entries.removeIf(e -> e.position().offset().intValue() == 42);
            case "missing-inventory-start" -> cycles = Set.of(BigInteger.valueOf(2));
            case "visibility" -> visibility = null;
            case "selection" -> config = config("DCOC1107\\.(A|B)");
            case "member-jid" -> {
                int i = 0;
                while (entries.get(i).position().offset().intValue() != 42) {
                    i++;
                }
                byte[] changed = entries.get(i).bytes().clone();
                changed[197] ^= 1;
                entries.set(i, new Entry(entries.get(i).position(), changed));
                // Even an internally consistent raw manifest cannot waive strict source binding validation.
                digest = As400OnlineHandoff.digest(entries);
            }
            default -> throw new AssertionError(fault);
        }
        final var altered = new Evidence(entries, input.definitions(), input.order(), input.journal(),
                fault.equals("member-jid") ? As400OnlineHandoff.fingerprint(input.definitions(), config, digest) : input.fingerprint(),
                input.lower(), input.upper(), cycles, digest, visibility);
        final var selected = config;
        assertThatThrownBy(() -> As400OnlineHandoff.prepare(altered, Fixture.position(42), selected)).isInstanceOf(Exception.class);
    }

    @Test
    void ordinaryLegacyOffsetsStillCannotEnterBufferedRecovery() throws Exception {
        var config = config("DCOC1107\\.A");
        var input = evidence(config);
        assertThatThrownBy(() -> As400OnlineHandoff.coordinator(input, Map.of("offset.event_sequence", "42"), r -> true))
                .hasMessageContaining("legacy migration is not automatic");
    }

    @Test
    void earlierSourceContextCannotBeDiscardedWhenChoosingTheTransactionFloor() throws Exception {
        var config = config("DCOC1107\\.A");
        var input = evidence(config);
        var entries = new ArrayList<>(input.entries());
        byte[] row = entries.stream().filter(e -> e.position().offset().intValue() == 42).findFirst().orElseThrow().bytes().clone();
        java.nio.ByteBuffer.wrap(row).putLong(24, 29); // Deliberately damaged pre-floor native fixture row.
        row[197] ^= 1;
        entries.add(0, new Entry(Fixture.position(29), row));
        String digest = As400OnlineHandoff.digest(entries);
        var changed = new Evidence(entries, input.definitions(), input.order(), input.journal(),
                As400OnlineHandoff.fingerprint(input.definitions(), config, digest), input.lower(), input.upper(), input.inventory(), digest, input.visibility());
        assertThatThrownBy(() -> As400OnlineHandoff.prepare(changed, Fixture.position(42), config)).isInstanceOf(Exception.class);
    }
}
