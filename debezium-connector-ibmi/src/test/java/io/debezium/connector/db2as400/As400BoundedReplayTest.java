/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400ContinuousRuntimeTest.Source;

class As400BoundedReplayTest {
    @TempDir
    Path directory;

    @Test
    void backlogLargerThanReadBudgetMakesBoundedProgressWithoutChangingOutputOrder() throws Exception {
        var f = new Source(directory, new As400OperatorCutReader.Limits(4, 5, 1_000_000));
        f.tail = 64;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = f.runtime(f.initial, (offset, scan) -> f.reader.readPrefix(offset, scan.orElse(null)), output, records::add)) {
            Map<String, String> saved = f.initial;
            int polls = 0;
            while (true) {
                var progress = runtime.poll(saved);
                assertThat(progress.entries()).isBetween(1, 5);
                saved = progress.checkpoint();
                assertThat(++polls).isLessThan(30);
                if (progress.scan().orElseThrow().offset().intValueExact() == 64) {
                    break;
                }
            }
            assertThat(polls).isGreaterThan(1);
            assertThat(saved).containsEntry("completed_through.sequence", "59");
        }
        assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
    }

    @Test
    void freshInstancesReconstructBeforeWAcrossMultipleReadsWithoutAdvancingDurableCheckpoint() throws Exception {
        var initial = new Source(directory);
        Map<String, String> durable;
        List<SourceRecord> records = new ArrayList<>();
        try (var output = initial.output(); var runtime = initial.runtime(initial.initial, initial::read, output, records::add)) {
            durable = runtime.poll(initial.initial).checkpoint();
        }
        assertThat(durable).containsEntry("replay_from.sequence", "31").containsEntry("completed_through.sequence", "42");
        var f = new Source(directory, new As400OperatorCutReader.Limits(4, 3, 1_000_000));
        f.tail = 64;
        for (int crash = 0; crash < 2; crash++) {
            try (var output = f.output(); var runtime = f.runtime(durable, (offset, scan) -> f.reader.readPrefix(offset, scan.orElse(null)), output, records::add)) {
                var progress = runtime.poll(durable);
                assertThat(progress.scan().orElseThrow().offset().intValueExact()).isLessThan(42);
                assertThat(progress.checkpoint()).isEqualTo(durable);
            }
            assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
        }
        try (var output = f.output(); var runtime = f.runtime(durable, (offset, scan) -> f.reader.readPrefix(offset, scan.orElse(null)), output, records::add)) {
            var saved = durable;
            int polls = 0;
            while (true) {
                var progress = runtime.poll(saved);
                if (progress.scan().orElseThrow().offset().intValueExact() < 42) {
                    assertThat(progress.checkpoint()).isEqualTo(durable);
                    assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8");
                }
                saved = progress.checkpoint();
                assertThat(++polls).isLessThan(50);
                if (progress.scan().orElseThrow().offset().intValueExact() == 64) {
                    break;
                }
            }
        }
        assertThat(As400RecoveryTestSupport.data(records)).containsExactly("c:A:8", "t:B", "c:B:100", "c:A:9");
    }

    @ParameterizedTest
    @ValueSource(ints = { 31, 42 })
    void missingDurableAnchorRejectsBeforeAnyPrefixOutput(int removed) throws Exception {
        var f = new Source(directory, new As400OperatorCutReader.Limits(4, 3, 1_000_000));
        Map<String, String> saved = f.fixture.replay(f.initial, 42, record -> true);
        f.tail = 64;
        f.fixture.entries.removeIf(e -> e.position().offset().intValueExact() == removed);
        List<SourceRecord> records = new ArrayList<>();
        try (var output = f.output(); var runtime = f.runtime(saved, (offset, scan) -> f.reader.readPrefix(offset, scan.orElse(null)), output, records::add)) {
            assertThrows(Exception.class, () -> runtime.poll(saved));
            assertThat(records).isEmpty();
        }
    }

    @Test
    void insufficientByteBudgetCannotSpinWithoutProgress() throws Exception {
        var f = new Source(directory, new As400OperatorCutReader.Limits(4, 3, 236));
        assertThrows(Exception.class, () -> f.reader.readPrefix(f.initial, null));
    }
}
