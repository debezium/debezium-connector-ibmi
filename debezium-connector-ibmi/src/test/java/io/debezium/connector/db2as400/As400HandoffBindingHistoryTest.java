/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;

class As400HandoffBindingHistoryTest {
    @TempDir
    Path directory;

    @Test
    void nativeOriginHistoryPersistsBeforeOffsetAndFreshRecoveryKeepsExactFwWithoutFixtureFingerprint() throws Exception {
        var json = new ObjectMapper();
        var raw = Configuration.create().with("name", "handofftest").with("database.hostname", "no-live-ibmi.invalid")
                .with("topic.prefix", "test").with("database.dbname", "DB").with("database.schema", "DCNC0966")
                .with("skipped.operations", "none").with("provide.transaction.metadata", false).with("probe.topic", "events-handofftest").build();
        As400ProspectiveReplay.NativeInput input;
        try (var stream = getClass().getResourceAsStream("/native-transactions/prospective-boundary.json")) {
            input = As400ProspectiveReplay.prepare(json.readTree(stream), new As400ConnectorConfig(raw), As400OnlineFenceEvidenceTest::replayMetadata);
        }
        var file = directory.resolve("bindings.jsonl");
        var legacy = Map.of("offset.processed", "true", "offset.event_sequence", "57", "snapshot_completed", "true",
                "offset.receiver", input.origin().receiver().name(), "offset.receiver_library", input.origin().receiver().library());
        var empty = new As400HandoffBindingHistory(raw, input.origin(), () -> new As400RecoveryTestSupport.FileHistory(file));
        assertThat(empty.pending(legacy)).isEmpty();
        assertThat(Files.exists(file)).isFalse();
        var initial = new As400HandoffBindingHistory(raw, input.origin(), () -> new As400RecoveryTestSupport.FileHistory(file))
                .initialize(input.evidence(), input.candidate().checkpoint());
        assertThat(initial.checkpoint().replayFrom()).isEqualTo(input.candidate().checkpoint().replayFrom());
        assertThat(initial.checkpoint().completedThrough()).isEqualTo(input.cut());
        assertThat(initial.checkpoint().captureFingerprint()).isNotEqualTo(input.evidence().fingerprint());
        assertThat(Files.readAllLines(file)).hasSize(1);
        assertThat(json.readTree(Files.readString(file)).path("position").path("sequence").asText()).isEqualTo("72");
        // No native capture or current catalog is used to recover pending facts.
        var fresh = new As400HandoffBindingHistory(raw, input.origin(), () -> new As400RecoveryTestSupport.FileHistory(file));
        var pending = fresh.pending(legacy).orElseThrow();
        assertThat(pending.origin()).isEqualTo(input.origin());
        assertThat(pending.definitions()).isEqualTo(initial.runtime().definitions());
        assertThat(fresh.resumeCandidate(input.evidence(), input.candidate().checkpoint(), pending).checkpoint()).isEqualTo(initial.checkpoint());
        assertThatThrownBy(() -> fresh.pending(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> fresh.pending(initial.checkpoint().toOffset())).isInstanceOf(IllegalArgumentException.class);
        // Simulate a crash after history acknowledgement but before offset commit.
        var repeated = new As400HandoffBindingHistory(raw, input.origin(), () -> new As400RecoveryTestSupport.FileHistory(file))
                .initialize(input.evidence(), input.candidate().checkpoint());
        assertThat(repeated.checkpoint()).isEqualTo(initial.checkpoint());
        assertThat(Files.readAllLines(file)).hasSize(1);
        Map<String, ?> loaded = json.readValue(json.writeValueAsBytes(initial.checkpoint().toOffset()), new TypeReference<Map<String, Object>>() {
        });
        var rotated = raw.edit().with("value.converter.basic.auth.user.info", "fixture:rotated").build();
        var recovered = new As400HandoffBindingHistory(rotated, input.origin(), () -> new As400RecoveryTestSupport.FileHistory(file))
                .recover(input.evidence(), loaded);
        assertThat(recovered.checkpoint()).isEqualTo(initial.checkpoint());
        assertThat(recovered.runtime().definitions()).isEqualTo(initial.runtime().definitions());
        for (String key : java.util.List.of("probe.topic", "table.include.list", "table.exclude.list", "schema.include.list", "schema.exclude.list")) {
            var changed = raw.edit().with(key, "other").build();
            assertThatThrownBy(() -> new As400HandoffBindingHistory(changed, input.origin(), () -> new As400RecoveryTestSupport.FileHistory(file))
                    .recover(input.evidence(), loaded)).isInstanceOf(Exception.class);
        }
        Files.delete(file);
        assertThatThrownBy(() -> fresh.resumeCandidate(input.evidence(), input.candidate().checkpoint(), pending)).isInstanceOf(Exception.class);
        assertThatThrownBy(() -> new As400HandoffBindingHistory(raw, input.origin(), () -> new As400RecoveryTestSupport.FileHistory(file))
                .recover(input.evidence(), loaded)).isInstanceOf(Exception.class);
        assertThat(Files.exists(file)).isFalse();
    }
}
