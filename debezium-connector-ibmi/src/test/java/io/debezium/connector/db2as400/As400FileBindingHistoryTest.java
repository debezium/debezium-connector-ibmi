/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;
import io.debezium.relational.Tables;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.relational.history.TableChanges;
import io.debezium.storage.file.history.FileSchemaHistory;

class As400FileBindingHistoryTest {
    @TempDir
    Path directory;

    private Configuration config(Path file) {
        return Configuration.create().with("schema.history.internal", FileSchemaHistory.class.getName()).with(FileSchemaHistory.FILE_PATH, file.toString()).build();
    }

    private SchemaHistory open(Path file, SchemaHistory implementation, List<HistoryRecord> recovered) {
        var listener = org.mockito.Mockito.mock(SchemaHistoryListener.class);
        org.mockito.Mockito.doAnswer(call -> recovered.add(call.getArgument(0))).when(listener)
                .onChangeFromHistory(org.mockito.ArgumentMatchers.any());
        implementation.configure(config(file), HistoryRecordComparator.INSTANCE, listener, false);
        return implementation;
    }

    private List<HistoryRecord> recover(Path file, SchemaHistory implementation) throws Exception {
        var records = new ArrayList<HistoryRecord>();
        var history = open(file, implementation, records);
        try {
            history.start();
            history.recover(Map.of("server", "test"), Map.of(), new Tables(), null);
        }
        finally {
            history.stop();
        }
        return records;
    }

    private void append(SchemaHistory history, String sequence) {
        history.record(Map.of("server", "test"), Map.of("sequence", sequence), null, null, null, new TableChanges(), null);
    }

    private Path writeOne() {
        Path file = directory.resolve("history.jsonl");
        var history = open(file, new As400FileBindingHistory(), new ArrayList<>());
        try {
            history.initializeStorage();
            history.start();
            append(history, "1");
        }
        finally {
            history.stop();
        }
        return file;
    }

    @Test
    void shouldPersistStandardHistoryFormatAndRecoverWithAFreshInstance() throws Exception {
        Path file = directory.resolve("nested/data/history.jsonl");
        var history = open(file, new As400FileBindingHistory(), new ArrayList<>());
        history.initializeStorage();
        try {
            history.start();
            append(history, "1");
            append(history, "2");
        }
        finally {
            history.stop();
        }
        var strict = recover(file, new As400FileBindingHistory());
        var standard = recover(file, new FileSchemaHistory());
        assertThat(strict).hasSize(2);
        assertThat(strict.stream().map(r -> r.document().toString()).toList())
                .containsExactlyElementsOf(standard.stream().map(r -> r.document().toString()).toList());
        assertThat(Files.readAllLines(file)).hasSize(2);
        assertThatThrownBy(() -> append(history, "3")).hasMessageContaining("stopped");
        assertThat(Files.readAllLines(file)).hasSize(2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "truncated", "blank", "invalid-json", "invalid-utf8", "incomplete", "oversized" })
    void shouldRejectLostOrDamagedHistoryWithoutRepairingIt(String damage) throws Exception {
        Path file = writeOne();
        byte[] good = Files.readAllBytes(file);
        switch (damage) {
            case "missing" -> Files.delete(file);
            case "truncated" -> Files.write(file, java.util.Arrays.copyOf(good, good.length - 1));
            case "blank" -> Files.writeString(file, "\n", StandardOpenOption.APPEND);
            case "invalid-json" -> Files.writeString(file, "{\n", StandardOpenOption.APPEND);
            case "invalid-utf8" -> Files.write(file, new byte[]{ (byte) 0xc3, 0x28, '\n' }, StandardOpenOption.APPEND);
            case "incomplete" -> Files.writeString(file, "{}\n", StandardOpenOption.APPEND);
            case "oversized" -> Files.writeString(file, "x".repeat(As400BindingHistory.MAX_RECORD_BYTES + 1) + "\n", StandardOpenOption.APPEND);
        }
        byte[] before = Files.exists(file) ? Files.readAllBytes(file) : null;
        assertThatThrownBy(() -> recover(file, new As400FileBindingHistory())).isInstanceOf(RuntimeException.class);
        if (before == null) {
            assertThat(file).doesNotExist();
        }
        else {
            assertThat(Files.readAllBytes(file)).isEqualTo(before);
        }
    }

    @Test
    void shouldRejectLockedOrMissingHistoryBeforeAppendAndPermitRecoveryAfterRelease() throws Exception {
        Path file = writeOne();
        byte[] before = Files.readAllBytes(file);
        var writer = open(file, new As400FileBindingHistory(), new ArrayList<>());
        try {
            writer.start();
            try (var channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE); var lock = channel.lock()) {
                assertThatThrownBy(() -> append(writer, "2")).isInstanceOf(RuntimeException.class);
                assertThatThrownBy(() -> recover(file, new As400FileBindingHistory())).isInstanceOf(RuntimeException.class);
                assertThat(Files.readAllBytes(file)).isEqualTo(before);
            }
            Path moved = directory.resolve("retained.jsonl");
            Files.move(file, moved);
            assertThatThrownBy(() -> append(writer, "2")).hasMessageContaining("Binding history disappeared");
            assertThat(file).doesNotExist();
            assertThat(Files.readAllBytes(moved)).isEqualTo(before);
            Files.move(moved, file);
            append(writer, "2");
        }
        finally {
            writer.stop();
        }
        assertThat(recover(file, new As400FileBindingHistory())).hasSize(2);
    }

    @Test
    void shouldRecoverTheEligibleCandidatePrefixWithoutLoadingAnUnboundedFile() throws Exception {
        Path file = writeOne();
        String line = Files.readString(file);
        Files.writeString(file, line.repeat(As400BindingHistory.MAX_HISTORY_RECORDS + 1));
        var records = new ArrayList<HistoryRecord>();
        var history = new As400FileBindingHistory();
        open(file, history, records);
        history.allowCandidateAppends();
        try {
            history.start();
            history.recover(Map.of("server", "test"), Map.of(), new Tables(), null);
            assertThat(records).hasSize(As400BindingHistory.MAX_HISTORY_RECORDS);
        }
        finally {
            history.stop();
        }
        assertThatThrownBy(() -> recover(file, new As400FileBindingHistory())).isInstanceOf(RuntimeException.class);
        var writer = open(file, new As400FileBindingHistory(), new ArrayList<>());
        String before = Files.readString(file);
        try {
            writer.start();
            assertThatThrownBy(() -> append(writer, "2")).isInstanceOf(RuntimeException.class);
        }
        finally {
            writer.stop();
        }
        assertThat(Files.readString(file)).isEqualTo(before);
    }

    @Test
    void shouldSelectOnlyTheDurableOnlineCandidateAndRejectLostHistory() throws Exception {
        var fixture = new Fixture(directory);
        fixture.config = config(fixture.file);
        fixture.factory = As400BindingHistory.storage(fixture.config);
        var legacy = Fixture.legacyOffset(28);
        var first = fixture.history().recordOnlineCandidate(fixture.journalIdentity, Fixture.position(29), fixture.receiverIdentity, fixture.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), fixture.receiverIdentity), legacy);
        var saved = As400Checkpoint.afterVerifiedBoundary(fixture.journalIdentity, first.fingerprint(), Fixture.position(29), fixture.receiverIdentity).toOffset();
        fixture.history().recordOnlineCandidate(fixture.journalIdentity, Fixture.position(31), fixture.receiverIdentity, fixture.definitions,
                Fixture.POLICY, new As400BindingHistory.Discovery(Fixture.position(29), fixture.receiverIdentity), legacy);
        var reopened = new As400BindingHistory(As400BindingHistory.storage(fixture.config), fixture.config, Fixture.POLICY.sourcePartition());
        assertThat(reopened.recoverOnlineCandidate(saved, fixture.journalIdentity, Fixture.POLICY).fingerprint()).isEqualTo(first.fingerprint());
        assertThat(reopened.pendingOnlineCandidate(legacy, Fixture.POLICY).orElseThrow().origin()).isEqualTo(Fixture.position(31));
        Files.delete(fixture.file);
        assertThatThrownBy(() -> reopened.recoverOnlineCandidate(saved, fixture.journalIdentity, Fixture.POLICY)).hasMessageContaining("history is absent");
        assertThat(fixture.file).doesNotExist();
    }

    @Test
    void shouldRejectTheUnhardenedFileStoreBeforeAnyHistoryWrite() throws Exception {
        var f = new Fixture(directory);
        f.config = config(f.file);
        f.factory = FileSchemaHistory::new;
        assertThatThrownBy(f::initialize).hasMessageContaining("strict recovery and forced writes");
        assertThat(f.file).doesNotExist();
    }
}
