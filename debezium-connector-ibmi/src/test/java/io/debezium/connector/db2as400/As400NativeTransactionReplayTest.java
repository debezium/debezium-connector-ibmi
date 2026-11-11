/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;

import io.debezium.connector.db2as400.As400SourceNormalizer.NormalizationException;
import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;

/** Native characterization, not permission to publish or a substitute for startup proof. */
class As400NativeTransactionReplayTest {
    private static final CompleteRowEntryDecoder ROW = new CompleteRowEntryDecoder(
            new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
    private static final Map<String, List<Integer>> INSERTS = Map.ofEntries(
            Map.entry("initial/T_commit", List.of(100)),
            Map.entry("initial/T_rollback", List.of(200)),
            Map.entry("initial/T_savepoint", List.of(300)),
            Map.entry("initial/D_commit", List.of(100, 101)),
            Map.entry("initial/D_rollback", List.of(200, 201)),
            Map.entry("initial/D_savepoint", List.of(300, 301, 302, 304, 303)),
            Map.entry("initial/interleaved", List.of(400)),
            Map.entry("initial/immediate_rollback", List.of(601)),
            Map.entry("dcg/T_commit", List.of(1000, 101)),
            Map.entry("dcg/T_rollback", List.of(1100, 201)),
            Map.entry("dcg/T_savepoint", List.of(1200, 301, 302, 304, 303)),
            Map.entry("dcg/D_commit", List.of(2000, 101)),
            Map.entry("dcg/D_rollback", List.of(2100, 201)),
            Map.entry("dcg/D_savepoint", List.of(2200, 301, 302, 304, 303)),
            Map.entry("dcg/interleaved", List.of(401, 501, 402)));

    static Stream<Arguments> cases() throws IOException {
        List<Arguments> cases = new ArrayList<>();
        for (String family : List.of("initial", "dcg")) {
            for (JsonNode scenario : fixture(family)) {
                cases.add(Arguments.of(family, scenario.get("name").asText(), scenario));
            }
        }
        return cases.stream();
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 53 })
    void nativeThreadOmissionIsPreservedAtTheDocumentedRelativeByteOffset(int offset) throws Exception {
        int count = 0;
        for (String family : List.of("initial", "dcg")) {
            for (JsonNode scenario : fixture(family)) {
                for (Entry entry : entries(scenario)) {
                    // RJNE0200 stores the unsigned 8-byte thread ID at relative offset 40.
                    // The native bytes are zero; do not manufacture a producer ID here.
                    BigInteger nativeThread = new BigInteger(1, Arrays.copyOfRange(entry.bytes(), 40, 48));
                    assertThat(nativeThread).isZero();
                    byte[] page = new byte[offset + entry.bytes().length];
                    Arrays.fill(page, 0, offset, (byte) 0xa5);
                    System.arraycopy(entry.bytes(), 0, page, offset, entry.bytes().length);
                    EntryHeader decoded = new EntryHeaderDecoder().decode(page, offset);
                    assertThat(decoded.getRecordInfo().threadId()).isEqualTo(nativeThread);
                    assertThat(decoded.getRecordInfo()).isEqualTo(entry.header().getRecordInfo());
                    assertThat(decoded.getSequenceNumber()).isEqualTo(entry.header().getSequenceNumber());
                    assertThat(decoded.getCommitCycle()).isEqualTo(entry.header().getCommitCycle());
                    if (decoded.getJournalCode() == 'R') {
                        // A present job does not establish a missing thread within that job.
                        assertThat(decoded.getRecordInfo().jobName()).isNotBlank();
                        assertThat(decoded.getRecordInfo().jobNumber()).isNotEqualTo("000000");
                    }
                    count++;
                }
            }
        }
        assertThat(count).isEqualTo(287);
    }

    @ParameterizedTest(name = "{0}/{1}")
    @MethodSource("cases")
    void strictWireDecodersReadActualRowsAndControls(String family, String name, JsonNode scenario) throws Exception {
        List<Integer> inserts = new ArrayList<>();
        Set<BigInteger> savepoints = new HashSet<>();
        List<Character> clears = new ArrayList<>();
        for (Entry entry : entries(scenario)) {
            EntryHeader header = entry.header();
            if (header.getJournalCode() == 'R' && Set.of("PT", "PX", "UB", "UP", "DL").contains(header.getEntryType())) {
                Object[] values = ROW.decode(header, entry.bytes(), 0);
                assertThat(values).hasSize(1);
                assertThat(values[0]).isInstanceOf(Integer.class);
                if (Set.of("PT", "PX").contains(header.getEntryType())) {
                    inserts.add((Integer) values[0]);
                }
            }
            if (header.getJournalCode() == 'C') {
                var control = new TransactionControlEntryDecoder().decode(header, entry.bytes(), 0);
                if (header.getEntryType().equals("SB")) {
                    savepoints.add(header.getSequenceNumber());
                }
                if (control.savepointSequence() != null) {
                    assertThat(savepoints).contains(control.savepointSequence());
                    assertThat(control.savepointSequence()).isLessThan(header.getSequenceNumber());
                }
            }
            if (header.getJournalCode() == 'D' && header.getEntryType().equals("CG")) {
                var clear = new FileChangeEntryDecoder().decode(header, entry.bytes(), 0);
                assertThat(clear.clearsRows()).isTrue();
                assertThat(clear.file()).isEqualTo(header.getFile());
                assertThat(clear.library()).isEqualTo(header.getLibrary());
                clears.add(clear.changeType());
            }
        }
        assertThat(inserts).containsExactlyElementsOf(INSERTS.get(family + "/" + name));
        if (family.equals("initial")) {
            // Same-table uncommitted writes prevented D/CG; DELETE used individual rows.
            assertThat(clears).isEmpty();
        }
        else if (name.equals("interleaved")) {
            assertThat(clears).containsExactly('8', '3');
        }
        else {
            char subtype = name.startsWith("T") ? '8' : '3';
            assertThat(clears).containsExactlyElementsOf(name.endsWith("savepoint") ? List.of(subtype, subtype) : List.of(subtype));
        }
    }

    @ParameterizedTest(name = "{0}/{1}")
    @MethodSource("cases")
    void swappedFileAndMemberBindingsFailClosedWithoutPublishingPartialTransactions(String family, String name, JsonNode scenario) throws Exception {
        List<Entry> entries = entries(scenario);
        Map<String, TableBinding> bindings = new HashMap<>();
        for (var entry : bindings().entrySet()) {
            TableIdentity identity = entry.getValue().identity();
            bindings.put(entry.getKey(), new TableBinding(new TableIdentity(identity.tableId(), identity.physicalFile(), identity.member(),
                    identity.memberJournalId(), identity.fileJournalId(), identity.recordFormat()), ROW));
        }
        // These are diagnostic bindings. A member JID must not be treated as a file JID.
        var buffer = new As400TransactionBuffer("FIXTURE_ONLY", new Limits(16, 1000, 100, 65536));
        var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024);
        Entry rejected = null;
        for (Entry entry : entries) {
            var header = entry.header();
            // Component input contract: all C controls and selected R/D-CG operations.
            // Object/journaling transitions remain in the fixture and need separate validation.
            if (!(header.getJournalCode() == 'C' || header.getJournalCode() == 'R'
                    || header.getJournalCode() == 'D' && header.getEntryType().equals("CG"))) {
                continue;
            }
            try {
                assertThat(normalizer.accept(header, entry.bytes(), 0, entry.position())).isEmpty();
            }
            catch (NormalizationException failure) {
                assertThat(failure.getCause().getMessage()).containsAnyOf(
                        "Missing or ambiguous physical object identity", "Entry does not match verified single-member object/format binding");
                rejected = entry;
                break;
            }
        }
        assertThat(rejected).as("Native identities must not accept swapped file/member roles").isNotNull();
        Entry failed = rejected;
        assertThat(assertThrows(NormalizationException.class,
                () -> normalizer.accept(failed.header(), failed.bytes(), 0, failed.position())).getMessage()).contains("is stopped");
        Entry control = entries.stream().filter(entry -> entry.header().getJournalCode() == 'C').findFirst().orElseThrow();
        assertThat(assertThrows(As400TransactionBuffer.BufferException.class,
                () -> buffer.acceptControl(As400SourceMetadata.from(control.header(), control.position()),
                        new TransactionControlEntryDecoder().decode(control.header(), control.bytes(), 0)))
                .getMessage()).contains("is stopped");
        // abort() latches failure; close() releases diagnostic state and retained payloads.
        buffer.close();
        assertThat(buffer.usage().transactions()).isZero();
        assertThat(buffer.usage().retainedEntries()).isZero();
        assertThat(buffer.usage().payloadBytes()).isZero();
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "D_commit", "T_rollback", "D_rollback", "interleaved" })
    void nativePayloadComponentReplayWithSyntheticThreadUsesPairedIdsAndOwnedValues(String name) throws Exception {
        Map<String, TableBinding> bindings = bindings();
        List<CommittedBatch> batches = new ArrayList<>();
        Map<JournalPosition, As400SourceMetadata> sources = new HashMap<>();
        try (var buffer = new As400TransactionBuffer("FIXTURE_ONLY", new Limits(16, 1000, 100, 65536))) {
            var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024);
            for (Entry raw : entries(scenario("dcg", name))) {
                Entry entry = withSyntheticThread(raw);
                if (!selected(entry.header())) {
                    continue;
                }
                sources.put(entry.position(), As400SourceMetadata.from(entry.header(), entry.position()));
                var result = normalizer.accept(entry.header(), entry.bytes(), 0, entry.position());
                // Reuse/discard every transport buffer before checking committed output.
                Arrays.fill(entry.bytes(), (byte) 0);
                if (result.isPresent()) {
                    batches.add(result.get());
                    buffer.completeBatch(result.get());
                }
            }
            assertThat(buffer.usage().transactions()).isZero();
        }
        List<String> operations = new ArrayList<>();
        for (CommittedBatch batch : batches) {
            assertThat(batch.startSource()).isEqualTo(sources.get(batch.startSource().position()));
            assertThat(batch.commitSource()).isEqualTo(sources.get(batch.commitPosition()));
            for (var change : batch.changes()) {
                assertThat(change.source()).isEqualTo(sources.get(change.position()));
                assertThat(change.table()).isEqualTo(bindings.get(change.table().physicalFile()).identity());
                String event = change.operation().code() + ":" + change.table().physicalFile();
                if (change.after() != null) {
                    event += ":" + change.after().values()[0];
                    assertThat(change.record().recordNumber()).isEqualTo(change.source().recordInfo().recordNumber());
                    assertThat(change.record().table()).isEqualTo(change.table());
                    assertThat(change.source().recordInfo().objectJournalId()).isEqualTo(change.table().memberJournalId());
                }
                else {
                    assertThat(change.source().recordInfo().objectJournalId()).isEqualTo(change.table().fileJournalId());
                }
                operations.add(event);
            }
        }
        switch (name) {
            case "T_commit" -> assertThat(operations).containsExactly("c:MARK:1000", "t:TC2", "c:TC2:101");
            case "D_commit" -> assertThat(operations).containsExactly("c:MARK:2000", "t:DC2", "c:DC2:101");
            case "interleaved" -> {
                assertThat(batches).hasSize(2);
                assertThat(operations).containsExactly("t:IB2", "c:IB2:501", "t:IA2", "c:IA2:401", "c:IA2:402");
                assertThat(batches.get(0).commitPosition().offset()).isLessThan(batches.get(1).commitPosition().offset());
            }
            default -> assertThat(batches).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "D_commit", "T_rollback", "D_rollback", "T_savepoint", "D_savepoint", "interleaved" })
    void originalNativeRowsPassObjectBindingButStillRejectMissingThreadIdentity(String name) throws Exception {
        Map<String, TableBinding> bindings = bindings();
        try (var buffer = new As400TransactionBuffer("FIXTURE_ONLY", new Limits(16, 1000, 100, 65536))) {
            var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024);
            Entry failed = null;
            for (Entry entry : entries(scenario("dcg", name))) {
                if (!selected(entry.header())) {
                    continue;
                }
                if (entry.header().getJournalCode() == 'R') {
                    assertThat(entry.header().getRecordInfo().threadId()).isZero();
                    var failure = assertThrows(NormalizationException.class,
                            () -> normalizer.accept(entry.header(), entry.bytes(), 0, entry.position()));
                    assertThat(failure.getCause().getMessage()).isEqualTo("Invalid unsigned source identifier");
                    failed = entry;
                    break;
                }
                assertThat(normalizer.accept(entry.header(), entry.bytes(), 0, entry.position())).isEmpty();
            }
            assertThat(failed).isNotNull();
            Entry rejected = failed;
            assertThat(assertThrows(NormalizationException.class,
                    () -> normalizer.accept(rejected.header(), rejected.bytes(), 0, rejected.position())).getMessage()).contains("is stopped");
            // Terminal controls cannot turn a rejected native transaction into output.
            int terminals = 0;
            for (Entry terminal : entries(scenario("dcg", name))) {
                if (terminal.header().getJournalCode() == 'C' && Set.of("CM", "RB").contains(terminal.header().getEntryType())) {
                    terminals++;
                    assertThat(assertThrows(NormalizationException.class,
                            () -> normalizer.accept(terminal.header(), terminal.bytes(), 0, terminal.position())).getMessage()).contains("is stopped");
                }
            }
            assertThat(terminals).isPositive();
            assertThrows(As400TransactionBuffer.BufferException.class, buffer::pendingBatch);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_savepoint", "D_savepoint" })
    void syntheticThreadAndCorrectIdentityDoNotAuthorizeUnresolvedNativeSavepointRollback(String name) throws Exception {
        Map<String, TableBinding> bindings = bindings();
        try (var buffer = new As400TransactionBuffer("FIXTURE_ONLY", new Limits(16, 1000, 100, 65536))) {
            var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024);
            Entry failed = null;
            for (Entry raw : entries(scenario("dcg", name))) {
                Entry entry = withSyntheticThread(raw);
                if (!selected(entry.header())) {
                    continue;
                }
                if (entry.header().getEntryType().equals("SU")) {
                    var failure = assertThrows(NormalizationException.class,
                            () -> normalizer.accept(entry.header(), entry.bytes(), 0, entry.position()));
                    assertThat(failure.getCause().getMessage()).contains("Savepoint rollback disposition is unresolved");
                    failed = entry;
                    break;
                }
                assertThat(normalizer.accept(entry.header(), entry.bytes(), 0, entry.position())).isEmpty();
            }
            assertThat(failed).isNotNull();
            assertThrows(As400TransactionBuffer.BufferException.class, buffer::pendingBatch);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "D_commit", "T_rollback", "D_rollback", "T_savepoint", "D_savepoint", "interleaved" })
    void syntheticThreadAndCorrectIdentityDoNotAuthorizeNativeJournalingTransitions(String name) throws Exception {
        Map<String, TableBinding> bindings = bindings();
        try (var buffer = new As400TransactionBuffer("FIXTURE_ONLY", new Limits(16, 1000, 100, 65536))) {
            var normalizer = new As400SourceNormalizer(buffer, (file, library) -> bindings.get(file), 1024);
            Entry failed = null;
            for (Entry raw : entries(scenario("dcg", name))) {
                Entry entry = withSyntheticThread(raw);
                if (entry.header().getJournalCode() == 'F' && entry.header().getEntryType().equals("CH")) {
                    assertThrows(NormalizationException.class, () -> normalizer.accept(entry.header(), entry.bytes(), 0, entry.position()));
                    failed = entry;
                    break;
                }
                assertThat(normalizer.accept(entry.header(), entry.bytes(), 0, entry.position())).isEmpty();
            }
            assertThat(failed).isNotNull();
            assertThrows(As400TransactionBuffer.BufferException.class, buffer::pendingBatch);
        }
    }

    private static Entry withSyntheticThread(Entry entry) {
        if (entry.header().getJournalCode() != 'R') {
            return entry;
        }
        // Native capture omitted thread identity. Do not loosen record() or rewrite
        // the fixtures: explicitly synthesize it only for downstream component tests.
        assertThat(entry.header().getRecordInfo().threadId()).isZero();
        byte[] bytes = entry.bytes().clone();
        ByteBuffer.wrap(bytes).putLong(40, 1);
        return new Entry(bytes, new EntryHeaderDecoder().decode(bytes, 0), entry.position());
    }

    private static boolean selected(EntryHeader header) {
        // Component-only projection; never a production journal filtering policy.
        return header.getJournalCode() == 'C' || header.getJournalCode() == 'R'
                || header.getJournalCode() == 'D' && header.getEntryType().equals("CG");
    }

    private static Map<String, TableBinding> bindings() throws IOException {
        Map<String, TableBinding> result = new HashMap<>();
        try (var stream = As400NativeTransactionReplayTest.class.getResourceAsStream("/native-transactions/bindings.json")) {
            for (JsonNode table : new ObjectMapper().readTree(stream).get("tables")) {
                String file = table.get("file").asText();
                result.put(file, new TableBinding(new TableIdentity(new TableId("TEST", table.get("library").asText(), file), file,
                        table.get("member").asText(), table.get("fileJournalId").asText(), table.get("memberJournalId").asText(), "FIXTURE_INT_NOT_NULL"), ROW));
            }
        }
        return result;
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_commit", "D_commit" })
    void nativeClearAndReloadIdentifyDifferentObjectKindsAndJids(String name) throws Exception {
        List<Entry> entries = entries(scenario("dcg", name));
        Entry clear = entries.stream().filter(e -> e.header().getEntryType().equals("CG")).findFirst().orElseThrow();
        Entry row = entries.stream().filter(e -> e.header().getJournalCode() == 'R'
                && e.header().getFile().equals(clear.header().getFile())).findFirst().orElseThrow();
        assertThat(clear.header().getRecordInfo().objectType()).isEqualTo("*FILE");
        assertThat(row.header().getRecordInfo().objectType()).isEqualTo("*QDDS");
        assertThat(clear.header().getRecordInfo().objectJournalId()).isNotEqualTo(row.header().getRecordInfo().objectJournalId());
        assertThat(clear.header().getCommitCycle()).isEqualTo(row.header().getCommitCycle());
        assertThat(row.header().getMember()).isEqualTo(row.header().getFile());
    }

    @ParameterizedTest
    @ValueSource(strings = { "T_savepoint", "D_savepoint" })
    void repeatedSqlRollbackUsesReestablishedNativeSavepointTargets(String name) throws Exception {
        List<BigInteger> starts = new ArrayList<>();
        List<BigInteger> targets = new ArrayList<>();
        for (Entry entry : entries(scenario("dcg", name))) {
            var header = entry.header();
            if (header.getJournalCode() == 'C' && header.getControlInfo().nestedCommitLevel() == 2) {
                if (header.getEntryType().equals("SB")) {
                    starts.add(header.getSequenceNumber());
                }
                if (header.getEntryType().equals("SU")) {
                    targets.add(new TransactionControlEntryDecoder().decode(header, entry.bytes(), 0).savepointSequence());
                }
            }
        }
        // SQL rolls back twice to S1, but the second SU targets a newly established SB.
        // This is trace evidence, not approval of a general savepoint policy.
        assertThat(starts).hasSize(3);
        assertThat(targets).containsExactly(starts.get(0), starts.get(1));
    }

    @Test
    void actualInterleavingCommitsLaterStartedDeleteBeforeEarlierTruncate() throws Exception {
        List<Entry> entries = entries(scenario("dcg", "interleaved"));
        Map<String, BigInteger> clearCycles = new HashMap<>();
        List<BigInteger> commits = new ArrayList<>();
        for (Entry entry : entries) {
            if (entry.header().getEntryType().equals("CG")) {
                clearCycles.put(entry.header().getFile(), entry.header().getCommitCycle());
            }
            if (entry.header().getJournalCode() == 'C' && entry.header().getEntryType().equals("CM")) {
                commits.add(entry.header().getCommitCycle());
            }
        }
        assertThat(clearCycles.get("IA2")).isLessThan(clearCycles.get("IB2"));
        assertThat(commits).containsExactly(clearCycles.get("IB2"), clearCycles.get("IA2"));
    }

    private static JsonNode fixture(String family) throws IOException {
        try (var stream = As400NativeTransactionReplayTest.class.getResourceAsStream("/native-transactions/" + family + ".json")) {
            return new ObjectMapper().readTree(stream).get("cases");
        }
    }

    private static JsonNode scenario(String family, String name) throws IOException {
        for (JsonNode scenario : fixture(family)) {
            if (scenario.get("name").asText().equals(name)) {
                return scenario;
            }
        }
        throw new IllegalArgumentException("Missing native scenario " + name);
    }

    private static List<Entry> entries(JsonNode scenario) {
        List<Entry> result = new ArrayList<>();
        for (JsonNode raw : scenario.get("entries")) {
            byte[] bytes = HexFormat.of().parseHex(raw.get("hex").asText());
            EntryHeader header = new EntryHeaderDecoder().decode(bytes, 0);
            assertThat(header.getSequenceNumber().toString()).isEqualTo(raw.get("sequence").asText());
            result.add(new Entry(bytes, header, new JournalPosition(header.getSequenceNumber(),
                    new JournalReceiver(raw.get("receiver").asText(), raw.get("receiver_library").asText()))));
        }
        return result;
    }

    private record Entry(byte[] bytes, EntryHeader header, JournalPosition position) {
    }
}
