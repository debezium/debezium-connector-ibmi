/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.ibm.as400.access.AS400Text;

import io.debezium.connector.db2as400.As400TransactionBuffer.BufferException;
import io.debezium.connector.db2as400.As400TransactionBuffer.Change;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.RecordIdentity;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.connector.db2as400.As400TransactionBuffer.Usage;
import io.debezium.data.Envelope.Operation;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Control;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader.ControlInfo;
import io.debezium.relational.TableId;

class As400TransactionBufferTest {
    private static final BigInteger A = BigInteger.TEN;
    private static final BigInteger B = BigInteger.valueOf(20);
    private static final JournalReceiver RECEIVER = new JournalReceiver("RCV", "LIB");
    private static final TableIdentity TABLE = new TableIdentity(new TableId("DB", "LIB", "TEST"), "TEST", "TEST", "0102030405060708090a",
            As400ConnectProbeConnector.MEMBER_JID, "FMT1");
    private static final TableIdentity OTHER = new TableIdentity(new TableId("DB", "LIB", "OTHER"), "OTHER", "OTHER", "1112030405060708090a", "1b1c1d1e1f2021222324",
            "FMT2");
    private static final RecordIdentity RECORD = record(TABLE, "JOB/THREAD", 1);
    private As400TransactionBuffer buffer = buffer(new Limits(10, 100, 20, 1024));
    private long sequence;

    @Test
    void publishesClearAndReloadOnlyAtCommitInOperationOrder() throws Exception {
        JournalPosition start = control("SC", A);
        JournalPosition first = next();
        buffer.insert(source(first, A), RECORD, bytes(1));
        JournalPosition clear = next();
        buffer.truncate(source(clear, A), TABLE);
        JournalPosition reload = next();
        buffer.insert(source(reload, A), RECORD, bytes(2));
        assertThat(buffer.pendingBatch()).isEmpty();
        assertThat(buffer.oldestTransactionPosition()).contains(start);

        CommittedBatch batch = commit(A);
        assertThat(batch.changes()).extracting(Change::operation).containsExactly(Operation.CREATE, Operation.TRUNCATE, Operation.CREATE);
        assertThat(batch.changes()).extracting(Change::position).containsExactly(first, clear, reload);
        assertThat(batch.changes().get(1).record()).isNull();
        assertThat(batch.changes().get(1).before()).isNull();
        assertThat(batch.changes().get(1).after()).isNull();
        assertThat(payload(batch.changes().get(2).after())).containsExactly(2);
        assertThat(buffer.pendingBatch()).containsSame(batch);
        assertThat(buffer.oldestTransactionPosition()).contains(start);
        assertThat(buffer.usage()).isEqualTo(new Usage(1, 4, 0, 2));
        buffer.completeBatch(batch);
        assertEmpty();
    }

    @Test
    void fullRollbackDiscardsClearReloadImagesAndSavepoints() throws Exception {
        control("SC", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        control("SB", A);
        buffer.truncate(source(next(), A), TABLE);
        buffer.insert(source(next(), A), RECORD, bytes(2));
        buffer.beforeImage(source(next(), A), RECORD, bytes(3));
        control("R1", A);
        buffer.compensation(source(next(), A));
        control("RB", A);
        assertEmpty();
    }

    @Test
    void rollbackReleasesOnlyItsOwnCycleAndAllowsNewWork() throws Exception {
        buffer = buffer(new Limits(2, 4, 1, 2));
        RecordIdentity other = record(OTHER, "JOB/THREAD", 1);
        control("SC", A);
        buffer.truncate(source(next(), A), TABLE);
        JournalPosition startB = control("SC", B);
        buffer.beforeImage(source(next(), B), other, bytes(1));
        control("RB", A);
        assertThat(buffer.oldestTransactionPosition()).contains(startB);
        assertThat(buffer.usage()).isEqualTo(new Usage(1, 2, 0, 1));
        control("SC", A);
        buffer.truncate(source(next(), A), TABLE);
        buffer.update(source(next(), B), other, bytes(2));
        CommittedBatch b = commit(B);
        assertThat(payload(b.changes().get(0).before())).containsExactly(1);
        buffer.completeBatch(b);
        control("RB", A);
        assertEmpty();
    }

    @Test
    void interleavedTransactionsPublishInCommitOrderAndRetainOldestStart() throws Exception {
        RecordIdentity other = record(OTHER, "JOB/THREAD", 1);
        JournalPosition startA = control("SC", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        JournalPosition startB = control("SC", B);
        buffer.truncate(source(next(), B), OTHER);
        buffer.insert(source(next(), A), RECORD, bytes(2));
        buffer.insert(source(next(), B), other, bytes(3));
        CommittedBatch b = commit(B);
        assertThat(b.transactionId().start()).isEqualTo(startB);
        assertThat(b.changes()).extracting(Change::operation).containsExactly(Operation.TRUNCATE, Operation.CREATE);
        assertThat(buffer.oldestTransactionPosition()).contains(startA);
        buffer.completeBatch(b);
        CommittedBatch a = commit(A);
        assertThat(a.changes()).extracting(change -> payload(change.after())[0]).containsExactly((byte) 1, (byte) 2);
        assertThat(a.transactionId().start()).isEqualTo(startA);
        buffer.completeBatch(a);
        assertEmpty();
    }

    @Test
    void updatePairsAreScopedByCycleRecordProducerAndTable() throws Exception {
        control("SC", A);
        control("SC", B);
        List<RecordIdentity> records = List.of(RECORD, record(TABLE, "JOB/THREAD", 2),
                record(TABLE, "OTHERJOB/THREAD", 1), record(OTHER, "JOB/THREAD", 1));
        for (int i = 0; i < records.size(); i++) {
            buffer.beforeImage(source(next(), A), records.get(i), bytes(i));
            buffer.beforeImage(source(next(), B), records.get(i), bytes(i + 10));
        }
        for (int i = records.size() - 1; i >= 0; i--) {
            buffer.update(source(next(), B), records.get(i), bytes(i + 20));
            buffer.update(source(next(), A), records.get(i), bytes(i + 30));
        }
        CommittedBatch b = commit(B);
        assertThat(b.changes()).extracting(change -> payload(change.before())[0]).containsExactly((byte) 13, (byte) 12, (byte) 11, (byte) 10);
        assertThat(b.changes()).extracting(change -> payload(change.after())[0]).containsExactly((byte) 23, (byte) 22, (byte) 21, (byte) 20);
        buffer.completeBatch(b);
        CommittedBatch a = commit(A);
        assertThat(a.changes()).extracting(change -> payload(change.before())[0]).containsExactly((byte) 3, (byte) 2, (byte) 1, (byte) 0);
        assertThat(a.changes()).extracting(Change::operation).containsOnly(Operation.UPDATE);
        buffer.completeBatch(a);
        assertEmpty();
    }

    @Test
    void clearInvalidatesOnlyItsOwnCycleAndTableBeforeImages() throws Exception {
        control("SC", A);
        control("SC", B);
        RecordIdentity other = record(OTHER, "JOB/THREAD", 1);
        buffer.beforeImage(source(next(), A), RECORD, bytes(1));
        buffer.beforeImage(source(next(), A), other, bytes(2));
        buffer.beforeImage(source(next(), B), RECORD, bytes(3));
        buffer.truncate(source(next(), A), TABLE);
        buffer.update(source(next(), A), other, bytes(4));
        CommittedBatch a = commit(A);
        assertThat(a.changes()).extracting(Change::operation).containsExactly(Operation.TRUNCATE, Operation.UPDATE);
        assertThat(payload(a.changes().get(1).before())).containsExactly(2);
        buffer.completeBatch(a);
        buffer.update(source(next(), B), RECORD, bytes(5));
        CommittedBatch b = commit(B);
        assertThat(payload(b.changes().get(0).before())).containsExactly(3);
        buffer.completeBatch(b);
        assertEmpty();
    }

    @Test
    void clearPreventsReuseOfStaleBeforeImage() throws Exception {
        control("SC", A);
        buffer.beforeImage(source(next(), A), RECORD, bytes(1));
        buffer.truncate(source(next(), A), TABLE);
        assertThrows(BufferException.class, () -> buffer.update(source(next(), A), RECORD, bytes(2)));
        assertStopped();
    }

    @Test
    void savepointRollbackRemovesClearAndReloadAndCanRepeatWithRetainedTarget() throws Exception {
        control("SC", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        JournalPosition target = control("SB", A);
        buffer.truncate(source(next(), A), TABLE);
        buffer.insert(source(next(), A), RECORD, bytes(2));
        rollbackTo(A, target, SavepointRollback.RETAIN_TARGET);
        assertThat(buffer.usage()).isEqualTo(new Usage(1, 2, 1, 1));
        buffer.truncate(source(next(), A), TABLE);
        buffer.insert(source(next(), A), RECORD, bytes(3));
        rollbackTo(A, target, SavepointRollback.RETAIN_TARGET);
        buffer.insert(source(next(), A), RECORD, bytes(4));
        CommittedBatch batch = commit(A);
        assertThat(batch.changes()).extracting(change -> payload(change.after())[0]).containsExactly((byte) 1, (byte) 4);
        buffer.completeBatch(batch);
        assertEmpty();
    }

    @Test
    void rollbackOfReloadPreservesEarlierClear() throws Exception {
        control("SC", A);
        buffer.truncate(source(next(), A), TABLE);
        JournalPosition target = control("SB", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        rollbackTo(A, target, SavepointRollback.RETAIN_TARGET);
        buffer.insert(source(next(), A), RECORD, bytes(2));
        CommittedBatch batch = commit(A);
        assertThat(batch.changes()).extracting(Change::operation).containsExactly(Operation.TRUNCATE, Operation.CREATE);
        assertThat(payload(batch.changes().get(1).after())).containsExactly(2);
    }

    @Test
    void rollbackToNonTopSavepointRemovesDescendantsAndPendingImages() throws Exception {
        control("SC", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        JournalPosition outer = control("SB", A);
        buffer.delete(source(next(), A), RECORD, bytes(1));
        control("SB", A);
        buffer.truncate(source(next(), A), TABLE);
        buffer.beforeImage(source(next(), A), RECORD, bytes(2));
        buffer.compensation(source(next(), A));
        rollbackTo(A, outer, SavepointRollback.RELEASE_TARGET);
        assertThat(buffer.usage()).isEqualTo(new Usage(1, 2, 0, 1));
        buffer.beforeImage(source(next(), A), RECORD, bytes(3));
        buffer.update(source(next(), A), RECORD, bytes(4));
        CommittedBatch batch = commit(A);
        assertThat(batch.changes()).extracting(Change::operation).containsExactly(Operation.CREATE, Operation.UPDATE);
        assertThat(payload(batch.changes().get(1).before())).containsExactly(3);
        buffer.completeBatch(batch);
        assertEmpty();
    }

    @Test
    void releasingNonTopSavepointKeepsChangesButRemovesTargetAndDescendants() throws Exception {
        control("SC", A);
        JournalPosition outer = control("SB", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        control("SB", A);
        buffer.truncate(source(next(), A), TABLE);
        targetControl("SQ", A, outer, SavepointRollback.UNRESOLVED);
        assertThat(buffer.usage().savepoints()).isZero();
        assertThat(commit(A).changes()).extracting(Change::operation).containsExactly(Operation.CREATE, Operation.TRUNCATE);
    }

    @ParameterizedTest
    @ValueSource(strings = { "released", "descendant", "other-cycle", "unresolved" })
    void unknownOrUnresolvedSavepointTargetsStopBuffer(String scenario) throws Exception {
        control("SC", A);
        JournalPosition outer = control("SB", A);
        JournalPosition inner = control("SB", A);
        switch (scenario) {
            case "released" -> rollbackTo(A, outer, SavepointRollback.RELEASE_TARGET);
            case "descendant" -> rollbackTo(A, outer, SavepointRollback.RETAIN_TARGET);
            case "other-cycle" -> control("SC", B);
            default -> {
            }
        }
        BigInteger cycle = scenario.equals("other-cycle") ? B : A;
        JournalPosition target = scenario.equals("descendant") ? inner : outer;
        SavepointRollback disposition = scenario.equals("unresolved") ? SavepointRollback.UNRESOLVED : SavepointRollback.RETAIN_TARGET;
        assertThrows(BufferException.class, () -> rollbackTo(cycle, target, disposition));
        assertStopped();
    }

    @ParameterizedTest
    @ValueSource(strings = { "CM", "SB", "SQ", "duplicate", "insert", "delete" })
    void unpairedUpdateCannotCrossForwardBoundaries(String operation) throws Exception {
        control("SC", A);
        JournalPosition target = control("SB", A);
        buffer.beforeImage(source(next(), A), RECORD, bytes(1));
        assertThrows(BufferException.class, () -> {
            switch (operation) {
                case "duplicate" -> buffer.beforeImage(source(next(), A), RECORD, bytes(2));
                case "insert" -> buffer.insert(source(next(), A), RECORD, bytes(2));
                case "delete" -> buffer.delete(source(next(), A), RECORD, bytes(2));
                case "SQ" -> targetControl("SQ", A, target, SavepointRollback.UNRESOLVED);
                default -> control(operation, A);
            }
        });
        assertStopped();
    }

    @Test
    void rolledBackPendingImageCannotBePairedLater() throws Exception {
        control("SC", A);
        JournalPosition target = control("SB", A);
        buffer.beforeImage(source(next(), A), RECORD, bytes(1));
        rollbackTo(A, target, SavepointRollback.RETAIN_TARGET);
        assertThrows(BufferException.class, () -> buffer.update(source(next(), A), RECORD, bytes(2)));
        assertStopped();
    }

    @ParameterizedTest
    @CsvSource({ "CM,3,0,false,false,0", "CM,4,0,false,false,0", "CM,X,0,false,false,0",
            "RB,1,0,false,false,0", "RB,3,0,false,false,0", "RB,X,0,false,false,0",
            "CM,0,1,false,false,0", "RB,0,1,false,false,0", "CM,0,-1,false,false,0",
            "CM,0,0,true,false,0", "CM,0,0,false,true,0", "CM,0,0,false,false,32",
            "PC,0,0,false,false,0", "CN,0,0,false,false,0", "ZZ,0,0,false,false,0" })
    void unsafeDecodedControlsCannotPublishOrDiscardPendingWork(String type, char flag, long level, boolean xid, boolean luw, int flags) throws Exception {
        JournalPosition start = control("SC", A);
        buffer.truncate(source(next(), A), TABLE);
        Control decoded = decode(type, A, new ControlInfo(flag, level, xid, luw, (byte) flags), null);
        assertThrows(BufferException.class, () -> accept(next(), decoded));
        assertThat(buffer.oldestTransactionPosition()).contains(start);
        assertThat(buffer.usage().transactions()).isEqualTo(1);
        assertStopped();
    }

    @ParameterizedTest
    @CsvSource({ "CM,0", "CM,2", "RB,0", "RB,2" })
    void honorsSupportedOutcomesFromActualControlDecoder(String type, char flag) throws Exception {
        control("SC", A);
        buffer.truncate(source(next(), A), TABLE);
        var result = accept(next(), decode(type, A, new ControlInfo(flag, 0, false, false, (byte) 0), null));
        if (type.equals("CM")) {
            assertThat(result).isPresent();
            buffer.completeBatch(result.orElseThrow());
        }
        else {
            assertThat(result).isEmpty();
        }
        assertEmpty();
    }

    @ParameterizedTest
    @CsvSource({ "undo,commit", "undo,insert", "R1,commit", "R1,insert", "R1,savepoint" })
    void unresolvedRollbackCannotPublishOrResumeForwardChanges(String begin, String next) throws Exception {
        control("SC", A);
        JournalPosition target = control("SB", A);
        buffer.truncate(source(next(), A), TABLE);
        if (begin.equals("undo")) {
            buffer.compensation(source(next(), A));
        }
        else {
            control("R1", A);
        }
        assertThrows(BufferException.class, () -> {
            switch (next) {
                case "commit" -> commit(A);
                case "insert" -> buffer.insert(source(next(), A), RECORD, bytes(1));
                default -> rollbackTo(A, target, SavepointRollback.RETAIN_TARGET);
            }
        });
        assertStopped();
    }

    @ParameterizedTest
    @ValueSource(strings = { "bytes", "entries", "transactions", "savepoints" })
    void capacityExhaustionStopsWithoutEvictingPendingWork(String resource) throws Exception {
        buffer = buffer(new Limits(1, resource.equals("bytes") ? 100 : 2, 1, resource.equals("bytes") ? 1 : 1024));
        JournalPosition start = control("SC", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        control("SB", A);
        assertThrows(BufferException.class, () -> {
            switch (resource) {
                case "bytes" -> buffer.beforeImage(source(next(), A), RECORD, bytes(1, 2));
                case "entries" -> buffer.truncate(source(next(), A), TABLE);
                case "transactions" -> control("SC", B);
                default -> control("SB", A);
            }
        });
        assertThat(buffer.usage()).isEqualTo(new Usage(1, 2, 1, 1));
        assertThat(buffer.oldestTransactionPosition()).contains(start);
        assertStopped();
    }

    @Test
    void savepointRollbackAndCompletionReleaseCapacityForReuse() throws Exception {
        buffer = buffer(new Limits(1, 2, 1, 2));
        control("SC", A);
        JournalPosition target = control("SB", A);
        buffer.beforeImage(source(next(), A), RECORD, bytes(1));
        buffer.update(source(next(), A), RECORD, bytes(2));
        assertThat(buffer.usage()).isEqualTo(new Usage(1, 2, 1, 2));
        rollbackTo(A, target, SavepointRollback.RELEASE_TARGET);
        assertThat(buffer.usage()).isEqualTo(new Usage(1, 1, 0, 0));
        buffer.insert(source(next(), A), RECORD, bytes(3, 4));
        CommittedBatch batch = commit(A);
        buffer.completeBatch(batch);
        control("SC", B);
        buffer.delete(source(next(), B), RECORD, bytes(5, 6));
        CommittedBatch other = commit(B);
        assertThat(payload(other.changes().get(0).before())).containsExactly(5, 6);
        buffer.completeBatch(other);
        assertEmpty();
    }

    @Test
    void batchOwnsPayloadsAndDoesNotExposeMutableArraysOrLists() throws Exception {
        control("SC", A);
        byte[] original = new byte[]{ 1 };
        JournalPosition beforePosition = next();
        buffer.beforeImage(source(beforePosition, A), RECORD, As400RowData.copyOf(new Object[]{ original }, 1024));
        original[0] = 2;
        buffer.update(source(next(), A), RECORD, As400RowData.copyOf(new Object[]{ original }, 1024));
        original[0] = 3;
        CommittedBatch batch = commit(A);
        Change change = batch.changes().get(0);
        payload(change.before())[0] = 4;
        payload(change.after())[0] = 5;
        assertThat(payload(change.before())).containsExactly(1);
        assertThat(payload(change.after())).containsExactly(2);
        assertThat(change.before().position()).isEqualTo(beforePosition);
        assertThrows(UnsupportedOperationException.class, () -> batch.changes().clear());
    }

    @Test
    void nextInputCannotOvertakePendingBatchEvenForAnotherCycle() throws Exception {
        JournalPosition start = control("SC", A);
        control("SC", B);
        CommittedBatch batch = commit(A);
        assertThat(batch.changes()).isEmpty();
        assertThrows(BufferException.class, () -> buffer.insert(source(next(), B), RECORD, bytes(1)));
        assertThrows(BufferException.class, () -> buffer.completeBatch(batch));
        assertThat(buffer.oldestTransactionPosition()).contains(start);
    }

    @Test
    void completionMustMatchActualPendingBatch() throws Exception {
        control("SC", A);
        CommittedBatch batch = commit(A);
        CommittedBatch copy = new CommittedBatch(batch.transactionId(), batch.startSource(), batch.commitSource(), batch.changes());
        assertThrows(BufferException.class, () -> buffer.completeBatch(copy));
        assertThat(buffer.usage().transactions()).isEqualTo(1);
        assertStopped();
    }

    @Test
    void oldestStartUsesAdmissionOrderAcrossReceiverRollover() throws Exception {
        JournalPosition old = new JournalPosition(BigInteger.valueOf(9999), new JournalReceiver("ZOLD", "LIB"));
        accept(old, decode("SC", A));
        JournalPosition fresh = new JournalPosition(BigInteger.ONE, new JournalReceiver("ANEW", "LIB"));
        accept(fresh, decode("SC", B));
        assertThat(buffer.oldestTransactionPosition()).contains(old);
        CommittedBatch first = accept(new JournalPosition(BigInteger.TWO, fresh.receiver()), decode("CM", A)).orElseThrow();
        buffer.completeBatch(first);
        assertThat(buffer.oldestTransactionPosition()).contains(fresh);
    }

    @Test
    void reusedResolvedCycleGetsDistinctStartIdentity() throws Exception {
        control("SC", A);
        CommittedBatch first = commit(A);
        buffer.completeBatch(first);
        control("SC", A);
        CommittedBatch second = commit(A);
        assertThat(first.transactionId()).isNotEqualTo(second.transactionId());
        assertThat(second.transactionId().journalIdentity()).isEqualTo("LIB/JRN/incarnation1");
    }

    @ParameterizedTest
    @ValueSource(strings = { "zero", "missing", "duplicate", "nested-start" })
    void cyclesMustHaveOneSupportedStart(String scenario) throws Exception {
        control("SC", A);
        assertThrows(BufferException.class, () -> {
            switch (scenario) {
                case "zero" -> control("SC", BigInteger.ZERO);
                case "missing" -> buffer.insert(source(next(), B), RECORD, bytes(1));
                case "duplicate" -> control("SC", A);
                default -> accept(next(), decode("SC", B, new ControlInfo('0', 1, false, false, (byte) 0), null));
            }
        });
        assertStopped();
    }

    @ParameterizedTest
    @ValueSource(strings = { "member", "object", "member-jid", "format", "file" })
    void changedObjectMappingWhileAnyCycleIsPendingStopsCapture(String field) throws Exception {
        control("SC", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        control("SC", B);
        TableIdentity changed = new TableIdentity(TABLE.tableId(), field.equals("file") ? "NEWFILE" : TABLE.physicalFile(),
                field.equals("member") ? "NEWMBR" : TABLE.member(), field.equals("object") ? "2122030405060708090a" : TABLE.fileJournalId(),
                field.equals("member-jid") ? "3132030405060708090a" : TABLE.memberJournalId(),
                field.equals("format") ? "NEWFMT" : TABLE.recordFormat());
        assertThrows(BufferException.class, () -> buffer.truncate(source(next(), B), changed));
        assertStopped();
    }

    @ParameterizedTest
    @ValueSource(strings = { "BC", "EC" })
    void lifecycleControlsCannotResolveOrReleaseActiveCycles(String type) throws Exception {
        JournalPosition start = control("SC", A);
        control(type, BigInteger.ZERO);
        assertThat(buffer.oldestTransactionPosition()).contains(start);
        assertThrows(BufferException.class, () -> control(type, A));
        assertStopped();
    }

    @ParameterizedTest
    @ValueSource(strings = { "position", "receiver", "sequence", "order", "image", "record", "table", "control" })
    void malformedNormalizedInputsPermanentlyStopBuffer(String field) throws Exception {
        JournalPosition start = control("SC", A);
        assertThrows(BufferException.class, () -> {
            switch (field) {
                case "position" -> buffer.truncate(source(null, A), TABLE);
                case "receiver" -> buffer.truncate(source(new JournalPosition(BigInteger.TWO, null), A), TABLE);
                case "sequence" -> buffer.truncate(source(new JournalPosition(BigInteger.ZERO, RECEIVER), A), TABLE);
                case "order" -> buffer.truncate(source(start, A), TABLE);
                case "image" -> buffer.insert(source(next(), A), RECORD, null);
                case "record" -> buffer.insert(source(next(), A), null, bytes(1));
                case "table" -> buffer.truncate(source(next(), A), null);
                default -> accept(next(), null);
            }
        });
        assertStopped();
    }

    @Test
    void closeDiscardsAllMemoryAndPreventsReuse() throws Exception {
        control("SC", A);
        buffer.insert(source(next(), A), RECORD, bytes(1));
        commit(A);
        buffer.close();
        assertThat(buffer.usage()).isEqualTo(new Usage(0, 0, 0, 0));
        assertThat(buffer.oldestTransactionPosition()).isEmpty();
        assertStopped();
    }

    private java.util.Optional<CommittedBatch> accept(JournalPosition position, Control control) {
        return accept(position, control, SavepointRollback.UNRESOLVED);
    }

    private java.util.Optional<CommittedBatch> accept(JournalPosition position, Control control, SavepointRollback disposition) {
        As400SourceMetadata source = control == null ? source(position, A)
                : new As400SourceMetadata(position, Instant.EPOCH, BigInteger.ONE, control.commitCycle(), 'C', control.entryType(),
                        EntryHeader.RecordInfo.UNKNOWN, control.controlInfo());
        return buffer.acceptControl(source, control, disposition);
    }

    private static As400SourceMetadata source(JournalPosition position, BigInteger cycle) {
        return position == null ? null
                : new As400SourceMetadata(position, Instant.EPOCH, BigInteger.ONE, cycle, 'R', "PT",
                        EntryHeader.RecordInfo.UNKNOWN, new ControlInfo('0', 0, false, false, (byte) 0));
    }

    private static byte[] payload(As400TransactionBuffer.Image image) {
        return (byte[]) image.values()[0];
    }

    private static As400TransactionBuffer buffer(Limits limits) {
        return new As400TransactionBuffer("LIB/JRN/incarnation1", limits);
    }

    private JournalPosition next() {
        return new JournalPosition(BigInteger.valueOf(++sequence), RECEIVER);
    }

    private JournalPosition control(String type, BigInteger cycle) throws Exception {
        JournalPosition position = next();
        accept(position, decode(type, cycle));
        return position;
    }

    private CommittedBatch commit(BigInteger cycle) throws Exception {
        return accept(next(), decode("CM", cycle)).orElseThrow();
    }

    private void rollbackTo(BigInteger cycle, JournalPosition target, SavepointRollback disposition) throws Exception {
        targetControl("SU", cycle, target, disposition);
    }

    private void targetControl(String type, BigInteger cycle, JournalPosition target, SavepointRollback disposition) throws Exception {
        accept(next(), decode(type, cycle, new ControlInfo('0', 1, false, false, (byte) 0), target.offset()), disposition);
    }

    /** Exercise the real control decoder, including SQ/SU's EBCDIC prefix and target. */
    private Control decode(String type, BigInteger cycle) throws Exception {
        return decode(type, cycle, new ControlInfo('0', 0, false, false, (byte) 0), null);
    }

    private Control decode(String type, BigInteger cycle, ControlInfo info, BigInteger target) throws Exception {
        byte[] data = new byte[276];
        if (target != null) {
            new AS400Text(5).toBytes("00020", data, 240);
            new AS400Text(20).toBytes(String.format("%020d", target), data, 256);
        }
        EntryHeader header = new EntryHeader(data.length, 0, 240, BigInteger.valueOf(sequence), BigInteger.ZERO,
                Instant.EPOCH, 'C', type, "", cycle, data.length, 0, "RCV", "LIB", info);
        return new TransactionControlEntryDecoder().decode(header, data, 0);
    }

    private static RecordIdentity record(TableIdentity table, String producer, long number) {
        return new RecordIdentity(table, producer, BigInteger.valueOf(number));
    }

    private static As400RowData bytes(int... values) {
        byte[] result = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            result[i] = (byte) values[i];
        }
        return As400RowData.copyOf(new Object[]{ result }, 1024);
    }

    private void assertStopped() throws Exception {
        assertThrows(BufferException.class, () -> buffer.pendingBatch());
        Control control = decode("CM", A);
        assertThrows(BufferException.class, () -> accept(next(), control));
    }

    private void assertEmpty() {
        assertThat(buffer.pendingBatch()).isEmpty();
        assertThat(buffer.oldestTransactionPosition()).isEmpty();
        assertThat(buffer.usage()).isEqualTo(new Usage(0, 0, 0, 0));
    }
}
