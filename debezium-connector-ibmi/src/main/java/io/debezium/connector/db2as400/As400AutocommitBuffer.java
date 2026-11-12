/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.debezium.connector.db2as400.As400TransactionBuffer.Change;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.Image;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.connector.db2as400.As400TransactionBuffer.RecordIdentity;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.connector.db2as400.As400TransactionBuffer.TransactionId;
import io.debezium.connector.db2as400.As400TransactionBuffer.Usage;
import io.debezium.data.Envelope.Operation;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;

/**
 * Owned cycle-zero changes, preserving native source identity without inventing
 * commit cycles. UB pins F until its exact producer/table/RRN UP is complete.
 * Optional ordinary-CDC path only; the existing strict profile does not use it.
 */
final class As400AutocommitBuffer implements AutoCloseable {
    private final String journal;
    private final Limits limits;
    private final Supplier<Usage> transactions;
    private final Map<RecordIdentity, Image> before;
    private final As400SpillStore spill;
    private CommittedBatch pending;
    private long bytes;
    private int entries;
    private boolean stopped;

    As400AutocommitBuffer(String journal, Limits limits, Supplier<Usage> transactions) {
        this(journal, limits, transactions, null);
    }

    As400AutocommitBuffer(String journal, Limits limits, Supplier<Usage> transactions, As400SpillStore spill) {
        this.journal = Objects.requireNonNull(journal);
        this.limits = Objects.requireNonNull(limits);
        this.transactions = Objects.requireNonNull(transactions);
        this.spill = spill;
        before = spill == null ? new LinkedHashMap<>() : spill.map("autocommit-before", As400SpillCodec.RECORD, As400SpillCodec.IMAGE);
    }

    void requireIdleTable(TableIdentity table) {
        require(!stopped && pending == null && before.keySet().stream().noneMatch(record -> record.table().tableId().equals(table.tableId())),
                "Schema refresh crosses an incomplete autocommit update");
    }

    Optional<CommittedBatch> accept(As400SourceMetadata source, TableIdentity table, RecordIdentity record, As400RowData row, boolean completedReplay) {
        try {
            require(!stopped && pending == null, "Autocommit buffer stopped or output pending");
            require(source.commitCycle().signum() == 0 && source.controlInfo().nestedCommitLevel() == 0,
                    "Autocommit entry has a transaction scope");
            Objects.requireNonNull(table);
            for (RecordIdentity known : before.keySet()) {
                require(!known.table().tableId().equals(table.tableId()) || known.table().compatibleBinding(table), "Binding changed across a pending update");
            }
            String type = source.journalCode() + "/" + source.entryType();
            if (type.equals("F/CR") || type.equals("D/CG")) {
                require(before.keySet().stream().noneMatch(r -> r.table().equals(table)), "Clear crosses an incomplete autocommit update");
                reserve(0, 1);
                pending = batch(source, source, new Change(Operation.TRUNCATE, table, null, source, null, null));
            }
            else {
                require(record != null && record.table().equals(table) && row != null, "Missing autocommit row identity/image");
                Image earlier = before.get(record);
                if (type.equals("R/UB")) {
                    require(earlier == null, "Repeated autocommit before-image");
                    reserve(row.payloadBytes(), 1);
                    before.put(record, new Image(source, row));
                }
                else if (type.equals("R/UP")) {
                    if (earlier == null && completedReplay) {
                        return Optional.empty(); // Completed UP may have its UB before F; it cannot produce output.
                    }
                    require(earlier != null, "Autocommit update has no exact matching before-image");
                    reserve(row.payloadBytes(), 0);
                    before.remove(record);
                    pending = batch(earlier.source(), source, new Change(Operation.UPDATE, table, record, source, earlier, new Image(source, row)));
                }
                else {
                    require(earlier == null && (type.equals("R/PT") || type.equals("R/PX") || type.equals("R/DL")), "Unsupported or conflicting autocommit row");
                    reserve(row.payloadBytes(), 1);
                    Image image = new Image(source, row);
                    boolean delete = type.equals("R/DL");
                    pending = batch(source, source, new Change(delete ? Operation.DELETE : Operation.CREATE, table, record, source,
                            delete ? image : null, delete ? null : image));
                }
            }
            return Optional.ofNullable(pending);
        }
        catch (RuntimeException e) {
            stopped = true;
            throw e;
        }
    }

    private CommittedBatch batch(As400SourceMetadata start, As400SourceMetadata end, Change change) {
        return new CommittedBatch(new TransactionId(journal, BigInteger.ZERO, start.position()), start, end, List.of(change));
    }

    void complete(CommittedBatch batch) {
        require(!stopped && batch != null && batch == pending, "Wrong autocommit completion");
        bytes -= batch.changes().get(0).bytes();
        entries--;
        pending = null;
    }

    long payloadBytes() {
        return bytes;
    }

    java.util.Collection<JournalPosition> retainedStarts() {
        return new java.util.AbstractCollection<>() {
            @Override
            public int size() {
                return before.size() + (pending == null ? 0 : 1);
            }

            @Override
            public java.util.Iterator<JournalPosition> iterator() {
                return java.util.stream.Stream.concat(before.values().stream().map(Image::position),
                        pending == null ? java.util.stream.Stream.empty() : java.util.stream.Stream.of(pending.startSource().position())).iterator();
            }
        };
    }

    /** Called after transaction admission as well, so the two buffers share one bound. */
    void checkCapacity() {
        Usage other = transactions.get();
        require(bytes <= limits.maxPayloadBytes() - other.payloadBytes()
                && entries <= limits.maxRetainedEntries() - other.retainedEntries(), "Combined transaction/autocommit capacity exceeded");
    }

    private void reserve(long payload, int count) {
        require(payload <= limits.maxPayloadBytes() - bytes && count <= limits.maxRetainedEntries() - entries, "Autocommit capacity exceeded");
        bytes += payload;
        entries += count;
        checkCapacity();
    }

    private void require(boolean valid, String message) {
        if (!valid) {
            stopped = true;
            throw new IllegalArgumentException(message);
        }
    }

    @Override
    public void close() {
        stopped = true;
        if (spill == null) {
            before.clear();
        }
        pending = null;
        entries = 0;
        bytes = 0;
    }
}
