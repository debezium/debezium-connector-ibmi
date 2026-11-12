/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import io.debezium.DebeziumException;
import io.debezium.connector.db2as400.As400TransactionBuffer.CommittedBatch;
import io.debezium.connector.db2as400.As400TransactionBuffer.RecordIdentity;
import io.debezium.connector.db2as400.As400TransactionBuffer.SavepointRollback;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Control;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder.Kind;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/**
 * Normalizes selected captured records plus ALL C controls into the transaction buffer.
 * The reader resolves the CURRENT entry receiver, verifies
 * journal incarnation/order, and must not pass a next-entry/continuation cursor here.
 */
final class As400SourceNormalizer {
    private static final BigInteger MAX_UNSIGNED = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private static final Set<String> FORWARD = Set.of("PT", "PX", "UB", "UP", "DL");
    private static final Set<String> COMPENSATION = Set.of("BR", "DR", "UR");
    private final As400TransactionBuffer buffer;
    private final TableResolver tables;
    private final SavepointPolicy savepoints;
    private final long maximumRowBytes;
    private final As400AutocommitBuffer autocommit;
    private final TransactionControlEntryDecoder controls = new TransactionControlEntryDecoder();
    private final FileChangeEntryDecoder clears = new FileChangeEntryDecoder(RawJournalPage.METADATA_CCSID);
    private boolean stopped;

    As400SourceNormalizer(As400TransactionBuffer buffer, TableResolver tables, long maximumRowBytes) {
        this(buffer, tables, maximumRowBytes, (source, control) -> SavepointRollback.UNRESOLVED);
    }

    As400SourceNormalizer(As400TransactionBuffer buffer, TableResolver tables, long maximumRowBytes, SavepointPolicy savepoints) {
        this(buffer, tables, maximumRowBytes, savepoints, null);
    }

    As400SourceNormalizer(As400TransactionBuffer buffer, TableResolver tables, long maximumRowBytes, SavepointPolicy savepoints,
                          As400AutocommitBuffer autocommit) {
        this.autocommit = autocommit;
        this.buffer = Objects.requireNonNull(buffer);
        this.tables = Objects.requireNonNull(tables);
        this.savepoints = Objects.requireNonNull(savepoints);
        if (maximumRowBytes <= 0) {
            throw new IllegalArgumentException("A finite positive materialization limit is required");
        }
        this.maximumRowBytes = maximumRowBytes;
    }

    Optional<CommittedBatch> accept(EntryHeader header, byte[] data, int offset, JournalPosition currentPosition) {
        return accept(header, data, offset, currentPosition, false);
    }

    Optional<CommittedBatch> accept(EntryHeader header, byte[] data, int offset, JournalPosition currentPosition, boolean completedReplay) {
        if (stopped) {
            throw new NormalizationException("Source normalizer is stopped; reconstruct from a safe checkpoint", null);
        }
        try {
            As400SourceMetadata source = validateSource(header, data, offset, currentPosition, tables);
            if (header.getJournalCode() == 'C') {
                Control control = controls.decode(header, data, offset);
                SavepointRollback disposition = control.kind() == Kind.ROLLBACK_SAVEPOINT
                        ? savepoints.disposition(source, control)
                        : SavepointRollback.UNRESOLVED;
                return buffer.acceptControl(source, control, disposition);
            }
            if (header.getCommitCycle().signum() == 0) {
                if (autocommit == null) {
                    throw new IllegalArgumentException("Cycle-zero operation requires the future output coordinator");
                }
                TableBinding table = table(header, header.getFile(), header.getLibrary(), header.getMember(), ObjectKind.MEMBER_DATA);
                buffer.requireNoConflictingClear(source.commitCycle(), table.identity());
                if (header.getJournalCode() == 'F' && header.getEntryType().equals("CR")) {
                    table.requireTableWideClear();
                    return autocommit.accept(source, table.identity(), null, null, completedReplay);
                }
                if (header.getJournalCode() != 'R' || !FORWARD.contains(header.getEntryType())) {
                    throw new IllegalArgumentException("Unsupported cycle-zero source operation");
                }
                return autocommit.accept(source, table.identity(), record(header, table.identity()),
                        table.materialize(header, data, offset, maximumRowBytes), completedReplay);
            }
            if (header.getJournalCode() == 'D' && header.getEntryType().equals("CG")) {
                var clear = clears.decode(header, data, offset);
                if (!clear.clearsRows()) {
                    TableBinding binding = table(header, clear.file(), clear.library(), clear.member(), ObjectKind.FILE);
                    buffer.requireIdleTable(binding.identity());
                    if (autocommit != null) {
                        autocommit.requireIdleTable(binding.identity());
                    }
                    if (clear.changeType() != '0') {
                        throw new IllegalArgumentException("Unsupported transactional D/CG schema change");
                    }
                    tables.schemaChange(new RawJournalPage.Entry(currentPosition, header,
                            java.util.Arrays.copyOfRange(data, offset, offset + header.getEndOffset())));
                    return Optional.empty();
                }
                if (!header.getFile().equals(clear.file()) || !header.getLibrary().equals(clear.library())
                        || (!header.getMember().isEmpty() && !header.getMember().equals(clear.member()))) {
                    throw new IllegalArgumentException("D/CG payload and header object identities disagree");
                }
                TableBinding table = table(header, clear.file(), clear.library(), clear.member(), ObjectKind.FILE);
                table.requireTableWideClear();
                buffer.truncate(source, table.identity());
                return Optional.empty();
            }
            if (header.getJournalCode() != 'R' || (!FORWARD.contains(header.getEntryType()) && !COMPENSATION.contains(header.getEntryType()))) {
                throw new IllegalArgumentException("Unsupported source operation " + header.getJournalCode() + "/" + header.getEntryType());
            }
            TableBinding table = table(header, header.getFile(), header.getLibrary(), header.getMember(), ObjectKind.MEMBER_DATA);
            RecordIdentity record = record(header, table.identity());
            if (COMPENSATION.contains(header.getEntryType())) {
                // Never decode rollback rows as forward changes. R1/BR/DR/UR also prevent
                // an ambiguous PT/PX/UP during rollback from being emitted as forward output.
                buffer.compensation(source);
                return Optional.empty();
            }
            As400RowData values = table.materialize(header, data, offset, maximumRowBytes);
            switch (header.getEntryType()) {
                case "PT", "PX" -> buffer.insert(source, record, values);
                case "UB" -> buffer.beforeImage(source, record, values);
                case "UP" -> buffer.update(source, record, values);
                case "DL" -> buffer.delete(source, record, values);
                default -> throw new IllegalStateException("Unclassified forward record");
            }
            return Optional.empty();
        }
        catch (Exception e) {
            stopped = true;
            buffer.abort();
            if (autocommit != null) {
                autocommit.close();
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            // Identify the entry without formatting its row values here.
            throw new NormalizationException("Cannot normalize source entry at " + currentPosition, e);
        }
    }

    static As400SourceMetadata validateSource(EntryHeader header, byte[] data, int offset, JournalPosition position) {
        return validateSource(header, data, offset, position, false);
    }

    static As400SourceMetadata validateSource(EntryHeader header, byte[] data, int offset, JournalPosition position, TableResolver tables) throws Exception {
        if (header.getPointerHandle() == 0) {
            return validateSource(header, data, offset, position);
        }
        var binding = Objects.requireNonNull(tables.resolve(header.getFile(), header.getLibrary(), header.getMember()), "Missing excluded LOB binding");
        var identity = binding.identity();
        if (!binding.excludedLob() || !header.getFile().equals(identity.physicalFile()) || !header.getLibrary().equals(identity.tableId().schema())
                || !header.getMember().equals(identity.member()) || !header.getRecordInfo().objectJournalId().equals(identity.memberJournalId())) {
            throw new IllegalArgumentException("Pointer row requires its exact verified excluded LOB binding");
        }
        binding.validateExcludedRow(header, data, offset);
        return validateSource(header, data, offset, position, true);
    }

    private static As400SourceMetadata validateSource(EntryHeader header, byte[] data, int offset, JournalPosition position, boolean excludedPointer) {
        Objects.requireNonNull(header);
        Objects.requireNonNull(data);
        Objects.requireNonNull(position);
        Objects.requireNonNull(position.receiver());
        unsigned(position.offset(), false);
        name(position.receiver().name(), 10);
        name(position.receiver().library(), 10);
        if (offset < 0 || header.getEndOffset() < 236 || (long) offset + header.getEndOffset() > data.length) {
            throw new IllegalArgumentException("Invalid current entry bounds");
        }
        unsigned(header.getSystemSequenceNumber(), true);
        unsigned(header.getCommitCycle(), true);
        name(header.getEntryType(), 2);
        var info = Objects.requireNonNull(header.getControlInfo());
        if (info.nestedCommitLevel() < 0 || info.hasTransactionIdentifier() || info.hasLogicalUnitOfWork()
                || (info.flags() & 0x3f) != (excludedPointer ? 0x20 : 0) || !excludedPointer && header.getPointerHandle() != 0) {
            throw new IllegalArgumentException("Unsupported, minimized, incomplete, or pointer-backed source entry");
        }
        return As400SourceMetadata.from(header, position);
    }

    private TableBinding table(EntryHeader header, String file, String library, String member, ObjectKind kind) throws Exception {
        name(file, 10);
        name(library, 10);
        var info = Objects.requireNonNull(header.getRecordInfo());
        if (info.objectNameIndicator() != '1' || !kind.nativeType.equals(info.objectType())
                || !TableIdentity.validJournalId(info.objectJournalId())) {
            throw new IllegalArgumentException("Missing or ambiguous physical object identity");
        }
        TableBinding binding = Objects.requireNonNull(tables.resolve(file, library, member), "Missing verified table binding");
        TableIdentity identity = binding.identity();
        if (!file.equals(identity.physicalFile()) || !library.equals(identity.tableId().schema())
                || !info.objectJournalId().equals(kind == ObjectKind.FILE ? identity.fileJournalId() : identity.memberJournalId())
                || (!member.equals(identity.member()) && !(kind == ObjectKind.FILE && member.isEmpty()))) {
            throw new IllegalArgumentException("Entry does not match verified file/member object/format binding");
        }
        return binding;
    }

    private RecordIdentity record(EntryHeader header, TableIdentity table) {
        var info = header.getRecordInfo();
        unsigned(info.recordNumber(), false);
        unsigned(info.threadId(), false);
        name(info.jobName(), 10);
        name(info.jobUser(), 10);
        name(info.systemName(), 8);
        if (info.jobNumber() == null || !info.jobNumber().matches("[0-9]{6}") || info.jobNumber().equals("000000")) {
            throw new IllegalArgumentException("Missing source job identity");
        }
        // Length-delimited fields avoid collisions even for unusual quoted source names.
        String producer = part(info.systemName()) + part(info.jobNumber()) + part(info.jobUser()) + part(info.jobName()) + part(info.threadId().toString());
        return new RecordIdentity(table, producer, info.recordNumber());
    }

    private static String part(String value) {
        return value.length() + ":" + value;
    }

    private static void name(String value, int max) {
        if (value == null || value.isBlank() || value.length() > max || value.startsWith("*")) {
            throw new IllegalArgumentException("Missing or unsupported normalized source identifier");
        }
    }

    private static void unsigned(BigInteger value, boolean zeroAllowed) {
        if (value == null || value.signum() < 0 || (!zeroAllowed && value.signum() == 0) || value.compareTo(MAX_UNSIGNED) > 0) {
            throw new IllegalArgumentException("Invalid unsigned source identifier");
        }
    }

    private enum ObjectKind {
        FILE("*FILE"),
        MEMBER_DATA("*QDDS");

        private final String nativeType;

        ObjectKind(String nativeType) {
            this.nativeType = nativeType;
        }
    }

    /**
     * Bootstrap must prove both JIDs belong to this exact file and member in the
     * same journal incarnation and pin the association/layout for the replay window.
     * Never fill a missing JID from the incoming row or infer one JID from the other.
     */
    interface TableResolver {
        TableBinding resolve(String physicalFile, String library) throws Exception;

        default TableBinding resolve(String physicalFile, String library, String member) throws Exception {
            TableBinding binding = resolve(physicalFile, library);
            return binding == null || member.isEmpty() ? binding : binding.forMember(member);
        }

        default boolean allowsSchemaChange(JournalPosition opening) {
            return false;
        }

        default boolean creationLifecycle(RawJournalPage.Entry entry) throws Exception {
            return false;
        }

        default void schemaChange(RawJournalPage.Entry entry) throws Exception {
            throw new IllegalArgumentException("Unsupported transactional D/CG schema change");
        }

        default As400Checkpoint schemaCheckpoint(As400Checkpoint checkpoint) {
            if (!checkpoint.schemaState().equals(As400SchemaState.EMPTY)) {
                throw new IllegalArgumentException("Schema history requires an admitted schema timeline");
            }
            return checkpoint;
        }
    }

    record TableBinding(TableIdentity identity, CompleteRowEntryDecoder decoder, boolean rrnKey, boolean excluded, java.util.Map<String, String> members,
            boolean excludedLob) {
        TableBinding(TableIdentity identity, CompleteRowEntryDecoder decoder, boolean rrnKey, boolean excluded, java.util.Map<String, String> members) {
            this(identity, decoder, rrnKey, excluded, members, false);
        }

        TableBinding(TableIdentity identity, CompleteRowEntryDecoder decoder, boolean rrnKey, boolean excluded) {
            this(identity, decoder, rrnKey, excluded, java.util.Map.of(identity.member(), identity.memberJournalId()));
        }

        TableBinding(TableIdentity identity, CompleteRowEntryDecoder decoder) {
            this(identity, decoder, false, false);
        }

        TableBinding(TableIdentity identity, CompleteRowEntryDecoder decoder, boolean rrnKey) {
            this(identity, decoder, rrnKey, false);
        }

        Object[] values(EntryHeader header, byte[] data, int offset) throws java.io.IOException {
            if (excluded) {
                throw new IllegalStateException("Excluded binding cannot decode output columns");
            }
            Object[] physical = decoder.decode(header, data, offset);
            return rrnKey ? As400BindingDefinition.withRrn(physical, header.getRecordInfo().recordNumber()) : physical;
        }

        TableBinding {
            Objects.requireNonNull(identity);
            members = As400BindingDefinition.validatedMembers(identity, members);
            if (!excluded) {
                Objects.requireNonNull(decoder);
            }
            if (excluded && (decoder != null || rrnKey) || excludedLob && !excluded) {
                throw new IllegalArgumentException("Excluded identity and complete output decoder are distinct bindings");
            }
            name(identity.tableId().schema(), 10);
            name(identity.physicalFile(), 10);
            name(identity.member(), 10);
        }

        TableBinding forMember(String member) {
            String jid = members.get(member);
            if (jid == null) {
                throw new IllegalArgumentException("Unknown member in verified file binding");
            }
            return identity.member().equals(member) ? this
                    : new TableBinding(new TableIdentity(identity.tableId(), identity.physicalFile(), member,
                            identity.fileJournalId(), jid, identity.recordFormat()), decoder, rrnKey, excluded, members, excludedLob);
        }

        void requireTableWideClear() {
            if (members.size() != 1 && !excluded) {
                throw new IllegalArgumentException("A member clear cannot be emitted as a whole-table truncate for a multi-member file");
            }
        }

        As400RowData materialize(EntryHeader header, byte[] data, int offset, long maximumBytes) throws java.io.IOException {
            if (!excluded) {
                return As400RowData.copyOf(values(header, data, offset), maximumBytes);
            }
            validateExcludedRow(header, data, offset);
            return As400RowData.excluded();
        }

        void validateExcludedRow(EntryHeader header, byte[] data, int offset) throws java.io.IOException {
            // No value codecs or nullable guesses for excluded files. Retain the
            // real row/clear effects, pairing and savepoints, and still reject an
            // incomplete physical image or a changed pinned format.
            boolean pointer = excludedLob && header.getPointerHandle() != 0;
            if (pointer) {
                io.debezium.ibmi.db2.journal.retrieve.PointerRowEntryDecoder.metadata(header, data, offset);
            }
            if (header.getJournalCode() != 'R' || !(FORWARD.contains(header.getEntryType()) || pointer && COMPENSATION.contains(header.getEntryType()))
                    || header.getControlInfo() == null
                    || (header.getControlInfo().flags() & 0x3f) != (pointer ? 0x20 : 0) || !pointer && header.getPointerHandle() != 0
                    || (header.getEntryType().equals("UB") || header.getEntryType().equals("DL")) && header.getControlInfo().indicatorFlag() != '1') {
                throw new java.io.IOException("Incomplete or unsupported excluded row image");
            }
            String[] format = identity.recordFormat().split(":", -1);
            int length = Integer.parseInt(format[2]), fields = Integer.parseInt(format[3]);
            int prefix = (int) header.getEntrySpecificDataOffset(), end = header.getEndOffset();
            if (offset < 0 || (long) offset + end > data.length || prefix < 236 || (long) prefix + 16 + length > end) {
                throw new java.io.IOException("Incomplete excluded row payload");
            }
            int declared = 0;
            for (int i = 0; i < 5; i++) {
                int digit = Byte.toUnsignedInt(data[offset + prefix + i]);
                if (digit < 0xf0 || digit > 0xf9) {
                    throw new java.io.IOException("Invalid excluded row length");
                }
                declared = declared * 10 + digit - 0xf0;
            }
            if (declared != length) {
                throw new java.io.IOException("Excluded row differs from pinned physical format");
            }
            int map = header.getNullValueOffest();
            if (map != 0) {
                if (map < 236 || (long) map + 4 > end) {
                    throw new java.io.IOException("Incomplete excluded null map");
                }
                int count = java.nio.ByteBuffer.wrap(data, offset + map, 4).getInt();
                if (count != 0 && count != fields || (long) map + 4 + count > end
                        || map < prefix + 16 + length && (long) map + 4 + count > prefix) {
                    throw new java.io.IOException("Invalid excluded null map bounds");
                }
                for (int i = 0; i < count; i++) {
                    int value = Byte.toUnsignedInt(data[offset + map + 4 + i]);
                    if (value != 0xf0 && value != 0xf1) {
                        throw new java.io.IOException("Defaulted or invalid excluded null indicator");
                    }
                }
            }
        }
    }

    /**
     * Retention/release claims require source evidence. Conservative consumption
     * instead restricts accepted histories: subsequent references to a consumed
     * marker fail closed. Neither choice changes the default unresolved policy.
     */
    interface SavepointPolicy {
        SavepointRollback disposition(As400SourceMetadata source, Control control) throws Exception;

        static SavepointPolicy consumingTargets() {
            return (source, control) -> SavepointRollback.CONSUME_TARGET;
        }
    }

    static final class NormalizationException extends DebeziumException {
        NormalizationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
