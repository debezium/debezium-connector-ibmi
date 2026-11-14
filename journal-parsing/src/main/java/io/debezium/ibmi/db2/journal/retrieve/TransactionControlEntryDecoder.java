/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400ZonedDecimal;

import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeader;

/**
 * Classifies C controls without authorizing transaction publication. In particular,
 * nested or distributed cycles still require a transaction-aware consumer.
 * See https://www.ibm.com/docs/en/ssw_ibm_i_74/rzaki/rzakivarlength.htm.
 */
public class TransactionControlEntryDecoder implements JournalEntryDeocder<TransactionControlEntryDecoder.Control> {
    private static final int SAVEPOINT_SEQUENCE_LENGTH = 20;
    private static final BigInteger MAX_SEQUENCE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    private final AS400ZonedDecimal length = new AS400ZonedDecimal(5, 0);
    private final AS400Text sequence = new AS400Text(SAVEPOINT_SEQUENCE_LENGTH);

    @Override
    public Control decode(EntryHeader header, byte[] data, int offset) throws IOException {
        if (header.getJournalCode() != 'C') {
            throw new IOException("Expected a C journal control entry");
        }
        Kind kind = switch (header.getEntryType()) {
            case "SC" -> Kind.START;
            case "CM" -> Kind.COMMIT;
            case "RB" -> Kind.ROLLBACK;
            case "R1" -> Kind.START_ROLLBACK;
            case "SB" -> Kind.START_SAVEPOINT;
            case "SQ" -> Kind.RELEASE_SAVEPOINT;
            case "SU" -> Kind.ROLLBACK_SAVEPOINT;
            case "BC" -> Kind.BEGIN_COMMITMENT_CONTROL;
            case "EC" -> Kind.END_COMMITMENT_CONTROL;
            default -> Kind.UNSUPPORTED;
        };
        char flag = header.getControlInfo().indicatorFlag();
        Outcome outcome = switch (kind) {
            case COMMIT -> switch (flag) {
                case '0', '2' -> Outcome.DURABLE_COMMIT;
                case '3', '4' -> Outcome.NON_DURABLE_COMMIT;
                default -> Outcome.UNKNOWN;
            };
            case ROLLBACK -> switch (flag) {
                case '0', '2' -> Outcome.FULL_ROLLBACK;
                case '1', '3' -> Outcome.INCOMPLETE_ROLLBACK;
                default -> Outcome.UNKNOWN;
            };
            case UNSUPPORTED -> Outcome.UNKNOWN;
            default -> Outcome.NONE;
        };
        BigInteger target = kind == Kind.RELEASE_SAVEPOINT || kind == Kind.ROLLBACK_SAVEPOINT
                ? savepointSequence(header, data, offset)
                : null;
        return new Control(header.getEntryType(), kind, header.getCommitCycle(), header.getControlInfo(), outcome, target);
    }

    private BigInteger savepointSequence(EntryHeader header, byte[] data, int offset) throws IOException {
        long prefix = (long) offset + header.getEntrySpecificDataOffset();
        long end = Math.min(data.length, (long) offset + header.getEndOffset());
        if (offset < 0 || header.getEntrySpecificDataOffset() <= 0 || prefix + ENTRY_SPECIFIC_DATA_OFFSET > end) {
            throw new IOException("Incomplete savepoint entry-specific data prefix");
        }
        try {
            int dataLength = ((BigDecimal) length.toObject(data, (int) prefix)).intValueExact();
            long start = prefix + ENTRY_SPECIFIC_DATA_OFFSET;
            if (dataLength < SAVEPOINT_SEQUENCE_LENGTH || start + dataLength > end) {
                throw new IOException("Incomplete savepoint target sequence");
            }
            String value = ((String) sequence.toObject(data, (int) start)).trim();
            if (!value.matches("[0-9]+")) {
                throw new IOException("Invalid savepoint target sequence");
            }
            BigInteger target = new BigInteger(value);
            if (target.signum() <= 0 || target.compareTo(MAX_SEQUENCE) > 0) {
                throw new IOException("Savepoint target sequence is outside the journal sequence range");
            }
            return target;
        }
        catch (RuntimeException e) {
            throw new IOException("Invalid savepoint entry-specific data", e);
        }
    }

    public enum Kind {
        START,
        COMMIT,
        ROLLBACK,
        START_ROLLBACK,
        START_SAVEPOINT,
        RELEASE_SAVEPOINT,
        ROLLBACK_SAVEPOINT,
        BEGIN_COMMITMENT_CONTROL,
        END_COMMITMENT_CONTROL,
        UNSUPPORTED
    }

    public enum Outcome {
        NONE,
        DURABLE_COMMIT,
        NON_DURABLE_COMMIT,
        FULL_ROLLBACK,
        INCOMPLETE_ROLLBACK,
        UNKNOWN
    }

    public record Control(String entryType, Kind kind, BigInteger commitCycle, EntryHeader.ControlInfo controlInfo,
            Outcome outcome, BigInteger savepointSequence) {
    }
}
