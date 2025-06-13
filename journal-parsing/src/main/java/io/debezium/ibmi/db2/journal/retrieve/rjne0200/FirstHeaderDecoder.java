/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rjne0200;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.StringHelpers;

public class FirstHeaderDecoder {
    private static final Logger log = LoggerFactory.getLogger(FirstHeaderDecoder.class);

    private final AS400Structure structure;

    public FirstHeaderDecoder() {
        final ArrayList<AS400DataType> dataTypes = new ArrayList<>();
        final FieldDescription[] fds = new FieldDescription[]{
                new BinaryFieldDescription(new AS400Bin4(), "0 bytes returned"),
                new BinaryFieldDescription(new AS400Bin4(), "1 offset to first journal entry"),
                new BinaryFieldDescription(new AS400Bin4(), "2 number of entries retrieved"),
                new CharacterFieldDescription(new AS400Text(1), "3 continuation indicator"),
                new CharacterFieldDescription(new AS400Text(10), "4 continuation starting receiver"),
                new CharacterFieldDescription(new AS400Text(10), "5 continuation starting receiver library"),
                new CharacterFieldDescription(new AS400Text(20), "6 continutation starting sequence number"),
                // new CharacterFieldDescription(new AS400Text(11), "7 reserved"),
        };
        for (int i = 0; i < fds.length; i++) {
            dataTypes.add(fds[i].getDataType());
        }
        structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
    }

    public FirstHeader decode(byte[] data, JournalProcessedPosition fetchedToJournalPosition) {
        final Object[] os = (Object[]) structure.toObject(data);

        JournalProcessedPosition pos = fetchedToJournalPosition;
        final Integer offset = (Integer) os[1];
        OffsetStatus status = (offset != null && offset > 0) ? OffsetStatus.DATA
                : OffsetStatus.NO_DATA;

        if ("1".equals(os[3])) {
            status = OffsetStatus.MORE_DATA_NEW_OFFSET;
            final String receiver = StringHelpers.safeTrim((String) os[4]);
            final String library = StringHelpers.safeTrim((String) os[5]);
            final String offsetStr = StringHelpers.safeTrim((String) os[6]);
            final BigInteger nextOffset = new BigInteger(offsetStr);
            log.debug("continuation offset {} {}", receiver, nextOffset);
            final JournalPosition jp = new JournalPosition(nextOffset, new JournalReceiver(receiver, library));
            pos = new JournalProcessedPosition(jp, Instant.EPOCH, false);
        }

        return new FirstHeader(
                (Integer) os[0],
                offset,
                (Integer) os[2],
                status,
                pos);
    }
}
