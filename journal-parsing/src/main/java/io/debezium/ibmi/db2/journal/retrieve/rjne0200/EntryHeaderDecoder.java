/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rjne0200;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Bin1;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400Timestamp;
import com.ibm.as400.access.AS400UnsignedBin2;
import com.ibm.as400.access.AS400UnsignedBin4;
import com.ibm.as400.access.AS400UnsignedBin8;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;
import com.ibm.as400.access.TimestampFieldDescription;

import io.debezium.ibmi.db2.journal.retrieve.XaTransactionDecoder;

public class EntryHeaderDecoder {
    private final AS400Structure structure;
    private final XaTransactionDecoder txDecoder = new XaTransactionDecoder();
    private final ReceiverNameDecoder nameDecoder = new ReceiverNameDecoder();
    private static String[] EMPTY = { "", "" };
    private static final Logger log = LoggerFactory.getLogger(EntryHeaderDecoder.class);

    public EntryHeaderDecoder() {
        ArrayList<AS400DataType> dataTypes = new ArrayList<AS400DataType>();
        AS400Timestamp timeType = new AS400Timestamp();

        try {
            Field privateDTSFormat = AS400Timestamp.class.getDeclaredField("FORMAT_DTS");
            privateDTSFormat.setAccessible(true);
            int dtsformat = (int) privateDTSFormat.get(timeType);
            Method privateSetFormat = AS400Timestamp.class.getDeclaredMethod("setFormat", int.class);
            privateSetFormat.setAccessible(true);
            privateSetFormat.invoke(timeType, dtsformat);
        }
        catch (Exception e) {
            log.error("failed setting up date formatting", e);
        }

        FieldDescription[] fds = new FieldDescription[]{
                new BinaryFieldDescription(new AS400UnsignedBin4(), "0 displacement to next entries headers"),
                new BinaryFieldDescription(new AS400UnsignedBin4(), "1 displacement to null value indicators"),
                new BinaryFieldDescription(new AS400UnsignedBin4(), "2 displacement to this entry specific data "),
                new BinaryFieldDescription(new AS400UnsignedBin4(), "3 displacement to this entry transaction identifier"),
                new BinaryFieldDescription(new AS400UnsignedBin4(), "4 displacement to this entry logical unit of work"),
                new BinaryFieldDescription(new AS400UnsignedBin4(), "5 displacement to this entry receiver information"),
                new BinaryFieldDescription(new AS400UnsignedBin8(), "6 sequence number"),
                new TimestampFieldDescription(timeType, "7 unformatted timestamp"),
                new BinaryFieldDescription(new AS400UnsignedBin8(), "8 thread identifier"),
                new BinaryFieldDescription(new AS400UnsignedBin8(), "9 system sequence number"),
                new BinaryFieldDescription(new AS400UnsignedBin8(), "10 count/relative record number"),
                new BinaryFieldDescription(new AS400UnsignedBin8(), "11 commit cycle identifier"),
                new BinaryFieldDescription(new AS400UnsignedBin4(), "12 pointer handle"),
                new BinaryFieldDescription(new AS400UnsignedBin2(), "13 remote port"),
                new BinaryFieldDescription(new AS400UnsignedBin2(), "14 arm number"),
                new BinaryFieldDescription(new AS400UnsignedBin2(), "15 program library ASP number"),
                new CharacterFieldDescription(new AS400Text(16), "16 remote access"),
                new CharacterFieldDescription(new AS400Text(1), "17 journal code"),
                new CharacterFieldDescription(new AS400Text(2), "18 entry type"),
                new CharacterFieldDescription(new AS400Text(10), "19 job name"),
                new CharacterFieldDescription(new AS400Text(10), "20 user name"),
                new CharacterFieldDescription(new AS400Text(6), "21 job number"),
                new CharacterFieldDescription(new AS400Text(10), "22 program name"),
                new CharacterFieldDescription(new AS400Text(10), "23 program library name"),
                new CharacterFieldDescription(new AS400Text(10), "24 program ASP device name"),
                new CharacterFieldDescription(new AS400Text(30), "25 object"),
                new CharacterFieldDescription(new AS400Text(10), "26 user profile"),
                new CharacterFieldDescription(new AS400Text(10), "27 Journal identifier"),
                new CharacterFieldDescription(new AS400Text(1), "28 address family"),
                new CharacterFieldDescription(new AS400Text(8), "29 System name"),
                new CharacterFieldDescription(new AS400Text(1), "30 Indicator flag"),
                new CharacterFieldDescription(new AS400Text(1), "31 Object name identifier"),
                new BinaryFieldDescription(new AS400Bin1(), "32 bit flags")
        };
        for (int i = 0; i < fds.length; i++) {
            dataTypes.add(fds[i].getDataType());
        }
        structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
    }

    public EntryHeader decode(byte[] data, int offset) {
        Object[] os = (Object[]) structure.toObject(data, offset);
        Long nextEntryOffset = (Long) os[0];
        Long nullEntryOffset = (Long) os[1];
        Long entrySpecificDataOffset = (Long) os[2];
        BigInteger sequenceNumber = (BigInteger) os[6];
        BigInteger systemSequenceNumber = ((BigInteger) os[9]);
        java.sql.Timestamp timestamp = ((java.sql.Timestamp) os[7]);
        char journalCode = ((String) os[17]).charAt(0);
        String entryType = (String) os[18];
        String objectName = (String) os[25];
        BigInteger commitCycle = (BigInteger) os[11];
        Long pointerHandle = (Long) os[12];
        int receiverOffset = ((Long) os[5]).intValue();
        String[] receiver = EMPTY;
        if (receiverOffset > 0) {
            receiver = nameDecoder.decode(data, offset + receiverOffset);
        }

        byte flags = (Byte) os[32];
        // MSB = 0 format for flag numbering
        // 218(0) DA(0) BIT(1) Referential constraint
        // 218(1) DA(1) BIT(1) Trigger
        // 218(2) DA(2) BIT(1) Incomplete data
        // 218(3) DA(3) BIT(1) Ignored during APYJRNCHG or RMVJRNCHG
        // 218(4) DA(4) BIT(1) Minimized entry specific data
        // 218(5) DA(5) BIT(1) File type indicator
        // 218(6) DA(6) BIT(1) Minimized on field boundaries
        // 218(7) DA(7) BIT(1) Reserved

        // log.debug("flags: {}", flags);
        // log.debug("Incomplete flags: {}", ((flags&32) != 0));
        // log.debug("Minimised flags: {}", ((flags&8) != 0));
        // log.debug("File type flags: {}", ((flags&4) != 0));

        int endOffset = (int) ((nextEntryOffset == 0) ? data.length - offset : nextEntryOffset);
        if (nextEntryOffset > Integer.MAX_VALUE || nullEntryOffset > Integer.MAX_VALUE) {
            throw new RuntimeException(
                    "Offsets too big for data, these are used as offsets into the buffer the data is in, they should never be this big nextEntryOffset " + nextEntryOffset
                            + ", nullEntryOffset " + nullEntryOffset);
        }
        Instant time = (timestamp == null) ? Instant.ofEpochSecond(0) : timestamp.toInstant();
        return new EntryHeader(nextEntryOffset.intValue(), nullEntryOffset.intValue(), entrySpecificDataOffset, sequenceNumber, systemSequenceNumber,
                time, journalCode, entryType, objectName, commitCycle, endOffset, pointerHandle, receiver[0], receiver[1]);

    }

}
