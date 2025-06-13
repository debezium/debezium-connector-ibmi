/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400Timestamp;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.StringHelpers;

// https://www.ibm.com/docs/en/i/7.2?topic=ssw_ibm_i_72/apis/QJORJRNI.htm "Key 1 output section"
public class ReceiverDecoder {
    private final AS400Structure structure;
    private static final Logger log = LoggerFactory.getLogger(ReceiverDecoder.class);

    public ReceiverDecoder() {
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
        catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchFieldException | SecurityException | NoSuchMethodException e) {
            throw new RuntimeException("Failed to setup ReceiverDecoder", e);
        }

        FieldDescription[] fds = new FieldDescription[]{
                new CharacterFieldDescription(new AS400Text(10), "0 receiver name"),
                new CharacterFieldDescription(new AS400Text(10), "1 library name"),
                new CharacterFieldDescription(new AS400Text(5), "2 receiver number"),
                new CharacterFieldDescription(new AS400Text(13), "3 attach date and time"),

                new CharacterFieldDescription(new AS400Text(1), "4 status"),
                new CharacterFieldDescription(new AS400Text(13), "5 save date and time"),
                new CharacterFieldDescription(new AS400Text(8), "6 local journal system"),
                new CharacterFieldDescription(new AS400Text(8), "7 source journal system"),
                new BinaryFieldDescription(new AS400Bin4(), "8 receiver size")
                // reserved char(56)
        };
        for (int i = 0; i < fds.length; i++) {
            dataTypes.add(fds[i].getDataType());
        }
        structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
    }

    public JournalReceiverInfo decode(byte[] data, int offset) {
        Object[] os = (Object[]) structure.toObject(data, offset);
        String itime = (String) os[3];
        String receiverNumber = (String) os[2];
        Optional<Integer> chain = Optional.empty();
        try {
            if (receiverNumber != null && receiverNumber.length() == 5) {
                String chainStr = receiverNumber.substring(0, 2);
                chain = Optional.of(Integer.valueOf(chainStr));
            }
        }
        catch (Exception e) {
        }
        Date attached = toDate(itime);
        JournalReceiverInfo jr = new JournalReceiverInfo(
                new JournalReceiver(
                        StringHelpers.safeTrim((String) os[0]),
                        StringHelpers.safeTrim((String) os[1])),
                attached,
                JournalStatus.valueOfString((String) os[4]),
                chain);
        return jr;
    }

    public static Date toDate(String itime) {
        try {
            int century = 19 + Integer.valueOf(itime.substring(0, 1));
            String stime = String.format("%d%s", century, itime.substring(1));
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            Date attached = sdf.parse(stime);
            return attached;
        }
        catch (ParseException e) {
            return null;
        }
    }

}
