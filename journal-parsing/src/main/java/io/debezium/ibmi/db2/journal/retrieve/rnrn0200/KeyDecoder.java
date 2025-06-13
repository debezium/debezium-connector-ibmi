/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rnrn0200;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Timestamp;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.FieldDescription;

// https://www.ibm.com/docs/en/i/7.2?topic=ssw_ibm_i_72/apis/QJORJRNI.htm "Key section"
public class KeyDecoder {
    private static final Logger log = LoggerFactory.getLogger(KeyDecoder.class);

    private final AS400Structure structure;

    public KeyDecoder() {
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
            log.error("Failed to decode receiver data", e);
        }

        FieldDescription[] fds = new FieldDescription[]{
                new BinaryFieldDescription(new AS400Bin4(), "0 key"),
                new BinaryFieldDescription(new AS400Bin4(), "1 offset to key info"),
                new BinaryFieldDescription(new AS400Bin4(), "2 length of key info header"),
                new BinaryFieldDescription(new AS400Bin4(), "3 numer of entries"),
                new BinaryFieldDescription(new AS400Bin4(), "4 length of each entry")
                // reserved char(56)
        };
        for (int i = 0; i < fds.length; i++) {
            dataTypes.add(fds[i].getDataType());
        }
        structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
    }

    public KeyHeader decode(byte[] data, int offset) {
        Object[] os = (Object[]) structure.toObject(data, offset);
        return new KeyHeader((Integer) os[0], (Integer) os[1], (Integer) os[2], (Integer) os[3], (Integer) os[4]);
    }

}
