/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.util.ArrayList;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;

public class XaTransactionDecoder {
    final AS400Structure structure;

    /*
     * long formatID; Format id
     * Value of -1 (constant NULLXID) means that XID is null.
     * long gtrid_length; Length of local transaction id
     * long bqual_length; Length of branch qualifier
     * char data[XIDDATASIZE]; Transaction branch id
     */
    public XaTransactionDecoder() {
        FieldDescription[] fds = new FieldDescription[]{
                new BinaryFieldDescription(new AS400Bin4(), "SRCDAT"),
                new BinaryFieldDescription(new AS400Bin4(), "SRCSEQ"),
                new CharacterFieldDescription(new AS400Text(80), "SRCDTA"),
        };
        ArrayList<AS400DataType> dataTypes = new ArrayList<AS400DataType>();
        for (int i = 0; i < fds.length; i++) {
            dataTypes.add(fds[i].getDataType());
        }
        structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
    }

    public XaTransaction decode(byte[] data, int offset) {
        Object[] os = (Object[]) structure.toObject(data, offset);
        XaTransaction tx = new XaTransaction((Long) os[0], (Long) os[1], (String) os[3]);
        return tx;
    }

}
