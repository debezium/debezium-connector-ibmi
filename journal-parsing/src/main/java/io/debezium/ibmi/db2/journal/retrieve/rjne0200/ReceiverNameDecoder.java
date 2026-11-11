/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve.rjne0200;

import java.util.ArrayList;

import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.BinaryFieldDescription;
import com.ibm.as400.access.CharacterFieldDescription;
import com.ibm.as400.access.FieldDescription;

public class ReceiverNameDecoder {

    private final AS400Structure structure;

    public ReceiverNameDecoder() {
        this(65535);
    }

    public ReceiverNameDecoder(int ccsid) {
        ArrayList<AS400DataType> dataTypes = new ArrayList<AS400DataType>();
        FieldDescription[] fds = new FieldDescription[]{
                new CharacterFieldDescription(new AS400Text(10, ccsid), "0 receiver name"),
                new CharacterFieldDescription(new AS400Text(10, ccsid), "1 library name"),
                new CharacterFieldDescription(new AS400Text(10, ccsid), "2 ASP device name"),
                new BinaryFieldDescription(new AS400Bin2(), "3 ASP number") };
        for (int i = 0; i < fds.length; i++) {
            dataTypes.add(fds[i].getDataType());
        }
        structure = new AS400Structure(dataTypes.toArray(new AS400DataType[dataTypes.size()]));
    }

    public String[] decode(byte[] data, int offset) {
        Object[] os = (Object[]) structure.toObject(data, offset);
        String receiver = (String) os[0];
        String library = (String) os[1];
        return new String[]{ receiver, library };
    }

}
