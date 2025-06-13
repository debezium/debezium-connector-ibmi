/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.data.types;

import java.util.Arrays;

import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.InternalErrorException;
import com.ibm.as400.access.Trace;

public class AS400VarBin implements AS400DataType {
    private final AS400Bin2 as400Bin2 = new AS400Bin2();
    private final int maxLenght;
    private final static byte[] defaultValue = new byte[0];
    private int actualLength;

    public AS400VarBin(int maxLenght) {
        this.maxLenght = maxLenght;
    }

    @Override
    public int getByteLength() {
        return maxLenght + 2;
    }

    @Override
    public Object getDefaultValue() {
        return defaultValue;
    }

    @Override
    public int getInstanceType() {
        return -1;
    }

    @Override
    public Class<?> getJavaType() {
        return byte[].class;
    }

    @Override
    public byte[] toBytes(Object javaValue) {
        return null;
    }

    @Override
    public int toBytes(Object javaValue, byte[] as400Value) {
        return 0;
    }

    @Override
    public int toBytes(Object javaValue, byte[] as400Value, int offset) {
        return 0;
    }

    @Override
    public Object toObject(byte[] data) {
        return toObject(data, 0);
    }

    @Override
    public Object toObject(byte[] data, int offset) {
        actualLength = (Short) as400Bin2.toObject(data, offset);
        // note the below insert will put data in as EBIDIC so test = a385a2a3 and not the ascii 74657374
        // INSERT INTO msbinary (id, mybin) VALUES (2, CAST('test' AS VARBINARY(10)))
        return Arrays.copyOfRange(data, offset + 2, offset + 2 + actualLength);
    }

    @Override
    public Object clone() {
        try {
            return super.clone(); // Object.clone does not throw exception.
        }
        catch (CloneNotSupportedException e) {
            Trace.log(Trace.ERROR, "Unexpected CloneNotSupportedException:", e);
            throw new InternalErrorException(InternalErrorException.UNEXPECTED_EXCEPTION);
        }
    }

}
