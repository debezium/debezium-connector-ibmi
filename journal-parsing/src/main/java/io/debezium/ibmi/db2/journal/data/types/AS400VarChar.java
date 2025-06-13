/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.data.types;

import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.InternalErrorException;
import com.ibm.as400.access.Trace;

public class AS400VarChar implements AS400DataType {
    private final AS400Bin2 as400Bin2 = new AS400Bin2();
    private final int maxLenght;
    private final static String defaultValue = "";
    private int actualLength;
    private final int ccsid;
    /*
     * as400 correctly decodes the charset but relies on the length being the buffer
     * length not the number of characters.
     *
     * The length in the system catalogue is the number of characters.
     *
     * The character_octet_length seems to represent the buffer length.
     *
     * For a var char the length is encoded in the first two bytes of the buffer and
     * again this is the number of characters not the buffer length.
     *
     * So we need the bytesPerChar multiplier to calculate the buffer size or we'd
     * need to resolve the number of characters for the ccsid
     *
     * PRG reference
     * https://www.ibm.com/docs/en/i/7.5?topic=type-graphic-format#dgraph
     */
    private final int bytesPerChar;

    public AS400VarChar(int maxLenght, int bytesPerChar) {
        this.maxLenght = maxLenght * bytesPerChar;
        this.bytesPerChar = bytesPerChar;
        ccsid = -1;
    }

    public AS400VarChar(int maxLenght, int bytesPerChar, int ccsid) {
        this.ccsid = ccsid;
        this.bytesPerChar = bytesPerChar;
        this.maxLenght = maxLenght * bytesPerChar;
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
        return AS400DataType.TYPE_TEXT;
    }

    @Override
    public Class<?> getJavaType() {
        return String.class;
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
        final int lenOffset = 2;
        actualLength = (Short) as400Bin2.toObject(data, offset);
        actualLength *= bytesPerChar;
        final AS400Text txt = (ccsid > 0) ? new AS400Text(actualLength, ccsid) : new AS400Text(actualLength);
        final String text = (String) txt.toObject(data, offset + lenOffset);
        return text;
    }

    @Override
    public Object clone() {
        try {
            return super.clone(); // Object.clone does not throw exception.
        }
        catch (final CloneNotSupportedException e) {
            Trace.log(Trace.ERROR, "Unexpected CloneNotSupportedException:", e);
            throw new InternalErrorException(InternalErrorException.UNEXPECTED_EXCEPTION);
        }
    }

}
