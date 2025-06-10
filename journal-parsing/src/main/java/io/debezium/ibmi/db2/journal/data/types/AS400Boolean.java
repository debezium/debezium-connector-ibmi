/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.data.types;

import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400ZonedDecimal;
import com.ibm.as400.access.ExtendedIllegalArgumentException;
import com.ibm.as400.access.InternalErrorException;
import com.ibm.as400.access.Trace;

public class AS400Boolean implements AS400DataType {
    private static final long serialVersionUID = 1L;

    private static final Boolean DEFAULT_VALUE = Boolean.FALSE;

    // Boolean values are journaled as a zoned decimal; one digit, no decimal positions
    private final AS400ZonedDecimal ZD = new AS400ZonedDecimal(1, 0);

    @Override
    public Object getDefaultValue() {
        return DEFAULT_VALUE;
    }

    @Override
    public int getInstanceType() {
        // There is no appropriate instance type value defined for BOOLEAN in AS400DataType
        return -1;
    }

    @Override
    public Class<Boolean> getJavaType() {
        return Boolean.class;
    }

    @Override
    public Boolean toObject(final byte[] as400Value) {
        return toBoolean(as400Value, 0);
    }

    @Override
    public Boolean toObject(final byte[] as400Value, final int offset) {
        return toBoolean(as400Value, offset);
    }

    private Boolean toBoolean(final byte[] as400Value, final int offset) {
        final byte[] oneByte = new byte[]{ as400Value[offset] };
        // Convert the one byte array to a number
        final Number n = (Number) ZD.toObject(oneByte, 0);
        // Expecting a 1 or 0 from the super class method
        switch (null != n ? n.intValue() : -1) {
            case 0:
                return Boolean.FALSE;
            case 1:
                return Boolean.TRUE;
            default:
                throw new ExtendedIllegalArgumentException(String.valueOf(n), ExtendedIllegalArgumentException.PARAMETER_VALUE_NOT_VALID);
        }
    }

    @Override
    public int getByteLength() {
        return 1;
    }

    @Override
    public byte[] toBytes(final Object javaValue) {
        return new byte[0];
    }

    @Override
    public int toBytes(final Object javaValue, final byte[] as400Value) {
        return 0;
    }

    @Override
    public int toBytes(final Object javaValue, final byte[] as400Value, final int offset) {
        return 0;
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
