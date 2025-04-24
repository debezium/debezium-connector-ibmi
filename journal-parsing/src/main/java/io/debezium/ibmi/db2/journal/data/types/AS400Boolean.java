package io.debezium.ibmi.db2.journal.data.types;

import com.ibm.as400.access.AS400ZonedDecimal;
import com.ibm.as400.access.ExtendedIllegalArgumentException;

public class AS400Boolean extends AS400ZonedDecimal {
    private static final long serialVersionUID = 1L;

    private static final Boolean DEFAULT_VALUE = Boolean.FALSE;

    public AS400Boolean() {
        // One digit, no decimal positions
        super(1, 0);
    }

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
        // Call the super class method to convert the one byte array to a number
        final Number n = (Number) super.toObject(oneByte, 0);
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
}
