/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.SQLXML;
import java.time.ZoneOffset;

import org.apache.kafka.connect.data.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;

/**
 * Conversion of DB2 specific datatypes.
 *
 */
public class As400ValueConverters extends JdbcValueConverters {
    private static final Logger log = LoggerFactory.getLogger(As400ValueConverters.class);

    public As400ValueConverters() {
    }

    public As400ValueConverters(DecimalMode decimalMode) {
        super(decimalMode, TemporalPrecisionMode.ADAPTIVE, ZoneOffset.UTC, null, null, null);
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data == null) {
            return super.convertString(column, fieldDefn, data);
        }
        if (!(data instanceof SQLXML)) {
            String str = data.toString();
            Pair fixed = removeBadCharacters(str);
            if (fixed.modified) {
                String cname = (column == null) ? "" : String.format(" column name %s", column.name());
                String fname = (fieldDefn == null) ? "" : String.format(" fieldDefn name %s", fieldDefn.name());
                log.warn("removed binary data from{}{}", cname, fname);
            }
            return super.convertString(column, fieldDefn, fixed.value.trim());
        }
        return super.convertString(column, fieldDefn, data);
    }

    public static Pair removeBadCharacters(String rawString) {
        if (rawString == null) {
            return new Pair(false, rawString);
        }
        StringBuilder newString = new StringBuilder(rawString.length());
        boolean modified = false;
        for (int offset = 0; offset < rawString.length();) {
            int codePoint = rawString.codePointAt(offset);
            offset += Character.charCount(codePoint);

            // Replace invisible control characters and unused code points
            switch (Character.getType(codePoint)) {
                case Character.CONTROL: // \p{Cc}
                case Character.FORMAT: // \p{Cf}
                case Character.PRIVATE_USE: // \p{Co}
                case Character.SURROGATE: // \p{Cs}
                case Character.UNASSIGNED: // \p{Cn}
                    newString.append('?');
                    modified = true;
                    break;
                default:
                    newString.append(Character.toChars(codePoint));
                    break;
            }
        }
        return new Pair(modified, newString.toString());
    }

    public static class Pair {
        boolean modified;
        String value;

        public Pair(boolean modified, String value) {
            super();
            this.modified = modified;
            this.value = value;
        }

    }
}
