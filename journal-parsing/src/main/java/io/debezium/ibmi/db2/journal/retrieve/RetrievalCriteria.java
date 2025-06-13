/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Structure;
import com.ibm.as400.access.AS400Text;

/**
 * Inner class representing Journal Entry to Retrieve section. Mainly used to
 * encode the following: a. Binary(4) - Number of Variable Length Records b1.
 * Binary(4) - Length of Variable Length Record - length(b1+b2+b3+b4) b2.
 * Binary(4) - Key b3. Binary(4) - Length of Data - length(b4) b4. Char(*) -
 * Data
 *
 * To see an example using AS400Structure for a composite type of data types:
 * http://publib.boulder.ibm.com/html/as400/java/rzahh115.htm#HDRRZAHH-COMEX
 *
 * TODO optimise so we don't have to rebuild entire thing for every offset
 *
 * @author loosely based off work by Stanley
 *
 */
public class RetrievalCriteria {
    private static final int AS400_TEXT_20_LENGTTH = (new AS400Text(20)).getByteLength();
    private static final Logger log = LoggerFactory.getLogger(RetrievalCriteria.class);
    private ArrayList<AS400DataType> structure = new ArrayList<AS400DataType>();
    private ArrayList<Object> data = new ArrayList<Object>();

    public AS400DataType[] getStructure() {
        return structure.toArray(new AS400DataType[structure.size()]);
    }

    public Object[] getObject() {
        return data.toArray(new Object[0]);
    }

    /**
     * Add retrieval criteria 01: range of journal receivers. This can be used to
     * indicate where to start when previous returned continuation handle='1'.
     *
     * @param value
     */
    public void withReceiverRange(String value) {
        String temp = ((String) value).trim();
        if (temp.equals("*CURAVLCHN") || temp.equals("*CURCHAIN") || temp.equals("*CURRENT")) {
            temp = StringHelpers.padRight(temp, 40);
        }
        else {
            throw new IllegalArgumentException(
                    String.format("value '%s' for 'Range of journal receivers' must be either '*CURAVLCHN' or '*CURCHAIN' or '*CURRENT'", value));
        }
        addStructureData(RetrieveKey.RCVRNG, new AS400Text(40), temp);
    }

    public void withReceiverRange(String startReceiver, String startLibrary, String endReceiver, String endLibrary) {
        String padded = String.format("%-10s%-10s%-10s%-10s", startReceiver, startLibrary, endReceiver, endLibrary);
        addStructureData(RetrieveKey.RCVRNG, new AS400Text(40), padded);
    }

    public void withLenNullPointerIndicatorVarLength() {
        addStructureData(RetrieveKey.NULLINDLEN, new AS400Text(10), StringHelpers.padRight("*VARLEN", 10));
    }

    public void withNullPointerIndicatorLength(int value) {
        String padded = null;
        if (value % 16 != 0) {
            throw new IllegalArgumentException(String.format(
                    "Value %d for 'Null value indicators length' should be divisible by 16", value));
        }
        padded = StringHelpers.padLeft(Integer.toString(value), 10);
        addStructureData(RetrieveKey.NULLINDLEN, new AS400Text(10), padded);
    }

    /**
     * Add retrieval criteria 02: starting sequence number. This can be used to
     * indicate where to start when previous returned continuation handle='1'.
     *
     * @param value
     */
    public void withFromEnt(FromEnt value) {
        String temp = StringHelpers.padRight(value.getValue(), AS400_TEXT_20_LENGTTH);
        addStructureData(RetrieveKey.FROMENT, new AS400Text(20), temp);
    }

    /**
     * Add retrieval criteria 02: starting sequence number. This can be used to
     * indicate where to start when previous returned continuation handle='1'.
     *
     * @param value
     */
    public void withFromEnt(BigInteger value) {
        String temp = String.format("%20d", value);
        addStructureData(RetrieveKey.FROMENT, new AS400Text(20), temp);
    }

    public void withStart() {
        addStructureData(RetrieveKey.FROMENT, new AS400Text(20), "*FIRST");
    }

    public void withEnd() {
        addStructureData(RetrieveKey.TOENT, new AS400Text(20), "*LAST");
    }

    public void withEnd(BigInteger value) {
        String temp = String.format("%20d", value);
        addStructureData(RetrieveKey.TOENT, new AS400Text(20), temp);
    }

    /**
     * Add retrieval criteria 06: max number of entries to retrieve. This indicates
     * the 'max' number of entries to retrieve, not number of entries retrieved in
     * this call.
     *
     * @param eKey
     * @param value
     */
    private void withMaxEntriesToReturn(Integer value) {
        addStructureData(RetrieveKey.NBRENT, new AS400Bin4(), value);
    }

    /**
     * Add retrieval criteria 07: journal codes. Input parameter must be one of the
     * below: - *ALL (String). - *CTL (String). - JournalCode[] to hold all the
     * desired journal codes to retrieve. Currently only '*ALLSLT' is implemented if
     * JournalCode[] is passed in.
     *
     * @param value
     */
    public void withJrnCde(JournalCode[] value) {
        JournalCode[] range = (JournalCode[]) value;
        StringBuilder code = new StringBuilder();
        for (int i = 0; i < range.length; i++) {
            code.append(StringHelpers.padRight(range[i].getKey(), 10));
            code.append(StringHelpers.padRight("*ALLSLT", 10));
        }
        _withJrnCde(range.length, code.toString());
    }

    private void _withJrnCde(int count, String codes) {
        Object[] temp2 = new Object[2];
        temp2[0] = Integer.valueOf(count);
        temp2[1] = codes;

        AS400DataType type[] = new AS400DataType[2];
        type[0] = new AS400Bin4();
        type[1] = new AS400Text(codes.length());
        AS400Structure temp2Structure = new AS400Structure(type);

        addStructureData(RetrieveKey.JRNCDE, temp2Structure, temp2);
    }

    /**
     * Add retrieval criteria 08: journal entry types.
     *
     * @param value
     */
    public void withEntTyp(JournalEntryType[] range) {
        String temp = null;
        int count = 0;

        StringBuilder entry = new StringBuilder();
        for (int i = 0; i < range.length; i++) {
            entry.append(StringHelpers.padRight(range[i].getKey(), 10));
        }
        temp = entry.toString();
        count = range.length;

        Object[] temp2 = new Object[2];
        temp2[0] = Integer.valueOf(count);
        temp2[1] = temp;

        AS400DataType[] type = new AS400DataType[2];
        type[0] = new AS400Bin4();
        type[1] = new AS400Text(temp.length());
        AS400Structure temp2Structure = new AS400Structure(type);

        addStructureData(RetrieveKey.ENTTYP, temp2Structure, temp2);
    }

    /**
     * Add retrieval criteria 16: FILE. Input parameter must be one of the
     *          BINARY(4)   Number in array
     *  Note: These fields repeat for each file member.
     *           CHAR(10)    File name
     *           CHAR(10)    Library name
     *           CHAR(10)    Member name
     *
     * @param fileFilters
     */
    public void withFILE(List<FileFilter> fileFilters) {
        if (fileFilters == null) {
            return;
        }
        int length = fileFilters.size();
        if (length > 300) {
            log.error("unable to filter for more than 300 files requested length was {}", length);
            return;
        }

        Object[] fdata = new Object[length * 3 + 1];
        AS400DataType types[] = new AS400DataType[length * 3 + 1];
        fdata[0] = Integer.valueOf(length);
        types[0] = new AS400Bin4();

        int i = 1;
        for (FileFilter f : fileFilters) {
            types[i] = new AS400Text(10);
            fdata[i++] = StringHelpers.padRight(f.table().toUpperCase(), 10);
            types[i] = new AS400Text(10);
            fdata[i++] = StringHelpers.padRight(f.schema().toUpperCase(), 10);
            types[i] = new AS400Text(10);
            fdata[i++] = "*ALL      ";
        }

        AS400Structure asStructure = new AS400Structure(types);
        addStructureData(RetrieveKey.FILE, asStructure, fdata);
    }

    public void reset() {
        structure.clear();
        data.clear();
        structure.add(new AS400Bin4());
        data.add(Integer.valueOf(0));
    }

    /**
     * Add additional selection entry to two ArrayList: structure and data.
     *
     * @param rKey
     * @param dataType
     * @param value
     */
    private void addStructureData(RetrieveKey rKey, AS400DataType valueType, Object value) {
        AS400Bin4 totalLengthType = new AS400Bin4();
        AS400Bin4 keyType = new AS400Bin4();
        AS400Bin4 valueLenghtType = new AS400Bin4();

        int totalLengthValue = totalLengthType.getByteLength() + keyType.getByteLength()
                + valueLenghtType.getByteLength() + valueType.getByteLength();
        Integer key = Integer.valueOf(rKey.getKey());
        Integer valueLength = Integer.valueOf(valueType.getByteLength());

        structure.add(totalLengthType);
        structure.add(keyType);
        structure.add(valueLenghtType);
        structure.add(valueType);

        data.add(Integer.valueOf(totalLengthValue));
        data.add(key);
        data.add(valueLength);
        data.add(value);

        // pump up "Number of Variable Length Records" by 1
        data.set(0, (Integer) data.get(0) + 1);
    }

    public enum RetrieveKey {
        RCVRNG(1, "Range of journal receivers"),
        FROMENT(2, "Starting sequence number"),
        FROMTIME(3, "Starting time stamp"),
        TOENT(4, "Ending sequence number"),
        TOTIME(5, "Ending time stamp"),
        NBRENT(6, "Number of entries"),
        JRNCDE(7, "Journal codes"),
        ENTTYP(8, "Journal entry types"),
        JOB(9, "Job"),
        PGM(10, "Program"),
        USRPRF(11, "User profile"),
        CMTCYCID(12, "Commit cycle identifier"),
        DEPENT(13, "Dependent entries"),
        INCENT(14, "Include entries"),
        NULLINDLEN(15, "Null value indicators length"),
        FILE(16, "File"),
        OBJ(17, "Object"),
        OBJPATH(18, "Object Path"),
        OBJFID(19, "Object file identifier"),
        SUBTREE(20, "Directory substree"),
        PATTERN(21, "Name pattern"),
        FMTMINDTA(22, "Format Minimized Data");

        private int key;
        private String description;

        RetrieveKey(int key, String description) {
            this.key = key;
            this.description = description;
        }

        public int getKey() {
            return this.key;
        }

        public String getDescription() {
            return this.description;
        }

        @Override
        public String toString() {
            return String.format("%s, (%d)", getDescription(), getKey());
        }
    }

    public enum JournalCode {
        A("A", "System accounting entry"),
        B("B", "Integrated file system operation"),
        C("C", "Commitment control operation"),
        D("D", "Database file operation"),
        E("E", "Data area operation"),
        F("F", "Database file member operation"),
        I("I", "Internal operation"),
        J("J", "Journal or journal receiver operation"),
        L("L", "License management"),
        M("M", "Network management data"),
        P("P", "Performance tuning entry"),
        Q("Q", "Data queue operation"),
        R("R", "Record level operation"),
        S("S", "Distributed mail service for SNA distribution services (SNADS), network alerts, or mail server framework"),
        T("T", "Audit trail entry"),
        U("U", "User generated");

        private String key;
        private String description;

        JournalCode(String key, String description) {
            this.key = key;
            this.description = description;
        }

        public String getKey() {
            return this.key;
        }

        public String getDescription() {
            return this.description;
        }

        @Override
        public String toString() {
            return String.format("%s, (%s)", getDescription(), getKey());
        }
    }

    public enum JournalEntryType {
        BR("BR", "Before-image of record updated for rollback"),
        DL("DL", "Record deleted from physical file member"),
        DR("DR", "Record deleted for rollback"),
        IL("IL", "Increment record limit"),
        PT("PT", "Record added to physical file member"),
        PX("PX", "Record added directly to physical file member"),
        UB("UB", "Before-image of record updated in physical file member"),
        UP("UP", "After-image of record updated in physical file member"),
        UR("UR", "After-image of record updated for rollback"),
        ALL("*ALL", "All entry types"),

        // D journal types file operations
        CT("CT", "file created"),
        CG("CG", "file changed"),

        // C journal type commit control
        SC("SC", "start commit"),
        CM("CM", "end commit");

        private String key;
        private String description;

        JournalEntryType(String key, String description) {
            this.key = key;
            this.description = description;
        }

        public String getKey() {
            return this.key;
        }

        public String getDescription() {
            return this.description;
        }

        @Override
        public String toString() {
            return String.format("%s, (%s)", getDescription(), getKey());
        }
    }

    public enum FromEnt {
        FIRST("*FIRST"),
        LAST("*LAST");

        private final String value;

        FromEnt(String s) {
            this.value = s;
        }

        public String getValue() {
            return value;
        }
    }
}
