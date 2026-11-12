/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BytesPerChar {
    /* @See com.fnz.db2.journal.data.types.AS400VarChar */
    private static final String GET_OCTET_LENGTH = "select table_name, system_table_name, column_name, system_column_name, length, character_octet_length FROM qsys2.SYSCOLUMNS where table_schema=? and (system_table_name = ? or table_name = ?)";

    private record ColumnName(String schema, String table, String column) {
    }

    private final Map<ColumnName, Integer> octetLenghtMap = new HashMap<>();
    private final Connect<Connection, SQLException> jdbcConnect;
    static final Logger log = LoggerFactory.getLogger(BytesPerChar.class);

    public BytesPerChar(final Connect<Connection, SQLException> jdbcConnect) {
        this.jdbcConnect = jdbcConnect;
    }

    public Integer getBytesPerChar(String schema, String table, String columnName) {
        final ColumnName canonicalName = new ColumnName(schema, table, columnName);
        if (octetLenghtMap.containsKey(canonicalName)) {
            return octetLenghtMap.get(canonicalName);
        }

        try {
            fetchAllOctetLengthForTable(schema, table);

            return octetLenghtMap.get(canonicalName);
        }
        catch (final SQLException e) {
            log.error("failed to fetch octet length", e);
            return -1;
        }
    }

    void clearTable(String schema, String table) {
        octetLenghtMap.keySet().removeIf(name -> name.schema().equals(schema) && name.table().equals(table));
    }

    private void fetchAllOctetLengthForTable(String schema, String table) throws SQLException {
        final Connection con = jdbcConnect.connection();
        try (PreparedStatement ps = con.prepareStatement(GET_OCTET_LENGTH)) {
            ps.setString(1, schema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            ps.setString(3, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final String longTableName = StringHelpers.safeTrim(rs.getString(1));
                    final String shortTableName = StringHelpers.safeTrim(rs.getString(2));
                    final String longcolumn = StringHelpers.safeTrim(rs.getString(3));
                    final String shortcolumn = StringHelpers.safeTrim(rs.getString(4));
                    final int length = rs.getInt(5);
                    final int octetLength = rs.getInt(6);
                    add(schema, longTableName, longcolumn, length, octetLength);
                    add(schema, shortTableName, shortcolumn, length, octetLength);
                }
            }
        }
    }

    public void add(String schema, String table, String column, int length, int octetLength) {
        final ColumnName canonicalLongName = new ColumnName(schema, table, column);
        int bytesPerChar = octetLength / length;
        bytesPerChar = (bytesPerChar < 1) ? 1 : bytesPerChar;
        log.debug("bytes per char {} {} {} {}", schema, table, column, bytesPerChar);
        octetLenghtMap.put(canonicalLongName, bytesPerChar);
    }

}
