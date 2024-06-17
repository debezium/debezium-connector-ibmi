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

public class CcsidCache {
    private static final String GET_CCSID = "select table_name, system_table_name, column_name, system_column_name, ccsid FROM qsys2.SYSCOLUMNS where table_schema=? and (system_table_name = ? or table_name = ?)";
    private final Map<String, Integer> ccsidMap = new HashMap<>();
    private final Connect<Connection, SQLException> jdbcConnect;
    static final Logger log = LoggerFactory.getLogger(CcsidCache.class);
    private final int fromCcsid;
    private final int toCcsid;

    public CcsidCache(final Connect<Connection, SQLException> jdbcConnect, int fromCcsid, int toCcsid) {
        this.jdbcConnect = jdbcConnect;
        this.fromCcsid = fromCcsid;
        this.toCcsid = toCcsid;

    }

    public Integer getCcsid(String schema, String table, String columnName) {
        final String canonicalName = String.format("%s.%s.%s", schema, table, columnName);
        if (ccsidMap.containsKey(canonicalName)) {
            return ccsidMap.get(canonicalName);
        }

        try {
            fetchAllCcsidForTable(schema, table);

            return ccsidMap.get(canonicalName);
        }
        catch (final SQLException e) {
            log.error("failed to fetch ccsid", e);
            return -1;
        }
    }

    private void fetchAllCcsidForTable(String schema, String table) throws SQLException {
        final Connection con = jdbcConnect.connection();
        try (PreparedStatement ps = con.prepareStatement(GET_CCSID)) {
            ps.setString(1, schema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            ps.setString(3, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final String longTableName = StringHelpers.safeTrim(rs.getString(1));
                    final String shortTableName = StringHelpers.safeTrim(rs.getString(2));
                    final String longcolumn = StringHelpers.safeTrim(rs.getString(3));
                    final String shortcolumn = StringHelpers.safeTrim(rs.getString(4));
                    final Object ccsidObj = rs.getObject(5);
                    final String canonicalLongName = String.format("%s.%s.%s", schema, longTableName, longcolumn);
                    final String canonicalShortName = String.format("%s.%s.%s", schema, shortTableName, shortcolumn);
                    int ccsid = (ccsidObj == null) ? -1 : (Integer) ccsidObj;

                    if (fromCcsid == ccsid && toCcsid != -1) {
                        log.debug("overriding ccsid for {} was {} now {}", longTableName, fromCcsid, toCcsid);
                        ccsid = toCcsid;
                    }
                    ccsidMap.put(canonicalLongName, ccsid);
                    ccsidMap.put(canonicalShortName, ccsid);
                }
            }
        }
    }

}
