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
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400Date;
import com.ibm.as400.access.AS400Time;

/**
 * Caches the IBM i date/time format and separator for DATE and TIME columns.
 * <p>
 * {@link java.sql.DatabaseMetaData#getColumns} exposes the SQL type ({@code DATE}/{@code TIME}) but not the IBM i
 * {@code DATFMT}/{@code TIMFMT} nor its separator, so the journal decoder cannot tell a {@code *ISO} column from a
 * {@code *EUR} one. Because the decoder parses raw journal bytes via {@code AS400Structure.toObject}, the
 * {@link AS400Date}/{@link AS400Time} format must match the file's DDS format both to parse the value <em>and</em> to
 * size the column correctly within the record structure (e.g. {@code *MDY} is 8 bytes vs {@code *ISO} 10 bytes).
 * <p>
 * The format/separator are read from {@code QSYS2.SYSCOLUMNS} and cached per {@code schema.table.column}, mirroring
 * {@link CcsidCache} and {@link BytesPerChar}.
 */
public class DateTimeFormatCache {
    // SYSCOLUMNS2 (not SYSCOLUMNS) exposes the DDS-derived date/time format and separator columns; they were added in
    // IBM i 7.4 TR5 / 7.3 TR11. On older releases the prepare fails, lookup() swallows it and the decoder falls back to
    // the default *ISO behaviour. SYSCOLUMNS2 is a superset of SYSCOLUMNS and IBM recommends it for performance.
    private static final String GET_FORMATS = "select table_name, system_table_name, column_name, system_column_name, "
            + "date_format, date_separator, time_format, time_separator "
            + "FROM qsys2.SYSCOLUMNS2 where table_schema=? and (system_table_name = ? or table_name = ?)";

    static final Logger log = LoggerFactory.getLogger(DateTimeFormatCache.class);

    private final Map<String, Optional<FormatAndSeparator>> dateMap = new HashMap<>();
    private final Map<String, Optional<FormatAndSeparator>> timeMap = new HashMap<>();
    private final Connect<Connection, SQLException> jdbcConnect;

    public DateTimeFormatCache(final Connect<Connection, SQLException> jdbcConnect) {
        this.jdbcConnect = jdbcConnect;
    }

    record FormatAndSeparator(int format, Character separator) {
    }

    /**
     * @return the per-column {@link AS400Date}, or empty when the format is unknown (no row, null connection or a
     *         lookup failure) so the caller can fall back to the default {@code *ISO} behaviour.
     */
    public Optional<AS400Date> getDate(String schema, String table, String columnName) {
        return lookup(dateMap, schema, table, columnName)
                .map(fs -> fs.separator() == null
                        ? new AS400Date(fs.format())
                        : new AS400Date(fs.format(), fs.separator()));
    }

    /**
     * @return the per-column {@link AS400Time}, or empty when the format is unknown (see {@link #getDate}).
     */
    public Optional<AS400Time> getTime(String schema, String table, String columnName) {
        return lookup(timeMap, schema, table, columnName)
                .map(fs -> fs.separator() == null
                        ? new AS400Time(fs.format())
                        : new AS400Time(fs.format(), fs.separator()));
    }

    private Optional<FormatAndSeparator> lookup(Map<String, Optional<FormatAndSeparator>> map, String schema,
                                                String table, String columnName) {
        final String canonicalName = String.format("%s.%s.%s", schema, table, columnName);
        if (map.containsKey(canonicalName)) {
            return map.get(canonicalName);
        }

        try {
            fetchAllFormatsForTable(schema, table);
        }
        catch (final Exception e) {
            log.error("failed to fetch date/time formats for {}.{}", schema, table, e);
        }
        // cache the negative result so an unknown column doesn't re-query on every record
        return map.computeIfAbsent(canonicalName, k -> Optional.empty());
    }

    private void fetchAllFormatsForTable(String schema, String table) throws SQLException {
        final Connection con = jdbcConnect.connection();
        try (PreparedStatement ps = con.prepareStatement(GET_FORMATS)) {
            ps.setString(1, schema.toUpperCase());
            ps.setString(2, table.toUpperCase());
            ps.setString(3, table.toUpperCase());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    final String longTableName = StringHelpers.safeTrim(rs.getString(1));
                    final String shortTableName = StringHelpers.safeTrim(rs.getString(2));
                    final String longColumn = StringHelpers.safeTrim(rs.getString(3));
                    final String shortColumn = StringHelpers.safeTrim(rs.getString(4));
                    final String dateFormat = StringHelpers.safeTrim(rs.getString(5));
                    final String dateSeparator = rs.getString(6);
                    final String timeFormat = StringHelpers.safeTrim(rs.getString(7));
                    final String timeSeparator = rs.getString(8);

                    final Optional<FormatAndSeparator> date = mapDateFormat(dateFormat, dateSeparator);
                    final Optional<FormatAndSeparator> time = mapTimeFormat(timeFormat, timeSeparator);

                    put(dateMap, schema, longTableName, longColumn, date);
                    put(dateMap, schema, shortTableName, shortColumn, date);
                    put(timeMap, schema, longTableName, longColumn, time);
                    put(timeMap, schema, shortTableName, shortColumn, time);
                }
            }
        }
    }

    private static void put(Map<String, Optional<FormatAndSeparator>> map, String schema, String table, String column,
                            Optional<FormatAndSeparator> value) {
        map.put(String.format("%s.%s.%s", schema, table, column), value);
    }

    /**
     * Maps a {@code QSYS2.SYSCOLUMNS.DATE_FORMAT} value (e.g. {@code ISO}, {@code EUR}, optionally {@code *}-prefixed)
     * to its jt400 {@link AS400Date} {@code FORMAT_*} constant. Pure (no connection) so it is unit-testable.
     */
    static Optional<FormatAndSeparator> mapDateFormat(String datfmt, String separator) {
        final Integer format = switch (normalise(datfmt)) {
            case "ISO" -> AS400Date.FORMAT_ISO;
            case "USA" -> AS400Date.FORMAT_USA;
            case "EUR" -> AS400Date.FORMAT_EUR;
            case "JIS" -> AS400Date.FORMAT_JIS;
            case "MDY" -> AS400Date.FORMAT_MDY;
            case "DMY" -> AS400Date.FORMAT_DMY;
            case "YMD" -> AS400Date.FORMAT_YMD;
            case "JUL" -> AS400Date.FORMAT_JUL;
            default -> null;
        };
        if (format == null) {
            return Optional.empty();
        }
        return Optional.of(new FormatAndSeparator(format, separator(separator)));
    }

    /**
     * Maps a {@code QSYS2.SYSCOLUMNS.TIME_FORMAT} value to its jt400 {@link AS400Time} {@code FORMAT_*} constant.
     */
    static Optional<FormatAndSeparator> mapTimeFormat(String timfmt, String separator) {
        final Integer format = switch (normalise(timfmt)) {
            case "ISO" -> AS400Time.FORMAT_ISO;
            case "USA" -> AS400Time.FORMAT_USA;
            case "EUR" -> AS400Time.FORMAT_EUR;
            case "JIS" -> AS400Time.FORMAT_JIS;
            case "HMS" -> AS400Time.FORMAT_HMS;
            default -> null;
        };
        if (format == null) {
            return Optional.empty();
        }
        return Optional.of(new FormatAndSeparator(format, separator(separator)));
    }

    private static String normalise(String format) {
        if (format == null) {
            return "";
        }
        final String trimmed = format.trim().toUpperCase();
        return trimmed.startsWith("*") ? trimmed.substring(1) : trimmed;
    }

    /**
     * The catalog stores the separator as a single character. A null/blank/empty value means the DDS did not specify a
     * {@code DATSEP}/{@code TIMSEP}, i.e. "use the format's default separator" (e.g. {@code .} for {@code *EUR}); this
     * is returned as {@code null} so {@link #getDate}/{@link #getTime} construct with the single-argument
     * {@code AS400Date}/{@code AS400Time} constructor, which applies that default. It must NOT be treated as
     * "no separator" — that would size the column 2 bytes short (e.g. {@code *EUR} 8 vs 10) and corrupt the record.
     */
    private static Character separator(String separator) {
        if (separator == null || separator.isEmpty() || separator.isBlank()) {
            return null;
        }
        return separator.charAt(0);
    }
}
