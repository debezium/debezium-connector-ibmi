/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.jupiter.api.Test;

import com.ibm.as400.access.AS400Date;
import com.ibm.as400.access.AS400Time;

import io.debezium.ibmi.db2.journal.retrieve.DateTimeFormatCache.FormatAndSeparator;

public class DateTimeFormatCacheTest {

    @Test
    public void mapsAllDateFormats() {
        assertEquals(AS400Date.FORMAT_ISO, DateTimeFormatCache.mapDateFormat("ISO", "-").orElseThrow().format());
        assertEquals(AS400Date.FORMAT_USA, DateTimeFormatCache.mapDateFormat("USA", "/").orElseThrow().format());
        assertEquals(AS400Date.FORMAT_EUR, DateTimeFormatCache.mapDateFormat("EUR", ".").orElseThrow().format());
        assertEquals(AS400Date.FORMAT_JIS, DateTimeFormatCache.mapDateFormat("JIS", "-").orElseThrow().format());
        assertEquals(AS400Date.FORMAT_MDY, DateTimeFormatCache.mapDateFormat("MDY", "/").orElseThrow().format());
        assertEquals(AS400Date.FORMAT_DMY, DateTimeFormatCache.mapDateFormat("DMY", "/").orElseThrow().format());
        assertEquals(AS400Date.FORMAT_YMD, DateTimeFormatCache.mapDateFormat("YMD", "/").orElseThrow().format());
        assertEquals(AS400Date.FORMAT_JUL, DateTimeFormatCache.mapDateFormat("JUL", "/").orElseThrow().format());
    }

    @Test
    public void mapsAllTimeFormats() {
        assertEquals(AS400Time.FORMAT_ISO, DateTimeFormatCache.mapTimeFormat("ISO", ".").orElseThrow().format());
        assertEquals(AS400Time.FORMAT_USA, DateTimeFormatCache.mapTimeFormat("USA", ":").orElseThrow().format());
        assertEquals(AS400Time.FORMAT_EUR, DateTimeFormatCache.mapTimeFormat("EUR", ".").orElseThrow().format());
        assertEquals(AS400Time.FORMAT_JIS, DateTimeFormatCache.mapTimeFormat("JIS", ":").orElseThrow().format());
        assertEquals(AS400Time.FORMAT_HMS, DateTimeFormatCache.mapTimeFormat("HMS", ":").orElseThrow().format());
    }

    @Test
    public void acceptsStarPrefixAndIsCaseInsensitive() {
        assertEquals(AS400Date.FORMAT_EUR, DateTimeFormatCache.mapDateFormat("*EUR", ".").orElseThrow().format());
        assertEquals(AS400Date.FORMAT_EUR, DateTimeFormatCache.mapDateFormat("eur", ".").orElseThrow().format());
    }

    @Test
    public void unknownOrNullFormatIsEmpty() {
        assertTrue(DateTimeFormatCache.mapDateFormat(null, "-").isEmpty());
        assertTrue(DateTimeFormatCache.mapDateFormat("", "-").isEmpty());
        assertTrue(DateTimeFormatCache.mapDateFormat("BOGUS", "-").isEmpty());
        assertTrue(DateTimeFormatCache.mapTimeFormat(null, ":").isEmpty());
    }

    @Test
    public void blankSeparatorBecomesNull() {
        final FormatAndSeparator blank = DateTimeFormatCache.mapDateFormat("ISO", " ").orElseThrow();
        assertEquals(null, blank.separator());
        final FormatAndSeparator empty = DateTimeFormatCache.mapDateFormat("ISO", "").orElseThrow();
        assertEquals(null, empty.separator());
        final FormatAndSeparator dot = DateTimeFormatCache.mapDateFormat("EUR", ".").orElseThrow();
        assertEquals(Character.valueOf('.'), dot.separator());
    }

    @Test
    public void getDateReadsCatalogFormatAndSeparator() throws Exception {
        final ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(true, false);
        when(rs.getString(1)).thenReturn("ORDERS"); // long table name
        when(rs.getString(2)).thenReturn("ORDRS"); // system (short) table name
        when(rs.getString(3)).thenReturn("ORDER_DATE"); // long column name
        when(rs.getString(4)).thenReturn("ORDDT"); // system (short) column name
        when(rs.getString(5)).thenReturn("EUR"); // date_format
        when(rs.getString(6)).thenReturn("."); // date_separator
        when(rs.getString(7)).thenReturn(null); // time_format
        when(rs.getString(8)).thenReturn(null); // time_separator

        final PreparedStatement ps = mock(PreparedStatement.class);
        when(ps.executeQuery()).thenReturn(rs);
        final Connection con = mock(Connection.class);
        when(con.prepareStatement(anyString())).thenReturn(ps);

        final DateTimeFormatCache cache = new DateTimeFormatCache(() -> con);

        final AS400Date date = cache.getDate("SCHEMA", "ORDERS", "ORDER_DATE").orElseThrow();
        assertEquals(AS400Date.FORMAT_EUR, date.getFormat());
        assertEquals(Character.valueOf('.'), date.getSeparator());
        // *EUR is dd.MM.yyyy -> 10 bytes; a mismatched *ISO assumption would mis-size the column
        assertEquals(10, date.getByteLength());

        // short table/column name resolves to the same format; a non-date column stays empty
        assertEquals(AS400Date.FORMAT_EUR, cache.getDate("SCHEMA", "ORDRS", "ORDDT").orElseThrow().getFormat());
        assertTrue(cache.getDate("SCHEMA", "ORDERS", "UNKNOWN_COL").isEmpty());
    }

    @Test
    public void lookupFailureReturnsEmpty() {
        // null connection supplier -> NPE inside fetch is swallowed, caller falls back to default
        final DateTimeFormatCache cache = new DateTimeFormatCache(() -> {
            throw new java.sql.SQLException("boom");
        });
        assertTrue(cache.getDate("SCHEMA", "ORDERS", "ORDER_DATE").isEmpty());
        assertTrue(cache.getTime("SCHEMA", "ORDERS", "ORDER_TIME").isEmpty());
    }

    @Test
    public void negativeLookupIsCached() throws Exception {
        final ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(false);
        final PreparedStatement ps = mock(PreparedStatement.class);
        when(ps.executeQuery()).thenReturn(rs);
        final Connection con = mock(Connection.class);
        when(con.prepareStatement(anyString())).thenReturn(ps);

        final int[] calls = { 0 };
        final DateTimeFormatCache cache = new DateTimeFormatCache(() -> {
            calls[0]++;
            return con;
        });

        assertTrue(cache.getDate("SCHEMA", "ORDERS", "ORDER_DATE").isEmpty());
        assertTrue(cache.getDate("SCHEMA", "ORDERS", "ORDER_DATE").isEmpty());
        assertEquals(1, calls[0], "second lookup of the same column must hit the negative cache");
    }
}
