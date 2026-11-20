/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class StringHelpersTest {
    @ParameterizedTest
    @CsvSource(value = { "1234567890|10|1234567890", "12345|4|1234", "12|4|12  " }, delimiter = '|', ignoreLeadingAndTrailingWhitespace = false)
    void shouldFitFixedWidthFieldWhenPaddingRight(String input, int width, String expected) {
        assertEquals(expected, StringHelpers.padRight(input, width));
    }

    @ParameterizedTest
    @CsvSource(value = { "1234567890|10|1234567890", "12345|4|1234", "12|4|  12" }, delimiter = '|', ignoreLeadingAndTrailingWhitespace = false)
    void shouldFitFixedWidthFieldWhenPaddingLeft(String input, int width, String expected) {
        assertEquals(expected, StringHelpers.padLeft(input, width));
    }
}
