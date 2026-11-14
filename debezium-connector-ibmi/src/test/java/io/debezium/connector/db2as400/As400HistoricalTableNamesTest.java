/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400Text;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.TableId;

class As400HistoricalTableNamesTest {
    private RawJournalPage.Entry entry(long sequence) throws Exception {
        try (var input = getClass().getResourceAsStream("/native-transactions/transactional-ddl-rollback.json")) {
            for (var row : new ObjectMapper().readTree(input).path("entries")) {
                byte[] bytes = Base64.getDecoder().decode(row.path("raw").asText());
                var header = new EntryHeaderDecoder(37).decode(bytes, 0);
                if (header.getSequenceNumber().equals(BigInteger.valueOf(sequence))) {
                    return new RawJournalPage.Entry(new JournalPosition(header.getSequenceNumber(), new JournalReceiver("QSQJRN0002", "DCNC0005")), header, bytes);
                }
            }
        }
        throw new IllegalArgumentException("Missing fixture entry");
    }

    private byte[] callbacks(RawJournalPage.Entry entry, String oldName, String newName) {
        byte[] result = new byte[8708];
        var buffer = ByteBuffer.wrap(result);
        var roles = new java.util.LinkedHashMap<Integer, String>();
        roles.put(1, entry.header().getFile());
        if (oldName != null) {
            roles.put(3, oldName);
        }
        if (newName != null) {
            roles.put(4, newName);
        }
        buffer.putInt(0, roles.size());
        int at = 4;
        for (var role : roles.entrySet()) {
            buffer.putInt(at, 537).putInt(at + 4, role.getValue().length()).putInt(at + 8, entry.header().getLibrary().length()).putInt(at + 12, 3);
            new AS400Text(258, 37).toBytes(role.getValue(), result, at + 16);
            new AS400Text(258, 37).toBytes(entry.header().getLibrary(), result, at + 274);
            buffer.putInt(at + 532, role.getKey());
            result[at + 536] = (byte) 0xf0;
            at += 544;
        }
        return result;
    }

    @ParameterizedTest
    @CsvSource({ "VERY_LONG_NAME,VERY_LONG_NAME", "'\"MixedCaseName\"',MixedCaseName", "'\"abc\"',abc", "'\"a\"\"b\"','a\"b'" })
    void shouldPreserveHistoricalSqlCaseAndEscapedQuotes(String nativeName, String expected) throws Exception {
        var entry = entry(59);
        var names = As400HistoricalTableNames.decode(entry, callbacks(entry, nativeName, null));
        assertThat(names.creation(entry, "CATALOG")).isEqualTo(new TableId("CATALOG", "DCNC0005", expected));
    }

    @Test
    void shouldRetainTheProvenSqlNameWhenLabelCallbacksOmitIt() throws Exception {
        var change = entry(83);
        var previous = new TableId(null, "DCNC0005", "LONG_ORIGINAL_NAME");
        assertThat(As400HistoricalTableNames.decode(change, callbacks(change, null, null)).changed(change, previous)).isEqualTo(previous);
        assertThat(As400HistoricalTableNames.decode(change, callbacks(change, "LONG_ORIGINAL_NAME", "LONG_RENAMED_NAME")).changed(change, previous))
                .isEqualTo(new TableId(null, "DCNC0005", "LONG_RENAMED_NAME"));
        assertThatThrownBy(() -> As400HistoricalTableNames.decode(change, callbacks(change, "DIFFERENT", "LONG_RENAMED_NAME")).changed(change, previous))
                .hasMessageContaining("chain differs");
    }

    @ParameterizedTest
    @CsvSource({ "0,LONG_ORIGINAL_NAME", "2,U" })
    void shouldDistinguishAnAlterFromRemovalOfTheAlternativeSqlName(char subtype, String expected) throws Exception {
        var original = entry(83);
        byte[] raw = original.data();
        raw[original.header().getEntrySpecificDataOffset() + 16 + 108] = new AS400Text(1, 37).toBytes(String.valueOf(subtype))[0];
        var change = new RawJournalPage.Entry(original.position(), new EntryHeaderDecoder(37).decode(raw, 0), raw);
        // Both native operations return old-name role 3 with no new-name role 4.
        // The D/CG subtype distinguishes retained ALTER names from removal.
        var observed = As400HistoricalTableNames.decode(change, callbacks(change, "LONG_ORIGINAL_NAME", null));
        assertThat(observed.changed(change, new TableId(null, "DCNC0005", "LONG_ORIGINAL_NAME")))
                .isEqualTo(new TableId(null, "DCNC0005", expected));
    }

    @Test
    void shouldRejectNamesFromAnotherRetainedJournalEntry() throws Exception {
        var first = entry(59);
        var names = As400HistoricalTableNames.decode(first, callbacks(first, null, null));
        byte[] changed = first.data();
        ByteBuffer.wrap(changed).putLong(24, 60);
        var second = new RawJournalPage.Entry(new JournalPosition(BigInteger.valueOf(60), first.position().receiver()),
                new EntryHeaderDecoder(37).decode(changed, 0), changed);
        assertThatThrownBy(() -> names.creation(second, null)).hasMessageContaining("do not match");
    }

    @ParameterizedTest
    @ValueSource(strings = { "A", "\"a\"", "signal" })
    void shouldRejectASelectedOrSignalNameEvenWhenItsNativeNameIsDifferent(String name) throws Exception {
        var creation = entry(59);
        var progress = new As400FileCreation.Progress(creation);
        var prefix = new java.util.ArrayList<RawJournalPage.Entry>();
        for (long at = 59; at <= 64; at++) {
            var item = entry(at);
            prefix.add(item);
            if (at < 64) {
                progress.physical(item);
            }
        }
        var receiver = creation.position().receiver();
        var order = new As400JournalOrder(java.util.List.of(receiver), java.util.Map.of(receiver, "fixture-receiver"),
                java.util.Map.of(receiver, new As400JournalOrder.Range(BigInteger.ONE, BigInteger.valueOf(100))));
        var config = new As400ConnectorConfig(io.debezium.config.Configuration.create().with("topic.prefix", "names")
                .with("table.include.list", "DCNC0005.A").with("signal.data.collection", "DCNC0005.signal").build());
        var selected = new As400SourceNormalizer.TableResolver() {
            @Override
            public As400SourceNormalizer.TableBinding resolve(String file, String library) {
                throw new AssertionError("No current definition may be substituted");
            }

            @Override
            public boolean needsValues(TableId table) {
                return config.getTableFilters().dataCollectionFilter().isIncluded(table) || config.isSignalDataCollection(table);
            }
        };
        assertThatThrownBy(() -> As400ExcludedCreation.prove(progress.identity(), prefix,
                new JournalPosition(BigInteger.valueOf(91), receiver), order,
                raw -> As400HistoricalTableNames.decode(raw, callbacks(raw, name, null)), null, selected))
                .hasMessageContaining("Selected transient table");
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing", "overflow", "short", "layout", "length", "library", "target", "repeated", "new-without-old", "post-state" })
    void shouldRejectIncompleteOrAmbiguousNativeNames(String fault) throws Exception {
        var entry = entry(59);
        byte[] values = callbacks(entry, "LONG_NAME", null);
        var buffer = ByteBuffer.wrap(values);
        switch (fault) {
            case "missing" -> buffer.putInt(0, 0);
            case "overflow" -> buffer.putInt(0, 17);
            case "short" -> values = Arrays.copyOf(values, 8707);
            case "layout" -> buffer.putInt(4, 538);
            case "length" -> buffer.putInt(8, 259);
            case "library" -> values[4 + 274] = (byte) 0xe7;
            case "target" -> values[4 + 16] = (byte) 0xe7;
            case "repeated" -> buffer.putInt(4 + 544 + 532, 1);
            case "new-without-old" -> buffer.putInt(4 + 544 + 532, 4);
            case "post-state" -> values[4 + 536] = (byte) 0xf1;
            default -> throw new AssertionError(fault);
        }
        byte[] malformed = values;
        assertThatThrownBy(() -> As400HistoricalTableNames.decode(entry, malformed)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldKeepNativeInspectionInNamesOnlyModeAndReleasePointerOwnership() throws Exception {
        String source = As400NativeHistoricalNames.resource("inspect.clle");
        String template = source.lines().filter(line -> line.contains("VAR(&T) VALUE('")).findFirst().orElseThrow();
        String value = template.substring(template.indexOf("VALUE('") + 7, template.lastIndexOf("')"));
        assertThat(value).hasSize(26);
        assertThat(value.charAt(24)).isEqualTo('1');
        assertThat(source.lines().filter(line -> line.contains("QDBRPLAY"))).hasSize(2); // comment and the one fixed call
        assertThat(source).contains("PRC('QjoDeletePointerHandle')", "VALUE(%BIN(&D 65 4) +", "%BIN(&D 73 4) + %BIN(&D 81 4)", "16773120");
        assertThat(source).doesNotContain("CRTPF", "CREATE TABLE", "DLTF", "DCNC");
        assertThat(As400NativeHistoricalNames.resource("capture.clp")).doesNotContain("VAR(&P) VALUE");
    }
}
