/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400BindingDefinition.Codec;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

class As400BindingDefinitionTest {
    record Example(Codec codec, int jdbc, String hex, Object expected) {
    }

    static Stream<Example> codecs() {
        return Stream.of(new Example(new Codec("BIN2", 2, 0, 0), Types.SMALLINT, "0007", (short) 7),
                new Example(new Codec("BIN4", 4, 0, 0), Types.INTEGER, "0000002a", 42),
                new Example(new Codec("BIN8", 8, 0, 0), Types.BIGINT, "000000000000002a", 42L),
                new Example(new Codec("FLOAT4", 4, 0, 0), Types.REAL, "3fc00000", 1.5f),
                new Example(new Codec("FLOAT8", 8, 0, 0), Types.DOUBLE, "3ff8000000000000", 1.5d),
                new Example(new Codec("BOOLEAN", 1, 0, 0), Types.BOOLEAN, "f1", true),
                new Example(new Codec("PACKED", 5, 2, 0), Types.DECIMAL, "12345c", new BigDecimal("123.45")),
                new Example(new Codec("ZONED", 5, 2, 0), Types.NUMERIC, "f1f2f3f4c5", new BigDecimal("123.45")),
                new Example(new Codec("BINARY", 3, 0, 0), Types.BINARY, "000102", new byte[]{ 0, 1, 2 }),
                new Example(new Codec("VARBINARY", 3, 0, 0), Types.VARBINARY, "0003000102", new byte[]{ 0, 1, 2 }),
                new Example(new Codec("TEXT", 3, 0, 37), Types.CHAR, "c1c2c3", "ABC"),
                new Example(new Codec("VARCHAR", 3, 0, 37), Types.VARCHAR, "0003c1c2c3", "ABC"));
    }

    @ParameterizedTest
    @MethodSource("codecs")
    void effectiveCodecSchemaKeysAndNullabilitySurviveStandardHistoryBytes(Example example) throws Exception {
        var definition = definition(example, true);
        var serializer = new JsonTableChangeSerializer();
        var document = Document.create();
        document.setArray("tables", serializer.serialize(new TableChanges().create(definition.storedTable())));
        var reloaded = DocumentReader.defaultReader().read(DocumentWriter.defaultWriter().write(document));
        var table = serializer.deserialize(reloaded.getArray("tables"), false).iterator().next().getTable();
        var recovered = As400BindingDefinition.restore(table);
        assertThat(recovered.identity()).isEqualTo(definition.identity());
        assertThat(recovered.schema()).isEqualTo(definition.schema());
        assertThat(recovered.codecs()).isEqualTo(definition.codecs());
        assertThat(recovered.schema().primaryKeyColumnNames()).containsExactly("ID");
        assertThat(recovered.schema().columns().get(0).isOptional()).isTrue();
        assertThat(recovered.codecs().get(0).decoder().toObject(HexFormat.of().parseHex(example.hex()))).isEqualTo(example.expected());
        recovered.binding(1024);
    }

    @ParameterizedTest
    @ValueSource(strings = { "DATE:10:0:0", "TIME:8:0:0", "TIMESTAMP:26:0:0", "XML:4:0:0", "BIN4:8:0:0", "BIN4:4:1:0",
            "TEXT:4:0:0", "VARCHAR:4:0:1208", "VARBINARY:65535:0:0", "PACKED:64:0:0", "BIN4:04:0:0", "BIN4:4:0:0:extra" })
    void unqualifiedOrImplicitCodecSettingsFailClosed(String encoded) {
        assertThrows(IllegalArgumentException.class, () -> Codec.decode(encoded));
    }

    @Test
    void incorrectSchemaTypeOrPhysicalFormatCannotBecomeAStoredBinding() {
        var example = new Example(new Codec("BIN4", 4, 0, 0), Types.INTEGER, "0000002a", 42);
        var valid = definition(example, false);
        assertThrows(IllegalArgumentException.class, () -> new As400BindingDefinition(valid.identity(), valid.schema(), List.of(new Codec("FLOAT4", 4, 0, 0))));
        var id = valid.identity();
        var wrongLength = new TableIdentity(id.tableId(), id.physicalFile(), id.member(), id.fileJournalId(), id.memberJournalId(), "T:0123456789ABC:8:1:false");
        assertThrows(IllegalArgumentException.class, () -> new As400BindingDefinition(wrongLength, valid.schema(), valid.codecs()));
        assertThrows(IllegalArgumentException.class, () -> valid.binding(3));
    }

    private static As400BindingDefinition definition(Example example, boolean nullable) {
        Codec codec = example.codec();
        var id = new TableId(null, "LIB", "T");
        boolean varying = codec.kind().equals("VARBINARY") || codec.kind().equals("VARCHAR");
        var identity = new TableIdentity(id, "T", "T", "00000000000000000001", "00000000000000000002",
                "T:0123456789ABC:" + codec.decoder().getByteLength() + ":1:" + varying);
        var column = Column.editor().name("ID").position(1).jdbcType(example.jdbc()).type(codec.kind()).length(codec.size()).scale(codec.scale())
                .optional(nullable).create();
        var table = Table.editor().tableId(id).addColumn(column).setPrimaryKeyNames("ID").create();
        return new As400BindingDefinition(identity, table, List.of(codec));
    }
}
