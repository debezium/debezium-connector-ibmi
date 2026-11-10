/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class As400NativeConnectProfileTest {
    @TempDir
    Path directory;

    @Test
    void nativeTestProfileRejectsPartialProjectionAndNullableKeyLayouts() throws Exception {
        var definition = new As400RecoveryTestSupport.Fixture(directory).definitions.get(0);
        As400NativeConnectProbeConnector.requireIntegerTable(definition);
        var withoutKey = new As400BindingDefinition(definition.identity(), definition.schema().edit().setPrimaryKeyNames(List.of()).create(), definition.codecs());
        assertThrows(IllegalArgumentException.class, () -> As400NativeConnectProbeConnector.requireIntegerTable(withoutKey));
        var nullable = definition.schema().columns().get(0).edit().optional(true).create();
        var optional = new As400BindingDefinition(definition.identity(), definition.schema().edit().addColumn(nullable).create(), definition.codecs());
        assertThrows(IllegalArgumentException.class, () -> As400NativeConnectProbeConnector.requireIntegerTable(optional));
    }

    @Test
    void controllerCannotSelectSharedLibraryOrUnboundedOperation() {
        assertThrows(IllegalArgumentException.class, () -> As400NativeConnectTransactions.main(new String[]{ "CONLIBV", directory.toString(), "truncate-commit" }));
        assertThrows(IllegalArgumentException.class, () -> As400NativeConnectTransactions.main(new String[]{ "DCNC0001", directory.toString(), "arbitrary" }));
    }
}
