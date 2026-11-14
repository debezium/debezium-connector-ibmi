/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.io.TempDir;

import io.debezium.config.Configuration;

/** Run the same public-behavior contracts against disk storage, without copied test bodies. */
class As400SpillVariantsTest {
    @TempDir
    Path directory;
    private final List<As400SpillStore> stores = new ArrayList<>();

    private As400SpillStore store() {
        return store(new As400SpillStore.Settings(directory, 1, 16, 128L * 1024 * 1024));
    }

    private As400SpillStore store(As400SpillStore.Settings settings) {
        var store = new As400SpillStore(settings);
        stores.add(store);
        return store;
    }

    @AfterEach
    void closeStorage() {
        stores.forEach(As400SpillStore::close);
    }

    @Nested
    class Transactions extends As400TransactionBufferTest {
        @Override
        protected As400SpillStore spill() {
            return store();
        }
    }

    @Nested
    class Dispatch extends As400BatchDispatcherTest {
        @Override
        protected As400SpillStore spill() {
            // The large key-change contract uses configured output/buffer limits.
            // Match its disk profile too: an unrelated 128 MiB fixture quota can
            // reject valid output during MVStore chunk reclamation on slower CI.
            // Explicit small-budget rejection remains in As400SpillStoreTest.
            var settings = new As400DcgConfig(Configuration.from(As400DcgConfigTest.properties())).runtimeLimits.storage();
            return store(new As400SpillStore.Settings(directory, settings.cacheMegabytes(), settings.writeBufferKilobytes(), settings.maximumBytes()));
        }
    }

    @Nested
    class Replay extends As400ReplayCoordinatorTest {
        @Override
        protected As400SpillStore spill() {
            return store();
        }
    }

    @Nested
    class Lifecycle extends As400IncrementalLifecycleTest {
        @Override
        protected As400SpillStore spill() {
            return store();
        }
    }
}
