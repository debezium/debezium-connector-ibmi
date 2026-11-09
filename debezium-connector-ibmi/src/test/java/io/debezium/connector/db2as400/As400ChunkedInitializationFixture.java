/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** Controlled source that never allocates the complete baseline or opens IBM i. */
class As400ChunkedInitializationFixture extends As400InitializationTaskFixture {
    final int count;
    As400SnapshotChunks.Limits limits;
    volatile int delivered;
    volatile String chunkFault = "";

    As400ChunkedInitializationFixture(Path path, int count) throws Exception {
        super(path);
        this.count = count;
        limits = new As400SnapshotChunks.Limits(256, 1_000_000, Math.max(1, count), Math.max(1, count * 8L));
    }

    @Override
    public boolean supportsChunks() {
        return true;
    }

    @Override
    public Optional<As400SnapshotChunks.Limits> chunkLimits() {
        return Optional.of(limits);
    }

    @Override
    public void scanBaseline(String epoch, List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
        gate("prepare");
        int remaining = count;
        for (int t = 0; t < definitions.size(); t++) {
            var table = definitions.get(t).schema().id();
            int rows = remaining / (definitions.size() - t);
            remaining -= rows;
            for (int id = 1; id <= rows;) {
                var chunk = new ArrayList<Row>();
                for (int n = 0; n < limits.rows() && id <= rows; n++, id++) {
                    Object value = rowFault.equals("type") ? "wrong" : rowFault.equals("null-key") ? null : id;
                    Object[] values = rowFault.equals("projection") ? new Object[]{ value, 2 } : new Object[]{ value };
                    chunk.add(new Row(table, As400RowData.copyOf(values, 65536), Instant.EPOCH));
                }
                consumer.chunk(table, chunk);
                delivered += chunk.size();
                gate("chunk");
                if (chunkFault.equals("after-chunk")) {
                    throw new IllegalStateException("Snapshot cursor close failed");
                }
            }
            if (!chunkFault.equals("missing-eof")) {
                consumer.complete(table);
            }
            if (chunkFault.equals("duplicate-eof")) {
                consumer.complete(table);
            }
        }
        // The existing mocked boundary refuses to run until all reads/close finish.
        setup.session.prepareBaseline(epoch, definitions);
        materialized = true;
    }

    @Override
    public List<Row> prepareBaseline(String epoch, List<As400BindingDefinition> definitions) {
        throw new AssertionError("Chunk path called whole-baseline materialization");
    }
}
