/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.DataCollection;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.relational.TableId;

/**
 * Verifies that an in-progress incremental snapshot survives the
 * {@link As400OffsetContext#getOffset()} → {@link As400OffsetContext.Loader#load(Map)}
 * round-trip, so that connector restarts resume the snapshot instead of dropping its state.
 */
public class As400OffsetContextTest {

    private static final String CORRELATION_ID = "test-correlation-id";
    private static final String DATA_COLLECTION = "MYDB.MYSCHEMA.MYTABLE";

    private As400ConnectorConfig newConfig() {
        return new As400ConnectorConfig(Configuration.create()
                .with(CommonConnectorConfig.TOPIC_PREFIX, "serverX")
                .with(As400ConnectorConfig.DATABASE_NAME, "serverX")
                .build());
    }

    private JournalProcessedPosition newPosition() {
        return new JournalProcessedPosition(
                BigInteger.valueOf(82),
                new JournalReceiver("RECV008", "RCV_LIB"),
                Instant.ofEpochSecond(123_456L),
                true);
    }

    @Test
    public void inProgressIncrementalSnapshotSurvivesOffsetRoundTrip() {
        final As400ConnectorConfig config = newConfig();
        final As400OffsetContext original = new As400OffsetContext(config, newPosition());

        @SuppressWarnings("unchecked")
        final IncrementalSnapshotContext<TableId> snapshot = (IncrementalSnapshotContext<TableId>) original
                .getIncrementalSnapshotContext();
        final List<DataCollection<TableId>> added = snapshot.addDataCollectionNamesToSnapshot(
                CORRELATION_ID, List.of(DATA_COLLECTION), List.of(), "");
        assertThat(added).hasSize(1);
        snapshot.nextChunkPosition(new Object[]{ "key-99" });
        final Object[] lastEmitted = new Object[]{ "key-50" };
        snapshot.sendEvent(lastEmitted);
        snapshot.maximumKey(new Object[]{ "key-1000" });

        assertThat(snapshot.snapshotRunning()).isTrue();

        final Map<String, Object> offset = (Map<String, Object>) original.getOffset();

        assertThat(offset)
                .containsKey(AbstractIncrementalSnapshotContext.EVENT_PRIMARY_KEY)
                .containsKey(AbstractIncrementalSnapshotContext.TABLE_MAXIMUM_KEY)
                .containsEntry(AbstractIncrementalSnapshotContext.CORRELATION_ID, CORRELATION_ID)
                .containsKey("incremental_snapshot_collections");

        final As400OffsetContext reloaded = new As400OffsetContext.Loader(config).load(offset);

        final IncrementalSnapshotContext<?> reloadedSnapshot = reloaded.getIncrementalSnapshotContext();
        assertThat(reloadedSnapshot.snapshotRunning())
                .as("reloaded context still has the snapshot active")
                .isTrue();
        assertThat(reloadedSnapshot.getCorrelationId()).isEqualTo(CORRELATION_ID);
        assertThat(reloadedSnapshot.dataCollectionsToBeSnapshottedCount()).isEqualTo(1);
        assertThat(reloadedSnapshot.currentDataCollectionId().getId().toString()).isEqualTo(DATA_COLLECTION);
        assertThat(reloadedSnapshot.chunkEndPosititon())
                .as("on reload, chunk end position becomes the last emitted row so resumption starts there")
                .containsExactly(lastEmitted);
        assertThat(reloadedSnapshot.maximumKey()).contains(new Object[]{ "key-1000" });
        assertThat(reloadedSnapshot.isNonInitialChunk()).isTrue();
    }

    @Test
    public void noIncrementalSnapshotKeysWhenSnapshotNotRunning() {
        final As400ConnectorConfig config = newConfig();
        final As400OffsetContext original = new As400OffsetContext(config, newPosition());

        final Map<String, ?> offset = original.getOffset();

        assertThat(offset)
                .doesNotContainKey(AbstractIncrementalSnapshotContext.EVENT_PRIMARY_KEY)
                .doesNotContainKey(AbstractIncrementalSnapshotContext.TABLE_MAXIMUM_KEY)
                .doesNotContainKey(AbstractIncrementalSnapshotContext.CORRELATION_ID)
                .doesNotContainKey("incremental_snapshot_collections");

        final As400OffsetContext reloaded = new As400OffsetContext.Loader(config).load(offset);
        assertThat(reloaded.getIncrementalSnapshotContext().snapshotRunning()).isFalse();
    }
}