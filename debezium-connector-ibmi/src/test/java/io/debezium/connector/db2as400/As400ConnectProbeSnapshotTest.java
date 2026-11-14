/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

class As400ConnectProbeSnapshotTest {
    @Test
    void shouldRetainLatestOffsetsAndTombstonesWithoutIncludingLaterAppends() {
        var snapshot = new As400ConnectProbeConnector.SnapshotRows(true);
        for (int offset = 0; offset < 2000; offset++) {
            snapshot.accept(record(offset, "connector", "position-" + offset), 2002);
        }
        snapshot.accept(record(2000, "deleted", "old-position"), 2002);
        snapshot.accept(record(2001, "deleted", null), 2002);
        snapshot.accept(record(2002, "connector", "later-append"), 2002);
        assertThat(snapshot.rows()).hasSize(2);
        assertThat(snapshot.rows().get(0)).containsEntry("offset", 1999L).containsEntry("value", "position-1999");
        assertThat(snapshot.rows().get(1)).containsEntry("offset", 2001L).containsEntry("value", null);
    }

    @Test
    void shouldPreserveEveryOutputEventAndRejectAnOversizedOrdinarySnapshot() {
        var snapshot = new As400ConnectProbeConnector.SnapshotRows(false);
        for (int offset = 0; offset < 1000; offset++) {
            snapshot.accept(record(offset, "same-key", "same-value"), 1001);
        }
        assertThat(snapshot.rows()).hasSize(1000);
        assertThatThrownBy(() -> snapshot.accept(record(1000, "same-key", "same-value"), 1001))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("retained-state bound");
    }

    @Test
    void shouldRejectUnboundedOffsetKeysAndValues() {
        var snapshot = new As400ConnectProbeConnector.SnapshotRows(true);
        assertThatThrownBy(() -> snapshot.accept(record(0, null, "position"), 2000))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("offset key");
        assertThatThrownBy(() -> snapshot.accept(record(1, "connector", "x".repeat(8 * 1024 * 1024)), 2000))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("retained-state bound");
        assertThat(snapshot.rows()).isEmpty();
        for (int offset = 2; offset < 1002; offset++) {
            snapshot.accept(record(offset, "key-" + offset, "position"), 2000);
        }
        assertThatThrownBy(() -> snapshot.accept(record(1002, "extra-key", "position"), 2000))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("retained-state bound");
    }

    @Test
    void shouldBoundScannedHistoryEvenWhenOnlyOneOffsetIsRetained() {
        var snapshot = new As400ConnectProbeConnector.SnapshotRows(true);
        for (int offset = 0; offset < 100_000; offset++) {
            snapshot.accept(record(offset, "connector", "position"), 100_001);
        }
        assertThat(snapshot.rows()).hasSize(1);
        assertThatThrownBy(() -> snapshot.accept(record(100_000, "connector", "position"), 100_001))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("scan bound");
    }

    private static ConsumerRecord<String, String> record(long offset, String key, String value) {
        return new ConsumerRecord<>("offsets", 0, offset, key, value);
    }
}
