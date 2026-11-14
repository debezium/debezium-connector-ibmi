/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400RecoveryTaskTest.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class As400SnapshotContractTest {
    @TempDir
    Path directory;

    @ParameterizedTest
    @ValueSource(ints = { 0, 1, 600, 900 })
    void lastRowAndJmxProgressFollowActualTaskIncludingTrailingEmptyTables(int count) throws Exception {
        var fixture = fixture(count);
        try (var run = new As400InitializationTaskTest.Running(fixture, null, input -> input)) {
            var properties = As400RecoveryTaskTest.properties();
            properties.put("max.batch.size", "128");
            properties.put("max.queue.size", "1024");
            run.started = true;
            run.task.start(properties);
            int rows = 0;
            int last = 0;
            while (true) {
                var batch = run.next();
                for (var record : batch) {
                    assertThat(record.sourceOffset()).doesNotContainKey("offset.version");
                    if (record.value() instanceof Struct value) {
                        assertThat(last).isZero();
                        rows++;
                        String flag = value.getStruct("source").getString("snapshot");
                        assertThat(flag).isIn("true", "last");
                        if (flag.equals("last")) {
                            last++;
                            assertThat(rows).isEqualTo(count);
                        }
                    }
                }
                run.ack(batch);
                if (batch.stream().anyMatch(r -> "BASELINE".equals(r.value()))) {
                    break;
                }
            }
            assertThat(rows).isEqualTo(count);
            assertThat(last).isEqualTo(count == 0 ? 0 : 1);
            run.ack(run.through("INITIAL"));
            run.ack(run.throughCheckpoint("42"));
            String tag = properties.get("custom.metric.tags").split("=", 2)[1];
            await(() -> Boolean.TRUE.equals(metric(tag, "snapshot", "SnapshotCompleted")));
            assertThat(metric(tag, "snapshot", "TotalNumberOfEventsSeen")).isEqualTo((long) count);
            assertThat(metric(tag, "snapshot", "TotalNumberOfReadEventsSeen")).isEqualTo((long) count);
            if (count > 0) {
                assertThat(metric(tag, "snapshot", "LastEvent").toString()).contains("baseline-complete");
            }
            var scanned = (javax.management.openmbean.TabularData) metric(tag, "snapshot", "RowsScanned");
            assertThat(scanned.values().stream().mapToLong(row -> (Long) ((javax.management.openmbean.CompositeData) row).get("value")).sum()).isEqualTo(count);
            assertThat(metric(tag, "snapshot", "RemainingTableCount")).isEqualTo(0);
            await(() -> metric(tag, "streaming", "DurableCompletedPosition").toString().endsWith(":42"));
            assertThat(metric(tag, "streaming", "ScanPosition").toString()).endsWith(":42");
            assertThat(metric(tag, "streaming", "JournalOffset")).isEqualTo(42L);
            assertThat((long) metric(tag, "streaming", "LastProcessedMs")).isPositive();
            assertThat(metric(tag, "streaming", "JournalBehind")).isEqualTo(-1L);
            assertThat(fixture.handoffs).hasValue(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "initial_only", "no_data" })
    void actualTaskCannotBypassSnapshotPolicy(String mode) throws Exception {
        var fixture = fixture(3);
        try (var run = new As400InitializationTaskTest.Running(fixture, null, input -> input)) {
            var properties = As400RecoveryTaskTest.properties();
            properties.put("snapshot.mode", mode);
            run.started = true;
            assertThrows(IllegalArgumentException.class, () -> run.task.start(properties));
            assertThat(fixture.delivered).isZero();
            assertThat(fixture.handoffs).hasValue(0);
            assertThat(run.durable.get()).isNull();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void finalRowDoesNotMakeAnInterruptedBaselineResumable(boolean acknowledgeLast) throws Exception {
        var fixture = fixture(600);
        Map<String, ?> saved;
        try (var run = new As400InitializationTaskTest.Running(fixture, null, input -> input)) {
            run.start();
            for (int i = 0; i < 3; i++) {
                run.ack(run.through("CHUNK"));
            }
            var baseline = run.through("BASELINE");
            assertThat(baseline.stream().filter(r -> r.value() instanceof Struct v && "last".equals(v.getStruct("source").getString("snapshot")))).hasSize(1);
            if (acknowledgeLast) {
                run.ack(baseline);
                run.through("INITIAL"); // Deliberately never acknowledge the initial F/W.
            }
            saved = run.durable.get();
            assertThat(saved).doesNotContainKey("offset.version");
            assertThat(fixture.handoffs).hasValue(0);
        }
        try (var replacement = new As400InitializationTaskTest.Running(directory, saved)) {
            assertThrows(Exception.class, replacement::start);
            assertThat(replacement.durable.get()).isEqualTo(saved);
        }
    }

    private As400ChunkedInitializationFixture fixture(int count) throws Exception {
        return new As400ChunkedInitializationFixture(directory, count) {
            @Override
            public void scanBaseline(String epoch, List<As400BindingDefinition> definitions, As400SnapshotChunks.Consumer consumer) throws Exception {
                var table = definitions.get(0).schema().id();
                for (int start = 1; start <= count; start += limits.rows()) {
                    var rows = new ArrayList<Row>();
                    for (int id = start; id < start + limits.rows() && id <= count; id++) {
                        rows.add(new Row(table, As400RowData.copyOf(new Object[]{ id }, 65536), Instant.EPOCH));
                    }
                    consumer.chunk(table, rows);
                    delivered += rows.size();
                }
                for (var definition : definitions) {
                    consumer.complete(definition.schema().id());
                }
                setup.session.prepareBaseline(epoch, definitions);
                materialized = true;
            }
        };
    }

    private static Object metric(String tag, String context, String attribute) {
        try {
            var server = ManagementFactory.getPlatformMBeanServer();
            var beans = server.queryNames(new ObjectName("debezium.*:type=connector-metrics,*"), null);
            var bean = beans.stream().filter(n -> tag.equals(n.getKeyProperty("test")) && context.equals(n.getKeyProperty("context"))).findFirst().orElseThrow();
            return server.getAttribute(bean, attribute);
        }
        catch (Exception e) {
            throw new AssertionError("Missing snapshot/recovery JMX attribute " + attribute, e);
        }
    }
}
