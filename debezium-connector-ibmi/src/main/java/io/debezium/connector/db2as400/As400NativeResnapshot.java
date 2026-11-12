/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.jdbc.DefaultMainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.notification.channels.NotificationChannel;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;

/** Ordinary requested snapshot with its previous native recovery checkpoint retained until delivery completes. */
final class As400NativeResnapshot {
    private As400NativeResnapshot() {
    }

    static As400Checkpoint execute(As400TaskRecovery input, As400ConnectorConfig config,
                                   CdcSourceTaskContext<As400ConnectorConfig> taskContext, As400Checkpoint original,
                                   As400BindingHistory.Recovered history, ChangeEventSourceContext context,
                                   SnapshotProgressListener<As400Partition> listener, List<NotificationChannel> channels,
                                   As400TaskHandoff.Output output, Function<Map<String, ?>, SourceRecord> marker, boolean interrupted)
            throws Exception {
        var snapshotter = config.getServiceRegistry().getService(SnapshotterService.class).getSnapshotter();
        if (!snapshotter.shouldSnapshotData(true, interrupted)) {
            throw new IllegalArgumentException("A pending full snapshot requires a snapshotting policy; its old F/W remains authoritative");
        }
        if (!original.snapshotState().isEmpty()) {
            throw new IllegalArgumentException("Complete the active incremental snapshot before requesting a full restart snapshot");
        }
        var pending = As400FullSnapshotState.pending(original);
        var expected = new HashMap<String, TableSchema>();
        As400Checkpoint[] candidate = { null };
        var checkedOutput = new As400TaskHandoff.Output() {
            private SourceRecord stamp(SourceRecord record) {
                if (record.value() instanceof Struct value && value.schema().field("op") != null && "r".equals(value.getString("op"))) {
                    var schema = Objects.requireNonNull(expected.get(record.topic()), "Snapshot table has no retained native definition");
                    if (!Objects.equals(schema.keySchema(), record.keySchema()) || !schema.valueSchema().equals(value.getStruct("after").schema())) {
                        throw new IllegalArgumentException("Current snapshot schema differs from the qualified journal boundary");
                    }
                }
                return new SourceRecord(record.sourcePartition(), pending, record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                        record.valueSchema(), record.value(), record.timestamp(), record.headers());
            }

            @Override
            public void emit(SourceRecord record) throws Exception {
                output.emit(stamp(record));
            }

            private SourceRecord boundary;

            @Override
            public void barrier(SourceRecord record) throws Exception {
                boundary = stamp(record);
                output.barrier(boundary);
            }

            @Override
            public String acknowledge(SourceRecord record) throws Exception {
                return output.acknowledge(Objects.requireNonNull(boundary));
            }
        };
        var connections = new DefaultMainConnectionProvidingConnectionFactory<>(() -> {
            try {
                return input.incrementalSnapshotConnection(config);
            }
            catch (RuntimeException failure) {
                throw failure;
            }
            catch (Exception failure) {
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new io.debezium.DebeziumException("Cannot open requested snapshot connection", failure);
            }
        });
        var previous = new As400OffsetContext(config);
        if (!interrupted) {
            previous.postSnapshotCompletion();
        }
        As400LegacySnapshotBootstrap.execute(config, taskContext, connections, () -> {
            JournalPosition target = input.snapshotPosition();
            var definitions = new HashMap<String, As400BindingDefinition>();
            history.definitions().forEach(d -> definitions.put(As400SchemaHistory.key(d), d));
            // Replay validates every transaction/lifecycle transition but emits
            // nothing. Only the subsequently acknowledged SQL snapshot can make
            // this candidate completed-output position durable.
            try (var replay = new As400ContinuousRuntime(original.toOffset(), history, input, (batch, records) -> {
            }, record -> {
                throw new IllegalStateException("Snapshot boundary reconstruction cannot publish journal output");
            }, input.partition().getSourcePartition(), () -> {
            }, input.runtimeLimits(), d -> definitions.put(As400SchemaHistory.key(d), d))) {
                Map<String, ?> progress = original.toOffset();
                while (true) {
                    if (!context.isRunning()) {
                        throw new InterruptedException("Requested snapshot stopped before its boundary was reconstructed");
                    }
                    input.checkQualified(config.getConfig());
                    var read = replay.poll(progress, original.toOffset());
                    progress = read.checkpoint();
                    if (!read.deferred() && (read.scan().map(p -> p.offset().compareTo(target.offset()) >= 0).orElse(false)
                            || read.entries() == 0 && original.completedThrough().equals(target))) {
                        break;
                    }
                    if (read.deferred()) {
                        Thread.sleep(250);
                    }
                }
                candidate[0] = snapshotter.shouldStreamEventsStartingFromSnapshot()
                        ? As400Checkpoint.fromOffset(progress, original.journalIdentity(), original.captureFingerprint())
                        : original;
            }
            TopicNamingStrategy<TableId> topics = config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
            var schemas = new As400NativeTableSchemas(List.copyOf(definitions.values()), config, topics);
            for (var definition : definitions.values()) {
                var id = definition.identity().tableId();
                if (config.getTableFilters().dataCollectionFilter().isIncluded(id)) {
                    if (expected.put(topics.dataChangeTopic(id), schemas.schemaFor(id)) != null) {
                        throw new IllegalArgumentException("Ambiguous snapshot topic mapping");
                    }
                }
            }
            return snapshotter.shouldStreamEventsStartingFromSnapshot() ? target : original.completedThrough();
        }, context, previous, listener, channels, checkedOutput, marker);
        var completed = Objects.requireNonNull(candidate[0], "Snapshot did not establish a native boundary");
        var barrier = marker.apply(completed.toOffset());
        output.barrier(barrier);
        output.acknowledge(barrier);
        return completed;
    }
}
