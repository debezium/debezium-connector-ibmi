/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.apache.kafka.connect.source.SourceRecord;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.QueueProvider;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.notification.channels.NotificationChannel;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.CustomConverterRegistry;
import io.debezium.relational.TableId;
import io.debezium.schema.SchemaFactory;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;

/** Ordinary snapshotter with bounded, acknowledged delivery before online handoff. */
final class As400LegacySnapshotBootstrap {
    private As400LegacySnapshotBootstrap() {
    }

    static Map<String, ?> execute(As400ConnectorConfig config, CdcSourceTaskContext<As400ConnectorConfig> taskContext,
                                  MainConnectionProvidingConnectionFactory<As400JdbcConnection> connections,
                                  Callable<JournalPosition> currentPosition, ChangeEventSourceContext context, As400OffsetContext previous,
                                  SnapshotProgressListener<As400Partition> listener, List<NotificationChannel> channels, As400TaskHandoff.Output output,
                                  Function<Map<String, ?>, SourceRecord> marker)
            throws Exception {
        var partition = new As400Partition(config.getLogicalName());
        TopicNamingStrategy<TableId> topics = config.getTopicNamingStrategy(As400ConnectorConfig.TOPIC_NAMING_STRATEGY, true);
        try (var jdbc = connections.mainConnection();
                var schemas = new As400DatabaseSchema(config, jdbc, topics, config.schemaNameAdjuster(),
                        config.getServiceRegistry().tryGetService(CustomConverterRegistry.class), taskContext)) {
            var delivery = new Delivery(output, marker);
            var queue = new ChangeEventQueue.Builder<DataChangeEvent>().maxBatchSize(256).maxQueueSize(512).pollInterval(Duration.ZERO)
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext("truncate-initial-snapshot"))
                    .queueProvider(delivery).build();
            try (var dispatcher = new As400EventDispatcher(config, topics, schemas, queue, config.getTableFilters().dataCollectionFilter(),
                    DataChangeEvent::new, new As400EventMetadataProvider(), config.schemaNameAdjuster(), null, new DebeziumHeaderProducer(taskContext))) {
                var notifications = new NotificationService<As400Partition, As400OffsetContext>(channels, config, SchemaFactory.get(), dispatcher::enqueueNotification);
                try {
                    var source = new As400SnapshotChangeEventSource(config, currentPosition, connections, schemas, dispatcher, Clock.system(), listener,
                            notifications, config.getServiceRegistry().tryGetService(SnapshotterService.class));
                    var result = source.execute(context, partition, previous, source.getSnapshottingTask(partition, previous));
                    if (!context.isRunning() || !result.isCompletedOrSkipped() || result.getOffset() == null) {
                        throw new InterruptedException("Ordinary first-start snapshot did not complete");
                    }
                    var saved = Map.copyOf(result.getOffset().getOffset());
                    As400LegacyHandoff.startingPosition(As400LegacyHandoff.strings(saved));
                    delivery.flush(saved);
                    return saved;
                }
                finally {
                    notifications.stop();
                }
            }
        }
    }

    /** No snapshot rows are retained here; a durable boundary bounds the outer task's acknowledgement state. */
    static final class Delivery implements QueueProvider<DataChangeEvent> {
        private final As400TaskHandoff.Output output;
        private final Function<Map<String, ?>, SourceRecord> marker;
        private final ArrayDeque<SourceRecord> beforeOffset = new ArrayDeque<>();
        private Exception failure;
        private int count;
        private Map<String, ?> pinned;

        Delivery(As400TaskHandoff.Output output, Function<Map<String, ?>, SourceRecord> marker) {
            this.output = output;
            this.marker = marker;
        }

        @Override
        public void configure(Map<String, ?> config) {
        }

        @Override
        public String name() {
            return "acknowledged-initial-snapshot";
        }

        @Override
        public synchronized void enqueue(DataChangeEvent event) throws InterruptedException {
            try {
                check();
                var record = event.getRecord();
                // Core emits STARTED/resolved notifications before determining
                // its journal position. Do not write an empty source offset.
                if (record.sourceOffset() == null || record.sourceOffset().isEmpty()) {
                    if (beforeOffset.size() >= 256) {
                        throw new IllegalStateException("Too many notifications before the snapshot offset");
                    }
                    beforeOffset.addLast(record);
                }
                else {
                    releaseNotifications(record.sourceOffset());
                    emit(record);
                }
            }
            catch (InterruptedException interrupted) {
                failure = interrupted;
                throw interrupted;
            }
            catch (Exception failure) {
                this.failure = failure;
                throw new IllegalStateException("Initial snapshot delivery failed before online handoff", failure);
            }
        }

        synchronized void flush(Map<String, ?> offset) throws Exception {
            check();
            var safe = pinned == null ? offset : pinned;
            releaseNotifications(safe);
            boundary(safe);
        }

        /** Hold every automatic boundary, including an enqueue's 256-record flush. */
        synchronized void pin(Map<String, ?> offset) throws Exception {
            check();
            if (pinned == null) {
                pinned = Map.copyOf(offset);
                flush(pinned);
            }
        }

        synchronized void releasePin() throws Exception {
            check();
            pinned = null;
        }

        synchronized Map<String, ?> safeOffset(Map<String, ?> offset) {
            return pinned == null ? offset : pinned;
        }

        synchronized boolean pending() {
            return count > 0 || !beforeOffset.isEmpty();
        }

        private void boundary(Map<String, ?> offset) throws Exception {
            var barrier = marker.apply(offset);
            output.barrier(barrier);
            output.acknowledge(barrier);
            count = 0;
        }

        private void emit(SourceRecord record) throws Exception {
            if (pinned != null) {
                record = new SourceRecord(record.sourcePartition(), pinned, record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                        record.valueSchema(), record.value(), record.timestamp(), record.headers());
            }
            output.emit(record);
            if (++count == 256) {
                boundary(record.sourceOffset());
            }
        }

        private void releaseNotifications(Map<String, ?> offset) throws Exception {
            while (!beforeOffset.isEmpty()) {
                var record = beforeOffset.removeFirst();
                emit(new SourceRecord(record.sourcePartition(), offset, record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                        record.valueSchema(), record.value(), record.timestamp(), record.headers()));
            }
        }

        private void check() throws Exception {
            if (failure != null) {
                throw failure; // A notification channel may catch interruption; it cannot authorize completion afterward.
            }
        }

        @Override
        public DataChangeEvent poll() {
            return null;
        }

        @Override
        public int size() {
            return 0;
        }
    }
}
