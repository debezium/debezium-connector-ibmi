/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.schema.AbstractTopicNamingStrategy;

/** Actual task/coordinator, with native admission only for the explicitly owned IBM i test fixture. */
public class As400NativeNormalTaskProbeConnector extends As400ConnectProbeConnector {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static Properties credentials;

    @Override
    public Class<? extends Task> taskClass() {
        return NativeTask.class;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("worker")) {
            credentials = new Properties();
            credentials.load(System.in);
            for (String key : List.of("database.hostname", "database.user", "database.password")) {
                if (credentials.getProperty(key, "").isBlank()) {
                    throw new IllegalArgumentException("Native worker credentials required on stdin");
                }
            }
            org.apache.kafka.connect.cli.ConnectDistributed.main(new String[]{ args[1] });
            return;
        }
        As400ConnectOwnershipProbeConnector.main(args);
    }

    public static final class Topics extends AbstractTopicNamingStrategy<TableId> {
        private String topic;

        public Topics(Properties properties) {
            super(properties);
        }

        @Override
        public void configure(Properties properties) {
            super.configure(properties);
            topic = properties.getProperty("probe.topic");
        }

        @Override
        public String dataChangeTopic(TableId table) {
            return topic + "-" + table.table();
        }

        @Override
        public String heartbeatTopic() {
            return topic;
        }

        @Override
        public String transactionTopic() {
            return topic + "-tx";
        }
    }

    public static final class ContinuousInterceptor extends As400ContinuousConnectProbeConnector.ContinuousInterceptor {
    }

    public static class NativeTask extends As400ConnectorTask {
        private String generation;
        private As400NativeTaskSession session;
        private As400TaskRecovery admittedRecovery;
        private Configuration admissionConfig;
        private int polled;
        private List<SourceRecord> waiting = List.of();

        @Override
        As400TaskInitialization initializationForTesting(Configuration config) {
            admissionConfig = config;
            generation = System.getenv("PROBE_WORKER") + "-" + UUID.randomUUID();
            try {
                if (credentials == null || System.getenv("PROBE_WORKER") == null) {
                    throw new IllegalStateException("Explicit native worker launcher required");
                }
                var loaded = context.offsetStorageReader().offset(new As400Partition("test").getSourcePartition());
                write(generation + ".start.json", Map.of("generation", generation, "loaded", loaded == null ? Map.of() : loaded));
                session = nativeSession(config, generation, loaded,
                        evidence -> new As400ContinuousConnectSource(credentials, config.getString("probe.library"), evidence,
                                "broker:9092", "history-" + config.getString("name")));
                As400TaskInitialization initialization = loaded == null ? initialization(session) : null;
                admittedRecovery = initialization == null ? session : initialization.recovery();
                return initialization;
            }
            catch (Exception error) {
                failed(error);
                throw new IllegalStateException("Native normal-task admission failed", error);
            }
        }

        @Override
        As400TaskRecovery recoveryForTesting(Configuration config) {
            return admittedRecovery;
        }

        As400NativeTaskSession nativeSession(Configuration config, String generation, Map<String, ?> loaded, As400NativeTaskSession.Factory factory) throws Exception {
            return new As400NativeTaskSession(config, CONTROL, Path.of("/worker.properties"), generation, loaded, factory);
        }

        As400TaskInitialization initialization(As400NativeTaskSession source) throws Exception {
            return source;
        }

        @Override
        protected List<SourceRecord> doPoll() throws InterruptedException {
            try {
                var failure = getErrorHandler().orElseThrow().getProducerThrowable();
                if (failure != null) {
                    throw new IllegalStateException("Native normal-task failed", failure);
                }
                if (session.stopped.get()) {
                    waiting = List.of();
                    return List.of();
                }
                // A fault gate may retain a previously polled record. Keep the
                // same local admission check that real task poll would perform.
                admittedRecovery.checkQualified(admissionConfig);
                boolean retained = !waiting.isEmpty();
                if (waiting.isEmpty()) {
                    waiting = super.doPoll();
                    for (var record : waiting) {
                        // This actual queued barrier exists only after immutable
                        // history readback. Observe it before releasing its poll.
                        if (record.topic().endsWith(".initialization") && "INITIAL".equals(record.value())) {
                            write(generation + ".history-durable.json", Map.of("checkpoint", record.sourceOffset()));
                        }
                    }
                }
                Path permit = CONTROL.resolve(generation + ".permit");
                if (!waiting.isEmpty() && Files.exists(permit) && Integer.parseInt(Files.readString(permit).trim()) > polled) {
                    if (retained) {
                        checkRecoveryPublication();
                    }
                    var records = waiting;
                    waiting = List.of();
                    for (var record : records) {
                        String op = record.value() instanceof Struct value && value.schema().field("op") != null ? value.getString("op") : "marker";
                        record.headers().addString("probe", JSON.writeValueAsString(Map.of("op", op, "generation", generation, "index", polled++)));
                    }
                    return records;
                }
                Thread.sleep(20);
                return List.of();
            }
            catch (Exception error) {
                failed(error);
                if (error instanceof InterruptedException interrupted) {
                    throw interrupted;
                }
                throw new IllegalStateException(error);
            }
        }

        @Override
        public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
            try {
                super.commitRecord(record, metadata);
                append(generation, "commitRecord", Map.of("offset", record.sourceOffset(), "topic", record.topic()));
            }
            catch (RuntimeException error) {
                failed(error);
                throw error;
            }
        }

        @Override
        public void commit() throws InterruptedException {
            try {
                super.commit();
                append(generation, "commit", Map.of());
            }
            catch (RuntimeException error) {
                failed(error);
                throw error;
            }
        }

        private void failed(Throwable error) {
            StringBuilder detail = new StringBuilder();
            for (Throwable cause = error; cause != null; cause = cause.getCause()) {
                detail.append(cause).append("; ");
            }
            String safe = detail.toString();
            if (credentials != null) {
                for (String key : credentials.stringPropertyNames()) {
                    String value = credentials.getProperty(key);
                    if (!value.isEmpty()) {
                        safe = safe.replace(value, "[redacted]");
                    }
                }
            }
            write(generation + ".failed.json", Map.of("error", safe));
        }
    }
}
