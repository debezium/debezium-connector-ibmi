/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceTaskContext;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/**
 * Explicit isolated test entry. The real task owns discovery, legacy progress,
 * history activation and native continuation; no controller-supplied boundary or
 * captured transport is accepted. This does not enable a public migration mode.
 */
public final class As400AutomaticHandoffProbeConnector extends As400ConnectOwnershipProbeConnector {
    private static Properties credentials;
    static final Schema CONTROL_SCHEMA = As400NativeHandoffSession.CONTROL_SCHEMA;

    public static void main(String[] args) throws Exception {
        if (args.length == 2 && args[0].equals("worker")) {
            credentials = new Properties();
            credentials.load(System.in);
            org.apache.kafka.connect.cli.ConnectDistributed.main(new String[]{ args[1] });
            return;
        }
        As400ConnectOwnershipProbeConnector.main(args);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return AutomaticTask.class;
    }

    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        // Admission happens in AutomaticTask before native handles are opened.
        // Do not inherit the synthetic source's required output topic.
        return new org.apache.kafka.common.config.ConfigDef();
    }

    @Override
    public boolean alterOffsets(Map<String, String> config, Map<Map<String, ?>, Map<String, ?>> offsets) {
        require("automatic".equals(config.get("name")) && "test".equals(config.get("topic.prefix"))
                && offsets.keySet().stream().allMatch(p -> Map.of("server", "test").equals(p)),
                "Only the isolated automatic test partition may be altered");
        return true; // Explicit stopped-connector fixture setup and fault injection only.
    }

    public static final class AutomaticTask extends As400ConnectorTask {
        private CdcSourceTaskContext<As400ConnectorConfig> nativeContext;

        @Override
        @SuppressWarnings("unchecked")
        public CdcSourceTaskContext<As400ConnectorConfig> preStart(Configuration raw) {
            nativeContext = (CdcSourceTaskContext<As400ConnectorConfig>) super.preStart(raw);
            return nativeContext;
        }

        @Override
        protected As400TaskRecovery recoveryForTesting(Configuration raw) {
            try {
                String library = raw.getString("database.schema", "");
                require(credentials != null && System.getenv("PROBE_WORKER") != null && library.matches("DCNC[0-9]{4}")
                        && "stdin-automatic-handoff.invalid".equals(raw.getString("database.hostname"))
                        && "automatic".equals(raw.getString("name"))
                        && Files.readString(Path.of("/control/automatic.owned-fixture")).equals(library), "Explicit isolated automatic task required");
                return new Session(nativeContext.getConfig(), context, nativeContext);
            }
            catch (Exception failure) {
                throw new IllegalStateException("Isolated automatic admission failed", failure);
            }
        }
    }

    static final class Session extends As400NativeHandoffSession {
        private final Path evidence;
        private final long deadline = System.nanoTime() + Duration.ofMinutes(15).toNanos();

        Session(As400ConnectorConfig config, SourceTaskContext context, CdcSourceTaskContext<As400ConnectorConfig> taskContext) throws Exception {
            super(config, context, taskContext, nativeRaw(config, context), new JournalReceiver("QSQJRN", config.getSchema()), policy(config),
                    "bindings-automatic", "events-automatic-control", As400DcgConfig.onlineCandidate(config.getConfig()).runtimeLimits,
                    config.getConfig().getBoolean("probe.source.offsets", false));
            evidence = Path.of("/control/automatic-" + System.getenv("PROBE_WORKER") + "-" + java.util.UUID.randomUUID());
            Files.createDirectory(evidence);
        }

        private static Configuration nativeRaw(As400ConnectorConfig config, SourceTaskContext context) throws Exception {
            var admission = As400DcgConfig.onlineCandidate(config.getConfig());
            if (config.getConfig().getBoolean("probe.source.offsets", false)) {
                Properties worker = new Properties();
                try (var stream = Files.newInputStream(Path.of("/worker.properties"))) {
                    worker.load(stream);
                }
                require("disabled".equals(worker.getProperty("exactly.once.source.support")) && context.transactionContext() == null,
                        "Native source-offset experiment requires a worker without source transactions");
            }
            else {
                admission.requireRuntime(context);
            }
            require(admission.journal.equals(new JournalReceiver("QSQJRN", config.getSchema()))
                    && admission.historyTopic.equals("bindings-automatic"), "Exact isolated native and history targets required");
            return config.getConfig().edit().with(Configuration.from(credentials))
                    .with("schema.history.internal.kafka.bootstrap.servers", "broker:9092")
                    .with("schema.history.internal.kafka.topic", "bindings-automatic").build();
        }

        private static As400CaptureFingerprint.Policy policy(As400ConnectorConfig config) {
            var p = As400DcgConfig.onlineCandidate(config.getConfig()).policy;
            var output = new java.util.TreeMap<>(p.output());
            output.put("control.topic", "events-automatic-control");
            return new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), output,
                    p.transactionMetadata(), p.tombstones(), p.sourcePartition());
        }

        @Override
        void checkBudget() {
            require(System.nanoTime() < deadline, "Automatic native task exceeded test budget");
        }

        @Override
        boolean activationDeferred() {
            return Files.exists(Path.of("/control/automatic.hold-pending"));
        }

        @Override
        void observe(String name, java.util.function.Supplier<Object> value) throws Exception {
            Files.writeString(evidence.resolve(name + ".json"), new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(value.get()));
            var hold = Path.of("/control/automatic.hold-phase");
            while (Files.exists(hold) && Files.readString(hold).equals(name)) {
                checkBudget();
                Thread.sleep(100);
            }
        }
    }

    static List<As400BindingDefinition> legacyCatalog(List<As400BindingDefinition> definitions, String server) {
        return As400NativeHandoffSession.legacyCatalog(definitions, server);
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
