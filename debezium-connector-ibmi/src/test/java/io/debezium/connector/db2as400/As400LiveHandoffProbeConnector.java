/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.connector.Task;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * Explicit isolated native qualification. The controller owns boundary acquisition,
 * writers, schema and retention. The actual normal task reads the legacy prefix
 * again from IBM i, activates standard history, then reads ongoing native changes.
 * No captured transport fallback is permitted after native admission.
 */
public final class As400LiveHandoffProbeConnector extends As400OnlineHandoffProbeConnector {
    private static Properties credentials;

    public static void main(String[] args) throws Exception {
        if (args.length == 3 && args[0].equals("verify")) {
            var json = new com.fasterxml.jackson.databind.ObjectMapper();
            require(Files.size(Path.of(args[1])) <= 16 * 1024 * 1024, "Bounded native bundle required");
            var verified = As400ProspectiveReplay.verify(json.readTree(Path.of(args[1]).toFile()));
            Files.writeString(Path.of(args[2]), json.writerWithDefaultPrettyPrinter().writeValueAsString(verified), java.nio.file.StandardOpenOption.CREATE_NEW);
            return;
        }
        if (args.length == 2 && args[0].equals("worker")) {
            credentials = new Properties();
            credentials.load(System.in);
            for (String key : List.of("database.hostname", "database.user", "database.password")) {
                require(!credentials.getProperty(key, "").isBlank(), "Native credentials required on stdin");
            }
            org.apache.kafka.connect.cli.ConnectDistributed.main(new String[]{ args[1] });
            return;
        }
        As400ConnectOwnershipProbeConnector.main(args);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LiveTask.class;
    }

    public static final class LiveTask extends HandoffTask {
        private final Object handles = new Object();
        private final AtomicBoolean terminal = new AtomicBoolean();
        private Configuration configuration;
        private volatile As400ContinuousConnectSource source;
        private String ownedLibrary;
        private int intervals;

        @Override
        boolean liveSource() {
            return true;
        }

        @Override
        void checkSource(Configuration raw) {
            String library = raw.getString("database.schema", "");
            require(!terminal.get() && credentials != null && System.getenv("PROBE_WORKER") != null
                    && library.matches("DCNC[0-9]{4}") && "stdin-native-handoff.invalid".equals(raw.getString("database.hostname"))
                    && "true".equals(raw.getString("probe.normal.recovery")) && "true".equals(raw.getString("probe.standard.history"))
                    && "true".equals(raw.getString("probe.native.history.discovery")),
                    "Explicit owned native handoff admission required");
            try {
                if (ownedLibrary == null) {
                    require(Files.readString(CONTROL.resolve(raw.getString("name") + ".owned-fixture")).equals(library), "Owned native fixture differs");
                    ownedLibrary = library;
                }
                require(ownedLibrary.equals(library), "Owned native fixture changed");
            }
            catch (java.io.IOException error) {
                throw new IllegalStateException("Owned native fixture unavailable", error);
            }
            configuration = raw;
        }

        private As400ContinuousConnectSource source() throws Exception {
            synchronized (handles) {
                require(!terminal.get(), "Native handoff stopped");
                if (source == null) {
                    Path evidence = CONTROL.resolve("live-reader-" + UUID.randomUUID());
                    Files.createDirectory(evidence);
                    source = new As400ContinuousConnectSource(credentials, configuration.getString("database.schema"), evidence,
                            "broker:9092", "bindings-" + configuration.getString("name"));
                }
            }
            try (var connection = source.connection(false)) {
                require(!terminal.get(), "Native handoff stopped during sign-on");
            }
            return source;
        }

        @Override
        List<Entry> readLegacyEntries(As400OnlineHandoff.Evidence captured, JournalPosition from, JournalPosition through, List<Entry> expected) throws Exception {
            var nativeSource = source();
            var before = As400OnlineNativeEvidence.collect(nativeSource.journals, nativeSource.identities, from);
            var requests = captured.definitions().stream().map(d -> new As400OperatorTableEvidence.Request(nativeSource.journal,
                    d.identity().tableId().schema(), d.identity().physicalFile())).toList();
            var acquired = nativeSource.acquisition.collect(requests);
            require(acquired.definitions().equals(captured.definitions()), "Live bindings differ from the audited boundary");
            var page = nativeSource.guardedReader.readRawPage(from, through, 1000);
            require(page.continuation().isEmpty(), "Native legacy prefix exceeds the bounded qualification interval");
            var after = As400OnlineNativeEvidence.collect(nativeSource.journals, nativeSource.identities, from);
            var window = new As400OnlineNativeEvidence.Window(from, through, before, after);
            require(window.journalToken().equals(captured.journal())
                    && window.receiverTokens().get(from.receiver()).equals(captured.order().incarnation(from))
                    && window.receiverTokens().get(through.receiver()).equals(captured.order().incarnation(through)), "Native prefix identity changed");
            var nativeEntries = page.entries().stream().map(e -> new Entry(e.position(), e.data())).toList();
            var pinned = captured.entries().stream().filter(e -> captured.order().compare(e.position(), from) >= 0
                    && captured.order().compare(e.position(), through) <= 0).toList();
            require(content(nativeEntries).equals(content(pinned)), "Native legacy prefix differs from complete audited journal history");
            var selected = nativeEntries.stream().filter(e -> captured.order().compare(e.position(), from) > 0
                    && LEGACY_TYPES.contains(new EntryHeaderDecoder().decode(e.bytes(), 0).getEntryType())).toList();
            require(content(selected).equals(content(expected)), "Native legacy dispatch selection differs");
            nativeSource.save("live-legacy-prefix.json", Map.of("native_identity", window.report(), "records", nativeEntries.stream()
                    .map(e -> Map.of("sequence", e.position().offset().toString(), "hex", java.util.HexFormat.of().formatHex(e.bytes()))).toList()));
            return selected;
        }

        private static List<String> content(List<Entry> entries) throws Exception {
            var digests = new ArrayList<String>();
            for (var entry : entries) {
                digests.add(As400JournalCaptureContent.digest(entry.bytes(), entry.position()));
            }
            return digests;
        }

        @Override
        NativeRuntime nativeRuntime(Configuration raw, String generation, As400BindingHistory.Recovered history) throws Exception {
            var nativeSource = source();
            var reader = new As400OperatorCutReader(nativeSource.journals, nativeSource.identities, nativeSource.journal,
                    history, nativeSource.guardedReader, nativeSource.cutLimits);
            write(generation + ".live-reader.json", Map.of("history", history.fingerprint(), "evidence", nativeSource.evidence.toString()));
            return new NativeRuntime() {
                @Override
                public As400OperatorCutReader.Cut read(Map<String, ?> durable, Optional<RawJournalPage.Entry> scan) throws Exception {
                    checkSource(raw);
                    require(++intervals <= 1000, "Native qualification read limit exceeded");
                    // Fixed bounded polling; no controller-selected cuts and no
                    // catalogue query for every output record.
                    Thread.sleep(250);
                    var cut = reader.readPrefix(durable, scan.orElse(null));
                    checkSource(raw);
                    write(generation + ".native-progress.json", Map.of("durable", durable, "scan", scan.map(e -> e.position().offset().toString()).orElse(""),
                            "tail", cut.tail().offset().toString(), "entries", cut.entries().size(), "interval", intervals));
                    return cut;
                }

                @Override
                public void requestStop() {
                    stopNativeSource();
                }

                @Override
                public void close() {
                    closeNativeSource();
                }
            };
        }

        @Override
        void stopNativeSource() {
            terminal.set(true);
            synchronized (handles) {
                if (source != null) {
                    source.lifecycle.requestStop();
                    source.guardedReader.cancelJob();
                }
            }
        }

        @Override
        void closeNativeSource() {
            stopNativeSource();
            if (source != null) {
                source.close();
            }
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
