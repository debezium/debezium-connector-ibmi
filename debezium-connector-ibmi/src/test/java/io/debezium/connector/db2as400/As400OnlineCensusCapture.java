/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.SocketProperties;

import io.debezium.document.Document;
import io.debezium.document.DocumentWriter;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RawJournalPage;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

/** Explicit isolated test entry point. Only short-boundary writes markers and briefly allocates files. */
public final class As400OnlineCensusCapture {
    private static final ObjectMapper JSON = new ObjectMapper();
    static final int MAX_CAPTURE_ENTRIES = 20_000;

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || !args[0].matches("DCNC[0-9]{4}")
                || !List.of("preflight", "anchor", "capture", "bindings", "scope", "directory", "pending", "controls", "audit", "short-boundary").contains(args[2])) {
            throw new IllegalArgumentException(
                    "DCNCdddd new-directory preflight|anchor|bindings|scope|directory|pending|capture|controls|audit [from-anchor to-anchor response-buffer-bytes]");
        }
        String library = args[0];
        Path directory = Path.of(args[1]);
        Files.createDirectory(directory);
        Properties credentials = new Properties();
        credentials.load(System.in);
        var report = new LinkedHashMap<String, Object>();
        report.put("startup_proof", false);
        report.put("library", library);
        report.put("mode", args[2]);
        if (args[2].equals("controls")) {
            report.put("selection", "C/J controls only; no row or file-lifecycle coverage");
        }
        if (args[2].equals("audit")) {
            report.put("selection", "Native default all codes/types, no object selection; test-only audit, never startup proof");
        }
        AS400 transport = new AS400(credentials.getProperty("database.hostname"), credentials.getProperty("database.user"),
                credentials.getProperty("database.password"));
        SocketProperties sockets = new SocketProperties();
        sockets.setSoTimeout(15000);
        transport.setSocketProperties(sockets);
        transport.setThreadUsed(false);
        JournalReceiver journal = new JournalReceiver("QSQJRN", library);
        try {
            try (Connection ignored = connection(credentials)) {
                // Fail before any native observation if the configured server does not match.
            }
            if (args[2].equals("preflight")) {
                try (Connection sql = connection(credentials); var statement = sql.createStatement()) {
                    statement.setQueryTimeout(5);
                    report.put("database_version", sql.getMetaData().getDatabaseProductVersion());
                    try (var result = statement.executeQuery(As400NativeTransactionCensus.AUTHORITY_SQL)) {
                        if (!result.next()) {
                            throw new IllegalStateException("Missing authority observation");
                        }
                        report.put("authority", Map.of("JOBCTL", result.getInt("JOBCTL"), "SQLADM", result.getInt("SQLADM"), "SYSMON", result.getInt("SYSMON")));
                    }
                }
            }
            else if (args[2].equals("directory") || args[2].equals("pending")) {
                var journals = new As400OperatorJournalEvidence(() -> connection(credentials), journal, new As400OperatorJournalEvidence.Limits(16, 5));
                var identities = new As400JdbcNativeIdentityEvidence(() -> connection(credentials), new As400JdbcNativeIdentityEvidence.Limits(17, 5, 1024));
                var before = As400OnlineNativeEvidence.collectDirectory(journals, identities);
                report.put("directory_evidence", As400OnlineNativeEvidence.sample(before));
                if (args[2].equals("pending")) {
                    var pending = new As400PendingReceiverEvidence(() -> connection(credentials), journal, 16, 5).collect();
                    report.put("pending_receivers", As400NativeHistoryDiscoveryProbe.pending(pending));
                    var after = As400OnlineNativeEvidence.collectDirectory(journals, identities);
                    report.put("directory_after", As400OnlineNativeEvidence.sample(after));
                    As400OnlineNativeEvidence.requireStableDirectory(before, after);
                    pending.verifyWindow(before.directory(), after.directory());
                }
            }
            else if (args[2].equals("scope")) {
                As400JournalFileScope.Scope scope;
                try (var sql = connection(credentials, false)) {
                    scope = new As400JournalFileScope(journal, 5).collect(sql);
                }
                var limits = new As400OperatorTableEvidence.Limits(50, 5);
                var tables = new As400OperatorTableEvidence(() -> connection(credentials), limits);
                var acquired = new As400NativeBindingAcquisition(() -> connection(credentials), tables, limits).collect(scope.files());
                report.put("scope", scope.files().stream().map(r -> Map.of("library", r.library(), "file", r.file())).toList());
                report.put("journaled_libraries", scope.journaledLibraries());
                report.put("targets", acquired.observations().stream().map(o -> Map.of("library", o.request().library(), "file", o.request().file(),
                        "member", o.member())).toList());
                report.put("acquired_binding_count", acquired.definitions().size());
                try (var sql = connection(credentials, false)) {
                    if (!scope.equals(new As400JournalFileScope(journal, 5).collect(sql))) {
                        throw new IllegalArgumentException("Native journal membership changed during acquisition");
                    }
                }
            }
            else if (args[2].equals("short-boundary")) {
                var limits = new As400OperatorTableEvidence.Limits(50, 5);
                var acquisition = new As400NativeBindingAcquisition(() -> connection(credentials, false),
                        new As400OperatorTableEvidence(() -> connection(credentials, false), limits), limits);
                var reader = reader(transport, new JournalInfo(journal.name(), library, false), new JournalInfoRetrieval(0, 0, 0), 1024 * 1024);
                // This mode includes metadata, the pulse and a separate retained
                // control read. Per-query and held-interval limits stay unchanged.
                long deadline = System.nanoTime() + 100_000_000_000L;
                var collector = new As400NativeBoundaryCollector(journal, () -> connection(credentials, false), acquisition, reader, () -> {
                    if (Thread.currentThread().isInterrupted() || System.nanoTime() >= deadline) {
                        throw new IllegalStateException("Bounded native attempt stopped");
                    }
                });
                var candidate = collector.collect();
                report.put("candidate_present", candidate.isPresent());
                if (candidate.isPresent()) {
                    var c = candidate.orElseThrow();
                    report.put("program", Map.of("nonce", c.program().nonce(), "scope_digest", c.program().scopeDigest(), "targets", c.program().targets()));
                    Files.writeString(directory.resolve("statement.sql"), c.program().statement());
                    report.put("completion", Map.of("job", c.completion().job(), "thread", c.completion().thread().toString(),
                            "acquired", c.completion().acquired().toString(), "released", c.completion().released().toString()));
                    report.put("origin", position(c.origin()));
                    report.put("history_floor", position(c.historyFloor()));
                    report.put("pending", As400NativeHistoryDiscoveryProbe.pending(c.pending()));
                    report.put("pending_before",
                            As400OnlineNativeEvidence.sample(new As400OnlineNativeEvidence.Sample(c.pendingBefore().directory(), c.pendingBefore().identities())));
                    report.put("pending_after",
                            As400OnlineNativeEvidence.sample(new As400OnlineNativeEvidence.Sample(c.pendingAfter().directory(), c.pendingAfter().identities())));
                    report.put("native_identity_evidence", new As400OnlineNativeEvidence.Window(c.window().from(), c.window().through(),
                            new As400OnlineNativeEvidence.Sample(c.window().before().directory(), c.window().before().identities()),
                            new As400OnlineNativeEvidence.Sample(c.window().after().directory(), c.window().after().identities())).report());
                    var changes = new TableChanges();
                    c.bindings().definitions().forEach(d -> changes.create(d.storedTable()));
                    var bindings = Document.create();
                    bindings.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
                    Files.writeString(directory.resolve("bindings.json"), DocumentWriter.defaultWriter().write(bindings));
                    var entries = new ArrayList<Map<String, Object>>();
                    for (var entry : c.audit().entries()) {
                        entries.add(Map.of("position", position(entry.position()), "raw", java.util.Base64.getEncoder().encodeToString(entry.data()),
                                "sha256", As400OnlineHandoff.hash(entry.data()), "type", entry.header().getJournalCode() + "/" + entry.header().getEntryType()));
                    }
                    report.put("entries", entries);
                    var journals = new As400OperatorJournalEvidence(() -> connection(credentials), journal, new As400OperatorJournalEvidence.Limits(16, 5));
                    var identities = new As400JdbcNativeIdentityEvidence(() -> connection(credentials), new As400JdbcNativeIdentityEvidence.Limits(17, 5, 1024));
                    var history = new As400NativeHandoffHistory(journals, identities, reader, () -> {
                        if (Thread.currentThread().isInterrupted() || System.nanoTime() >= deadline) {
                            throw new IllegalStateException("Bounded native control discovery stopped");
                        }
                    });
                    var controls = history.collect(c, c.window().through());
                    var controlEntries = new ArrayList<Map<String, Object>>();
                    for (var entry : controls.entries()) {
                        controlEntries.add(Map.of("position", position(entry.position()), "raw", java.util.Base64.getEncoder().encodeToString(entry.data()),
                                "sha256", As400OnlineHandoff.hash(entry.data()), "type", entry.header().getJournalCode() + "/" + entry.header().getEntryType()));
                    }
                    report.put("controls", controlEntries);
                    report.put("control_inventory", Map.of("open_at_origin", controls.inventory().openAtLower().keySet().stream().map(Object::toString).toList(),
                            "open_through", controls.inventory().openThrough().keySet().stream().map(Object::toString).toList(),
                            "terminal_count", controls.inventory().terminals().size(), "maintenance_count", controls.inventory().maintenance().size()));
                    var cut = c.audit().entries().stream().filter(e -> e.header().getJournalCode() == 'R' && e.header().getEntryType().equals("PT")
                            && e.header().getCommitCycle().signum() == 0 && e.position().offset().compareTo(c.origin().offset()) > 0)
                            .map(RawJournalPage.Entry::position).max(java.util.Comparator.comparing(JournalPosition::offset)).orElseThrow();
                    var config = new As400ConnectorConfig(io.debezium.config.Configuration.create().with("topic.prefix", "test")
                            .with("database.dbname", credentials.getProperty("database.dbname")).with("database.schema", library)
                            .with("skipped.operations", "none").with("provide.transaction.metadata", false).build());
                    var policy = new As400CaptureFingerprint.Policy(Map.of(), java.util.Set.of(), Map.of("version", "strict-v1"),
                            Map.of("version", "native-v1"), Map.of("version", "test-v1"), false, true, Map.of("server", "test"));
                    var prepared = history.prepare(c, cut, config, policy);
                    report.put("prepared_replay", Map.of("checkpoint", prepared.replay().candidate().checkpoint().toOffset(),
                            "scan", position(prepared.replay().candidate().scan()), "publication_acknowledged", false,
                            "completed_prior_cycles", prepared.replay().completedPriorCycles().stream().map(Object::toString).toList(),
                            "row_count", prepared.replay().projection().entries().size()));
                }
            }
            else if (args[2].equals("bindings")) {
                var identities = new As400JdbcNativeIdentityEvidence(() -> connection(credentials), new As400JdbcNativeIdentityEvidence.Limits(1, 5, 1024));
                var target = As400OnlineNativeEvidence.target(journal, As400JdbcNativeIdentityEvidence.Kind.JOURNAL);
                var before = identities.collect(List.of(target));
                var info = new JournalInfoRetrieval(0, 0, 0);
                var journalInfo = new JournalInfo(journal.name(), library, false);
                var from = info.getCurrentPosition(transport, journalInfo);
                var limits = new As400OperatorTableEvidence.Limits(4, 5);
                var tables = new As400OperatorTableEvidence(() -> connection(credentials), limits);
                var acquired = new As400NativeBindingAcquisition(() -> connection(credentials), tables, limits)
                        .collect(List.of("A", "B", "M", "U").stream().map(name -> new As400OperatorTableEvidence.Request(journal, library, name)).toList());
                var changes = new TableChanges();
                acquired.definitions().forEach(definition -> changes.create(definition.storedTable()));
                Document document = Document.create();
                document.setArray("tables", new JsonTableChangeSerializer().serialize(changes));
                String serialized = DocumentWriter.defaultWriter().write(document);
                Files.writeString(directory.resolve("bindings.json"), serialized);
                var through = info.getCurrentPosition(transport, journalInfo);
                if (!before.equals(identities.collect(List.of(target)))) {
                    throw new IllegalStateException("Journal changed during metadata acquisition");
                }
                report.put("binding_provenance", Map.of("profile", "current-metadata-only-v1", "historical_schema_proof", false,
                        "from", position(from), "through", position(through), "journal_identity", As400OperatorCutReader.token(target, before.objects().get(target)),
                        "bindings_sha256", As400OnlineHandoff.hash(serialized.getBytes(java.nio.charset.StandardCharsets.UTF_8))));
                report.put("observations", acquired.observations().stream().map(Object::toString).toList());
            }
            else {
                var info = new JournalInfoRetrieval(0, 0, 0);
                var journalInfo = new JournalInfo(journal.name(), library, false);
                JournalPosition from;
                JournalPosition through;
                int bufferBytes;
                if (args[2].equals("anchor")) {
                    from = info.getCurrentPosition(transport, journalInfo);
                    through = from;
                    bufferBytes = 1024 * 1024;
                }
                else {
                    if (args.length != 6) {
                        throw new IllegalArgumentException("Explicit start/end anchors and response buffer bound required");
                    }
                    from = position(JSON.readTree(Path.of(args[3]).toFile()).get("from"));
                    through = position(JSON.readTree(Path.of(args[4]).toFile()).get("through"));
                    bufferBytes = Integer.parseInt(args[5]);
                }
                var reader = reader(transport, journalInfo, info, bufferBytes);
                report.put("response_buffer_bytes", bufferBytes);
                var journals = new As400OperatorJournalEvidence(() -> connection(credentials), journal, new As400OperatorJournalEvidence.Limits(16, 5));
                var identities = new As400JdbcNativeIdentityEvidence(() -> connection(credentials), new As400JdbcNativeIdentityEvidence.Limits(17, 5, 1024));
                var nativeBefore = As400OnlineNativeEvidence.collect(journals, identities, from);
                var observation = nativeBefore.directory();
                report.put("receiver_observation", observation(observation));
                var ordered = new ArrayList<As400OperatorJournalEvidence.Receiver>();
                JournalReceiver name = from.receiver();
                while (true) {
                    JournalReceiver nextName = name;
                    var receiver = observation.receivers().stream().filter(r -> r.name().equals(nextName)).findFirst().orElseThrow();
                    receiver.requireReadableProfile();
                    if (ordered.stream().anyMatch(r -> r.name().equals(nextName))) {
                        throw new IllegalArgumentException("Receiver chain repeats");
                    }
                    if (!ordered.isEmpty() && !receiver.previous().orElseThrow().equals(ordered.get(ordered.size() - 1).name())) {
                        throw new IllegalArgumentException("Receiver links disagree");
                    }
                    ordered.add(receiver);
                    if (name.equals(through.receiver())) {
                        break;
                    }
                    name = receiver.next().orElseThrow();
                }
                report.put("from", position(from));
                report.put("through", position(through));
                var pages = new ArrayList<Map<String, Object>>();
                var entries = new ArrayList<Map<String, Object>>();
                report.put("pages", pages);
                report.put("entries", entries);
                long deadline = System.nanoTime() + 60_000_000_000L;
                for (var receiver : ordered) {
                    JournalPosition next = receiver.name().equals(from.receiver()) ? from : new JournalPosition(receiver.first(), receiver.name());
                    JournalPosition end = receiver.name().equals(through.receiver()) ? through : new JournalPosition(receiver.last(), receiver.name());
                    while (true) {
                        if (pages.size() >= 512 || entries.size() >= MAX_CAPTURE_ENTRIES || System.nanoTime() >= deadline) {
                            throw new IllegalStateException("Bounded capture exhausted");
                        }
                        RawJournalPage page;
                        if (args[2].equals("controls")) {
                            // Explicit diagnostic conversion for the common evidence writer.
                            // This capture is not full row/lifecycle coverage or publishable input.
                            var controls = reader.readControlHistoryPage(next, end, MAX_CAPTURE_ENTRIES - entries.size());
                            page = new RawJournalPage(controls.entries(), controls.continuation(), controls.bytesReturned());
                        }
                        else if (args[2].equals("audit")) {
                            var audit = reader.readAuditHistoryPage(next, end, MAX_CAPTURE_ENTRIES - entries.size());
                            page = new RawJournalPage(audit.entries(), audit.continuation(), audit.bytesReturned());
                        }
                        else {
                            page = readPage(reader, next, end, MAX_CAPTURE_ENTRIES - entries.size());
                        }
                        for (var entry : page.entries()) {
                            String file = "entry-" + entries.size() + ".bin";
                            Files.write(directory.resolve(file), entry.data());
                            var h = entry.header();
                            var item = new LinkedHashMap<String, Object>();
                            item.put("file", file);
                            item.put("position", position(entry.position()));
                            item.put("type", h.getJournalCode() + "/" + h.getEntryType());
                            item.put("cycle", h.getCommitCycle().toString());
                            item.put("table", h.getFile());
                            item.put("library", h.getLibrary());
                            item.put("member", h.getMember());
                            item.put("time", h.getTime().toString());
                            item.put("record", h.getRecordInfo());
                            if (args[2].equals("audit")) {
                                item.put("entry_specific_data_offset", h.getEntrySpecificDataOffset());
                                item.put("entry_end_offset", h.getEndOffset());
                            }
                            item.put("sha256", HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(entry.data())));
                            entries.add(item);
                        }
                        pages.add(Map.of("from", position(next), "through", position(end), "count", page.entries().size(),
                                "bytes_returned", page.bytesReturned(), "continuation", page.continuation().map(As400OnlineCensusCapture::position).orElse(Map.of())));
                        save(directory, report);
                        if (page.continuation().isEmpty()) {
                            break;
                        }
                        next = page.continuation().orElseThrow();
                    }
                }
                if (args[2].equals("anchor") && (entries.size() != 1 || !entries.get(0).get("position").equals(position(from)))) {
                    throw new IllegalStateException("Current position is not a readable exact anchor");
                }
                var nativeAfter = As400OnlineNativeEvidence.collect(journals, identities, from);
                report.put("receiver_recheck", observation(nativeAfter.directory()));
                report.put("native_identity_evidence", new As400OnlineNativeEvidence.Window(from, through, nativeBefore, nativeAfter).report());
            }
            report.put("status", args[2].equals("audit") ? "audit-captured-unqualified" : args[2].equals("controls") ? "controls-captured" : "captured");
        }
        catch (Exception e) {
            report.put("status", "rejected");
            report.put("error", e.toString());
            if (e instanceof As400NativeBoundaryCollector.AuditRejected rejected) {
                report.put("audit_rejection", rejected.getCause().toString());
                report.put("from", position(rejected.window.from()));
                report.put("through", position(rejected.window.through()));
                report.put("raw", rejected.page.entries().stream().map(entry -> Map.of("position", position(entry.position()),
                        "raw", java.util.Base64.getEncoder().encodeToString(entry.data()))).toList());
            }
            throw e;
        }
        finally {
            transport.disconnectAllServices();
            report.put("transport_disconnected", !transport.isConnected());
            save(directory, report);
        }
    }

    static RetrieveJournal reader(AS400 transport, JournalInfo journal, JournalInfoRetrieval info, int bufferBytes) {
        if (bufferBytes < 4096 || bufferBytes > 1024 * 1024) {
            throw new IllegalArgumentException("Response buffer must be 4096..1048576 bytes");
        }
        // Native buffer exhaustion supplies continuation. NBRENT would complete
        // a count-limited selection without proving this fixed cut was exhausted.
        return new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> transport).withJournalInfo(journal)
                .withTransactionControls(true).withServerFiltering(false).withJournalBufferSize(bufferBytes).build(), info);
    }

    static RawJournalPage readPage(RetrieveJournal reader, JournalPosition from, JournalPosition through, int remainingEntries) throws Exception {
        if (remainingEntries <= 0 || remainingEntries > MAX_CAPTURE_ENTRIES) {
            throw new IllegalArgumentException("Capture entry budget exhausted or invalid");
        }
        // The decoder returns this prefix and a continuation even when the
        // native response includes more entries; it still validates the tail.
        return reader.readRawPage(from, through, remainingEntries);
    }

    private static Connection connection(Properties credentials) throws SQLException {
        return connection(credentials, true);
    }

    private static Connection connection(Properties credentials, boolean readOnly) throws SQLException {
        Properties jdbc = new Properties();
        jdbc.setProperty("user", credentials.getProperty("database.user"));
        jdbc.setProperty("password", credentials.getProperty("database.password"));
        jdbc.setProperty("prompt", "false");
        jdbc.setProperty("socket timeout", "15000");
        jdbc.setProperty("thread used", "false");
        jdbc.setProperty("lazy close", "false");
        Connection connection = DriverManager.getConnection("jdbc:as400://" + credentials.getProperty("database.hostname"), jdbc);
        try {
            connection.setTransactionIsolation(Connection.TRANSACTION_NONE);
            connection.setReadOnly(readOnly);
            try (var statement = connection.createStatement()) {
                statement.setQueryTimeout(5);
                try (var rows = statement.executeQuery("SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")) {
                    if (!rows.next() || !rows.getString(1).trim().equals(credentials.getProperty("database.dbname")) || rows.next()) {
                        throw new IllegalStateException("Unexpected native server");
                    }
                }
            }
            return connection;
        }
        catch (Exception e) {
            connection.close();
            throw e;
        }
    }

    private static Map<String, String> position(JournalPosition position) {
        return Map.of("sequence", position.offset().toString(), "receiver", position.receiver().name(), "library", position.receiver().library());
    }

    private static List<Map<String, Object>> observation(As400OperatorJournalEvidence.Observation observation) {
        return observation.receivers().stream().map(receiver -> {
            var fields = new LinkedHashMap<String, Object>();
            fields.put("name", receiver.name().name());
            fields.put("library", receiver.name().library());
            fields.put("status", receiver.status());
            fields.put("first", receiver.first().toString());
            fields.put("last", receiver.last().toString());
            fields.put("previous", receiver.previous().map(r -> r.library() + "/" + r.name()).orElse(""));
            fields.put("next", receiver.next().map(r -> r.library() + "/" + r.name()).orElse(""));
            fields.put("job", receiver.job());
            fields.put("thread", receiver.thread());
            fields.put("minimize", receiver.minimizeFiles());
            fields.put("luw", receiver.luw());
            fields.put("xid", receiver.xid());
            return (Map<String, Object>) fields;
        }).toList();
    }

    private static JournalPosition position(JsonNode position) {
        return new JournalPosition(new BigInteger(position.get("sequence").asText()),
                new JournalReceiver(position.get("receiver").asText(), position.get("library").asText()));
    }

    private static void save(Path directory, Map<String, Object> report) throws Exception {
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("capture.json").toFile(), report);
    }
}
