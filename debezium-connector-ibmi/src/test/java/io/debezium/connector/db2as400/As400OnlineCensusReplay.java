/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.Limits;
import io.debezium.document.DocumentReader;
import io.debezium.ibmi.db2.journal.retrieve.FileChangeEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.TransactionControlEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;
import io.debezium.relational.history.JsonTableChangeSerializer;

/** Offline fixture evidence only. No credentials, connections, broker publication or durable checkpoint. */
public final class As400OnlineCensusReplay {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final int MAX_BYTES = 64 * 1024 * 1024;

    record Entry(JournalPosition position, byte[] bytes) {
    }

    record FixtureContext(As400JournalOrder order, As400Checkpoint baseline) {
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Existing isolated census evidence directory required");
        }
        Path root = Path.of(args[0]);
        require(!Files.exists(root.resolve("replay.json")), "Replay evidence already exists");
        var result = new LinkedHashMap<String, Object>();
        result.put("startup_proof", false);
        try {
            JsonNode capture = json(root.resolve("oracle/capture.json"));
            require(capture.path("status").asText().equals("captured"), "Capture did not complete");
            String library = capture.path("library").asText();
            require(library.matches("DC[A-Z]{2}[0-9]{4}"), "Retained isolated fixture library required");
            var definitions = new ArrayList<As400BindingDefinition>();
            Path bindings = root.resolve("native-bindings/bindings.json");
            require(Files.size(bindings) <= 1024 * 1024, "Binding evidence exceeds bound");
            var document = DocumentReader.defaultReader().read(Files.readString(bindings));
            for (var change : new JsonTableChangeSerializer().deserialize(document.getArray("tables"), false)) {
                var definition = As400BindingDefinition.restore(change.getTable());
                require(definition.identity().tableId().schema().equals(library), "Binding belongs to another library");
                definitions.add(definition);
            }
            require(!definitions.isEmpty() && definitions.size() <= 16, "Bounded isolated fixture bindings required");
            var entries = new ArrayList<Entry>();
            require(capture.path("entries").isArray() && capture.path("entries").size() > 1
                    && capture.path("entries").size() <= As400OnlineCensusCapture.MAX_CAPTURE_ENTRIES, "Empty or excessive capture");
            long bytes = 0;
            for (JsonNode entry : capture.path("entries")) {
                Entry loaded = load(root.resolve("oracle"), entry);
                bytes += loaded.bytes().length;
                require(bytes <= MAX_BYTES, "Raw evidence exceeds replay budget");
                entries.add(loaded);
            }
            require(entries.get(0).position().equals(position(capture.get("from")))
                    && entries.get(entries.size() - 1).position().equals(position(capture.get("through"))), "Capture does not contain both exact anchors");
            anchor(root.resolve("baseline"), entries.get(0));
            anchor(root.resolve("final"), entries.get(entries.size() - 1));
            // The controller establishes this anchor before starting any writer.
            // It is not a general closed-boundary discovery or legacy bootstrap.
            result.putAll(replay(entries.subList(1, entries.size()), definitions, entries.get(0).position()));
            result.put("recovery", As400OnlineCensusRecovery.verify(entries.subList(1, entries.size()), definitions, entries.get(0).position()));
            result.put("comparison_captures", comparisonCaptures(root));
            result.put("anchor_excluded", entries.get(0).position().toString());
            result.put("bindings_sha256", digest(Files.readAllBytes(bindings)));
        }
        catch (Exception e) {
            result.put("status", "rejected");
            result.put("error", e.toString());
            throw e;
        }
        finally {
            Files.writeString(root.resolve("replay.json"), JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result), StandardOpenOption.CREATE_NEW);
        }
    }

    static Map<String, Object> replay(List<Entry> entries, List<As400BindingDefinition> definitions, JournalPosition fixtureBaseline) throws Exception {
        var bindings = new HashMap<String, TableBinding>();
        var idColumns = new HashMap<String, Integer>();
        for (var definition : definitions) {
            String key = definition.identity().tableId().schema() + "/" + definition.identity().physicalFile();
            require(bindings.putIfAbsent(key, definition.binding(1024 * 1024)) == null, "Duplicate table binding");
            var id = definition.schema().columnWithName("ID");
            require(definition.schema().columns().size() == 1 && id != null && id.jdbcType() == java.sql.Types.INTEGER && !id.isOptional()
                    && definition.schema().primaryKeyColumnNames().equals(List.of("ID")), "Fixture ID primary key required");
            idColumns.put(key, id.position() - 1);
        }
        require(!bindings.isEmpty() && entries.size() <= As400OnlineCensusCapture.MAX_CAPTURE_ENTRIES, "Missing bindings or excessive capture");
        var context = fixtureContext(entries, fixtureBaseline);
        var validated = new ArrayList<Entry>();
        try (var lifecycle = new As400JournalLifecycle(new As400JournalLifecycle.Limits(20_000, MAX_BYTES, 128, 2048, 1024, 4096),
                (file, library) -> bindings.get(library + "/" + file), context.order(), context.baseline(), true)) {
            for (Entry entry : entries) {
                lifecycle.accept(new EntryHeaderDecoder().decode(entry.bytes(), 0), entry.bytes(), 0, entry.position());
                while (lifecycle.hasReady()) {
                    var next = lifecycle.poll().orElseThrow();
                    validated.add(new Entry(next.position(), next.bytes()));
                }
            }
            lifecycle.verifyTail();
        }
        var events = new ArrayList<Map<String, Object>>();
        var batches = new ArrayList<Map<String, Object>>();
        var owners = new HashMap<BigInteger, String>();
        var limits = new Limits(128, 40_000, 2048, MAX_BYTES);
        try (var transactions = new As400TransactionBuffer("offline-fixture-candidate", limits);
                var autocommit = new As400AutocommitBuffer("offline-fixture-candidate", limits, transactions::usage)) {
            var normalizer = new As400SourceNormalizer(transactions, (file, library) -> bindings.get(library + "/" + file), 1024 * 1024,
                    As400SourceNormalizer.SavepointPolicy.consumingTargets(), autocommit);
            BigInteger previous = BigInteger.ZERO;
            for (Entry entry : validated) {
                require(entry.position().offset().compareTo(previous) > 0, "Capture repeats, resets or reorders a sequence");
                previous = entry.position().offset();
                var header = new EntryHeaderDecoder().decode(entry.bytes(), 0);
                var source = As400SourceNormalizer.validateSource(header, entry.bytes(), 0, entry.position());
                var info = source.recordInfo();
                require(info.threadId().signum() > 0 && info.jobNumber().matches("[0-9]{6}") && !info.jobNumber().equals("000000")
                        && !info.jobUser().isBlank() && !info.jobName().isBlank(), "Missing native job/thread identity");
                String job = info.jobNumber() + "/" + info.jobUser() + "/" + info.jobName();
                String owner = job + "/" + info.threadId();
                BigInteger cycle = source.commitCycle();
                if (header.getJournalCode() == 'C' && header.getEntryType().equals("SC")) {
                    require(cycle.equals(previous), "Native cycle start does not identify its own sequence");
                }
                if (cycle.signum() > 0) {
                    String prior = owners.putIfAbsent(cycle, owner);
                    require(prior == null || prior.equals(owner), "Native cycle changed producer");
                }
                var batch = normalizer.accept(header, entry.bytes(), 0, entry.position());
                autocommit.checkCapacity();
                var event = new LinkedHashMap<String, Object>();
                event.put("sequence", previous.toString());
                event.put("cycle", cycle.toString());
                event.put("job", job);
                event.put("thread", info.threadId().toString());
                event.put("type", header.getJournalCode() + "/" + header.getEntryType());
                event.put("library", header.getLibrary());
                event.put("file", header.getFile());
                if (header.getJournalCode() == 'R' && List.of("PT", "PX", "UB", "UP", "DL").contains(header.getEntryType())) {
                    String key = header.getLibrary() + "/" + header.getFile();
                    Object[] values = bindings.get(key).values(header, entry.bytes(), 0);
                    event.put("id", values[idColumns.get(key)]);
                }
                if (header.getJournalCode() == 'D') {
                    var clear = new FileChangeEntryDecoder().decode(header, entry.bytes(), 0);
                    event.put("subtype", Character.toString(clear.changeType()));
                }
                if (header.getJournalCode() == 'C') {
                    var control = new TransactionControlEntryDecoder().decode(header, entry.bytes(), 0);
                    if (control.savepointSequence() != null) {
                        event.put("target", control.savepointSequence().toString());
                    }
                }
                events.add(event);
                if (batch.isPresent()) {
                    var completed = batch.orElseThrow();
                    var changes = new ArrayList<Map<String, Object>>();
                    for (var change : completed.changes()) {
                        var row = new LinkedHashMap<String, Object>();
                        row.put("operation", change.operation().code());
                        row.put("library", change.table().tableId().schema());
                        row.put("file", change.table().physicalFile());
                        row.put("sequence", change.position().offset().toString());
                        if (change.after() != null || change.before() != null) {
                            String key = change.table().tableId().schema() + "/" + change.table().physicalFile();
                            row.put("id", (change.after() != null ? change.after() : change.before()).values()[idColumns.get(key)]);
                        }
                        changes.add(row);
                    }
                    batches.add(Map.of("sequence", previous.toString(), "cycle", cycle.toString(), "job", job, "changes", changes));
                    if (cycle.signum() == 0) {
                        autocommit.complete(completed);
                    }
                    else {
                        transactions.completeBatch(completed);
                    }
                }
            }
            require(transactions.usage().transactions() == 0 && autocommit.retainedStarts().isEmpty(), "Capture ends with unresolved transaction or row state");
        }
        return Map.of("status", "replayed", "startup_proof", false, "savepoint_policy", "consume-target-conservatively", "events", events, "batches", batches);
    }

    static FixtureContext fixtureContext(List<Entry> entries, JournalPosition fixtureBaseline) {
        // Observed ordering and stand-in incarnations are fixture preconditions;
        // neither is a native startup proof. The checkpoint is never installed.
        String candidate = "f".repeat(64);
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        var incarnations = new HashMap<JournalReceiver, String>();
        ranges.put(fixtureBaseline.receiver(), new As400JournalOrder.Range(fixtureBaseline.offset(), fixtureBaseline.offset()));
        JournalPosition previous = fixtureBaseline;
        for (Entry entry : entries) {
            require(entry.position().offset().compareTo(previous.offset()) > 0, "Capture repeats, resets or reorders a sequence");
            var range = ranges.get(entry.position().receiver());
            require(range == null || previous.receiver().equals(entry.position().receiver()), "Capture revisits an earlier receiver");
            ranges.put(entry.position().receiver(), new As400JournalOrder.Range(range == null ? entry.position().offset() : range.first(), entry.position().offset()));
            previous = entry.position();
        }
        ranges.keySet().forEach(receiver -> incarnations.put(receiver, candidate));
        var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), incarnations, ranges);
        return new FixtureContext(order, As400Checkpoint.afterVerifiedBoundary(candidate, "v1:" + candidate, fixtureBaseline, candidate));
    }

    static Entry load(Path directory, JsonNode entry) throws Exception {
        String name = entry.path("file").asText();
        require(name.matches("entry-[0-9]+\\.bin"), "Unexpected raw entry path");
        Path file = directory.resolve(name);
        require(Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS) && Files.size(file) <= 1024 * 1024, "Missing or excessive raw entry file");
        byte[] bytes = Files.readAllBytes(file);
        return load(bytes, entry);
    }

    /** Reuse the same raw-header and summary checks for embedded offline captures. */
    static Entry load(byte[] bytes, JsonNode entry) throws Exception {
        require(bytes.length <= 1024 * 1024, "Excessive raw entry bytes");
        require(digest(bytes).equals(entry.path("sha256").asText()), "Raw entry digest disagrees");
        var header = new EntryHeaderDecoder().decode(bytes, 0);
        JournalPosition position = position(entry.get("position"));
        // Auxiliary lifecycle entries have their own allowed flags. The real
        // lifecycle validator must classify them before source normalization.
        As400SourceMetadata.from(header, position);
        require((header.getJournalCode() + "/" + header.getEntryType()).equals(entry.path("type").asText())
                && header.getCommitCycle().toString().equals(entry.path("cycle").asText()), "Capture summary disagrees with native bytes");
        return new Entry(position, bytes);
    }

    private static void anchor(Path directory, Entry expected) throws Exception {
        JsonNode capture = json(directory.resolve("capture.json"));
        require(capture.path("status").asText().equals("captured") && capture.path("entries").size() == 1, "Missing exact anchor");
        Entry actual = load(directory, capture.path("entries").get(0));
        require(actual.position().equals(expected.position())
                && As400JournalCaptureContent.digest(actual.bytes(), actual.position()).equals(As400JournalCaptureContent.digest(expected.bytes(), expected.position())),
                "Anchor differs from full capture");
    }

    private static Map<String, Object> comparisonCaptures(Path root) throws Exception {
        var directories = new ArrayList<Path>();
        directories.add(root.resolve("oracle"));
        int discovered = 1;
        try (var children = Files.newDirectoryStream(root, "attempt-*-bracket")) {
            for (Path child : children) {
                require(Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS) && ++discovered <= 256, "Unbounded comparison directories");
                if (child.getFileName().toString().endsWith("-controls-bracket")) {
                    // Separate C/J discovery evidence is audited against the C
                    // projection later; it cannot be a complete CDC comparison.
                    JsonNode controls = json(child.resolve("capture.json"));
                    require(controls.path("mode").asText().equals("controls") && controls.path("status").asText().equals("controls-captured"),
                            "Incomplete or mislabeled discovery capture");
                    continue;
                }
                directories.add(child);
            }
        }
        var comparisons = new LinkedHashMap<String, Object>();
        long bytes = 0;
        int count = 0;
        for (Path directory : directories) {
            JsonNode capture = json(directory.resolve("capture.json"));
            require(capture.path("status").asText().equals("captured") && capture.path("entries").isArray(), "Incomplete comparison capture");
            var normalized = new ArrayList<Map<String, Object>>();
            for (JsonNode item : capture.path("entries")) {
                require(++count <= 100_000, "Comparison entry budget exceeded");
                Entry entry = load(directory, item); // Recheck each raw copy's original checksum and summary.
                bytes += entry.bytes().length;
                require(bytes <= 2L * MAX_BYTES, "Comparison byte budget exceeded");
                normalized.add(Map.of("position", item.get("position"), "type", item.get("type").asText(), "cycle", item.get("cycle").asText(),
                        "sha256", item.get("sha256").asText(), "content_sha256", As400JournalCaptureContent.digest(entry.bytes(), entry.position())));
            }
            comparisons.put(directory.getFileName().toString(), normalized);
        }
        return comparisons;
    }

    private static JsonNode json(Path path) throws Exception {
        require(Files.size(path) <= 16 * 1024 * 1024, "Evidence document exceeds bound");
        return JSON.readTree(path.toFile());
    }

    private static JournalPosition position(JsonNode node) {
        return new JournalPosition(new BigInteger(node.path("sequence").asText()), new JournalReceiver(node.path("receiver").asText(), node.path("library").asText()));
    }

    private static String digest(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
