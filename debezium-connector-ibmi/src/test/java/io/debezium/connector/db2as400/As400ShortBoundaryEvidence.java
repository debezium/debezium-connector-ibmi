/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * TEST ONLY. Joins observations from the actual short-pulse collector. An accepted
 * observation is not general startup proof: operator retention, fixed-schema CDC,
 * trusted collection and the separate complete transaction history remain required.
 */
final class As400ShortBoundaryEvidence {
    private static final ObjectMapper JSON = new ObjectMapper();

    record Candidate(JournalPosition origin, JournalPosition auditedThrough, As400OnlineFenceEvidence.Candidate metadata,
            As400OnlineNativeEvidence.Window window) {
    }

    static Candidate validate(JsonNode input) throws Exception {
        require(input.path("profile").asText().equals("native-short-boundary-observation-v1")
                && input.path("startup_proof").isBoolean() && !input.path("startup_proof").asBoolean(), "Explicit test-only boundary observation required");
        var before = As400OnlineFenceEvidence.validateOffLock(input.path("before"));
        var after = As400OnlineFenceEvidence.validateOffLock(input.path("after"));
        require(before.tables().equals(after.tables()) && before.identities().equals(after.identities())
                && input.path("before").path("native_scope").equals(input.path("after").path("native_scope")),
                "Native scope, schema or identity changed across boundary");
        var request = input.path("request");
        String library = request.path("library").asText();
        String nonce = request.path("nonce").asText();
        require(library.equals(input.path("before").path("library").asText()) && nonce.matches("[A-F0-9]{32}"), "Foreign pulse request");
        var targets = new ArrayList<List<String>>();
        before.tables().observations().forEach(o -> targets.add(List.of(o.request().file(), o.member())));
        require(JSON.valueToTree(targets).equals(request.path("targets")), "Pulse does not cover complete native scope");
        String evidenceHash = hash(input.path("before"));
        require(evidenceHash.equals(request.path("evidence_sha256").asText()), "Pulse metadata digest differs");
        String scopeHash = hash(JSON.valueToTree(Map.of("journal", List.of(library, "QSQJRN"), "targets", targets,
                "evidence_sha256", evidenceHash))).toUpperCase(java.util.Locale.ROOT);
        var pulse = input.path("pulse");
        String statement = new As400ShortBoundaryProgram(new io.debezium.ibmi.db2.journal.retrieve.JournalReceiver("QSQJRN", library),
                targets.stream().map(t -> new As400ShortBoundaryProgram.Target(library, t.get(0), t.get(1))).toList(), nonce, scopeHash).statement();
        require(input.path("statement").asText().equals(statement)
                && As400OnlineHandoff.hash(statement.getBytes(StandardCharsets.US_ASCII)).equals(pulse.path("program_sha256").asText()),
                "Pulse differs from minimal allocate/marker/release program");
        require(pulse.path("response").asText().equals("success") && pulse.path("status").asText().equals("returned-and-locks-absent")
                && pulse.path("holder_drained").isBoolean() && pulse.path("holder_drained").asBoolean()
                && pulse.path("remaining_locks").isArray() && pulse.path("remaining_locks").isEmpty()
                && pulse.path("fault_after_marker").isBoolean() && !pulse.path("fault_after_marker").asBoolean()
                && pulse.path("contention_only_delay_seconds").isIntegralNumber() && pulse.path("contention_only_delay_seconds").asInt() == 0,
                "Failed, lost, delayed or undrained pulse");
        require(input.path("before").path("finished").path("monotonic_ns").canConvertToLong()
                && pulse.path("started").path("monotonic_ns").canConvertToLong()
                && pulse.path("finished").path("monotonic_ns").canConvertToLong()
                && input.path("after").path("started").path("monotonic_ns").canConvertToLong(), "Missing collector ordering");
        require(input.path("before").path("finished").path("monotonic_ns").asLong() < pulse.path("started").path("monotonic_ns").asLong()
                && pulse.path("started").path("monotonic_ns").asLong() < pulse.path("finished").path("monotonic_ns").asLong()
                && pulse.path("finished").path("monotonic_ns").asLong() < input.path("after").path("started").path("monotonic_ns").asLong(),
                "Metadata overlaps held interval or collector order changed");
        require(pulse.path("result_rows").isArray() && pulse.path("result_rows").size() == 1, "Missing or duplicate pulse completion");
        var result = pulse.path("result_rows").get(0);
        require(result.path("ATTEMPT").asText().equals(nonce) && result.path("SCOPE_DIGEST").asText().equals(scopeHash), "Foreign completion");
        var acquired = time(result.path("ACQUIRED").asText());
        var released = time(result.path("RELEASED").asText());
        var duration = Duration.between(acquired, released);
        require(!duration.isNegative() && duration.compareTo(Duration.ofSeconds(1)) <= 0, "Pulse exceeded qualified fixture hold bound");

        var interval = input.path("interval");
        require(interval.path("holder").isArray() && interval.path("holder").size() == 1, "Exact holder observation required");
        var holder = interval.path("holder").get(0);
        String job = holder.path("JOB").asText();
        String thread = holder.path("THREAD_HEX").asText();
        require(job.equals(pulse.path("job").asText()) && job.matches("[0-9]{6}/[A-Z0-9_#$@]{1,10}/[A-Z0-9_#$@]{1,10}")
                && thread.matches("[0-9A-F]{16}") && new BigInteger(thread, 16).signum() > 0, "Incomplete holder identity");
        var from = As400OnlineHandoff.position(interval.path("from"));
        var through = As400OnlineHandoff.position(interval.path("through"));
        require(from.receiver().equals(through.receiver()), "Rotation during pulse requires a new attempt");
        var capture = input.path("raw");
        require(capture.path("mode").asText().equals("audit") && capture.path("status").asText().equals("audit-captured-unqualified")
                && capture.path("transport_disconnected").asBoolean() && capture.path("entries").isArray()
                && capture.path("entries").size() >= 4 && capture.path("entries").size() <= 2000, "Complete bounded all-code capture required");
        var window = As400OnlineNativeEvidence.restore(capture.path("native_identity_evidence"));
        require(window.from().equals(from) && window.through().equals(through)
                && As400OnlineHandoff.position(capture.path("from")).equals(from) && As400OnlineHandoff.position(capture.path("through")).equals(through)
                && window.before().identities().equals(before.identities()) && window.after().identities().equals(after.identities()),
                "Audit interval or native identity differs from metadata");
        // This first assembler deliberately admits only a single complete native
        // page. More pages are retryable refusal, never a filtered/truncated audit.
        require(capture.path("pages").isArray() && capture.path("pages").size() == 1, "Single complete audit page required");
        var page = capture.path("pages").get(0);
        require(As400OnlineHandoff.position(page.path("from")).equals(from) && As400OnlineHandoff.position(page.path("through")).equals(through)
                && page.path("continuation").isObject() && page.path("continuation").isEmpty()
                && page.path("count").asInt() == capture.path("entries").size(), "Incomplete audit page");
        var entries = new ArrayList<io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry>();
        long totalBytes = 0;
        for (var item : capture.path("entries")) {
            require(item.path("raw").asText().length() <= 1_398_104, "Excessive raw audit entry");
            byte[] bytes = Base64.getDecoder().decode(item.path("raw").asText());
            require((totalBytes += bytes.length) <= 16 * 1024 * 1024, "Excessive audit bytes");
            var entry = As400OnlineCensusReplay.load(bytes, item);
            entries.add(new io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry(entry.position(), new EntryHeaderDecoder().decode(bytes, 0), bytes));
        }
        var program = new As400ShortBoundaryProgram(new io.debezium.ibmi.db2.journal.retrieve.JournalReceiver("QSQJRN", library),
                targets.stream().map(t -> new As400ShortBoundaryProgram.Target(library, t.get(0), t.get(1))).toList(), nonce, scopeHash);
        var completion = new As400ShortBoundaryAudit.Completion(job, new BigInteger(thread, 16), acquired, released);
        var origin = As400ShortBoundaryAudit.validate(program, completion, window.nativeWindow(),
                new io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal.AuditPage(entries, java.util.Optional.empty(), page.path("bytes_returned").asInt()),
                before.tables().definitions(), before.boundary(), after.boundary());
        return new Candidate(origin, through, before, window);
    }

    private static LocalDateTime time(String value) {
        return LocalDateTime.parse(value.replace(' ', 'T'));
    }

    private static String hash(JsonNode node) throws Exception {
        return As400OnlineHandoff.hash(JSON.writeValueAsBytes(canonical(node)));
    }

    private static Object canonical(JsonNode node) {
        if (node.isObject()) {
            var sorted = new TreeMap<String, Object>();
            node.properties().forEach(e -> sorted.put(e.getKey(), canonical(e.getValue())));
            return sorted;
        }
        if (node.isArray()) {
            var items = new ArrayList<>();
            node.forEach(e -> items.add(canonical(e)));
            return items;
        }
        return JSON.convertValue(node, Object.class);
    }

    private static void require(boolean valid, String message) {
        if (!valid) {
            throw new IllegalArgumentException(message);
        }
    }
}
