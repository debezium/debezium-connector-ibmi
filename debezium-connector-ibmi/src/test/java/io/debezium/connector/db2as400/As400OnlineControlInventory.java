/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;

import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * TEST ONLY. Reconstructs the C-control projection of an explicitly bounded C/J
 * history. It cannot establish row, schema, lifecycle or publication completeness.
 * The caller must separately establish the receiver-pending observation and the
 * complete read. J entries are retained as unresolved maintenance evidence; they
 * are never interpreted as transaction boundaries or discarded as harmless.
 */
final class As400OnlineControlInventory {
    private static final int MAX_ENTRIES = 20_000;
    private static final long MAX_BYTES = 64 * 1024 * 1024;

    record Producer(String system, String job, String user, String number, BigInteger thread) {
    }

    record Start(As400SourceMetadata source, Producer producer) {
    }

    record Maintenance(JournalPosition position, String type, String contentDigest) {
    }

    record Inventory(Map<BigInteger, Start> openAtLower, Map<BigInteger, Start> openThrough,
            Map<BigInteger, JournalPosition> terminals, Map<BigInteger, JournalPosition> completedBeforeHistory, List<Maintenance> maintenance) {
        Inventory {
            openAtLower = Map.copyOf(openAtLower);
            openThrough = Map.copyOf(openThrough);
            terminals = Map.copyOf(terminals);
            completedBeforeHistory = Map.copyOf(completedBeforeHistory);
            maintenance = List.copyOf(maintenance);
        }
    }

    /** Join separately captured discovery to the complete row fixture; never promote either to startup proof. */
    static Inventory load(JsonNode discovery, List<Entry> complete, As400OnlineNativeEvidence.Window completeWindow,
                          JournalPosition lower, JournalPosition upper)
            throws Exception {
        require(discovery.path("profile").asText().equals("receiver-history-candidate-v1")
                && discovery.path("startup_proof").isBoolean() && !discovery.path("startup_proof").asBoolean()
                && completeWindow != null, "Explicit unqualified native discovery required");
        var window = As400OnlineNativeEvidence.restore(discovery.path("native_identity_evidence"));
        var before = As400OnlineNativeEvidence.restoreSample(discovery.path("directory_before"));
        var after = As400OnlineNativeEvidence.restoreSample(discovery.path("directory_after"));
        As400OnlineNativeEvidence.requireStableDirectory(before, after);
        require(window.through().equals(upper) && As400OnlineHandoff.position(discovery.path("lower")).equals(lower),
                "Discovery and publication observation brackets differ");
        require(before.identities().objects().entrySet().containsAll(window.before().identities().objects().entrySet())
                && after.identities().objects().entrySet().containsAll(window.after().identities().objects().entrySet()),
                "Discovery history identity differs from pending-receiver observation");
        require(completeWindow.journalToken().equals(window.journalToken())
                && completeWindow.receiverTokens().entrySet().containsAll(window.receiverTokens().entrySet()),
                "Row and control history have different native identities");
        require(discovery.path("entries").isArray() && discovery.path("entries").size() <= MAX_ENTRIES, "Bounded discovery entries required");
        var history = new ArrayList<Entry>();
        long bytes = 0;
        for (var item : discovery.path("entries")) {
            byte[] data = Base64.getDecoder().decode(item.path("raw").asText());
            require((bytes += data.length) <= MAX_BYTES && As400OnlineHandoff.hash(data).equals(item.path("sha256").asText()),
                    "Changed or excessive discovery bytes");
            var entry = new Entry(As400OnlineHandoff.position(item.path("position")), data);
            As400JournalCaptureContent.digest(data, entry.position());
            history.add(entry);
        }
        require(!complete.isEmpty(), "Complete row history required independently of control discovery");
        BigInteger from = window.from().offset().max(complete.get(0).position().offset());
        require(controlProjection(complete, from, upper.offset()).equals(controlProjection(history, from, upper.offset())),
                "C-control projection differs from complete row history");
        var inventory = reconstruct(history, window, lower);
        for (Start start : inventory.openAtLower().values()) {
            require(complete.stream().anyMatch(e -> e.position().equals(start.source().position())),
                    "Open cycle predates independently captured row/schema history");
        }
        return inventory;
    }

    private static List<String> controlProjection(List<Entry> entries, BigInteger from, BigInteger through) throws Exception {
        var result = new ArrayList<String>();
        var headers = new EntryHeaderDecoder();
        for (Entry entry : entries) {
            if (entry.position().offset().compareTo(from) >= 0 && entry.position().offset().compareTo(through) <= 0
                    && headers.decode(entry.bytes(), 0).getJournalCode() == 'C') {
                result.add(As400JournalCaptureContent.digest(entry.bytes(), entry.position()));
            }
        }
        return List.copyOf(result);
    }

    static Inventory reconstruct(List<Entry> history, As400OnlineNativeEvidence.Window window, JournalPosition lower) throws Exception {
        require(history != null && history.size() <= MAX_ENTRIES, "Bounded C/J history required");
        var ranges = new LinkedHashMap<JournalReceiver, As400JournalOrder.Range>();
        As400OperatorCutReader.chain(window.before().directory(), window.from().receiver())
                .forEach(r -> ranges.put(r.name(), new As400JournalOrder.Range(r.first(), r.last())));
        var order = new As400JournalOrder(new ArrayList<>(ranges.keySet()), window.receiverTokens(), ranges);
        return reconstructControlledProjection(history, order, window.journalToken(), window.from(), lower, window.through());
    }

    /** Pure control-state model. A supplied range/order is NOT native completeness or startup evidence. */
    static Inventory reconstructControlledProjection(List<Entry> history, As400JournalOrder order, String journal,
                                                     JournalPosition from, JournalPosition lower, JournalPosition through)
            throws Exception {
        require(history != null && history.size() <= MAX_ENTRIES, "Bounded C/J history required");
        var entries = history.stream().map(e -> new io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry(
                e.position(), new EntryHeaderDecoder().decode(e.bytes(), 0), e.bytes())).toList();
        var result = As400ControlInventory.reconstruct(entries, order, journal, from, lower, through);
        var atLower = new LinkedHashMap<BigInteger, Start>();
        var open = new LinkedHashMap<BigInteger, Start>();
        result.openAtLower().forEach((cycle, start) -> atLower.put(cycle, start(start)));
        result.openThrough().forEach((cycle, start) -> open.put(cycle, start(start)));
        return new Inventory(atLower, open, result.terminals(), result.completedBeforeHistory(), result.maintenance().stream()
                .map(m -> new Maintenance(m.position(), m.type(), m.contentDigest())).toList());
    }

    private static Start start(As400ControlInventory.Start start) {
        var p = start.producer();
        return new Start(start.source(), new Producer(p.system(), p.job(), p.user(), p.number(), p.thread()));
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
