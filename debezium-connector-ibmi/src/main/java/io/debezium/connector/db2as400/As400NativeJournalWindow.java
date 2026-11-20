/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Candidates;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Kind;
import io.debezium.connector.db2as400.As400JdbcNativeIdentityEvidence.Target;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Observation;
import io.debezium.connector.db2as400.As400OperatorJournalEvidence.Receiver;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/**
 * Typed native identity and receiver continuity observations. Matching samples
 * bind an interval to retained objects; they do not prove historical schemas,
 * transaction completeness, maintenance exclusion or publication acknowledgement.
 */
final class As400NativeJournalWindow {
    record Sample(Observation directory, Candidates identities) {
    }

    record Window(JournalPosition from, JournalPosition through, Sample before, Sample after) {
        Window {
            require(before.directory().journal().equals(after.directory().journal()), "Journal changed during capture");
            stable(before.directory().attached().equals(after.directory().attached()), "Journal attachment changed during capture");
            var chain = As400OperatorCutReader.chain(before.directory(), from.receiver());
            var recheck = As400OperatorCutReader.chain(after.directory(), from.receiver());
            stable(chain.size() == recheck.size(), "Receiver chain changed during capture");
            require(before.identities().objects().keySet().equals(java.util.Set.copyOf(targets(before.directory(), from)))
                    && before.identities().equals(after.identities()), "Missing, extra or changed native object identity");
            for (int i = 0; i < chain.size(); i++) {
                Receiver a = chain.get(i), b = recheck.get(i);
                require(a.name().equals(b.name()) && a.first().equals(b.first()) && a.previous().equals(b.previous()) && a.next().equals(b.next())
                        && sameReadableStatus(a.status(), b.status()) && a.minimizeFiles().equals(b.minimizeFiles()) && a.job().equals(b.job())
                        && a.thread().equals(b.thread()) && a.luw().equals(b.luw()) && a.xid().equals(b.xid())
                        && (a.status().equals("ATTACHED") ? b.last().compareTo(a.last()) >= 0 : a.last().equals(b.last())),
                        "Previously observed receiver content or profile changed");
            }
            // Both endpoints must have existed before the fixed-cut read began.
            int first = index(chain, from), last = index(chain, through);
            require(first <= last && (first != last || from.offset().compareTo(through.offset()) <= 0), "Reversed native interval");
        }

        String journalToken() {
            Target target = target(before.directory().journal(), Kind.JOURNAL);
            return As400OperatorCutReader.token(target, before.identities().objects().get(target));
        }

        Map<JournalReceiver, String> receiverTokens() {
            var tokens = new LinkedHashMap<JournalReceiver, String>();
            for (Receiver receiver : As400OperatorCutReader.chain(before.directory(), from.receiver())) {
                Target target = target(receiver.name(), Kind.RECEIVER);
                tokens.put(receiver.name(), As400OperatorCutReader.token(target, before.identities().objects().get(target)));
            }
            return Map.copyOf(tokens);
        }

    }

    static Sample collect(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities, JournalPosition from) throws Exception {
        Observation directory = journals.collectFrom(from.receiver());
        return new Sample(directory, identities.collect(targets(directory, from)));
    }

    static Sample collectDirectory(As400OperatorJournalEvidence journals, As400JdbcNativeIdentityEvidence identities) throws Exception {
        Observation directory = journals.collect();
        return new Sample(directory, identities.collect(directoryTargets(directory)));
    }

    static void requireStableDirectory(Sample before, Sample after) {
        require(before.directory().journal().equals(after.directory().journal()), "Native journal changed");
        stable(before.directory().attached().equals(after.directory().attached())
                && before.directory().receivers().size() == after.directory().receivers().size(), "Native directory attachment or membership changed");
        require(before.identities().objects().keySet().equals(java.util.Set.copyOf(directoryTargets(before.directory())))
                && after.identities().objects().keySet().equals(java.util.Set.copyOf(directoryTargets(after.directory())))
                && before.identities().equals(after.identities()), "Missing or changed directory object identity");
        var later = new java.util.HashMap<JournalReceiver, Receiver>();
        after.directory().receivers().forEach(r -> require(later.putIfAbsent(r.name(), r) == null, "Duplicate directory receiver"));
        for (Receiver earlier : before.directory().receivers()) {
            Receiver next = later.remove(earlier.name());
            require(next != null, "Directory receiver disappeared");
            require(sameReadableStatus(earlier.status(), next.status()), "Receiver became unavailable");
            Receiver comparable = new Receiver(next.name(), earlier.status(), next.first(), earlier.last(), next.previous(), next.next(),
                    next.minimizeFiles(), next.job(), next.thread(), next.luw(), next.xid());
            require(earlier.equals(comparable) && (earlier.status().equals("ATTACHED") ? next.last().compareTo(earlier.last()) >= 0
                    : next.last().equals(earlier.last())), "Native receiver range, links or recording profile changed");
        }
    }

    private static List<Target> directoryTargets(Observation directory) {
        require(!directory.receivers().isEmpty(), "Complete directory required");
        var targets = new ArrayList<Target>();
        targets.add(target(directory.journal(), Kind.JOURNAL));
        directory.receivers().forEach(r -> targets.add(target(r.name(), Kind.RECEIVER)));
        require(java.util.Set.copyOf(targets).size() == targets.size(), "Duplicate native directory target");
        return List.copyOf(targets);
    }

    static List<Target> targets(Observation directory, JournalPosition from) {
        var chain = As400OperatorCutReader.chain(directory, from.receiver());
        var result = new ArrayList<Target>();
        result.add(target(directory.journal(), Kind.JOURNAL));
        chain.forEach(r -> result.add(target(r.name(), Kind.RECEIVER)));
        return List.copyOf(result);
    }

    static Target target(JournalReceiver name, Kind kind) {
        return new Target("/QSYS.LIB/" + name.library() + ".LIB/" + name.name() + (kind == Kind.JOURNAL ? ".JRN" : ".JRNRCV"), kind);
    }

    private static int index(List<Receiver> chain, JournalPosition position) {
        for (int i = 0; i < chain.size(); i++) {
            Receiver receiver = chain.get(i);
            if (receiver.name().equals(position.receiver())) {
                require(position.offset().compareTo(receiver.first()) >= 0 && position.offset().compareTo(receiver.last()) <= 0,
                        "Native anchor is outside its observed receiver");
                return i;
            }
        }
        throw new IllegalArgumentException("Native anchor receiver absent");
    }

    static boolean sameReadableStatus(String first, String second) {
        return first.equals(second) || java.util.Set.of("ONLINE", "SAVED").contains(first)
                && java.util.Set.of("ONLINE", "SAVED").contains(second);
    }

    private static void stable(boolean condition, String reason) {
        if (!condition) {
            throw new As400OperatorCutReader.UnstableCutException(reason);
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
