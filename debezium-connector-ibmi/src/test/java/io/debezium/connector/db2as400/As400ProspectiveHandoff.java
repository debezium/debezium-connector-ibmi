/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.util.List;
import java.util.Set;

import io.debezium.connector.db2as400.As400OnlineCensusReplay.Entry;
import io.debezium.connector.db2as400.As400OnlineHandoff.Candidate;
import io.debezium.connector.db2as400.As400OnlineHandoff.Evidence;
import io.debezium.connector.db2as400.As400OnlineHandoff.Visibility;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.rjne0200.EntryHeaderDecoder;

/**
 * TEST ONLY. Conditional prospective-schema handoff, with complete control history
 * and an already-qualified row/schema origin supplied by the fixture. It proves
 * neither origin acquisition nor native history completeness. No durable writes.
 */
final class As400ProspectiveHandoff {
    /** This temporary refusal allows a FUTURE scheduler to keep ordinary legacy CDC moving. */
    static final class PriorEpochPending extends IllegalArgumentException {
        private final Set<BigInteger> cycles;

        PriorEpochPending(Set<BigInteger> cycles) {
            super("Prior-schema transactions remain open; continue legacy publication and defer migration");
            this.cycles = Set.copyOf(cycles);
        }

        Set<BigInteger> cycles() {
            return cycles;
        }
    }

    record Prepared(Evidence projection, Candidate candidate, Set<BigInteger> completedPriorCycles) {
        Prepared {
            completedPriorCycles = Set.copyOf(completedPriorCycles);
        }
    }

    static Prepared prepare(Evidence future, List<Entry> controls, JournalPosition schemaOrigin, JournalPosition cut,
                            As400ConnectorConfig config)
            throws Exception {
        require(future.visibility() == Visibility.CONTROLLED_FIXTURE_ASSUMPTION, "Explicit controlled origin/completeness premise required");
        require(As400OnlineHandoff.digest(future.entries()).equals(future.historyDigest())
                && future.fingerprint().equals(As400OnlineHandoff.fingerprint(future.definitions(), config, future.historyDigest())),
                "Prospective bytes, bindings or capture policy changed");
        try {
            var result = As400ProspectiveHandoffReplay.prepare(As400OnlineHandoff.owned(future),
                    controls.stream().map(e -> new io.debezium.ibmi.db2.journal.retrieve.RawJournalPage.Entry(
                            e.position(), new EntryHeaderDecoder().decode(e.bytes(), 0), e.bytes())).toList(),
                    schemaOrigin, cut, config);
            var entries = result.projection().entries().stream().map(e -> new Entry(e.position(), e.data())).toList();
            String digest = As400OnlineHandoff.digest(entries);
            var projection = new Evidence(entries, future.definitions(), future.order(), future.journal(),
                    As400OnlineHandoff.fingerprint(future.definitions(), config, digest), entries.get(0).position(), entries.get(0).position(),
                    Set.of(), digest, future.visibility());
            var c = result.candidate().checkpoint();
            var checkpoint = new As400Checkpoint(c.journalIdentity(), projection.fingerprint(), c.replayFrom(), c.replayInclusive(), c.completedThrough(),
                    c.replayReceiverIdentity(), c.completedReceiverIdentity());
            return new Prepared(projection, new Candidate(checkpoint, result.candidate().scan(), result.candidate().openCycles()), result.completedPriorCycles());
        }
        catch (As400ProspectiveHandoffReplay.PriorEpochPending pending) {
            throw new PriorEpochPending(pending.cycles());
        }
        catch (As400HandoffReplay.SelectedPrefixPending pending) {
            throw new As400OnlineHandoff.SelectedPrefixPending(pending.cycles());
        }
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
