/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 * Executable interleaving model, NOT a model of undocumented IBM i guarantees.
 * Time is a logical scheduler tick. Cycle IDs are never lock-space identities.
 */
class As400OnlineInventoryModelTest {
    private static final int L = 3;
    private static final int P = 5;
    private static final int Q = 10;

    private record Cycle(int id, String definition, int start, int terminal) {
        boolean active(int time) {
            return start <= time && time < terminal;
        }
    }

    private record Observation(Set<Integer> census, Set<Integer> interval, Set<Integer> retainedStarts, boolean complete) {
        Set<Integer> reconstruct() {
            if (!complete) {
                throw new IllegalArgumentException("Incomplete observation");
            }
            Set<Integer> union = new HashSet<>(census);
            union.addAll(interval);
            if (!retainedStarts.containsAll(union)) {
                throw new IllegalArgumentException("Required original start unavailable");
            }
            return Set.copyOf(union);
        }
    }

    private static Observation observe(List<Cycle> cycles, int lookup, boolean persistentVisible, boolean terminalVisible) {
        Set<Integer> census = new HashSet<>();
        Set<Integer> interval = new HashSet<>();
        Set<Integer> starts = new HashSet<>();
        for (Cycle c : cycles) {
            starts.add(c.id);
            if (persistentVisible && c.active(lookup)) {
                census.add(c.id);
            }
            if ((c.start > L && c.start <= Q) || (terminalVisible && c.terminal > L && c.terminal <= Q)) {
                interval.add(c.id);
            }
        }
        return new Observation(census, interval, starts, true);
    }

    @Test
    void everyInterleavingIsCoveredUnderTheTwoExplicitVisibilityAssumptions() {
        int schedules = 0;
        for (int lookup = P; lookup <= Q; lookup++) {
            for (int start = 0; start <= Q; start++) {
                for (int terminal = start + 1; terminal <= Q + 2; terminal++) {
                    Cycle c = new Cycle(1, "reusable-definition", start, terminal);
                    boolean needed = c.active(L) || (start > L && start <= Q);
                    var observed = observe(List.of(c), lookup, true, true).reconstruct();
                    if (needed) {
                        assertThat(observed).as("start=%s, terminal=%s, lookup=%s", start, terminal, lookup).contains(c.id);
                    }
                    schedules++;
                }
            }
        }
        assertThat(schedules).isEqualTo(462);
    }

    @Test
    void silentPersistentDefinitionOmissionIsAnUndetectableCounterexample() {
        Cycle old = new Cycle(1, "old", 1, Q + 2);
        var observation = observe(List.of(old), 7, false, true);
        assertThat(old.active(L)).isTrue();
        assertThat(observation.reconstruct()).isEmpty();
        // A complete-looking empty result cannot prove absence; native evidence is required.
        assertThat(observation).isEqualTo(new Observation(Set.of(), Set.of(), Set.of(1), true));
    }

    @Test
    void retiredDefinitionWithDelayedTerminalVisibilityIsAnotherCounterexample() {
        Cycle old = new Cycle(1, "old", 1, 6);
        assertThat(observe(List.of(old), 7, true, false).reconstruct()).isEmpty();
        assertThat(observe(List.of(old), 7, true, true).reconstruct()).containsExactly(1);
    }

    @Test
    void definitionReuseDoesNotReplaceTheOldCycleWithItsSuccessor() {
        var cycles = List.of(new Cycle(11, "same-lock-space", 1, 6), new Cycle(12, "same-lock-space", 7, Q + 2));
        for (int lookup = P; lookup <= Q; lookup++) {
            assertThat(observe(cycles, lookup, true, true).reconstruct()).containsExactlyInAnyOrder(11, 12);
        }
    }

    @Test
    void lateSelectedTableInvolvementAndZeroPendingChangesCannotFilterTheInventory() {
        Cycle silent = new Cycle(31, "unselected-work-before-L", 1, Q + 2);
        var allResources = observe(List.of(silent), 7, true, true);
        assertThat(allResources.reconstruct()).containsExactly(31);
        // Either a table filter or a pending-change-count filter can hide the same open cycle.
        assertThat(new Observation(Set.of(), allResources.interval, allResources.retainedStarts, true).reconstruct()).isEmpty();
    }

    @Test
    void knownIncompleteObservationRejectsInsteadOfCertifyingAnEmptyInventory() {
        assertThatThrownBy(() -> new Observation(Set.of(), Set.of(), Set.of(), false).reconstruct())
                .hasMessageContaining("Incomplete observation");
    }

    @Test
    void aCycleIdWithoutItsRetainedOriginalStartCannotBootstrapRecovery() {
        assertThatThrownBy(() -> new Observation(Set.of(31), Set.of(42), Set.of(42), true).reconstruct())
                .hasMessageContaining("Required original start unavailable");
        // Neither a timestamp nor the earliest observed cycle certifies an empty F-1.
    }
}
