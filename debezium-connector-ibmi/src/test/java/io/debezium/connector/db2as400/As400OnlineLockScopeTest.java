/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class As400OnlineLockScopeTest {
    static ObjectNode fixture() throws Exception {
        return As400OnlineFenceEvidenceTest.withUnverifiedAccessControlsForReplay(
                (ObjectNode) new ObjectMapper().readTree(As400OnlineLockScopeTest.class.getResourceAsStream("/native-transactions/automatic-fence-native-scope.json")));
    }

    @Test
    void discoversAdditionalFileAndActualMemberAndReusesNativeBindingChecks() throws Exception {
        var input = fixture();
        var scope = input.get("native_scope");
        var targets = As400OnlineLockScope.targets(scope.get("discovered"), scope.get("members"), input.get("library").asText());
        assertThat(targets.stream().map(As400OnlineLockScope.Target::file)).containsExactly("A", "B", "EXTRA", "M", "U");
        assertThat(targets.get(2).member()).isEqualTo("DATA");
        var candidate = As400OnlineFenceEvidence.validate(input);
        assertThat(candidate.tables().definitions()).hasSize(5);
        assertThat(candidate.tables().observations().get(2).member()).isEqualTo("DATA");
        assertThat(input.get("startup_proof").asBoolean()).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-file", "duplicate-file", "foreign-file", "logical-file", "unsupported-object", "wrong-journal", "wrong-asp", "implicit",
            "missing-count", "negative-count", "fractional-count", "hidden-file", "multiple-members", "access-path", "data-area", "data-queue", "ifs", "remote",
            "missing-library", "duplicate-member", "missing-member", "foreign-member" })
    void incompleteOrUnsupportedInventoryNeverProvidesTargets(String fault) throws Exception {
        var input = fixture();
        var scope = input.get("native_scope");
        var discovered = scope.get("discovered");
        var rows = (ArrayNode) discovered.get("objects");
        var first = (ObjectNode) rows.get(0);
        var count = (ObjectNode) discovered.get("counts").get(0);
        var members = (ArrayNode) scope.get("members");
        switch (fault) {
            case "missing-file" -> rows.remove(0);
            case "duplicate-file" -> rows.add(first.deepCopy());
            case "foreign-file" -> first.put("OBJECT_LIBRARY", "OTHER");
            case "logical-file" -> first.put("FILE_TYPE", "LF");
            case "unsupported-object" -> first.put("OBJECT_TYPE", "*DTAARA");
            case "wrong-journal" -> first.put("JOURNAL_NAME", "OTHER");
            case "wrong-asp" -> first.put("ASP", "2");
            case "implicit" -> first.put("JOURNALED_TYPE", "1");
            case "missing-count" -> count.remove("JOURNALED_FILES");
            case "negative-count" -> count.put("JOURNALED_FILES", -1);
            case "fractional-count" -> count.put("JOURNALED_FILES", 5.1);
            case "hidden-file" -> count.put("JOURNALED_FILES", 6);
            case "multiple-members" -> count.put("JOURNALED_MEMBERS", 6);
            case "access-path" -> count.put("JOURNALED_ACCESS_PATHS", 1);
            case "data-area" -> count.put("JOURNALED_DATA_AREAS", 1);
            case "data-queue" -> count.put("JOURNALED_DATA_QUEUES", 1);
            case "ifs" -> count.put("JOURNALED_IFS_OBJECTS", 1);
            case "remote" -> count.put("NUMBER_REMOTE_JOURNALS", 1);
            case "missing-library" -> rows.remove(rows.size() - 1);
            case "duplicate-member" -> members.set(1, members.get(0).deepCopy());
            case "missing-member" -> members.remove(0);
            case "foreign-member" -> ((ObjectNode) members.get(0)).put("SYSTEM_TABLE_SCHEMA", "OTHER");
            default -> throw new AssertionError(fault);
        }
        assertThrows(IllegalArgumentException.class,
                () -> As400OnlineLockScope.targets(discovered, members, input.get("library").asText()));
    }

    @ParameterizedTest
    @ValueSource(strings = { "changed-inventory", "changed-member", "missing-target", "missing-native-scope", "downgrade", "promoted", "journaling-change" })
    void scopeOrBindingChangesCannotBecomeBoundaryEvidence(String fault) throws Exception {
        var input = fixture();
        switch (fault) {
            case "changed-inventory" -> ((ObjectNode) input.at("/native_scope/after/objects/0")).put("OBJECT_NAME", "CHANGED");
            case "changed-member" -> ((ObjectNode) input.at("/native_scope/members/0")).put("SYSTEM_TABLE_MEMBER", "CHANGED");
            case "missing-target" -> ((ArrayNode) input.get("targets")).remove(2);
            case "missing-native-scope" -> input.remove("native_scope");
            case "downgrade" -> input.put("profile", As400OnlineFenceEvidence.PROFILE);
            case "promoted" -> input.put("startup_proof", true);
            case "journaling-change" -> {
                // Inventory is still identical; the existing exact native object
                // validator must independently reject a changed journaling binding.
                for (var query : input.get("queries")) {
                    if (query.path("name").asText().equals("object") && query.path("phase").asText().equals("after")) {
                        ((ObjectNode) query.get("rows").get(0)).put("JOURNAL_NAME", "OTHER");
                    }
                }
            }
            default -> throw new AssertionError(fault);
        }
        assertThrows(Exception.class, () -> As400OnlineFenceEvidence.validate(input));
    }
}
