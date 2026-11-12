/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

class As400OnlineFenceEvidenceTest {
    static As400OnlineFenceEvidence.Candidate replayMetadata(JsonNode captured) throws Exception {
        return As400OnlineFenceEvidence.validateOffLock(withUnverifiedMetadataForReplay(captured));
    }

    /**
     * Component-only assumptions for captures that predate RCAC and JDBC output metadata.
     * Never edits or publishes the captured resources. The native validators and
     * command-line replay still reject those files without real control evidence.
     */
    static <T extends JsonNode> T withUnverifiedMetadataForReplay(T original) {
        T copy = original.deepCopy();
        addUnverifiedMetadata(copy);
        return copy;
    }

    private static void addUnverifiedMetadata(JsonNode node) {
        if (node.isObject() && node.path("queries").isArray() && node.path("targets").isArray()) {
            ArrayNode queries = (ArrayNode) node.get("queries");
            for (JsonNode target : node.get("targets")) {
                String library = target.path("library").asText();
                String file = target.path("file").asText();
                ObjectNode query = queries.addObject().put("name", "controls").put("phase", "layout")
                        .put("sql", As400NativeBindingAcquisition.CONTROLS_SQL).put("synthetic_test_assumption", true);
                query.putArray("parameters").add(library).add(file);
                query.putArray("rows").addObject().put("TABLE_SCHEMA", library).put("SYSTEM_TABLE_SCHEMA", library)
                        .put("TABLE_NAME", file).put("SYSTEM_TABLE_NAME", file).put("CONTROL", " ");
            }
            var jdbc = ((ObjectNode) node).putObject("jdbc_metadata").put("search_escape", "\\").put("synthetic_test_assumption", true);
            var outputTables = jdbc.putArray("tables");
            var mapper = new ObjectMapper();
            for (var query : queries) {
                if (query.path("name").asText().equals("columns")) {
                    var output = outputTables.addObject().put("schema_pattern", query.get("parameters").get(0).asText().replace("_", "\\_"))
                            .put("table_pattern", query.get("parameters").get(1).asText().replace("_", "\\_"));
                    var rows = output.putArray("rows");
                    for (var column : query.get("rows")) {
                        Map<String, Object> physical = mapper.convertValue(column, new TypeReference<>() {
                        });
                        rows.add(mapper.valueToTree(As400NativeBindingAcquisitionTest.jdbcColumn(physical)));
                    }
                }
            }
            return;
        }
        if (node.isContainerNode()) {
            node.forEach(As400OnlineFenceEvidenceTest::addUnverifiedMetadata);
        }
    }

    static ObjectNode fixture() throws Exception {
        return withUnverifiedMetadataForReplay((ObjectNode) new ObjectMapper()
                .readTree(As400OnlineFenceEvidenceTest.class.getResourceAsStream("/native-transactions/automatic-fence-metadata.json")));
    }

    @Test
    void shouldRejectHistoricalCaptureThatCannotEstablishAccessControlSettings() throws Exception {
        var raw = new ObjectMapper().readTree(getClass().getResourceAsStream("/native-transactions/automatic-fence-metadata.json"));
        assertThat(assertThrows(Exception.class, () -> As400OnlineFenceEvidence.validate(raw)))
                .hasStackTraceContaining("Native validator requested uncaptured evidence");
    }

    @Test
    void shouldRejectHistoricalCaptureWithoutJdbcOutputEvidence() throws Exception {
        var data = fixture();
        data.remove("jdbc_metadata");
        assertThat(assertThrows(Exception.class, () -> As400OnlineFenceEvidence.validate(data)))
                .hasStackTraceContaining("Native validator requested uncaptured JDBC output evidence");
    }

    @Test
    void nativeMetadataUsesExistingValidatorsWithoutPromotingHistoryOrCheckpoint() throws Exception {
        var candidate = As400OnlineFenceEvidence.validate(fixture());
        assertThat(candidate.tables().definitions()).hasSize(3);
        var mixed = candidate.tables().definitions().get(2);
        assertThat(mixed.schema().id().table()).isEqualTo("M");
        assertThat(mixed.codecs().stream().map(As400BindingDefinition.Codec::kind)).containsExactly("BIN4", "VARCHAR", "PACKED", "BIN2");
        assertThat(mixed.schema().columns().stream().map(c -> c.isOptional())).containsExactly(false, true, true, true);
        assertThat(mixed.identity().fileJournalId()).isNotEqualTo(mixed.identity().memberJournalId());
        assertThat(candidate.boundary().offset()).isEqualTo(java.math.BigInteger.valueOf(86));
        assertThat(candidate.boundary().receiver().name()).isEqualTo("QSQJRN0002");
        // Old receivers remain observations, not certified replay history.
        assertThrows(IllegalArgumentException.class, () -> candidate.journal().receivers().get(0).requireReadableProfile());
    }

    @Test
    void offLockMetadataCannotMasqueradeAsLockedEvidence() throws Exception {
        ObjectNode data = withUnverifiedMetadataForReplay((ObjectNode) new ObjectMapper()
                .readTree(getClass().getResourceAsStream("/native-transactions/automatic-fence-native-scope.json")));
        assertThrows(IllegalArgumentException.class, () -> As400OnlineFenceEvidence.validateOffLock(data));
        data.put("profile", As400OnlineFenceEvidence.OFFLOCK_PROFILE).put("status", "observed").put("outside_lock", true);
        assertThat(As400OnlineFenceEvidence.validateOffLock(data).tables().definitions()).hasSize(5);
        assertThrows(IllegalArgumentException.class, () -> As400OnlineFenceEvidence.validate(data));
        data.put("outside_lock", false);
        assertThrows(IllegalArgumentException.class, () -> As400OnlineFenceEvidence.validateOffLock(data));
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-scope", "missing-query", "promoted", "receiver", "member" })
    void offLockMetadataPreservesExistingScopeAndIdentityChecks(String fault) throws Exception {
        ObjectNode data = As400OnlineLockScopeTest.fixture();
        data.put("profile", As400OnlineFenceEvidence.OFFLOCK_PROFILE).put("status", "observed").put("outside_lock", true);
        switch (fault) {
            case "missing-scope" -> data.remove("native_scope");
            case "missing-query" -> ((ArrayNode) data.get("queries")).remove(0);
            case "promoted" -> data.put("startup_proof", true);
            case "member" -> ((ObjectNode) data.get("targets").get(0)).put("member", "OTHER");
            case "receiver" -> {
                for (JsonNode query : data.get("queries")) {
                    if (query.path("name").asText().equals("journal") && query.path("phase").asText().equals("after")) {
                        ((ObjectNode) query.get("rows").get(0)).put("ATTACHED_JOURNAL_RECEIVER_NAME", "OTHER");
                    }
                }
            }
            default -> throw new AssertionError(fault);
        }
        assertThrows(Exception.class, () -> As400OnlineFenceEvidence.validateOffLock(data));
    }

    @ParameterizedTest
    @ValueSource(strings = { "partial", "promoted", "duplicate-target", "missing-query", "extra-query", "warning-status", "wrong-sql", "wrong-parameter", "wrong-phase" })
    void rejectsIncompleteOrForeignCapture(String fault) throws Exception {
        var data = fixture();
        ArrayNode queries = (ArrayNode) data.get("queries");
        switch (fault) {
            case "partial" -> data.remove("startup_proof");
            case "promoted" -> data.put("startup_proof", true);
            case "duplicate-target" -> ((ArrayNode) data.get("targets")).add(data.get("targets").get(0).deepCopy());
            case "missing-query" -> queries.remove(queries.size() - 1);
            case "extra-query" -> queries.add(queries.get(0).deepCopy());
            case "warning-status" -> data.put("status", "warning");
            case "wrong-sql" -> ((ObjectNode) queries.get(0)).put("sql", "SELECT 1");
            case "wrong-parameter" -> ((ArrayNode) queries.get(0).get("parameters")).set(0, new ObjectMapper().valueToTree("OTHER"));
            case "wrong-phase" -> ((ObjectNode) queries.get(0)).put("phase", "after");
            default -> throw new AssertionError(fault);
        }
        assertThrows(Exception.class, () -> As400OnlineFenceEvidence.validate(data));
    }

    @ParameterizedTest
    @ValueSource(strings = { "file-jid", "member-jid", "format", "member", "missing-column", "overlap", "key", "journal", "receiver-profile", "receiver-range",
            "old-range" })
    void rejectsBindingChangesAndUnsupportedAttachedHistory(String fault) throws Exception {
        var data = fixture();
        for (JsonNode query : data.get("queries")) {
            String name = query.get("name").asText();
            boolean after = query.get("phase").asText().equals("after");
            ArrayNode rows = (ArrayNode) query.get("rows");
            if (rows.isEmpty()) {
                continue;
            }
            ObjectNode first = (ObjectNode) rows.get(0);
            if (fault.equals("file-jid") && after && name.equals("object") && first.path("OBJECT_TYPE").asText().equals("*FILE")) {
                first.put("OBJECT_JID", "00000000000000000001");
            }
            if (fault.equals("member-jid") && after && name.equals("object") && first.path("OBJECT_TYPE").asText().equals("*MBR")) {
                first.put("OBJECT_JID", "00000000000000000001");
            }
            if (fault.equals("format") && after && name.equals("format")) {
                first.put("FORMAT_LEVEL_ID", "0000000000000");
            }
            if (fault.equals("member") && after && name.equals("member")) {
                first.put("SYSTEM_TABLE_MEMBER", "OTHER");
            }
            if (fault.equals("missing-column") && name.equals("columns")) {
                first.remove("IS_NULLABLE");
            }
            if (fault.equals("overlap") && name.equals("columns")) {
                first.put("BUFFER_POSITION", 2);
            }
            if (fault.equals("key") && name.equals("keys")) {
                first.put("COLUMN_NAME", "UNKNOWN");
            }
            if (fault.equals("journal") && after && name.equals("journal")) {
                first.put("ATTACHED_JOURNAL_RECEIVER_NAME", "QSQJRN9999");
            }
            if (fault.equals("old-range") && after && name.equals("receivers")) {
                first.put("LAST_SEQUENCE_NUMBER", first.get("LAST_SEQUENCE_NUMBER").asLong() + 1);
            }
            if (name.equals("receivers") && after && List.of("receiver-profile", "receiver-range").contains(fault)) {
                for (JsonNode row : rows) {
                    if (row.get("STATUS").asText().equals("ATTACHED")) {
                        ((ObjectNode) row).put(fault.equals("receiver-profile") ? "FIXED_LENGTH_DATA_INCLUDES_THREAD_ID" : "LAST_SEQUENCE_NUMBER",
                                fault.equals("receiver-profile") ? "NO" : "0");
                    }
                }
            }
        }
        assertThrows(Exception.class, () -> As400OnlineFenceEvidence.validate(data));
    }
}
