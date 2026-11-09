/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.connector.db2as400.As400CaptureFingerprint.Policy;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.relational.TableId;

class As400CaptureFingerprintTest {
    // Generated with the preceding single-JID implementation and the policy below.
    static final String SINGLE_JID_FINGERPRINT = "v1:ecc5ac066dcc39370cb92a0b0e8967d14ef1dfe54a1b4bbf625871c19792ebbb";
    private static final TableIdentity TABLE = new TableIdentity(new TableId(null, "LIB", "SQL_TABLE"), "FILE", "MBR", "0102030405060708090a",
            As400ConnectProbeConnector.MEMBER_JID, "format-level-1");

    static Policy policy() {
        return new Policy(Map.of("table.include.list", "LIB.SQL_TABLE"), Set.of(), Map.of("version", "complete-inline-v1", "savepoints", "reject", "ccsid", "37"),
                Map.of("version", "schema-v1", "decimal.handling.mode", "precise"), Map.of("version", "output-v1", "topic.prefix", "test"),
                true, true, Map.of("server", "test"));
    }

    @Test
    void pairedIdentityCannotReuseTheSingleJidFingerprint() {
        assertThat(policy().fingerprint(List.of(TABLE))).isNotEqualTo(SINGLE_JID_FINGERPRINT);
    }

    @Test
    void canonicalFingerprintIsIndependentOfMapSetAndTableIterationOrder() {
        Policy first = policy();
        Map<String, String> decoder = new LinkedHashMap<>();
        decoder.put("ccsid", "37");
        decoder.put("savepoints", "reject");
        decoder.put("version", "complete-inline-v1");
        Policy second = new Policy(first.filters(), Set.of("t", "d"), decoder, first.schema(), first.output(),
                first.transactionMetadata(), first.tombstones(), first.sourcePartition());
        first = new Policy(first.filters(), Set.of("d", "t"), first.decoder(), first.schema(), first.output(),
                first.transactionMetadata(), first.tombstones(), first.sourcePartition());
        TableIdentity other = new TableIdentity(new TableId(null, "LIB", "OTHER"), "OTHER", "MBR", "1112030405060708090a", "1b1c1d1e1f2021222324", "format-level-2");
        assertThat(first.fingerprint(List.of(TABLE, other))).matches("v1:[0-9a-f]{64}")
                .isEqualTo(second.fingerprint(List.of(other, TABLE)));
    }

    @ParameterizedTest
    @ValueSource(strings = { "filter", "operation", "decoder", "schema", "output", "metadata", "tombstones", "partition",
            "catalog", "library", "logical-name", "physical-name", "member", "object", "member-jid", "swapped-jids", "format" })
    void everyReplayRelevantPolicyAndIdentityChangeInvalidatesTheFingerprint(String change) {
        Policy original = policy();
        Map<String, String> filters = new HashMap<>(original.filters());
        Map<String, String> decoder = new HashMap<>(original.decoder());
        Map<String, String> schema = new HashMap<>(original.schema());
        Map<String, String> output = new HashMap<>(original.output());
        Set<String> skipped = original.skippedOperations();
        Map<String, String> partition = original.sourcePartition();
        boolean metadata = true;
        boolean tombstones = true;
        TableIdentity table = TABLE;
        switch (change) {
            case "filter" -> filters.put("column.exclude.list", "LIB.SQL_TABLE.VALUE");
            case "operation" -> skipped = Set.of("t");
            case "decoder" -> decoder.put("ccsid", "500");
            case "schema" -> schema.put("decimal.handling.mode", "double");
            case "output" -> output.put("topic.prefix", "other");
            case "metadata" -> metadata = false;
            case "tombstones" -> tombstones = false;
            case "partition" -> partition = Map.of("server", "other");
            case "catalog" -> table = identity(new TableId("CAT", "LIB", "SQL_TABLE"), "FILE", "MBR", TABLE.fileJournalId(), TABLE.recordFormat());
            case "library" -> table = identity(new TableId(null, "LIB2", "SQL_TABLE"), "FILE", "MBR", TABLE.fileJournalId(), TABLE.recordFormat());
            case "logical-name" -> table = identity(new TableId(null, "LIB", "OTHER"), "FILE", "MBR", TABLE.fileJournalId(), TABLE.recordFormat());
            case "physical-name" -> table = identity(TABLE.tableId(), "OTHER", "MBR", TABLE.fileJournalId(), TABLE.recordFormat());
            case "member" -> table = identity(TABLE.tableId(), "FILE", "MBR2", TABLE.fileJournalId(), TABLE.recordFormat());
            case "object" -> table = identity(TABLE.tableId(), "FILE", "MBR", "1112030405060708090a", TABLE.recordFormat());
            case "member-jid" -> table = new TableIdentity(TABLE.tableId(), "FILE", "MBR", TABLE.fileJournalId(), "3132030405060708090a", TABLE.recordFormat());
            case "swapped-jids" -> table = new TableIdentity(TABLE.tableId(), "FILE", "MBR", TABLE.memberJournalId(), TABLE.fileJournalId(), TABLE.recordFormat());
            case "format" -> table = identity(TABLE.tableId(), "FILE", "MBR", TABLE.fileJournalId(), "format-level-2");
            default -> throw new IllegalArgumentException(change);
        }
        Policy changed = new Policy(filters, skipped, decoder, schema, output, metadata, tombstones, partition);
        assertThat(changed.fingerprint(List.of(table))).isNotEqualTo(original.fingerprint(List.of(TABLE)));
    }

    @Test
    void delimitersUnicodeAndQualifiedNamesCannotAliasAnotherPolicy() {
        assertThat(As400CaptureFingerprint.hash("domain", Map.of("a", "b:c", "d", "é")))
                .isNotEqualTo(As400CaptureFingerprint.hash("domain", Map.of("a:b", "c", "d", "é")))
                .isNotEqualTo(As400CaptureFingerprint.hash("other-domain", Map.of("a", "b:c", "d", "é")));
        TableIdentity other = identity(new TableId(null, "L.IB", "SQL_TABLE"), TABLE.physicalFile(), TABLE.member(), TABLE.fileJournalId(), TABLE.recordFormat());
        TableIdentity third = identity(new TableId(null, "L", "IB.SQL_TABLE"), TABLE.physicalFile(), TABLE.member(), TABLE.fileJournalId(), TABLE.recordFormat());
        assertThat(policy().fingerprint(List.of(other))).isNotEqualTo(policy().fingerprint(List.of(third)));
    }

    @Test
    void policyOwnsItsCollectionsAndRequiresExplicitVersionsAndKnownOperations() {
        Policy original = policy();
        Map<String, String> decoder = new HashMap<>(original.decoder());
        Policy copy = new Policy(original.filters(), original.skippedOperations(), decoder, original.schema(), original.output(), true, true, original.sourcePartition());
        String before = copy.fingerprint(List.of(TABLE));
        decoder.put("ccsid", "500");
        assertThat(copy.fingerprint(List.of(TABLE))).isEqualTo(before);
        assertThrows(UnsupportedOperationException.class, () -> copy.decoder().clear());
        assertThrows(IllegalArgumentException.class, () -> new Policy(original.filters(), Set.of("unknown"), decoder, original.schema(), original.output(), true, true,
                original.sourcePartition()));
        decoder.remove("version");
        assertThrows(IllegalArgumentException.class, () -> new Policy(original.filters(), Set.of(), decoder, original.schema(), original.output(), true, true,
                original.sourcePartition()));
        assertThrows(IllegalArgumentException.class, () -> copy.fingerprint(List.of()));
        List<TableIdentity> duplicate = new ArrayList<>(List.of(TABLE, TABLE));
        assertThrows(IllegalArgumentException.class, () -> copy.fingerprint(duplicate));
    }

    private static TableIdentity identity(TableId logical, String file, String member, String object, String format) {
        return new TableIdentity(logical, file, member, object, As400ConnectProbeConnector.MEMBER_JID, format);
    }
}
