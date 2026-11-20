/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;

/** Canonical, versioned replay policy. Never hash a Properties.toString() or credentials. */
final class As400CaptureFingerprint {
    private As400CaptureFingerprint() {
    }

    /**
     * Effective policies after defaults, not raw connector properties. The integration
     * adapter must include every filter, key/column mapping, converter, naming and
     * transform policy affecting replay. Resource limits and credentials do not belong
     * here. Unvalidated transforms must be rejected by startup's delivery gate.
     */
    record Policy(Map<String, String> filters, Set<String> skippedOperations,
            Map<String, String> decoder, Map<String, String> schema, Map<String, String> output,
            boolean transactionMetadata, boolean tombstones, Map<String, String> sourcePartition) {
        Policy {
            filters = strings(filters, false);
            skippedOperations = Set.copyOf(skippedOperations);
            if (!Set.of("c", "u", "d", "t").containsAll(skippedOperations)) {
                throw new IllegalArgumentException("Unsupported skipped operation");
            }
            decoder = strings(decoder, true);
            schema = strings(schema, true);
            output = strings(output, true);
            sourcePartition = strings(sourcePartition, true);
            if (!decoder.containsKey("version") || !schema.containsKey("version") || !output.containsKey("version")) {
                throw new IllegalArgumentException("Explicit decoder, schema and output policy versions are required");
            }
        }

        String fingerprint(List<TableIdentity> tables) {
            return fingerprint(tables, false);
        }

        /**
         * New online histories pin source reconstruction independently from
         * ordinary output reconfiguration. Older histories keep fingerprint().
         * Unknown settings remain pinned; only known output-only choices move.
         */
        String recoveryFingerprint(List<TableIdentity> tables) {
            return fingerprint(tables, true);
        }

        /** Complete retained schemas allow output selection to change without reinterpreting source bytes. */
        String selectableFingerprint(List<TableIdentity> tables) {
            return fingerprint(tables, true, true);
        }

        private Map<String, String> recoveryOutput() {
            var retained = new TreeMap<>(output);
            var aliases = java.util.Arrays.stream(output.getOrDefault("converters", "").split(",")).map(String::trim)
                    .filter(alias -> !alias.isEmpty()).toList();
            Set<String> mutable = Set.of("converters", "transforms", "predicates", "key.converter", "value.converter", "header.converter",
                    "topic.naming.strategy", "topic.delimiter", "topic.heartbeat.prefix", "topic.transaction", "topic.heartbeat.name",
                    "decimal.handling.mode", "binary.handling.mode", "time.precision.mode", "schema.name.adjustment.mode", "field.name.adjustment.mode",
                    "trim.non.xml.charsequence.field.mode", "datatype.propagate.source.type", "message.key.columns", "skip.messages.without.change",
                    "transaction.id.format");
            // A converter alias can legally be named "history" or "temporal".
            // It cannot erase connector-owned invariants with the same prefix.
            Set<String> invariant = Set.of("version", "binding.catalog", "history.storage", "history.topic", "key.mode", "topic.prefix", "control.topic",
                    "temporal.compatibility", "temporal.worker.zone", "temporal.worker.rules");
            retained.keySet().removeIf(key -> !invariant.contains(key) && (mutable.contains(key) || key.startsWith("transforms.") || key.startsWith("predicates.")
                    || key.startsWith("key.converter.") || key.startsWith("value.converter.") || key.startsWith("header.converter.")
                    || key.startsWith("column.") || aliases.stream().anyMatch(alias -> key.startsWith(alias + "."))));
            return retained;
        }

        private String fingerprint(List<TableIdentity> tables, boolean recovery) {
            return fingerprint(tables, recovery, false);
        }

        private String fingerprint(List<TableIdentity> tables, boolean recovery, boolean selectable) {
            if (tables.isEmpty() || Set.copyOf(tables).size() != tables.size()) {
                throw new IllegalArgumentException("Distinct captured table identities are required");
            }
            Map<String, String> fields = new TreeMap<>();
            flatten(fields, "filters", filters);
            var retainedDecoder = new TreeMap<>(decoder);
            if (selectable) {
                retainedDecoder.keySet().removeAll(Set.of("table.include.list", "table.exclude.list", "schema.include.list", "schema.exclude.list"));
            }
            flatten(fields, "decoder", retainedDecoder);
            flatten(fields, "schema", schema);
            flatten(fields, "output", recovery ? recoveryOutput() : output);
            flatten(fields, "partition", sourcePartition);
            if (!recovery) {
                fields.put("transaction.metadata", Boolean.toString(transactionMetadata));
                fields.put("tombstones", Boolean.toString(tombstones));
                skippedOperations.forEach(op -> fields.put("skipped." + op, "true"));
            }
            // Sort complete identity hashes, not ambiguous concatenated qualified names.
            List<String> identities = new ArrayList<>();
            for (TableIdentity table : tables) {
                Map<String, String> identity = new TreeMap<>();
                identity.put("catalog.present", Boolean.toString(table.tableId().catalog() != null));
                identity.put("catalog", table.tableId().catalog() == null ? "" : table.tableId().catalog());
                identity.put("schema", table.tableId().schema());
                identity.put("table", table.tableId().table());
                identity.put("file", table.physicalFile());
                identity.put("member", table.member());
                identity.put("file.jid", table.fileJournalId());
                identity.put("member.jid", table.memberJournalId());
                identity.put("format", table.recordFormat());
                // New identity domain invalidates the former single-JID binding.
                identities.add(hash("table-file-member-v2", identity));
            }
            identities.sort(String::compareTo);
            for (int i = 0; i < identities.size(); i++) {
                fields.put("table." + i, identities.get(i));
            }
            return "v1:" + hash(selectable ? "ibmi-recovery-selection-v1" : recovery ? "ibmi-recovery-v1" : "ibmi-capture-v1", fields);
        }
    }

    static String hash(String domain, Map<String, String> fields) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            try (DataOutputStream output = new DataOutputStream(new DigestOutputStream(OutputStream.nullOutputStream(), digest))) {
                write(output, domain);
                output.writeInt(fields.size());
                for (var entry : new TreeMap<>(fields).entrySet()) {
                    write(output, entry.getKey());
                    write(output, entry.getValue());
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        }
        catch (IOException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Cannot encode capture fingerprint", e);
        }
    }

    private static void write(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void flatten(Map<String, String> target, String group, Map<String, String> fields) {
        fields.forEach((key, value) -> target.put(group.length() + ":" + group + key.length() + ":" + key, value));
    }

    private static Map<String, String> strings(Map<String, String> values, boolean nonempty) {
        Map<String, String> copy = Map.copyOf(values);
        if ((nonempty && copy.isEmpty()) || copy.entrySet().stream().anyMatch(e -> e.getKey().isBlank() || e.getValue().isBlank())) {
            throw new IllegalArgumentException("Missing effective capture policy");
        }
        return copy;
    }
}
