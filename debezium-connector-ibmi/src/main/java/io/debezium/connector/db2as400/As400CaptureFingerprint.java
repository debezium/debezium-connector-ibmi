/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
            if (tables.isEmpty() || Set.copyOf(tables).size() != tables.size()) {
                throw new IllegalArgumentException("Distinct captured table identities are required");
            }
            Map<String, String> fields = new TreeMap<>();
            flatten(fields, "filters", filters);
            flatten(fields, "decoder", decoder);
            flatten(fields, "schema", schema);
            flatten(fields, "output", output);
            flatten(fields, "partition", sourcePartition);
            fields.put("transaction.metadata", Boolean.toString(transactionMetadata));
            fields.put("tombstones", Boolean.toString(tombstones));
            skippedOperations.forEach(op -> fields.put("skipped." + op, "true"));
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
            return "v1:" + hash("ibmi-capture-v1", fields);
        }
    }

    static String hash(String domain, Map<String, String> fields) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                write(output, domain);
                output.writeInt(fields.size());
                for (var entry : new TreeMap<>(fields).entrySet()) {
                    write(output, entry.getKey());
                    write(output, entry.getValue());
                }
            }
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
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
