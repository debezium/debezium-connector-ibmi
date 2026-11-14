/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.math.BigInteger;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import com.ibm.as400.access.AS400Bin2;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400Bin8;
import com.ibm.as400.access.AS400ByteArray;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Float4;
import com.ibm.as400.access.AS400Float8;
import com.ibm.as400.access.AS400PackedDecimal;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.AS400ZonedDecimal;

import io.debezium.connector.db2as400.As400SourceNormalizer.TableBinding;
import io.debezium.connector.db2as400.As400TransactionBuffer.TableIdentity;
import io.debezium.ibmi.db2.journal.data.types.AS400Boolean;
import io.debezium.ibmi.db2.journal.data.types.AS400Temporal;
import io.debezium.ibmi.db2.journal.data.types.AS400VarBin;
import io.debezium.ibmi.db2.journal.data.types.AS400VarChar;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.relational.Attribute;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

/**
 * Immutable, explicit physical decoder description. Construction is NOT evidence
 * of a native association or a consistent baseline; initialization must supply it.
 * No default charset, Java serialization, reflective codec or live metadata fallback.
 */
record As400BindingDefinition(TableIdentity identity, Table schema, List<As400BindingDefinition.Codec> codecs, Map<String, String> members) {

    private static final String PREFIX = "ibmi.binding.";
    private static final int MAX_ROW_BYTES = 16 * 1024 * 1024;
    static final String KEY_MODE = "ibmi.output.key.mode";
    static final String RRN_COLUMN = "__IBMI_RRN";
    private static final String EXCLUDED = "ibmi.output.excluded";
    private static final String EXCLUDED_LOB = "ibmi.output.excluded.lob";
    static final String UNOBSERVED_LAYOUT = "UNOBSERVED";

    As400BindingDefinition(TableIdentity identity, Table schema, List<Codec> codecs) {
        this(identity, schema, codecs, Map.of(identity.member(), identity.memberJournalId()));
    }

    As400BindingDefinition {
        Objects.requireNonNull(identity);
        members = validatedMembers(identity, members);
        Objects.requireNonNull(schema);
        codecs = List.copyOf(codecs);
        var excludedAttribute = schema.attributeWithName(EXCLUDED);
        boolean excluded = excludedAttribute != null;
        var lob = schema.attributeWithName(EXCLUDED_LOB);
        require(lob == null || excluded && "true".equals(lob.value()), "LOB metadata is only valid for an excluded binding");
        require(!excluded || "true".equals(excludedAttribute.value()) && codecs.isEmpty() && schema.columns().isEmpty()
                && schema.primaryKeyColumnNames().isEmpty() && schema.attributeWithName(KEY_MODE) == null,
                "An excluded binding contains identity only, never a partial output schema");
        require(schema.id().equals(identity.tableId()) && (excluded || !codecs.isEmpty()) && codecs.size() <= CompleteRowEntryDecoder.MAX_COLUMNS
                && codecs.size() == schema.columns().size(), "Complete matching schema and physical codecs required");
        require(schema.attributes().stream().noneMatch(a -> a.name().startsWith(PREFIX)), "Reserved binding attributes in input schema");
        require(schema.attributes().stream().map(Attribute::name).distinct().count() == schema.attributes().size(), "Duplicate schema attributes");
        var key = schema.attributeWithName(KEY_MODE);
        require(key == null || "rrn".equals(key.value()), "Unsupported native output key mode");
        require(key == null || schema.columns().stream().noneMatch(c -> c.name().equalsIgnoreCase(RRN_COLUMN)), "Native column collides with relative record key");
        long bytes = 0;
        boolean varying = false;
        for (int i = 0; i < codecs.size(); i++) {
            require(schema.columns().get(i).position() == i + 1, "Physical column order must be complete");
            require(codecs.get(i).matches(schema.columns().get(i).jdbcType()), "Column schema and physical codec types disagree");
            if (codecs.get(i).kind().equals("PACKED") || codecs.get(i).kind().equals("ZONED")) {
                require(schema.columns().get(i).length() == codecs.get(i).size()
                        && schema.columns().get(i).scale().orElse(0) == codecs.get(i).scale(), "Decimal schema and physical codec disagree");
            }
            if (codecs.get(i).temporal()) {
                require(schema.columns().get(i).length() == codecs.get(i).size()
                        && schema.columns().get(i).scale().orElse(-1) == codecs.get(i).scale(), "Temporal schema and physical codec disagree");
            }
            bytes += codecs.get(i).decoder().getByteLength();
            varying |= List.of("VARBINARY", "VARCHAR", "VARGRAPHIC").contains(codecs.get(i).kind());
        }
        String[] format = identity.recordFormat().split(":", -1);
        require(excluded && identity.recordFormat().equals(UNOBSERVED_LAYOUT)
                || format.length == 5 && format[0].matches("[A-Z0-9_#$@]{1,10}") && format[1].matches("[0-9A-F]{13}")
                        && format[2].matches("[1-9][0-9]{0,7}") && Integer.parseInt(format[2]) <= MAX_ROW_BYTES
                        && format[3].matches("[1-9][0-9]{0,3}") && Integer.parseInt(format[3]) <= CompleteRowEntryDecoder.MAX_COLUMNS
                        && List.of("true", "false").contains(format[4]),
                "Invalid native record format");
        // SYSFILES can retain CONTAINS_VARYING_LENGTH=YES after the last
        // varying column is dropped (observed with a four-byte INTEGER row).
        // Preserve that native observation in the identity; derive decoding
        // solely from the complete column descriptors and their exact lengths.
        // A varying codec still requires the native varying capability.
        require(excluded || format[2].equals(Long.toString(bytes)) && format[3].equals(Integer.toString(codecs.size()))
                && (!varying || Boolean.parseBoolean(format[4])), "Physical format and codecs disagree");
    }

    static As400BindingDefinition excluded(TableIdentity identity) {
        return excluded(identity, false);
    }

    static As400BindingDefinition excluded(TableIdentity identity, boolean lob) {
        var schema = Table.editor().tableId(identity.tableId()).addAttribute(Attribute.editor().name(EXCLUDED).value("true").create());
        if (lob) {
            schema.addAttribute(Attribute.editor().name(EXCLUDED_LOB).value("true").create());
        }
        return new As400BindingDefinition(identity, schema.create(), List.of());
    }

    boolean excluded() {
        return schema.attributeWithName(EXCLUDED) != null;
    }

    boolean unobservedLayout() {
        return excluded() && identity.recordFormat().equals(UNOBSERVED_LAYOUT);
    }

    /** Retained identity only; no earlier field layout is claimed to describe new excluded rows. */
    As400BindingDefinition withoutObservedLayout() {
        var id = new TableIdentity(identity.tableId(), identity.physicalFile(), identity.member(), identity.fileJournalId(), identity.memberJournalId(),
                UNOBSERVED_LAYOUT);
        var excluded = excluded(id);
        return new As400BindingDefinition(id, excluded.schema(), List.of(), members);
    }

    static Map<String, String> validatedMembers(TableIdentity identity, Map<String, String> members) {
        if (members instanceof MemberMap validated && validated.fileJournalId.equals(identity.fileJournalId())) {
            require(members.isEmpty() || identity.memberJournalId().equals(members.get(identity.member())), "Canonical member is absent or changed");
            return members;
        }
        require(identity.memberJournalId().equals(members.get(identity.member())), "Canonical member is absent or changed");
        var ordered = new TreeMap<>(members);
        require(ordered.values().stream().distinct().count() == ordered.size(), "Member journal identities must be distinct");
        ordered.forEach((member, jid) -> {
            require(member.matches("[A-Z0-9_#$@]{1,10}") && TableIdentity.validJournalId(jid) && !jid.equals(identity.fileJournalId()),
                    "Complete distinct member journal identity required");
        });
        return new MemberMap(identity.fileJournalId(), ordered);
    }

    /** The retained identity anchors history; its former member is no longer authorized for rows. */
    As400BindingDefinition withoutMembers() {
        require(members.size() == 1, "Last-member removal requires exactly one retained member");
        return new As400BindingDefinition(identity, schema, codecs, new MemberMap(identity.fileJournalId(), Map.of()));
    }

    boolean memberless() {
        return members.isEmpty();
    }

    /** Share one validated immutable mapping across member decoder views, never copy the whole file's members per row. */
    private static final class MemberMap extends java.util.AbstractMap<String, String> {
        private final String fileJournalId;
        private final Map<String, String> values;

        private MemberMap(String fileJournalId, Map<String, String> values) {
            this.fileJournalId = fileJournalId;
            this.values = java.util.Collections.unmodifiableMap(values);
        }

        @Override
        public String get(Object key) {
            return values.get(key);
        }

        @Override
        public java.util.Set<Entry<String, String>> entrySet() {
            return values.entrySet();
        }
    }

    TableIdentity memberIdentity(String member) {
        String jid = members.get(member);
        require(jid != null, "Unknown member in durable binding");
        return new TableIdentity(identity.tableId(), identity.physicalFile(), member, identity.fileJournalId(), jid, identity.recordFormat());
    }

    List<TableIdentity> identities() {
        return members.keySet().stream().map(this::memberIdentity).toList();
    }

    TableBinding binding(int maximumRowBytes) {
        if (excluded()) {
            return new TableBinding(identity, null, false, true, members, unobservedLayout() || schema.attributeWithName(EXCLUDED_LOB) != null);
        }
        boolean[] nullable = new boolean[codecs.size()];
        for (int i = 0; i < nullable.length; i++) {
            nullable[i] = schema.columns().get(i).isOptional();
        }
        return new TableBinding(identity, new CompleteRowEntryDecoder(codecs.stream().map(Codec::decoder).toArray(AS400DataType[]::new), nullable, maximumRowBytes),
                usesRrn(), false, members);
    }

    boolean usesRrn() {
        return schema.attributeWithName(KEY_MODE) != null;
    }

    Table outputTable() {
        if (!usesRrn()) {
            return schema;
        }
        return schema.edit().addColumn(Column.editor().name(RRN_COLUMN).position(codecs.size() + 1).jdbcType(Types.BIGINT).type("BIGINT")
                .optional(false).create()).setPrimaryKeyNames(RRN_COLUMN).create();
    }

    static Object[] withRrn(Object[] physical, BigInteger rrn) {
        require(rrn != null && rrn.signum() > 0 && rrn.bitLength() <= 63, "Positive representable relative record number required");
        Object[] projected = Arrays.copyOf(physical, physical.length + 1);
        projected[physical.length] = rrn.longValueExact();
        return projected;
    }

    Table storedTable() {
        Map<String, String> attributes = new TreeMap<>();
        schema.attributes().forEach(a -> attributes.put(a.name(), a.value()));
        // Keep existing single-member history byte-compatible. Version 2 pins
        // every member, sharing the file's schema and physical decoders.
        // Version 3 explicitly retains a physical file with no active members.
        // Older readers must reject it rather than revive its historical anchor.
        attributes.put(PREFIX + "version", memberless() ? "3" : members.size() == 1 ? "1" : "2");
        if (members.size() > 1) {
            members.forEach((member, jid) -> attributes.put(PREFIX + "members." + member, jid));
        }
        attributes.put(PREFIX + "file", identity.physicalFile());
        attributes.put(PREFIX + "member", identity.member());
        attributes.put(PREFIX + "file.jid", identity.fileJournalId());
        attributes.put(PREFIX + "member.jid", identity.memberJournalId());
        attributes.put(PREFIX + "format", identity.recordFormat());
        for (int i = 0; i < codecs.size(); i++) {
            attributes.put(PREFIX + "codec." + i, codecs.get(i).encode());
        }
        var table = schema.edit();
        schema.attributes().forEach(a -> table.removeAttribute(a.name()));
        attributes.forEach((name, value) -> table.addAttribute(Attribute.editor().name(name).value(value).create()));
        return table.create();
    }

    static As400BindingDefinition restore(Table stored) {
        String version = value(stored, "version");
        require(List.of("1", "2", "3").contains(version), "Unsupported binding version");
        var identity = new TableIdentity(stored.id(), value(stored, "file"), value(stored, "member"),
                value(stored, "file.jid"), value(stored, "member.jid"), value(stored, "format"));
        List<Codec> codecs = new ArrayList<>();
        for (int i = 0; i < stored.columns().size(); i++) {
            codecs.add(Codec.decode(value(stored, "codec." + i)));
        }
        var clean = stored.edit();
        stored.attributes().stream().filter(a -> a.name().startsWith(PREFIX)).forEach(a -> clean.removeAttribute(a.name()));
        Map<String, String> members = new TreeMap<>();
        if (version.equals("1")) {
            members.put(identity.member(), identity.memberJournalId());
        }
        else if (version.equals("2")) {
            stored.attributes().stream().filter(a -> a.name().startsWith(PREFIX + "members."))
                    .forEach(a -> require(members.put(a.name().substring((PREFIX + "members.").length()), a.value()) == null, "Duplicate member binding"));
            require(members.size() > 1, "Version 2 requires a complete multi-member binding");
        }
        var restored = new As400BindingDefinition(identity, clean.create(), codecs,
                version.equals("3") ? new MemberMap(identity.fileJournalId(), Map.of()) : members);
        // Reject extra/duplicate attributes and noncanonical descriptors instead of dropping them.
        require(stored.attributes().stream().sorted(Comparator.comparing(Attribute::name)).toList().equals(restored.storedTable().attributes()),
                "Incomplete or unsupported binding attributes");
        return restored;
    }

    private static String value(Table table, String suffix) {
        var attribute = table.attributeWithName(PREFIX + suffix);
        require(attribute != null && attribute.value() != null, "Missing binding " + suffix);
        return attribute.value();
    }

    /** Parameters are effective values, not source SQL sizes awaiting a catalog lookup. */
    record Codec(String kind, int size, int scale, int ccsid) {
        Codec {
            Objects.requireNonNull(kind);
            require(size > 0 && size <= 65535 && scale >= 0 && scale <= size, "Invalid physical codec size/scale");
            if (kind.equals("DATE_ISO") || kind.equals("TIME_ISO") || kind.equals("TIMESTAMP_ISO")) {
                require(ccsid == 37 || ccsid == 500 || ccsid == 1140, "Unsupported temporal CCSID");
                require(kind.equals("TIMESTAMP_ISO") ? scale <= 9 : scale == 0, "Unsupported temporal precision");
            }
            else if (kind.equals("TEXT") || kind.equals("VARCHAR") || kind.equals("VARGRAPHIC")) {
                require(scale == 0 && ccsid > 0 && ccsid < 65535, "Explicit text CCSID required");
            }
            else {
                require(ccsid == 0 && (kind.equals("PACKED") || kind.equals("ZONED") || scale == 0), "Unexpected codec parameters");
            }
            require(switch (kind) {
                case "BIN2" -> size == 2;
                case "BIN4", "FLOAT4" -> size == 4;
                case "BIN8", "FLOAT8" -> size == 8;
                case "BOOLEAN" -> size == 1;
                case "PACKED", "ZONED" -> size <= 63;
                case "BINARY", "TEXT" -> true;
                case "VARBINARY", "VARCHAR" -> size <= Short.MAX_VALUE;
                case "VARGRAPHIC" -> size <= Short.MAX_VALUE / 2;
                case "DATE_ISO" -> size == 10;
                case "TIME_ISO" -> size == 8;
                case "TIMESTAMP_ISO" -> size == (scale == 0 ? 19 : 20 + scale);
                default -> false;
            }, "Unsupported durable codec");
        }

        AS400DataType decoder() {
            return switch (kind) {
                case "BIN2" -> new AS400Bin2();
                case "BIN4" -> new AS400Bin4();
                case "BIN8" -> new AS400Bin8();
                case "FLOAT4" -> new AS400Float4();
                case "FLOAT8" -> new AS400Float8();
                case "BOOLEAN" -> new AS400Boolean();
                case "PACKED" -> new AS400PackedDecimal(size, scale);
                case "ZONED" -> new AS400ZonedDecimal(size, scale);
                case "BINARY" -> new AS400ByteArray(size);
                case "VARBINARY" -> new AS400VarBin(size);
                case "TEXT" -> new AS400Text(size, ccsid);
                case "VARCHAR" -> new AS400VarChar(size, 1, ccsid);
                case "VARGRAPHIC" -> new AS400VarChar(size, 2, ccsid);
                case "DATE_ISO" -> new AS400Temporal(AS400Temporal.Kind.DATE, 0, ccsid);
                case "TIME_ISO" -> new AS400Temporal(AS400Temporal.Kind.TIME, 0, ccsid);
                case "TIMESTAMP_ISO" -> new AS400Temporal(AS400Temporal.Kind.TIMESTAMP, scale, ccsid);
                default -> throw new IllegalArgumentException("Unsupported durable codec");
            };
        }

        String encode() {
            return kind + ":" + size + ":" + scale + ":" + ccsid;
        }

        boolean temporal() {
            return kind.equals("DATE_ISO") || kind.equals("TIME_ISO") || kind.equals("TIMESTAMP_ISO");
        }

        boolean matches(int jdbcType) {
            return switch (kind) {
                case "BIN2" -> jdbcType == Types.SMALLINT;
                case "BIN4" -> jdbcType == Types.INTEGER;
                case "BIN8" -> jdbcType == Types.BIGINT;
                case "FLOAT4" -> jdbcType == Types.REAL;
                case "FLOAT8" -> jdbcType == Types.DOUBLE || jdbcType == Types.FLOAT;
                case "BOOLEAN" -> jdbcType == Types.BOOLEAN || jdbcType == Types.BIT;
                case "PACKED" -> jdbcType == Types.DECIMAL;
                case "ZONED" -> jdbcType == Types.NUMERIC;
                case "BINARY" -> jdbcType == Types.BINARY;
                case "VARBINARY" -> jdbcType == Types.VARBINARY;
                case "TEXT" -> jdbcType == Types.CHAR || jdbcType == Types.NCHAR;
                case "VARCHAR" -> jdbcType == Types.VARCHAR;
                case "VARGRAPHIC" -> jdbcType == Types.NVARCHAR;
                case "DATE_ISO" -> jdbcType == Types.DATE;
                case "TIME_ISO" -> jdbcType == Types.TIME;
                case "TIMESTAMP_ISO" -> jdbcType == Types.TIMESTAMP;
                default -> false;
            };
        }

        static Codec decode(String encoded) {
            String[] fields = encoded.split(":", -1);
            require(fields.length == 4, "Incomplete codec descriptor");
            var result = new Codec(fields[0], Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3]));
            require(result.encode().equals(encoded), "Noncanonical codec descriptor");
            return result;
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
