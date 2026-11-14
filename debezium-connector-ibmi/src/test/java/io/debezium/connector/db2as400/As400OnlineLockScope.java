/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Test-only complete current native scope observation. Matching samples do not prove a stable historical scope. */
public final class As400OnlineLockScope {
    static final int MAX_ROWS = 128;
    // QAWRKJRNA / QJOWRKJRNA is IBM's documented WRKJRNA output model.
    // Do not filter its rows: a foreign or unrecognized row must reject.
    // Native names in this isolated profile use the invariant EBCDIC repertoire.
    static final String SQL = """
            SELECT CAST(? AS VARCHAR(10)) AS REQUEST_LIBRARY, CAST(? AS VARCHAR(10)) AS REQUEST_JOURNAL,
                   TRIM(CAST(QJOJRNLIB AS CHAR(10) CCSID 37)) AS JOURNAL_LIBRARY,
                   TRIM(CAST(QJOJOURNAL AS CHAR(10) CCSID 37)) AS JOURNAL_NAME,
                   TRIM(CAST(QJOASP AS CHAR(10) CCSID 37)) AS ASP,
                   TRIM(CAST(QJOLIBRARY AS CHAR(10) CCSID 37)) AS OBJECT_LIBRARY,
                   TRIM(CAST(QJOOBJECT AS CHAR(10) CCSID 37)) AS OBJECT_NAME,
                   TRIM(CAST(QJOOBJTYPE AS CHAR(7) CCSID 37)) AS OBJECT_TYPE,
                   TRIM(CAST(QJOFILETYP AS CHAR(2) CCSID 37)) AS FILE_TYPE,
                   CAST(QJOJRNTYPE AS CHAR(1) CCSID 37) AS JOURNALED_TYPE,
                   HEX(QJOSYSTEM) AS SYSTEM_HEX, HEX(QJORELEASE) AS RELEASE_HEX,
                   HEX(QJOOBJTYPE) AS TYPE_HEX, HEX(QJOOBJECT) AS NAME_HEX,
                   HEX(QJOLIBRARY) AS LIBRARY_HEX, HEX(QJOJOURNAL) AS JOURNAL_HEX,
                   HEX(QJOJRNLIB) AS JOURNAL_LIBRARY_HEX, HEX(QJOASP) AS ASP_HEX,
                   HEX(QJOFILEID) AS FILE_ID_HEX, HEX(QJOPNM) AS PATH_HEX
            FROM QTEMP.DCGSCOPE ORDER BY QJOOBJTYPE, QJOLIBRARY, QJOOBJECT
            """;
    static final String COUNTS = """
            SELECT JOURNAL_LIBRARY, JOURNAL_NAME, JOURNAL_TYPE, JOURNAL_STATE,
                   JOURNALED_FILES, JOURNALED_MEMBERS, JOURNALED_LIBRARIES,
                   JOURNALED_DATA_AREAS, JOURNALED_DATA_QUEUES, JOURNALED_IFS_OBJECTS,
                   JOURNALED_ACCESS_PATHS, NUMBER_REMOTE_JOURNALS
            FROM QSYS2.JOURNAL_INFO WHERE JOURNAL_LIBRARY=? AND JOURNAL_NAME=?
            """;

    record Target(String library, String file, String member) {
    }

    static List<String> validate(JsonNode snapshot, String library) {
        JsonNode rows = snapshot.path("objects");
        require(library.matches("DCNC[0-9]{4}") && rows.isArray() && !rows.isEmpty() && rows.size() <= MAX_ROWS,
                "Bounded isolated native scope required");
        var targets = new ArrayList<String>();
        var objects = new HashSet<String>();
        int libraries = 0;
        require(snapshot.path("counts").isArray() && snapshot.path("counts").size() == 1, "Exact independent journal counts required");
        JsonNode first = snapshot.get("counts").get(0);
        require(text(first, "JOURNAL_LIBRARY").equals(library) && text(first, "JOURNAL_NAME").equals("QSQJRN")
                && text(first, "JOURNAL_TYPE").equals("*LOCAL") && text(first, "JOURNAL_STATE").equals("*ACTIVE"), "Wrong or inactive journal scope");
        var countNames = List.of("JOURNALED_FILES", "JOURNALED_MEMBERS", "JOURNALED_LIBRARIES", "JOURNALED_DATA_AREAS",
                "JOURNALED_DATA_QUEUES", "JOURNALED_IFS_OBJECTS", "JOURNALED_ACCESS_PATHS", "NUMBER_REMOTE_JOURNALS");
        for (JsonNode row : rows) {
            require(text(row, "JOURNAL_LIBRARY").equals(library) && text(row, "JOURNAL_NAME").equals("QSQJRN"), "Wrong object journal scope");
            require(text(row, "REQUEST_LIBRARY").equals(library) && text(row, "REQUEST_JOURNAL").equals("QSQJRN")
                    && text(row, "ASP").equals("1") && text(row, "JOURNALED_TYPE").equals("0"), "Unsupported native storage/request scope");
            String type = text(row, "OBJECT_TYPE"), objectLibrary = text(row, "OBJECT_LIBRARY"), name = text(row, "OBJECT_NAME");
            require(name.matches("[A-Z0-9_#$@]{1,10}") && objects.add(objectLibrary + "/" + name + "/" + type), "Missing or duplicate native object/member");
            switch (type) {
                case "*FILE" -> {
                    require(objectLibrary.equals(library) && text(row, "FILE_TYPE").equals("PF"), "Unsupported or incomplete journaled file");
                    targets.add(name);
                }
                case "*LIB" -> {
                    require(objectLibrary.equals("QSYS") && name.equals(library),
                            "Foreign or ambiguous journaled library");
                    libraries++;
                }
                default -> throw new IllegalArgumentException("Unsupported external journaled object: " + type);
            }
        }
        require(!targets.isEmpty() && targets.size() <= 50 && targets.size() == number(first, "JOURNALED_FILES")
                && targets.size() == number(first, "JOURNALED_MEMBERS") && libraries == number(first, "JOURNALED_LIBRARIES"),
                "Native file/member inventory is incomplete or exceeds allocation bound");
        for (String name : countNames.subList(3, countNames.size())) {
            require(number(first, name) == 0, "Unsupported or hidden journal resource: " + name);
        }
        return targets.stream().sorted().toList();
    }

    static List<Target> targets(JsonNode objects, JsonNode members, String library) {
        var files = validate(objects, library);
        require(members.isArray() && members.size() == files.size(), "Complete bounded member observations required");
        var result = new ArrayList<Target>();
        var seen = new HashSet<String>();
        for (JsonNode member : members) {
            String file = text(member, "SYSTEM_TABLE_NAME"), name = text(member, "SYSTEM_TABLE_MEMBER");
            require(text(member, "SYSTEM_TABLE_SCHEMA").equals(library) && files.contains(file) && seen.add(file)
                    && name.matches("[A-Z0-9_#$@]{1,10}"), "Missing, duplicate or foreign native member");
            result.add(new Target(library, file, name));
        }
        return result.stream().sorted(Comparator.comparing(Target::file)).toList();
    }

    static void same(JsonNode expected, JsonNode observed, String library) {
        require(validate(expected, library).equals(validate(observed, library)) && expected.equals(observed), "Native scope changed across the lock interval");
    }

    private static String text(JsonNode node, String field) {
        require(node.hasNonNull(field) && node.get(field).isTextual(), "Missing native scope field: " + field);
        return node.get(field).textValue().trim();
    }

    private static int number(JsonNode node, String field) {
        require(node.hasNonNull(field) && (node.get(field).isIntegralNumber() || node.get(field).isTextual())
                && node.get(field).asText().matches("[0-9]{1,9}"), "Missing or invalid native scope count: " + field);
        return Integer.parseInt(node.get(field).asText());
    }

    private static void require(boolean value, String message) {
        if (!value) {
            throw new IllegalArgumentException(message);
        }
    }

    public static void main(String[] args) throws Exception {
        require(args.length == 3, "library raw-scope.json new-targets.json required");
        var source = Path.of(args[1]);
        require(Files.size(source) <= 1024 * 1024 && !Files.exists(Path.of(args[2])), "Bounded input and new output required");
        var json = new ObjectMapper();
        var input = json.readTree(source.toFile());
        json.writerWithDefaultPrettyPrinter().writeValue(Path.of(args[2]).toFile(),
                Map.of("targets", targets(input.path("scope"), input.path("members"), args[0]), "startup_proof", false));
    }
}
