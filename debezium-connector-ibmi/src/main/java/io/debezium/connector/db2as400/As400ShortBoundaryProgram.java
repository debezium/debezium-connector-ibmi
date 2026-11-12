/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Collectors;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

/**
 * Minimal server-owned allocate/marker/release statement. This describes an
 * attempt, not a certificate: native scope, metadata, history, marker bytes and
 * completed holder drain must be validated separately. No metadata query runs
 * while the allocations are held. An ambiguous allocation or failed release
 * must not be converted into successful initialization. The caller must establish
 * that the dedicated job has no matching allocations before invoking the program.
 */
record As400ShortBoundaryProgram(JournalReceiver journal, List<Target> targets, String nonce, String scopeDigest) {

    private static final int OBJECTS_PER_COMMAND = 50;
    static final String PREPARE_BATCHES_SQL = "DECLARE GLOBAL TEMPORARY TABLE SESSION.DCGBATCH "
            + "(BATCH INTEGER NOT NULL, HELD SMALLINT NOT NULL, ALLOCATE_COMMAND VARCHAR(4096) NOT NULL, "
            + "RELEASE_COMMAND VARCHAR(4096) NOT NULL) ON COMMIT PRESERVE ROWS NOT LOGGED";

    static final String INSERT_BATCH_SQL = "INSERT INTO SESSION.DCGBATCH VALUES(?, 0, ?, ?)";

    static final String PREPARE_SQL = "DECLARE GLOBAL TEMPORARY TABLE SESSION.DCGPULSE "
            + "(ATTEMPT CHAR(32), SCOPE_DIGEST CHAR(64), ACQUIRED TIMESTAMP(6), RELEASED TIMESTAMP(6)) "
            + "ON COMMIT PRESERVE ROWS NOT LOGGED";

    record Target(String library, String file, String member) {
        Target {
            name(library);
            name(file);
            require(member != null && member.matches("[A-Z0-9_#$@]{1,10}"), "Exact native member required");
        }
    }

    As400ShortBoundaryProgram {
        name(journal.library());
        name(journal.name());
        targets = List.copyOf(targets);
        require(!targets.isEmpty()
                && targets.equals(targets.stream().sorted(Comparator.comparing(Target::library).thenComparing(Target::file).thenComparing(Target::member)).toList())
                && targets.stream().distinct().count() == targets.size(),
                "One ordered complete file/member scope required");
        require(nonce != null && nonce.matches("[A-F0-9]{32}") && scopeDigest != null && scopeDigest.matches("[A-F0-9]{64}"),
                "Fresh attempt and complete scope digest required");
    }

    String payload() {
        return "DCGB1 " + nonce + " N " + scopeDigest;
    }

    String opening() {
        return opening(journal, nonce);
    }

    static String opening(JournalReceiver journal, String nonce) {
        name(journal.library());
        name(journal.name());
        require(nonce != null && nonce.matches("[A-F0-9]{32}"), "Fresh attempt required");
        return "SNDJRNE JRN(" + journal.library() + "/" + journal.name() + ") TYPE(DN) ENTDTA('DCGB1 " + nonce
                + " A') FILE(*NONE) FORCE(*NO) OVRSTATE(*NONE) OVRRMTFTR(*NONE)";
    }

    String closing() {
        return marker("DCGB1 " + nonce + " Z " + scopeDigest, true);
    }

    private String marker(String payload, boolean force) {
        return "SNDJRNE JRN(" + journal.library() + "/" + journal.name() + ") TYPE(DN) ENTDTA(" + literal(payload)
                + ") FILE(*NONE) FORCE(" + (force ? "*YES" : "*NO") + ") OVRSTATE(*NONE) OVRRMTFTR(*NONE)";
    }

    String release() {
        require(!batched(), "Batched allocations require their job-local release ledger");
        String objects = targets.stream().map(t -> "(" + t.library() + "/" + t.file() + " *FILE *EXCLRD " + t.member() + ")")
                .collect(Collectors.joining(" "));
        return "DLCOBJ OBJ(" + objects + ") SCOPE(*JOB)";
    }

    String statement() {
        if (batched()) {
            return batchedStatement();
        }
        String objects = targets.stream().map(t -> "(" + t.library() + "/" + t.file() + " *FILE *EXCLRD " + t.member() + ")")
                .collect(Collectors.joining(" "));
        String allocate = "ALCOBJ OBJ(" + objects + ") WAIT(1) CONFLICT(*RQSRLS) SCOPE(*JOB)";
        String release = release();
        var lines = new ArrayList<>(List.of("BEGIN NOT ATOMIC", "DECLARE HELD SMALLINT DEFAULT 0;", "DECLARE FAILED SMALLINT DEFAULT 0;",
                "DECLARE RELEASE_FAILED SMALLINT DEFAULT 0;", "DECLARE ACQUIRED TIMESTAMP(6);", "DECLARE RELEASED TIMESTAMP(6);", "BEGIN"));
        lines.addAll(handlers("FAILED"));
        // Set before the native call: warnings/errors can arrive after a partial
        // allocation. The dedicated holder was independently checked for locks.
        lines.addAll(List.of("SET HELD=1;", "CALL QSYS2.QCMDEXC(" + literal(allocate) + ");", "SET ACQUIRED=CURRENT_TIMESTAMP;",
                "CALL QSYS2.QCMDEXC(" + literal(marker(payload(), false)) + ");", "END;"));
        for (int attempt = 0; attempt < 3; attempt++) {
            lines.addAll(List.of("IF HELD=1 THEN", "SET RELEASE_FAILED=0;", "BEGIN"));
            lines.addAll(handlers("RELEASE_FAILED"));
            lines.addAll(List.of("CALL QSYS2.QCMDEXC(" + literal(release) + ");", "SET HELD=0;", "SET RELEASED=CURRENT_TIMESTAMP;", "END;",
                    "IF RELEASE_FAILED=1 THEN", "SET FAILED=1;", "END IF;", "END IF;"));
        }
        lines.addAll(List.of("IF FAILED=0 AND HELD=0 THEN", "BEGIN"));
        lines.addAll(handlers("FAILED"));
        lines.addAll(List.of("INSERT INTO SESSION.DCGPULSE VALUES(" + literal(nonce) + ", " + literal(scopeDigest) + ", ACQUIRED, RELEASED);", "END;", "END IF;",
                "IF HELD=1 THEN", "SIGNAL SQLSTATE '75002' SET MESSAGE_TEXT='Pulse release failed; independent drain required';", "END IF;",
                "IF FAILED=1 THEN", "SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT='Pulse failed; independent drain required';", "END IF;", "END"));
        return String.join("\n", lines);
    }

    boolean batched() {
        return targets.size() > OBJECTS_PER_COMMAND;
    }

    record Allocation(String allocate, String release) {
    }

    int batchCount() {
        return (targets.size() - 1) / OBJECTS_PER_COMMAND + 1;
    }

    Allocation allocationBatch(int index) {
        require(index >= 0 && index < batchCount(), "Unknown allocation group");
        int start = index * OBJECTS_PER_COMMAND;
        String objects = targets.subList(start, start + Math.min(OBJECTS_PER_COMMAND, targets.size() - start)).stream()
                .map(t -> "(" + t.library() + "/" + t.file() + " *FILE *EXCLRD " + t.member() + ")").collect(Collectors.joining(" "));
        return new Allocation("ALCOBJ OBJ(" + objects + ") WAIT(" + (index == 0 ? 1 : 0) + ") CONFLICT(*RQSRLS) SCOPE(*JOB)",
                "DLCOBJ OBJ(" + objects + ") SCOPE(*JOB)");
    }

    private String batchedStatement() {
        var lines = new ArrayList<>(List.of("BEGIN NOT ATOMIC", "DECLARE FAILED SMALLINT DEFAULT 0;",
                "DECLARE RELEASE_FAILED SMALLINT DEFAULT 0;", "DECLARE STARTED TIMESTAMP(6);", "DECLARE ACQUIRED TIMESTAMP(6);",
                "DECLARE RELEASED TIMESTAMP(6);"));
        // The caller has populated the job-local command ledger before this
        // statement is submitted. Compilation and SQL text size do not grow
        // with the number of files; no catalog query runs while locks are held.
        lines.add("IF (SELECT COUNT(*) FROM SESSION.DCGBATCH) <> " + batchCount() + " THEN");
        lines.add("SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT='Incomplete allocation ledger';");
        lines.add("END IF;");
        lines.add("BEGIN");
        lines.addAll(handlers("FAILED"));
        lines.addAll(List.of("SET STARTED=CURRENT_TIMESTAMP;",
                "FOR ALLOCATION AS AC CURSOR FOR SELECT BATCH AS BATCH_ID, ALLOCATE_COMMAND FROM SESSION.DCGBATCH ORDER BY BATCH DO",
                "IF CURRENT_TIMESTAMP > STARTED + 1 SECOND THEN",
                "SIGNAL SQLSTATE '75003' SET MESSAGE_TEXT='Allocation budget exhausted';", "END IF;",
                "UPDATE SESSION.DCGBATCH SET HELD=1 WHERE BATCH=ALLOCATION.BATCH_ID;",
                "CALL QSYS2.QCMDEXC(ALLOCATION.ALLOCATE_COMMAND);", "END FOR;"));
        lines.addAll(List.of("IF CURRENT_TIMESTAMP > STARTED + 1 SECOND THEN",
                "SIGNAL SQLSTATE '75003' SET MESSAGE_TEXT='Allocation budget exhausted';", "END IF;",
                // Conservatively include time spent acquiring the earlier groups
                // in the completed hold bound, not just the final all-held interval.
                "SET ACQUIRED=STARTED;", "CALL QSYS2.QCMDEXC(" + literal(marker(payload(), false)) + ");", "END;"));
        lines.addAll(batchRelease());
        lines.addAll(List.of("SET RELEASED=CURRENT_TIMESTAMP;", "IF FAILED=1 THEN",
                "SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT='Pulse failed; independent drain required';", "END IF;",
                "INSERT INTO SESSION.DCGPULSE VALUES(" + literal(nonce) + ", " + literal(scopeDigest) + ", ACQUIRED, RELEASED);", "END"));
        return String.join("\n", lines);
    }

    String retryBatchRelease() {
        require(batched(), "Batch release requires a batch ledger");
        var lines = new ArrayList<>(List.of("BEGIN NOT ATOMIC", "DECLARE FAILED SMALLINT DEFAULT 0;", "DECLARE RELEASE_FAILED SMALLINT DEFAULT 0;"));
        lines.addAll(batchRelease());
        lines.add("END");
        return String.join("\n", lines);
    }

    private List<String> batchRelease() {
        var lines = new ArrayList<String>();
        lines.add("FOR RELEASING AS RC CURSOR FOR SELECT BATCH AS BATCH_ID, RELEASE_COMMAND FROM SESSION.DCGBATCH ORDER BY BATCH DESC DO");
        for (int attempt = 0; attempt < 3; attempt++) {
            lines.addAll(List.of("SET RELEASE_FAILED=0;", "BEGIN"));
            lines.addAll(handlers("RELEASE_FAILED"));
            lines.addAll(List.of("IF EXISTS (SELECT 1 FROM SESSION.DCGBATCH WHERE BATCH=RELEASING.BATCH_ID AND HELD=1) THEN",
                    "CALL QSYS2.QCMDEXC(RELEASING.RELEASE_COMMAND);",
                    "UPDATE SESSION.DCGBATCH SET HELD=0 WHERE BATCH=RELEASING.BATCH_ID;", "END IF;", "END;",
                    "IF RELEASE_FAILED=1 THEN", "SET FAILED=1;", "END IF;"));
        }
        lines.add("END FOR;");
        lines.addAll(List.of("IF EXISTS (SELECT 1 FROM SESSION.DCGBATCH WHERE HELD=1) THEN",
                "SIGNAL SQLSTATE '75002' SET MESSAGE_TEXT='Pulse release failed; independent drain required';", "END IF;"));
        return lines;
    }

    String programDigest() {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            digest.update(statement().getBytes(StandardCharsets.US_ASCII));
            if (batched()) {
                // Bind the prepared command data as well as the constant SQL.
                for (int i = 0; i < batchCount(); i++) {
                    var batch = allocationBatch(i);
                    digest.update(("\n" + batch.allocate() + "\n" + batch.release()).getBytes(StandardCharsets.US_ASCII));
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        }
        catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String literal(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    private static List<String> handlers(String flag) {
        return List.of("SQLEXCEPTION", "SQLWARNING", "NOT FOUND").stream().map(c -> "DECLARE EXIT HANDLER FOR " + c + " SET " + flag + "=1;").toList();
    }

    private static void name(String value) {
        require(value != null && value.matches("[A-Z#$@][A-Z0-9_#$@]{0,9}"), "Exact basic native object name required");
    }

    private static void require(boolean valid, String reason) {
        if (!valid) {
            throw new IllegalArgumentException(reason);
        }
    }
}
