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
 * must not be converted into successful initialization or a blind DLCOBJ retry.
 */
record As400ShortBoundaryProgram(JournalReceiver journal, List<Target> targets, String nonce, String scopeDigest) {

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
        require(!targets.isEmpty() && targets.size() <= 50
                && targets.equals(targets.stream().sorted(Comparator.comparing(Target::library).thenComparing(Target::file)).toList())
                && targets.stream().map(t -> t.library() + "/" + t.file()).distinct().count() == targets.size(),
                "One ordered bounded single-member scope required");
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

    String statement() {
        String objects = targets.stream().map(t -> "(" + t.library() + "/" + t.file() + " *FILE *EXCLRD " + t.member() + ")")
                .collect(Collectors.joining(" "));
        String allocate = "ALCOBJ OBJ(" + objects + ") WAIT(1) CONFLICT(*RQSRLS) SCOPE(*JOB)";
        String release = "DLCOBJ OBJ(" + objects + ") SCOPE(*JOB)";
        var lines = new ArrayList<>(List.of("BEGIN NOT ATOMIC", "DECLARE HELD SMALLINT DEFAULT 0;", "DECLARE FAILED SMALLINT DEFAULT 0;",
                "DECLARE RELEASE_FAILED SMALLINT DEFAULT 0;", "DECLARE ACQUIRED TIMESTAMP(6);", "DECLARE RELEASED TIMESTAMP(6);", "BEGIN"));
        lines.addAll(handlers("FAILED"));
        lines.addAll(List.of("CALL QSYS2.QCMDEXC(" + literal(allocate) + ");", "SET HELD=1;", "SET ACQUIRED=CURRENT_TIMESTAMP;",
                "CALL QSYS2.QCMDEXC(" + literal(marker(payload(), false)) + ");", "END;", "IF HELD=1 THEN", "BEGIN"));
        lines.addAll(handlers("RELEASE_FAILED"));
        lines.addAll(List.of("CALL QSYS2.QCMDEXC(" + literal(release) + ");", "SET HELD=0;", "SET RELEASED=CURRENT_TIMESTAMP;", "END;", "END IF;",
                "IF FAILED=0 AND RELEASE_FAILED=0 THEN", "BEGIN"));
        lines.addAll(handlers("FAILED"));
        lines.addAll(List.of("INSERT INTO SESSION.DCGPULSE VALUES(" + literal(nonce) + ", " + literal(scopeDigest) + ", ACQUIRED, RELEASED);", "END;", "END IF;",
                "IF FAILED=1 OR RELEASE_FAILED=1 THEN", "SIGNAL SQLSTATE '75001' SET MESSAGE_TEXT='Pulse failed; independent drain required';", "END IF;", "END"));
        return String.join("\n", lines);
    }

    String programDigest() {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(statement().getBytes(StandardCharsets.US_ASCII)));
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
