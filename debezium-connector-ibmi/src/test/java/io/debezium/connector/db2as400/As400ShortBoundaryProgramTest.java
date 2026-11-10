/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;

class As400ShortBoundaryProgramTest {
    @Test
    void rendersTheExactProgramThatWasExecutedOnTheNativeServer() throws Exception {
        try (var stream = getClass().getResourceAsStream("/native-transactions/prospective-boundary.json")) {
            var captured = new ObjectMapper().readTree(stream).path("boundary");
            String library = captured.path("request").path("library").asText();
            var targets = java.util.stream.StreamSupport.stream(captured.path("request").path("targets").spliterator(), false)
                    .map(t -> new As400ShortBoundaryProgram.Target(library, t.get(0).asText(), t.get(1).asText())).toList();
            var program = new As400ShortBoundaryProgram(new JournalReceiver("QSQJRN", library), targets,
                    captured.path("request").path("nonce").asText(), captured.path("pulse").path("result_rows").get(0).path("SCOPE_DIGEST").asText());
            assertThat(program.statement()).isEqualTo(captured.path("statement").asText());
            assertThat(program.programDigest()).isEqualTo(captured.path("pulse").path("program_sha256").asText());
            assertThat(program.opening()).contains("FORCE(*NO)");
            assertThat(program.closing()).contains("FORCE(*YES)");
            assertThat(program.statement()).doesNotContain("SELECT", "FORCE(*YES)", "COMMIT", "ROLLBACK");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "*ALL", "FILE MBR", "FILE)", "FILE'", "FILE;", "../FILE", "TOOLONGNAME1", "" })
    void doesNotInterpolateSelectorsOrCommandsIntoNativeAllocation(String file) {
        assertThrows(IllegalArgumentException.class, () -> new As400ShortBoundaryProgram.Target("LIB", file, "DATA"));
    }

    @Test
    void duplicatePhysicalFilesCannotMasqueradeAsSingleMemberScope() {
        var first = new As400ShortBoundaryProgram.Target("LIB", "FILE", "FIRST");
        var second = new As400ShortBoundaryProgram.Target("LIB", "FILE", "SECOND");
        assertThrows(IllegalArgumentException.class, () -> new As400ShortBoundaryProgram(new JournalReceiver("JRN", "JLIB"),
                List.of(first, second), "A".repeat(32), "B".repeat(64)));
    }
}
