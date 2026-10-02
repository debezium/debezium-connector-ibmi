/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.ibm.as400.access.AS400;

@ExtendWith(MockitoExtension.class)
class JournalInfoRetrievalGetJournalTest {

    @Mock
    AS400 as400;

    private final JournalInfo journalA = new JournalInfo("JRN", "JRNLIB", false);
    private final JournalInfo journalB = new JournalInfo("OTHERJRN", "JRNLIB", false);

    @Test
    void multipleLibrariesSharingOneJournalResolveToSingleJournal() throws Exception {
        // Given two tables in two different libraries that both journal to JRN
        final JournalInfoRetrieval retrieval = spy(new JournalInfoRetrieval(0L, 0L, 0L));
        doReturn(journalA).when(retrieval).getJournal(as400, "LIB1", "T1");
        doReturn(journalA).when(retrieval).getJournal(as400, "LIB2", "T2");

        // When resolving the journal for the include list
        final JournalInfo result = retrieval.getJournal(as400, "LIB1",
                List.of(new FileFilter("LIB1", "T1"), new FileFilter("LIB2", "T2")));

        // Then the shared journal is returned without error
        assertEquals(journalA, result);
    }

    @Test
    void multipleLibrariesWithDistinctJournalsFailWithExplicitMessage() throws Exception {
        // Given two tables in two libraries that journal to different journals
        final JournalInfoRetrieval retrieval = spy(new JournalInfoRetrieval(0L, 0L, 0L));
        doReturn(journalA).when(retrieval).getJournal(as400, "LIB1", "T1");
        doReturn(journalB).when(retrieval).getJournal(as400, "LIB2", "T2");

        // When resolving the journal for the include list
        final IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> retrieval.getJournal(as400, "LIB1",
                        List.of(new FileFilter("LIB1", "T1"), new FileFilter("LIB2", "T2"))));

        // Then the failure clearly names the multi-journal situation and the libraries involved
        assertTrue(ex.getMessage().contains("more than one journal"), ex.getMessage());
        assertTrue(ex.getMessage().contains("LIB1"), ex.getMessage());
        assertTrue(ex.getMessage().contains("LIB2"), ex.getMessage());
    }
}
