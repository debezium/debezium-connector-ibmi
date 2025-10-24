/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.RetrievalCriteria.JournalCode;

public record RetrieveConfig(Connect<AS400, IOException> as400, JournalInfo journalInfo, int journalBufferSize,
        boolean filtering, JournalCode[] filterCodes, List<FileFilter> includeFiles, int maxServerSideEntries,
        File dumpFolder, long journalCacheDelay, long pollInterval) {

    public static final int DEFAULT_MAX_SERVER_SIDE_ENTRIES = 1000000;
}
