/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;

import io.debezium.ibmi.db2.journal.retrieve.RetrievalCriteria.JournalCode;

public class RetrieveConfigBuilder {

    private static final Logger log = LoggerFactory.getLogger(RetrieveConfigBuilder.class);

    private Connect<AS400, IOException> as400;
    private JournalInfo journalInfo;
    private File dumpFolder;
    private int journalBufferSize = ParameterListBuilder.DEFAULT_JOURNAL_BUFFER_SIZE;
    private JournalCode[] filterCodes = new JournalCode[]{};
    private List<FileFilter> includeFiles = Collections.<FileFilter> emptyList();
    private int maxServerSideEntries = RetrieveConfig.DEFAULT_MAX_SERVER_SIDE_ENTRIES;
    private boolean filtering;
    private long journalCacheDelay = 35000;
    private long pollInterval = 2000;

    public RetrieveConfigBuilder() {
    }

    public RetrieveConfigBuilder withAs400(Connect<AS400, IOException> as400) {
        this.as400 = as400;
        return this;
    }

    public RetrieveConfigBuilder withJournalInfo(JournalInfo journalInfo) {
        this.journalInfo = journalInfo;
        return this;
    }

    public RetrieveConfigBuilder withDumpFolder(String dumpFolder) {
        if (dumpFolder != null && !dumpFolder.isBlank()) {
            final File f = new File(dumpFolder);
            if (f.exists()) {
                this.dumpFolder = f;
                return this;
            }
            else {
                log.error("ignoring dump folder {} as it doesn't exist", dumpFolder);
            }
        }
        return this;
    }

    public RetrieveConfigBuilder withJournalBufferSize(int journalBufferSize) {
        this.journalBufferSize = journalBufferSize;
        return this;
    }

    public RetrieveConfigBuilder withFilterCodes(JournalCode[] filterCodes) {
        if (filterCodes == null) {
            this.filterCodes = new JournalCode[]{};
        }
        else {
            this.filterCodes = filterCodes;
        }
        return this;
    }

    public RetrieveConfigBuilder withServerFiltering(boolean filtering) {
        this.filtering = filtering;
        return this;
    }

    public RetrieveConfigBuilder withIncludeFiles(List<FileFilter> includeFiles) {
        if (includeFiles != null) {
            if (includeFiles.size() < 300) {
                this.includeFiles = includeFiles;
            }
            else {
                log.error("ignoring filter list as too many files included {} limit 300", includeFiles.size());
                this.includeFiles = Collections.<FileFilter> emptyList();
            }
        }
        else {
            this.includeFiles = Collections.<FileFilter> emptyList();
        }

        return this;
    }

    public RetrieveConfigBuilder withMaxServerSideEntries(Integer maxServerSideEntries) {
        if (maxServerSideEntries != null) {
            this.maxServerSideEntries = maxServerSideEntries.intValue();
        }
        return this;
    }

    public RetrieveConfigBuilder withJournalCacheDelay(long journalCacheDelay) {
        this.journalCacheDelay = journalCacheDelay;
        return this;
    }

    public RetrieveConfigBuilder withPollInterval(long pollInterval) {
        this.pollInterval = pollInterval;
        return this;
    }

    public RetrieveConfig build() {
        return new RetrieveConfig(as400, journalInfo, journalBufferSize, filtering, filterCodes, includeFiles, maxServerSideEntries, dumpFolder, journalCacheDelay,
                pollInterval);
    }
}
