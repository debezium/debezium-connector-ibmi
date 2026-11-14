/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;

/** Compatibility entry point for retained fixture comparisons. */
final class As400JournalCaptureContent {
    static String digest(byte[] raw, JournalPosition position) throws Exception {
        return As400JournalRecordContent.digest(raw, position);
    }
}
