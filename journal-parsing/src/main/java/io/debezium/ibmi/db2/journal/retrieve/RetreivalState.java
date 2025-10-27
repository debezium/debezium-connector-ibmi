/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.retrieve;

public enum RetreivalState {
    Success,
    MoreDataAvailable,
    NotCalled;

    public boolean hasData() {
        return this == Success || this == MoreDataAvailable;
    }
}