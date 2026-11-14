/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.SQLException;

/** A completed metadata observation does not satisfy the native capture contract. */
final class As400NativeEvidenceException extends SQLException {
    private final boolean absent;

    As400NativeEvidenceException(String message) {
        super(message);
        absent = false;
    }

    As400NativeEvidenceException(String message, Throwable cause) {
        super(message, cause);
        absent = false;
    }

    private As400NativeEvidenceException(String message, boolean absent) {
        super(message);
        this.absent = absent;
    }

    static As400NativeEvidenceException absentCatalog() {
        return new As400NativeEvidenceException("Required native catalog observation is absent", true);
    }

    boolean catalogAbsent() {
        return absent;
    }
}
