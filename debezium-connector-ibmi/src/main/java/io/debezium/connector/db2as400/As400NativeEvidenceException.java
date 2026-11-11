/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.SQLException;

/** A completed metadata observation does not satisfy the native capture contract. */
final class As400NativeEvidenceException extends SQLException {
    As400NativeEvidenceException(String message) {
        super(message);
    }

    As400NativeEvidenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
