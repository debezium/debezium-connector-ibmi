/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.Connection;
import java.sql.SQLException;

/** Opens a new owned JDBC connection with a finite connection-establishment timeout. */
@FunctionalInterface
interface As400JdbcConnectionFactory {
    Connection open() throws SQLException;
}
