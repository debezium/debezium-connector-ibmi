/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.ibmi.db2.journal.test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400JDBCConnectionForcedCcsid;
import com.ibm.as400.access.AS400JDBCDriverRegistration;

import io.debezium.ibmi.db2.journal.retrieve.Connect;

public class TestConnector {
    private static final Logger log = LoggerFactory.getLogger(TestConnector.class);
    private Connect<AS400, IOException> as400Connect;
    private Connect<Connection, SQLException> sqlConnect;
    private final String schema;
    private final Properties props;
    private final String url;

    public TestConnector() throws SQLException {
        this(System.getenv("ISERIES_SCHEMA"));
    }

    public TestConnector(String schema) throws SQLException {
        this.schema = schema;
        final String user = System.getenv("ISERIES_USER");
        final String password = System.getenv("ISERIES_PASSWORD");
        final String host = System.getenv("ISERIES_HOST");
        final String fromCcsid = System.getenv("FROM_CCSID");
        final String toCcsid = System.getenv("TO_CCSID");

        url = String.format("jdbc:as400://%s", host);

        props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        if (fromCcsid != null && toCcsid != null) {
            AS400JDBCDriverRegistration.registerCcsidDriver();
            props.setProperty(AS400JDBCConnectionForcedCcsid.FROM_CCSID, fromCcsid);
            props.setProperty(AS400JDBCConnectionForcedCcsid.TO_CCSID, toCcsid);
            log.info("forced ccsid {}", toCcsid);
        }

        this.as400Connect = new Connect<>() {
            AS400 as400 = new AS400(host, user, password.toCharArray());

            @Override
            public AS400 connection() {
                return as400;
            }
        };

        this.sqlConnect = new Connect<>() {
            Connection con = DriverManager.getConnection(url, props);

            @Override
            public Connection connection() throws SQLException {
                return con;
            }

        };
    }

    public Connect<AS400, IOException> getAs400() {
        return as400Connect;
    }

    public Connect<Connection, SQLException> getJdbc() {
        return sqlConnect;
    }

    /**
     * used for testing with multiple connections
     *
     * @return
     * @throws SQLException
     */
    public Connection getNewJdbcConnection() throws SQLException {
        return DriverManager.getConnection(url, props);
    }

    public String getSchema() {
        return this.schema;
    }
}
