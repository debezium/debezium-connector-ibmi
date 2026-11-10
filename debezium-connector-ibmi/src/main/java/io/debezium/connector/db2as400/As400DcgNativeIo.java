/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.SecureAS400;
import com.ibm.as400.access.SocketProperties;

import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.JournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.relational.history.SchemaHistory;

/** Native handles with terminal admission shared by JDBC and journal retrieval. */
final class As400DcgNativeIo implements AutoCloseable {
    final JournalReadLifecycle lifecycle = new JournalReadLifecycle();
    final RetrieveJournal reader;
    final As400OperatorTableEvidence tables;
    final As400NativeBindingAcquisition acquisition;
    final As400NativeTransactionCensus census;
    final As400OperatorJournalEvidence journals;
    final As400JdbcNativeIdentityEvidence identities;
    final As400BindingHistory history;
    final As400OperatorCutReader.Limits cutLimits = new As400OperatorCutReader.Limits(128, 10000, 16_000_000);
    private final Configuration raw;
    private final String server;
    private final AS400 transport;
    private final boolean secure;
    private volatile Exception failedConnectionCleanup;

    As400DcgNativeIo(As400DcgConfig config) throws Exception {
        this(config.raw, config.server, config.journal, config.rrnKeys, config.policy, config.historyTopic, config.prefix);
    }

    /** Shared native handles; callers must separately qualify startup and publication. */
    As400DcgNativeIo(Configuration raw, String server, JournalReceiver journal, boolean rrnKeys,
                     As400CaptureFingerprint.Policy policy, String historyTopic, String prefix)
            throws Exception {
        this.raw = raw;
        this.server = server;
        secure = raw.getBoolean("secure", true);
        String host = As400DcgConfig.required(raw, "database.hostname");
        String user = As400DcgConfig.required(raw, "database.user");
        String password = As400DcgConfig.required(raw, "database.password");
        transport = secure ? new SecureAS400(host, user, password) : new AS400(host, user, password);
        SocketProperties sockets = new SocketProperties();
        sockets.setSoTimeout(15000);
        sockets.setLoginTimeout(15000);
        transport.setSocketProperties(sockets);
        transport.setThreadUsed(false);
        reader = new RetrieveJournal(new RetrieveConfigBuilder().withAs400(() -> transport)
                .withJournalInfo(new JournalInfo(journal.name(), journal.library(), false))
                .withTransactionControls(true).withServerFiltering(false).withJournalBufferSize(1024 * 1024).withMaxServerSideEntries(1000).build(),
                new JournalInfoRetrieval(0, 0, 0), lifecycle);
        var tableLimits = new As400OperatorTableEvidence.Limits(100, 5);
        tables = new As400OperatorTableEvidence(() -> connection(false), tableLimits);
        acquisition = new As400NativeBindingAcquisition(() -> connection(false), tables, tableLimits, rrnKeys);
        census = new As400NativeTransactionCensus(() -> connection(false), 1024, 5);
        journals = new As400OperatorJournalEvidence(() -> connection(false), journal, new As400OperatorJournalEvidence.Limits(128, 5), true);
        identities = new As400JdbcNativeIdentityEvidence(() -> connection(false), new As400JdbcNativeIdentityEvidence.Limits(220, 5, 65536));
        var historyConfig = raw.edit().with(SchemaHistory.NAME, historyTopic)
                .with(SchemaHistory.INTERNAL_CONNECTOR_CLASS, As400RpcConnector.class.getName())
                .with(SchemaHistory.INTERNAL_CONNECTOR_ID, prefix).build();
        history = new As400BindingHistory(As400KafkaBindingHistory::new, historyConfig, policy.sourcePartition());
    }

    Connection connection(boolean transaction) throws SQLException {
        JournalReadLifecycle.Lease lease;
        try {
            lease = lifecycle.enter();
        }
        catch (java.io.IOException error) {
            throw new SQLException("Native D/CG session stopped", error);
        }
        Connection opened = null;
        try {
            Properties p = new Properties();
            p.setProperty("user", raw.getString("database.user"));
            p.setProperty("password", raw.getString("database.password"));
            p.setProperty("secure", Boolean.toString(secure));
            p.setProperty("prompt", "false");
            p.setProperty("socket timeout", "15000");
            p.setProperty("login timeout", "15");
            p.setProperty("thread used", "false");
            p.setProperty("lazy close", "false");
            opened = new com.ibm.as400.access.AS400JDBCDriver().connect("jdbc:as400://" + raw.getString("database.hostname"), p);
            lifecycle.checkOpen();
            Connection c = (Connection) guard(opened, lifecycle, lease);
            c.setTransactionIsolation(Connection.TRANSACTION_NONE);
            try (var st = c.createStatement()) {
                st.setQueryTimeout(5);
                st.setMaxRows(2);
                try (var rows = st.executeQuery("SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")) {
                    if (!rows.next() || !server.equals(rows.getString(1).trim()) || rows.next()) {
                        throw new SQLException("IBM i relational database identity differs from database.dbname");
                    }
                    noWarnings(rows, st);
                }
            }
            if (transaction) {
                c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                c.setAutoCommit(false);
            }
            return c;
        }
        catch (Exception error) {
            if (opened != null) {
                try {
                    opened.close();
                }
                catch (Exception close) {
                    error.addSuppressed(close);
                    failedConnectionCleanup = close;
                }
            }
            lease.close();
            if (error instanceof SQLException sql) {
                throw sql;
            }
            throw new SQLException("Cannot establish qualified native D/CG connection", error);
        }
    }

    static Object guard(Object delegate, JournalReadLifecycle lifecycle, JournalReadLifecycle.Lease connectionLease) {
        Class<?> type = delegate instanceof Connection ? Connection.class
                : delegate instanceof CallableStatement ? CallableStatement.class
                        : delegate instanceof PreparedStatement ? PreparedStatement.class
                                : delegate instanceof Statement ? Statement.class
                                        : delegate instanceof DatabaseMetaData ? DatabaseMetaData.class
                                                : delegate instanceof ResultSetMetaData ? ResultSetMetaData.class
                                                        : delegate instanceof ParameterMetaData ? ParameterMetaData.class : ResultSet.class;
        return Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{ type }, (proxy, method, args) -> {
            if (method.getName().equals("unwrap")) {
                throw new SQLException("Raw native handles cannot escape lifecycle admission");
            }
            boolean cleanup = List.of("close", "rollback", "isClosed").contains(method.getName())
                    || (delegate instanceof Statement && method.getName().equals("cancel"));
            JournalReadLifecycle.Lease call = null;
            boolean completed = false;
            try {
                if (!cleanup) {
                    call = lifecycle.enter();
                }
                Object result = method.invoke(delegate, args);
                completed = true;
                if (result instanceof Statement || result instanceof ResultSet || result instanceof DatabaseMetaData
                        || result instanceof ResultSetMetaData || result instanceof ParameterMetaData) {
                    return guard(result, lifecycle, null);
                }
                if (result instanceof Connection) {
                    throw new SQLException("Raw native connection cannot escape lifecycle admission");
                }
                return result;
            }
            catch (InvocationTargetException error) {
                throw error.getCause();
            }
            catch (java.io.IOException error) {
                throw new SQLException("Native D/CG session stopped", error);
            }
            finally {
                if (call != null) {
                    call.close();
                }
                if (connectionLease != null && completed && method.getName().equals("close")) {
                    connectionLease.close();
                }
            }
        });
    }

    static void noWarnings(ResultSet rows, Statement statement) throws SQLException {
        if (rows.getWarnings() != null || statement.getWarnings() != null) {
            throw new SQLException("Incomplete native evidence: SQL warning");
        }
    }

    void requestStop() {
        lifecycle.requestStop();
        reader.cancelJob();
    }

    @Override
    public void close() {
        requestStop();
        lifecycle.awaitDrained();
        transport.disconnectAllServices();
        if (failedConnectionCleanup != null) {
            throw new IllegalStateException("Native connection cleanup failed; drain unverified", failedConnectionCleanup);
        }
    }
}
