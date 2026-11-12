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
    private volatile String server;
    private final AS400 transport;
    private final boolean secure;
    private final java.util.Set<As400JournalCapture> captures = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private volatile Exception failedConnectionCleanup;
    private final As400AsyncCleanup cancellation = new As400AsyncCleanup("ibmi-native-cancel");
    private final As400AsyncCleanup cleanup = new As400AsyncCleanup("ibmi-native-close");

    As400DcgNativeIo(As400DcgConfig config) throws Exception {
        this(config.raw, config.server, config.journal, config.rrnKeys, config.policy, config.historyTopic, config.prefix);
    }

    /** Shared native handles; callers must separately qualify startup and publication. */
    As400DcgNativeIo(Configuration raw, String server, JournalReceiver journal, boolean rrnKeys,
                     As400CaptureFingerprint.Policy policy, String historyTopic, String prefix)
            throws Exception {
        this.raw = raw;
        this.server = server == null || server.isBlank() ? null : server;
        var ccsids = As400DcgConfig.ccsidMapping(raw);
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
        // Native inventory already bounds the complete set by independent
        // counts. Individual evidence queries retain their small batch limits.
        var tableLimits = new As400OperatorTableEvidence.Limits(Integer.MAX_VALUE, 5);
        tables = new As400OperatorTableEvidence(() -> connection(false), tableLimits);
        var connector = new As400ConnectorConfig(raw);
        var filters = connector.getTableFilters().dataCollectionFilter();
        acquisition = new As400NativeBindingAcquisition(() -> connection(false), tables, tableLimits, rrnKeys,
                table -> !As400DcgConfig.transactionalTruncateEnabled(raw)
                        || filters.isIncluded(new io.debezium.relational.TableId(this.server, table.schema(), table.table()))
                        || connector.isSignalDataCollection(new io.debezium.relational.TableId(this.server, table.schema(), table.table())),
                ccsids, As400DcgConfig.transactionalTruncateEnabled(raw));
        census = new As400NativeTransactionCensus(() -> connection(false), 1024, 5);
        journals = As400OperatorJournalEvidence.retained(() -> connection(false), journal);
        identities = new As400JdbcNativeIdentityEvidence(() -> connection(false), new As400JdbcNativeIdentityEvidence.Limits(Integer.MAX_VALUE, 5, 65536));
        history = bindingHistory(raw, policy, historyTopic, prefix);
    }

    static As400BindingHistory bindingHistory(Configuration raw, As400CaptureFingerprint.Policy policy, String historyTopic, String prefix) {
        var historyConfig = raw.edit().with(SchemaHistory.NAME, historyTopic == null ? prefix + "-bindings" : historyTopic)
                .with(SchemaHistory.INTERNAL_CONNECTOR_CLASS, As400RpcConnector.class.getName())
                .with(SchemaHistory.INTERNAL_CONNECTOR_ID, prefix).build();
        return new As400BindingHistory(As400BindingHistory.storage(raw), historyConfig, policy.sourcePartition());
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
                    if (!rows.next()) {
                        throw new SQLException("IBM i relational database identity is missing");
                    }
                    String observed = rows.getString(1);
                    if (rows.next()) {
                        throw new SQLException("IBM i relational database identity is ambiguous");
                    }
                    noWarnings(rows, st);
                    observeServer(observed);
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

    /** Same CURRENT_SERVER discovery as ordinary JDBC startup when dbname is omitted. */
    private synchronized void observeServer(String observed) throws SQLException {
        if (observed == null || observed.isBlank() || server != null && !server.equals(observed.trim())) {
            throw new SQLException("IBM i relational database identity differs from database.dbname or the established session");
        }
        server = observed.trim();
    }

    String server() {
        return java.util.Objects.requireNonNull(server, "Native connection has not established its database identity");
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
            try {
                if (!cleanup) {
                    call = lifecycle.enter();
                }
                Object result = method.invoke(delegate, args);
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
                if (connectionLease != null && method.getName().equals("close")) {
                    lifecycle.failedCleanup(error.getCause());
                }
                throw error.getCause();
            }
            catch (java.io.IOException error) {
                throw new SQLException("Native D/CG session stopped", error);
            }
            finally {
                if (call != null) {
                    call.close();
                }
                if (connectionLease != null && method.getName().equals("close")) {
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

    void requireRetention() throws SQLException {
        journals.collect();
    }

    As400HandoffWork handoffWork(As400ContinuousRuntime.Limits limits, Runnable checkRunning) {
        // Startup indexes use a small fixed page cache even when ordinary
        // transaction buffering uses memory. The disk budget remains explicit.
        var settings = As400HandoffWork.settings(
                java.nio.file.Path.of(raw.getString("truncate.buffer.directory", System.getProperty("java.io.tmpdir"))),
                raw.getLong("truncate.buffer.disk.max.bytes", 10L * 1024 * 1024 * 1024));
        return new As400HandoffWork(settings, limits, () -> capture(checkRunning), checkRunning);
    }

    As400JournalCapture capture(Runnable checkRunning) throws java.io.IOException {
        try (var lease = lifecycle.enter()) {
            var reference = new java.util.concurrent.atomic.AtomicReference<As400JournalCapture>();
            var capture = new As400JournalCapture(
                    java.nio.file.Path.of(raw.getString("truncate.buffer.directory", System.getProperty("java.io.tmpdir"))),
                    raw.getLong("truncate.buffer.disk.max.bytes", 10L * 1024 * 1024 * 1024), checkRunning,
                    () -> captures.remove(reference.get()));
            reference.set(capture);
            captures.add(capture);
            try {
                lifecycle.checkOpen();
                return capture;
            }
            catch (Throwable failure) {
                try {
                    capture.close();
                }
                catch (Throwable close) {
                    failure.addSuppressed(close);
                }
                throw failure;
            }
        }
    }

    void failedCleanup(Throwable failure) {
        lifecycle.failedCleanup(failure);
    }

    void requestStop() {
        lifecycle.requestStop();
        cancellation.start(reader::cancelJob);
    }

    @Override
    public void close() {
        closeWithin(As400AsyncCleanup.STOP_TIMEOUT);
    }

    void closeWithin(java.time.Duration timeout) {
        requestStop();
        cleanup.start(() -> {
            try {
                cancellation.awaitInCleanupWorker();
                lifecycle.awaitDrained();
            }
            finally {
                // A failed JDBC close or allocation release must remain a
                // reported failure, but must not leak the separate RPC handle.
                try {
                    transport.disconnectAllServices();
                }
                finally {
                    Exception failure = null;
                    for (var capture : java.util.List.copyOf(captures)) {
                        try {
                            capture.close();
                        }
                        catch (Exception error) {
                            if (failure == null) {
                                failure = error;
                            }
                            else {
                                failure.addSuppressed(error);
                            }
                        }
                    }
                    if (failure != null) {
                        throw failure;
                    }
                }
            }
            if (failedConnectionCleanup != null) {
                throw new IllegalStateException("Native connection cleanup failed; drain unverified", failedConnectionCleanup);
            }
        });
        cleanup.await(timeout);
    }
}
