/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

/** Controlled JDBC transport paired with the real RPC and streaming lifecycle. */
final class As400TaskStopFixture {
    final As400ReaderFixture reader;
    final As400JdbcConnection jdbc;
    final List<Map<String, Object>> jdbcEvents = new CopyOnWriteArrayList<>();
    volatile boolean jdbcConnected;
    volatile Hook jdbcHook = action -> {
    };
    private final As400ConnectorConfig config;

    interface Hook {
        void call(String action) throws Exception;
    }

    As400TaskStopFixture(String owner, As400ConnectorConfig config, long start, Consumer<Map<String, Object>> observer) throws Exception {
        this.config = config;
        reader = new As400ReaderFixture(owner, config, start, observer);
        jdbc = mock(As400JdbcConnection.class, invocation -> {
            String method = invocation.getMethod().getName();
            if (method.equals("getRealDatabaseName")) {
                return "DB";
            }
            if (method.equals("toString")) {
                return "controlled-jdbc-" + owner;
            }
            if (!method.equals("connect") && !method.equals("close")) {
                throw new AssertionError("Undeclared JDBC action: " + method);
            }
            Map<String, Object> entered = Map.of("owner", owner, "action", "jdbc-" + method + "-enter", "fields", Map.of());
            jdbcEvents.add(entered);
            observer.accept(entered);
            jdbcHook.call(method);
            jdbcConnected = method.equals("connect");
            Map<String, Object> exited = Map.of("owner", owner, "action", "jdbc-" + method + "-exit", "fields", Map.of());
            jdbcEvents.add(exited);
            observer.accept(exited);
            return method.equals("connect") ? invocation.getMock() : null;
        });
    }

    As400StreamingChangeEventSource source(EventDispatcher<As400Partition, TableId> dispatcher, ErrorHandler errors,
                                           As400RpcConnection.BlockingReceiverConsumer consumer) {
        // Only the record consumer is substituted to exercise the unwired D/CG
        // coordinator. execute(), retries, heartbeat gating and cleanup are real.
        var source = new As400StreamingChangeEventSource(config, reader.rpc, jdbc, dispatcher, errors, Clock.system(), mock(As400DatabaseSchema.class)) {
            @Override
            As400RpcConnection.BlockingReceiverConsumer processJournalEntries(As400Partition partition, As400OffsetContext offset) {
                return consumer;
            }
        };
        source.init(reader.scan);
        return source;
    }

    void install(As400ConnectorTask task) throws Exception {
        var rpcField = As400ConnectorTask.class.getDeclaredField("rpcConnection");
        rpcField.setAccessible(true);
        rpcField.set(task, reader.rpc);
        var jdbcField = As400ConnectorTask.class.getDeclaredField("jdbcConnection");
        jdbcField.setAccessible(true);
        jdbcField.set(task, jdbc);
    }
}
