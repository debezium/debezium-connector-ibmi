/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static io.debezium.connector.db2as400.As400ConnectProbeConnector.RECEIVER;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.withSettings;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.mockito.invocation.InvocationOnMock;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Text;
import com.ibm.as400.access.Job;
import com.ibm.as400.access.ProgramParameter;
import com.ibm.as400.access.SecureAS400;
import com.ibm.as400.access.ServiceProgramCall;

import io.debezium.config.Configuration;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfo;
import io.debezium.ibmi.db2.journal.retrieve.JournalInfoRetrieval;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RetrievalState;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveConfigBuilder;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.DetailedJournalReceiver;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalReceiverInfo;
import io.debezium.ibmi.db2.journal.retrieve.rnrn0200.JournalStatus;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;

/** Test transport only. Real RPC/reader methods; no live AS400 or Job is constructed. */
final class As400ReaderFixture {
    final As400RpcConnection rpc;
    final RetrieveJournal journal;
    final JournalReadLifecycle lifecycle = spy(new JournalReadLifecycle());
    final As400OffsetContext scan;
    final List<Map<String, Object>> operations = new CopyOnWriteArrayList<>();
    private final String owner;
    private final Consumer<Map<String, Object>> observer;
    private final AtomicInteger connections = new AtomicInteger();
    private final Map<Object, Boolean> alive = Collections.synchronizedMap(new IdentityHashMap<>());
    private final Map<Object, String> roles = Collections.synchronizedMap(new IdentityHashMap<>());
    volatile Callable<Void> duringCancellation = () -> null;
    volatile boolean failCancellationDisconnect;
    volatile boolean reconnectInsideAdmittedRun;
    private final ThreadLocal<Integer> aliveCalls = ThreadLocal.withInitial(() -> 0);
    private volatile long end = 2;
    private volatile byte[] page;
    private volatile Callable<Void> duringRead = () -> null;

    enum Boundary {
        CURRENT_RECEIVER,
        RECEIVER_LIST,
        SERVER_JOB,
        CONFIGURED_CALL,
        REGISTERED_ADMISSION,
        REGISTERED_RUN
    }

    @FunctionalInterface
    interface MetadataHook {
        void call(AS400 connection) throws Exception;
    }

    volatile MetadataHook duringMetadata = connection -> {
    };

    @FunctionalInterface
    interface BoundaryHook {
        void reached(Boundary boundary) throws Exception;
    }

    volatile BoundaryHook boundaryHook = boundary -> {
    };

    Map<String, Boolean> activeReadState() {
        var state = journal.readState();
        return Map.of("registered", state.registered(), "cancelled", state.cancellationRequested());
    }

    private void boundary(Boundary point) throws Exception {
        note("boundary", Map.of("point", point.name(), "read", activeReadState()));
        boundaryHook.reached(point);
    }

    As400ReaderFixture(String owner, As400ConnectorConfig config, long start, Consumer<Map<String, Object>> observer) throws Exception {
        this.owner = owner;
        this.observer = observer;
        // Bypass ONLY construction, which discovers a real source. All lifecycle
        // and read methods below are the production implementation, not copied loops.
        JournalInfo info = new JournalInfo("JRN", "LIB", false);
        JournalInfoRetrieval metadata = mock(JournalInfoRetrieval.class, invocation -> switch (invocation.getMethod().getName()) {
            case "getDelayedDetailedJournalReceiver" -> {
                note("receiver-metadata", Map.of("operation", "current receiver"));
                boundary(Boundary.CURRENT_RECEIVER);
                yield Optional.of(receiver());
            }
            case "getReceivers" -> {
                note("receiver-metadata", Map.of("operation", "receiver list"));
                boundary(Boundary.RECEIVER_LIST);
                yield new ArrayList<>(List.of(receiver()));
            }
            case "getCurrentPosition", "getReceiverDetails" -> {
                AS400 connection = invocation.getArgument(0);
                if (!alive.containsKey(connection)) {
                    throw unexpected("metadata", "foreign connection");
                }
                note("position-metadata", Map.of("operation", invocation.getMethod().getName()));
                duringMetadata.call(connection);
                yield invocation.getMethod().getName().equals("getCurrentPosition")
                        ? new JournalPosition(receiver().end(), RECEIVER)
                        : receiver();
            }
            default -> throw unexpected("metadata", invocation.getMethod().getName());
        });
        java.util.function.Function<As400RpcConnection, RetrieveJournal> readerFactory = connection -> new RetrieveJournal(
                new RetrieveConfigBuilder().withAs400(connection).withJournalInfo(info)
                        .withJournalBufferSize(4096).withMaxServerSideEntries(10).withTransactionControls(true).build(),
                metadata, lifecycle);
        rpc = mock(As400RpcConnection.class, withSettings().useConstructor(config, null, metadata, info, lifecycle, readerFactory)
                .defaultAnswer(CALLS_REAL_METHODS));
        journal = rpc.journal();
        org.mockito.Mockito.doNothing().when(rpc).logOffsets(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        doAnswer(invocation -> {
            if (activeReadState().get("registered")) {
                // Exact registration-to-run admission boundary, before acquiring
                // the transport lease. This hook is test-only, on the real gate.
                boundary(Boundary.REGISTERED_ADMISSION);
            }
            return invocation.callRealMethod();
        }).when(lifecycle).enter();
        // Mockito construction scopes are thread-local. Install the controlled
        // JT400 transport on the real asynchronous terminal cleanup thread too.
        doAnswer(invocation -> nativeScope(() -> {
            try {
                invocation.callRealMethod();
            }
            catch (Exception e) {
                throw e;
            }
            catch (Throwable e) {
                throw new AssertionError(e);
            }
            return null;
        })).when(rpc).drainStopped();
        scan = new As400OffsetContext(config, new JournalProcessedPosition(BigInteger.valueOf(start), RECEIVER, Instant.EPOCH, true));
    }

    static As400ConnectorConfig config(boolean secure) {
        return new As400ConnectorConfig(Configuration.create().with("topic.prefix", "synthetic")
                .with("database.hostname", "no-live-ibmi.invalid").with("database.user", "fixture")
                .with("database.password", "fixture").with("database.dbname", "DB").with("database.schema", "LIB")
                .with("secure", secure).with("skipped.operations", "none").build());
    }

    private DetailedJournalReceiver receiver() {
        return new DetailedJournalReceiver(new JournalReceiverInfo(RECEIVER, null, JournalStatus.Attached, Optional.empty()),
                BigInteger.ONE, BigInteger.valueOf(end), Optional.empty(), 4096, end);
    }

    <T> T nativeScope(Callable<T> action) throws Exception {
        aliveCalls.set(0);
        try (var plain = mockConstruction(AS400.class, withSettings().defaultAnswer(this::connectionCall), (m, context) -> {
            constructedConnection(m, context.arguments(), false);
        });
                var secure = mockConstruction(SecureAS400.class, withSettings().defaultAnswer(this::connectionCall), (m, context) -> {
                    constructedConnection(m, context.arguments(), true);
                });
                var parameters = mockConstruction(ProgramParameter.class);
                var jobs = mockConstruction(Job.class, withSettings().defaultAnswer(this::cancelCall), (m, context) -> {
                    List<?> a = context.arguments();
                    if (a.size() != 4 || !a.get(1).equals(jobName()) || !a.get(2).equals("FIXTURE") || !a.get(3).equals("000001")
                            || !alive.containsKey(a.get(0))) {
                        throw unexpected("job", "foreign cancellation target " + a);
                    }
                    note("cancel-target", Map.of("name", a.get(1), "user", a.get(2), "number", a.get(3)));
                });
                var calls = mockConstruction(ServiceProgramCall.class, context -> withSettings().defaultAnswer(new Program((AS400) context.arguments().get(0))),
                        (m, context) -> {
                            if (context.arguments().size() != 1 || !alive.containsKey(context.arguments().get(0))) {
                                throw unexpected("program", "foreign connection");
                            }
                        })) {
            return action.call();
        }
        finally {
            aliveCalls.remove();
        }
    }

    private void constructedConnection(Object connection, List<?> arguments, boolean secure) {
        boolean copy = arguments.size() == 1 && alive.containsKey(arguments.get(0));
        boolean fresh = arguments.size() == 3 && "no-live-ibmi.invalid".equals(arguments.get(0))
                && "fixture".equals(arguments.get(1)) && arguments.get(2) instanceof char[];
        if (!copy && !fresh) {
            throw unexpected("connection", "unexpected constructor");
        }
        alive.put(connection, false);
        roles.put(connection, copy ? "cancellation" : "reader");
        note(copy ? "copy-connection" : "new-connection", Map.of("secure", secure, "role", roles.get(connection)));
    }

    private Object connectionCall(InvocationOnMock i) {
        String method = i.getMethod().getName();
        return switch (method) {
            case "isConnectionAlive" -> {
                if (!i.getArgument(0).equals(AS400.COMMAND)) {
                    throw unexpected("connection", method);
                }
                int count = aliveCalls.get() + 1;
                aliveCalls.set(count);
                // Keep the regression finite if close/cancel accidentally re-enters
                // connection() again. Passing cases must never reach this guard.
                if (count > 32) {
                    throw unexpected("connection", "reentrant disconnected cancellation");
                }
                yield alive.get(i.getMock());
            }
            case "setSocketProperties" -> {
                note("socket-properties", Map.of());
                yield null;
            }
            case "connectService" -> {
                if (!i.getArgument(0).equals(AS400.COMMAND)) {
                    throw unexpected("connection", method);
                }
                alive.put(i.getMock(), true);
                connections.incrementAndGet();
                note("connect", Map.of("service", AS400.COMMAND));
                yield null;
            }
            case "disconnectAllServices" -> {
                alive.put(i.getMock(), false);
                String role = roles.get(i.getMock());
                note("disconnect", Map.of("role", role));
                if (failCancellationDisconnect && role.equals("cancellation")) {
                    throw new IllegalStateException("controlled cancellation disconnect failure");
                }
                yield null;
            }
            case "toString" -> "fixture-connection-" + owner;
            default -> throw unexpected("connection", method);
        };
    }

    private String jobName() {
        return "J" + Integer.toUnsignedString(owner.hashCode(), 16);
    }

    private Object cancelCall(InvocationOnMock i) throws Exception {
        if (i.getMethod().getName().equals("end") && i.getArgument(0).equals(0)) {
            note("job-end", Map.of("job", jobName()));
            duringCancellation.call();
            return null;
        }
        if (i.getMethod().getName().equals("toString")) {
            return "fixture-cancel-" + owner;
        }
        throw unexpected("cancel", i.getMethod().getName());
    }

    private final class Program implements org.mockito.stubbing.Answer<Object> {
        private final AS400 transport;
        private ProgramParameter[] parameters;

        private Program(AS400 transport) {
            this.transport = transport;
        }

        private Map<String, Object> transportState() {
            return Map.of("connected", Boolean.TRUE.equals(alive.get(transport)), "role", roles.get(transport));
        }

        private final Job server = mock(Job.class, i -> switch (i.getMethod().getName()) {
            case "getCodedCharacterSetID" -> 37;
            case "getName" -> jobName();
            case "getUser" -> "FIXTURE";
            case "getNumber" -> "000001";
            case "getQualifiedJobName" -> "000001/FIXTURE/" + jobName();
            case "setLoggingLevel" -> {
                if (!i.getArgument(0).equals(0)) {
                    throw unexpected("job", "logging level");
                }
                note("job-logging", Map.of("level", 0, "transport", transportState()));
                yield null;
            }
            case "toString" -> "fixture-job-" + owner;
            default -> throw unexpected("server-job", i.getMethod().getName());
        });

        @Override
        public Object answer(InvocationOnMock i) throws Throwable {
            return switch (i.getMethod().getName()) {
                case "getServerJob" -> {
                    boundary(Boundary.SERVER_JOB);
                    note("server-job", Map.of("transport", transportState()));
                    yield server;
                }
                case "setProgram" -> {
                    if (!i.getArgument(0).equals(JournalInfoRetrieval.JOURNAL_SERVICE_LIB)) {
                        throw unexpected("program", "unexpected service");
                    }
                    parameters = i.getArgument(1);
                    yield null;
                }
                case "setProcedureName" -> {
                    if (!i.getArgument(0).equals("QjoRetrieveJournalEntries")) {
                        throw unexpected("program", "unexpected procedure");
                    }
                    yield null;
                }
                case "setAlignOn16Bytes" -> {
                    if (!Boolean.TRUE.equals(i.getArgument(0))) {
                        throw unexpected("program", "alignment");
                    }
                    yield null;
                }
                case "setReturnValueFormat" -> {
                    if (!Integer.valueOf(ServiceProgramCall.RETURN_INTEGER).equals(i.getArgument(0))) {
                        throw unexpected("program", "return format");
                    }
                    boundary(Boundary.CONFIGURED_CALL);
                    yield null;
                }
                case "run" -> {
                    // At method entry, after production registration, before any
                    // simulated transport request. Do not mistake this for an
                    // instruction-level breakpoint before the JVM invokes run().
                    boundary(Boundary.REGISTERED_RUN);
                    if (reconnectInsideAdmittedRun) {
                        // Adversarial driver model: an already-admitted call can
                        // revive its captured transport while stop is draining.
                        transport.connectService(AS400.COMMAND);
                    }
                    note("read-enter", Map.of("through", end, "transport", transportState(), "read", activeReadState()));
                    duringRead.call();
                    org.mockito.Mockito.when(parameters[0].getOutputData()).thenReturn(page.clone());
                    note("read-return", Map.of("through", end));
                    yield true;
                }
                case "toString" -> "fixture-program-" + owner;
                default -> throw unexpected("program", i.getMethod().getName());
            };
        }
    }

    RetrievalState read(As400ConnectProbeConnector.Event event, BooleanSupplier running,
                        As400RpcConnection.BlockingReceiverConsumer consumer, Callable<Void> during)
            throws Exception {
        prepare(event, during);
        ChangeEventSourceContext context = mock(ChangeEventSourceContext.class, i -> {
            if (i.getMethod().getName().equals("isRunning")) {
                return running.getAsBoolean();
            }
            throw unexpected("context", i.getMethod().getName());
        });
        return nativeScope(() -> rpc.getJournalEntries(context, scan, consumer, mock(WatchDog.class)));
    }

    void prepare(As400ConnectProbeConnector.Event event, Callable<Void> during) throws Exception {
        end = event.position().offset().longValue();
        page = page(event);
        duringRead = during;
    }

    void close() throws Exception {
        nativeScope(() -> {
            rpc.close();
            note("close-return", Map.of());
            return null;
        });
    }

    void stop() throws Exception {
        nativeScope(() -> {
            rpc.stop();
            note("stop-return", Map.of());
            return null;
        });
    }

    void reconnect() throws Exception {
        nativeScope(() -> {
            rpc.close();
            rpc.connection();
            return null;
        });
    }

    void disconnectTransport() {
        alive.replaceAll((connection, connected) -> false);
    }

    boolean allReaderTransportsDisconnected() {
        synchronized (alive) {
            return alive.entrySet().stream().filter(e -> roles.get(e.getKey()).equals("reader")).noneMatch(Map.Entry::getValue);
        }
    }

    int connectionCount() {
        return connections.get();
    }

    private AssertionError unexpected(String boundary, String action) {
        note("undeclared-action", Map.of("boundary", boundary, "action", action));
        return new AssertionError("Fixture rejected " + boundary + ": " + action);
    }

    private void note(String action, Map<String, ?> fields) {
        Map<String, Object> event = Map.of("owner", owner, "action", action, "fields", fields);
        operations.add(event);
        observer.accept(event);
    }

    private static void text(byte[] bytes, int offset, int length, String value) {
        new AS400Text(length, 37).toBytes(value, bytes, offset);
    }

    static byte[] page(As400ConnectProbeConnector.Event event) throws Exception {
        var h = event.header();
        byte[] entry = event.data().clone();
        // Encode the public RJNE0200 wire layout, independent of decoder fields.
        java.util.Arrays.fill(entry, 0, 236, (byte) 0);
        var header = ByteBuffer.wrap(entry);
        header.putInt(8, h.getEntrySpecificDataOffset()).putLong(24, h.getSequenceNumber().longValueExact())
                .putLong(40, 1).putLong(48, 1).putLong(56, 1).putLong(64, h.getCommitCycle().longValueExact());
        text(entry, 82, 16, "");
        text(entry, 98, 1, String.valueOf(h.getJournalCode()));
        text(entry, 99, 2, h.getEntryType());
        text(entry, 101, 10, "JOB");
        text(entry, 111, 10, "USER");
        text(entry, 121, 6, "123456");
        text(entry, 127, 10, "PGM");
        text(entry, 137, 10, "LIB");
        text(entry, 147, 10, "");
        text(entry, 157, 30, h.getObjectName());
        text(entry, 187, 10, "USER");
        text(entry, 207, 1, "");
        text(entry, 208, 8, "SYS");
        text(entry, 216, 1, String.valueOf(h.getControlInfo().indicatorFlag()));
        text(entry, 217, 1, "1");
        entry[218] = (byte) h.getControlInfo().flags();
        text(entry, 219, 10, h.getRecordInfo().objectType());
        header.putInt(232, (int) h.getControlInfo().nestedCommitLevel());
        // JT400's DTS decoder uses an unsigned midpoint at 2000-01-01 and
        // 2^12 units per microsecond; its toBytes() still writes text format.
        long micros = Math.multiplyExact(h.getTime().getEpochSecond(), 1000000L) + h.getTime().getNano() / 1000;
        long dts = BigInteger.valueOf(micros).subtract(new BigInteger("946684800000000")).shiftLeft(12)
                .add(BigInteger.ONE.shiftLeft(63)).longValue();
        ByteBuffer.wrap(entry).putLong(32, dts);
        System.arraycopy(HexFormat.of().parseHex(h.getRecordInfo().objectJournalId()), 0, entry, 197, 10);
        byte[] result = new byte[64 + entry.length];
        ByteBuffer.wrap(result).putInt(result.length).putInt(64).putInt(1);
        new AS400Text(1).toBytes("0", result, 12);
        new AS400Text(40).toBytes("", result, 13);
        System.arraycopy(entry, 0, result, 64, entry.length);
        return result;
    }
}
