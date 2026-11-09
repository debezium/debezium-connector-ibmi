/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400Bin4;
import com.ibm.as400.access.AS400DataType;
import com.ibm.as400.access.AS400Exception;
import com.ibm.as400.access.ConnectionEvent;
import com.ibm.as400.access.ConnectionListener;
import com.ibm.as400.access.Job;
import com.ibm.as400.access.ObjectDoesNotExistException;
import com.ibm.as400.access.ServiceProgramCall;
import com.ibm.as400.access.SocketProperties;

import io.debezium.config.Configuration;
import io.debezium.connector.db2as400.metrics.As400StreamingChangeEventSourceMetrics;
import io.debezium.ibmi.db2.journal.retrieve.CompleteRowEntryDecoder;
import io.debezium.ibmi.db2.journal.retrieve.FileFilter;
import io.debezium.ibmi.db2.journal.retrieve.JournalPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalProcessedPosition;
import io.debezium.ibmi.db2.journal.retrieve.JournalReadLifecycle;
import io.debezium.ibmi.db2.journal.retrieve.RetrieveJournal;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;

/**
 * Explicit live experiment, never a default unit/IT test or production connector.
 * Only DCTASKQA.NATREAD is read/written. A lifecycle spy observes registration
 * and holds the captured real COMMAND job; all JT400 calls execute normally.
 * Kafka records/checkpoints are a diagnostic oracle, not a Connect task or
 * production D/CG adapter. Fixture identities are not native startup proof.
 */
public final class As400NativeReaderStopProbe {
    private static final String LIBRARY = "DCTASKQA";
    private static final String TABLE = "NATREAD";
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Path directory;
    private final String bootstrap;
    private final String token;
    private final String events;
    private final String offsets;
    private final Map<String, Object> report = new LinkedHashMap<>();
    private final List<Map<String, Object>> checks = new ArrayList<>();
    private final AtomicInteger emitted = new AtomicInteger();
    private JournalPosition boundary;

    private As400NativeReaderStopProbe(Path directory, String bootstrap, String token) {
        if (!bootstrap.matches("127\\.0\\.0\\.1:[0-9]{1,5}") || !token.matches("[a-f0-9]{12}")) {
            throw new IllegalArgumentException("Requires loopback Kafka and unique fixture token");
        }
        this.directory = directory;
        this.bootstrap = bootstrap;
        this.token = token;
        events = "native-stop-" + token + "-events";
        offsets = "native-stop-" + token + "-offsets";
        report.put("checks", checks);
        report.put("scope", "Real native RPC/cancellation with diagnostic Kafka publication; no Connect reassignment or D/CG startup proof");
        report.put("transactional_streaming_enabled", false);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("evidence-directory loopback-kafka fixture-token; credentials on stdin");
        }
        Properties properties = new Properties();
        properties.load(System.in);
        if (!LIBRARY.equals(properties.getProperty("native.fixture"))) {
            throw new IllegalArgumentException("Missing isolated fixture guard");
        }
        var probe = new As400NativeReaderStopProbe(Path.of(args[0]), args[1], args[2]);
        try {
            probe.run(properties);
            probe.report.put("status", "passed");
        }
        catch (Throwable error) {
            probe.report.put("status", "failed");
            probe.report.put("error", error.toString());
            throw error;
        }
        finally {
            probe.save();
        }
    }

    private void run(Properties properties) throws Exception {
        System.setProperty("com.ibm.as400.access.AS400.guiAvailable", "False");
        var config = new As400ConnectorConfig(Configuration.from(properties).edit()
                .with("database.dbname", LIBRARY).with("database.schema", LIBRARY)
                .with("topic.prefix", "native-stop").with("table.include.list", LIBRARY + "." + TABLE)
                .with("socket.timeout", 120000).with("journal.additional.delay", 0).build());
        Properties jdbc = new Properties();
        jdbc.setProperty("user", config.getUser());
        jdbc.setProperty("password", config.getPassword());
        jdbc.setProperty("prompt", "false");
        jdbc.setProperty("socket timeout", "15000");
        AS400 observer = new AS400(config.getHostname(), config.getUser(), config.getPassword());
        SocketProperties sockets = new SocketProperties();
        sockets.setSoTimeout(15000);
        observer.setSocketProperties(sockets);
        var executor = Executors.newFixedThreadPool(2, runnable -> {
            Thread thread = new Thread(runnable, "native-stop-probe");
            thread.setDaemon(true);
            return thread;
        });
        As400RpcConnection old = null;
        As400RpcConnection replacement = null;
        Job heldJob = null;
        AtomicBoolean held = new AtomicBoolean();
        Map<String, Object> heldIdentity = null;
        try (Connection sql = DriverManager.getConnection("jdbc:as400://" + config.getHostname(), jdbc);
                Admin admin = Admin.create(Map.of("bootstrap.servers", bootstrap, "default.api.timeout.ms", 15000, "request.timeout.ms", 10000));
                KafkaProducer<String, String> oldProducer = producer()) {
            sql.setTransactionIsolation(Connection.TRANSACTION_NONE);
            report.put("row_decoder", "CompleteRowEntryDecoder in publication path; no legacy fallback; transactional D/CG disabled");
            try (var stmt = sql.createStatement(); var rows = stmt.executeQuery("SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1")) {
                require(rows.next() && "AEQA".equals(rows.getString(1).trim()), "target is AEQA");
            }
            try (var stmt = sql.createStatement(); var rows = stmt.executeQuery("SELECT COUNT(*) FROM DCTASKQA.NATREAD")) {
                require(rows.next() && rows.getInt(1) == 0, "owned fixture starts empty");
            }
            admin.createTopics(List.of(new NewTopic(events, 1, (short) 1), new NewTopic(offsets, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            oldProducer.initTransactions();
            old = reader(config);
            replacement = reader(config);
            boundary = old.getCurrentPosition();
            require(LIBRARY.equals(boundary.receiver().library()) && boundary.offset().signum() > 0, "initial receiver belongs to isolated library");
            publish(oldProducer, null, checkpoint(boundary.offset()));
            Map<String, String> initial = lastCheckpoint("initial");
            require(initial.equals(checkpoint(boundary.offset())), "baseline read independently from committed Kafka offsets");
            var oldScan = scan(config, initial);
            var newScan = scan(config, lastCheckpoint("replacement-load"));
            AS400 oldTransport = old.connection();
            AS400 newTransport = replacement.connection();
            Job oldServer = new ServiceProgramCall(oldTransport).getServerJob();
            Job newServer = new ServiceProgramCall(newTransport).getServerJob();
            require(!oldServer.getQualifiedJobName().equals(newServer.getQualifiedJobName()), "old and replacement have distinct native jobs");
            require(Arrays.stream(oldTransport.getJobs(AS400.COMMAND)).anyMatch(j -> j.getQualifiedJobName().equals(oldServer.getQualifiedJobName())),
                    "old job is bound to this reader COMMAND connection");
            Map<String, Object> oldIdentity = jobState(observer, oldServer);
            Map<String, Object> newIdentity = jobState(observer, newServer);
            require(sameJob(oldIdentity, oldIdentity) && sameJob(newIdentity, newIdentity), "native job identities are complete and active");
            heldIdentity = oldIdentity;
            report.put("old_job", oldIdentity);
            report.put("replacement_job", newIdentity);
            heldJob = copyJob(observer, oldServer);
            Job target = heldJob;
            var journal = (RetrieveJournal) field(old, "retrieveJournal");
            var lifecycle = spy((JournalReadLifecycle) field(old, "lifecycle"));
            set(old, "lifecycle", lifecycle);
            set(journal, "lifecycle", lifecycle);
            set(field(journal, "journalReceivers"), "lifecycle", lifecycle);
            CountDownLatch nativeHeld = new CountDownLatch(1);
            AtomicBoolean once = new AtomicBoolean();
            AtomicReference<Thread> readerThread = new AtomicReference<>();
            doAnswer(invocation -> {
                JournalReadLifecycle.Lease lease = (JournalReadLifecycle.Lease) invocation.callRealMethod();
                try {
                    Object active = field(journal, "activeRead");
                    if (Thread.currentThread() == readerThread.get() && active != null && once.compareAndSet(false, true)) {
                        Job captured = (Job) field(active, "job");
                        require(field(active, "connection") == oldTransport && captured.getQualifiedJobName().equals(oldServer.getQualifiedJobName()),
                                "registered call captures only the bound old job");
                        require(sameJob(oldIdentity, jobState(observer, oldServer)), "old native job lifetime matches immediately before hold");
                        // Native fault only: hold this exact owned job before the real run().
                        report.put("hold_attempted", true);
                        save();
                        attemptHold(held, target);
                        Map<String, Object> state = jobState(observer, oldServer);
                        report.put("held_job", state);
                        require(Job.ACTIVE_JOB_STATUS_HELD.equals(state.get("active_status")), "IBM i reports the old job held");
                        nativeHeld.countDown();
                    }
                    return lease;
                }
                catch (Throwable failure) {
                    report.put("admission_failure", failure.toString());
                    try {
                        save();
                    }
                    catch (IOException evidenceFailure) {
                        failure.addSuppressed(evidenceFailure);
                    }
                    nativeHeld.countDown();
                    lease.close();
                    throw failure;
                }
            }).when(lifecycle).enter();
            AtomicInteger connectedAfterStop = new AtomicInteger();
            AtomicBoolean stopRequested = new AtomicBoolean();
            oldTransport.addConnectionListener(new ConnectionListener() {
                @Override
                public void connected(ConnectionEvent event) {
                    if (stopRequested.get()) {
                        connectedAfterStop.incrementAndGet();
                    }
                }

                @Override
                public void disconnected(ConnectionEvent event) {
                }
            });
            execute(sql, "INSERT INTO DCTASKQA.NATREAD VALUES (1)");
            As400RpcConnection oldReader = old;
            AtomicInteger oldCallbacks = new AtomicInteger();
            var reading = executor.submit(() -> {
                readerThread.set(Thread.currentThread());
                try {
                    oldReader.getJournalEntries(context(), oldScan, (sequence, raw, header) -> oldCallbacks.incrementAndGet(), mock(WatchDog.class));
                    return "returned normally";
                }
                catch (Exception error) {
                    return error.getClass().getName() + ": " + error.getMessage();
                }
            });
            require(nativeHeld.await(25, TimeUnit.SECONDS), "native hold reached within test deadline");
            long callDeadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (!inNativeRun(readerThread.get()) && !reading.isDone() && System.nanoTime() < callDeadline) {
                Thread.sleep(25);
            }
            if (reading.isDone()) {
                report.put("old_read_result", reading.get(10, TimeUnit.SECONDS));
            }
            require(!reading.isDone() && inNativeRun(readerThread.get()), "old thread is blocked inside real ServiceProgramCall.run");
            report.put("held_call_stack", Arrays.stream(readerThread.get().getStackTrace()).map(Object::toString).toList());
            save();
            try (KafkaProducer<String, String> publisher = producer()) {
                publisher.initTransactions();
                readUntil(replacement, newScan, publisher, 1);
                execute(sql, "CALL QSYS2.QCMDEXC('CLRPFM FILE(DCTASKQA/NATREAD) MBR(NATREAD)')");
                execute(sql, "INSERT INTO DCTASKQA.NATREAD VALUES (2)");
                readUntil(replacement, newScan, publisher, 3);
                Map<String, String> accepted = lastCheckpoint("while-old-held");
                require(!reading.isDone() && oldCallbacks.get() == 0 && oldScan.getPosition().asJournalPosition().equals(boundary),
                        "replacement commits output while old call remains held without scan advancement");
                report.put("old_scan_while_held", oldScan.getPosition().getOffset().toString());
                requireFenced(oldProducer);
                stopRequested.set(true);
                report.put("stop_requested_at", Instant.now().toString());
                long stopStart = System.nanoTime();
                var stopping = executor.submit(oldReader::stop);
                stopping.get(30, TimeUnit.SECONDS);
                report.put("stop_completed_at", Instant.now().toString());
                report.put("observed_stop_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - stopStart));
                String result = reading.get(10, TimeUnit.SECONDS);
                report.put("old_read_result", result);
                require(!result.equals("returned normally") && oldCallbacks.get() == 0, "cancelled old call emits no record");
                require(!oldTransport.isConnected(AS400.COMMAND) && field(old, "as400") == null && field(journal, "activeRead") == null,
                        "completed stop disconnects old transport and clears active handle");
                Map<String, Object> ended = jobState(observer, oldServer);
                report.put("old_job_immediately_after_stop", ended);
                long jobDeadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
                while ("*ACTIVE".equals(ended.get("status")) && System.nanoTime() < jobDeadline) {
                    Thread.sleep(100);
                    ended = jobState(observer, oldServer);
                }
                report.put("old_job_after_stop", ended);
                report.put("old_job_termination_observed_at", Instant.now().toString());
                require(!"*ACTIVE".equals(ended.get("status")), "old native job is no longer active within observation deadline");
                held.set(false);
                try {
                    old.connection();
                    throw new AssertionError("Stopped reader reconnected");
                }
                catch (IOException expected) {
                    require(connectedAfterStop.get() == 0, "stopped reader rejects reconnect without new native connection");
                }
                require(oldScan.getPosition().asJournalPosition().equals(boundary) && lastCheckpoint("after-old-stop").equals(accepted),
                        "old completion changes neither scan nor committed checkpoint");
                require(sameJob(newIdentity, jobState(observer, newServer)) && newTransport.isConnectionAlive(AS400.COMMAND),
                        "replacement retains its original native job and connection");
                execute(sql, "INSERT INTO DCTASKQA.NATREAD VALUES (3)");
                readUntil(replacement, newScan, publisher, 4);
                var rows = snapshot(events, "final-events");
                List<String> operations = rows.stream().map(row -> row.get("op") + (row.containsKey("id") ? ":" + row.get("id") : "")).toList();
                require(operations.equals(List.of("c:1", "t", "c:2", "c:3")), "committed native events remain ordered through cancellation");
                List<Map<String, Object>> history = snapshot(offsets, "final-checkpoints");
                BigInteger previous = BigInteger.ZERO;
                for (var offset : history) {
                    BigInteger completed = new BigInteger(offset.get("completed_through.sequence").toString());
                    require(completed.compareTo(previous) >= 0 && offset.get("replay_from.sequence").equals(completed.toString()),
                            "diagnostic committed F/W does not regress");
                    previous = completed;
                }
                require(history.size() == 5 && previous.equals(new BigInteger(rows.get(rows.size() - 1).get("sequence").toString())),
                        "final checkpoint covers exactly the last emitted native record");
                try (var stmt = sql.createStatement(); var values = stmt.executeQuery("SELECT ID FROM DCTASKQA.NATREAD ORDER BY ID")) {
                    List<Integer> source = new ArrayList<>();
                    while (values.next()) {
                        source.add(values.getInt(1));
                    }
                    require(source.equals(List.of(2, 3)), "isolated source has expected clear/reload result");
                }
                report.put("replacement_job_after_progress", jobState(observer, newServer));
            }
        }
        finally {
            // Release only this fixture's still-held job on failure; never wildcard jobs.
            if (held.get() && heldJob != null) {
                try {
                    report.put("emergency_release", recoverHold(held, heldJob, heldIdentity, job -> jobState(observer, job)));
                }
                catch (Exception error) {
                    report.put("emergency_release", error.toString());
                }
            }
            if (old != null) {
                old.stop();
            }
            if (replacement != null) {
                replacement.stop();
            }
            observer.disconnectAllServices();
            executor.shutdownNow();
            boolean terminated = executor.awaitTermination(15, TimeUnit.SECONDS);
            report.put("reader_threads_terminated", terminated);
            require(terminated, "all local native reader threads terminated");
        }
    }

    private As400RpcConnection reader(As400ConnectorConfig config) {
        return new As400RpcConnection(config, mock(As400StreamingChangeEventSourceMetrics.class), List.of(new FileFilter(LIBRARY, TABLE)), 0);
    }

    private As400OffsetContext scan(As400ConnectorConfig config, Map<String, String> saved) {
        var checkpoint = As400Checkpoint.fromOffset(saved, "fixture-" + token, "v1:" + "b".repeat(64));
        return new As400OffsetContext(config, new JournalProcessedPosition(checkpoint.replayFrom(), Instant.EPOCH, !checkpoint.replayInclusive()));
    }

    private Map<String, String> checkpoint(BigInteger sequence) {
        // The fresh, exclusively controlled fixture uses commitment level NONE.
        // These lab identities/boundaries MUST NOT be used as production startup proof.
        return As400Checkpoint.afterVerifiedBoundary("fixture-" + token, "v1:" + "b".repeat(64),
                new JournalPosition(sequence, boundary.receiver()), "fixture-receiver-" + token).toOffset();
    }

    private void readUntil(As400RpcConnection reader, As400OffsetContext scan, KafkaProducer<String, String> publisher, int count) throws Exception {
        var decoder = new CompleteRowEntryDecoder(new AS400DataType[]{ new AS400Bin4() }, new boolean[]{ false }, 64);
        long deadline = System.nanoTime() + Duration.ofSeconds(40).toNanos();
        while (emitted.get() < count && System.nanoTime() < deadline) {
            reader.getJournalEntries(context(), scan, (sequence, raw, header) -> {
                if (!LIBRARY.equals(header.getLibrary()) || !TABLE.equals(header.getFile())) {
                    return;
                }
                if (header.getJournalCode() != 'R' && !(header.getJournalCode() == 'F' && header.getEntryType().equals("CR"))) {
                    return;
                }
                if (header.getCommitCycle().signum() != 0 || !boundary.receiver().equals(raw.getPosition().getReceiver())) {
                    throw new IOException("Fixture changed receiver or entered commitment control");
                }
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("sequence", sequence.toString());
                event.put("receiver", boundary.receiver().name());
                event.put("library", header.getLibrary());
                event.put("file", header.getFile());
                event.put("native_code", header.getJournalCode() + "/" + header.getEntryType());
                if (header.getJournalCode() == 'F') {
                    event.put("op", "t");
                }
                else if (List.of("PT", "PX").contains(header.getEntryType())) {
                    event.put("op", "c");
                    try {
                        Object[] decoded = raw.decode((entry, bytes, offset) -> {
                            Map<String, Object> diagnostic = new LinkedHashMap<>();
                            diagnostic.put("sequence", sequence.toString());
                            diagnostic.put("entry_hex", HexFormat.of().formatHex(Arrays.copyOfRange(bytes, offset, offset + entry.getEndOffset())));
                            diagnostic.put("entry_end", entry.getEndOffset());
                            diagnostic.put("payload_offset", entry.getEntrySpecificDataOffset());
                            diagnostic.put("null_offset", entry.getNullValueOffest());
                            try {
                                Object[] values = decoder.decode(entry, bytes, offset);
                                diagnostic.put("strict_values", Arrays.asList(values));
                                return values;
                            }
                            catch (IOException rejected) {
                                diagnostic.put("strict_rejection", rejected.getMessage());
                                throw rejected;
                            }
                            finally {
                                JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("row-" + sequence + ".json").toFile(), diagnostic);
                            }
                        });
                        event.put("id", decoded[0]);
                    }
                    catch (Exception error) {
                        throw new IOException("Native fixture row decode failed", error);
                    }
                }
                else {
                    throw new IOException("Unexpected fixture row operation");
                }
                try {
                    publish(publisher, event, checkpoint(sequence));
                    emitted.incrementAndGet();
                }
                catch (Exception error) {
                    throw new IOException("Diagnostic Kafka publication failed", error);
                }
            }, mock(WatchDog.class));
            Thread.sleep(25);
        }
        require(emitted.get() == count, "replacement reaches expected native event count " + count);
    }

    private KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of("bootstrap.servers", bootstrap, "key.serializer", StringSerializer.class,
                "value.serializer", StringSerializer.class, "transactional.id", "native-stop-" + token,
                "max.block.ms", 15000, "request.timeout.ms", 10000, "delivery.timeout.ms", 20000));
    }

    private void publish(KafkaProducer<String, String> producer, Map<String, Object> event, Map<String, String> checkpoint) throws Exception {
        producer.beginTransaction();
        if (event != null) {
            producer.send(new ProducerRecord<>(events, 0, "fixture", JSON.writeValueAsString(event))).get(15, TimeUnit.SECONDS);
        }
        producer.send(new ProducerRecord<>(offsets, 0, "fixture", JSON.writeValueAsString(checkpoint))).get(15, TimeUnit.SECONDS);
        producer.commitTransaction();
    }

    private void requireFenced(KafkaProducer<String, String> producer) throws Exception {
        try {
            publish(producer, Map.of("op", "stale"), checkpoint(boundary.offset()));
            throw new AssertionError("Stale producer accepted output");
        }
        catch (Exception error) {
            Throwable cause = error;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            require(cause instanceof ProducerFencedException || cause instanceof InvalidProducerEpochException, "Kafka fences stale diagnostic producer");
            report.put("stale_producer_error", cause.getClass().getName());
        }
    }

    private List<Map<String, Object>> snapshot(String topic, String label) throws Exception {
        List<Map<String, Object>> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of("bootstrap.servers", bootstrap,
                "key.deserializer", StringDeserializer.class, "value.deserializer", StringDeserializer.class,
                "enable.auto.commit", false, "isolation.level", "read_committed", "default.api.timeout.ms", 15000, "request.timeout.ms", 10000))) {
            var partition = new TopicPartition(topic, 0);
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            long end = consumer.endOffsets(List.of(partition)).get(partition);
            long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
            while (consumer.position(partition) < end) {
                if (System.nanoTime() > deadline || result.size() > 100) {
                    throw new IllegalStateException("Diagnostic consumer exceeded bound");
                }
                consumer.poll(Duration.ofMillis(100)).forEach(record -> {
                    try {
                        result.add(JSON.readValue(record.value(), Map.class));
                    }
                    catch (IOException error) {
                        throw new IllegalStateException(error);
                    }
                });
            }
            report.put(label + "_consumer", Map.of("end", end, "position", consumer.position(partition)));
        }
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve(label + ".json").toFile(), result);
        return result;
    }

    private Map<String, String> lastCheckpoint(String label) throws Exception {
        var rows = snapshot(offsets, label);
        if (rows.isEmpty()) {
            throw new IllegalStateException("Missing diagnostic checkpoint");
        }
        Map<String, String> result = new HashMap<>();
        rows.get(rows.size() - 1).forEach((key, value) -> result.put(key, value.toString()));
        return result;
    }

    private static ChangeEventSourceContext context() {
        return mock(ChangeEventSourceContext.class, invocation -> {
            if (invocation.getMethod().getName().equals("isRunning")) {
                return true;
            }
            throw new AssertionError("Unexpected context method");
        });
    }

    private static boolean inNativeRun(Thread thread) {
        return thread != null && Arrays.stream(thread.getStackTrace()).anyMatch(frame -> frame.getClassName().equals(ServiceProgramCall.class.getName())
                && frame.getMethodName().equals("run"));
    }

    private static Job copyJob(AS400 observer, Job job) {
        return new Job(observer, job.getName(), job.getUser(), job.getNumber());
    }

    private static Map<String, Object> jobState(AS400 observer, Job job) throws Exception {
        Job fresh = copyJob(observer, job);
        try {
            String status = fresh.getStatus();
            if (!"*ACTIVE".equals(status)) {
                return Map.of("qualified_name", fresh.getQualifiedJobName(), "status", status);
            }
            return Map.of("qualified_name", fresh.getQualifiedJobName(), "status", status,
                    "active_status", String.valueOf(fresh.getValue(Job.ACTIVE_JOB_STATUS)),
                    "internal_id", HexFormat.of().formatHex(fresh.getInternalJobIdentifier()),
                    "entered_system", fresh.getJobEnterSystemDate().toInstant().toString());
        }
        catch (ObjectDoesNotExistException missing) {
            return Map.of("qualified_name", fresh.getQualifiedJobName(), "status", "not-found");
        }
        catch (AS400Exception failure) {
            if (failure.getAS400Message() != null && "CPF3C53".equals(failure.getAS400Message().getID())) {
                return Map.of("qualified_name", fresh.getQualifiedJobName(), "status", "not-found", "message_id", "CPF3C53");
            }
            throw failure;
        }
    }

    // A failed acknowledgement does not prove that the native hold failed.
    static void attemptHold(AtomicBoolean attempted, Job job) throws Exception {
        attempted.set(true);
        job.hold(false);
    }

    @FunctionalInterface
    interface JobObservation {
        Map<String, Object> read(Job job) throws Exception;
    }

    static String recoverHold(AtomicBoolean attempted, Job job, Map<String, Object> identity, JobObservation observation) throws Exception {
        if (!attempted.get()) {
            return "no hold attempted";
        }
        Map<String, Object> current = observation.read(job);
        if (sameJob(identity, current) && Job.ACTIVE_JOB_STATUS_HELD.equals(current.get("active_status"))) {
            job.release();
            return "matching held job released";
        }
        return "not released: job ended, changed identity, or is no longer held";
    }

    static boolean sameJob(Map<String, Object> first, Map<String, Object> second) {
        if (first == null || second == null || !"*ACTIVE".equals(first.get("status")) || !"*ACTIVE".equals(second.get("status"))) {
            return false;
        }
        for (String name : List.of("qualified_name", "internal_id", "entered_system")) {
            if (!(first.get(name) instanceof String value) || value.isBlank() || !value.equals(second.get(name))) {
                return false;
            }
        }
        String id = first.get("internal_id").toString();
        return id.matches("[0-9a-f]+") && id.chars().anyMatch(character -> character != '0');
    }

    private static void execute(Connection connection, String statement) throws Exception {
        try (var sql = connection.createStatement()) {
            sql.setQueryTimeout(20);
            sql.execute(statement);
        }
    }

    private static Object field(Object object, String name) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    private static void set(Object object, String name, Object value) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(object, value);
    }

    private synchronized void require(boolean passed, String name) throws IOException {
        checks.add(Map.of("name", name, "status", passed ? "passed" : "failed"));
        save();
        if (!passed) {
            throw new AssertionError(name);
        }
    }

    private synchronized void save() throws IOException {
        JSON.writerWithDefaultPrettyPrinter().writeValue(directory.resolve("native-report.json").toFile(), report);
    }
}
