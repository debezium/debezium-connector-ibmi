/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.source.TransactionContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.db2as400.As400RecoveryTestSupport.Fixture;

class As400NativeHandoffSessionTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldRejectChangedRuntimeCapabilityAndStoppedPublicationAfterInitialAdmission(boolean sourceOffsets) throws Exception {
        var raw = Configuration.create().with("topic.prefix", "test").with("database.schema", "LIB").with("database.dbname", "DB")
                .with("skipped.operations", "none").with("provide.transaction.metadata", false)
                .with("exactly.once.support", "requested").with("transaction.boundary", "connector").with("errors.tolerance", "none")
                .with("event.converting.failure.handling.mode", "fail").build();
        var config = new As400ConnectorConfig(raw);
        var context = mock(SourceTaskContext.class);
        var transactions = mock(TransactionContext.class);
        when(context.transactionContext()).thenReturn(sourceOffsets ? null : transactions);
        var offsets = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsets);
        when(offsets.offset(Map.of("server", "test"))).thenReturn(new java.util.HashMap<>(Fixture.legacyOffset(29)));
        var p = Fixture.POLICY;
        var policy = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), p.output(),
                false, true, Map.of("server", "test"));
        try (var nativeHandles = mockConstruction(As400DcgNativeIo.class);
                var session = new As400NativeHandoffSession(config, context, mock(CdcSourceTaskContext.class), raw,
                        Fixture.JOURNAL, policy, "history", "control", As400ContinuousRuntime.Limits.experimental(), sourceOffsets)) {
            session.checkQualified(raw);
            when(context.transactionContext()).thenReturn(sourceOffsets ? transactions : null);
            assertThatThrownBy(() -> session.checkQualified(raw)).hasMessageContaining("capability changed");
            when(context.transactionContext()).thenReturn(sourceOffsets ? null : transactions);
            session.requestStop();
            assertThatThrownBy(() -> session.checkQualified(raw)).hasMessageContaining("stopped");
            verify(offsets).offset(Map.of("server", "test"));
            verifyNoMoreInteractions(offsets);
            nativeHandles.constructed().forEach(handle -> {
                verify(handle).requestStop();
                verifyNoMoreInteractions(handle);
            });
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "transactions", "unordered", "filter", "missing-offset", "malformed-delivery" })
    void shouldRejectUnqualifiedSourceOffsetHandoffBeforeNativeHandlesAreCreated(String fault) {
        var raw = Configuration.create().with("topic.prefix", "test").with("database.schema", "LIB").with("database.dbname", "DB")
                .with("skipped.operations", "none").with("provide.transaction.metadata", false).with("heartbeat.interval.ms", 1000)
                .with("errors.tolerance", "none").with("event.converting.failure.handling.mode", "fail");
        if (fault.equals("unordered")) {
            raw.with("record.processing.order", "UNORDERED");
        }
        if (fault.equals("filter")) {
            raw.with("transforms", "drop");
        }
        var config = new As400ConnectorConfig(raw.build());
        var context = mock(SourceTaskContext.class);
        var offsets = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsets);
        if (fault.equals("transactions")) {
            when(context.transactionContext()).thenReturn(mock(TransactionContext.class));
        }
        if (fault.equals("malformed-delivery")) {
            var saved = new java.util.HashMap<String, Object>(Fixture.legacyOffset(29));
            saved.put(As400DeliveryOffset.VERSION, "2");
            when(offsets.offset(Map.of("server", "test"))).thenReturn(saved);
        }
        var p = Fixture.POLICY;
        var policy = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), p.output(),
                false, true, Map.of("server", "test"));
        try (var nativeHandles = mockConstruction(As400DcgNativeIo.class)) {
            assertThatThrownBy(() -> new As400NativeHandoffSession(config, context, mock(CdcSourceTaskContext.class), Configuration.empty(),
                    Fixture.JOURNAL, policy, "history", "control", As400ContinuousRuntime.Limits.experimental(), true))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(nativeHandles.constructed()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "rejected-audit", "unsupported-schema", "failed-drain", "wrapped-failed-drain", "native-failure", "stopped" })
    void unqualifiedAuditCanRetryButNativeFailureOrUnfinishedDrainCannot(String fault) throws Exception {
        var raw = Configuration.create().with("topic.prefix", "test").with("database.schema", "LIB").with("database.dbname", "DB")
                .with("skipped.operations", "none").with("provide.transaction.metadata", false).with("heartbeat.interval.ms", 1000)
                .with("exactly.once.support", "requested").with("transaction.boundary", "connector").with("errors.tolerance", "none")
                .with("event.converting.failure.handling.mode", "fail").build();
        var context = mock(SourceTaskContext.class);
        when(context.transactionContext()).thenReturn(mock(TransactionContext.class));
        var offsets = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsets);
        when(offsets.offset(Map.of("server", "test"))).thenReturn(Map.of("offset.event_sequence", "42", "offset.receiver", "RCV",
                "offset.receiver_library", "LIB", "offset.processed", "true", "snapshot_completed", "true"));
        var p = Fixture.POLICY;
        var policy = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), p.output(), false, true,
                Map.of("server", "test"));
        var rejected = new As400NativeBoundaryCollector.AuditRejected(new IllegalArgumentException("Incomplete raw A/Z audit interval"), null, null);
        if (fault.equals("failed-drain")) {
            rejected.addSuppressed(new java.io.IOException("Native release uncertain"));
        }
        Exception failure = fault.equals("native-failure") ? new java.io.IOException("Native read failed")
                : fault.equals("unsupported-schema") ? new As400NativeEvidenceException("Unsupported native column type CLOB") : rejected;
        if (fault.equals("wrapped-failed-drain")) {
            var cause = new IllegalArgumentException("Unsupported column type");
            cause.addSuppressed(new java.sql.SQLException("Connection close failed"));
            failure = new As400NativeEvidenceException("Unsupported native evidence", cause);
        }
        final Exception expected = failure;
        try (var nativeHandles = mockConstruction(As400DcgNativeIo.class);
                var collectors = mockConstruction(As400NativeBoundaryCollector.class, (collector, ignored) -> when(collector.collect()).thenThrow(expected));
                var session = new As400NativeHandoffSession(new As400ConnectorConfig(raw), context, mock(CdcSourceTaskContext.class), raw,
                        Fixture.JOURNAL, policy, "history", "control", As400ContinuousRuntime.Limits.experimental())) {
            assertThat(session.runtimeLimits()).isEqualTo(As400ContinuousRuntime.Limits.experimental());
            if (fault.equals("stopped")) {
                session.requestStop();
                assertThatThrownBy(() -> session.attemptBoundary(1)).hasMessageContaining("stopped");
                assertThat(collectors.constructed()).isEmpty();
            }
            else if ((fault.equals("rejected-audit") || fault.equals("unsupported-schema"))) {
                assertThat(session.attemptBoundary(1)).isEmpty();
                assertThat(session.attemptBoundary(2)).isEmpty();
                assertThat(collectors.constructed()).hasSize(2);
            }
            else {
                assertThatThrownBy(() -> session.attemptBoundary(1)).isSameAs(expected);
            }
            // Candidate rejection never reads offsets again, persists history or
            // enters native row publication. Native handles are still owned until close.
            if (!fault.equals("stopped")) {
                verifyNoInteractions(nativeHandles.constructed().get(0));
                var discovery = nativeHandles.constructed().get(1);
                verify(discovery, times((fault.equals("rejected-audit") || fault.equals("unsupported-schema")) ? 2 : 1)).requireRetention();
                verifyNoMoreInteractions(discovery);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "missing-offset", "partial-offset", "warn", "negative-heartbeat", "no-transactions", "policy" })
    void unsafeAdmissionFailsBeforeCreatingAnyNativeHandles(String fault) {
        var raw = Configuration.create().with("topic.prefix", "test").with("database.schema", "LIB").with("database.dbname", "DB")
                .with("skipped.operations", "none").with("provide.transaction.metadata", false).with("heartbeat.interval.ms", 1000)
                .with("exactly.once.support", "requested").with("transaction.boundary", "connector").with("errors.tolerance", "none")
                .with("event.converting.failure.handling.mode", "fail");
        if (fault.equals("warn")) {
            raw.with("event.processing.failure.handling.mode", "warn");
        }
        if (fault.equals("negative-heartbeat")) {
            raw.with("heartbeat.interval.ms", -1);
        }
        var config = new As400ConnectorConfig(raw.build());
        var context = mock(SourceTaskContext.class);
        var offsets = mock(OffsetStorageReader.class);
        when(context.offsetStorageReader()).thenReturn(offsets);
        when(offsets.offset(Map.of("server", "test"))).thenReturn(null);
        if (!fault.equals("no-transactions")) {
            when(context.transactionContext()).thenReturn(mock(TransactionContext.class));
        }
        if (fault.equals("partial-offset")) {
            when(offsets.offset(Map.of("server", "test"))).thenReturn(Map.of("offset.event_sequence", "42"));
        }
        var p = Fixture.POLICY;
        var policy = new As400CaptureFingerprint.Policy(p.filters(), p.skippedOperations(), p.decoder(), p.schema(), p.output(),
                fault.equals("policy"), true, Map.of("server", "test"));
        // Deliberately no native credentials: the expected failure must precede
        // even native-handle construction, not merely fail a later connection.
        String message = switch (fault) {
            case "missing-offset" -> "Missing offsets cannot select automatic migration";
            case "partial-offset" -> "Completed legacy stream required";
            case "policy" -> "Handoff policy differs";
            case "no-transactions" -> "Qualified native handoff";
            default -> "Buffered dispatch requires";
        };
        assertThatThrownBy(() -> new As400NativeHandoffSession(config, context, mock(CdcSourceTaskContext.class), Configuration.empty(),
                Fixture.JOURNAL, policy, "history", "control", As400ContinuousRuntime.Limits.experimental()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining(message);
        if (!fault.endsWith("offset")) {
            verifyNoInteractions(offsets);
        }
    }
}
