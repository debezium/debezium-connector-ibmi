/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.ibm.as400.access.Job;

class As400NativeReaderStopProbeTest {
    private static final Map<String, Object> OWNED = Map.of("qualified_name", "123456/QUSER/QZRCSRVS", "status", "*ACTIVE",
            "internal_id", "123456abcdef", "entered_system", "2026-11-07T00:00:00Z");

    @Test
    void acceptsOnlyMatchingActiveLifetime() {
        assertThat(As400NativeReaderStopProbe.sameJob(OWNED, new HashMap<>(OWNED))).isTrue();
        assertThat(As400NativeReaderStopProbe.sameJob(null, OWNED)).isFalse();
        assertThat(As400NativeReaderStopProbe.sameJob(OWNED, null)).isFalse();
    }

    static Stream<Arguments> changedIdentities() {
        return Stream.of(Arguments.of("qualified_name", "654321/QUSER/QZRCSRVS"),
                Arguments.of("internal_id", "abcdef123456"), Arguments.of("entered_system", "2026-11-07T01:00:00Z"),
                Arguments.of("status", "*OUTQ"), Arguments.of("status", "not-found"),
                Arguments.of("qualified_name", ""), Arguments.of("internal_id", ""), Arguments.of("entered_system", ""));
    }

    @ParameterizedTest
    @MethodSource("changedIdentities")
    void refusesChangedMissingOrEndedJobBeforeNativeMutation(String key, String changed) {
        var current = new HashMap<>(OWNED);
        current.put(key, changed);
        assertThat(As400NativeReaderStopProbe.sameJob(OWNED, current)).isFalse();
        current.remove(key);
        assertThat(As400NativeReaderStopProbe.sameJob(OWNED, current)).isFalse();
    }

    @Test
    void equalIncompleteIdentitiesCannotAuthorizeNativeMutation() {
        for (String key : new String[]{ "qualified_name", "internal_id", "entered_system" }) {
            var incomplete = new HashMap<>(OWNED);
            incomplete.remove(key);
            assertThat(As400NativeReaderStopProbe.sameJob(incomplete, incomplete)).isFalse();
            incomplete.put(key, " ");
            assertThat(As400NativeReaderStopProbe.sameJob(incomplete, incomplete)).isFalse();
        }
        for (String id : new String[]{ "00000000", "not-an-id" }) {
            var invalid = new HashMap<>(OWNED);
            invalid.put("internal_id", id);
            assertThat(As400NativeReaderStopProbe.sameJob(invalid, invalid)).isFalse();
        }
    }

    @Test
    void lostHoldAcknowledgementStillRecoversOwnedJobAndPreservesFailure() throws Exception {
        Job job = mock(Job.class);
        AtomicBoolean attempted = new AtomicBoolean();
        AtomicBoolean actuallyHeld = new AtomicBoolean();
        IOException lostReply = new IOException("hold applied, acknowledgement lost");
        doAnswer(invocation -> {
            assertThat(attempted).isTrue();
            actuallyHeld.set(true);
            throw lostReply;
        }).when(job).hold(false);
        doAnswer(invocation -> {
            actuallyHeld.set(false);
            return null;
        }).when(job).release();
        Throwable failure = catchThrowable(() -> As400NativeReaderStopProbe.attemptHold(attempted, job));
        assertThat(failure).isSameAs(lostReply);
        assertThat(actuallyHeld).isTrue();
        String cleanup = As400NativeReaderStopProbe.recoverHold(attempted, job, OWNED, observed -> {
            assertThat(observed).isSameAs(job);
            var state = new HashMap<>(OWNED);
            state.put("active_status", Job.ACTIVE_JOB_STATUS_HELD);
            return state;
        });
        assertThat(cleanup).isEqualTo("matching held job released");
        assertThat(actuallyHeld).isFalse();
        assertThat(failure).isSameAs(lostReply);
        verify(job).release();
    }

    @Test
    void uncertainHoldNeverReleasesReusedOrUnheldJob() throws Exception {
        Job job = mock(Job.class);
        AtomicBoolean attempted = new AtomicBoolean(true);
        var changed = new HashMap<>(OWNED);
        changed.put("internal_id", "abcdefabcdef");
        changed.put("active_status", Job.ACTIVE_JOB_STATUS_HELD);
        As400NativeReaderStopProbe.recoverHold(attempted, job, OWNED, ignored -> changed);
        var unheld = new HashMap<>(OWNED);
        unheld.put("active_status", Job.ACTIVE_JOB_STATUS_RUNNING);
        As400NativeReaderStopProbe.recoverHold(attempted, job, OWNED, ignored -> unheld);
        As400NativeReaderStopProbe.recoverHold(new AtomicBoolean(false), job, OWNED, ignored -> {
            throw new AssertionError("No native observation without an attempted hold");
        });
        verify(job, never()).release();
    }

}
