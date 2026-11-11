/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;

class As400ErrorHandlerTest {
    @ParameterizedTest
    @ValueSource(strings = { "row", "truncate", "metadata", "transport" })
    void shouldRetryOnlyTransportErrorsEvenWhenDecodeFailuresContainIOException(String kind) {
        var malformed = new IOException("same unreadable journal bytes");
        Throwable failure = switch (kind) {
            case "row" -> new As400StreamingChangeEventSource.RowProcessingException("invalid row", malformed);
            case "truncate" -> new As400StreamingChangeEventSource.TruncateException("invalid clear", malformed);
            case "metadata" -> new As400StreamingChangeEventSource.TransactionMetadataException("invalid metadata", malformed);
            default -> malformed;
        };
        var queue = mock(ChangeEventQueue.class);
        var config = new As400ConnectorConfig(Configuration.create().with("topic.prefix", "test").build());
        var handler = new As400ErrorHandler(config, queue);
        handler.setProducerThrowable(new ConnectException("coordinator failure", failure));
        var delivered = ArgumentCaptor.forClass(RuntimeException.class);
        verify(queue).producerException(delivered.capture());
        assertThat(delivered.getValue() instanceof RetriableException).isEqualTo(kind.equals("transport"));
        assertThat(delivered.getValue()).hasRootCause(malformed);
    }
}
