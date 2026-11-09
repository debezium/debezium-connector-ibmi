/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/** Local-only fault writer, run in an isolated container with no source task or IBM i access. */
public final class As400OffsetStartupFault {
    public static void main(String[] args) throws Exception {
        if (args.length != 1 || !args[0].equals("/control/open-offset-ready")) {
            throw new IllegalArgumentException("Explicit isolated local fault marker required");
        }
        try (var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", "broker:9092",
                "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class,
                "transactional.id", "group-native-native-0", "transaction.timeout.ms", 180000))) {
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>("offsets-native", 0, "[\"fault\",{}]", "{}")).get();
            producer.flush();
            Files.createFile(Path.of(args[0]));
            // The host kills this container after observing the marker. No commit,
            // abort, manual fencing or connector offset replacement is performed.
            Thread.sleep(240000);
            throw new IllegalStateException("Fault producer was not killed within the test deadline");
        }
    }
}
