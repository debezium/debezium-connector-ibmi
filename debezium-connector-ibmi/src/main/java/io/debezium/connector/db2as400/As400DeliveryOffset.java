/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/** Delivery watermark in the framework offset store; never a journal scan or recovery position. */
final class As400DeliveryOffset {
    static final String VERSION = "delivery.version";
    static final String SESSION = "delivery.session";
    static final String SEQUENCE = "delivery.sequence";
    static final String RECEIPT = "delivery.receipt";
    private static final Set<String> KEYS = Set.of(VERSION, SESSION, SEQUENCE);

    private As400DeliveryOffset() {
    }

    /** Validate the complete optional envelope before passing F/W to the existing strict decoder. */
    static Map<String, ?> strip(Map<String, ?> source) {
        if (source == null || source.keySet().stream().noneMatch(key -> key.startsWith("delivery."))) {
            return source;
        }
        boolean signed = "2".equals(source.get(VERSION));
        if ((!signed && !"1".equals(source.get(VERSION))) || !(source.get(SESSION) instanceof String session)
                || !UUID.fromString(session).toString().equals(session)
                || !(source.get(SEQUENCE) instanceof String sequence) || !sequence.matches("[1-9][0-9]{0,18}")
                || Long.parseLong(sequence) <= 0
                || signed && (!(source.get(RECEIPT) instanceof String receipt) || !receipt.matches("[0-9a-f]{64}"))
                || source.keySet().stream().anyMatch(key -> key.startsWith("delivery.") && !KEYS.contains(key) && !(signed && key.equals(RECEIPT)))) {
            throw new IllegalArgumentException("Malformed delivery offset");
        }
        var result = new HashMap<String, Object>(source);
        KEYS.forEach(result::remove);
        result.remove(RECEIPT);
        return Map.copyOf(result);
    }

    /**
     * Task-local receipt for exact emitted offsets, so delivered records can be
     * discarded before a flush. This is not fencing or authentication of restored
     * history: a fresh task validates the saved F/W through the ordinary startup
     * protocol and never needs this ephemeral key. Owned by the synchronized tracker.
     */
    static final class Signer {
        private final String session;
        private final Mac mac;

        Signer(String session) {
            this.session = session;
            try {
                byte[] key = new byte[32];
                new SecureRandom().nextBytes(key);
                mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(key, "HmacSHA256"));
                java.util.Arrays.fill(key, (byte) 0);
            }
            catch (GeneralSecurityException e) {
                throw new IllegalStateException("Delivery receipt provider unavailable", e);
            }
        }

        Map<String, ?> stamp(Map<String, ?> source, long sequence) {
            if (source == null || source.keySet().stream().anyMatch(key -> key.startsWith("delivery.")) || sequence <= 0) {
                throw new IllegalArgumentException("Invalid or already stamped delivery offset");
            }
            var result = new HashMap<String, Object>(source);
            result.put(VERSION, "2");
            result.put(SESSION, session);
            result.put(SEQUENCE, Long.toString(sequence));
            result.put(RECEIPT, HexFormat.of().formatHex(digest(result)));
            return Map.copyOf(result);
        }

        long verify(Map<String, ?> offset) {
            strip(offset);
            if (offset == null || !"2".equals(offset.get(VERSION)) || !session.equals(offset.get(SESSION))
                    || !MessageDigest.isEqual(HexFormat.of().parseHex((String) offset.get(RECEIPT)), digest(offset))) {
                throw new IllegalStateException("Durable offset has no matching emitted delivery receipt");
            }
            return Long.parseLong((String) offset.get(SEQUENCE));
        }

        private byte[] digest(Map<String, ?> offset) {
            mac.reset();
            for (var field : new TreeMap<>(offset).entrySet()) {
                if (!field.getKey().equals(RECEIPT)) {
                    if (!(field.getValue() instanceof String value)) {
                        throw new IllegalArgumentException("Native delivery offsets require string fields");
                    }
                    text(field.getKey());
                    text(value);
                }
            }
            return mac.doFinal();
        }

        private void text(String value) {
            int length = value.length();
            for (int shift = 24; shift >= 0; shift -= 8) {
                mac.update((byte) (length >>> shift));
            }
            for (int i = 0; i < length; i++) {
                mac.update((byte) (value.charAt(i) >>> 8));
                mac.update((byte) value.charAt(i));
            }
        }
    }
}
