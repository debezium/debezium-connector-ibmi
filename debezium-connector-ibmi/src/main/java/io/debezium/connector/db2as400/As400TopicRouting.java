/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Standard Connect transform configuration. Connect/Engine owns transformation,
 * filtering and errors. Pure RegexRouter chains additionally permit destination
 * preflight; arbitrary SMT destinations cannot be inferred from a table name.
 */
final class As400TopicRouting {
    private record Route(Pattern regex, String replacement) {
        String topic(String source) {
            var match = regex.matcher(source);
            return match.matches() ? match.replaceFirst(replacement) : source;
        }
    }

    private final List<Route> routes;
    private final Map<String, String> settings;
    private final boolean predictable;

    private As400TopicRouting(List<Route> routes, Map<String, String> settings, boolean predictable) {
        this.routes = List.copyOf(routes);
        this.settings = Map.copyOf(settings);
        this.predictable = predictable;
    }

    static As400TopicRouting from(Map<String, String> properties) {
        var routes = new ArrayList<Route>();
        var settings = new TreeMap<String, String>();
        boolean predictable = true;
        String configured = properties.getOrDefault("transforms", "").trim();
        if (!configured.isEmpty()) {
            var aliases = java.util.Arrays.stream(configured.split(",", -1)).map(String::trim).toList();
            if (aliases.stream().anyMatch(String::isEmpty) || aliases.stream().distinct().count() != aliases.size()) {
                throw new IllegalArgumentException("Distinct nonempty transform aliases required");
            }
            settings.put("transforms", String.join(",", aliases));
            for (String alias : aliases) {
                String prefix = "transforms." + alias + ".";
                String type = properties.get(prefix + "type");
                if (type == null || !type.trim().matches("[A-Za-z_$][A-Za-z0-9_$]*(\\.[A-Za-z_$][A-Za-z0-9_$]*)+")) {
                    throw new IllegalArgumentException("Explicit transform class required for " + alias);
                }
                properties.forEach((key, value) -> {
                    if (key.startsWith(prefix) && value != null) {
                        As400ConverterFingerprint.requireNonSecretCustomSetting(key);
                        settings.put(key, value);
                    }
                });
                type = type.trim();
                settings.put(prefix + "type", type);
                if (!"org.apache.kafka.connect.transforms.RegexRouter".equals(type) || properties.containsKey(prefix + "predicate")) {
                    predictable = false;
                    continue;
                }
                String regex = properties.get(prefix + "regex");
                String replacement = properties.get(prefix + "replacement");
                if (regex == null || replacement == null) {
                    throw new IllegalArgumentException("RegexRouter requires explicit regex and replacement");
                }
                // RegexRouter uses ConfigDef.STRING, which trims both settings.
                // Prediction and history must describe the worker's effective rules.
                regex = regex.trim();
                replacement = replacement.trim();
                routes.add(new Route(Pattern.compile(regex), replacement));
                settings.put(prefix + "type", type);
                settings.put(prefix + "regex", regex);
                settings.put(prefix + "replacement", replacement);
            }
        }
        for (var property : properties.entrySet()) {
            String key = property.getKey();
            if (key.equals("predicates") || key.startsWith("predicates.")) {
                As400ConverterFingerprint.requireNonSecretCustomSetting(key);
                if (property.getValue() != null) {
                    settings.put(key, property.getValue());
                }
            }
            if (key.startsWith("transforms.")
                    && property.getValue() != null && !property.getValue().isEmpty() && !settings.containsKey(key)) {
                throw new IllegalArgumentException("Transform option has no configured alias: " + key);
            }
        }
        return new As400TopicRouting(routes, settings, predictable);
    }

    Map<String, String> settings() {
        return settings;
    }

    boolean predictable() {
        return predictable;
    }

    String topic(String source) {
        if (!predictable) {
            throw new IllegalStateException("The configured transforms require an actual record to determine its destination");
        }
        String result = source;
        for (Route route : routes) {
            result = route.topic(result);
        }
        if (result == null || result.isEmpty()) {
            throw new IllegalArgumentException("Routing produced an empty destination");
        }
        return result;
    }

    String destination(String source, String historyTopic) {
        // Schema history has its own producer; it never passes through Connect SMTs.
        return source.equals(historyTopic) ? source : topic(source);
    }
}
