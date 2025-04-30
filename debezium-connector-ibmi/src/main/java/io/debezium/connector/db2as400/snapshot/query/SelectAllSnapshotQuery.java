/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400.snapshot.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import io.debezium.annotation.ConnectorSpecific;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.db2as400.As400RpcConnector;
import io.debezium.snapshot.spi.SnapshotQuery;

@ConnectorSpecific(connector = As400RpcConnector.class)
public class SelectAllSnapshotQuery implements SnapshotQuery {

    @Override
    public String name() {
        return CommonConnectorConfig.SnapshotQueryMode.SELECT_ALL.getValue();
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public Optional<String> snapshotQuery(String tableId, List<String> snapshotSelectColumns) {

        // if we include single quotes the column names turn into 00001,00002,... which we then can't map to the table
        return Optional.of(snapshotSelectColumns.stream().map(x -> x.replace("'", "\""))
                .collect(Collectors.joining(", ", "SELECT ", " FROM " + tableId)));
    }
}
