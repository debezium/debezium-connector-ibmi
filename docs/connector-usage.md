# Configuration and troubleshooting

## Ordinary Kafka Connect configuration

The following is an ordinary row-capture request. Replace the source names and
use a configured secret provider for the password. The `${file:...}` example
requires the Kafka Connect file config provider to be enabled on the worker.
Converter choices and Kafka bootstrap settings belong to the deployment; this
example inherits the worker's converters and preserves Debezium envelopes.

```json
{
  "name": "ibmi-source",
  "config": {
    "connector.class": "io.debezium.connector.db2as400.As400RpcConnector",
    "tasks.max": "1",
    "database.hostname": "ibmi.example.test",
    "database.dbname": "DATABASE",
    "database.schema": "APPLICATION",
    "database.user": "CDC_USER",
    "database.password": "${file:/run/secrets/ibmi.properties:password}",
    "secure": "true",
    "topic.prefix": "ibmi",
    "table.include.list": "APPLICATION.TABLE1",
    "snapshot.mode": "initial"
  }
}
```

Submit a locally saved `connector.json` to the Connect REST endpoint:

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @connector.json http://localhost:8083/connectors
curl --fail-with-body http://localhost:8083/connectors/ibmi-source/status
```

For updates, PUT only the inner `config` object to
`/connectors/ibmi-source/config`. Keep existing capture selection unchanged while
activating transactional truncate; a filter change is a separate operation with
its own snapshot/history requirements. Read the
[online contract](dcg-online-configuration.md) before adding transactional settings.
Do not add an unwrap transform without checking how it handles truncate events.

## Encoding, transport and monitoring

- `from.ccsid` and `to.ccsid` override the declared source encoding across the
  capture. Prefer correcting invalid source metadata; an override can corrupt
  correctly encoded tables if applied indiscriminately.
- JDBC properties use the `driver.` prefix, for example `driver.date format`.
  The date format defaults to `iso`. Connection timeout/keepalive settings must
  suit the actual network; old deployment examples are not universal defaults.
- For a private CA, verify the certificate through your administrator and import
  it into the worker's configured JVM truststore. `openssl s_client -showcerts
  -connect <host>:9471` can inspect the presented chain; receiving a certificate
  does not itself establish that it should be trusted. Do not disable TLS as a
  certificate-management workaround.
- Debezium exposes metrics through JMX. Existing examples are in
  `../metrics/prometheus` and `../metrics/grafana.txt`; a Prometheus port depends
  on the exporter configured by the deployment, not the source connector.

If rows are missing, inspect `DSPFD FILE(LIBRARY/TABLE)` and confirm that the file
is journaled to the intended journal with the required complete images
(`IMAGES(*BOTH)` for captured updates). Check filters, receiver availability,
source permissions and task errors before changing offsets.

Receiver loss cannot be repaired by inventing a new position. The transactional
path rejects missing recovery evidence; retain the failed generation for
inspection and follow the recovery contract. Heap and disk budgets depend on
catalog size, transaction history and worker load. The historical 1 GiB heap
recommendation is a starting point, not a capacity guarantee.

## Earlier release configuration changes

- Debezium 3.0: `from_ccsid`/`to_ccsid` became `from.ccsid`/`to.ccsid`; JDBC
  properties use `driver.<parameter>`.
- Connector 1.10.4 introduced `secure`, defaulting to `true`.
- Connector 1.10.2 corrected receiver-reset and continuation-offset data loss.
- Avro converters require their own converter/schema-registry configuration;
  they are optional rather than a requirement for all deployments.

See [contributor testing](dcg-contributor-testing.md) for maintained local runtime
and debugging prerequisites. The old private deployment links and IDE-specific
launch configurations are not required to build or test this repository.
