# Configuration and troubleshooting

## Ordinary Kafka Connect configuration

Start with the [row-capture example in the README](../README.md#2-configure-row-capture).
It inherits the worker's converters and preserves Debezium envelopes. Kafka
bootstrap settings, REST access and converter configuration belong to the
deployment; a schema registry is required only by converters that use one.

For updates, PUT only the inner `config` object to
`/connectors/ibmi-source/config`. Keep existing capture selection unchanged while
activating transactional truncate; a filter change is a separate operation with
its own snapshot/history requirements. Read the
[online contract](dcg-online-configuration.md) before adding transactional settings.
Do not add an unwrap transform without checking how it handles truncate events.

## Credentials and worker configuration

The README uses Kafka Connect's file config provider for the password. Enable it
in the worker properties on every worker eligible to run this connector:

```properties
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

The reference `${file:/run/secrets/ibmi.properties:password}` reads the `password`
property from that file. Provision the file through your deployment's secret
management, restrict its permissions, and make it available at the same path on
each eligible worker. Do not commit the file or its contents. If your deployment
already uses another config provider, use its reference syntax instead.

## Source authorities and receiver retention

The ordinary reader needs access to the journal, receivers and captured files.
An IBM i administrator can adapt these example grants to the actual libraries
and service account:

```text
GRTOBJAUT OBJ(<JRNLIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<JRNLIB>/*ALL) OBJTYPE(*JRNRCV) USER(<CDC_USER>) AUT(*USE)
GRTOBJAUT OBJ(<JRNLIB>/<JRN>) OBJTYPE(*JRN) USER(<CDC_USER>) AUT(*USE *OBJEXIST)
GRTOBJAUT OBJ(<APPLICATION_LIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<APPLICATION_LIB>/*ALL) OBJTYPE(*FILE) USER(<CDC_USER>) AUT(*USE)
```

These are **not a complete authority specification** for transactional capture.
That path also uses native object/member identities, QSYS2 catalog and lock
services, allocation/marker/release sessions, and reader-job identification and
cancellation. Qualify those authorities for the complete journal scope, including
files excluded from output. An authorization failure must not look like an empty
inventory or a successful lock release. See the
[online admission requirements](dcg-online-configuration.md#admission-requirements).

Retain receivers back to the replay floor, including history needed by unfinished
transactions on excluded tables. Automatic receiver deletion cannot discard that
evidence. Keep matching binding history immutable and durable alongside source
offsets. Missing history stops recovery; erasing offsets or history is not a
repair. Restore the required evidence or explicitly start a new consistent
connector generation.

## Nontransactional clears

With `truncate.transactional.enabled=false` and `dcg.mode=disabled`, setting
`skipped.operations=none` captures qualified F/CR clears outside commitment
control on single-member files. A nonzero commit cycle or ambiguous member scope
is rejected. Autocommit or SQL `IMMEDIATE` alone does not prove a zero commit
cycle; inspect the actual journal. See [F/CR integration testing](fcr-testing.md).

Consumers and transforms must preserve and understand `op=t`. Source capture does
not enable sink truncate handling. A truncate is not a series of per-key deletes
and does not erase old keys from Kafka's compacted history. Previously stale
destination data still needs reconciliation if no later clear is applied.

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
  [Prometheus configuration](../debezium-connector-ibmi/metrics/prometheus.txt) and
  [Grafana configuration](../debezium-connector-ibmi/metrics/grafana.json); a Prometheus port depends
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

## Upgrading this branch

Check these changes even with transactional capture disabled:

- Processing mode `fail` stops before journal advancement on decode/dispatch
  errors that older versions could log and lose. Ordinary row `warn`/`skip`
  processing remains explicitly lossy. Online activation requires processing
  mode `fail`; conversion WARN/SKIP is a separate setting.
- An existing `skipped.operations=none` configuration enables F/CR capture and
  its transaction/member checks described above.
- Row source metadata gains optional string `source.rrn`, adapted from
  [PR #92](https://github.com/debezium/debezium-connector-ibmi/pull/92) by Jona J.
  It does not change keys. IBM i can reuse or reorganize relative record numbers;
  they are member coordinates, not permanent business identifiers.

Ordinary source offsets provide at-least-once delivery: an interrupted output
prefix can repeat. An interrupted queue is retried from retained progress.
Transaction metadata after a mid-transaction legacy restart describes the
observed suffix, not reconstructed counts from before the saved offset.

After online handoff, the original connector cannot read the new offsets/history.
Downgrades require a compatible reader; replacing the binary does not by itself
provide an in-place rollback. Keep the retained receiver and history evidence.
The [compatibility matrix](dcg-compatibility-comparison.md) and
[online contract](dcg-online-configuration.md) describe qualified schema/member
changes and rejection of unavailable historical names or layouts.

## Earlier release configuration changes

- Debezium 3.0: `from_ccsid`/`to_ccsid` became `from.ccsid`/`to.ccsid`; JDBC
  properties use `driver.<parameter>`.
- Connector 1.10.4 introduced `secure`, defaulting to `true`.
- Connector 1.10.2 corrected receiver-reset and continuation-offset data loss.
- Avro converters require their own converter/schema-registry configuration;
  they are optional rather than a requirement for all deployments.

See [contributor testing](dcg-contributor-testing.md) for local runtime and
debugging prerequisites.
