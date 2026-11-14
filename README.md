# Debezium connector for IBM i

Capture changes from Db2 for IBM i and publish standard Debezium change events.
The connector takes an initial snapshot and then reads the IBM i journal for
inserts, updates and deletes. Optional transactional truncate capture also emits
table clears, so consumers can apply a clear followed by a reload correctly.

This branch proposes transactional truncate support in
[PR #117](https://github.com/debezium/debezium-connector-ibmi/pull/117), tracking
[debezium/dbz#3027](https://github.com/debezium/dbz/issues/3027).
The feature is **off by default**. See the
[compatibility matrix](docs/dcg-compatibility-comparison.md) for supported behavior
and the [validation report](docs/dcg-validation.md) for tested profiles and limits.

[Get started](#getting-started) · [Enable truncate capture](#transactional-truncate-capture) ·
[Documentation](#documentation) · [Contribute](#development-and-contributions)

## Getting started

You need an IBM i source with journaling enabled, a service account with access
to the required files and journal receivers, and a Kafka Connect deployment with
durable source offsets. Configure Kafka connectivity and converters on the
workers. A schema registry is needed only if your converter requires one.

Check [source authorities and receiver retention](docs/connector-usage.md#source-authorities-and-receiver-retention)
before starting. TLS is enabled by default; the worker JVM must trust the IBM i
server's certificate.

Standalone Connect, Embedded Engine and Debezium Server have different offset,
history and acknowledgement settings. The example below uses Kafka Connect;
see [runtime compatibility](docs/dcg-compatibility-baseline.md#deployment-evidence)
for the other tested profiles.

### 1. Build and install

Use **JDK 21** and the Maven wrapper:

```sh
./mvnw -B -ntp clean install -Passembly -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
```

The build runs offline Java tests and creates the complete plugin distribution:

```text
debezium-connector-ibmi/target/debezium-connector-ibmi-3.8.0-SNAPSHOT-plugin.zip
debezium-connector-ibmi/target/debezium-connector-ibmi-3.8.0-SNAPSHOT-plugin.tar.gz
```

Extract either archive into a dedicated directory under each worker's
`plugin.path`, then restart the workers. Install the complete distribution,
including its dependencies.

The project uses Debezium `3.8.0-SNAPSHOT`. If matching core dependencies are
unavailable, build the modules listed in
[the Maven workflow](.github/workflows/cross-maven.yml) first. `-DskipITs` skips
live integration tests; it does not skip the offline Java tests.

### 2. Configure row capture

Save the following as `connector.json`, replacing the source names. The password
reference requires the worker's
[file config provider](docs/connector-usage.md#credentials-and-worker-configuration).
It is a reference to your deployment's secret file, not a literal password.

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

Submit it to your Connect REST endpoint:

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  --data-binary @connector.json http://localhost:8083/connectors
curl --fail-with-body http://localhost:8083/connectors/ibmi-source/status
```

The connector snapshots the selected table, then streams its changes using the
configured topic naming strategy. For updates, send only the inner `config`
object with `PUT /connectors/ibmi-source/config`. See
[configuration and troubleshooting](docs/connector-usage.md) for encoding,
transport, monitoring and journal diagnostics.

## Transactional truncate capture

IBM i can clear a file without recording a delete for every row. Native
`CLRPFM` can produce `F/CR` journal records; SQL `TRUNCATE` and optimized bulk
`DELETE` can produce `D/CG` subtypes 8 and 3. These names identify native journal
record types; the actual records depend on the operation and transaction context.

The connector represents a clear as a standard Debezium truncate event:
`op=t`, a null key, and null `before` and `after` values.

| Source behavior | Transactional capture |
| --- | --- |
| Clear, insert replacement rows, commit | Publishes the clear before the replacement rows |
| Clear and rows followed by a full rollback | Publishes neither the clear nor the rolled-back rows |
| A clear transaction stays open | Unrelated committed changes continue |
| Task stops before delivery finishes | Rebuilds pending work from retained journal history and durable source offsets |

### Enable on an existing stream

Read the [online configuration contract](docs/dcg-online-configuration.md) first.
Add these settings to the existing connector configuration:

```properties
truncate.transactional.enabled=true
dcg.mode=disabled
skipped.operations=none
event.processing.failure.handling.mode=fail
dcg.journal=JRNLIB/JOURNAL
dcg.retention=operator-managed
```

These are additions, not a complete configuration. Preserve the existing source
partition, topic prefix and capture selection during handoff. Standard
`table.include.list` and `table.exclude.list` apply; there is no separate
per-table truncate flag.

Activation starts from an existing durable streaming offset without another
snapshot or an administrator-managed writer pause. The connector validates the
journal scope and uses brief automatic write-excluding locks to establish a safe
boundary. Busy attempts defer while ordinary capture continues. Metadata
collection runs outside the held interval; total activation time is not the time
application writes are locked. The scope includes journaled files excluded from
output. History storage, source authorities and complete captured-row images must
satisfy the linked contract.

A separate [operator-controlled first-start profile](docs/dcg-configured-native.md)
supports a planned initialization window. Do not mix its settings with online
activation.

### Choose transaction storage

| Setting | When to use it |
| --- | --- |
| `truncate.buffer.storage=disk` (default) | Large or unpredictable pending transactions; H2 MVStore keeps transaction payloads off the Java heap |
| `truncate.buffer.storage=memory` | Predictable transaction sizes with sufficient heap; avoids transaction spill-store overhead |

Memory mode defaults to 64 open transactions, 10,000 retained entries and 16 MiB
of source payloads. Entries include before-images and bookkeeping, and transaction
context includes excluded files. Java objects and prepared output require
additional heap. Exceeding a budget stops capture with its checkpoint preserved.
See [storage budgets](docs/dcg-configured-native.md#transaction-working-storage).

Both modes use the same reader and recovery rules. Disk working files are
disposable; source offsets and immutable schema/binding history govern recovery.
Memory mode still needs writable scratch space for startup/handoff and durable
offset/history storage.

The [matched performance comparison](docs/dcg-performance-comparison.md#matched-reader-and-storage-controls--2026-11-14)
separates storage cost from the legacy reader's five-second delay. Lower latency
than the legacy default is not evidence that disk is faster than memory.

### Operating requirements and boundaries

- **Consumers must handle `op=t`.** Source capture does not enable sink truncate
  handling. A truncate does not erase old keys from Kafka's compacted history.
- **Clear/reload ordering is per table.** Use one Kafka partition per table topic,
  or equivalent downstream coordination, to order a keyless clear against keyed
  rows. Ordinary row capture can use multiple partitions.
- **Retain recovery evidence.** Keep journal receivers back to the oldest
  unfinished transaction, including excluded tables, and retain immutable matching
  history. Missing evidence stops recovery; clearing offsets is not a repair.
- **Delivery is ordinarily at least once.** Interrupted output can repeat.
  Kafka source transactions have a separately qualified runtime contract.
- **Member-only clears need care.** Existing multi-member rows share keys and
  topics, and snapshots read the first member. A clear of one member cannot
  safely become a whole-table truncate in that format.
- **Downgrades require compatible history readers.** After handoff, the original
  connector cannot read the new offsets/history.

Captured external LOB/XML values, member-aware output and arbitrary historical
DDL reconstruction are not added. Remote journals and independent ASPs remain
unqualified; native storage-pool validation used pool 1. The
[compatibility matrix](docs/dcg-compatibility-comparison.md) distinguishes inherited
behavior, added capabilities and remaining support boundaries.

## Upgrading from the original connector

Review these changes even when transactional capture is disabled:

- Processing mode `fail` stops on decode/dispatch errors that older versions
  could log and drop. Ordinary `warn`/`skip` processing remains explicitly
  lossy; online activation requires `fail`.
- An existing `skipped.operations=none` setting enables F/CR capture and its
  transaction/member checks. See [nontransactional clears](docs/connector-usage.md#nontransactional-clears).
- Row source metadata gains optional string `source.rrn`. This does not change
  event keys; a relative record number is not a permanent business identifier.

See [upgrade details](docs/connector-usage.md#upgrading-this-branch) before replacing
a running connector.

## Documentation

| Task | Guide |
| --- | --- |
| Configure source access, encoding, TLS and monitoring | [Configuration and troubleshooting](docs/connector-usage.md) |
| Enable transactional truncate and plan recovery | [Online configuration](docs/dcg-online-configuration.md) |
| Check deployment and feature compatibility | [Compatibility matrix](docs/dcg-compatibility-comparison.md) · [Runtime evidence](docs/dcg-compatibility-baseline.md) |
| Understand buffering, journal positions and restart | [Architecture](docs/dcg-architecture.md) |
| Compare latency, CPU and storage choices | [Performance measurements and methods](docs/dcg-performance-comparison.md) |
| Assess completed tests and their limits | [Validation report](docs/dcg-validation.md) |
| Reproduce tests or validate a source system | [Contributor testing](docs/dcg-contributor-testing.md) · [F/CR testing](docs/fcr-testing.md) |

Validation includes offline recovery/failure tests, local Kafka checks, isolated
native commit/rollback and lifecycle cases, performance comparisons, a 128-file
journal scope, and restart/continuous endurance runs. The linked reports retain
artifact identities, original failed attempts, cleanup results and measurement
limits. These are bounded results, not maximum-capacity or universal workload
qualification. Upstream CI and review status are tracked on the PR.

## Development and contributions

Run the offline checks without an IBM i account:

```sh
./mvnw -B -ntp verify -DskipITs
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

Local Kafka tests require the runtime described in the testing guide. Native
tests require a real IBM i system and a fresh, explicitly owned disposable
library; Db2 LUW in a container is not an equivalent source.

| Directory | Purpose |
| --- | --- |
| `debezium-connector-ibmi/` | Connector task, snapshot/streaming runtime and recovery |
| `journal-parsing/` | Journal retrieval and native row decoding |
| `jt400-override-ccsid/` | JT400 encoding support |
| `tools/` | Optional validation harnesses |
| `docs/` | Configuration, design and evidence |

Follow the [Debezium contribution guide](https://github.com/debezium/debezium/blob/main/CONTRIBUTING.md)
and [repository guidance](AGENTS.md). Contributions use DCO sign-offs; upstream
author registration in `COPYRIGHT.txt` and
`jenkins-jobs/scripts/config/Aliases.txt` is a separate contribution step.

For questions, use the [Debezium community channels](https://debezium.io/community/).
Report bugs and enhancement requests in the
[Debezium issue tracker](https://github.com/debezium/dbz/issues), including the
connector/runtime versions, relevant configuration with secrets removed, and
a reproducible case.

## License

[Apache License 2.0](LICENSE.txt).
