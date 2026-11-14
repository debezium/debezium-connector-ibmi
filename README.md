# Debezium connector for IBM i

Capture changes from the Db2 for IBM i journal and publish Debezium change events.
The connector supports snapshots and streaming row changes. This branch adds
optional transactional truncate capture, so a clear followed by a reload does
not leave stale rows in a downstream system.

**Status:** the complete transactional-truncate proposal is in
[draft PR #117](https://github.com/debezium/debezium-connector-ibmi/pull/117), tracking
[debezium/dbz#3027](https://github.com/debezium/dbz/issues/3027).
The feature defaults to off. Bounded native validation has passed; upstream CI,
maintainer acceptance and broader workload qualification remain separate gates.

## How clears are captured

IBM i can clear a file without recording a delete for every row. Native `CLRPFM`
can produce `F/CR`; SQL `TRUNCATE` and optimized bulk `DELETE` can produce `D/CG`
subtypes 8 and 3. The actual journal records depend on the operation and context.
The connector emits the standard Debezium truncate envelope: `op=t`, a null key,
and null `before` and `after` values.

With transactional capture enabled:

- `TRUNCATE → INSERT → COMMIT` publishes the truncate before the inserted rows.
- A full rollback publishes neither the clear nor its rolled-back rows.
- Unrelated committed changes continue while a clear transaction remains open.
- Recovery rebuilds unfinished work from retained journal records. Separate scan,
  replay-floor and acknowledged-output positions prevent a successful read from
  being mistaken for durable delivery.

Working transaction data spills to H2 MVStore on disk by default. Standard source
offsets and immutable schema/binding history are the recovery authority; the
working spill files are disposable. No custom ownership or receipt service is
required. See the [architecture](docs/dcg-architecture.md).

## Build and install

Use JDK 21 and the Maven wrapper:

```sh
./mvnw -B -ntp clean install -Passembly -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

This repository uses Debezium `3.8.0-SNAPSHOT` dependencies. If they are unavailable,
build the matching core modules listed in
[the Maven workflow](.github/workflows/cross-maven.yml) first. `-DskipITs` avoids
live IBM i integration tests; the default Java tests need no IBM i account.

The distribution is generated under `debezium-connector-ibmi/target/` as
`debezium-connector-ibmi-3.8.0-SNAPSHOT-plugin.zip` and `.tar.gz`.
Extract the complete distribution into a dedicated Kafka Connect plugin directory
and restart the workers. Configure the connector class as
`io.debezium.connector.db2as400.As400RpcConnector`.

Set `database.hostname`, `database.user`, `database.password`, `database.dbname`
and `database.schema` for the source, plus `topic.prefix` and the standard
Debezium table selection and snapshot settings. Supply credentials through the
runtime's secret/config provider. TLS is controlled by `secure` and defaults to
`true`; install the source's trusted CA certificate in the worker JVM truststore.
Kafka bootstrap servers, converters, schema registry and REST listeners belong
to the chosen runtime/deployment; a schema registry is not a universal requirement
of this connector.

Distributed Connect, standalone Connect and Embedded Engine have distinct offset
and delivery contracts. The tested Debezium Server JVM/HTTP profile uses ordered
processing with file-backed history and offsets. See the
[runtime compatibility evidence](docs/dcg-compatibility-baseline.md) before choosing
a deployment mode; one validated Server sink does not qualify every sink.

See [configuration and troubleshooting](docs/connector-usage.md) for a minimal
connector request, CCSID settings, TLS and journal diagnostics.

## Enable transactional truncate on an existing stream

Read the [online configuration contract](docs/dcg-online-configuration.md) before
enabling the feature. The essential additions to an already-streaming connector
are:

```properties
truncate.transactional.enabled=true
skipped.operations=none
event.processing.failure.handling.mode=fail
dcg.journal=JRNLIB/JOURNAL
dcg.retention=operator-managed
```

Keep the existing source partition, topic prefix and capture selection during
handoff. Use the normal `table.include.list`/`table.exclude.list`; there is no
separate per-table truncate flag. Leave `dcg.mode=disabled` for this online path.
The settings above are additions, not a complete connector configuration: durable
history, internal topics or file storage, source authorities and complete captured
images must also satisfy the linked contract.

The connector can start from an existing durable legacy offset without another
snapshot or an administrator-managed writer pause. It discovers and validates
the journal scope, then uses brief automatic write-excluding locks to establish
a safe handoff. Busy attempts defer while ordinary capture continues. Metadata
collection runs outside the held interval. A transactional clear encountered
before handoff holds its affected table until its outcome is known; unrelated
tables continue.

The separate [operator-controlled first-start profile](docs/dcg-configured-native.md)
supports an explicitly planned initialization window. Do not mix its settings
with the online activation path.

### F/CR without transactional capture

With `truncate.transactional.enabled=false` and `dcg.mode=disabled`, enabling
`skipped.operations=none` captures qualified clears outside commitment control
on single-member files. A nonzero commit cycle or ambiguous member scope is
rejected. Autocommit or SQL `IMMEDIATE` alone does not prove a zero commit cycle;
inspect the source journal. See [F/CR integration testing](docs/fcr-testing.md).

### Ordering and downstream handling

A keyless truncate cannot be ordered against keyed rows in different Kafka
partitions. Use **one partition per table topic**, or equivalent downstream
coordination, when whole-table clear/reload ordering is required. Ordinary row
capture can retain multiple partitions. Internal history/control topics have
their own single-partition requirements.

Consumers and transforms must preserve and understand `op=t`. Source capture does
not enable sink truncate handling. A truncate is not a series of per-key deletes
and does not erase old keys from Kafka's compacted history. Previously stale
destination data still needs reconciliation if no later clear is applied.

## IBM i authorities and retained history

The ordinary journal reader needs access to its journal, receivers and captured
files. Example grants, adapted to the actual libraries and service account:

```text
GRTOBJAUT OBJ(<JRNLIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<JRNLIB>/*ALL) OBJTYPE(*JRNRCV) USER(<CDC_USER>) AUT(*USE)
GRTOBJAUT OBJ(<JRNLIB>/<JRN>) OBJTYPE(*JRN) USER(<CDC_USER>) AUT(*USE *OBJEXIST)
GRTOBJAUT OBJ(<APPLICATION_LIB>) OBJTYPE(*LIB) USER(<CDC_USER>) AUT(*EXECUTE)
GRTOBJAUT OBJ(<APPLICATION_LIB>/*ALL) OBJTYPE(*FILE) USER(<CDC_USER>) AUT(*USE)
```

These grants are **not a complete authority specification** for transactional
capture. That path also uses native object/member identities, QSYS2 catalog and
lock services, dedicated allocation/marker/release sessions and reader-job
identification/cancellation. Qualify those authorities for the complete journal
scope, including files excluded from output. An authorization failure must not
look like an empty inventory or a successful lock release.

Operators must retain receivers back to the replay floor, including history
needed by unfinished transactions on excluded tables. Automatic receiver deletion
cannot discard that evidence. Keep binding history immutable and durable alongside
the source offsets. Missing history stops recovery; erasing offsets or history is
not a repair. Restoring the required evidence or creating a new consistent
generation is an explicit recovery operation.

## Upgrade behavior and support boundaries

Even with transactional capture disabled, check these upgrade changes:

- Processing mode `fail` now stops before journal advancement on decode/dispatch
  errors that older versions could log and lose. Ordinary row `warn`/`skip`
  processing remains explicitly lossy; online activation requires `fail`.
- An existing `skipped.operations=none` configuration enables F/CR capture and its
  transaction/member safety checks.
- Row source metadata gains optional string `source.rrn`, adapted from
  [PR #92](https://github.com/debezium/debezium-connector-ibmi/pull/92) by Jona J.
  It does not change keys. IBM i may reuse or reorganize relative record numbers;
  they are member coordinates, not permanent business identifiers.

Ordinary offsets provide at-least-once delivery: an interrupted output prefix can
repeat. Kafka source transactions have a separately qualified runtime contract.
An interrupted queue is retried from retained progress; transaction metadata
after a mid-transaction restart describes the observed suffix, not reconstructed
counts from before the saved offset.

The connector preserves existing row keys, topics and supported conversions.
It does not add captured external LOB/XML values, arbitrary historical DDL
reconstruction, or member-aware keys/topics/snapshots. Ordinary multi-member rows
retain the original shared-topic contract; clearing one member cannot safely
become a whole-table `op=t`. Unknown historical names/layouts or missing identity
proof reject rather than silently misroute data. Remote journals and independent
ASPs remain unqualified. Native storage-pool validation has used pool 1.

After online handoff, the original connector cannot read the new offsets/history.
Downgrades require a compatible reader; replacing the binary is not automatically
an in-place rollback. Read the [compatibility matrix](docs/dcg-compatibility-comparison.md)
and [configuration restrictions](docs/dcg-online-configuration.md) for the exact
qualified cases and remaining gaps.

## Validation and contributor guidance

The published runtime at `96a395a` has the following bounded evidence:

| Check | Result and scope |
| --- | --- |
| Offline verification | 5,285 Java executions on matching production sources; formatter/import and assembly checks. Subsequent test-inspector correction: four distinct Java cases and 153 Python tests |
| Captured file moves | Four native configurations, 148 assertions: both/source/destination selection with ordinary offsets, plus both-library capture with Kafka source transactions; restart, clear outcomes and exact broker output checked |
| Original vs. candidate | Six serial 120-second native runs, 144,000 verified changes total at about 201 changes/second. Feature-off CPU/change +3.6%; disk path +23.6%, with lower observed delivery delay at that load |
| Restart endurance | Fresh 20-minute mixed-workload run: 92,401 observed events, three task restarts, three receiver rotations, 5,000-row commit/rollback and unrelated progress during an open clear |

The first endurance attempt exposed a test-inspector record limit; its failure
is preserved and the corrected harness passed on a fresh fixture. One initial
move run needed a controller resume before its workload; subsequent runs used
the corrected harness. All owned native fixtures and local runtimes were removed
and independently checked. These are not maximum-capacity, multi-day or universal
replacement-readiness claims. Upstream CI results are tracked separately on the PR.

- [Reproduce offline, local Kafka and optional isolated native tests](docs/dcg-contributor-testing.md)
- [Validation scope, artifact identity and historical evidence](docs/dcg-validation.md)
- [Performance methods, measurements and limitations](docs/dcg-performance-comparison.md)
- [Repository contribution guidance](AGENTS.md)

The modules are `debezium-connector-ibmi` (task/runtime), `journal-parsing`
(retrieval/decoding), and `jt400-override-ccsid` (driver encoding support).
Optional validation harnesses live in `tools/`; contracts and results live in
`docs/`. Native tests require a fresh, owned, disposable library and explicit
authorization; they must never target an existing application library.
