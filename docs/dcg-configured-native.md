# Operator-controlled initialization

This optional profile initializes a new transactional-truncate generation during
an operator-controlled first-start window. It uses the packaged
`As400RpcConnector` / `As400ConnectorTask` in Kafka Connect, with no fixture
provider or separate ownership/receipt service. `dcg.mode=disabled` is the default.

This profile has a narrower source and runtime contract than
[online activation](dcg-online-configuration.md). Use online activation to enable
transactional capture from existing streaming offsets without a planned writer
pause. The settings below are for a new generation, not a migration of legacy
offsets.

## Supported configuration

`snapshot.mode=initial` is the only admitted snapshot policy: create the
baseline on an explicitly admitted first start, then use durable F/W on restart.
Other modes, including `no_data` and `initial_only`, reject before I/O; they are
never silently converted to initial-plus-streaming. Nonempty snapshots emit one
final `source.snapshot=last`, even with trailing empty tables. An all-empty
snapshot has no data record to mark; its durable BASELINE/INITIAL barriers still
control completion. A LAST row carries a pending offset, never early F/W.


- IBM i local journal in system ASP 1, one library, 1–100 explicitly named
  single-member physical data files. Every journaled file must be bound in
  `dcg.tables` and `dcg.controlled.tables`. Standard `table.include.list` or
  `table.exclude.list` selects output within that inventory. Journal reading,
  native binding history and transaction validation still cover every file;
  filtering never removes transaction controls or permits an unknown layout.
  The library itself may be journaled if named in `dcg.journaled.libraries`.
  Data areas/queues, IFS objects, access paths and remote journals are rejected.
- Fixed table definitions with optional SQL primary keys or DDS physical-file keys, or explicit
  `dcg.key.mode=rrn` for native relative-record-number keys, and the strict native codecs:
  small/integer/big integers, real/double, boolean, decimal/numeric, supported
  CCSID char/varchar, graphic/vargraphic, binary/varbinary and character
  `FOR BIT DATA`, plus explicitly ISO DATE/TIME and
  TIMESTAMP(0–9). Temporal layouts require CCSID 37, 500 or 1140 and exact catalog
  storage/buffer lengths and precision. TIME preserves `24:00:00` as 24 hours;
  DATE/TIMESTAMP values have no timezone conversion. Timestamp precision above 9,
  timestamp hour 24, non-ISO layouts, LOB/XML and other layouts remain rejected.
  TIMESTAMP(7–9) also requires a value representable by Debezium's epoch-nanosecond
  converter; see [validation scope](dcg-validation.md). Mixed-column native
  qualification covers the columns in the validation report, not every possible
  CCSID/type/precision combination. Unsupported metadata fails before output.
  Active row or column access controls reject: SQL snapshots can be masked or
  filtered while native journal images are not. Acquisition requires an exact
  `QSYS2.SYSTABLES.CONTROL` observation with no enforcement.
- Both before/after journal images, no minimized images, no removed internal or
  fixed-length data, no journal cache. Required receiver fields include job and
  thread identity; XA/LUW identity layouts are not admitted. Every required
  receiver is checked, not just the current journal settings.
- Operator-managed retention of the complete receiver chain from durable F through
  the current attachment, including W and immutable binding history. No sequence
  reset, replacement, forced receiver deletion, uncoordinated DDL or journaling
  changes. The collector follows exact reciprocal links; names do not define order.
  At most 128 required receivers may be spanned at once. Retired receivers before
  F do not impose a lifetime rotation limit. This does not authorize their deletion.
  Task-owned native journal observations also require
  `JOURNAL_INFO.DELETE_RECEIVER_OPTION=NO`; automatic deletion does not track F/W.
  Manual receiver deletion remains an operator responsibility.
- Native journal requests and metadata names use explicit CCSID 37, independently
  of the worker JVM locale. Before each raw read, the actual API job must report
  CCSID 37; another or unknown CCSID rejects before that API call. This is the
  validated native metadata profile, not a claim that every IBM i name encoding
  is interchangeable. Row character columns still use their acquired CCSIDs.
  The metadata profile is included in the capture fingerprint.
- Distributed Kafka Connect compatible with the connector's Debezium version,
  one task, source transactions enabled on the
  worker, `exactly.once.support=requested`, `transaction.boundary=connector`.
  The task requires the public `SourceTaskContext.transactionContext()` capability
  and requests a Kafka transaction commit for each nonempty returned poll.
  Local validation covers **4.2.0 and 4.3.1**; see [validation scope](dcg-validation.md).
  Admission checks the transaction capability, not a version-string allowlist.
  This does not claim testing of every compatible worker release.
  Source exactly-once capability remains **UNSUPPORTED**: an interrupted committed
  source transaction can replay its complete output batch.
- `heartbeat.interval.ms=0` disables public heartbeats. Recovery progress uses
  `__debezium-truncate.<topic.prefix>` in that case, which must be precreated with
  one partition. Both `event.processing.failure.handling.mode` and
  `event.converting.failure.handling.mode` must resolve to `fail`, and
  unchanged updates may be skipped with the ordinary `skip.messages.without.change`
  setting. A skipped update still participates in transaction recovery and checkpoint
  advancement. Admission checks processing/conversion modes before
  opening the operator plan, enrolling, reading native data or publishing a
  snapshot. Dispatcher construction retains the same checks as a second boundary.
- Pre-created, distinct data, transaction, initialization and binding-history
  topics, plus heartbeat when enabled or the control topic when disabled.
  Binding history and control require one partition; initialization verifies
  emptiness across every partition of each output topic. Data topics may retain
  multiple partitions, with Kafka's usual absence of cross-partition ordering
  between keyless truncates and keyed rows. Use one data partition when that
  ordering is required. Preserve the internal topology for the
  connector's lifetime. The built-in `HostnamePrefixNamingScheme` (ordinary
  default) or `SchemaTopicNamingStrategy`, explicit Kafka
  Connect key/value Converter classes, only qualified `RegexRouter` SMT chains,
  no predicates or custom Debezium type
  converters, tolerated record errors or producer interceptors. Local converter
  contracts cover JSON with and without schemas and Avro with a mock registry;
  they do not qualify an external registry service. Converter serialization settings
  are pinned in the capture fingerprint; recognized authentication credentials
  are excluded as described below. Connect must fail converter errors; no record is
  acknowledged after a tolerated drop. Keep converter binaries and worker-level
  interceptors consistent across workers.
- Native output uses the ordinary IBM i converter for character cleanup and
  `trim.non.xml.charsequence.field.mode` (default `both`). It preserves the chosen
  `adaptive` (ordinary default), `adaptive_time_microseconds` or `connect` time precision.
  Standard decimal modes, column filters/masking/truncation, custom
  `message.key.columns`, schema/field name adjustment and tombstone selection use
  the same Debezium schema builder as ordinary output. Binary conversion preserves
  the ordinary connector's existing byte-output behavior.
  Effective defaults are pinned in history, just like explicit settings.
- Worker and connector Kafka clients must use the intended Kafka cluster. Explicit
  producer/admin bootstrap overrides must match the history bootstrap. Kafka
  replication, `min.insync.replicas`, ACLs, transport security and durable storage
  remain deployment responsibilities. The local single-broker evidence does not
  validate broker-loss availability or a particular TLS/SASL deployment.

Use the locally qualified worker timeout profile: `request.timeout.ms=5000`,
`default.api.timeout.ms=10000`, `worker.sync.timeout.ms=20000`, and
`rebalance.timeout.ms=30000`. The original shorter catch-up configuration still
reproduces a returning-worker failure and is not the supported profile. These
values were exercised with the complete worker settings retained in the reports,
including `exactly.once.source.support=enabled` and a policy allowing the declared
client overrides. The task can verify the public transactional context, but cannot
inspect every worker setting through `SourceTaskContext`; operators must verify
the worker configuration. See the [validation scope](dcg-contributor-testing.md)
and the [validation scope](dcg-validation.md). A new timeout,
broker topology, transport or worker version requires deployment qualification;
these local values are not a universal recovery SLA.

The tested worker also uses `producer.transaction.timeout.ms=180000`; the local
broker checks for expired transactions every 10 seconds. After abrupt worker
death, an unfinished Kafka offset transaction can hold replacement startup before
group membership or source-task startup until Kafka aborts it. Local fault
validation observed this wait beyond 100 seconds and automatic recovery within
the experiment's 240-second membership budget. A healthy REST endpoint alone
does not mean the worker has joined or the task has resumed. Account for the
configured transaction timeout in startup monitoring; do not reset offsets or
relax committed reads to bypass it. See the [validation scope](dcg-validation.md)
for the measured intervals and the distinction between the controlled local
reproduction and the retained earlier native timeout.

The [connector configuration template](examples/dcg-connector.json) uses placeholders;
its source TLS setting must match a qualified server/trust configuration. Supply
Kafka security separately for the intended cluster. The native experiments use
the isolated IBM i test transport settings recorded in their reports.

The concrete new fields are `dcg.mode` (`disabled`, `initialize`, `resume`),
`dcg.journal` (`LIBRARY/JOURNAL`), `dcg.tables`, `dcg.controlled.tables`,
`dcg.journaled.libraries`, `dcg.key.mode`, `dcg.initialization.window`, and
`dcg.retention=operator-managed`. Names are exact basic uppercase system names;
comma-separated lists have no whitespace or regexes. Capture, journal, schema/key
bindings, naming and output policy are fingerprinted. A changed fingerprint fails
recovery; passwords, initialization mode/path and resource budgets are not capture
semantics and can change without replacing the history.

## Converter credentials and checkpoint compatibility

For the standard Confluent Avro, JSON Schema and Protobuf Converter classes,
documented Schema Registry credentials do not enter the capture fingerprint:
`basic.auth.user.info`, `bearer.auth.token`, `bearer.auth.client.id`,
`bearer.auth.client.secret`, `sasl.jaas.config`, SSL key/keystore/truststore
passwords, inline keystore private keys/certificate chains and inline truststore
certificates. Their `schema.registry.` aliases receive the same treatment, under
either `key.converter.` or `value.converter.`. Changing, adding or removing these
values does not change recovery policy. This is configuration classification,
not new live qualification of every converter or authentication provider.

Registry URLs, converter classes, subject naming, schema options and authentication
provider/source selection remain pinned. Registry URLs with `user:password@host`
reject; use separate authentication properties. Recognized credential names on
other Converter classes, and unclassified password/secret/token properties,
reject instead of being silently hashed or ignored. Custom authentication
providers need explicit classification and regression coverage. Diagnostics name
the setting without printing its value. See the [Schema Registry authentication
properties](https://docs.confluent.io/platform/current/schema-registry/sr-client-configs.html).

Existing checkpoints created **without** converter credentials preserve their
exact fingerprint and can resume, including after credentials are added. A
pre-fix generation that **did hash credentials** cannot be automatically migrated,
even if the old credential is still supplied: its opaque digest cannot prove
that only authentication changed. Recovery rejects without changing history or
offsets. Preserve that generation and its evidence, then use the existing
operator-controlled initialization procedure with new empty data/history topics
and a new topic prefix. Never reset offsets or edit the fingerprint to bypass
rejection. New generations use credential-independent fingerprints and allow
subsequent credential rotation. Frozen pre-fix fixtures test both cases.

## Keys for native physical files

`dcg.key.mode=primary-key` is the default and uses declared SQL primary keys when
present, otherwise the same `QSYS.QADBKATR` physical-file key fallback as the
ordinary reader. Completely read, empty key catalogs establish null keys.
Keyless files' locked snapshot is ordered by physical RRN without adding that
number to the output key or columns. Standard `message.key.columns` remains an
explicit output override; it does not change physical row identity.

Use `dcg.key.mode=rrn` explicitly when the captured files need physical row keys.
This mode applies to every captured file in that connector. Each row key is a
single non-null `INT64` field named `__IBMI_RRN`, also appended to the row value.
The snapshot reads `CAST(RRN(T) AS BIGINT)` from the actual single-member file;
journal rows use the recorded relative record number. Original columns, native
file/member bindings, and physical decoder layouts are preserved. Duplicate row
values remain separate records. Deleted slots can leave gaps; neither the row's
business values nor its position in the snapshot determines its key. Truncate
still has a null key and `op=t`, followed by the reloaded rows in topic order.

The key mode is saved in immutable binding history and the capture fingerprint.
Changing modes requires a new generation and snapshot; ordinary restart cannot
reinterpret existing output. A physical column named `__IBMI_RRN` (ignoring
case), missing/nonpositive/out-of-range record numbers, and ambiguous snapshot
ordering reject. The existing identity, full-image, nullability, defaulted-field,
minimized-row, journaling-transition and savepoint checks remain required.

Relative record numbers are physical identities within the pinned member and
capture generation. IBM documents that [RGZPFM can move records to different
relative record numbers](https://www.ibm.com/support/pages/rgzpfm-frequently-asked-questions).
Uncoordinated reorganization remains unsupported: an unmatched `F/RG` stops
capture without checkpoint progress and fails again after restart. Do not skip
that entry or reset offsets to continue. Plan maintenance and a new generation
when the physical identity contract cannot be preserved. Native validation of
normal rows/clears is not a claim of RRN-safe RGZPFM support. For declared primary
keys, captured classic `F/RG` and subtype-E `F/RM` records are accepted after exact
identity checks; physical record movement does not change the logical key.
Concurrent reorganization, restore and ambiguous transition records remain rejected.

The ordinary native runtime accepts validated open/close (`F/OP`, `F/CL`), save
history (`F/MS`, `D/DH`), save-while-active (`F/SS`, `D/DW`), authority grant (`D/GT`),
and record-limit (`R/IL`) metadata without producing table events or changing W.
These entries never resolve a transaction or remove a pending before-image.
Both `OMTJRNE(*NONE)` and `OMTJRNE(*OPNCLO)` are supported: the clear's matched
start-journaling companions must still restore `IMAGES(*BOTH)`, without inheritance
or remote journal filtering. An unmatched start/end-journaling record still rejects.
CL-command subtype-1 `D/CG` records, including `CHGPF SIZE(*NOMAX)`, now use
identity-checked metadata acquisition. Captured column/key definitions must
remain unchanged; commands that require an unavailable intermediate row layout
still reject. The subtype alone is not proof that a command changed only file
size. Abnormal-end/IPL entries (`D/ID`, `F/IU`, `C/BA`) and restore remain
unsupported; preserve the checkpoint/history and investigate rather than skipping
them or resetting offsets. No system IPL or shared-job termination was tested.

Binding history now uses a versioned connector-owned encoding. A saved legacy
version-one history fixture is recovered without relying on the current Debezium
core serializer. Unknown versions, changed table contents and fingerprints still
reject; this is serialization compatibility, not permission to alter a capture.

## Initial start and ordinary restart

First start requires a fresh output/history generation and a version-2 operator
plan prepared with `tools/dcg-start-window`. Stop **every writer to the dedicated
journal**, including uncaptured/internal producers, and administrative maintenance
for this one initial window. The plan is a durable single-use operator assertion,
not a server-wide lock. Its directory must be persistent, shared consistently by
all possible first-start workers, and support the CLI's atomic file operations.
Native locks, exact resource inventory, current transaction census, schemas,
paired file/member identities and stable journal boundary checks validate what
can be checked from the connector. They do not exclude an administrator or every
possible native API on the operator's behalf.

The helper creates a version-2 chunked plan by default, including when the total
row budget exceeds 9,000. `--chunk-rows` and `--chunk-bytes` may be supplied
together to lower the per-chunk limits. `--legacy-list-plan` is only for the
historical list-based experiment; normal connector startup rejects it. Plan
validation still runs with Python optimization enabled. `status` reports local
plan/enrollment state; neither that file nor `revoke` proves that native work has
drained. Keep writers paused until completed native drain is independently known.

Use explicit total row/owned-byte and time budgets. The scanner streams at most
256 rows / 1,000,000 owned bytes per chunk, with a 65,536-byte row limit. It validates
and converts a complete chunk before publication, then waits for Kafka commit,
ordered callbacks and matching durable offset readback. Completed snapshot chunks
have **pending initialization offsets**, not streaming F/W. After every table
reports EOF and closes its cursor, the existing initializer chooses and rechecks
B, persists and reads back binding history, accepts the INITIAL F/W acknowledgement,
then releases the native locks and hands off to continuous reading.

Keep the new generation isolated from downstream consumers until initialization
completes. A failed start can leave committed snapshot rows; it cannot retract them
or resume that partial snapshot. Crash, expiry or revocation requires investigation
and a **new empty generation and new operator window**, not deleting offsets and
retrying over the existing topics. Partial/legacy/malformed offsets reject before
native reads. Missing offsets in resume mode reject. Even initialize mode with a
new operator plan rejects existing output/history; no silent reset is available.

After successful initialization, ordinary restart reads the existing F/W and
binding history and does **not** reopen the operator plan or stop source writers.
`dcg.mode=initialize` also takes this resume path if a complete checkpoint exists;
operators may switch it to `resume`. Adding/removing captured files or changing
output/schema policy requires a deliberately planned new generation.

## Continuous behavior, bounds and failure

R is the last scanned position in memory. F is the oldest journal position needed
to rebuild pending work. W is the completed-output boundary. Only F/W are durable
restart positions; R never becomes a restart shortcut. The reader validates F/W
anchors independently and reads a bounded prefix (10,000 entries / 16 MB / 128
pages), carrying exact R bytes to the next read. Fresh recovery can span multiple
prefixes before reaching W; no output or checkpoint advance is allowed during
that reconstruction. A positively observed concurrent receiver attachment change
discards the complete unverified cut. After three raced attempts the configured
source yields without output or position changes and retries on its next poll.
Static contradictory metadata, missing receivers and identity/link failures are
not converted into retries. The original strict test profile still stops after
its bounded retry exhaustion.

Committed clear/reload batches are ordered through the real Debezium dispatcher.
Rollback output is withheld. The observed native savepoint policy remains
conservative: unresolved or reused consumed targets reject; no undocumented
release semantics are guessed. A long open clear retains its transaction and F
while unrelated captured tables continue. Resource exhaustion or unsupported
source semantics stop the task without acknowledging the offending history.

The ordinary reader admits one documented metadata-only member event: `F/CB`
subtype `C` (statistics request). It validates complete entry bounds, the pinned
member identity, matching payload names, and the absence of transaction/pointer/
minimization metadata. The event emits no record and does not advance W or alter
an open transaction/lifecycle window. Other F/CB subtypes remain unsupported.
See IBM's [object-level journal entry layout](https://www.ibm.com/docs/en/ssw_ibm_i_74/rzaki/rzakivarlength.htm).

### Transaction working storage

`truncate.buffer.storage=disk` is the default. Selecting `memory` explicitly uses
the following finite budgets, including journal-wide transaction and savepoint
context even for excluded files:

| Property | Default | Allowed range |
| --- | ---: | ---: |
| `dcg.buffer.max.transactions` | 64 | 1–1,024 |
| `dcg.buffer.max.entries` | 10,000 | 1–100,000 |
| `dcg.buffer.max.savepoints` | 1,024 | 1–100,000 |
| `dcg.buffer.max.bytes` | 16,777,216 | 65,536–268,435,456 |

Entries include retained changes, before-images and table identities, not just
rows. Bytes count owned source payloads and exclude Java objects, schemas and
serialized output; choose JVM memory with that overhead in mind. Prepared output
is bounded by four times the entry budget plus two transaction markers, including
key-change delete/tombstone/create records and dispatcher heartbeats. Live
acknowledgement bookkeeping is retired after verified commits
in groups of at most 256 records even within a large batch. The batch records
continue to carry the previous W; only completion permits W to advance. A crash
can therefore repeat a clear and reload prefix. A consumer must process `op=t`
as a whole-table clear, use row keys for subsequent changes, and consume
`read_committed` Kafka records in partition order. An upsert-only consumer is
insufficient. The reference consumer in the probe is an oracle, not certification
of a particular sink plugin.

`truncate.buffer.storage=disk` stores transaction working data in private local
files using the standalone, pure-Java H2 MVStore library. It spills transaction
images and indexes, savepoints, autocommit before-images, lifecycle windows and
prepared Connect output. Schemas are stored once in a dictionary with a bounded
in-memory cache. The dispatcher drains one logical change at a time into this
store; the coordinator still prepares the whole committed batch before publishing
its first record. Preparation or storage failure cannot advance W.

| Property | Disk-mode default | Meaning |
| --- | ---: | --- |
| `truncate.buffer.directory` | JVM temporary directory | Existing absolute writable local directory; each task creates a private child directory |
| `truncate.buffer.cache.mb` | 8 | MVStore page cache in MiB; allowed 1–1,024 |
| `truncate.buffer.write.kb` | 1,024 | Dirty-page flush threshold in KiB; allowed 16–65,536 |
| `truncate.buffer.disk.max.bytes` | 10,737,418,240 | Stops when either encoded working data or physical spill-file size exceeds this budget |

In disk mode, omitted `dcg.buffer.max.transactions` and
`dcg.buffer.max.savepoints` use 2,147,483,645; omitted
`dcg.buffer.max.entries` uses 536,870,911, which keeps the output expansion
calculation within its integer representation. Omitted `dcg.buffer.max.bytes`
uses the disk budget. Explicit values still apply; existing low memory-profile
values are not automatically removed from a connector configuration. These
integer representation bounds are not tested operating capacities. Disk space,
throughput and retained journal history remain practical limits.

The page-cache setting is not a total JVM memory cap. Dirty pages, one decoded
record, a bounded schema cache, native read pages, table schemas and the existing
output/acknowledgement queues also consume memory. Disk mode does not resolve
the separate file inventory, binding history or receiver-chain size limits.

Local files are disposable, never an authoritative checkpoint or a shared owner
registry. A task restart creates a fresh store and rebuilds from acknowledged
F/W and the retained journal. Graceful shutdown and failed processing remove only
the current task's owned files. A killed process can leave an orphaned private
directory; operators may remove it after confirming that its task has exited.
Do not delete a live task's files. No cross-worker shared volume is required.
Disk usage is checked at bounded flushes, so a flush can exceed the budget before
the task stops. A full or unavailable filesystem also stops processing; it does
not permit dropping records, advancing the recovery checkpoint or falling back
to the legacy reader after migration.

Stopping closes RPC/JDBC admission, requests active-call cancellation, drains the
source and closes owned handles. A stop request is not proof of completed drain.
Late native calls cannot start further reads through their wrapped handles.
Task stop waits at most 30 seconds for its asynchronous native cleanup.
Cancellation/close failure or timeout does not become a successful drain; cleanup
may continue after that deadline with terminal admission closed. Kafka fencing permits a legitimate replacement to
progress independently, subject to the source's locks and retained history.

Use normal Connect task state/error monitoring and the offset API to monitor
F/W, replay distance and retention needs. Never release required receivers based
on R, a send callback, elapsed time, or a worker's disappearance. Backup restore,
point-in-time rollback of Kafka state and automatic retention release remain
outside this ordinary-CDC contract.

## Reproducible qualification

`tools/dcg-packaged-probe NEW_EVIDENCE_DIRECTORY DCNCdddd [smoke|large|partial]`
builds the distribution, installs only those jars in the worker plugin directory,
and controls a newly owned IBM i test fixture. Credentials go through stdin into a
container tmpfs file using the standard FileConfigProvider. Audit/transaction
helpers run in separate JVMs and are excluded from the worker plugin path.

The retained report must include source/distribution/plugin hashes, raw broker
records, exact connector/worker settings, native inventory and cleanup evidence.
See [validation coverage](dcg-validation.md) for recorded results and their limits,
and [contributor testing](dcg-contributor-testing.md) for prerequisites and extended
workload commands. Local ownership/recovery tests use controlled native I/O;
they do not replace qualification against IBM i.


## Framework and native API references

Kafka's [4.3.1 transactional source worker](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ExactlyOnceWorkerSourceTask.java)
creates the public transaction context for connector-defined boundaries and calls
source commit callbacks after the broker transaction succeeds. That precise
implementation and the local fault experiments support this profile; ordinary
source callbacks alone are not a durability proof.

IBM's [OBJECT_STATISTICS service](https://www.ibm.com/docs/en/i/7.5.0?topic=services-object-statistics-table-function)
can report the last journal after journaling stops. The scope validator therefore
requires current `JOURNALED='YES'` as well as exact journal names, file/member counts
and both-image recording. An absent/ambiguous row or warning is not positive proof.

## Monitoring

Standard snapshot JMX metrics report selected tables, scanned rows, emitted read
events and completion/abort. `ScanPosition` is volatile R;
`DurableReplayFloor` and `DurableCompletedPosition` are independently read-back
F/W. They include receiver library/name and the full unsigned sequence.
`JournalOffset` retains the legacy numeric R gauge (or -1 if outside signed-long
range). `JournalBehind=-1` means unavailable: this path does not infer a current
journal head from a bounded cut. `LastProcessedMs` records the latest processed
interval's wall-clock time. `BufferedTransactions`, `BufferedBytes` (owned row
payload plus staged raw lifecycle data) and `OldestTransactionAgeInMilliseconds`
make open transactions visible without conflating them with durable progress.
Age uses source timestamps and therefore depends on source/worker clock agreement.

See [validation scope](dcg-validation.md) and
[portable validation](dcg-contributor-testing.md). Historical qualification reports
remain evidence for their recorded revisions, not blanket qualification of later
changes. The [validation scope](dcg-validation.md) records the separately
captured layouts, strict rejection cases and snapshot/replay equality coverage.
