# Online transactional truncate capture

The local implementation adds `truncate.transactional.enabled=false`. Setting it
explicitly to `true` admits the native transactional truncate runtime from an
**existing, completed ordinary streaming offset**. It uses the normal
`As400RpcConnector` and task, the same topic prefix and source partition, and
standard `table.include.list` or `table.exclude.list`. There is no separate
per-table enable setting. No snapshot is taken by this transition. Preserve the
existing capture selection exactly: the old RPC filter resolves concrete SQL
table names to system file names; do not simultaneously replace that selection
with a new regex policy.

This is separate from `dcg.mode=initialize/resume`, the earlier operator-window
profile. Those settings cannot be combined. Existing versioned operator-window
history and earlier experimental histories cannot be reinterpreted as online
history. The ordinary default and F/CR behavior remain unchanged.

## What enabling it does

The connector discovers every physical file/member on the configured local
journal, acquires native schemas and verifies discovery history in the background.
While it does that work, the legacy row dispatcher continues publishing. Final
reconstruction of the exact acknowledged legacy prefix still runs on the source
thread before activation; its duration grows with the required history. Moving
that final pass into the background requires reconciling further legacy output,
not just starting an asynchronous read at an older offset. A server-owned
allocate/marker/release program briefly requests write-excluding locks on the
**whole discovered journal scope**, including files filtered out of output.
Metadata queries, Kafka writes and acknowledgements run outside that interval.
A busy allocation is released and deferred while ordinary CDC continues. An
unqualified audit, unsupported table layout or stale unpublished candidate is
deferred with logged exponential backoff while legacy CDC continues. It never
authorizes a boundary. After a handoff barrier is offered, uncertain acknowledgement
or invalid recovery history fails; that candidate can no longer be replaced.

New online binding proposals use one bounded record per table and a final descriptor.
Only a complete descriptor whose table records, ordering, identities and policy
fingerprint verify can be selected by a durable source offset. Partial proposals
left by an interrupted writer grant no authority; an identical retry may finish
one. Recovery scans a fixed history cut, selects the descriptor, then streams its
table records in a second pass. It does not load every proposal into memory.
Individual records remain limited to 4 MiB, but the new format has no 1,024-record
or 16 MiB lifetime ceiling. Kafka fetches are bounded and use an inactivity timeout;
file history appends a proposal under one lock with one final forced write.

Existing `v1:` offset fingerprints keep their original immutable-prefix selection
rules (1,024 records / 16 MiB) and remain readable alongside newer proposals. New
paged generations use `v2:` fingerprints; older connector builds cannot read them.
Keep the matching connector build, source offsets and history together when
planning a rollback. This changes no completed F/W position and grants no history
compaction, reset, deletion or retention release. The stricter operator-window
single-manifest format retains its existing bounds.

After the completed legacy output barrier is durable in the source offset store, the task
can publish the new recovery checkpoint. The immutable binding history alone
never grants that permission. When writers become idle before the native marker,
a separate bounded read must observe that exact object-free user marker, prove
there are no intervening CDC records, and revalidate native identities. Only then
may the normal output barrier acknowledge that actual position. If the prospective
interval is still empty, complete retained C/J history must also prove that no
transaction is open. The initial exclusive F=W then names that observed marker;
recovery verifies it independently, and it never becomes a table event. An empty
poll or receiver-tail number alone cannot advance the offset.

F is the oldest journal position still required for
replay; W is completed output. The scan position R remains separate and cannot
substitute for either. An open clear buffers its transaction's output while
unrelated committed changes continue. A fresh task rebuilds unfinished work from
F and does not establish another initial boundary.

The allocation uses `WAIT(1)`; that is not a universal one-second writer-pause
promise. Server scheduling, conflicting locks and release/drain failures matter.
Earlier isolated healthy pulses held allocations for approximately 1–2 ms, while
collecting metadata took much longer outside those allocations. Enabling the
feature on a busy journal may defer admission for an unbounded time.

If the legacy prefix encounters a transactional clear while admission is waiting,
it holds subsequent changes for that table and continues publishing other tables.
Every source offset and automatic delivery acknowledgement stays at the safe
pre-clear position. After the actual commit or rollback, retained native history
must establish the original transaction start, exact file/member identities,
unchanged schemas and the complete clear/savepoint/restore sequence. A committed
clear precedes its replacement rows; rolled-back work is discarded. The existing
legacy transaction metadata context closes only after all its held rows are
released. A following cycle-zero F/CR is validated against its exact single-member
binding and delivered in source order after that outcome; it is not discarded as
administrative metadata. A background metadata timeout outside the allocation interval defers
admission with backoff; it does not authorize publication or weaken lock-release
checks.

This transition still uses legacy offsets, before native F/W recovery takes over.
A replacement reconstructs the held work from the saved position and retained
original transaction history, including starts in earlier receivers. Ordinary
rows already emitted after that position can repeat on restart. Retain that
original history even when it precedes the saved legacy offset; do not delete
receivers based only on the latest emitted row. Missing history, changed schemas,
unresolved native sequences or a same-table transactional prefix already exposed
before the clear are rejected without acknowledging past the clear. Temporary
indexes and row images use disposable disk storage; they never replace the journal
as recovery authority. After all held work drains, normal online admission resumes
without resnapshotting. Clears skipped before the saved legacy position are not
repaired by enabling the feature.

A failed `DLCOBJ` must not be treated as a released lock. The server program makes
three bounded release attempts; after an explicit terminal release failure, the
client attempts one exact release on the same job. An uncertain network response
is never retried. Independent lock observation is required before accepting a
candidate. Job-scoped locks can survive closing a pooled SQL connection. If release
cannot be verified, the task reports the owning job and fails; an operator must
verify that job's locks and release only the connector-owned allocation. Repeatedly
restarting the connector does not prove those locks are gone. `CONFLICT(*RQSRLS)`
can close conflicting pseudo-closed cursors, causing applications to reopen them.

## Admission requirements

Use the common [runtime, output and retention contract](dcg-configured-native.md),
with these online-specific differences:

- An existing stream requires a complete legacy offset and completed snapshot,
  or matching online history and its durable F/W checkpoint.
  An unprocessed anchor is replayed before it can acknowledge a boundary.
  Foreign or incompatible offsets reject. Existing streams may
  retain `snapshot.mode=initial`, `no_data` or `when_needed`; none requests another
  snapshot at this transition. Existing snapshot SELECT overrides and snapshot
  query/locking settings may remain configured; they are not run by online
  handoff and do not change its streaming fingerprint. A new connector with
  demonstrably empty binding history uses the ordinary snapshotter first,
  including `no_data`. `initial_only` waits for durable snapshot completion and then
  stops without native handoff or a streaming reader. A recognizable interrupted ordinary snapshot can repeat
  through that snapshotter while history is still empty. Its completed offset
  must be durable before native handoff. Missing offsets with retained history
  always reject. With an existing native checkpoint, `always` and configured
  snapshotter policies can request a new ordinary snapshot. Old F/W remains
  authoritative during bounded snapshot delivery; an interrupted snapshot must
  finish under a compatible policy before streaming. Snapshot-only policies
  finish after durable delivery. The operator-window baseline still rejects snapshot overrides.
  Do not clear offsets or discard history to work around rejection.
- Set `truncate.transactional.enabled=true`, keep `dcg.mode=disabled`, and omit
  `dcg.tables`, `dcg.controlled.tables`, `dcg.journaled.libraries` and
  `dcg.initialization.window`. The connector discovers the full inventory instead.
- Set `dcg.journal=LIBRARY/JOURNAL` and `dcg.retention=operator-managed`. Retain the
  legacy starting receiver, the discovered transaction history and every receiver
  subsequently required by F/W. No automated retention release is granted.
  Task-owned native journal observations require
  `JOURNAL_INFO.DELETE_RECEIVER_OPTION=NO`; automatic deletion can remove a
  receiver still needed for replay. The setting cannot prevent manual history
  deletion. Older captured journal observations alone are not retention evidence.
- The current online scope is one active local journal in the system/basic ASP namespace (numeric pools 1–32; live qualification used pool 1) with
  physical files. Data and source files must pass the same identity and format
  checks. Ordinary rows from multiple members retain the existing
  shared table topic and key format; complete member identities are retained in
  binding history. A captured member-only clear still rejects because it cannot
  safely erase the combined table output. Logical files/indexes, data areas,
  queues and access paths may share the journal; they are not row-capture tables.
  There is no fixed connector file-count ceiling:
  independent native counts bound the temporary inventory and JDBC read, which
  must agree before and after discovery. Native allocation commands contain at
  most 50 files each. Larger scopes preload bounded command records into a
  job-local ledger before allocation, then run a compact server program with a
  one-second total acquisition budget and immediate attempts after the first batch. The same server call releases every
  attempted batch before returning. A job-local ledger supports release recovery;
  an uncertain transport response never authorizes another allocation or release.
  Metadata is collected outside the held interval. Busy/over-budget attempts leave
  legacy capture running and use the existing retry backoff. Removing the file
  ceiling does not guarantee that every journal can acquire a safe boundary in
  that time. Native audit limits and retained-history checks still apply.
  Table schema/identity metadata still uses memory proportional to the catalog,
  as it does in the ordinary connector; paged history bounds serialization and
  recovery buffers, while the optional disk buffer bounds transaction payloads.
  Receiver discovery uses IBM i's observed directory count and exact linked
  traversal rather than fixed 128/1,023-receiver ceilings. Full directory samples
  must match their independent before/after counts and attachment; each required
  receiver still needs reciprocal links, a readable recording profile and an
  exact native object identity. Receiver metadata uses memory proportional to
  the required directory or retained chain. This is not a constant-memory catalog.
  Captured files require supported physical layouts; a primary key is optional.
  SQL names can differ from physical system file names. Output uses the SQL name;
  journal identity and locking continue to use the independently verified physical
  file/member. SQL primary keys take precedence over the ordinary connector's
  DDS physical-file key fallback. Complete empty key catalogs prove a keyless table.
  Keyless tables retain null Kafka keys. `source.rrn` is row metadata, not an
  implicit key conversion. New online histories also retain complete schemas for supported excluded files,
  allowing standard selection changes at restart. Excluded files retain native
  file/member identities, format shape and transaction effects without decoding
  output values. Unsupported or inaccessible excluded layouts remain identity-only.
  They must still satisfy the complete-member-inventory, non-minimized-image and
  journal-identity checks. Expanding capture selection cannot promote identity-only history into
  a row schema. Excluded native LOB files retain a separate identity-only binding:
  bounded pointer-row length/flags, null maps and transaction effects are validated,
  but external values are never read or published. Every returned pointer handle
  is released in the retrieving native job, including entries deferred to a later
  page. Cleanup failure prevents progress and cancels that job. Captured LOB/XML
  values remain unsupported; an excluded binding cannot become a captured schema
  just by changing a filter. Minimized/defaulted or ambiguous row metadata still
  rejects. Catalog reads use bounded batches without caching.
  Excluded updates may use `IMAGES(*AFTER)` because no before-image is published
  or paired for them. Captured updates still require their exact before-image.
  Grant/revoke and trigger creation/removal records are identity-checked metadata.
  Triggered row changes are still processed as ordinary journaled rows. Constraint changes on
  excluded files do not produce output; captured constraint changes remain
  rejected until their effect on output keys can be established. A complete
  physical-file deletion retires its binding in durable schema history. A
  transactional DROP requires its verified commit and physical deletion records;
  rollback retains the original binding. The old decoder remains available for
  buffered rows until the physical deletion completes. DROP
  does not emit a truncate or delete existing topic records. A later creation
  under the same name requires a newly qualified file/member identity.
  Object-identity queries group at most 16 exact file/member paths; this bounds
  each SQL request, not the total file inventory. Missing, duplicate, foreign,
  mismatched or incomplete observations reject the whole acquisition. The full
  before/after metadata and raw journal checks still apply.
  Observed column changes and ordinary file creation have validated transitions:
  persist the new schema before affected output, and replay the boundary from
  history after restart. Multiple intervening definitions, incomplete creation,
  identity replacement and unqualified journaling transitions still reject;
  current catalog equality alone cannot prove an older row layout. This is
  limited current-definition refresh, consistent with the original connector,
  rather than arbitrary historical DDL reconstruction.
  Both `OMTJRNE(*NONE)` and `OMTJRNE(*OPNCLO)` are supported. Complete before/after
  images remain mandatory; the clear's matched journaling companions cannot enable
  remote filtering or inheritance. Ordinary open/close records are identity-checked
  metadata, not table changes or transaction completion.
- The connector creates one task, as in ordinary CDC; `tasks.max` is a ceiling,
  so an omitted or larger positive value does not create additional tasks.
  Use a runtime compatible with the connector's Debezium version.
  When the task has no source transaction context, online startup uses ordinary
  durable source offsets. Omit `exactly.once.support` and `transaction.boundary`
  for this contract; Kafka source transactions are not required. When distributed
  Connect supplies a source transaction context, retain
  `exactly.once.support=requested` and `transaction.boundary=connector`.
  The task selects and checks the contract using the actual runtime capability,
  not a user-supplied claim about the worker.
  Both paths support optional `provide.transaction.metadata=true` and require
  truncate to be absent from `skipped.operations` and `errors.tolerance=none`.
  `event.processing.failure.handling.mode` must be `fail` (the default), checked
  before native acquisition. A lossy legacy prefix cannot establish a safe
  migration boundary. Ordinary value conversion still supports
  `event.converting.failure.handling.mode=fail|warn|skip`;
  decode/history failures and failed clear dispatch remain fatal. `skipped.operations=none` emits every operation; ordinary `c/u/d`
  filters are supported. Recovery still processes all source records.
  With transaction metadata enabled, handoff waits for existing cycles to finish
  while legacy CDC continues, and native output preserves numeric commit-cycle IDs.
  Ordinary offset delivery also requires ordered processing; it rejects
  `record.processing.order=UNORDERED`. `heartbeat.interval.ms=0` retains
  the ordinary disabled-heartbeat behavior. Native recovery progress then uses the
  existing internal control topic; it does not create a public heartbeat topic.
  Exactly-once source delivery remains unsupported: recovery can replay an
  already published prefix of an interrupted transaction's output batch.
  Ordinary offsets do not provide broker fencing or atomic output/offset commits.
  An embedded application must acknowledge only successfully handled records and
  prevent overlapping connector instances. Binding history can use Kafka or
  the file store below. The tested Server JVM/HTTP profile has separate native
  evidence; other sinks need their own delivery and acknowledgement validation.
- Keep converters, naming, keys, tombstones and type handling consistent across
  workers. Standard worker key/value converter defaults may be inherited; the
  connector does not need duplicate converter settings. Producer tracing
  interceptors and partitioners remain worker-owned output choices. Recovery
  history must not be transformed by producer/consumer interceptors. The online fingerprint also pins the
  native database catalog used by the existing topic names.
  Standard `org.apache.kafka.connect.transforms.RegexRouter` chains are supported
  through the normal `transforms` settings. Connect/Engine applies each SMT;
  the connector predicts the final destination for topic preflight and checks
  acknowledgements against that destination. Data, heartbeat and control topics
  follow the chain; schema-history and offset-storage topics do not. Keep routed
  destinations distinct and preserve the required control/history partitioning.
  Data topics may be created through ordinary Connect/broker configuration;
  only control/history storage requires pre-existing ordered topics for online
  capture. Multiple data partitions retain ordinary CDC behavior but cannot
  provide cross-partition ordering between keyless truncate and keyed rows.
  New version-7 history permits output changes at task restart without changing
  the source checkpoint; older history retains its original strict policy.
  Standard payload transforms and predicates are also supported through the actual
  runtime, with exact source-partition and signed-offset acknowledgement. Their
  output topics cannot always be predicted. Keep history/control destinations
  distinct, and preserve recovery control/heartbeat records: filtering the record
  that carries a new checkpoint can prevent that offset from being written.
  `ExtractNewRecordState` in the tested Debezium version drops truncate events;
  use a truncate-aware transform when downstream consumers need `op=t`.
  Keep the existing topic naming strategy, character trim mode and time precision;
  enabling this option does not require changing those ordinary defaults.
  Native output now shares the ordinary character cleanup and trimming converter.
  Standard decimal modes, schema/table/column selection, column masking/truncation,
  `message.key.columns`, field/schema name adjustment, unchanged-update skipping
  and `tombstones.on.delete=false` are preserved. Built-in topic delimiters,
  heartbeat/transaction topic naming and source-type propagation are supported
  and fingerprinted. Explicit ordinary naming defaults retain the previous
  default fingerprint.
  New version-7 history separates these output choices from the source identities,
  physical codecs and retained history needed for recovery. Older history still
  requires its original output settings. Binary mode uses the same existing converter:
  this release does not silently fix the ordinary connector's byte output when
  another `binary.handling.mode` is configured.
  Native layout acquisition supports `CHAR/VARCHAR FOR BIT DATA`, Unicode graphic
  fields and explicit character CCSIDs using verified catalog byte lengths. A
  varying graphic prefix counts double-byte units; bounds are checked before
  decoding so it cannot read the next column. Local coverage is separate from
  fresh native validation; see [validation evidence](dcg-validation.md).
  Explicit `from.ccsid`/`to.ccsid` substitutions are supported for matching text
  fields. Physical storage lengths are still validated independently; binary,
  numeric and temporal fields are unchanged. The effective codec is saved in
  binding history and pinned in the capture fingerprint. Disabled or identical
  mappings preserve the prior fingerprint. Active mappings using implicit job
  CCSID 0 or a binary target are rejected before native connections open.
  Custom Debezium `converters` use the ordinary service registry and apply to
  native keys/values. Version-7 history permits changing converter classes and
  nonsecret options at restart; missing implementations or sensitive options reject before
  native admission.
  An explicitly configured `header.converter` and its serialization settings
  are supported and fingerprinted alongside the key/value converters. An absent
  header-converter configuration retains the previous fingerprint. Version-7
  recovery permits serialization changes at restart; older generations retain
  their original policy. Converter credentials never belong in the fingerprint.
  Output type names, precision and scale come from the same JDBC metadata API as
  legacy streaming, separately from physical storage widths. Both descriptions
  are checked against the same acquired table and saved in history. Existing
  saved generations retain their recorded descriptors on upgrade; recovery never
  substitutes today's catalog. Physical layouts support IBM i's documented
  8,000-field limit; the optional explicit RRN key adds one output field. Row,
  record and history byte budgets still apply.
  Online temporal output also preserves the actual legacy JT400 decoder's time-zone
  behavior, including its existing quirks. All workers must retain the same JVM
  time zone and time-zone rules; both are fingerprinted. This transition does not
  silently correct already-published DATE/TIME/TIMESTAMP values. Online migration
  rejects timestamp precision other than six because the old timestamp byte decoder
  has a fixed width. The operator-window profile retains its timezone-free decoder.
  Earlier experimental native output fingerprints (`native-configured-v1` and
  `native-online-candidate-v2`, and online histories without the
  `legacy-jt400-v1` temporal policy) are incompatible with this correction. Preserve
  their history and offsets; do not reset or edit them to bypass rejection.
  They require an explicit data migration or a new isolated generation, not a
  silent restart. Ordinary legacy offsets do not use those native fingerprints.
- For Kafka-backed history, precreate distinct data topics, the configured schema-history topic and
  `__debezium-truncate.<topic.prefix>`, plus a heartbeat topic when heartbeats are
  enabled. History and control topics require one partition. Existing data topics
  may keep multiple partitions, but Kafka cannot order a null-key truncate against
  row events in other partitions. Use one partition when that ordering is required.
  Internal clients
  cannot create them on observation. Retain all binding history without compaction
  or expiry. The control topic carries checkpoint barriers, not table data.

## File-backed history with ordinary source offsets

The online path recognizes the same storage settings as other Debezium
connectors:

```properties
schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
schema.history.internal.file.filename=/persistent/ibmi/history.jsonl
offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
offset.storage.file.filename=/persistent/ibmi/offsets.dat
record.processing.order=ORDERED
```

The `offset.storage` properties belong to Engine/standalone configuration, not
the distributed worker's connector configuration. Debezium Server prefixes
source properties with `debezium.source.`. Its tested JVM/HTTP profile is
[qualified separately](dcg-validation.md#server-jvmhttp-profile--2026-11-11).
The file-history profile uses ordinary offset
acknowledgement, so it does not accept a Kafka source transaction context.
The older operator-window initialization profile remains Kafka-only.

File history does not require Kafka bootstrap addresses, a history topic or the
Kafka topic preflight. Data/control names remain distinct, and ordered successful
delivery plus durable source offsets still control handoff. A consumer must
handle the internal control destination as well as table events; discarding
records and claiming successful acknowledgement can invalidate recovery.
Sink-specific ordering, keyless-truncate support and ownership remain the
application's responsibility, as with ordinary Engine delivery.

The history file contains immutable native bindings needed to decode replay.
Keep it with its matching source offsets on persistent storage. It is **not**
transaction spill storage and must not be deleted on shutdown. A replacement may
use another path for the same retained file contents, but switching an existing
Kafka generation to the file contract is not an automatic migration. Missing,
foreign or incompatible history rejects before recovered output. Do not recreate
history from today's catalog to bypass that failure.

The connector uses a strict adapter for Debezium's JSON-lines file format. It
rejects blank/malformed records, invalid UTF-8, incomplete final records and
history beyond the applicable recovery bounds. File operations take short,
nonblocking locks; they do not grant task ownership or fence a different Engine.
Each append and recovered cut is forced to disk before it can authorize a
checkpoint. Creating storage also forces directory entries. Use a writable local
filesystem that supports these locking and durability operations, with persistent
parent directories and a regular history file (not a symlink). A storage failure
stops processing; it does not acknowledge or silently repair the damaged file.
Local Linux filesystem behavior is validated separately from any network-mounted
or platform-specific storage. File history shares the existing bounded proposal
prefix; it does not remove the native file-scope/history-volume limits.

`truncate.buffer.storage=disk` is the default; the
[disk-buffer configuration](dcg-configured-native.md#transaction-working-storage)
uses local scratch files for rows, transaction bookkeeping, lifecycle validation
and prepared output. A replacement task reconstructs from journal history and
durable offsets and does not need access to the previous task's files. Exceeding
a configured resource budget stops capture with the durable checkpoint preserved;
it does not silently evict an open transaction. This storage option does not
remove runtime admission checks or the need to retain complete native history.

Excluded rows are identity/format checked but do not consume one retained output
entry per row. Transaction starts, savepoints, lifecycle state and table identities
still include excluded files: a cycle can later touch a captured table. Thus the
transaction/scope budgets and the oldest required journal position are journal-wide.
The optional `memory` profile retains its explicit 64-cycle/10,000-entry defaults;
exceeding them requires increasing the budget or switching to disk before restart.

Boundary raw-journal audits always read bounded native pages. Captures above
1 MiB spill to private task-owned scratch files, including when transaction
storage is `memory`. They use `truncate.buffer.directory` (the JVM temporary
directory by default) and the `truncate.buffer.disk.max.bytes` budget per capture.
The directory and budget are checked before native initialization. Normal close,
rejection and stop remove owned captures; a killed process can leave scratch files
for the container or operator to reclaim. Replacement tasks never trust or reuse
these files. Checksums, exact page ordering and native identity checks remain
mandatory. This removes the raw boundary's single-page/2,000-entry limit.

Online handoff also reads its retained control and row history in bounded pages,
using scratch captures and disk-backed indexes instead of a fixed 128-page,
20,000-entry or 64 MiB total-history ceiling. The indexes record cycle starts,
terminals, prior-schema membership and ordered projections without retaining a
second copy of every raw record in the heap. Each index or reconstruction store
uses a 2 MiB page cache and at most 256 KiB of dirty-page buffering. These are
storage settings, not a bound on the whole task's heap.

This startup scratch is used even with memory transaction buffering. The
configured `truncate.buffer.disk.max.bytes` applies separately to each raw
capture and each index/reconstruction store; it is not an aggregate task quota.
Concurrent open transactions, savepoints and lifecycle work still obey the
configured runtime limits. Choose disk transaction buffering for larger working
sets. A disk failure, incomplete history, unsupported old schema or ambiguous
transaction outcome cannot advance the handoff. Normal success, rejection and
stop remove the attempt's files; a replacement reconstructs from native history
and the saved discovery floor, never from previous scratch files.

The native runtime validates before/after images, full required job/thread fields,
receiver continuity, object/member identities and relevant journal history on
startup and recovery. Operator retention and fixed-source obligations still apply.

Once a handoff barrier or F/W is durable, disabling the flag cannot fall back to
legacy streaming. Restore the matching configuration and retained history to
resume; do not reset the source to a fabricated offset.

## Local and isolated native verification

The local `tools/dcg-normal-task-probe NEW_DIRECTORY compatibility` variant
exercises actual task/dispatcher publication, source offsets, worker replacement
and missing-offset rejection with three data partitions and disabled heartbeats,
tombstones and transaction metadata. It uses captured journal fixtures and
controlled native I/O; it is not an IBM i qualification run. The original strict
restart, pause and duplicate-record scenarios remain available unchanged.

`tools/dcg-normal-task-probe NEW_DIRECTORY standalone` runs actual standalone
Connect with file-backed offsets, controlled native I/O and Kafka binding
history. It verifies progress before offset flush, crash replay, independent
file-offset readback and rejection of missing offsets with retained history.
The actual Engine tests exercise the same acknowledgement contract with file
offsets. Neither test claims broker fencing or qualification of the packaged
Debezium Server application.

`python3 tools/dcg-public-online-probe.py NEW_DIRECTORY DCNCdddd source-offsets`
validates the ordinary packaged plugin against an explicitly authorized fresh
IBM i fixture with Kafka source transactions disabled. It starts from real
legacy snapshot/streaming offsets, uses public configuration without a test
provider, and exercises online handoff, both clear subtypes, restart and cleanup.
This run passed; see [the evidence and remaining deployment gaps](dcg-validation.md#ordinary-source-offset-handoff--2026-11-11).

`python3 tools/dcg-file-history-probe.py NEW_DIRECTORY DCNCdddd` runs the shipping
connector in the actual Engine with file offsets/history and no Kafka broker.
It passed both clear subtypes, recovery, ordering and missing-history rejection.
A separate packaged Server JVM/HTTP run also passed against an isolated fixture;
see [its configuration and evidence](dcg-validation.md#server-jvmhttp-profile--2026-11-11).

`python3 tools/dcg-legacy-compatibility-probe.py NEW_DIRECTORY DCNCdddd` prepares
the ordinary packaged connector with `no_data`, disabled heartbeats/tombstones,
three-partition data topics, a long SQL table name, Unicode/binary columns and
standard output mappings, plus a keyed DDS file without an SQL primary key. It
checks stored Unicode/binary bytes independently, compares actual legacy/native
output, exercises commit/rollback and restart, and verifies cleanup. Its
all-partition audit does not assert cross-partition ordering. The bounded native
run passed; see [the recorded evidence and limits](dcg-validation.md#legacy-configuration-compatibility--2026-11-11).

`python3 tools/dcg-online-scope-probe.py NEW_DIRECTORY DCNCdddd` exercises the
packaged connector against 56 journaled files, a keyless captured table and an
excluded GRAPHIC layout. It checks a conflict in the second lock batch, continued
legacy/unrelated-table CDC, null keys and `source.rrn`, commit/rollback, durable
restart, no resnapshot and cleanup. It uses the same explicit native credentials
and isolated-library contract as the runner below.

`tools/dcg-public-online-probe.py NEW_DIRECTORY DCNCdddd` uses the portable
owner-only `IBMI_TEST_CONFIG` contract described in
[contributor testing](dcg-contributor-testing.md). It creates and removes only a
fresh isolated fixture and labeled local Kafka resources. The worker loads the
assembled distribution from its plugin directory; test helpers run only on an
independent audit classpath. Credentials use the standard FileConfigProvider in
container tmpfs and are excluded from retained configs and logs.

The probe starts ordinary snapshot/streaming, obtains its actual durable offset,
then changes the public setting. It checks busy-boundary CDC progress, no second
snapshot, durable handoff, open clear and receiver rotation, task crash/reassignment,
rollback suppression, committed truncate-before-reload, optimized DELETE subtypes,
mixed values and completed drain. Raw journal and independent read-committed Kafka
evidence are retained. The stricter deterministic crash-boundary tests remain in
[validation scope](dcg-contributor-testing.md).

This is a bounded qualification profile, not validation of every IBM i release,
CCSID, schema, Kafka topology or operational failure. See the
[validation scope and recorded native results](dcg-validation.md) for what actually ran
and what remains untested. The ordinary-plugin native qualification passed in
the documented profile; the public option remains off by default.

Native journal metadata also requires an API job with CCSID 37. Its request and
response codecs are explicit, so a German, Danish or Japanese worker locale does
not reinterpret object names. This does not change column CCSIDs. Existing
experimental native fingerprints without this metadata policy reject; ordinary
legacy offsets can still be considered for a newly qualified handoff.

### Output configuration changes on restart

New online binding histories use manifest version 8. Their recovery fingerprint
keeps journal/file/member identities, physical codecs, captured definitions,
source partition, history/control destinations and temporal
interpretation fixed. Output-only settings may change at a normal task restart:
transaction metadata, row-operation filters, tombstones, standard transforms,
converters, column mappings, key mappings and supported output naming options.
The task still validates the new delivery configuration before publication.

Recovery reconstructs open transactions from the same durable F/W and applies
the new output settings to records emitted by the replacement task. As with the
ordinary connector, changing a key, topic or converter is an operator decision
that can change downstream records; a crash may replay an unflushed prefix under
the new settings. This does not migrate existing Kafka records or provide
exactly-once delivery. Truncate capture must remain enabled.

Versions 5 and 6 retain their original full-policy checks, including independently
captured compatibility fixtures. No old history is rewritten or silently
reinterpreted. An unselected older proposal cannot impose its output configuration
on a separately selected newer checkpoint. Physical decoder, source identity,
unknown policy and history changes continue to reject. Version 7 retains its pinned
capture selection; version 8 also permits table/schema include and exclude changes
using the complete schemas already stored for those files. It does not snapshot
a newly included table or invent a schema for identity-only history. An in-flight
transaction is rebuilt from F and selected when it completes after W.
