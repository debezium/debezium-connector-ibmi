# Local task lifecycle and checkpoint receipt prototype

The [cleanup review](dcg-code-review.md) moved this historical implementation and
its tests to `src/prototype-test/java`. Use `-Pdcg-prototype-tests` to include
them; `tools/dcg-task-probe` selects it automatically. The configured connector
uses standard Connect ownership/offsets and does not ship this receipt authority.

This is an unwired protocol and a test-scope integration fixture. It does not
change `As400ConnectorTask`, enable transactional journal capture, install source
protection or select a production registry. F/CR PR #114 stays separate.

## Contract

`As400CheckpointReceipts` associates a receipt with an enrollment, authorized owner
generation, manifest revision, source partition and exact versioned F/W checkpoint.
The receipt id hashes a versioned, sorted, length-delimited UTF-8 representation.
Exactness here means all checkpoint field names, string values and types; it does
not mean preserving incidental JSON object-key order in Kafka's serialization.
The immutable checkpoint contains no scan cursor R. A store implementation must
check the owner and predecessor receipt atomically and return only after the write
is durable. A stale/revoked owner, unavailable store, changed predecessor, changed
scope or regressed checkpoint must stop receipt processing.

The adapter follows these boundaries:

1. Register the original SourceRecords returned by a poll, retaining bounded,
   immutable offset snapshots. A foreign or mutated record cannot acknowledge
   that registration.
2. Treat `commitRecord(record, metadata)` as a candidate only. Require a real
   acknowledgement for the expected topic/partition; a dropped record with null
   metadata cannot satisfy required delivery. Nothing is persisted by this call.
3. When Debezium's `performCommit()` forwards `commitOffset(partition, offset)`,
   require a completely acknowledged poll batch and an exact candidate match.
   Independently read back the committed offset. A matching W with a different F,
   receiver identity, fingerprint or other field is insufficient.
4. Persist only with an atomic owner/predecessor check. Ownership can change while
   the durable read is in progress; the check before reading is insufficient.
5. On a fresh instance, reconcile the loaded checkpoint by another exact committed
   read-back before publication. An older receipt may reflect a crash between
   offset commit and receipt persistence. Reconciliation must not move behind an
   accepted receipt or silently reset missing/corrupt state. It does not invent a
   callback or infer durability from scan progress. If a receipt already exists,
   publication remains blocked until reconciliation succeeds. A missing Kafka
   offset in that state rejects startup; it cannot select the fixture baseline.
6. Close the adapter on failure or shutdown. A later callback cannot reopen it.
   Reconciliation is not an alternate acknowledgement path once publication starts.

The interfaces are a contract, not a production implementation. `Store` must be
an actual protected authority, and `DurableOffsets` must independently establish
committed bytes for the correct connector/partition. The adapter does not grant
ownership, prove producer fencing, validate native history, terminate source jobs,
release receivers or authorize maintenance. Merely implementing these interfaces
with convenient local state does not establish those protections.

## Real code exercised and fixture substitutions

The test connector's `RuntimeTask` subclasses the normal `As400ConnectorTask`.
It uses the inherited final `BaseSourceTask.start(Map)`, `poll()` and `stop()`;
invokes normal `preStart`; calls the unchanged `As400ConnectorTask.doPoll()`;
and forwards `commitRecord`/`commit` to the real base methods. The real
`ChangeEventSourceCoordinator.commitOffset()` invokes the fixture streaming
source's receipt callback, and its actual stop path closes dispatcher/services.

Only the protected source-start method is replaced. A synthetic schema and journal
trace replace JDBC/RPC, snapshot discovery and native startup validation. The
fixture injects the normal task's private queue/error-handler fields by reflection;
there is no new production configuration or injection seam. Metrics are inert test
doubles; native source execution is absent. Therefore this is **not** validation
of complete production startup, snapshotting or the live reader.

The [committed-output adapter](dcg-output-adapter.md) uses the actual
`As400ChangeRecordEmitter`, `EventDispatcher`, transaction monitor and heartbeat
implementation. It prepares keyed rows, keyless truncates, primary-key change
subrecords, tombstones and real BEGIN/END metadata. A bounded staging queue collects
complete output before the replay coordinator stamps the frozen checkpoint and
enqueues it to the normal task queue. The fixture preserves keys, envelopes and
headers through Kafka's JSON converter. Trace metadata goes in a separate test
header. Raw broker snapshots preserve the real values; comparisons use source or
transaction timestamps, separately from producer-assigned Kafka timestamps.

All fixture outputs intentionally share one disposable topic/partition so their
broker order is observable. A one-hour heartbeat interval prevents incidental
timer output during these short scenarios; the adapter forces a real checkpoint
heartbeat after each committed batch. Separate unit tests exercise timer expiry
inside batch dispatch. The eventual idle source loop/scheduler, SMTs, multiple
output topics/partitions and arbitrary worker modes remain unvalidated. Required
missing/foreign/dropped callbacks fail closed. Existing model tests retain their
separate transaction/rollback coverage.

## Test authority and framework limits

`As400ProbeReceiptStore` is a local filesystem fixture shared by the disposable
workers. The controller initializes a fresh registry explicitly. Tasks never
convert a missing registry into an empty one. A file lock serializes owner and
receipt checks; forced temporary-file writes, atomic replacement and directory
fsync exercise process-crash persistence. The test grants a generation after the
pinned Connect runtime initializes the task's transactional producer. It persists
receipts using `context.offsetStorageReader()` read-back, then the runner compares
them with independent `read_committed` snapshots of the actual offset topic.

This does **not** select a production filesystem registry or prove anti-rollback,
host power-loss, multi-host storage or administrative restore behavior. The fixture
is not an enrollment authority for IBM i. The controller also explicitly names
which disposable worker may claim the next fixture generation. A resumed old
worker can rejoin Connect and receive a new task assignment; that new task must
not accidentally become a newly authorized owner during the stale-callback test.
This test scheduling restriction is independent of the receipt adapter's rejection
of the original stale callback. It is not a production fencing/authorization API.
Connect's public task context does not
supply a production proof binding our external enrollment/generation to the broker
producer epoch or a source-reader barrier. That ownership adapter remains open.

The receipt experiment relies on the exact Kafka 4.3.1 distributed-worker
transactional mode already exercised by the [feasibility test](dcg-connect-feasibility.md).
It must not be generalized to arbitrary `commit()` callbacks or worker modes.
The connector still declares source exactly-once support **unsupported**; poll
transactions plus conservative F/previous-W offsets permit whole-batch duplicates.
No generic `BaseSourceTask.lastOffsets` value is promoted directly to a receipt.

## Run and failure cases

```sh
mise exec java@temurin-21 -- tools/dcg-task-probe /absolute/path/to/new-evidence-directory
```

Requirements and Docker isolation match the [first harness](dcg-connect-feasibility.md#run-and-evidence):
local Unix-socket Docker, a digest-pinned image, an internal network, no published
ports, no IBM i endpoint/credential input and cleanup of only owned resources.
The fixture uses an invalid hostname and never constructs a source connection.
The test classes and local store are excluded from the production connector jar.
Disposable output topics disable time retention so complete diagnostic traces
remain available, including historical timestamps from earlier fixture versions.
This setting is confined to these local topics.

The driver reuses delayed acknowledgement, completed-batch restart and interrupted
reload cases, then adds worker kills immediately before and after receipt
persistence. A suspended worker holds a successful Kafka callback while a new owner
reconciles and completes output; its resumed stale callback must be rejected
without changing the receipt or output. A three-record poll case exercises
`BaseSourceTask` callback coalescing and a normal Connect stop request exercises
shutdown. Unit tests separately cover missing/corrupt state, scope mismatches,
mutation, dropped/foreign callbacks, stale owners during read-back, predecessor
races, bounds, regressed offsets and fresh-store recovery.
An offset-loss restart case deletes only the disposable scenario's offset topic
after accepting W=9, then restarts with the receipt and output intact. Startup
must fail before preparation/polling, emit zero new records (including truncates)
and preserve the accepted receipt.

A mixed-output case interrupts a batch after the actual primary-key-change delete
and its tombstone. A fresh worker must repeat BEGIN, truncate, all surviving row
changes, both tombstones and END before publishing W=9 on the heartbeat. The
runner checks real transaction IDs/counts, original keys, null tombstone values,
conservative offsets and final state computed from the actual envelopes. This
projection is a local assertion, not a deployed sink test.

Each run retains worker configuration, code/plugin hashes, actual broker data and
offset snapshots, receipt registry files, callbacks, crash markers and cleanup
results in `report.json` and adjacent evidence files. Crashes here are worker
process failures, not source IPLs or physical-storage failures.

## Observed local verification

The final November 6, 2026 output-adapter run passed 53 Kafka/Connect assertions,
including real BEGIN/END counts, key-change/tombstone replay, actual checkpoint
heartbeats, both receipt crash boundaries, stale callback rejection, exact
checkpoint recovery, missing-offset startup rejection and normal shutdown. All
owned Docker containers/networks were removed. Runtime source/plugin hashes and
production class bytes match the final local build.

The full Maven verification passed 732 unit tests, including 31 output-adapter and
23 receipt cases. After the fixture authorization correction, those 54 focused
cases passed again. Plugin packaging and formatter/import validation also passed.
These counts cover the local D/CG branch, not live IBM i acceptance or source
exactly-once certification. Earlier evidence retains the initial 32-assertion
receipt-only run and the failed/interrupted diagnostic runs; it is not overwritten.

The fixture returns empty polls while gated, allowing the base task's lifecycle
lock to be released for shutdown. Independent actual-file review identified the
missing-offset restart gap; the adapter now requires reconciliation before resumed
publication, and the fixture rejects a missing offset when a receipt exists. The
final runtime run repeated every scenario after these corrections. The production
task implementation was unchanged.

## Remaining gates

All native/startup/protection release gates remain open. The
[local runtime profile revision](dcg-runtime-profile-revision.md) separates the
candidate first CDC profile from optional cleanup/maintenance and independent
restore authority. Prefer standard Connect offsets/fencing where a reviewed and
validated alternative satisfies the accepted contract; this fixture does not
establish that a separate production registry is mandatory. Its receipt checks,
missing-offset rejection and regression tests remain unchanged. Before production
wiring, settle the supported runtime, native history/identity/baseline evidence
and actual reader/dispatcher integration, with explicit acceptance-gate mapping.
The versioned-offset loader rejection and transactional clear guard remain active.
No receipt from this fixture permits retention release or streaming enablement.
