# D/CG follow-up validation

This follow-up tracks debezium/dbz#3027 and depends on the F/CR-only change for
debezium/dbz#3025.
It preserves the prototype decoder and dispatch logic, but does not support
commitment-controlled clears. It is not ready to claim support for the live
D/CG operations observed below.

The [maintainer review draft](dcg-maintainer-proposal.md) records earlier local
evidence and the pending ownership, storage and native-protection decisions.
The [current candidate-runtime evidence](#local-standard-connect-ownership-and-timeout-matrix)
and [acceptance checklist](dcg-startup-acceptance.md#recorded-local-evidence-and-open-gates)
record the subsequent ownership/restart experiments and completed timeout matrix.

## Live observations

Tested with a disposable DTEST.TESTT table and 1,200 synthetic rows per case on
IBM i 7.5, Db2 PTF group SF99950 level 11. Connections used either JDBC
TRANSACTION_NONE or TRANSACTION_READ_COMMITTED with explicit commit/rollback.

| Operation | Journal entry | Commit cycle | Rows after |
| --- | --- | --- | --- |
| none-delete | F/CR | zero | 0 |
| none-truncate | F/CR | zero | 0 |
| commit-delete | D/CG subtype 3 | nonzero | 0 |
| rollback-delete | D/CG subtype 3 | nonzero | 1200 |
| commit-truncate | D/CG subtype 8 | nonzero | 0 |
| rollback-truncate | D/CG subtype 8 | nonzero | 1200 |

No case produced individual R/DL delete entries for the clear itself. The
rollback cases restored all 1,200 rows. Therefore a D/CG subtype alone is not
sufficient to emit an irreversible table-wide event.

The decoder read all four D/CG entries through the real QjoRetrieveJournalEntries
RPC path: correct file/library, blank member, and subtype 3 or 8. The 16-byte API
prefix declared 4,500 bytes of entry-specific data. Sanitized fixtures preserve
the fields used by the decoder and the observed lengths; see
`journal-parsing/src/test/resources/dcg/README.md`.

## Remaining work

### Journal-control groundwork

The parser now exposes RJNE0200's indicator flag, nested commit level, separate
bit flags, and XA/LUW identifier presence. `TransactionControlEntryDecoder`
classifies commit/rollback outcomes and reads bounded SQ/SU savepoint targets.
Unknown flags and unsupported control types remain explicit; classification is
not authorization to emit a transaction. SB internal data is not interpreted.

`RetrieveConfigBuilder.withTransactionControls(true)` requests all entry types
for the selected journal codes and bypasses active file selection for code C
only. This prevents a fixed allowlist from hiding rollback, savepoint, or unknown
controls. Bypassing file selection requires journal `*OBJEXIST` authority. The
mode defaults to false and is **not enabled by the streaming connector yet**;
the transaction-buffering stage must consume the controls before enabling it.
Route C entries by journal code before any enum-based unknown-entry filtering.

Tests exercise encoded API selection criteria and the real entry iteration and
decoding loop, including malformed bounds and outcome flags. The control fixtures
are [synthetic documented wire layouts](../journal-parsing/src/test/resources/transaction-controls/README.md),
not live captures. The earlier live probe retained D/CG payloads only. A live
filtered retrieval and captured control headers/payloads are still required to
validate the deployed release. The existing nonzero-cycle clear rejection remains.

### In-memory transaction state machine

`As400TransactionBuffer` is implemented locally as a single-threaded component
for normalized, nonzero local commit cycles. It is **not connected to streaming**.
It retains owned column values and immutable source metadata, buffers from SC, pairs update images by cycle,
table/member/object/format, source producer, and relative record number, and keeps
inserts, updates, deletes, and clears in operation order. A durable CM exposes one
immutable batch. Further journal input is blocked until the caller completes
that batch. Full RB discards the cycle. Empty committed cycles also return a batch
so the replay coordinator can checkpoint them without transaction metadata.

Savepoint rollback discards the suffix after its target and all descendant
markers; release retains changes and removes the target and descendant markers.
The rollback API requires an explicit retain-target or release-target decision.
The live adapter must establish which applies to each IBM i control sequence;
an unresolved decision stops processing. Repeated rollback to a retained target
is supported. Pending update images cannot cross savepoint establishment/release
or commit. Rollback discards affected pending images, and a clear invalidates
only its own cycle/table's images. Explicitly classified compensation records
cannot become forward output; they block forward processing and commit until
SU or a full RB resolves them.

Limits bound active cycles, savepoint markers, retained entries, and owned row
payload bytes. Retained entries include changes, unpaired images, and per-cycle
table identities; the byte counter is not an estimate of total JVM heap. Table
identities remain until the cycle finishes, even after savepoint rollback.
Malformed input, ambiguous update pairs, object changes, unsupported outcomes,
unvalidated XA/LUW identifiers, and exhausted limits stop the component without
eviction. A stopped buffer must be reconstructed from a safe checkpoint. The
checkpoint/reconstruction component is implemented below; runtime wiring and
durability integration tests remain pending.

Unit tests feed the real transaction-control decoder into the buffer and cover
clear/reload ordering, interleaved commits, full and savepoint rollback, repeated
and non-top savepoint targets, image isolation, immutable payload ownership,
capacity reclamation, unsafe outcomes, and delivery backpressure. These are local
component tests, not live source, RPC-to-dispatch, Kafka, or restart tests.

The buffer accepts caller-verified receiver order and journal incarnation; it
does not validate receiver chains or detect journal resets. Cycle-zero operations
belong to the future output coordinator. Completing a batch releases buffer
memory, not a durable offset; the coordinator must also bound its output queue
and stop retaining completed batches.

### Source normalization adapter

`As400SourceNormalizer` now feeds the buffer through the real fixed-header,
transaction-control, D/CG, and complete-row decoders. It remains **disconnected
from streaming**. It accepts selected captured object records and all C controls;
unknown or unsupported records are errors, never silently skipped through the
legacy entry-type enum filter. Cycle-zero operations and F/CR require a future
coordinator and remain outside this adapter.

RJNE0200 decoding now retains the unsigned relative record number and thread ID,
qualified source job, system name, program/user metadata, and the exact ten-byte
object journal identifier (hex encoded). The adapter requires unambiguous job,
thread, object, and member identity for rows. `As400SourceMetadata` preserves the
event timestamp, current receiver/sequence, system sequence, cycle, entry type,
and decoded header metadata independently of future durable offsets. Buffered
updates retain metadata for both images; batches retain SC and CM metadata.

`CompleteRowEntryDecoder` uses a pinned physical layout, bounds each field within
the declared row, validates variable-field lengths and `*VARLEN` null indicators,
and rejects missing/defaulted columns. `As400RowData` copies mutable binary and
date/time values, preserves timestamp nanoseconds and decimal scale, and returns
defensive copies. A finite physical-row bound, materialized-payload bound, and
column-count bound apply before admission to the bounded transaction buffer.
Payload accounting is not total JVM heap accounting; output copies and transient
decoding allocations also require bounded queues and layouts.

The first supported subset requires complete inline row images. Pointer-backed
data, XML (whose legacy decoder currently returns an empty placeholder), unknown
column codecs, incomplete/minimized images, missing before-images, unvalidated
XA/LUW identifiers, and transactional schema changes are explicit errors. No
current-row SQL lookup substitutes for a historical journal image. BR/DR/UR are
classified as compensation and never emitted as forward changes. A forward-looking
record after R1 or a compensation record is rejected until rollback resolves;
unmarked/ambiguous undo sequences still need live validation. SU retention/release
remains unresolved by default and requires a validated release-specific policy.

Any normalization failure stops the adapter **and** latches the buffer as failed.
Ignoring that exception cannot make a subsequent CM publish incomplete work.
The future reader must likewise treat fixed-header decoding failures as fatal.

Bootstrap must supply a verified single-member `TableBinding` with matching JID,
physical names, immutable decoder, and a record-format identity valid throughout
the replay window. The adapter checks every row/clear against that binding;
it does not query a live catalog or prove that a caller-supplied binding is valid.
Automatic catalog/bootstrap verification, receiver-chain/incarnation checks,
source-specific compensation/savepoint traces, recovery, and dispatch remain
integration gates. Unsupported inline formats must remain disabled or fail until
complete materializers are implemented and validated.

Unit tests use synthetic RJNE0200 entries at nonzero buffer offsets, traverse the
actual decoders into committed batches, overwrite source buffers, interleave
cycles, and verify metadata/ownership, rollback, limits, unsupported operations,
and fatal normalization errors. Synthetic DTS timestamps exercise the real header
timestamp decoder. These fixtures are not new live IBM i evidence.

References: [IBM RJNE0200 API layout and field meanings](https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm),
[IBM row/control entry layouts](https://www.ibm.com/docs/en/ssw_ibm_i_74/rzaki/rzakivarlength.htm),
and [IBM journal entry types](https://www.ibm.com/docs/en/i/7.5.0?topic=information-all-journal-entries-by-code-type).

### Checkpoint and replay recovery

`As400Checkpoint` and `As400ReplayCoordinator` now implement the recovery state
machine locally, **without enabling transactional streaming**. The v2 offset
contains journal incarnation, a versioned capture fingerprint, inclusive/exclusive
replay floor F, and completed-output boundary W. Each anchor also carries its
receiver incarnation; a reused receiver name cannot satisfy an old checkpoint. The current scan position R exists
only in memory. Source event coordinates remain separate from all three.

Offsets are immutable flat string maps suitable for Kafka Connect's offset store.
The codec rejects legacy, mixed, malformed, and unknown-version offsets, changed
journal identity, and changed capture fingerprints. `capture_fingerprint` has the
form `v1:<64 lowercase SHA-256 hex digits>`; startup must compute it from the
deterministic capture/decoder/output policies. The existing legacy offset loader
also rejects an explicit `offset.version`, preventing accidental v2 interpretation
as a legacy scan position. No automatic migration or startup boundary is inferred.
The initial `afterVerifiedBoundary` factory is only for a startup boundary already
proven to have no open cycles and a consistent downstream baseline.

The coordinator starts at F, buffers complete known cycles, and suppresses output
only at terminal boundaries at/before W. Rows before W belonging to later commits
survive. A suffix whose SC is before F is tracked as a bounded orphan until its
durable CM or full RB proves it is suppressible. It does not require missing pre-F
update images or savepoint markers to normalize discarded output. Unresolved
orphans at W, a missing floor/fence, a missing start after W, unsupported controls,
non-durable outcomes, and order regressions stop recovery. Heartbeats during
catch-up retain the loaded checkpoint until W has actually been reached.

`As400JournalOrder` compares positions using an immutable receiver chain and
per-receiver sequence ranges, including rollover with sequence reset. It never
sorts receiver names or timestamps. Unknown/repeated receivers or positions beyond
the verified window fail. Startup validation now constructs this order from
incarnation-qualified source links (see below). A previously completed W receiver
may be needed only as ordering metadata when F follows W. Newly discovered
receivers or an extended high-water mark require verified refresh/restart before
this component consumes them.
The reader must call `verifyEndOfAvailableJournal` at a verified tail to detect
a missing F/W; an empty RPC page is not such a proof.

One committed batch is prepared and delivered at a time. A synchronous record
factory must materialize all required `SourceRecord`s before any enqueue; a finite
record-count bound applies. The coordinator stamps every record with the same
immutable `(F, previous W)`, including BEGIN/END, tombstones, primary-key-change
subrecords, and internally generated heartbeats. Wrong source partitions,
preparation errors, false enqueue returns, exceptions, and interruptions stop the
coordinator without completing that batch. A factory cannot swallow a rejected
record and make the remaining partial batch succeed. Empty cycles and intentional
output filtering can complete without emitting transaction metadata.

W advances only after the whole batch has been successfully enqueued, or after a
successful full rollback/zero-output commit. Other open cycles continue pinning F.
With none, F is W with exclusive replay. A subsequent ordered heartbeat or record
carries the new checkpoint. **Enqueue is not durability.** Restart must load the
last offset actually persisted by the offset store; it must not use the latest
in-memory checkpoint. Source-partition acknowledgement must cover all preceding
records before a newer checkpoint is durable. Scheduled heartbeats must use the
same serialized thread/queue, and fail-on-error delivery remains mandatory.

Tests restart fresh coordinators, buffers, normalizers, and decoders from
JSON-serialized offsets. They interrupt a clear/reload batch at every output
position using false returns, exceptions, and interrupts, then require the test
projection to converge after whole-batch replay. They also cover generated
interleaved checkpoints, orphan update/savepoint suffixes, cycle-ID reuse,
rollover, empty/filtered cycles, output bounds, missing boundaries, and crashes
before heartbeat enqueue, after enqueue, and after acknowledgement. Output uses
real `SourceRecord`s with a deterministic test factory and a queue that models
prefix acknowledgements. This is not a Kafka durability, fresh-JVM, or live IBM i
test. The actual `EventDispatcher` and RPC read loop are not wired to this layer.

### Verified startup, receiver chain and capture fingerprint

`As400RecoveryStartup` now validates startup evidence and constructs a recovery
coordinator from the resulting pinned bindings, checkpoint, source partition and
receiver order. It remains **unwired to the connector task**. Its read-only
`Source`/`Inspection` interface defines the required proof contract. The SQL
observation collector below now implements documented read-only queries, but
refuses to promote incomplete observations into that contract. There is no new
configuration switch.
Synthetic test evidence is not proof of IBM i authority, historical schema,
receiver provenance, writer quiescence or Kafka acknowledgement ordering.

Resume requires a complete v2 durable checkpoint. Null, legacy, unknown-version,
and incomplete offsets fail; none falls back to current position or initialization.
The unpublished v2 draft now requires `replay_from.receiver_identity` and
`completed_through.receiver_identity`. Earlier local v2 drafts without those
fields are rejected because their names alone cannot distinguish recreated
receivers. There is no deployed offset migration or backward-compatibility claim.
The coordinator preserves these identities whenever F/W advance.

Journal identity is a domain-separated hash of stable system identity, qualified
journal name, journal incarnation and chain epoch. These must be authoritative
incarnation tokens from the source adapter, not a hostname, attach timestamp,
sequence number or user-supplied label used as a substitute. `As400ReceiverChain`
follows explicit next-receiver links qualified by receiver incarnation. It rejects
missing links, cycles, forks, disconnected components, repeated names, mixed
journals, unexpected sequence reset and a misplaced/missing attached tail.
Receiver order is independent of catalog iteration order and lexical names.

The validator checks both checkpoint anchors against the receiver incarnations
and sequence ranges and requires every receiver from F through the inspection
tail to be fully retained. Partial/unavailable replay receivers are fatal. When
W is older than F, its receiver can be retained as identity/link/range metadata
without reading its old entries. The inclusive F must probe as an actual `C/SC`;
W within the replay window must probe as a top-level durable CM or full RB.
Exclusive F still requires an exact readable anchor. Probes use the existing
source-header validation and control decoder. Empty/uninitialized attached
receivers without a positive observed range are currently rejected; there is no
guessed successor or fallback to a later suffix.

`As400CaptureFingerprint.Policy` owns its collections and hashes a canonical,
length-delimited UTF-8 encoding using SHA-256. Map/set/table iteration order does
not affect the result. It includes effective filters, skipped operations,
versioned decoder/schema/output policies, transaction metadata, tombstones,
source partition, logical/physical table mapping, member, object JID and record
format identity. Raw credentials and resource limits are not inputs. The future
configuration adapter must supply effective values after defaults, including all
key/column mappings, converter, topic and transform policies affecting replay;
it must not omit settings merely because they were not explicitly configured.
Only the supported inline decoder and unresolved-savepoint rejection policy are
currently accepted by the startup factory.

Current catalog bindings must identify distinct physical and logical tables on
this journal, each with exactly one matching member and a valid object JID. A
current catalog query alone is insufficient: the inspection must also return a
complete lifecycle-history audit of the exact F-to-tail interval and captured
identity set. Missing coverage, wrong boundaries/inclusivity/revision, or any
invalidating member, object or format change fails. The adapter must construct
the pinned decoder from that verified format. The legacy receiver caches and
present-day `SYSPARTITIONSTAT` check cannot provide this proof by themselves.

Startup checks require control authority, all control types independently of
file filters, lifecycle visibility, complete inline images, working heartbeats,
fail-on-error processing and ordered source-partition acknowledgement. Remote or
distributed operation, incremental snapshots, concurrent snapshot handoff and
unvalidated output paths are rejected. These are explicit integration evidence
requirements; passing booleans in the synthetic tests does not establish them for
Kafka Connect or IBM i.

Initialization is a separate explicit method. It requires a matching durable
baseline, a held external writer fence and a complete census with no open cycles
across the **whole journal**. Idle observations or a census restricted to captured
tables are insufficient. This component does not acquire locks, quiesce writers,
run snapshots, migrate existing consumers or mutate IBM i. A future adapter must
provide that controlled handoff evidence under the still-held fence.

Source identity, receiver metadata, catalog and high-water mark are rechecked
after validation and again immediately before coordinator construction; changed
or expired evidence fails without publication. The standalone verified session
owns its inspection and coordinator together. Closing it aborts the coordinator
before releasing the inspection; it cannot release the fence while its coordinator
keeps running. Source failures/interrupts close the inspection, and coordinator
construction is single-use. Transferring retention/schema/fence protection to a
real reader, refreshing the verified window, and detecting later lifecycle
changes are still integration requirements. The adapter must make observations
and lifecycle audits under a consistent inspection with bounded I/O and retained
replay data; two equal unprotected catalog reads do not establish those guarantees.

Tests cover canonical/config-sensitive fingerprints, receiver replacement and
broken topology, unavailable intermediate receivers, sequence reset, exact-boundary
probes, history/scope mismatches, unsafe delivery settings, incomplete startup
censuses, lost fences and metadata races. A validated startup creates a coordinator,
advances it across rollover, serializes its checkpoint and starts another validated
instance. Existing interrupted-delivery recovery regressions continue to run.
No live metadata collector, fresh-JVM, Kafka or IBM i validation was run here.

### Read-only SQL evidence collection

`As400JdbcStartupEvidence` now collects bounded, parameterized observations from
journal/receiver views, current file/member catalogs, visible transaction
resources and explicit per-receiver `DISPLAY_JOURNAL` ranges. The
[API assessment](dcg-evidence-apis.md) records the documented facts, exact limits,
service/PTF caveats and source links.

The investigation did not establish authoritative incarnation tokens or protected
inspection/history/fence guarantees from these observations. The collector keeps
those gaps explicit in `As400StartupEvidence.Report`; its startup adapter refuses
both resume and initialization rather than manufacturing verified identities,
complete history or a quiescent handoff. The remaining proof model must be resolved
before live task integration. No live connection or mutation was performed.

Fixture tests exercise real query construction and decoding with mocked JDBC,
including failures, resource closure and rejection by the actual startup validator.
They do not validate SQL execution on IBM i or replace the existing integration
and live release gates.

### Native object identity candidate collection

`As400JdbcNativeIdentityEvidence` implements the fixture-tested portion of N1-N3
and identity-mismatch detection in N6 from the
[startup acceptance matrix](dcg-startup-acceptance.md). It remains separate from
`As400RecoveryStartup.Source`, the task and all checkpoint/streaming code.
There is no incarnation-token conversion or proof-producing bridge. The existing
SQL startup adapter still rejects resume and initialization.

The collector binds the exact QSYS.LIB path and native object type to a fixed
`IFS_OBJECT_STATISTICS` SELECT with recursion and error suppression disabled.
Its first profile accepts canonical uppercase basic system names for *JRN,
*JRNRCV, *FILE and *MBR, requires system ASP 1 and `STORED_LOCAL=YES`, and rejects
symbolic results. Extended/quoted/lowercase names, wildcards, relative paths,
traversal and IASP/remote paths are explicitly unsupported rather than normalized
or searched. Exactly one matching row is required; a second row is a failure.
There is no parent-directory enumeration, retry or weaker fallback query.

The path projection is bounded to 55 characters and retains its original length,
so truncation cannot turn a longer path into a matching prefix. Symbolic-link
presence is projected without fetching its potentially large target text. The
three identity components retain the exact documented SQL BIGINT domain, including
zero, with no floating-point conversion or wildcard/sentinel substitution. Their
values alone do not establish a replacement-resistant source incarnation.

Limits bound object count, statement time and optional raw-read bytes. There are
at most one query per object for collection, or two for an around-read observation;
each statement permits one row plus an ambiguity sentinel. Connection acquisition
and the supplied read callback must themselves use bounded timeouts. Read-only
mode is requested and checked; the collector executes only the fixed SELECT.
Warnings, missing/extra/malformed rows, service/authority errors, interrupts and
close failures prevent a successful result. Connections/statements/results are
owned and closed. Query timeouts and result caps do not prove the SQL service
performs no internal directory work.

`collectAroundRead` copies a bounded borrowed byte array before collecting the
same targets again on its owned JDBC connection. A changed tuple or failed
second observation discards the result; returned maps and bytes are owned.
Matching observations still do not prove source binding, stable history, a
change-and-change-back exclusion, or a protected handoff. The callback contract
forbids publication and offset advancement, and the helper is not wired to RPC.

The new tests use hand-authored projected SQL fixtures and mocked transport.
They exercise exact paths/parameters, full integer precision, each tuple
component changing from zero, malformed results and warnings (including warnings
cleared by moving past a row), aggregate limits, interruption, cleanup, buffer
reuse and failed re-observation. Existing actual-startup rejection regressions
continue to pass. No SQL was executed against IBM i. In particular, *FILE is a
directory-like object: the deployed service must return the exact starting-object
row for this query or collection fails. No fixture proves that behavior or
N4/N5 lifetime semantics. All protection and live validation gates remain open.

Reference: [IBM IFS_OBJECT_STATISTICS](https://www.ibm.com/docs/en/i/7.5.0?topic=services-ifs-object-statistics-table-function).
The additional query review used IBM's indexed documentation after Context7
returned no matching documentation and its CLI rejected `--research`.

### Remaining streaming integration

The [local runtime profile revision](dcg-runtime-profile-revision.md) now evaluates
a smaller CDC profile using standard Connect offsets/fencing, separating optional
retention-release and independent restore guarantees. Its
[C1-C12 acceptance cases](dcg-startup-acceptance.md#candidate-cdc-profile-gate-c)
now have scoped local publication/recovery evidence from the experiments below.
This documentation update records those results; it runs no new runtime tests, changes
no historical test count and relaxes no guard.
The stronger protocol below remains the current implementation's contract until
an approved per-gate replacement has been validated.

The [protection feasibility proposal](dcg-protection-feasibility.md) now compares
receiver retention, maintenance control and publication fencing options, with a
P1-P8 evidence map and operator/maintainer decisions. It recommends evaluating a
finite retain-all profile and the actual Kafka Connect ownership/offset boundary
before implementing automatic cleanup or enabling streaming. This is a local
design assessment; no source-side provider or live protection has been validated.

The test-scope [native identity probe and runbook](dcg-native-identity-runbook.md)
prepare the next source validation step. The launcher has separate offline `plan`
and explicit `collect` modes, invokes the real candidate collector, checks a named
release/PTF profile and writes pseudonymized observations. Local fixtures and an
offline smoke test do not close native/protection gates. No live probe run is
authorized or performed by this preparation.

The [source identity and protected recovery protocol](dcg-startup-protocol.md)
now defines a candidate native identity source (`IFS_OBJECT_STATISTICS`), durable
receiver pins, maintenance protection, a bounded history cut and controlled
baseline/handoff requirements. The [acceptance matrix](dcg-startup-acceptance.md)
contains fixture, enforcement and live gates. Native candidate collection now
has the local coverage described above; the full protocol remains unimplemented
and supplies no verified proof. The source adapter continues to reject startup. Native ID lifetime,
concrete protection-provider enforcement and the integration gates remain open.

Before supporting these transactional operations, connect normalized changes and
the replay coordinator to the real reader, dispatcher, heartbeat scheduler and
offset store. Prove Kafka Connect acknowledgement ordering and delayed-ack/crash
behavior using the actual output path; eliminate legacy skip/reset/reconnect
behavior in buffered mode. Implement and validate the proposed identity/protection
providers, complete the live evidence and effective-configuration adapters and protected
startup-to-reader handoff for the validation protocol above, plus
cycle-zero ordering, source-specific rollback/savepoint validation, and live
source/Kafka/sink convergence tests. Keep the nonzero-commit-cycle clear rejection
until those guarantees are established.

The [transaction and recovery design](dcg-transaction-design.md) proposes whole-cycle
buffering, commit-order delivery, savepoint handling, and separate replay/checkpoint
positions. It includes the implementation and evidence gates for completing this
follow-up while keeping the F/CR PR separate. The new transaction components are
not active in the current streaming path.

The observations do not prove that D/CG can never occur with commit cycle zero
on another release or SQL path. That currently permitted path has synthetic unit
coverage but no successful live event-emission test. The RPC decoder checks are
not an end-to-end connector or sink test.

Reference: [IBM: SQL fast delete](https://www.ibm.com/support/pages/system/files/inline-files/Get%20Up%20to%20Speed%20on%20SQL.pdf).

## Local Kafka Connect feasibility experiment

`tools/dcg-connect-probe` runs a pinned Kafka 4.3.1 broker and separate distributed
Connect worker processes against the real replay coordinator with synthetic
journal input. It checks actual persisted F/W, delayed heartbeat acknowledgement,
fresh-process recovery after completed and interrupted output, whole-batch replay
ordering, and stale producers after network disconnection or process suspension.
See the [experiment and its limits](dcg-connect-feasibility.md).

This is a manual, local Docker test, outside ordinary CI and live IBM i tests.
The test connector explicitly declares exactly-once source delivery unsupported;
worker transactions and fencing do not remove whole-batch replay duplicates.
It does not exercise `As400ConnectorTask`, the full Debezium dispatch path, a live sink,
source-reader fencing or the startup protection provider. Transactional streaming
remains disabled, and all native/protection release gates remain open.

## Local normal-task and receipt prototype

The [task/receipt prototype](dcg-task-receipt-prototype.md) adds an unwired receipt
protocol plus a test-scope subclass of `As400ConnectorTask`. It exercises the real
base task lifecycle, normal queue polling, emitter, dispatcher and coordinator
commit callback with synthetic journal input. It requires exact durable offset
read-back and owner/predecessor comparison before writing a candidate receipt to
a local test registry. It tests process crashes before/after receipt persistence,
stale successful callbacks after ownership transfer and coalesced poll batches.

The fixture replaces source startup/schema discovery and inert metrics; it does
not validate JDBC/RPC, native startup proof or snapshots. The filesystem registry
is test infrastructure, not a selected production authority. External ownership
to producer-generation binding and restore protection remain unresolved.

## Real committed-output adapter

The [committed-output adapter](dcg-output-adapter.md) now prepares the full batch
through the real Debezium emitter/dispatcher, transaction monitor and heartbeat.
It retains metadata, key-change records and tombstones under one conservative
checkpoint, rejects output suppression/error-tolerant configuration, and fails
before publication on staging overflow or conversion failure. It remains unwired
to the production source. The full local verification passes 732 unit tests,
including 31 new output-adapter cases, and plugin packaging/style checks pass.
The normal-task harness now uses this adapter and includes a mixed-output crash
and restart case; its reports retain exact source/plugin provenance.

The remaining dependency is an accepted runtime/source profile and validated
adapters. The local evidence below advances the standard-framework candidate;
the local filesystem registry proves neither a mandatory production dependency nor
an alternative native guarantee.
Cycle-zero buffered integration, the actual idle reader/scheduler, complete
startup/snapshot handoff and source-specific live acceptance remain open. Existing
production guards are unchanged; no successful local test enables streaming.

## Local standard-Connect ownership and timeout matrix

The [ownership experiment](dcg-connect-ownership-experiment.md) exercises the actual
task/dispatcher and F/W recovery without custom owner or receipt gates in the
candidate publication path. Historical successful scenarios cover suspended-worker
broker fencing, delayed acknowledgement/completed restart, interrupted and mixed
output replay, and a separate missing/malformed/incompatible-offset rejection run.
The historical full run remained failed at network-partition reassignment.

The [catch-up investigation](dcg-config-catchup-investigation.md) identified a
returning-worker administration-client/config-sync timing interaction. The
[fixed-profile matrix](dcg-timeout-profile-validation.md) then passed twelve new
partition cases, seventeen checks each, over six timing combinations repeated
twice. It retained the original failing control and all safety assertions. Each
candidate loaded exact W=6, continued through W=9 and stopped normally, using one
existing bounded retry for the explicit retryable configuration-writer stop
response. No source publication failure was retried. Post-failure independent
broker checks for the control confirmed unchanged output/exact F=W=6 and complete
configuration history. Source/artifact provenance, cleanup and Advisor review
passed. These are saved local Docker results, not newly run CI or IBM i tests.

The fixed profile and measurement definitions are in the matrix report. It uses
synthetic source startup/input, one record per poll and separate single-partition
output topics. Local-close rejection in the twelve partition cases is distinct
from the earlier suspended-producer broker-fencing observation. Missing-offset
and other historical scenarios were not rerun by this matrix. The existing
732-test Java suite was not rerun for the timing-only addition or this doc update.

The [design's next implementation step](dcg-runtime-profile-revision.md#next-implementation-step-test-only-reader-lifecycle-harness)
is a test-only incremental reader lifecycle harness for the local C4 boundary.
The [acceptance checklist](dcg-startup-acceptance.md#next-local-implementation-checklist-c4)
keeps native job effects, source continuity/identity, startup/snapshot handoff,
transaction semantics and real source/sink convergence open. No full C or N/P/H/B
gate is promoted, no custom production dependency is removed, and transactional
streaming and F/CR PR #114 remain unchanged.
