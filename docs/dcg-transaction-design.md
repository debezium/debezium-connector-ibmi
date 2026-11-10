# D/CG transaction and recovery design

Status: staged implementation of a proposed design, 2026-11-06. Transactional
capture is not enabled; no new live validation is implied by this document.

The [local runtime profile revision](dcg-runtime-profile-revision.md) keeps this
transaction/F/W algorithm and prefers standard Connect offsets and fencing for a
candidate first CDC profile. It separates optional retention-release and restore
guarantees. Current protected-startup checks and tests are unchanged; any mechanism
replacement requires an approved gate mapping and new evidence before enablement.

The parser, transaction buffer, inline source adapter, and checkpoint/replay
coordinator, and startup/receiver/fingerprint validation protocol are implemented
locally; see the
[current validation status](dcg-validation.md#checkpoint-and-replay-recovery).
They remain separate from streaming. A read-only SQL observation collector now
exists, but the [API assessment](dcg-evidence-apis.md) leaves incarnation and
protection proofs unavailable and startup blocked. The follow-up
[source identity and protection protocol](dcg-startup-protocol.md) selects a
native identity candidate and specifies controlled enrollment, durable retention,
maintenance exclusion, bounded history and writer-fenced initialization. Its
[acceptance gates](dcg-startup-acceptance.md) have partial local fixture/Kafka
evidence, with no complete native or streaming gate passed; no runtime behavior
or checkpoint schema was changed by that design. Completing authoritative
evidence/configuration adapters and protected startup-to-reader handoff,
release-specific rollback/savepoint traces, real reader integration, the remaining
Kafka acknowledgement/failure boundaries, and live validation remain required.
The current clear guard is unchanged.

The [completed local timeout matrix](dcg-timeout-profile-validation.md) adds twelve
passing partition/reassignment runs under a fixed candidate Connect profile,
using the real task/dispatcher and durable F/W without custom owner/receipt gates.
Earlier restart, interrupted output and missing-offset results remain separate
evidence. These tests use synthetic source startup/input and do not close native
identity, history, reader or baseline requirements. The
[C acceptance checklist](dcg-startup-acceptance.md#recorded-local-evidence-and-open-gates)
records the validated subsets and remaining obligations. The next concrete step
is the [test-only incremental reader lifecycle harness](dcg-runtime-profile-revision.md#next-implementation-step-test-only-reader-lifecycle-harness),
including close/reconnect and server-job cancellation isolation. Production wiring
and existing source-reader barriers remain unchanged.

This proposal completes the semantics needed for
[issue #3027](https://github.com/debezium/dbz/issues/3027) and
[draft PR #115](https://github.com/debezium/debezium-connector-ibmi/pull/115).
It belongs only to the D/CG follow-up. Keep
[F/CR PR #114](https://github.com/debezium/debezium-connector-ibmi/pull/114)
at its existing scope: single-member clears outside commitment control. Do not
make that PR depend on this work. Retain the D/CG nonzero-commit-cycle rejection
until the implementation and release gates below pass.

## Decision

Introduce an explicitly enabled transaction-buffering mode for the whole
connector stream. Buffer all captured row changes and clears belonging to a
commit cycle, beginning at its start; emit only after a supported durable commit,
discard after a successful full rollback, and undo buffered changes for savepoint
rollback. Emit committed transactions serially, in their commit-entry order,
preserving operation order within each transaction.

Use a bounded in-memory buffer reconstructed from retained journal receivers.
Persist two recovery coordinates: a replay floor and a completed-output boundary.
Checkpoint whole transactions, rather than positions within a transaction's
output. This deliberately permits replay of an entire committed transaction
after interrupted delivery. It avoids claiming that dispatching one operation
atomically publishes every resulting record, tombstone, and metadata event.

The mode must apply to row-only transactions too. Waiting until D/CG is seen is
too late: earlier inserts or updates could already have escaped, and later reload
rows could otherwise arrive before the clear. `skipped.operations` remains an
output filter, independent of transaction correctness. Proposed configuration
names, subject to maintainer agreement, are `transaction.buffering.mode=none|memory`
(default `none`) and finite limits for bytes, events, and active cycles. Capturing
truncates additionally requires a `skipped.operations` value that permits `t`.

## Evidence and current implementation gaps

[Recorded validation](dcg-validation.md) established that D/CG subtypes 3 and 8
occurred with nonzero commit cycles on the tested IBM i 7.5 system. Both rollback
cases restored all 1,200 rows. Four real RPC entries were decoded; the then-recorded
87 passing unit tests did not establish transaction-safe event emission.

At D/CG branch commit `838359c2811baf96529502cd6c6023573722df29`:

- `As400StreamingChangeEventSource` dispatches rows while reading the journal.
  Its `txMap` holds Debezium metadata contexts, not pending changes. The global
  before-image cache is keyed only by table. `R/DR` is dispatched as a delete.
- `RetrieveJournal` requests `C/SC` and `C/CM`, but omits rollback and savepoint
  controls. `RetrievalCriteria.withJrnCde` uses `*ALLSLT` for every code; transaction
  controls must be available independently of file selection.
- `EntryHeaderDecoder` reads, but does not expose, the indicator flag, record
  number, job/thread identity, object journal identifier, or transaction details
  needed for reliable classification and image pairing.
- `As400RpcConnection.getJournalEntries` advances `offsetContext.position` after
  the consumer returns and at the continuation boundary. That one position is
  also serialized by `As400OffsetContext.getOffset`. `LostJournalException` resets
  the position; an oversized journal entry has a skip path in `RetrieveJournal`.
  Both behaviors are incompatible with recovery that depends on retained entries.
- The per-entry catch can log and continue after a processing error. Row dispatch
  ignores the dispatcher's boolean result. Both must become fatal in buffered
  mode when a required captured change cannot be represented.
- `As400OffsetContext` does not store transaction metadata state. The current
  snapshot offset is just the current journal position; it does not establish a
  transaction-complete starting boundary.

These changes belong to PR #115 or its transaction prerequisite, not PR #114.

## Supported boundary and prerequisites

The first implementation covers one local journal, ordinary local commit cycles,
single-member files, and a fixed capture configuration and object/record-format
mapping throughout the replay window. It makes no atomicity promise across
journals or Kafka topics. Distributed/XA transactions, remote journal failover,
prepared or heuristic outcomes, partial rollback, non-durable commits, and
transactional DDL beyond the validated D/CG clear path remain unsupported and
must be detected before publication.

Validate those prerequisites when enabling the mode. Keep incremental snapshots
disabled initially: the existing immediate signal/watermark processing has not
been designed for delayed transaction output. Initial snapshots and upgrades
require the controlled starting procedure below. Supporting snapshot concurrency
later requires its own transaction-consistent handoff design.

Do not claim historical single-member or schema correctness from a present-day
catalog query alone. Member additions/removals, file recreation/renaming, and
record-format changes must be detected in the journal and invalidate affected
pending data. Retain the original object identity and format with buffered values;
stop when historical decoding cannot be established. Until format history is
implemented, no DDL or member-layout changes may occur during capture lag or a
required replay window. Recheck clear scope before publication without replacing
the original identity with a newly discovered object of the same name.

## Retrieval and identity

The [startup protocol](dcg-startup-protocol.md#identity-contract) refines the
provenance of the identities below: native local object tuples plus a controlled
source enrollment and verified receiver manifest, not an assumed IBM chain UUID.
These are proposed adapter/schema changes, not values to inject into the existing
opaque-token interfaces. The initial real-source profile rejects sequence reset
and restore/replacement until separate support is proven.

Define a `JournalCursor` containing journal identity/incarnation, receiver library
and name, receiver-chain identity/order, sequence number, and inclusive/exclusive
semantics. Resolve missing per-entry receiver names immediately from the retrieval
context. Receiver rollover is valid; sequence reset or receiver recreation must
be distinguished from normal rollover. Never compare receivers lexically, compare
sequence numbers from unrelated receivers, or calculate a successor with `+1`.
Use the retrieval continuation and verified receiver chain. A gap or ambiguous
incarnation is fatal.

Define a transaction key from the journal incarnation and the full `C/SC` cursor,
with its commit-cycle ID. The numeric cycle ID alone is insufficient across
sequence resets. Keep lightweight state for every observed active cycle, including
cycles with no captured changes yet: they might touch a captured table later.
Associate later records by cycle and validated journal epoch; ambiguity is fatal.

Buffered mode must retrieve all relevant `C` controls independently of the table
filter (`*IGNFILSLT` when using the file key). IBM documents an additional
`*OBJEXIST` journal authority requirement for that selection; validate permissions.
Initially retrieve all entry types for the selected journal codes and classify in
the client rather than retaining an allowlist that hides unsupported controls.
Row/object filtering must not hide D/CG, member/format lifecycle changes, or the
rollback companions needed to validate the selected tables. Add retrieval tests
and live evidence with a nonempty file filter. Explicitly classify observed
`F/CH`, `F/EJ`, `D/EF`, `D/JF`, and `F/JM` companions to SQL clears; do not mistake
these internal transitions for a second truncate or indiscriminately ignore
schema/identity changes. An unknown control affecting a captured transaction
stops capture.

Copy bytes/decoded values and the resolved identity when accepting an entry.
The reusable RPC output buffer and pointer handles are not durable storage.
Retain a schema/record-format identity with the normalized operation. Any
unsupported pointer-backed or incomplete value must stop capture, not become a
null value in the buffer.

## Transaction state machine

| Input | Required behavior |
| --- | --- |
| `C/SC` | Open a cycle; remember its start cursor. No BEGIN event is published yet. |
| Forward row operation or D/CG 3/8, nonzero cycle | Normalize and append to that cycle; publish nothing. Validate identity, payload, and member scope. |
| `C/SB` | Record a savepoint/nested-cycle marker and its operation index. Internal SQL savepoints count too. |
| `C/SQ` | Resolve its referenced savepoint; retain its changes in the parent cycle. This is not a top-level commit. |
| `C/SU` | Remove changes after the referenced savepoint, including clears and reload rows; invalidate affected pending before-images and nested markers. Preserve earlier changes. |
| Supported durable `C/CM` | Validate the complete cycle, freeze its surviving operations, and drain one output batch before processing another completion boundary. |
| Successful full `C/RB` | Discard the whole cycle and all before-images/savepoints. Emit no data or successful transaction metadata for it. |
| Rollback start/compensation | Track rollback progress; never publish compensation as forward row changes. It does not itself resolve the cycle. |
| Unsupported/ambiguous outcome | Stop with a recoverable checkpoint still preceding the unresolved output. |

Decode the outcome flag, not just the entry type. IBM's layouts identify durable
commit flags 0/2, non-durable commit flags 3/4, successful rollback flags 0/2, and
incomplete rollback flags 1/3. Reject the latter cases and unknown flags. Preserve
the indicator flag from RJNE0200 and prove its mapping to the documented JOFLAG
with raw-header fixtures before enabling support. Do not confuse it with the
separate bit flags. `C/EC` ends commitment control; it is not a substitute for a
commit or rollback outcome. Prepared/heuristic/canceled rollback markers require
explicit rejection, not implicit success.

Savepoint rollback is required for safe support, not an optional optimization.
IBM describes SQ/SU references to the sequence where a savepoint was established;
parse the release-appropriate payload with strict bounds and validate against
live captures. Do not assume that every rollback targets the most recent marker.
Resolve the target in the same transaction, remove canceled descendants, and
implement the source's actual target-retention/release behavior. Repeated rollback
to a retained SQL savepoint and internal SQL nesting are release gates. An
unresolvable target is fatal. Do not infer undocumented fields inside `C/SB`.

Rollback compensation may use more than `R/DR`, `R/BR`, and `R/UR`; restored
deletions and object-level undo also need classification. Capture complete
savepoint and full-rollback traces with rows before and after a clear. Reject an
ambiguous trace until its semantics are known. Discarding a suffix at SU and also
applying its compensation would undo it twice.

Pair update before/after images within the same transaction, physical object,
member, record identity, and validated journal pairing sequence. Extend the
header decoder to retain the necessary fields. Interleaved transactions must not
share one table-level before-image slot. Consume a paired image once; discard it
on rollback, savepoint cancellation, or an applicable clear. If the available
image mode cannot identify the pair safely, stop or use a separately documented
supported missing-before-image behavior; never borrow another transaction's row.

## Ordering and output

Within one committed transaction, preserve forward journal operation order after
savepoint cancellation. Do not coalesce clears or earlier row changes. For example,
`insert A; clear; insert B; commit` emits `c(A), t, c(B)`. Full rollback emits none.
`insert A; savepoint S; clear; insert B; rollback to S; commit` emits only `c(A)`.

Across transactions, use durable commit-cursor order, not transaction start order
or the original positions of individual data entries. A transaction that commits
while another remains open may be emitted first. A zero-cycle row operation or
clear is a singleton batch at its own cursor and participates in the same output
ordering. Do not sort back to raw data-entry order after buffering. Prove with
live concurrent writers that the supported source locking/clear behavior agrees
with the resulting state; reject a source path whose completion cannot be placed
in this order. Nonzero-cycle F/CR remains rejected in the first buffered release
unless separately validated; PR #114 remains unchanged in either case.

Only one batch is drained at a time. Preserve each event's original journal
sequence, receiver, and source timestamp. These values can move backward between
commit-ordered batches and are not recovery checkpoints. Use a stable transaction
ID from the transaction key. If transaction metadata is enabled, publish BEGIN,
surviving data, and END during committed-batch delivery. Let Debezium's transaction
monitor count emitted events once; remove the current extra manual counting.
Keep a non-null metadata context even outside a transaction. Empty and rolled-back
cycles produce no successful transaction metadata. Metadata does not make Kafka
delivery atomic and is not needed to make buffering correct.

For a table containing both keyless `t` and keyed rows, require one Kafka partition
and ordered delivery/application. Parallel consumers, routing transforms, or sinks
that reorder or discard `t` invalidate the truncate/reload ordering guarantee.
Across table topics, consumers needing transaction-atomic visibility must buffer
using transaction metadata; this connector proposal does not provide that atomicity.

## Recovery checkpoint: replay floor and completed boundary

Use a versioned offset containing:

| Field | Meaning |
| --- | --- |
| `offset.version=2` | Selects buffered recovery rules. |
| `journal_identity` | Stable journal/receiver-chain incarnation used to reject replacement or discontinuity. |
| `replay_from` (F) | Earliest cursor needed to rebuild every unresolved cycle and any batch not yet checkpointed. Explicit inclusive/exclusive flag. |
| `completed_through` (W) | Completion/singleton boundary through which all required output batches were successfully enqueued, or cycles successfully discarded. |
| F/W `receiver_identity` | Source receiver incarnation for each anchor; qualified names alone cannot distinguish recreation. Required in the current unpublished v2 draft. |
| `capture_fingerprint` | Versioned fingerprint of filters, operation selection, relevant decoder/schema policy, and transaction-metadata settings used for deterministic replay. |

The current RPC read cursor R is independent, in memory. Reading to R does not
authorize checkpointing R. Legacy receiver/sequence fields, if retained for
tooling, must describe F with the correct processed flag, never disguise R as a
safe recovery position. The loader must reject an unknown offset version; old
connector binaries cannot safely consume v2 offsets and rollback requires a
separate controlled migration rather than merely changing the plugin JAR.

At a batch's boundary, calculate F as the earliest start among all open cycles
and the batch being drained. Freeze `(F, previous W)` for **every SourceRecord
emitted by the batch**, including BEGIN/END, multiple records from a primary-key
update, tombstones, and any heartbeat triggered internally by the dispatcher.
Use immutable offset snapshots, not a shared context later mutated by read-ahead
or a scheduled heartbeat. No record inside the batch claims that this batch is
complete, even its last data event or END record.

Only after every required dispatch succeeds may W move to the batch's completion
cursor and the completed cycle leave the pending set. Recompute F from remaining
open cycles; with none, use the verified cursor immediately after the fully
handled boundary (or that boundary with `processed=true`). A zero-output rollback
or filtered transaction can also move W, with other open starts still pinning F.

Publish this new checkpoint on a subsequent record or an ordered heartbeat after
the batch. Buffered mode should require working heartbeat output so an idle or
rollback-only stream can persist progress; fail configuration validation if that
path is disabled. A heartbeat emitted while reading an open cycle carries the
safe checkpoint, not R. Inspect the scheduled heartbeat implementation for mutable
context capture. Framework offset persistence must wait for acknowledgement of
all preceding SourceRecords in this source partition; prove that behavior in the
Kafka Connect integration test. A task enqueue is not a durable acknowledgement.

If dispatch returns false or throws, stop before advancing W. Pre-apply intentional
filters so false from a required dispatch is a failure. Validate the whole batch
before emitting its first record, but allow for partial output on a later queue,
serialization, transform, or broker failure. Buffered mode requires fail-on-error
delivery configuration; `errors.tolerance=all`, dropping required records, and
unordered embedded-engine acknowledgement are outside the guarantee. Never retry
the remainder of a failed batch while allowing a newer batch to overtake it.

### Restart algorithm

1. Load the last **durably persisted** `(F, W)` together. Validate source identity,
   receiver availability/continuity, configuration fingerprint, and schema/scope
   prerequisites. Rebuild from F; do not skip to W or the latest journal position.
2. Re-read and rebuild transaction/savepoint state. Suppress a transaction's output
   only after its completion cursor is known to be at or before W. Never suppress
   an individual row merely because its original sequence is below W: it may
   belong to a transaction that commits later.
3. Recovery may see the suffix of a cycle that started before F and completed
   before W. Keep it as an orphan until its completion proves it is suppressible.
   Do not require a missing pre-F image or savepoint to normalize this orphan's
   discarded output; track its identity and terminal outcome without publishing it.
   At the recovery fence W, any unresolved cycle without its start is a checkpoint
   integrity failure. A new captured cycle after the fence also requires its start.
4. Reconstruct pending cycles spanning W and retain them. Resume output for complete
   batches after W, in the same commit/singleton order. Entire batches may repeat.
   No subsequent batch may overtake replay of an interrupted clear/reload batch.
5. On connection loss, either preserve the accepted scan state exactly or terminate
   and rebuild from a safe checkpoint. Do not reconnect using an advanced read
   cursor with an empty transaction buffer.

Example: A starts at 100 and clears at 110; B starts at 120 and commits at 130;
A commits at 150. After B is checkpointed, F is 100 and W is 130. Restart at 100,
rebuild both, suppress B at 130, then emit A at 150, including A's change at 110.
Saving only 130 would lose A's clear; suppressing all entries at/below 130 would
lose it too.

If a crash occurs after `t, c(B)` from a batch but before its checkpoint is durable,
recovery repeats the complete surviving batch. A duplicate truncate is not
harmless in isolation: convergence depends on replaying its subsequent reload
and every later uncheckpointed batch in order. Consumers can observe temporary
partial state and duplicate events. This is at-least-once delivery, not exactly
once or atomic downstream visibility. Event deduplication, if used, must use stable
event identities and preserve this ordering; suppressing only reload duplicates
while applying a repeated truncate is unsafe.

## Startup, limits, and failures

Use the [controlled initialization and re-enrollment protocol](dcg-startup-protocol.md#controlled-initialization-and-re-enrollment)
and its separate retention, maintenance and writer-fence obligations to implement
this section. The current read-only collector neither acquires those protections
nor proves their existence. Unsupported environments remain blocked.

Do not automatically reinterpret a legacy offset as a safe v2 transaction boundary.
It contains no oldest-open-cycle information, and earlier uncommitted row changes
may already have reached consumers. Enabling buffered mode initially requires
quiescing writes affecting the captured tables, resolving every existing cycle on
the journal (an apparently unrelated cycle may later touch a captured table),
verifying a complete journal boundary, and establishing a consistent downstream
baseline (normally a fresh controlled snapshot). Keep writes quiesced through
snapshot and stream handoff. Merely observing an idle journal is not proof that
there are no open cycles. Automating this verification is an implementation gate;
it is not a request to alter a live system now. Never silently reset offsets.

Bound total bytes, event count, active-cycle count, and per-cycle overhead. Alert
on age, oldest pinned receiver, and replay distance. Do not evict a transaction
on TTL, discard it at a memory limit, or wait indefinitely with the reader paused
before the commit it needs to free memory. For the initial memory-only mode,
fail visibly on a limit and retain the recoverable checkpoint; recovery requires
raising capacity or changing the workload. A later spill-to-disk design needs
its own integrity/lifecycle rules and is not required to trust local disk after
a task moves to another worker.

Receiver retention must cover the earliest durable F plus downtime and replay,
not just the most recently read entry. Missing receiver, truncated/oversized
entry, discontinuity, unsupported control, invalid scope, or decode failure stops
capture through the error handler. Remove the current lost-journal reset and
oversized-entry skip behavior from the buffered path. Metrics distinguish scanned
position, safe checkpoint, oldest open cycle, and output progress; the existing
offset activity monitor must not label a deliberately pinned F as a dead reader.

## Implementation sequence and acceptance gates

All steps remain on the D/CG branch or a dedicated prerequisite branch; no changes
are required in PR #114. Rebase #115 onto the merged F/CR foundation when available.

| Step | Implementation | Required evidence |
| --- | --- | --- |
| 1. Complete journal controls | Header/outcome fields; SC/CM/RB/SB/SQ/SU and unsupported-control classification; receiver-aware identity; file-filter bypass for controls | Sanitized RJNE0200 fixtures, bounds/flag tests, live retrieval with file filtering, stable start/cycle identity across rollover |
| 2. Buffer and normalize | Whole captured cycles, scoped before-images, savepoints, object identity/format, memory accounting | Interleaved rows/clears; nested and non-top savepoint rollback; compensation classification; deterministic bounded behavior |
| 3. Recovery and dispatch | Independent R/F/W; immutable v2 offsets; serial whole-batch output; strict errors; heartbeat checkpointing | Actual RPC loop and EventDispatcher tests, fresh-process recovery from serialized offsets, false-dispatch and partial multi-record failure regressions |
| 4. Controlled startup | Legacy migration, quiescent snapshot/handoff, config fingerprint, unsupported-mode checks | Old/new/unknown offset versions, configuration change rejection, open-cycle detection, no silent skip/reset |
| 5. Live and downstream validation | Both D/CG subtypes under commit, rollback, savepoints, concurrent writers, receiver rollover, restart, Kafka and a truncate-aware test sink | Source and sink converge after recovery; record order and checkpoints captured; no partial/rolled-back clear published |

Test the following failure boundaries, with transaction metadata both enabled and
disabled: before clear; after clear but before CM/RB; after CM but before BEGIN;
after BEGIN, truncate, first reload row, tombstone or key-change subrecord; after
END but before checkpoint; after checkpoint enqueue but before persistence; and
after persistence. For each, restart a new connector instance from the actual
stored offset. Test delayed acknowledgements, empty/filtered transactions,
heartbeat-only progress while another cycle remains open, and an A/B commit
interleaving with F less than W. Require the final truncate-aware projection to
match committed source state, not merely an expected count of `op=t` events.

Negative cases include unknown or non-durable CM flags, incomplete RB, missing SC,
orphan savepoints, unresolved prepare, captured DDL/member change, ambiguous
compensation, buffer exhaustion, missing receiver, reset/reused sequence, malformed
payload, and every dispatch failure mode. None may advance past lost required
output. Verify the complete state graph with model tests, then real connector
tests; an abstract replay model is not a substitute for IBM or Kafka evidence.

## Open evidence gates before implementation can claim support

The architecture above is the proposed decision. The following source details
still require raw fixtures and live confirmation, not guessed byte layouts:

- RJNE0200 indicator-to-JOFLAG mapping, all supported commit/rollback outcomes,
  and detection of prepared/XA/heuristic cycles before publication.
- Savepoint target lifecycle (including repeated rollback), internal nested SQL
  savepoints, and every forward-looking record emitted as rollback compensation.
- Object/record identity across D/CG's observed internal file transitions and
  receiver rollover, with server-side file filtering enabled.
- A verifiable quiescent startup/snapshot boundary and unchanged historical
  member/record-format scope throughout recovery.
- Remaining ordered-offset, failed-commit and idle-reader/heartbeat boundaries in
  the actual Debezium 3.8/Kafka Connect path. The selected synthetic-source
  task/dispatcher/restart and timeout cases now have local evidence; real reader
  integration and the complete failure matrix remain open.

Existing evidence supports decoding, successful full rollback at the source, and
the need for buffering. It does not satisfy these transaction-support gates.

## References

- [IBM journal entry types](https://www.ibm.com/docs/en/i/7.5.0?topic=information-all-journal-entries-by-code-type)
  and [commit-control sequence](https://www.ibm.com/docs/en/i/7.6.0?topic=objects-sequence-journal-entries-under-commitment-control):
  control meanings and internal savepoints. Release-specific 7.5 fixtures remain required.
- [IBM layouts](https://www.ibm.com/docs/en/ssw_ibm_i_74/rzaki/rzakivarlength.htm):
  CM/RB outcome flags and SQ/SU targets. Consult the deployed release before coding offsets.
- [IBM Retrieve Journal Entries API](https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm):
  selection exceptions, authority, continuation, and commit-cycle identity.
- [IBM commit operation](https://www.ibm.com/docs/en/i/7.5.0?topic=work-commit-operation):
  journal commit record, object-level completion, and lock release.
- [Debezium Oracle connector documentation](https://github.com/debezium/debezium/blob/main/documentation/modules/ROOT/pages/connectors/oracle.adoc):
  precedent for buffering interleaved changes until their transaction outcome.
- [Debezium EventDispatcher at the tested core revision](https://github.com/debezium/debezium/blob/5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8/debezium-connector-common/src/main/java/io/debezium/pipeline/EventDispatcher.java)
  and [TransactionMonitor](https://github.com/debezium/debezium/blob/5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8/debezium-connector-common/src/main/java/io/debezium/pipeline/txmetadata/TransactionMonitor.java):
  dispatch results, multi-record output, internal heartbeats, and metadata lifecycle.

References were checked on 2026-11-06 using current documentation lookup and IBM's
indexed documentation. Direct IBM page retrieval returned HTTP 403 in this session;
exact release-specific payload offsets remain explicitly unverified above.
