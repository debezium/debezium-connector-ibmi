# Committed D/CG output adapter

`As400BatchDispatcher` connects the local transaction/replay model to Debezium's
real emitter, transaction monitor, dispatcher and heartbeat implementation. It is
package-private and remains disconnected from the production streaming source.
It introduces no runtime setting and does not relax the transactional clear or
versioned-offset guards. F/CR PR #114 is unchanged.

## Output boundary

The replay coordinator calls the adapter only after a durable source commit has
produced a batch. The adapter stages every required record before handing any
record back to the coordinator. The coordinator then replaces all event-position
offsets with the same immutable `(F, previous W)` checkpoint before publishing.
Only successful publication of the complete batch lets the coordinator advance W.

Staging uses the real `EventDispatcher` and `As400ChangeRecordEmitter`, including:

- Real BEGIN/END metadata when enabled, with a stable transaction ID derived from
  the existing journal identity, cycle and qualified start position. Each batch
  gets a fresh, non-null `TransactionContext`; whole-batch recovery resets counters.
- The real transaction monitor's event counts. A primary-key change counts its
  emitted delete and create once each; its tombstone does not count as another
  change. No additional manual event counting occurs in this adapter.
- Keyless truncate envelopes, keyed row envelopes, primary-key change headers and
  delete tombstones. One normalized change can produce multiple SourceRecords.
- Original per-change sequence, receiver/library and source timestamp, even when
  a transaction crosses a receiver boundary. Envelope processing time remains
  separate from source time and the recovery checkpoint.
- Heartbeats generated inside dispatch, which pass through the same staging and
  frozen-offset path as the data and transaction records.

The private staging provider has a record limit below the enclosing queue's limit.
It throws at that bound instead of waiting for a poll on the same producer thread.
Missing schema, conversion failure, rejected dispatch, overflow, cancellation or
reentrant/foreign-thread use stops the adapter. Preparation failure leaves the
external sink empty; a failure during the coordinator's later enqueue loop can
still leave partial output and requires whole-batch replay.

The constructor requires fail-on-error conversion and processing, enabled
heartbeats, no skipped operations and no unchanged-message suppression. Intentional
capture filtering must happen before batch preparation. This is an integration
contract; it does not claim the production source already supplies those filters
or validates worker errors/SMTs. Cycle-zero batches are explicitly rejected here
until their source/replay integration is implemented under the accepted profile.

## Heartbeat scheduling

`publishCheckpoint(replay, force)` creates a real Debezium heartbeat through the
same staging queue, then lets the coordinator stamp the current safe F/W. It can
publish progress after a completed batch, an empty commit or a rollback. While a
cycle remains open, its start pins F and the scan cursor cannot become W.

The adapter checks the real heartbeat timer when `force` is false. Both timer
checks and publication must run on the same thread as batch preparation. It does
not start an executor or register a callback capturing a mutable offset context.
The eventual source loop must call this method while idle and after source
progress; a production scheduler/reader is still an integration gate. Calling
from another thread or reentering while a batch is prepared fails closed.

## Local verification

The full local Maven verification passes 732 unit tests. The 31 adapter cases use
real Debezium dispatch with fixture schemas and journal bytes. They cover actual
transaction counts, delete tombstones and primary-key headers, every interruption
boundary of a ten-record batch, fresh adapter/coordinator restart, receiver
rollover, disabled metadata/tombstones, ordinary updates, preparation failure,
output bounds, timer-generated heartbeats, cancellation, empty commits, rollback
and unsafe configurations. These are local tests; they establish no native source
identity or source protection.

The [normal-task experiment](dcg-task-receipt-prototype.md) now uses this adapter
instead of hand-made BEGIN/END/heartbeat records. It routes data, transaction
metadata and heartbeats to one disposable single-partition topic so it can inspect
their total broker order. Its additional mixed-output case kills the first worker
after a real key-change delete/tombstone, restarts a fresh task from the committed
old checkpoint and checks complete replay and final rows from actual envelopes.
A simple in-process projection checks convergence; this is not a deployed sink.

The final local run passes 53 assertions with matching source/plugin hashes and
production bytecode; cleanup removes every owned container/network.

The experiment still replaces source startup, schema discovery and ownership with
fixtures. It does not establish production behavior for multiple output topics,
SMTs, arbitrary worker modes, source-reader fencing, snapshot handoff or native
journal retention. Review the run's recorded source/plugin hashes when comparing
results with later local edits.

## Boundary before production wiring

The next runtime stage depends on the decisions in the
[maintainer proposal](dcg-maintainer-proposal.md#decisions-required-before-production-adapters)
and the [local runtime profile revision](dcg-runtime-profile-revision.md). Evaluate
standard Connect offsets/fencing for the first CDC profile; automated retention
release and independent restore authority are separate capabilities. This proposal
does not relax the current startup/protection checks or promote synthetic proof
flags to a provider. Replacing a mechanism requires an approved gate mapping and
evidence for its alternative. Native identity lifetime, history completeness,
baseline, rollback/savepoint behavior and source to sink convergence remain gates.
A backend or supported runtime cannot be selected from the local fixture results.
Do not enable transactional streaming or implement automatic retention release
from the successful local output tests.
