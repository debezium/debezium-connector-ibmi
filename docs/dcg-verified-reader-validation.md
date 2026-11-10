# Verified startup-to-lifecycle reader handoff

Status: staged local integration, 2026-11-07. Transactional streaming remains
disabled. No IBM i connection, source mutation, Kafka broker run or push was
performed in this step.

## Implemented handoff

`As400RecoveryStartup.resumeLifecycle` and `initializeLifecycle` require a
separately fingerprinted policy: the pinned complete-inline decoder, conservative
savepoint consumption, the bounded single-receiver lifecycle algorithm and no
skipped output operations. The existing startup entry points retain their strict
savepoint policy. The new fingerprint cannot silently reuse the strict profile's
offsets; missing offsets still require explicit controlled initialization.

All existing identity, receiver-chain, table-binding, delivery, exact-boundary,
history and initialization checks remain. The lifecycle profile also decodes the
raw boundary bytes rather than trusting a separately supplied header. It refuses
a recovery interval crossing receivers. Its profile name identifies the selected
algorithm; it is not evidence that a live source release/PTF has been qualified.

A verified lifecycle startup can create one `As400VerifiedLifecycleReader`. It
cannot expose the underlying coordinator through the old standalone handoff.
The reader owns the verified inspection and incremental coordinator together and
accepts bounded intervals only within the already verified history cut H.

For each interval it:

1. Rechecks the held inspection, identities, policy and initialization fence when
   applicable, then requires complete matching history for the interval.
2. Requires an ordered replay plan from that protected history, with every
   relevant selected entry and the exact requested bounds. It validates the
   inclusive floor, range, order, duplicates, terminal entry and entry-count limit.
3. Reads each planned position exactly, verifies its receiver incarnation,
   bounds and raw sequence, and owns its bytes under a cumulative size limit.
4. Rechecks the inspection after all reads, before accepting any resulting
   records into the incremental classifier and publishing completed transactions.
5. Publishes only the coordinator's F/W through ordered records/heartbeats. The
   read cursor is not a durable offset. A later cut requires fresh verification;
   an open transaction is reconstructed from durable F/W in the next session.

The source enumeration contract is substantive. Ordered sequence numbers alone
cannot establish completeness, and valid selected records can have numeric gaps.
`Inspection.replayPlan` must enumerate every required entry from complete,
protected history, independently of row filters that could hide controls or
lifecycle records. Its default implementation rejects the request. Source-side
enumeration and exact reads must also enforce finite allocation/query limits;
the consumer's bounds are additional checks, not a substitute for those limits.
This change does not implement a live enumerator or turn a caller's list into
proof. The SQL collector and native identity candidates remain observations.

## Stop and protection lifetime

Read and publication operations use the existing `JournalReadLifecycle` admission
and drain barrier. Stop prevents further admission and publication. Checks around
inspection calls and sink enqueue prevent a late return or swallowed reentrant
request from continuing the batch. An already admitted source call may return
after stop was requested; it cannot publish its result afterward.

Closing either the verified startup or its reader requests stop, waits for
admitted work to drain, closes the coordinator, and only then closes the inspection.
It does not hold the startup monitor while waiting for drain. Reentrant close
from inside an active callback requests stop and rejects rather than deadlocking
or releasing protection early. Interruption does not bypass the drain barrier.
There is no new native cancellation or bounded shutdown claim: a source call that
does not return leaves close pending and the inspection held.

Inspection closure is not permission to release durable journal retention or
authorize maintenance. Those obligations remain with the existing protection
contract. This wrapper neither installs a provider nor replaces Kafka fencing.

## Verification

Full `verify -DskipITs` passed **1,576 tests** across 41 classes, including formatter
and import validation. The initial focused startup, fingerprint, SQL-evidence and
incremental-reader run passed 729 tests; the post-review startup/reader run passed
305 tests. The 225 new cases include:

- Fresh verified sessions after every captured boundary in eight native scenarios,
  including both clear subtypes, full and savepoint rollback, interleaving and an
  update. Native record bytes are retained; startup, protection and completeness
  evidence are explicitly synthetic fixtures.
- Strict/new profile mismatch, missing offsets, unsupported policies and all
  required controlled-initialization evidence, plus a non-reusable reader handoff.
- Missing enumeration support, incomplete or mismatched history/plans, missing
  exact entries, wrong incarnations/positions/raw bytes, bounds exhaustion and
  changed evidence after reads. Required inclusive starts cannot be omitted.
- Interrupted batch publication (false, exception and interruption), whole-batch
  replay through a fresh verified session, and swallowed reentrant operations.
- Stop at observe/history/plan/exact-read/preparation/enqueue boundaries. A held
  old read keeps its inspection while an independent replacement finishes; close
  releases it exactly once after drain. Concurrent/reentrant close does not deadlock.
- An open transaction spanning two separately verified cuts, and rejection of an
  attempted read beyond the current cut without additional source work.

Advisor inspected the actual files and found an initialized-session stop gap:
`observe()` could request stop, yet `recheck()` would still call `handoff()`.
Admission is now checked between every inspection call. The added initialized
heartbeat regression asserts that handoff is never invoked after that stop,
no record is emitted and the checkpoint is unchanged. The finding was corrected
and reverified locally; no second independent review pass is claimed.

These new tests use a simple record factory to inspect the handoff's output. The
existing real dispatcher/RPC-page tests, Kafka probes, strict startup tests and
all native fixtures are unchanged and remain separate evidence. No new production
configuration or transactional streaming path enables this reader. F/CR PR #114
and the original feature worktree are unchanged.

## Remaining live boundary

The next concrete source-side work is a bounded IBM i implementation that can
provide the retained protection, complete history enumeration, exact raw reads,
native identity lineage and historical table bindings required by `Inspection`.
The present SQL collector intentionally cannot supply them. Candidate observations
must remain candidates until their native/operational acceptance cases pass.
Normal append/tail extension, receiver rollover, baseline handoff and the supported
deployment profile still require their applicable acceptance evidence. No
[C/N/P/H/B release gate](dcg-startup-acceptance.md) is waived by this integration.

Evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-verified-lifecycle-reader/20261107/`.
It contains the before snapshot, complete incremental patch, source/build hashes,
verification logs, machine-readable test results and independent review result.
