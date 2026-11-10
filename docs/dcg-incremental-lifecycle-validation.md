# Incremental D/CG lifecycle and recovery integration

Status: local staged implementation, 2026-11-07. Transactional streaming remains
disabled. This builds on the [native discrimination experiment](dcg-native-lifecycle-discrimination.md),
using its captured records without making new IBM i connections or mutations.

The subsequent [verified reader handoff](dcg-verified-reader-validation.md) adds a
separately fingerprinted startup profile and bounded protected read sessions. Its
fixture coverage does not supply the missing live inspection/protection adapter.

## What changed

`As400JournalLifecycle` is a bounded, incremental classifier connected to an
explicit opt-in constructor of `As400ReplayCoordinator`. The ordinary constructor
retains strict lifecycle rejection. No connector configuration, production task
startup or streaming branch enables the new path. The selected native profile
uses conservative savepoint consumption; it does not infer native marker lifetime.

Every entry is copied and decoded from owned bytes. A precursor opens a lifecycle
window; the classifier requires the exact type-specific sequence, producer,
paired file/member identities, flags, payload, commit cycle and nested scope.
Other producers may progress while that window is open, but their component
records are held too. Only after all overlapping windows close can the coordinator
normalize held records and publish completed batches in commit order. A mismatch,
unsupported record, verified tail inside a window, or exhausted bound stops the
coordinator. Empty RPC pages must not be reported as a verified journal tail.

The bounds cover held entry count/bytes, active cycles, savepoints, retained clear
identities and recovery dependencies. There is no eviction or skip-on-error path.
The reusable RPC page can be overwritten after the callback returns. The copied
record is also decoded again, so a supplied header cannot substitute metadata for
different bytes.

## Checkpoints retain dependencies automatically

R now follows every accepted raw journal entry, including lifecycle companions.
F/W remain the durable replay start/completed-output positions; R is never stored
in a recovery offset. Normalization and publication can lag R while a window is
incomplete. The coordinator applies lifecycle dependencies whenever it creates a
checkpoint, including checkpoints frozen into data/metadata records and published
heartbeats. Callers no longer apply a separate test-only checkpoint transformation.

A full rollback that has a cleanup tail is held until that tail validates. Its W
can then become C/RB, but F still includes the original C/SC because cleanup lies
after W. A fresh instance reconstructs the relationship before accepting the tail.
No new offset field or custom durable store is introduced.

Dependencies also matter after W passes a completed transaction. If another open
cycle keeps F inside that completed transaction's lifecycle, replay still needs
the earlier C/SC. The classifier retains it. It computes the transitive closure
of those intervals, then releases dependencies only when the resulting replay
interval no longer touches them. Thus completing B does not discard context that
will be needed while reconstructing A. Once no open cycle or lifecycle interval
requires older input, F can return to exclusive W.

One validated window can release multiple commits. The coordinator drains these
batches serially, preparing each complete batch before enqueue and freezing a
separate previous-W checkpoint for each. A failure in the second batch preserves
the completed first batch's boundary. Enqueue still is not a durable Kafka
acknowledgement; tests explicitly serialize the offset from an acknowledged record
or heartbeat when modeling a restart.

## Verification and its limits

`As400IncrementalLifecycleTest` adds 337 cases:

- Thirteen original native scenarios traverse the incremental classifier, strict
  normalization, transaction buffer, replay coordinator and real Debezium
  dispatcher. No two-pass candidate projection supplies their input. Actual
  captured threads, sequence numbers, cycles, flags and payloads are preserved.
- Fresh instances restart after every entry boundary using serialized heartbeat
  F/W. Commit, full rollback, repeated savepoint rollback, atomic failure,
  non-atomic error, interleaving and key-changing update outcomes are asserted.
  The real dispatcher emits delete/tombstone/create for the captured key change.
- Thirty-nine cases exercise the real `As400RpcConnection.getJournalEntries`
  loop plus real `RetrieveJournal` entry iteration and decoding with page sizes
  one, three and the whole capture. Transport retrieval alone is replaced with
  fixture pages. A separate invalid-companion case verifies the RPC scan does
  not advance past the rejected entry. This is not a new JT400 transport or live
  cancellation experiment; earlier transport/stop tests are preserved.
- Deliberately reordered, consistently renumbered fixtures test overlapping
  producers and completed cycles whose starts must remain before F. These are
  labeled synthetic interleavings, not claimed as new native observations.
- Interrupted delivery through the real dispatcher, multiple queued commits,
  page-buffer reuse at nonzero offsets, capacity exhaustion, wrong identities,
  scope/cycle mismatches during replay, unsupported receiver changes, incomplete
  windows, missing rollback context and the captured real missed-write gap reject
  or recover with the expected F/W and output.

The existing two-pass candidate and stricter tests are unchanged. No new Kafka
broker experiment is claimed; broker fencing/acknowledgement evidence remains in
the existing probe reports. No source fixture was created, so this step requires
no IBM i cleanup. F/CR PR #114 and the original feature worktree are unchanged;
nothing was committed or pushed.

Full `verify -DskipITs` passed **1,351 tests** across 40 classes, including formatter
and import validation. The focused lifecycle/replay/dispatcher run passed 484
tests. This does not include live IBM i integration tests.

Advisor inspected the actual repository files and verification and passed the
incremental classifier, checkpoint retention and queued delivery review, with the
staged-profile limits explicit. Its result is saved with this run's evidence.

## Remaining gates

The profile intentionally supports one verified receiver and the observed
recording settings, with at most one canceled clear in each rollback window.
Receiver rollover and additional cleanup variations reject. Paired bindings,
receiver incarnation/ranges and schema history are supplied fixture evidence in
these tests. The classifier does not establish native startup proof, protection,
source lineage, historical decoder validity or a consistent initial baseline.

The next integration boundary is a verified startup-to-reader adapter that
supplies that evidence and the exact profile/fingerprint, plus continuous
receiver/history validation. It must preserve current startup checks and reject
missing proof. Enabling transactional streaming, expanding receiver support or
claiming a supported production deployment still requires the applicable
[acceptance gates](dcg-startup-acceptance.md); none is waived by these local tests.

Evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-incremental-lifecycle/20261107/`.
The before snapshot, incremental patch, test/build logs, final source hashes and
independent review result belong to this run. Native provenance/cleanup remain
in the earlier discrimination and thread-validation evidence directories.
