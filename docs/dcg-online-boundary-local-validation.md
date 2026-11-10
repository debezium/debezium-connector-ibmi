# Local validation of an online legacy migration boundary

This is a test-only continuation of [the API assessment](dcg-online-boundary-validation.md).
It does not enable migration, acquire native transaction inventory, or change the
connector's startup/streaming guards. It uses local Kafka and controlled JT400
objects; it never connects to IBM i. The earlier F/CR branch is unchanged.

## What the experiment exercises

`As400OnlineInventoryModelTest` enumerates 462 logical schedules. With complete
visibility of a persistent active definition and visibility of a retired cycle's
terminal record by the end of the journal bracket, the census/log union covers
every required cycle. It also constructs missed-cycle counterexamples when either
assumption is removed. Definition reuse does not merge cycles. Neither capture
filters nor zero pending changes may exclude a transaction from the inventory.
Known incomplete observations and unavailable original starts reject reconstruction.

This is a conditional algorithm test. It does not establish either IBM i API
assumption. The model's `complete` flag is a stated precondition, not an implemented
authority, continuity, receiver-identity or completeness validator.

`tools/dcg-legacy-boundary-probe` builds a test plugin and runs an isolated local
Kafka/Connect experiment. It reuses the existing ownership/normal-task packaging,
worker timeout profile, independent consumers and owned-resource cleanup. It does
not add a custom ownership or receipt store. It exercises:

- Real `As400ConnectorTask` polling, acknowledgement and stop methods, with
  test-only startup and queue/connection injection.
- Real coordinator execution/context and controlled reader stop/close.
- Real `As400RpcConnection.getJournalEntries`, `RetrieveJournal`, native-page
  parsing, legacy `processJournalEntries`, record emitter and Debezium dispatcher.
- Mocked JT400 connections, jobs and service-program calls; fixed integer schemas
  and controlled JDBC metadata. The row decoder delegates to the strict decoder.
- Actual broker transactions and Connect source offsets, not a copied Kafka loop
  or a local callback treated as durable delivery.

The test startup is **not** normal native initialization, and this is **not** a
D/CG recovery admission path. The artifact contains test classes and must not be
distributed as the connector plugin.

## Publication contract under test

The two ordinary-offset controls start from processed legacy 40. One publishes
an idle control, the other uses the real legacy path to publish source row 42.
Both must durably store the same ordinary offset 40. This checks that the source
journal and ordinary offset alone cannot identify the publication history.

The candidate starts at a controller-supplied fixture offset 39. It reads a
before-image at 40 and a key-changing after-image at 42, followed by a nontransactional
F/CR at 43 and a reload row at 44. The real dispatcher produces delete, tombstone,
create, truncate, create. The fixture enqueues a separate identifiable barrier
only after that complete output. The source stops admitting further journal work;
it does not stop database writers.

With `transaction.boundary=connector`, one-record task polls remain in the same
Kafka transaction until the barrier requests `commitTransaction(record)`. The
normal task's per-poll configured-transaction hook is deliberately not installed
in this test-only startup. This is the candidate behavior being evaluated, not a
claim that the existing normal task already implements a migration barrier.

The independently consumed durable barrier carries processed legacy cut 44,
its test policy label and originating generation. It contains no F/W or scan-R
fields. A replacement accepts the completed barrier only as a drained legacy
fixture, emits nothing, and does not enter D/CG recovery. Offset loss rejects
before source work. The label is not a substitute for the real journal identity,
receiver incarnation and capture fingerprint required by a future native path.

The fault matrix covers termination before enqueue, after part of the key change,
after all data and barrier enqueue but before barrier polling, after Kafka commit
but before the barrier callback, and suspended-worker reassignment. Independent
consumers check committed output and offsets; uncommitted reads establish that
partial-output faults actually reached the broker. Cleanup removes only resources
owned by the experiment.

Read-committed consumers and the qualified Connect EOS profile are premises of
this candidate. The test makes no cross-topic ordering claim and does not imply
retroactive exactly-once delivery for records emitted before the barrier protocol.

## Composition with the conditional handoff proof

Two additional `As400LegacyHandoffTest` cases isolate what an acknowledged exact
cut changes. At fixture cut 40 with the existing A-only capture filter, ordinary
legacy 40 is ambiguous; an explicitly assumed complete-output cut permits the
strict replay path to reconstruct context and emit A:8 once after a fresh instance
loads the candidate checkpoint. F stays 31 and the output checkpoint becomes 42.
It does not reinterpret legacy 40 as W.

The second case still rejects a selected-table prefix in an open transaction and
missing retained history. Existing tests remain unchanged in behavior. There is
no new public per-table truncate setting; these cases use ordinary capture filters.

These are **separate fixtures**: the Kafka experiment uses synthetic LIB/RCV
coordinates, while the handoff proof uses the retained DCOC1107 capture and its
fixture-supplied closed anchor. No broker offset is relabeled as that native
receiver or passed off as native startup evidence. This is a compositional local
proof, not a complete broker-to-native migration test. A future combined native
fixture must bind the same acknowledged cut to acquired identities/history and a
validated nonempty transaction context.

## Limitations found while running the probe

The first development attempt exposed fixture setup mistakes: an unnamed
control schema was unsuitable for BaseSourceTask's envelope check, dispatcher
shutdown needed a heartbeat instance, and the synthetic scan context had not
marked the baseline snapshot complete. These were fixed in test code. The stopped attempt and cleanup evidence are retained.

The next attempt exposed an existing legacy-path limitation with
`provide.transaction.metadata=true`: an autocommit row sets a null transaction
context, and the dispatcher throws from `TransactionMonitor.dataEvent`. The legacy
handler logs and catches that exception; no row is emitted. The experiment now
explicitly rejects that setting, matching the previous conditional handoff proof's
metadata-disabled scope. No connector code was changed to conceal this result.

This also means that a future production barrier must fail closed on **all** row
processing/dispatch failures: acknowledging a queue marker cannot prove that every
intended source row entered the queue. The current local success cases exercise
known valid input and assert every expected record. They do not qualify legacy
error swallowing as safe. Transaction metadata support and a fail-closed legacy
publication hook require their own implementation and failure tests before that
broader configuration can be supported.

Native receiver pagination/continuation semantics, inventory visibility under
concurrent commits, authority completeness, retained starts, binding history and
bootstrap from nonempty transaction context remain unproved by this experiment.
The synthetic pages end at known processed entries; a real cut must not be guessed
by incrementing an ordinary offset. Dirty open prefixes remain rejected even with
a perfect inventory and barrier. There is no bounded activation-time guarantee
under continuous incompatible transactions, and no fallback writer pause.

## Reproduction and evidence

Run `mise exec java@temurin-21 -- tools/dcg-legacy-boundary-probe NEW_EVIDENCE_DIRECTORY`
from this worktree. The directory must not already exist. A local Unix-socket
Docker daemon and the harness's pinned Kafka image are required. The network is
internal and no IBM i credentials are accepted. The ordinary Maven unit suite
includes the schedule model and conditional handoff cases.

Evidence for this development run is retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-online-boundary-local-20261108/`.
Each Kafka attempt retains configuration, staged artifact hashes, control events,
independent consumed records/offsets, worker/broker logs, assertions and cleanup.
The final manifest records the passing attempt, unit/build results and file
preservation checks. Earlier interrupted/failed attempts remain labeled as such.

## Completed local results

The final `kafka-3` attempt passed all 38 assertions on the pinned Kafka 4.3.1
image, including all five fault schedules and missing-offset rejection. Owned
container/network cleanup passed. Independent broker logs record task producer
epochs 0, 2, 4 and 6 for the reassignment scenario; the replacement's committed
barrier and record set remain unchanged when the old worker returns.

All 2,395 unit tests passed. Maven verify/assembly, formatter/import validation,
Python/shell syntax and diff whitespace checks passed. Every staged Java class
matches the final build byte for byte, and the main connector jar contains no
barrier-probe or inventory-model test class. The only post-run Python change
suppresses import bytecode caches; it does not alter the fault schedules.

File hashes confirm production code and the protected D/CG, F/CR and original
truncate worktrees are unchanged. The exact seven-file local delta and the
verification manifest are retained with the evidence. No branch was pushed,
no migration path was enabled, and no IBM i connection or mutation occurred.
