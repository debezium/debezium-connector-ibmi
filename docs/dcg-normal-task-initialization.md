# Explicit initialization in the normal D/CG task

Later configured native work is recorded in [the configured profile](dcg-configured-native.md)
and [its validation report](dcg-configured-validation.md). Statements below about
test-only admission describe the historical stage covered by this document.

This extends the resume-only integration in [normal task integration](dcg-normal-task-integration.md).
The actual `As400ConnectorTask` and Debezium coordinator now run the existing
`As400NativeInitialization` protocol in their snapshot phase, through a separate
package-private `initializationForTesting` admission. The default hook returns
null. No shipped provider, public enablement, native connection or transactional
streaming guard is changed. F/CR PR #114 remains separate.

## Startup and acknowledgement sequence

An explicitly admitted first start requires absent source offsets. Any existing
offset, including a pending snapshot offset, rejects this path. Resume remains a
separate admission requiring exact durable F/W and initialized immutable history;
a missing offset cannot choose a synthetic start position.

The initialization provider consumes a durable, single-use fresh-generation
enrollment and supplies the held source session to the existing initializer. The
initializer validates a complete empty transaction census and native bindings.
The provider materializes owned snapshot rows before the initializer selects and
verifies boundary B. Selecting the baseline itself can create journal entries;
therefore B is never sampled before baseline preparation.

The task builds snapshot `op=r` envelopes and complete keys with acquired native
schemas. It does not use the integer-only output schema from early experiments.
The first admitted implementation bounds the materialized baseline to 1,000 rows
and 1 MB, with complete projection/type/key checks. A provider must also bound its
own allocation before returning the rows. This is a deliberately bounded test
entry, not a large-table snapshot implementation.

Snapshot rows enter the real task queue, followed by a baseline barrier. Pending
initialization offsets cannot be used as F/W. The task waits for the ordered
`commitRecord` prefix, the qualified EOS/poll `commit` callback and exact offset
storage readback. The existing initializer then rechecks native state, writes an
immutable binding-history record containing the Kafka baseline receipt, recovers
that record and rechecks native state again. Only then does the task enqueue an
initial F=W=B exclusive barrier and await its durable commit/readback.

The baseline and initial checkpoint barriers use a separate
`<heartbeat-topic>.initialization` topic. They do not mix string markers with the
normal heartbeat schema. The coordinator receives the acknowledged offset,
independently recovers history and native output schemas, constructs the existing
continuous runtime, and invokes the provider's explicit handoff. Only successful
handoff may end initialization exclusion. Journal reading then proceeds using the
same queue, callback tracker and stop/drain lifecycle as resume. Scan R stays
in memory; only conservative F/W enters durable offsets.

The first-start provider does not act as a publication owner. Kafka Connect still
owns task reassignment and broker fencing. Single-use enrollment protects the
choice to initialize a fresh output/history generation; it is not checked to
authorize every publication. The candidate never calls the older custom
ownership/receipt machinery.

## What a qualified provider must prove

The new interface is a dependency contract, not a proof supplied by its name or
by connector configuration. A native implementation must establish all of these:

- Explicit enrollment in a dedicated, empty output/history generation, with a
  durable single-use marker. Inspect raw broker log positions, including aborted
  output and existing history. Missing offsets or a lost enrollment file alone
  are not evidence that the generation is empty.
- Source-wide exclusion of every relevant writer, DDL, maintenance operation and
  API producer during initialization. A table lock, a quiet interval or absence
  of visible transactions is not a general journal-wide exclusion mechanism.
- Complete census authority, native identity/schema/member bindings, the same
  session epoch and scope throughout, and an exact readable boundary.
- Operator-managed retention for every receiver/history record required by F/W.
  This change supplies no automatic retention release or protected restore.
- Bounded owned baseline values, no source work deferred until acknowledgement,
  and one cancellation/cleanup owner covering initialization and runtime handles.
- A reader continuation from the acknowledged boundary and an explicit success
  transition out of source exclusion. Failure or stop must not silently release
  exclusion, replay initialization into an existing generation, or claim that a
  stop request has completed drain.

The local provider controls all source producers and replays captured native
bytes. Its session token files model this authority **only inside that fixture**.
They do not qualify an arbitrary IBM i journal. Normal task startup still has no
native implementation of this contract.

## Crash, failure and stop behavior

A partial snapshot, an acknowledged baseline without history, and durable history
without an acknowledged initial checkpoint are quarantined. Their pending offsets
cannot resume streaming. Retained output/history or the consumed enrollment also
prevent a missing-offset restart from initializing again. Operator recovery of
such a generation is deliberately outside this test entry.

Once initial F/W and matching initialized history are durable, a fresh task can
use the existing resume admission without repeating the snapshot or requiring a
new initialization writer fence. It still validates the runtime, native journal,
retention, fingerprint and history. An interrupted streaming batch replays from
its earlier W, preserving F; an already delivered prefix may repeat. The source
contract remains at least once, not atomic visibility of an entire native
transaction across polls or topics.

Stop closes admission and callback tracking, requests cancellation, then waits
for earlier operations and cleanup. A late prepare/history/checkpoint/handoff
operation cannot begin streaming. Tests distinguish stop request from completed
drain and allow an independently admitted replacement to progress while an old
operation is held. Session loss, changed epoch, missing readback, dropped callbacks
or incomplete/wrongly typed snapshot rows fail closed.

## Local verification and evidence

`As400InitializationTaskTest` adds 20 cases using actual final task `start(Map)`,
`poll`, callbacks, coordinator and `stop`. It covers both barriers, empty baselines,
crash/restart at four boundaries, reused or lost-offset enrollment, stop during
four held phases with replacement progress, session/acknowledgement failures and
baseline projection/key/type/size rejection. A deterministic concurrent-callback
case holds the provider after it releases exclusion but before handoff returns:
the callback waits for the task's atomic transition rather than spuriously
rejecting the released initialization session. Cancellation remains independent
of this transition lock. The previous 29 recovery-task tests
and all stricter initialization, ownership, decoding and native-safety tests are
preserved.

`mise exec java@temurin-21 -- tools/dcg-first-start-probe NEW_EVIDENCE_DIRECTORY [ack|crash|qualification|stop|fencing]`
compiles the test bundle and builds its test dependency classpath before using
local Kafka Connect 4.3.1 on an internal Docker network. Its test task inherits
normal startup and the actual coordinator; it injects no task queue or coordinator
and creates no independent initializer thread. Only the admission provider and
native I/O are controlled. `doPoll` adds bounded fault gates and diagnostic headers
around the real task poll; callbacks delegate to the real task before logging.
No F/W is seeded through the offsets API. The task creates its initial offset.

The runner retains broker readback for separate data, transaction, heartbeat,
initialization and history topics. It checks delayed acknowledgements, interrupted
first starts, qualified-session loss, terminal stop, fresh-instance recovery,
legitimate task reassignment and broker rejection of a stale producer. Failed
runs collect independent broker evidence before owned containers/network cleanup.
No IBM i library is created or accessed.

The bounded profile remains one task, one partition per topic, worker EOS enabled,
connector EOS requested, poll transaction boundaries, no SMTs or tolerated errors,
and 5s request / 10s API / 20s worker sync / 30s rebalance timeouts. The connector
does not advertise exactly-once source semantics. No cross-topic total ordering,
broker-loss durability, arbitrary-schema/DDL, large-table snapshot, sustained-load
or sink qualification is added.

Evidence directory on this workstation:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261108/normal-task-initialization/`.
The pre-turn archive, turn-only patch and hash audit distinguish this change from
the branch's existing local work. Final verification and review results are recorded below.

## Next gate

The follow-up [native normal-task validation](dcg-native-normal-task-validation.md)
connects an isolated edgeqa provider to these interfaces and the real coordinator.
The following general deployment gates still apply beyond that controlled fixture.

The native follow-up validates a test-only qualified-session provider for an
explicitly controller-owned isolated source, using the same task entry and no
synthetic schema or offset seeding. It closes the live normal-task integration
gate for that bounded fixture. General source exclusion, public configuration and
support admission remain unresolved; transactional streaming stays disabled until
those gates pass. See the follow-up report for native evidence and cleanup results.

## Verification results

The final full suite (`verify-02.log`) passed 2,434 tests across 61 distinct
classes, with no failures, errors or skips. This includes 20 new actual-task
initialization cases and the existing 29 recovery cases. `package-02.log` records
successful assembly plus formatter/import validation; `git diff --check` passes.

`kafka-01` passed the focused 15-check acknowledgement/restart run. `kafka-02`
passed the complete 29-check fault matrix before the handoff concurrency fix.
Inspection then identified the interval between provider session release and the
task recording handoff. The deterministic regression fails with session checks
outside the transition lock (`handoff-regression-red.log`), and passes with the
atomic transition. Initial compile/test diagnostics are retained as well; the
asynchronous skipped-prefix failure is asserted through the real task poll.

`kafka-03` repeated all 29 assertions with the final compiled implementation.
All three local runs passed owned container/network cleanup. `audit.json` matches
staged main classes and the packaged connector to the verified build, checks that
test admission/provider classes are absent from the distributed plugin, and
confirms the pre-existing tests, core initializer/acknowledgement/recovery
components, transactional guards, F/CR worktree and original feature worktree
remain unchanged. `turn.patch` contains only this implementation step.

No IBM i connection or mutation, commit, push, or PR update occurred. The result
validates normal-task integration against controlled source I/O and real local
Kafka, not native source-session qualification.

## Independent review

Advisor was called exactly once after implementation, final unit/build checks,
the final Kafka matrix and artifact audit. It inspected actual repository files
and retained evidence and reported: “No actionable issues found in inspected
files and retained evidence; tests were not rerun, and native provider
qualification remains outside this review.” `advisor-review.json` retains the
result. No native-session proof or permission to enable streaming follows from
this local review.
