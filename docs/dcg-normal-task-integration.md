# D/CG recovery in the normal task lifecycle

Later configured native work is recorded in [the configured profile](dcg-configured-native.md)
and [its validation report](dcg-configured-validation.md). The later
[legacy handoff continuation](#legacy-handoff-activation-2026-11-09-continuation)
adds a distinct startup origin to the actual task. Earlier statements below about
test-only admission describe their historical stage.

The [joined live-source validation](dcg-live-online-handoff-validation.md) now
exercises this task transition with a native prefix reread and continuous IBM i
reader, including an open clear across receiver rotation and fresh-task recovery.
Its initial boundary is still controller-acquired; public automatic migration
remains disabled.

This change connects the existing D/CG adapters to `As400ConnectorTask` and the
real Debezium `ChangeEventSourceCoordinator`, through **test-only admission**.
It does not enable transactional streaming for the normal connector. There is no
public configuration switch or discovered provider for this path. F/CR PR #114
and its worktree remain unchanged. No IBM i connection, source mutation, commit
or push is part of this work.

## Implemented scope

1. **Resume from validated durable state.** The task's package-private
   `recoveryForTesting` hook defaults to null. An admitted test task supplies the
   expected journal identity, capture fingerprint, qualified runtime/session and
   retained history/interval provider. The task uses its real Connect
   `OffsetStorageReader`, rejects missing/legacy/mixed/mismatched offsets before
   source work, and executes history recovery on the coordinator thread. The
   resume-only snapshot phase requires initialized immutable binding history,
   rechecks that the durable offset has not changed, and builds native output
   schemas from recovered definitions. It neither snapshots nor queries today's
   catalog to invent historical bindings. No JDBC or legacy RPC reader is opened
   by this branch of task startup.
2. **Output and durable acknowledgement.** `As400RecoverySource` runs the existing
   `As400ContinuousRuntime`, strict cut reader, decoder, normalizer, transaction
   buffer and `As400BatchDispatcher`. Actual Debezium envelopes, native schemas,
   keys and source metadata enter the normal task queue and `poll()` path. The
   batch dispatcher also receives the coordinator's real streaming metrics.
   A real heartbeat carries the conservative F/W checkpoint after each productive
   interval. The ordered acknowledgement barrier requires successful
   `commitRecord` callbacks for the entire prefix, the qualified EOS/poll worker's
   `commit` callback, and exact independent offset-storage readback. Only then may
   the coordinator's durable offset context advance and the next interval begin.
   A producer callback or `BaseSourceTask.performCommit()` alone is insufficient.
3. **Stop, drain and replacement.** The coordinator's normal startup and execution
   are retained; its stop override closes source/callback admission and requests
   cancellation before waiting for the coordinator executor. Source/provider work
   admitted earlier must return before resource cleanup can finish. Cleanup runs
   once and completed drain is reported only after it succeeds. Cancellation or
   cleanup failure remains a failure. Late records/callbacks cannot revive a
   stopped source. Recovery/acknowledgement failures reach the actual task poll;
   they do not use BaseSourceTask's automatic retry of an already stopped session.
   A replacement starts its own validation and reconstructs from durable F/W.

F is the retained replay floor, W is the completed-output boundary, and R is the
in-memory scan position. R remains separate and is never serialized as a restart
checkpoint. A clear can remain open while unrelated completed transactions are
published. Crash recovery replays a whole incomplete output batch; it can duplicate
an already delivered prefix. This remains an at-least-once source contract. Kafka
transactions per poll do not make an entire native transaction atomically visible
across multiple polls or topics.

The admission provider is a dependency contract, not proof created by this task.
It must attest the qualified runtime and source session, validate the capture
policy against the fingerprint, recover retained bindings, preserve operator
retention, and stop new source operations when cancellation is requested. No
shipped implementation claims general native startup authority. Existing native
identity, member/file binding, receiver continuity, null/row/schema, journaling
transition and savepoint checks are reused without relaxation.

## Local acceptance coverage

`As400RecoveryTaskTest` invokes the actual final `start(Map)`, `poll()`,
`commitRecord`, `commit` and `stop()` methods. Only admission and external I/O are
controlled; the coordinator, task queue, streaming loop and dispatcher are not
replaced. Its 29 cases cover:

- Open-clear unrelated output, ordered acknowledgement and fresh-instance replay.
- Interrupted truncate delivery retaining earlier W and replaying the full clear
  batch in order; R does not become an offset.
- Missing, legacy, mixed scan/FW, wrong-journal and wrong-fingerprint offsets;
  missing, malformed and uninitialized history; no implicit reinitialization.
- Missing durable readback, dropped/mismatched acknowledgements, a skipped prefix,
  changed session and unsupported runtime/signal settings; unexpected reader
  interruption fails the task instead of leaving a silent stopped source.
- Stop during held history recovery, interval reading and resource cleanup, while
  a replacement makes progress; late callbacks; cancellation/cleanup failures.
- The public task still has no recovery admission and the legacy offset loader
  still rejects F/W offsets.

`tools/dcg-normal-task-probe NEW_EVIDENCE_DIRECTORY` exercises the same path in
local Kafka Connect 4.3.1 using a Docker internal network. Its test connector
inherits normal task startup/poll/stop and overrides only admission plus callback
logging that delegates to the actual task first. Captured IBM i bytes are supplied
through controlled I/O. The controller explicitly writes an acknowledged test
baseline and immutable binding history to Kafka, then seeds the stopped
connector's F/W through the public offsets API. Tasks never initialize history.
Replacement fixture definitions are discarded before history recovery.

The runner checks unrelated-table progress with an open clear, actual durable F/W,
Kafka task reassignment while the old reader is held, late-reader drain with no
extra output/checkpoint, committed clear/reload ordering, invisible rollback and
missing-offset rejection. Independent read-committed consumers inspect data and
Connect offsets, including after failures. The candidate artifact excludes the
custom ownership/receipt authority. The prior stricter tests and Kafka harnesses
remain intact; their full fault matrices are not repeated by this bounded test.

The test runtime uses worker EOS enabled, connector exactly-once support requested,
poll transaction boundaries, no tolerated errors or SMTs, one task and one
partition per output topic. The fixed timeout profile remains 5s client request,
10s client API, 20s worker sync and 30s rebalance. The source does not advertise
exactly-once support. The runner verifies removal of its containers and network.
It creates no IBM i library.

Evidence root on this workstation:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/normal-task-runtime/`.
The per-attempt logs/reports retain diagnostic failures, independent broker
readback, exact plugin hashes and cleanup. `before.json`, `before.zip` and the
turn-only patch distinguish this work from the branch's existing local changes.

## Remaining gates

The subsequent [explicit first-start integration](dcg-normal-task-initialization.md)
connects initialization and snapshot acknowledgement to this coordinator through
test-only admission. The results below describe the earlier resume-only step;
native qualified-session admission and public enablement remain separate gates.

This resumes an already initialized source. Connecting explicit native
initialization and snapshot acknowledgement to the normal task, selecting a
qualified journal-wide writer/maintenance fence and native session provider, and
mapping the supported profile into public configuration remain separate work.
Missing offsets continue to reject even when history exists. Operator-managed
retention remains mandatory. There is no automatic retention release or protected
restore guarantee.

This local experiment adds no native release/PTF, arbitrary-schema/DDL,
sustained-load, broker-loss durability, or truncate-aware sink qualification.
Prior isolated edgeqa continuous-runtime evidence is recorded separately in
[continuous runtime validation](dcg-continuous-runtime-validation.md); it is not a
live run of the newly integrated normal task. Normal transactional streaming stays
disabled until the initialization, deployment and validation gates are complete.

## Verification results

The full local suite passed 2,414 tests across 60 distinct classes (including the
29 new actual-task cases), with no failures, errors or skips. Assembly and
formatter/import validation also pass. Logs count distinct current executions,
not stale report files or duplicate module runs.

`kafka-01` rejected startup because the test bundle lacked the normal metrics
DDSketch dependency. `kafka-02` rejected fixture history/reader setup because the
container JRE required the existing Byte Buddy test agent to be supplied at JVM
startup. Both failures emitted no source output; both retained independent broker
evidence and passed container/network cleanup. These are diagnostic attempts, not
successful runtime validation. `kafka-03` passed all 11 assertions and cleanup.

The final `kafka-04` build repeated all 11 assertions after adding the unexpected
reader-interruption regression. Cleanup passed in all four local attempts.
`verify-02.log`, `package-02.log`, `tests-summary.json`, `kafka-04/report.json` and
`audit.json` record the final checks. The audit matches all staged main classes to
the checked build, confirms test admission classes are absent from the distributed
plugin, and verifies that existing tests, transactional guards and both protected
worktrees match their pre-turn contents.

## Independent review

Advisor was called once after implementation, the full suite, assembly checks,
final local Kafka run and audit. It reported: “No actionable issues found in the
inspected implementation and retained evidence; tests were not rerun.” The saved
result is `advisor-review.json` in the evidence directory. This review does not
supply native normal-task validation or remove the remaining initialization and
support gates.

## Legacy handoff activation (2026-11-09 continuation)

The normal recovery path now models an acknowledged legacy handoff separately
from an acknowledged initial snapshot. Public native admission still returns
`allowLegacyHandoffRecovery=false`; the captured-source experiment is the only
adapter opting in. Automatic migration remains disabled. No F/CR change is
included.

`As400LegacyHandoff` binds the independently computed initial F/W to the exact
legacy source offset, including the migration marker. The pending schema manifest
continues to identify N and the capture policy. `As400BindingHistory` appends a
version-3 activation record in the same ordinary history store only after exact
Connect offset readback of that marker, and checks the offset again before and
after persistence. It never initializes missing history during activation.
The record includes a canonical digest, native receiver identity and the full
initial checkpoint. Fresh recovery validates its source, preceding pending
manifest, empty schema changes and exact canonical representation. Conflicting
activations or schema records after activation reject; identical activation
retries are harmless.

The existing version-1 pending schemas and version-2 snapshot manifests retain
their original fingerprints and format. The source checkpoint remains offset
version 2 with separate F/W; R is never serialized. Activation does not alter the
binding fingerprint, but normal startup requires the separate activation record.
Deleting it therefore fails closed. Snapshot initialization is not manufactured
or inferred from pending history. A checkpoint before activation's original F/W,
or one including an originally excluded boundary, cannot resume.

The actual `As400RecoverySource.prepare` verifies the startup origin before
journal reading or output. Existing native admission still requires its previous
snapshot origin; the qualified test adapter permits the new handoff origin.
The actual task's test admission can use a real Connect TransactionContext with
its strict connector-managed EOS profile. Existing EOS/poll tests and the
configured native transaction path retain their behavior. Normal runtime requests
per-poll Kafka commits: an interrupted source batch may replay from its previous
completed W. This is distinct from the older probe's whole-batch transaction
boundary; neither is advertised as universal source exactly-once delivery.

Local regressions exercise history-only state, absent/wrong/changed durable
offsets, activation persistence followed by a crash before F/W commit, conflicting
activation, missing/reordered/corrupted history, default admission refusal and
actual normal-task interrupted-clear restart. The normal task emits no snapshot,
keeps W behind unfinished output, and replays the incomplete clear batch from F.

The Kafka experiment enables this path only with both
`DCG_HANDOFF_STANDARD_HISTORY=true` and `DCG_HANDOFF_NORMAL_RECOVERY=true`.
Its original modes and safety assertions remain. The final continuation also
runs the initial legacy-to-F/W transition inside the actual normal task and
coordinator. `As400TaskHandoff` is a qualified provider owned by that source's
cancellation/drain lifecycle. The normal source registers every legacy output,
commits only explicitly enqueued handoff boundaries, checks exact durable offset
readback and transitions directly into its existing continuous runtime. There is
no restart or snapshot at this transition. A replacement uses the same normal
startup and recovered bindings. The source rejects an offset changed between
initial load and handoff, before invoking the provider. The reader transport is
captured native data, explicitly not a simultaneous live IBM i/Kafka run.
The new `activationcallback` scenario kills the first task after activation is
stored while its durable offset is still the acknowledged legacy barrier.

Evidence is retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-normal-handoff-20261109/`.
The combined live-source handoff is still a native qualification gate; no candidate
observation is promoted to native startup proof by this patch. Operator-managed
retention and the fixed-schema, identity, journaling-transition and
unsupported-history checks remain required.

The first in-place interruption run (`kafka-inplace`) deliberately held the Kafka
sender after the truncate acknowledgement. That also stranded an offset-topic
transaction, preventing a replacement worker from completing its startup offset
read before the fixture's membership timeout. The failure and independent broker
offsets/output are retained, and cleanup passed. The refined injection holds the
source-offset send **before enqueue**, leaving the existing offset-topic prefix
readable while the truncate remains uncommitted. Worker timeout settings and the
original safety assertions are unchanged.

The normal runtime uses conservative offsets on source data records. In the exact
interruption ledger, A14 may therefore already be committed while W remains 66;
a fresh task repeats A14 before completing the clear. The normal-mode oracle
requires exactly that independently identified uncheckpointed row repetition;
it does not accept arbitrary duplicates, omissions or partial clears. The older
whole-batch probe retains its exact no-duplicate oracle unchanged. This distinction
is part of the source at-least-once contract, not a broker-fencing failure.

### Continuation verification and admission boundary

`inplace-verify.log` records all **2,755 Java tests** passing (including the 15
new activation/normal-task cases), with no failures, errors or skips. The bounded
Python harness suite passes **124 tests** in `python-final.log`.
`inplace-package.log` records assembly, formatter and import validation passing.
`artifact-audit.json` verifies all **170** distributed connector main classes
against the compiled classes and the actual staged Kafka experiment. Test-only
admission classes are absent from the distribution. `scope-audit.json` confirms
the three protected worktrees, their indexes and commits are unchanged.

The joined live-native provider has not been implemented or admitted by this
continuation. Specifically, the captured provider pins a bounded read-only bundle,
uses controlled transport for the legacy prefix, and verifies replay anchors
against that bundle. It cannot be turned into a live provider by changing a host
property: a live implementation must acquire and validate the short boundary,
continue ordinary legacy output while an attempt is deferred, audit through its
actual acknowledged cut, and recover later F/W from native retained history.
The standard task/history/acknowledgement integration here supplies that provider's
startup contract; it does not replace those source qualifications. No additional
owner/receipt store, per-table truncate option or public migration flag is added.

`kafka-inplace-final/report.json` passes **70 assertions across eight scenarios**:
before the legacy barrier, pending-history persistence, lost legacy-barrier
callback, activation persistence before F/W, lost F/W callback, interrupted clear
delivery, legitimate reassignment with a returning old worker, and missing
history after the acknowledged legacy barrier. It also verifies missing-offset
rejection, later history loss and absence of custom owner/receipt authority.
Local container/network cleanup passes. The earlier focused final-Java run
`kafka-inplace-offset` passes 24 assertions covering interruption and reassignment.
The original strict mode regression remains `kafka-original-regression`, with
17 assertions and cleanup passing.

The final harness corrects its normal-mode `delivery_contract` report wording to
state per-poll transactions and source at-least-once replay. The staged run already
records this in `recovery_contract` and enforces the exact repetition oracle.
`staged-harness-label-diff.patch` shows that only this report label differs from
the staged Python file; no algorithm or assertion changed after staging.
No native IBM i connection or fixture mutation was performed in this continuation.

Advisor was called exactly once for this continuation after implementation and
local verification. Its actual-file review passed: no actionable issue was found
in the inspected files and retained evidence, and no file-access limitation was
reported. The review also confirmed the final eight-scenario Kafka matrix and
cleanup. The exact result is retained in `advisor-review.json`; it does not
qualify live native admission or enable automatic migration.
