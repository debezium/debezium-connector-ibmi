# Planned first-start window for D/CG

The first supported direction is an explicit operator-managed pause for initial
snapshot and handoff. Ordinary recovery uses validated durable F/W and immutable
binding history without opening another window. An open clear does not require a
journal-wide pause during normal CDC: unrelated committed tables continue.

This implementation adds the planned-window policy to the actual task interfaces
and tests it on isolated edgeqa sources. Normal transactional streaming remains
disabled. The native admission is still a test entry limited to owned `DCNCdddd`
fixtures; no public configuration silently activates it. F/CR PR #114 is separate.

## Operator contract

Before opening a window, the operator must stop **every writer, DDL operation,
maintenance operation and API producer using the capture journal**, including
uncaptured resources, and retain that control until successful handoff or explicit
failure recovery. Pausing the connector or stopping only captured-table writers
does not satisfy this requirement. Required receiver/history retention remains
operator-managed.

`tools/dcg-start-window prepare` records the operator identity, unique window ID,
exact journal, captured tables, complete controlled file scope, opening time,
deadline and materialization bounds in a new persistent directory. It requires
explicit writer/maintenance/retention acknowledgements. This command neither
stops source jobs nor proves native exclusion. The directory must be durable and
shared by replacement workers; it must not be deleted to force initialization.
There is no arbitrary shell-command execution from connector configuration.

The source provider must independently qualify the source session. The isolated
provider controls all fixture producers, verifies native EXCLRD locks on A/B/M/U,
compares the journal's total file/member/resource counts with the exact owned
files, rejects unsupported resources, and rechecks scope at enrollment, snapshot,
checkpoint acknowledgement and handoff. Existing census, native identity,
recording-option, schema/member, boundary and retention guards remain in force.
The operator plan supplements these checks; it cannot replace them.

IBM documents that `LOCK TABLE ... IN EXCLUSIVE MODE ALLOW READ` blocks concurrent
application updates to that table and that the lock ends with the transaction or
connection. It does not establish a fence for arbitrary journal administration.
See [IBM LOCK TABLE](https://www.ibm.com/docs/en/i/7.6.0?topic=statements-lock-table)
and [IBM JOURNAL_INFO](https://www.ibm.com/docs/ssw_ibm_i_76/rzajq/rzajqviewjournalinfo.htm).
The documentation lookup's indexed IBM i entry was unhelpful and its CLI did not
support `--research`; these primary IBM pages and actual native observations were
used instead.

## Durable window and task lifecycle

`As400PlannedStartWindow` reads a bounded immutable plan and consumes a single-use
enrollment with CREATE_NEW and fsync. It rejects changed/missing plans, explicit
revocation, expired windows and mismatched capture scope. Both wall time and
elapsed monotonic time enforce the remaining deadline, so moving the wall clock
back cannot extend an already running window. Supported plans are at most 30
minutes. This limit is a policy bound, not a promised completion time.

`As400PlannedTaskInitialization` wraps an independently qualified initialization
provider. It records phase times, delegates the existing native initialization
and both Kafka acknowledgement barriers, and records HANDED_OFF only after the
existing reader handoff succeeds. The same source owns cancellation and cleanup.
Window policy is inactive after successful handoff and is not selected for an
ordinary recovered checkpoint. It is not another output-offset authority or a
replacement for Kafka fencing.

Task poll now rechecks local admission before and after dequeuing. This lets a
deadline or revocation fail the task even while the coordinator awaits a Kafka
acknowledgement. The native provider also checks the window before native entry
and while materializing rows. Failure cannot fabricate an initialization checkpoint
or bypass acknowledgement to start the reader. Already admitted transport/producer work can require drain;
a deadline is not proof that all native work has instantly ended.

Failure cleanup rolls back/closes native handles, but does not tell operators to
resume writers, remove enrollment, reinitialize an existing output generation or
claim a successful handoff. QUARANTINED records ordinary failure; abrupt process
death can leave the last phase unchanged with durable enrollment still present.
Pending snapshot offsets and consumed enrollment reject fresh-instance retries.
If complete F/W and matching history became durable before an interruption,
ordinary validated recovery remains possible even when the old attempt did not
record successful handoff. The operator window is not a second checkpoint authority.
An operator must explicitly resolve the incomplete generation. This change does
not automate destructive output/history cleanup or invent a missing F/W.

`tools/dcg-start-window status DIRECTORY` displays the plan and phase.
`tools/dcg-start-window revoke DIRECTORY` records revocation; it does not restart
source jobs. Operators can resume writers only after successful handoff, or after
separately establishing safe teardown of an interrupted generation. Expiry alone
does not authorize resuming writers.

## Bounds and measured duration

The existing default remains 1,000 materialized rows / 1 MB of owned values.
Explicit planned admission can request up to 9,000 rows and 16 MB. The row ceiling
leaves room for barriers under the unchanged 10,000-record acknowledgement bound.
The native row limit remains 64 KB. Bounds are checked during native
materialization and again before the task queues output. These are payload bounds,
not promises about total JVM memory use; schemas/envelopes have additional cost.
Larger-than-supported sources fail before snapshot publication. This is not an
unbounded or disk-spooled snapshot implementation. The native test source also
retains its independent 20-minute lifetime / 1,000-read ceiling; a longer operator
plan does not override that test bound.

The larger experiment uses 128 records per poll and a bounded 1,024-record queue.
It retains the qualified Kafka EOS/poll profile and conservative acknowledgement
semantics; the source delivery contract remains whole-batch at least once. The
older one-record-per-poll native/Kafka assertions remain available unchanged.

The timer starts when the operator opens the window after fixture loading,
worker startup and topic provisioning. It includes native locks/census/bindings,
snapshot materialization, Kafka publication and durable acknowledgements, history
persistence and reader handoff. Fixture creation/loading is reported separately.
These local Kafka/edgeqa timings do not estimate a real auction's snapshot,
network, broker replication or operational job-stop time.

## Validation and evidence

`tools/dcg-planned-start-probe NEW_DIRECTORY DCNCdddd ROWS_PER_TABLE MODE` runs
one isolated fixture, where MODE is success, timeout, revoke, crash or bounds.
The success path verifies every baseline key/value, removes the active operator
plan after handoff, observes unrelated-table progress during a native clear, and
restarts from durable F/W before committing that clear. Failure cases retain
broker output/offset/history evidence and reject fresh-instance initialization.
The unchanged owned-fixture cleanup requires matching inventory, no pending
transactions or file/journal locks, exact receiver inquiry ownership, RESTRICT
drops and independent schema/library/inquiry absence queries.

Evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261108/planned-first-start/`.
Final measured results, verification and independent review are recorded below.

Local verification passed all 2,479 unit tests across 63 test classes, including
26 new planned-window cases. The unchanged Kafka initialization experiment passed
all 29 assertions, including its stricter acknowledgement, failure and replacement
cases. Four operator CLI checks passed. Maven verify, assembly packaging,
formatter/import validation and whitespace checks passed. Source and staged class
hashes tie the native experiments to this implementation; test-only native
providers are excluded from the distributable connector.

The retained diagnostic runs are not counted as successful end-to-end runs:

- `live-02` and `live-03` exposed observation ordering in the controller: native
  handle drain precedes the policy wrapper's quarantine write, and an expected
  failure marker must not abort waiting for later drain. The runner now waits for
  both outcomes explicitly, with separate bounds.
- `live-04` compared broker records while the original worker could still commit.
  The corrected crash test kills that worker first, reads stable committed
  records and offsets with an independent broker client, and compares the
  replacement against that baseline. `live-14` passed with an incomplete snapshot
  offset rejected and no replacement output.
- `live-05` reached the native row limit, but poll observed the already-stopped
  session before the coordinator reported the materialization exception. The
  runner now requires the intended cause in the actual failure marker or retained
  worker log; a generic stopped-session error alone cannot pass the fault test.
- `live-06` completed the 1,500-row initialization in 287,248 ms, then correctly
  stopped on a SQL0462 `OBJECT_STATISTICS` warning during journal metadata
  acquisition. Another test library was being dropped at that time. The retained
  read-only view definition shows that `JOURNAL_INFO` enumerates libraries before
  examining journals, so concurrent fixture removal is a plausible cause, not a
  proven diagnosis of the returned SQLSTATE. Warning rejection remains unchanged.
  The final larger run is serialized after all fault-fixture cleanup.

These observations also limit the supported claim. Hundreds to low thousands of
rows already require minutes in this conservative native test path. The results
validate correctness and bounded failure handling; they do not qualify the pause
duration for large auction databases. A generally usable native session provider,
representative sizing and any optimization of repeated session validation still
need evidence before public transactional admission can be enabled.

The completed fault reruns passed on 1,500-row fixtures: `live-12` enforced a
35-second window and recorded quarantine at 35,089 ms; `live-13` recorded quarantine
1,038 ms after explicit revocation; `live-14` rejected an interrupted snapshot on
a fresh worker without additional output or checkpoint advancement; and `live-15`
rejected a 1,000-row limit before publishing the 1,500-row baseline. Expiry,
revocation and the row-limit failure all produced zero data with publication
permits withheld and rejected reuse of the enrolled generation. These observed
latencies are not worst-case bounds for arbitrary in-flight native calls.

Both complete success runs passed: 900 rows in `live-01` required 215,646 ms
(3m 35.646s), and 1,500 rows in the serialized `live-07` required 288,870 ms
(4m 48.870s) from opening the window through reader handoff. The latter snapshot
was materialized at 62,181 ms; materialization alone is not the end of the pause.
Both runs checked every native key and mixed-column value, observed unrelated
committed CDC while a clear remained open, restarted with the active plan removed,
recovered the saved F/W without another snapshot, and emitted truncate before its
reload rows after commit. Native reader/JDBC drain and fixture cleanup passed.

The six accepted native runs passed 39 assertions. All eleven created libraries,
including those from diagnostic failures, have retained independent table,
schema, native library and inquiry absence results. `validation-summary.json`
selects successful reruns without rewriting the earlier failures;
`timing-table.json`, `fault-timings.json`, `audit.json` and `turn.patch` preserve
the measurements, artifact/source provenance and current-turn patch. The original
F/CR and feature worktrees, normal streaming guards and protected recovery files
are unchanged; nothing was pushed.

Independent Advisor review passed after implementation and verification. Advisor
inspected the actual target files and retained evidence and found no actionable
issues. It did not rerun tests or live checks; qualification for general shared
journals remains unverified. The exact result is retained in `advisor-review.json`.

The subsequent [startup profiling and optimization](dcg-startup-performance.md)
retains this operator contract and materialization limits, adds an explicit native
check before queued publication, and records a matched 1,500-row comparison with
a 15.0% shorter startup window. Its evidence and review are recorded separately.

The [bounded snapshot proposal](dcg-bounded-snapshot-design.md) separates per-chunk
memory/acknowledgement limits from total attempt budgets in a future plan version.
It retains this operator contract and partial-start rejection; pending chunks do
not authorize restart. The current plan format and materialization limits remain
unchanged until implementation and the proposed failure tests pass.

The subsequent [bounded snapshot implementation](dcg-bounded-snapshot-validation.md)
adds a separate version-2 plan for chunk and total attempt budgets through test
admission. Its evidence and limitations are separate from these version-1 runs.
