# Native D/CG integration through the normal task

This adds a test-only native provider for the existing normal-task initialization
and recovery interfaces. `As400NativeNormalTaskProbeConnector.NativeTask` inherits
`As400ConnectorTask` startup, the real Debezium coordinator/snapshot phase, queue,
acknowledgement tracker, continuous runtime, dispatcher and shutdown. It does not
inject a queue or coordinator, create an independent initializer thread, or seed
F/W through a controller. Normal transactional streaming remains disabled; F/CR
PR #114 and all main sources are unchanged by this step.

The user authorized connection and mutations in isolated edgeqa fixtures. The
first access check found an expired AWS token. After the user refreshed the
session, a read-only preflight verified CURRENT_SERVER=AEQA. No existing auction
library is used. The runner requires an absent `DCNCdddd` library, creates its own
A/B/M/U tables and journal, retains every required receiver, and verifies cleanup.
Credentials remain on stdin or in memory-backed usql configuration, outside
Connect configuration and saved evidence.

## Provider and lifecycle

`As400NativeTaskSession` supplies both explicit first-start admission and recovery.
Its factory constructs the previously tested `As400ContinuousConnectSource` only
after configuration and fixture admission checks. Construction after stop cancels
the handles without opening a native connection. A resume offset's journal and
fingerprint are untrusted expectations: current native journal identity and
immutable binding history/policy must match before output. Recovery does not
substitute current table schemas for historical bindings.

First start requires explicit fresh-generation enrollment and raw empty-log checks
on all data, transaction, heartbeat, initialization and history topics. Enrollment
is durable and single-use even if the process fails before snapshot output. A
missing offset cannot reinitialize an existing generation. Pending initialization
offsets cannot resume streaming.

The fixture controller owns all writers, DDL, maintenance and receiver retention
for its fresh journal. One native JDBC transaction additionally holds verified
EXCLRD locks on A/B/M and uncaptured U. The provider rechecks the same connection,
transaction and controller epoch. **These locks and token files are not a general
fence for arbitrary shared journals or administrative/API producers.** No general
native session provider is enabled or advertised.

The provider acquires complete native bindings and materializes owned rows before
boundary selection. It reads every acquired column, with a 100-row limit per
fixture table, 64-KB row limit and 1-MB aggregate limit. It rejects query warnings,
unsupported values and tables outside the owned capture. The actual task builds
snapshot envelopes and keys through `As400NativeTableSchemas`; M includes decimal,
CCSID text, binary, nullable, large integer and floating-point columns.

The existing task/initializer protocol requires committed snapshot callbacks and
exact offset readback, immutable history persistence/recovery, and then committed
exclusive initial F=W=B. A fresh native journal observation is retained and
compared with the initial checkpoint. History and a reader continuation are
recovered before successful handoff releases the native snapshot locks. Ordinary
resume does not take those locks or repeat the snapshot.

Continuous reading reuses the strict native cut reader and guarded RPC/JDBC
lifecycle. No controller journal-cut requests are used. At the next read the test
provider records the exact durable F/W and previously accepted in-memory scan R;
this occurs only after the real task's acknowledgement/readback barrier. Raw
interval/page captures retain native object, receiver, job and thread identities.
The task's heartbeat carries conservative F/W, rather than a probe-invented
streaming marker. One partition per table preserves table-local ordering; there
is no cross-topic total-order claim.

Stop closes source admission and requests native cancellation. After admitted work
returns, cleanup rolls back/closes any snapshot connection **before** waiting for
native reader/JDBC drain. An incomplete generation stays enrolled/quarantined;
cleanup of physical handles does not emit a successful handoff marker or authorize
another snapshot. Stop request and completed cleanup have separate evidence.

The task's test poll wrapper retains actual SourceRecord identity and delegates
to the real queue. It can temporarily hold one polled record for deterministic
fault timing. Observing the actual queued INITIAL barrier supplies the
history-durable test marker; producer callbacks and durable acknowledgement remain
in the real task. Diagnostic headers choose a truncate send independently of idle
poll timing. This is test instrumentation, excluded from the distributed plugin.

## Local and native acceptance

Local cases cover unsupported worker/capture profiles, absent fixture/session/
enrollment authority, failed-enrollment reuse, stop before native construction,
construction returning after stop, current-journal identity mismatch, and cleanup
order without claiming handoff. Existing initialization, recovery, cancellation,
strict decoder and Kafka tests remain unchanged.

The native runner reuses the complete continuous-runtime scenario:

- Delayed snapshot and initial-checkpoint acknowledgements; crash after initial
  durable F/W and fresh task recovery without another snapshot.
- Exact mixed-column snapshot and update values, keys and legacy source metadata.
- Native D/CG subtype 8 TRUNCATE and subtype 3 optimized DELETE, commit and rollback.
- Unrelated A and M CDC while B's clear remains open; restart of that open cycle.
- Receiver rotation preserving the old replay floor while completed output
  advances on the successor; fresh-task replay across the receiver boundary.
- Legitimate Connect reassignment and actual broker fencing of a held old truncate
  send; unchanged output/offsets after the stale producer is released.
- Missing-offset rejection against retained output/history, no implicit baseline,
  separate stop/drain evidence, and native/Kafka projection agreement.

Run `mise exec java@temurin-21 -- tools/dcg-native-normal-task-probe NEW_EVIDENCE_DIRECTORY DCNCdddd`.
The wrapper builds test classes and dependencies. The native fixture manager and
older continuous/Kafka harnesses are reused without changing their assertions.
The local Kafka profile stays digest-pinned 4.3.1, EOS enabled/requested with poll
boundaries, no SMTs or tolerated errors, one task, one partition per topic and the
validated timeout profile. There is no exactly-once source support declaration:
a whole interrupted native output batch can replay a previously delivered prefix.
The fixture is bounded to 20 minutes/1,000 reads per task, with existing finite
receiver, pending-state and materialization limits.

The runner retains independent broker evidence after failures, stops only owned
transaction controllers/containers, checks ownership and exact native inventory,
requires no pending transactions or file/journal locks, and drops the fixture with
RESTRICT. Receiver inquiries require an exact receiver/library/originating-job
match. Schema, library and inquiry absence are independently queried afterward.
Cleanup failure is reported, never assumed successful.

Evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261108/native-normal-task/`.

## Verification results

`verify-01.log` records 2,453 tests across 62 classes, with no failures, errors or
skips, including 19 new native-provider admission/lifecycle cases. The existing
20 normal-task initialization and 29 recovery cases remain unchanged.
`package-01.log` records successful assembly plus formatter/import validation;
`git diff --check` passes.

`live-01/report.json` records all 47 functional checks passing against edgeqa's
fresh `DCNC7201` library and local Kafka. This run uses the actual task/coordinator
and native acquired schemas. Independent broker output/offset/history readbacks,
native raw pages/intervals, receiver settings, final source rows, worker logs,
stop/drain markers and artifact hashes are retained in `live-01/`. Native subtype
8 TRUNCATE and subtype 3 optimized DELETE both exercised commit and rollback;
unrelated A/M changes progressed while B's clear remained open. Cross-receiver
restart, stale-producer fencing and missing-offset rejection passed. This is
bounded validation of the documented profile, not a general exactly-once claim.

The original runner result remains **failed due to cleanup**, rather than being
rewritten as an uninterrupted successful run. All functional checks and owned
local container/network cleanup had passed when the first native cleanup query
encountered a sign-on communication failure. A read-only retry timed out. When
access returned, the unchanged inventory guard stopped teardown because the
system-managed fixture journal had gained receiver `QSQJRN0004`.

`cleanup-extra-receiver-proof/` separately establishes the additional receiver's
attachment to the owned journal, reciprocal predecessor/successor links,
unchanged original object creation metadata, the four owned journaled files and
absence of pending transactions. The additional receiver starts at sequence 1;
this evidence authorizes only fixture teardown, **not** CDC continuity across a
sequence reset or a claim that this run validated system restart. The cause of
the connection failure was not established.

`resume-cleanup.py` retains the exact bounded recovery procedure. After that
specific ownership check it reuses the unchanged cleanup helper, including live
inventory/transaction/lock checks, RESTRICT drops and exact inquiry-origin checks.
`cleanup-retry-02-result.json` and `cleanup-retry-02/` confirm successful cleanup:
independent queries returned no tables, schema, library or outstanding related
inquiries. The first failures and all cleanup attempts remain available.

`audit.json` and `turn.patch` distinguish this step from pre-existing branch work.
The staged native provider and all staged main classes match the verified build;
the packaged connector matches that build and excludes test admission/provider
classes. All main sources, pre-existing tests, transactional guards, protected
F/CR and original feature worktrees are unchanged. No commit, push or PR update
occurred.

## Independent review

Advisor was called exactly once after implementation, local checks, the native
functional run, recovered cleanup and the artifact/preservation audit. It
inspected the actual files and retained evidence and passed: "No actionable
issues found in inspected files and retained functional/cleanup evidence; tests
and live checks were not rerun." `advisor-review.json` retains the result. The
review does not extend the source-session, deployment or delivery guarantees
beyond the bounded profile described here.

## Remaining support boundary

This validates the actual task with a qualified, controller-owned isolated native
source. It does not qualify a general initialization fence for existing/shared
journals, public configuration/admission, arbitrary source layouts/DDL, multiple
tasks/partitions, other Kafka Connect modes, automatic receiver release, protected
restore, broker-loss durability, sustained load or downstream sink handling.
Operator-managed retention remains mandatory. Enabling normal transactional
streaming remains a separate change after the supported deployment contract is
accepted; this test entry is not that enablement.
