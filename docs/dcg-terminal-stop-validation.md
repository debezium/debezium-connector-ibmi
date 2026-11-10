# Terminal stop and RPC admission

The separately authorized production follow-up closes the local admission gap
recorded by the [startup/stop characterizations](dcg-reader-startup-stop-validation.md).
`As400RpcConnection.stop()` permanently rejects new operations, cancels the
captured active read, drains admitted operations and disconnects their captured
transports. Position and receiver-validation metadata lookups also clean their
captured transport before releasing admission, even if ordinary close detached
it or replaced the RPC connection. Late metadata results are rejected after stop
is requested. `close()` remains reconnectable. Task `doStop()` now invokes terminal
RPC stop. The completed captured-job cancellation fix remains intact.

Transactional streaming stays disabled. No startup proof, ownership/receipt gate,
F/W recovery rule, source protection requirement or F/CR PR #114 changes. No push
or IBM i connection occurred. All changes and evidence are local.

## Contract and handoff

`JournalReadLifecycle` is shared by the RPC connection, journal reader and
pagination. Its short monitor serializes admission against the stop request;
native calls execute outside that monitor. Leases cover the whole RPC read,
receiver discovery/setup and registered transport call. Metadata/connection entry
points also reject admission after terminal stop. Stage checks reject obsolete
discovery/setup continuations before their next operation.

Stop has two distinct moments:

1. **Requested:** admission closes permanently. Cancellation targets only the
   captured active read. Work already admitted may still execute or return,
   including a call between its admission and Java `run()` invocation.
2. **Completed:** all admitted operations have left their leases, including
   exceptional exits and final cleanup. No new reader work can start afterward.

The drain barrier closes the check/action race; flag checks alone would not.
Registration checks stop under the existing active-read monitor, and transport
admission follows registration. Stop either rejects that transport admission or
cancels/drains it. A second stop also waits for draining. Interruption while
waiting for the drain cannot report incomplete stop as successful; the thread's
interrupt status is restored afterward. The stopping thread must not hold a reader lease itself.

Native calls, cancellation and draining do not hold the lifecycle monitor across
network I/O. The existing cancellation monitor and lock order are preserved.
Normal connection loss still uses `close()` followed by `connection()` on the
same instance. Terminal stop requires a new RPC instance for future work.

An already-admitted driver operation could reconnect its captured transport while
stop is pending. Reader `finally` blocks release those captured transports before
their leases close, even when an ordinary close already detached the RPC field.
Stop performs a final disconnect of its captured connection as well. Cleanup
failures retain logging; local completion does not prove a native job terminated.

There is **no bounded native shutdown guarantee**. If a driver operation or
cancellation never returns, stop remains pending rather than falsely reporting
quiescence. Debezium's base task invokes `doStop()` after coordinator shutdown
(including its failure/finally path); this patch does not move cancellation ahead
of that framework stage. The exact worker shutdown/deadline behavior remains an
integration/native validation obligation, and the full C4 gate stays blocked.
The surrounding streaming retry/JDBC lifecycle is outside these RPC boundary
cases; it needs stopped-context integration coverage before broader source
quiescence can be claimed.

## Deterministic coverage

The real RPC loop, pagination, registration, cancellation, raw decoder,
normalizer and replay coordinator execute against strict controlled JT400 calls.
The six stop boundaries are current-receiver lookup, receiver-list lookup,
server-job lookup, final call setup, registration before transport admission,
and entry into `run()`. Each has plain/secure and late-data/error variants:
24 cases. Before run starts, the planned transport outcome is intentionally
unreachable after stop. The already-entered-run cases exercise both outcomes.

The exact pre-admission boundary is supplied by spying on the real lifecycle
entry method after registration. No production callback or test-only seam is
introduced. The separate mocked `run()`-entry boundary still occurs after Java
method invocation; it is not proof of native transport startup timing.

In every boundary case:

- Stop remains pending while the controlled old operation is held. An independent
  replacement reconstructs from saved inclusive F=2/W=1 and accepts W=6 before
  the old operation is released, then progresses to W=9.
- No new `run()` invocation, RPC reconnect or subsequent metadata/job-logging
  stage starts after the controlled stop request. An already-entered simulated
  run may finish while stop drains. After completed stop, reconnect and read
  attempts fail without any new transport operation.
- The old consumer emits nothing, its scan R and saved F/W remain unchanged,
  and its late completion does not change replacement operations, checkpoint,
  output count or projection. Emitted source offsets contain no scan-only field.
- Registered calls retain exactly-once cancellation and active-handle invalidation.
  Repeated stop and an interrupted stopping thread cannot bypass the drain.

Six additional cases cover task `doStop()` before first read, setup failure
releasing its lease, and an admitted driver model reconnecting after ordinary
close and throwing `Error`. Final cleanup leaves every controlled reader transport
disconnected, including a connection absent from the RPC field at terminal stop.
Eight metadata regressions added after independent review pause current-position
and receiver-validation lookups, detach their connections, request stop and then
resume with a modeled transport reconnect and data/error. They cover plain/secure
connections and both null/replacement RPC fields at stop, verify all captured
transports are disconnected, reject late results and reject subsequent metadata
work. The original review found these paths lacked captured-transport cleanup;
the correction performs cleanup in `finally` before releasing each lease.

Test waits are bounded; executor threads are joined and daemonized so a failure
cannot strand the test JVM. The 16 earlier cancellation/reconnect tests retain
their existing assertions. The false-dispatch test only initializes the new
lifecycle field in its construction-bypassing mock; its assertions are unchanged.

Saved F/W in these new cases uses an explicit local prefix-acknowledgement model,
not Kafka durable storage. The existing Kafka late-data/error scenarios are
separate regressions with actual task/dispatcher, committed records and offsets.
They retain ordinary close/reconnect and all original assertions; they do not
exercise the new terminal-stop boundary matrix or certify full task shutdown.
Native job identity/lifetime, authority, driver/PTF behavior and C5/C6/C7 remain
unverified; no source-reader barrier or custom dependency is removed.

## Evidence and reproduction

Evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-terminal-stop/`.

- `verify-final.log`, `tests.json`: 786 executed unit tests passed, including
  38 terminal-stop cases, the 16 previous lifecycle cases and stricter recovery,
  receipt, missing-offset and false-dispatch regressions.
- `cases/*.json`: all 24 final boundary traces, admission/cancellation observations,
  old R/F/W, replacement progress and absence of operations after completed stop.
- `package.log`: assembly and formatter/import validation.
- `kafka-data-final/report.json`, `kafka-error-final/report.json`: both unchanged Kafka
  regressions passed all 25 checks and owned-resource cleanup. Final runtime
  class bytes match both saved plugins; these are ordinary-close scenarios,
  not the new terminal-stop matrix.
- `before.json`, `change.patch`, `validation.json`: task baseline, isolated delta,
  protected worktree/guard checks and final source/plugin provenance.
- `advisor-review.txt`: actual-file review found the metadata cleanup gap;
  `advisor-resolution.md` records its correction and subsequent verification.
  Advisor was called once; no second review pass is claimed.
- The earlier `kafka-data/`, `kafka-error/` and `*-before-advisor.log` artifacts
  preserve the successful pre-correction evidence. Use the final reports above
  for final-source provenance.
- Earlier targeted logs are retained development evidence. The first runs caught
  fixture synchronization problems: a spy gate held a synchronized method monitor,
  and an initial connection close signalled the stop latch. The final fixture
  gates outside the admission monitor and signals only terminal close.

Run from `dbz-3025-dcg-followup`, choosing fresh evidence directories:

```sh
mise exec java@temurin-21 -- ./mvnw -B -ntp verify -DskipITs -Ddcg.startup.stop.evidence=/absolute/new/evidence/cases
mise exec java@temurin-21 -- ./mvnw -B -ntp package -Passembly -DskipTests -DskipITs -Dformat.formatter.goal=validate -Dformat.imports.goal=check
python3 -B tools/dcg-reader-lifecycle-probe.py /absolute/new/evidence/kafka-data late-data
python3 -B tools/dcg-reader-lifecycle-probe.py /absolute/new/evidence/kafka-error late-error
```

The probe classpath must already exist as described in the
[reader harness reproduction instructions](dcg-reader-lifecycle-validation.md#reproduce-and-inspect-evidence).
No IBM i credentials or live source are used.

## Subsequent task integration

The [task-stop follow-up](dcg-task-stop-validation.md) adds stopped-context
retry/JDBC protection and real coordinator/task Kafka integration. It also records
a separately authorized isolated edgeqa smoke test. The no-live-test statements
and results above describe this earlier RPC-boundary task; they are preserved as
historical evidence. Native fault/cancellation and shutdown deadlines remain open.
