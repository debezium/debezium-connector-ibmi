# Local reader cancellation fix

The disconnected in-flight recursion found by the
[reader lifecycle harness](dcg-reader-lifecycle-validation.md) is fixed locally on
`dbz-3025-dcg-followup`. This changes the existing reader lifecycle in two production
files; it does not wire the D/CG transaction coordinator into streaming, remove
startup/ownership/retention checks, or change F/CR PR #114. Nothing was pushed and
there was no IBM i connection.

## Behavior and synchronization

`RetrieveJournal` registers the connection and server-job handle belonging to the
call immediately before `ServiceProgramCall.run()`. Cancellation copies that
captured connection and targets that job. It never calls the RPC connection
provider, so a disconnected transport cannot recursively enter reconnect/close.
The temporary cancellation connection is disconnected in `finally`, including
when `Job.end(0)` throws. Cleanup failures are logged; they do not prevent the
main reader connection's disconnect attempt.

A per-reader monitor serializes cancellation, completion and active-call
registration. The normal blocking `run()` does not hold that monitor, allowing
asynchronous cancellation. A cancellation attempt marks that call before using its
job handle. Repeated stops cannot retry it. The read remains registered until its
own `finally` clears it, on success, checked/runtime exception or error. A new call
cannot register over a still-active call. The monitor protects cancellation's use
of the job handle through cleanup of its temporary connection; native job identity
lifetime remains an external validation requirement.

`As400RpcConnection.close()` and `connection()` now serialize access to each
instance's connection. Close detaches and captures the old connection, cancels its
registered read and disconnects that captured connection in `finally`. Reconnect
cannot interleave and have the old close disconnect or null the new connection.
`isValid()` uses the same monitor. A partially initialized connection can also be
closed when no `RetrieveJournal` has been installed yet.

Lock ordering is RPC connection monitor, then active-read monitor. Read setup
obtains the RPC connection before registering under the active-read monitor;
cancellation never acquires the RPC connection monitor from inside that monitor.
Separate reader instances do not share these monitors, so an old cancellation
held in the fixture does not prevent the replacement from making progress.

Cancellation is attempted at most once per active call. If it fails, the main
connection is still disconnected; no automatic retry with an aging native job
handle is introduced. Configured transport timeouts are unchanged. The installed
JT400 21.0.4 copy constructor copies socket properties (local bytecode inspection
is saved with the evidence), but neither that inspection nor the mocks establish
an end-to-end native shutdown deadline. Test fault gates have finite deadlines.

## Local verification

The original recursion characterization is now a passing reconnect/close
regression for both plain and secure transports. There are 16 lifecycle cases:

- Existing real raw-page decoding, owned row values and source metadata, distinct
  scan R and durable F/W, inclusive restart after interrupted batch delivery,
  connected cancellation/late-data/late-error races and strict transport checks.
- A disconnected, held native read is cancelled once through real reconnect and
  repeated-close paths. Only the expected old job is targeted, the temporary
  connection is disconnected, and cancellation does not create a new main
  connection. The explicit requested reconnect does create its own new connection.
- Checked/runtime read failures and errors clear the active handle before a later
  close/read; no completed or failed call is cancelled afterward.
- Exceptions in `Job.end` and in temporary-connection disconnect still lead to
  both cleanup attempts. These assertions establish invocation/ownership in the
  fixture, not successful native termination after a real cleanup failure.
- Cancellation is held while read completion and a repeated close race with it.
  A separate replacement reader progresses through sequence 9; the old cleanup
  cannot touch that replacement's job or cursor.
- A concurrent reconnect on the same RPC instance waits for old cancellation and
  close to finish; the new connection stays valid after the old read returns.

The Kafka harness retains the original partition/publication/reassignment method
and every existing assertion. It now also disconnects the **simulated IBM i
transport** while the old native read is held, after the replacement has accepted
W=6. The original producer is released, its worker rejoins, and actual task stop
must complete cancellation before the native call returns late data or an error.
New per-generation assertions require exactly one main connection open/close,
exactly one cancellation/copy/disconnect for the old reader and none for the other
readers. No owner token or receipt authority is added to the candidate path.

The fixed candidate timeout profile, source-transaction configuration, independent
committed-output/offset observations, original producer-rejection assertion and
100-second reassignment deadline are unchanged. The old producer is rejected by
local close in this partition scenario; this does not replace or claim a new pass
for the separate unchanged suspended-worker broker-fencing scenario. The eventual
new task on the original worker must load the exact durable F/W at W=6 and finish
through W=9. Delivery remains whole-batch at-least-once.

## What remains open

This closes the reproduced **registered in-flight cancellation** defect locally.
It is not the complete C4 proof. The subsequent
[startup/stop boundary tests](dcg-reader-startup-stop-validation.md) cover discovery,
setup and entry into `run()` and expose post-close reconnect/new-call admission.
Those characterizations preserved this fix; production was not changed by that
test-only follow-up. The subsequent [terminal-stop implementation](dcg-terminal-stop-validation.md)
adds admission/draining without changing ordinary close/reconnect or the captured
job cancellation protocol. The full C4 gate remains blocked. Native
job authority, identity reuse/lifetime, cancellation behavior, disconnect effects,
release/PTF differences and timeout behavior still need separately authorized
disposable IBM i validation. The legacy lost-journal reset and oversize-entry skip
paths have not become safe recovery behavior. C5/C6 continuity/history and C7
initialization remain independent gates. Keep transactional streaming disabled and
all existing source-reader/startup protections until the full gates pass.

## Evidence and reproduction

Local evidence is under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-reader-cancellation/`:

- `verify-final.log`, `tests.json`: 748 executed unit tests passed, including the 16
  lifecycle cases and preserved stricter/false-dispatch regressions. Counts exclude
  stale XML reports from earlier builds.
- `package.log`: plugin assembly and formatter/import validation passed.
- `kafka-data/report.json`, `kafka-error/report.json`: separate late-data and
  late-error reports: 25 checks and owned cleanup passed in each, including the
  additional disconnected-transport/cleanup assertions.
- Each Kafka evidence directory contains the actual plugin, source/plugin hashes,
  configuration, reader/job traces, independent output/offset snapshots and owned
  cleanup results. Original reproductions and stricter probes are unchanged.
  The final same-instance reconnect unit test was added after these Kafka runs;
  it is not packaged in the probe. Final runtime/transport class bytes and executable
  probe source match both saved plugins. No runtime code changed after those runs.
- `before.json`, `change.patch`, `validation.json`: baseline, this turn's change,
  guard/protected-branch checks and final source/class/artifact provenance.

Build and run using the commands in the
[reader harness reproduction section](dcg-reader-lifecycle-validation.md#reproduce-and-inspect-evidence),
with new evidence directories. All tests run locally; no IBM i credentials are used.
