# Deterministic startup/stop boundary coverage

Historical characterization: the separately authorized
[terminal-stop implementation](dcg-terminal-stop-validation.md) now closes the
local admission/drain gap. This report preserves the original observations and
evidence; ordinary `close()` remains reconnectable.

The new test-only cases expose a remaining C4 admission gap: after a stopped
context and two successful `close()` calls, discovery/setup can continue and
invoke a new `ServiceProgramCall.run()`. Discovery can reopen the RPC connection.
The completed [in-flight cancellation fix](dcg-reader-cancellation-validation.md)
is preserved. No production code, streaming guard, existing Kafka scenario or
stricter test was changed, and no IBM i connection or push occurred.

A production lifecycle change is required to establish a stronger terminal-stop
contract. It is described below and has **not** been made. The existing `close()`
is also used for ordinary reconnect; these results do not establish a regression
in the cancellation fix or imply that `close()` already promised terminal stop.

## Actual paths and stop points

`As400ReaderStartupStopTest` drives the real RPC loop, pagination, `RetrieveJournal`
setup/registration/cancellation, raw journal decoding, normalizer and replay
coordinator. `As400ReaderFixture` adds bounded test callbacks at controlled metadata
and JT400 method boundaries. Its existing strict constructor/method interception
continues to prevent live AS400/Job construction or undeclared transport actions.
No production test seam was required.

Each point is exercised with plain and secure transport and a late data/error
outcome: five points times two transports times two outcomes, for 20 cases.
The late error is injected into the eventual simulated transport request; these
are not separate failure injections into every discovery/setup API.

| Point | Where execution pauses | What close sees | Observed continuation |
| --- | --- | --- | --- |
| `CURRENT_RECEIVER` | Current receiver metadata lookup, before it returns | No registered call | RPC reconnect and new `run()` invocation after close |
| `RECEIVER_LIST` | Initial receiver-list lookup, before it returns | No registered call | RPC reconnect and new `run()` invocation after close |
| `SERVER_JOB` | `getServerJob()` entry, before returning the controlled handle | No registered call | Job logging call and new `run()` invocation after close, using the disconnected captured transport |
| `CONFIGURED_CALL` | Final `setReturnValueFormat` call before registration | No registered call | Registration and new `run()` invocation after close |
| `REGISTERED_RUN` | Mocked `run()` entry, after registration but before simulated transport work | Registered call | Cancellation targets the old job once; the transport model is subsequently released |

The last two points bracket the registration/start handoff. The `run()`-entry
point is **not** an instruction-level breakpoint between assignment of
`activeRead` and the JVM's method invocation. In that case `run()` has already
been invoked before close. Releasing its adversarial transport model afterward
does not prove the real driver sends a new request after cancellation. The other
four points do observe a new Java `run()` invocation after close. Actual native
execution, implicit JT400 reconnect/rejection and cancellation effects remain
unverified.

The fixture observes `activeRead` and `cancellationRequested` under the real
implementation's monitor through reflection. It does not alter either value or
install an ownership/startup proof. Latches control the interleavings; there are
no timing sleeps or polling-based assumptions about reaching a stop point.
Every wait is bounded, threads are joined, and final fixture cleanup follows.

## F/W and replacement isolation

A seed reader first decodes the real raw fixtures for transaction start at 2,
truncate at 3 and inserts at 4 and 5. The real coordinator emits a checkpoint with
inclusive F=2 and W=1 while that transaction is open. A local prefix-acknowledgement
store accepts it, and JSON round trips transfer only that saved checkpoint into
independent old and replacement readers/coordinators.

The receiver-list case pauses on a fresh reader's first entry at 2. The other
cases restore the open transaction through scan R=5 and pause while reading its
commit at 6. Before close, the old context changes to stopped, matching the
existing task-stop order. After close returns, the replacement reconstructs the
transaction from F and its local store accepts W=6 while the old reader remains
held. The old gate is then released, followed by checks that the replacement's
operations, record count, checkpoint and accepted F/W are unchanged. Replacement
processing subsequently reaches accepted W=9 and the expected table projection.

Results in all 20 cases:

- No additional old consumer invocation and no old output record is enqueued.
- Old saved inclusive F=2/W=1 remains exactly unchanged. It is not replaced with
  the mutable RPC scan position.
- With late data, the hot reader's RPC R can advance from 5 to 6 even though its
  coordinator remains at 5. In the cold case the first entry becomes processed
  at R=2 without reaching the coordinator. With a transport error the old RPC
  position remains at its prior value.
- The replacement's accepted checkpoint at 6 is unchanged by old completion, and
  the replacement then progresses to 9. Old cleanup never targets its job or
  connection. Source offsets emitted by the coordinator contain no scan-only field.
- Active-call state clears on both outcomes. The registered-entry case retains
  the completed fix's one cancellation and temporary-connection cleanup behavior.

The prefix store is an explicit **unit-test model**, not Kafka durable storage.
Its payload markers are not the real dispatcher envelopes. These cases prove
local control flow, coordinator offset stamping and instance isolation under the
stopped-context contract. They do not prove broker fencing or native identity
lifetime. The original Kafka reader scenarios are rerun separately with the
updated shared fixture and retain all original publication, metadata, F/W,
reassignment and cleanup assertions. They do not gain the five new startup fault
points merely because the fixture supports them.

## C4 result and required production work

Of the 20 cases, 16 observe new Java `run()` calls after close; 8 also observe
explicit RPC reconnect. Four cover a call already entered before close and retain
their separate transport-model limitation. A green characterization test means
these observations and isolation assertions were reproduced. It does **not** pass
the no-new-source-work-after-terminal-stop obligation. C4 remains blocked, and
all current source-reader protections and transactional streaming guards remain.

The required production change is a lifecycle admission/stop protocol spanning
receiver discovery, call setup, registration and transport startup. It must
invalidate an old attempt before terminal stop returns, prevent stale setup from
reconnecting or registering afterward, and define how an admitted but not yet
started native call is completed/cancelled. Normal reconnect must remain distinct
from terminal task stop; simply making every `close()` permanently terminal would
also break its current reconnect use.

That change needs a defined start/stop handoff and cleanup contract, not just a
new `isRunning()` check: a check by itself can race the next native action. Once
implemented separately, the relevant characterizations should require no new
post-stop invocation/reconnect and preserve the current cancellation/F/W tests.
Native job lifetime, authority, driver behavior and C5/C6/C7 source/startup gates
still need their own evidence. No custom dependency is removed by these tests.

## Evidence and reproduction

Local evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-reader-startup-stop/`.

- `verify-final.log`, `tests.json`: all 768 executed unit tests passed, including 20 new
  startup/stop characterizations and the 16 existing cancellation/lifecycle cases.
- `cases/*.json`: gate/cancellation state, ordered old/replacement operation traces,
  calls after close, old R/coordinator progress, unchanged F/W, accepted replacement
  checkpoints at 6 and 9, and final cleanup. `cases-initial/` and earlier logs are
  retained development evidence; use `cases/` for the final classification.
- `kafka-data/report.json`, `kafka-error/report.json`: all 25 unchanged Kafka
  scenario checks and owned cleanup passed in each run, evaluated separately from
  the new unit fault points.
- `package.log`: assembly plus formatter/import validation. Test hooks/classes and
  instrumentation are excluded from the production plugin.
- `before.json`, `change.patch`, `validation.json`: this task's file baseline,
  isolated delta, unchanged production/guard/protected-branch checks, and final
  source/plugin/class provenance. The existing stricter tests/probes are unchanged.

To execute and optionally save the new per-case traces, choose a new local evidence
folder and run from `dbz-3025-dcg-followup`:

```sh
mise exec java@temurin-21 -- ./mvnw -B -ntp verify -DskipITs -Ddcg.startup.stop.evidence=/absolute/new/evidence/cases
```

Without `dcg.startup.stop.evidence`, these tests write no extra case files. Run the
separate Kafka regression scenarios with the commands in the
[reader harness reproduction section](dcg-reader-lifecycle-validation.md#reproduce-and-inspect-evidence).
