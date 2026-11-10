# Repeated local timeout-profile validation

This follows the [configuration-log investigation](dcg-config-catchup-investigation.md).
It tests a fixed candidate runtime profile across different network-fault timings.
It does not change production code, enable transactional streaming, remove receipt
checks, or change F/CR PR #114. There is no IBM i connection or live source mutation.

## Fixed profile and fault matrix

The candidate uses the same digest-pinned Kafka/Connect 4.3.1 image, synthetic
source, actual connector task/dispatcher, separate single-partition output topics,
and whole-batch at-least-once contract as the ownership experiment. Its four worker
overrides are fixed throughout the run:

| Worker property | Value |
| --- | --- |
| `request.timeout.ms` | `5000` |
| `default.api.timeout.ms` | `10000` |
| `worker.sync.timeout.ms` | `20000` |
| `rebalance.timeout.ms` | `30000` |

As in the earlier aligned comparison, the idempotent administrative stop's HTTP
deadline is 60 seconds, with the original bounded explicit-response retry policy.
This is a test transport setting. No publication failure is retried, and the
existing 100-second task reassignment deadline is unchanged.

Six timing combinations are each run twice on fresh brokers:

| Additional isolation hold | Delay before the second worker failure |
| --- | --- |
| 0 seconds | 0 seconds |
| 0 seconds | 5 seconds |
| 0 seconds | 25 seconds |
| 12 seconds | 0 seconds |
| 12 seconds | 5 seconds |
| 12 seconds | 25 seconds |

The isolation hold is inserted after the replacement reaches W=6, while the old
send remains blocked and its worker remains disconnected. Only afterward does
the original method release the send and reconnect the worker. The second delay
begins after the original stale-send rejection and unchanged-output/checkpoint
assertions, immediately before killing the replacement. It is a fixed delay,
not a readiness or authorization gate. All other fault sequencing is inherited.

These are **additional holds**, not total outage durations or precise placement
against a Kafka request's deadline. Startup, takeover and observation also take
time. Each report uses a monotonic clock to record deliberate delay durations,
the elapsed interval between completion of the disconnect and reconnect commands,
and the wait for a new prepared task after the replacement-kill command returns.
The `isolation_seconds` field measures that command-completion interval; it is not
a packet-level outage measurement. UTC timestamps help correlate worker logs.
The prepared-task observation does not itself prove publication: the original
remaining assertions must also pass. These measurements are not an end-to-end
failure-to-serving latency or a recovery SLA.

The evidence runner shuffles the six combinations within each repetition using
fixed seeds 3026 and 3027 and runs at most two cases concurrently. Every case has
its own broker, internal Docker network and worker containers, without host ports.
This is one host/load setting, not a load or capacity benchmark. Failed cases are
retained and are never automatically retried. An additional unmodified original
configuration run serves as a control; its result is reported separately from the
twelve candidate cases and cannot be counted as a candidate pass.

## Assertions and pass criteria

`tools/dcg-timeout-profile-probe.py` subclasses the existing aligned comparison.
It invokes the original `fencing(False)` method; it does not copy or weaken it.
The original reproduction, comparison runner and Java fixtures remain unchanged.

Each successful case must pass all existing checks for stale-send rejection,
unchanged accepted output/F/W, legitimate reassignment to a new task on the old
worker, exact persisted W=6 reload, later output through W=9, row/source metadata,
transaction metadata, heartbeat and immutable offsets, synthetic projection,
monotonic independently read committed W history, and normal task shutdown.
The candidate artifact must exclude the custom ownership/receipt classes. Fault
gates must finish within their existing bounds, and no custom authorization files
may exist. One additional check verifies that both requested timing interventions
ran in order and for at least their requested duration: 17 checks per passing case.

Failure handling uses the existing fresh observer JVM/container to inspect broker
transactions, config/offset/output topic ends and records under both isolation
levels. Its comparison baseline is the replacement's accepted W=6 state before
the second failure. If a case progresses legitimately to W=9 and later fails
administrative shutdown, unequal W=6 comparison flags must not be described as
stale output or checkpoint regression. The recorded task/publication checks and
final Kafka records distinguish these outcomes. Missing/incomplete diagnostic
evidence must remain a limitation rather than a passing assertion.

## Reproduction and evidence

Build the existing test classes and classpath using the ownership-probe launcher,
or the same Maven build commands documented there. Then run one case, with a new
destination directory:

```sh
python3 -B tools/dcg-timeout-profile-probe.py /absolute/new/evidence \
  --isolation-hold-seconds 12 --second-fault-delay-seconds 25
```

The two arguments are restricted to the matrix values. A new destination is
required so previous evidence cannot be overwritten. The original reproduction
remains available through `tools/dcg-ownership-probe ... partition`.

Evidence for this validation is in
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-timeout-profile`:

* `before.json`: hashes of all existing worktree files and protected worktree heads.
* `run-matrix.py` and `plan.json`: exact orchestration, order, repetitions and concurrency.
* Per-case directories: source/artifact hashes, exact properties, commands and logs,
  real broker offsets/output, task events, timing measurements and cleanup results.
* `results.json` and `matrix-summary.json`: every attempted case, including failures.
* Final validation inventory: source preservation, unchanged original assertions,
  artifact provenance, complete case accounting and owned-resource cleanup.

## Observed results (2026-11-06)

All **12 candidate runs passed all 17 checks** (204 recorded passing checks).
Each combination completed twice, using the fixed profile and unchanged
100-second reassignment bound. The new task loaded the exact accepted W=6
checkpoint, published the remaining work through W=9, preserved the output and
source-metadata assertions, and completed normal stop. No fault gate expired.

| Extra isolation hold | Second-failure delay | Complete runs | Waits for the prepared task |
| --- | --- | --- | --- |
| 0 seconds | 0 seconds | 2/2 | 16.0 and 15.8 seconds |
| 0 seconds | 5 seconds | 2/2 | 18.8 and 17.3 seconds |
| 0 seconds | 25 seconds | 2/2 | 15.5 and 14.3 seconds |
| 12 seconds | 0 seconds | 2/2 | 18.0 and 20.5 seconds |
| 12 seconds | 5 seconds | 2/2 | 19.0 and 16.0 seconds |
| 12 seconds | 25 seconds | 2/2 | 15.3 and 16.8 seconds |

The waits ranged from 14.264 to 20.531 seconds after the kill command returned.
The saved local prepared-marker file timestamps also follow that command's
completion in all twelve cases, excluding an already-prepared task from these
observations. This overall wait includes coordination and task preparation;
`worker.sync.timeout.ms` is a budget for a configuration refresh operation, not
the total reassignment duration.

Every candidate encountered exactly one of Kafka's explicitly retryable
configuration-writer responses during administrative stop. The existing bounded
stop retry succeeded in every case. No source publication or failed safety
assertion was retried. A passing result therefore includes that existing
administrative recovery policy; it does not mean every REST request succeeded
on its first attempt.

The **unmodified control failed** its 100-second new-assignment wait, reproducing
the original problem. Its worker configuration retained the original 3-second
sync budget. Independent broker evidence after failure found complete config
history through offset 12 in both isolation modes, both inspected transactions
`Empty`, unchanged committed output and the exact accepted F=W=6 checkpoint.
The control retains its nonzero exit and failed report; it is not counted among
the twelve successful candidate runs.

The test-class build and Python import/CLI/syntax checks passed. All 233 files
that existed before this task, including production code, Java fixtures, original
reproduction, prior comparisons and stricter receipt tests, remain hash-identical.
Only this report and the new timing runner were added. The distributable plugin
is byte-identical to the previously verified artifact; all ten packaged jars
exclude the test fixtures. Current source and staged artifact hashes match every
case. F/CR and the original feature worktrees are unchanged. All thirteen owned
Docker environments were removed. The unchanged Java unit suite was not rerun;
its prior 732-test result is recorded in the preceding investigation's evidence.

## Interpretation limits

This passing matrix establishes finite local evidence for this profile
under these fault schedules. It does not make the values universal defaults,
prove a recovery SLA, establish source exactly-once delivery, or close the native
startup/receiver-history/reader-lifetime gates. Two repetitions per combination
cannot establish a reliability probability or exhaust asynchronous interleavings.
No IBM i transaction, rollback or live sink behavior is exercised. Transactional
streaming remains disabled regardless of the matrix result.
