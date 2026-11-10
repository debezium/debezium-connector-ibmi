# D/CG first-start profiling and bounded optimization

This follows the [planned first-start window](dcg-planned-first-start.md). It
changes the admitted task path only. Public transactional streaming remains
disabled, the native provider remains restricted to controller-owned fixtures,
and snapshot row/byte limits and operator retention requirements are unchanged.

## Measurement

The planned native test provider now records inclusive monotonic timings and call
counts for held-session checks, the driver's `isValid(2)` operation, native
snapshot preparation, enrollment, checkpoint acceptance, handoff, poll and Kafka
callbacks. Wall-clock milestones identify materialization, first returned record,
first acknowledged record, and acknowledged baseline/initial barriers. Evidence
is written atomically to each generation's `startup-profile.json`; counters are
test-only and are excluded from the connector distribution.

Nested and concurrent timings overlap: their totals must not be added to obtain
wall time. The materialization-to-first-returned-record interval includes owned
envelope construction and subsequent native boundary checks. The first returned
record through baseline acknowledgement includes queue delivery, Kafka work and
callback validation; it is not an isolated broker benchmark. The whole operator
window remains the relevant pause measurement.

The instrumented baseline keeps the previous checks unchanged. A separate fresh
fixture runs the optimized task with the same 1,500 rows, mixed-column values,
128-record poll limit, queue, broker image, worker configuration and profiler.
All native runs are sequential to avoid the cross-library metadata warning
observed during the previous experiment. The original stricter Kafka initialization
harness runs separately without IBM i access.

## Changes and safety argument

Kafka 4.3.1's `ExactlyOnceWorkerSourceTask` calls the source task's `commit()` even
when it has neither a transaction nor offsets to flush. Its record callbacks are
delivered after a successful producer transaction, followed by the task commit.
See the [pinned Kafka implementation](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ExactlyOnceWorkerSourceTask.java).
The implementation was inspected directly after the documentation lookup returned
the more general connector-development guidance.

The measured driver is JTOpen 21.0.4. Its `isValid(2)` sends a native test-connection
request and creates a timeout thread; it is not a local flag check. The matching
[published source archive](https://repo.maven.apache.org/maven2/net/sf/jt400/jt400/21.0.4/jt400-21.0.4-sources.jar)
is retained with the evidence.

Two sources of avoidable native work are removed:

- A commit callback with no acknowledged pending records performs local admission,
  stop, deadline, revocation and profile checks, but does not probe the native
  session or modify acknowledgement state. Registered-but-unacknowledged records
  remain pending. A callback racing the empty observation remains pending for a
  later fully validated commit. The existing initialization handoff mutex still
  serializes even empty callbacks with session release.
- Building envelopes from already owned snapshot rows performs local admission
  checks for every row. Full native-session and epoch validation surrounds this
  construction. Nothing is queued during it. Native reads, bounds, every enqueue,
  record acknowledgement, advancing commit, checkpoint acceptance and handoff
  retain their native checks.

Before a nonempty queue result leaves the actual task, it now revalidates the
held native session. The native test adapter uses that same method when releasing
a deliberately retained poll result. This makes the publication boundary explicit
while avoiding incidental native polling from empty commits. No successful native
check is cached, and no time-based grace period is introduced.

The original handoff test caught the first implementation returning an empty
commit during incomplete session release. The handoff mutex was restored on that
path; the test and its assertion were preserved. Five new actual-task tests cover
empty commits during blocked preparation, absence of fabricated acknowledgement,
native loss before advancing commit, queued/retained publication after session
loss, and loss after materialization before snapshot exposure. Existing timeout,
revocation, restart, missing-offset, ordering and lifecycle tests remain in place.

## Evidence and limits

Evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-startup-performance-20261108/`.

The evidence retains the initial local handoff-test failure, its correction,
per-generation profiles, native raw records and broker output/offset/history,
source and artifact hashes, and independent fixture cleanup. These are bounded
single-host Kafka/edgeqa experiments, not a throughput guarantee or general
shared-journal qualification. The 9,000-row/16 MB ceiling remains; larger snapshots
still need a separate bounded-memory design. Performance improvements alone do
not authorize public transactional streaming.

## Completed results

The matched 1,500-row comparison reduced the operator window from 288,210 ms
(4m 48.210s) to 244,974 ms (4m 4.974s): 43,236 ms, or 15.0%, shorter. Native
connection-validation calls fell from 12,004 to 7,513. This is a measured
improvement, not evidence that a multi-minute pause on a small snapshot scales to
large auction databases. Per-record enqueue/acknowledgement validation and the
native metadata/boundary rechecks still account for substantial work.

The intermediate candidate completed in 236,038 ms. It retained a duplicate check
when the test adapter immediately returned a fresh poll result; the final adapter
only repeats the check for deliberately retained output. Poll batch formation and
runtime timings vary, so the final comparison uses the final candidate, not the
fastest observed attempt. All three success runs verified keys, native mixed
values, unrelated-table progress during an open clear, ordinary restart without
another window, and truncate-before-reload ordering.

The final timeout, revocation and partial-snapshot crash runs passed. Expiry and
revocation published no data and installed no initialized checkpoint; replacement
workers rejected those enrolled attempts. The crash comparison used independent
broker readback after the original worker died and proved the replacement added
no output or checkpoint advancement. The final native cases passed 25 assertions;
the baseline and intermediate success runs passed another 18. All six native
fixtures and local resources were cleaned up, with retained independent table,
schema, library and inquiry absence results.

Local verification passed 2,484 tests across 64 classes, including the five new
cases. The unchanged Kafka first-start harness passed all 29 checks. Packaging,
formatter/import validation and whitespace checks passed. `audit.json` verifies
the baseline main classes against the saved pre-change plugin, candidate main
classes against the current package, protected worktrees and unchanged safety
files. F/CR PR #114 remains unchanged; nothing was pushed.

One auxiliary profiler-only correction followed native staging: its `native-drained`
timing event is now emitted only after successful close, rather than in `finally`.
The saved `profile-before-drain-marker-fix.java` records the staged instrumentation;
`profile-drain-check.log` records the local recompile and affected tests. Startup
timing code and all main classes are unchanged by that correction. Native drain
assertions use the original canonical drain marker and independent cleanup queries,
not this auxiliary event. The audit reports the instrumentation difference explicitly.

`validation-summary.json`, `benchmark.json`, `phase-benchmark.json` and
`native-call-table.json` contain the results and their evidence paths. Inclusive
method timings and labelled wall intervals remain separate; no missing native
proof, offset authority or public enablement is inferred from a faster run.

Independent Advisor review passed after inspecting actual target files and retained
evidence, with no actionable findings. Advisor did not rerun tests or live checks
and explicitly noted the disclosed profiler-only correction lacks a live rerun.
The exact verdict is retained in `advisor-review.json`.

The proposed next step is [bounded snapshot delivery](dcg-bounded-snapshot-design.md):
retire pending chunks independently, keep B after all native snapshot cursors have
closed, and validate native chunk/publication/commit boundaries. This is a design
only; the existing row/byte limits and runtime checks have not changed.

The subsequent [bounded snapshot implementation](dcg-bounded-snapshot-validation.md)
records the new test-admitted chunk path and its validation. The measurements above
remain the pre-chunk comparison; version-1 materialization limits are preserved.
