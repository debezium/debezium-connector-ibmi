# Local incremental reader lifecycle validation

This is the historical harness-only validation record. The
[cancellation follow-up](dcg-reader-cancellation-validation.md) fixes the local
recursion and handle/temporary-connection cleanup findings below. Original test
counts, unchanged-production statements and hashes here refer to the pre-fix run;
they are retained as evidence, not the current implementation status.

This is the test-only local portion of [C4](dcg-startup-acceptance.md#next-local-implementation-checklist-c4).
The harness is implemented without a production test seam. It adds local coverage,
**not approval to remove the source-reader barrier**. A disconnected cancellation
case exposes legacy recursion. Transactional streaming remains disabled, and no
production source, existing probe, stricter test, or F/CR branch was changed.
There was no IBM i connection.

## Paths actually exercised

`As400ReaderFixture` bypasses only `As400RpcConnection` construction, which would
perform live discovery. It injects a real `RetrieveJournal`, real pagination and
raw RJNE0200 responses. The actual RPC read loop, binary decoders, connection,
close and cancellation methods execute. Only receiver metadata, JT400 transport,
server jobs and the watchdog are controlled. A private diagnostic rate limiter
suppresses periodic live diagnostic queries; it does not replace the reader loop.
Reflection and Mockito constructor interception stay in test scope.

The transport refuses undeclared methods, constructors, services, procedures,
job targets, alignment and return formats. AS400/SecureAS400 and Job construction
are intercepted on each calling thread; dummy configuration uses
`no-live-ibmi.invalid`. The Docker network is internal and has no host ports.
The production plugin archives exclude the new test classes and Mockito libraries.

The native-operation inventory includes:

- Receiver metadata lookup, socket setup and COMMAND service connection.
- `QjoRetrieveJournalEntries` through a controlled `ServiceProgramCall`.
- Server-job `setLoggingLevel(0)` during retrieval.
- Cancellation through a separate copied AS400/SecureAS400 connection and
  `Job.end(0)`, with the old reader's simulated job identity.
- Main-connection disconnect, close and reconnect.

The job calls mean this lifecycle must not be described as entirely read-only.
A mocked `Job.end` proves which test handle was selected; it cannot prove actual
IBM i job identity lifetime, authority, termination or connection cleanup effects.

## Unit and Kafka coverage

`As400ReaderLifecycleTest` has eight executed cases. They cover plain and secure
transport construction, raw-page source metadata and owned row values, independent
old/replacement readers, cancellation/reconnect/repeated stop with a held read,
late data, late errors, inclusive replay after interrupted batch delivery, strict
transport rejection, and the bounded legacy disconnected-cancellation failure.
The negative legacy case passes as a **characterization of a blocker**, not as a
successful safe-close test.

`As400ReaderLifecycleProbeConnector` keeps the actual connector task, BaseSourceTask
poll/commit/stop lifecycle, dispatcher, emitter, transaction metadata and heartbeat.
Its separate reader thread retrieves one raw fixture page at a time and feeds the
real replay coordinator. The dispatcher is constructed on that thread to preserve
its existing single-thread requirement. Startup identities, receiver metadata,
schema and source events remain synthetic. The candidate publication path has no
custom owner or receipt authority; missing offsets still fail through the existing
resume-only validation. `ExactlyOnceSupport` remains `UNSUPPORTED`.

`tools/dcg-reader-lifecycle-probe.py` subclasses the aligned timeout comparison and
calls the existing `fencing(False)` implementation without copying or weakening
its Kafka assertions. Both outcome variants use the same fixed four worker timeout
properties, 60-second administrative stop HTTP deadline, bounded retry policy,
100-second reassignment deadline and digest-pinned Kafka/Connect 4.3.1 image as the
[timeout matrix](dcg-timeout-profile-validation.md). The delivery contract remains
whole-batch at-least-once, with one partition per data/transaction/heartbeat topic.

The old task holds a native read at sequence 7 after preparing the batch ending at
6; its producer separately holds the truncate send. Connect assigns a replacement,
which loads the exact durable F/W and commits through W=6. The controller then
releases the old send and restores its network, waits for the **actual task close**
to cancel the old simulated job, and releases the native read as data or an error.
Connect can defer task cleanup while the producer call is blocked. The harness
observes that ordering instead of manufacturing a task-stop or ownership token.
The replacement remains active during late completion. Its later removal must
cause legitimate reassignment to a new task on the original worker, loading W=6
and publishing through W=9.

All original oracles remain: actual producer rejection, independently consumed
`read_committed` output and offsets, exact checkpoint reload, immutable conservative
F/W, source metadata/keys, transaction counts, heartbeat checkpoint, convergent
projection, monotonic W and normal task stop. The partition scenario observes a
**locally closed stale producer**; it must not be relabeled as a new broker-fencing
test. The separate original suspended-worker broker-fencing test is unchanged.
Fault waits are finite; every run owns and cleans its containers/network. The
existing independent broker observer is retained for failures after an accepted
replacement checkpoint, including failures in the added native assertions.

## Scan R is not durable F/W

The late-data case advances the stopped old reader's mutable R from 6 to 7,
because the legacy loop calls `nextEntry()` before checking the running context.
It delivers no entry to the old coordinator. Its enqueue checkpoint stays at W=6;
the late-error variant leaves R=6. Neither value authorizes publication or changes
the accepted Kafka checkpoint. A replacement can scan/prepare through 9 while Kafka
has accepted only W=6. The broker offset is the durability oracle, not either
reader's in-memory progress or a prepared/finished marker.

The interrupted-delivery unit case accepts the first record, rejects the second,
and restarts a fresh RPC reader/coordinator using durable inclusive F=2, W=1.
The RPC scan stays at 5 when delivery of the commit at 6 fails. The new reader must
include the entry at F, rebuild and replay the whole batch, and keep old durable W
until a later checkpoint is actually accepted. No scan-only offset is published.

## Remaining blockers and boundaries

The disconnected in-flight case follows real methods:
`connection()` calls `close()`, which calls `cancelJob()`, which calls `connection()`
again while the same disconnected connection and job are still held. The fixture
fails after 32 connection checks to bound the recursion. Production still has the
recursive path. This is a local failure of the desired close behavior, so the
complete local/native C4 safety claim is **blocked**.

Inspection also shows that `ServiceProgramCall.run()` clears the stored job only
on normal return, cancellation's copied connection has no explicit close in that
method, and legacy lost-journal reset/oversize-entry skip behavior is not a safe
buffered-recovery contract. These paths have not been promoted into recovery
proof. The late-error race demonstrates isolation of the controlled replacement;
it does not certify stale native job handles or resource lifetimes.

The next production work would need a separately authorized, unwired lifecycle
refactor: snapshot/cancel the old call without re-entering the connection being
closed, define job-handle invalidation and cancellation-connection cleanup, and
turn the recursion characterization into a safe-close regression. That is a
production behavior change, **not a missing test seam**, and is outside this task.
Keep all existing startup/retention/ownership checks and streaming guards meanwhile.
Native job authority/lifetime, cancellation effects and release/PTF behavior still
require separately authorized disposable IBM i validation. C5/C6 receiver history
and C7 initialization remain independent gates.

## Reproduce and inspect evidence

From `dbz-3025-dcg-followup`, with the matching Debezium dependencies installed:

```sh
mise exec java@temurin-21 -- ./mvnw -B -ntp verify -DskipITs
mise exec java@temurin-21 -- ./mvnw -B -ntp -pl debezium-connector-ibmi dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=target/connect-probe-classpath.txt
python3 -B tools/dcg-reader-lifecycle-probe.py /absolute/new/evidence-data late-data
python3 -B tools/dcg-reader-lifecycle-probe.py /absolute/new/evidence-error late-error
```

Use a new evidence directory for each run. The runner does not retry failed cases
or replace their evidence. A local Unix Docker daemon is required. No IBM i
configuration or credentials are needed.

Local evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-reader-lifecycle/`.

- `verify-final.log`, `tests.json`: 740 executed tests passed, including all eight
  new lifecycle cases and existing stricter/false-dispatch regressions. Test totals
  are taken from suites executed by this build; stale XML files are excluded.
- `package.log`: assembly build and formatter/import validation passed.
- `kafka-data-3/report.json`: late-data case, 22 checks passed and owned cleanup passed.
- `kafka-error-1/report.json`: late-error case, 22 checks passed and owned cleanup passed.
- `kafka-data-1/` and `kafka-data-2/`: retained development failures, respectively
  dispatcher thread initialization and waiting for task close before releasing the
  producer. Both cleaned their owned resources. Neither counts as a safety pass.
- Each Kafka run archives plugin/source hashes, worker and connector configuration,
  command/worker logs, reader/job operation traces and independent output/offset
  snapshots. Generation names are diagnostic correlation only.

The late-data run predates only the unit test's wildcard-to-explicit import fix;
that unit test is not in the probe jar. All packaged reader/connector class bytes
and executable probe sources match the final run evidence. The final full unit
build uses the corrected imports. `validation.json` records source/packaged-class
checks, protected worktree status and production archive exclusions.
