# Deferred online handoff and automatic boundary feasibility

The test-only handoff now keeps legacy publication moving when a selected table
has an unfinished transaction whose prefix was already processed. It records
ordinary progress through Kafka Connect, survives replacement, and retries the
handoff after that transaction finishes. This is a continuation of the
[receiver-history investigation](dcg-receiver-history-discovery.md), not normal
startup admission. Automatic migration remains disabled. No production Java,
transactional streaming guard, F/CR branch or published PR changed in this work.

## Deferred publication and recovery

`As400OnlineHandoff.SelectedPrefixPending` identifies only the temporary refusal
for an unfinished selected prefix. Candidate preparation performs its existing
reconstruction and cut checks before returning this refusal. Other decode,
identity, incomplete-history, lifecycle and savepoint failures still fail closed;
the test loop does not treat every exception as a reason to continue. A cut with
no remaining open cycles is now accepted when the existing reconstruction checks
succeed. It still derives W from a real completed operation, not the scan R.

The test connector uses the real legacy RPC loop, row decoder, streaming handler,
queue, task and dispatcher. The legacy phase skips truncate as an ordinary legacy
configuration would; the buffered phase enables it. At each bounded experimental
deferral point, a control record carries the processed legacy receiver, sequence
and timestamp plus the captured-evidence fingerprint. Kafka Connect commits that
record and the preceding data. No F/W checkpoint is installed yet. The task waits
for the real commit callback before progressing to the next test segment.

The `native-capture-progress-v1` marker is test-only offset metadata, not a custom
owner or receipt store. A replacement validates the exact recorded boundary,
bundle and fingerprint and resumes from that legacy position. It does not start
again from the initial seed. An unresolved legacy cached before-image rejects a
progress checkpoint, since that cache would otherwise disappear on replacement.
This check is necessary for this controlled prefix; it is not proof that an
arbitrary source has no unfinished autocommit update.

After the temporary refusal clears, the existing ordered publication barrier and
F/W transition apply. Complete context is still reconstructed from retained
history with `CONTROLLED_FIXTURE_ASSUMPTION`. Nothing repairs legacy records that
were already published before migration. Already-visible legacy rollback behavior
is preserved. This is why the expected legacy prefix and transaction-buffered
suffix are checked separately.

## Local Kafka evidence

Current evidence directory:

`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-deferred-handoff-20261109/`

`kafka-deferred-02` uses the original native `DCNC0925` capture from
`dcg-membership-resolution-20261109/live-06`. It starts from the ordinary legacy
seed. At R=310 and R=350, selected B clear cycle 293 is still open; its rollback
is the actual native C/RB at 371. The broker independently reports 244 then 284
committed records while migration is deferred. The intervening 40 records are
unrelated A inserts. Neither progress offset contains version-2 F/W.

The controller holds the second progress callback **after** the broker commits
it, kills that worker, and starts a replacement. The replacement loads the actual
R=350 progress offset without a controller rewrite. At the final candidate cut
417 it publishes one legacy barrier and one migration checkpoint. Final committed
output contains exactly 565 records, one truncate, no snapshot, and F=W=654.
Fourteen unrelated A inserts between the later clear at 425 and its commit at 442
appear before the clear's output. Original keys, source times and receiver
coordinates match the retained native bytes. The record total differs from the
earlier cut-162 experiment because more of the workload retains legacy publication
semantics before migration.

The original five scenarios remain the default matrix. `kafka-regression` passes
all 34 assertions: pre-barrier failure, lost barrier callback, lost F/W callback,
interrupted clear/reload, and reassignment with an old worker returning. Each has
562 exact committed records, one truncate, no snapshot and final F=W=654. The
missing-offset rejection and actual source producer fencing are preserved. The
independent broker audit verifies the old producer's
`InvalidProducerEpochException` and all source coordinates. Both runs cleaned up
their local Docker resources.

`kafka-deferred-01` is an earlier successful variant that kills the worker after
its progress callback. Its staged artifact predates the final seed-timestamp
preservation adjustment; it is supplementary evidence, not verification of the
final artifact. The mistaken-path `kafka-deferred-02-path-error.log` failed before
creating a run or resources. An initial unit assertion incorrectly expected W=64
at R=64; the actual last completed operation is W=59. That assertion was corrected
and the failed log retained. No runtime checkpoint behavior was weakened.

Reproduction uses the existing explicit-config harness, compiled test classes and
a retained native capture; no IBM i connection is made by this command:

```sh
DCG_HANDOFF_SCENARIOS=deferred DCG_HANDOFF_DEFER_CUTS=310,350 \
  mise exec java@temurin-21 -- python3 -B tools/dcg-online-handoff-probe.py \
  NEW_DIRECTORY CAPTURE_DIRECTORY CAPTURE_DIRECTORY/replay.json 417
```

These coordinates belong to this capture. They are not a production scheduler or
an assumption about other journals. Delete/tombstone pairs are kept on the same
side of a progress boundary, with orphan or mismatched tombstones rejected.

## Native automatic-lock feasibility

Two fresh libraries, `DCNC0927` and `DCNC0928`, tested a possible alternative to
unproved zero-blocking observation. These experiments wrote only isolated fixture
objects. Both libraries, receivers and associated inquiries were verified absent
afterward; connections closed without cleanup errors and temporary credentials
were deleted. No application jobs were ended.

IBM documents [ALCOBJ][allocate] as all-or-none, supports `WAIT(0)`, and defines
[EXCLRD compatibility][locks] to allow readers while excluding other writers.
This is an automatic write-blocking mechanism. It is not evidence that writers
can always continue without delay.

`native-fence-01` requested an exact single-member B `*EXCLRD` allocation. A
conflicting uncommitted B update caused CPF1002 after 0.080 seconds with no held
lock. After rollback, allocation succeeded even while the writer session remained
open. An incompatible writer allocation was rejected while the holder owned the
lock. A healthy hold around the existing native anchor collector lasted 4.207
seconds, including new JVM/JT400 startup; eight unrelated A writes completed during
that interval. Explicit release took 0.043 seconds and was independently checked.
A subsequent B update succeeded. This is not a measurement of an optimized
in-process metadata collection path or a bound on failed connections.

A multi-call client-held CL lock is insufficient because its release would depend
on the client returning. `native-fence-02` instead used one IBM i
[compound dynamic statement][compound] containing allocation, a bounded exact
receiver query, release, and an exception handler that releases before rethrowing.
The statement is retained in the evidence directory. No persistent procedure was
installed; IBM documents the compound statement's temporary program in QTEMP.

The normal call and an injected SQL error both left no held lock. A separate
owned client was killed only after an independent connection observed its held
lock. The server-side statement contained a deliberate two-second delay before
release. The lock was absent 2.627 seconds after the client kill, five unrelated
writes progressed, and a later B update/commit succeeded. The observer did not
use ENDJOB on a disconnected, potentially reusable database server job.

Whole compound-call durations were 3.439 seconds for initial busy rejection,
1.271 seconds for normal completion, and 1.249 seconds for injected failure. These
include statement preparation/compilation and are **not measured lock hold times**.
`WAIT(0)` limits waiting for the requested allocation, not total SQL execution or
network delay. A killed local process closes its socket; it is not a network black
hole. The experiments establish neither a hard wall-clock bound during source
failure nor a general startup proof.

## Contract and implementation work still required

The user has now explicitly accepted brief automatic write locks. An administrator
does not need to arrange a writer pause; application writes to the locked files
may wait while the connector collects its boundary. This decision does not accept
indefinite pauses or turn a file lock into complete transaction-history proof.

The [automatic boundary experiment](dcg-automatic-boundary-validation.md) now
captures complete acquired binding metadata and a native source boundary in one
server-side release scope. It remains test-only candidate evidence. Before normal
startup can use it with the deferred legacy handoff:

- Validate the entire captured-table set, not only one B member. Busy acquisition
  must release everything and leave legacy CDC progressing; no public per-table
  truncate mode is needed.
- Prove which native locks exclude unfinished autocommit updates, selected-table
  transaction effects, DDL and journaling changes. `*EXCLRD` success alone is not
  yet a complete file/member/history certificate. Reject unsupported changes.
- Capture schemas, paired file/member identities, receiver identity/continuity
  and the exact origin under one qualified interval. Do not apply current schemas
  to older rows or infer an empty global transaction inventory from a file lock.
- Reconstruct retained C/J context for older cycles, preserve savepoints and
  lifecycle dependencies, and prove that the selected row context can start at
  the new origin. Keep schema history durable before publishing any F/W.
- Validate the combined live native reader and Kafka barrier with concurrent
  writers, mixed columns, rotation, crash/reassignment and cancellation. The
  separate native lock experiment and captured Kafka replay do not establish
  this combined behavior.
- Measure actual lock hold time independently of preparation, including all
  selected tables and failure cleanup. Do not promise a hard pause limit from
  these healthy timings.

The test-only deferred handoff loop is useful for either boundary strategy. It
allows migration to remain pending without turning an old open clear into a
pause of unrelated CDC. General normal-task migration remains disabled until its
source evidence and these integration checks are satisfied.

[allocate]: https://www.ibm.com/docs/en/i/7.6.0?topic=ssw_ibm_i_76%2Fcl%2Falcobj.html
[locks]: https://www.ibm.com/docs/en/i/7.6.0?topic=resources-lock-states-objects
[compound]: https://www.ibm.com/docs/ssw_ibm_i_74/db2/rbafzcompounddynstmt.htm

## Final verification and review

Maven verify passed 2,643 Java tests (200 parser, 2,443 connector), and all 77
Python tests passed. Assembly packaging, formatter/import validation and
`git diff --check` passed. Both final Kafka artifacts contain 329 local classes
matching the final build, and all staged harness/plugin hashes match the saved
reports. Source comparison confirms no production-source change in this
continuation; protected worktrees, branch heads and indexes are unchanged.

The explicit `advisor_review` tool inspected actual repository files and saved
broker evidence and passed with no actionable issue or file-access limitation.
This review covers the conditional implementation and its reported evidence;
it does not qualify automatic native startup or enable migration.
