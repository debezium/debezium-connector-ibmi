# Native initialization with local Kafka Connect

This is a test-only integration on `dbz-3025-dcg-followup`. It combines the
existing native initialization adapter with real edgeqa JDBC/journal acquisition,
the actual `As400ConnectorTask` poll/commit/stop lifecycle, strict row decoding,
source normalization, transaction buffering and real Kafka Connect publication.
It does not enable production transactional D/CG streaming or alter F/CR PR #114.

## Completed result — 2026-11-07

The final edgeqa run, `live-07` with isolated library `DCNC7007`, passed all 38
assertions. Native snapshot acknowledgement, persisted initialization history,
exclusive initial F=W=B, fresh-task reader handoff, open-clear restart, stale
producer fencing and post-rebalance checkpoint recovery passed. Actual D/CG
subtype 8 TRUNCATE and subtype 3 optimized DELETE both passed commit and rollback
cases. Unrelated A rows reached Kafka while each B clear remained open, including
across a worker crash. Committed B output was truncate followed by its three
replacement rows; rolled-back clears and rows remained absent.

Final native contents matched the Kafka projections: A contained
`1,2,3,8,9,10,11,81`; B contained `400,401,402`; M and uncaptured U retained
`1,2,3`. Stop request and completed native drain were observed separately.
Both job-local DELETE overrides were verified and discarded. Local resources
and all seven attempted native fixture libraries (`DCNC7001`–`DCNC7007`) were
removed, with independent absence checks and no outstanding fixture inquiries.

The final Maven verification passed 2,313 tests across 54 classes. Assembly,
formatter/import validation and diff checks passed. Two additional local
regressions use the retained replay-floor capture. The source/runtime hash audit
confirms the tested harness matches the final sources, the test classes are
excluded from the shipped plugin, all pre-existing files and production guards
are unchanged, and both protected worktrees are unchanged. Existing stricter
Kafka harnesses were preserved; their earlier completed runs were not rerun.
Advisor inspected the actual files and retained evidence and passed the review
with no actionable defect within this test-only scope. Advisor did not rerun
the tests or live experiment; its result is saved in `advisor-review.json`.

Local evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/native-connect/`.
The final report is `live-07/report.json`; raw source, offsets, output and history
are under `live-07/control/`; cleanup proof is under `live-07/native/`. See
`audit.json`, `tests-summary.json`, `verify-final.log`, `package-final.log`,
`turn.patch` and `replay-floor-regression.json` for local verification.

## Bounded profile

`tools/dcg-native-connect-probe NEW_EVIDENCE_DIRECTORY DCNCdddd` builds the test
classes and runs the experiment. It requires the configured aesql Python
environment, an authenticated `legacy-mfa` profile, local Unix-socket Docker and
JDK 21. The script explicitly performs edgeqa writes, exclusively in a newly
created, previously absent `DCNCdddd` library. It does not accept an existing
auction library. Credentials enter Java through stdin and usql through a
memory-backed config; worker output is redacted before persistence. Private DNS
is resolved on the host and mapped only inside the owned native containers.

The fixture has A/B/M (captured) and U (uncaptured), each with the complete schema
`ID INTEGER NOT NULL PRIMARY KEY`. Unsupported or partially represented schemas
reject. This is not validation of arbitrary source layouts. The experiment owns
all writers, DDL and maintenance in its journal. During initialization one JDBC
transaction holds verified exclusive, read-allowing locks on all four files.
The qualified-session token is required throughout initialization. These locks
and controller ownership are not a general production fence against arbitrary
API resources, administrators or other jobs.

Before transactions begin, the fixture rotates its own receiver while preserving
existing fixed fields and adding thread identifiers. It verifies job/thread
recording, complete fixed/internal entries, both row images, retained receivers
and disabled automatic receiver deletion. Every receiver is retained until
final verification. This implements the selected operator-managed retention
profile; it does not prove a production retention-enforcement service.

For the tiny DELETE fixture, the dedicated writer job creates a QTEMP QAQQINI
override, sets and reads back `SQL_FAST_DELETE_ROW_COUNT=1`, and discards it in
`finally` after rollback/commit. No shared QAQQINI setting changes. The raw-journal
assertion still requires D/CG subtype 3: successful SQL or individual R/DL row
deletes do not qualify as optimized-clear coverage. This uses IBM's documented
[job-local override procedure](https://www.ibm.com/docs/en/i/7.5.0?topic=services-override-qaqqini-procedure)
and [fast-delete option](https://www.ibm.com/docs/en/i/7.5.0?topic=qaqqini-query-options).

Kafka is the existing digest-pinned 4.3.1 local harness, with the validated
timeout profile, a single broker with replication factor one, one partition per
table topic and a dedicated history topic.
Each task poll is a broker transaction. Publication and source offsets use
standard Connect fencing; custom owner/receipt authority classes are excluded
from the candidate artifact. The local enrollment file only prevents reuse of
an incomplete initialization generation. It is not publication authority.
The connector still advertises exactly-once support as unsupported; this
experiment's requested mode is not a production support declaration.
Recovery retains the whole-batch at-least-once contract: an interrupted batch
can replay an already published prefix. Broker transactions/fencing prevent
stale-worker publication but do not by themselves make an entire source
transaction one atomic Kafka transaction. Ordering assertions apply within each
single-partition table topic, not across topics.

## Acceptance path

1. Acquire actual native bindings and snapshot all rows while the qualified
   session is held. Materialize rows before selecting the initial boundary B.
2. Hold the baseline record's producer acknowledgement. Snapshot records alone
   must not install history, an initial F/W checkpoint or a reader handoff.
3. Release the acknowledgement, require committed Connect callbacks and exact
   source-offset readback, persist native binding history and recover it fresh.
4. Hold the initial checkpoint acknowledgement, then verify durable exclusive
   F=W=B before crashing the task at the pre-handoff boundary. A replacement
   must recover history and offsets without repeating the snapshot.
5. Open native SQL TRUNCATE on B, insert three uncommitted replacement rows and
   independently commit A:8. Consume Kafka to prove A progressed while B stayed
   hidden. F retains the open cycle while W advances with completed output.
6. Crash and replace the task while that native transaction remains open. Commit
   A:81 and verify fresh replay rebuilds B's open cycle without duplicating A:8.
7. Commit B; hold the actual truncate send, suspend its worker and let another
   worker take ownership. Verify the replacement emits truncate then replacement
   rows in B's partition, and the old producer is broker-fenced when released.
   Returning/removing the old worker can legitimately restart the replacement
   task again. Stop/resume through Connect and observe the new generation before
   continuing the remaining cases; require the same durable checkpoint.
8. Exercise optimized DELETE rollback, TRUNCATE rollback and optimized DELETE
   commit. Inspect actual raw D/CG subtypes 3/8 and independently consume output
   and offsets. Rolled-back clears and replacement rows must stay absent; A must
   progress before each transaction terminates.
9. Compare final native table contents with Kafka projections, stop the task and
   distinguish stop request from completed native reader drain.

Each named native cut reloads durable F/W and binding history and reconstructs
open transactions through the existing replay coordinator. It does not use
current table metadata while a clear is open. Scan R is recorded separately from
durable F/W. Incoming and resulting task checkpoints must both match independent
Kafka offset reads. A new clear is identified beyond incoming W; older entries
between F and W remain in the raw capture and undergo the same strict replay.
This bounded profile requires those clear records to remain on the configured
receiver; a receiver rotation needs a separate scenario.
Raw journal bytes retain actual object/job/thread identities; no
synthetic thread or sequence substitutions are permitted. Per-task work is
bounded to 12 requested cuts and 20 minutes; each native transaction has a
12-minute rollback deadline, and acknowledgement/phase/query waits are bounded.
This is controlled incremental reading, not a continuous production source loop.

The task uses a bookkeeping coordinator stub for callback observation and close;
actual data uses the existing real batch dispatcher with schemas checked against
the complete native integer-row profile. The existing stricter local tests and
synthetic initialization failure cases remain unchanged.

## Cleanup and evidence

The runner retains sanitized logs, native metadata and lock evidence, raw journal
records, Kafka history/output/offset snapshots, generation lifecycle markers and
source/artifact hashes. Failure diagnostics use an independent broker observer.
On failure it aborts its own transactions, terminates only owned containers and
verifies their networks are removed before native cleanup.

Cleanup verifies ownership, the exact object inventory, no pending transactions,
no native file/journal locks and no unexpected inquiries. It drops only the owned
tables and schema with RESTRICT. A receiver inquiry is answered only after its
exact receiver/library and originating drop job are matched to the fixture's
library lock. Independent final queries require schema/library and inquiries to
be absent. Any missing proof leaves cleanup explicitly blocked.

## Scope still unqualified

This does not qualify a general production snapshot/session implementation,
continuous production reader wiring, arbitrary schemas/DDL, multiple source
tasks or partitions, automatic retention release, restored Kafka offsets,
broker-loss durability, sustained-load latency/throughput, downstream sinks or
production deployment. Existing identity, receiver history,
journaling-transition, savepoint and unsupported-record checks remain intact.
Production transactional streaming stays disabled pending those integration and
support gates.

## Retained diagnostic attempts

Earlier failed attempts remain under the same evidence directory. They exposed
fixture inventory including schema-generated logical catalog files, an omitted
worker log configuration, container-private DNS resolution, and a runner command
sent to a task generation that Connect had legitimately retired during rebalance.
Those harness issues were corrected without changing production code or relaxing
acceptance assertions. Independent broker snapshots were retained for failures
after connector creation.

The first DELETE attempt also produced actual R/DL row deletes, not D/CG.3, for
the tiny table. Strict replay accepted those ordinary rows, but the explicit raw
subtype assertion failed as intended. The corrected case uses the established
job-local fast-delete override and three-row reloads. No successful SQL statement
is substituted for native subtype evidence. Every failed fixture was cleaned up
and its absence independently verified.

A further diagnostic attempt showed why the three positions matter: after a
rollback, scan R had advanced beyond completed-output W while the conservative
replay floor F still retained the earlier clear and its journaling transitions.
An assertion that every cut contained only one clear was incorrect. The revised
assertion checks exactly one new clear beyond incoming W while preserving all
older replay records. A local regression against the retained raw capture checks
both acceptance of the new subtype and rejection of an older matching subtype.
