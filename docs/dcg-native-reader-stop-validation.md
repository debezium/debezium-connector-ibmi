# Native reader interruption: bounded edgeqa case passed

Follow-up: the corrected strict decoder has also passed a fresh native run in
the test publication path. See [strict reader validation](dcg-strict-reader-validation.md).
The report below preserves the earlier run and its legacy-decoder scope.

On 2026-11-07, the isolated native reader interruption/replacement experiment
passed all 32 checks on edgeqa (AEQA, IBM i 7.5). The old native COMMAND job was
held while its real `ServiceProgramCall.run()` was blocked. A replacement reader
captured an insert, native clear and reload, then captured another insert after
the old reader was stopped. Its original native job and connection survived.

The old reader emitted no records and did not advance its scan. Kafka rejected
its stale diagnostic producer. Independent `read_committed` consumers confirmed
four ordered events (`c:1`, `t`, `c:2`, `c:3`) and five nonregressing diagnostic
checkpoints, including the initial checkpoint. The final source rows were 2 and 3.
The successful run measured 306 ms from stop request to reader drain. IBM i's job
still reported `*ACTIVE`/`END` immediately afterward; a bounded fresh observation
then reported `*OUTQ`. This separates completed reader drain from native job
termination and is an observation, not a guaranteed shutdown deadline.

**Transactional D/CG streaming remains disabled.** This is a native lifecycle
validation using ordinary inserts and F/CR outside commitment control, not proof
of transactional clear recovery. Production code and F/CR PR #114 were unchanged;
nothing was pushed or deployed.

## Scope and decoder finding

`As400NativeReaderStopProbe` is an explicit test-class entry point, never a
default Maven unit/integration test or production plugin component. It uses the
real RPC reader, receiver pagination, transport, native call, cancellation and
terminal drain. A test-only spy on the existing lifecycle holds the exact old
job at active-call registration. No production test seam was added.

Publication uses a standalone diagnostic Kafka producer, not Kafka Connect or
the Debezium event dispatcher. Source operations occurred only in the fresh
`DCTASKQA.NATREAD` fixture; Kafka ran on this workstation with a loopback listener.
Checkpoint identities are fixture tokens and F equals W for these operations
outside commitment control. This does not prove native startup identity/history,
retention, transactional D/CG replay, live Connect reassignment, or a sink.
Existing local Connect/strict-recovery assertions remain unchanged.

The live run exposed a separate compatibility gap in `CompleteRowEntryDecoder`:
for this required INTEGER column, IBM i returned a nonzero null-map displacement
pointing to a zero-length map. The strict candidate requires a map length equal
to the column count whenever that displacement is nonzero and rejected the row.
The first native held-job attempt therefore failed before replacement output;
its held job was released and both readers drained. Independent broker evidence
shows zero event records and only the initial checkpoint for that failed attempt.

The completed lifecycle run explicitly used the connector's existing
`JdbcFileDecoder` for row identity. It also evaluated the strict candidate
separately and saved its rejection and the native bytes for every insert.
**This is not a production fallback or a strict-decoder pass.** The strict
implementation and its tests were not changed during that native run. The
subsequent [local decoder correction](dcg-null-indicator-validation.md) accepts
the captured empty-map form only for entirely non-nullable layouts and preserves
nullable/defaulted/minimized-row rejection. It is verified by offline replay and
regression tests; it does not change this native run's historical result.
Transactional enablement still requires the remaining gates.

## Ownership, failure recovery and cleanup

The controller verified AEQA identity and that both the SQL schema and native
library were absent before creating `DCTASKQA`. It saved an ownership marker and
object creation identities. The fixture contained one non-null INTEGER primary
key column and one member. Existing auctions and DTEST were not touched.

Old and replacement jobs had distinct qualified names, internal identifiers and
entry timestamps; the old job was also checked against its connection's COMMAND
job inventory. The hold attempt was recorded before issuing the command. On an
uncertain/failing hold, cleanup releases only a fresh observation matching that
saved active lifetime and held status. An independent helper using another
connection was available to reconcile that same identity if the probe stalled.
The successful run needed no emergency release. Independent post-run job checks
found the old job ended and the replacement job absent.

Cleanup compared the library inventory with its original creation identities
and verified no table locks. After dropping only `NATREAD RESTRICT`, the journal
reported zero journaled files, members and commitment definitions. The schema
was then dropped with `RESTRICT`. Its unsaved-receiver inquiry was answered only
for the exact owned `DCTASKQA/QSQJRN0001` receiver, message key `0002C710` and
cleanup job. No global reply setting was changed. Independent checks verified
that the schema, native library and pending fixture inquiries were absent.
The owned local broker and network were removed and their absence verified.

Initial cleanup queries mistakenly used CL-style message-filter values; these
returned no rows and are not absence evidence. Corrected queries used IBM's
`INQUIRY` filter and matched the exact message and cleanup job before replying.
The DROP client had timed out while the server awaited that reply; completion
is established by the independent postchecks, not the client exit status.

Credentials remained in memory/stdin, with captured console output redacted.
The controller selected edgeqa's existing non-TLS test connection profile;
connector defaults were not changed. Read/fault observations have test deadlines,
but production stop retains its unbounded native-drain contract. A timeout alone
never establishes that a native call or job ended.

## Evidence and checks

Current evidence:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-stop/live-20261107/`.

- `probe/native-report.json`: 32 passed native/Kafka diagnostic checks, identities,
  held call stack, separate stop/drain/job-termination observations.
- `independent-events.jsonl`, `independent-offsets.jsonl`: separate broker CLI
  consumers matched the probe's final records and checkpoints exactly.
- `probe/row-*.json`: actual fixture records and strict-candidate rejections.
- `attempt-4-strict/`: preserved strict-decoder failure, recovery and broker output.
- `attempt-5-job-transition/`: preserved overly early job-status assertion; later
  observation confirmed termination. The final harness waits within a deadline.
- `attempt-1/`, `attempt-2/`, `attempt-3/`: earlier harness configuration failures
  before native mutation/hold (Kafka timeout mismatch, Docker host reachability,
  and selection of edgeqa's connection profile). Their evidence remains preserved.
- `objects-created.json`, `objects-before-cleanup.json`, `locks-verified.json`,
  `journal-after-table-drop.json`, `cleanup-inquiry-verified.json`,
  `schema-absent.json`, `library-absent.json`, `inquiries-absent.json`: fixture
  ownership, cleanup and independent absence checks.
- `runtime-sha256.json`, `validation.json`: runtime/source hashes, preserved
  production/protected branches, final checks and independent review result.
- `verify.log`: all 818 unit tests passed after the harness changes.
- `package.log`: assembly and formatter/import validation.

The parent evidence directory preserves the earlier maintenance-blocked attempt
and its local preparation checks. Those historical records do not describe the
current live result. The original Advisor finding about lost hold acknowledgements
was fixed before live execution and has two local regression cases. This turn's
Advisor review passed after inspecting actual files and saved evidence. It
confirmed the bounded lifecycle result and hold-recovery fix while keeping
strict-decoder compatibility open; it did not rerun tests or live operations.
The review is saved as `advisor-review.json` with the current evidence.

Native hold and cleanup APIs:
[IBM HLDJOB](https://www.ibm.com/docs/bg/i/7.4.0?topic=ssw_ibm_i_74%2Fcl%2Fhldjob.html),
[IBM MESSAGE_QUEUE_INFO](https://www.ibm.com/docs/en/i/7.4.0?topic=services-message-queue-info-table-function).
