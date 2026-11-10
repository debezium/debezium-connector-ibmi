# Task stop, streaming retries and JDBC cleanup

This follow-up integrates the local terminal-stop contract with the actual
Debezium coordinator, streaming loop and task cleanup. Transactional D/CG streaming
remains disabled. F/CR PR #114 and its worktree are unchanged; nothing is pushed.

## Behavior

A streaming context that has stopped cannot enter a new error-driven reconnect.
The retry method checks the context between RPC close/connect and JDBC close/connect,
and also refuses external retry requests after streaming exits. Idle heartbeat
publication checks the context again after its wait. A lease on the existing
reader lifecycle covers the entire retry, including JDBC. Task cleanup therefore
waits for an admitted retry to finish before closing the task's JDBC connection.
Ordinary reconnect while the context is running remains supported.

Stop request and completed drain are distinct. An already admitted call may
continue while stop is pending; a flag check cannot make an invocation atomic.
Completed terminal stop waits for admitted work and closes its transports. This
work does not establish a native shutdown deadline or make arbitrary driver calls
interruptible. JDBC operations outside the retry path (for example snapshot or
row-metadata work) are not covered by this retry lease. The new held-metadata Kafka
case is an RPC metadata lookup, not a live JDBC query.

## Local coverage

`As400StreamingTaskStopTest` adds 20 deterministic cases: plain/secure variants
of five stopped-context outcomes (I/O, SQL connection, interrupt, wrapped RPC
metadata error and idle), ordinary reconnect, and stop during each of the four
RPC/JDBC retry stages. Actual task cleanup must wait for a held admitted retry.
An independent reader can progress; the old reader cannot reopen after completion.
The prior cancellation, terminal admission, receipt, missing-offset,
false-dispatch and recovery tests retain their assertions.

`As400TaskStopProbeConnector` and `tools/dcg-task-stop-probe.py` add separate
Kafka experiments for a held RPC read and held RPC metadata call. They run the
real Connect task start/poll/commit/stop, real Debezium coordinator executor/context
and shutdown waits, real streaming execute/retry loop, RPC reader/close/stop and
batch dispatcher. JT400/JDBC calls are strict controlled transports. Initial
snapshot/startup and the unwired D/CG consumer/heartbeat adapter remain synthetic;
this is not production D/CG wiring or native startup proof.

The candidate path retains standard Connect publication and offsets with no
custom owner or receipt gate. The inherited scenario and its assertions are
unchanged: rejection of the stale producer after local close, independently read committed
output/offsets, replacement W=6, legitimate reassignment from inclusive F=2/W=6,
final W=9 and final projection. The old reader has scanned R=6 and prepared W=6;
its prepared checkpoint is not described as its durable Kafka checkpoint.
The replacement's committed checkpoint is the durable oracle. New observations
keep the old call held through coordinator shutdown and terminal-stop request,
prove replacement W=6 while old drain is pending, then release the old error and
verify no new RPC/JDBC work or old checkpoint advancement. The old and
legitimately reassigned task finish with their own transports disconnected.
The intermediate replacement is intentionally killed by the inherited scenario:
exit 137 proves process termination, not completed Java cleanup or native job
termination. Its Java drain marker must be absent.

Earlier broker-fencing and missing-offset cases are retained unchanged; these
new cases use the partition scenario and observe local producer-close rejection.
The earlier ordinary-close Kafka probes are preserved separately. Their
characterizations must not be silently reinterpreted as terminal-stop tests.

## Native smoke test and limits

The user authorized mutations confined to DTEST or an isolated edgeqa library.
A fresh DTEST schema and TESTT table were created after confirming both DTEST and
DCTASKQA were absent. The current branch's compiled classes ran the three existing
`As400ConnectorIT` cases: snapshot/inserts, CLRPFM/reload, and SQL TRUNCATE
IMMEDIATE/reload outside commitment control. All three passed and each embedded
connector stopped normally. Events stayed in the local embedded-test process;
no external Kafka or sink was used. The SQL case validates the supported F/CR
behavior, not transactional D/CG recovery. Credentials stayed in memory and saved
logs were redacted. After checking for locks, the owned table and schema were
removed. The schema deletion inquiry was answered only for its exact DTEST
receiver message key; fresh checks confirmed the schema, library and pending
DTEST inquiries were absent. `edgeqa-validation.json` records cleanup evidence.

This smoke test checks compatibility and ordinary native cleanup. It does not
force an uncooperative native call, establish stale-job lifetime/cancellation
isolation, validate transactional rollback/restart or satisfy all of C4-C7.
Full C4 remains blocked on those native fault cases. All current startup and
transactional-streaming guards and stronger tests remain in place.

## Reproduction and evidence

From the follow-up worktree, after preparing the existing probe classpath:

```sh
mise exec java@temurin-21 -- ./mvnw -B -ntp verify -DskipITs
python3 -B tools/dcg-task-stop-probe.py /absolute/new/rpc-evidence rpc-error
python3 -B tools/dcg-task-stop-probe.py /absolute/new/metadata-evidence metadata-error
```

Evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-task-stop/`.
`verify-2.log` records 806 passing unit tests, including the 20 new cases.
`package.log` records assembly and formatter/import validation.
`kafka-rpc-final/report.json` and `kafka-metadata-final/report.json` each record
27 passing checks; `kafka-ordinary-error/report.json` records the unchanged
ordinary-close regression's 25 checks. All three runs verify owned-resource
cleanup. The final `validation.json` records the isolated source delta,
protected worktrees and source/plugin/packaged-class hashes. `edgeqa-validation.json`
and `edgeqa-java-tests.log` record the three native smoke tests and fixture cleanup. Earlier failed
runs are retained, including the initial subclass-reflection fixture error and the initial verifier incorrectly
waiting for Java cleanup after the intentional replacement SIGKILL.
Use the final report rather than inferring completion from an intermediate log.

Independent Advisor review passed after inspecting actual files and saved
evidence. It reported no new actionable issue; it did not rerun tests and
explicitly retained the native forced-cancellation limitation. Advisor was called
once for this task; its exact result is saved in `advisor-review.json`.
