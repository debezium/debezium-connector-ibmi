# Live native source and normal-task online handoff

This continuation joins the [normal-task handoff](dcg-normal-task-integration.md)
to an actual IBM i source and local Kafka Connect. It does not admit automatic
migration in the public connector. The native boundary is still acquired by the
isolated test controller, which owns schema changes, writers and retention.
No snapshot receipt is manufactured and no custom publication owner/receipt gate
is added.

Evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-live-handoff-20261109/`.

## Implementation

`tools/dcg-live-handoff-probe.py` reuses the prospective boundary collector,
ordinary history, normal-task handoff and existing native Connect launcher.
It requires an explicit private `IBMI_TEST_CONFIG`, a new evidence directory and
a fresh `DCNCdddd` library. The portable runner contains no Auction Edge/AWS
credential dependency. The private host adapter is retained outside the repository.
Credentials reach worker JVMs through stdin and remain absent from Connect config.

`As400LiveHandoffProbeConnector` is a test entry point. It rereads the actual native
legacy prefix, checks its paired table bindings and native journal/receiver
identities against the audited boundary, and compares complete source content.
Documented transport placement/alignment differences are normalized; source bytes,
identities, timestamps, flags and payloads are retained. The legacy handler/RPC
processing loop is still fed through the existing controlled cursor adapter.
The source records feeding that cursor are now reread from IBM i, not supplied
only by the saved capture. This distinction is explicit in the report.

After the acknowledged legacy barrier and F/W transition, the actual task's
`As400RecoverySource` uses `As400OperatorCutReader` through the existing guarded
JT400/JDBC transport. It reads the live tail incrementally, with separate scan R,
immutable acquired bindings and durable F/W. There are no controller-selected
runtime cuts and no table-catalog lookup for each emitted record. The capture's
old receiver ranges are not used to authorize later native F/W: the real reader
must validate retained identities, links and anchors before output.

Fresh recovery reads the standard durable binding history. The test launcher
still loads its initial boundary bundle/configuration; this is not yet a public
restart implementation independent of test admission. Source shutdown uses the
existing terminal admission, exact reader cancellation and completed-drain paths.

The original captured-source and stricter whole-batch test modes remain intact.
The new entry point uses the normal task's at-least-once source contract and
Connect transactions per poll. It does not claim atomic visibility of an entire
native transaction or universal absence of replayed prefixes.

## Native results and retained failures

`live-01` / `DCNC0971` established the handoff and replacement, but its rollback
wait incorrectly required completed-output W to reach later metadata records.
The raw source showed the supported C/RB terminal at 94 and scan through 98.
Those coordinates have different meanings. The test now independently reads the
bounded native C/RB or C/CM record for the exact writer and waits for that terminal.
No production checkpoint rule was changed. Native and Docker resources were
removed; idle boundary-session rollback errors are retained in this failed run.

`live-02` / `DCNC0972` found a second harness mismatch: DISPLAY_JOURNAL job fields
are padded SQL character fields. The independent terminal selector now removes
only right-padding spaces and still rejects missing, duplicate, foreign and
nontransactional terminals. Boundary sessions are drained before the longer
Kafka run. Native and local cleanup passed without connection-close errors.

`live-03` / `DCNC0973` passed all 15 runtime assertions. It seeded only the
ordinary legacy offset, published the exact expected prefix, acknowledged the
legacy barrier, persisted the separate activation, and changed to F/W with no
snapshot. While a real B truncate/reload remained open, an A insert reached Kafka.
Receiver rotation put F=84 inclusive in QSQJRN0002 and W=96 in QSQJRN0003, with
different verified native receiver identities. A fresh worker loaded that exact
offset without a controller rewrite. Rollback emitted neither the clear nor its
reload. A subsequent committed clear emitted A's unrelated row, then B's truncate
and reload in the required order. Independent native final-table queries matched.

The successful lock pulse in `live-03` held allocations for 0.001473 seconds;
its client call lasted 1.097784784 seconds. These are observations for this fixture,
not a hard maximum under contention or native I/O failure. Metadata stayed outside
the allocation interval. The busy attempt refused the old transaction while
ordinary fixture writes continued. That source-write check is not proof of Kafka
CDC progress during automatic boundary acquisition, since the controller acquires
the boundary before starting this task.

All workers and local Kafka resources drained/terminated in `live-03`. Initial
native cleanup rejected its ownership inventory because OBJECT_STATISTICS hid B's
attribute/creation fields while the clear was open. The rotation helper had
overwritten the original full identity with those missing fields. It now retains
every existing identity and extends ownership only for the exact newly generated
receiver, checking the receiver profile and membership. Cleanup still requires
the full original identities to match after drain. Missing metadata never grants
permission to delete a replacement or unknown object.

`cleanup-0973.py` used the retained original inventory plus the independently
recorded new receiver to rerun the unchanged strict drop checks. The result in
`live-03/cleanup-recovery-result.json` verifies schema/library absence and no
remaining inquiries. The initial blocked report is retained, not relabeled.

## Remaining normal-startup gate

This proves the joined publication/recovery behavior for the controlled source.
It does not prove that a generic existing installation can supply the same safe
starting boundary. The remaining implementation must acquire native scope,
paired bindings, all-code boundary audit and complete older control history inside
the normal task, while legacy CDC continues. It must retry only recognized busy
conditions, retain fatal rejection of missing/ambiguous evidence, and persist the
same pending history and acknowledged activation. It must also remove the test
bundle/configuration dependency from ordinary restart. No per-table truncate
option is proposed; ordinary capture filters remain the selection mechanism.

In particular, the prospective fixture's `history_from` is recorded by its
controller before the deliberately created old transaction. This bounds the
known fixture history; it is not discovery of every pre-existing transaction in
an arbitrary journal. The separate pending-receiver/C/J discovery work must be
joined to startup before this premise can be removed. Neither the live worker
replacement nor matching current metadata repairs that missing initial proof.

The [documented receiver `PENDING_TRANSACTIONS` field][receiver-pending] describes receivers containing
entries for transactions that have not committed or rolled back. That supports
the existing pending-receiver/history investigation, but a matching pair of
catalog samples is not an atomic transaction census or historical schema proof.
The complete bracketed C/J history and prospective-schema checks remain necessary.
The first native qualification here deliberately retries after prior-schema
cycles finish. The general prior-cycle deferral model remains locally tested.
No unrestricted DDL/membership, restore or missing-history guarantee is added.

The documentation lookup found no matching Context7 passage; its suggested
`--research` flag was unsupported. Indexed primary IBM documentation confirms the
field definition, but direct page opens returned HTTP 403. No additional platform
guarantee is claimed from that lookup.

## Local verification

Maven verify passed 2,755 tests (200 parser, 2,555 connector). Assembly packaging,
formatter/import validation and `git diff --check` passed. The Python suite adds
terminal-coordinate, padded native fields, owned-session drain and rotation
ownership regressions. The saved Maven module summaries are authoritative: stale
Surefire XML files from older source sets must not be added to the current count.

All 361 staged local Java classes in `live-03` match the verified build. The only
subsequent staged-source difference is the rotation ownership cleanup correction,
which was also used for the successful native cleanup recovery. Public admission,
production Java, F/CR and protected worktrees remain unchanged by this continuation.

`live-04` / `DCNC0974` repeated the complete native/Kafka experiment with the final
rotation ownership correction. All 15 assertions passed; source connections,
native worker processes, Docker containers/networks and the exact fixture were
cleaned up successfully. Independent schema/library/inquiry checks passed, with
no connection-close errors. This run held the successful pulse for 0.009220
seconds (1.375461890-second client call). The earlier failures remain retained.

The pre-review Python suite passed all 130 tests. `kafka-original-regression` also
passed all nine assertions in the original stricter reassignment mode, including
broker rejection of output from the returning old worker and unchanged committed
output/F/W. Its Docker cleanup passed. This targeted repeat preserves the earlier
complete fault matrices without unnecessarily rerunning every unchanged scenario.

## Independent review and correction

The explicit `advisor_review` inspected actual repository files and saved evidence
without an access limitation. It confirmed the successful native run but requested
a cleanup correction: a Docker-command timeout could bypass controller-session
cleanup and leave the saved runtime report marked passed.

Cleanup now aggregates command failures and continues independent container/network
checks, persists a failed experiment, and always attempts rollback/close of owned
controller sessions. Native fixture deletion requires both local and native-worker
cleanup to be verified. If that cannot be established, the exact fixture remains
with an explicit blocked-cleanup report; a stale successful runtime status cannot
make the command succeed. Regression tests inject a real `TimeoutExpired` and
check continued cleanup, failed/blocked reporting, and no fixture deletion.

The post-review Python suite passed all **134** tests (`python-08.log`), and
`git diff --check` passed. Java and the staged plugin remain unchanged; the live
run was not repeated for this exceptional-path Python correction. The live-04
harness therefore predates this correction; its successful cleanup remains
evidence of the normal path, and the new local tests establish the timeout path.
Advisor was called once; this is a corrected concern with subsequent verification,
not a claim of a second Advisor pass. Automatic native startup remains disabled.

[receiver-pending]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-journal-receiver-info-view
