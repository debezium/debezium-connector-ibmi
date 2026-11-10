# Task-owned online boundary and recovery

This continues [native startup collection](dcg-native-startup-collection.md).
This report describes the test-entry qualification. The subsequent
[online configuration](dcg-online-configuration.md) connects that implementation
to an explicit default-off setting in the normal connector. The operator-configured
initialization/resume semantics and the separate F/CR worktree are
unchanged. Topic preflight is shared with that configured path and forbids
consumer-driven topic creation.
The experiment starts with an ordinary legacy offset; its controller supplies no
boundary bundle, schema bindings, object identities or candidate F/W checkpoint.

## Shared native implementation

The native journal/receiver window, short server-owned pulse, complete raw audit,
control reconstruction and prospective replay now have shared implementations.
The earlier fixture adapters delegate to those implementations. Metadata queries,
schema acquisition, history writes and Kafka acknowledgement remain outside the
allocate/marker/release interval. Ambiguous release, missing history, changed
identities, unsupported records and unresolved transaction outcomes still reject.

`As400LegacyNativePrefix` reads bounded owned native pages and invokes the same
legacy row handler and dispatcher as the RPC callback. Decode/dispatch failure
cannot advance its scan. The task registers and acknowledges a complete-output
Kafka barrier before calling any prefix durable. A cached update before-image
prevents that barrier. Native anchors, receiver continuity and identities are
checked before dispatch, independently from the background boundary collector.

The explicit `As400AutomaticHandoffProbeConnector` provider runs in the actual
task/coordinator and uses actual JDBC, JT400, dispatcher, Kafka history and Connect
offsets. A busy allocation defers the attempt while legacy row publication
continues. A successful candidate is stored as pending binding history; activation
still requires complete native history through the exact durable legacy barrier.
No custom owner registry or receipt gate grants publication permission.

The orchestration now resides in `As400NativeHandoffSession`. The test provider
supplies only isolated credentials, a test lifetime, evidence recording and crash
holds. The reusable component contains no fixture path, credential lookup or
test worker identity. Diagnostic raw-page conversion is lazy; a normal no-op
observer does not base64-copy pages. Construction still does not grant public
admission. Its internal online configuration reuses `As400DcgConfig`'s complete
output/converter/error contract and runtime checks, discovers inventory natively,
and pins a distinct `native-online-candidate-v2` policy. It cannot consume the
older simplified experiment fingerprint or silently enable transaction metadata,
RRN key conversion, an operator initialization, or a configured table inventory.

## Pending history survives replacement

Binding manifests retain a discovery floor and its native receiver identity,
separate from the schema origin. An old transaction can finish while a task is
down, so the replacement must not substitute a new pending-transaction census for
the saved floor. `As400BindingHistory.pendingOnlineCandidate` validates the complete existing
legacy offset and all history records without fabricating output F/W.

The replacement rereads C/J history from that floor and row history from the
schema origin, using saved physical bindings and newly verified native source
identities. It rejects a missing original transaction start, changed receiver,
incomplete handoff barrier or unavailable discovery floor. None of these records
permits receiver deletion.

The current candidate path writes immutable version-5 manifests. A durable Kafka
Connect F/W offset selects a manifest by its complete fingerprint, including its
origin, discovery floor, receiver identities, schemas and output policy. Before
F/W exists, the first canonical proposal is selected and the exact legacy barrier
must be acknowledged before F/W can be emitted. Binding history alone is never
permission to publish. No separate activation-history append occurs in this path.

This addresses a reproduced race: the schema-history producer is separate from
the fenced Connect source producer. An old task can finish a delayed history write
after its replacement has advanced. The earlier strict version-4/3 protocol rejects
that late proposal and loses restart availability. Version 5 permits canonical,
unselected proposals and identical duplicates without changing the selected epoch.
Malformed, foreign-source, mixed-version or incomplete history still rejects.
Recovery reads a fixed, bounded Kafka log prefix; later appends do not invalidate
that read, but removal, holes or truncation do. The existing strict reader retains
its moving-log rejection. There is no silent upgrade from the older experimental
history format: strict and version-5 history reject each other.

## Preserve the legacy output identity

The first actual-dispatch experiment exposed an output compatibility gap: legacy
JDBC table IDs contain the native database catalog, while the acquired physical
bindings use library/file IDs without a catalog. The selected schema topic naming
strategy includes the catalog when invoked by the normal task. The test adapter
therefore adds the verified native `CURRENT_SERVER` to the acquired output table
identity before persisting it. It preserves file/member journal IDs, physical
format, codecs, columns and keys; it cannot overwrite an existing catalog.
The mapping and server are pinned in the pending history fingerprint. A local
regression checks history round-trip and identical legacy/recovery topic names.
This is not general admission for arbitrary custom naming or schema mappings.

An initial legacy cursor at a before-image rebuilds that image from the independently
validated native anchor before replaying its update. The migration path also checks
the before/after pair's native producer, commit cycle, member JID and row identity.
A missing/mismatched pair or overlapping before-images for the same table rejects
before a progress barrier. This is conservative admission, not new support for
every legacy interleaving. Ordinary RPC behavior and its explicit lossy modes remain
separate. The native experiment checks both old-key deletion/tombstone and new-key
creation; local failure cases check that no record or position escapes a mismatch.

## Native evidence

Evidence is under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-task-boundary-20261109/`.

The component `native-05` run in DCNC0983 passed actual native control back-reading,
strict row preparation and independent marker validation while 200 writes ran.
Its healthy server ACQUIRED-to-RELEASED interval was 1.467 ms; collecting the
candidate took 57.844 seconds outside those allocations. Those observations are
neither a hard writer-pause bound nor Kafka publication proof. The saved candidate
explicitly says publication was not acknowledged. Native cleanup passed.

The automatic task attempts retain their failures rather than count them as
qualification: connector validation inherited a placeholder-host check, then a
synthetic required topic; a subsequent attempt used TLS against the isolated
server's non-TLS JT400 endpoint; the next caught the catalog/topic mismatch above.
The exact native libraries and local workers were cleaned up. DCNC0984 required a
separate cleanup retry after ODBC pooling reused a read-only connection; its final
library/schema/inquiry absence checks passed. Pooling is disabled before opening
fixture connections in subsequent attempts.

DCNC0988 and DCNC0989 passed the busy-boundary/committed-legacy-progress check,
then rejected an audit sequence gap before activation. The latter retains the
complete rejected raw page and independent committed Kafka state. Independent
read-only `DISPLAY_JOURNAL` calls with `INCLUDE_INTERNAL=NO` and `YES` identify
sequence 166 as an internal F/DE entry preceding opening marker A=173. Native raw
retrieval returns 171 entries; including internal entries returns 172. N=260 and
Z=331 bound an uninterrupted observed interval. Both fixtures were cleaned up.

IBM's [journal API documentation][journal-api] explicitly permits gaps for internal
entries; [DISPLAY_JOURNAL][display] documents the additional internal-entry option.
The corrected audit permits increasing, bounded gaps outside its actual inclusive
anchor/A/Z interval while retaining strict adjacency within that interval. It
still requires complete successful native retrieval, exact endpoints, exact
markers and unchanged native identities. Missing rows inside A/Z remain a failure;
the existing regression is retained alongside new before-A/after-Z gap cases.
This correction does not classify or accept hidden entries inside the boundary.

`automatic-07` / DCNC0990 recovered pending history in its replacement but failed
when Debezium classified an unnamed string-schema control record. The marker now
uses a named schema, with a regression using the real envelope classifier.
`automatic-08` / DCNC0991 passed its 13 Kafka assertions, then failed its final
source projection comparison because the harness expected unsorted IDs from an
ORDER BY query. The retained source rows have the expected contents; the failed
run is not counted as qualification. Both fixtures were cleaned up.

`automatic-09` / DCNC0992 passed all 13 assertions using the extracted native
session: busy-boundary deferral with committed legacy progress, pending-history
worker kill/replacement without another boundary, every concurrent row once with
no snapshot, receiver rotation and unrelated-table progress with an open clear,
fresh F/W recovery, rollback suppression, ordered committed truncate/reload,
primary-key delete/tombstone/create, completed drain and missing-offset rejection
without output/history changes. It delivered 448 concurrent inserts. Its observed
native ACQUIRED-to-RELEASED interval was 1.353 ms; this excludes allocation waiting
and does not establish a universal writer-pause bound. Native/local cleanup passed.
This run predates the stricter legacy before-image identity check and the complete
candidate configuration policy; later runs must qualify those changes separately.

`automatic-10` / DCNC0993 passed the new real legacy primary-key-update assertion
and busy-progress checks, then rejected an A/Z audit containing gaps 179→181 and
202→204. No handoff was accepted. Raw evidence, independent broker observations
and successful cleanup are retained. The missing entries in this run were not
independently classified; they must not be asserted to be the same internal F/DE
entry diagnosed in DCNC0989.

The scheduler now discards an unqualified audit and retries while the independent
legacy prefix keeps publishing. The audit itself still rejects exactly as before;
no pending history or checkpoint is created from it. Source/read failures, stop,
and any suppressed native release/drain error still fail. Local regressions cover
those distinctions. Native qualification of the retry and crash boundaries follows.

`automatic-11` / DCNC0994 passed 14 assertions, including a real rejected audit
followed by a successful retry, legacy key changes, and replacement after the exact
legacy barrier was durable but before activation. `automatic-12` / DCNC0995 also
passed 14 assertions, killing the worker after activation history but before F/W.
Both preserve ongoing unrelated-table output, receiver rotation, rollback,
ordered truncate/reload, missing-offset rejection and completed drain. All fixture
and local-worker cleanup checks passed. These runs used the older version-4/3
history protocol; they do not qualify the subsequent version-5 change.

## Version-5 verification

`candidate-history-broker-02` passed all 15 local Kafka phases and removed its owned
broker. The new phase writes a competing proposal and a duplicate through an
independent Kafka producer, recovers the selected epoch with a fresh reader, and
rejects missing offsets, use of the strict reader, and a corrupt unselected record.
Its offset input is controlled: this phase proves storage/selection, not native
source identity or a full distributed task handoff.

`As400OnlineHistoryRaceTest` also runs the real task/coordinator/dispatcher over
captured native records: after an open clear and a late proposal, a fresh task
recovers the selected F/W and publishes the expected commit sequence. Lost offsets
fail before output or history modification. Separate regressions retain strict
history behavior and exercise append, gap, removal and truncation at a fixed read
cut. The full local verification and Python harness checks pass.

`automatic-13` / DCNC0996 could not connect to Edgeqa: the server disconnected
during ODBC sign-on (`08S01`, `CWBCO1047`, communication return code 8405).
AWS credential retrieval succeeded. No fixture was created, no source output was
published, and no native version-5 validation can be claimed from that attempt.
`automatic-14` / DCNC0997 repeated the same pre-creation sign-on failure. Both
reports record `fixture_created=false` and `fixture_cleanup.status=not-created`.
The last created fixture, DCNC0995, was already verified absent.

The final local check (`verify-06.log`) passed 2,949 Java unit tests; the 136 Python
tests and assembly/formatter/import validation also passed. These checks do not
substitute for the blocked native version-5 run or enable public automatic startup.

Before the November 10 retry, the explicit `advisor_review` tool inspected the
actual repository files after verification and returned a pass: no actionable
defect in its targeted review.
It explicitly limited that result to a non-exhaustive review and retained the
blocked native version-5 validation requirement. The assembled connector's 212
class files match the compiled classes; protected worktree HEADs, logical indexes
and file hashes are unchanged. No commit or push was performed.

## November 10 retry findings

`automatic-15` / DCNC0998 connected and passed the pending-history worker
replacement check. It then failed at the harness's next `JOURNAL_INFO` observation:
the ODBC session, idle across the restart, was disconnected during `SQLPrepare`.
The writer had drained all 600 inserts without an error. Independent broker failure
observations and a replacement JVM thread dump are retained. That dump sampled a
JDBC sign-on read; the replacement subsequently progressed. The underlying reason
for the connection delay/disconnect is unconfirmed. The run is failed, not full
native qualification. Native fixture and local-worker cleanup passed; rollback
of one already-disconnected fixture session reported a connection error.

The automatic harness now opens short-lived read-only sessions for observations,
and closes acknowledged mutation sessions before long waits when no transaction
needs to survive. It never reconnects an open transaction or retries an uncertain
write. Three local lifecycle regressions verify distinct read-only sessions,
propagated read failure without retry, and registration of an unsuccessfully closed
session for final cleanup.

`automatic-16` / DCNC0999 exposed an idle-restart admission bug and timed out at
the unchanged handoff deadline. Its replacement was polling an idle legacy prefix;
the session required fresh progress before attempting handoff from the already
validated offset. Native and local cleanup passed and the failure is retained.
`As400LegacyNativePrefix` now separately exposes a complete position after a
successful native read/identity/anchor validation, even if no new entry was read.
It does not advance that position or emit idle progress markers. An incomplete
before-image blocks it, a failed read clears it, and terminal stop rejects access.
The session still reconstructs history and independently acknowledges the exact
HANDOFF and MIGRATION barriers. New local idle/idle-before-image cases pass.
The pending-history native case now drains the writer and waits for every row and
the legacy offset to commit before killing the worker, making idle recovery an
explicit requirement rather than relying on continuing source activity.

`automatic-17` / DCNC1000 passed all 14 assertions with that stronger idle case.
The stopped writer's last native row and durable legacy position were both 663;
the saved candidate origin was 555. The replacement completed handoff without
new fixture writes or another boundary, delivered all 600 concurrent inserts once
without snapshotting, and passed the open-clear/rotation/unrelated-table progress,
fresh F/W restart, rollback, truncate/reload ordering, key-change, completed-drain
and missing-offset checks. Native and local cleanup passed with no connection
cleanup errors. All 212 staged connector main classes match the current compiled
classes. This uses the explicit test entry point, not public automatic admission.

After the idle fix, `verify-07.log` passed 2,951 Java tests. `python-tests-07.log`
passed 139 Python tests, and `package-03.log` passed assembly and formatter/import
validation. The prior failed attempts remain in the evidence directory.

`automatic-18` / DCNC1001 passed all 14 assertions and native/local cleanup at the
acknowledged-legacy-barrier crash point. The replacement reconstructed the exact
barrier without a new native boundary or activation-history append. The source
classes are unchanged from the successful idle-restart run.

`automatic-19` / DCNC1002 passed its pre-checkpoint-publication crash/recovery
assertion but later timed out at the harness's `writer-complete` wait. Independent
broker evidence contains all 600 expected inserts through native sequence 662,
and the durable checkpoint is F=W=662. Receiver metadata retained before cleanup
reports a last sequence of 663. The raw entry at 663 was not retained, so its type
must not be inferred. The worker kept emitting heartbeats and reported no error.
Native and local cleanup passed; the overall run remains failed.

The harness had incorrectly required W to reach the attached receiver's metadata
tail, which need not be a CDC-selected record. It now waits for the exact expected
rows and for W to cover their actual native output coordinates. The existing
complete row list, order, no-snapshot and final source-projection assertions remain;
rollback/commit waits still use independently observed C/RB and C/CM records.
New cases reject ambiguous output coordinates and prove that an unrelated receiver
tail cannot replace the expected row coordinate. Each requested checkpoint target
is retained, and a bounded read-only `DISPLAY_JOURNAL(INCLUDE_INTERNAL=>'YES')`
observation saves any entries after the last expected writer row. No connector
offset or safety check was relaxed. `python-tests-09.log` passed all 141 tests.
An earlier AWS endpoint connectivity failure occurred before DCNC1002 creation;
its log is retained separately from this native attempt.

`automatic-20` / DCNC1003 passed all 14 assertions at the pre-checkpoint-publication
crash point, including exact barrier reconstruction with unchanged binding history,
all expected rows once, rollback, ordering, open-clear unrelated progress, receiver
rotation, a second task replacement, completed drain and missing-offset rejection.
The independent `writer-tail-native.json` capture identifies sequence 663 as U/DN
after the last writer row at 662 in this fixture. That entry is outside the C/D/F/R
CDC selection; the checkpoint is not required to advance to it. This independently
validates the revised harness target without assigning a type to the unretained
entry from DCNC1002. Native and local cleanup passed with no connection cleanup
errors.

The version-5 native crash matrix is therefore complete for the explicit test
entry point: saved pending history with an idle restart (DCNC1000), acknowledged
legacy barrier (DCNC1001), and immediately before new checkpoint publication
(DCNC1003). The latter two also verify no activation-history rewrite. All use the
same connector main classes. Earlier failed attempts are not counted as passes.
This resolves the native validation block described above; it does not enable
public automatic migration or broaden the supported schema/runtime profile.

The November 10 explicit `advisor_review` call returned a pass after inspecting
the actual files and targeted evidence: no actionable defect. Its evidence review
did not fully decode every raw broker record; it is not an exhaustive independent
reproduction of the native runs. `retry-20261110-validation.json` and
`retry-20261110-advisor.json` retain the result and limits. The F/CR, original
feature and earlier D/CG worktrees retain their HEADs, logical indexes and files.
No commit, push or public automatic admission was performed.

## Remaining admission gates

The test entry accepts a bounded, fixed-schema, no-transaction-metadata profile
with operator-managed retention and connector-managed Kafka transactions. Public
automatic admission, broader naming/converter compatibility and the separate
legacy transaction-metadata transition are not enabled by this experiment.
Native latency observations cannot establish a universal cancellation deadline.
Any public adapter must preserve these checks and the completed stricter tests.

[journal-api]: https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm
[display]: https://www.ibm.com/docs/en/i/7.4.0?topic=services-display-journal-table-function
