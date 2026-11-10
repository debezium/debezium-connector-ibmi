# Native binding acquisition and controlled initialization

Status: implementation and bounded native qualification, 2026-11-07.
Transactional D/CG streaming remains disabled. This is an ordinary operator-retention
initialization adapter, not an enabled source task or automatic snapshot on missing offsets.
F/CR PR #114 and its worktree are unchanged. No push or publication is part of this work.

## Native bindings

`As400NativeBindingAcquisition` obtains physical file/member identities and their distinct
journal IDs from `As400OperatorTableEvidence`, then reads exact, parameterized
`QSYS2.SYSCOLUMNS2` and primary-key catalog queries. The effective codecs come from native
metadata, not the fixture DDL or permissive legacy JDBC decoder. It validates physical
field order, buffer start/length, storage bytes, native record size/count, SQL/system
mapping, key order, nullability and explicit CCSID. It recollects the paired native objects
and format after acquisition. Initialization also repeats the entire acquisition, including
keys and columns, before writing history and before accepting the initial offset.

The first profile requires a primary key, basic system table/member names, one member,
contiguous full physical fields, and the existing strict codec set. Hidden fields,
field procedures, distinct types, temporal/LOB/XML/pointer layouts, multibyte varying
strings, implicit/binary text CCSIDs, partial results and query warnings reject.
Catalog SQL/metadata privileges and the PTF level exposing `BUFFER_POSITION` and
`BUFFER_LENGTH` are prerequisites. Missing columns do not fall back to inferred offsets.
Existing complete-row checks for null indicators, minimized/defaulted images and
unsupported source records remain intact. No source CCSID remapping is introduced.

Repeated observations detect changes; they do not themselves provide a writer or
administrative fence. The returned acquisition is usable for initialization only under
the controlled session contract below. Recovery still uses the accepted history epoch,
not a current catalog read during an open clear.

## Complete starting transaction context

`As400NativeTransactionCensus` checks the effective authority of its own SQL job using
`SQL_CHECK_SPECIAL_AUTHORITY('*JOBCTL')` and
`SQL_CHECK_FUNCTION_USAGE('QIBM_DB_SQLADM'/'QIBM_DB_SYSMON')`. At least one documented
cross-user access path must be available before and after enumeration. A missing API,
unknown result, lost authority, warning, row-bound overflow or duplicate definition rejects.
It never grants authority or treats a successful restricted SELECT as completeness.

It enumerates all commitment definitions and their resources for this journal, without
filtering on captured files or pending counts. A non-null commit-cycle ID rejects even
when pending changes are zero. Matching resources must have zero pending changes, no
cycle, RESET state, local SYSBAS scope and no XA branch state. Prepared, distributed,
partial and unresolved states reject. RESET alone, zero pending changes and completed
rollback percentages are not accepted as proof of a terminal transaction.

This check is valid **under a held journal-wide initialization writer fence**. It does
not invent one from a catalog read, a quiet interval, an empty result, or locks on the
captured tables. The fence must cover writers on uncaptured tables and API resources
that could join this journal. Once initialization finishes, this census is not polled
as a requirement for ordinary CDC; unrelated committed transactions can continue while
a clear remains open.

## Initialization ordering

`As400NativeInitialization.initialize(Session)` is an explicit operation. It requires
a session implementing the existing controlled-initialization requirements:

1. Establish a durable enrollment and exclusive output ownership; hold the journal-wide
   writer/DDL/maintenance fence and required operator retention through handoff.
2. Check complete empty transaction context, acquire native bindings, and prepare/materialize
   the bounded snapshot under the fence. Only then probe an exact
   readable native boundary B. The existing raw cut reader validates journal/receiver
   identities, recording options, the anchor bytes and the unchanged tail. A temporary
   probe cursor is internal and cannot escape as an initial source offset. Empty or
   unreadable boundaries reject; there is no synthetic W=1 or fabricated commit marker.
3. Deliver and durably acknowledge the prepared snapshot/new downstream generation
   at B without additional source reads. Any required sink barrier belongs to this acknowledgement. The session returns
   an immutable baseline receipt; enqueueing output or returning from SELECT is insufficient.
4. Recheck fence epoch, empty census, complete bindings and the same B. Persist one
   binding-history record containing the full schema/codecs, enrollment epoch and baseline
   receipt. Recover it through a fresh backend before exposing a dependent checkpoint.
5. Recheck the source again. The session atomically checks admission/ownership, installs
   the exact initial F=B (exclusive), W=B and reader continuation, and awaits durable
   acknowledgement. The initializer returns only after this callback and a final held-fence
   check. The caller still owns the fence; this adapter never releases it automatically.

A failed snapshot, write/readback failure, lost fence, changed keys/schema/identity,
changed boundary, nonempty census or uncertain offset acknowledgement cannot return
success. History can remain after an interrupted handoff. Normal resume still rejects
missing offsets even with that history present; recovery/re-enrollment is explicit.
There is no new external binding/ownership store. The standard schema-history backend
holds the initialization receipt alongside the binding epoch (history-record version 2);
existing version-1 binding histories remain strictly readable with their original scope.
Neither version bypasses the missing-offset rule.

`Session` is an integration contract, not a shipped production fence provider. A callable
boolean declaring the source quiet is not a valid implementation. No task configuration
selects this adapter. Deployment-specific writer exclusion, snapshot output and Kafka
Connect initial-offset/reader admission still require concrete integration and qualification.
The existing optional protected-retention/receipt path is unchanged.

## Acceptance and evidence

Local tests cover the captured native mixed-column metadata, missing/ambiguous/unsupported
layouts, key/column drift, partial authority and census results, a cycle with zero pending
changes, an uncaptured-file cycle, missing boundary bytes, failed snapshot acknowledgement,
history write/ack faults, fence loss before/after history, schema drift after history,
failed offset installation, baseline-receipt tampering and fresh-instance missing-offset
rejection. Existing stricter recovery, lifecycle, cancellation and Kafka tests are preserved.

The fresh owned edgeqa library `DCNI1107` has captured A/B and a mixed-layout M,
plus uncaptured U. Acquisition uses the actual catalogs for all three captured tables.
M covers integer widths, packed/zoned decimals, fixed/varying text with CCSIDs 37/500,
fixed/varying binary, floats and nullable fields. The effective SQLADM authorization
check succeeded. A transaction on U was detected and rejected before any checkpoint;
rollback restored a successful empty census. No shared auction tables were touched.

The initial native attempt exposed a real ordering requirement: the snapshot SELECTs
added C/BC (cycle zero) after the originally chosen B. The adapter rejected the moved
boundary without creating an offset. Snapshot preparation now runs before boundary
selection, under the same held fence. A regression requires preparation before the
first boundary observation. A separate probe-only CHGJOB attempt was denied before
initialization and removed; no extra authority was requested or granted.

The corrected native matrix uses five separate JVM phases: native metadata/census,
initialization, owned source transactions, recovery, and missing-offset rejection.
Initialization verifies native `*EXCLRD` locks for A/B/M/U, materializes the bounded
snapshot before B, fsyncs and rereads a local snapshot, persists version-2 binding
history in a real local Kafka broker, independently consumes that record, and only
then fsyncs the initial offset. A fresh JVM loads bindings from history without
current table metadata, reads actual source entries, and passes them through the
strict decoder, normalizer, lifecycle buffer and test dispatcher. Expected output is
`t:B`, `c:B:100`, `c:A:8`, `c:M:2`. The complete M row with ten null fields is also
checked directly against the recovered physical decoder. Missing offsets reject.
The test dispatcher projects ID; this does not qualify full mixed-column Connect
output conversion. Raw captured bytes and both F/W checkpoints are retained locally.

This qualifies the native adapters and orchestration in an isolated controller-owned
source. Native file locks plus control of every fixture writer do not establish a
general fence against API producers or administrative actions on an existing journal.
The baseline and offset stores in this probe are local fsynced files, not Kafka Connect
source offsets or a production sink. Real Kafka is used for binding history. No native
source jobs outside the fixture, user privileges or shared journal settings are changed.

The test-only entry point is `As400NativeInitializationProbe`; it requires explicit
owned-library/local-broker arguments and receives credentials through stdin. It is not
an automatic integration test or a production connector mode.

## Remaining production gates

- Implement and qualify the session with the actual snapshot/output/Connect task lifecycle,
  including atomic owner admission at initial-offset installation and stop/drain behavior.
  The isolated controller is not a general journal-wide writer-fence provider.
- Validate the production handoff and a fresh task replacement against live open TRUNCATE
  and optimized DELETE transactions, while unrelated committed output continues.
- Preserve operator-managed retention, schema/identity/history failure checks, receiver
  rollover/deletion tests and downstream convergence requirements before enabling streaming.
- Initialization rejects a nonempty/ambiguous census rather than waiting without a bound.
  A source that cannot supply the session's fence needs a separately proved initialization
  protocol; catalog equality or table-only locking must not be substituted silently.

References: [SYSCOLUMNS2](https://www.ibm.com/docs/en/i/7.4.0?topic=views-syscolumns2),
[buffer-field availability](https://www.ibm.com/support/pages/qsys2syscolumns2),
[DB_TRANSACTION_INFO](https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-info-view),
[DB_TRANSACTION_JOURNAL_INFO](https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-journal-info-table-function),
[effective function authority](https://www.ibm.com/docs/en/i/7.5.0?topic=services-sql-check-function-usage-scalar-function),
[LOCK TABLE scope and lifetime](https://www.ibm.com/docs/en/i/7.5.0?topic=statements-lock-table).

## Final verification record

Maven verify passed **2,297 tests across 52 classes**, with no failures, errors or skips.
Checkstyle, formatter/import validation, assembly packaging and diff checks passed.
The preserved fourteen-phase local Kafka history matrix passed against the changed
history adapter. The final native five-JVM matrix is `live/kafka-04`; initial F/W
was 101 (exclusive) and recovered completed output reached 119. Earlier `kafka-01`
and `kafka-02` are retained failed reproductions, not successful initialization proof.
`kafka-03` passed before final test-harness formatting; `kafka-04` is the final repeat.

Evidence is under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/native-initialization/`:
verification/package logs, `tests.json`, `kafka-regression/report.json`,
`live/kafka-04/report.json`, independent broker history, raw replay, baseline and
checkpoint files, native lock evidence and exact runtime hashes. Credentials stayed
in memory/stdin and were not written to source/configuration artifacts.

Independent final checks confirmed A=[1,2,3,8], B=[100], M=[1,2], U empty,
no fixture file/journal locks, no pending receiver transactions and the exact owned
inventory. Cleanup dropped only the four owned tables and `DCNI1107` using RESTRICT.
Only CPA7025 inquiries for its three owned receivers and verified drop job were answered.
The schema, library and outstanding inquiries are absent (`live/cleanup.json`). All
owned local Kafka containers were removed and absence verified.

Advisor reviewed the actual repository files and saved evidence and passed with no
actionable defect. Actual-file access succeeded. Advisor did not rerun the native
tests, and its IBM census documentation request returned HTTP 403. The implementation
research separately retrieved the documented census/authority semantics through IBM
search results; this is not an independent native rerun. The single explicit review
is saved as `advisor-review.json`. The audit confirms the protected worktrees and
transactional streaming guard are unchanged and the final native runtime hashes match.
