# Native abandoned-optimization and multiple-clear validation

On 2026-11-07, seven bounded scenarios in fresh edgeqa library `DCCV1107`
produced 185 native RJNE0200 entries. A separate history capture retained 248
entries, including creation/member-start observations used for fixture-only
paired JIDs. Raw pages, recording settings and independent source outcomes are
saved locally. This is native capture followed by local replay, not live connector
publication or production startup validation.

## New behavior established

An attempted optimized clear is not necessarily a completed D/CG operation.
After a successful truncate and reload, optimized DELETE attempted F/CH subtype 3,
then canceled its internal marker with C/SU and emitted ordinary R/DL instead.
The committed output must therefore preserve the first truncate, insert, row
delete and final insert; it must not manufacture a second truncate. Full rollback
of this combination restored the original rows.

Another attempted TRUNCATE returned SQL0910 (pending change, SQLSTATE 57007),
after F/CH and C/SU. The harness rolled the transaction back; its final source
rows were unchanged. This is a deliberately preserved SQL-rejection case, not a
successful second-clear observation.

Nested and non-top savepoint rollback cases completed and restored the expected
rows. Two-table rollback produced a complete clear for each table, then native
cleanup in reverse clear order. Full rollback used F/EJ, D/EF, D/JF, F/JM for each
table. Savepoint rollback used F/MD, D/EF, D/JF, F/JM for each table and one final
replacement C/SB after both sets. Two completed D/CG clears of the same table in
one rollback window were not established by these captures; the second optimized
attempt fell back or failed. No different unobserved tail ordering is accepted.

## Implementation

The incremental classifier now accepts an abandoned attempt only when C/SU is the
immediate next record from the same native producer, before F/EJ begins, and its
cycle, nested level and target match the exact opening marker. It forwards the
rollback control through the existing normalizer/buffer and retains SC-to-SU
replay context. It emits no clear for the abandoned attempt. Once journaling
transition begins, the complete existing validated sequence is still required.

Rollback windows retain one expected typed file/member identity per transition
and unwind clears in reverse operation order. The classifier does not release
held component records until every required tail, including a replacement marker
when applicable, has completed. Missing, changed, reordered or unflagged native
transitions still stop capture. Existing limits, conservative marker consumption,
real missed-write rejection and F/W dependency closure are retained.

The seven raw scenarios extend the existing real dispatcher, fresh-instance
restart-at-every-entry and real RPC-page tests (page sizes 1, 3 and 100). A local
projection of their emitted changes matches the independent native source rows.
Additional negatives cover wrong abandoned marker/cycle/level/producer, cancellation
after F/EJ, missing second-table cleanup, wrong table/flags and an early marker.
Interrupted delivery at all seven output boundaries of the mixed clear/row-delete
batch verifies replay of its tombstone and metadata as well as its data records.
Existing tests, native fixtures and Kafka experiments remain in place.

## Native identity observations and cleanup

The actual `As400JdbcNativeIdentityEvidence` collector successfully read the exact
journal, attached receiver, physical file and member paths twice using fresh
connections. Both samples returned equal numeric tuples for each object. This
confirms the deployed query works for this fixture. It does not establish identity
lifetime across IPL, restore, replacement or rollover, nor provide retained
protection, historical decoding or an atomic inspection. No candidate was promoted
to startup proof and no PTF compatibility range is claimed.

The fixture's receiver was changed before transactions to add thread identifiers
while preserving its existing fixed-length fields. Actual nonzero thread bytes,
sequence numbers, flags and payloads are retained in the sanitized test captures.
No synthetic thread identity was substituted.

Independent reads checked the final rows, exact owned object inventory, empty
locks, receiver pending-transaction flags and observed jobs' pending work. Every
captured cycle had a terminal outcome. Cleanup removed seven owned tables, both
owned receivers and the library. Only the exact verified cleanup job's two unsaved
receiver inquiries were answered. Independent checks confirmed the SQL schema,
native library and pending fixture inquiries were absent. No source job was ended,
no unrelated schema was changed, and no global reply policy was changed.

Transactional streaming and the current protected startup checks remain disabled
and unchanged respectively. F/CR PR #114 is untouched; nothing was committed or
pushed. The [remaining contract decision](dcg-completion-decision.md) concerns the
production source adapter, not the correctness of these observed transaction cases.

Evidence:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-completion/20261107/`.
The directory contains the scope, before snapshot, native controllers/classes,
raw captures, provenance, cleanup evidence, tests, incremental patch and review.

## Local verification

Full Maven `verify -DskipITs` passed 1,812 tests across 41 classes, including
formatter/import validation. After adding the explicit native final-row projection
assertion, the expanded lifecycle suite passed all 573 cases again. The 236 added
cases extend the existing suite; historical counts in earlier reports remain
unchanged. The final audit compares tracked and untracked files with the saved
baseline, verifies raw-fixture provenance and cleanup, and confirms the F/CR and
original feature worktrees are unchanged. Independent review is recorded alongside
the validation artifact. Advisor inspected the actual files and evidence and
passed the scoped lifecycle changes, cleanup and explicit pending contract decision.
It also confirmed the prior stop-admission correction. This is not approval to
enable transactional streaming or a pass for the remaining production gates.
