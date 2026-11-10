# D/CG lifecycle discrimination and conservative savepoint handling

Status: **bounded native experiment and local implementation passed; transactional
streaming remains disabled**, 2026-11-07. This supersedes the stopping decision in
the [earlier lifecycle assessment](dcg-native-lifecycle-validation.md). Those
questions did not require an IBM support response before further useful work:
the source can be tested against a contrasting operation, and the implementation
can reject uncertain future behavior instead of guessing it.

The subsequent [incremental integration](dcg-incremental-lifecycle-validation.md)
implements record-by-record classification and automatic checkpoint dependency
retention inside the coordinator. The two-pass experiment described below is
preserved as earlier evidence; its manual checkpoint adjustment is not the new
integration path. Transactional streaming remains disabled.

## Answers supported by this work

### Savepoint lifetime need not be guessed

IBM documents that [SQL rollback to a savepoint][rollback] preserves the SQL
savepoint name and releases later savepoints. That does not imply that the same
journal C/SB sequence remains the usable marker. In the new native capture,
ROLLBACK TO S1 targets 89, writes a new SB at 94, then the next rollback targets
94 and writes SB 98. RELEASE S1 targets 98. A separately captured atomic block
failure similarly has SU 117 targeting SB 113, followed by SB 118 and SQ 119.
No meaning is assigned to undocumented C/SB payload bits.

The buffer now offers explicit `CONSUME_TARGET` behavior through
`SavepointPolicy.consumingTargets()`: undo to an existing target, remove that
target and its descendants locally, and reject a later reference to a consumed
sequence. A new C/SB can establish a new usable target. This can reject a valid
but unvalidated source pattern; it cannot silently retain or publish changes by
assuming an unknown marker is valid. It does not assert that IBM released the
SQL savepoint. Existing RETAIN_TARGET/RELEASE_TARGET behavior and the default
UNRESOLVED rejection remain unchanged. The new policy is not wired into streaming.

### A real journaling gap is distinguishable in the tested profile

The fresh isolated library `DCLF1107` had five single-member integer-key tables.
Before table creation, its journal attached a new receiver preserving the existing
job/user/program fields and adding thread recording. Both before/after images
were verified. Ordinary subtype 8 TRUNCATE and subtype 3 optimized DELETE produced
the same flagged companion sequences as the previous run.

For contrast, the test deliberately ended journaling on its own GAP file,
inserted 999, restarted journaling to its own journal, and inserted 1000. The
native interval contains F/EJ, D/EF, D/JF, F/JM with **flags zero**, followed by
the row for 1000. Independent source reads include both 999 and 1000. File/member
JIDs remain the same. Thus balanced end/start records and matching identities
alone demonstrably conceal a real missed row; they are insufficient criteria.
The flagged clear companions differ from this negative case.

The test-only `As400NativeLifecycleCandidate` recognizes a much narrower pattern:
an open cycle/savepoint, complete qualified producer including native thread,
paired typed identities, exact cycle-zero auxiliary flags/payloads, matched clear
subtype and nested level, and the matching C/SQ. For savepoint cleanup it requires
a known canceled clear, the matching F/MD/file/member sequence and a new C/SB at
the matching scope. Ordinary gaps, unmatched deletion, changed identities,
producers or recording settings, and incomplete sequences reject.

This is an offline, bounded **two-pass hypothesis checker**: it inspects every
returned record before returning a component projection. It is not an incremental
production classifier, a general ignore-flag filter, or proof that the pattern
holds across every IBM i release/PTF. It handles one receiver, one canceled clear
per rollback window and the observed recording profile; unsupported variations
reject. The existing runtime's stricter lifecycle rejection remains in place.

### Existing F/W can preserve rollback-tail context

W may be C/RB while its cycle-zero cleanup records appear later. Releasing all
context at W loses the relationship needed to validate that tail. The candidate
records the original C/SC, the rollback position and the validated tail end.
While W is before that tail end, F remains at the original C/SC, inclusive.
A later completed-output boundary beyond the tail can release this extra replay
requirement. No new checkpoint format is needed.

Two tests use the actual replay coordinator and serialize the adjusted F/W into
a fresh instance. They reconstruct the completed rollback without publishing any
row or truncate. Starting with only the tail is rejected. The candidate checkpoint
transformation is test-only and is applied explicitly; it is **not** installed
in the live offset publication path. Runtime integration must apply the floor
before every batch/heartbeat checkpoint, preserve it while a tail is incomplete,
and handle bounded incremental reads and interleaving without borrowing future
records from this two-pass checker. These remain implementation/validation work,
not an unresolved need to invent another checkpoint field.

## Native outcomes and verification

Six fresh cases yielded 61 native entries. A separate 106-entry history capture
includes creation and paired identity observations and overlaps the scenarios.
Raw pages and source checks are saved before cleanup. Sanitized fixtures retain
actual thread IDs, sequences, cycles, flags and payloads, with consistent mapping
of private header labels and JIDs.

| Case | Independently observed source result | Local result |
| --- | --- | --- |
| TRUNCATE commit | Only reload 101 | Clear then reload |
| Optimized DELETE commit | Only reload 101 | Clear then reload |
| Deliberate journal gap | 999 and 1000 exist; only 1000 is journaled | Reject complete gap sequence |
| Repeated SQL savepoint rollback | 10 and 13 survive; 11 and 12 do not | Consume/re-establish markers; emit only 10 and 13 |
| Duplicate-key multi-row INSERT | First 11 survives the error and later commit in this run | Preserve journaled 10,11,13; do not infer rollback from the SQL error |
| Explicit BEGIN ATOMIC failure | 20 and 24 survive; 21 does not | Undo 21 using native SU; emit 20 and 24 |

The last two cases are deliberately distinct. The first failed INSERT did not
provide a nested-rollback case, so the experiment added an explicit
[atomic dynamic compound statement][atomic], whose documented behavior supplied
the rollback oracle. Both outcomes are preserved, not rewritten into successes.

The new 40 local cases include thirteen positive native scenarios across old and
new captures, the real missed-write negative case, malformed/incomplete lifecycle
sequences, member-deletion correlation failures, consumed-target reuse rejection,
unchanged default-policy rejection and fresh-instance F/W tests. All **1,014 tests**
in the full `verify -DskipITs` run pass, including formatter/import validation and
the existing reader, startup, receipt and recovery tests. No local Kafka broker
experiment was repeated; the existing probe code/assertions are unchanged.

## Cleanup and limits

Independent queries verified final rows, object creation identity, empty table
locks, terminal captured cycles, receiver pending-transactions NO and no pending
work/savepoints in the observed source jobs. All five owned tables, both receivers
and the library were removed. Fresh schema, library and pending-inquiry queries
returned no rows. IBM reused the inquiry message key; each reply was checked
against its exact owned receiver text and cleanup job, not against key uniqueness.
No pooled source job was ended.

F/CR PR #114 and the original feature worktree remain unchanged. Nothing was
pushed. Paired identities are controlled-fixture candidates, not native startup
proof. Transactional streaming remains disabled; source history, baseline,
incarnation and incremental runtime integration gates are not waived by these
results. Optional IBM clarification could broaden support, but is not a
prerequisite for the conservative policy and bounded candidate established here.

Evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-lifecycle-discrimination/20261107/`.
It contains the capture/atomic/history raw pages and scripts, receiver metadata,
source outcomes, cleanup checks, fixture provenance, `verify.log`, `tests.json`,
incremental source patch and verification hashes.

[rollback]: https://www.ibm.com/docs/en/i/7.4.0?topic=statements-rollback
[atomic]: https://www.ibm.com/docs/en/i/7.5.0?topic=statements-compound-dynamic
