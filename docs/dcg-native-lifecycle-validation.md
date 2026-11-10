# D/CG native lifecycle and recovery assessment

**Subsequent result:** the [native discrimination experiment and conservative
policy](dcg-native-lifecycle-discrimination.md) supersede this assessment's stopping
decision. Additional native cases and a more restrictive algorithm permit progress
without first obtaining IBM clarification. The results below remain the historical
scope of this earlier assessment; they do not describe the latest candidate.

Status: **blocked on source semantics, not enabled**, 2026-11-07. This is an
offline continuation of the [native thread validation](dcg-native-thread-validation.md).
It reuses the actual native captures; no new IBM i connection, mutation, fixture,
Kafka run or publication occurred. Production sources and all existing guards
remain unchanged. F/CR PR #114 is independent.

## Decision

Do not implement a permissive lifecycle filter or choose a production C/SU policy
from the currently passing component examples. The available evidence does not
yet justify either. This is a concrete stopping point for enabling this design,
not a conclusion that D/CG replication is impossible or that all IBM interfaces
have been exhausted. A documented rule or IBM clarification is needed before the
remaining lifecycle handling can be implemented safely. A local, unpublished
[technical question packet](dcg-native-lifecycle-questions.md) records the exact
questions and required follow-up tests.

The current native startup/history evidence gaps also remain open. Resolving
lifecycle classification will not by itself supply a protected file/member
binding, a consistent initial baseline, receiver-incarnation proof, or approve
the candidate ordinary-CDC support contract.

## What the native captures establish

Every raw page used here was cross-checked against the existing sanitized test
fixture. All 180 entries preserve their native sequence, commit cycle, thread,
nested level, flags and entry-specific payload. File/member JIDs are paired
candidate observations from the prior controlled fixture, not startup proof.

The TRUNCATE commit example is:

| Sequence | Entry | Cycle | Observation |
| --- | --- | --- | --- |
| 106 | C/SC | 106 | Top-level transaction starts |
| 108 | C/SB | 106 | Nested level 2; one opaque payload byte `80` |
| 109 | F/CH | 0 | Member identity; payload is EBCDIC `8` |
| 110–113 | F/EJ, D/EF, D/JF, F/JM | 0 | Each has ignore-on-apply/remove bit `0x10` |
| 114 | D/CG | 106 | Subtype 8, file identity, nested level 2 |
| 115 | C/SQ | 106 | Targets the C/SB at 108 |
| 116–117 | Row reload, C/CM | 106 | Reload followed by durable commit |

Subtype 3 optimized DELETE has the analogous observed sequence. End/start records
use the existing member/file JIDs. The start records have indicator `1` and
payload `100`: IBM documents the indicator as before-and-after images and the
three payload characters as omission, inheritance and remote-filter settings.
That layout describes recording settings; it does not attach these cycle-zero
records to a particular transaction or establish historical continuity.

There are two additional boundary cases:

- Full rollback C/RB at 135 is followed by cycle-zero F/EJ, D/EF, D/JF, F/JM at
  136–139. The DELETE rollback similarly ends at 222 with a tail at 223–226.
  A restart immediately after W=C/RB can therefore begin inside this lifecycle
  tail. A classifier relying only on an in-memory open cycle would lose context.
- Savepoint rollback includes F/MD member-deletion records with **flags zero**
  and **commit cycle zero**, followed by file/member journal-start activity.
  The TRUNCATE examples are 163 and 182; DELETE examples are 250 and 269.
  Thus a rule based only on the ignore bit would not handle the complete stream.

The source's final rows were independently checked in the prior live run. These
observations show that the captured operations completed as intended, not that
all similarly shaped records can be treated as harmless housekeeping. The native
retrieval profile also omits some internal F/DE entries; numerical adjacency must
not be substituted for a proved complete history profile.

## Documentation findings and limits

- IBM's [entry catalog][types] recommends processing D/CG instead of the legacy
  F/CH companion. F/CH is therefore not itself evidence that a second truncate
  must be emitted. Treating only F/CH as advisory still leaves the F/EJ boundary.
- The [retrieve API][retrieve] defines the ignore flag in terms of APYJRNCHG and
  RMVJRNCHG. The reviewed reference does not establish a general CDC exemption or
  a complete correlation rule for these cycle-zero lifecycle records.
- The [entry catalog][types] identifies F/MD with actual member/file removal and
  describes C/SB's entry-specific data as internal. We do not assign supported
  meanings to observed `40`/`80` bytes.
- The [variable layouts][layouts] provide the 20-character C/SQ and C/SU target
  sequence. They do not settle the target's lifetime after C/SU. The catalog's
  C/SU prose repeats the C/SQ release/commit description, so it cannot resolve
  that ambiguity. The [sequence reference][sequence] does identify SU as rollback.
- IBM's [SQL Replication processing table][replication] specifies full refresh
  on F/EJ, including system-caused events, and attempted refresh on F/MD. This is
  evidence for that product's documented behavior, **not** proof that every IBM
  replication product needs refresh or that a safe D/CG classifier cannot exist.
- IBM's [JID explanation][jid] says a restore attempts to preserve the saved JID.
  Matching JIDs alone consequently cannot replace the identity/history gates.

Some direct IBM Docs requests returned HTTP 403. The assessment used indexed
official reference content and accessible IBM support pages, not an inferred
layout from secondary sources. No claim of an exhaustive IBM support search is
made. A later authoritative answer may remove these blockers.

## Local regression results

`As400NativeLifecycleRecoveryTest` adds 20 cases to the existing native fixtures:

- Seven complete clear streams stop before publishing. R stays at the last
  accepted input, F remains at the open C/SC and W remains at the previous
  completion. A fresh coordinator reconstructed from serialized F/W stops at the
  same unsupported record with no output or checkpoint advancement.
- Seven explicitly counterfactual projections omit only F/CH. They still stop
  on the native F/EJ without publishing or advancing the checkpoint. This is not
  a proposed production filter.
- Two component-only rollback replays reach C/RB, then reject the native tail.
  A new instance starting after that W also rejects the tail. This demonstrates
  fail-closed recovery, not successful full-stream rollback support or Kafka
  offset durability.
- Two savepoint component cases run both existing policies. RELEASE_TARGET and
  RETAIN_TARGET produce identical correct final changes but different retained
  savepoint state. These captures therefore do not select one policy. The
  production default remains UNRESOLVED; existing rejection tests remain intact.
- Two cases inspect both unflagged F/MD entries in each savepoint capture and
  require rejection with zero output and an unchanged checkpoint.

All 295 selected tests pass, including the earlier native, normalization, buffer
and replay tests, with formatter/import validation. The explicit Maven selection
is repeated by the inherited archunit execution; 295 is the unique case count,
not the doubled execution count. These tests use fixture-only checkpoint anchors
and bindings; they do not exercise native startup authorization or a real offset
store. No production or Kafka-probe source changed, so the broader suite and
local Kafka experiment were not repeated.

Advisor reviewed the actual files and evidence and passed this scoped result:
the semantics blocker is justified, and the tests support keeping rejection and
recovery guards without claiming that D/CG is impossible. The saved result is
`advisor-review.json` in the audit directory below.

## Evidence

Local audit directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-transition-validation/20261107/`.

- `analyze-native.py`, `native-observations.json`: raw/fixture byte checks, typed
  trace inventory, terminal tails and saved cleanup evidence.
- `verification.log`, `tests.json`: final selected test and formatting results.
- `before.zip`, `before.json`, `change.patch`, `validation.json`: incremental
  changes, hashes and protected-worktree checks.

Original pages and cleanup evidence remain under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-thread-validation/20261107/`.
The prior library/schema/inquiry absence checks still read `[]`; this offline
assessment created no new fixture requiring cleanup.

[types]: https://www.ibm.com/docs/en/i/7.5.0?topic=information-all-journal-entries-by-code-type
[retrieve]: https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm
[layouts]: https://www.ibm.com/docs/en/i/7.5.0?topic=information-layouts-variable-length-portion-journal-entries
[sequence]: https://www.ibm.com/docs/en/i/7.6.0?topic=objects-sequence-journal-entries-under-commitment-control
[replication]: https://www.ibm.com/docs/en/idr/11.4.0?topic=configuring-how-capture-program-processes-journal-entry-types-sql-replication-system-i
[jid]: https://www.ibm.com/support/pages/node/641447
