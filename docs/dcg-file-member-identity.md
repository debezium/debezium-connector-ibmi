# Separate file and member identities for D/CG normalization

The local normalization model now requires two distinct journal identifiers:
the physical file's `*FILE` JID for D/CG and the single member's `*QDDS` JID for
row images and rollback compensation. It no longer models the file and its data
member as one journal object. Transactional streaming remains disabled.

## Enforced contract

`TableIdentity` contains the logical table, physical file and member names,
`fileJournalId`, `memberJournalId`, and the pinned record-format identity. Both
JIDs must be canonical 20-character lowercase hex values, nonzero, not an omitted
space value, and different from each other. There is no single-JID constructor,
inference from adjacent values, or fallback to a name-only match. `TableBinding`
also requires qualified physical names and a decoder.

| Input | Required object type | Required JID | Name/member checks |
| --- | --- | --- | --- |
| D/CG subtype 3 or 8 | `*FILE` | `fileJournalId` | Header and payload must agree with the bound file/library; any supplied member must match. A blank member is usable only within the caller's already proved single-member scope. |
| R/PT, PX, UB, UP, DL | `*QDDS` | `memberJournalId` | Exact file/library/member match plus the existing producer, record-number, row-layout and metadata checks |
| R/BR, DR, UR | `*QDDS` | `memberJournalId` | The same member and producer checks; still classified as compensation, never forward row changes |

The source metadata retains the actual entry's object type and JID. Row identity
still includes the qualified producer and physical relative record number, and
owns the complete paired table identity. Buffered values remain owned copies.

Both IDs participate in table equality, so either changing while a captured cycle
is pending stops the buffer. Both are included under named roles in the capture
fingerprint, using the new `table-file-member-v2` identity hash domain. The outer
`v1:` hash encoding and F/W offset codec are unchanged, but a previous single-JID
fingerprint cannot resume under the new model. There is no automatic checkpoint
migration or reset. Startup also rejects JIDs aliased by another captured
file/member binding, rechecks both IDs before reader handoff, and includes the
whole pair in the protected history window.

## What still must prove the relationship

Constructing a pair validates its representation; it does **not** prove that
the two objects belong together. The existing verified-startup contract remains
responsible for the following evidence before returning a production binding:

1. Resolve both JIDs against the exact journal, under a known journal/system
   incarnation. The file result must be `*FILE` with the expected library/file;
   the member result must be `*QDDS` with that same library/file and the exact
   nonblank member. Missing, truncated, unresolved, mismatched or wrong-type
   results are rejections. Neither a neighboring JID nor two entries with equal
   names proves this association.
2. Verify the exact single-member scope and physical decoder layout under the
   same protected inspection. Bind this to the logical capture mapping and
   fingerprint. An extra member, ambiguous alias, changed format, or inconsistent
   repeated observation blocks startup.
3. Prove that the association, identities and decoder remain valid over the
   required F-to-scan/recovery interval and inspection-to-reader handoff. Current
   catalog equality cannot prove absence of prior replacement, rename, member
   change, altered/restored format or journaling interruption. Retain the existing
   historical evidence and protection requirements; reject when proof is absent.
4. Audit the F/CH, F/EJ, D/EF, D/JF and F/JM transitions captured around native
   clears and rollbacks. This change neither whitelists them nor proves that a
   particular transition is harmless because surrounding names/JIDs agree.

IBM's [QJORJIDI API](https://www.ibm.com/docs/en/i/7.4.0?topic=ssw_ibm_i_74%2Fapis%2FQJORJIDI.html)
can resolve a JID's **current** type, object/library and member within a specified
journal. It returns member names for `*QDDS`/`*QDDSI`; `*QDDSI` is not an accepted
row-data identity here. Its IFS file-identifier field does not provide a database
file/member incarnation. This is a candidate current-association observation,
not historical identity or retention proof. The
[retrieval API](https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm) documents
separate `*FILE`, `*QDDS` and `*QDDSI` object kinds. No new live collector or startup
proof promotion was added. The existing JDBC evidence adapter still refuses to
manufacture a verified startup observation.

## Captured-fixture validation and limits

The original 287 captured scenario entries are unchanged. The new test-only
`native-transactions/bindings.json` associates their consistently sanitized JIDs
using the saved D/CT file-creation and F/JM member-start observations in the
original native history. The member JIDs also match saved seed/reload rows.
This records the controlled fixture's observed association; it is not trusted
runtime bootstrap evidence and cannot establish native lifetime semantics.
`binding-provenance.json` preserves the original observations and mapping locally.

An additional pre-existing rejection is now reachable: every captured row has a
zero thread identifier. The normalizer's producer-identity guard still rejects
it. Original-record tests require that rejection before any returned batch; no
production change accepts a missing thread or substitutes a job-only identity.
The [thread investigation](dcg-native-thread-identity.md) confirms zero is already
present in the original pages and the documented decoder offset is correct.
Positive native replay needs fresh source records with thread recording enabled
and verified per-receiver evidence; no safe job-only fallback is established.
The subsequent [thread-enabled native validation](dcg-native-thread-validation.md)
supplies those records for one isolated fixture and passes component replay with
actual thread values. It preserves the remaining journaling/savepoint rejections
and does not promote candidate file/member bindings to startup proof.

To isolate the file/member change from that unresolved input requirement, clearly
named **component tests** clone the row headers and supply synthetic thread ID 1.
They do not edit the saved fixtures. With that explicit assumption and the
component input contract (all C controls plus R/D-CG operations):

- Both committed clear/reload paths retain source metadata, paired identities,
  owned values and clear-before-reload order after input bytes are overwritten.
- Full rollback returns no batch; interleaved transactions retain B-before-A
  commit order.
- Savepoint traces still reject the default unresolved C/SU disposition.
- Feeding the complete captured stream, even with synthetic thread metadata,
  still rejects the unvalidated journaling transition before output.
- Swapped, incomplete, omitted, aliased and mismatched bindings are rejected;
  member-only changes invalidate buffering, fingerprint and startup handoff.

These passing component cases are not native streaming, startup, checkpoint or
sink validation. C1 and the broader release gates remain open.

## Verification

All 928 unit tests pass, including the updated raw-native regressions and existing
strict recovery/ownership, reader-cancellation and terminal-stop tests. The
previous single-JID fingerprint was reproduced from the saved pre-change source
and is rejected by startup regression coverage.

The existing local Kafka ownership probe's interrupted-delivery scenario also
passes with the updated synthetic file/member identities. It exercises actual
Kafka Connect task/dispatcher delivery, fresh-worker F/W recovery, complete-batch
replay, row/source metadata preservation and ordered output without custom owner
or receipt gates in the candidate path. Its source is synthetic and it does not
validate native identity evidence. All 13 checks and owned Docker cleanup pass.
Assembly packaging and formatter/import validation pass as well.

The independent Advisor review passed the scoped file/member identity change
after implementation and verification. It found support in the actual code,
tests and saved evidence, with native validation limitations remaining explicit.
The saved review is `advisor-review.json` in the evidence directory below.

Evidence for this local change is under:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-file-member-identity/20261107/`.
Original live evidence remains under the sibling
`dcg-native-transactions/20261107/` directory. No IBM i connection or mutation,
commit, push, or change to F/CR PR #114 occurred during this follow-up.
