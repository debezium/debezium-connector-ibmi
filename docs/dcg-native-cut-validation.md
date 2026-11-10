# Native ordinary-cut continuation

Status: bounded edgeqa IBM i 7.5 qualification, 2026-11-07. Transactional streaming
remains disabled. This does not qualify a production task, change F/CR PR #114,
install a retention mechanism, or authorize publication.

The user renewed AWS access. Evidence is local under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/`:
`live-02` contains the owned source experiment, raw bytes, exact helper sources,
class hashes and process results; `native-cut-resume` contains local verification.
Credentials stayed in memory and Java stdin. The isolated library was `DCOB1107`.

## Exact empty-selection semantics

An attached receiver can contain only unselected journal records. In the native
example, receiver 4 contained J/PR at sequence 46. The C/D/F/R selection returned
CPF7062. JT400 escape-message mode returned `run=false` and no output buffer, so
that result remains rejected. A J selection returned the actual record.

The corrected raw request uses a 256-byte **by-reference** QUS error-code parameter.
Transport success alone is insufficient: missing receiver CPF9801 and invalid
range CPF7054 also returned `run=true` with an empty-looking response. Each must
be rejected by its structured error record.

Only CPF7062 together with a complete, exactly 64-byte returned RJNE0200 header,
zero entries, zero first-entry offset and no continuation is classified as empty.
Missing, contradictory, oversized or truncated error/response structures reject.
Success requires an unambiguous zero-error structure. API failures cannot contribute
partial entries. Legacy retrieval/error handling is unchanged, and raw failures
still cancel only their captured job. Empty selection does not waive receiver
identity/range verification, re-observation or the F/W anchors.

`raw-empty-01` preserves escape-mode, missing-receiver and invalid-range controls.
`raw-empty-structured-01` was an invalid pass-by-value attempt (CPF3CF1), not evidence
of empty semantics. `raw-empty-structured-02` has the valid by-reference results
and raw error/response buffers. Each diagnostic saved and ended its exact server
jobs. The exact CPF7062 error and returned header are regression fixtures under
`journal-parsing/src/test/resources/raw-journal/`.

## Native observations and replay

The fixture's SQL-created journal initially used REMOVE_INTERNAL_ENTRIES=YES.
The candidate correctly rejected it. Only this owned journal was changed: a new
receiver retained MAXOPT2, fixed fields and thread recording while removing
RMVINTENT. No production guard was relaxed. Current journal settings do not prove
that every historical receiver had the same policy.

The actual journal/receiver, exact native object and table-format collectors ran
together with `RetrieveJournal.readRawPage`. Native record-format discovery returned
AS400Bin4, four bytes, nonnullable ID for A/B. Its format ID was empty; SYSFILES
provided the physical format level. The helper's integer-only bindings and explicit
fixture fingerprint are **not** a general production codec/bootstrap adapter.

The controller created an explicit checkpoint at the owned quiescent fixture's
sequence 29. This is an artificial baseline for the experiment, not a production
transaction census or snapshot protocol. Strict decoding, normalization, ordinary
lifecycle/buffering and the real Debezium dispatcher then produced:

- A key change: delete ID 1, insert ID 4.
- Committed B truncate/reload: truncate, insert ID 100, with F=W=43.
- A newly attached idle receiver: no output, unchanged F/W.
- Appending A ID 5 after that idle interval: exactly one insert; a fresh restart
  emitted nothing and preserved the new checkpoint.

`raw-cut-02` preserves the original idle-receiver rejection; `raw-resume-01`
passed using the corrected structured-error handling. The dispatcher emitted
in-memory SourceRecords. No live connector delivered this fixture to Kafka or a sink.

## Open clear: safe rejection, availability limitation

During an uncommitted SQL clear, the B SYSPARTITIONSTAT query waited and returned
no member. The candidate rejected the entire cut before it could reach publication.
This is an observed metadata-availability gap, not proof that the table was removed.
Do not substitute cached identities or discard the table to make this read pass.

Two earlier attempts also exposed diagnostic timeout mistakes. With JT400's
background reader enabled, a 15-second receive timeout expired otherwise idle
transaction/RPC connections. One rollback exception hid the original metadata
failure. The preserved diagnostic now records the original error, suppressed
rollback error, SQL stages and exact JDBC job identifiers. Later diagnostic
connections used `thread used=false` with a bounded receive timeout, as documented
by IBM. Production connection settings and cancellation paths were unchanged.
The probe retained its 180-second outer bound.

`raw-recovery-01`, `-02` and `-03` independently recovered these abandoned/finished
attempts from saved F/W. They emitted only the unrelated committed A rows (6, 8,
10), with no rolled-back B output. The fixture's independently queried rows matched.

`raw-open-cut-02` passed both cases with the corrected diagnostic settings:

- Open TRUNCATE plus B reload and an independent A insert: the metadata read failed,
  output remained empty, and the input checkpoint was unchanged. After commit,
  a fresh cut/decoder/coordinator emitted A ID 11, B truncate, B ID 400 in that order.
- Open optimized DELETE plus B reload and an independent A insert: the open cut
  again failed without output/checkpoint change. After rollback, replay emitted
  only A ID 12. B remained ID 400.
- Fresh-instance replay after each completed case emitted nothing and preserved F/W.

This qualifies safe failure and recovery; it does **not** demonstrate live progress
of unrelated captured tables while a clear remains open. The current whole-cut
policy pauses the entire captured set until its table observations become available.
The user initially selected temporary pauses, then explicitly rejected pausing
unrelated tables after the potentially unbounded duration was explained. The
whole-cut candidate therefore does not satisfy the accepted runtime requirement.
First evaluate whether documented current observations can be obtained during the
open clear. If they cannot, keeping unrelated commits flowing requires an explicit
historical-binding design and validation. Missing current observations cannot be
accepted as proof. Local interleaving/F-before-W tests
remain valid component evidence but do not establish this native liveness property.

The subsequent [open-clear progress experiment](dcg-open-clear-progress.md)
confirms that raw native reads contain unrelated committed rows before the clear
finishes. Local replay with controller-qualified retained bindings delivers those
rows and preserves F/W across restart. Several current metadata APIs remain
unavailable during the clear. Durable binding-history/runtime integration is still
required; the whole-cut candidate has not been made permissive.

## Missing retained history and cleanup

After all source transactions ended and locks/ownership/inventory were checked,
the controller deliberately deleted only detached owned receivers 1, 2 and 3,
oldest first. The attached receiver 4 stayed intact. This was manual disposable
fault injection, never a connector permission to release retention.

`raw-missing-history-01` rejected the old F/W=43 checkpoint because its required
receiver was missing, returned no output and left that checkpoint unchanged.
The newer checkpoint whose F/W were in retained receiver 4 resumed successfully,
without output or checkpoint movement. History loss cannot silently choose a newer
suffix or initialize a fresh stream.

Final independent source checks found A=[2,3,4,5,6,8,9,10,11,12], B=[400], no file
locks and no pending transactions. Cleanup removed A/B and then paused on an added
check for zero implicit commitment definitions. That count is not a pending-change
count: IBM retains commitment definitions across completed transactions. The follow-up
saved the count of one, reconfirmed zero journaled source files/members, no journal
locks and no receiver pending work, and used DROP SCHEMA RESTRICT with its normal
dependency checks. No reused server job was ended. Only the verified owned CPA7025
receiver inquiry was answered. Schema, library and pending-inquiry absence passed;
`live-02/cleanup.json` records the result. Failed attempts and raw evidence remain local.

## Verification and remaining gates

Regression tests cover the native empty header/error pair, contradictory error/data,
failed transports, required F/W anchors, re-observation changes and temporary missing
member observations before/after retrieval. A fresh read must reconstruct from the
unchanged durable checkpoint; rejected speculative bytes never become an offset.

Production still needs native codec/schema policy integration, complete JDBC/RPC
terminal admission/drain, an explicit consistent baseline/census, and the actual
native source-task-to-Kafka/sink restart/failure matrix. Current observation tokens
remain restricted ordinary-profile evidence, not protected startup/maintenance proof.
The protected startup and receipt path, existing Kafka fault matrix, and the
nonzero-commit-cycle streaming guard remain intact.

References:
[QjoRetrieveJournalEntries](https://www.ibm.com/docs/en/i/7.4.0?topic=q-retrieve-journal-entries-qjoretrievejournalentries),
[JT400 socket timeouts](https://www.ibm.com/support/pages/ibm-toolbox-java-socket-timeout),
[DLTJRNRCV](https://www.ibm.com/docs/en/i/7.5.0?topic=d-delete-journal-receiver),
[Commitment definitions](https://www.ibm.com/docs/en/i/7.4.0?topic=concepts-commitment-definition).

Current local checks passed: Maven verify with formatter/import validation ran
2,115 tests across 46 classes (194 journal tests and 1,921 connector tests), with
zero failures, errors or skips. Assembly packaging and diff whitespace validation
passed. Report aggregation uses only classes present in this verify log; stale
Surefire files from earlier runs are excluded. Existing Kafka tests were preserved;
the prior checkpoint's Kafka regressions were not rerun for this raw-response fix.
Protected F/CR and original feature worktree hashes and HEADs were unchanged.

Advisor inspected the actual repository files and saved native evidence and passed
with no actionable defect. Access succeeded. Advisor did not independently rerun
native experiments or validate production-task integration. The single explicit
review result for this continuation is saved in `native-cut-resume/advisor-review.json`.
