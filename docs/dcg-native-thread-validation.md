# Native thread recording validation on edgeqa

The bounded 2026-11-07 run confirms that a receiver configured to record threads
returns usable native thread identifiers. All 33 captured row entries contain
nonzero IDs, and raw-buffer replay preserves them without substituting synthetic
values. The existing producer-identity guard remains unchanged.

This closes the missing-thread obstacle for this fixture and recording profile,
not the broader D/CG release gate. Complete D/CG streams still stop at cycle-zero
F/CH, and the default unresolved-savepoint policy still stops C/SU. Transactional
streaming remains disabled.

## Isolation and recording settings

The fresh library `DCTH1107` was independently verified absent on edgeqa (`AEQA`,
IBM i 7.5) before creation. Only its journal, receivers and ten small tables were
changed. The tables each began with three integer IDs. No shared journal, server
job, QAQQINI setting or production connector was changed.

The automatically created journal initially recorded `*JOB`, `*USR` and `*PGM`.
Those fields were preserved and `*THD` was added with an explicitly continuous
receiver change **before table creation and test transactions**:

- `QSQJRN0001`: sequences 1–3, thread recording `NO`.
- `QSQJRN0002`: starts at sequence 4, job/user/program/thread recording `YES`.
- Program-library, system-sequence, remote-address, LUW and XID options retained
  their original `NO` values. Fixed-length minimization was already disabled.

Per-receiver settings, names, ranges and attachment metadata were saved before
and after the change, before and after capture/history reads, and before cleanup.
All scenario intervals stayed inside `QSQJRN0002`, with its recording flags
unchanged. The old receiver was retained until the owned fixture was cleaned up.
These checks follow IBM's [thread-field semantics][retrieve], [receiver metadata
fields][receiver] and [receiver-change requirement for FIXLENDTA][change].

A `JOURNALED_OBJECTS` metadata query hit SQL0666. An exact-library
`OBJECT_STATISTICS` query verified all ten files use `*BOTH` images; no query
threshold was raised. A release query was corrected to the installed view's
available columns. Both initial errors and successful checks are retained.

## Captures and offline results

The eight scenarios produced 180 captured entries, including five D/CG subtype 8
clears, five subtype 3 clears and 33 R entries. The row thread IDs are 100, 197,
323 and 42384. A separate 269-entry history capture includes file creation and
member-journaling observations; it overlaps the scenario captures.

| Scenario | Source outcome, independently checked | Offline component result |
| --- | --- | --- |
| TRUNCATE commit | Target contains only reload 101 | Marker insert, truncate, reload in order |
| Optimized DELETE commit | Target contains only reload 101 | Marker insert, truncate, reload in order |
| Both full rollbacks | Original 1,2,3 restored; transaction marker absent | No committed output batch |
| Both repeated/nested savepoint cases | Original 1,2,3 plus final 303; rolled-back reloads absent | Reject unresolved C/SU before any batch |
| Interleaved transactions | B contains 501; A contains 401,402 | Later-started B commits before A; each clear precedes its reloads |
| Update commit | 1 becomes 11; 2 and 3 retained | One update with before=1, after=11 and matching native job/thread/RRN |

The component input consists of all C controls plus R and D/CG operations. It is
not a production filtering policy. Separate complete-stream attempts retain the
other records: all seven clear scenarios reject F/CH before any batch. Those
native F/CH entries have cycle zero and reach that guard before operation
classification. The ordinary update scenario succeeds with its complete captured
stream. No cycle, thread, savepoint or journaling field was rewritten to obtain
these results.

Raw replay uses the real strict decoder, source normalizer and transaction buffer.
It checks paired file/member identities, relative record numbers and original
source metadata, then overwrites the transport buffers to verify owned values.
The actual UB/UP pair retains the same qualified producer and record number.
This run does not establish all possible IBM i job/thread lifetime semantics or
permit a job-only identity when thread information is missing.

Bindings are controlled-fixture candidate observations from D/CT `*FILE` and F/JM
`*QDDS` records, checked against subsequent clear and row JIDs. They do not prove
protected identity history, native incarnation or startup handoff. No checkpoint,
Kafka publication or sink validation was performed in this run; the existing
F/W recovery code and tests are preserved.

## Cleanup and regression coverage

Independent usql checks confirmed all final rows, exact object creation identity,
empty table locks, terminal captured cycles, receiver pending-transactions `NO`
and no pending work/savepoints in observed source jobs. Ten owned tables were
dropped with RESTRICT, followed by the owned schema. No pooled job was ended.

IBM i reused a message key for the second unsaved-receiver inquiry. The first
cleanup controller stopped on its duplicate-key guard. Cleanup then resumed only
after rechecking the second receiver's exact text, the original cleanup job and
that job's library locks. Both replies concerned the two owned fixture receivers.
Fresh queries confirmed the schema, library and pending inquiries were absent.
Cleanup completion is established by those independent queries, not an inferred
child-process exit code. All raw evidence was saved before removal.

`As400NativeThreadReplayTest` adds 24 local cases using 180 sanitized native entries
and paired bindings. Native thread bytes, sequences, commit cycles, row numbers,
payloads and flags remain exact; only documented private header labels/JIDs and
standalone framing are sanitized. The original zero-thread fixtures and stricter
tests remain unchanged. All **275 relevant tests** pass with formatter/import
validation. The inherited Maven archunit execution repeats the same explicitly
selected tests; the reported count is not doubled.

Advisor independently reviewed the actual files, native bytes, replay tests and
cleanup evidence and passed the bounded result. The remaining D/CG gates stay
explicit and unchanged; the saved review is `advisor-review.json`.

Production sources, transactional streaming guards and F/CR PR #114 remain
unchanged. Nothing was pushed. The broader unit suite and Kafka experiment were
not repeated because production/probe code did not change.

## Evidence and remaining work

Local evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-thread-validation/20261107/`.

- `receivers-*.json`, `capture-receivers-*.json`, `history-receivers-*.json`:
  recording settings and source interval coverage.
- `capture/` and `history/`: original native pages and recorded metadata.
- `paired-bindings.json`, `raw-replay.json`, `native-thread-evidence.json`:
  candidate identity observations, actual-byte replay and independent byte audit.
- `fixture-provenance.json` and `sanitization-map.json`: original-to-test mapping.
- `safe-to-cleanup.json`, `cleanup.json`, `*-absent.json`: independent source
  outcomes, drain checks and verified removal.
- `focused-tests.log`, `validation.json`, `change.patch`: local verification and
  the incremental change, including untracked files.

The subsequent [lifecycle/recovery assessment](dcg-native-lifecycle-validation.md)
isolates a source-semantics blocker and records 20 additional local cases: restart
cannot bypass the unsupported native tails, and both savepoint target-lifetime
policies fit the current final-row observations. A local unpublished technical
question packet identifies the authoritative clarification needed. Savepoint
mapping, protected file/member association, startup/restart and end-to-end
publication gates also remain open. Future capture/recovery preflight must verify
job/thread recording for every required receiver; this successful fresh fixture
does not repair historical records from a receiver that omitted those fields.

[retrieve]: https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm
[receiver]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-journal-receiver-info-view
[change]: https://www.ibm.com/docs/en/i/7.4.0?topic=ssw_ibm_i_74%2Fcl%2Fchgjrn.html
