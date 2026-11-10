# Native thread identity: omission, not a decoder correction

Follow-up: a [fresh receiver configured to record threads](dcg-native-thread-validation.md)
now supplies nonzero native IDs and passes the component replay without thread
substitution. This report retains the earlier investigation and its zero-thread
negative fixtures; the full D/CG streaming gates remain open.

The saved native D/CG records already contain zero in the thread identifier's
eight bytes. The RJNE0200 decoder reads the correct field and the fixture
sanitizer preserves it. There is no supported retrieval/decoder correction that
can recover an unrecorded thread ID. Production code and transactional streaming
guards remain unchanged; a job-only identity or synthetic thread is not accepted.

## What the local investigation proves

The offline audit independently reads bytes 40 through 47, relative to each
entry header, as an unsigned big-endian integer. It compares the original native
page, the recorded Java decoding and the repository fixture. The adjacent
sequence, record number and commit-cycle fields also match their recorded values.

| Saved input | Entries | Thread field | Comparison |
| --- | ---: | --- | --- |
| Initial eight scenarios | 112 | All zero | Original page, recorded decoder and sanitized fixture agree |
| Seven D/CG scenarios | 175 | All zero | Original page, recorded decoder and sanitized fixture agree |
| Separate native history | 446 | All zero | Original page and recorded decoder agree |

The history overlaps the scenarios; these are not 733 independent journal
events. All 83 scenario R entries have job metadata but no thread value. The
sanitizer changes job labels, not the thread bytes. Its original files, all 287
scenario entries, the paired bindings and raw captures remain unchanged.

`EntryHeaderDecoder` uses the documented RJNE0200 offset 40 and an unsigned
8-byte conversion. The capture used `RJNE0200`, transaction controls enabled and
server file filtering disabled. Its retrieval selection cannot recreate data
that was not deposited. Neither the API entry-type selection nor null-map
formatting selects whether the journal stores thread identity.

## IBM semantics and the remaining uncertainty

IBM's [Retrieve Journal Entries API][retrieve] defines zero for this field when
thread recording (`FIXLENDTA(*THD)`) was absent, or fixed-length minimization was
enabled, when the containing receiver was attached. Zero therefore cannot be
treated as a known producer thread. The [fixed-length options][fixed] describe
`*THD` as the option for distinguishing threads within a job.

The fixture setup used `CREATE SCHEMA`, which creates its QSQJRN journal, and the
saved setup contains no explicit thread-recording configuration. This and the
raw bytes are consistent with source omission. **No per-receiver fixed-length
settings snapshot was saved**, so this investigation does not claim to have
independently verified the exact setting on the now-deleted fixture receiver.
It establishes that decoding and sanitization did not cause the zero value.

A new retrieval option or a change to today's journal settings cannot fill
these old bytes. [Changing fixed-length settings requires a receiver change][change].
The old captures remain negative fixtures for missing producer metadata.

## Required evidence before a positive native replay

For a future, explicitly authorized fresh isolated fixture:

1. Preserve the existing fixed-length fields and include `*THD` **before** the
   first transaction to be replayed. Do not use fixed-length minimization. If
   changing an already-created fixture journal, attach a new receiver and keep
   the sequence continuous. Do not alter a shared journal or discard history.
   Do not indiscriminately add `*LUW`/`*XID`; those identifiers remain unsupported
   by this normalizer.
2. Save exact journal/receiver names, sequence interval and attachment metadata,
   plus the per-receiver `FIXED_LENGTH_DATA_INCLUDES_JOB_NAME` and
   `FIXED_LENGTH_DATA_INCLUDES_THREAD_ID` values from
   [QSYS2.JOURNAL_RECEIVER_INFO][receiver]. Both must be `YES` for the required
   history; absent rows, nulls, unknown values or an interval crossing an
   unverified receiver do not establish this requirement. A current
   `JOURNAL_INFO` setting alone does not prove what an older receiver recorded.
3. Capture actual row entries and demonstrate nonzero thread values at the
   documented byte position, matching the decoder and complete job identity.
   Verify producer attribution across relevant before/after images and
   interleaved operations; do not require one thread per entire transaction.
   Metadata settings alone do not authorize an entry with a missing thread.
4. Replay without substituting thread values. Keep incomplete/mismatched object
   bindings, unvalidated journaling transitions and unresolved savepoints fatal.
   Passing the thread check alone does not pass native startup, recovery or C1.

The existing startup evidence adapter does not yet collect these per-receiver
thread/job fields. That evidence and its coverage through the F-to-scan interval
and reader handoff remain an explicit integration requirement. No new startup
proof or capability is inferred here. This investigation executes none of the
future source operations above.

## Regression coverage and evidence

Native header regressions compare all 287 captured values at zero and nonzero
buffer offsets. The latter places nonzero bytes before the real entry, so an
absolute-offset mistake cannot silently read zero and pass. No source thread
value is substituted in these tests. The existing seven missing-thread cases
also verify that retrying a rejected entry or feeding later CM/RB controls cannot
resume normalization or return a batch. Existing synthetic tests continue to
check nonzero unsigned IDs and reject mismatched job/thread/record update pairs.

All 251 tests in the native-transaction, source-normalizer, transaction-buffer
and replay-coordinator suites pass, with formatter/import validation. The broader
suite and Kafka experiment were not repeated: production and probe code are
unchanged from the preceding verified file/member identity work.

Advisor independently reviewed the actual files and saved evidence and passed:
the original-byte audit and regressions support retaining the producer guard,
with the historical receiver-setting uncertainty explicitly preserved.

The local evidence directory is:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-thread-identity/20261107/`.
It contains the reproducible offline audit, file hashes, `native-thread-audit.json`,
test logs and the incremental patch. Original live evidence stays in the sibling
`dcg-native-transactions/20261107/` directory.

Documentation lookup found no matching Context7 content; its advertised research
flag was unavailable. Direct IBM page fetches returned HTTP 403, so the findings
above use indexed IBM documentation, with links retained below. No live IBM i
connection, mutation, push or change to F/CR PR #114 occurred.

[retrieve]: https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm
[fixed]: https://www.ibm.com/docs/en/i/7.4.0?topic=journals-fixed-length-options-journal-entries
[change]: https://www.ibm.com/docs/en/i/7.4.0?topic=ssw_ibm_i_74%2Fcl%2Fchgjrn.html
[receiver]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-journal-receiver-info-view
