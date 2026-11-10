# Bounded native D/CG transaction validation

Follow-up: the [paired file/member identity model](dcg-file-member-identity.md)
addresses the object-identity representation below using local tests. Original
rows then reach the separate missing-thread guard; full native integration stays
blocked. This report retains the results of the original live run.

The edgeqa run on 2026-11-07 captured **both actual D/CG subtypes** with the expected
source commit/rollback outcomes. Offline replay found a concrete integration
blocker: the source normalizer assumes a single `*FILE` identity, but native row
records use the member data object's `*QDDS` identity and a different journal ID.
The adapter stopped before returning any output batch. **C1 is not passed and
transactional streaming remains disabled.**

## Scope and source results

The fresh isolated library was `DCTG1107` on edgeqa (`AEQA`), IBM i 7.5. Installed
database PTF group SF99950 was level 11; the full group inventory is saved. Eighteen
small, single-member fixture tables each began with three integer IDs. SQL ran
in dedicated JDBC sessions; inspection and fixture management used usql. Fast
DELETE used `OVERRIDE_QAQQINI` in the test job's QTEMP with
`SQL_FAST_DELETE_ROW_COUNT=1`. No shared QAQQINI settings were changed.

| Case | Native observation | Independently checked source outcome |
| --- | --- | --- |
| TRUNCATE commit | D/CG.8 inside a nonzero cycle; clear then reload 101 | Target contains only 101; preceding marker-table insert committed |
| TRUNCATE rollback | D/CG.8, reload 201, compensation and C/RB | Original IDs 1,2,3 restored; marker insert absent |
| TRUNCATE savepoints | Two D/CG.8 clears; nested savepoint, two rollbacks to SQL S1, release and commit | Original IDs plus final 303; rolled-back 301/302/304 absent |
| Optimized DELETE commit | D/CG.3, no individual forward row deletes for the clear, reload 101 | Target contains only 101; marker-table insert committed |
| Optimized DELETE rollback | D/CG.3, reload 201, compensation and C/RB | Original IDs restored; marker insert absent |
| Optimized DELETE savepoints | Two D/CG.3 clears with the same SQL savepoint sequence | Original IDs plus final 303; rolled-back reloads absent |
| Interleaved | A starts with D/CG.8; B starts with D/CG.3 and commits first; then A commits | B contains 501; A contains 401,402; C/CM order is B then A |

Eight earlier control cases remain recorded. A pending insert into the same table
caused TRUNCATE to return SQL0910 / SQLSTATE 57007; optimized DELETE instead fell
back to individual R/DL entries. Moving preceding work to a separate marker table
allowed the D/CG cases above. TRUNCATE IMMEDIATE produced **F/CR with cycle zero**:
rollback undid its later insert, while the clear persisted. These observations
prevent mistaking a successful SQL statement or a DELETE fallback for D/CG coverage.

The run was bounded to these 15 scenarios, 18 small tables and two transaction
connections at a time. Each JDBC statement had a 12-second query timeout; native
sockets had a 15-second timeout. Each capture allowed at most 16 one-MiB pages
and 5,000 entries; the controller allowed 240 seconds per invocation. Driver
timeouts are test bounds, not a general native cancellation guarantee.

## Raw capture and local replay

The existing `RetrieveJournal` and real `QjoRetrieveJournalEntries` transport ran
with all entry types, C/D/R/F codes, no file filter, and transaction controls
enabled **only in the standalone capture harness**. Unmodified RJNE0200 response
buffers, entry offsets, resolved receiver, SQL operations, SQL errors, source
snapshots and runtime class/jar hashes are retained. The seven D/CG scenarios
contain 175 entries, including ten clears (five of each subtype). The eight
controls contain 112 entries.

A separate native history read returned 446 entries through sequence 515.
Independent `DISPLAY_JOURNAL` returned 515 metadata entries over the same history.
All C/R/D-CG sequence/code/type inventories match; non-C object types and JIDs
also match. For C controls the SQL JID field is populated while the native header's
object JID is zero; these are not interchangeable physical-object bindings.
Among C/D/R/F the only omissions from native retrieval
were 23 internal F/DE deleted-record-count entries. Other SQL inventory entries
use codes outside that native profile. No claim of an unfiltered all-code capture
or complete historical continuity is made.

Offline replay used the **unchanged** production `EntryHeaderDecoder`,
`CompleteRowEntryDecoder`, `FileChangeEntryDecoder`, `TransactionControlEntryDecoder`,
`As400SourceNormalizer` and `As400TransactionBuffer`. All 51 forward row images
in the two scenario sets decoded successfully; D/CG subtypes and C controls also
decoded without errors. Both all-scoped-record replay and component-contract
replay (all C plus R/D-CG) stopped before output. No Kafka producer, checkpoint
authority, dispatcher, sink, or live connector was run in this experiment.

The diagnostic table bindings use the first observed row identity and the known
fixture layout, not a verified bootstrap. They cannot prove native identity or
history. Six D/CG component cases reject their first `*QDDS` row; the interleaved
case reaches D/CG first and rejects its file JID against the member binding.
There is no fallback or resumed processing after a rejection. The buffer latches
abort and retains diagnostic state until close, which releases it.

## Findings and next implementation boundary

1. **Separate file and member identities.** D/CG carries `*FILE`; R entries carry
   `*QDDS`. Native history shows the member JID already present in the seed rows
   and unchanged through the clear/reload. The distinct file JID is not evidence
   that the member was replaced. IBM documents this object-type distinction in
   [QJORJRNE](https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm) and the
   [journal identifier information API](https://www.ibm.com/docs/en/i/7.4.0?topic=ssw_ibm_i_74%2Fapis%2FQJORJIDI.html).
   The next implementation should model and validate the file-to-member binding;
   simply allowing another object type while comparing one JID would be wrong.
2. **Preserve journaling transitions for validation.** Actual clears include
   F/CH, F/EJ, D/EF, D/JF and F/JM around D/CG; rollback includes further such
   transitions. They must not be silently discarded as harmless metadata. The
   captured traces support investigating their relationship to the clear, not
   relaxing source continuity checks.
3. **SQL savepoint names do not imply one persistent native marker.** Repeated
   rollback to S1 produced a new level-2 C/SB between C/SU targets. Internal
   level-3 savepoints also occurred. The traces are preserved, but the default
   unresolved-savepoint rejection remains. Full rollback/compensation, nested
   SQL patterns and broader release coverage still need an approved mapping.

This is native evidence for a subset of C1, not a successful normalized native
transaction publication test. Identity/history, startup, replay/restart,
framework integration and source-to-sink gates remain open. No custom dependency
was removed, no production code changed, and F/CR PR #114 was untouched.

## Cleanup and verification

Every source outcome was checked independently through usql. The exact object
inventory and creation timestamps matched the owned fixture, and all 18 table
lock checks were empty. Sixteen observed native commit cycles had terminal CM/RB
boundaries. Idle pooled-job commitment definitions remained registered after
JDBC close: the captured jobs were in RESET with no pending changes or savepoints.
No pooled server job was ended or changed.

Owned tables were dropped with RESTRICT. After all files/members were gone, two
idle definitions were still registered; a restrictive drop of the owned schema
succeeded. Only its exact unsaved-receiver inquiry was answered after matching
the receiver, library, message key and cleanup job's object lock. Fresh queries
verified schema, library and pending-inquiry absence. No existing auction or
preexisting DTEST library was altered.

The 35 new offline fixture tests pass; all **882 unit tests**, Maven verify,
assembly packaging and formatter/import validation pass. Native observations
and adapter rejection are tested separately. Sanitized repo fixtures contain all
287 scenario entries; unmodified captures and provenance remain local. The
initial test run incorrectly expected abort to free state immediately; assertions
now verify the actual latch-then-close lifecycle without changing production.

Advisor inspected the actual files and saved evidence and passed with no new
actionable issue. It confirmed the bounded native results, cleanup and explicitly
blocked normalization integration; it did not rerun tests or live operations.

Evidence directory (local, not published):
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-transactions/20261107/`

- `capture/`, `capture-dcg/`, `history/`: original native response buffers and manifests.
- `independent-journal.json`, `cleanup-transaction-boundaries.json`, `cleanup-rows-*.json`: independent journal and final source checks.
- `replay-initial.json`, `replay-dcg.json`: raw offline decoder/adapter/buffer results.
- `NativeTransactions.java`, `NativeDcgTransactions.java`, `NativeHistory.java`, `NativeReplay.java`, `run*.py`: finite capture/replay controllers; credentials passed in memory.
- `fixture-provenance.json`, `runtime*-sha256.json`, `before.json`, `validation.json`: fixture derivation, tested runtime and scope preservation.
- `cleanup.json`, `schema-absent.json`, `library-absent.json`, `inquiries-absent.json`: verified removal; earlier guarded cleanup stops remain recorded.
- `verify.log`, `package.log`, `advisor-review.json`: build and independent review evidence.

SQL behavior references: [TRUNCATE](https://www.ibm.com/docs/en/i/7.4.0?topic=statements-truncate),
[DELETE](https://www.ibm.com/docs/en/i/7.5.0?topic=statements-delete),
[job-local QAQQINI override](https://www.ibm.com/docs/en/i/7.5.0?topic=services-override-qaqqini-procedure).
