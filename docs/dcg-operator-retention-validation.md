# Ordinary CDC adapter validation

Status: local implementation and bounded IBM i 7.5 evidence, 2026-11-07.
The user selected operator-managed retention in
[the completion decision](dcg-completion-decision.md). This report does not enable
transactional streaming, qualify a production startup adapter, or modify F/CR PR #114.

## Implemented components

- `RetrieveJournal.readRawPage` requests an explicit inclusive range in one named
  receiver. It selects C/D/F/R, all entry types, no object/file filters, and
  `*VARLEN` null indicators. It copies complete entries and uses IBM's continuation
  receiver/sequence, without numeric adjacency assumptions or legacy scan updates.
  It rejects transport failures and API errors except the specifically verified
  empty CPF7062 result described in the continuation report. It rejects partial/malformed results, unsupported
  pointer handles, excessive results, and a buffer too small for the next entry.
  A rejected response requests cancellation of its captured native job while that
  read is still registered. It never applies the legacy oversized-entry skip.
- `As400OperatorTableEvidence` performs bounded, parameterized, read-only current
  observations. Exact file/member paths independently return their JIDs and native
  object tuples; SYSFILES supplies the physical format level, length and field count;
  SYSPARTITIONSTAT requires one member and an exact SQL/system-name mapping. Unknown,
  missing, duplicate, warning-bearing, remote, symbolic, non-system-ASP, nonphysical,
  LOB or mismatched observations reject. These are observations, not a protected
  `Source/Inspection` implementation or historical layout proof.
- `As400OperatorJournalEvidence` reads a bounded current journal/receiver directory,
  requiring a local active journal in system ASP 1, unminimized retained receivers,
  job/thread recording and the supported fixed-field profile. The candidate rejects
  journal caching rather than treating a delay as a durable-tail guarantee. Its
  combined query/profile passed the later bounded native execution described below;
  this remains current observation, not historical recording-policy proof.
- `As400OperatorCutReader` composes the concrete observation collectors and real raw
  reader. Resume requires an existing version-2 checkpoint before any source I/O.
  It verifies fingerprint, logical table associations, native journal/receiver
  tokens, reciprocal receiver links, ranges and both F/W anchors. A cut has finite
  page/entry/byte bounds; table and native object observations must match afterward.
  The attached tail may grow beyond the captured cut; attachment changes, shrinking
  ranges, failed pages and ambiguous history reject the entire cut. These native
  tokens are specific to the candidate ordinary profile and are not protected
  startup proof. Native lifetime/source binding and task integration remain gates.
- An explicit ordinary mode adds cycle-zero row/F/CR handling to normalization,
  buffering, replay and the real dispatcher. A pending UB retains F until its exact
  producer/table/RRN UP completes. It preserves native cycle zero and does not emit
  invented transaction BEGIN/END events. Transactional changes retain their existing
  commit/rollback/savepoint behavior. The two buffers share entry/payload bounds.
- The ordinary lifecycle mode accepts positions in a caller-verified receiver chain.
  Its live-cut check permits an unfinished lifecycle while retaining F; it cannot
  accept an unreconstructed durable W. The default strict profile still rejects
  cycle-zero input and receiver changes. Ordinary snapshots retain W at or after
  the replay floor, including when a new UB/transaction begins after W. This gives
  operators one history-retention floor while preserving the W anchor required by
  startup. No task configuration enables ordinary mode.

## Native experiment and preserved failures

The fresh, owned `DCOA1107` library contained only integer-key tables A and B and
its journal/receivers. Thread recording was added before table creation, preserving
the other fixed-length options. Original captures, scripts, source observations and
class hashes are under the local evidence directory
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/`.

The complete ordinary capture contains 40 selected records, including its sequence-24
baseline anchor. It covers an ordinary key-changing update, delete and insert,
CLRPFM, SQL TRUNCATE IMMEDIATE, committed transactional TRUNCATE, and optimized DELETE
followed by rollback. The immediate clear emitted F/CR. The transactional paths
emitted D/CG subtype 8 and subtype 3 respectively. Actual thread bytes and payloads
are retained in the sanitized `ordinary-cdc.json` fixture.

A second transaction starts in QSQJRN0002, performs a transactional truncate, changes
to QSQJRN0003, reloads and commits. The capture contains 13 selected records including
its sequence-66 anchor. Reciprocal receiver links, ranges and recording settings
were collected. Numeric gaps at receiver boundaries include unselected journal
records and are not treated as proof of lost data.

Preserved attempts establish three important boundaries:

1. D/CT creation records contain native pointer handles even for integer-only tables.
   The production parser rejects them. Initialization after table creation must
   establish its baseline explicitly; later unsupported DDL cannot be hidden by an
   entry-type filter. The initial diagnostic attempt predated failure cancellation;
   it disconnected transport but did not save/prove termination of that server job.
   Later diagnostics captured and ended their exact jobs. No pointer data was used.
2. A 2 KiB buffer cannot hold the observed D/CG record (4,800 bytes). It rejects rather
   than skipping. The complete read used an 8 KiB buffer and native continuation.
3. The experimental NBRENT=2 request returned two entries and continuation=0 despite
   later selected entries in the requested range. That attempt is **not complete
   capture evidence**. The option was removed. A request regression explicitly
   excludes key 6; byte/page/entry bounds remain enforced locally with fail-on-limit.

## Logical association and physical object replacement

Current file/member JIDs and record-format identifiers stayed unchanged through the
validated clears. The underlying IFS file/generation tuples changed after committed
TRUNCATE and returned to their previous values after rollback. The rollover probe
intentionally failed its whole-observation equality assertion and preserved the
before/after evidence. Its source transaction and raw capture completed; this is
not a passed startup-identity test.

Do not weaken the strict native identity collector or interpret equality as a
maintenance/protection lease. The ordinary adapter must distinguish the journal
association used for decoding from the native object observed during a particular
read. It must validate current association, fixed layout, exact history and allowed
lifecycle transitions before publication. An unexpected tuple change during a
bounded read still discards that read; it cannot be dismissed just because a table
name matches. Native journal/receiver incarnation and historical binding validation
remain integration gates. The accepted operator restrictions exclude unsupported
restore/clone, maintenance and schema changes; they are not evidence that these
operations are technically prevented.

## Local replay and cleanup

Tests replay the mixed native history through the strict row decoder, normalizer,
transaction/autocommit buffers and real Debezium dispatcher. They cover fresh
instances after every native entry, interrupted output including key-change
tombstones and truncate/reload, receiver-spanning restart, missing offsets, missing
or mismatched update images and the unchanged strict-mode rejections. Synthetic
interleavings additionally check pending UB across another transaction's commit,
and a completed UP whose UB precedes F. Those orderings are explicitly synthetic.
The final row projection agrees with the independently queried source state.
Additional tests exercise missing/legacy offsets before I/O, changed fingerprints
and object identities, receiver forks/resets/missing links, missing F/W anchors,
partial reads, size limits, native continuation and changes during re-observation.
Raw transport regressions verify that failures and pointer-backed responses end
only the captured native job and clear its registration without advancing position.

Final independent source checks found A=[11], B=[200], no file locks and no pending
transactions in the three owned receivers. Cleanup dropped only A/B and the owned
library/journal/receivers, answered only verified owned receiver inquiries, and
confirmed schema, library and inquiry absence. No live connector published these
captures to Kafka, no production system was changed, and nothing was pushed.

## Remaining production gates

The new components are not a complete source task. The concrete cut adapter now
checks bounded current observations and loaded F/W/fingerprint locally, but its
combined native execution, source/transport binding and native identity lifetime
still require qualification. It must acquire and pin actual row codecs and carry
the existing terminal-stop/admission barrier through its complete JDBC/RPC path.
The no-selected-entry case is now characterized and guarded by the structured
API error record; see the continuation report for its exact scope. First initialization needs
its explicit consistent baseline/census procedure; normal resume must reject missing
offsets. The complete native source-task-to-Kafka/sink restart and failure matrix
must pass before introducing an enablement setting. Existing protected startup,
receipt and Kafka experiments remain intact.

Primary API references: [Retrieve Journal Entries](https://www.ibm.com/docs/en/i/7.4.0?topic=q-retrieve-journal-entries-qjoretrievejournalentries),
[IFS_OBJECT_STATISTICS](https://www.ibm.com/docs/en/i/7.5.0?topic=services-ifs-object-statistics-table-function),
[SYSFILES](https://www.ibm.com/docs/en/i/7.5.0?topic=services-sysfiles-view).

## Native qualification continued after access renewal

The expired-session boundary was resolved after the user renewed AWS access.
The fresh `DCOB1107` experiment and empty-response correction are recorded in
[the native cut continuation](dcg-native-cut-validation.md). Both owned libraries
have now been removed and their absence checked. The source-task integration and
availability questions in that report remain open; a passed bounded capture is
not an enabled connector.

## Final local verification for this checkpoint

Maven `verify -DskipITs` with formatter/import validation passed all 2,096 unit
and fixture tests (46 test classes). Assembly packaging and `git diff --check`
also passed. The exact current source was packaged into the local Kafka probes:

- Connect-only mixed interrupted delivery: passed, 13 assertions.
- Connect-only missing/malformed/incompatible offsets: passed, 12 assertions.
- Existing strict missing-offset-with-receipt regression: passed, 4 assertions;
  no output and the accepted receipt remained unchanged.

Each disposable Kafka environment was removed and its cleanup verified. These
are regressions of the existing synthetic-source task harnesses; they do **not**
qualify the new ordinary cut reader as a native Kafka source. The previous fault
matrix was preserved, not rerun wholesale. Exact run reports, source hashes and
logs are in the evidence directory above. These are the earlier checkpoint results; current-turn verification is recorded
in the continuation report.


Advisor reviewed the actual worktree files after these checks and passed with no
actionable defect in the inspected changes. File access succeeded. The review
explicitly retained the native qualification and integration limitations above;
it does not certify an enabled production task or replace native source-task integration.
The single review result is preserved in `advisor-review.json` in the evidence
directory.
