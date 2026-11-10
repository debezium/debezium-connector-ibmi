# Online legacy migration boundary: API assessment and decisive validation

Status: local design assessment, 2026-11-08. No connector code changed, IBM i
connection made, migration enabled, or material published. This follows the
[conditional legacy handoff proof](dcg-legacy-handoff-proof.md). Its fixture-supplied
closed anchor is still a test precondition, not an online discovery implementation.

## Recommendation

Investigate a **journal-bracketed transaction inventory**: collect the current
commit cycles while independently retaining journal entries covering that
collection. Reconstruct the cycles that are still open and those that finish
during collection. The desired boundary can have open transactions; it need not
be a moment when the journal is empty of transactions.

Separately, use a **connector-owned publication barrier** to establish exactly
which journal entries the legacy task finished publishing. A database API cannot
recover that fact from the lagging legacy offset.

These mechanisms would remove two reasons for requiring a writer pause. They do
not yet resolve a transaction whose rows have already been published by the
legacy connector but whose outcome is still pending. Keep the existing rejection
of that case. Complete source history does not undo already-visible output.

The highest-value native experiment is a bounded, shadow-only race capture with
one silent long-running transaction and others ending/restarting during inventory.
The highest-value preceding local experiment is the publication barrier through
the actual legacy RPC/task and Kafka offset acknowledgement paths. Neither
experiment was executed in this assessment.

## What the documented APIs provide

| Interface | Documented fact useful here | Limit of the evidence |
| --- | --- | --- |
| [DB_TRANSACTION_INFO][definitions] | One row per commitment definition, including a lock-space ID and state. Cross-user visibility requires the documented authority. | The reviewed description does not establish a sufficiently strong concurrent-enumeration contract for this migration. An empty caller-visible result is insufficient. |
| [DB_TRANSACTION_JOURNAL_INFO][resources] | Journal resources for a lock space; `COMMIT_CYCLE` points to the beginning of the current journal-specific unit of work. Null means no qualifying work has been journaled for that resource in the current unit of work. | A changing definition can end or start another cycle between enumeration and resource lookup. Zero pending changes and 100% rollback progress do not establish terminal outcome. |
| [Commit cycle identifier][cycle] | The cycle identifier is the sequence of its `C/SC` entry; one transaction can have a distinct cycle in each journal. | Resolve the sequence within the correct journal incarnation and receiver chain. It is not a cross-journal transaction ID or a reusable receiver-independent coordinate across resets. |
| [QjoRetrieveJournalEntries][entries] | Explicit receiver/sequence bounds, transaction identifiers, raw entry formats, and pagination are available. Selection keys restrict the result. | Completion of a filtered request is not coverage of excluded entries. Partial results/errors, unknown controls, unavailable receivers, and ambiguous sequence resets must reject. |
| [JOURNAL_RECEIVER_INFO][receivers] | Receiver links, ranges, availability and pending-transaction status can corroborate a read. | Pending status now does not identify every transaction open at an earlier legacy offset. It is not a historical transaction inventory. |
| [QTNRCMTI][current] | Commitment information for the requesting program's activation group/job context. | It is not a system-wide transaction inventory; system-use-only definitions are excluded. It cannot replace the required cross-job observation. |

The [journal-cache documentation][cache] says cached entries are not visible to
the retrieval API. Keep the existing rejection of journal caching. This condition
alone does not prove the exact ordering between transaction-state retirement and
visibility of its terminal record; that is a specific validation question below.

The prior native report records IBM i 7.5 with database PTF group SF99950 level 11.
That is historical evidence, not a fresh service-availability check. The existing
collector already uses these transaction services. Neither that earlier fenced
initialization nor repeated successful SQL calls qualify unfenced enumeration.

## Candidate inventory algorithm and its proof obligation

This is a proposed observer, not permission to call the current
`As400NativeTransactionCensus.requireEmpty` without its writer fence.
Retain the qualified local-journal/SYSBAS scope, operator-managed retention,
required job/thread fields, immutable historical bindings, and rejection of
unsupported distributed/prepared transactions and ambiguous lifecycle records.
Continuing ordinary writes does not authorize unvalidated schema or journal
administration changes during acquisition.

Use a fixed legacy coordinate L for an attempt. In the eventual online preparation
phase, legacy CDC may keep advancing while this shadow observer works. Results
must be extended/revalidated against the final publication barrier before they
could authorize a switch; a stale observation is not transferable automatically.

1. Confirm existing retention and the qualified journal/receiver identity and
   recording settings. Retain a real readable journal anchor P before enumerating
   transactions. L must be at or before P. No fabricated sequence, source marker,
   lock, or offset change is necessary for this observation.
2. Enumerate every visible commitment definition, checking cross-user authority,
   warnings, resource bounds, and every definition's resources for the journal.
   Do not restrict the inventory to currently captured files or positive pending
   counts. Preserve lock-space IDs, actual commit-cycle IDs, states, and the start
   and completion of each query. A vanished definition or lookup failure can abort
   an attempt; it cannot be silently treated as proof that no transaction existed.
3. After all resource lookups finish, establish another readable anchor Q. Capture
   a complete, retained interval from L through Q with all relevant controls and
   lifecycle records, independently of the legacy file/type filters. Revalidate
   identities, chain links, ranges, read completion, and anchor bytes. Receiver
   attachment races must follow the existing bounded discard/retry discipline.
4. Take the union of cycles reported by the inventory and positive commit-cycle
   IDs in that journal interval. Read each required `C/SC` and the intervening
   transaction/lifecycle history, resolving its actual receiver. Apply the existing
   file/member identity, decoder, savepoint and durable-terminal checks.
5. Reconstruct the transactions open at the target boundary. A future replay floor
   must retain every needed start and earlier lifecycle/binding dependency, not
   merely the smallest observed row sequence. Missing history is a rejection,
   never permission to advance the floor to the oldest available suffix.

The logical case split is useful even without an atomic SQL snapshot:

- A transaction already open at L that **finishes by Q** must leave its terminal
  record in the retained interval. That record identifies the earlier cycle.
- A transaction already open at L that **remains open through Q** must be returned
  by a complete inventory of persistent definitions/resources, even if it produces
  no new row during the entire observation.
- A transaction that first journals work **after L** must leave its start and work
  in the interval, or begin after Q and be discovered by continued reading.

This argument is conditional on two native properties that the reviewed API
descriptions do not establish strongly enough to enable migration:

**Persistent visibility:** a definition/resource that remains active throughout
the enumeration cannot be silently omitted, with the required authority, despite
other definitions being created, ended or reused concurrently.

**Terminal coverage:** when a transaction disappears from current-state results,
its terminal record can be included in the fully validated post-enumeration
journal interval. A terminal that becomes visible only after Q would invalidate
the argument if the transaction was also absent from the inventory.

These are different from requiring all rows to describe the same instant. The
experiment must test both. Passing finite races is qualification evidence; it
cannot create an API guarantee absent from the platform contract. If behavior or
documentation remains ambiguous, seek IBM confirmation or use another documented
mechanism, and continue to reject automatic admission.

For example, if a cycle starts at 10, L is 20, and it commits while the observer
is looking up its lock space, it can vanish from the current-resource result.
Its commit in the L-to-Q interval must recover cycle 10 and lead back to its start.
A different cycle that starts at 9 and remains completely silent through Q must
be discovered by the inventory. Two equal empty queries would miss the distinction.

Do not manufacture `afterVerifiedBoundary(F - 1)` from the discovered earliest
start. Other transactions can overlap that position. A future bootstrap needs a
verified **nonempty** transaction context and explicit handling of completed
historical prefixes. The existing proof currently starts from a known closed
anchor; replacing that precondition needs its own test-only adapter and recovery
tests. Existing orphan-prefix handling is a reusable component, not evidence that
this bootstrap already works.

## The legacy publication boundary is a separate problem

The real RPC callback test demonstrates that row 42 can carry offset 40. A saved
40 can also be a heartbeat after scanning 40 with row 42 not yet delivered.
Both situations can have exactly the same IBM i journal and saved offset. No
transaction query, journal lookback, repeated read, or source timestamp can tell
which publication history occurred. Native history can narrow possible overlap;
it cannot certify Kafka delivery.

The proposed local validation should exercise an ordered barrier owned by the
legacy task:

- Stop admitting entries beyond a chosen, fully processed cut. Drain the complete
  output of that cut, including update/key-change/tombstone subrecords and metadata
  where applicable. Source applications continue writing to the retained journal.
- Enqueue an identifiable control/heartbeat record after that output and await
  its actual durable Connect source-offset acknowledgement. Freezing a map or
  enqueueing a record is not acknowledgement.
- Ensure the old task cannot publish after the barrier or after replacement, using
  the existing qualified Kafka Connect ownership/fencing contract. Tie the evidence
  to the same source partition, generation and capture policy.
- Only then interpret the cut as an exact legacy **publication** boundary. Derive
  F/W separately from validated transaction context; the barrier is not a commit
  record and must not be relabeled as W.

This requires a future test-only integration first; no barrier code or offset
format was added here. Use standard Connect offsets/fencing, not a new custom
owner or receipt store. The durable transition must distinguish a pending barrier
from a completed migration, including offset loss and reassignment. Do not add a
public per-table truncate option; keep the ordinary capture filters unchanged.

Even an exact publication boundary can have a dirty open cycle. If the old task
already exposed its rows, a later rollback or savepoint can invalidate them.
Retain the proof's rejection. During bounded preflight the legacy task can continue
its existing CDC; decline activation when no eligible boundary is found. Do not
hold all table output indefinitely waiting for a long transaction, silently omit
its truncate, or use an administrator writer pause as an implicit fallback.
Supporting every such boundary would require a separate transition/compensation
design. There is no bounded activation-time claim for a continuously busy source.

## Concrete validation: native census/log race capture

Plan only: a later explicitly authorized run in one fresh isolated library with
its own journal and retained receivers. No existing auction tables, database
snapshot, application-wide pause, production access or Kafka data publication is
needed to validate the inventory. Native workload mutations belong only to this
fixture. Existing cleanup/ownership checks remain mandatory.

Use independent writer sessions and a controller ledger that records actual
transaction starts/outcomes and native job/thread identity. Keep an unrelated
autocommit writer moving throughout discovery. The ledger and the final full raw
capture from fixture creation are the oracle; the inventory cannot grade itself.

| Case | Controlled schedule | Required result |
| --- | --- | --- |
| Silent old cycle | Write before L, remain open without more work throughout inventory and Q. | Inventory identifies its real cycle; replay retrieves the earlier start. |
| End before resource lookup | Enumerate the lock space, then commit or fully roll back before its resource lookup. | Terminal coverage recovers the original cycle, or the attempt explicitly rejects. |
| Reused definition | End the old cycle and start another on the same session while the inventory runs. | Both cycles are accounted for; lock-space ID is not treated as transaction identity. |
| New definition | Create another session/cycle during enumeration. | Its journal start is discovered even if the definition list did not include it. |
| Late table involvement | A pre-L cycle modifies an unselected fixture table, then later touches a captured table. | Whole-journal context preserves its start; table filtering cannot hide it. |
| Zero pending / savepoint | Roll back to a savepoint but keep the transaction open; exercise a clear and nested savepoint controls. | Zero pending changes does not certify completion; native cycles and savepoints remain resolved. |
| Rollback in progress | Hold observation during rollback and completion. | No percentage or transient state is accepted as a durable terminal. |
| Receiver rotation | Rotate the fixture receiver during inventory/retrieval. | Bounded revalidation or explicit rejection; no skipped interval or guessed ordering. |
| Legacy pagination | Run the real legacy reader as a nonpublishing observer with small native pages, type/file filters and a receiver rollover. | Record actual `(source entry, serialized offset, processed flag, receiver)` pairs; qualify the barrier's cut at page ends and continuation positions instead of assuming a numeric one-entry lag. |
| Missing old history | Remove a required start from a local replay copy of the retained capture. | Reject with no candidate checkpoint; no receiver deletion is needed to inject this fault. |
| Incomplete observation | Inject authority restriction, timeout, warning, partial response, or row-bound overflow locally; exercise native variants only with already-authorized credentials. | Reject; never interpret reduced visibility as an empty inventory. |

Use explicit limits, initially at most 10 repetitions per race, 60 seconds per
discovery attempt, and a 10-minute fixture-run deadline. These are proposed test
budgets, not CDC latency guarantees. Save per-query intervals/results, authority
observations, per-receiver settings/incarnations, exact request bounds, raw pages,
continuations/errors, discovered cycles, fetched starts, and unrelated-writer
progress. A safety rejection is acceptable; persistent rejection under ordinary
churn means the candidate is not operationally qualified.

Replay every accepted result locally through the strict normalizer/buffer, with
fresh-instance restart before and after the candidate inventory checkpoint. The
new checkpoint remains a test artifact until a separately proved publication
boundary and bootstrap exist. Never substitute synthetic thread IDs or infer
coverage from a digest of an already incomplete capture.

Native success means zero missed relevant cycles against the independent oracle,
correct original starts across the retained chain, explicit rejection of every
incomplete attempt, no weakening of identity/savepoint guards, and continued
unrelated writer progress. Record attempts that succeed, reject or time out and
their duration. Terminate fixture sessions, resolve their transactions, verify
locks and pending work are gone, and verify removal of only the owned fixture.
None of those cleanup actions occurred in this documentation-only turn.

## Immediate local validation before any native run

Implement a test-only adversarial inventory model and publication-barrier harness:

1. Enumerate the race cases above locally. Require discovery under the stated
   visibility/terminal assumptions and explicit counterexamples when either is
   removed. This validates the algorithm's logic, not IBM i's implementation.
2. Exercise the real legacy RPC/dispatcher and local Kafka with two histories
   yielding the same ordinary offset: row 42 published versus not published with
   offset 40. Verify that ordinary offsets remain ambiguous and that an
   acknowledged barrier resolves the complete-output cut.
3. Crash before enqueue, after partial row output, after barrier enqueue, after
   Kafka commit but before callback, and during reassignment. Require replay or
   rejection without offset invention, leaked partial truncate batches, or old
   owner publication. Preserve separate L/R/F/W and all existing stricter tests.
4. Feed the resulting explicit boundary to the test-only handoff proof. Retain
   dirty-prefix and insufficient-history rejection. A partial local success is
   not permission to enable automatic migration.

This sequence resolves the connector-controlled ambiguity locally before spending
another native session on the two platform-level visibility questions. Neither
the old whole-journal writer fence nor new per-table truncate configuration is a
premise of the proposed design.

## Research and change limits

Reviewed the actual handoff test, legacy RPC ordering, transaction census, strict
journal cut reader and prior native reports. The cut reader allows growth of the
attached receiver within a fixed inspected interval and rejects an attachment
race; it currently requires an existing version-2 checkpoint, so it cannot simply
be called with a legacy offset to bootstrap this proposal.

Context7 resolved IBM i but returned no matching documentation for these APIs.
Several direct IBM pages returned HTTP 403; IBM's search-indexed primary API
references supplied the cited field and authority descriptions. No undocumented
internal API or claim of having exhausted every IBM interface is used here.
There were no new live observations, executable connector changes or new runtime
test results. The preceding handoff proof's 2,386 passing tests and Advisor pass
remain prior evidence, not tests of this proposed inventory or barrier.

[definitions]: https://www.ibm.com/docs/en/i/7.6.0?topic=services-db-transaction-info-view
[resources]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-journal-info-table-function
[cycle]: https://www.ibm.com/docs/en/i/7.5.0?topic=objects-commit-cycle-identifier
[entries]: https://www.ibm.com/docs/en/i/7.5.0?topic=q-retrieve-journal-entries-qjoretrievejournalentries
[receivers]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-journal-receiver-info-view
[current]: https://www.ibm.com/docs/en/i/7.5.0?topic=q-retrieve-commitment-information-qtnrcmti
[cache]: https://www.ibm.com/support/pages/basics-journal-cache
