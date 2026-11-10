# Receiver history as an online transaction-discovery boundary

This continues [the native handoff investigation](dcg-native-handoff-followup.md).
It is discovery work, not automatic migration admission. The existing strict row
reader, normalization/lifecycle checks, F/W recovery and publication guards remain
in force. Normal startup does not call the new control-history method.

## What the native membership check established

Evidence is retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-membership-resolution-20261109/`.
The disposable driver compares `QSYS2.JOURNAL_INFO` with the actual
`QjoRetrieveJournalInformation` RJRN0200 header at offset 336, retaining each raw
header and checksum. Both interfaces report zero initially, one after the first
fixture transaction and two after the second. Both remain at two after commit,
rollback, session disconnect and receiver rotation, even when the complete SQL
transaction/resource census has no resource for the fixture journal.

The scalar therefore cannot be treated as an exact count of currently enumerable
transaction resources. It is not sufficient to establish an empty or complete
in-flight inventory. We do not subtract retained registrations or convert a
disagreement into a successful census. This is native API behavior, not merely
a stale value in one ODBC connection.

`live-01`, library `DCNC0920`, explicitly ended only its two still-owned fixture
commitment definitions. The scalar stayed at two while their resource rows
disappeared. Subsequent ODBC rollback on an ended session failed with 08S01. The
run remains failed; fixture removal and independent absence checks passed.
`live-02`, `DCNC0921`, used ordinary commit/rollback/disconnect and completed
cleanly. Its strict replay matched 212 writer operations and 209 completed
batches. No existing application job was ended or reconfigured.

## Documented alternative

IBM defines [`PENDING_TRANSACTIONS`][pending] for each receiver in terms of
entries belonging to transactions that have not committed or rolled back. A
[commit cycle identifier][cycle] identifies the original C/SC entry in that
journal. IBM also documents that [receiver deletion is rejected][deletion]
while a receiver is needed by unfinished commitment work, and describes the
ordering of the terminal journal write before a [commit][commit] or
[rollback][rollback] finishes.

These interfaces support investigating a history floor rather than enumerating
every current commitment definition:

- A cycle that stays open throughout an observation interval must retain its
  original receiver, which remains pending.
- A cycle that finishes during the interval can clear that flag. Its terminal
  record in the surrounding journal interval still supplies its original cycle
  coordinate; that start must be read back.
- A cycle starting during the interval has its C/SC inside that interval.

The candidate floor is the earliest of the pending receivers' first positions,
the lower observation anchor and the cycle-start coordinates in the surrounding
records. Both original-start presence and complete bounded reading still need
validation. A lower bound is not itself a transaction start or a completed-output
checkpoint. In particular, it says nothing about the already-lagging legacy offset.

The test collector requires a bounded complete directory whose row count matches
`NUMBER_JOURNAL_RECEIVERS`, reciprocal receiver links, increasing native ranges,
one unchanged attachment, a local active uncached journal and known pending flags.
Unavailable/partial/freed receivers, warnings, query failures, count disagreement,
sequence resets and directory changes reject. It records native identities for
the entire directory before and after the observation, including older receivers
outside the immediate raw bracket. Back-read and bracket identities must match
those observations. Ordinary operator retention and the existing restrictions on
restore, maintenance and schema changes still apply; this grants no deletion
permission or independent old-backup detection.

## Separate control discovery from complete row capture

The existing raw CDC reader selects all **C/D/F/R types**, without file or entry
type filters. Its selection does not include J maintenance records. Earlier
references to an "unfiltered" capture mean this C/D/F/R selection, not every
journal code. A J-maintenance rejection cannot be established from that selection
alone.

The receiver-floor back-read also exposed pointer-backed historical entries
before the fixture's normal row-capture baseline. `live-04`, `DCNC0923`, rejected
that response and cancelled only its captured RPC job. The strict rejection is
preserved. IBM documents [pointer-backed journal data][pointers]; it cannot be
made complete by treating a pointer as an owned row value.

`RetrieveJournal.readControlHistoryPage` adds a separate bounded **C/J** query for
discovery. It returns a distinct `ControlPage`, retains all C/J entry types and
uses the existing transport admission, terminal stop, cancellation, structured API
error and native continuation handling. There are no file/job/program/user or
entry-count filters. Unexpected returned codes and pointer-backed responses still
reject. Normal CDC retrieval keeps its original selection and position behavior.
No per-record native call is added to normal streaming.

The explicit test capture modes are `directory` and `controls`. A control capture
has status `controls-captured`, so it cannot be accepted as a complete row capture
by the replay entry point. C/J history must be audited against the overlapping C
projection of a separate C/D/F/R capture; J entries provide additional maintenance
evidence. Neither result substitutes for the other.

`live-03`, `DCNC0922`, tested the candidate receiver floor before adding actual C/J
back-reading. Nine schedules retained every required original cycle coordinate;
a rotation inside observation correctly rejected. Strict replay matched 283
operations and 272 completed batches while 269 unrelated A inserts continued.
That result did not yet establish actual back-read accessibility or the missing
J-code coverage. It remains explicitly unqualified. All fixtures were removed.

## Native back-read and reconstructed controls

`live-05`, `DCNC0924`, completed the actual C/J back-reads for nine accepted
schedules. Rotation inside observation rejected. The independent audit checked
826 raw copies (234,141 bytes), their original checksums, decoded coordinates,
native identity windows and the full directory observations. The C projection
agreed with the separate C/D/F/R bracket and the full controlled-workload oracle;
the J projection independently exposed the rotation. Every required original
start was present in the actual back-read, including a silent cycle in a detached
receiver. Strict row replay matched 572 writer operations and 561 completed
batches, including 558 unrelated A inserts. All 41 modeled recovery cases passed.
No Kafka broker was involved in that native run. Fixture, inquiry and credential
cleanup passed without connection-close errors.

`As400OnlineControlInventory` now reconstructs the control projection with the
existing transaction buffer. Repository fixture `receiver-control-history.json`
retains the original native bytes and identity observations for all nine schedules.
It verifies native producers, exact original C/SC coordinates, ordered bounds,
savepoint targets and supported durable terminals. A savepoint rollback keeps the
cycle open. Missing starts, changed/missing producers, incomplete or non-durable
terminals, nested boundaries, unknown controls and observation-time maintenance
reject before returning a result. The result contains no offset, sink or
publication authority.

There is one narrow historical-tail distinction: a supported top-level terminal
strictly before the lower observation anchor can identify a cycle that started
before the captured history. It is reported separately as completed-before-history,
not reconstructed as an active cycle or accepted as a completed-output checkpoint.
Other orphan controls still reject. A terminal inside the observation interval
always requires its original start. This distinction is only for current control
inventory; it does not waive complete row or lifecycle recovery.

The native history also contains J/XP. IBM classifies this as an [internal journal
entry][codes], not a documented transaction boundary. Discovery preserves its
position and content digest along with J/PR and J/NR; it does not silently whitelist
the historical J stream as harmless. Any J record inside the observation interval
rejects the current candidate. Maintenance interpretation outside that interval
remains a separate gate before normal-task admission.

The captured-handoff loader can now join this independently captured control
history to its complete row fixture. It revalidates native directory/receiver
identities, matches the overlapping C projections and reconstructs the inventory
instead of trusting an exported cycle list. An active start that predates the
independently captured row/schema history rejects. The original explicit
`CONTROLLED_FIXTURE_ASSUMPTION` remains; neither the exporter nor matching hashes
promote this into a normal startup proof. Existing census fixtures and their
stricter Kafka fault assertions remain available unchanged.

## Joined native history and Kafka fault validation

`live-06`, `DCNC0925`, repeats the receiver-history schedules and adds a reload
row in the committed optimized-DELETE transaction. Strict replay matched 574
writer operations and 562 completed batches while 559 unrelated A inserts
continued. The raw/identity audit again checked the separate C/J and C/D/F/R
projections against the full fixture oracle. Source and compiled-class manifests
were unchanged across the native run (546 entries). Native fixture, inquiry and
credential cleanup passed with no connection-close errors.

The local handoff exporter now accepts this receiver-discovery evidence as an
explicit alternative to the earlier SQL census fixture. It recomputes the floor
from the saved pending-receiver observation, verifies the capture selections and
cleanup, and exports the original raw control bytes and complete native directory
observations. The Java loader independently reconstructs the controls, compares
their overlap with the complete row capture, and checks native identities. It
still requires `CONTROLLED_FIXTURE_ASSUMPTION`; this is not a new admission mode.

The chosen cut is R=162, with F=99 inclusive and W=160. These are different
positions with different meanings: replay starts at the retained original cycle,
W is a completed output boundary, and R is how far preparation scanned. The
capture contains two open cycles at the cut. Recovery retains actual native
thread, journal and receiver identities and the acquired table bindings.

`kafka-07` exposed a controller deadlock: the longer legacy prefix filled the
64-record queue while the controller waited for the barrier to be enqueued before
allowing the first poll. The failed run, independent broker observations and
successful Docker cleanup remain retained. The controller now permits the
scenario's required prefix polls before waiting for the barrier. The original
faults and publication/offset assertions are unchanged. The failed and corrected
runs have identical plugin, source bundle and expected records.

`kafka-08` passed all 34 assertions across five scenarios: failure before the
legacy barrier, loss of its commit callback, loss of the migration callback,
interrupted clear/reload delivery, and task reassignment with a returning old
worker. Missing offsets reject without output or a replacement checkpoint. Every
scenario produced exactly 562 committed data records, including one truncate and
no snapshot records, ending at F=W=654. An independent broker audit checked all
2,810 source timestamps and receiver coordinates, keyless truncate envelopes,
native-identity offsets, and the old source producer's actual
`InvalidProducerEpochException`. Fourteen unrelated A records between the clear
and its terminal reached committed output before that clear in each scenario.

This is actual local Kafka Connect, the task/RPC legacy handler and dispatcher
processing captured source bytes through controlled transport. It is not a
simultaneously connected IBM i-to-Kafka migration. There is no custom owner or
receipt gate. The independently checked staged plugin contains 325 local class
files matching the final build. Docker cleanup passed.

`live-07`, `DCNC0926`, additionally creates a savepoint before the session's first
write, rotates the receiver, and only then performs a clear and reload. No earlier
C/SC appeared for the savepoint-only session. The later write deposited original
start 163 and its savepoint controls, followed by terminal 244. All five receiver
observations, strict replay and independent audits passed: 279 operations, 276
completed batches and 275 unrelated inserts. This resolves that fixture's
late-first-write case; it does not prove a general atomic inventory. Source/class
manifests were unchanged and fixture/inquiry/connection/credential cleanup passed.

Final local verification passed 2,640 Java tests (200 parser and 2,440 connector)
and 75 Python tests, Maven verify and assembly, and formatter/import validation.
Protected F/CR, original truncate and earlier D/CG worktrees, all branch heads and
indexes are unchanged. The only production source change in this work is the
separate C/J reader method; normal streaming still uses its original selection.

The explicit `advisor_review` tool then inspected the actual repository files
and retained evidence and passed: no actionable defect was found in the inspected
work. It agreed that general online admission remains unproved and appropriately
disabled. No file-access limitation was reported in that result. This is a review
of the conditional implementation and evidence, not approval to enable migration.

## Remaining integration gates

Discovery can remove the dependency on an exact transaction-definition count.
It does not establish a complete published baseline or historical row schemas.
Before a normal-task handoff can use it, the implementation must:

- Join the bounded pending-receiver collection, reconstructed C controls and
  historical maintenance interpretation into one admitted native observation.
  The control reconstruction is tested; it alone cannot certify collection
  completeness or harmlessness of the entire historical J stream.
- Obtain a complete C/D/F/R interval for reconstruction with trustworthy paired
  file/member bindings. Current metadata must not be applied to older row history
  merely because the discovery floor reaches it. An older unsupported interval
  must defer or reject migration while legacy CDC continues.
- Preserve autocommit before-image and file-lifecycle dependencies as well as
  commitment-control cycles. A C/J-only inventory cannot certify their absence.
- Bind the prepared context to the actual task's exact, durably acknowledged
  publication barrier. Reject dirty selected prefixes, missing offsets, identity
  changes and unsupported history before a migration checkpoint or output.
- Validate the combined native reader/barrier/reassignment path with unrelated
  Kafka publication continuing. The current native write ledger and local modeled
  recovery are different evidence from that combined deployment.

The [deferred handoff continuation](dcg-deferred-handoff-validation.md) now commits
legacy progress while a selected prefix remains open and survives replacement
before the later migration. Its original source-origin assumptions remain explicit.

These gates retain automatic migration disabled. They do not require adding a
public per-table truncate mode or changing F/CR.

### Precise unresolved source guarantee

The remaining question is not whether the transaction buffer can emit `op=t` or
recover a committed batch. The evidence above exercises both. It is how to admit
an arbitrary existing stream before its first buffered checkpoint without a
known complete row/schema origin.

For example, a pending-receiver scan can discover every commitment-control cycle
and still say nothing about an autocommit update whose R/UB precedes a proposed
warm-up origin and whose R/UP has not yet been read. An empty local before-image
map is not proof that such work is absent. The current strict decoder continues
to reject an unpaired after-image. Similarly, today's schema cannot certify an
older row's layout, and preserved J/XP bytes do not explain that internal event's
effect. The fixture's controlled creation/history supplies facts that a running
installation's legacy offset does not contain.

The additional primary references reviewed are IBM's [journal processing
description][processing], [entry layouts][layouts], [Send Journal Entry][send]
and [object lock rules][locks]. They explain journal images and forced storage;
they do not establish an atomic R/UB–R/UP observation boundary or a complete
unfinished-autocommit inventory. Forcing a user entry is therefore not admitted
as a writer-drain certificate. The documentation lookup service returned no
matching API passage, and its suggested research flag was unsupported; the IBM
references were checked separately. Absence from these sources is not a claim
that IBM has no possible supported mechanism.

Before promoting this route, obtain a documented boundary mechanism that either
accounts for unfinished nontransactional updates or excludes them, and validate
its interaction with the historical schema epoch. A connector-controlled short
native lock is a different possible mechanism: [ALCOBJ][allocate] documents an
all-or-none request and `WAIT(0)`, but such a lock blocks writes while held and
conflicts with existing update opens. The user subsequently accepted brief
automatic write locks; that changes the permitted mechanism, not the source
guarantees already established. Subsequent [isolated feasibility tests](dcg-deferred-handoff-validation.md)
exercise immediate busy rejection and server-side release after an error or killed
client. They do not establish a hard hold-time bound under a lost connection or
qualify complete native startup. The next [metadata collection experiment](dcg-automatic-boundary-validation.md)
retains these limitations and does not admit automatic migration.

Concrete questions for IBM/source maintainers, prepared locally only:

- Can a journal reader observe an R/UB without its corresponding R/UP having been
  deposited, or can another operation/receiver switch occur between their
  deposition? Which documented interface identifies all outstanding pairs at a
  chosen native sequence, including work outside commitment control?
- Does `PENDING_TRANSACTIONS` protect the original C/SC receiver for every
  supported still-open local cycle, including zero-pending/savepoint-only states,
  until a supported durable C/CM or complete C/RB is visible to QJORJRNE? Are any
  supported local states omitted from this relation?
- Which J maintenance types must invalidate a historical binding epoch, and what
  supported evidence can qualify an immutable file/member/schema relationship
  from a warm-up boundary while application DML continues?

No question, issue, comment or code was published. Automatic migration remains
disabled because these source guarantees are unresolved, not because Kafka
fencing or clear-batch replay failed.

[pending]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-journal-receiver-info-view
[processing]: https://www.ibm.com/docs/en/i/7.5.0?topic=concepts-how-journal-management-works
[layouts]: https://www.ibm.com/docs/en/i/7.5.0?topic=information-layouts-variable-length-portion-journal-entries
[send]: https://www.ibm.com/docs/en/i/7.5.0?topic=ssw_ibm_i_75%2Fcl%2Fsndjrne.html
[locks]: https://www.ibm.com/docs/en/i/7.6.0?topic=resources-lock-states-objects
[allocate]: https://www.ibm.com/docs/en/i/7.6.0?topic=ssw_ibm_i_76%2Fcl%2Falcobj.html
[cycle]: https://www.ibm.com/docs/en/i/7.5.0?topic=objects-commit-cycle-identifier
[deletion]: https://www.ibm.com/support/pages/msgcpf7024-rc2msgcpf7018-rc2-determining-open-commit-definition
[commit]: https://www.ibm.com/docs/en/i/7.5.0?topic=work-commit-operation
[rollback]: https://www.ibm.com/docs/en/i/7.6.0?topic=work-rollback-operation
[pointers]: https://www.ibm.com/docs/en/i/7.5.0?topic=information-working-pointers-in-journal-entries
[codes]: https://www.ibm.com/docs/en/i/7.6.0?topic=information-all-journal-entries-by-code-type
