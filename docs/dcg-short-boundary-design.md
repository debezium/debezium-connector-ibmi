# Test-only short boundary with metadata outside the locks

Status: the offline generator and first isolated native experiment are implemented.
The [native validation report](dcg-short-boundary-validation.md) records successful
short pulses, strict raw marker checks, maintenance conflicts and rejected
membership races. These are test-only candidates; startup admission and complete
maintenance visibility remain unqualified. Automatic migration remains disabled.
Production source, F/CR and offsets are unchanged by this experiment. The
[continued native and local validation](dcg-short-boundary-validation.md#continued-work-late-membership-and-prospective-schema-history)
adds late-file rejection and a prospective-schema handoff model; neither enables
automatic migration.

## Decision

Move every inventory, schema, identity, receiver and transaction-history query
outside the application write-lock interval. Test a minimal server-owned sequence:
**allocate the discovered members, deposit one small journal marker, release**.
After release, audit the metadata and retained journal history before declaring
anything consistent. A consistent result is still an unqualified candidate.

This addresses the two observed problems differently:

- Removing metadata calls from the held interval removes the particular
  metadata/maintenance overlap that accompanied the 35.910-second call. It does
  not prove a hard timeout for allocation, marker deposit, native I/O or release.
- A new file is not physically excluded by locks on existing members. Treat
  membership or maintenance changes as attempt invalidation. Require a complete
  change-observation contract before a clean interval could become proof. A pair
  of matching inventories is never sufficient.

The decisive first implementation is the small lock-and-marker experiment, with
its results compared against the retained lock-scope failures. Do not first add
another schema-history store, ownership mechanism or production admission flag.

## Evidence and API basis

The [completed scope experiment](dcg-lock-scope-validation.md) found all five
fixture files and their actual members, including EXTRA/DATA. It rejected an
unfinished autocommit update and observed ALTER TABLE waiting until release.
ENDJRNPF conflicted, but the collector remained in a native call for 35.910 seconds
before reporting its soft budget failure. New-file creation completed while the
existing member locks remained held; a changed inventory rejected that candidate.
The trace does not establish the precise internal lock dependency.

IBM documents [SNDJRNE][send] as writing a U entry. Its force option controls
forcing the receiver to auxiliary storage. With the default state override,
*STANDBY can suppress the entry without reporting an error. Therefore a successful
command return is not sufficient evidence that our marker exists. IBM's
[QJOSJRNE API][send-api] can additionally return the deposited entry's sequence,
receiver and library; this is an alternative implementation to investigate, not
an existing test seam.

IBM documents file locks for [STRJRNPF][start] and [ENDJRNPF][end]. Those statements
do not establish an atomic lock on journal membership. The reviewed ALCOBJ object
list does not offer *JRN/*JRNRCV; preserve the restriction recorded in the
[startup protocol](dcg-startup-protocol.md). A library lock would also fail to
cover a file attached from a previously undiscovered library. Do not prescribe
an unsupported journal lock, a global library sweep, authority revocation or a
new exit program as an implicit solution.

Context7 returned no matching IBM i passage and its suggested research option
was unsupported. The linked primary IBM references supply the API facts here.
They do not supply a complete maintenance-event visibility contract or a maximum
marker-deposit duration. Those remain explicit native qualification gates.

## Separate coordinates and evidence

| Symbol | Meaning | Must not be used as |
| --- | --- | --- |
| A | Opening audit marker, before the first metadata collection | A complete transaction-history origin |
| N | Marker deposited while all requested member allocations are held | A completed-output position, empty transaction census or durable F/W |
| Z | Closing audit marker, after post-lock metadata collection | Proof that maintenance cannot occur later |
| L | Exact acknowledged legacy publication barrier from the existing handoff | The ordinary lagging legacy offset |
| R | Volatile reader scan position | Durable acknowledgement |
| F / W | Durable replay floor / completed-output position | Either audit marker or a timestamp |

A, N and Z are qualified receiver/sequence coordinates, ordered through a
validated receiver chain, not sequence arithmetic or wall-clock timestamps.
Native object identities remain separate from names and journal IDs. A scope hash
binds the request to its observations; it proves neither completeness nor trust.

The new artifact must use a distinct profile, for example
`offlock-metadata-marker-candidate-v1`. It records the complete scope and bindings,
all three markers and their owned bytes, journal/receiver identity observations,
query provenance, actual lock request and release evidence, lifecycle coverage,
and explicit unproved premises. It always has `startup_proof=false`, no accepted
checkpoint and no recovery-store side effect. Never relabel off-lock observations
as the existing `online-native-scope-fence-candidate-v1` locked capture.

## Proposed sequence

### 1. Prepare outside the locks

Preserve the explicit disposable-source checks, operator-managed retention,
qualified source/session assumptions and local-journal restrictions. Collect no
credentials in the marker. Preallocate bounded local/QTEMP result buffers and
prepare the server statement before requesting any application locks.

Write a uniquely tagged opening marker A, then collect complete WRKJRNA scope,
independent JOURNAL_INFO counts, exact members, native identities and acquired
schemas with the existing validators. Bracket each off-lock metadata bundle with
its own scope/identity recheck; inconsistent or incomplete preparation stops
before allocation. Keep the existing 50-file, single-member and supported-resource
bounds. Output filters must not reduce this set. Do not split an excessive set
into multiple independently locked batches and call that one boundary.

Metadata work and retention/history preflight may take time, but application
write locks are not held. Legacy CDC continues independently. A denied source
request, unsupported object or incomplete inventory is a rejection, not an empty
set. Fix the complete ordered target list and its digest before entering the pulse.

### 2. Run a minimal server-owned lock interval

Reuse the existing single-call SQL compound structure and release handlers, using
one exact all-or-none ALCOBJ request with *EXCLRD, WAIT(1), CONFLICT(*RQSRLS), and
actual member names. Retain the busy refusal and partial-lock-release checks.
There is one attempt, with no retries, discovery, compilation of new helpers or
client callback inside the held region.

After allocation succeeds, record an in-memory server timestamp, send one marker
N and immediately DLCOBJ on success or failure. Store timing results into owned
temporary results only after release; instrument the implementation to establish
that even optional result bookkeeping does not precede release. Every exception
or warning after confirmed acquisition follows the release path. A release
failure remains a drain failure;
there must be no synthetic success or checkpoint.

For the smallest prototype, use the existing QCMDEXC mechanism to call SNDJRNE,
with an exact fixture journal, FILE(*NONE), no state/filter override and FORCE(*NO).
Use a bounded invariant-character payload (at most 512 bytes) containing format
version, attempt nonce, phase N and the expected scope digest. No application
transaction is committed, rolled back or associated with the marker. The U entry
is bookkeeping, not a truncate or application SourceRecord.

Keep WRKJRNA, JOURNAL_INFO, IFS_OBJECT_STATISTICS, SYSFILES, SYSPARTITIONSTAT,
column/key queries, receiver discovery, journal reads, transaction census, storage
force, Kafka waits, history writes and host-side rendezvous out of this interval.
A cheap timestamp or deadline check is instrumentation, not cancellation of a
native call. SNDJRNE itself can still block: test that risk, do not assume it away.
If it has the same pathological latency, reject this mechanism instead of raising
the budget. A QJOSJRNE wrapper returning coordinates is a later option only if its
same-job invocation and release behavior can be established without production
changes. Do not add such a wrapper merely to avoid a post-release lookup.

### 3. Observe and validate after release

Independently establish that the holder has drained and its locks are absent.
All potentially blocking post-processing now runs without those application
allocations. Recollect complete metadata, scope and identities. Write a unique
closing marker Z with forcing outside the lock interval. Resolve A/N/Z from
bounded owned native evidence and require exactly one of each, correct journal,
attempt, phase, payload, job context and receiver ordering. Validate errors,
warnings and truncation; a lost response is not permission to assume success.

For the first native profile require A/N/Z in the same unchanged receiver with
unchanged native identity and recording settings. Reject rotation during the
attempt; separately test that rejection. This avoids claiming that a force of
the newly attached receiver proves durability of the earlier receiver. Ordinary
runtime receiver rotation remains governed by its existing recovery checks.
A later extension can qualify a linked multi-receiver audit explicitly.

FORCE(*NO) inside the pulse avoids deliberately adding forced-storage latency
there. A subsequent successful force/read is additional durability evidence; it
is not an empty-transaction or writer-drain certificate. If forcing, rereading or
retention validation fails, discard the candidate. No initial F/W is exposed.

### 4. Audit changes, not just end states

Read the entire bounded audit interval A through Z. Collect all relevant journal
codes, including C/D/F/R, J controls and our U markers, independently of table
filters. The existing raw reader requests C/D/F/R, and its control-history path
requests C/J. Neither alone proves this new audit complete. A test-only capture
adapter must demonstrate its selection/coverage; it cannot silently inherit those
filters and declare a full audit. Reuse strict bounds, owned values, receiver
validation, pointer rejection and exact-job cancellation/drain handling. No change
to production retrieval, decoder or lifecycle allowlists is authorized here.

Audit all returned records. Accept only explicitly supported controls/data and
our exact markers for their stated purpose; reject unknown lifecycle effects,
missing pages, unsupported pointer data, changed profiles, ambiguous identity,
sequence reset or incomplete coverage. No general U-entry bypass, schema fallback,
SQL-to-native replay substitution or permissive error handling is introduced.

Reject the attempt for file/member creation, attachment, detachment, rename,
replacement, schema/format changes, journaling interruption or relevant recording
changes in the interval. Include files not in the discovered set and changes in
other libraries associated with this journal. Equal counts, equal final identities,
or matching schema hashes cannot excuse a change-and-revert interval. Known
maintenance evidence is sufficient to reject even when the complete payload
cannot be interpreted. Missing information is insufficient to accept.

The current 43-entry SQL maintenance diagnostic is useful for scheduling and
comparison. It is not accepted strict replay input. Its pointer-backed native
capture remains rejected. The new experiment must retain that negative case,
not quietly bless its SQL substitute.

### 5. Preserve the remaining handoff gates

Return only an auditable candidate or rejection. A candidate never calls the
normal task coordinator, records binding history, emits data, advances a barrier,
or publishes a checkpoint. Ordinary legacy progress, old task fencing and F/W
recovery remain untouched by this experiment.

The retained-history work must still find original C/SC and earlier savepoints,
including silent/zero-pending and rolled-back-tail cases. Excluding an active row
operation does not prove every commitment definition is closed. N is a proposed
row-binding origin only. Current metadata cannot decode older rows or repair an
already-published selected prefix.

The current one-epoch As400BindingHistory rejects an epoch after F. Keep that
check. If F precedes N, this candidate is not representable as a normal checkpoint
until a separately designed, durable distinction between control replay and row
binding history passes its tests. Do not set F=N to bypass missing context.

Joining a future candidate to L requires the existing exact legacy barrier,
Kafka acknowledgement and dirty-prefix rules. Lifecycle/identity validation must
cover the tail through that actual cut, with no unaudited gap after Z; a stale
candidate is discarded or requalified, not activated by a timer. Runtime change
detection must then stop before interpreting affected rows or acknowledging past
an unsupported transition. No marker serves as Kafka ownership/fencing authority.

## The unresolved maintenance premise

This is an optimistic invalidation design, not a new maintenance lock. To promote
it beyond a lab candidate, establish a documented, release-specific relation
between changes to membership/schema/journaling, their observable records, and
when affected data may be read or emitted. In particular:

- Every invalidating operation must be prevented at N or leave a complete,
  interpretable trace in the interval needed to certify N.
- Account for operations already in progress before A, operations that change
  state and revert between snapshots, and ones whose first/terminal observable
  record arrives only after Z. A delay or two matching scans is not that proof.
- Future lifecycle records must become observable early enough for the continuous
  reader to reject before affected data/output/offset advancement. Journal-write
  failures, omitted records, loss of authority and journaling gaps cannot become
  benign absence.

These premises are not established by the reviewed IBM references or the previous
fixture. Test artifacts must explicitly report `maintenance_visibility_proven=false`
until an independently justified provider exists. Even a clean, fully readable
interval remains `consistent-unqualified`. A finite stress test can falsify these
premises but cannot establish a missing general platform guarantee.

If that contract cannot be established, this route cannot safely enable arbitrary
online migration. The alternatives need an explicit supported maintenance fence
or a different native capture mechanism. Do not infer authority to install exits,
change application privileges or request a manual writer pause from this design.

## Implementation slices and acceptance

The first slice below adds a test-only pulse generator and local checks for its program shape:
no metadata or network rendezvous is reachable between allocate and release,
including error paths. Preserve the old collector and its failing reproduction.
Then add the isolated lock/marker experiment, followed by the off-lock bundle and
strict audit model. Do not connect it to the live handoff publication path yet.

| ID | Validation | Required result |
| --- | --- | --- |
| SB1 | Local generated-program inspection and success/warning/error/release-failure schedules | Only allocate, bounded marker and release in the held region; no checkpoint/output; release failure is not completed drain |
| SB2 | Captured metadata with an extra file/member, hidden resource, same-count replacement, missing member and changed JID/format | Discover actual complete targets or reject before allocation; retain original strict binding checks |
| SB3 | Missing/duplicate/foreign marker, silent *STANDBY omission, response loss, marker present after caller error | No trusted N from command success alone; failed/ambiguous attempts remain rejected |
| SB4 | Local equal before/after inventories with transient attach/detach, drop/recreate or schema change/revert in journal history | Reject; equality cannot erase an invalidating transition |
| SB5 | Local incomplete page/code coverage, pointer payload, missing start, unresolved savepoint, unsupported control | Existing strict rejection, zero output and unchanged durable state |
| SB6 | Native minimal pulse versus old collector under active autocommit UPDATE and concurrent ALTER/ENDJRNPF | Busy attempt releases; successful pulse excludes the update; no metadata work occurs while held; independently measure call and hold time |
| SB7 | Native new file/member or attach from a second isolated library at each phase and between final metadata reads | A changed/ambiguous audit is rejected; never lock just the original file set and label it complete |
| SB8 | Native transient create/drop and END/STR journaling; operation beginning before A and ending after Z | Preserve raw evidence; detect transition or report unproved coverage; never promote an empty audit to general safety |
| SB9 | Native marker latency/error, receiver rotation, killed client and separate lost-response/network case | Autonomous release/drain evidence, no assumed five-second bound, no coordinate from the wrong receiver, no ending an unrelated/reused server job |
| SB10 | Local fresh instance at prepared/acquired/marked/released/audited boundaries | Ignore orphan markers without a complete certificate; no automatic enrollment; existing offsets/history unchanged |
| SB11 | Later local integration with exact L, N>F, selected dirty prefix and interrupted batch delivery | Preserve R/F/W separation and current refusal; existing Kafka/recovery suites must still pass before any integration claim |

A first performance qualification should repeat the five-file pulse cold and
warm, recording allocation, successful hold, failed-acquisition partial hold,
release and total call separately. Use a one-second healthy successful-hold target
for the experiment, not a supported hard ceiling or public configuration. Test
larger sets separately before broadening the profile. Any overrun or unverified
drain fails that qualification; do not automatically increase the target. A short
average cannot hide a long tail. Distinguish socket-close recovery from a genuine
network black hole and a stalled source service.

Between failed attempts, keep the legacy path progressing and use bounded retry
frequency/backoff outside all locks. Record unrelated source writes separately
from Kafka CDC delivery. The first implementation slice proves neither new Kafka
continuity nor general online migration.

For further authorized native runs, use fresh isolated fixture libraries and the
existing ownership/cleanup harness. Preserve original API buffers/error results,
marker bytes, metadata queries/counts/identities, job and lock observations,
timing, actor ledger and independent post-run cleanup. Retain failed runs and
report any incomplete trace. The initial design performed no live work; subsequent
authorized runs and their limitations are recorded in the linked validation report.

## First local implementation slice (2026-11-09)

[`tools/dcg-short-boundary.py`](../tools/dcg-short-boundary.py) has no native
executor, credential loader or runtime integration. It owns an ordered list of
1..50 exact file/member pairs in one `DCNCdddd` library. A digest binds that list,
the exact QSQJRN journal and the supplied off-lock evidence bundle hash. Structural
validation does not establish that the native inventory was complete or authentic.
The future caller must obtain it through the existing native scope validators.

The generated dynamic compound has exactly three native calls: one ALCOBJ with
explicit job scope, one bounded SNDJRNE, and one matching DLCOBJ. Inner EXIT
handlers for exceptions, warnings and not-found conditions lead to a common
release path after confirmed acquisition. Release and post-release result writing
have separate handlers. A warning cannot be cleared by later successful SQL and
reported as success. A release failure is never retried inside the program. The
only held-interval instrumentation is scalar state and an acquisition timestamp;
temporary result insertion occurs after release. Declare the session temporary
result table using `prepare_sql` beforehand, and use a fresh dedicated holder
with no existing target allocations. No application COMMIT/ROLLBACK is generated.

This narrows one premise in the initial sketch: an error/warning during ALCOBJ
does **not** authorize a blind DLCOBJ. IBM's [allocation][allocate] and
[deallocation][deallocate] references warn that releasing an object without a
confirmed explicit allocation can release internal locks. A documented allocation
refusal grants none of the requested locks. An ambiguous return after a possible
side effect instead leaves drain unknown. The generator rejects both without
writing a marker/result; independent exact-job cleanup and lock-absence evidence
remain mandatory. It does not claim autonomous cleanup of ambiguous acquisition,
termination, or a native call that never returns. Those remain native gates, not
successful local release cases. The existing collector is unchanged.

The handler structure follows the documented [dynamic compound][compound]
semantics: EXIT leaves its block, handlers do not catch their own conditions, and
unhandled warnings can otherwise be cleared by later statements. The local model
does not establish that this particular SQL compiles or that native QCMDEXC errors
and warnings follow the modeled schedule. The generated program raises a generic
pulse error; any future native runner must also retain original native job/message
diagnostics and independent cleanup evidence, including failed runs.

The offline N-marker validator requires a successful matching attempt and program
hash, one post-release timing row, explicit holder-drained/locks-absent observations,
and exactly one bounded marker with matching job/thread, journal and receiver
identities, interval, version, nonce, phase and scope digest. It rejects warnings,
truncation, pointer-backed payloads, application commit cycles, stale responses,
absent/duplicate markers and byte changes. The first fixture uses explicit CCSID
37 bytes, not trimmed strings or an assumed ASCII decoding. Native CCSID fidelity
must still be established. All supplied completion/context observations are local
fixtures at this stage, not independently collected native proof.

Even a match returns only `marker-consistent-unqualified`, with `startup_proof=false`
and `maintenance_visibility_proven=false`. This checks N only: A/Z, native forcing,
full audit coverage, receiver stability across the attempt, maintenance visibility,
and the binding-history/legacy-cut integration remain outside this slice. It
creates no recovery state or output. No R, F, W or L is derived from this marker.

Generate an inspectable artifact offline with
`python3 tools/dcg-short-boundary.py /path/to/request.json`. The JSON request has
`library`, ordered `targets` pairs such as `[["EXTRA", "DATA"]]`, a 32-character
uppercase hexadecimal `nonce`, and the lowercase 64-character `evidence_sha256`.
Output contains separate preparation and pulse SQL; it never executes either.
Native execution would require a fresh nonce, not reuse of an example request.

Run the local checks with
`PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools -p 'test_dcg_*.py'`.
The new tests parse the emitted SQL's restricted control flow and inject conditions
before and after command effects. They check success ordering, acquisition refusal
and ambiguity, marker errors, release failure/warning, result failure, malicious
inputs, owned values and marker rejection. Mutation checks insert metadata or
early result bookkeeping and must fail. This covers the local portions of SB1/SB3
and orphan-marker rejection; it does not claim task-restart integration (SB10/11).
All 105 Python tests passed, including the existing 88; Java/recovery source and
tests are unchanged and were not rerun for this Python-only slice. No IBM i
connection, push or production admission change occurred.

[send]: https://www.ibm.com/docs/en/ssw_ibm_i_74/cl/sndjrne.htm
[send-api]: https://www.ibm.com/docs/en/i/7.5.0?topic=q-send-journal-entry-qjosjrne
[start]: https://www.ibm.com/docs/en/i/7.4.0?topic=ssw_ibm_i_74%2Fcl%2Fstrjrnpf.html
[end]: https://www.ibm.com/docs/en/i/7.6.0?topic=e-end-journal-physical-file
[allocate]: https://www.ibm.com/docs/en/i/7.4.0?topic=beginning-allocate-object
[deallocate]: https://www.ibm.com/docs/en/i/7.4.0?topic=ssw_ibm_i_74%2Fcl%2Fdlcobj.html
[compound]: https://www.ibm.com/docs/ssw_ibm_i_74/db2/rbafzcompounddynstmt.htm


## Prospective row-history refinement

The continuation tests a smaller alternative to reconstructing historical row
schemas before N. Retain complete older C/J context, keep ordinary legacy CDC
running, and defer until every pre-N cycle has a supported terminal. Decode only
prospective rows with the newly acquired bindings. Do not classify arbitrary
errors as temporary deferrals: unresolved savepoints, missing controls, source
identity changes, unsupported lifecycle effects and malformed rows still reject.

A usable exact legacy cut L must satisfy the existing already-published-prefix
rules and yield an ordinary checkpoint with F >= N **and** F later than the last
pre-N cycle terminal. Otherwise a restart would still encounter an old cycle
without its pre-N row schema. Continue legacy CDC and requalify a later cut rather
than inventing an F/N boundary or filtering retained source records. Persist the
qualified bindings before exposing F/W, as required by the existing design.

The native prospective fixture first encounters busy locks, makes unrelated
source progress, then retries after the old writer commits. The general
prior-cycle deferral is currently a local model; only the retry with no old cycles
remaining at N has native capture evidence. Neither result constitutes a normal
task scheduler or a bound on how soon a busy workload will become migratable.
Migration can remain pending while legacy CDC continues; a future user-visible
activation state must not announce truncate capture until the actual handoff
checkpoint is durable.
