# Automatic short-lock boundary experiment

The user accepted brief automatic write locks. The implementation in this change
is a **test-only native candidate collector**, not automatic migration admission.
No production Java, F/CR branch, normal transactional-streaming guard, deployed
connector or published PR changes. No administrator-managed writer pause or new
public per-table truncate setting is introduced.

## Collection and release

`tools/dcg-online-fence-probe.py` operates only on a fresh `DCNCdddd` fixture using
the existing explicit `IBMI_TEST_CONFIG` harness. It obtains all original metadata
SQL from `As400OnlineFenceEvidence.queries()`, which exports the existing native
collectors' constants. It prepares session temporary result tables and stages an
immutable copy of its Java classes/dependencies before source work.

One server-side compound statement allocates the exact file/member set, collects
metadata and a journal boundary, then releases the allocations. SQL exception
and warning handlers release before rethrowing. No client callback, result fetch,
Kafka acknowledgement, JVM startup or network rendezvous is needed to release.
Results are fetched from the temporary tables only after release. These tables
belong to the connection; no persistent stored procedure is installed.

The request uses `*EXCLRD`, `WAIT(1)` and `CONFLICT(*RQSRLS)`. IBM documents
[all-or-none allocation][allocate]. A one-second allocation wait lets the system
request release of eligible cached SQL opens; it does not end application jobs or
release application-owned locks. `WAIT(0)` repeatedly refused idle cached opens in
the first two isolated attempts. IBM's [pseudo-closed cursor explanation][cursor]
distinguishes these opens from active operations. Acquisition time is separate
from lock hold time, and temporary partial allocations during an unsuccessful
request are not evidence that application writes were never delayed.

Equivalent queries are grouped by observation phase and SQL, using a bounded
request relation and a lateral query. Every returned row retains its exact
request identifier. Foreign/missing identifiers and per-request overflow reject;
one request cannot consume another request's unused bound. The original logical
query responses are reconstructed for the existing Java metadata validators.
For four tables, 48 logical requests use 14 server query statements. The initial
three-table direct-query version used 38 server statements.

The captured evidence includes independent before/after member, format, native
file/member identity, journal/receiver directory and native journal/receiver
identity observations, plus complete columns and keys. The original strict
validators acquire mixed schemas, paired *FILE/*QDDS journal IDs and physical
identities. They reject missing observations/columns, changed bindings, layout
overlap, profile changes, receiver rotation and regressed ranges. Older receivers
with an unreadable recording profile remain unqualified observations; they are
never silently accepted as replay history.

The default five-second **between-query budget** is checked before and after each
server query. Expiry raises an error and executes the same release handler. This
is not a preemptive timeout for a native service stuck inside one query. Neither
`WAIT(1)` nor healthy measured durations establish a hard wall-clock pause bound
during source failure. Killing a local client closes its socket; it does not test
a network black hole. The observer never uses ENDJOB on a disconnected, possibly
reused database server job.

## Native and offline evidence

Evidence root:

`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-automatic-boundary-20261109/`

The four-table fixture includes A/B/M/U, including the output-excluded U file so
the experiment does not confuse output selection with complete source bindings.
M contains INTEGER, nullable VARCHAR, DECIMAL and SMALLINT columns. This is still
an explicitly known isolated set, not automatic discovery of every journaled
object in an arbitrary installation.

`live-07` (`DCNC0935`) passed candidate validation, independent lock checks and
cleanup. An open B update, SQL TRUNCATE and optimized DELETE each caused the
intended B allocation refusal, with no retained partial lock. Full collection
held locks for 3.312, 2.935 and 2.708 seconds. Injected SQL failure and a deliberate
budget overrun released all locks. After the observer saw locks on all four
members, it killed only its local child client; absence was observed 4.099 seconds
later. The concurrent A writer completed 119 inserts without errors; its longest
measured SQL call was 3.618 seconds. A is inside the locked set, so these figures
show resumed writer progress, not uninterrupted writes while the lock is held.

The retained native journal was replayed offline through the real lifecycle
classifier, strict row decoder, normalization adapter, transaction buffer,
dispatcher and recovery coordinator. `As400OnlineFenceReplay` joins the acquired
bindings to actual journal/receiver identities, checks all captured writer keys
and mixed values, rejects output from rolled-back clears, and preserves the
normal delete/tombstone/create sequence for a primary-key-changing update.
The existing recovery harness checks fresh instances and false/throwing output
delivery, independently preserving scan R and F/W. Acknowledgements in this
offline check are modeled; no broker or durable store is involved.

The final staged run, `live-10` (`DCNC0938`), repeats every native case and adds a
committed M truncate/reload after the locks release. Its three holds are 3.289,
2.781 and 2.691 seconds. The A writer completes 119 calls without errors; the
longest is 3.630 seconds. The killed client's lock disappears within 4.067 seconds
of the kill observation. Cleanup verifies the library, schema and receiver
inquiries absent, with no connection-cleanup errors. The offline audit produces
128 reference records, including exactly one committed truncate and the mixed
reload, and passes 41 recovery/rejection cases. The earlier `live-07` replay has
125 records and also passes 41 cases. All captured counts include transaction
metadata and the primary-key-change tombstone; they are not table-row counts.

This replay starts at the **controller-established fixture anchor before its
writers started**, not the candidate lock boundary. The fact that its current
schema is valid back to that anchor is a controlled fixture fact. It must not be
promoted into proof that today's metadata describes arbitrary earlier rows.
Actual native journal/receiver identities replace stand-in identities in this
new replay, but identity equality does not establish historical schema validity.

The final local Maven verify run passes 2,664 tests (200 parser and 2,464
connector), and all 82 Python tests pass. Assembly packaging, formatter/import
validation and `git diff --check` pass. The earlier real Kafka matrix remains
unchanged; it was not rerun for this separate native metadata collector.

The explicit Advisor review inspected the actual repository files and saved
evidence and passed with no actionable defect or file-access limitation.
The final run's 745 staged files retain their recorded hashes; all 550 staged
local classes match the final build, and its saved harness matches the source.
Source/index/HEAD comparison confirms no production-file change and no changes
to the protected F/CR, original truncate or earlier D/CG worktrees. Nothing was
committed, pushed or published.

All unsuccessful attempts and logs remain in the evidence root. `live-01/02`
refused cached opens; `live-03` collected valid current metadata but the initial
offline validator unnecessarily demanded a readable profile from an unused old
receiver; `live-04` hit a Python datetime conversion error after release.
`live-05/06` passed direct/batched collection, respectively. `live-08` safely
refused A before reaching the intended busy-B case; the harness now retries that
specific competing refusal within a finite budget and still requires observing
B's conflict. `live-09` failed when local recompilation removed classes while its
final capture JVM started; immutable per-run runtime staging fixes that harness
race. None of these failed runs count as full successful validation. Their
fixtures were removed and cleanup verified.

Initial offline output assertions incorrectly assumed a source `table` field,
omitted transaction metadata, then expected one update record for a primary-key
change. The corrected audit uses exact fixture topics and checks BEGIN/END,
delete/tombstone/create, row keys and values. These were test-oracle corrections;
no source decoder, dispatcher or failure guard was relaxed.

## Remaining admission work

The collector returns candidate metadata and a source observation N. N is not
completed output W, a durable checkpoint or a snapshot receipt. Existing legacy
progress/barrier and Kafka fencing tests remain separate evidence; the new native
collector is not connected to their publication path.

Before admitting an arbitrary already-streaming source:

- Establish the complete lock/binding scope from native journal membership,
  including files needed for transaction context but excluded from output.
  Qualify exclusion of unfinished autocommit operations, DDL and journaling
  changes, rather than inferring it solely from an update/clear conflict.
- Join retained original C/SC, savepoints and terminal context to the lock
  boundary. A file lock does not prove an empty global control inventory. Do not
  decode older row bytes with the newly acquired schema.
- Represent a binding origin N separately from an earlier control replay floor F.
  The current one-epoch binding history deliberately rejects an epoch after F;
  that check cannot simply be removed. Persist the qualified relationship before
  exposing any F/W transition.
- Join this evidence to the actual lagging legacy output barrier, then validate
  continuous native reads and real Kafka offsets across crash, reassignment,
  receiver rotation, busy retries and interrupted clear/reload output.
- Measure larger capture sets. Four-table timings do not bound the documented
  maximum 50-object allocation or arbitrary schema sizes. If the source cannot
  meet the collection budget, release and leave migration pending.

The existing deferred-handoff test already permits ordinary CDC to progress while
admission is pending. Automatic migration remains disabled until these source and
integration gates are met; no unsafe fallback is added.

## Reproduction

Build the test classes and existing `target/connect-probe-classpath.txt` first.
With explicit credentials for a disposable IBM i source, run:

```sh
mise exec java@temurin-21 -- python3 -B tools/dcg-online-fence-probe.py NEW_DIRECTORY DCNCdddd
```

This creates and removes only the fresh fixture, using the existing portable
native harness. No AWS helper is embedded in the repository. The saved
`runtime/manifest.json`, `harness.py`, SQL statements, raw entries and reports
identify what actually ran. `As400OnlineFenceReplay NEW_DIRECTORY`, invoked with
that staged classpath, performs the separate offline audit. Local unit/build
checks do not connect to IBM i.

[allocate]: https://www.ibm.com/docs/en/i/7.6.0?topic=beginning-allocate-object
[cursor]: https://www.ibm.com/support/pages/node/641911

## Native scope follow-up

The [scope qualification](dcg-lock-scope-validation.md) replaces fixture-defined
lock targets in a separate test entry point with exact-journal WRKJRNA discovery,
independent resource counts and actual member names. It preserves the original
collector and recovery tests. Current inventory consistency is now tested; atomic
exclusion of all membership/maintenance changes and a hard brief-pause bound are
still not established. Automatic migration remains disabled.
