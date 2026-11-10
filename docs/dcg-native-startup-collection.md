# Native startup collection, 2026-11-09

This continues [the live online handoff validation](dcg-live-online-handoff-validation.md).
Automatic migration remains disabled. The normal task can already exercise the
qualified legacy barrier, binding-history activation, F/W recovery and ongoing
native reader through the explicit test provider. It does not yet acquire and
admit an arbitrary online boundary itself. This work removes fixture-defined
history and native-collection mechanics from that remaining integration; it does
not claim that the integration is complete.

## Native history discovery

`As400PendingReceiverEvidence` reads a bounded, parameterized complete receiver
directory and its pending-transaction flags. Independent counts and attachment
must agree before and after; unknown flags, unavailable receivers, partial links,
sequence resets, warnings and query failures reject. Observed native identities
remain separately required. The oldest required position includes every pending
receiver's first position and every original commit-cycle coordinate referenced
in the complete surrounding audit interval. It is a history floor, never W or a
published checkpoint.

The native test adapter requires its pending sample to lie between independent
native directory observations, both inside the raw audit bracket. It checks
native identities, exact page bounds, no continuation, every raw coordinate and
content digest before deriving the floor. This prevents a later sample, taken
after a transaction ends, from being substituted for the bracketed observation.
Complete C/J back-reading, original-start reconstruction and prospective binding
checks still run before the handoff candidate is accepted. A discovered floor
alone cannot certify historical row layouts or an already-published legacy prefix.

The SQL receiver query deliberately has no ORDER BY: each returned row is owned,
bounded and checked before the small complete directory is sorted locally. The
first live attempt, DCNC0975, exceeded IBM i's query limit with a server sort
(SQL0666). Its failure evidence is retained; no timeout or completeness check was
relaxed. That fixture and its inquiries were removed and independently checked.
Its original report still says `preparing`; the process failed, and the later
harness now records such exceptions as failed. The old report is not rewritten.

DCNC0976 then passed the native/Kafka handoff experiment. The busy attempt found
floor 4; the successful candidate found floor 46 without a fixture-supplied start.
All 15 runtime assertions passed, including actual native legacy-prefix rereading,
durable handoff, unrelated-table progress with an open clear, receiver rotation,
worker replacement, rollback, and ordered committed clear/reload output. Source
sessions, native workers, Docker resources, the library and all three receivers
were cleaned up; independent schema/library/inquiry absence checks passed.

## Remaining native mechanics moved into reusable code

`RetrieveJournal.readAuditHistoryPage` is a distinct all-code audit method using
the existing bounded raw parser, native continuation and terminal-stop lifecycle.
It omits the code-selection key entirely, including codes outside the enum. It
cannot be substituted for the distinct C/J control result or silently change
ordinary CDC's existing selection. The test collector now uses this method;
reflective injection of the parameter builder has been removed.

`As400ShortBoundaryProgram` renders the exact server-owned allocate/marker/release
program previously exercised on Edgeqa. The existing native-evidence validator
uses this renderer instead of a duplicate Java implementation. Its ordered exact
file/member scope is bounded, and no metadata query or forced journal write occurs
inside the held interval. Rendering is not execution, drain proof or startup
admission. Existing native marker, timing, scope, metadata and failure checks remain.

`As400JournalFileScope` discovers every journaled physical file from WRKJRNA's
native output and independent JOURNAL_INFO counts. A fresh dedicated nontransactional
job owns only QTEMP output. Before generation it caps that output at 128 records
and uses default inquiry replies; unsupported resource counts reject before the
command. Returned counts, journal, storage, file types, member counts and library
membership must agree. Acquired native bindings supply actual member names; neither
configured output tables nor fixture definitions select the lock scope. Current
scope observations still require the outer audit to detect intervening changes.

The explicit native test capture mode `scope` runs that collector and actual
binding acquisition, repeats scope discovery through another fresh dedicated
connection, and returns candidate evidence. The live harness compares its before
and after targets with the existing independent complete native collector, outside
the held interval. The captured DCNC0946 fixture tests discovery of the fifth file
EXTRA whose member is DATA, plus incomplete and unsupported inventories.

## Verification boundary and remaining work

Evidence is retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-startup-20261109/`.
Final local Maven verify passed 2,827 tests (207 parser and 2,620 connector).
All 134 Python tests passed, as did assembly packaging, formatter/import validation
and `git diff --check`. The protected-worktree audit confirmed all three other
worktrees and every recorded HEAD/index unchanged.

The successful `native-02` run exercised native pending-receiver discovery but
predates the final bracket-containment checks, extracted all-code reader, program
renderer and Java file-scope collector. The saved native bytes pass the final local
checks; that is not a live run of those later changes.

`native-03` was blocked retrieving Edgeqa credentials by ExpiredTokenException,
before any IBM i connection or creation of the proposed DCNC0977 fixture. The user then renewed the session. No production credentials, cluster access
or sink deployment were used.

The subsequent `native-04` / DCNC0978 run passed all 15 native/Kafka assertions
with the final Java collector and audit code. It exercised the production file-scope
collector and acquired member bindings both before and after the busy and successful
pulse attempts, comparing them with the independent native scope collector. The
actual raw audit used the extracted all-code reader; the final bracket-containment
checks and shared Java program renderer also ran. The normal task activated the
handoff, continued unrelated-table output during an open clear, crossed a receiver
rotation, recovered in a replacement worker, suppressed rollback output, and emitted
the subsequent committed clear before reload. No snapshots were emitted. Source
sessions, native workers and Docker resources drained; the exact fixture library,
all three receivers and inquiries were removed. Independent absence checks passed,
with no connection-cleanup errors. This validates the component composition under
the explicit test controller, not a general automatic-startup provider.

The explicit Advisor tool inspected actual files and retained evidence and passed
without an access limitation. That review occurred while the renewed native run
was pending; the subsequent successful run is additional validation, not a second
Advisor pass. No Java or Python implementation changed between that review and the
successful native rerun; only the evidence report was updated.

Before enabling automatic startup, the connector still needs the task-owned
collector/scheduler that composes these native observations and the short server
program, defers unsafe candidates while legacy publication progresses, and binds
the accepted candidate to the exact durable legacy barrier. Ordinary restart must
recover from binding history and Kafka offsets without the controller's bundle.
The existing fixed-schema, operator-managed retention, stop/drain, missing-offset,
transaction and legacy-prefix checks must remain. The live rerun must qualify the
new collector, then task-owned admission and its failure/reassignment boundaries;
the successful controller-owned experiment is not a substitute.

F/CR behavior and its separate worktree are preserved. No code is committed or
pushed, and no public migration mode is enabled by this work.
