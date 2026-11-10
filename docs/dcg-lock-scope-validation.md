# Native qualification of the automatic lock scope

This is a test-only extension of the [short-lock experiment](dcg-automatic-boundary-validation.md).
Automatic migration remains disabled. No source checkpoint, historical binding,
legacy barrier or F/W recovery rule is replaced by these observations. No
production Java, Kafka harness or F/CR change is part of this work.

## Complete current file/member discovery

`tools/dcg-lock-scope-probe.py` obtains the current external journal inventory
using IBM's documented [WRKJRNA output model][wrk], QSYS/QAWRKJRNA, format
QJOWRKJRNA. The command names one exact journal. Its output is confined to the
owning connection's QTEMP/DCGSCOPE; it does not install a persistent procedure.
`DETAIL(*OUTPUT)` requests the supported external object categories, including
access paths and IFS resources. It does not claim to enumerate internal
commitment definitions or certify their history.

Before collecting, the harness checks independently queried JOURNAL_INFO counts.
It caps the temporary output file at 128 records with no automatic increments,
sets default inquiry replies only on its owned job, and bounds fetched results.
Overflow, SQL warnings, unsupported resources, missing fields or count mismatches
reject. It does not disable the query governor or change system/job wait policy.
The collector supports this isolated local *SYSBAS profile, at most 50 physical
files with one member each. Remote journals, additional member counts, logical
files, access paths, IFS objects, data areas and data queues reject rather than
silently reducing the lock scope.

`As400OnlineLockScope` checks the complete returned inventory against independent
file, member and library counts. Every discovered file gets an exact bounded
SYSPARTITIONSTAT query. The actual member name drives both ALCOBJ/DLCOBJ and the
native member-identity path. The fixture deliberately adds EXTRA outside the old
A/B/M/U definitions and renames its member to DATA. All five files are found by
native discovery; fixture definitions only check the experiment's expected result.
Capture/output selection is never used to omit lock targets.

The collector refreshes WRKJRNA output before each scope observation inside the
same server-side acquire/collect/release compound as the original metadata
collector. Discovery, before and after inventories must match exactly. Existing
native file/member, format/schema, journal/receiver identity and chain validators
still run. A new profile identifier prevents dropping scope evidence from an
otherwise scoped capture. The result remains `startup_proof: false`.

The initially attempted JOURNALED_OBJECTS view was rejected before execution by
edgeqa's predictive query governor. Even an exact-journal predicate was estimated
at 37,603 seconds; the finite query limit was retained. The direct-journal command
avoids that plan. This is an observation about this installation, not a claim that
the SQL view is unsupported on every IBM i system.

## Qualification limits

Equal inventories establish consistent current observations, not the absence of
all intervening changes. New files can appear after discovery. A change can also
occur after the last inventory sample, or change and revert between samples.
Neither these samples nor file data locks establish an atomic journal-wide
maintenance fence. Historical schemas, complete in-flight C controls and the
lagging legacy output boundary remain separate requirements.

The five-second check is still a **between-native-call budget**, not a preemptive
deadline. In `live-08`, the concurrent ENDJRNPF case observed native member lock
waits, returned CPF7031, and caused the collector to report budget expiry after a
35.672-second call. Both jobs drained and cleanup passed. This disqualifies any
claim that the current collector always limits an application's pause to five
seconds. The exact internal lock dependency is not established by that trace.

The harness observes actual actor wait locks and server timestamps. It never
counts authority errors as lock conflicts. A completion after release without an
observed wait remains unqualified. The active-autocommit case uses an AFTER UPDATE
trigger with a bounded delay: the statement is demonstrably unfinished while the
allocation is refused. It does not claim that the journal was cut specifically
between R/UB and R/UP. Unrelated A inserts must make progress around the refused attempt; a partial
allocation can briefly delay A too. The final evidence also verifies progress
after refusal while B remains unfinished. This is source writer evidence, not
a new Kafka CDC continuity run.

## Evidence and reproduction

Raw evidence root:

`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-lock-scope-20261109/`

Build the test classes and existing `target/connect-probe-classpath.txt`. With
explicit `IBMI_TEST_CONFIG` credentials for an authorized disposable IBM i source:

```sh
mise exec java@temurin-21 -- python3 -B tools/dcg-lock-scope-probe.py NEW_DIRECTORY DCNCdddd
```

The optional `discovery` argument performs just discovery and candidate validation.
The full run adds autocommit, ALTER TABLE, ENDJRNPF and newly journaled file races.
It records raw metadata, SQL, native anchor buffers, bounded SQL journal payload
responses, actor jobs/timestamps, lock observations and partial metadata from
rejected collections. SQL journal diagnostics do not substitute for RPC replay. It drains owned
threads/connections before removing the fixture, then verifies schema/library and
receiver inquiries absent. No broker publication occurs in this experiment.

The checked-in `automatic-fence-native-scope.json` is the unmodified successful
candidate from `live-06/DCNC0946`, whose fixture cleanup passed. The later matrix
failure in that run was an unnecessary DROP COLUMN conflicting with cached opens;
it does not invalidate the earlier candidate. The final harness leaves that
column until whole-fixture cleanup after all connections close. Offline regressions
use this raw candidate to verify complete scope, the distinct EXTRA/DATA member,
existing native binding checks, incomplete/unsupported inventory rejection,
changed scope/member rejection and forbidden profile downgrade/promotion.

[wrk]: https://www.ibm.com/docs/en/i/7.6.0?topic=w-work-journal-attributes

## Completed matrix and verification

The final run is `live-10/DCNC0950`:

| Case | Observation | Disposition |
| --- | --- | --- |
| Native scope | Five physical files, five exact members, one journaled library; EXTRA uses DATA | Complete current candidate; never historical startup proof |
| Unfinished autocommit update | B allocation refuses with CPF1002 before the delayed statement returns; no retained partial allocation | Exclusion observed |
| ALTER TABLE ADD COLUMN | Native wait observed; completes after server release | Exclusion observed for this operation |
| ENDJRNPF | Member wait and CPF7031; collector reports budget expiry after a 35.910-second call | Conflict detected, brief-pause qualification fails |
| Create another journaled file | NEWFILE completes while existing member locks remain held | Not excluded; changed inventory rejects the candidate |

The baseline hold is 3.461 seconds; the ALTER overlap hold is 4.132 seconds.
The A writer completes 19 inserts; four complete after the B refusal but before
B's autocommit statement returns. These timings describe this five-file fixture,
not maximum bounds or large-installation performance.

The SQL diagnostic contains 43 journal entries, with a maximum original payload
of 6,640 bytes. `DATA_LENGTH` retains original length; IBM's SUBSTR expression
pads the saved 16,000-byte `DATA_HEX` result with zeros. The evidence audit verifies
that padding, preserving the raw responses unchanged. Fixed source anchors,
SQL text and parameters accompany the capture. No types are filtered from this
independent SQL diagnostic. The strict RPC reader rejects pointer-backed data in
the maintenance trace and terminates its owned read job. That rejection remains
intact; no complete RPC replay is claimed for this trace and SQL data is not fed
to the transaction buffer.

Every one of the twelve fresh libraries used by the discovery/debugging/matrix
runs was removed, with library/schema and receiver-inquiry absence verified.
There are no connection-cleanup errors. Failed attempts remain saved, including
the query-governor refusal, owned-job DFTWAIT authority failure, the unnecessary
DROP COLUMN conflict, a Python timestamp conversion error, and the first raw
reader rejection. Those attempts are not counted as complete matrix passes.

All 2,694 Java tests (200 parser, 2,494 connector) and 88 Python tests pass,
including 30 new native-scope cases. The first verify runs passed tests but
identified a test-file license-header mismatch. After correcting only that
header, verify checks passed with tests skipped to avoid another identical suite
run. Assembly packaging, formatter/import validation and diff checks also pass.
The prior Kafka and stricter recovery cases are preserved; no new Kafka run is
claimed by this source-evidence experiment.

The remaining concrete problem is a boundary that both excludes maintenance and
keeps the application lock interval bounded. Repeating current inventory samples
or raising the soft timeout does not solve either issue. Automatic migration must
stay disabled while a different collection/maintenance protocol is designed and
qualified, and while the existing historical-origin/control/legacy-offset gates
remain open.


Advisor inspected the actual repository files and retained evidence and passed,
with no file-access limitation. The subsequent change only sets the inherited
`source_connected` report flag after successful native preflight. Saved native
reports retain their original false placeholder; their successful preflight,
owned fixture operations and cleanup independently establish the live connection.
The final Python source differs from the staged run only by that bookkeeping
line, checked separately; native operations and assertions are unchanged. All
Python tests were rerun. The staged 749 files remain intact and all 553 local
classes match the verified build. Production source, HEAD/index and protected
worktrees match the pre-task inventory; nothing was committed or pushed.

The [next test-only boundary design](dcg-short-boundary-design.md) moves all
metadata work outside the held interval, proposes one small in-lock journal
marker, and audits scope/lifecycle changes after release. It explicitly retains
unproved maintenance visibility and native latency as gates. It is a design,
not a new implementation or permission to enable automatic migration.
