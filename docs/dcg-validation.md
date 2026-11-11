# Transactional truncate validation scope

This change tracks [debezium/dbz#3027](https://github.com/debezium/dbz/issues/3027).
The current contracts are the [architecture](dcg-architecture.md),
[online configuration](dcg-online-configuration.md) and separate
[configured first-start profile](dcg-configured-native.md). Historical reports
record intermediate implementations and must not be read as current admission
requirements or current feature status.

## What is tested

| Layer | Behavior protected | Where to reproduce |
| --- | --- | --- |
| Offline Java | Native decoding, owned values, file/member identities, transaction and savepoint handling, F/W replay, snapshot acknowledgement, dispatch failure, stop/admission and immutable offsets | Maven verification and the captured/synthetic fixtures in `src/test/resources` |
| Offline Python | Explicit native-test authorization, credential redaction, bounded allocation/release, marker validation and cleanup safeguards | `python3 -B -m unittest discover -s tools -p 'test_*.py'` |
| Local Kafka Connect | Real task/dispatcher, durable acknowledgement, missing-offset rejection, interrupted batches, fencing and legitimate reassignment | `tools/dcg-ownership-probe`; CI runs `ack` and `offsets` on Kafka 4.2.0 and 4.3.1 |
| Packaged native connector | Initial snapshot, online handoff, no second snapshot, clear commit/rollback, receiver rotation, mixed values, unrelated-table progress and replacement | `tools/dcg-public-online-probe.py` with an explicitly authorized isolated IBM i fixture |
| Performance/restart | Old-versus-new workload comparison, complete update images after restart, open-clear progress and bounded sustained workload | `tools/dcg-performance-probe.py` and `tools/dcg-soak-probe.py` |

See [contributor testing](dcg-contributor-testing.md) for prerequisites, commands,
credential handling and cleanup checks. The original local Kafka `partition`
failure reproduction and the passing timeout-profile matrix runner remain
available. A passing synthetic Kafka run does not prove native startup semantics.
The unit tests retain real captured bytes as well as deliberately malformed
records; fixture provenance is documented beside the resources.

## Recorded native results

On the isolated IBM i 7.5 test system, commitment-controlled SQL TRUNCATE produced
D/CG subtype 8 and optimized DELETE produced subtype 3. Both could roll back
without individual row-delete journal records, which is why a clear cannot be
published before its transaction outcome. Outside commitment control the tested
clear operations produced F/CR. These are observed cases, not a promise about
all SQL execution plans, layouts or IBM i releases.

The 2026-11-10 ordinary-plugin online run passed 17 assertions with local Kafka
Connect 4.3.1: existing streaming offsets, busy-boundary CDC progress, durable
handoff without a second snapshot, open-clear receiver rotation, replacement,
commit/rollback, both clear subtypes, decoded mixed values and rejected unsafe
startup. The measured 126.883 seconds from release of a deliberately busy
transaction to completed handoff includes metadata and Kafka acknowledgement;
it is not write-lock duration or an activation SLA. The
[validation scope](dcg-validation.md)
records failed attempts, corrections, artifact identity and successful cleanup.

The [restart and soak report](dcg-restart-soak-validation.md) records a subsequent
71,401-record transactional run, including a clear held open for 62.15 seconds
while 12 unrelated-table bursts progressed, and a 904.78-second sustained phase
with task replacement. The final legacy correction separately passed a
15,250-record native run and two 5,000-row restart trials. All owned fixtures
and local resources were verified removed. The transactional soak preceded the
last overlapping-before-image correction; it was not rerun afterward. The report
identifies both artifacts and the narrower class-level comparison.

The [performance comparison](dcg-performance-comparison.md) preserves the original
failed restart results rather than dropping them from the baseline. Measurements
showed no material slowdown in the exercised workload; they do not establish
statistical equivalence, saturation capacity, many-table performance or multi-day
leak freedom. Private raw captures and JVM profiles are not repository fixtures
and must not be published without inspection/redaction.

## Limits and release checks

- Transactional capture remains off by default and qualified only for the
  documented native layouts, journal/receiver settings and Connect profile.
  Unsupported schema changes, missing history, ambiguous identity and unresolved
  transaction/savepoint semantics fail closed.
- Retention is operator-managed. The connector does not prevent an operator from
  deleting required history, authorize automatic receiver release or repair an
  arbitrary offset/history restore.
- Whole-batch replay can repeat previously published records. No exactly-once
  source contract, cross-topic order, universal pause bound or unrestricted
  dynamic schema evolution is claimed.
- Native tests use disposable fixtures and local Kafka. They do not validate any
  production deployment, downstream sink or every IBM i release/CCSID/topology.
- Before release, verify the exact candidate artifact, current Maven/formatter
  and Python checks, the CI Kafka matrix, and the applicable isolated native
  scenarios. Record source/artifact hashes, failures and cleanup outcomes; do not
  relabel historical evidence as a run against a newer build.

Upstream acceptance is separate from local qualification. Contributor registration
in Debezium core's `COPYRIGHT.txt` and `Aliases.txt`, DCO-signed commits and
maintainer agreement on the supported configuration still apply.

## Evidence handling

Published fixtures replace source system names, user profiles and job numbers with
fixed-width aliases, including identities embedded in D/CG payloads. Aliases retain
producer distinctions and binary section offsets; raw-byte checksums are recomputed.
These are sanitized regression inputs, not byte-for-byte copies of a private source.

Private investigation logs are not contributor prerequisites. The reproducible
commands and environment requirements are in [contributor testing](dcg-contributor-testing.md).
Historical successful runs qualify the artifacts they used; rerun affected cases
before treating later changes as native-validated.

## Review corrections — 2026-11-11

The correction build passes 3,071 Java tests and 148 Python tests, assembly,
formatter/import validation and workflow linting. Targeted regressions exercise
blocked queue interruption, legacy transaction metadata after a fresh restart,
terminal decode errors, bounded stop with unfinished native work, raw-page
continuation, repeated equal-key tombstones, large key-change batches and native
output compatibility. Connector tests/probes use typed construction and lifecycle
seams instead of reflective private-field mutation.

Native checks additionally exercised a 12,001-entry receiver with requested page
budgets of 1, 10, 501 and 10,000. One actual 1 MiB response held 3,854 entries;
the real decoder returned the requested prefix and resumed at the actual next
entry. Boundary-program failure cases verified that failed object-lock release
can survive connection close in a pooled job. Release handling now verifies lock
absence, distinguishes known server failure from an uncertain network response,
and never treats connection close as release proof. Every owned fixture was
removed after verification, including failed attempts.

Captured records cover save operations, grants, record-limit growth and classic
reorganization. The classifier accepts only documented metadata-only forms after
identity/payload checks; classic reorganization requires logical primary keys.
Clear companions permit both `OMTJRNE(*NONE)` and `OMTJRNE(*OPNCLO)`, with complete
before/after images still mandatory and remote journal filtering rejected. A live
run exposed the old hard-coded `*OPNCLO` check after successful admission; that
failed run and cleanup are retained, and commit/rollback/savepoint regressions
cover the corrected start-journaling payload validation.
Restore, unqualified DDL, concurrent or RRN-key reorganization, and abnormal-IPL
semantics remain explicit failures. Two concurrent writers produced 200 actual
updates with adjacent before/after pairs in this experiment; this does not prove
that every possible native workload has that property.

The final ordinary-plugin native run passed 17 assertions with a worker using
`America/Chicago` and German locale. Six DATE/TIME/TIMESTAMP/null cases matched
the actual legacy output after online handoff, without another snapshot. Commit,
rollback, worker replacement and termination of an exact owned transaction job
preserved ordering and unrelated-table progress. With `OMTJRNE(*NONE)`, the live
runtime also continued across GRANT, classic RGZPFM, SAVOBJ, save-while-active and
SAVLIB; metadata-only entries emitted no table changes. That native-tested artifact was
`b02df4334fdc53098e914a7a604b83bbf5fba45330dc38f036df0b564d8230e3`.
Native jobs, fixture library/schema/inquiries and owned Docker resources passed
cleanup checks. Earlier harness and profile-check failures are retained as
failures, with their own cleanup evidence; the final run is a separate fresh fixture.

Actual local Connect 4.2/4.3 checks also passed repeated-tombstone acknowledgement,
replacement/recovery, missing-offset rejection and a 125-second pause/resume.
Those reports identify the preceding artifact; its only runtime-class difference
from that native build is the subsequently corrected clear-companion profile
validator. They are not relabeled as tests of a different distribution.

Independent review subsequently found that repeated online candidate replacement
could exhaust binding-history recovery bounds. The correction checks record and
UTF-8 byte capacity before appending and restricts online selection to a bounded
immutable prefix. A racing stale append cannot invalidate a selected epoch or
gain publication authority beyond that prefix. Legacy CDC continues when no new
proposal fits. Six additional deterministic cases cover count/byte limits, an
oversized UTF-8 record, competing writers and fresh-task recovery. Strict history
overflow and malformed eligible records remain errors. This history-only change
postdates the first native-tested artifact above and received separate local
verification before the final native rerun.
Actual Kafka 4.3.1 then retained 1,025 proposal records: a fresh reader recovered
the acknowledged eligible epoch, refused selection of the extra proposal and
left broker history unchanged. The actual-task replacement, drain, rollback and
missing-offset checks also passed again on the corrected classes. Both local
experiments verified Docker cleanup. The resulting distribution is
`3c2e3a50e9152d901dde232db8066c6b70c274d52d56d4e4fe6273de8fe78515`.
That exact distribution then passed all 17 native assertions again in a fresh
isolated IBM i fixture, with `America/Chicago` and German worker locale. Online
handoff, temporal values, unrelated-table progress, task replacement, commit and
rollback, owned-job termination and the maintenance operations above all passed.
The fixture library, schema and inquiries were verified absent after cleanup;
the owned local Kafka resources and temporary credentials were also removed.

The [repeated performance comparison](dcg-performance-comparison.md#review-correction-comparison--2026-11-11)
retains both a slower intermediate candidate and the improved final candidate,
with exact artifact identities. No production system or downstream sink was
used. Published Git history and upstream submission scope are separate from
these local verification results.
