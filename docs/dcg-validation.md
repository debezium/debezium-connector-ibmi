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
| Deferred online activation | Clear outcomes before handoff, pinned legacy offsets, unrelated rows, savepoints, receiver rotation and replacement | `tools/dcg-deferred-clear-probe.py`, using the same isolated-fixture authorization and cleanup |
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
showed no material delivery-delay slowdown in the exercised workload; CPU costs
and matched-dependency comparisons are recorded below. They do not establish
statistical equivalence, saturation capacity, many-table performance or multi-day
leak freedom. Private raw captures and JVM profiles are not repository fixtures
and must not be published without inspection/redaction.

## Limits and release checks

- Transactional capture remains off by default and qualified only for the
  documented native layouts, journal/receiver settings and deployment profiles.
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

## Larger journal scope, keyless tables and row RRN — 2026-11-11

The subsequent build passes 3,103 Java tests, 148 Python tests, assembly and
formatter/import validation. It adapts PR #92's optional string `source.rrn`
to both legacy and buffered row output without changing keys or offsets.
Snapshot and truncate records have no row RRN. Captured fixtures verify that
IBM i can reuse an RRN after a clear. Keyless native tables retain null keys;
excluded tables retain identity and transaction effects without column codecs.
Missing/mismatched bindings, malformed images and capture-selection changes
still reject.

`tools/dcg-online-scope-probe.py` passed 12 assertions against a fresh isolated
IBM i fixture and local Kafka Connect 4.3.1, using the ordinary packaged task.
Its 56 journaled files cross the native 50-object command boundary. A conflict
in the later allocation batch released earlier allocations while legacy CDC
continued. Admission subsequently succeeded with a keyless mixed-column table
and an excluded GRAPHIC layout. Ongoing writes used the same writer connection.
With a keyless clear open, unrelated-table output progressed; a replacement
worker recovered durable history without a new snapshot. Commit emitted
truncate then reload (RRN `1`), rollback emitted neither, and excluded values
and clears created no data topic. Fixture library/schema/inquiries and all
owned Docker resources were verified absent afterward.

The tested distribution SHA-256 is
`02f2dce43ef23252e06b5155533acc98b10adf497fc9a89e0cafe7712bba2bfa`.
The busy attempt took 145.83 seconds to finish its observation and refuse;
handoff took another 229.18 seconds after the blocker was released. These
include native metadata, continued writes and Kafka acknowledgement, not a
write-lock duration. The successful server program passed the completed hold
bound, now conservatively including acquisition of the earlier lock groups.
Catalog batching reduces each 56-file observation from 112 catalog queries to
four; exact-path file/member identity reads and before/after comparisons remain.
This is a bounded activation measurement, not a steady-state throughput claim.

The then-current 4,096-file resource ceiling was covered locally, including generated program
shape and 1,040-table fresh-instance identity-history recovery; native execution
in this run qualifies 56 files, not the full ceiling. Existing history-byte,
audit, layout, receiver-retention, ordering and worker-fencing requirements still
apply. One journal/task, system ASP 1, single-member files and fixed captured
schemas remain the supported native model.

Two earlier attempts are retained as failures: one exceeded the test's original
observation deadline, and one completed handoff before the idle test writer
received an ODBC connection-disconnected error. The latter's disconnect cause
was not established. The final run maintained writer activity and completed all
cases; it does not establish a universal idle-connection guarantee. Cleanup
passed for each attempt, including a separately verified cleanup retry after
the first runner initially omitted its 52 additional fixture tables.

## Legacy configuration compatibility — 2026-11-11

The next local candidate passes 3,139 Java tests, 149 Python tests, assembly,
formatter/import validation and whitespace checks. It preserves existing-stream
`initial`, `no_data` and `when_needed` modes; zero public heartbeats; optional
tombstones; standard schema/table/column selection, masking, truncation, custom
keys, decimal and field-name choices; and unchanged-update skipping. Output
schema/value tests compare the actual ordinary and native Debezium builders.
Skipped unchanged updates still advance a completed recovery checkpoint.
Missing offsets, partial snapshots, absent history and incompatible settings
continue to reject before native initialization.

Catalog acquisition now keeps SQL table names separate from physical file/member
identity and uses the ordinary DDS-key fallback after an empty SQL primary-key
catalog. Completely read empty catalogs preserve null keys. Local physical-layout
tests cover Unicode graphic/varying fields, UTF-8 character fields and
`CHAR/VARCHAR FOR BIT DATA`, including corrupt stride and following-field bounds.
The native runs below separately check the acquired layouts and actual output.

The actual-task local Kafka compatibility experiment passed 11 assertions with
three-partition data topics and public heartbeats, tombstones and transaction
metadata disabled. It verified unrelated-table progress during an open clear,
durable F/W recovery by a replacement worker, late-reader fencing and completed
drain, commit/rollback output, and missing-offset rejection with unchanged output.
The audit reads all partitions and does not assert an ordering that Kafka cannot
provide across them. History/control remain single-partition. All owned Docker
resources were removed. The original single-partition restart scenario also
passed all 11 assertions on this candidate, with heartbeats, tombstones and
transaction metadata enabled; its owned resources were removed as well.

The first local Kafka attempt is retained as a failure: the acknowledgement gate
still assumed partition zero. The corrected gate validates the reported partition
against an explicit SourceRecord partition, or accepts Kafka's chosen nonnegative
partition when unspecified. Topic, record identity, offset, duplicate-ack and
whole-prefix commit checks remain. Targeted tests cover reordered acknowledgements
across partitions, a missing earlier acknowledgement and an explicit-partition
mismatch. The successful experiment's 322 production classes match the final
compiled connector and journal parser. Distribution SHA-256:
`f7db355d0bf628332eb64e24f7642634d07197aa17fd1d42bf4a08133d628304`.

The ordinary packaged connector then passed two fresh isolated native runs on
the same distribution: 12 assertions with four files, followed by 13 assertions
with a fifth DDS physical file. Both used existing `no_data` offsets, disabled
public heartbeats/tombstones and three-partition data topics. The native catalog
proved that the DDS file has a physical key but no SQL primary-key constraint;
legacy and native output retained that same key. The fixture used a nullable DDS
key definition with non-null inserted key values.

Unicode values were inserted using UTF-16 hexadecimal literals, and independent
SQL `HEX` reads verified the exact stored UTF-16, UTF-8 and binary bytes before
comparing legacy/native Kafka output. The checks also covered the long SQL table
name versus physical file name, decimal output, column exclusion/masking, custom
keys, `source.rrn`, no resnapshot, unrelated CDC during an open clear, immutable
history on task replacement, rollback suppression and one committed truncate and
reload. Audits consumed every data partition without claiming cross-partition
ordering. Both runs completed terminal drain and verified fixture rows, then
verified schema/library/inquiry removal and removed their owned Kafka resources.
All 322 packaged production classes match the verified compiled classes.

The first native attempt is retained as a failed harness attempt: it expected
the nonexistent `source.table` field. The corrected assertion checks the actual
logical SQL topic. The original text literals also underwent lossy conversion
before insertion; the successful runs use hexadecimal literals and exact-byte
assertions instead of accepting parity alone. Cleanup passed for that attempt.
Resolved source IPs are now registered for redaction in the automatic and live
handoff runners; an offline regression checks retained container-command logs.

Independent Advisor review passed the production changes after local verification
and before native qualification. The subsequent changes were to test assertions,
fixture coverage, evidence redaction and documentation; no production source
changed after that review. The earlier 56-file native run above tested its earlier
artifact, not this candidate. At that stage, 4,096 files was the resource ceiling,
also bounded by history/audit bytes; it is not a claim of a 4,096-file live run.

This is not completion of every compatibility gap. The original connector already
used one task and documented no remote journals or LOB/XML support. Native ASP 1,
single-member identity binding, fixed-schema/capture-history transitions,
alternative-runtime qualification, online transaction metadata, custom
Debezium converters and SMTs remain separate gaps or recovery requirements.
Their checks have not been removed. In particular, complete retained transaction
history and rejection of ambiguous or masked source data protect correctness;
matching an older unsafe behavior would not establish truncate support.

## Ordinary source-offset handoff — 2026-11-11

Online admission now uses ordinary durable source offsets when the actual task
context has no source transaction capability. The existing transaction-enabled
contract remains available and retains its stricter validation. This change
does not enable the feature by default or change operator-window initialization.

Local verification passed 4,020 Java tests (229 parser and 3,791 connector),
149 Python tests, plugin assembly and formatter/import checks. Fifteen actual
Engine cases exercise file offsets, initialization, legacy handoff, delivery and
restart. They include process replacement after an acknowledged legacy row,
handoff barrier and initial F/W; failed offset writes; missing history; and
bounded acknowledgement state while offset flushes lag.

Actual standalone Kafka Connect 4.3.1 passed the file-offset scenario twice.
In the final run, multiple reader cuts, a truncate/reload and unrelated output
completed in 5.162 seconds with a 60-second offset-flush interval. Killing the
worker before flush and starting a new one replayed the unsaved suffix from
the same offset file. An independent Kafka `FileOffsetBackingStore` reader
verified completed F/W. Missing offsets with retained history rejected with
zero output. These tests use controlled native I/O; they do not claim standalone
reassignment or fencing. The original transaction-enabled distributed scenario
also passed its 11 assertions.

Two isolated native runs separately qualified ordinary-offset legacy handoff
through the test entry point. One killed the worker after the acknowledged
legacy barrier but before initial F/W. Its replacement recovered that exact
boundary without writing an activation-history record. Both preserved busy
boundary deferral, native keys/before-images, unrelated-table progress,
receiver rotation, rollback invisibility, ordered truncate/reload and
missing-offset rejection.

The final native run used the **shipping plugin and public configuration**, with
no test provider, synthetic starting offset or injected native bindings. Worker
source transactions were disabled before ordinary legacy startup and online
opt-in. All 17 checks passed: actual snapshot/streaming offsets, busy-boundary
CDC progress, handoff without another snapshot, unrelated-table progress across
receiver rotation with an open clear, killed-worker recovery, rollback
invisibility, ordered committed truncate/reload, optimized DELETE commit and
rollback, mixed-column values, native drain, missing-offset rejection and unsafe
downgrade rejection. An independent raw journal audit confirmed two subtype-8
and two subtype-3 records. The handoff completed 111.849 seconds after release
of the deliberately busy transaction; that includes metadata and acknowledgement
work and is not the duration of the write lock or an activation SLA.

The tested plugin ZIP SHA-256 is
`781a6651b7e29f6f20233864d0a6463c7518c1c539fc2525c843b082d77fbfa8`.
All 249 packaged connector classes match the verified compiled classes and
have current production sources; test providers are absent from the plugin.
Source and harness hashes were retained with the raw evidence. The earlier
native setup attempt that failed in Python path handling is retained separately;
it did not exercise connector behavior and its fixture cleanup passed.
Every owned native fixture was verified absent, including its library, schema
and pending inquiries. Native worker drain and removal of owned local Kafka
containers/networks passed. Temporary credentials were removed after validation.

This ordinary-offset contract is at least once. It does not provide broker
fencing or atomic output/offset commits, and a crash may replay a delivered
prefix. That qualification used Kafka binding history and topic checks; the
file-backed alternative is qualified separately below. Engine
tests are not packaged Debezium Server tests, and the operator-window profile
has not been changed to use ordinary offsets. Other native identity, retention,
schema-transition and output-compatibility restrictions remain explicit in the
[compatibility baseline](dcg-compatibility-baseline.md).

## File history and broker-free Engine — 2026-11-11

Online capture now accepts Debezium's standard `FileSchemaHistory` configuration
with ordinary source offsets. The adapter uses the standard JSON-lines format
with strict UTF-8/record validation, bounded recovery, serialized file access and
forced writes/readback. It rejects missing or uncertain history rather than
repairing it or advancing a checkpoint. The Kafka-backed path retains its prior
fingerprints and checks. The operator-window profile still requires Kafka.

Local verification passed 4,037 Java tests (229 parser and 3,808 connector),
150 Python tests, plugin assembly and formatter/import validation. All 16 actual
Engine recovery cases use file history and file offsets. Coverage includes
history-file lock failure after the legacy barrier, fresh-instance recovery,
interrupted output, failed offset flush, malformed/truncated history, eligible
history-prefix bounds and unchanged-file assertions after rejection.

The shipping plugin then passed all 12 checks in an actual Engine process against
a fresh isolated IBM i fixture, without a Kafka broker or test native provider.
The connector produced its own initial snapshot and legacy offsets. Online
activation added no snapshot, and unrelated CDC continued while a clear remained
open and the journal receiver rotated. A killed process recovered the same
history and durable F/W. Rolled-back TRUNCATE and optimized DELETE produced no
clear/reload output; committed operations emitted truncate before reload.
Mixed-column values survived restart. An independent raw journal audit found two
subtype-8 and two subtype-3 records. Removing history caused zero output from the
replacement and left the actual offset file byte-for-byte unchanged.

The tested plugin ZIP SHA-256 is
`f3274692ee1ffc7e1ec4de90f8afc400ab9f417bec708e798f8ee552efe914e6`.
All 251 packaged connector classes match the verified compiled classes and have
current production sources. Source files were unchanged between assembly and
native qualification. Test providers and the local consumer helper are absent
from the shipping plugin. The first native attempt exposed a double-close in the
test consumer after legacy streaming; that helper was corrected before the
successful run. Both attempts retain their evidence and passed fixture cleanup.
The final run verified native schema/library/inquiries absent, process drain and
removal of temporary Java configuration files.

To reproduce with a fresh owned fixture and explicit test credentials:

```sh
IBMI_TEST_CONFIG=/secure/ibmi-test.json python3 tools/dcg-file-history-probe.py /absolute/new/evidence-directory DCNC1234
```

Build the plugin and test runtime classpath first as for the other native probes.
The runner uses the ordinary connector, its real offsets, a local durable file
consumer and the shared native fixture/cleanup checks. No broker is started.
This qualifies ordered at-least-once Engine delivery on the tested Linux local
filesystem. It does not supply cross-process fencing or atomic output/offset
commits, or arbitrary filesystem/converter/sink combinations. The separate Server
qualification is described below. Preserve the history file together with
the corresponding offsets and retained IBM i journal history.

## Server JVM/HTTP profile — 2026-11-11

A separate packaged JVM application used unchanged Debezium Server core and its
standard HTTP sink from source commit
`8fb514f8a4d37ad986c5bd697270d0e6d30f9eb4`, with Quarkus 3.40.0 and the shipping
IBM i connector. Its Debezium/Quarkus dependencies are 3.8.0-SNAPSHOT artifacts;
their exact hashes and the application POM are retained with the evidence. This
is a custom Server distribution containing the core and HTTP sink, not a test
source task. Its dependency build differs from the earlier Engine run and is
recorded separately. No Kafka broker or production service was used.

The standard HTTP sink delivered JSON values with schemas to a loopback-only
collector, which forced each received event before returning success. The
connector used standard file offsets/history and ordered processing. Source
properties had the `debezium.source.` prefix and were supplied through Server's
normal `config/application.properties` location. An earlier custom-configuration
location attempt did not populate the Engine configuration; the successful
application uses the standard location. Initial packaging/configuration failures
are retained separately and their native fixture cleanup passed.

All 12 native checks passed: ordinary snapshot and real file offsets; online
handoff without another snapshot; unrelated-table progress while a clear was
open across receiver rotation; process death and fresh-process recovery from
durable F/W/history; rollback suppression; committed truncate before reload;
optimized DELETE commit/rollback; mixed-column values; independent journal
confirmation of both clear subtypes; and missing-history rejection with zero
new output and an unchanged offset file. The HTTP sink carries values, so this
run checks row identity through those values. Actual record keys were inspected
in the separate Engine test.

The Server runner JAR SHA-256 is
`056e23ab5a36a2bccffe8032874aefd9f51d2b4e12beae474fd88931db6258e4`.
The source connector, parser and JT400 override JARs are the same binaries used
in the successful Engine qualification. The final native schema, library and
pending inquiries were verified absent. Every Server process and the local
collector stopped, temporary configuration directories were removed, and the
temporary test credentials were deleted.

This qualifies the tested JVM/HTTP/JSON profile with local file storage and
ordered at-least-once delivery. It is not a claim of every Server release,
native-image builds, every sink or failure mode, or output/offset atomicity.
Other native scope, schema-transition and converter/transform requirements
remain in the [compatibility baseline](dcg-compatibility-baseline.md).

## Scalable online binding history and inventory (November 11, 2026)

New online generations no longer impose a 4,096-file ceiling or a 1,024-record /
16 MiB history lifetime limit. Native inventory uses independently observed file
and library counts to bound its temporary outfile and JDBC read. Missing objects,
changed counts, unsupported resource kinds and incomplete observations still
reject. Fetch size never exceeds the observed row bound, as required by JT400.

The online history format stores one canonical table binding per bounded record,
followed by a descriptor binding their order, count, digest, native origin and
capture policy. No descriptor means no selectable generation. Recovery streams
the log twice and materializes only the selected catalog. Existing `v1:` offsets
retain their original physical eligible-prefix rules, including actual stored
bytes rather than reserialized JSON size. New generations use `v2:` fingerprints;
this is a forward format change, not permission to rewrite old offsets or history.

Local validation passed **4,059 Java tests** (229 parser and 3,830 connector),
plugin assembly, formatter/import checks and `git diff --check`. Targeted cases
cover interrupted and interleaved proposals, identical retries, missing/reordered
or changed pages, conflicting descriptors, foreign history, exact older selection,
legacy wire-byte limits, and prepare failures before any native allocation. A
synthetic history captured with the preceding connector build verifies unchanged
old fingerprints and native marker digests.

A 5,000-table catalog produced **20,499,732 bytes** of history. Writing, reopening
and checking idempotent reuse passed with a **96 MiB maximum Java heap** in the
JUnit test. A separate fresh-process replay of six retained proposal copies read
**122,998,392 bytes** with the same heap cap in **6.6 seconds**. These are bounded
local startup/recovery measurements, not a native throughput benchmark. An earlier
96 MiB run exposed redundant retained catalog copies; the corrected implementation
reuses verified immutable definitions and avoids materializing a proposal merely
to inspect its descriptor. Table metadata still scales with catalog size; this
qualification bounds transient serialization/history buffers, not all schema memory.

The large-catalog test can be repeated without IBM i or Kafka:

```sh
./mvnw -B -ntp test -DskipITs \
  '-Dtest=As400PagedBindingHistoryTest#shouldRecoverCatalogBeyondPreviousTableAndHistoryBoundsWithFreshInstances' \
  -Dsurefire.failIfNoSpecifiedTests=false -DargLine=-Xmx96m
```

For scopes over 50 files, parameterized command batches are populated outside the
held interval. A compact server SQL program traverses that job-local ledger; SQL
text and each JDBC batch stay bounded as file count grows. Allocation retains the
one-second overall acquisition budget and releases attempted groups in reverse
order. A failed ledger insert, warning or stop cannot start allocation. Ambiguous
transport results still require independently verified drain. This uses the
[IBM i SQL FOR statement](https://www.ibm.com/docs/en/i/7.5.0?topic=pl-statement),
[QCMDEXC expression argument](https://www.ibm.com/docs/en/i/7.5.0?topic=services-qcmdexc-procedure)
and [CHGPF outfile size](https://www.ibm.com/docs/en/i/7.5.0?topic=ssw_ibm_i_75%2Fcl%2Fchgpf.html)
contracts; metadata remains outside the held interval.

The final plugin also passed two isolated native runs: **12 checks with 56 files
in distributed Kafka Connect**, and **12 checks in the actual Engine with file
history/offsets and no Kafka broker**. The former covered a busy file in a later
allocation group, release of earlier allocations, continuing legacy CDC,
activation, unrelated-table progress with an open clear, replacement recovery,
keyless truncate/reload ordering and rollback invisibility. The latter covered
ordinary snapshot/offset startup, online handoff, receiver rotation, mixed-column
recovery, commit/rollback and missing-history rejection. Both used the ordinary
connector class and shipping plugin, not a test task/provider.

In the 56-file run, the refusal phase took **149.6 seconds** and successful
activation after releasing the busy writer took **228.5 seconds**. Those are
end-to-end preparation/audit/acknowledgement measurements, not write-lock hold
times; legacy writes continued during preparation. Larger native inventories
still need performance qualification. **5,000 tables were tested locally; 56
files remain the largest live scope validated here.**

Earlier attempts are retained as failure evidence: one exposed the JT400 fetch
size constraint (fixed and regression-tested), and another rejected an IBM i
`SQL0462` partial-metadata warning. The latter occurred while another isolated
fixture was being cleaned up; causation was not established. The subsequent
serial run passed. No warning or incomplete-evidence check was relaxed. All five
libraries used across the successful and failed attempts were removed, with
schema/library absence, released fixture locks and no outstanding fixture
inquiries verified. Temporary credentials and owned local processes were removed.

### Bounded native object-query batches

Paired file/member identity collection now groups at most sixteen exact paths
in a parameterized SQL request. The
[IFS_OBJECT_STATISTICS](https://www.ibm.com/docs/en/i/7.5.0?topic=services-ifs-object-statistics-table-function)
arguments and returned fields are unchanged; each invocation retains
`IGNORE_ERRORS => 'NO'`. Combining them with
[UNION ALL](https://www.ibm.com/docs/en/i/7.5.0?topic=queries-fullselect)
preserves duplicates for rejection. Every expected path must occur exactly once
with its expected object type, journal and complete native identity. Query and
fetch bounds include one extra row to detect excess results. Errors, warnings
(including a warning after the last row), interruption and missing/foreign rows
reject the acquisition before another batch starts. Both full observations
around schema acquisition and around the native boundary remain in place;
metadata is still collected outside the write-lock interval.

A same-fixture comparison used **56 journaled files**, with three captured
schemas and identity-only bindings for the remaining files. Each build ran three
fresh-connection binding acquisitions against the same unchanged fixture:

| Build | Acquisition times | Median | Object queries per acquisition |
| --- | --- | ---: | ---: |
| Before batching | 56.528, 54.745, 54.665 seconds | 54.745 seconds | 224 |
| Bounded batches | 17.360, 17.188, 16.889 seconds | 17.188 seconds | 14 |

All six results had identical native-observation and serialized-binding hashes.
The median acquisition time fell **68.6%**. This measures binding acquisition,
including both identity observations and schema reads, not complete activation,
write-lock duration or steady-state CDC throughput. Each build ran sequentially;
the small sample does not control for all server-load or cache effects. The query
count reduction is deterministic and separately checked in the local tests.

The same fixture then passed **12 ordinary packaged-connector checks** with local
distributed Kafka Connect: legacy snapshot/offset startup, a later allocation
group refusing a busy file and releasing earlier groups, online activation,
unrelated-table progress during an open clear, crash/reassignment, unchanged
history on recovery, committed truncate-before-reload ordering, rollback
invisibility, keyless RRN metadata and excluded-table suppression. The refusal
phase took **107.9 seconds** and activation after releasing the busy writer took
**150.7 seconds**. These full-path measurements include metadata, ongoing writes
and Kafka acknowledgement; they are not lock-hold times or a controlled
end-to-end comparison with the preceding run.

Local verification of that candidate passed **4,067 Java tests** (229 parser and 3,838 connector),
followed by a clean plugin assembly and formatter/import validation. The tested
plugin ZIP SHA-256 is
`de51fe563146fb1a1b088cfcdbf870254dcba486fa1ee1d3aad1f487ea7f0192`.
Its 251 connector classes match the compiled production classes; the obsolete
single-object decoding helper is absent. The earlier captured-row fixtures still
supply their independent before/after observations exactly once; combining their
results for a bounded query never supplies missing evidence.

Both isolated libraries used for the initial measurement and the comparison /
connector run were removed. Cleanup verified schema/library absence, released
fixture locks and no outstanding fixture inquiries. Owned local Kafka resources
and temporary credentials were removed. This change does not qualify larger
native catalogs, additional IBM i versions or additional column layouts.

A final input check found that two requests assigning different journals to the
same physical file could cross catalog-query batches and collide in the later
object-target map. The regression first demonstrated acceptance of this unsafe
input. Requests now require globally distinct library/file pairs before opening
any connection; a journal difference cannot make the same physical file distinct.
The existing cross-library test still accepts equal file names in different
libraries. Independent Advisor review passed the batching implementation before
this final guard was added; it did not separately review the guard.

The corrected final build passed **4,068 Java tests** (229 parser and 3,839
connector), assembly and formatter/import checks. A third fresh isolated library
then passed the same **12 packaged-connector checks** across 56 files. Refusal
took **107.7 seconds** and activation **147.0 seconds**. The tested final plugin
ZIP SHA-256 is
`e04cc151423322e37aefa4fc42b9423c38eaa3756a88e329298184ba06efdc36`;
all 251 production classes match the assembled artifact. This final fixture,
owned Kafka resources and temporary credentials were also removed and cleanup
verified. The acquisition timings above belong to the preceding candidate;
the final input guard changes request validation before any query is issued.

### Reusing native lock-observation statements

Each independent observer now prepares one authority-check statement and one
lock-query statement, reusing the handles across files. Both observer passes
still open fresh connections. Every file retains its library and file authority
checks and its exact library/file/holder-job lock query. No authority or result
is cached, and the server allocation/release program is unchanged. These queries
remain outside the interval during which the application write locks are held.

The [JDBC statement warning contract](https://docs.oracle.com/en/java/javase/17/docs/api/java.sql/java/sql/Statement.html#getWarnings())
clears statement warnings on re-execution. Warnings are therefore checked after
each command and after each lock-result cursor closes, before reusing the handle.
A later-file error, warning, remaining allocation or cancellation rejects the
attempt without proceeding to the next file. Cancellation after a completed
independent release observation preserves that release proof while preventing
the completion query or closing marker. Incomplete release observations still
produce an unverified-cleanup error.

Three sequential measurements per build used the same unchanged **56-file**
native fixture. Each measurement covered opening the pulse, both observation
passes, the server program, completion validation, closing marker and connection
close:

| Build | Complete operation times | Median | Lock-query prepares | Command prepares |
| --- | --- | ---: | ---: | ---: |
| Before statement reuse | 81.619, 82.117, 82.454 seconds | 82.117 seconds | 112 | 226 |
| Reused observer statements | 19.573, 19.333, 19.890 seconds | 19.573 seconds | 2 | 4 |

All six measurements used the same scope digest and executed **112 lock queries
and 226 commands** each: 224 authority checks and the opening/closing markers.
The median complete operation time fell **76.2%**. Native allocation/release
intervals were **144–190 milliseconds** in the optimized samples; for this
multi-group program that interval includes acquiring and releasing the groups.
The roughly 20-second operation time is not a write-lock duration. This small,
sequential sample does not control for every server-load or cache effect, and it
does not measure steady-state CDC throughput or qualify larger native catalogs.

The profiled candidate ZIP SHA-256 was
`261c13973e62d5930b6b3878ad6c8bbf3cd086f2382b87ab413148141657ffc0`.
The final build adds stop checks around observer connection close, covered by
two additional cancellation/drain tests; it does not change the measured SQL
operations. Final local verification passed **4,091 Java tests** (229 parser and
3,862 connector), assembly and formatter/import validation. The final plugin ZIP
SHA-256 is
`07b38d3ba89abb3129cadad6b1e9d3c7515d2bdd57f79cdf4ce711e828724386`;
all 251 connector classes match the compiled production classes.

That final artifact passed **12 ordinary packaged-connector checks** on the same
56-file fixture with local distributed Kafka Connect. Coverage included refusal
and release when a later allocation group was busy, continued legacy CDC,
activation, unrelated-table progress during an open clear, worker crash and
replacement, unchanged recovery history, committed truncate-before-reload
ordering, rollback invisibility, keyless RRN metadata and excluded-table
suppression. The refusal phase took **45.7 seconds** and activation after
releasing the busy writer took **87.5 seconds**, compared with 107.7 and 147.0
seconds in the preceding build's run. These end-to-end observations include
ongoing writes and Kafka acknowledgement; the earlier run used a different
fixture and is not a controlled comparison.

Both isolated libraries used for the initial profile and the comparison /
connector run were removed. Cleanup verified schema/library absence, released
fixture allocations and no outstanding fixture inquiries. Owned local Kafka
resources and temporary credentials were removed. The largest live scope
validated remains **56 files**; this change does not add native version, schema
or steady-state throughput coverage.

Independent Advisor review inspected the final lock-observation changes,
stop/drain handling, physical-file uniqueness guard and verification evidence.
It found no actionable issues, closing the earlier guard's independent-review
gap as well as reviewing this optimization.


## Paged boundary audits and receiver inventories

The raw boundary audit reads native pages of at most 1,000 entries and retains
large captures in private scratch storage. It no longer requires one complete
native page or rejects a boundary solely for exceeding 2,000 entries. The exact
A/N/Z marker, native identity, source-byte, ordering, transaction-discovery and
cleanup checks remain in place. Control-history and prospective legacy-prefix
replay were extended separately in the working-history validation below.

Local verification passed 4,129 tests (229 parser and 3,900 connector cases).
Subsequent targeted checks covered scratch admission for memory transactions;
all 150 Python harness tests also passed. Coverage includes failed continuation
pages, missing/foreign/repeated entries, corrupt or truncated scratch files,
quota exhaustion, cancellation, and an unconsumed capture at normal or failed
native shutdown. A child JVM with a 64 MiB heap wrote and read back a 128 MiB
raw capture. This demonstrates bounded capture working memory, not the memory
footprint of a whole Kafka Connect worker.

Receiver inventories now use independently observed IBM i counts and exact
linked reads. Local fixtures cover complete 5,000-receiver directories,
underreported or changing counts, late malformed pending flags, exact identity
queries beyond 1,024 objects, and validation of every required receiver identity
and reciprocal link. Catalog metadata remains proportional to the number of
receivers; these fixtures do not establish native performance at that scale.

A fresh isolated native run used the ordinary packaged connector, local Kafka,
56 journaled files and concurrent inserts. It passed all 13 checks, including a
6,404-entry boundary audit, every expected ongoing insert, busy-boundary release,
unrelated-table progress during an open clear, killed-worker recovery, commit
ordering and rollback invisibility. Independent paged journal evidence retained
12,061 entries. Fixture, session and local Kafka cleanup passed. An earlier
attempt validated 6,604 entries but its test observer rejected more than 1,000
Kafka records; that attempt is retained as a failed test run. The repeat uses the
existing larger independent Kafka observer and retains all output assertions.

These native results qualify the paged boundary and the ordinary receiver
inventory queries for this fixture, not thousands of native receivers. Scratch
configuration admission was tightened after that native run and checked locally;
subsequent performance runs use the updated assembly. No production system was
accessed and the separate F/CR branch was unchanged.

## Repeated native performance comparison — 2026-11-11

Eight successful isolated runs compared the original source baseline
`2e19ad27c934826d96a84202dfbe4509b5d8f666` with the current packaged connector:
truncate disabled, enabled with memory working storage, and enabled with disk
working storage. Each profile ran twice; the second half reversed profile order.
The worker used the same Kafka 4.3.1 image, shared Debezium dependencies, two
active JVM processors and a 512 MiB maximum heap. The ordinary packaged
connector produced the events; an independent continuous `read_committed`
consumer checked every expected key, mixed-column value and update before-image.

Each run excluded two warm-up rounds and measured five rounds per workload.
The table gives median host-submit-to-last-observation times over ten trials per
profile. These are finite batches, not sustained throughput or source-commit
latency. The p95/p99 values retained in the report are trial percentiles relative
to batch submission, not a long-running per-row latency distribution.

| Profile | 250 integer inserts | 250 mixed inserts | 250 mixed updates | 5,000-row restart and drain |
| --- | ---: | ---: | ---: | ---: |
| Original | 5,423 ms | 5,635 ms | 5,423 ms | Not qualified |
| Current, truncate disabled | 5,397 ms | 5,613 ms | 5,404 ms | 13,794 ms |
| Current, memory | 3,768 ms | 3,793 ms | 3,794 ms | 6,144 ms |
| Current, disk | 3,763 ms | 3,766 ms | 3,814 ms | 6,598 ms |

The disabled path differed by less than 0.5% from the original. Enabled memory
capture completed these batches 30–33% sooner. Disk batch medians were within
1% of memory; its restart median was 7.4% longer. Each current profile passed
four restart trials with exact output checks and no duplicates. Resume timing
includes task restart and recovery validation; source preload is excluded.

Whole-worker resource samples cover only the measured burst interval. Median
CPU time per delivered row was 2.773 ms for the original, 2.776 ms disabled,
2.011 ms enabled-memory and 2.163 ms enabled-disk. Median run peak resident
memory was respectively 470, 461, 431 and 442 MiB. Median total GC pause was
12, 19, 30 and 58 ms per measured interval. These observations show no serious
regression for this workload; they do not establish a worst-case heap bound,
sustained capacity, or the cost of very large open transactions.

An earlier original-connector restart attempt delivered one duplicate among
15,250 expected events: its saved legacy offset remained one journal entry
behind the last delivered row. The strict duplicate assertion failed and that
run is retained separately. Neither the original binary nor the assertion was
changed. Subsequent original runs measured uninterrupted bursts only; there is
no qualified original-versus-current restart comparison. Current restart runs
retained all assertions.

Every run, including the failed original restart attempt, verified native
fixture/session cleanup and removed its owned Kafka resources. The original ZIP
SHA-256 was
`da0cbb3ab5228fbe676520e905b88f36158f4adc0a73a856bd18b4583908339a`;
the current ZIP SHA-256 was
`cdbdc742ff5f43d42dbad18507d4b00aa36ef1ca714e6b3596c2f95732de0868`.
Raw consumer evidence, per-trial measurements, JFR captures, resource samples
and artifact provenance were retained. Independent Advisor review inspected the
paged-capture and receiver changes, actual files and scoped verification evidence
without access limitations and found no actionable issue.

## Paged handoff working history — 2026-11-11

Control discovery and prospective legacy-prefix reconstruction now use the same
owned paged raw captures, plus disposable disk indexes. Production handoff no
longer rejects total history solely for crossing 128 pages, 20,000 entries or
64 MiB. Each native page and raw entry remains bounded, and configured transaction,
lifecycle and disk budgets still apply. Scratch does not authorize a checkpoint:
a replacement reconstructs from the source and immutable binding history.

Generated local cases reconstruct 33,000 source entries and 22,000 controls across
more than 128 pages, then repeat from a fresh reader with empty scratch storage.
A separate 64 MiB-heap child processes 2,600 32 KiB row payloads through the real
decoder, normalization and handoff state machines. Fault cases retain unchanged
pending history and clean scratch after source I/O failure, repeated or foreign
entries, invalid continuation, mismatched control projections, a late corrupt
row, cancellation, raw quota and index quota exhaustion. Existing prior-schema,
missing-start, open-transaction, selected-prefix and lifecycle-overlap guards
run against the disk-backed history too.

Full Maven verification passed 4,151 Java tests. The final cancellation and
owned-view adjustments passed 76 affected cases, followed by assembly and
formatter/import validation. The ordinary packaged artifact with SHA-256
`85e81ba0d08ffb82eb13cb7cf22a67ccf6969ed26703c5c466d479f4a793dd82`
passed 14 checks on a fresh isolated 56-file fixture with local Kafka and
concurrent writes. The boundary audit contained 6,504 entries and final handoff
reconstructed 3,000 source entries. Checks included scratch removal before
continued capture, keyless RRN, an excluded GRAPHIC layout, unrelated-table
progress, killed-worker recovery and commit/rollback ordering. Fixture/session
and local Kafka cleanup passed. This native run does not itself qualify a
33,000-entry handoff or 5,000 native receivers; those are local generated cases.

The repeated performance matrix above used an earlier artifact. Its steady
streaming, replay, dispatcher, transaction-buffer, task and recovery-source class
bytes match this handoff artifact; the changed classes concern startup capture
and history. The earlier Advisor pass covered paged audits and receiver scaling,
not these subsequent handoff-index changes.

## Background discovery and delivery-header ownership — 2026-11-11

Control-history discovery now runs on the existing collector thread while legacy
CDC continues. The collector returns owned definitions and discovery positions,
then closes its raw captures and indexes. History publication, exact-prefix
reconstruction and activation remain on the source thread. This does not make
the final fixed-prefix reconstruction asynchronous or constant-time.

A fresh isolated native run held the collector before discovery completed and
proved that an independently submitted legacy row reached a durable source
offset without activation or binding-history publication. Stopping drained the
collector and removed scratch. A replacement reacquired the boundary, delivered
the concurrent rows exactly once and took no additional snapshot. All 14 checks
passed, including unrelated-table progress through receiver rotation while a
clear stayed open, killed-worker recovery, rollback invisibility, committed
truncate/reload order, key changes and missing-offset rejection. Native fixture,
session and local Kafka cleanup passed.

An earlier attempt exposed a real header-ownership defect: the legacy dispatcher
shares headers between a delete and its tombstone. Registering the delete had
added the internal delivery marker to both records. Both acknowledgement
contracts now publish their own record and duplicate the headers; their exact
original-to-published mapping also preserves the connector's transaction-boundary
request. Regressions cover shared application headers, repeated equal tombstones,
unchanged F/W, complete-prefix acknowledgement and late callback rejection.
The local Kafka 4.3.1 normal-task duplicate scenario passed with 300 repeated
tombstones crossing the 256-record delivery boundary and a restart.

Full Maven verification passed 4,153 Java tests, followed by assembly and
formatter/import validation. The native repeat used the corresponding current
classes; its test-only plugin dependency staging now includes H2 MVStore. The
ordinary distribution already included that dependency. Both the earlier header
failure and the intervening missing-test-dependency failure are retained, with
successful cleanup. The ordinary distribution ZIP SHA-256 was
`c1060ead39b39bdfce9795c8ce29d0b76ec648923b79a866615d95f4cd1b330d`.
The earlier Advisor pass does not cover these later changes.

## Continuous offered-load comparison — 2026-11-11

Four subsequent serial runs used the same original ZIP and the delivery-header
ZIP identified above. Each used a fresh isolated source library, the ordinary
packaged connector, Kafka 4.3.1, two active JVM processors and a 512 MiB heap.
After two warm-up rounds, an independent writer submitted 100-row operations at
up to two SQL statements per second for about one minute, cycling through
integer inserts, mixed-column inserts and mixed-column updates. The writer never
waited for CDC. Each profile delivered all 12,300 expected operations with exact
keys, values and update before-images, no late duplicates, and verified cleanup.

| Profile | Median delivery | p95 delivery | p99 delivery | CPU per row | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original | 5,237 ms | 5,373 ms | 5,529 ms | 0.843 ms | 607 MiB |
| Current, truncate disabled | 5,225 ms | 5,328 ms | 5,411 ms | 0.874 ms | 407 MiB |
| Current, memory | 5,303 ms | 7,095 ms | 7,791 ms | 0.897 ms | 438 MiB |
| Current, disk | 5,369 ms | 7,062 ms | 7,303 ms | 0.979 ms | 452 MiB |

These times run from host batch submission to independent `read_committed`
observation, not from source commit. Each profile has one continuous-load trial;
the results are neither maximum capacity nor a long soak. The original and
disabled paths use Connect poll transaction boundaries; the enabled path uses
its connector-managed boundaries. Total worker resident memory includes native
memory and can exceed the configured Java heap.

The default path again had similar latency. Both enabled storage modes showed
about 32% higher p95 latency than the original under this offered load. Disk CPU
per row was about 16% higher than the original and 9% higher than enabled memory.
This is a remaining performance difference, despite the earlier finite-batch
results. Socket recordings show most enabled-path native read time in JDBC
metadata transport; they are transport observations, not exact SQL request
counts. Subsequent interval-connection work must be measured separately before
claiming that it resolves the difference.

### Repeated comparison after interval connection reuse

The subsequent ordinary distribution
`3f92ac7fb79fd06b75b68758816e813faa4fe8830030cc178068cbf38f8cd8f2`
uses one qualified connection per native read interval. It still makes fresh
before/after identity and receiver checks; it does not cache those observations
across intervals. Open/read/close failures prevent an interval from escaping or
advancing F/W. A close failure cannot be mistaken for a safely retryable cut.

Four more serial trials used the same 60-second workload, 512 MiB worker heap,
two-CPU limit and Kafka 4.3.1. Run order was enabled disk, original, enabled memory,
then disabled. Every trial observed all 12,300 operations at roughly 201 offered
operations/second, checked exact values/before-images and excluded late
duplicates. Every native fixture and owned Kafka resource was cleaned up.

| Profile | p95 delivery | p99 delivery | CPU per row | Peak worker RSS | Maximum observed heap after GC |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original, repeat | 5,203 ms | 5,215 ms | 0.847 ms | 457 MiB | 43 MiB |
| Current, disabled | 5,207 ms | 5,217 ms | 0.882 ms | 436 MiB | 43 MiB |
| Current, memory | 3,689 ms | 3,779 ms | 0.897 ms | 441 MiB | 52 MiB |
| Current, disk | 3,710 ms | 3,827 ms | 1.046 ms | 437 MiB | 51 MiB |

The preceding enabled-path latency regression did not recur: p95 was about 29%
lower than the repeated original and about 48% lower than the preceding enabled
runs. Disabled latency remained essentially unchanged. CPU per row was 4.1%
higher for disabled, 5.9% higher for enabled memory and 23.4% higher for enabled
disk than the repeated original. Disk cost 16.6% more CPU than enabled memory.
Do not describe the disk option as free or all performance as identical. Its
purpose is bounded payload memory for large working sets. This remains a bounded
offered-load comparison, not a maximum-throughput, multi-hour soak or statistical
equivalence result. The earlier failed-performance observation remains above.

## Output metadata and ordinary configuration compatibility — 2026-11-11

Online startup accepts retained snapshot SELECT/query/locking settings without
executing another snapshot. Operator-window initialization still rejects those
overrides. Built-in topic delimiter and heartbeat/transaction names, plus
source-type propagation, are pinned when non-default; explicit defaults preserve
the old fingerprint. Both existing built-in naming strategies retain their
ordinary behavior.

The live comparison found that physical catalog widths and type names were not
the legacy output descriptors. Acquisition now reads exact, escaped JDBC table
metadata on the qualified session and validates column identity, order, type,
nullability and identity flags against the independently verified physical
layout. Integer precision, graphic/binary names and explicit zero scale follow
`JdbcFileDecoder.getRecordFormat`, including its zero-for-null scale behavior.
Old immutable generations are not reinterpreted. Missing, partial, conflicting
or failed metadata reads reject acquisition before history/publication.

The ordinary packaged ZIP
`3f92ac7fb79fd06b75b68758816e813faa4fe8830030cc178068cbf38f8cd8f2`
passed 14 native checks in a fresh isolated library with local Kafka. The test
compares actual legacy and new key/value schemas with source-type propagation
enabled, a non-default topic delimiter, three data partitions, an SQL name
different from its physical file name, DDS keys, masking and column exclusion,
Unicode/binary values, no heartbeat and no tombstones. It also verifies no
resnapshot, unrelated-table progress during an open clear, worker restart,
commit/rollback output and fixture/session/Kafka cleanup. Three preceding failed
comparisons are retained with successful cleanup; the final assertion was not
weakened. Three partitions do not provide global truncate/row ordering.

The connector-specific 4,096-column ceiling is replaced with IBM i's documented
[8,000-field limit](https://www.ibm.com/docs/en/i/7.5.0?topic=concepts-database-file-sizes).
Local tests acquire and reload an 8,000-field binding, decode its last nullable
field, and spill 8,001 owned values including an explicitly configured RRN key.
Excess fields, an invalid final null indicator and insufficient byte budgets
still reject. This is local wide-row evidence, not an 8,000-column native test.

Full Maven verification passed 4,199 tests (230 parser, 3,969 connector). After
the final streaming-scale correction, 256 affected tests and assembly with
formatter/import validation passed. All 151 Python harness tests passed.
Historical component fixtures explicitly label their synthetic JDBC/RCAC
assumptions; unaugmented captures lacking this evidence cannot pass the current
native metadata validator. The earlier Advisor pass covers the initial paged
audit/receiver changes only, not these subsequent compatibility corrections.

## Explicit character conversion and header configuration — 2026-11-11

The new path accepts the existing explicit `from.ccsid`/`to.ccsid` substitution
for matching text codecs. It validates physical metadata first and retains
storage widths, binary fields, numeric fields and temporal interpretation.
Persisted bindings contain the effective codec; the fingerprint includes an
active mapping, while disabled and identical mappings retain the old policy.
Malformed settings, implicit job encoding and binary output targets fail before
native connections open. Tests include fixed and varying text, nonmatching
fields, saved-history recovery, missing physical CCSID and byte-budget checks.

The ordinary packaged ZIP
`fb6dfbeb68c0112e3b5c2ae23d196fae6a0158d454f1a888b7528a9fcbaba732`
passed 17 native checks and cleanup. The fixture independently verifies stored
bytes `BA BB B0 5A 40`; both actual legacy and new output decode them as `¬|¢] `
with an explicit 37-to-500 mapping. Restart retains the saved schema and codec.
The same run preserves Unicode/binary output, column/key options, unrelated-table
progress during an open clear, rollback, committed truncate/reload, three data
partitions and no public heartbeat or tombstones. This qualifies this explicit
mapping, not every CCSID pair. Full Maven verification passed 4,213 tests and
assembly/formatter/import checks; all 151 Python harness tests passed.

Explicit header converters and their serialization settings are also pinned.
An absent override preserves the prior fingerprint. Credential classification,
secret-bearing URL rejection and changed-history rejection include header
configuration. The 217 affected configuration/fingerprint/recovery tests passed.
A subsequent native run reached recovered output and rollback but failed on an
incomplete native journal metadata observation before the final commit check;
its evidence and successful fixture/Kafka cleanup are retained. It is not
reported as a successful header-converter qualification.

IBM's [SQL0462 guidance](https://www.ibm.com/support/pages/sql0462-queries-over-ibm-created-sql-views-qsys2)
and [OBJECT_STATISTICS diagnostic fix](https://www.ibm.com/support/pages/apar/SE74160)
describe warnings caused by authority, missing objects or object locks inside
metadata views. The installed journal view enumerates libraries internally.
The observed generic warning does not establish which cause occurred. The
runtime now discards the entire unpublished interval and allows at most two
complete recollections for an isolated SQL0462/01548 warning. Repeated warnings,
additional chained warnings, failed connection close, cancellation and all
other evidence errors remain failures. Warning-bearing rows never become proof,
output or offset advancement. The 166 affected evidence/reader/runtime tests and
assembly with formatting/import validation passed. Local tests cover late
warnings, persistent failures, close failure and unchanged scan/durable F/W.

The repeat using ordinary plugin
`6a8fd1e800f2b0639e9d83ca439c5cde038fb01741dd3673dd9383179eb12f53`
passed all 17 native checks and fixture/Kafka cleanup, including the explicit
JSON header converter, restart, rollback and committed truncate/reload. No native
metadata warning recurred in that run; the retry/error boundaries are established
by controlled local tests. The full local suite passed 4,228 tests. A subsequent
mixed warning/receiver-race regression prevents those failures from entering
the attachment-only deferral path; all 167 affected tests passed afterward.
These native checks do not qualify arbitrary header-converter implementations
or change the existing prohibition on tolerated conversion failures.

## Native schema reconstruction feasibility — 2026-11-11

An isolated source recorded three D/CG subtype-0 entries: add an integer column,
drop that column, and add a four-character column under the same name. The raw
3,328-byte payloads and their hashes were retained. IBM's
[QDBRPLAY API](https://www.ibm.com/docs/en/i/7.6.0?topic=q-replay-database-operation-qdbrplay)
replayed each change into an empty shadow file through a guarded rename exit.
Names-only calls left the shadow unchanged; applying each entry produced the
expected successive layouts. The original source schema and six rows remained
unchanged. The fixture, helper program and source sessions were cleaned up.

A second prototype used only QTEMP tables, source members, a compiled CL exit
and a user space inside its own command-server job. Two fresh jobs independently
reconstructed all three layouts from the saved payloads after the original
fixture had been deleted. [QUSLFLD](https://www.ibm.com/docs/en/i/7.6.0?topic=q-list-fields-quslfld)
reported the expected field names, native types and widths. An intentionally
unmapped target produced CPF9898 and left the shadow at its baseline. Every
owned temporary object was explicitly removed and checked absent in the
successful run. An independent JOB_INFO check confirmed that all nine command
jobs from the temporary-prototype attempts had ended; the original library
remained absent. These are direct API experiments outside the connector entry
point, not end-to-end schema evolution or journal-row decoding tests.

Earlier failures are retained: a primary-key constraint is not permitted in
QTEMP, and this QUSLFLD implementation leaves its generic subset flag byte zero.
The successful experiment omitted the constraint and checked the two known
short-name fields individually. It does not qualify arbitrary catalog output.
A SQL external-procedure wrapper was deliberately not created: IBM documents
that a QTEMP external procedure can leave catalog entries after job termination.
The direct command/program-call experiment avoids that wrapper.

The callback's packed native layout was checked against the installed QSYSINC
header and captured arguments. Its object-use field is at byte 532 in a
537-byte structure; the current web table reports a different offset. Unknown
lengths, object types, target uses, names and libraries are rejected by this
prototype. No opaque internal DDL data was interpreted as SQL or column metadata.

A further fresh fixture captured native bindings before and after each of the
three changes and retained 40 entries from the ordinary QjoRetrieveJournalEntries
reader. The three raw DDL payloads exactly matched the SQL-captured payloads.
After fixture cleanup, two new temporary jobs reconstructed all four native
record-format IDs, record lengths and field counts, including the repeated
baseline after dropping the added column. The strict decoder and source
normalizer recovered the three selected autocommit inserts as `[10, 42]`, `[11]`
and `[12, "text"]` using the captured epoch bindings. Deliberately using the
integer-era decoder for the equally wide text-era row produced a wrong integer;
width checks alone cannot make that transition safe.

This diagnostic deliberately selects those rows and supplies their epoch
bindings. It does not admit the surrounding lifecycle records or make the
connector discover schema epochs automatically. Each ALTER included F/CH, F/EJ,
D/EF, D/JF and F/JM before D/CG, while the sampled file/member journal IDs stayed
constant. The stop/start-journaling sequence needs its own complete historical
validation; equal endpoint IDs do not prove that no unjournaled changes occurred.
The first capture runner failed while serializing a Decimal-valued diagnostic
after capture; cleanup passed. A separate continuation verified those saved
captures and completed the native format comparison. Both results are retained.

If historical reconstruction is pursued separately, removing the related
schema-transition guards would require broader validation:

- Complete journal-entry acquisition, including pointer-backed and oversized
  data, with native ownership released on every failure.
- A trusted starting schema and matching native format identities beyond the
  three tested changes; replay success alone is insufficient because the API
  can continue after file/member level-ID warnings.
- Capture keys, constraints, defaults, long names and dependent objects. The API
  does not rename every internal dependency, and it does not replay under
  commitment control. A names-only pass is not a general sandbox guarantee.
- Ordered row decoding across multiple changes, retained before-images and
  in-flight transactions, including restart before and after history and output
  acknowledgement. Selected autocommit row decoding above is not that integrated
  proof. Failed reconstruction must leave durable F/W unchanged.
- Immutable history publication before new-schema output, and admission of
  journal membership/capture-selection changes without invalidating older replay.

These experiments are retained as optional research. General historical schema
reconstruction is outside the original connector's guarantees and is no longer
a compatibility requirement for this change. No dedicated working library is
required or awaiting approval. Basic current-definition refresh remains in
scope; unknown historical layouts must still be rejected. Neither the API
prototypes nor their results enable schema transitions in the shipping runtime.
See the [compatibility baseline](dcg-compatibility-baseline.md).


## Compatibility completion checks — 2026-11-12

These checks compare against the preserved original `2e19ad27` plugin, not merely
inherited Debezium settings. Historical DDL reconstruction and a shadow working
library are outside that baseline. See the
[capability audit](dcg-compatibility-baseline.md#baseline-audit-and-subsequent-compatibility-fixes-november-12-2026).

The packaged native schema/selection run passed observed column add/drop,
replacement with an unrelated transaction retaining F, and capture selection
expansion to a previously excluded table with an open clear. History was reused,
no snapshot was added, and committed truncate/reload order was preserved.

Requested full snapshot validation passed with an additional 300 rows, an open
unrelated transaction, durable snapshot completion, ordinary restart and a later
clear/reload. Local actual-task cases also cover partial delivery, fresh-instance
restart, snapshot-only configuration and incompatible partial-start rejection.
Run `tools/dcg-full-snapshot-probe.py NEW_DIRECTORY DCNCdddd eos` with the same
explicit fixture credentials and prerequisites as the public online probe.

New-table discovery passed in the packaged public connector with Kafka source
transactions enabled and, separately, ordinary source offsets. The ordinary-offset
run also discovered the omitted database name from `CURRENT_SERVER`. It validated the native creation lifecycle, published the
new table without restart or resnapshot, retained its schema before output, and
replayed creation from F during worker replacement without duplicate rows or
another history append. A subsequent transactional clear preceded reload while
another table continued publishing. The first attempt failed during ordinary
startup because the test named a table before it existed; its cleanup passed.
The passing test used ordinary capture-all selection. Run
`tools/dcg-file-creation-probe.py NEW_DIRECTORY DCNCdddd eos` or `source-offsets`.
The runner also exercises an omitted database name via `CURRENT_SERVER` discovery.
Every completed native fixture and local Kafka deployment above was removed and
verified absent. These runs do not imply deployment or downstream sink qualification.

Local Maven verification passed 4,467 tests (249 parser, 4,218 connector),
including cancellation during pointer-release setup and a structured release
error; neither permits journal progress. Python harness verification
passed 152 tests. Assembly, formatter/import validation and whitespace checks
passed. Native metadata fixtures do not authorize startup on another system.

### Final compatibility artifact performance

The ordinary plugin ZIP
`3df76e426810063b52d7f40a3b97daf9e207389d1a43fdd7f9c1f267c0f43640`
was compared with the preserved original plugin in four separate owned native
fixtures. Each used WARN row-conversion handling, inherited worker converters
and task/error defaults, two warm-up rounds, then 60 seconds of independently
paced inserts and updates. The writer did not wait for CDC delivery. All four
profiles delivered exactly 12,300 measured row changes, including mixed values
and complete update before-images, and passed source/session/Kafka cleanup.

| Profile | Offered operations/s | Median delay | p95 delay | p99 delay | CPU ms/operation | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Original | 201.27 | 5.215 s | 5.293 s | 5.320 s | 0.846 | 415.6 MiB |
| New, feature disabled | 201.20 | 5.214 s | 5.296 s | 5.348 s | 0.864 | 431.3 MiB |
| Enabled, memory buffer | 201.26 | 2.920 s | 4.003 s | 4.079 s | 0.884 | 442.8 MiB |
| Enabled, disk buffer | 201.23 | 3.055 s | 4.153 s | 5.325 s | 1.036 | 443.8 MiB |

Delay is host SQL submission to independent `read_committed` observation, not
source-commit latency. CPU and resident memory cover the whole Connect worker
over the measured interval, excluding warm-up and startup. Flight recordings
were retained. Disabled-path delay stayed close to the original; measured CPU
was about 2.1% higher. Enabled memory and disk used about 4.4% and 22.4% more CPU
than the original, respectively. Disk bounds large buffered payloads; this small
working set does not demonstrate a resident-memory saving. These are one run per
profile at a bounded offered load, not saturation capacity, statistical
equivalence, a large-catalog benchmark or a multi-day soak.

Reproduce with `tools/dcg-sustained-probe.py NEW_DIRECTORY DCNCdddd
--distribution PLUGIN.zip --label LABEL --mode legacy --rows 250 --seconds 60
--failure-handling warn --worker-defaults`; use `--mode transactional` and
`--buffer-storage memory` or `disk` for the enabled profiles. Use a different
fresh directory and owned library for every run. Preserve the original plugin
and its matching dependencies when comparing against the baseline.


## Member and mixed-journal compatibility — 2026-11-12

The public online path now preserves the original connector's multi-member row
behavior. Complete file/member bindings are acquired from native metadata, held
in immutable history and selected by the journal member identity. Output still
uses the original table topic and keys, and snapshots retain the original
first-member behavior. This does not claim member-aware output or snapshots.

Two owned mixed-resource fixtures passed with the ordinary packaged plugin:
ordinary source offsets with disk working storage (10 checks), and source
transactions with memory working storage (11 checks). Each started with two
members, activated from real legacy offsets, created a third member while
streaming, and recovered after worker replacement without another snapshot or
history append. An index existed before startup; another was created and dropped
while streaming. Journaled data areas, a data queue and access paths coexisted
with captured physical files. Access-path start/end did not become row changes
or relax checks on row journaling.

The final memory run also committed primary-key updates to three members in one
transaction. All three rows had RRN 1. Each produced its own correct before-image,
delete key and create key. An unrelated member continued while another table's
clear was open. The clear and reload appeared after commit. A subsequent
`CLRPFM` of only one captured member stopped with no table-wide `op=t` and no
completed-output advancement past the clear. Data topics in this probe have
three partitions; these assertions do not establish global consumer ordering.
The separate single-partition ordering tests remain the ordering evidence.

The disk run used plugin ZIP
`f8c800e37d8680bd334f4e8ce1c1caf6b5b80a186cda6c5c405dcd8068b3973b`.
The final memory run used
`e1bab9b952c1729b7e26e408b60f40f15032b8ba3e98af23b00218f88a250db1`.
All owned libraries, jobs and Kafka deployments were cleaned up and checked.
Run `tools/dcg-member-compatibility-probe.py NEW_DIRECTORY DCNCdddd eos` or
`source-offsets` with the contributor-test prerequisites.

Local captured-fixture tests cover complete and interrupted F/JM–D/MA–F/MC
member association, wrong file/member identities, missing history and replay.
A controlled interleaving places an existing member's row between F/JM and D/MA:
its completed W advances, while F pins the association and its history is already
durable. Fresh runtimes recover using that history in both memory and disk modes.
An interrupted output callback also reuses history without another native refresh.
This interleaving is deliberately controlled; it is not presented as a captured
native crash. Logical-file fixtures preserve IBM's file-type flag while replacing
private names/identities and removing opaque apply data. They are decoder evidence,
not startup proof. A contradictory physical clear cannot be ignored as logical
metadata, and pointer-backed logical creation releases its native handle.

Full Maven verification passed 4,515 Java tests (250 parser, 4,265 connector).
All 152 Python harness tests, plugin assembly and formatter/import validation
passed. F/CR PR #114's branch was not changed.

### Repeated performance comparison after member support

All four profiles again delivered exactly 12,300 independently paced row changes,
including mixed values and update before-images, after two warm-up rounds.
WARN handling, inherited worker defaults, a 60-second measurement interval and
approximately 201 offered operations/s were unchanged. Native and local-runtime
cleanup passed in every profile.

| Profile | Offered operations/s | Median delay | p95 delay | p99 delay | CPU ms/operation | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Original | 201.28 | 5.219 s | 5.295 s | 5.336 s | 0.848 | 455.6 MiB |
| New, feature disabled | 201.30 | 5.216 s | 5.284 s | 5.318 s | 0.858 | 465.4 MiB |
| Enabled, memory buffer | 201.25 | 2.992 s | 3.993 s | 4.113 s | 0.899 | 452.5 MiB |
| Enabled, disk buffer | 201.24 | 2.972 s | 4.016 s | 4.104 s | 1.059 | 445.4 MiB |

Whole-worker CPU was about 1.2%, 6.0% and 24.8% above the original for disabled,
memory and disk profiles. Delay stayed similar with the feature disabled and
was lower with either enabled profile at this offered load. These are bounded
comparisons, not saturation capacity, statistical equivalence, large-catalog
measurements or a long-duration soak. Disk storage's additional CPU cost is an
explicit tradeoff; this workload does not establish memory savings at scale.

The timed candidate ZIP is
`152f9a2099d470ddfddef0272a54d57af0978cf663cf457b7615dd359e0c2395`;
all three candidate runs used that same immutable distribution. The final member
restart fix changes only `As400SchemaChange.class` in production bytecode. That
fix is exercised by the final memory/native and local restart tests above, not
by this timed binary. The timed workload does not create members. The original
ZIP and reproduction commands are the same as in the preceding comparison.

### Excluded native LOB/source-file coexistence — 2026-11-12

The original `2e19ad27` plugin accepted selected ordinary-table CDC while an
excluded CLOB table and a journaled source physical file received insert/update/
delete operations. The successful control used 6 KB CLOBs so the independent SQL
journal audit remained within its existing 16 KB bound. A preceding 60 KB control
also produced the selected rows, but its audit rejected a truncated SQL payload;
that overall run is retained as failed, not relabeled successful.

The candidate public-plugin runs used 60 KB, NULL and empty CLOB values, acquired
native identity-only LOB bindings, real legacy offsets and automatic activation.
Ordinary source offsets with disk buffering and Kafka source transactions with
memory buffering each passed eight checks. They cover mixed-table commit and
rollback, unrelated output while a transaction/clear is open, worker replacement
with durable F/W and unchanged binding history, selected truncate/reload, no
resnapshot and no excluded data topic. The final source-transaction run also
includes insert/update/delete on a journaled source physical file. Both fixture
libraries, native sessions/inquiries and local Kafka resources were cleaned up.
The source-file control is coexistence evidence, not an exhaustive source-file
value-conversion qualification.

| Run | Plugin ZIP SHA-256 | Result |
| --- | --- | --- |
| Original excluded CLOB/source-file control | `da0cbb3ab5228fbe676520e905b88f36158f4adc0a73a856bd18b4583908339a` | Passed, including bounded independent raw audit and cleanup |
| Candidate ordinary source offsets, disk | `d253280dddc1df4b548267790490c720c6374c209871594e92d3ce4c05cbe1f6` | Eight checks passed; cleanup passed |
| Candidate source transactions, memory, source-file coexistence | `f04d01a414bfbb93c79d1755b73e31d4deb065dcd1fe16d395ccae13df14c724` | Eight checks passed; cleanup passed |

A first candidate run failed safely at its first excluded pointer-backed row:
continuous output selection had discarded the retained LOB capability. Completed
output did not pass that row; broker evidence and cleanup were retained. The fix
preserves both LOB capability and the complete member map when creating an
excluded binding. `As400ExcludedLobTest` now exercises the real continuous runtime
and fresh immutable history in addition to memory/disk coordinator replay and
interrupted delivery. It rejects captured/unqualified pointer rows, wrong native
member identity, minimized images, defaulted null indicators and incomplete or
changed physical formats before output/checkpoint advancement.

Two owned raw captures established native CLOB descriptor behavior with default
allocation and `ALLOCATE(1024)`, including NULL/empty values. The sanitized fixture
retains native thread IDs, flags, lengths, null maps, operation order and distinct
producer identities; process-local addresses, private names/JIDs and opaque clear
apply bytes were removed. It is replay evidence, **not startup proof**.
[IBM's documented LOB journal layout](https://www.ibm.com/docs/en/i/7.5.0?topic=objects-journal-entry-layout-lob-columns)
places external data behind pointers. The connector does not follow those values.
Recovery comparison pins operation/source/length/null metadata, explicitly not an
external-value checksum. Only exact verified excluded bindings may consume it.
The raw reader releases every returned handle in its retrieving job, including
budget-deferred entries; release failure cancels that job before progress.

Reproduce with an explicit test configuration and a fresh isolated library:

```sh
IBMI_TEST_CONFIG=/path/to/ibmi-test.json python tools/dcg-excluded-lob-probe.py NEW_DIRECTORY DCNCdddd source-offsets
# Use another fresh directory/library for the stricter Kafka contract:
IBMI_TEST_CONFIG=/path/to/ibmi-test.json python tools/dcg-excluded-lob-probe.py NEW_DIRECTORY DCNCdddd eos
```

Final local verification passed **4,536 Java tests** (256 parser, 4,280 connector),
152 Python harness tests, Maven verify/assembly and formatter/import validation.
The new native entry point also passed Python compilation. Captured LOB/XML value
support is not claimed: the original CLOB decoder was unimplemented, and its XML
row decoder returned empty text rather than the document. General historical DDL,
member-aware topics/keys/snapshots, universal deployment qualification and
maximum-capacity claims are outside this compatibility evidence.

### Final compatibility performance after member and LOB fixes

All four final runs used separate fresh native libraries, the same mixed-column
workload, WARN conversion handling, inherited worker defaults, two warm-up rounds
and 60 seconds of independent paced writes. Each processed 12,300 measured
changes at approximately 201 operations/second. All seven checks per profile
passed: exact keys/values/before-images, complete sustained output, no late
duplicates and resource samples. Native fixtures, sessions and local Kafka
resources were cleaned up. No other test workload from this thread ran during
these timed intervals.

| Profile | Median delay | p95 delay | p99 delay | Worker CPU ms/operation | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original | 5.214 s | 5.296 s | 5.332 s | 0.838 | 418.8 MiB |
| Candidate, feature disabled | 5.219 s | 5.323 s | 5.390 s | 0.916 | 440.5 MiB |
| Candidate, memory buffer | 2.975 s | 4.008 s | 4.133 s | 0.918 | 450.8 MiB |
| Candidate, disk buffer | 3.017 s | 4.045 s | 4.106 s | 1.029 | 443.9 MiB |

The original ZIP is `da0cbb3ab5228fbe676520e905b88f36158f4adc0a73a856bd18b4583908339a`; all three
candidate profiles use `f04d01a414bfbb93c79d1755b73e31d4deb065dcd1fe16d395ccae13df14c724`. The candidate
production sources match the final successful native source-transaction run.

Disabled median delay was within 0.1% of the original; whole-worker CPU cost was
9.3% higher. Memory and disk CPU costs were 9.6% and 22.8% higher,
respectively. Earlier candidate runs in this report remain separate evidence and
had smaller disabled-path CPU differences. This is one run per final profile,
not a statistical equivalence result. RSS includes the whole worker, and this
small workload is not evidence of disk-spill memory savings. Latency is host SQL
submission to independent `read_committed` observation, not database commit
latency. Maximum throughput, large-catalog capacity and multi-day behavior were
not established by these measurements.

Use `tools/dcg-sustained-probe.py NEW_DIRECTORY DCNCdddd --distribution PLUGIN.zip
--label LABEL --mode legacy|transactional --buffer-storage memory|disk --rows 250
--seconds 60 --failure-handling warn --worker-defaults` with a fresh directory and
library for each profile. Set `IBMI_TEST_CONFIG` and run with the project's build
JDK. The [comparison chart](dcg-compatibility-comparison.md) uses these final
measurements; the preceding results and failures are retained for traceability.

### Advisor correction: excluded LOB rows during schema observation

Independent Advisor review accessed the actual repository files and identified
one remaining interaction: schema refresh validated unrelated rows without the
retained binding resolver. A valid excluded pointer-backed row arriving during
the metadata-observation interval therefore rejected an otherwise supported
ALTER and repeated the failure on restart.

Both native schema-qualification passes now receive the verified bindings from
the active schema timeline. This allows only the already qualified excluded LOB
metadata; it does not promote current catalog guesses or permit captured pointer
values. New memory/disk tests run the continuous runtime through schema history
and a fresh restart. Missing bindings, captured pointers, changed member identity,
minimized/defaulted images and incomplete descriptors still reject before the
new schema can be accepted.

The corrected distribution is
`2165299640c663344283580c5cf9ed5c9e91d08fed45b0231c280d515f213744`.
Maven verification passed **4,544 Java tests** (256 parser, 4,288 connector),
followed by assembly and formatter/import checks. The existing 152 Python
harness checks remain passing; the extended native entry point also compiles.

The extended packaged-plugin run passed ten checks with ordinary source offsets
and disk buffering in a fresh isolated library. It retained the preceding mixed
commit/rollback, clear and replacement assertions, then stopped the reader while
fixture writers performed a captured-table ALTER, an excluded CLOB insert and a
new-schema insert. Independent journal evidence verified that order. On resume,
the real task qualified the entire interval, emitted the new row and durably
advanced W. A killed/replaced worker reused unchanged schema history and emitted
only the next new row. Native fixture, connection/inquiry and local Kafka cleanup
passed. All production sources matched the archived test input.

The preceding attempt passed its first eight checks but IBM i cancelled the
fixture's ALTER because it lacked the data-loss acknowledgement. That failed run
and its successful cleanup are retained. The harness now uses the same dedicated
job-scoped QAQQINI override as the existing schema probe, restores it before close,
and never changes a shared reply list or QAQQINI table.

The review was focused rather than exhaustive. The correction was verified
locally and natively after the finding; there was no second independent review.
The performance candidate above predates only this schema-qualification/binding
plumbing correction. Its workload does not perform ALTER; it must not be described
as a timed run of the corrected binary.

### Matched-dependency performance comparison

Final artifact inspection found that the original benchmark package and the
candidate contained different Debezium core snapshots. The earlier results above
remain valid package measurements, but their deltas cannot isolate connector
changes. The unchanged original source at `2e19ad27` was rebuilt using the same
resolved dependencies as the candidate, without installing or changing source.
SHA-256 comparison confirms all seven shared third-party/core jars are identical.
The candidate additionally includes the required history and H2 working-storage
libraries; those costs remain part of its measurement.

The matched original passed a new isolated native run with the same WARN setting,
worker defaults, two warm-up rounds and 12,300 changes at approximately 201
operations/second. Every key, mixed value and before-image matched, there were no
late duplicates, and fixture/session/Kafka cleanup passed. The chart now compares
that matched baseline with the three candidate runs above. No other test workload
from this thread ran during any measured interval.

| Profile | Median delay | p95 delay | p99 delay | Worker CPU ms/operation | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original, matching shared dependencies | 5.227 s | 5.316 s | 5.376 s | 0.849 | 459.5 MiB |
| Candidate, feature disabled | 5.219 s | 5.323 s | 5.390 s | 0.916 | 440.5 MiB |
| Candidate, memory buffer | 2.975 s | 4.008 s | 4.133 s | 0.918 | 450.8 MiB |
| Candidate, disk buffer | 3.017 s | 4.045 s | 4.106 s | 1.029 | 443.9 MiB |

Disabled median delay differed by about 0.1%; worker CPU was 7.8% higher. Enabled
memory and disk used 8.1% and 21.1% more CPU than the original, respectively, with
lower observed delay at this offered load. This does not establish equal CPU
cost, maximum throughput, large-catalog capacity or statistical equivalence.
RSS includes the whole worker; this workload does not isolate memory savings
from spilling large transactions.

The matched original ZIP is
`087582d28edf821060153f00b533deb916d78b3ec4681841b9c4480898e4f42b`;
the timed candidate remains
`f04d01a414bfbb93c79d1755b73e31d4deb065dcd1fe16d395ccae13df14c724`.
The later corrected ZIP `2165299640c663344283580c5cf9ed5c9e91d08fed45b0231c280d515f213744`
is verified by the schema/LOB/restart tests above, not by these timings. Only
schema qualification and its binding-resolver plumbing changed; the timed
workload contains no ALTER operation. Reproduction uses the same sustained
runner command above, with a fresh directory/library and an explicitly selected
immutable distribution for each run. Compare shared dependency hashes as well
as source revisions before interpreting future results.

## Subsequent review corrections — 2026-11-12

The review found ordinary journal activity that could stop the enabled path:
excluded-file DROP/REVOKE, another table's clear during ALTER observation,
AFTER-only excluded updates, large excluded transactions and obsolete unreadable
receivers. Corrections preserve exact object/member identity and required-history
checks. Excluded rows no longer retain individual output entries, disk working
storage is the default, and online activation rejects processing WARN/SKIP before
opening native handles. Conversion handling remains a separate setting.

An ordinary-plugin native run passed transactional DROP rollback/commit/recreate,
ALTER followed by another table's clear, excluded AFTER-only updates, a 10,000-row
excluded transaction and activation with an unused FREED receiver. Restart checks
retained progress without resnapshotting. Those results used connector JAR
`9c63a92b4364b7a3c20eef00c62953b6ab9e5fcec572efea20453a94533d0300`;
they preceded the deferred-clear work below.

The legacy prefix now holds a transactional clear and subsequent changes of its
table while admission is pending. Other tables continue, with every source
offset pinned until retained native history proves the held outcome. The public
`dcg-deferred-clear-probe.py` runner passed **26 assertions** on the ordinary
packaged task with Kafka source transactions and transaction metadata enabled:
commit, rollback, savepoints, both D/CG subtypes, 600 unrelated rows across
automatic flushes, receiver rotation, replacement, consecutive clears, a following F/CR across restart and eventual
handoff without a second snapshot. A preceding build passed the ordinary
source-offset variant. Missing history, changed bindings and an already-published
same-table transactional prefix remain explicit failures; this does not repair
arbitrary previously exposed legacy transactions.

The final distribution ZIP is
`c7c2c428a984e6097aa4ec2bd10ef55d467dc193972f67fe533c2472ecb91ed2`.
All 197 production source files match the archived native test input. Maven
verification passed **4,653 Java tests** (256 parser and 4,397 connector), assembly
and formatter/import validation; **152 Python checks** passed. Local regressions
also cover partial dispatch, fresh-instance replay, incremental-window cleanup,
idle rollback completion, forty empty receivers and byte-limited read progress.
The last two tests reproduced permanent reader-limit failures before correction.

An initial native attempt exposed a background metadata timeout while a clear was
open. Read-only metadata work outside allocation now defers admission; uncertain
lock release still fails. That failed attempt is retained separately. Native
fixture/library/inquiry, reader-job and local Kafka cleanup passed, followed by
independent read-only verification that all six deferred-clear fixtures were
absent. Raw captures remain private evidence until sanitized.

Advisor inspected the actual files after the first 23-check run and found a valid
F/CR following a held transactional clear would be rejected as a schema change.
Two local commit/restart cases reproduced the failure. The correction shares the
strict member-binding validator, preserves single-member and cycle-zero checks,
and delivers the F/CR after the transactional outcome. Negative identity/scope
cases and the full suite passed. The final 26-check native run above killed the
worker at the pinned offset, committed the clear, performed CLRPFM and a reload,
then verified `t, c, t, c` after replacement. There was no second Advisor review;
this is the correction and re-verification of its reported concern.

These are correctness results, not new performance measurements. The preceding
timed candidate, maximum native scope and deployment results retain their original
artifact qualifications. Long-lived journal-wide transaction retention,
LOB-heavy cleanup cost and unusual resource/snapshot settings still need workload
qualification; passing these bounded tests does not establish universal capacity
or production-deployment readiness.
