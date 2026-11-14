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


## Transactional DDL and catalog-churn corrections — 2026-11-13

Independent review exposed permanent stops during ordinary DDL, deferred clear
ordering and catalog churn. The corrections retain source identities and recovery
positions while handling the following observed sequences.

| Scenario | Verification |
| --- | --- |
| Two jobs clear one table in the opposite order to their transaction starts | Native SQL/SQL and CLRPFM/SQL sequences emit exactly `t,c,t,c`; other-table output continues |
| Commitment-control GRANT/REVOKE, CREATE and ALTER | Native commits and full rollbacks, nested statement savepoints, durable schema history and restart |
| Consecutive ALTERs before observation | Native final-schema output; local memory/disk restart; captured intermediate layouts still require actual history |
| Rolled-back CREATE/ALTER rows | Native discard; fresh-instance replay before physical restoration finishes; schema history retains lifecycle marks rather than a mark per discarded row |
| Excluded program-described file and member removal | Native file creation, member addition/removal and continued captured-table output; exact IFS member identities replace absent SQL member rows |
| Catalog SQL0462 warnings | Three separately owned library drops caused 13 retry log entries without manual task restart; local mixed/unknown-warning and cleanup-failure cases remain fatal |
| Empty and retained receiver chains | Real reader/restart across 130 empty receivers locally; 20 native receiver rotations with an open transaction and later schema/offset recovery |
| Captured `IMAGES(*AFTER)` | Enablement is deferred with zero MIGRATION markers; legacy output and offsets continue |
| Restart while a selected table's clear is still open | Real replacement uses retained history, then releases the committed clear/reload without a resnapshot |

The source-offset regression run passed 24 checks. The subsequent source-transaction
run, with transaction metadata, passed 27, including foreign-catalog churn. The
expanded public schema runner passed 15 checks with 20 retained receiver rotations;
the image-admission runner passed five. Every owned fixture and local Kafka runtime
was cleaned up and checked. These tests exercised the packaged connector, not a
custom publication owner or receipt provider.

The native runs also caught two mistakes in the first correction attempts: an
admission query lost the SQL warning classification, and a current-catalog image
check incorrectly rejected restart during a clear's temporary journaling stop.
Both failed runs are retained as failures. Recovery now relies on retained journal
history. Image validation applies before accepting a new boundary and before
HANDOFF is offered, while an unsafe candidate can still be replaced. An already
acknowledged handoff does not acquire a new idle-catalog prerequisite.

Full Maven verification passed 4,700 Java tests; the Python harness suite passed
152. Focused admission checks and assembly validation passed after moving the
image recheck before HANDOFF. The public `dcg-schema-refresh-probe.py` now includes
transactional authority changes, nested ALTER, consecutive ALTER and full rollback
cases, using the existing fresh-fixture and verified-cleanup runner.

Artifact provenance remains explicit: the 27-check native run and image check
used ZIP `b8ff2358d4abc30cadc942df18742bf134c4b53d2f04d875ebfe88fbf5f3a70a`.
The successful schema/restart run used
`7c2387d9b9df1d725cf3decf0f5e315a0c4c2b2e925923ef8e4aa9c54e683159`.
The final pre-HANDOFF placement correction is ZIP
`650cf6bf00fb06fcea31b5f4a109079b07046a4b46b4673bfc0400b5d062373b`;
the preceding native results are not relabeled as runs of that final artifact.

This does not establish complete DDL/member parity. Unfinished schema transactions
can pause scanning; removal of the final member and captured key-constraint changes
remain unqualified. Missing historical captured layouts and already-published
unsafe prefixes still reject. The configuration contract now explains that
skipping a rejected clear loses it, and describes evidence recovery or snapshot
reconciliation. The unrelated-table progress guarantee for open clears does not
promise progress past an unfinished schema transaction.

That correction ZIP (`650cf6bf`) also passed a serial 60-second performance
comparison against the original connector, with 12,300 independently verified
changes per profile. These measurements precede the third and fourth review
corrections. Feature-off latency was essentially unchanged;
CPU per change increased by 4.5% with the feature off and 22.8% with disk buffering.
Enabled latency was lower. These finite workloads do not establish saturation
throughput, large-catalog capacity, or long-term memory behavior.

Advisor inspected the actual repository files after these corrections and checks
and reported no new actionable issue in the reviewed paths. The remaining limits
above still apply; the next adversarial review is a separate release gate.

## Third independent-review corrections — 2026-11-13

Schema retention now settles the shared replay floor before pruning either the
lifecycle dependencies or schema timeline. A previously over-advanced schema
cursor can be repaired only from its unchanged, complete retained F/W interval
and immutable history; missing openings or changed identities still reject.
Later statements in the same transaction retain their own nested savepoint
scopes. A schema commit observed beyond the current reader window waits for a
fresh verified cut instead of failing against the older window.

Native validation used fresh owned fixtures, ordinary packaged tasks and local
Kafka. All fixture, inquiry and local-container cleanup checks passed:

| Regression | Native result | Tested ZIP SHA-256 prefix |
| --- | --- | --- |
| Clear lifecycle retaining F before an unrelated ALTER | Both control and ALTER cases restart; committed clears are not repeated | `904bea56` |
| LABEL/COMMENT on captured and excluded tables; excluded AFTER/BOTH image changes | Subsequent rows and restart pass; exact file/member JIDs and the native F/JC apply flag remain checked | `2fb2bda1` |
| One ALTER, repeated same-table ALTER, two-table ALTER in one transaction | All three commit patterns and subsequent task restart pass with transaction metadata enabled | `2fb2bda1` |
| Open ALTER across worker replacement | Open for 113.9 seconds; resumes 16.1 seconds after COMMIT without manual task restart; later full rollback discards both tables' rows | `5555cbd6` |
| Upgrade from the actual preceding faulty build | Saved F=68/W=94 with schema floor 1 resumes at unchanged F, repaired floor 0 and subsequent W=95; no resnapshot or repeated clear | old `650cf6bf` to `5555cbd6` |

The final candidate ZIP is
`5555cbd695a7094e6c1ebe59afb3d1fb27af9bb5c5caa491531e03d44dfcb77f`.
The earlier rows in the table are not relabeled as tests of that ZIP. Their
schema/admin implementations are unchanged; the final revision shares the bounded
SQL0462 recollection policy with handoff-prefix publication.

Final local verification passed 4,724 Java tests (4,468 connector and 256 parser),
152 Python tests, Maven verify/package, formatting/import validation and diff
whitespace checks. The public schema runner also exercises LABEL/COMMENT, excluded
image changes and a two-table transaction. Local fixture tests cover the separate
schema-acquisition/reader-window race, complete-history repair versus missing
history, and persistent catalog warnings without output/offset advancement.

A cold run reproduced the spill-file budget failure despite a 25 ms reclamation
attempt. The corrected implementation allows up to one second to reclaim obsolete
scratch chunks before enforcing the same physical budget. The full suite and
three additional cold 6,700-key-change runs passed. Pending values are never evicted.
For 1,000 empty detached receivers, the real cut-reader test reduces subsequent
idle RPC reads from at least 1,000 to at most five without changing R/F/W or hiding
a later row. Fresh catalog/identity validation still runs. These are focused
resource checks, not new native throughput or saturation measurements.

The unfinished-DDL pause remains open: unrelated output stayed unpublished during
the native 60-second observation. Wait/resume logging now identifies it, but does
not remove it. Savepoint rollback of DDL, unavailable intermediate captured
layouts, captured key-constraint changes and final-member removal remain separate
qualification gaps. These corrections do not establish replacement readiness.

## Fourth independent-review corrections — 2026-11-13

The next correction separates metadata-only LABEL/COMMENT pairs from physical
ALTER lifecycles. Each pair still requires matching producer, file/member journal
identities, flags and subtype; a transactional pair also requires an observed
transaction and matching nested savepoint. A complete later clear can establish
that the file layout is unchanged, but its records remain in the ordinary
transaction lifecycle so schema acquisition cannot consume or discard the clear.

An unfinished ALTER now stages its owned raw records in the same bounded memory
or disk budget as other transaction data. Other transactions can advance completed
output W while replay floor F retains the unfinished transaction. On commit or full
rollback, every changed table is qualified before the staged records are decoded.
Schema history records the transaction outcome as its visibility boundary; old
history encoding remains readable. Restart reconstructs this context from F.
An already delivered safe prefix can also be acknowledged when later DDL remains
incomplete. This does not acknowledge the unfinished DDL itself.

Native checks used the ordinary packaged connector with fresh isolated fixtures
and local Kafka. The following successful runs retained raw journal intervals,
consumer records, durable offsets and exact artifact identities:

| Scenario | Result | Tested ZIP SHA-256 prefix |
| --- | --- | --- |
| Open ALTERs, unrelated output before commit, replacement while both ALTERs remain open, full rollback and later capture-selection restart | Expanded public schema runner: 18 checks passed with source transactions and transaction metadata | `c198b774` |
| CREATE then LABEL; captured/excluded rename round trips; LABEL then ALTER/LABEL/TRUNCATE in one transaction; ALTER then optimized DELETE; remote-filter eligibility changes | Final correction build: 16 checks passed using ordinary source offsets, including task replacement and exact clear/reload order | `6a98bbc0` |

Both D/CG clear subtypes were observed in the latter workload: 8 for TRUNCATE and
3 for optimized DELETE. All owned libraries, schemas, inquiry messages and local
Kafka resources were confirmed absent after cleanup; connection cleanup reported
no errors. The preceding broad run differs from the final artifact only in output
schema-cache bookkeeping: the final build retains rename aliases only for actual
output schemas and removes the old alias during refresh. It is not relabeled as
a run of the final ZIP.

The final ZIP SHA-256 is
`6a98bbc07a058e5ead251ca5ecf1c94c395010c0a6c2d919e8be4d8065de10a0`.
Its retained manifest matches all 198 production sources and 15 packaged JARs.
Full verification passed 4,788 Java tests and 152 Python tests. After the final
cache adjustment, 175 affected Java tests and assembly/formatter/import validation
passed again. The local tests exercise memory and disk storage, fresh-instance
restart, interrupted delivery, resource limits and malformed identity/history
rejection. The task-level safe-prefix regression fails against the prior behavior.

The public `tools/dcg-schema-refresh-probe.py` now includes metadata neighbors,
transactional LABEL/COMMENT, open-ALTER unrelated progress and replacement. Rename
tests additionally cover a one-way rename locally, including the new output topic
schema and retirement of the old schema; native validation here covers round
trips. Same-library rename requires unchanged native file/member identities and
layout. Cross-library moves, independent member renames and unavailable
intermediate captured definitions are still rejected.

Earlier failed runs remain retained: one exposed lost capture-selection context
while validating an excluded file's image settings; another exposed a metadata
prefix waiting unnecessarily for a later ALTER. The corrected runs above preserve
the original output, checkpoint and cleanup assertions.

Unfinished CREATE or DROP can still pause unrelated publication. DDL savepoint
rollback, unavailable intermediate captured layouts, captured key-constraint
changes and final-member removal remain separate qualification gaps. The bounded
30-observation SQL0462 retry policy is unchanged; no duration guarantee or new
native catalog-churn timing result is claimed. These results do not by themselves
establish replacement readiness or performance parity.

Advisor inspected the actual changed repository files, including the new rename
handler, after implementation and local/native correctness checks. It reported
no actionable issue in those changes. That read-only review did not rerun tests
or evaluate the performance comparison, which finished afterwards. The next
independent adversarial review remains a separate release gate.

The final ZIP subsequently passed six serial performance runs against the original
connector: two per profile, in forward and reverse order, each with 12,300 exact
measured changes after warm-up. All six fixtures and local runtimes were cleaned
up and verified. Feature-off median delivery delay differed by 0.03%, with 5.2%
more worker CPU per change. Disk buffering lowered median delay by 41.6%, with
25.0% more CPU. The [current table](dcg-compatibility-comparison.md#performance)
and [reproduction method](dcg-performance-comparison.md#fourth-review-correction-comparison--2026-11-13)
retain the exact artifacts, per-run ranges and measurement limitations. These
short tests do not establish saturation throughput or long-term memory parity.

## Unfinished CREATE/DROP recovery — 2026-11-13

Transactional CREATE and DROP now retain owned source records while unrelated
committed changes advance W; F retains the original transaction start. Creation
validates the physical-file/member association without inventing a schema or
querying an uncommitted definition. Commit acquires and persists the definition
before publishing rows. Full rollback leaves no created binding. DROP keeps the
old decoder long enough to publish earlier rows on commit or undo them on rollback.

The packaged plugin (`a26e1964…`) passed 24 assertions in each of two fresh native
fixtures: ordinary source offsets, and Kafka source transactions with transaction
metadata. Each run exercised CREATE/commit, CREATE/rollback, DROP/rollback and
DROP/commit. Every transaction stayed open across unrelated output and worker
replacement. Final topic contents matched the committed writer ledger with no
duplicate or rolled-back rows. The public reproduction is
`tools/dcg-schema-refresh-probe.py --open-objects`; it uses the standard packaged
task and retains native journal, offset and broker evidence.

The first correction run exposed a previously untested INSERT→DROP→ROLLBACK
sequence. The native rollback includes row-compensation records after D/DT.
The deletion validator now requires their exact transaction, producer and member,
and only accepts a full rollback outcome. The ordinary transaction buffer still
checks each compensation during replay. Two sanitized native records were added
to the existing deletion fixture, including hash and privacy checks; schema
history does not retain a mark per undone row. A separate initial runner error
used an unsupported legacy table-filter expression; it was corrected before these
successful runs. Failed-run evidence was retained and those fixtures were cleaned.

Local verification passed 4,558 connector and 256 parser tests, 152 Python tests,
Maven verify, assembly packaging, formatter/import validation and diff checks.
Tests cover memory/disk budgets, fresh instances before and after outcomes,
malformed identities/savepoints, and rollback rows followed by an invalid commit.
No new throughput or long-duration memory result is claimed for this build.
The subsequent independent review's mixed-statement and row-savepoint findings
remain separate work; this result does not establish replacement readiness.

## Fifth independent-review corrections — 2026-11-13

Source checkpoint `deb8139` addresses all six reported stops. The correction keeps
schema qualification separate from row/clear delivery: a later clear or its undo
records cannot be consumed as schema metadata. Identity, producer, savepoint,
history and unavailable-intermediate-layout checks remain enforced.

| Reported case | Verified behavior |
| --- | --- |
| ALTER followed by TRUNCATE/ROLLBACK of the same file before observation | Qualifies the complete clear and physical rollback; emits no rolled-back clear; unrelated output continues. |
| CREATE followed by another transition before acquisition | Handles ALTER before its first row, an initial primary key before its first row, rows followed by a committed clear, and an empty CREATE/DROP without querying a vanished definition. |
| Row-only savepoint rollback after ALTER in one transaction | Keeps the committed ALTER, discards the undone row and publishes the surviving row. A savepoint that undoes DDL still needs its own qualification. |
| CHGPF SIZE on an excluded file | Handles the subtype-1 metadata pair with exact identity/layout checks. The native workload also covers a captured file. |
| CHECK or other constraint-only changes on captured files | Keeps the decoder and event key already chosen for capture, matching legacy streaming. CHECK add/drop and primary-key removal/re-addition do not reacquire a current key for older buffered rows. |
| Same-library rename with different SQL/system names | Accepts the validated old-name metadata prefix before the native rename pair; retains name-based capture selection and the new SQL topic name across restart. |

The ordinary packaged connector passed 17 checks in each of two fresh isolated
native fixtures: ordinary source offsets, and Kafka source transactions with
transaction metadata enabled. Both runs used the same final ZIP:
`7f00362ce398f6923d1f7b211f1ff97deb0797413e65d5d973310d913aadc46d`.
The manifest matches all 198 production source files and all 15 JARs. The embedded
connector revision also matches `deb8139`.

The public `tools/dcg-schema-refresh-probe.py --mixed-outcomes` workload creates
observation windows with an ordinary stopped/restarted reader, then checks the
actual task, dispatcher, durable offsets and broker output. It exercises captured
and excluded long-name renames, new-table clear/reload order, CHECK add/drop,
primary-key removal followed by LABEL, worker replacement and key re-addition.
Final keys, values and record order match the committed writer ledger with no
duplicates or rolled-back rows in these runs. All owned schemas, libraries,
inquiry messages and local Kafka resources were confirmed absent after cleanup;
worker/connection cleanup reported no errors. Ordinary source-offset delivery
remains at least once; this successful crash schedule is not a general exactly-once
claim for that mode.

Full local verification passed 4,867 Java tests (4,611 connector and 256 parser)
and 152 Python tests. After adding the final rename-key history regression, all
365 affected tests passed again. Maven verify/package, formatter/import validation
and diff checks passed. Local cases additionally cover memory/disk recovery,
malformed rollback identities/producers, missing undo records, ambiguous savepoint
targets and unavailable historical key/layout boundaries. Earlier failed runs
and negative-control evidence remain retained.

The separate [schema-latency comparison](dcg-performance-comparison.md#fifth-review-schema-change-latency--2026-11-13)
shows a modest improvement from reusing the observation connection, with material
extra latency still present. It measures end-to-end probe delivery, not a fixed
connector pause, and does not replace the earlier row-throughput/CPU measurements.
Forward recovery reads older schema history; older candidates cannot necessarily
read new visibility fields, so downgrade after schema-history advancement is not
assumed safe. Unavailable intermediate captured definitions, rollback of DDL to
a savepoint, final-member removal and cross-library/member renames remain separate
qualification gaps. These bounded corrections do not establish replacement readiness.

### Advisor follow-up: clear during transactional creation

Advisor inspected the actual files after the preceding checks and found that a
clear of an uncommitted creation could stage cycle-zero physical companion records
for transactional replay, which correctly rejected them. The correction separates
physical file/member identity from the row schema: the existing lifecycle validates
the companions, while D/CG remains deferred until commit or is discarded on full
rollback. The transaction buffer's positive-cycle requirement is unchanged.

Native qualification refined the test sequence. CREATE followed by an uncommitted
INSERT then TRUNCATE returned SQL0910. CREATE followed by TRUNCATE on the empty
table succeeds with SQL0100 and produces subtype-8 D/CG in the creation transaction.
An INSERT can follow that clear. A separate optimized DELETE after an uncommitted
INSERT abandoned its clear attempt and used row deletes instead. These are bounded
observations, consistent with [IBM's TRUNCATE example](https://www.ibm.com/support/pages/ibm-i%E4%B8%8Atruncate-table%E7%9A%84%E5%BA%94%E7%94%A8),
not a claim that every DELETE uses the same strategy. Failed setup runs and raw
journal evidence were retained, and all their fixtures were cleaned.

The abandoned attempt writes F/CH then C/SU without beginning journaling teardown.
Schema observation now recognizes that cancellation; the ordinary lifecycle still
requires its exact observed transaction/savepoint. Cancellation after F/EJ has
begun, wrong savepoint targets and mismatched identities remain rejected. Local
tests model the native empty-table clear and separately cover the failed attempt,
memory/disk recovery, full rollback and interrupted delivery. Both defects have
negative controls that fail against the preceding implementation.

Checkpoint `48978c6` passed full Maven verification: 4,634 connector and 256 parser
tests, plus 152 Python tests. Assembly, formatter/import validation and diff checks
passed. Its frozen plugin SHA-256 is
`6e317878fe16b2d7fc0655e399ffec18c2a6996a5a3570f96c0dad173c0c6bdb`;
the embedded revision and all 198 production sources match that checkpoint, with
all 15 packaged JAR hashes retained. Advisor's finding was corrected and reverified;
the tool was not called again to claim an independent pass on the correction.

That exact plugin then passed 47 checks in each of two fresh native fixtures:
ordinary source offsets, and source transactions with transaction metadata. The
public `--mixed-outcomes` runner repeats the six review corrections and adds
commit/full rollback for both an empty-table clear and a rejected clear inside
CREATE. Each of those four transactions remains open across unrelated output and
worker replacement. Raw journal records distinguish real D/CG from an abandoned
attempt; broker output and final source contents match the committed ledger.
The committed empty-table clear emits `t` before its inserted row. Neither the
failed clear nor a rolled-back creation emits a truncate. Every check passed,
including final task drain, native connection cleanup and fixture/local-runtime
removal. These runs validate the tested crash schedules; ordinary source offsets
still provide at-least-once delivery. Performance measurements above retain their
earlier artifact identities.

### Final-candidate sustained performance

The exact `48978c6` plugin was subsequently compared with the original connector
in six serial fresh-fixture runs, two per profile in forward and reverse order.
All 73,800 measured changes passed exact key/value/before-image and output-count
checks. Every native session, fixture and owned local Kafka resource was cleaned;
an independent read-only check confirmed zero remaining schemas, libraries,
journals and inquiry messages for the six fixtures. Resource sampling and artifact
identity checks passed. A scan of uncompressed evidence, including exported
recordings, found no copy of the test password; the owned credential file was
removed.

Feature-off median delivery delay and CPU/change differed by −0.10% and −0.6%
respectively. The disk-buffer profile delivered changes with 42.5% lower median
delay and 19.7% more CPU/change. The
[performance table and method](dcg-performance-comparison.md#fifth-review-final-candidate-comparison--2026-11-13)
retain exact artifacts and per-run ranges. These are two 60-second runs per profile
at approximately 201 offered changes/second, not a saturation, statistical
equivalence or long-duration memory result. Schema-change latency and the next
independent DDL review remain separate release gates. No PR was updated from these
measurements.

## Sixth-review follow-up qualification — 2026-11-13

The independent review confirmed all six fifth-review corrections on the frozen
`48978c6` plugin, then reproduced SQL-only renames failing for both captured and
excluded long-name tables. Their native system names remained unchanged and the
journal contained a metadata pair without D/FN. Local follow-up now preserves the
physical identity and event key while applying the new SQL name to capture
selection, topics and immutable restart history. Tests cover both names selected,
only either name selected and neither selected, plus rejection of changed layouts,
changed physical identity, ambiguous intermediate names and pending old-name rows.

Local recovery tests also cover excluded newly created tables loaded before an
ALTER, initial key addition or qualified CL-command change. The acquired SQL name
must establish exclusion. Complete lifecycle history bounds the older row layout;
no excluded values are decoded, and the physical envelope, image and null-map
checks remain enforced. Selected rows that need an unavailable historical layout
still reject. Memory and disk cases include a fresh restart before the later
transition and after completed output.

Checkpoint `c882e84` passed full Maven verification with 4,663 connector and 256
parser tests, plus 152 Python tests. Its frozen plugin SHA-256 is
`8aec6640d0142ce6c98663a65b3ce488e4193ebe19a9982717dbd2da1c01cc40`.
That plugin passed all 19 checks in an ordinary-source-offset native run: loaded
excluded creations, all four SQL-name selection combinations, exact topic/key/value
output, unrelated progress and worker replacement. Fixture and local-runtime
cleanup passed. An earlier attempt stopped on a harness comparison against an
untrimmed fixed-width catalog name; its failure evidence and successful cleanup
remain retained. These are bounded native results, not general exactly-once
delivery or performance claims. The earlier performance measurements and independent
review apply to their recorded immutable artifacts. The public
`dcg-schema-refresh-probe.py --metadata-followups` mode exercises these cases,
exact broker output, unrelated progress and worker replacement in a fresh owned
fixture. Ordinary source offsets and source transactions require separate runs.

Further local cases cover a committed ALTER followed by a physical rename before
observation, with rows on both sides, and an empty CREATE followed by a physical
rename. The ALTER keeps its original SQL name until the separate rename boundary.
The creation may use the final name only when no earlier row or clear needs the
unknown original SQL name. Native file/member identity and binding-collision
checks remain mandatory. Memory/disk tests restart between the two boundaries and
after completion, and reject mismatched catalog/native identities or ambiguous
earlier output. Subsequent native testing exposed two additional forms: internal
savepoints between an ALTER's physical companions, and a metadata pair before
renaming and dropping the old object in a table swap. Both now retain their exact
transaction, file and member checks.

The frozen `80d192b` plugin passed all 23 checks in an EOS run with transaction
metadata. It covers the four SQL-name selection combinations, loaded excluded
creations, CREATE/ALTER followed by physical rename, and a captured table swap
ending in DROP. Exact keys, values and topics survived worker replacement; dropping
the old object did not remove the replacement binding or emit an invented truncate.
Its ZIP SHA-256 is
`92b1d4419eb9f095d50247c180f98da1496ce85b9b8f19e7116bf83720b10ca8`.
Full verification passed 4,698 connector and 256 parser tests; assembly and
formatter/import checks passed. Independent checks confirmed fixture and owned
Docker resources absent after cleanup.

### Known ALTER/DROP and disappearing catalogs

A completed ALTER followed by DROP can retire a retained identity without fetching
a vanished definition when no intervening row needs it. The tests preserve
rejection of rows requiring that layout, foreign identities, incomplete physical
companions and rollback. If a catalog disappears between the initial journal read
and acquisition, one retry is allowed only when a new retained DROP or physical
rename explains the disappearance of that exact file. Other missing evidence,
authority errors and unsupported layouts retain their failure behavior.

The `e27565c` plugin (production changes at `e5ad14d`, then a harness correction)
passed the public `--retired-definitions` EOS experiment with transaction metadata.
Both a captured and an excluded file were altered then dropped; unrelated output
and the already published captured row remained exact through worker replacement.
Six empty CREATE/DROP gaps were exercised: 0.5, 1.5, 3, 4, 6 and 8 seconds. Two
observations actually entered the new retry path and continued without a task
failure. These gaps describe test scheduling, not a guarantee that every host hits
the race at the same delay. Task, fixture and local-runtime cleanup passed.
The frozen ZIP SHA-256 is
`0d3a2aa3bd6e041bec78ec689875d7f60dfc84aa03ccddf75af7c5f3883e4296`.
Full verification passed 4,715 connector and 256 parser tests, and 152 Python
harness tests; assembly and formatting checks passed. The first native attempt
used an invalid insert for the harness's mixed-column table; its failure and
cleanup evidence were retained before rerunning with the single-column fixture.

CREATE/load/DROP now has a packaged historical-name path. A separate
names-only prototype first recovered
exact names after the objects and their library were removed. A CL-only version
then read detached receivers after table deletion, including a 46,272-byte
pointer-backed creation, quoted names, LABEL/COMMENT and an SQL-only rename.
It changed neither fixture objects nor the source journal; owned-job cleanup and
independent fixture absence checks passed. These API observations are separate from the packaged run below.

The adapter binds each callback to the retained journal entry, preserves quoted
SQL names, uses ordinary case-insensitive filtering and signal-table selection,
and rejects missing/ambiguous/selected historical names. It does not infer a row
layout. Memory and disk tests retain F across unrelated output and reconstruct
from a fresh instance before the transient table's deletion. The helper fixes
QDBRPLAY to names-only mode, bounds native spaces, releases pointer handles in
the retrieving job and ends only its private API job during stop.

Local verification passed 4,739 connector and 256 parser tests and 152 Python
tests. The first full run exposed a poll/close race: a closed session could
replace a previously queued retriable transport error with a fatal admission
error. The corrected poll preserves that queued failure; both acknowledgement
modes now force this race deterministically. The failure log is retained, and the
full suite passed after the correction.

The frozen `73be095` packaged plugin passed all nine checks in the public
`--transient-tables` EOS experiment with transaction metadata. It covers ordinary
CREATE/load/DROP, CTAS, a single-transaction lifetime, SQL-only rename and LABEL
during the transient lifetime, and a quoted mixed-case name. Each was read from a
detached receiver after deletion. Exact unrelated output survived worker
replacement. Fixture, journal, inquiry and owned Docker absence were independently
verified after cleanup. Its ZIP SHA-256 is
`80a4d2efc3f1335c9c96038136a8c7b06bdbabb1a0566397e2fce47992e039f0`.
This does not establish selected transient-row decoding or the helper's
least-privilege authority profile.

Kiro's frozen-checkpoint addendum also identified committed ALTER followed by
rolled-back or still-open ALTER, plus metadata acquisition races. The complete
later rollback now has local recovery coverage: its exact physical restoration
must precede metadata acquisition, and its transaction retains its own history
and row-discard boundary. A qualified transition overlapping catalog acquisition
gets one fresh observation per poll; continued movement defers without output or
checkpoint advancement. Identity/history failures do not enter that retry path.
The frozen `93b386f` plugin passed the public `--schema-observation` EOS run
with transaction metadata. It checked committed ALTER followed by rolled-back
ALTER on both captured and excluded files; the captured layout and exact row
ledger survived worker replacement. Five scheduled CREATE follow-ups covered
LABEL, CHECK, ALTER and physical RENAME at 3–5 second gaps. Three metadata-race
retries and one missing-catalog retry were actually observed. Task and fixture
cleanup passed. Its ZIP SHA-256 is
`ce3efbcd2ecc85abe1a5deec3ef52180a67a75598b71aafc3d18870c1fc471ad`.
Its full verification passed 4,745 connector and 256 parser tests plus 152 Python
tests. A still-open second ALTER inside the first ALTER's observation window
remains a separate check. Creation rows before SQL-name changes now require native
name evidence and cannot silently move to a later selected topic.
These commits have not had another independent adversarial review. Earlier
performance results retain their own artifact identities.

The excluded-table variant now has local memory/disk recovery coverage. If a
later open ALTER prevents catalog acquisition, the already committed prefix can
retain an identity-only definition only after native SQL-name evidence confirms
that its name remains excluded. Its row layout is explicitly unobserved, never
replaced with the earlier layout. Unrelated output advances with F retained, and
a fresh instance reconstructs the open cycle. Complete-image, member, null-map,
transaction and selection guards remain tested. An identity-only definition
cannot build a captured output schema.

The frozen `c5c4718` plugin passed all 13 checks in the public
`--open-later-alter` EOS run with transaction metadata. In both the rollback and
commit cases, unrelated rows and W advanced while the second excluded-table
ALTER stayed open, and F retained its transaction start. A killed worker was
replaced before each outcome; uncommitted captured rows stayed invisible,
rollback discarded its row, and commit published its row exactly once. A later
LABEL reacquired the complete definition. This does not establish automatic
layout acquisition when capture selection is expanded from identity-only
history. Fixture, journal, inquiry and owned Docker absence were independently
verified after cleanup. Its ZIP SHA-256 is
`66b8698c13f5e2e057be9d285f46323b2ebc5e04a37bbdb316bb57f8d9e3a415`.
The production changes passed 4,755 connector and 256 parser tests; the final
harness correction passed assembly, formatter/import checks and all 152 Python
tests. The preceding native attempt passed its first open-transaction progress
check, then failed because the harness lacked an assertion helper. Its failure
and independently verified cleanup evidence were retained.

A separate baseline run used the original `2e19ad27` connector and parser JARs,
with the same dependency set used in the earlier comparison. With two ALTERs of
an excluded table, an unrelated row appeared while the second ALTER remained
open (7.5 seconds). For a captured table, the unrelated row was absent throughout
the 32.1-second observation, then appeared after rollback. Both probes and their
follow-ups were delivered once; cleanup was independently verified. This is a
bounded observation of the original connector's captured-table pause, not a
throughput measurement or proof of all open-DDL behavior. The baseline ZIP hash is
`d30e4a8e5a6646a5df009ad4e53b7444dee02dc9486430a023591219fe62e2a8`.

The follow-up removes the LABEL dependency when full rollback leaves a stable
restored layout. Memory/disk tests restart from the identity-only prefix, observe
the full restoration, expand capture selection, and publish the next row with
the acquired columns without replaying earlier excluded rows. Later metadata or
key changes prevent the rollback observation from adopting their layout;
different native identities still fail, and already known rollback definitions
remain unchanged.

The frozen `012b022` plugin passed all 16 checks of the updated
`--open-later-alter` EOS experiment with transaction metadata. Both outcomes
preserved unrelated progress and invisible uncommitted rows across worker
replacement. Capture selection was expanded after each outcome without a LABEL
or resnapshot; only the two newly inserted rows appeared, with the correct
restored/committed columns. The exact captured-row ledger passed. Full local
verification passed 4,760 connector and 256 parser tests plus 152 Python tests;
assembly and formatter/import checks passed. Its ZIP SHA-256 is
`3ec09b25e7bfd72d562346a8cae8731ab0ff5e45afa80fe9b1a7dcfcbd039b13`.

A separate negative test on that same artifact passed eight checks. An uppercase
SQL table and a delimited lowercase name coexisted on different native files.
The ordinary include predicate selected both. After the lowercase table was
loaded, deleted and read from a detached receiver, its historical name caused
rejection at creation; monitored output and the durable checkpoint stayed
unchanged, including after worker replacement. This proves the exclusion shortcut
does not accept that selected transient lifetime; it does not add historical
decoding for selected vanished tables. Two earlier fixture failures were retained:
an invalid explicit system name for a short SQL name, and an assertion that
missed padding in the fixed-width catalog name. Both completed cleanup before
the corrected run used a fresh library.
Both final runs passed task/fixture cleanup, followed by independent checks for
remaining schemas, libraries, journals, inquiry messages and owned Docker
resources. No such resources remained.

### Historical names and excluded lifetimes — 2026-11-13

The `9b30614` plugin passed bounded native EOS runs with transaction metadata for
four newly created excluded CLOB lifetimes (retained, dropped, committed and fully
rolled back) and seven short/long physical-rename lifetimes. Unrelated output and
durable offsets progressed; worker replacement preserved the exact row ledger.
Its ZIP SHA-256 is
`b0b53ea58d35d93969a41ca8f5b0933e41362aa308b92d304ce1f98013159902`.

All four historical-name rejection cases passed on that artifact: rows and
empty-table clears followed by a SQL rename, with either the old or new name
selected. Monitored output and durable offsets remained unchanged, including
after worker replacement. The empty clears produced D/CG subtype 8 with no R
records. The harness retains and accepts only the documented SQL0100 empty-table
condition; other diagnostics still reject. See IBM's
[TRUNCATE reference](https://www.ibm.com/docs/en/ssw_ibm_i_74/db2/rbafztruncate.htm).

Native names-only controls distinguish full ALTER (subtype 0) retaining its SQL
name, LABEL/COMMENT carrying neither name, and subtype-2 removal of an alternative
SQL name. These observations came from retained receivers after object deletion;
the name inspection changed neither source objects nor the journal. Missing name
evidence never becomes permission to discard rows or clear events.

The follow-up `2c049ca` plugin passed the known excluded ALTER/INSERT/DROP sequence
in one transaction. Unrelated rows and durable W progressed while it remained
open, with F pinned to the exact C/SC. A worker was killed and replaced before
commit, and again after physical deletion. The unrelated-row ledger remained
exactly once in this EOS run. Its ZIP SHA-256 is
`2c6aac350dfc5fc14ff64f0d5369b72bcbd95f0b3a55dde9e5d2d899bf572b41`.

Full verification at `2c049ca` passed 4,865 connector and 256 parser tests;
assembly, formatter and import validation passed. Independent offline review
verified the exact artifacts and passed 70 focused cases at `9b30614`, then 21 at
`2c049ca`, including independent admission and saved-history challenges. Those
reviews found no new concrete blocker within their bounded scopes. Earlier
missing-proof fixture failures remain preserved separately, not recounted as
passes. The final-window qualification seam was tested locally; a timed native
catalog race was not simulated in these runs.

Every completed native run above passed task/fixture cleanup and independent
checks for remaining schemas, libraries, journals, inquiry messages and owned
Docker resources. Earlier harness errors were retained and corrected on fresh
fixtures. The runs above do not establish shared ALTER/DROP rollback, selected
unavailable historical layouts, least-privilege authority for names-only
observation or whole-feature readiness by themselves. Earlier performance
measurements retain their original artifact identities.

### Shared ALTER/DROP full rollback — 2026-11-13

The `89ff007` plugin passed a bounded native EOS run with transaction metadata
for both a captured mixed-column table and an excluded table. Each transaction
altered its table, inserted a row into it and another captured table, dropped the
altered table, and fully rolled back. Unrelated autocommit rows and durable W
continued while the transaction was open; F retained its exact C/SC. The worker
was killed and replaced before each rollback and again after each restoration.

The actual journal contained D/DT, row compensation, the shared C/RB and the
four F/EJ, D/EF, D/JF, F/JM restoration records. Neither transaction published
its rolled-back rows. The captured table then published a new row using all its
original columns, including their NULL values, without the rolled-back added
column. The unrelated-row ledger was exact across all four replacements.
All nine run checks and independent fixture/worker cleanup checks passed.

Tested ZIP SHA-256:
`9fe97cfbf89f4ebd72e5aff801e323693323efd279a76296cc50597d2c6f03dc`.
Full verification passed 4,880 connector and 256 parser tests, plus assembly,
formatter and import checks. Independent offline review verified the artifact
and passed 23 distinct cases: 15 rollback regressions, two committed controls
and six independent safety checks. Wrong transaction, producer, member,
incomplete restoration and savepoint outcomes do not authorize rollback history.
Fresh replay revalidates row identities even when that history is already saved.

The first native attempt stopped after the excluded case because its private
runner tried to serialize a decimal transaction identifier directly as JSON.
Its evidence remains a failed harness run. A separate cleanup completed and
independently verified removal; the corrected run used a new library.

To reproduce the source sequence, use a fresh isolated journaled library and
the ordinary packaged plugin with `truncate.transactional.enabled=true`. In one
commitment-controlled session, run `ALTER TABLE U ADD COLUMN X1 INTEGER DEFAULT 7`,
`INSERT INTO U (ID) VALUES (703)`, an insert into another captured table, and
`DROP TABLE U RESTRICT`, then `ROLLBACK`. Insert unrelated autocommit probe rows
before and after DROP. Kill and replace the Connect worker while that transaction
is open and after rollback; retain the receivers and binding history. Repeat
with a captured table, insert into its original columns after restoration, and
check the source rows, emitted schemas, exact output ledger and durable F/W.
The ordinary Java fixture regression covers interruption before the final
restoration record, which this native run did not time deliberately.

This result qualifies the tested full rollback. It does not supply missing
historical layouts for a committed captured transaction or qualify DDL savepoint
rollback, unrestricted DDL compositions, downgrade or whole-feature readiness.


## Known-table name boundaries — 2026-11-13

The independent review demonstrated two incorrect clear routes and an adjacent
row route in controlled captured-journal fixtures. A later SQL rename could
change the topic or capture selection of an earlier clear. A rename followed by
a clear and DROP could instead retain the old name for that clear. The physical
rename/row/SQL-rename case could move an earlier row to the later SQL name.
The clear reproductions used subtype 3 optimized DELETE producing `op=t`;
subtype 8 was added to the correction tests explicitly.

The correction separates proven name boundaries. Metadata before a clear is
qualified with historical SQL names, complete object identities and transaction
outcomes, without inventing an intermediate row layout. A physical rename
followed by rows retains its own identity until the later SQL metadata boundary.
Both pre-catalog collection and final-window qualification use these checks.
The local tests exercise old-only, new-only and combined capture selection,
LABEL controls, full clear rollback, memory/disk buffers and fresh history
recovery. Missing or conflicting name proof rejects. An independent follow-up
found a savepoint rollback that could undo the name change before a later commit;
the name-only proof now rejects that unproven outcome. Native rollback semantics
for that SQL-name change are not claimed.

The first packaged native attempt exposed another journal variant: subtype 8
contained a deeper C/SB/C/SQ pair between D/EF and D/JF. The lifecycle now applies
the same exact transaction, producer, nesting and release checks already used
for ALTER at that position. Restart, full rollback and malformed-scope tests
cover the correction. The failed native attempt is retained separately, with
fixture cleanup verified; it is not counted as a passing routing test.

Three corrected native runs used fresh isolated libraries and the ordinary
packaged task. All captured SQL TRUNCATE as subtype 8, with raw journal evidence:

| Capture selection | Build | Delivery | Result |
| --- | --- | --- | --- |
| Old SQL names only | `cde0281` | Ordinary source offsets | 16 checks passed, including worker replacement and exact topic contents |
| New SQL names only | `efa9bb0` | Connect source transactions with transaction metadata | 16 checks passed, including worker replacement and exact topic contents |
| Both SQL names | `1a5f489` | Connect source transactions with transaction metadata | 16 checks passed, including worker replacement and exact topic contents |

The first build's ZIP SHA-256 is
`68930e1cca1e9b590e6f83a20182720bc5eb2f6095bc675d1c0a0fb1e6c83846`;
the second is
`2cda04e5645d8b7aa53ec74312c9c36a58fe43f8a316391ab4ddd4c84aea6843`;
the third is
`ad4ae9836996c784e087c2e63f04b7d2bfc01c001fe145f42d6a819b9d3f59ce`.
Each artifact retains its source/JAR manifest and embedded commit. These runs
exercise committed native name changes and clears, not SQL-name savepoint
rollback. The ordinary fixture tests remain the subtype 3 evidence for these
particular compositions.

To reproduce, first let the connector acquire a long SQL-name table with a
separate system name. Stop only the connector while application writes continue.
Run LABEL, an empty transactional TRUNCATE with COMMIT, then an SQL-only rename;
resume and verify the truncate belongs to the original name. In another table,
run SQL-only rename, committed empty TRUNCATE, then DROP; verify the truncate
belongs to the renamed table. For rows, change only the system name, insert a
row, change the SQL name, and insert another row. Test selection of either name,
replace the worker, and check the exact records and durable offsets. Do not
substitute a row DELETE for independently observed D/CG clear evidence.

The independent follow-up at `efa9bb0` passed 28 targeted parameter cases.
Separate rejection assertions verified that a savepoint rollback covering the
name change produces no output, returned checkpoint or installed history.
Savepoint rollback targets after the name change remain accepted controls. The
review also checked the internal-clear nesting correction. This is local
fixture evidence; it does not establish native SQL-rename savepoint rollback.

## Empty final-member recovery — 2026-11-13

A direct comparison established this as an availability regression. The original
`2e19ad27` plugin continued capture after the last empty member was removed,
restarted with the file still memberless, accepted a new member and restarted
again. The earlier transactional build `89ff007` instead stopped at the removal;
its four later progress checks failed. Both used the same isolated DDS-keyed
fixture shape and local Kafka, with exact observed output and durable offsets.
The initial baseline attempt was a harness failure caused by waiting for an
idle legacy offset with heartbeats disabled. That attempt is retained separately;
the corrected comparison used the ordinary heartbeat setting on both builds.

Build `1a5f489` retains the physical file with zero active members in durable
history. The removed member remains only a historical anchor and cannot authorize
rows or clears. A later exact native member association establishes the new
active identity; it does not silently reuse a stale member binding.

The frozen plugin with SHA-256
`ad4ae9836996c784e087c2e63f04b7d2bfc01c001fe145f42d6a819b9d3f59ce`
passed all six observed native steps in a fresh isolated library: initial insert,
individual delete to empty the member, unrelated publication after removal,
publication after worker replacement while no member existed, a row in the newly
added member, and another row after a second replacement. Each observed row was
unique and reached the durable checkpoint; source contents were checked too.
This run used ordinary source offsets. It does not claim that removing a populated
member emits `op=t`, or add member-aware topics, keys or snapshots.

Eleven new local parameter cases cover memory/disk storage, captured/excluded
bindings, re-adding the same or another member name, repeated fresh history
recovery, and stale-member rows rejected before output or checkpoint advancement.
The explicit memberless history format rejects malformed member attributes and
cannot be treated as an older single-member format. Existing multi-member removal
controls still pass. Full Maven verification at this source checkpoint passed
4,965 connector and 256 parser executions, with assembly, formatter and import
validation also passing.

To reproduce, start with one empty member, publish and individually delete a row,
then run `RMVM FILE(library/file) MBR(member)`. Publish to another captured table,
replace the worker, and publish another unrelated row. Run `ADDPFM` for the file,
insert into its new member, replace the worker again, and verify the exact topic
contents and durable offsets. Preserve the raw member-association records and
binding history. The new memberless table-binding format is not readable by
earlier experimental builds; retain a compatible plugin for restart.

Runner cleanup and a separate read-only check verified removal of all owned
libraries, journals, inquiry messages and local Docker objects from these name
and member runs, including the failed internal-savepoint attempt. A scan of
1,887 repository/evidence files found no configured password bytes in UTF-8,
UTF-16 or CCSID 37. That byte scan did not decompress archives.

## Same-file member rename — 2026-11-13

A second native comparison used `RNMM` to rename the only member of an existing
DDS-keyed file, publish an unrelated row and a row in the renamed member,
replace the worker, then rename the member back. The original `2e19ad27` plugin
passed all five observed steps. Build `1a5f489` stopped at F/MN; all four later
progress observations failed, including after replacement. The first baseline
attempt stopped in the harness because its member-name assertion omitted the
padding of the catalog's fixed-width field. That attempt is retained separately;
the corrected query uses TRIM without changing the expected name or row checks.

Build `265a310` qualifies the standalone F/MN using its old header name, both
payload names, retained member JID and unchanged physical file/library. It
preserves the schema, key, SQL table and topic. It does not turn a member rename
into a file rename or claim a cross-library move. The qualified transition is
stored before subsequent rows, and an F/MN already owned by a physical-file
rename stays part of that existing lifecycle.

The frozen ZIP SHA-256 is
`271b89d3dcad7ac9ce9dc725880eaaa2d64d1737407d06c40cc9f04282bea487`.
Its source/JAR manifest and embedded revision match the committed build. It
passed the same five native observations on a fresh isolated fixture using
ordinary source offsets, with unique records, durable progress and final source
contents checked. The raw interval retains both F/MN records and subsequent
rows under their new member names.

Fourteen new local cases cover memory/disk recovery, captured/excluded output,
exact keys and topics, secondary-member preservation, malformed identities and
names, collisions, unsupported transactional context, incomplete payloads, and
stale old-name rows rejected on repeated fresh recovery. They preserve no-output
and unchanged-history assertions. Full Maven verification passed 4,979 connector
and 256 parser executions. Assembly, formatting and import validation passed.

To reproduce, use one existing member in an owned fixture, publish a keyed row,
then run `RNMM FILE(library/file) MBR(old) NEWMBR(new)`. Publish unrelated and
renamed-member rows, replace the task, publish another row, rename back and
verify the exact ledger and durable offsets. The old and new member share the
same native identity; a foreign identity or a target name already used by
another member must reject. See [IBM's RNMM command](https://www.ibm.com/docs/en/ssw_ibm_i_74/cl/rnmm.htm)
and [journal entry layout](https://www.ibm.com/docs/en/i/7.5.0?topic=information-layouts-variable-length-portion-journal-entries).

Independent review of the same frozen build passed 49 targeted cases: the 14
new cases, 29 relevant controls and six additional authority checks. It verified
cached-history replay, preservation of other members and ownership of F/MN
records within a physical-file rename. This was offline evidence; the native
comparison above was a separate run. Independent cleanup found no remaining
fixture schemas, libraries, journals, inquiry messages or local Docker objects.
A byte scan of 1,210 repository/evidence files found no configured password in
UTF-8, UTF-16 or CCSID 37; it did not decompress archives.

## File moves between libraries — 2026-11-13

A native comparison moved an excluded physical file between two fresh owned
libraries in the same storage pool, keeping its original journal. It inserted a
row in the destination, published unrelated captured rows, replaced the worker,
and moved the file back. The original `2e19ad27` plugin passed all four progress
observations. Build `265a310` passed the initial observation but failed at D/FM;
the other three observations failed, including after replacement. The first
baseline attempt encountered a harness error serializing an audit timestamp
after the move. Its result is retained separately, and both libraries were
cleaned before the corrected baseline ran.

Build `9036bcf` passed the same four observations using ordinary source offsets.
The final independent topic dump contains each probe row exactly once, and the
fixture's final source rows and durable offsets agree. These are availability
and identity checks, not a cross-partition ordering or exactly-once guarantee.
The frozen ZIP SHA-256 is
`ff3de39f6080e24256ae44206fd692157958cedbd13735dff0db4a78945bbdba`;
its manifest, compiled classes and embedded revision match the committed build.

The correction validates D/FM and every retained member's F/MM, then saves the
unchanged file/member identities under the new library. It does not acquire a
later catalog name or layout. Twenty-one new local cases cover both buffer
types, old/new/both/excluded capture selection, long SQL names, fresh recovery
between association records, stale-library rows, foreign identities, incomplete
member sets and unfinished transactions. Full Maven verification passed 5,000
connector and 256 parser executions; assembly, formatter and import validation
passed. Local captured-table tests do not establish native captured-move support.
The native comparison covered an excluded file only, with one member; it did not
test a cross-pool save/restore or a whole-library rename.

To reproduce, create two isolated libraries and an excluded journaled file in
one, confirm the target has no matching object, and use
`MOVOBJ OBJ(source/file) OBJTYPE(*FILE) TOLIB(target)`. Verify that journaling
still uses the source journal, publish unrelated captured rows, replace the task,
move the file back and compare the exact rows and offsets. Cleanup must track
the same owned object's identity in whichever library currently holds it.
See [IBM's MOVOBJ command](https://www.ibm.com/docs/en/i/7.5.0?topic=cbm-move-object-1)
and [D/FM and F/MM entry descriptions](https://www.ibm.com/docs/en/i/7.5.0?topic=information-all-journal-entries-by-code-type).

The independent offline review at `9036bcf` passed 47 targeted cases, including
six additional checks of cached member evidence, destination collisions and
SQL-name/key authority. That result covers plain moves, not every statement
composition. Independent cleanup verified all eight libraries used by the
plain-move comparisons, with no owned local Docker resources left.

### ALTER followed by a move during catch-up

A separate native experiment stopped the worker, altered an existing excluded
file, moved it to the other owned library, then restarted. Build `9036bcf` failed
with `Unqualified schema-change lifecycle`; another task replacement failed the
same way. This was a combined-statement gap in schema acquisition. The original
connector was not rerun for this combined case; its passing plain-move comparison
above remains a separate result.

Build `04d40bd` follows the complete, verified move to locate the altered file's
catalog definition. It records the ALTER and move at separate positions, keeping
rows before the move under their original library/topic. Repeated moves are
followed only while the native identities, SQL name and layout remain proven.
A later move during acquisition triggers re-observation; a different catalog
identity, SQL name, library or subsequent layout cannot authorize the earlier
ALTER.

The frozen ZIP SHA-256 is
`6772a7f39e6c45f9584b24e638a38599cf6862971209c7e32a30f7b936fcb614`.
Its manifest, compiled classes and embedded revision match `04d40bd`. The same
native experiment passed all four progress observations on fresh fixtures:
before the move, after ALTER/move catch-up, after another worker replacement,
and after moving back. Each unrelated probe row appeared exactly once in the
final topic dump; final source contents and durable offsets were checked.
This uses ordinary source offsets and an excluded moved file. It does not
establish native captured-move routing or broker exactly-once delivery.

Fourteen additional local cases cover memory/disk restart, captured/excluded
rows, a move round trip before acquisition, foreign catalog/row identities,
unproven SQL names and layouts, and the late-acquisition race. Invalid proof
emits no records and leaves history unchanged. Full Maven verification passed
5,014 connector and 256 parser executions; assembly, formatting and imports
passed. The before-fix native failure and its cleanup evidence are preserved.
Runner cleanup and a separate read-only check verified that all four libraries
from the failing and corrected combined cases, their journals and inquiry
messages, and their local Docker resources were absent.

Independent review of the same frozen build passed 57 distinct offline cases:
the 14 added cases, 37 controls and six additional checks. Both capture-selection
directions preserved topics, keys and the ALTER layout through recovery. Changed
saved member-association bytes rejected without new output, returned checkpoint
or history mutation. Artifact identity and Advisor review passed. These local
results remain separate from the native experiment above; they do not establish
every DDL composition or whole-feature readiness.

### CREATE and load followed by a move during catch-up

A further native comparison created a new excluded file, inserted a row and
moved it before the stopped connector could acquire its creation. The original
`2e19ad27` build continued, including worker replacement and move-back. Build
`04d40bd` failed with `Unqualified schema-change lifecycle`; replacement failed
at the same retained sequence. These pre-fix results remain separate from the
passing ALTER/move case above.

Build `7587127` validates the new file/member association and the complete
D/FM/F/MM move before locating the catalog in its destination. It qualifies the
creation under the original library and replays the move separately. The final
catalog cannot supply a different identity or layout for earlier records, and
incomplete creation/outcome evidence cannot authorize output. No new history
format or change to ordinary row dispatch is involved.

The frozen ZIP SHA-256 is
`02f853357a08935881fd87fd791f8fdca21125bf10c0de6d5ee4b9ed122ad2e5`.
Its manifest, compiled classes and embedded revision match `7587127`. On fresh
native fixtures it passed all four observations: before the move, after the
CREATE/load/move catch-up, after worker replacement and after moving back.
Each unrelated probe appeared once in the final topic dump; source contents and
durable offsets were also checked. This validates excluded-file progress with
ordinary source offsets. Captured cross-library routing and broker exactly-once
delivery remain outside this native result.

Fifteen added local cases cover old/new/excluded capture selection in both
storage modes, exact topics and keys across four fresh recoveries, and a durable
checkpoint between D/FM and F/MM. Conflicting catalog, row or member identities,
later layouts, incomplete moves and unfinished creation reject without output or
new history. A move after the catalog observation requires re-observation. The
focused DDL suite passed 531 cases. Full Maven verification passed 5,029 connector
and 256 parser executions; assembly, formatter and import validation passed.

Runner cleanup and a separate read-only check found no remaining schemas,
libraries, journals, inquiry messages or owned Docker resources from all three
comparisons. A byte scan of 2,604 repository/evidence files found no configured
password in UTF-8, UTF-16 or CCSID 37; archives were not decompressed. The
performance matrix remains pinned to `04d40bd`, not this subsequent correction.

Independent review of the same frozen build passed 88 distinct offline cases:
the 15 added cases, 59 controls and 14 additional recovery/authority checks.
Repeated moves preserved the original, intermediate and final library topics
and keys through six fresh recoveries, including checkpoints between each
D/FM/F/MM pair. Changed saved companion bytes and unsafe historical-name
selection rejected without new output, checkpoint return or history mutation.
Artifact identity and Advisor review passed. The native result above is separate;
these local checks do not establish arbitrary DDL combinations or whole-feature
readiness.

### Captured library-move validation on the published build — 2026-11-13

The published `96a395a9c4dd64ce474895d3ce339cbea6d864c1` plugin was exercised in
four fresh two-library fixtures. Its SHA-256 is
`c8a28302ff5b8bebf54d7922c40dba9e937842d9dacfaa01d7b3d1727a96cd80`.
Preflight matched all 203 production sources and 15 packaged JARs to the frozen
publication manifest. The production code and plugin were unchanged during the
runs; the new runner is `tools/dcg-library-move-probe.py`.

Ordinary-offset runs selected both libraries, only the original library, and
only the destination. A fourth run selected both under Kafka source transactions.
Each passed 37 assertions, for 148 total. A stopped reader caught up through an
existing table's ALTER/row/move and a new table's CREATE/row/move. Exact broker
ledgers checked topics, keys, values and order before and after each move. The
runner replaced the worker during an open truncate of the moved table, checked
unrelated progress with F behind W, then checked commit, rollback, move-back and
another replacement. No duplicate output appeared in these bounded schedules.
Ordinary offsets still provide at-least-once delivery outside those schedules.

A separate audit of the retained broker dumps checked source coordinates and
row RRNs. Each native journal interval contained four D/FM moves and two subtype-8
D/CG clears; these were SQL TRUNCATE tests, not optimized DELETE tests. All four
runner cleanups passed. Independent read-only checks found no remaining fixture
schemas, libraries, journals, inquiry messages or owned Docker resources.

Two test-controller setup mistakes are retained separately. The initial include
filter named a not-yet-created table and failed inherited legacy startup; its
fixture cleanup passed. The corrected runner uses standard exclusion filters.
The first successful both-library ordinary-offset run required an explicit
controller resume before any move workload because the shared legacy setup
leaves the connector stopped. That missing step was added to the runner; the
other three successful runs needed no intervention. These were harness changes,
not connector fixes. This evidence does not qualify cross-pool moves, save/restore,
multi-member clear semantics or every possible DDL composition.

### Published-build performance and endurance — 2026-11-14

The same `96a395a` plugin passed a matched six-run comparison with the original
connector: two 120-second measurements per profile, 144,000 verified changes in
total. Feature-disabled median delay was 5.130 s versus the original's 5.124 s,
with 3.6% more CPU per change. The disk-buffer profile's median delay was 1.934 s,
with 23.6% more CPU per change. All sustained approximately 200.8 changes/second.
See the [full comparison](dcg-performance-comparison.md#published-build-performance-comparison--2026-11-14)
for dependencies, worker mode, ranges and measurement limits.

A separate packaged disk-buffer run completed 1,204.7 seconds of mixed-column
workload with Kafka source transactions. Its 243 timed rounds produced 72,900
inserts/updates; this serial burst workload waits for observation between bursts
and is not the 200-change/second performance workload. Including warm-up, backlog,
transaction and restart probes, the independent observer retained exactly 92,401
events: 67,400 creates, 25,000 updates and one truncate. All 37 runner assertions
passed. A separate audit checked exact counts, duplicate keys/operations and
broker offsets, discarded rollback rows, clear/reload order and checkpoint
progress.

Before the timed interval, two 5,000-row backlog/resume trials and separate
5,000-row transaction commit/rollback cases passed. A clear stayed open for 61.0
seconds while ten unrelated bursts progressed and three journal receivers were
rotated. Its replay floor remained protected, and commit released the clear
before all 1,000 reload rows. Three task stop/resume cycles during the timed
workload preserved immutable history and non-regressing F/W. These are graceful
task restarts; the preceding library-move runs separately killed and replaced
workers while a clear was open.

Whole-worker resident memory peaked at 490.6 MiB across the run and 486.1 MiB
during the timed interval. Quarter-by-quarter median RSS was 445.6, 430.8, 422.1
and 423.0 MiB. The 261 recorded collections left 36.8–55.0 MiB of used heap after
GC, ending at 37.9 MiB; total GC pause time was 788 ms. This one run with task
restarts does not establish multi-day leak freedom or maximum capacity.

The first endurance attempt is retained as a harness failure, not a connector
failure or completed soak. Its second restart exceeded the generic broker
inspector's 1,000-record limit while reading obsolete offset versions. The
test-only inspector now has an explicit latest-value-per-key mode for offsets,
including tombstones and a fixed broker endpoint. Key/data/scan/time bounds
remain, and ordinary event inspection still retains every record. Four targeted
Java cases and the 153-test Python harness suite passed. The full endurance
schedule then ran again in a fresh fixture; the connector ZIP was unchanged.

Independent final queries found no remaining schemas, libraries, journals or
inquiry messages for all 18 owned fixture libraries across successful and failed
attempts. All 13 owned Docker run prefixes were absent. Final verification again
matched all 203 production sources and 15 plugin JARs to the publication manifest.
No production connector change was needed for this validation.
