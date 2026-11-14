# Connector compatibility and performance comparison

This comparison describes the verified transactional-truncate candidate on
November 13, 2026. Transactional capture remains **opt-in and under review**.
An available option is not a claim that every deployment or workload has been
qualified. Qualified scope and remaining release gates are listed explicitly.

The original benchmark plugin was built from `2e19ad27`. Upstream `main` at
`ea277c0099876fc2b6f1d5e88e3dde1a60409b4c` differs from that commit only in CI
workflows: its connector code and Maven configuration are unchanged. The
matched benchmark distributions contain byte-identical shared Debezium core and
driver jars; the candidate additionally packages history and disk-buffer storage.
Current-upstream CI remains a separate merge gate.

## Compatibility

| Capability | Original connector | Verified candidate with transactional capture enabled |
| --- | --- | --- |
| Ordinary insert/update/delete | Supported | Preserved; native comparisons check keys, values and update before-images |
| Committed SQL truncate / optimized bulk delete | Clear can be missed when IBM i omits individual row deletes | Emits keyless `op=t` before committed reload rows; discards rolled-back work |
| Other tables while a clear is open | Ordinary journal streaming | Unrelated committed table changes continue; no journal-wide transaction pause |
| Existing-stream activation | No transactional-truncate handoff | Uses existing offsets without a resnapshot; brief automatic lock boundary, with busy attempts deferred. A clear encountered while waiting holds its table until retained history proves its outcome; other tables continue |
| Distributed Kafka Connect | Supported | Ordinary offsets or the stricter source-transaction contract; Kafka 4.2/4.3 qualification retained |
| Standalone / Embedded Engine | Existing deployment paths | Actual standalone and Engine restart/offset tests; native Engine validation without Kafka |
| Debezium Server | Included in upstream Server since DBZ-9015 | Native validation of the Server JVM/HTTP profile with file history and offsets; other sink combinations are not universally qualified |
| Initial, `no_data`, requested full and incremental snapshots | Ordinary snapshotter and core incremental algorithm | Integrated with durable acknowledgement, interrupted snapshot recovery and F/W replay |
| SQL/DDS keys, keyless tables and ordinary output settings | Supported according to existing schema/type handling | Preserved; SQL/system names, supported text/binary/temporal types, CCSID overrides, column/key settings and custom converters covered |
| Transforms, predicates and operation filters | Applied by the runtime | Uses the same runtime mechanisms; recovery progress must survive filtering and consumers must retain truncate events when required |
| WARN/SKIP row conversion | Supported | Preserved through `event.converting.failure.handling.mode`; transactional enablement requires processing mode `fail` before handoff. Failed clear delivery, invalid decoding and missing recovery evidence remain fatal |
| Current-definition refresh and new-table discovery | Refreshes current JDBC metadata | Qualified CREATE/ALTER, full rollback, LABEL/COMMENT and same-library physical renames; statements may share a final definition when no captured row requires an unavailable intermediate layout. History is retained for restart |
| Moving a file to another library | Excluded-file native comparison continues unrelated CDC | Native plain, ALTER/row/move and CREATE/row/move tests pass, including original/destination capture selection, replacement and move-back. Both-library capture also passes with Kafka source transactions; cross-pool save/restore remains unqualified |
| Arbitrary historical DDL reconstruction | Not provided | Not added to the compatibility requirements; ambiguous historical layouts reject |
| File/receiver count | No fixed connector file-count limit | No arbitrary connector count ceiling; bounded acquisition/history processing. Largest live scope tested: 56 files on a preceding build; the current dedicated runs use smaller scopes. Larger local fixtures are not native capacity evidence |
| Large transaction payloads | No equivalent transaction buffer | H2 MVStore working storage by default; explicit memory profile available. Excluded rows do not retain individual output entries; transaction context is still journal-wide. Metadata scales with catalog size |
| Multiple Kafka data partitions | Supported | Accepted; whole-table ordering of keyless truncates and keyed rows requires one partition or equivalent downstream ordering |
| Native storage pools | No explicit ASP-1-only admission | System/basic pools 1–32 accepted by evidence validation; live qualification used pool 1. Independent ASPs remain unqualified |
| Multiple file members | Row capture shares keys/topic; snapshot reads the first member | Preserves that row/snapshot contract, validates every member identity, discovers newly added members, preserves same-file member renames and recovers their history after restart. A removed final member leaves a file with no active row identity until a new association is qualified |
| Mixed journal resources | No whole-journal PF-only admission | Logical files/indexes, data areas, data queues and access paths can coexist with captured physical files; their metadata cannot establish a row binding |
| Excluded LOB/source/program-described files | Do not prevent selected ordinary table capture | Retains native identities and transaction effects without publishing excluded values; program-described member identities come from the native object catalog |
| Clearing one member of a multi-member file | No reliable table-wide clear event | Rejects a captured member clear without advancing completed output; no unsafe table-wide `op=t` or new member-aware output contract |

The original two-member native control emitted duplicate keys from separate
members into one topic and snapshotted only the first member. It does not establish
complete member-aware semantics. A separate control accepted SQL `TRUNCATE` on a
one-member DDS file and rejected it after a second member was added. Native
`CLRPFM MBR(SECOND)` cleared only that member, while `MBR(*ALL)` cleared both.
A partial member clear cannot safely become a whole-table `op=t` in the existing
combined output. The blanket single-member/physical-file-only journal admission
restriction has been removed. This does not add member-aware keys, topics or
snapshots that the original connector did not provide.

The packaged multi-member test starts with two members, enables transactional
capture, adds a third while streaming, restarts from durable history and checks
the existing keys and topics. It also checks unrelated member progress during a
single-member table's open truncate, committed clear delivery and safe member-clear
rejection. Index creation/deletion and access-path journaling changes run in the
same fixture alongside a journaled data area and queue. Local memory/disk tests
cover a crash after unrelated output advances between the new member's native
association records; history must already be durable and F must still pin the
complete association for replay.

The excluded-LOB control uses the original packaged connector as well as the
candidate, with a separately owned fixture. Candidate tests preserve selected
output through 60 KB, NULL and empty CLOB values, mixed commit/rollback and a
replacement during an open transaction. Native pointer-row checks establish
operation/source/length/flags/null metadata only; they do not validate individual LOB descriptors or checksum external
LOB values. Capturing those values is not added here: the original CLOB decoder
was absent, and its XML row decoder returned an empty string rather than the
actual document. Unknown or incomplete row layouts still reject.

Complete source images, required native identifiers, retained receiver history
back to the replay floor, and matching immutable binding history remain necessary
for recovery. Delivery is at least once; an interrupted output batch can replay
a published prefix. Ordinary offsets do not add broker fencing to an embedded
application. Unknown source layouts and missing history reject before advancement.

Open CREATE, ALTER and DROP transactions retain raw entries while unrelated
committed output continues, including across restart. Incomplete physical
companions or unavailable metadata at the outcome can still defer scanning. Repeated
ALTERs with captured rows between unavailable historical layouts still reject.
Removal of an empty final member now retains the file while unrelated capture
and restart continue; native member addition re-establishes its active identity.
This does not interpret populated-member removal as a table-wide truncate.
Constraint-only additions/removals retain
the decoder and event key already chosen for the capture, matching legacy streaming.
They do not automatically replace an emitted key with a newly declared database key. These
are remaining qualification limits, not claims of complete DDL/member parity.
A rejected clear cannot be repaired simply by disabling truncate capture; doing
so loses the clear. The configuration contract describes evidence recovery and
snapshot-based reconciliation when the original evidence cannot be recovered.

See the [baseline audit](dcg-compatibility-baseline.md) and
[configuration contract](dcg-online-configuration.md) for exact qualifications.

## Performance

The published candidate at `96a395a` was compared with the original connector in six serial
runs: original, disabled, enabled, then the reverse order. Each used a fresh
native fixture and local Kafka. After two warm-up rounds, every run delivered all
24,000 expected changes at about 200.8 changes/second for 120 seconds: 144,000 changes
in total. Every key, mixed value and update before-image was checked. All six
runs passed fixture, session and local-runtime cleanup.

| Profile | Median delay | p95 delay | CPU ms/change | CPU vs. original | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original connector | 5.124 s | 5.193 s | 0.737 | Baseline | 412.1–435.6 MiB |
| Candidate, feature disabled | 5.130 s | 5.197 s | 0.763 | +3.6% | 408.9–471.6 MiB |
| Candidate, disk buffer | 1.934 s | 2.688 s | 0.911 | +23.6% | 440.7–482.4 MiB |

Delay and CPU columns are medians of the two run-level measurements; RSS lists
the range of their peaks. Feature-off median delay increased 0.12%, with 3.6%
more CPU per change. Enabled median delay was 62.3% lower, with 23.6% more CPU.
All profiles sustained the offered workload; this is not a claim of equal CPU
cost or maximum throughput. RSS is resident memory, not Java heap. Delay measures
SQL submission to independent Kafka observation, not database commit time. Two
short runs per profile do not establish statistical equivalence, large-catalog
capacity, large-transaction memory behavior or multi-day stability. The
memory-buffer profile and schema-change latency were not rerun in this matrix.
The published build includes the CREATE/move correction at `7587127`.

The candidate ZIP is
`c8a28302ff5b8bebf54d7922c40dba9e937842d9dacfaa01d7b3d1727a96cd80`.
The matched baseline ZIP is
`46f4010f4c0683a43ad6c3602e881b13d125c8d04de940d250b370bc07c1ef6c`.
Its original connector and parser jars are unchanged. Its driver override differs
from the preceding baseline only in manifest metadata; driver classes and resources
are identical. Shared Debezium and driver dependencies match the candidate; the
candidate additionally includes history and disk storage. The Kafka image was
identical across runs. Kafka source transactions used poll boundaries for the
original/disabled paths and connector boundaries after enablement. See the
[reproduction method and per-run ranges](dcg-performance-comparison.md#published-build-performance-comparison--2026-11-14).
Earlier results retain their own artifact identities in the
[validation report](dcg-validation.md#matched-dependency-performance-comparison).

A separate final-build disk-buffer run completed 20 minutes with three task
restarts, three receiver rotations during an open clear, and 5,000-row
commit/rollback and backlog cases. All 92,401 observed events passed final
count/duplicate checks. Timed-quarter median RSS was 445.6, 430.8, 422.1 and
423.0 MiB, with a full-run peak of 490.6 MiB. This bounded serial-burst run is not
a maximum-throughput or multi-day memory qualification. Its first attempt hit
an offset-inspector limit; the test-only correction and complete fresh rerun are
documented in the [endurance evidence](dcg-validation.md#published-build-performance-and-endurance--2026-11-14).
Independent cleanup confirmed all fixtures and owned local runtimes absent.

## Verification and review

Full Maven verification at `7587127` passed 5,285 Java test executions (5,029
connector and 256 parser). The Python
harness suite passed 153 with the published-build validation harness changes. Assembly,
formatter and import validation also passed. Bounded packaged native
runs cover the review's transactional DDL, overlapping clears, catalog churn,
program-described files, member removal, retained receivers and restart cases.
Each run retains its own artifact identity; earlier native or performance results
are not automatically attributed to later builds. Advisor inspected the actual
files and identified a deferred-creation clear issue, which was corrected and
reverified with targeted negative controls. See the
[latest correction evidence](dcg-validation.md#fifth-independent-review-corrections--2026-11-13)
and [schema-change latency measurements](dcg-performance-comparison.md#fifth-review-schema-change-latency--2026-11-13)
for exact qualifications and remaining limits. Later bounded reviews and native
runs cover [historical names and excluded lifetimes](dcg-validation.md#historical-names-and-excluded-lifetimes--2026-11-13),
including committed shared ALTER/DROP transactions and the subsequent
[full rollback validation](dcg-validation.md#shared-alterdrop-full-rollback--2026-11-13),
[known-table name boundaries](dcg-validation.md#known-table-name-boundaries--2026-11-13),
[empty final-member recovery](dcg-validation.md#empty-final-member-recovery--2026-11-13),
[same-file member renames](dcg-validation.md#same-file-member-rename--2026-11-13)
and [file moves between libraries](dcg-validation.md#file-moves-between-libraries--2026-11-13).
Those results do not qualify every DDL composition. Broader review and upstream CI remain
release gates; this is not a replacement-readiness claim.
