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
| File/receiver count | No fixed connector file-count limit | No arbitrary connector count ceiling; bounded acquisition/history processing. Largest live scope tested: 128 journaled files, with three captured tables. Handoff after a deliberate blocker was released took 437 seconds; this is not a large-catalog throughput qualification |
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

The published runtime at `96a395a` has two matched native comparisons against
original connector/parser `2e19ad27`, with byte-matched shared dependencies and
local Kafka 4.3.1. Six earlier 120-second runs covered approximately 201 submitted
changes/second (144,000 verified changes, two repetitions per profile). Ten later
120-second runs compared reader, buffer storage and the legacy delay at two
requested rates (768,300 verified changes, one run per profile/rate). Every
measured key, mixed value, update before-image and final count passed.

The 500-change/second comparison reached approximately its requested rate in
all five profiles:

| Profile | Median delay | p95 delay | Worker CPU ms/change | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: |
| Original, default 5 s margin | 5.214 s | 5.301 s | 0.323 | 411.7 MiB |
| Candidate, feature disabled | 5.212 s | 5.298 s | 0.323 | 414.7 MiB |
| Transactional, disk | 2.310 s | 3.155 s | 0.453 | 453.2 MiB |
| Transactional, memory | 2.256 s | 3.129 s | 0.377 | 450.0 MiB |
| Original, zero-delay diagnostic | 0.172 s | 0.252 s | 0.320 | 457.6 MiB |

The disk path's median delay was 55.7% lower than the original default, with
40.1% more whole-worker CPU per change. Disk was not faster than memory. The
zero-delay control shows that the original's fixed margin accounts for most of
the latency difference; it does not validate removing that safety margin under
delayed journal visibility. The legacy default remains unchanged.

At a requested 1,000 changes/second, the writer achieved 769–783, so those runs do
not establish 1,000-change/second capacity. Median delays were 5.226 s original,
5.231 s disabled, 2.491 s disk, 2.372 s memory and 0.181 s zero-delay diagnostic.
CPU/change was 8.9% above the original with the feature disabled and 67.3%
above it with disk buffering at these slightly different achieved loads. The
earlier two-repeat 201-change/second comparison found a 62.3% median delay
reduction and 23.6% CPU increase for disk; these observations must not be combined
into a single load-independent performance guarantee.

Delay is SQL submission to independent `read_committed` Kafka observation,
including database/network and Kafka transaction visibility, not sink latency.
Kafka source transactions used poll boundaries for legacy and connector
boundaries after enablement. RSS is whole-worker resident memory, not retained
heap. These are bounded workloads, not maximum throughput, statistical
equivalence, large-catalog latency or multi-day stability results. The storage
comparison changes only `truncate.buffer.storage` within the transactional path;
its memory limits and remaining scratch/history requirements still apply.

The candidate ZIP is
`c8a28302ff5b8bebf54d7922c40dba9e937842d9dacfaa01d7b3d1727a96cd80`.
The matched baseline ZIP is
`46f4010f4c0683a43ad6c3602e881b13d125c8d04de940d250b370bc07c1ef6c`.
See the [per-run results and exact test settings](dcg-performance-comparison.md#matched-reader-and-storage-controls--2026-11-14)
and the [earlier repeated comparison](dcg-performance-comparison.md#published-build-performance-comparison--2026-11-14).

A separate final-build disk-buffer run completed 20 minutes with three task
restarts, three receiver rotations during an open clear, and 5,000-row
commit/rollback and backlog cases. All 92,401 observed events passed final
count/duplicate checks. Timed-quarter median RSS was 445.6, 430.8, 422.1 and
423.0 MiB, with a full-run peak of 490.6 MiB. This bounded serial-burst run is not
a maximum-throughput or multi-day memory qualification. Its first attempt hit
an offset-inspector limit; the test-only correction and complete fresh rerun are
documented in the [endurance evidence](dcg-validation.md#published-build-performance-and-endurance--2026-11-14).
Independent cleanup confirmed all fixtures and owned local runtimes absent.

A subsequent fresh run completed 3,605.4 seconds without a task restart during
the timed hour. Its 660 serial-burst rounds produced 99,000 mixed changes;
116,851 total events passed the independent count, duplicate, rollback and
clear/reload audit. Timed-quarter median worker RSS was 454.4, 451.8, 452.6 and
453.7 MiB, with a timed peak of 495.0 MiB. All 120 storage observations reported
the same 3.125 MiB working file in the same worker process. Thirty-second storage
samples can miss transient peaks. This is continuous-runtime endurance evidence,
separate from both the restart test and the paced throughput/latency comparisons;
it does not establish multi-day stability. See the
[one-hour validation](dcg-validation.md#one-hour-without-task-restarts--2026-11-14).

## Verification and review

Full Maven verification at `7587127` passed 5,285 Java test executions (5,029
connector and 256 parser). The Python
harness suite passed 153 with the published-build validation harness changes.
Later observer/spill-test corrections passed 875 distinct focused Java cases in
each configured execution, and the larger-profile harness passed 155 Python
tests. Assembly, formatter and import validation also passed on the matching
production sources. Bounded packaged native
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
