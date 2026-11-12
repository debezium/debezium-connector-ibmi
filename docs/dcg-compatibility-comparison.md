# Connector compatibility and performance comparison

This comparison describes the verified transactional-truncate candidate on
November 12, 2026. Transactional capture remains **opt-in and under review**.
An available option is not a claim that every deployment or workload has been
qualified. Remaining gaps are listed explicitly.

The original benchmark plugin was built from `2e19ad27`. Upstream `main` at
`ea277c0099876fc2b6f1d5e88e3dde1a60409b4c` differs from that commit only in CI
workflows: its connector code and Maven configuration are unchanged. Both
benchmark distributions use the pinned local Debezium dependency build documented
in the validation report; current-upstream CI remains a separate merge gate.

## Compatibility

| Capability | Original connector | Verified candidate with transactional capture enabled |
| --- | --- | --- |
| Ordinary insert/update/delete | Supported | Preserved; native comparisons check keys, values and update before-images |
| Committed SQL truncate / optimized bulk delete | Clear can be missed when IBM i omits individual row deletes | Emits keyless `op=t` before committed reload rows; discards rolled-back work |
| Other tables while a clear is open | Ordinary journal streaming | Unrelated committed table changes continue; no journal-wide transaction pause |
| Existing-stream activation | No transactional-truncate handoff | Uses existing offsets without a resnapshot; brief automatic lock boundary, with busy attempts deferred |
| Distributed Kafka Connect | Supported | Ordinary offsets or the stricter source-transaction contract; Kafka 4.2/4.3 qualification retained |
| Standalone / Embedded Engine | Existing deployment paths | Actual standalone and Engine restart/offset tests; native Engine validation without Kafka |
| Debezium Server | Included in upstream Server since DBZ-9015 | Native validation of the Server JVM/HTTP profile with file history and offsets; other sink combinations are not universally qualified |
| Initial, `no_data`, requested full and incremental snapshots | Ordinary snapshotter and core incremental algorithm | Integrated with durable acknowledgement, interrupted snapshot recovery and F/W replay |
| SQL/DDS keys, keyless tables and ordinary output settings | Supported according to existing schema/type handling | Preserved; SQL/system names, supported text/binary/temporal types, CCSID overrides, column/key settings and custom converters covered |
| Transforms, predicates and operation filters | Applied by the runtime | Uses the same runtime mechanisms; recovery progress must survive filtering and consumers must retain truncate events when required |
| WARN/SKIP row conversion | Supported | Preserved for ordinary rows; failed clear delivery, invalid decoding and missing recovery evidence remain fatal |
| Current-definition refresh and new-table discovery | Refreshes current JDBC metadata | Qualified observed ALTER/creation boundaries are persisted before output and replayed on restart |
| Arbitrary historical DDL reconstruction | Not provided | Not added to the compatibility requirements; ambiguous historical layouts reject |
| File/receiver count | No fixed connector file-count limit | No arbitrary connector count ceiling; bounded acquisition/history processing. Largest live scope tested: 56 files; larger local fixture tests are not native capacity evidence |
| Large transaction payloads | No equivalent transaction buffer | Optional H2 MVStore working storage; memory/disk recovery and constrained-heap/failure tests. Metadata still scales with catalog size |
| Multiple Kafka data partitions | Supported | Accepted; whole-table ordering of keyless truncates and keyed rows requires one partition or equivalent downstream ordering |
| Native storage pools | No explicit ASP-1-only admission | System/basic pools 1–32 accepted by evidence validation; live qualification used pool 1. Independent ASPs remain unqualified |
| Multiple file members / mixed journal resources | Partial multi-member row capture, first-member snapshot, shared keys/topic; no whole-journal PF-only admission | **Open gap:** current transactional admission still requires a qualifying single-member physical-file inventory, including excluded files |

The original two-member native control emitted duplicate keys from separate
members into one topic and snapshotted only the first member. It does not establish
complete member-aware semantics. A separate control accepted SQL `TRUNCATE` on a
one-member DDS file and rejected it after a second member was added. Native
`CLRPFM MBR(SECOND)` cleared only that member, while `MBR(*ALL)` cleared both.
A partial member clear cannot safely become a whole-table `op=t` in the existing
combined output. Preserving ordinary row compatibility and narrowing the blanket
admission restriction remain work in progress; this candidate is not feature complete.

Complete source images, required native identifiers, retained receiver history
back to the replay floor, and matching immutable binding history remain necessary
for recovery. Delivery is at least once; an interrupted output batch can replay
a published prefix. Ordinary offsets do not add broker fencing to an embedded
application. Unknown source layouts and missing history reject before advancement.

See the [baseline audit](dcg-compatibility-baseline.md) and
[configuration contract](dcg-online-configuration.md) for exact qualifications.

## Performance

![Bounded native performance comparison](images/transactional-truncate-performance.png)

Each profile used a separate disposable native fixture, the ordinary packaged
connector, local Kafka and an independent `read_committed` observer. Two warm-up
rounds preceded 60 seconds of independent paced writes: 12,300 measured changes
at approximately 201 operations/second. All profiles used WARN conversion
handling and inherited worker defaults. Every expected record, mixed value and
update before-image was checked, and fixture/session/Kafka cleanup passed.

| Profile | Median delay | p95 delay | p99 delay | Worker CPU ms/operation | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original | 5.215 s | 5.293 s | 5.320 s | 0.846 | 415.6 MiB |
| Candidate, feature disabled | 5.214 s | 5.296 s | 5.348 s | 0.864 | 431.3 MiB |
| Candidate, memory buffer | 2.920 s | 4.003 s | 4.079 s | 0.884 | 442.8 MiB |
| Candidate, disk buffer | 3.055 s | 4.153 s | 5.325 s | 1.036 | 443.8 MiB |

Delay is SQL submission to observation, not database commit latency. Resource
measurements cover the whole worker during the measured interval. Disabled-path
delay stayed close to the original and CPU was about 2.1% higher. Enabled memory
and disk used about 4.4% and 22.4% more CPU, respectively. Disk buffering bounds
large payload working sets; this small workload does not demonstrate lower RSS.
These measurements are **not maximum throughput, statistical equivalence, a
large-catalog benchmark or a multi-day soak**.

The measured candidate ZIP SHA-256 is
`3df76e426810063b52d7f40a3b97daf9e207389d1a43fdd7f9c1f267c0f43640`.
The original ZIP SHA-256 is
`da0cbb3ab5228fbe676520e905b88f36158f4adc0a73a856bd18b4583908339a`.
Reproduction commands and earlier successful/failed qualification results are in
the [validation report](dcg-validation.md#final-compatibility-artifact-performance).

## Verification and review

The candidate passed 4,467 Java tests, 152 Python harness tests, Maven verification,
assembly and formatter/import checks. Packaged native runs cover schema/capture
selection changes, requested full snapshots, new-table discovery, restart and
ordered truncate/reload using both ordinary offsets and source transactions.
The independent actual-file review found no actionable issue in the sampled
recovery, schema-transition and pointer-cleanup paths; it was not exhaustive
across all changed files. Native fixtures and local runtimes were cleaned up.
No production deployment or universal downstream-sink qualification is claimed.
