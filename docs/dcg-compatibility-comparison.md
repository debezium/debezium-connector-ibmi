# Feature compatibility

Transactional truncate capture extends ordinary row capture and is opt-in.
The table distinguishes existing behavior from the added transactional path.
The reference connector implementation is `2e19ad27`; deployment conditions are
in [runtime compatibility](dcg-compatibility-baseline.md), and measured results
are in [validation coverage](dcg-validation.md).

## Compatibility

| Capability | Original connector | With transactional capture enabled |
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

See [runtime compatibility](dcg-compatibility-baseline.md) and
[configuration contract](dcg-online-configuration.md) for exact qualifications.

## Performance and verification

[Performance methods and results](dcg-performance-comparison.md) compare the
original reader, the feature-disabled path, and transactional disk/memory
storage. They distinguish the legacy delay from storage cost and report CPU
tradeoffs as well as latency. They are not capacity or equivalence guarantees.

[Validation coverage](dcg-validation.md) records the tested artifact, native
scenario scope, failed attempts and endurance results. Use the
[testing guide](dcg-contributor-testing.md) to reproduce affected cases on a new
build instead of attributing an earlier run to changed code.
