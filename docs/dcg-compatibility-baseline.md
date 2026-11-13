# Compatibility baseline for transactional truncate

The existing Debezium IBM i (AS400) connector is the compatibility baseline.
Transactional truncate must extend that connector, not replace its runtime,
configuration, supported row types, or deployment choices with a narrower product.
Keeping the feature disabled preserves existing CDC, but is not sufficient to
declare the transactional feature compatible.

## Deployment evidence

The original source baseline, `2e19ad27c934826d96a84202dfbe4509b5d8f666`,
already uses `AbstractAsyncEngineConnectorTest` for its snapshot/streaming
integration test. Embedded execution is therefore an existing integration path,
not a new requirement invented for truncate.

Debezium Server added the IBM i connector to its distribution on May 7, 2025,
in [DBZ-9015](https://github.com/debezium/debezium-server/commit/2d10004aa229a1375821008df147a98ebaf1db55).
Earlier distributions lacking that dependency must not be confused with a
connector that fundamentally requires distributed Kafka Connect. Distribution
inclusion establishes packaging; it is not evidence that this branch's new
transactional runtime has passed Server integration tests.

Online startup now selects its acknowledgement contract from the actual source
task context. Without a source transaction context it uses ordinary durable
offsets, with at-least-once delivery. With a transaction context it retains the
existing stricter connector-managed Kafka transaction contract. Transaction
reconstruction and the native startup/history checks are shared. The earlier
operator-window initialization profile still requires source transactions.

The native online implementation now supports Kafka-backed history or file
history through Debezium's standard configuration. The file profile uses
ordinary offset acknowledgements and does not open Kafka clients for history or
topic preflight. Separate native runs qualified the actual Engine and a packaged
Debezium Server JVM application with the standard HTTP sink. Each runtime still
needs evidence from its actual startup, acknowledgement and restart paths;
qualification of one sink/serialization profile is not universal sink coverage.

### Source-offset acknowledgement qualification

The ordinary source-offset contract is available to the public online opt-in
path. It is selected from the actual runtime capability; no test provider is
required. The feature remains disabled by default and still requires complete
legacy or F/W offsets and the matching history. The existing stricter EOS tests
remain intact.

Every published record in this alternative carries an additional delivery
session, increasing delivery number and receipt in its source offset. F/W retain their
existing meaning and scan R is never persisted. The envelope is validated and
removed before the existing strict checkpoint decoder reads F/W. Older F/W
offsets without the envelope remain valid. An older connector build does not
understand the envelope; do not downgrade an experimental generation by manually
deleting these fields.

`commitRecord` means delivery, not a completed offset write. `commit` promotes
only the acknowledged prefix whose exact delivery marker is read back from the
offset store. This distinction matters when newer producer callbacks arrive
while an earlier flush is finishing: all records in a source transaction can
share the same conservative F/W, so F/W equality alone cannot identify the
flushed prefix. Missing, malformed, foreign, regressing or skipped-prefix
acknowledgements fail closed. A failed flush or empty commit cannot advance
durable F/W or authorize startup handoff.

After startup, reading and publication continue without waiting for an offset
flush. The live runtime tracks completed enqueue progress separately from scan R
and the last confirmed durable F/W. Native reads still verify the retained
journal and receiver chain from the durable floor, even when it lags. Missing
history, regressing saved positions or saved positions beyond completed enqueue
progress fail closed. A replacement always starts from saved F/W, never the live
enqueue position.

Streaming retains at most 256 records for delivery acknowledgement, then releases
that prefix once it has been delivered. Slow or failed offset writes do not
accumulate a second unbounded record buffer. Each version-2 delivery envelope
contains a task-local HMAC-SHA256 receipt over the exact offset. This lets a later
flush prove an already-released prefix using constant-size state and the highest
contiguous delivered number. The implementation uses only the JDK; there is no
receipt database, shared key or ownership service. It does not authenticate
restored history or fence another task: a new task has a new ephemeral key and
validates saved F/W through the existing startup protocol. Older version-1
delivery envelopes remain readable at restart.

`As400EmbeddedRecoveryTest` runs the actual Debezium engine, connector task,
coordinator and dispatcher with file-backed offsets and controlled native I/O.
It covers acknowledged snapshot startup and reader handoff, fresh-engine restart
with both memory and disk working storage, interrupted truncate delivery after a
flushed prefix, missing offsets with retained history, rejection of partial
initialization, retry after a failed offset write, continuous progress across
unflushed cuts, and bounded acknowledgement state beyond 900 output records.
Legacy handoff cases restart after an acknowledged legacy row, the handoff
barrier and initial F/W. They also reject missing history after either barrier
and withhold activation after a failed offset write. These are local runtime
tests with controlled native I/O, not Debezium Server application tests.

The ordinary contract is **at least once**: a crash can replay a delivered
truncate and its rows. It does not supply Kafka broker fencing, atomic delivery
across output topics, or protection from independently started overlapping
embedded applications. The application must preserve order and acknowledge only
successfully handled records. Tolerated conversion failures remain rejected. Standard transforms and intentional
data-record filtering now use the runtime acknowledgement contract described below;
recovery progress records must remain deliverable.

The existing local Connect probe has a separate `source-offsets` scenario, with
source transactions explicitly disabled. It retains the normal-task restart,
unrelated-table progress, late-read drain and missing-offset assertions:

```sh
tools/dcg-normal-task-probe /absolute/new/evidence-directory source-offsets
tools/dcg-normal-task-probe /absolute/another/evidence-directory source-offsets-lag
tools/dcg-normal-task-probe /absolute/standalone/evidence-directory standalone
```

The standalone scenario runs `connect-standalone.sh` with its real file offset
backend. It does not simulate standalone behavior in a distributed worker.
Multiple cuts progress before a 60-second flush, process death causes replay
from the saved file, and a fresh worker reads that same file without reseeding.
An independent `FileOffsetBackingStore` reader verifies the completed checkpoint.
Deleting offsets while retaining binding history rejects with zero output.
This local test controls native I/O; it does not claim automatic task reassignment
or broker fencing for standalone Connect.

The lag scenario uses the actual nontransactional Kafka worker with a 60-second
offset flush interval. Multiple reader cuts, the committed truncate/reload and
an unrelated-table update completed in 5.7 seconds while saved F/W remained at
the starting checkpoint. Killing that worker before flush and starting a new
one replayed the unsaved suffix in order. This is an at-least-once recovery
result, not a claim of duplicate-free delivery. A bounded comparison using the
previous candidate's preserved binary timed out waiting for truncate delivery
before flush; broker evidence showed only the earlier unrelated row and the
unchanged initial checkpoint. These results establish removal of the flush
dependency, not a general native throughput benchmark.

Local validation on November 11, 2026 passed 4,020 Maven tests (229 parser,
3,791 connector), including 15 actual-engine cases and the existing strict
acknowledgement suites. All 149 Python harness tests, plugin assembly and
formatter/import validation passed.

The Kafka 4.3.1 normal-task probe passed with source transactions disabled, and
the original transaction-enabled scenario passed again. Both preserved ordering,
unrelated-table progress while a clear was open, replay across reassignment,
stop-request versus completed drain, rollback invisibility and missing-offset
rejection. The disabled-transaction run used the actual `WorkerSourceTask` and
Kafka offset topic; it does not claim broker fencing. A newly reassigned task may
write a heartbeat at unchanged F/W with a fresh delivery session. The probe
verifies that receipt against that new task and independently excludes any
additional delivery or stale offset from the stopped generation.

The standalone file-offset scenario passed twice, including its strengthened
missing-offset rejection, and the original transaction-enabled distributed
scenario passed again. All their owned containers and networks were removed.

Separate isolated native tests used the real online legacy handoff with Kafka
source transactions disabled, immutable Kafka binding history and ordinary
offset acknowledgements. They passed busy-boundary deferral, concurrent row
capture, primary-key changes, unrelated-table progress during an open clear,
receiver rotation, fresh-worker F/W recovery, rollback invisibility and ordered
truncate/reload output. Killing a worker after the acknowledged legacy barrier
and before initial F/W also recovered the exact boundary without appending an
activation-history record. These two runs used the internal admission entry
point; their native cleanup and local Kafka cleanup passed.

The packaged public online path also passed all 17 native checks with source
transactions disabled. It used the shipping `As400RpcConnector`, no test provider,
and offsets produced by the ordinary connector's snapshot and streaming. It
verified both D/CG subtypes, no additional snapshot, rollback invisibility,
unrelated-table progress, receiver rotation, replacement, mixed-column output,
terminal drain, missing-offset rejection and rejection of unsafe downgrade.
See [the native result](dcg-validation.md#ordinary-source-offset-handoff--2026-11-11).

### File-backed history without a broker

The online path also accepts standard `FileSchemaHistory` configuration with
`FileOffsetBackingStore`. Its binding adapter preserves the standard JSON-lines
format while requiring bounded, strict recovery and forced writes. Missing,
malformed, truncated, oversized or locked history cannot authorize a checkpoint.
The file contains durable recovery history; it is not the disposable transaction
spill store. Existing Kafka history and fingerprints retain their prior behavior.

All 16 actual-engine recovery cases now use this file adapter. An additional
handoff failure case holds the real history-file lock: the acknowledged legacy
barrier remains recoverable, initial F/W and native reads stay blocked, and a
fresh Engine can resume once the lock is released. The full local verification
passed 4,037 Java tests and 150 Python tests, plus assembly and formatting checks.

The packaged public connector passed 12 checks against an isolated native
fixture in the actual Engine with **no Kafka broker**. It created its own legacy
snapshot and file offsets, enabled online capture without another snapshot,
kept unrelated-table CDC moving across receiver rotation, recovered after
process death, suppressed rolled-back clears and ordered committed clears before
their rows. Both D/CG subtypes and mixed-column values were verified. Removing
required history stopped a fresh process before output and left its offset file
unchanged. Fixture and process cleanup passed. See
[the file-history result](dcg-validation.md#file-history-and-broker-free-engine--2026-11-11).

The separate Server run passed the same 12 checks using unchanged upstream
Server core/HTTP-sink classes, the shipping connector and a local HTTP collector.
It used standard Server configuration, file offsets/history and JSON with
schemas. This qualifies that JVM/HTTP profile, including native handoff and
process replacement; it does not cover every Server distribution, sink or
serialization combination. HTTP delivery in this test carries values, so its
row-identity assertions inspect the row payload rather than proving record-key
transmission. The Engine test separately inspects actual keys. The original
transaction-enabled path and startup durability barriers retain their stricter
acknowledgement behavior. Details and pinned versions are in
[the Server result](dcg-validation.md#server-jvmhttp-profile--2026-11-11).

## Compatibility work

| Area | Required result | Evidence still required |
| --- | --- | --- |
| Existing CDC | Default configuration and existing offsets retain their behavior; F/CR remains separate | Differential tests and existing integration suites |
| Deployment | Embedded Engine, Server and standalone operation do not depend on distributed Connect fencing | Engine, standalone and the tested Server JVM/HTTP profile passed; other Server sink/serialization combinations need their own delivery evidence |
| Buffering | Large transactions, many open transactions, savepoints and prepared output cannot exhaust the heap | Disk spill, constrained-heap tests, disk-full/corruption/cleanup tests and throughput comparison |
| File scope and history | File/receiver/history volume is processed incrementally rather than rejected at an arbitrary count | Paged acquisition and history, larger native scope, interruption/restart |
| Output compatibility | Existing keys, values, headers, topic selection and supported transforms/converters remain consistent | Differential records, filtered/dropped records, conversion failures and durable acknowledgement |
| Schema and membership changes | Preserve basic current-definition refresh where the original supports it | Safe refresh with in-flight work; general historical DDL reconstruction is outside the original connector's guarantees and outside this compatibility target |
| IBM i capabilities | Distinguish existing connector limitations from new profile restrictions | Documented semantics and isolated native captures; never infer correctness from the old path ignoring a journal entry |

### Standard topic routing, November 12, 2026

The transactional path now accepts standard Kafka Connect `RegexRouter` chains.
Connect or Engine performs the actual transform. The connector predicts the
destination for topic preflight and delivery acknowledgement, preserving keys,
values, headers and source offsets. Schema history has its own producer and is
not routed. Router order, expressions and replacements are pinned in the capture
fingerprint; configurations without routing retain their previous fingerprint.

Kafka Connect acknowledges the original source record with the final broker
topic. Engine can acknowledge the transformed copy. The latter is matched using
the existing unique delivery offset and every record field, allowing only the
configured topic change. Wrong payloads, offsets, partitions, headers, stale
callbacks and duplicate callbacks cannot advance the checkpoint. The lookup is
retired with the existing bounded acknowledgement state.

Local Kafka 4.3.1 runs passed with source transactions enabled and disabled. Both
used the actual task, coordinator and dispatcher with controlled native I/O.
They checked replacement recovery, unrelated-table progress during an open clear,
late-reader drain, rollback invisibility and missing-offset rejection. Independent
broker checks found only the routed output topics. Both runs removed their owned
containers and networks. Reproduce them with:

```sh
tools/dcg-normal-task-probe /absolute/new/eos-evidence routing
tools/dcg-normal-task-probe /absolute/new/ordinary-evidence routing-source-offsets
```

Actual Engine tests cover routed snapshot completion, memory/disk recovery and
restart after each acknowledged legacy-handoff phase with file offsets/history.
Full verification passed 4,261 Java tests (235 parser and 4,026 connector,
including 21 actual Engine cases). Advisor identified a configuration-parsing
mismatch: Kafka trims surrounding whitespace from both router settings. After
matching that behavior in prediction and fingerprinting, all 197 affected Java
cases and 151 Python tests passed, including differential whitespace cases
against Kafka's actual `RegexRouter`. These are local routing/recovery results,
with no new IBM i connection or native throughput claim for that routing change.
The subsequent qualification below extends this to payload transforms, predicates
and custom converters. Distinct output/control/history destinations remain required.

## Bounded local storage

Use task-local disk storage for reconstructible working state. The IBM i journal
and the acknowledged source offset remain the recovery authority. A replacement
task must be able to start on another machine with an empty scratch directory.
Local spill files are neither shared ownership records nor durable checkpoints.

The implemented optional storage backend is standalone H2 MVStore
(`com.h2database:h2-mvstore:2.5.252`): a roughly 362 KiB, pure-Java artifact with
no transitive dependencies. It provides indexed disk maps without a SQL engine
or native SQLite libraries. Its page-cache setting does not cap total process
memory: dirty pages, decoded values, map metadata, record preparation and output
queues must also be bounded and measured.

Select it with `truncate.buffer.storage=disk`. The memory profile remains the
default while local/native performance qualification continues. Both backends
run the same transaction, replay, real-dispatcher and captured native lifecycle
test bodies. A separate child JVM exercises a 128 MiB transaction and its prepared
output with a 64 MiB heap. This is a local working-storage result, not evidence
that every native file count or deployment mode is now supported.

Use a fixed number of indexed maps rather than one map per transaction. Spill
payloads and bookkeeping, including pending before-images, savepoints, lifecycle
windows and prepared output. Avoid copying an entire committed batch into a
second in-memory list. Preserve preparation failure before publication and the
separate scan R, replay floor F and completed-output W positions.

Use explicit typed encoding; do not deserialize arbitrary Java objects. Own row
values and preserve byte arrays, timestamp nanoseconds, decimals, source identity
and schema information. An I/O error, exhausted disk budget or malformed stored
value must stop processing without advancing the safe checkpoint. Cleanup must
only remove the current task's owned scratch files.

Storage alone does not prove schema transitions, safe migration, receiver
retention, remote journal identities or equivalent runtime acknowledgements.
Keep their correctness checks until the corresponding implementation and tests
replace them. Do not label the connector fully compatible while those gaps remain.

## Local storage validation, November 11, 2026

The full local Maven verification passed 3,978 tests (229 parser and 3,749
connector cases). After tightening ownership of mutable schema defaults, 971
affected cases passed again. Plugin assembly and formatter/import validation
also passed. These counts include the same behavior contracts exercised with
both memory and disk backends, not duplicated test implementations.

Coverage includes fresh-instance restart with empty scratch storage, interrupted
batch delivery, captured native commit/rollback/savepoint lifecycles, actual-task
durable acknowledgement, idle reader handoff, cleanup after delivery failure,
and disk exhaustion during preparation with zero output and restart from the
same offset. An I/O cause inside a storage failure is non-retriable; the existing
journal transport retry behavior is preserved.

The constrained-heap child process buffers 4,096 independent 32 KiB row payloads
(128 MiB total), prepares their output, verifies order and releases the batch
with a 64 MiB maximum heap. This covers working storage, not arbitrary schema
inventory size or a complete Connect worker's heap.

An additional local comparison uses the same buffering/preparation/read workload
with a 512 MiB heap. Three fresh-process runs per case produced these median
times; startup is included and cleanup, IBM i I/O, Kafka and downstream work are
excluded:

| Workload | Memory | Disk |
| --- | ---: | ---: |
| 10,000 rows, 64-byte payload each | 73 ms | 578 ms |
| 4,096 rows, 32 KiB payload each | 228 ms | 3,052 ms |

Disk runs used a 2 MiB page cache and 256 KiB dirty-page threshold. Disk storage
costs CPU and I/O; it is not a throughput optimization for small transactions.
The benefit is retaining work larger than the heap without evicting uncommitted
changes. At that stage memory remained the default. Subsequent adversarial
validation found that journal-wide activity on excluded tables exhausted its
small defaults. The default is now disk storage, and excluded rows no longer
retain individual output entries. The timings above describe the earlier
storage experiment, not a new performance measurement of that correction.

## Subsequent scaling and performance evidence

The [native performance matrix](dcg-validation.md#repeated-native-performance-comparison--2026-11-11)
now covers two runs each of the original connector, the current disabled path,
and enabled memory/disk paths. Disabled batch medians differ by less than 0.5%;
the enabled profiles complete those finite batches sooner. Disk restart is
7.4% slower than enabled memory. These measurements do not establish sustained
capacity or unlimited transaction/catalog size.

The subsequent [continuous-load comparison](dcg-validation.md#repeated-comparison-after-interval-connection-reuse)
found a 32% p95 latency regression in the enabled profiles and then verified its
removal after reusing a qualified connection within each native read interval.
At approximately 201 operations/second, the new p95 was 3.69 seconds in memory
mode and 3.71 seconds in disk mode versus 5.20 seconds for the repeated original.
The disabled path stayed at 5.21 seconds. CPU per row remained 5.9% above the
original for enabled memory and 23.4% above it for disk. Every expected operation
and cleanup check passed. This is bounded offered-load evidence, not proof of
equal maximum throughput or multi-hour behavior.

Paged raw audits, receiver inventories, binding history and handoff working
history have removed independent fixed total-count ceilings. The transaction
and lifecycle budgets still prevent uncontrolled resource use; disk mode makes
large working sets independent of payload heap size. Native scope evidence
remains 56 files. Catalog metadata remains proportional to catalog size.

Explicit legacy character-code substitutions and header converter configuration
are now supported and pinned in recovery history. The native comparison passed
17 checks with an explicit 37-to-500 text mapping and JSON header converter,
including restart, commit/rollback and exact legacy/new schemas and values.
The [recorded compatibility evidence](dcg-validation.md#explicit-character-conversion-and-header-configuration--2026-11-11)
also records the preceding failed run, the bounded metadata recollection fix,
and the limits of this qualification.

Basic schema refresh remains a compatibility target. General reconstruction of
historical schema versions is not provided by the original connector and is not
a requirement for this feature. Clearing the original reader's current schema
cache does not reconstruct older layouts after a restart. The following native
experiment is retained as optional research, not a release gate:
IBM's [QDBRPLAY API](https://www.ibm.com/docs/en/i/7.6.0?topic=q-replay-database-operation-qdbrplay)
reconstructed three historical ALTER TABLE layouts in an empty shadow file.
The same sequence passed twice in separate command jobs using only their QTEMP
objects, after the original source fixture had been removed. An unmapped target
was rejected without changing the shadow layout. This is evidence about the API,
not schema-transition support in the connector.

The temporary reconstruction covers adding an integer column, dropping it, and
adding a text column. A further capture matched their native record-format IDs
and decoded selected autocommit rows with the acquired epoch bindings. It does
not establish capture-key changes, dependency isolation for arbitrary DDL,
pointer-backed large entries, transactional DDL or complete runtime recovery
across these epochs. A QTEMP primary-key
constraint was rejected by IBM; capture keys cannot be inferred from that shadow
file. Implementing that broader guarantee would require schema epochs tied to
retained positions and durable history before affected output. That work is
deferred. Current guards against interpreting rows with an unproven layout remain
in force. See the
[schema replay evidence and remaining gates](dcg-validation.md#native-schema-reconstruction-feasibility--2026-11-11).

### Reuse ordinary metadata acquisition; defer a reconstruction library

A dedicated IBM i working library is not a requirement of this change. The
shadow-file experiment remains feasibility evidence, not the selected design.
Truncate does not itself change column definitions or capture keys. General
historical DDL reconstruction exceeds the original connector's guarantees and is
outside the agreed compatibility scope. It is not a prerequisite for truncate or
ordinary restart, and no working-library decision is pending.

Ordinary streaming and native binding acquisition now share
`JdbcFileDecoder.readColumnMetadata`. This preserves the original JDBC type
names, precision, scale, nullability and identity descriptors without extra
native metadata queries. Native acquisition still independently validates the
complete physical layout, keys and source identity before persisting a binding.
Recovery still reads that binding from history and never replaces it with the
current catalog definition.

The ordinary refresh also invalidates cached CCSIDs and character widths for
both system and SQL table aliases. Previously a schema cache clear left these
values behind, so the rebuilt decoder could retain the old encoding or width.
Native-to-SQL name cache entries are now keyed by library as well as file name;
refreshing one file does not invalidate an unrelated library's mapping. Cached
row decoding adds no native queries. Metadata is reacquired on a refresh.

The compatibility boundary remains explicit:

| Scenario | Original connector | Transactional path after this change |
| --- | --- | --- |
| Rows and clears with an unchanged definition | Uses the cached current definition | Uses the validated, persisted definition; no reconstruction objects |
| Ordinary schema refresh at a D/CG schema-change record | Clears and reloads current JDBC metadata | Validates one observed committed change, saves the new definition before output, and replays that boundary from retained history |
| Column encoding/width changes followed by ordinary refresh | Could retain stale auxiliary caches | Ordinary refresh invalidates those caches; native refresh independently checks physical identity and the complete change interval |
| Restart with old rows and an unchanged schema epoch | Reads current metadata as needed | Restores the saved physical and output schema without a current-catalog fallback |
| Several ALTER operations while capture is behind, including a change and reversal | No historical reconstruction; the latest definition can misdecode intermediate rows | Rejects the transition before using an unproven definition, including equal-width changes |
| Key-only constraint changes | D/DC and D/AC are not handled as schema refresh; automatic live key changes are not an established guarantee | Rejects unsupported key-only transitions; arbitrary historical key reconstruction is outside this compatibility target |

The captured equal-width ALTER/reversal regression demonstrates why endpoint
metadata equality is insufficient: the old integer decoder reads a newer float
value as a valid but incorrect integer. Both the transition rejection and the
unchanged durable checkpoint across restart remain tested. Removing that guard
would copy an existing weakness rather than establish compatibility safely.

Further historical reconstruction is deferred. Basic current-definition refresh
remains in scope because the original connector offers it. A refresh must not
silently substitute today's schema for buffered rows whose layout is unknown;
the existing rejection preserves that safety boundary without claiming broader
historical recovery support.


## Baseline audit and subsequent compatibility fixes, November 12, 2026

The baseline is actual behavior at `2e19ad27`, including a live run of its
preserved plugin. Inherited Debezium settings alone do not establish that a
feature works, and the old README's TODO list is not a complete capability map.
Historical DDL reconstruction, a shadow reconstruction library, remote journals
and failover are not added to this feature's completion criteria. The original
connector does not provide those guarantees.

| Capability | Original evidence | Current transactional path | Remaining work |
| --- | --- | --- | --- |
| Custom Debezium converters | Ordinary schema builder uses `CustomConverterRegistry` | Uses the same configured registry; converted keys/values survive history recovery and real Engine restart | Native qualification of additional converter implementations is application-specific |
| Transaction metadata | Legacy cycle IDs and transaction monitor | Online output supports metadata, preserves numeric cycle IDs, and defers handoff while any old metadata cycle remains open | Keep the existing incomplete-history and delivery checks |
| Standard transforms and predicates | Applied by Connect/Engine, including README's unwrap example | Actual unwrap/filter chains pass Engine and both Connect delivery contracts | Operators must preserve progress records and choose transforms that preserve truncate if downstream clear delivery is required |
| Operation filters | Core dispatcher honors `skipped.operations` | Online capture supports ordinary `c/u/d` filters while requiring `t` capture; recovery still processes all source operations | Version-7 histories permit output filter changes at restart; old histories retain strict compatibility |
| Initial snapshot and `no_data` startup | Actual task runs the ordinary snapshotter | Ordinary first start is integrated; its final offset must be durable before handoff | Native `initial` and `no_data` passed; local `initial_only` checks also prevent handoff or streaming after durable snapshot completion. Deleted offsets with retained history still reject |
| Signal-based incremental snapshots | Original plugin reread the four expected rows in an isolated native test | Uses the core chunk algorithm with durable chunk state, serialized signals and committed-clear deduplication | Packaged ordinary-offset and EOS runs passed handoff, interruption/restart and clear interleaving |
| Current-definition refresh | Original D/CG schema-change handling clears and reloads JDBC metadata | Observed committed column changes now persist and replay schema boundaries | Native add/drop, interrupted transaction, replacement and clear/reload passed; ambiguous historical layouts still reject |
| Capture selection and output changes on restart | Ordinary task accepts updated configuration | Version-8 history permits standard selection changes when complete schemas were retained | Local and packaged native restart coverage pass, including selecting a table whose transaction remains open. Identity-only bindings and older versions remain guarded |
| Error handling | Ordinary row CDC supports WARN/SKIP | Buffered ordinary rows preserve WARN/SKIP and restore transaction metadata after an intentionally skipped row | Decode/history failures and failed clear dispatch still stop; targeted real-dispatcher comparisons pass |
| Native storage scope | Original has no explicit ASP-1 check | Journal/receiver evidence accepts documented system/basic ASP numbers; independent ASPs remain rejected | Numeric native inventory and file/member guards accept system/basic pools 1–32. The available live test server has only ASP 1 |
| Connection retries | Ordinary task retries communication failures | The public native provider can reconstruct a fresh session after a recognized connection failure, following completed drain | Actual-task tests cover both acknowledgement contracts; invalid history, decode errors and uncertain cleanup still stop |

The incremental-snapshot baseline test first timed out because its assertion
expected `source.snapshot=incremental`. The original actually emitted the four
`op=r` records with `source.snapshot=false`. Independent inspection verified the
records and IDs; the failed harness result is retained, not relabeled as a clean
pass. The owned signal table and fixture library were removed and verified absent.
This is a real compatibility gap despite the old README's TODO.

### Output behavior and recovery

The native schema builder now uses the same custom-converter service as ordinary
streaming. Registration happens before native admission when needed; the actual
task reuses that service. New version-7 history permits converter reconfiguration
at restart; older generations retain their full policy. Missing implementations and sensitive custom settings reject before
native work. Existing generations without custom converters retain their previous
fingerprint. Tests compare ordinary/native keys and values and exercise real
Engine snapshot, restart and conversion failure before batch publication.

Online transaction metadata remains optional. Its transaction IDs use the same
numeric commit cycle as ordinary streaming. A handoff with metadata enabled waits
for old cycles, including excluded-table cycles, to finish while legacy CDC
continues. Local tests retain actual dispatcher counts/order and history replay.
Separate packaged native runs passed both ordinary offsets and Kafka source
transactions: commit/rollback for both clear subtypes, balanced BEGIN/END records,
clear/reload ordering, receiver rotation, replacement and missing-offset rejection.
Both fixture and local runtime cleanups passed.

Connect/Engine owns standard transform execution. Pure `RegexRouter` chains
retain exact predicted-destination checks. Other transforms use the unchanged
source partition and signed delivery offset to correlate acknowledged records;
changed source coordinates, foreign callbacks and incomplete prefixes reject.
The output payload may legitimately change. Binding history is never transformed.
Unknown data destinations are not guessed during topic preflight. The old
operator-window profile still rejects unknown destinations because it requires
proof that every initial destination is empty.

Actual `ExtractNewRecordState` and predicate/filter chains passed in Engine and
Kafka 4.3.1 with source transactions both enabled and disabled. This unwrap version
intentionally drops `op=t`; that is the SMT's behavior on every source connector.
Use a truncate-aware transformation or preserve the Debezium envelope when a
consumer needs the clear event. Successful filtering is processing, not evidence
that a sink received a truncate.

Recovery progress records must survive the transform chain. A retained failing
experiment filtered the heartbeat carrying the next checkpoint. Kafka writes
source offsets for producer-dispatched records; a filtered record alone does not
write that newer offset. The connector correctly withheld progress. The passing
experiments preserve control/heartbeat records and filter only data. No callback
or transform is allowed to fabricate a durable checkpoint.

Per-record admission now checks terminal stop and the actual runtime acknowledgement
capability. Static configuration and transform rules are validated before native
handles are created, rather than parsed again for each record. Reconfiguration
creates a new task and repeats admission. A capability change or terminal stop
still rejects publication.

### System and basic user storage pools

IBM documents [`QSYS.LIB`](https://www.ibm.com/docs/en/i/7.4.0?topic=systems-library-file-system-qsyslib)
as the namespace for the system and basic user ASPs. Its
[`ASP_INFO`](https://www.ibm.com/docs/ssw_ibm_i_74/rzajq/rzajqviewaspinfo.htm)
identifies ASPs 1–32 as system/basic and 33–255 as independent.
Journal and receiver numeric ASP checks now accept 1–32 while retaining
`JOURNAL_ASPGRP=*SYSBAS`, exact paths, object generations and receiver-chain checks.
Independent groups still require explicit path/session qualification.

Local tests cover journal/receiver pools that differ, the upper basic-pool limit,
and rejection of missing, fractional and independent-pool values. A read-only
native inventory found only ASP 1. The `WRKJRNA` outfile's `QJOASP` field is described
as **Library ASP device**, not a numeric pool. Inventory admission now checks
`JOURNAL_INFO.ASP_NUMBER` and `JOURNAL_ASPGRP` before and after collection; exact
file/member queries independently validate each numeric pool and object identity. No storage pool or server-wide
configuration was created or changed.

### Confirmed boundary of the original schema and snapshot behavior

A fresh isolated native capture of dropping and restoring a primary key emitted
D/DC and D/AC. The original `JournalEntryType` and streaming switch do not handle
those as schema refresh. General live key-change recovery is therefore not a
missing original guarantee. An observed column ALTER is different: it emits the
D/CG schema change that the original reader handles. The new path now validates
that complete interval, records its schema before publication, and can restart
with F before the change and W after it. It does not execute historical DDL.

The original streaming loop also never acknowledges the pause used by core
blocking-snapshot signals. Presence of that core action does not establish working
blocking snapshots in this connector. Incremental snapshots are a separate,
confirmed original capability and remain covered.

The original plugin was also run with a newly created table and a requested
`always` snapshot. Both worked, so both are compatibility requirements. The
transactional task now runs the ordinary snapshotter when its policy requests
another snapshot. It retains old F/W until delivery completes, persists partial
snapshot state, and rejects recovery with incompatible snapshot settings. Local
actual-task tests cover `always`, `configuration_based`, snapshot-only delivery,
interruption and fresh-task recovery. A packaged native run passed a 300-row
additional snapshot while another transaction held F, then restart and ordered
truncate/reload. This does not reconstruct historical DDL.

New-file discovery validates the native D/CT, D/JF, F/JM, D/MA and F/MC creation
sequence and independently acquires the new current schema. Creation history is
durable before publication. F remains before the opening record when another
table advances W partway through that sequence, so a replacement can replay it.
Only documented D/CT metadata is interpreted; IBM's opaque apply image is never
used as a schema. Pointer handles are released in the retrieving API job,
including entries deferred by the page budget. Failed release rejects the whole
response and ends only that captured job. Row-image completeness checks remain
unchanged. Captured-fixture tests cover identity mismatch, missing history,
partial creation and restart with both memory and disk buffering. Packaged
native runs passed discovery, worker replacement and later truncate/reload with
both Kafka source transactions and ordinary offsets; all fixture and runtime
cleanup checks passed.

Online output no longer requires every data topic to be pre-created. Connect
and the broker retain their ordinary topic-creation behavior; internal control
and recovery topics keep their ordering checks. A blank/omitted `database.dbname`
uses ordinary `CURRENT_SERVER` discovery, then pins that identity for subsequent
connections. Dotted topic prefixes are accepted within Kafka's topic-name limit.


### Native multi-member baseline

The original plugin was run against an owned two-member physical file. It emitted
rows from both members into the same table topic, including two different rows
with the same Kafka key, and emitted no member discriminator in `source`. A
requested full SQL snapshot returned only the first member. The fixture was
removed and verified absent. Thus the original has partial multi-member row
streaming, not complete member-aware snapshot or identity semantics.

A separate native control distinguishes SQL `TRUNCATE` from `CLRPFM`. SQL
`TRUNCATE ... REUSE STORAGE IMMEDIATE` succeeded on the one-member DDS file,
then was rejected with SQL7008 reason 10 after a second member was added. An
alias targeting that second member was rejected too; both members retained their
rows. `DELETE` through the alias and `CLRPFM MBR(SECOND)` affected only the second
member, while `CLRPFM MBR(*ALL)` emptied both. The initial rejected attempt and
the completed control both passed cleanup. This is evidence for the tested
traditional DDS file, not a claim about SQL-partitioned tables or every IBM i
release. Ordinary SQL reads of one member do not prove that SQL `TRUNCATE` is
supported on a multi-member file.

IBM documents the named-member and `*ALL` behavior in
[CLRPFM](https://www.ibm.com/docs/en/i/7.4.0?topic=c-clear-physical-file-member).
Mapping a one-member clear to a whole-table Debezium `op=t` is unsafe when several
members share the output topic: it could erase rows from untouched members.

The online runtime now retains every member's native JID and streams its rows
into the existing table topic and key shape. Before/after images use member and
producer identity internally, so equal RRNs in different members cannot pair.
Snapshots retain the original first-member behavior. Version-1 single-member
binding records remain byte-compatible; multi-member records use version 2.
A complete native F/JM, D/MA, F/MC sequence can add a member during streaming.
The retained physical-file schema is reused. After validating the complete
sequence, the new association is persisted at its opening, before even unrelated
output can advance W. F pins the full sequence until F/MC is processed. This lets
a fresh task recover if another member publishes between the association records.
Missing or inconsistent associations reject.

A captured member clear still requires an unambiguous single-member file. It
cannot become a table-wide event when the original output merges multiple
members. This preserves existing row identities and adds no member-aware key,
topic or snapshot contract. Excluded files retain their member identities and
transaction effects without emitting a truncate.

Native file inventory now requests WRKJRNA `DETAIL(*JRNFILE *JRNLIB)` and compares
independent file/member counts around discovery. `*OUTPUT` also includes access
paths, which can duplicate physical-file rows, and non-row resources; those are
not additional captured tables. Logical-file metadata is classified using the
documented RJNE0200 file-type flag, with matching object type and creation
attributes. Access-path start/end records use the distinct `*QDDSI` identity.
Neither classification supplies a physical row binding or relaxes physical
start/end-journaling checks.

Online startup also permits inherited worker key/value converters, standard
`errors.tolerance=none` and task-count defaults, producer tracing interceptors,
and ordinary producer partitioners. Kafka Connect owns those output services and
the connector still returns exactly one task even when `tasks.max` is larger.
Tolerated serialization failures and overrides of recovery-history processing
remain rejected; successful ordinary row processing is distinct from losing a
required truncate or durable progress record.

### Excluded external values and source physical files

The original packaged connector was run with excluded CLOB and source physical
files on the captured tables' journal. Selected ordinary rows continued through
insert/update/delete on both excluded files. The buffered path preserves this
selection behavior. `SYSFILES` distinguishes `DATA` and `SOURCE` physical files;
both require the same exact native object/member, format and image checks.
[IBM documents the source-file format](https://www.ibm.com/docs/en/i/7.6.0?topic=file-creating-source-without-dds)
as including sequence/date fields before the source text; these are not silently
removed from the pinned physical format.

Excluded LOB files retain identity-only history with an explicit native LOB
capability. Their bounded row descriptors, identities and null metadata are
validated; their transaction effects, before-image pairing, clear companions and
rollback controls still participate in F/W recovery. External values are never
followed. The API reader releases every returned pointer handle in the original
retrieving job before returning a page, including deferred entries; a release
failure prevents progress and cancels the job. Captured LOB values remain
unsupported, and changing a filter cannot promote identity-only history into an
output schema. The original `JdbcFileDecoder` did not implement CLOB capture;
`AS400Xml.toObject()` returned an empty string, not the XML document.

The continuous runtime must preserve the excluded LOB capability and the complete
member mapping when applying output selection. A live failure exposed a discarded
capability in that adapter; a new continuous-runtime/fresh-history regression
covers it alongside component replay and native publication tests. See the
[validation report](dcg-validation.md) for successful and failed evidence.
