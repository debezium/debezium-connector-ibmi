# Bounded first-start implementation and validation

Later configured native work is recorded in [the configured profile](dcg-configured-native.md)
and [its validation report](dcg-configured-validation.md). Statements below about
test-only admission describe the historical stage covered by this document.

This implements the [bounded snapshot design](dcg-bounded-snapshot-design.md)
through the existing test-only initialization admission. Normal transactional
streaming remains disabled. No public provider or configuration enables it;
F/CR PR #114, ordinary F/W codecs and transaction buffering are unchanged.

## Implementation

`As400SnapshotChunks` validates one owned chunk at a time, exact table order,
row/byte budgets and explicit completed-table evidence. Each chunk has a unique
`chunks-1` pending data offset and a different chunk-complete barrier offset.
`As400RecoverySource` converts the complete chunk with acquired native schemas
before exposing its records, then waits for the existing ordered acknowledgements,
qualified commit and exact offset readback. Only then does it retire the prefix
and admit the next chunk. The ledger retains at most one chunk plus its barrier;
its original 10,000-entry protection remains unchanged.

Completed chunks contribute to a bounded, versioned receipt digest and per-table
counts. Every table must report EOF after its cursor closes. Only after the whole
scan returns does the existing native initializer select B and publish BASELINE.
The pending final baseline offset binds the completion summary and native B.
Full native rechecks, immutable history write/readback, INITIAL F/W and serialized
reader handoff retain their ordering. Chunk offsets never decode as F/W, and
retired prefixes cannot resume an interrupted snapshot.

The version-2 operator plan separates `chunk.rows`/`chunk.bytes` from total
`snapshot.rows`/`snapshot.bytes`. Chunk limits are at most 256 rows / 1,000,000
owned bytes, with the existing 65,536-byte single-row bound. Totals are explicitly
bounded positive longs with checked accumulation. Version 1 keeps its original
9,000-row/16 MB materialization limits; an unsupported provider rejects the new
plan. Both wall and monotonic deadlines, single-use enrollment, scope and retention
requirements remain. Barrier waits are capped by the smaller of 120 seconds and
the remaining window/native lifetime, and recheck local admission while waiting.

The new chunk path uses local checks for each enqueue and record callback.
Registration binds that optimization to exact SourceRecord identities; arbitrary
headers/offset fields cannot select it. Full native checks surround chunk
construction/enqueue, every nonempty task publication, advancing commits and
barrier completion. BASELINE/INITIAL, list-provider and normal CDC callbacks retain
the conservative path. Even empty callbacks remain serialized with handoff.

The test-only native scanner uses a forward-only, read-only cursor on the same
held connection, acquired column/key metadata and no pagination or reconnect.
It preserves at most one owned byte-limit lookahead, rejects JDBC warnings and
oversized/unsupported values, and observes genuine EOF with `setMaxRows(0)`.
Every cursor/statement closes before table completion and B. Connection probes
serialize with native cursor work; the mutex is released while waiting on Kafka.
Terminal stop can cancel the active statement through the existing guarded
connection. Cancellation failure cannot report successful drain.

## Driver evidence and limits

The retained JTOpen 21.0.4 source shows `AS400JDBCStatement` selecting a
`JDServerRowCache`, with fetch size or a configured block-byte size determining
the request bound. `JDServerRowCache.fetch` releases the prior reply and replaces
its current server-data block; this path does not accumulate the whole result.
Variable-field compression can make the byte buffer, rather than requested row
count, determine a fetch. Therefore `setFetchSize(256)` is not an exact driver
memory ceiling. Statement cancellation is callable while the statement is busy
and closes its result set. These pinned sources are retained with this turn's
evidence, alongside the actual staged driver hashes.

Native profiles record chunk rows/owned bytes, poll sizes and sampled used heap.
Heap samples include the JVM, Kafka, schemas and buffers; they are not live-set
proof or an exact bound on total heap. Controlled-source tests inspect the actual
acknowledgement ledger while delivering 30,000 rows, with a 257-record maximum.
The memory contract is bounded retained state independent of total source rows,
plus separately bounded driver/queue/producer buffers and captured schema state.

## Failure and compatibility contract

Already committed chunks can be visible as snapshot prefixes. A late total-budget,
native-session, query, close or acknowledgement failure leaves that generation
incomplete and quarantined; no initialized F/W is manufactured and a replacement
cannot emit another snapshot into it. Missing offsets do not authorize first start.
An independently durable complete INITIAL with matching history remains eligible
for ordinary validated recovery even if the old attempt did not record handoff.
This protocol does not retract Kafka records, promote a partial sink generation or
provide whole-snapshot atomic visibility.

The operator still holds every journal writer, DDL, maintenance and API producer
through first-start handoff. Chunking bounds memory and reduces probes; it does not
remove that pause. Ordinary restart does not reopen the operator window, and an
open clear during normal CDC still permits unrelated committed-table progress.
General shared-journal admission, public enablement, native version portability,
arbitrary schema/DDL and truncate-aware sink convergence remain separate gates.

## Evidence

Evidence root:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-bounded-snapshot-implementation-20261108/`.
It retains the pre-change archive/hashes, initial test-compilation diagnostic,
verification logs, operator CLI checks, pinned driver sources, native raw captures,
broker output/offset/history, per-generation profiles and independent cleanup.
The completed runs below exercise the real task/coordinator/dispatcher and local
Kafka with acquired native schemas. They do not close every permutation of the
S1-S14 qualification matrix.

### Completed checks (2026-11-08)

- `verify-budget-preflight.log`: 2,518 unit cases in 66 classes passed, including
  the existing stricter recovery/ownership cases. Reactor totals are 194 parser
  and 2,324 connector tests; `tests.json` deduplicates Surefire report variants.
- `package-budget-preflight.log`: assembly, formatter and import validation passed.
  `cli-tests.json`: five version-1/version-2 plan CLI checks passed.
- `kafka-regression/report.json`: all 29 unchanged local ownership/restart
  assertions passed with cleanup, including broker fencing and reassignment.
- Eight fresh edgeqa fixture libraries, DCNC7501 through DCNC7508, passed 58
  native/Kafka assertions. Every report independently confirms removal of its SQL
  schema and native library, no remaining fixture inquiries, and local cleanup.

| Run | Snapshot rows | Whole operator window | Native `isValid` calls | Peak owned chunk |
| --- | ---: | ---: | ---: | ---: |
| DCNC7501 | 1,500 | 108,549 ms | 120 | 256 rows / 19,712 bytes |
| DCNC7502 | 12,000 | 122,095 ms | 624 | 256 rows / 19,712 bytes |
| DCNC7504 | 30,000 | 134,510 ms | 1,482 | 256 rows / 19,712 bytes |
| DCNC7508, final build | 30,000 | 132,863 ms | 1,482 | 256 rows / 19,712 bytes |

These windows include native setup, scan, snapshot acknowledgement, immutable
history, durable initial offset and handoff; fixture loading is excluded. All four
success runs check every baseline key/value, native mixed columns, unrelated-table
CDC while a clear is open, removal of the active plan before restart, F/W recovery,
truncate-before-reload ordering and no duplicate snapshot. The 30,000-row final
control reruns the successful path on the exact final main/provider classes.

The matched 1,500-row prior profile took 244,974 ms with 7,513 native `isValid`
calls. The chunk run took 108,549 ms with 120 calls: a 55.7% shorter observed
whole window. This is one matched fixture/profile comparison, not a throughput or
pause-duration guarantee. Native timing sums overlap and must not be added.
Maximum sampled used heap was 120,717,680 / 123,131,848 / 126,278,072 /
129,597,496 bytes respectively; these samples do not prove retained heap bounds.

| Failure | Committed snapshot prefix | Required outcome observed |
| --- | ---: | --- |
| Window revoked | 256 rows | Quarantined; native drain; no initialized F/W/history; replacement adds no output |
| Worker killed | 128 rows | Independent post-death broker readback; no initialized F/W/history; replacement adds no output |
| 35-second window expires | 0 rows | Native cursor had opened; wait expires and drains; quarantine at 35,171 ms; replacement adds no output |
| 1,000-row total budget, 1,500 source rows | 1,000 rows | Stops at bound; no initialized F/W/history; replacement adds no output |

The crash run's last pre-death window timestamp is not a measured stop or crash
latency. No native hard drain deadline is asserted. `fault-results.json` and each
run's independent `read_committed` output/offset/history evidence retain the
partial-prefix results rather than describing them as zero-output failures.

### Acceptance coverage and remaining scope

| Design cases | Implemented/observed | Limit of this evidence |
| --- | --- | --- |
| S1-S3 | Actual-task 0/9,000/9,001/12,000/30,000 rows; exact keys/values, bounded ledger, byte lookahead, row/total failures, warning/fetch/close failures and explicit EOF | No exhaustive arbitrary-width/schema/driver allocation qualification |
| S4-S6 | Split polls, no commit, stale/wrong barrier readback, native loss before advancing commit, post-stop callback rejection, bounded waits and held-fetch cancellation while replacement progresses | Every possible native/network timing is not enumerated |
| S7-S10 | Partial-start/missing-offset rejection, early unsupported/mismatched-budget rejection, native crash/revocation/expiry/budget failures; existing finalization/history/handoff and identity checks retained | Not a new native crash run at every instruction or every metadata transition |
| S11 | Existing 29-assertion real Kafka ownership/fencing regression plus chunk runs with independent broker evidence | Chunk-specific held-old-producer fencing was not repeated at every chunk phase |
| S12 | Held native cursor crosses Kafka waits/probes; exact mixed-column values; pinned driver block-cache inspection; controlled held-fetch cancellation; successful native drain/cleanup | Heap samples are not allocation profiling; native stuck-fetch cancellation has no hard deadline |
| S13 | All four native successful runs preserve ordinary restart and unrelated-table progress while a clear is open | Sink convergence and broader workload qualification remain separate |
| S14 | Matched 1,500 comparison and bounded 12,000/30,000 runs, split polls, no-publication timeout, measured window/call/chunk bounds | No complete slow-broker or one-record-poll performance matrix; no unlimited-size claim |

### Provenance and support boundary

`final-audit.json` compares all tracked/untracked source files with `before.json`
and `before.zip`. Both protected worktrees and all branch heads are unchanged.
Normal admission/transactional guards, F/W codecs and transaction-buffer sources
are unchanged this turn. `turn.patch` is the implementation-only delta, including
new files, rather than the much larger accumulated branch diff.

The first seven native runs share the implemented main/native-provider classes
apart from a final constructor preflight improvement: mismatched plan/provider
budgets now reject before enrollment/native I/O. They all used matching budgets.
`planned-before-budget-preflight.java` preserves that earlier source, and
`staged-classes.json` enumerates the single differing main class. The final
30,000-row control matches all staged main/native-provider classes in the final
build. The archived first-run Python controller differs only in later failure-mode
handling; its exact hash is retained. `artifact-hashes.json` and
`package-audit.json` identify the built distribution and verify that test-only
native providers are not packaged as public functionality.

This completes the bounded snapshot implementation and its scoped validation,
not public transactional-streaming enablement. A supported journal-wide qualified
session provider, deployment/version qualification, broader schema/load and sink
validation are still required for general use. No push or F/CR PR change was made.
Independent `advisor_review` completed after final implementation, verification
and source/binary checks: **pass, no actionable issues**. Advisor inspected the
actual target files and retained evidence. It did not rerun tests or live checks;
general native qualification and hard heap/drain guarantees remain unverified.
The exact returned review is retained in `advisor-review.json`.
