# D/CG bounded first-start delivery design

Status: proposed, 2026-11-08. This document changes no runtime behavior or support
admission. Normal transactional streaming remains disabled. F/CR PR #114 is
separate. No new Kafka or native experiment was run to prepare this design.

The subsequent [implementation and validation](dcg-bounded-snapshot-validation.md)
records the work against this proposal; the original design review is retained
below separately from implementation evidence.

## Recommendation and evidence

Add an explicitly admitted, versioned first-start path that reads and delivers
one bounded snapshot chunk at a time. Retire each chunk's acknowledgement state
after its Kafka barrier and independent offset readback. Keep every chunk offset
incomplete; only the final baseline receipt, durable binding history and initial
F/W allow ordinary recovery. Reduce native checks around individual rows by
validating native read chunks and actual publication/commit boundaries instead.

The [completed profiles](dcg-startup-performance.md) select the next work:

| Measured item, matched 1,500-row run | Baseline | Final existing candidate |
| --- | ---: | ---: |
| Whole operator window | 288,210 ms | 244,974 ms |
| Native snapshot preparation, inclusive | 38,678.6 ms | 30,807.5 ms |
| Materialized to first poll return | 55,747 ms | 17,681 ms |
| First poll return to baseline acknowledgement | 109,465 ms | 110,789 ms |
| Baseline acknowledgement to initial acknowledgement | 47,800 ms | 46,140 ms |
| Initial acknowledgement to native handoff | 13,521 ms | 13,847 ms |
| Native `isValid(2)` calls | 12,004 | 7,513 |

The final candidate still performs 3,004 held-session checks at enqueue (two per
row/barrier), 1,502 at record acknowledgement, and a native check for each row
read. It performs 734 nonempty publication checks and 734 advancing-commit checks.
The configured 128-record poll maximum therefore did not mean full polls. Removing
enqueue round trips should help fill polls, but that is a hypothesis to measure.

The first change already removed native probes from owned-envelope construction
and idle commits. Do not redo those optimizations or remove the handoff mutex.
The next opportunities are native row reading, chunk enqueue and acknowledgement
staging. Keep the expensive final census, schema/identity, exact boundary and
history rechecks: these timings provide no evidence they are redundant.

These are one matched pair of controlled-source runs, not scaling forecasts.
Inclusive timings overlap and the wall intervals omit enrollment work; do not sum
them as a phase decomposition. Publication time includes validation and callbacks,
not only broker work. The auxiliary drain-profiler correction and its lack of a
live rerun remain disclosed in the profiling report.

Retained evidence:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-startup-performance-20261108/`:
`validation-summary.json`, `native-call-table.json`, per-generation
`startup-profile.json`, broker readbacks, cleanup records and `advisor-review.json`.

## Preserve the source boundary and recovery contract

Snapshot cursor open/close can append journal entries. Do **not** move B before
the SELECTs, treat an intermediate chunk as a journal position, or reopen a source
cursor after B. The current `As400NativeInitialization.Session` deliberately
requires source preparation before B and no deferred source reads in final
baseline acknowledgement. Preserve that ordering.

The new preparation phase streams *pending* output while scanning. It returns
only after every captured table reached genuine EOF, all cursors/statements
closed successfully, every chunk was durably acknowledged, and the same qualified
source session is still held. The initializer then selects B, performs its empty
census, and publishes the final BASELINE barrier. Thus no row requires a journal
coordinate before B exists. Existing snapshot records already use pending
initialization offsets and acquired native schemas, rather than F/W.

This changes the preparation contract from “hold all rows before any output” to
“finish all source reads and pending chunk deliveries before B.” It is an explicit
versioned protocol change, not a reinterpretation of the existing list interface.
Under continuous qualified exclusion the captured data state cannot change
throughout that interval; the completed baseline represents that state at B even
if snapshot cursor bookkeeping appended entries. Final checks still reject any
observable boundary, binding or census change after B. A quiet journal or a
successful liveness probe alone does not establish this exclusion.

The complete sequence is:

1. Require absent offsets, fresh dedicated output/history, durable single-use
   enrollment, the immutable operator plan and a separately qualified held source
   session. Establish complete journal scope, empty census and acquired bindings.
2. For each captured table, scan bounded owned chunks under that session. Validate,
   enqueue and durably acknowledge each CHUNK barrier before reading the next
   chunk. Keep bounded table-completion counts and a rolling receipt summary.
3. Observe EOF for all tables, close all snapshot handles, verify warnings and
   session continuity, and return the completion summary. No source cursor remains.
4. Select and verify B using the existing strict native boundary reader, recheck
   census/session, and durably acknowledge BASELINE with the complete summary and
   B in its pending offset. This is the final baseline receipt.
5. Preserve the existing full native recheck, history write/readback and second
   full native recheck. Publish INITIAL with the existing F=W=B exclusive offset;
   await exact durable readback and checkpoint acceptance.
6. Recover matching initialized history, construct the continuous reader, and
   complete the existing serialized handoff before ending source exclusion.

No initial F/W exists before step 5. Scan R belongs to the continuous reader and
remains separate; a chunk ordinal or EOF marker is neither R, F nor W. Ordinary
restart from valid F/W/history never selects the chunk reader or opens a new
operator window. Unrelated committed tables must still progress during an open
clear after handoff. First-start exclusion still covers **all** journal writers,
including uncaptured resources; chunking does not shorten that scope.

## Bounded scan and delivery contract

Introduce a synchronous internal chunk-source contract alongside the existing
list provider, initially available only through explicit test admission. Proposed
operations are `scanBaseline(epoch, definitions, limits, chunkConsumer)` and a
bounded completion result. The native implementation owns its cursor, projection,
codec conversion, warnings, statement and cancellation registration. The consumer
receives an immutable owned chunk and returns only after durable retirement. It
must never receive a ResultSet, lazy value, JDBC locator or driver-owned buffer.

Use acquired schemas and complete existing row/key conversion. Scan each table
once on the same held connection; no OFFSET pagination, inferred integer-only
key, reconnect or parallel source scan. Deterministic ordering must use the
qualified key metadata; unsupported ordering/schema profiles reject admission.
Never transfer a cursor to a replacement or resume a partially read table.

Separate four bounds in a new immutable plan version:

| Bound | Initial test candidate | Meaning |
| --- | --- | --- |
| Chunk rows / owned bytes | 256 rows / 1,000,000 bytes | Whichever is reached first; no automatic growth |
| Single owned row | Existing 65,536 bytes | Reject oversized/unsupported values before queueing that chunk |
| In-flight delivery | One chunk plus one CHUNK barrier | Keep the existing 10,000-entry ledger ceiling; do not raise it |
| Total attempt | Explicit finite row/byte budgets and existing deadlines | Independent of chunk size; count with checked long arithmetic |

Start qualification with 12,000-row and then 30,000-row fixtures, with explicit
total budgets matching each fixture. Those are experiment sizes, not supported
deployment maxima. Keep the old plan version's defaults and 9,000-row/16 MB
materialization bounds unchanged; do not silently reinterpret `snapshot.rows` or
`snapshot.bytes` as per-chunk limits. New chunk and total budget fields must be
covered by the plan's immutable digest and enrollment. Unknown versions or
unsupported provider/limit combinations fail before source I/O. Total byte budgets
may exceed 16 MB only in the new explicitly admitted protocol.

At most one extra owned row may be held as byte-limit lookahead. Reject any row
that cannot fit an empty chunk; carry a valid lookahead into the next chunk without
refetching or dropping it. Before B there must be no lookahead or unread row.
An empty fetch or `setMaxRows`-limited result is not EOF. Remove the old per-table
limit-as-completion possibility: use explicit over-limit detection, actual cursor
EOF and warnings, and fail on truncation/uncertain completion. Do not count rows
up front as the proof of a complete scan.

Keep only one chunk's owned values, envelopes and acknowledgement references,
bounded driver fetch buffers, one lookahead, and O(captured tables) completion
state (the current plan allows at most 100 tables). No list of all rows, all
SourceRecords, all chunk receipts or all primary keys may accumulate. Release
chunk objects only after durable retirement; do not prefetch while it awaits Kafka.
Queue limits must accommodate the candidate chunk/barrier or apply ordinary
bounded backpressure. Include duplicate owned/envelope copies, queue/producer
buffers and schema overhead in memory measurements: a payload bound is not an
exact JVM heap cap. JDBC fetch size alone is not proof of bounded driver memory;
verify the pinned driver's behavior and allocation profile before native acceptance.

A chunk may span several Connect polls; one poll need not equal one chunk. Keep
one source partition and the existing qualified EOS/poll, one-partition-per-topic,
no-transforms/no-tolerated-errors profile. Await the exact chunk barrier before
admitting another chunk. The next table may start only after the previous table's
last chunk is retired and its cursor closed. Empty tables produce explicit EOF
completion, not fake data; an entirely empty baseline still has a BASELINE barrier.

## Pending offsets and bounded receipts

Give every chunk a monotonically increasing ordinal within the enrollment epoch.
Use a new pending-offset version containing epoch, phase, ordinal and bounded
summary identity. Distinguish data offsets from the chunk-complete barrier offset.
Neither is accepted by the existing F/W decoder. Exact map equality on durable
readback must distinguish adjacent chunks; the existing shared `snapshot` phase
is insufficient. Never infer success merely from an equal row count or the most
recent Kafka callback.

For each chunk: register exact SourceRecord identities; require all ordered record
metadata, the qualified successful commit and exact durable barrier offset;
then retire the entire prefix through that barrier. Reject missing, duplicate,
foreign, reordered-to-skip-prefix, wrong-topic/partition or post-stop callbacks.
Offset readback failure must stop the attempt before retirement or another chunk.

After retirement, fold the epoch, ordinal, table identity, row/byte counts and
barrier Kafka receipt into a versioned, length-prefixed rolling SHA-256 summary.
Keep only the current digest and bounded per-table counts/EOF flags. This is an
audit link between retired chunks, not an independent durability authority or a
substitute for checking native values. It cannot resume an incomplete attempt.
The final summary must cover the acquired binding-set identity, the exact expected
table set with each table's counts/EOF, the retired ordinal/count totals and the
rolling receipt digest. Reject duplicate/missing table completion or totals that
do not equal the checked chunk accumulators; use a versioned canonical encoding.

BASELINE's new pending offset binds epoch, complete chunk count, total rows/bytes,
the bounded completion-summary digest and exact native B/identities. Require all
expected table EOF flags before generating it. Retain the expanded summary in
test evidence. Keep the existing history `Initialization(epoch, baselineReceipt)`
format: its receipt is the Kafka coordinate of this final BASELINE barrier, not
the last CHUNK receipt or the rolling digest. INITIAL and ordinary F/W/history
formats need no change. Historical completed baselines remain readable.

After partial chunk publication, consumers can observe committed snapshot prefixes.
There is no whole-snapshot atomicity or automatic rollback of emitted rows. The
dedicated generation remains incomplete and cannot be promoted by this protocol;
sink activation is a separate obligation. EOF, queue acceptance, chunk commit or
window status must not masquerade as complete startup. Late total-budget failure
may leave a committed prefix in the new path, unlike the old materialize-first
bound failure. Explicit quarantine and partial-start rejection are mandatory.

## Native checks at safe boundaries

Apply the reduction only to the explicitly admitted chunk path. Keep the existing
list provider, normal runtime enqueue path and stricter tests. A held-session
check includes the provider's actual epoch/scope contract; `isValid` alone is not
a lock, census, maintenance or ownership proof. No TTL or grace period is added.

| Boundary | Required validation and effect |
| --- | --- |
| Before starting each native chunk and after owning its values | Full same-session/epoch validation. Per-row local stop, deadline, revocation, profile, type, size and count checks remain. No native result is exposed until the post-read validation and warning checks pass. |
| Before and after enqueueing an owned chunk | Full held-session validation. Individual registration/enqueue uses local guards, including after a blocked queue wait. Only qualified owned values may be queued. |
| Every actual nonempty task poll return | Retain full native publication check. Releasing test-retained output must revalidate again. A prior chunk check cannot authorize later publication. |
| Every snapshot record callback | Validate local admission, exact registered record identity and Kafka metadata; stage acknowledgement only. No native round trip or durable promotion at this step in the new path. |
| Every commit with staged snapshot receipts | Full native validation, serialized with initialization/handoff, before acknowledgement promotion. An empty commit retains its existing local-only behavior and handoff mutex. |
| Every CHUNK/BASELINE/INITIAL barrier wait | Bounded wait, full held-session validation and independent exact offset readback before retirement/progression. Keep strict BASELINE/INITIAL callbacks and finalization checks. |
| EOF, B selection, history and handoff | Preserve complete census, scope, schema/key/identity, receiver/boundary and retention checks at their existing stages, plus complete-scan proof. No optimization based on profile duration. |

The existing qualified Kafka 4.3.1 worker commits its producer transaction before
calling the individual record callbacks and then the task commit. Staging receipts
locally is therefore allowed only as internal evidence awaiting the fully checked
task commit; it is not a new pre-broker-commit hook. If native validation fails in
that callback series, output/offsets may already be committed. Stop and quarantine;
do not claim Kafka rollback or zero output. Before INITIAL, those offsets remain
incomplete. After a complete INITIAL is independently durable with matching
history, ordinary validated recovery remains possible despite a failed callback
or missing HANDED_OFF status. Preserve that existing distinction.

Each barrier wait uses the smaller of its existing 120-second cap and the remaining
operator/native budget, with local stop/revocation/deadline checks on wakeup. It
must not reset the total-attempt deadline for each chunk. Retain the existing
30-minute maximum plan and the native test provider's independent 20-minute
lifetime; neither is a guarantee that in-flight transport has drained by expiry.

Implementation must bind the optimized callback path to registered initialization
phase/record identities, not a caller-supplied header or an untrusted offset field.
Normal CDC, BASELINE and INITIAL retain the existing conservative callback path.
Stop and handoff synchronization must cover local callbacks too. A late callback
must not revive a stopped tracker or change a replacement's state.

Do not hold a native-session mutex while waiting for queue space or Kafka: the
task thread must be able to perform publication/commit validation. Serialize
connection/cursor operations with the native probes, release that serialization
before calling the chunk consumer, and keep stop/cancellation independent. Qualify
validation on the same connection while a result set is open; do not assume the
old all-materialized test proved this new lifetime. No replacement connection or
implicit autocommit is allowed. An interrupted fetch, ambiguous close, JDBC warning,
lost epoch or failed cancellation prevents completion; stop request is not drain.
Include the existing query timeout in the open-cursor experiment: Kafka waits
must not silently extend a native query limit or turn a timeout into EOF. Failure
to qualify this cursor lifetime blocks the native adapter; the local chunk tests
alone cannot justify removing that check.

The expected call structure is O(chunks + actual nonempty polls + fixed final
checks), rather than additional probes for each fetched/enqueued/acknowledged row.
One-record polls can still make poll checks scale with row count. Measure poll
occupancy and retain those checks; do not achieve a call target by weakening them.

## Implementation order

1. Add the test-admitted chunk contract, versioned plan/budgets, pending offsets,
   complete-scan summary and single-chunk consumer. Reuse existing acknowledgement
   prefix retirement. Preserve the list path and all native check frequency first.
   Use controlled source fixtures to deliver more than 9,000 rows through the
   actual task; prove bounded ledger/state and all restart failure boundaries.
2. Add explicit chunk enqueue and acknowledgement-staging admission to the real
   task integration, with deterministic native-call counters and loss/stop races.
   Only then reduce per-row native checks in the new path. Retain publication,
   advancing-commit, finalization and ordinary-restart checks.
3. Implement the native scanner and cancellation ownership in the existing test
   provider, then exercise real local Kafka and isolated edgeqa sequentially.
   Preserve old harness modes and assertions, and collect profiles plus independent
   broker/native cleanup evidence. Do not enable the normal streaming guard.

Primary implementation touchpoints: `As400TaskInitialization`,
`As400PlannedStartWindow` / `As400PlannedTaskInitialization`,
`As400NativeInitialization.Session`, `As400RecoverySource` and its acknowledgement
tracker; test providers `As400NativeTaskSession` / `As400PlannedNativeProbeConnector`,
the actual-task fixtures and `tools/dcg-start-window` / `dcg-planned-start-probe`.
`As400ConnectorTask` publication and handoff semantics must remain intact. Do not
change transaction buffering, F/W codecs, binding-history recovery or F/CR to solve
snapshot sizing. Keep the original stricter tests, not rewritten weaker versions.

## Required acceptance and failure tests

All following cases are **planned**, not passed by the existing profiles.

| ID | Experiment | Required result |
| --- | --- | --- |
| S1 | 9,000, 9,001, 12,000 and 30,000 owned rows through the actual task; mixed keys/types, multiple tables, empty table and all-empty baseline | Exact keys/values/counts; one chunk plus barrier in ledger, bounded lookahead and no retained state growing with total rows; old list limits still enforced. |
| S2 | Exact byte/row boundary, skewed 64 KB rows, too-large value, count/byte overflow, total limit at chunk boundary and midway through next chunk | No drop/refetch/duplicate; incomplete chunk withheld; earlier committed prefix stays pending, no initialized history/F/W; replacement produces zero additional output. |
| S3 | False/truncated EOF, warning during fetch/close, missing/duplicate table completion, cursor open/close appending native entries | No false completion; B selected only after genuine EOF and all closes; cursor bookkeeping is before B; any post-B boundary change rejects. |
| S4 | Chunk split across polls, repeated empty commits, delayed/missing/duplicate/wrong-topic callbacks, stale previous-chunk offset and readback error | Exact ordered prefix and current barrier required; no retirement or next chunk on uncertainty; retired prefix is no longer retained. |
| S5 | Native session loss before/after fetch, during enqueue/backpressure, before queued or retained poll return, and between record callbacks and commit | No unqualified chunk exposure; poll rejection before return; callback failure cannot promote receipts; already broker-committed prefixes are acknowledged as incomplete, not erased. |
| S6 | Revocation, deadline, stop or old task held in fetch/poll/ack/close while replacement starts | No newly admitted native work; late callbacks rejected after terminal stop; cancellation and drain distinguished; replacement rejects partial enrollment and cannot share old cursor/state; Kafka fences stale publication. |
| S7 | Fresh-process crash before first chunk, during Kafka transaction, after chunk commit before/after retirement, after final EOF, around BASELINE/history/INITIAL/handoff | Zero replacement output for every incomplete generation; complete matching durable F/W/history resumes without a new window. Retain independent broker records/offsets/history after old worker termination. |
| S8 | Null/deleted/corrupt/pending offset with prior enrollment/output/history; changed plan/epoch/scope; missing or changed history | Reject before resumed publication; never infer a fresh generation, synthetic W=1 or resumable snapshot cursor from missing offsets. |
| S9 | Changed file/member identity, schema/key/codec, census authority, journal scope, receiver or recording options at each existing final recheck | Existing fail-closed checks still fire; no initial F/W on failed finalization; history already written before a later failed recheck cannot alone authorize recovery. |
| S10 | Halt inside source close or handoff while empty/record/commit callbacks race | Existing handoff serialization and terminal admission preserved; no false drain/HANDED_OFF; no callback sees half-released source exclusion. |
| S11 | Real Kafka qualified profile: delayed acks, split polls, worker death/reassignment, held old output and fencing; stricter harness unchanged | CHUNK durability includes independent read_committed output/offset readback; ledger bounded across >10,000 total records; stale worker cannot publish after replacement. |
| S12 | Native mixed-column fixtures, held open cursor across Kafka waits and probes; injected interruption; allocations/driver fetch instrumentation | Owned values remain intact; same native session/locks; no hidden full-result buffering or cursor invalidation; native source handles drain before verified fixture cleanup. |
| S13 | Completed first start, remove active plan, leave a clear open, deliver unrelated commits, restart before clear commit/rollback | No new operator pause; unrelated CDC advances; conservative F/W replays correctly, completed output is suppressed and clear precedes reload. |
| S14 | Matched 1,500-row before/after; then bounded larger runs, normal and slow broker, 1/128-record poll profiles | Preserve correctness; report whole-window time, pause deadline, poll sizes, call counts by boundary and peak retained state. No speed claim from method sums or a changed fixture/profile. |

Local deterministic tests can prove state transitions, memory ownership, bounds,
callback/readback rejection and cancellation admission. Real local Kafka adds
broker transaction/offset visibility and fencing evidence. Neither proves IBM i
cursor/lock/driver lifetime, native cancellation or pause duration. Those require
the bounded native cases, raw journal captures, same-session evidence and cleanup.
First retain the 1,500-row control, then attempt 12,000 and 30,000 only within the
existing operator/native lifetime budgets. If the larger case expires, record a
failed sizing experiment and verify quarantine/cleanup; do not extend deadlines
or weaken checks to obtain a pass.

No hard maximum pause, supported auction size, sink convergence, arbitrary shared
journal qualification or public enablement follows from this design. The operator
still controls first start and required receiver retention; interrupted generation
recovery remains explicit. Existing stronger ownership/receipt/retention-release
tests stay separate and unchanged.

## Design review

Advisor inspected the actual design files, runtime contracts and retained startup
profiles and reported no actionable issues. This is design review only: chunk
behavior and native cursor/memory/cancellation acceptance remain unvalidated.
Local document/link and file-hash checks passed; runtime tests were not rerun for
this documentation-only change. Review, turn-only patch and preservation audit are
retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-bounded-snapshot-design-20261108/`.
