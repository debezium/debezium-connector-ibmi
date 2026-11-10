# D/CG native row-key qualification

This work qualifies the IBM i **source connector** independently of downstream
sinks and cloud infrastructure. Tests run in local Kafka Connect and fresh
isolated edgeqa libraries only. No production EKS access is required or used.
The legacy transactional guard and disabled-by-default D/CG setting remain.
F/CR PR #114 stays separate.

## Implementation

Explicit `dcg.key.mode=rrn` supports native single-member files without a declared
SQL primary key. Snapshot and journal output share the native `__IBMI_RRN` INT64
key. The physical schema and decoder remain unchanged; the additional output
field is derived only after complete native row decoding. Binding history saves
the key policy, and the capture fingerprint prevents changing it on restart.
Primary-key mode remains the default, with its existing rejection of missing keys.

## Acceptance checks

- Missing SQL primary keys are accepted only with explicit RRN mode; corrupt key
  metadata and output-name collisions reject.
- Snapshot keys come from native RRN, including gaps. Null, zero, negative,
  duplicate or reordered snapshot record numbers cannot complete initialization.
- Journal insert/update/delete projection preserves row identity; a clear remains
  keyless, and subsequent slot reuse is interpreted in journal order.
- Schema-history round-trip retains RRN mode and every physical codec. Owned output
  values retain exact bytes, decimal scale, nullable columns and 64-bit integers.
- F/RG reorganization rejects without output/checkpoint progress, including on a
  fresh restart. This is failure coverage, not qualification of reorganization.
- The native test-only harness `tools/dcg-rrn-probe` uses the distribution plugin
  with the ordinary task. Five fresh fixture files have their SQL primary keys
  removed before initialization. It checks actual snapshot/journal RRN values,
  deleted slots, identical row values, commit/rollback, worker death/replacement,
  receiver rotation, mixed update/delete, durable F/W and missing-offset rejection.
  Its independent Kafka consumer is an output oracle, not a sink plugin.

## Retained evidence

Evidence directory for this change:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-deployment-qualification-20261108/`.

The native source-only report, local test report, package provenance and
protected-worktree audit are recorded there. A successful native test requires
successful fixture cleanup as well as passing source checks. Failed attempts and
any limitations are retained alongside successful runs.

`rrn-source-03` passed all **35** source assertions against isolated library
**DCNC7707** using the distribution plugin in ordinary Kafka Connect. It verified
the five keyless files, exact snapshot keys, commit/rollback, recovery after worker
death, unrelated-table progress during an open clear, receiver rotation, mixed
values and update/delete before-images, native statistics requests, duplicate row
values with distinct RRNs, missing-offset rejection and completed drain. Final
`(RRN, ID)` projections matched independent native queries for all five files.
The comparison is not a full-value comparison of every final column; separate
mixed-column assertions check decimals, text, binary, nulls and 64-bit integers.

The first-start window reached handoff in **122,136 ms** for this 15-row baseline.
This is a single bounded smoke observation, not a throughput estimate or maximum
window. The earlier 15,015-row primary-key run is documented separately in
[configured validation](dcg-configured-validation.md).

Both native and local cleanup passed: the fixture schema/library and deletion
inquiries were absent, and local containers/networks were removed. Raw statistics
bytes are retained in `rrn-source-03/control/statistics-native.json`; the unchanged
F/CB record is also checked into the `native-transactions/statistics-request.json`
unit fixture. That component fixture's synthetic binding/checkpoint does not
replace the native run's acquired binding and durable offset evidence.

Final local `clean install -Passembly` passed **2,646 tests**, with formatter,
import and package validation. The final distribution's 205 main classes and
main source files match those staged for `rrn-source-03`. The protected F/CR and
original truncate worktrees match their pre-turn inventory. Independent Advisor
review inspected the actual target files, native fixture bytes and packaged
classes and passed with no actionable issues. It did not rerun tests or native
checks; broader workloads and long-running behavior remain unverified. The
verbatim result is retained in `advisor-review.json` beside the verification logs.

## Failed attempts retained

The first extended native RRN run stopped at an unrecognized lifecycle entry
before a mixed update could be acknowledged. The sequential reproduction
`rrn-source-02` identified it as **F/CB subtype C**, a statistics request generated
after the valid R/UB and R/UP pair. Its SQL journal capture is retained in
`rrn-source-02/native/failure-journal.json`. IBM documents that subtype in the
[object-level entry layout](https://www.ibm.com/docs/en/ssw_ibm_i_74/rzaki/rzakivarlength.htm).
The reader now admits only that validated statistics subtype; structural A/B/D,
unknown subtypes, incomplete payloads, conflicting identity and transaction flags
still reject. Regression tests prove that accepting it does not emit output or
advance durable F/W, including across a pending update and fresh restart.
A concurrent source-only run stopped on SQL0462 from native journal metadata
collection. All failed runs stopped rather than accepting uncertain evidence,
and their fixtures and local environments were removed successfully. Neither is
counted as a passing qualification. The follow-up runs are sequential and retain
a bounded current-receiver journal
capture before cleanup on failure. The lifecycle error now includes its journal
code/type; no unknown entry or metadata warning has been silently ignored.

A separate small native update reproduction emitted the expected R/UB and R/UP
pair and was cleaned up. It did not trigger the later statistics request and was
not treated as a passing replacement for the full sequence. The post-fix native
run retains actual RJNE0200 bytes as well as independent Kafka output evidence.

## Support boundary

This extends the [configured profile](dcg-configured-native.md); it does not
qualify arbitrary existing IBM i journals or deployment settings. Dedicated
journal/capture scope, fixed supported schemas, single-member files,
operator-managed retention, an operator-controlled first-start window and the
qualified Kafka Connect/output configuration still apply. Physical file
reorganization and unrecognized structural changes stop capture; they are not
silently accepted. Delivery remains whole-batch at-least-once. The mode is
disabled by default, and the legacy transactional-clear guard is unchanged.

Sink-specific behavior, ASI deployment settings and cloud infrastructure are not
source-connector release gates. Nothing was deployed or pushed, and F/CR PR #114
and the original truncate worktree were not changed.
