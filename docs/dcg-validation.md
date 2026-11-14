# Validation coverage

Use the [testing guide](dcg-contributor-testing.md) to reproduce checks. Tests
exercise different layers; a fixture test does not establish native IBM i
semantics, and a passing native scenario does not qualify every workload.

## Regression coverage

| Behavior | Maintained checks |
| --- | --- |
| Row layouts, pointer authority, journal parsing and pagination | Parser tests and sanitized raw fixtures in `journal-parsing/src/test` |
| File/member identity, historical names, schema and capture selection | Binding, lifecycle and transactional DDL tests in `debezium-connector-ibmi/src/test` |
| Commit, full rollback, savepoints and interleaved transactions | Transaction buffer and replay-coordinator tests, in memory and disk modes |
| Recovery after incomplete delivery | Fresh-instance F/W replay, immutable history, missing/changed offsets, malformed evidence and foreign-identity controls |
| Snapshots, output and shutdown | Actual task/coordinator/dispatcher and Engine tests; snapshot markers, acknowledgements, interrupted batches, stop/admission and drain |
| Native test safety | Python tests for explicit configuration, write opt-in, credential redaction, fixture ownership, bounded operations and cleanup |
| Kafka Connect ownership | Local `dcg-ownership-probe`: acknowledgement, missing-offset rejection, interrupted delivery, fencing and reassignment |
| Packaged connector integration | Public native probe: real snapshot/streaming offsets, online handoff, clear outcomes, unrelated progress, receiver rotation and replacement |

The source transaction, history, identity and failure assertions are required
regressions. Test helpers stage controlled inputs or observe results; they must
not silently substitute synthetic startup proof for native qualification.
The retained local Kafka `partition` case intentionally reproduces a failing
timeout profile; the corrected timeout-profile runner is a separate test.

## Recorded native evidence

These are bounded observations on an isolated IBM i 7.5 system. SQL TRUNCATE
produced D/CG subtype 8 and optimized DELETE produced subtype 3 in the tested
transaction contexts. Both can roll back without individual row-delete records.
Outside commitment control, tested clears produced F/CR. SQL spelling alone
does not establish the journal subtype or transaction outcome.

The latest performance, library-move, larger-scope and endurance batch used:

- Source: `96a395a9c4dd64ce474895d3ce339cbea6d864c1`.
- Plugin ZIP SHA-256:
  `c8a28302ff5b8bebf54d7922c40dba9e937842d9dacfaa01d7b3d1727a96cd80`.
- Preflight: all 203 production sources and 15 packaged JARs matched the frozen
  manifest. Subsequent documentation and test-harness changes did not change
  those production sources.

| Scenario | Result | What it establishes |
| --- | --- | --- |
| Captured library moves | Four configurations, 148 assertions | Both/source/destination selection with ordinary offsets and both-library capture with Kafka source transactions; exact topics, keys, values, clear/reload order, rollback, replacement and move-back |
| Matched performance | Six repeated runs at about 201 changes/s plus ten reader/storage/delay controls; 912,300 verified changes | Bounded latency and CPU comparisons with exact output; [methods and results](dcg-performance-comparison.md) |
| Larger journal scope | 128 journaled files, three captured tables, 12 assertions | Busy-lock release, unrelated progress, keyless clear/rollback and worker replacement; not throughput from 128 captured tables |
| Restart endurance | 20 minutes, 92,401 observed events | Three task restarts, three receiver rotations, 5,000-row commit/rollback and unrelated output during an open clear |
| Continuous endurance | 3,605.4 seconds without a task restart; 116,851 total events | 99,000 timed changes in 660 serial-burst rounds; exact count, duplicate, rollback and clear/reload checks |

The library-move batch exercised SQL TRUNCATE (subtype 8); it does not add
subtype-3 coverage. The larger-scope handoff completed 437.4 seconds after a
deliberate blocker was released. This is total qualification time, not the
duration of application write locks.

During the uninterrupted hour, quarter-median whole-worker RSS was 454.4, 451.8,
452.6 and 453.7 MiB; timed peak RSS was 495.0 MiB. All 120 storage samples showed
the same 3.125 MiB H2 working file. Thirty-second samples can miss transient
peaks, and file size is not live payload size. JFR covered the full hour: 329
collections, 1,062.5 ms total GC pause time, and 44.5–57.5 MiB heap after GC,
ending at 45.5 MiB. These results do not establish multi-day leak freedom,
saturation capacity or very large pending-transaction capacity.

Earlier native runs cover ordinary offsets, Engine/file history, the Server
JVM/HTTP profile, mixed columns and excluded resources, DDL, historical names,
member lifecycles, and both clear subtypes. Those runs used earlier artifacts;
they are not reruns of every scenario on the ZIP above. The
[runtime guide](dcg-compatibility-baseline.md) and
[compatibility matrix](dcg-compatibility-comparison.md) state the supported
contracts and remaining boundaries.

### Failed attempts and corrections

Failed attempts remain evidence rather than successful measurements:

- One library-move setup named a not-yet-created table in the legacy include
  filter. Another required a controller resume before its workload. The runner
  now uses the correct setup and resume sequence.
- The first restart-endurance attempt hit the offset inspector's 1,000-record
  limit. A bounded latest-value inspector for offsets fixed the harness; event
  inspection still retains every record. A fresh full run passed.
- The first 128-file attempt exceeded the controller's six-minute observation
  deadline. A fresh run passed with a 15-minute deadline and unchanged connector
  behavior and safety assertions.
- A large-batch unit test used a 128 MiB spill quota instead of the configured
  disk profile. The fixture now uses the configured profile; deliberate
  small-budget exhaustion controls remain.

These corrections changed test setup, not connector resource limits. Independent
cleanup checks covered failed and successful native attempts and found their
owned libraries, journals, inquiry messages and local Docker resources absent.

## Revalidating a change

Run the affected regression tests and relevant integration profile. Record the
source revision, distribution/dependency hashes, JDK/runtime versions, settings,
workload, assertions and cleanup outcome. Report fresh native results only for
the artifact actually run. Upstream CI status belongs in the build system and
code review, not in this static coverage document.

For performance, keep offered and achieved rates separate, measure delivery
latency independently from throughput, and disclose shared-host activity.
Retain the original failure when a corrected rerun succeeds. A short test is
not a maximum-capacity or deployment-readiness claim.

Sanitized repository fixtures replace system, user, job and object names,
including identities embedded in binary payloads, while preserving producer
relationships and section offsets. Inspect raw captures before sharing: native
journal contents, logs and JVM profiles can contain private data.
