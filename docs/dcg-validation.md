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
[archived report](https://github.com/gardnmi/debezium-connector-ibmi/blob/fc4ffaa2aa7bf8acc5d9630d493450391814f8c0/docs/dcg-public-online-validation.md)
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
showed no material slowdown in the exercised workload; they do not establish
statistical equivalence, saturation capacity, many-table performance or multi-day
leak freedom. Private raw captures and JVM profiles are not repository fixtures
and must not be published without inspection/redaction.

## Limits and release checks

- Transactional capture remains off by default and qualified only for the
  documented native layouts, journal/receiver settings and Connect profile.
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

## Historical evidence

Superseded design proposals, investigation diaries and per-stage reports were
removed from the PR's current tree to keep the review focused. Their complete,
immutable contents remain at the
[pre-cleanup development commit](https://github.com/gardnmi/debezium-connector-ibmi/tree/fc4ffaa2aa7bf8acc5d9630d493450391814f8c0/docs).
This includes native API research, the timeout matrix, first-start profiles and
failed experiments. Links to individual archived reports deliberately pin that
commit. The abandoned custom-owner/receipt prototype runner and its unused SQL
fixture are also retained in Git history; they are not build prerequisites or
additional guarantees of the current runtime.
