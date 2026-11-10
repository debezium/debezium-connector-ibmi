# Configured native D/CG validation

This report covers the local `dbz-3025-dcg-followup` implementation on 2026-11-08.
The [configured profile and runbook](dcg-configured-native.md) defines its scope.
The distribution now supplies a configured native initialization/recovery provider
to the normal `As400RpcConnector` / `As400ConnectorTask`. The worker loads only
distribution jars; separate test controller and audit JVMs are not task providers.
The configured publication path uses standard Kafka Connect transactions, offsets
and fencing, without the stricter prototype's owner or receipt gates. At this
validation stage, unused stricter classes remained in the distribution. The
subsequent [code review and cleanup](dcg-code-review.md) moves those prototypes
to an optional test profile; they are absent from the current distribution.
This report's earlier artifact hashes remain historical evidence.

The mode remains disabled by default. Enabling it is an explicit operator action
for the declared profile, not a change to the legacy transactional-clear guard.
No source production system, existing auction fixture, F/CR branch, upstream PR,
deployment or remote branch was changed.

## Retained evidence

The local evidence root is:

`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-provider-20261108/`

`before.json`, `before.zip`, `turn.patch` and `final-audit.json` identify this
turn's changes independently of the branch's earlier uncommitted implementation.
`staged-classes.json`, `package-audit.json` and `artifact-hashes.json` identify
tested distributions and final artifacts. The final native smoke run also retains
`source-at-stage.zip`. Test reports are `tests.json` and `tests-summary.json`.
Fixture records, exact public worker/connector configuration, broker audit files,
and source cleanup inventories are retained under each experiment directory.
Credentials are redacted and supplied transiently through stdin/FileConfigProvider.

## Native distribution experiments

These use fresh isolated edgeqa libraries and digest-pinned local Kafka/Connect
4.3.1, with five captured tables and single-partition topics. Native schema
acquisition, source identities, transaction census, journal retrieval and
continuous handoff are real. Broker output is read independently using
`read_committed`.

| Evidence | Result and scope |
| --- | --- |
| `packaged-02`, DCNC7602 | Initial packaged normal-task smoke: baseline, commit/rollback, restart, unrelated progress, rotation and drain passed. Earlier scope settings are retained as historical evidence, not the final configuration. |
| `packaged-04`, DCNC7604 | Killed first start after committed snapshot output. Replacement rejected pending initialization offsets, emitted no additional records and installed no binding history. |
| `packaged-05`, DCNC7605 | 15,015 baseline rows with exact keys, 5,000-row reloads, truncate and optimized-delete commit/rollback, task death and fresh worker recovery, unrelated-table progress, 17 rapid receiver rotations, mixed-column update, missing-offset rejection and normal drain passed. |
| `packaged-06`, DCNC7606 | All 32 final configured startup/driver smoke checks passed, including mixed-column delete before-image and tombstone, ordinary recovery, both missing-offset admission modes, and cleanup. |

The large first-start window reached handoff in **142,885 ms** according to its
durable window status file. This includes the native validation, snapshot,
acknowledgement and handoff work after enrollment. It is a single fixture
observation, not a throughput benchmark or maximum required maintenance window.
The snapshot is chunked beyond the former 9,000-row ceiling; total row/byte/time
budgets remain explicit and bounded.

During the open clear on B, unrelated A/EXTRA events reached committed Kafka output
before the clear was resolved. Killing the worker did not allow B's uncommitted
clear to escape. A replacement reconstructed from durable F/W and binding history
without a new initialization window or duplicated baseline. Commit published the
clear before reload; rollback withheld the clear/reload. Optimized DELETE tests
used the existing job-local fast-delete override and verified its removal.

Mixed native values cover exact decimals, CCSID text, binary data, nullable
columns and 64-bit integers. The reference consumer clears on `op=t`, then applies
keyed row changes in table-partition order. Its final **key sets** match all native
fixture tables. This is not a full-value comparison of every row/type and does
not certify an external sink implementation. Mixed update/delete assertions
independently check the actual before/after values and tombstone.

Every completed native experiment, including the two failed attempts below,
retains successful exact-owned fixture cleanup: no schema/library or pending
fixture deletion inquiry remains. Local worker/broker containers and networks
are also removed. See each `report.json`, native inventory and cleanup files;
cleanup is not inferred from task shutdown.

## Failures preserved and corrected

- `packaged-01`, DCNC7601: the harness installed dependency jars as separate
  isolated plugin roots. Plugin discovery failed before native task operation.
  The harness now installs the distribution together under `plugin/ibmi/`.
- `packaged-03`, DCNC7603: a rapid receiver attachment changed between metadata
  queries. Valid metadata was classified as a permanent contradiction. The
  collector now rechecks current attachment and classifies only a positively
  observed concurrent change as a transient cut. The complete unverified cut is
  discarded. After three raced attempts, the configured runtime yields with
  unchanged R/F/W and no output. Static contradictions still fail. The 17-rotation
  repeat passed; the original strict retry-exhaustion test is preserved.
- Failed JDBC setup followed by failed JDBC close previously could look like
  completed drain. The final build records that cleanup failure and rejects a
  successful drain report. A dedicated regression covers both failures together.
  This final error-path change follows native `packaged-06` staging; the exact
  source difference is retained in `post-native-main.patch`. It does
  not change the successful native path exercised by that run.

Other final hardening includes explicit native driver construction under plugin
isolation, bounded connection validation, terminal admission for JDBC metadata
handles, rejection of unsupported output/capture settings, and rejecting a new
operator plan over existing output/history when source offsets are missing.
The final admission also rejects custom producer serializers, partitioners,
transactional IDs and history interceptors. Together with the failed-cleanup fix,
these are the two main classes changed after final native staging. Their exact
delta is retained and regression tested; the successfully exercised native
configuration and capture path are unchanged.

## Local recovery and fencing checks

- Full Maven verification and assembly/package with formatter/import validation
  pass. Exact suite counts and logs are in `tests-summary.json`, `verify-final.log`
  and `package-final.log`. No live integration-test suite is claimed: the native
  checks above use the dedicated bounded harness.
- Bounded replay tests reconstruct before W across multiple prefixes and verify
  F/W anchors independently. Scan R is never saved as the restart checkpoint.
- A 5,000-row streaming batch is interrupted after committed output. Fresh task
  reconstruction replays the clear and complete batch; W advances only after the
  final acknowledgement. At most 256 live acknowledgements are retained. Callback
  delivery without matching durable readback cannot start the next chunk.
- Receiver-order tests cover 1,000 rotations without lifetime metadata growth,
  retain required F/W and initialization anchor identities, and reject changed
  overlapping identities before modifying the verified order.
- `ownership/` passed the ordinary acknowledgement, interrupted/mixed batch and
  broker-fencing assertions before the **preserved original catch-up failure** in
  its partition scenario. Independent post-failure broker evidence confirms
  unchanged committed output/checkpoint. It is a diagnostic failure, not a pass.
- `aligned-partition/` passed all 17 assertions with the qualified timeout profile,
  including legitimate reassignment back to the original worker, conservative
  checkpoints and no custom publication authority. This supplements the previously
  retained repeated timeout matrix, not a replacement for its evidence.
- `offsets/` passed all 12 assertions for missing, malformed and incompatible
  durable offsets, with zero new output or checkpoint replacement.

The native worker-kill cases and local broker/network faults prove different
boundaries. They do not claim a native IBM i network partition occurred in the
packaged large run. Source delivery remains **whole-batch at-least-once**. Kafka
transactions/fencing do not make the IBM source transaction and Kafka one atomic
transaction, and the connector honestly reports source exactly-once unsupported.

## Source-connector release boundary

The configured candidate is runnable and locally/native-fixture validated against
the source contract: native journal decoding, commit/rollback, keyless truncate
output, per-topic ordering, snapshot acknowledgement, durable F/W, fresh-instance
recovery, unrelated-table progress, and stop/drain. The follow-up
[record-number validation](dcg-rrn-validation.md) extends key support for files
without declared SQL primary keys.

Connector qualification uses local Kafka Connect and fresh isolated IBM i
fixtures. It requires neither production EKS access nor an AWS emulator. The
connector implementation has no AWS dependency. Downstream sink configuration,
cloud deployment inspection and migration of an existing ASI installation are
separate integration concerns; they are not source-connector release gates.
The optional local sink experiment is supplementary evidence only.

The supported source profile still requires capture of every file on the
dedicated journal, fixed supported schemas, a single controlled library and an
operator first-start window. Existing legacy offsets cannot be migrated
implicitly. Unsupported configurations remain rejected. Kafka broker durability,
transport security and sizing remain deployment responsibilities; local evidence
is not an availability SLA for every broker topology. Backup restore, automatic
receiver release and arbitrary DDL are outside the support contract.

The earlier configured-native review passed after actual-file inspection of the
then-current implementation and evidence (`dcg-native-provider-20261108/advisor-review.json`).
It did not rerun native tests. Its 2,616-test total applies to that earlier revision;
the record-number validation document records the subsequent checks and review.
Nothing has been pushed or deployed.
