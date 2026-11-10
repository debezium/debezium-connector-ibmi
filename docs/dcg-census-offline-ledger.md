# Offline census capture and writer-ledger validation

Local-only work while edgeqa is unavailable. No IBM i connection, credential
retrieval, native mutation, production code change, migration admission, F/CR
change or publication is part of this step. The earlier sign-on failures remain
the latest native result; this is not a successful retry of that experiment.

## What the checker adds

`As400OnlineCensusReplay` is an explicit test entry point with one argument: an
existing census evidence directory. It reads the stored binding definitions,
baseline/final anchors and owned raw journal entries. It accepts only bounded
isolated-fixture evidence and the controller's single INTEGER ID primary-key
schema. It verifies entry hashes, summary coordinates/types/cycles, both exact
anchor bytes, native producer identities and monotonic sequences. It never reads
credentials or connects to a source. `replay.json` uses CREATE_NEW to preserve
earlier evidence rather than overwriting it.

The replay uses the existing `As400JournalLifecycle`, `As400SourceNormalizer`,
strict row decoder, transaction buffer and autocommit buffer. Journaling
transitions are validated before normalization, including their distinct allowed
flags and file/member identities. The existing conservative savepoint-target
consumption policy remains explicit; a later reference to a consumed marker
rejects. Missing starts, interrupted lifecycle sequences, mismatched identities,
unsupported records and open transaction state at the final capture reject.
This first pass collects output batches in memory for comparison. A second,
offline recovery pass now exercises the real dispatcher as described below;
neither pass acknowledges offsets through a durable store.

The native controller establishes its baseline before opening writer sessions.
The replay excludes exactly that baseline entry. To reuse the lifecycle validator,
it constructs an in-memory checkpoint with this actual fixture coordinate and
explicit placeholder incarnation/fingerprint values. Those values are only test
preconditions. They are not discovered source identities, historical binding
proof, an arbitrary legacy boundary, or a durable F/W checkpoint. The retained
older test fixtures likewise provide a known controlled starting context.

`audit_ledger` independently interprets the controller's narrowly allowed SQL
commands. For each writer job it checks every inserted ID and table, D/CG subtype,
transaction cycle continuity, commit versus rollback, and the exact ordered
changes in completed batches. It models the explicit SQL savepoints separately
from the native buffer. A missing whole transaction therefore fails even when
both journal captures omit it. Unknown SQL, failed writer commands, unexpected
journal work, changed producers, incomplete output and rollback output reject.
The unrelated autocommit writer is checked too. Queues are consumed in linear
time; these additions do not execute on the normal connector path.

IBM i emits internal savepoint controls in addition to explicit SQL SAVEPOINT
commands. The ledger checker does not invent a one-to-one mapping. The existing
lifecycle validator and buffer validate native targets, while the independent
SQL model checks their resulting output. Equivalent missing internal controls
are not ruled out merely by an equal final result. Consequently this remains a
controlled-workload consistency check, not proof that every native journal
record was captured or that concurrent transaction inventory is complete.

The controller now writes its preliminary transaction marker to fixture table M
for clear scenarios, leaving target B unchanged before its first clear. Retained
captures show an already-modified target can abandon optimized clearing and fall
back to individual deletes. The next native run must still establish the actual
subtype; unexpected fallback rejects this experiment rather than being relabeled
as a D/CG clear. The controller continues to keep unrelated writes on A moving.

## Local coverage and remaining gates

Retained native captures exercise clear commit/rollback, nested and non-top
savepoints, two-table clears, ordinary autocommit progress and receiver rotation.
The replayed changes reproduce the previously observed final rows for the seven
clear scenarios. Damaged local copies cover missing starts/outcomes, wrong JIDs,
missing threads, minimized rows, reordered entries, missing savepoint targets,
changed hashes/summaries, wrong stored bindings and altered anchors. The actual
offline main entry point is exercised without credentials, including refusal to
overwrite its output. Python cases independently test writer-log reconciliation,
including a whole cycle absent from both captures and wrong or partial output.

Passing these local checks does not establish live census visibility, terminal
visibility timing, receiver identity/retention history, or safe migration from a
lagging legacy offset. No new transaction-inventory checkpoint is produced.
Every report keeps `startup_proof=false`; a complete future fixture run remains
`captured-unqualified` even after the writer ledger matches. The offline recovery
pass now composes restart and interrupted delivery with a capture automatically.
New native census evidence and its composition with the actual Kafka publication
barrier remain required before migration could be considered.

Verification artifacts are under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-census-offline-ledger-20261109/`.
They retain the pre-change hashes, exact turn patch, local verification logs,
offline replay outputs from the retained fixtures, preservation checks and
Advisor result. No live access is needed to rerun the focused Java test or
`python3 -m unittest discover -s tools -p 'test_dcg_online_census_probe.py' -v`.

Local Maven verify, assembly and format/import validation passed with 2,488 Java
tests, including 24 new replay cases. The controller's 23 Python cases and four
existing fixture-manager cases passed. A separate local plumbing check consumed
Java's retained two-table rollback replay in the Python ledger checker and
rejected a missing row and a wrong outcome. Its expected commands are reconstructed
from that controlled historical fixture, not a contemporaneous writer ledger;
the upcoming native controller run must supply the latter independently.

For that earlier stage, Advisor inspected the actual repository files and passed:
no actionable issue found, with the offline consistency claims and their stated
limits supported by the saved evidence.

## Offline restart and interrupted-delivery extension

`As400OnlineCensusRecovery` is a test-source helper called by the same explicit
replay entry point. It creates fresh instances of `As400ReplayCoordinator`,
`As400BatchDispatcher` and `As400NativeTableSchemas` for each restart. Each instance
deserializes binding history again using the existing stored-table format; no
in-memory transaction buffer or output schema is reused. The retained native
fixtures cover multiple clears, savepoint rollback, transaction rollback,
ordinary CDC and receiver rotation. The census controller's current single-ID
fixture restriction remains; this extension does not claim mixed-column coverage.

The helper compares a complete reference run with interrupted runs. It tests
restart both after a modeled acknowledgement of the dispatcher's checkpoint
heartbeat and with only an earlier data-record offset saved. The heartbeat case
must reproduce the reference output without omission, repetition or reordering.
The earlier-offset case must replay every batch after saved W, the completed
output position. A failed enqueue is injected both as `false` and as an exception,
including partway through transaction metadata, rows and tombstones. F/W must
remain exactly as they were before delivery of the failed batch. A fresh instance
must replay that complete batch.
Already delivered portions of an unacknowledged batch can repeat, including its
truncate; this is a recovery consistency check, not an exactly-once claim.

Saved F/W maps are JSON-round-tripped. F is the earliest journal position needed
for reconstruction; W is completed output. Scan position R is reported separately
and cannot appear in their serialized offset. Missing offsets, changed capture
fingerprints and replaced receiver identities must reject with zero output. The
harness additionally checks that the available fixture contains both required
F/W anchors before replay. Its missing-anchor cases test that local precondition,
not native receiver retention or a newly implemented startup validator.

Comparison includes topic, Kafka partition, source partition, key/value schemas,
key/value contents (including source and transaction metadata), record timestamp
and headers. Only the instance-specific `__debezium.context.runId` header value is
normalized after checking it is a UUID. Fresh task instances intentionally have
different values. The processing clock, topic prefix and database configuration
are fixed test values; raw source coordinates and source timestamps are retained.
Source offsets are checked separately because a replay can safely carry an older
completion position on its records.

The experiment samples at most 12 raw-record restart boundaries and 12 output
failure boundaries per failure kind, evenly spaced including the first and last
candidates. Smaller sets are exhaustive. It rejects evidence exceeding the
existing capture bounds, caps total collected output at 40,000 records per run,
and checks a shared 60-second budget during replay and publication. Larger
captures are sampled, not exhaustively interrupted at every possible point.

Acknowledgements are modeled in memory. The helper neither starts Kafka nor
writes an offset store or custom receipt. The earlier real Kafka experiments
remain separate evidence. Placeholder fixture incarnation/fingerprint values
and known starting context remain test preconditions. Matching reference and
restart output establishes consistency under those preconditions; the separate
SQL writer-ledger comparison is still required to detect an omitted transaction.

The explicit main writes `recovery.status=verified-offline` only after all selected
cases pass and records the sampling and acknowledgement limits alongside the
results. A recovery failure rejects the whole replay. Existing `replay.json`
files are refused before starting the checks; create a separate evidence copy to
rerun an old capture. The helper is excluded from the normal plugin JAR and does
not change migration admission, F/CR, or production streaming behavior.

This extension's logs, per-fixture matrices, exact patch and preservation checks
are retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-census-recovery-20261109/`.

Local verification passed with 2,491 Java tests, including the 27 focused replay
and recovery cases, and the existing 23 controller plus four fixture-manager
Python cases. Maven verify, plugin assembly and format/import validation passed.
The four retained-capture matrix reports record 114 selected cases in total;
each restart case checks both modeled heartbeat acknowledgement and an earlier
data-record offset. These are local runs, with no new native or Kafka access.

Advisor inspected the actual files and this extension's exact diff after those
checks and passed, with no actionable issue found. Its review supports the
documented modeled-recovery claims; it does not qualify native census completeness
or a Kafka acknowledgement path. The saved preservation report confirms that
production sources, migration guards, repository HEAD/index and the protected
F/CR and earlier feature worktrees were unchanged by this extension.
