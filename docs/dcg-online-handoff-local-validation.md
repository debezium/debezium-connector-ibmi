# Captured online handoff through local Kafka Connect

This test-only continuation joins the [native census capture](dcg-online-census-native-validation.md)
and [local publication barrier](dcg-online-boundary-local-validation.md) using the
same source bytes, acquired table bindings and receiver coordinates. It does not
connect to IBM i, enable migration or change the normal connector task. The F/CR
branch and the earlier barrier, ownership, recovery and stricter tests are preserved.

## Candidate initialization without a closed anchor

`As400OnlineHandoff` accepts a bounded retained history, the observed census and
its journal bracket, pinned schemas, capture policy and an exact publication cut.
Complete inventory/terminal visibility and immutable historical bindings remain
explicit `CONTROLLED_FIXTURE_ASSUMPTION` inputs. A digest detects changed evidence;
it does not prove completeness of an arbitrary source journal.

The adapter finds the original starts for the inventory/bracket cycles, reconstructs
the cycles still open at the cut, and rejects missing starts or dirty selected-table
prefixes. A dirty prefix means the legacy task has already exposed part of an
unfinished transaction, including a skipped clear. A future rollback could invalidate
that output; a migration checkpoint cannot repair it. Rollback/savepoint/identity
checks stay in the real strict decoder, normalizer, lifecycle and recovery coordinator.

F comes from a retained start and the reconstructed lifecycle dependencies. W comes
from an actual completed transaction or autocommit operation at or before the cut.
The adapter does not manufacture `F-1`, call `afterVerifiedBoundary`, or relabel an
ordinary lagging offset or arbitrary scan R as W. It validates earlier retained
context before choosing F, rejecting unresolved rows or unsupported transitions.
Candidate preparation emits no records and writes no offsets or binding history.
A fresh coordinator reloads the candidate and reconstructs its context. A cut
with no remaining open cycles can also qualify; W still comes from an actual
completed operation, not the scan position.

## Publication and restart experiment

`As400OnlineHandoffProbeConnector` is a test connector. It uses the actual
`As400ConnectorTask` polling/acknowledgement/stop methods, coordinator execution,
legacy `As400RpcConnection.getJournalEntries` loop, streaming handler and dispatcher.
`RetrieveJournal` responses are controlled objects serving the captured headers and
bytes. The legacy phase uses the real legacy JDBC row decoder with its schema cache
populated from the acquired layouts; any attempted JDBC access throws. The buffered
phase uses the strict normalizer, transaction buffer, replay coordinator and native
schemas. No connection or raw native pagination occurs in this experiment.

Only the ordinary legacy starting offset is seeded using Connect's public stopped
connector API. The test task publishes an ordered legacy barrier after processing
the exact captured cut and preserving its actual offset timestamp/receiver fields.
Under the pinned Connect EOS profile, that marker commits all preceding legacy
output. The subsequent migration marker carries pure version-2 F/W. Buffered output
waits for acknowledgement; each complete buffered batch has its own ordered
checkpoint marker and Kafka transaction. A marker being enqueued is not treated
as durable. Independent broker consumers read committed output and offsets, with
uncommitted reads confirming that interrupted prefixes reached Kafka.

A replacement loads whichever offset Kafka actually retained: ordinary legacy,
acknowledged legacy barrier, or buffered F/W. The controller does not install the
candidate F/W. Missing offsets reject, as do foreign/malformed barriers, changed
capture policy/bindings, missing required anchors and damaged evidence. Connect
provides ownership and producer fencing. There is no custom owner registry or
receipt publication gate. Control files only inject bounded fault/poll timing.

The experiment holds these boundaries: partial legacy output before the barrier;
Kafka barrier commit before its callback; migration checkpoint commit before its
callback; partial truncate/reload output; and suspended-worker reassignment with
later return of the old worker. A missing-offset restart is tested after completion.
Read-committed consumption, connector-defined transaction boundaries, one task and
one partition per topic are premises. No cross-topic global order is inferred.
The probe combines data into one test topic and compares keys, values and original
receiver/sequence coordinates. IBM i's existing source envelope has no table field;
this run does not independently verify ordinary per-table topic routing.

## Retained native fixture and limits

The local run uses the cleaned-up DCNC0913 census capture, its independent SQL-ledger
reconciliation and original raw hashes. The standard `table.include.list` selects
A/B/M. U remains in transaction context and retains the old silent cycle, despite
being excluded from output. No public per-table truncate option is added.
The chosen cut is native sequence 83, with open cycles 46 and 83. Candidate F=46 and
W=82 are derived from the data; R=83 is retained separately as diagnostic evidence.
The legacy prefix includes the actual insert/rollback compensation before that cut.
Historical legacy output is not retroactively rewritten by buffered replay.

The capture has real file/member JIDs, schemas, job/thread metadata and receiver
coordinates. It did **not** establish native journal/receiver incarnations or the
platform's concurrent-inventory completeness guarantee. Therefore this experiment
uses explicitly named capture-scoped fixture tokens for those incarnations.
It does not pass them off as native identity evidence. The read-only digest-pinned
capture bundle is stable fixture history reloaded at every task startup, not a new
production recovery store or proof of Kafka binding-history initialization.

Transaction metadata is disabled for this transition proof. Native record delivery
uses the acquired schemas; the independent Python expected-output ledger is limited
to this fixture's single INTEGER primary-key rows. Other types, general schema
changes, source access, receiver-discovery races and historical binding acquisition
are outside this local run. Prior native and mixed-column evidence remains separate.
The source workload has already finished; replay timing is not live throughput or
proof of continued writer progress during an actual migration.

## Reproduction

The ordinary unit suite includes `As400OnlineHandoffTest`, using the repository's
redacted native transaction fixture. It needs no source credentials or Kafka.
The manual integration needs JDK 21, a local Unix-socket Docker daemon and an audited
census capture from `tools/dcg-online-census-probe.py` with a successful offline
writer-ledger reconciliation. Run:

```sh
mise exec java@temurin-21 -- tools/dcg-online-handoff-probe \
  NEW_EVIDENCE_DIRECTORY CAPTURE_DIRECTORY AUDITED_REPLAY_JSON CUT_SEQUENCE
```

For the retained DCNC0913 run, `CUT_SEQUENCE` is 83. The exporter pins the capture,
bindings and prior audit, verifies every raw hash and copies original bytes into
the read-only plugin fixture bundle. Original capture files are never rewritten.
An optional `DCG_HANDOFF_SCENARIOS` selects a comma-separated subset of the named
fault cases; the default runs all five. Subset runs cannot qualify the full matrix.
Every attempt retains staged plugin hashes, the exact Python harness, independently
consumed output/offsets, worker/broker logs, assertions and owned-resource cleanup.

## Local results, 2026-11-09

The complete five-case matrix passed with the pinned Apache Kafka 4.3.1 image in
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-combined-handoff-20261109/kafka-06/`.
Each replacement produced exactly the expected 426 committed data records,
including two truncates and one legacy delete tombstone, with zero snapshots.
Final F=W=785 was exclusive after the retained context resolved. The transition
itself persisted inclusive F=46/W=82, distinct from legacy cut R=83.
The missing-offset restart rejected without additional output or a new checkpoint.

`report.json` and the saved committed/uncommitted broker observations cover the
five fault boundaries. The returning old source producer logged
`InvalidProducerEpochException`; its return changed neither committed output nor
F/W. `broker-audit.json` additionally checks the independent expected ledger,
all 2,125 non-tombstone source timestamps, original receiver coordinates, final
offsets and the old source producer's epoch rejection. These checks do not depend
on the worker's in-memory checkpoint being durable. Owned Docker cleanup passed.

The full Maven verify/assembly build, formatter/import checks and 2,525 Java tests
passed, including 15 new candidate-handoff cases. The existing 38 census and four
fixture Python tests passed. The new launcher passed shell syntax validation;
the Python harness passed parsing and its full integration run. The staged 319
class files match the current build; the distributed connector JAR excludes the
new test adapter. `preservation.json` records unchanged production/pre-existing
files, HEAD/index and all protected worktrees, including F/CR.

Development attempts `kafka-01` through `kafka-05` are retained as failed evidence
with successful cleanup. They caught test wiring errors: using the strict forward
decoder for a legacy rollback compensation record, constructing the dispatcher
on the wrong thread, a transient REST status 404 after connector creation, blank
capture-policy fields, and a checker expecting a nonexistent source table field.
The final run uses the real legacy decoder, owns the buffered dispatcher on its
coordinator thread, bounds the creation-status wait, fingerprints explicit empty
configuration values and checks the actual source envelope. No production code
or existing safety assertion was changed to obtain the passing result.

Advisor inspected the actual files and retained broker evidence after verification
and passed with no actionable issue. Its conclusion is limited to this conditional
local handoff; native admission remains unproved. The review result is retained
as `advisor-review.json` beside the build and preservation evidence.

## Remaining admission gates

The [native evidence follow-up](dcg-native-handoff-evidence.md) adds actual
identity-observation support and binding acquisition provenance to the test path,
and narrows the documentation gap using IBM's commit/rollback ordering rules.
Its live validation is pending; it does not retroactively replace the fixture
identities or qualify native admission in the `kafka-06` result above.

This work validates a conditional local handoff, not safe automatic admission for
arbitrary existing connectors. The native persistent-visibility and terminal-coverage
contract still needs authoritative support or another documented mechanism. Complete
retained historical schemas, real journal/receiver incarnations and native legacy
pagination/continuation must be bound to the same live barrier. The combined path
then needs a fresh isolated native run with writers continuing and receiver rotation.
A declined migration must not indefinitely stop ordinary CDC; that normal-task
policy is not enabled or implemented by this test connector.

The [deferred handoff continuation](dcg-deferred-handoff-validation.md) adds durable
legacy progress while a selected prefix is unfinished, including replacement after
the progress offset commits but before its callback. The original five fault cases
remain the default matrix. Automatic source admission remains disabled.
