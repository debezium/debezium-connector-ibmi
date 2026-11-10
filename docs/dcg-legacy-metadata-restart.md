# Legacy metadata restart: local evidence and recovery boundary

The [metadata repair](dcg-legacy-transaction-metadata.md) fixes event emission
within a running legacy task. This follow-up tests what happens when a fresh task
loads the offset attached to an actual emitted record. It does **not** add a
production offset format or enable migration.

## Finding and implementation decision

Saving the current `TransactionContext` alongside the existing legacy offset is
unsafe. The offset points before the entry being emitted, while the monitor has
already counted that entry's change envelope. Restoring that pair and replaying
the entry increases the same event's transaction order a second time. Selecting
only the current transaction is also insufficient: other transactions can remain
open, including while the selected context is an inactive autocommit context.

Counters alone do not preserve row identity. An update's offset can point to its
already-processed before-image. Starting after that image loses the old key. In
the real dispatcher fixture, the original delete/tombstone/create sequence then
becomes a single update. This is an existing legacy restart limitation, exposed
by these tests, not a new behavior introduced by the metadata fix.

Do not introduce a second recovery protocol in the legacy reader to solve this
piecemeal. Prefer the existing D/CG recovery coordinator's retained replay floor
F, completed-output boundary W, binding history and complete replay of open
transactions and before-images. Keep the legacy migration proof's
metadata-disabled restriction until a separately tested transition accounts for
already-published BEGIN/END records and event orders. Disabling metadata during an
upgrade is not itself proof of safe migration and is not an automatic workaround.

The current buffered runtime already has fresh-instance and interrupted-output
tests in `As400OrdinaryCdcTest` and `As400IncrementalLifecycleTest`, including
pending before-images, interleaved completions and native captured batches. That
evidence applies to its validated F/W checkpoints. It cannot be retroactively
assigned to an ambiguous legacy offset.

## What the new tests actually do

`As400LegacyMetadataRestartTest` contains 11 cases. It shares the existing real
legacy streaming source, RPC advancement loop, dispatcher, record emitter and
transaction monitor fixture. Each replacement constructs a new source and
dispatcher. Journal entries and JDBC metadata are controlled; no IBM i connection
is made.

- A fresh task loading an actual row offset lacks the transaction start. The
  existing default fail-closed check reports the error, emits nothing and leaves
  the position unchanged.
- A fresh task loading an END record's offset and seeing only its commit cannot
  regenerate END. The existing unknown-commit behavior emits nothing and advances.
  This is an explicitly characterized recovery gap, not a passing safety claim.
- A deliberately naive snapshot of current counters reproduces double-counting
  after replay from the lagging row offset.
- Restoring all contexts from **before** the replayed row reproduces its original
  event order and final counts without another BEGIN. An empty open cycle must
  still be represented, and storing only the most recently selected cycle loses
  another interleaved transaction's END.
- Replaying END needs its context before commit dispatch, not the empty state
  after the original reader removed that context.
- Saving an autocommit record must retain other open contexts even though the
  selected inactive context has nothing to serialize.
- Each of the three key-change prefix cuts (delete, tombstone, create) exposes
  the same loss of before-image state. Replaying from before the image reconstructs
  the original old/new keys, output shape, source coordinates and transaction
  event orders. A previously delivered prefix is still duplicated; this alone is
  not exactly-once delivery.
- A controlled real dispatcher exception after the first key-change subrecord
  reaches the error handler with the journal still before the failing entry.
  A fresh fixture replaying the before-image and update reproduces the escaped
  delete and complete change group with the correct final event count.

Record comparisons retain keys, schemas, headers, before/after values, source
timestamps and coordinates, all transaction metadata and marker timestamps.
The three top-level processing-clock timestamps and the task run-ID header are
excluded from equality: they legitimately change on a fresh task. The row replay
case separately asserts that run IDs differ. Old/new-key headers remain checked,
and tombstones remain distinct records.

## Conditional experiment, not production recovery

The conditional successes explicitly inject transaction contexts into the fresh
test source. They use Debezium's `TransactionContext.store/load`, flattened into
test-only offset fields and round-tripped through Kafka Connect's schemaless
`JsonConverter`; `OffsetUtils.validateFormat` checks the primitive-valued map.
This shows that ordinary Connect offset values can represent the required
contexts. It does not establish when such a map is safe to publish.

The production loader does not restore those test fields. The experiment does
not persist a new context field in production records, add a custom owner or
receipt store, or validate a public format. It supplies the known pre-entry state
and retained before-image from the controlled fixture. It does not acquire them
from an arbitrary legacy offset or prove native receiver continuation semantics.

The prefix cases model possible delivery cuts; they do not assert a new Kafka
broker commit experiment. Even in the injected-failure case, an escaped record
means a record queued by the actual dispatcher, not independent broker proof of
durable delivery. Previous Kafka barrier evidence remains separate. No Kafka
broker or IBM i validation is claimed for this new test suite.

## Next gate for the truncate feature

Complete the existing online-boundary validation, rather than making legacy
metadata restart a hidden prerequisite for metadata-disabled migration. The next
native uncertainty is whether a bounded commitment-definition census plus a
journal bracket can account for every crossing transaction while writers keep
running. The concrete schedules, independent oracle and failure criteria remain
in [the native census/log plan](dcg-online-boundary-validation.md#concrete-validation-native-censuslog-race-capture).

That native gate must also establish retained starts, actual receiver pagination
and identity/history continuity before the existing local Kafka publication cut
can be composed with native recovery. Fixture injection and offset serialization
cannot answer those questions. A future metadata-enabled transition additionally
needs an explicit disposition for each already-visible metadata prefix. Neither
restriction is removed here, and application writers are not paused.

## Verification

Run the normal Maven unit suite or select
`As400LegacyMetadataRestartTest,As400LegacyTransactionMetadataTest,As400LegacyRowFailureTest`.
Evidence is retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-legacy-metadata-restart-20261108/`.
The initial targeted attempt compared processing-clock timestamps and failed for
that reason; its log is retained separately. A subsequent strengthened comparison
also identified the expected change in run-ID headers. Both development logs are
retained; final comparisons account for those expected differences explicitly.

The final full suite passed all 2,457 tests, including all 11 new restart cases.
Maven verify/assembly, package with formatter/import validation, and diff
whitespace checks passed. Hash comparisons confirm that every production source
and the protected F/CR, original truncate and D/CG worktrees are unchanged.

Production sources, migration/streaming admission and F/CR are unchanged. This
work does not push, publish or connect to IBM i.
