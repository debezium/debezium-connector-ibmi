# Legacy transaction metadata repair

The local online-boundary experiment exposed a null transaction context when the
ordinary legacy reader dispatched an autocommit row with
`provide.transaction.metadata=true`. The preceding
[row failure fix](dcg-legacy-row-failure-validation.md) stopped that error from
being silently acknowledged. This follow-up repairs the metadata context and
transaction marker lifecycle, using Debezium's actual dispatcher and transaction
monitor.

## Behavior

Each offset owns a reusable inactive `TransactionContext`. Selecting autocommit
uses that context without ending a different, interleaved transaction. A genuine
autocommit row has commit cycle zero: it carries no transaction block and does
not manufacture BEGIN/END markers. This includes key-changing updates and
nontransactional F/CR clears; their delete/tombstone/create or keyless truncate
records retain their existing wire behavior.

For an observed C/SC start, the reader selects that cycle's context before asking
the monitor to publish BEGIN. It does not pre-start the context when metadata is
enabled, because the monitor treats an already active matching context as a
replayed BEGIN. Every row selects its own cycle's context. The monitor alone
counts emitted change envelopes, so insert/delete records are no longer counted
twice. A key change contributes two change envelopes; its tombstone is not counted.

At C/CM, the reader selects the matching context and publishes END before
clearing/removing it. An unrelated or unknown commit cannot close another cycle.
Empty observed transactions produce BEGIN and a zero-count END. These are legacy
metadata markers, not a new transaction buffering or rollback implementation.

With metadata enabled, a positive commit cycle without an observed start is
rejected rather than relabeled as autocommit. In the default processing mode,
this row failure reaches the streaming error handler before RPC position
advancement. BEGIN/END dispatch failures now follow the same default fail-closed
boundary. Explicit `warn` and `skip` retain their deliberately lossy continuation
behavior. Existing transport retry classification and strict truncate handling
remain unchanged.

With metadata disabled, ordinary rows still require no observed transaction
start and produce no transaction markers. No public option, native query, row
decoding pass, offset format, or additional output topic is introduced by this
change. Autocommit reuses its offset-owned context instead of allocating a new
context for every record.

## Tests and evidence

`As400LegacyTransactionMetadataTest` uses the real legacy streaming consumer,
RPC advancement loop, record emitter, dispatcher and transaction monitor, with
controlled journal/JDBC inputs. It covers all five emitted row journal types,
key changes, F/CR compatibility, interleaved cycles and autocommit, empty commits,
unknown commits, missing starts, marker dispatch failures, explicit lossy modes,
metadata-disabled behavior and independent ownership of inactive contexts.

The existing row-failure regressions remain in place. The old regression that
expected the metadata null-pointer exception is replaced by successful real
dispatcher cases. The earlier Kafka failure/barrier evidence is retained as a
historical run; it is not presented as a run of this new build.

Local verification artifacts are retained under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-legacy-metadata-20261108/`:
targeted and full Maven logs, test results, package/format validation, the exact
turn patch, preservation checks and Advisor review. The ordinary Maven suite
requires no IBM i connection for these cases.

All 2,446 unit tests passed, including 21 metadata regressions and the 30 remaining
row-failure/compatibility cases. Maven verify with assembly, package and
formatter/import validation, and diff whitespace checks passed. The F/CR handler
is byte-identical to its pre-change version; the protected F/CR, original truncate
and D/CG worktrees retain their files, HEADs and staged entries.

## Remaining limits

The legacy reader still does not serialize its map of open transaction contexts
and counters into source offsets. This change therefore does not qualify metadata
continuity across a task restart inside a transaction. In particular, a fresh
reader encountering a positive-cycle row without its start must reject it in
default failure mode. An unknown commit alone produces no invented END.

The legacy path also does not gain rollback buffering, savepoint reconstruction,
or atomic publication of a multi-record update. A partial failure can already
have queued a prefix; the earlier barrier experiment's broker guarantees apply
only under its separately qualified Connect transaction profile.

The test-only migration barrier and conditional handoff proof still reject
metadata-enabled handoffs. Repairing ordinary event metadata does not establish
safe migration of already published metadata, counters or open transactions.
Native inventory visibility, complete retained history and a combined native
handoff remain separate gates. Migration and normal transactional streaming
admission are unchanged. This work performs no IBM i access and does not alter
or publish the F/CR PR branch.
