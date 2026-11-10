# Local proof of a legacy-to-transactional handoff

Status: test-only, conditional feasibility proof. No normal-task admission,
configuration option, offset migration command, or automatic retry was added.
The production legacy-offset rejection and all transactional startup guards remain
unchanged. This work lives alongside the held candidate in `dbz-3027-review-series`;
the original D/CG and F/CR branches were not edited.

## Follow peer connector conventions

Use the existing `table.include.list` / `table.exclude.list` to choose captured
tables and connector-wide `skipped.operations` to control operation types. Do not
add a separate per-table truncate list or mode. PostgreSQL and Oracle document
this arrangement for truncate events:

- [PostgreSQL truncate events and skipped.operations](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-truncate-events)
- [PostgreSQL table.include.list](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-table-include-list)
- [Oracle truncate events](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-truncate-events)
- [Oracle table.include.list](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-property-table-include-list)

Reviewed against the corresponding Debezium source documentation at
`0d2ebf88ceebd7d991b370cb8d028b91d4ac6f91`, as well as the online documentation.
This is a configuration convention, not a claim that every database supports
truncate events or IBM i's journal semantics. In particular, SQL Server documents
that its source connector does not produce truncate events.

The existing strict IBM i dispatcher requires `skipped.operations=none`. The proof
retains that check. Its table filter applies equally to rows and truncates, and
all controlled bindings and transaction controls remain available for decoding
and recovery, including those for excluded output tables. Selection cannot change
as part of this migration. Adding a newly captured table is a different problem
from enabling future truncate capture on a table already in the stream.

## What the legacy offset establishes

The tests construct ordinary offsets with the real `As400OffsetContext` serializer
and load them with its real loader. Their positions refer to actual captured
entries; these are not offsets captured from a live Kafka migration.

The existing RPC loop invokes the consumer **before** advancing the shared offset
to the current entry. A row at sequence 42 can therefore carry source sequence 42
and durable source offset 40. A direct test of `As400RpcConnection.getJournalEntries`
demonstrates this boundary with controlled transport. A heartbeat can also carry
40 after scanning 40. The offset map alone does not distinguish those situations.

The legacy offset also lacks the oldest open transaction, native object identities,
historical schemas, receiver incarnations, verified read coverage, and old-task
ownership. It cannot simply become a completed-output position in the new buffer.
`As400Checkpoint.fromOffset` continues to reject that conversion.

## Conditional handoff exercised locally

The harness is nested in `As400LegacyHandoffTest`; it is not a production adapter.
It reuses `open-clear-progress.json` without modifying captured sequence numbers,
thread identifiers, commit cycles, or payloads. Existing fixture bindings supply
the integer schema and distinct file/member journal identifiers.

The harness has the following explicit preconditions:

- The existing stream has an accepted, completed baseline. This does not certify
  the historical correctness of that baseline or repair previously skipped clears.
- The legacy task has stopped and cannot publish more records. There is no second
  publisher in this fixture. Real Kafka fencing for this migration remains a gate.
- Capture selection is unchanged. The fixture models the legacy entry-type filter,
  without native file filtering or the expanded transaction-control reader. A test
  checks the modeled type set against `RetrieveJournal` to detect drift.
- The legacy profile has transaction metadata disabled and skips truncate events.
  The candidate uses the current strict runtime's transaction metadata. Preserving
  an existing transaction metadata stream, including already-visible BEGIN records
  for crossing transactions, needs a separate overlap contract and is not claimed.
- A known closed history anchor, complete retained journal coverage, and stable
  historical bindings are available independently of the legacy offset. The
  controlled fixture establishes anchor 29. **No online mechanism for obtaining
  these facts is implemented or proved here.**

The fixture's digest checks that the supplied retained byte sequence has not lost
or reordered entries, including an entire transaction. A digest computed from
already incomplete live data would prove nothing about its completeness. Native
coverage must come from validated retrieval and receiver history, not this digest.

The test-only preparation reads from that anchor through legacy position L using
the actual strict decoder, journal lifecycle validator, transaction buffer and
dispatcher. It validates but discards completed prefix output. It does not emit
snapshot READ events, request a snapshot, reset a topic, stop application writers,
or create a new data baseline.

If an open cycle has already touched a captured table, preparation rejects before
external output or changing the durable offset. This includes an already-published
row and a clear that the legacy reader skipped. Suppressing that prefix at a later
commit would not solve rollback or savepoint compensation of already-visible rows.
The conservative test does not attempt to undo those rows or silently ignore the
crossing clear. A prefix remains unsafe even if a savepoint later removes it.

Preparation also rejects an ambiguous next legacy-returned data event: because
the offset can lag, that event may already have been published. Excluded output
tables still participate in strict transaction reconstruction. An excluded open
cycle does not by itself prevent migration of the unchanged captured scope.

On acceptance, the candidate uses the reconstructed replay floor F and completed
output W. Scan position R and legacy position L are not relabeled as W. For
example, at L=31, the captured transaction has started but has not changed rows;
the candidate still has W=29. Later A:8 commits while B's clear remains open,
yielding F=31/W=42. B's eventual commit publishes its truncate before its reload
row. A later rolled-back clear produces no truncate or reload row.

For L=43, 44 or 46, the earlier completed prefix is suppressed with W=43; the
second open cycle is reconstructed and rolled back. The earlier clear, skipped
by the old connector, is not retroactively emitted. This proves a future-capture
boundary, not correction of stale downstream data.

## Durability, restart, and failure cases

History is independently recovered before a candidate can be prepared. The test
uses the existing file-backed `SchemaHistory` fixture and never treats a current
catalog lookup as historical binding proof.

The local JSON offset file models the distinction between preparing a candidate
and acknowledging it durably. It is not a new recovery store or a simulation of
Kafka atomic transactions. The tests exercise:

- Crash before migration acknowledgement: the legacy map is unchanged; a new
  harness instance recomputes the same candidate from retained evidence.
- Restart after migration acknowledgement: a fresh coordinator and history reader
  reconstruct transaction state from F/W; no snapshot or live objects are reused.
- Restart after unrelated-table progress while a clear remains open.
- Rejection after each possible partial enqueue of the clear batch; durable F/W
  remains unchanged, and replacement replays the whole unacknowledged batch.
  A nontransactional consumer may observe duplicates. Exactly-once publication is
  not claimed by the list-backed sink.
- Missing or incomplete legacy offsets, pending baseline, an unprocessed
  continuation cursor, incompatible receiver or selection, absent closed anchor,
  missing coverage, missing start/whole cycle, reordered entries, and missing or
  damaged binding history. Rejections leave the supplied offset and history as-is.
- File/member identity corruption still rejects even when that table's output is
  excluded. Existing native savepoint, journaling-transition, row-decoder, replay,
  cancellation and F/CR tests remain applicable and unchanged.
- Additional captured two-table rollback and savepoint cases verify that ordinary
  table filtering retains both tables' transaction context while publishing only
  the selected table. These exercise the dispatcher, not a second migration trace.

Run the proof without Kafka, IBM i or credentials:

```sh
mise exec java@temurin-21 -- ./mvnw -B -ntp -pl debezium-connector-ibmi -am test \
  -DskipITs -Dtest=As400LegacyHandoffTest -Dsurefire.failIfNoSpecifiedTests=false
```

## What this does not yet establish

This is not a general online upgrade implementation. Some valid legacy positions
are deliberately rejected. A busy stream may keep encountering unsafe prefixes;
there is no bounded activation-time guarantee and no automatic strategy for
waiting while continuing legacy CDC. Requiring an administrator to pause writers
has not been substituted as a fallback.

Native evidence is still needed to establish, while writers continue:

1. A sufficient retained-history floor and complete context for every crossing
   transaction, without inventing a closed boundary from the legacy offset.
2. Exact legacy retrieval/continuation and emission overlap across native file
   filters, page boundaries and receiver rotation. The proof's one-receiver,
   processed-position profile is deliberately narrower.
3. The file/member/schema relationship and its continuity from the retained floor
   through activation, including native journaling transitions and savepoints.
4. That an eligible boundary can be reached in realistic busy workloads, its read
   cost and latency, and unrelated-table progress during the **migration**, not
   only after migration as demonstrated by the existing runtime captures.

Separate local Kafka integration is still required for an ordered, durable
migration acknowledgement, legacy-task drain/fencing, crash during the history
and offset transition, and replacement immediately before/after that transition.
Existing Kafka recovery/ownership experiments are useful prerequisites but do not
prove this newly proposed transition. Native tests would also need the actual
upgrade path, not a replay of these fixtures, before enabling it.

The next concrete gate is to establish the evidence needed for a trustworthy
online legacy boundary, including the lagging-offset overlap. If the available
history and native APIs cannot establish it, reject migration rather than publish
from a guessed boundary. No resnapshot or writer pause is inherently necessary
for future truncate capture; neither is safe automatic activation proved by an
ordinary legacy offset alone.
