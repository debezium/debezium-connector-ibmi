# Transactional truncate architecture

IBM i can journal a table clear without individual row deletes. `F/CR` describes
a member clear; object-level `D/CG` subtypes 8 and 3 describe SQL TRUNCATE and
optimized DELETE. A nonzero commit cycle means a clear can still roll back.
Publishing it immediately would irreversibly clear a destination before the
source transaction commits.

The legacy reader keeps its default behavior and rejects commitment-controlled
clears when truncate capture is enabled. Transactional capture is opt-in through
the [online configuration](dcg-online-configuration.md) or the separate
[configured first-start profile](dcg-configured-native.md). Those guides define
the supported source, runtime, retention and output settings. This document
describes the implementation shared by those paths.

## Native records and identity

`CompleteRowEntryDecoder` requires complete inline row images and validated
physical layouts. It bounds each field and rejects missing/defaulted columns,
unsupported codecs and minimized images. `As400SourceNormalizer` materializes
owned values, preserves original source coordinates and producer identity, and
uses bindings acquired from native metadata.

A physical file's `*FILE` journal identifier and its member data's `*QDDS`
identifier are distinct: D/CG must match the former, rows the latter. Bindings
join the exact file/member, format and schema with their native identities.
Matching names alone do not establish object continuity. Receiver metadata,
recorded job/thread fields and relevant journal history are validated as well.
Unknown records or incomplete bindings fail closed; current-row SQL reads never
replace historical journal images.

The implementation uses `QjoRetrieveJournalEntries` for journal records,
`JOURNAL_RECEIVER_INFO` for receiver relationships and recording settings, and
exact-path `IFS_OBJECT_STATISTICS` observations for object identities. Paired
physical-file and member-data journal identifiers are verified separately.
Identity observations alone do not grant retention protection or permission to
publish a checkpoint. See [validation scope](dcg-validation.md) for the limits of
the native evidence.

## Transaction buffering and delivery

`As400TransactionBuffer` groups rows and clears by the validated local commit
cycle. It pairs before/after images using producer, table/member, format and row
identity, and preserves operation order inside a transaction. A supported commit
exposes a batch; full rollback discards it. Savepoint rollback removes the
affected suffix only when its target and native semantics are established.
Ambiguous savepoint, compensation, XA/LUW or journaling transitions stop capture.

An open clear delays its own transaction, while unrelated committed transactions
continue. Output retains commit order between completed batches and operation
order within each batch. Buffer limits bound cycles, entries, savepoints and
owned payload bytes. Reaching a limit stops capture without evicting unfinished
work or advancing its durable checkpoint; payload accounting is not total JVM
heap accounting.

`As400BatchDispatcher` and the normal Debezium dispatcher emit ordinary row
envelopes, keys and tombstones, plus keyless `op=t` clears. A clear precedes its
committed reload rows. One Kafka partition per table topic is required for that
ordering. There is no global ordering claim across topics, and consumers must
explicitly support truncate events.

## Recovery positions and acknowledgement

The reader keeps three different positions:

| Position | Meaning | Durable? |
| --- | --- | --- |
| R | Current journal scan position | No |
| F | Oldest position needed to reconstruct unfinished or incompletely delivered work | Yes |
| W | Last completed output boundary | Yes |

An open transaction can keep F behind W while unrelated output progresses.
`As400ReplayCoordinator` rebuilds working state from retained journal
history starting at F. Already completed output through W remains suppressed;
the scan cursor alone cannot authorize progress. Offsets also bind journal and
receiver incarnations and a versioned capture fingerprint. Immutable binding
history supplies the matching schema and initialization context.

The normal task/coordinator publishes through its queue and dispatcher. Online
startup selects its acknowledgement contract from the actual task context.
Distributed Connect with source transactions retains connector-managed transaction
boundaries and broker fencing. A task without that capability instead confirms
the exact delivered prefix from its durable source offset store. Delivery
callbacks alone cannot advance durable F/W. Streaming can continue between
offset flushes, but startup and handoff still wait for their durable barriers.

Ordinary source offsets do not provide broker fencing or atomic output/offset
commits. Embedded applications must preserve acknowledgement order and prevent
independently overlapping connector instances. Neither path uses a custom owner
registry or receipt database. Runtime requirements are checked before native
startup side effects. See the [compatibility evidence](dcg-compatibility-baseline.md)
for standalone/Engine qualification and the remaining Server/history gates.

Delivery is whole-batch **at least once**, not source exactly once. If delivery
stops after a committed prefix, recovery can repeat the complete batch. Missing,
partial, incompatible or regressing recovery state must reject rather than invent
a new starting offset. Operator-managed retention must preserve all receivers
and immutable binding history required by F/W. Automatic retention release and
arbitrary offset/history restores are outside this contract.

## Online handoff

The online path starts from a completed ordinary streaming offset. Legacy CDC
continues while the task discovers the whole journal scope and gathers metadata.
A minimal server-owned allocate/marker/release program requests brief write
exclusion over that scope. Metadata queries, Kafka calls and acknowledgements
stay outside the held interval. Busy allocation defers admission while ordinary
CDC continues; uncertain release or invalid history cannot authorize handoff.

The task must prove the relevant in-flight transaction history and the interval
between the lagging legacy offset and the accepted native boundary. Only after
the legacy output barrier is durable may it publish the initial F/W checkpoint.
When the legacy stream is idle, an exact observed object-free marker can bridge
the gap only after excluding intervening CDC and revalidating the required
history and identities. Receiver-tail metadata or an empty poll is insufficient.

The handoff does not resnapshot or change standard table selection. Once its
barrier or F/W is durable, the task cannot fall back to interpreting that state
as an ordinary legacy offset. A replacement resumes from the matching history;
it does not acquire a new first-start boundary.

The separate configured profile creates a baseline under an operator-controlled
first-start window. Snapshot delivery is bounded, acknowledges completed chunks,
and persists history before exposing the initial checkpoint. Partial starts are
rejected. Its enrollment/history cannot be reinterpreted as online history.

## Stop and replacement

The RPC reader distinguishes terminal stop from ordinary reconnect. Admission
checks cover receiver discovery, call setup/registration and transport start;
stopped streaming retries cannot start new RPC/JDBC work. Cancellation and drain
must complete before a reader is treated as closed. A stop request alone is not
proof that native work has ended, and late callbacks cannot advance a replacement's
durable F/W.

See [contributor testing](dcg-contributor-testing.md) for the executable local and
optional native checks, and [validation scope](dcg-validation.md) for the evidence
and remaining operational limits.
