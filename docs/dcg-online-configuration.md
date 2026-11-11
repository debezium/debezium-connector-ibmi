# Online transactional truncate capture

The local implementation adds `truncate.transactional.enabled=false`. Setting it
explicitly to `true` admits the native transactional truncate runtime from an
**existing, completed ordinary streaming offset**. It uses the normal
`As400RpcConnector` and task, the same topic prefix and source partition, and
standard `table.include.list` or `table.exclude.list`. There is no separate
per-table enable setting. No snapshot is taken by this transition. Preserve the
existing capture selection exactly: the old RPC filter requires concrete system
table names; do not simultaneously replace that selection with a new regex policy.

This is separate from `dcg.mode=initialize/resume`, the earlier operator-window
profile. Those settings cannot be combined. Existing versioned operator-window
history and earlier experimental histories cannot be reinterpreted as online
history. The ordinary default and F/CR behavior remain unchanged.

## What enabling it does

The connector discovers every physical file/member on the configured local
journal, acquires native schemas and verifies retained history. While it does
that work, the legacy row dispatcher continues publishing. A server-owned
allocate/marker/release program briefly requests write-excluding locks on the
**whole discovered journal scope**, including files filtered out of output.
Metadata queries, Kafka writes and acknowledgements run outside that interval.
A busy allocation is released and deferred while ordinary CDC continues. An
unqualified audit, unsupported table layout or stale unpublished candidate is
deferred with logged exponential backoff while legacy CDC continues. It never
authorizes a boundary. After a handoff barrier is offered, uncertain acknowledgement
or invalid recovery history fails; that candidate can no longer be replaced.

Online binding proposals have a bounded eligible history prefix: at most 1,024
records and 16 MiB in total, with a 4 MiB limit per record. The writer checks
capacity before appending and reads its exact proposal back before offering a
handoff. If an old worker races the capacity check, an append beyond this prefix
cannot become a selected boundary or invalidate an earlier acknowledged one.
Invalid or missing records within the eligible prefix still fail recovery.
On capacity exhaustion, the task logs the refusal, stops requesting new boundaries
and continues legacy CDC. Restarting does not free capacity. Preserve the history
and offsets; no automatic compaction, history reset or retention release is allowed.
These proposal rules do not relax the stricter operator-window history checks.

After the completed legacy output barrier is durable in Kafka Connect, the task
can publish the new recovery checkpoint. The immutable binding history alone
never grants that permission. When writers become idle before the native marker,
a separate bounded read must observe that exact object-free user marker, prove
there are no intervening CDC records, and revalidate native identities. Only then
may the normal Kafka barrier acknowledge that actual position. If the prospective
interval is still empty, complete retained C/J history must also prove that no
transaction is open. The initial exclusive F=W then names that observed marker;
recovery verifies it independently, and it never becomes a table event. An empty
poll or receiver-tail number alone cannot advance the offset.

F is the oldest journal position still required for
replay; W is completed output. The scan position R remains separate and cannot
substitute for either. An open clear buffers its transaction's output while
unrelated committed changes continue. A fresh task rebuilds unfinished work from
F and does not establish another initial boundary.

The allocation uses `WAIT(1)`; that is not a universal one-second writer-pause
promise. Server scheduling, conflicting locks and release/drain failures matter.
Earlier isolated healthy pulses held allocations for approximately 1–2 ms, while
collecting metadata took much longer outside those allocations. Enabling the
feature on a busy journal may defer admission for an unbounded time. Applications
must not rely on transactional clears being captured until handoff completes.
This change cannot repair clears missed before its accepted boundary.

A failed `DLCOBJ` must not be treated as a released lock. The server program makes
three bounded release attempts; after an explicit terminal release failure, the
client attempts one exact release on the same job. An uncertain network response
is never retried. Independent lock observation is required before accepting a
candidate. Job-scoped locks can survive closing a pooled SQL connection. If release
cannot be verified, the task reports the owning job and fails; an operator must
verify that job's locks and release only the connector-owned allocation. Repeatedly
restarting the connector does not prove those locks are gone. `CONFLICT(*RQSRLS)`
can close conflicting pseudo-closed cursors, causing applications to reopen them.

## Admission requirements

Use the common [runtime, output and retention contract](dcg-configured-native.md),
with these online-specific differences:

- A complete legacy offset and completed snapshot, or the matching
  version-5 online history and durable F/W checkpoint, must already exist.
  An unprocessed anchor is replayed before it can acknowledge a boundary.
  Missing, partial, foreign or incompatible offsets reject. `snapshot.mode=initial`
  remains required but does not request another snapshot when the existing offset
  is admitted. Do not clear offsets or discard history to work around rejection.
- Set `truncate.transactional.enabled=true`, keep `dcg.mode=disabled`, and omit
  `dcg.tables`, `dcg.controlled.tables`, `dcg.journaled.libraries` and
  `dcg.initialization.window`. The connector discovers the full inventory instead.
- Set `dcg.journal=LIBRARY/JOURNAL` and `dcg.retention=operator-managed`. Retain the
  legacy starting receiver, the discovered transaction history and every receiver
  subsequently required by F/W. No automated retention release is granted.
  Task-owned native journal observations require
  `JOURNAL_INFO.DELETE_RECEIVER_OPTION=NO`; automatic deletion can remove a
  receiver still needed for replay. The setting cannot prevent manual history
  deletion. Older captured journal observations alone are not retention evidence.
- The current online scope is one active local journal in system ASP 1 with at
  most 50 single-member physical files. Every discovered file must have a supported
  fixed schema and declared primary key, even when filtered from output. The
  online mode rejects RRN key conversion, LOB/XML and unsupported physical layouts.
  Changes to schemas, journaling, capture selection or output identity require a
  separately validated transition; this is not dynamic schema evolution.
  Both `OMTJRNE(*NONE)` and `OMTJRNE(*OPNCLO)` are supported. Complete before/after
  images remain mandatory; the clear's matched journaling companions cannot enable
  remote filtering or inheritance. Ordinary open/close records are identity-checked
  metadata, not table changes or transaction completion.
- Use distributed Kafka Connect compatible with the connector's Debezium version
  (locally tested: 4.2.0 and 4.3.1), one task, worker source transactions
  enabled, `exactly.once.support=requested`, `transaction.boundary=connector`,
  `provide.transaction.metadata=false`, `skipped.operations=none`, positive
  heartbeats, and FAIL/no-tolerance processing and conversion. Exactly-once source
  delivery remains unsupported: recovery can replay an already published prefix
  of an interrupted transaction's output batch.
- Keep the qualified standard converters, naming, keys, tombstones and type
  handling consistent across workers. The online fingerprint also pins the
  native database catalog used by the existing topic names.
  Keep the existing topic naming strategy, character trim mode and time precision;
  enabling this option does not require changing those ordinary defaults.
  Native output now shares the ordinary character cleanup and trimming converter.
  Online temporal output also preserves the actual legacy JT400 decoder's time-zone
  behavior, including its existing quirks. All workers must retain the same JVM
  time zone and time-zone rules; both are fingerprinted. This transition does not
  silently correct already-published DATE/TIME/TIMESTAMP values. Online migration
  rejects timestamp precision other than six because the old timestamp byte decoder
  has a fixed width. The operator-window profile retains its timezone-free decoder.
  Earlier experimental native output fingerprints (`native-configured-v1` and
  `native-online-candidate-v2`, and online histories without the
  `legacy-jt400-v1` temporal policy) are incompatible with this correction. Preserve
  their history and offsets; do not reset or edit them to bypass rejection.
  They require an explicit data migration or a new isolated generation, not a
  silent restart. Ordinary legacy offsets do not use those native fingerprints.
- Precreate distinct single-partition data topics, the configured schema-history
  topic, heartbeat topic, and `__debezium-truncate.<topic.prefix>`. Internal clients
  cannot create them on observation. Retain all binding history without compaction
  or expiry. The control topic carries checkpoint barriers, not table data.

The existing `dcg.buffer.*` resource bounds apply. Exceeding them stops capture with
the durable checkpoint preserved; it does not silently evict an open transaction.
The native runtime validates before/after images, full required job/thread fields,
receiver continuity, object/member identities and relevant journal history on
startup and recovery. Operator retention and fixed-source obligations still apply.

Once a handoff barrier or F/W is durable, disabling the flag cannot fall back to
legacy streaming. Restore the matching configuration and retained history to
resume; do not reset the source to a fabricated offset.

## Local and isolated native verification

`tools/dcg-public-online-probe.py NEW_DIRECTORY DCNCdddd` uses the portable
owner-only `IBMI_TEST_CONFIG` contract described in
[contributor testing](dcg-contributor-testing.md). It creates and removes only a
fresh isolated fixture and labeled local Kafka resources. The worker loads the
assembled distribution from its plugin directory; test helpers run only on an
independent audit classpath. Credentials use the standard FileConfigProvider in
container tmpfs and are excluded from retained configs and logs.

The probe starts ordinary snapshot/streaming, obtains its actual durable offset,
then changes the public setting. It checks busy-boundary CDC progress, no second
snapshot, durable handoff, open clear and receiver rotation, task crash/reassignment,
rollback suppression, committed truncate-before-reload, optimized DELETE subtypes,
mixed values and completed drain. Raw journal and independent read-committed Kafka
evidence are retained. The stricter deterministic crash-boundary tests remain in
[validation scope](dcg-contributor-testing.md).

This is a bounded qualification profile, not validation of every IBM i release,
CCSID, schema, Kafka topology or operational failure. See the
[validation scope and recorded native results](dcg-validation.md) for what actually ran
and what remains untested. The ordinary-plugin native qualification passed in
the documented profile; the public option remains off by default.

Native journal metadata also requires an API job with CCSID 37. Its request and
response codecs are explicit, so a German, Danish or Japanese worker locale does
not reinterpret object names. This does not change column CCSIDs. Existing
experimental native fingerprints without this metadata policy reject; ordinary
legacy offsets can still be considered for a newly qualified handoff.
