# Online transactional truncate capture

The local implementation adds `truncate.transactional.enabled=false`. Setting it
explicitly to `true` admits the native transactional truncate runtime from an
**existing, completed ordinary streaming offset**. It uses the normal
`As400RpcConnector` and task, the same topic prefix and source partition, and
standard `table.include.list` or `table.exclude.list`. There is no separate
per-table enable setting. No snapshot is taken by this transition. Preserve the
existing capture selection exactly: the old RPC filter resolves concrete SQL
table names to system file names; do not simultaneously replace that selection
with a new regex policy.

This is separate from `dcg.mode=initialize/resume`, the earlier operator-window
profile. Those settings cannot be combined. Existing versioned operator-window
history and earlier experimental histories cannot be reinterpreted as online
history. The ordinary default and F/CR behavior remain unchanged.

## What enabling it does

The connector discovers every physical file/member on the configured local
journal, acquires native schemas and verifies discovery history in the background.
While it does that work, the legacy row dispatcher continues publishing. Final
reconstruction of the exact acknowledged legacy prefix still runs on the source
thread before activation; its duration grows with the required history. Moving
that final pass into the background requires reconciling further legacy output,
not just starting an asynchronous read at an older offset. A server-owned
allocate/marker/release program briefly requests write-excluding locks on the
**whole discovered journal scope**, including files filtered out of output.
Metadata queries, Kafka writes and acknowledgements run outside that interval.
A busy allocation is released and deferred while ordinary CDC continues. An
unqualified audit, unsupported table layout or stale unpublished candidate is
deferred with logged exponential backoff while legacy CDC continues. It never
authorizes a boundary. After a handoff barrier is offered, uncertain acknowledgement
or invalid recovery history fails; that candidate can no longer be replaced.

New online binding proposals use one bounded record per table and a final descriptor.
Only a complete descriptor whose table records, ordering, identities and policy
fingerprint verify can be selected by a durable source offset. Partial proposals
left by an interrupted writer grant no authority; an identical retry may finish
one. Recovery scans a fixed history cut, selects the descriptor, then streams its
table records in a second pass. It does not load every proposal into memory.
Individual records remain limited to 4 MiB, but the new format has no 1,024-record
or 16 MiB lifetime ceiling. Kafka fetches are bounded and use an inactivity timeout;
file history appends a proposal under one lock with one final forced write.

Existing `v1:` offset fingerprints keep their original immutable-prefix selection
rules (1,024 records / 16 MiB) and remain readable alongside newer proposals. New
paged generations use `v2:` fingerprints; older connector builds cannot read them.
Keep the matching connector build, source offsets and history together when
planning a rollback. This changes no completed F/W position and grants no history
compaction, reset, deletion or retention release. The stricter operator-window
single-manifest format retains its existing bounds.

After the completed legacy output barrier is durable in the source offset store, the task
can publish the new recovery checkpoint. The immutable binding history alone
never grants that permission. When writers become idle before the native marker,
a separate bounded read must observe that exact object-free user marker, prove
there are no intervening CDC records, and revalidate native identities. Only then
may the normal output barrier acknowledge that actual position. If the prospective
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
feature on a busy journal may defer admission for an unbounded time.

If the legacy prefix encounters a transactional clear while admission is waiting,
it holds subsequent changes for that table and continues publishing other tables.
Every source offset and automatic delivery acknowledgement stays at the safe
pre-clear position. After the actual commit or rollback, retained native history
must establish the original transaction start, exact file/member identities,
unchanged schemas and the complete clear/savepoint/restore sequence. A committed
clear precedes its replacement rows; rolled-back work is discarded. The existing
legacy transaction metadata context closes only after all its held rows are
released. A following cycle-zero F/CR is validated against its exact single-member
binding and delivered in source order after that outcome; it is not discarded as
administrative metadata. A background metadata timeout outside the allocation interval defers
admission with backoff; it does not authorize publication or weaken lock-release
checks.

This transition still uses legacy offsets, before native F/W recovery takes over.
A replacement reconstructs the held work from the saved position and retained
original transaction history, including starts in earlier receivers. Ordinary
rows already emitted after that position can repeat on restart. Retain that
original history even when it precedes the saved legacy offset; do not delete
receivers based only on the latest emitted row. Missing history, changed schemas,
unresolved native sequences or a same-table transactional prefix already exposed
before the clear are rejected without acknowledging past the clear. Temporary
indexes and row images use disposable disk storage; they never replace the journal
as recovery authority. After all held work drains, normal online admission resumes
without resnapshotting. Different jobs may have started their transactions in a
different order from their eventual clears; each outcome is matched to its own
producer and source position. Clears skipped before the saved legacy position are
not repaired by enabling the feature.

A rejected held clear is not a temporary admission delay: it stops the task at the
safe position, and restart rejects the same unresolved evidence. Disabling the
feature and skipping `t` would discard that clear; it is not a lossless recovery
procedure. Restore the required receiver history or correct the identified source
configuration first. If the original history or row definition cannot be recovered,
reconcile the affected destination from a new consistent snapshot and establish a
new connector generation under the normal snapshot procedure. Keep the old offsets
and history for diagnosis; never reset them merely to get past the failed entry.

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

- An existing stream requires a complete legacy offset and completed snapshot,
  or matching online history and its durable F/W checkpoint.
  An unprocessed anchor is replayed before it can acknowledge a boundary.
  Foreign or incompatible offsets reject. Existing streams may
  retain `snapshot.mode=initial`, `no_data` or `when_needed`; none requests another
  snapshot at this transition. Existing snapshot SELECT overrides and snapshot
  query/locking settings may remain configured; they are not run by online
  handoff and do not change its streaming fingerprint. A new connector with
  demonstrably empty binding history uses the ordinary snapshotter first,
  including `no_data`. `initial_only` waits for durable snapshot completion and then
  stops without native handoff or a streaming reader. A recognizable interrupted ordinary snapshot can repeat
  through that snapshotter while history is still empty. Its completed offset
  must be durable before native handoff. Missing offsets with retained history
  always reject. With an existing native checkpoint, `always` and configured
  snapshotter policies can request a new ordinary snapshot. Old F/W remains
  authoritative during bounded snapshot delivery; an interrupted snapshot must
  finish under a compatible policy before streaming. Snapshot-only policies
  finish after durable delivery. The operator-window baseline still rejects snapshot overrides.
  Do not clear offsets or discard history to work around rejection.
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
- The current online scope is one active local journal in the system/basic ASP namespace (numeric pools 1–32; live qualification used pool 1) with
  physical files. Data and source files must pass the same identity and format
  checks. Ordinary rows from multiple members retain the existing
  shared table topic and key format; complete member identities are retained in
  binding history. A captured member-only clear still rejects because it cannot
  safely erase the combined table output. Logical files/indexes, data areas,
  queues and access paths may share the journal; they are not row-capture tables.
  There is no fixed connector file-count ceiling:
  independent native counts bound the temporary inventory and JDBC read, which
  must agree before and after discovery. Native allocation commands contain at
  most 50 files each. Larger scopes preload bounded command records into a
  job-local ledger before allocation, then run a compact server program with a
  one-second total acquisition budget and immediate attempts after the first batch. The same server call releases every
  attempted batch before returning. A job-local ledger supports release recovery;
  an uncertain transport response never authorizes another allocation or release.
  Metadata is collected outside the held interval. Busy/over-budget attempts leave
  legacy capture running and use the existing retry backoff. Removing the file
  ceiling does not guarantee that every journal can acquire a safe boundary in
  that time. Native audit limits and retained-history checks still apply.
  Table schema/identity metadata still uses memory proportional to the catalog,
  as it does in the ordinary connector; paged history bounds serialization and
  recovery buffers, while the optional disk buffer bounds transaction payloads.
  Receiver discovery uses IBM i's observed directory count and exact linked
  traversal rather than fixed 128/1,023-receiver ceilings. Full directory samples
  must match their independent before/after counts and attachment; each required
  receiver still needs reciprocal links, a readable recording profile and an
  exact native object identity. Receiver metadata uses memory proportional to
  the required directory or retained chain. This is not a constant-memory catalog.
  Captured files require supported physical layouts; a primary key is optional.
  SQL names can differ from physical system file names. Output uses the SQL name;
  journal identity and locking continue to use the independently verified physical
  file/member. SQL primary keys take precedence over the ordinary connector's
  DDS physical-file key fallback. Complete empty key catalogs prove a keyless table.
  Keyless tables retain null Kafka keys. `source.rrn` is row metadata, not an
  implicit key conversion. New online histories also retain complete schemas for supported excluded files,
  allowing standard selection changes at restart. Excluded files retain native
  file/member identities, format shape and transaction effects without decoding
  output values. Unsupported or inaccessible excluded layouts remain identity-only.
  They must still satisfy the complete-member-inventory, non-minimized-image and
  journal-identity checks. Expanding capture selection cannot promote identity-only history into
  a row schema. Excluded native LOB files retain a separate identity-only binding:
  bounded pointer-row length/flags, null maps and transaction effects are validated,
  but external values are never read or published. Every returned pointer handle
  is released in the retrieving native job, including entries deferred to a later
  page. Cleanup failure prevents progress and cancels that job. Captured LOB/XML
  values remain unsupported; an excluded binding cannot become a captured schema
  just by changing a filter. Minimized/defaulted or ambiguous row metadata still
  rejects. Catalog reads use bounded batches without caching.
  Excluded updates may use `IMAGES(*AFTER)` because no before-image is published
  or paired for them. Captured files must use `IMAGES(*BOTH)`, checked before
  accepting an enablement candidate and again before offering its durable handoff.
  A busy or unsafe candidate is still replaceable at that point; no handoff is
  acknowledged first. Already acknowledged handoffs recover through their retained
  journal/history protocol instead of adding a current-catalog startup condition.
  Ordinary recovery validates the retained journal lifecycle instead of requiring
  today's catalog to look idle: a valid transactional clear can temporarily end
  journaling. Captured updates still require their exact before-image.
  Grant/revoke and trigger creation/removal records are identity-checked metadata,
  including commitment-control records tied to their actual producer, transaction
  start and active native savepoint.
  Triggered row changes are still processed as ordinary journaled rows. Constraint
  additions/removals (`D/AC`, `D/DC`) preserve the already chosen decoder and event
  key, as legacy streaming does; they do not reacquire today's key and apply it to
  older buffered rows. This includes CHECK, foreign-key and primary-key constraints.
  Removing a database uniqueness constraint does not make duplicate event keys
  unique; consumers still receive the retained key values. A qualified column
  schema refresh or a new capture establishes its own key definition. A newly
  created table may establish its initial key before its first row; earlier rows
  that need an unavailable initial key definition still reject. A complete
  physical-file deletion retires its binding in durable schema history. A
  transactional DROP requires its verified commit and physical deletion records;
  rollback retains the original binding. The old decoder remains available for
  buffered rows until the physical deletion completes. DROP
  does not emit a truncate or delete existing topic records. A later creation
  under the same name requires a newly qualified file/member identity. Member
  removal is qualified by its exact F/DM and F/MD pair and retains the identities
  of any remaining members. Removing the final member retains the physical file
  with no active member in durable history. Unrelated capture and restart can
  continue; rows or clears cannot use the removed member. A later member requires
  a newly qualified native association before its rows can be decoded. Removal
  itself does not emit a truncate or add member-aware keys or snapshots. The
  native compatibility test covers an empty final member, not implicit deletion
  of its populated contents. A program-described file excluded from capture uses
  native IFS member identities when the SQL member catalog contains no rows;
  it does not need a fabricated SQL schema. Capturing its untyped record layout
  is not supported.
  Object-identity queries group at most 16 exact file/member paths; this bounds
  each SQL request, not the total file inventory. Missing, duplicate, foreign,
  mismatched or incomplete observations reject the whole acquisition. The full
  before/after metadata and raw journal checks still apply.
  Observed column changes and ordinary file creation have validated transitions:
  persist the new schema before affected output, and replay the boundary from
  history after restart. Transactional CREATE and ALTER require their actual
  commit or full rollback; nested statement savepoints are matched to the native
  markers. A completed internal savepoint between an ALTER's D/EF and D/JF
  records must match its transaction, producer, deeper level and exact release
  target; it does not release the outer ALTER scope. IBM documents these
  [internal nested operations](https://www.ibm.com/docs/en/i/7.5.0?topic=objects-sequence-journal-entries-under-commitment-control).
  A rolled-back creation leaves no binding, and a rolled-back ALTER
  retains the original definition after verifying its journaling restoration.
  A savepoint established after the last schema change may undo rows without
  undoing that schema change; its compensation and rollback control still pass
  through the ordinary transaction buffer. Rolling back the DDL itself to a
  savepoint remains unqualified. Discarded rows do not add per-row records to
  schema history. Consecutive ALTERs
  may share one observed final definition when no captured row needs an intermediate
  layout; excluded rows do not require that layout. Captured intervening rows,
  incomplete creation, identity replacement and unqualified journaling transitions
  still reject: current catalog equality alone cannot prove an older row layout.
  CREATE followed by ALTER can acquire the final definition when no earlier row
  requires the creation's intermediate layout. The complete creation, ALTER and
  transaction outcomes remain in recovery history. For an excluded new table,
  earlier rows need no value decoder: the acquired SQL identity must prove its
  exclusion, and the complete creation/change interval bounds where historical
  row lengths may differ. Native identities, complete-image flags, payload and
  null-map bounds, transaction effects and replay history remain checked. This
  also covers initial key additions and qualified CL-command changes. A vanished
  table's unknown SQL identity is not inferred from its system name.
  A later fully rolled-back
  clear of the same file does not invalidate an earlier ALTER observation: its
  exact physical restoration must be present and still passes through truncate
  recovery without being consumed as schema-only metadata.
  CREATE followed by DROP without intervening rows needs no vanished catalog
  definition. CREATE followed by a clear retains the ordinary row/clear ordering
  and rollback rules; those records are not discarded as creation metadata.
  An open transactional CREATE, ALTER or DROP retains its owned journal records in the configured
  memory or disk buffer while unrelated committed changes continue. Scan R and
  completed output W may advance; replay floor F retains the original transaction
  start. The connector acquires the changed definition after the native commit or
  full rollback is observed, persists qualified schema history, then decodes and
  releases that transaction. Raw records share the transaction payload/entry
  budget; they are never evicted to make progress. A replacement rebuilds them
  from F. New history records identify the outcome at which the schema becomes
  visible; existing history records retain their original encoding and hashes.
  This is forward compatibility with retained old history, not permission to
  downgrade: older builds that cannot read outcome-visible history or combined
  ALTER/DROP lifecycles must not resume these offsets. Keep the matching connector,
  offsets and history together.
  An unfinished CREATE retains validated file/member identities without inventing
  a queryable schema. Its rows remain invisible until commit; full rollback
  leaves no binding. DROP retains the old decoder until its physical deletion
  completes, so earlier rows in the transaction can still publish at commit.
  Incomplete physical companions or unavailable outcome metadata can defer scanning;
  the connector logs the
  waiting position/table and elapsed wait. The connector refreshes observed
  current definitions; it cannot reconstruct arbitrary historical DDL.
  LABEL and COMMENT records retain their distinct physical-file or member
  identities, including their observed transaction/savepoint context. They can
  follow CREATE or ALTER in the same acquisition window. A miscellaneous change
  to a captured table must leave its retained column/key definition unchanged,
  or a subsequent qualified ALTER must establish the new definition without an
  intervening captured row that needs an unavailable layout. A complete metadata
  prefix with no intervening rows can retain the old definition until that ALTER,
  allowing unrelated output to continue while the ALTER is open.
  CL-command subtype-1 changes, such as `CHGPF SIZE(*NOMAX)`, also require exact
  file/member identity. For captured files the acquired column/key definition
  must remain unchanged. A command sequence needing an unavailable intermediate
  row layout is rejected rather than decoded using the final catalog definition.
  Same-library physical renames require the original file JID, every retained
  member's matching rename, and an acquired definition with the same columns,
  keys and record format. Recovery follows the new name through immutable
  history. Distinct SQL and system names may put a metadata pair on the old name
  before the physical rename. Historical callbacks establish the SQL name at
  that metadata boundary; the native name changes at the separately validated
  rename boundary.
  A committed ALTER followed by a physical rename can acquire the definition
  through the proven new file name while retaining the old SQL name for rows
  before D/FN. The rename remains a separate history and output boundary; later
  layout changes, incomplete member associations and ambiguous SQL names reject.
  An empty new file can also be renamed before its first metadata acquisition:
  the complete creation and rename association are retained together. This
  requires no earlier rows or clear that would need the unavailable old SQL name,
  and the new name must not replace another retained binding.
  A physical rename followed by a complete DROP can retire the old binding
  without querying the vanished alias when no row between them needs that
  alias. A replacement created under the original name has its own file/member
  identities; completing the old object's DROP cannot remove the replacement.
  Missing companions, an uncommitted or rolled-back DROP, and intervening rows
  that require the vanished SQL name do not qualify for this shortcut.
  For excluded tables, exact historical SQL names can instead establish a
  creation or rename prefix independently of a later DROP. Every name in that
  prefix must remain outside the ordinary capture predicate, including the
  signal table. The prefix is saved as a separate history transition, and
  ordinary replay handles the later rename and deletion. A new object's binding
  records identity only, with no inferred row layout. Replacement tasks can
  resume between these transitions; selected names and missing proof reject.
  A completed ALTER followed by a complete DROP can retire a known binding
  without reading a vanished catalog, provided no intervening row requires the
  missing definition. Both file and member identities and the separate outcomes
  remain checked. A missing catalog is retried once only when a new retained
  DROP or physical rename explains the disappearance of that exact file. The
  retry must qualify the full lifecycle; absence alone never proves exclusion.
  A validated transition that overlaps metadata acquisition also gets one fresh
  observation. Continued movement defers that poll without accepting stale
  metadata or advancing output. A committed ALTER followed by a fully rolled-back
  ALTER requires the latter's exact physical restoration before acquisition and
  keeps each transaction's history and row-discard boundary separate. A second
  ALTER still open inside the first's observation window can still defer scanning
  when a captured table needs an unavailable intermediate layout. For an excluded
  table, native SQL-name evidence can qualify the first committed prefix without
  querying the blocked catalog. History then stores identity only and explicitly
  marks the row layout unobserved. Unrelated output can advance while F retains
  the second transaction. This definition cannot supply captured values; a later
  capture/name transition requires a complete acquired schema. After full
  rollback, the restored definition is reacquired when retained history shows
  no later schema, name or key transition overlapping the catalog observation.
  A known pre-rollback definition is retained exactly. The names-only
  API authority described below also applies to this case. Older candidate builds
  cannot read this identity-only history and must not be used to roll it back.
  A rollback that reacquires a previously unobserved layout also needs the
  updated history reader: earlier candidates require identical definitions on
  both sides of every rollback and reject that record. Keep the required
  receivers and use a compatible build; changing the flag does not convert
  versioned recovery offsets back to legacy offsets.
  A new file created, loaded and dropped before observation can be discarded
  only when retained native D/CT and metadata records establish every SQL name
  in its lifetime and the ordinary capture predicate excludes all of them
  (including the signal table). Missing names and selected transient rows with
  no historical layout remain errors. A private API job lazily compiles a CL
  helper in QTEMP and invokes QDBRPLAY with its names-only flag fixed to `1`.
  It never replays DDL or creates a shadow application table. The job needs
  authority to create/compile its private CL programs and user spaces, read the
  retained receiver and call the native APIs; least-privilege authority for this
  optional observation has not yet been qualified. Pointer-backed metadata is
  copied and released in the retrieving job with IBM's 16,773,120-byte bound.
  Historical names are not row schemas, and a missing current catalog never
  supplies or substitutes a name. The replay floor retains the complete lifetime
  until deletion, so a replacement repeats the proof from retained history.
  This lifetime proof also covers a qualified ALTER before the DROP, including
  a shared transaction outcome. Pointer-backed rows of new excluded files are
  checked as bounded metadata; their external values are never read or published.
  Captured rows still require complete values and a supported historical layout.
  If an existing excluded table's ALTER commits before a later DROP, its known
  identity can retain an explicitly unobserved layout until ordinary replay
  retires the file. Its historical SQL name must remain excluded and unchanged.
  The same proof is required if the DROP first appears in the final journal
  observation after catalog acquisition.
  An existing excluded file can also be altered, loaded and dropped in one
  transaction. The shared commit must validate both lifecycles, and every row
  must retain the exact member identity and transaction. Recovery keeps the
  original binding and both lifecycles in history, pins the replay floor through
  physical deletion, and rechecks historical names and capture selection on
  restart. Selecting that table or losing its name evidence rejects replay;
  the saved history does not authorize discarding newly selected rows.
  A full rollback of ALTER/load/DROP can instead restore a known original
  definition for either a captured or excluded table. ALTER and DROP must
  identify the same full rollback; all four native restoration records and
  the exact row/member, producer and transaction identities must validate.
  The temporary layout is never used for output. The replay floor retains
  the unfinished restoration, including across replacement, before ordinary
  rows resume with the original definition. This does not qualify a savepoint
  rollback that undoes DDL or a committed captured layout that was never observed.
  A SQL-only rename can retain the system name and journal only a metadata pair.
  It requires unchanged physical/member identities, columns, record format and
  event key. Filtering and topics then use the SQL name at the qualified change
  boundary, with that change persisted for restart. A known physical rename can
  retain its original SQL name for rows before a later SQL-only rename. A clear
  between metadata boundaries keeps the historical name at that clear, including
  when a later DROP makes the catalog unavailable. The name-only proof never
  authorizes decoding rows with an unknown intermediate layout. Missing or
  conflicting name evidence, or a savepoint rollback that may have undone the
  name change, rejects before accepting that proof. Unavailable intermediate SQL
  names still reject; a rename does not remove records already published to the
  old topic.
  Creation-time name checks cover the entire observed interval, including rows,
  clears and metadata after an ALTER. A newly acquired SQL name cannot silently
  move earlier selected events to another topic or discard them under a changed
  filter. This also applies to a clear that emits no individual row records.
  Removing an alternative SQL name is distinguished from an ALTER that retains
  it: the qualified miscellaneous rename records the journaled native target as
  the resulting SQL name. LABEL/COMMENT callbacks that omit both names retain
  the proven name. These checks do not substitute a current catalog name for a
  missing historical name.
  Standalone same-file member renames qualify their exact F/MN record: the old
  member name and retained journal ID must agree, the payload must name the same
  file and library, and the new member name must be unused. The schema, event
  key, SQL table name and topic remain unchanged. Recovery retains the new
  association; the old member name cannot authorize later rows. A member record
  already belonging to a physical-file rename remains part of that transition.
  A file moved to another library qualifies its D/FM and one F/MM for each
  retained member. The library changes while the SQL table name, physical file
  name, format, key and native identities remain unchanged. The full association
  must be present before the transition is saved; F retains the opening until
  all member records have been replayed. An unfinished transaction on the file,
  a conflicting target binding or a changed identity rejects the move. Subsequent
  output uses the destination schema and the ordinary capture filter; the old
  library cannot authorize later rows. When a completed ALTER precedes the move
  in the same catch-up interval, the verified move locates the current catalog
  in its destination library. The ALTER and move retain separate history
  boundaries, so rows before the move keep their original library and topic.
  A completed CREATE followed by rows and a move can likewise acquire its
  surviving definition in the destination. Its native creation and complete
  file/member move must both qualify. The creation is saved under its original
  library, then the move changes subsequent routing; the destination catalog
  cannot relabel earlier rows or bypass capture selection. An unfinished
  creation or incomplete move cannot authorize that definition.
  Every member association must be present, and later layout/name changes cannot
  substitute a different current definition. A move during catalog acquisition
  requires a fresh observation. This does not qualify a cross-pool
  save/restore operation or a library-wide rename. Captured rows between
  unavailable intermediate SQL names remain unqualified.
  Journal-attribute changes may switch
  an excluded file between `IMAGES(*AFTER)` and `IMAGES(*BOTH)`; captured files
  continue to require `IMAGES(*BOTH)`.
  Both `OMTJRNE(*NONE)` and `OMTJRNE(*OPNCLO)` are supported. Complete before/after
  images remain mandatory, and inherited journaling remains unqualified.
  `RMTJRNFTR(*YES)` is accepted on the required local journal: it makes entries
  eligible for filtering during remote transmission, without removing local
  entries ([IBM command semantics](https://www.ibm.com/docs/en/i/7.5.0?topic=ssw_ibm_i_75%2Fcl%2Fchgjrnobj.html)).
  This does not admit reading a filtered remote journal. Ordinary open/close records are identity-checked
  metadata, not table changes or transaction completion. Empty receivers do not
  consume the entry-page budget or invent a completed output position. Long retained
  receiver chains use bounded identity-query batches and a directory observation;
  every poll still validates the required chain without a stale catalog cache.
  A verified empty detached receiver avoids another RPC page read only while its
  freshly observed native identity, range, links and recording settings match.
  A catalog observation reporting only SQL0462 warning conditions is discarded and
  retried without publishing or advancing checkpoints. After 30 consecutive
  incomplete observations, streaming or handoff-prefix publication stops with
  the underlying SQL warning; repair the catalog problem before restarting.
  A complete observation resets this limit. Other warnings, partial
  identity evidence and unverified cleanup remain errors.
- The connector creates one task, as in ordinary CDC; `tasks.max` is a ceiling,
  so an omitted or larger positive value does not create additional tasks.
  Use a runtime compatible with the connector's Debezium version.
  When the task has no source transaction context, online startup uses ordinary
  durable source offsets. Omit `exactly.once.support` and `transaction.boundary`
  for this contract; Kafka source transactions are not required. When distributed
  Connect supplies a source transaction context, retain
  `exactly.once.support=requested` and `transaction.boundary=connector`.
  The task selects and checks the contract using the actual runtime capability,
  not a user-supplied claim about the worker.
  Both paths support optional `provide.transaction.metadata=true` and require
  truncate to be absent from `skipped.operations` and `errors.tolerance=none`.
  `event.processing.failure.handling.mode` must be `fail` (the default), checked
  before native acquisition. A lossy legacy prefix cannot establish a safe
  migration boundary. Ordinary value conversion still supports
  `event.converting.failure.handling.mode=fail|warn|skip`;
  decode/history failures and failed clear dispatch remain fatal. `skipped.operations=none` emits every operation; ordinary `c/u/d`
  filters are supported. Recovery still processes all source records.
  With transaction metadata enabled, handoff waits for existing cycles to finish
  while legacy CDC continues, and native output preserves numeric commit-cycle IDs.
  Ordinary offset delivery also requires ordered processing; it rejects
  `record.processing.order=UNORDERED`. `heartbeat.interval.ms=0` retains
  the ordinary disabled-heartbeat behavior. Native recovery progress then uses the
  existing internal control topic; it does not create a public heartbeat topic.
  Exactly-once source delivery remains unsupported: recovery can replay an
  already published prefix of an interrupted transaction's output batch.
  Ordinary offsets do not provide broker fencing or atomic output/offset commits.
  An embedded application must acknowledge only successfully handled records and
  prevent overlapping connector instances. Binding history can use Kafka or
  the file store below. The tested Server JVM/HTTP profile has separate native
  evidence; other sinks need their own delivery and acknowledgement validation.
- Keep converters, naming, keys, tombstones and type handling consistent across
  workers. Standard worker key/value converter defaults may be inherited; the
  connector does not need duplicate converter settings. Producer tracing
  interceptors and partitioners remain worker-owned output choices. Recovery
  history must not be transformed by producer/consumer interceptors. The online fingerprint also pins the
  native database catalog used by the existing topic names.
  Standard `org.apache.kafka.connect.transforms.RegexRouter` chains are supported
  through the normal `transforms` settings. Connect/Engine applies each SMT;
  the connector predicts the final destination for topic preflight and checks
  acknowledgements against that destination. Data, heartbeat and control topics
  follow the chain; schema-history and offset-storage topics do not. Keep routed
  destinations distinct and preserve the required control/history partitioning.
  Data topics may be created through ordinary Connect/broker configuration;
  only control/history storage requires pre-existing ordered topics for online
  capture. Multiple data partitions retain ordinary CDC behavior but cannot
  provide cross-partition ordering between keyless truncate and keyed rows.
  New version-7 history permits output changes at task restart without changing
  the source checkpoint; older history retains its original strict policy.
  Standard payload transforms and predicates are also supported through the actual
  runtime, with exact source-partition and signed-offset acknowledgement. Their
  output topics cannot always be predicted. Keep history/control destinations
  distinct, and preserve recovery control/heartbeat records: filtering the record
  that carries a new checkpoint can prevent that offset from being written.
  `ExtractNewRecordState` in the tested Debezium version drops truncate events;
  use a truncate-aware transform when downstream consumers need `op=t`.
  Keep the existing topic naming strategy, character trim mode and time precision;
  enabling this option does not require changing those ordinary defaults.
  Native output now shares the ordinary character cleanup and trimming converter.
  Standard decimal modes, schema/table/column selection, column masking/truncation,
  `message.key.columns`, field/schema name adjustment, unchanged-update skipping
  and `tombstones.on.delete=false` are preserved. Built-in topic delimiters,
  heartbeat/transaction topic naming and source-type propagation are supported
  and fingerprinted. Explicit ordinary naming defaults retain the previous
  default fingerprint.
  New version-7 history separates these output choices from the source identities,
  physical codecs and retained history needed for recovery. Older history still
  requires its original output settings. Binary mode uses the same existing converter:
  this release does not silently fix the ordinary connector's byte output when
  another `binary.handling.mode` is configured.
  Native layout acquisition supports `CHAR/VARCHAR FOR BIT DATA`, Unicode graphic
  fields and explicit character CCSIDs using verified catalog byte lengths. A
  varying graphic prefix counts double-byte units; bounds are checked before
  decoding so it cannot read the next column. Local coverage is separate from
  fresh native validation; see [validation evidence](dcg-validation.md).
  Explicit `from.ccsid`/`to.ccsid` substitutions are supported for matching text
  fields. Physical storage lengths are still validated independently; binary,
  numeric and temporal fields are unchanged. The effective codec is saved in
  binding history and pinned in the capture fingerprint. Disabled or identical
  mappings preserve the prior fingerprint. Active mappings using implicit job
  CCSID 0 or a binary target are rejected before native connections open.
  Custom Debezium `converters` use the ordinary service registry and apply to
  native keys/values. Version-7 history permits changing converter classes and
  nonsecret options at restart; missing implementations or sensitive options reject before
  native admission.
  An explicitly configured `header.converter` and its serialization settings
  are supported and fingerprinted alongside the key/value converters. An absent
  header-converter configuration retains the previous fingerprint. Version-7
  recovery permits serialization changes at restart; older generations retain
  their original policy. Converter credentials never belong in the fingerprint.
  Output type names, precision and scale come from the same JDBC metadata API as
  legacy streaming, separately from physical storage widths. Both descriptions
  are checked against the same acquired table and saved in history. Existing
  saved generations retain their recorded descriptors on upgrade; recovery never
  substitutes today's catalog. Physical layouts support IBM i's documented
  8,000-field limit; the optional explicit RRN key adds one output field. Row,
  record and history byte budgets still apply.
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
- For Kafka-backed history, precreate distinct data topics, the configured schema-history topic and
  `__debezium-truncate.<topic.prefix>`, plus a heartbeat topic when heartbeats are
  enabled. History and control topics require one partition. Existing data topics
  may keep multiple partitions, but Kafka cannot order a null-key truncate against
  row events in other partitions. Use one partition when that ordering is required.
  Internal clients
  cannot create them on observation. Retain all binding history without compaction
  or expiry. The control topic carries checkpoint barriers, not table data.

## File-backed history with ordinary source offsets

The online path recognizes the same storage settings as other Debezium
connectors:

```properties
schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory
schema.history.internal.file.filename=/persistent/ibmi/history.jsonl
offset.storage=org.apache.kafka.connect.storage.FileOffsetBackingStore
offset.storage.file.filename=/persistent/ibmi/offsets.dat
record.processing.order=ORDERED
```

The `offset.storage` properties belong to Engine/standalone configuration, not
the distributed worker's connector configuration. Debezium Server prefixes
source properties with `debezium.source.`. Its tested JVM/HTTP profile is
[described separately](dcg-compatibility-baseline.md#deployment-evidence).
The file-history profile uses ordinary offset
acknowledgement, so it does not accept a Kafka source transaction context.
The older operator-window initialization profile remains Kafka-only.

File history does not require Kafka bootstrap addresses, a history topic or the
Kafka topic preflight. Data/control names remain distinct, and ordered successful
delivery plus durable source offsets still control handoff. A consumer must
handle the internal control destination as well as table events; discarding
records and claiming successful acknowledgement can invalidate recovery.
Sink-specific ordering, keyless-truncate support and ownership remain the
application's responsibility, as with ordinary Engine delivery.

The history file contains immutable native bindings needed to decode replay.
Keep it with its matching source offsets on persistent storage. It is **not**
transaction spill storage and must not be deleted on shutdown. A replacement may
use another path for the same retained file contents, but switching an existing
Kafka generation to the file contract is not an automatic migration. Missing,
foreign or incompatible history rejects before recovered output. Do not recreate
history from today's catalog to bypass that failure.

The connector uses a strict adapter for Debezium's JSON-lines file format. It
rejects blank/malformed records, invalid UTF-8, incomplete final records and
history beyond the applicable recovery bounds. File operations take short,
nonblocking locks; they do not grant task ownership or fence a different Engine.
Each append and recovered cut is forced to disk before it can authorize a
checkpoint. Creating storage also forces directory entries. Use a writable local
filesystem that supports these locking and durability operations, with persistent
parent directories and a regular history file (not a symlink). A storage failure
stops processing; it does not acknowledge or silently repair the damaged file.
Local Linux filesystem behavior is validated separately from any network-mounted
or platform-specific storage. File history shares the existing bounded proposal
prefix; it does not remove the native file-scope/history-volume limits.

`truncate.buffer.storage=disk` is the default; the
[disk-buffer configuration](dcg-configured-native.md#transaction-working-storage)
uses local scratch files for rows, transaction bookkeeping, lifecycle validation
and prepared output. A replacement task reconstructs from journal history and
durable offsets and does not need access to the previous task's files. Exceeding
a configured resource budget stops capture with the durable checkpoint preserved;
it does not silently evict an open transaction. This storage option does not
remove runtime admission checks or the need to retain complete native history.

Excluded rows are identity/format checked but do not consume one retained output
entry per row. Transaction starts, savepoints, lifecycle state and table identities
still include excluded files: a cycle can later touch a captured table. Thus the
transaction/scope budgets and the oldest required journal position are journal-wide.
The optional `memory` profile retains its explicit 64-cycle/10,000-entry defaults;
exceeding them requires increasing the budget or switching to disk before restart.

Boundary raw-journal audits always read bounded native pages. Captures above
1 MiB spill to private task-owned scratch files, including when transaction
storage is `memory`. They use `truncate.buffer.directory` (the JVM temporary
directory by default) and the `truncate.buffer.disk.max.bytes` budget per capture.
The directory and budget are checked before native initialization. Normal close,
rejection and stop remove owned captures; a killed process can leave scratch files
for the container or operator to reclaim. Replacement tasks never trust or reuse
these files. Checksums, exact page ordering and native identity checks remain
mandatory. This removes the raw boundary's single-page/2,000-entry limit.

Online handoff also reads its retained control and row history in bounded pages,
using scratch captures and disk-backed indexes instead of a fixed 128-page,
20,000-entry or 64 MiB total-history ceiling. The indexes record cycle starts,
terminals, prior-schema membership and ordered projections without retaining a
second copy of every raw record in the heap. Each index or reconstruction store
uses a 2 MiB page cache and at most 256 KiB of dirty-page buffering. These are
storage settings, not a bound on the whole task's heap.

This startup scratch is used even with memory transaction buffering. The
configured `truncate.buffer.disk.max.bytes` applies separately to each raw
capture and each index/reconstruction store; it is not an aggregate task quota.
Concurrent open transactions, savepoints and lifecycle work still obey the
configured runtime limits. Choose disk transaction buffering for larger working
sets. A disk failure, incomplete history, unsupported old schema or ambiguous
transaction outcome cannot advance the handoff. Normal success, rejection and
stop remove the attempt's files; a replacement reconstructs from native history
and the saved discovery floor, never from previous scratch files.

The native runtime validates before/after images, full required job/thread fields,
receiver continuity, object/member identities and relevant journal history on
startup and recovery. Operator retention and fixed-source obligations still apply.

Once a handoff barrier or F/W is durable, disabling the flag cannot fall back to
legacy streaming. Restore the matching configuration and retained history to
resume; do not reset the source to a fabricated offset.

## Verification and recovery compatibility

[Contributor testing](dcg-contributor-testing.md) lists prerequisites and commands
for local Kafka, standalone, Engine and isolated native profiles. The
[validation coverage](dcg-validation.md) distinguishes fixture checks from
recorded native observations and their limits. Local tests use controlled native
I/O; a passing local test does not qualify an IBM i release, CCSID or schema.

Native tests use fresh owned libraries, explicit fixture-write opt-in and
independent cleanup checks. The public-plugin profiles exercise ordinary startup,
existing-stream handoff, commit/rollback, unrelated output and recovery using the
assembled distribution. The option remains off by default.

Native journal metadata also requires an API job with CCSID 37. Its request and
response codecs are explicit, so a German, Danish or Japanese worker locale does
not reinterpret object names. This does not change column CCSIDs. Existing
experimental native fingerprints without this metadata policy reject; ordinary
legacy offsets can still be considered for a newly qualified handoff.

### History after the last member is removed

A physical file with no active members uses table-binding format version 3.
The retained file identity anchors recovery; its former member identity cannot
validate a new row. A later native member association supplies a new active
identity. Existing single-member and multi-member definitions keep their
previous formats.

Earlier experimental builds do not understand this memberless format and must
reject it. After such a transition has been saved, use a build that supports it
for recovery. Replacing the plugin with an older build is not a supported
rollback, and deleting offsets or history is not a safe downgrade procedure.
This table-binding format is separate from the manifest version described below.
Similarly, saved standalone member-rename and file-move transitions require a
reader that understands those transitions; earlier experimental builds are not
interchangeable.

### Output configuration changes on restart

New online binding histories use manifest version 8. Their recovery fingerprint
keeps journal/file/member identities, physical codecs, captured definitions,
source partition, history/control destinations and temporal
interpretation fixed. Output-only settings may change at a normal task restart:
transaction metadata, row-operation filters, tombstones, standard transforms,
converters, column mappings, key mappings and supported output naming options.
The task still validates the new delivery configuration before publication.

Recovery reconstructs open transactions from the same durable F/W and applies
the new output settings to records emitted by the replacement task. As with the
ordinary connector, changing a key, topic or converter is an operator decision
that can change downstream records; a crash may replay an unflushed prefix under
the new settings. This does not migrate existing Kafka records or provide
exactly-once delivery. Truncate capture must remain enabled.

Versions 5 and 6 retain their original full-policy checks, including independently
captured compatibility fixtures. No old history is rewritten or silently
reinterpreted. An unselected older proposal cannot impose its output configuration
on a separately selected newer checkpoint. Physical decoder, source identity,
unknown policy and history changes continue to reject. Version 7 retains its pinned
capture selection; version 8 also permits table/schema include and exclude changes
using the complete schemas already stored for those files. It does not snapshot
a newly included table or invent a schema for identity-only history. An in-flight
transaction is rebuilt from F and selected when it completes after W.
