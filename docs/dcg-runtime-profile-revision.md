# D/CG runtime profile revision

Later configured native work is recorded in [the configured profile](dcg-configured-native.md)
and [its validation report](dcg-configured-validation.md). Statements below about
test-only admission describe the historical stage covered by this document.

Status: **local design revision with partial Kafka validation**, 2026-11-06. This
document proposes a smaller first support profile; it does not implement or enable it.
No maintainer/operator agreement, production compatibility or new live validation
is claimed. Keep F/CR PR #114 independent and D/CG PR #115 draft.

## Decision and relationship to the current implementation

Keep transaction buffering and whole-batch F/W recovery. Prefer Kafka Connect's
normal source-offset storage and transactional task fencing before introducing a
second ownership/checkpoint authority. Separate ordinary CDC recovery from optional
automated retention release and independent restore/offset-rollback detection.

This revises the **preferred architecture to investigate**, not the current
acceptance contract or runtime. The existing
[protected startup protocol](dcg-startup-protocol.md),
[protection proposal](dcg-protection-feasibility.md) and N/P/H/B gates describe a
stronger contract. Their guards, receipt reconciliation and regression tests stay
intact. They must not be satisfied with fabricated tokens, permissive booleans or
operator acknowledgements standing in for native proof.

Before a future implementation replaces a mechanism, record the accepted profile,
its supported failure/administration boundary, the replacement evidence and the
explicit gate mapping in the [acceptance matrix](dcg-startup-acceptance.md#candidate-cdc-profile-gate-c).
An old obligation is either preserved, met by a verified alternative, or explicitly
outside an approved future profile; it is never silently marked passed. Until that
decision and evidence exist, the current stricter checks continue to block startup.
No production runtime setting, offset migration or exactly-once capability declaration is
introduced by this revision.

## Guarantees common to every supported profile

- Buffer all captured operations in each ordinary commit cycle, including row-only
  cycles. Publish only after a supported durable commit. Full rollback publishes
  no changes; savepoint rollback removes exactly the canceled changes. Unknown,
  incomplete, non-durable or unsupported outcomes stop capture.
- Materialize owned values and preserve row, object/member, format and original
  source metadata. Preserve within-cycle order and serial commit order across
  cycles. Cycle-zero operations need their existing integration gate; this revision
  does not enable them or expand nonzero-cycle F/CR support.
- Keep R as volatile scan progress, F as the earliest required replay cursor, and
  W as the completed-output boundary represented by the checkpoint. F and W are
  versioned and stored together with their identity and capture-policy context.
  In-memory completion is not durable output acknowledgement.
- Every record in a batch, including tombstones, primary-key change subrecords,
  BEGIN/END and internal heartbeats, carries immutable `(F, previous W)`. Advance
  the in-memory boundary only after every required dispatch succeeds; publish the
  new checkpoint on a subsequent ordered record/heartbeat. Only the checkpoint
  actually committed by the framework governs a new instance's recovery.
- Rebuild from F, suppress completed batches by their completion position relative
  to W, and finish replaying an interrupted batch before later batches. Preserve
  the current full-batch at-least-once claim, including temporary partial state
  and duplicates. Do not substitute scan position or discard long transactions
  to fit retention or memory limits.
- Validate source/object identity, receiver continuity, historical decoding scope
  and capture fingerprint. Required history gaps and ambiguous controls stop
  capture; no newer-suffix fallback, lost-journal reset, oversized-entry skip or
  implicit legacy-offset migration is allowed in the buffered path.
- Fail required dispatch, conversion, serialization and delivery errors. Exclude
  transforms/error policies that drop or reorder required output. Use one Kafka
  partition per table and a truncate-aware sink that applies clear/reload in order;
  do not deduplicate reload rows while reapplying their repeated clear.
- Establish a consistent initial baseline and complete transaction starting state.
  The present writer-fence/census checks remain required. A future alternative
  must prove an equivalent snapshot/history handoff, including cycles that begin
  on unrelated tables and later touch captured files.

The detailed [transaction/restart algorithm](dcg-transaction-design.md#recovery-checkpoint-replay-floor-and-completed-boundary)
and [output adapter rules](dcg-output-adapter.md) remain unchanged.

## Candidate first CDC profile

### Scope and administration assumptions

Evaluate one approved distributed Kafka Connect deployment, one stable connector
identity and source partition, one IBM i journal on system ASP 1, fixed
single-member files, fixed schema/decoder/capture policy, and bounded in-memory
transactions. Pin and test the worker, broker, connector/core and IBM i release/PTF
combination. Standalone/embedded runtimes, duplicate independent connectors,
cross-cluster takeover, source restore/clone/failover, journal sequence reset,
unvalidated schema/member changes, XA/prepared outcomes and concurrent/incremental
snapshots remain outside the initial profile.

Operators retain required journal history, protect offset/configuration access,
prevent unsupported source changes, and use an explicit recovery procedure after
history loss or maintenance. This is a conditional support contract, not automatic
protection against every privileged administrative action. Neither an outage
duration nor a configured retention interval proves history complete.

The connector does not install a deletion exit, delete receivers, release source
retention pins, authorize maintenance or advance a separate protection service in
this candidate profile. If an installation needs those actions, it needs the
additional protocol below. No existing service is presumed to implement it.

### Checkpoint authority

Prefer the framework's durable source offsets as the authoritative restart
checkpoint. F/W and bounded identity/fingerprint data fit this responsibility;
the connector must still supply correct immutable offsets. A second copy in a
custom receipt database is not inherently needed solely to restart a buffer.

This does not establish that all evidence fits in one offset. Source lineage,
schema/decoder history and initial-baseline evidence must be recoverable on a new
worker. Evaluate whether bounded anchors plus retained native history can establish
continuity, or whether separate durable metadata is required. Do not put an
unbounded receiver manifest into every SourceRecord or call today's catalog a
historical manifest. Acceptance C5/C6 must settle this before replacing a provider.

Keep `As400CheckpointReceipts` and its store contract unchanged. In the existing
prototype, an accepted receipt requires reconciliation; missing Kafka offsets
must yield zero output and leave the receipt unchanged. A future profile without
receipts must preserve the intent through an explicit initialization/resume
workflow: normal resume with missing, malformed or incompatible offsets fails.
Approved first initialization must be distinguishable using evidence that survives
loss of the source offset. No new mechanism or configuration syntax is selected
here. If that distinction cannot be established, startup remains blocked.

Offsets cannot prove that they and all related initialization/configuration state
have not been restored to an older consistent-looking backup. The candidate does
not promise such independent rollback detection. Loss or restoration of that state
is unsupported for automatic resume and requires controlled recovery; stronger
detection needs an independently protected authority as described below.

### Publisher ownership and Kafka transactions

Prefer Kafka Connect's framework source transactions and fencing of old task
generations. The framework owns the producer; do not add an independent producer,
invent a lease that supposedly fences it, or invoke private fencing endpoints.
Stable Connect group/connector/task identity and controls against independent
duplicate deployments are part of the candidate's operating scope. This is not a
global source lock across arbitrary connector names or Connect clusters.

Framework transactions atomically persist Kafka records and the offsets supplied
with them. They cannot correct an unsafe F/W, decode rollback, retain IBM i history
or make a source transaction spanning multiple polls atomic. Consumers of the
transactional output must use `read_committed`. Our conservative offsets still
allow an already committed Kafka prefix to repeat as part of whole-batch replay;
no source exactly-once guarantee follows from enabling framework transactions.

The local experiment used Kafka/Connect 4.3.1,
`exactly.once.source.support=enabled`, `transaction.boundary=poll`, and a probe
declaring `ExactlyOnceSupport.UNSUPPORTED` with `exactly.once.support=requested`.
The [recorded local evidence](#recorded-local-kafka-evidence) now covers selected
publication/recovery cases without custom owner or receipt gates. Those are
experiment facts, not a supported production recipe. The remaining C2/C3/C9 cases
must establish supported compatibility and failure behavior without changing the capability
declaration merely to pass configuration validation. Failure to find a supported
framework path blocks this candidate; it does not justify unfenced takeover.

Producer fencing does not stop an IBM i job. Ordinary takeover might tolerate a
stale **read-only** reader if it can neither publish accepted output nor change
shared checkpoints, release history, authorize maintenance or interfere with the
replacement's correctness. That is a hypothesis for C4, not a relaxation of the
current reader barrier. Any maintenance/retention action that depends on readers
being gone still needs the full source-reader isolation protocol.

### Source retention and failure behavior

Investigate operator-managed receiver retention plus reliable detection of missing
history, instead of making a native deletion veto universally mandatory. Coverage
must reach the oldest durable F across open transactions, downtime and replay;
recent scan progress is not a sufficient retention target. Observe and alert on
oldest required receiver, transaction age, replay distance and remaining capacity.

This alternative is acceptable only if exact IBM i APIs and retrieval behavior
establish completeness and identity throughout startup, page retrieval and handoff.
Test missing first/middle/boundary receivers, rollover, filtered controls, partial
responses, deletion during retrieval and identity change-and-change-back. An error
after required operations escaped or W advanced is not adequate detection.

Use trusted administrative restrictions or proven enforcement for changes that
cannot be detected reliably, including historical schema/member changes and
unsupported restore. Equal before/after observations do not prove exclusion of
an intervening change. If C5/C6 cannot justify a safe native history boundary, retain
the stronger protection requirement or reject the candidate profile. Do not
promote the existing candidate SQL collector to authoritative startup proof.

## Recorded local Kafka evidence

The [timeout-profile matrix](dcg-timeout-profile-validation.md) completed twelve
fresh-broker runs: two repetitions of each combination of an additional 0/12-second
isolation hold and a 0/5/25-second delay before removing the replacement worker.
All twelve passed all seventeen checks. The candidate used the real task/dispatcher
and F/W coordinator, synthetic journal input, separate single-partition data,
transaction and heartbeat topics, one record per poll, and no custom publication
owner or receipt authority. The source startup and ongoing native reader were not
installed. Whole-batch at-least-once remains the delivery contract.

The fixed local worker overrides were `request.timeout.ms=5000`,
`default.api.timeout.ms=10000`, `worker.sync.timeout.ms=20000` and
`rebalance.timeout.ms=30000`. The existing comparison-only administrative stop
HTTP deadline was 60 seconds. Each run needed one existing bounded retry for
Kafka's explicitly retryable config-writer response; no source publication or
failed assertion was retried. The unchanged 100-second reassignment wait passed.
The 14.264–20.531-second measurements start after the kill command completes and
end at the prepared-task observation; they are not a recovery SLA or the same
budget as a single config refresh.

The unmodified original configuration still failed its control run's reassignment
wait. Independent broker evidence afterward showed complete config history,
unchanged committed output and exact F=W=6. Preserve this failed reproduction;
the passing candidate does not turn it into a pass or establish universal timeout
defaults. See the [catch-up investigation](dcg-config-catchup-investigation.md)
for delayed administration-client recovery and the observation limits.

The earlier [ownership experiment](dcg-connect-ownership-experiment.md) also
recorded successful suspended-producer broker fencing, delayed-ack/completed
restart, interrupted-batch and key-change/tombstone replay, and a separate
missing/malformed/incompatible-offset rejection run. Its full run failed later
at partition/reassignment. Those are historical scenario results; the twelve-run
matrix repeated only the partition scenario under the fixed candidate profile.
Network-partition stale sends were rejected by local producer close; they are
not twelve new observations of broker producer-epoch fencing.

| Responsibility | What the local evidence supports | What remains before replacing a current mechanism |
| --- | --- | --- |
| Kafka publication ownership | Standard Connect mechanisms protect the tested candidate output without a separate owner/receipt gate; new tasks load W=6 and publish through W=9 | Complete C2/C9 profile and failure coverage, C4 reader isolation and C12 accepted scope; retain current checks meanwhile |
| Durable F/W recovery | Framework offsets support the tested resume/replay and fail-closed missing-offset cases without a second checkpoint authority in that candidate | C3 remaining boundaries, C5/C6 durable native history/decoder evidence and C7/C8 production initialization/resume distinction |
| Native reader and startup safety | No native claim follows: source startup/schema/journal input are synthetic | C4-C7 and all applicable N/P/H/B obligations remain open; SQL identity candidates remain observations only |
| Retention release and independent restore detection | No cleanup or maintenance permission is granted | Keep their separate stronger protocols and current receipt regressions; Kafka output success cannot replace their authority |

This advances the evidence for preferring standard Connect publication and offset
storage. It does not approve deleting a receipt implementation, removing the
reader barrier, changing capability declarations or enabling buffered streaming.
The [acceptance checklist](dcg-startup-acceptance.md#recorded-local-evidence-and-open-gates)
records partial evidence separately from full gate completion.

## Next implementation step: test-only reader lifecycle harness

The fixture-backed incremental reader lifecycle harness is now implemented for
the **local portion of C4**, retaining the real task/dispatcher and fixed candidate
Connect profile. See [implementation, results and blockers](dcg-reader-lifecycle-validation.md).
The original probe still prepares output during task start and is unchanged; the
new separate probe exercises an ongoing RPC reader. The
[cancellation follow-up](dcg-reader-cancellation-validation.md) fixes the discovered
recursion for registered in-flight reads and adds handle/temporary-connection
cleanup. The [startup/stop boundary tests](dcg-reader-startup-stop-validation.md)
exposed post-close reconnect/new-call admission during discovery/setup. The
[terminal-stop follow-up](dcg-terminal-stop-validation.md) closes the local gap
with permanent admission rejection, cancellation and draining. Ordinary close
remains reconnectable; a hung driver leaves terminal stop pending. Full C4 remains
blocked on native behavior and accepted runtime integration; keep the source-reader
barrier and transactional streaming guard.

The harness uses the actual `As400RpcConnection.getJournalEntries`, reconnect/close and
`RetrieveJournal` control flow, with controlled JT400 calls/job handles and raw
fixture responses at the transport boundary. It does not copy the read loop or substitute
an owner token for producer rejection. Test-scope `As400ReaderLifecycleTest` and a
separate incremental Connect fixture preserve the original probes and stricter
tests. Existing reflection and constructor interception suffice: no production
test seam was needed, and no IBM i connection was made.

This step must account for operations beyond reading journal bytes:
`As400RpcConnection.close()` invokes `RetrieveJournal.cancelJob()`, which calls
`Job.end(0)` using a separate connection; retrieval also changes its server job's
logging level. The lifecycle is therefore not assumed read-only. Inventory those
effects and require that all test cancellation/cleanup targets belong to the old
reader, never the replacement. Mocked native identities establish test isolation,
not IBM i job identity lifetime or permission semantics.

The [C4 local checklist](dcg-startup-acceptance.md#next-local-implementation-checklist-c4)
specifies blocked/late reads, reconnect/close races, independent reader state,
actual committed-output/checkpoint oracles and bounded fixture cleanup. The
completed passing cases and cancellation fix add local lifecycle evidence only.
Native job cancellation effects, release/PTF behavior and stale-job interference
still need separately authorized disposable
IBM i validation. C5/C6 continuity and C7 initialization remain independent blockers.
No custom production dependency is removed by the harness or its cancellation fix.

## Optional stronger guarantees

### Automated retention release and maintenance authorization

The [existing protection protocol](dcg-protection-feasibility.md#ownership-checkpoint-and-maintenance-protocol)
remains the candidate for installations that want automatic permission to remove
history or perform coordinated maintenance. Its durable owner/predecessor checks,
manifest-before-offset ordering, exact committed-checkpoint receipts, dependency
pins, reader/publication barriers and conservative behavior on uncertain state
remain necessary obligations unless an equivalent mechanism is proved.

A task enqueue or `commitRecord` acknowledgement cannot authorize deletion.
The inspected Debezium `BaseSourceTask` keeps callback-derived last offsets and
forwards them later; that alone does not identify the exact durable checkpoint
needed by an external cleanup authority. Do not weaken the receipt regression or
infer source cleanup permission from framework transactional support.

Reuse an existing supported authority if it can meet the contract; no storage
backend or native guard is selected. A guard that retains every receiver can be a
bounded disposable validation arrangement, but is not an indefinite production
retention strategy. Its administration, capacity and retirement rules remain gates.

### Independent restore and offset-rollback detection

If support must detect retired enrollments, old offset backups, source replacement
or source/metadata restore independently, retain an authority outside the relevant
restore domain, protected terminal enrollment records and explicit re-baselining.
A UUID, native object tuple, compacted topic or matching checkpoint copies does not
by itself supply that independence. Restoring both copies can erase the evidence.

This guarantee requires its own administrative trust model and fault tests. It is
separate from ordinary process-crash recovery and does not follow from Connect
producer fencing. The current stronger protocol and tests remain intact; this
revision proposes not requiring that service of every future CDC installation.

## Acceptance and staged adoption

The [C1-C12 cases](dcg-startup-acceptance.md#candidate-cdc-profile-gate-c)
describe how to evaluate this candidate and map it to the existing gates. Selected
local publication/recovery subcases now have recorded passing evidence. No complete
C or N/P/H/B release gate is newly passed by this document, and no native source or
external authority has been validated by the Kafka matrix.

1. Review the explicit support boundary and resolve initialization-state,
   framework-compatibility and native-continuity questions. Record a per-gate
   decision before proposing any replacement of a current mechanism.
2. The [terminal-stop/admission protocol](dcg-terminal-stop-validation.md) is now
   implemented and locally verified: new source work cannot start after stop
   completes, and admitted work must drain first. The [task-stop integration](dcg-task-stop-validation.md) now exercises the
   real coordinator/streaming/task lifecycle through local Kafka reassignment,
   plus stopped-context RPC/JDBC retries. A live isolated embedded smoke test
   passed ordinary capture/cleanup. Next validate native cancellation and stale-job
   isolation with controlled fault cases; do not infer a shutdown deadline from
   local transport models or the native smoke test. Retain current receipt,
   missing-offset, false-dispatch and startup regressions and extend the remaining
   Connect cases. Use fresh processes and actual committed offsets/output as the oracle.
3. Obtain release-specific source evidence, then validate the full reader,
   dispatcher, heartbeat and truncate-aware sink. A read-only edgeqa inspection can
   establish available versions/APIs/settings; it cannot establish destructive
   race, rollback or crash behavior. Such tests need a named disposable scope and
   explicit operation/cleanup authorization. Existing auctions are not fixtures.
4. Only after the approved gate mapping and all required evidence pass, propose
   production wiring or a changed support claim in separate work. Until then the
   nonzero-cycle clear guard, legacy/versioned-offset restrictions and startup
   rejection remain in force. No change to F/CR PR #114 is needed.

## Comparative evidence and limits

Reviewed against Debezium core commit
`5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8` and official documentation on 2026-11-06:

| System | Relevant precedent | Limit of the analogy |
| --- | --- | --- |
| [Oracle LogMiner](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-property-log-mining-buffer-type) | Default memory transaction buffer rebuilds from stored `scn`; `commit_scn` and finer transaction/event progress distinguish completed work. [Recovery explanation](https://debezium.io/blog/2023/06/29/debezium-oracle-series-part-3/) describes retained history reaching the oldest open transaction. | Operators must retain archive/redo history. Oracle TRUNCATE itself is not user-rollback-capable; buffer semantics are the precedent, not proof of IBM i clears. |
| [PostgreSQL](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html) | Normal pgoutput resolves committed/rolled-back transactions server-side. [Slots](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) retain replay resources. | IBM i raw journal retrieval is not that API. Slot retention also has configured limits and administrative failure modes. |
| [MySQL](https://debezium.io/documentation/reference/stable/connectors/mysql.html) | Ordinary InnoDB binlog transaction groups, detailed resume offsets and operator-configured binlog retention. | Temporary/nontransactional mixes have additional handling; [TRUNCATE](https://dev.mysql.com/doc/refman/8.4/en/truncate-table.html) implicitly commits and cannot be rolled back by the user. |
| [SQL Server](https://debezium.io/documentation/reference/stable/connectors/sqlserver.html) | Native CDC change tables and commit/change LSN restart positions; [cleanup](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server) uses a retention window. | The native capture engine does upstream work. SQL Server rejects TRUNCATE on CDC-enabled tables (error 4711); it does not demonstrate transactional clear capture. |
| [Db2 LUW](https://debezium.io/documentation/reference/stable/connectors/db2.html) | ASN Capture change-data tables, commit/change LSN offsets and an explicit pruning configuration. | A separate connector/product from Db2 for i; ASN is not available merely by using this IBM i connector. |
| [Kafka Connect 4.3](https://kafka.apache.org/43/configuration/kafka-connect-configs/#exactly-once-source-support) | Framework records/offset transactions and old-task fencing in a compatible distributed deployment. [Runtime/identity requirements](https://kafka.apache.org/43/kafka-connect/user-guide/#exactly-once-support) define its scope. | No IBM i history protection, complete-cycle baseline, external-store atomicity or automatic source exactly-once guarantee. |

The historical local output experiment passed 53 assertions with synthetic source
startup and a local fixture authority; its outputs shared one topic/partition.
It did not validate this smaller profile. The later ownership experiment and
twelve-run timeout matrix provide the scoped candidate evidence recorded above.
This documentation update records existing results; it runs no new runtime tests and does
not change historical unit-test counts or claim new CI/native evidence.


## Continuous native runtime checkpoint (2026-11-07)

The [continuous runtime validation](dcg-continuous-runtime-validation.md) supersedes
controller-triggered cuts for the candidate test entry point. One live coordinator
now retains transaction and lifecycle state across autonomous incremental reads.
Each interval validates durable F/W and native identity/retention evidence; scan R
is separate and never becomes a durable restart checkpoint. Receiver extension
preserves the earlier ranges needed by open transactions. Both snapshot and actual
dispatcher output use acquired native schemas, including mixed column types.

The bounded edgeqa/local Kafka run passed 47 assertions: open-clear unrelated
progress, receiver rotation with F in the predecessor, fresh-task replay, commit
and rollback of both native clear forms, old-producer fencing, legitimate
reassignment and missing-offset zero-output rejection. All fixture/local cleanup
passed. The complete current unit suite and assembly checks passed; existing
stricter experiments remain unchanged. See the linked report for raw evidence,
diagnostic failures, exact profile and verification counts.

This closes the controller-cut and integer-only-output limitations of the previous
native test connector. Production transactional streaming remains disabled.
General production initialization still requires a qualified journal-wide writer
and maintenance fence; the controller's ownership of an isolated fixture is not
that provider. Deployment/PTF qualification, arbitrary schema/DDL handling, load,
broker-loss durability and downstream truncate behavior remain separate gates.
The next integration work must define that production initialization provider and
map the qualified continuous path into the normal task lifecycle without weakening
retention, source identity, missing-offset or stop/drain checks. Enabling the
production guard is not justified by this bounded experiment alone.

## Normal task recovery integration (2026-11-07)

The [normal task integration](dcg-normal-task-integration.md) now maps validated
resume state into `As400ConnectorTask` and the real coordinator through test-only
admission. Its durable output/callback barrier and terminal stop/drain handling
reuse the continuous runtime. The public task cannot select this path, missing
offsets cannot initialize it, and normal transactional streaming remains disabled.
The next integration boundary is explicit native initialization and snapshot
acknowledgement in the normal task, with a qualified source-session/fence provider.
The current local checks do not supply that native authority or replace the
previous isolated edgeqa validation.
