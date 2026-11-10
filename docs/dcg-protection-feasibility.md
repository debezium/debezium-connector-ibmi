# D/CG recovery protection: feasibility and proposed first scope

Status: local design proposal, 2026-11-06. No provider is implemented, selected for
deployment or installed. No source connection, authority change, external message,
PR update or streaming enablement is authorized by this document. Keep F/CR PR #114
independent. This proposal implements the next design step from the
[startup protocol](dcg-startup-protocol.md#protection-contract), against
[acceptance gates P1–P8](dcg-startup-acceptance.md#retention-ownership-and-maintenance-gate-p).

**Profile scope:** retain this stronger protection proposal and all current
checks. The [local runtime profile revision](dcg-runtime-profile-revision.md) now
prefers evaluating standard Connect offsets/fencing and a smaller CDC contract
before requiring a separate registry or native deletion guard of every deployment.
The recommendations below apply to the stronger protection route; none is a passed
gate or universally selected dependency. Automated retention release and independent
restore detection remain separate capabilities. Current startup stays blocked
until the selected profile and every gate replacement are reviewed and validated.

A separate [local Kafka/Connect experiment](dcg-connect-feasibility.md) now supplies
an executable test of the publication/checkpoint portion. It uses synthetic input;
it does not implement the source protection provider or close a release gate.

The [local task/receipt prototype](dcg-task-receipt-prototype.md) now exercises
exact offset read-back and receipt crash ordering through the normal task lifecycle.
Its test authority is not a production provider and grants no source permissions.

The [maintainer review draft](dcg-maintainer-proposal.md) consolidates the current
evidence, proposed contract and decisions needed before production adapters.

## Recommendation and decision

Proceed with a **controlled feasibility prototype**, conditional on operator and
maintainer agreement. Do not claim general transactional D/CG support or wire the
observation collector into authoritative startup yet.

The smallest candidate combines:

- A dedicated receiver library and a native deletion guard that retains every
  receiver in that scope for the enrollment's entire lifetime. No checkpoint-based
  receiver cleanup in the first prototype.
- Enforced source authority and maintenance controls that cover the full recovery
  interval, including connector downtime. A separately held journal-wide writer
  fence is additionally required during initial enrollment/baseline.
- One logical connector owner, with a publication barrier verified at the actual
  Kafka Connect/producer boundary. Evaluate Connect's transactional task fencing
  first; do not equate it with this connector providing exactly-once delivery.
- Durable enrollment, ownership and receiver-manifest records, accessible to a
  replacement worker and protected from rollback with the source. Missing or
  uncertain state denies startup, takeover and maintenance permission.

This removes the first version's need for a distributed per-receiver deletion
allowlist and its garbage-collection protocol. It does **not** remove the need for
durable checkpoints, ownership fencing, protected history or lifecycle validation.
The storage cost makes this a finite, disposable validation profile, not a proposed
indefinite production retention policy. A production profile needs either a proven
existing retention service or a separately validated pin-aware implementation.

**No-go conditions:** no enforceable maintenance authority boundary; no verified
publication barrier; inability to retain the complete required history; unknown
native identity/SQL behavior; or upstream rejection of the required deployment
contract. More mocks or configuration acknowledgements cannot resolve these.

## Evidence and its limits

The following distinguishes documented mechanisms from proposed uses. The IBM
facts were available in indexed official documentation; direct page fetches were
denied during this review. Confirm the deployed release/PTF and native ABI before
implementation. Context7 did not return matching IBM i API documentation. No live
behavior has been verified by this design work.

| Evidence | Established fact | Consequence for this proposal |
| --- | --- | --- |
| E1: [IBM deletion exit][exit] | `QIBM_QJO_DLT_JRNRCV` / `DRCV0100` can veto deletion. Input includes receiver/library and possibly blank journal fields. Errors, invalid status, missing program and specified timeout cases make the receiver ineligible. Registration/removal requires `*ALLOBJ` and `*SECADM`. The default execution profile is QUSER unless configured; IPL limits include remote communications. | A source-local, bounded guard is plausible. Verify its exact failure/authority behavior on the supported release; do not make it depend on a remote service during a delete callback. |
| E2: [IBM DLTJRNRCV][delete] | `DLTOPT(*IGNEXITPGM)` bypasses the exit; IBM documents `*ALLOBJ` plus `*SECADM` for that option. | Privileged bypass is a trusted administration boundary. An exit cannot replace maintenance control. |
| E3: [Kafka 4.3 worker configuration][worker] and [user guide][connect] | Framework source support uses Kafka transactions and task-generation fencing. Source support requires distributed Connect and connector compatibility. | Evaluate the framework for publication ownership, with broker/worker tests. It supplies no IBM i retention or maintenance protection. |
| E4: local connector and dependencies | `As400RpcConnector.taskConfigs()` emits one task configuration. No exactly-once override is present in this connector or its inspected Debezium base classes. The locally installed Kafka 4.3.1 `SourceConnector` returns unspecified support and unsupported connector-defined transaction boundaries by default. | One configured task does not establish one effective publisher. No exactly-once capability declaration should be added without validating the offset/transaction semantics. |
| E5: local Debezium core | At core SHA `5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8`, `BaseSourceTask.commitRecord()` records a last offset; `commit()` requests later work and `performCommit()` forwards last offsets to the coordinator. | These methods alone are not evidence that a particular immutable checkpoint is durably committed. The real framework acknowledgement adapter remains a gate. |
| E6: current D/CG code | `Inspection` exposes observation/history/read/handoff/close. `Verified.recheck()` compares observations; the replay coordinator and normalized source adapter remain outside streaming. | A concrete protection session and ownership transfer must be added through integration. Current booleans/opaque tokens are not implementations of exclusion. |

Repository anchors: [connector](../debezium-connector-ibmi/src/main/java/io/debezium/connector/db2as400/As400RpcConnector.java),
[startup validator prototype](../debezium-connector-ibmi/src/prototype-test/java/io/debezium/connector/db2as400/As400RecoveryStartup.java),
[offset contract](dcg-transaction-design.md#recovery-checkpoint-replay-floor-and-completed-boundary), and
[current validation](dcg-validation.md#remaining-streaming-integration).
The Kafka dependency version is a local build fact, not an observation of any
deployed Connect cluster.

## Options considered

### Receiver retention

| Option | Strength | Cost or unresolved condition | Decision |
| --- | --- | --- | --- |
| Retention duration, `DLTRCV(*NO)`, monitoring, operator checkbox | Useful operational inputs | Do not prove exclusion of manual deletion, cover arbitrary lag or survive an unsafe override | Insufficient as proof |
| Existing managed retention service | Potential reuse of tested source administration | Must demonstrate every relevant P gate; no such service has been established in this work | Prefer reuse if operators can supply evidence |
| Native scoped retain-all guard | Protects all present/future receivers in a reserved library without interpreting F/W or consulting a network service | Requires installation, protected scope/registration, storage budget and a later retirement procedure | Recommended first feasibility candidate |
| Native pin-aware guard with durable registry | Allows bounded retention as durable checkpoints progress | Requires atomic allow/deny decisions, acknowledgement receipts, multiple-owner/inspector tracking, policy replication, restore protection and P1–P8 fault tests | Production follow-up, not first implementation |
| Durable raw/normalized spool replacing source replay dependency | Could eventually allow source cleanup after durable capture | Adds a second recovery log, its retention/ownership protocol and schema/lifecycle dependencies; not implemented here | Separate architecture proposal, not a shortcut |

For the retain-all candidate, reserve one receiver library exclusively for the
enrolled journal; all rollover receivers must stay in that scope. The exit must
deny deletion for the reserved library even if journal association is absent,
names are reused, the connector is stopped, or the control service is unreachable.
Library scope is a conservative denial rule, **not** object-incarnation proof.

The exit is system-wide registration. A scoped implementation must validate input
length/format before reading names, protect its scope policy, and deny ambiguous
requests. Its behavior for receivers outside the reserved scope must be explicit
and tested with other installed exits. It must never replace or remove another
product's exit. Missing/corrupt scope state must not silently allow a protected
receiver. A blanket deny-all prototype is acceptable only on a separately approved
disposable system where its impact on unrelated receivers is accepted.

Use local immutable policy or protected local storage in the callback. Do not call
Kafka, a remote registry, DNS or another network endpoint from it. A registration
listing is diagnostic, not an enforcement test. Removal of the registration is a
different failure from a registered program being unavailable: the former can
remove the guard entirely and must be excluded by authority/workflow control.

### Maintenance and initialization

| Option | What it could enforce | Limit / decision |
| --- | --- | --- |
| Catalog rechecks or a periodic lease | Detect some changes/lost contact | Cannot exclude a change followed by reversal or prevent maintenance during downtime; reject as sole protection |
| Restricted object/command/API authority plus a controlled maintenance entry point | Makes ordinary destructive administration unavailable to app/connector profiles, and permits it only after a verified barrier | Recommended candidate; exact effective-authority matrix and bypass tests are still required |
| Fully isolated source/maintenance window | Useful for disposable validation and controlled initial baseline | Pausing applications by instruction is insufficient; all journal writers must actually be fenced, including unrelated-table cycles |
| Broad native locks | May be useful for particular objects in a validated writer fence | No generic journal/receiver lock is established here; do not invent ALCOBJ coverage or treat per-call API locks as durable protection |

The authority review must cover direct CL commands, SQL DDL, APIs, adopted
authority, scheduled cleanup/backup/HA jobs, restore/clone, changes to the exit or
its policy, and changes to who can grant privileges. Restricting one command name
while another path can perform the operation is insufficient. Do not assume that
owning a disposable library or lacking `*ALLOBJ` proves effective exclusion.

App profiles need normal supported DML, including the validated clear path. They
must not retain unconstrained object/format/journaling maintenance rights. Validate
the D/CG companion lifecycle entries against that boundary: do not ban all such
entries and accidentally make the intended SQL path unusable, or allow every
schema change merely because a clear occurred nearby.

### Publication and stale workers

| Option | Behavior | Decision |
| --- | --- | --- |
| `tasks.max=1`, lease/generation field, JVM lock, pre-dispatch token check | Limits cooperative work; cannot revoke records already handed to Connect or an old producer | Insufficient alone |
| Cooperative close/drain/acknowledge barrier | Can stop one responsive owner and account for its queued work | Useful orderly-stop path; an unresponsive worker does not create a successful receipt |
| Kafka Connect transactional task-generation fencing | Broker-enforced candidate for rejecting old task producers; reuses framework rather than inventing an independent connector producer | Recommended feasibility investigation, conditional on exact worker/core/offset validation |
| Separate fenced publishing gateway | Can make generation checks and output acceptance share one authority | Adds another data path, failure domain and offset integration; fallback architecture only if maintainers accept it |
| Kill a process, delete a pod, expire a lease or change a credential | May be part of isolating an owner | Not proof that queued/in-flight requests cannot be appended later; not a complete publication barrier |

A stale truncate is more dangerous than a duplicate row update: it can clear rows
emitted by a replacement worker. Therefore the barrier must cover the data topic,
transaction metadata, tombstones, heartbeats and offset commits, including work
already removed from the connector queue. A check inside `dispatchDataChangeEvent`
cannot by itself fence the producer owned by the Connect worker.

The proposed offset design deliberately keeps W behind an in-progress batch, so
replay can duplicate records even if framework transactions atomically commit a
prefix with its conservative offset. Keep the claim **at-least-once whole-batch
replay**. Do not advertise `ExactlyOnceSupport.SUPPORTED`, claim IBM transaction
atomicity, or assume a configuration toggle aligns Kafka transaction boundaries
with IBM commit cycles. Consumers would need `isolation.level=read_committed` for a
future transactional publication profile; the existing truncate-aware, ordered sink
requirements remain. Do not invoke Connect's internal fencing endpoints as an
operator API; use supported runtime integration and test its actual effects.

If that framework cannot safely accommodate the replay contract and expose the
needed ownership barrier, the first runtime profile is blocked. Do not substitute
a lease check and proceed. Cooperative shutdown alone can support an orderly test,
but cannot close crash/reassignment acceptance cases.

## Proposed component placement and authority

| Component | Location and durable state | Allowed responsibility | Must not do |
| --- | --- | --- | --- |
| Receiver guard | IBM i program in system/basic ASP, protected local scope policy | Refuse deletion in the enrolled receiver scope across connector/controller outages | Install itself, use remote communication in the callback, or approve deletion because a lease expired |
| Maintenance controller | Operator-managed administration path plus enforced IBM i authorities | Acquire journal-wide initialization fence; permit planned maintenance only after reader/publication barriers | Infer safety from current catalog equality or Connect REST status alone |
| Protection authority | Platform-owned durable transactional store, outside the source's restore/clone unit | Atomically assign an enrollment/owner generation; persist manifests, checkpoint receipts and terminal epoch records | Treat unavailable/rolled-back state as empty or use last-writer-wins updates from competing owners |
| Worker/output adapter | Exact supported Kafka Connect runtime and connector task integration | Bind owner generation to reader lifetime, producer fencing, ordered output and durable offset acknowledgements | Interpret a connector queue enqueue or record acknowledgement as offset-store durability |
| Connector | Existing read-only evidence, transaction buffer and replay path plus future adapters | Validate proofs, close publication on loss, replay from accepted F/W | Grant itself administration rights, install exits, alter source objects or reset offsets to escape a failure |

No storage product is selected for the protection authority. Candidate reuse is an
existing operator-owned transactional registry with durable compare-and-set and
audited restore controls. A new relational service is possible but adds operations;
a compacted Kafka topic alone does not supply multi-writer compare-and-set, and a
worker-local file alone does not survive reassignment with an authoritative history.
Storage/backend choice needs operator input before implementation. A local durable
test registry can exercise crash boundaries but cannot satisfy the source gates.

Separate installation/security administration from normal connector and guard
execution. The installation role needs the documented registration privileges;
the connector must not receive them. Select a least-privilege execution profile
that can read the local guard policy even in the failure contexts being tested.
Operators must review object ownership, public authority, adopted authority,
registration protection and emergency bypass. No ready-to-run GRANT, exit
registration, deletion or maintenance commands are included here.

## Ownership, checkpoint and maintenance protocol

Use a durable enrollment ID E, monotonically ordered owner generation G, native
identity-qualified manifest revision M, and exact checkpoint identifier C. These
are proposed typed records, not replacement strings to inject into today's startup
SPI. Unknown revisions and missing evidence deny startup. The state model below
is a contract for an implementation, not a currently enforced state machine.

1. **Enroll under protection.** Establish the source guard and effective authority
   boundary before trusting history. Persist `ENROLLING(E)` outside the source
   rollback domain. Fence all journal writers, drain/census cycles, establish the
   durable baseline and initial checkpoint under the existing B gates, and only
   then make E eligible for an owner. The probe's identity tokens cannot substitute
   for this enrollment evidence. Uncertain enrollment remains blocked.
2. **Acquire one owner.** The authority compare-and-sets a generation; startup
   acquires required retention/maintenance protection before inspection. Before
   publication, verify that all older producers are fenced and the accepted stored
   checkpoint, manifest and source identities agree. Assignment by Connect or a
   clock-based lease expiration is not this proof. A new generation remains pending
   until the actual output barrier succeeds.
3. **Observe and replay under one session.** Transfer the same protected ownership
   from inspector to reader/dispatcher without release/reacquire. Continue validating
   rollover links and owned data. Invalidating evidence closes further publication
   and starts revocation; it does not silently adopt today's receiver suffix.
4. **Prepare evidence before a checkpoint.** Durably record the required manifest
   and protection for C before any output offset references C. R is only the scan
   cursor. Batch records retain the existing conservative F/previous-W snapshot;
   the subsequent safe checkpoint can advance only through the real output rules.
5. **Record actual durable acknowledgement.** Obtain a receipt for the exact
   immutable checkpoint bytes and source partition, under E/G, after the framework
   offset-store commit succeeds. Persist that receipt before authorizing any future
   pin release. An out-of-order/stale generation receipt cannot advance the registry.
   The worker adapter must establish this association; neither a generic `commit()`
   call nor the current `lastOffsets` map proves it. If the API cannot provide or
   verify it, cleanup stays disabled and the missing adapter remains a gate.
6. **Revoke before maintenance.** Compare-and-set `REVOKING(E,G)`, reject new
   acquisitions, close the reader/dispatch gate, and complete a verified output
   drain or producer-fencing barrier. Separately close or isolate source readers
   and inspectors. Only a durable combined barrier receipt permits `QUIESCED` and
   maintenance. Broker fencing alone does not terminate a source job; stopping a
   source job alone does not fence queued Kafka output.
7. **Handle failure conservatively.** Timeout, partition, controller crash or an
   unreachable old worker leaves retention in place and denies maintenance/new
   publication until the barrier is established. Cooperative close failure is not
   a release. Output already durably accepted is not rolled back. If a batch was
   interrupted, the accepted F/W still determines replay after safe ownership
   transfer; tentative R/W values cannot become recovery coordinates.
8. **Retire explicitly.** Destructive maintenance or unsupported lifecycle changes
   retire E and invalidate its checkpoints before the operation. Persist retirement
   outside the source backup/restore domain. A restored old checkpoint is rejected,
   even if its receivers happen to remain. New enrollment requires a controlled
   baseline, not edited tokens or reused offsets. The prototype never automatically
   releases its retain-all scope; eventual authorized cleanup requires terminal
   retirement, no remaining accepted checkpoint/reader, and a separately reviewed
   guard-removal/cleanup procedure.

These stores do not share an assumed atomic transaction. Crashes between manifest,
offset and receipt persistence must leave conservative retention and recoverable
pending state. Reconciliation reads the authoritative stored checkpoint and
validates exact identity/manifest references; it never fabricates a receipt from
the last record dispatched. A missing receipt may block progress or retain excess
history, but cannot justify deletion or reinterpret an earlier checkpoint.

The registry itself needs an anti-rollback/restore contract. Ordinary durable
writes do not detect restoration of an old registry backup alongside old offsets.
Revoke E before supported restores; loss of authoritative registry history requires
explicit new enrollment. Simultaneous administrative cloning of source, control
state and Kafka state is outside the claimed trust boundary, not solved by UUIDs.

## Smallest candidate support envelope

For the first feasibility run: one explicitly authorized disposable IBM i source,
system ASP 1, one local journal, an exclusive receiver library, fixed single-member
captured files, one effective source-partition owner and one partition per table
topic. Pin the tested source release/PTFs, JTOpen, connector/core and Connect/broker
versions; the local build uses Kafka APIs 4.3.1 but deployment compatibility is not
yet established. Use the real identity probe before promoting any native values.

Allow ordinary appends and validated receiver rollover only inside the reserved
scope. Require controlled enrollment and fixed capture/decoder policy. Exclude
XA/prepared/heuristic outcomes, remote failover, source clone/restore, sequence
reset, unvalidated lifecycle changes, concurrent/incremental snapshots, and
automatic receiver cleanup. Unsupported changes require explicit retirement and
new baseline. Supported local rollback/savepoint/D/CG traces still need source
validation; narrowing infrastructure does not waive transaction correctness.

Use a dedicated Connect deployment for evaluating transactional task fencing, with
operator-approved broker permissions and consumers that apply the intended
visibility rules. Do not change a shared cluster's exactly-once settings as part
of a connector experiment. An unavailable fencing path prevents takeover; it does
not trigger an automatic unsafe fallback to a new producer/connector identity.

Set a finite validation duration, measured receiver growth budget, capacity alarms
and an operator-owned source-writer stop procedure before enrollment. On pressure,
stop the experiment/writers or provide approved capacity while retaining history.
Stopping only the connector does not stop source growth. Never delete pinned
receivers or release the guard to relieve disk pressure. An environment unable to
accept this availability/storage tradeoff is unsuitable for the retain-all profile.

## P1–P8 implementation and evidence map

**No P gate is passed.** The proposed provider-specific mechanisms remain
unimplemented/unvalidated. Existing local recovery tests cover parts of the model;
the retain-all policy simplifies some obligations without closing source gates.

| Gate | Proposed mechanism | Required falsification / failure evidence |
| --- | --- | --- |
| P1 | Retain all scope covers older F, open cycles, exact probes and inspectors; production pins would use acknowledged checkpoint dependencies | Delay record/heartbeat and offset acknowledgements while R/W move; attempt source deletion and verify denial, persisted F/W unchanged |
| P2 | Evidence-before-checkpoint ordering; durable receipts; no prototype pin release | Fresh-process crashes before/after manifest, output, offset, receipt and retirement writes; accept only consistent durable state and retain on every uncertain boundary |
| P3 | Source-local guard independent of connector/network; external durable authority; missing state denies new actions | Guard/profile/policy unavailable, controller outage, worker move, source IPL and corrupt/old registry state; verify deletion refusal plus startup denial, not merely an exception in a mock |
| P4 | Guard plus effective source authority and planned-maintenance entry point; privileged bypass retires E under trusted admin procedure | Automatic/manual deletion, alternate command/API paths, exit removal/replacement, restore/rename/replacement and privileged bypass on a separately authorized disposable system; ordinary paths must be excluded, not just detected afterward |
| P5 | Combined reader/publication barrier with generation-qualified durable acknowledgement | Revoke after read, normalization, queue enqueue, worker poll, each multi-record send and offset flush; include a blocked old producer; no stale output after the accepted transfer/barrier |
| P6 | Pending revocation/transfer has no expiry-to-permission path; broker fence and source-reader isolation must both complete | Partition authority, source and Kafka separately, exceed lease/timeout, resume the old worker; deny maintenance/takeover if required proof remains unavailable and retain receivers |
| P7 | Fixed effective authorities throughout capture lag/downtime; retire before maintenance; narrow validated D/CG companion rules | Schema/member/options change and reversal, adopted-authority and scheduled-job paths, connector down; unchanged present-day catalog cannot clear the failure |
| P8 | Immutable accepted checkpoint/manifest references plus terminal epoch records; no prototype GC; strict restore policy | Restore an older checkpoint/control backup, present retired E, and propose GC while an older checkpoint/inspector is still accepted; reject unsafe recovery and never pick a newer replay suffix |

Local persistent-store and real Kafka worker tests can establish portions of P2,
P5, P6 and P8 before source access. Native deletion, authority and IPL cases require
separate live authorization; mocks cannot establish those protections. The N/H/B
gates, deployed rollback/savepoint semantics and final source/sink convergence
remain required even after all protection cases pass.

## Decisions requiring people, before runtime implementation

| Decision owner | Concrete decision / evidence requested | If unavailable |
| --- | --- | --- |
| IBM i operators/security | Dedicated disposable scope, capacity budget, permitted guard registration, execution profile, authority matrix and complete maintenance/writer coverage | No-go for protected source enrollment |
| Platform/Connect operators | Exact worker/broker versions, isolated fencing experiment, transactional permissions/consumer visibility, durable registry ownership and restore policy | No worker takeover or production registry implementation |
| Upstream connector maintainers | Whether an optional native guard/external protection contract is acceptable; packaging/ownership of native artifacts; supported runtime constraints and explicit at-least-once claim | Keep #115 blocked/draft; do not impose a new mandatory service through implementation |
| Connector implementer with maintainers | Which supported API proves the exact durable checkpoint receipt and closes the worker-owned publication path, without falsely advertising exactly-once support | No automatic retention release or streaming enablement |

No person has approved these decisions through this document and no existing
Auction Edge service is presumed to implement the contract. Prepare the proposal
for review; send it or update the PR only on explicit instruction.

## Recommended work sequence

1. Review this feasibility choice with operators/maintainers when communication
   is authorized. In parallel, identify and explicitly authorize the exact
   read-only disposable scope in the [native identity runbook](dcg-native-identity-runbook.md).
2. Build a **local real-Kafka/Connect feasibility test**, without IBM i, for an old
   blocked task, queued truncate/reload output, conservative F/W offsets and a
   replacement generation. Demonstrate broker fencing, consumer-visible ordering
   and exact offset durability. If the current replay contract is incompatible,
   stop and revise the design before declaring connector exactly-once capability.
3. Only after the source-side scope is agreed, prepare a separately reviewed native
   retain-all guard prototype and authority-test plan. Installing/running it requires
   new authorization; this document does not perform that work.
4. Implement durable enrollment/manifest/receipt storage and protected ownership
   transfer against the selected adapters. Exercise the P matrix and then H/B and
   the actual reader/dispatcher recovery path. Keep the transactional clear guard
   until all release criteria are satisfied.
5. Evaluate bounded production pin release as its own follow-up. Do not expand
   scope to automatic cleanup, shared-source failover or restore detection merely
   because the finite retain-all experiment succeeds.

This sequence is a conditional route to evidence. No installation, source mutation,
Kafka deployment, external message, push or permission grant was performed here.

[exit]: https://www.ibm.com/docs/en/i/7.4?topic=ssw_ibm_i_74%2Fapis%2FXDLTRCV.htm
[delete]: https://www.ibm.com/docs/en/i/7.5.0?topic=d-delete-journal-receiver
[worker]: https://kafka.apache.org/43/generated/connect_config.html#exactly.once.source.support
[connect]: https://kafka.apache.org/43/kafka-connect/user-guide/#exactly-once-support
