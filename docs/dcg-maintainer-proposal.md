# D/CG transactional capture: proposal for maintainer review

This is the historical proposal. The selected operator-retention implementation,
normal task integration and current evidence are now tracked in the
[source release checklist](dcg-source-release-checklist.md) and
[configured profile](dcg-configured-native.md). Historical test counts and open
implementation decisions below must not be read as the current release status.

Status: **local discussion draft**, November 6, 2026, for
[debezium/dbz#3027](https://github.com/debezium/dbz/issues/3027) and
[draft PR #115](https://github.com/debezium/debezium-connector-ibmi/pull/115).
The implementation and evidence summarized here include uncommitted work on
`dbz-3025-dcg-followup`; they are not a statement about the published PR's current
contents. No maintainer or operator decision below has been approved.

**Local revision:** the [runtime profile revision](dcg-runtime-profile-revision.md)
now prefers standard Kafka Connect offsets and publisher fencing for a candidate
first CDC profile. Automated retention release and independent restore detection
are separate optional guarantees. This revision has not been published to #115 and
does not replace any runtime guard or mark an existing acceptance gate passed.

## Problem and requested decision

IBM i SQL clears can generate D/CG subtype 8 for TRUNCATE or subtype 3 for an
optimized bulk DELETE, without individual row-delete entries. A clear within a
commit cycle can be rolled back. Emitting Debezium's irreversible table-wide
`op=t` before that outcome is known can empty a downstream table even though the
source retains its rows. After a worker replacement, a late truncate from the old
worker can also erase the replacement worker's reload.

The proposal is to buffer complete commit cycles, deliver committed batches in
commit order, and recover from retained journal history with conservative offsets.
Local implementations and Kafka experiments now exercise that model. The next
decision is whether a smaller supported runtime/source contract can meet ordinary
CDC recovery needs using existing framework mechanisms, and which installations
need the additional protection protocol, before production wiring.

Keep [F/CR PR #114](https://github.com/debezium/debezium-connector-ibmi/pull/114)
independent at its existing scope. Keep #115 draft and its nonzero-commit-cycle
clear rejection active until the release gates pass. This proposal does not add a
runtime flag, select a storage service, or authorize source administration.

## Evidence available for review

| Evidence | What it establishes | Limit |
| --- | --- | --- |
| [Recorded IBM i 7.5 observations](dcg-validation.md#live-observations) | D/CG subtypes 3 and 8 were decoded through the real RPC path with nonzero commit cycles; rollback restored all 1,200 synthetic rows in the recorded cases. | Historical disposable-source observations, not complete connector-to-sink transactional validation. No new source test was performed for this proposal. |
| [Local implementation and unit tests](dcg-validation.md) | Parser, bounded transaction model, owned source normalization, separate replay/checkpoint positions, and candidate startup validation. The final local full verification passed 732 unit tests, including 23 receipt and 31 committed-output cases; formatter/import checks passed. | Synthetic proofs and model tests do not establish native object lifetime, enforced source protection, or deployed SQL/savepoint semantics. These are local results, not upstream CI results. |
| [First Kafka/Connect experiment](dcg-connect-feasibility.md) | 19 assertions covered conservative offsets, whole-batch replay, and rejection of a resumed stale producer in the pinned runtime. | Synthetic source task; no IBM i reader or complete Debezium dispatch lifecycle. |
| [Normal task and receipt prototype](dcg-task-receipt-prototype.md) | 53 assertions cover real task polling/dispatch/commit paths, exact offset read-back, receipt write crashes, stale callbacks, missing-offset startup rejection, and normal shutdown. The [output adapter](dcg-output-adapter.md) supplies real transaction metadata, tombstones and heartbeats, including complete replay after an interrupted primary-key change. | Source startup and authority remain fixtures. The output experiment routes all records to one topic/partition; the production idle reader/scheduler, SMTs, multiple output topics and complete source startup remain unvalidated. |
| Independent actual-file review and follow-up | A missing-offset restart gap was corrected: an existing receipt requires reconciliation before publication; a missing Kafka offset rejects startup. The Kafka regression verifies zero new output and an unchanged accepted receipt. A fresh review passed; 23 focused receipt tests passed again. | Review of the local implementation and matching evidence, not approval of the production architecture or a native release gate. |

The local build uses connector version `3.8.0-SNAPSHOT`, Java source target 17,
build JDK 21, and Debezium core commit
`5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8`. The experiments pin Kafka/Connect 4.3.1
and a Docker image digest; their reports record source/plugin hashes, actual
offsets, broker records, failure markers and cleanup. Those versions are an
**experiment profile**, not an approved compatibility matrix. The launcher and
evidence format are documented in the linked experiment pages; generated logs and
machine-specific paths are not required by this proposal.

The tested worker uses `exactly.once.source.support=enabled`; the test connector
declares `ExactlyOnceSupport.UNSUPPORTED` and requests
`exactly.once.support=requested` with `transaction.boundary=poll`. This exercises
framework transactions while retaining the at-least-once source contract. It does
not settle the settings or compatibility of an eventual production profile.

## Proposed behavior and initial scope

Buffer every captured change in an ordinary local commit cycle, including
row-only cycles. Waiting for the first clear would allow earlier changes to
escape. A committed `insert A; clear; insert B` produces `c(A), t, c(B)` in that
order. A full rollback produces no data output. Savepoint rollback must remove
only the affected buffered changes; unknown or ambiguous controls stop capture.
The supported source release must supply validated control traces before support
is claimed.

Drain one batch at a time in durable commit-entry order, preserving operation
order within the batch. A supported zero-cycle operation is a singleton batch in
the same order. Nonzero-cycle F/CR remains excluded unless separately validated.
Preserve each event's original source identity, sequence and timestamp even when
commit-order delivery makes those sequence values appear out of order.

The delivery contract is **whole-batch at-least-once replay**. An interrupted
truncate/reload batch can repeat in full before subsequent batches. This permits
temporary partial downstream state and duplicate events. It is not an atomic
source-transaction publication guarantee or a source exactly-once claim. A sink
must apply keyless truncates and subsequent reload rows in order; dropping only
the repeated reload rows while reapplying the truncate is unsafe.
A future profile relying on Kafka transactional visibility also requires consumers
to use `isolation.level=read_committed`; that still does not make one IBM i commit
cycle atomic across its output records or topics.

The first candidate for source validation is deliberately narrow: one explicitly
authorized disposable IBM i source, system ASP 1, one local journal, fixed
single-member files and decoder/capture policy, one effective publisher per
source partition, and one Kafka partition per table topic. Use finite memory,
event and active-cycle bounds; exceeding them stops capture before unsafe output.
Exclude XA/prepared/heuristic outcomes, unvalidated partial/non-durable outcomes,
remote failover, source restore/clone, sequence reset, unvalidated DDL/member
changes, and concurrent/incremental snapshots.

Evaluate operator-managed retention with proven history/identity validation for
the candidate first profile. It must cover the oldest durable replay floor across
open transactions, downtime and replay; a duration setting is not proof of that
coverage. If missing history or relevant lifecycle races cannot be detected before
unsafe output, retain a proven protection mechanism or reject that profile.

The earlier retain-all native-guard proposal remains an option for bounded,
disposable validation and the stronger protection contract. It is not universally
selected for the first release or an indefinite production policy. Automatic
receiver deletion and checkpoint-based retention release remain separate work.

## Checkpoint and ownership contract

| Position | Meaning | Authority |
| --- | --- | --- |
| R: scan cursor | How far the reader has inspected the journal. | Volatile progress only; never sufficient to authorize restart or receiver release. |
| F: replay floor | Earliest retained position needed to rebuild unresolved cycles or incomplete output, with an explicit inclusive/exclusive boundary. | Stored together with W, journal/receiver identities and capture fingerprint. |
| W: completed-output boundary | Completion boundary represented by the checkpoint. | In-memory W can advance after successful dispatch; only the exact durably stored checkpoint can govern restart. Enqueue and individual record acknowledgement do not establish durability. |

Every SourceRecord within a batch must carry an immutable F/previous-W snapshot,
including all records produced by one row operation, tombstones and metadata.
Publish the advanced checkpoint only on a subsequent ordered record/heartbeat.
The local output adapter stages real heartbeat and transaction records; the production reader/idle scheduling path still needs integration.

For the candidate CDC profile, prefer the framework's durable source offsets as
the restart authority and its transactional task-generation fencing as the
publisher barrier. The connector still owns correct F/W, replay, identity and
baseline semantics. No second receipt store is inherently required solely to
rebuild the transaction buffer. Any separate lineage/schema/baseline evidence must
still be durable and accessible to a replacement worker.

Framework producer identities are scoped to the Connect group/connector/task;
independent duplicate connectors or clusters are outside the proposed first
profile. Fencing does not stop IBM i jobs, and Kafka poll transactions do not make
our conservative whole-batch replay exactly once. A supported deployment and
consumer configuration must pass [C1-C12](dcg-startup-acceptance.md#candidate-cdc-profile-gate-c).
The probe's `UNSUPPORTED`/`requested` combination is not a production recipe.

Normal resume with missing offsets must fail. A future initialization workflow
must distinguish an approved fresh baseline from an existing installation whose
offsets were lost; it cannot infer that distinction from the missing offset alone.
The current missing-offset/receipt reconciliation checks and regression remain
unchanged. The revision defines no new configuration or automatic migration.

### Optional stronger protection protocol

The following is the current receipt/enrollment protocol. It remains required by
the existing protected-startup implementation and is a candidate for optional
automated retention release, maintenance authorization and independent restore
detection. Separating those capabilities does not remove their current checks.

Bind a durable receipt to enrollment E, owner generation G, manifest revision M,
source partition and exact versioned checkpoint C. Exactness includes all field
names, string values and types, independent of incidental JSON key order. The
receipt store must atomically verify the owner and predecessor receipt and persist
before returning success. An unavailable authority, stale owner, changed scope,
regressed checkpoint or lost required record stops the protocol.

An implementation claiming that stronger protocol must satisfy these boundaries:

1. Establish protected enrollment, native identities, complete retained history,
   a journal-wide writer fence/cycle census and an acknowledged initial baseline.
   Persist complete evidence before an offset references it. The fixture's W=1
   baseline is not a production initialization policy.
2. Acquire a new generation as pending. Before publication, prove the old worker's
   output is fenced and the required source-reader barrier has completed. Neither
   a lease expiry nor `tasks.max=1` is sufficient. Transfer the same protection
   session from startup inspection to the reader without releasing it between them.
3. Read the exact durable F/W and reconcile any previous receipt before resumed
   publication. A receipt with a missing offset is a hard failure, not permission
   to take a new snapshot or select a newer journal suffix. Rebuild from F;
   suppress completed batches by their completion position relative to W.
4. Treat callbacks as candidates. Obtain exact committed-offset read-back under
   the verified owner, then persist the receipt with owner/predecessor compare-and-set.
   If Kafka commits before the receipt and the process dies, a fresh owner may
   reconcile only from independently verified matching durable state.
5. On revocation or maintenance, close new read/publication permission and complete
   both the output barrier and source-reader isolation. Broker fencing does not
   terminate an IBM i job. Retain history while either barrier is uncertain.

The local receipt adapter implements a portion of this protocol. It does not
grant ownership, prove native history, authorize maintenance or release receivers.

## Decisions required before production adapters

| Decision | Proposed direction | Decision owners and acceptance evidence |
| --- | --- | --- |
| Upstream feature boundary | Keep the transaction/replay core; evaluate the smaller CDC profile before requiring another service. Keep F/CR independent. | Maintainers: approve explicit support assumptions and per-gate replacement mapping. Current guards and N/P/H/B requirements remain until a reviewed alternative is validated. |
| Worker modes and publication ownership | Prefer framework source transactions and task fencing within one stable distributed Connect identity. | Maintainers/platform operators: prove supported configuration, old-producer rejection, immutable offsets and consumer visibility. Do not claim exactly-once or a global source lock. Test read-only stale-reader takeover separately; maintenance still needs reader isolation. |
| Durable recovery state | Prefer normal source offsets for F/W; determine the minimum separate initialization/lineage evidence needed. | Prove fresh-worker reconstruction and missing-offset rejection without treating missing state as a new installation. Keep current receipts; replacement requires C3/C5-C8. No new storage service selected. |
| Receiver retention and source continuity | Evaluate operator retention plus reliable history/identity validation. Retain stronger protection where detection or trusted restrictions are insufficient. | IBM i operators/maintainers: exact native API/release behavior, oldest-F coverage, races, historical schema/member continuity and baseline evidence. A retention setting or matching catalog queries alone cannot pass. |
| Optional cleanup/maintenance authority | Use the existing receipt/pin/barrier protocol only when claiming these additional capabilities; prefer supported existing mechanisms. | Prove durable owner/predecessor checks, evidence ordering, exact checkpoint acknowledgement, dependency retention and reader/publication barriers. No automatic cleanup in the candidate CDC profile. |
| Optional independent restore detection | Separate ordinary crash recovery from protection against old source/offset/authority backups. | Stronger detection requires an independent protected authority and its P/N/B tests. The candidate CDC profile excludes automatic restore/clone/reset recovery and requires an explicit new baseline when continuity is uncertain. |
| End-to-end validation | Dedicated disposable source/journal/library, isolated Connect and a truncate-aware test sink. | Operators and implementers: explicit object/operation/cleanup plan, writer/maintenance controls, storage budget, exact versions and expected persisted-offset/output oracles. Existing ASI auction schemas are not fixtures. |

Native guard details and alternatives are in the
[protection feasibility assessment](dcg-protection-feasibility.md). A registration
listing or successful read-only probe is observation, not proof of enforcement.
No native API choice is promoted to a supported guarantee by this draft.

## Implementation sequence after the decisions

1. Record the accepted CDC profile, operating assumptions and per-gate mapping.
   Investigate standard Connect mechanisms before selecting a custom authority.
   Keep the current production and startup guards in place.
2. Extend real Connect recovery/fencing tests for the proposed replacement path,
   preserving current receipt and missing-offset regressions. Resolve supported
   runtime compatibility, initialization evidence and exact F/W persistence.
3. Establish native identity/history completeness and a safe baseline under the
   accepted retention/maintenance boundary. Read-only inspection is candidate
   evidence only; destructive/transactional cases need an explicitly authorized
   disposable source and operation/cleanup plan.
4. Integrate the real reader, normalization, dispatch, idle heartbeat and offset
   lifecycle only under the reviewed contract. Required failures must stop capture
   without lost-journal resets, skipped entries or newer-suffix recovery.
5. Demonstrate the [candidate C gates](dcg-startup-acceptance.md#candidate-cdc-profile-gate-c)
   and every retained/replaced N/P/H/B and transaction obligation through fresh
   processes and a real truncate-aware sink. No optional gate is silently waived.
   Implement cleanup/maintenance or independent restore authority separately only
   if those capabilities are selected; their stronger protocol remains required.

Standard local CI should retain the deterministic unit/fixture tests and style
checks. The local Docker experiments are manual evidence today, not an existing
upstream CI job. Agree a pinned integration-test job and required checks with
maintainers; live IBM i tests require their own controlled runner and credentials.

## JDBC sink settings and the Db2 family distinction

The source operation, emitted `op=t` event and sink SQL are separate interfaces.
The JDBC sink's [`truncate.enabled`](https://debezium.io/documentation/reference/stable/connectors/jdbc.html#jdbc-property-truncate-enabled)
controls whether it applies incoming truncate events to its destination. It does
not enable journal capture in this source connector. The source also needs an
operation filter that permits `t`; neither switch establishes transaction safety.

Db2 for i already has SQL TRUNCATE; [IBM's SQL reference](https://www.ibm.com/docs/en/i/7.4.0?topic=statements-truncate)
documents rollback behavior without IMMEDIATE, and the recorded IBM i 7.5 tests
observed the relevant clear/rollback paths. The missing feature here is safe
journal-to-event capture, not the SQL statement itself.

The JDBC documentation's Db2 9.7 note describes an alternative using
`ALTER TABLE ... ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE`. Its explanation
about events emitted by a Db2 source is misleading relative to the implementation
inspected at core commit `5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8`:

- [`Db2DatabaseDialect.getTruncateStatement`](https://github.com/debezium/debezium/blob/5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8/debezium-connector-jdbc/src/main/java/io/debezium/connector/jdbc/dialect/db2/Db2DatabaseDialect.java#L223)
  generates that alternative as destination SQL. Its comment attributes the
  workaround to Hibernate execution while noting that plain JDBC works; this is
  an implementation rationale, not a reproduced root-cause diagnosis here.
- The separate [`Db2iDatabaseDialect`](https://github.com/debezium/debezium/blob/5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8/debezium-connector-jdbc/src/main/java/io/debezium/connector/jdbc/dialect/db2i/Db2iDatabaseDialect.java#L286)
  copies the same workaround and explicitly leaves native TRUNCATE for IBM i as
  a TODO. That does not establish that this SQL is suitable for an IBM i target.

This is a separate sink compatibility/documentation question. Do not prescribe
that ALTER statement for our IBM i source or assume that our source work validates
an IBM i JDBC sink. Choose the actual destination dialect for end-to-end testing;
verify its handling of truncate/reload and duplicates. Any sink correction belongs
in separate work, with no change to F/CR #114 or this source proposal's scope.

## Review outcome to record

Record accepted/rejected scope, named decision owners, selected supported APIs
and services, required revisions and gate evidence. Until those decisions and
tests exist, the result remains a local prototype and #115 remains draft.
Sending this proposal, updating either PR, installing providers, changing source
authorities, connecting to IBM i or running live mutations requires a separate
instruction. This document performs none of those actions.
