# D/CG protected startup acceptance matrix

Current configured-source status is tracked in the
[source release checklist](dcg-source-release-checklist.md). The dated sections
below preserve the evolution of the protected and ordinary contracts; older
"remaining" statements are not an additional sink/cloud requirement for the
configured source connector. The optional protected-path obligations remain.

Status: release gates with **partial local fixture and Kafka coverage**, 2026-11-06. These gates implement the
[source identity and recovery protocol](dcg-startup-protocol.md). Existing unit
tests use synthetic proofs; they do not satisfy the native, enforcement or live
gates below. N1-N3 and the identity-mismatch portion of N6 now have local fixture
coverage in `As400JdbcNativeIdentityEvidenceTest`; see
[validation](dcg-validation.md#native-object-identity-candidate-collection).
No native source, protection-provider or streaming gate is marked passed.

The 2026-11-07 [native lifecycle assessment](dcg-native-lifecycle-validation.md)
adds local restart rejection coverage, including cycle-zero tails after C/RB.
It also demonstrates that the two savepoint policies produce the same final rows
for those captures. The later [discrimination experiment](dcg-native-lifecycle-discrimination.md)
adds a real missed-write contrast, explicit conservative target consumption, a
bounded two-pass lifecycle candidate and serialized F/W context tests. IBM
clarification is no longer a prerequisite for those restricted algorithms.
The [incremental lifecycle integration](dcg-incremental-lifecycle-validation.md)
now applies the bounded rules and replay dependencies inside the coordinator,
with actual RPC-loop/dispatcher fixture coverage and serialized restarts at every
captured entry boundary. Production startup-to-reader wiring, receiver expansion
and native startup/history validation remain open. No C/N/P/H/B release gate is
waived or completed by these local experiments. The subsequent
[verified reader handoff](dcg-verified-reader-validation.md) retains the inspection
through bounded reads, output and terminal drain, and fingerprints the selected
lifecycle policy. Native protection/history/enumeration adapters remain unavailable;
their default paths still reject instead of treating observations as proof.

The [local runtime profile revision](dcg-runtime-profile-revision.md) adds the
[candidate C cases](#candidate-cdc-profile-gate-c) below to evaluate a smaller CDC
profile. The N/P/H/B rows remain unchanged and apply to the current protected
implementation. C cases neither waive those rows nor authorize a replacement:
record a reviewed per-gate contract decision and validate its alternative first.
Selected local C subcases have passing evidence recorded below; no complete C or
N/P/H/B release gate is newly passed by this documentation update.

For every rejection, assert no new coordinator/publication authorization, no
advance of the acknowledged F/W checkpoint, and no release of required durable
receiver pins. A running session's failure must close its publication gate and
terminate/recover under the existing whole-batch rules. Already acknowledged
output is not rolled back. Validate actual persisted offsets, not only an
in-memory position or mock method count.

The [additional native clear cases](dcg-multiple-clear-validation.md) validate
abandoned optimized attempts and two-table rollback windows with local restart
and dispatcher replay. Exact-path identity queries also succeeded on the owned
fixture, but remain observations. The [prepared production-contract decision](dcg-completion-decision.md)
is pending; no gate mapping or source-protection requirement is silently changed.

## Native object identity (gate N)

| ID | Scenario | Required oracle | Evidence required |
| --- | --- | --- | --- |
| N1 | Exact journal/receiver/file/member paths and full numeric tuples | Exact typed values, correct path/type/ASP, bounded parameterized read-only query, owned results; no enumeration fallback | JDBC result fixtures; later exact SELECTs on a disposable source |
| N2 | Same name; different generation, file ID or file-system ID | Reject replacement even when timestamps/ranges/JIDs match; each component participates in identity | Fixtures varying each component independently |
| N3 | Null/duplicate/absent identity, remote/symbolic result, wrong type/ASP, unavailable service, warning or ignored error | Reject; do not make missing rows or zero defaults into proof; preserve legitimate zero components without wildcard semantics | Fixtures and permission/service failure tests |
| N4 | Repeated observation, new connection/worker, ordinary receiver detach/rollover and normal IPL | Establish which tuple values remain stable for the exact deployed release/PTF and object types; unsupported changes invalidate the profile | Sanitized before/after native/SQL observations from explicitly authorized disposable IBM i |
| N5 | Delete/recreate same name; restore into existing name; source image restore/clone/endpoint reassignment | Replacement is rejected or enrollment is revoked before the operation; matching copied IDs alone never authorize resume | Destructive disposable-source tests only under separate explicit authorization; record any limits, do not generalize from one run |
| N6 | Identity changes during raw retrieval; change-and-change-back race | Mismatch discards owned results; protection prevents the race that equal reads cannot detect | Interleaved transport fixtures plus real protection-provider fault test |

N4/N5 require documentary lifecycle guarantees or an explicit supported
operational restriction in addition to observed outcomes. A few unchanged IDs
do not establish universal non-reuse or restore detection. No native adapter may
claim proof until its accepted scope is justified.

## Retention, ownership and maintenance (gate P)

The [protection feasibility proposal](dcg-protection-feasibility.md) maps candidate
mechanisms and failure tests to each row. Its restricted retain-all prototype is a
recommendation for evaluation; no provider is installed and no P gate is passed.

| ID | Scenario | Required oracle | Evidence required |
| --- | --- | --- | --- |
| P1 | F is older than R/W; output/heartbeat enqueued but unacknowledged | Oldest accepted durable checkpoint and all active inspectors keep needed receivers pinned | Registry/checkpoint integration with delayed acknowledgements |
| P2 | Crash before/after manifest durability, offset durability, pin update and pin release | Evidence precedes any referencing durable checkpoint; uncertainty retains too much, never too little | Fresh-process tests with a durable test registry and offset store at every write boundary |
| P3 | Registry unavailable/corrupt, connector crash, worker move, provider restart/IPL | Pin protection survives; missing protection state denies new startup and deletion | Failure fixtures plus authorized provider/source fault tests |
| P4 | Automatic/manual delete, privilege bypass, restore/rename and journal replacement | Normal destructive paths are blocked while protected; unsupported bypass revokes the epoch and requires explicit recovery | Disposable-source enforcement tests and documented admin boundary; an exit-registration listing alone fails this gate |
| P5 | Revocation races with read, normalization, multi-record dispatch and checkpoint acknowledgement | Reader/publication barrier precedes permitted maintenance; no stale owner can continue after ownership transfer | Real reader/dispatcher integration plus deterministic race tests |
| P6 | Network partition, lease expiry, old worker unreachable | Timeout never means permission to delete or mutate; fail closed until a tested fencing/recovery barrier completes | Two-worker partition test and protection-provider outage test |
| P7 | Schema/member/journaling option changes during lag or downtime, including change then reversal | Operations are excluded under valid protection or invalidate enrollment before resumption; present-day equality cannot excuse them | Administrative workflow enforcement and history fixtures |
| P8 | Old checkpoint restored after its pins would be collected | Reject a retired checkpoint or retain its dependencies; no silent fallback to a newer floor | Durable checkpoint/manifest GC and rollback tests |

Do not simulate a provider by returning `true` from a mock and mark gate P
complete. The tested mechanism must enforce the required exclusion. A deletion
exit only addresses deletion; it does not by itself satisfy P4/P7. Administrative
restore/clone outside the trusted boundary remains unsupported.

## History cut and replay handoff (gate H)

| ID | Scenario | Required oracle | Evidence required |
| --- | --- | --- | --- |
| H1 | A multi-receiver F..H interval with non-lexical names and ordinary rollover | Native-identity-qualified links determine order; exact bounds are scanned without object/JID filtering | Fixtures plus raw/SQL cross-check on a disposable journal |
| H2 | Missing/partial/freed middle receiver, fork, broken reciprocal link, reused name or sequence reset | Reject before publication; never choose a newer suffix | Receiver manifest and history fixtures |
| H3 | Reported tail includes cached/unreadable entries | No verified cut at that tail; bounded wait/failure or separately authorized durable boundary, never claim complete empty history | Cache-aware fixtures and authorized IBM i caching/force validation |
| H4 | Missing internal-entry option, sequence hole, hidden lifecycle entries, partial response or unknown control | `History.complete` is unavailable; no filtered or count-only substitute | SQL/RPC selection, truncation, error and classification fixtures |
| H5 | Equal SQL/raw row counts but different coordinates, code/type, identity or bounds | Reject mismatched proof; owned raw payload semantics must match the same protected interval | Adversarial paired SQL/RJNE0200 fixtures |
| H6 | Appends/rollover during verification and immediately before handoff | Frozen H remains the verified cut; extension proves every intervening entry, never skips to a later tail | Reader integration with controlled append schedules; this precedes relaxation of `Verified.recheck()` |
| H7 | Current format matches baseline but historical object/member/format continuity is missing | Reject; decoded D/CG companion transitions need an explicit validated rule | Versioned lifecycle fixtures, decoder-binding tests and later full source traces |
| H8 | Protection lost before, during or after inspection-to-reader transfer | No release/reacquire gap; one owner, deterministic close/abort and safe durable F/W | Ownership/race tests through actual reader/publication gate |
| H9 | Open A spans W while B completed, then interrupted clear/reload delivery after restart | Rebuild A from F, suppress B by completion, replay the whole interrupted batch before later batches; preserve event source metadata | Existing coordinator regressions extended through actual reader, dispatcher and fresh-process restart |

## Controlled baseline and migration (gate B)

| ID | Scenario | Required oracle | Evidence required |
| --- | --- | --- | --- |
| B1 | A cycle on an unrelated table later touches a captured file | Journal-wide fence/census catches it; a captured-table-only fence is refused | Multi-job source test with explicit disposable fixture authorization |
| B2 | Restricted census authority, missing resource, pending=0, rollback progress=100, prepared/XA or unresolved completion | Do not infer complete empty census; ambiguity blocks initialization | SQL fixtures and privileged/unprivileged comparison in an authorized source test |
| B3 | No durable boundary; optional marker absent, wrong bytes/epoch/coordinate, non-forced or unreadable | Initialization stays pending; marker is not misclassified as commit or proof of quiescence | Marker fixtures and later separately authorized source validation |
| B4 | Snapshot finishes but Kafka acknowledgement or required sink barrier is delayed | Writer fence stays held; no complete baseline/checkpoint claim | Actual output/baseline-store integration with delayed acknowledgements |
| B5 | Crash at every pending-enrollment/baseline-complete/initial-offset/handoff/fence-release boundary | Fresh instance accepts only complete matching durable evidence; uncertainty blocks/requires explicit recovery | Fresh-process tests with persistent stores and a simulated fence, followed by real provider validation |
| B6 | Legacy/v2/unknown offset, missing manifest or changed source/decoder/output policy | No implicit migration, token reinterpretation or reset; explicit new baseline protocol required | Codec and startup integration tests |
| B7 | Fence lost during snapshot or reader construction; stale worker resumes | Abort or quarantine pending baseline; retain safe pins; no publication under expired ownership | Concurrent baseline/reader/fence tests |

## Release and evidence rules

N1-N3 and the identity-mismatch part of N6 have fixture coverage only; startup
remains disabled. The actual SQL/path behavior portion of N1 and N4-N5 need separately
authorized IBM i work; all P/H/B gates and existing transaction/savepoint gates
must pass before any support claim or streaming enablement.

For live evidence record IBM i release/PTF/service levels, source/protection
profile, authority scope, sanitized native IDs, exact receiver ranges, raw entry
fixtures, connector/core revisions, actual stored offsets, and output ordering.
Use a dedicated disposable journal/library and a truncate-aware test sink.
Existing ASI auction schemas are not test fixtures. A future live plan must name
its created objects, operations and cleanup; this document authorizes none.

Then run actual Kafka Connect crash/acknowledgement tests and demonstrate source/
sink convergence for commit, rollback and interrupted clear/reload delivery.
Fixture success, a standalone replay model or observing `op=t` alone cannot close
those gates. Preserve F/CR PR #114 throughout; no push or PR update is included.

## Candidate CDC profile (gate C)

These remain acceptance requirements for the standard-Connect-first design in the
[runtime profile revision](dcg-runtime-profile-revision.md). The evidence checklist
below records observed subsets without changing these oracles. Retain
existing regression tests and their stricter current oracles. Where fixture/Kafka
coverage exists, extend it through the proposed real integration rather than
counting the same run as proof of native behavior or a replacement authority.

For rejection cases, measure actual committed F/W and Kafka output. Require no
new publication authorization and no checkpoint beyond missing/failed required
work. A known valid earlier output prefix is not retroactively undone. Do not
test safety solely by mock calls, event counts or an in-memory cursor.

| ID | Test and failure injection | Required result and evidence | Existing obligations |
| --- | --- | --- | --- |
| C1 | Both D/CG subtypes with row-only/mixed/interleaved cycles, full and nested/repeated/non-top savepoint rollback, compensation, unknown/non-durable outcomes and buffer exhaustion | Preserve owned row/source identity, commit order and bounded memory; no rolled-back clear or skipped required change. Rebuild A spanning W while suppressing completed B. Unit/model tests plus complete release-specific raw native traces; existing partial rollback observations do not close the matrix. | Transaction gates; H9 |
| C2 | Suspend old worker before send, replace it, then resume it; repeat with network partition and queued data/tombstone/metadata/heartbeat output | Broker/framework fences the old producer before accepted replacement output; no late committed clear or regressed offset. Use actual worker/broker errors and independent `read_committed` output/offset reads. Verify stable identity and controls excluding independent duplicate connectors. | Publication portions of P5/P6 |
| C3 | Fresh-process restart at every output/offset boundary across multiple polls; delayed acknowledgements, failed Kafka commit, primary-key subrecords, idle/filtered/rollback heartbeats | Every record carries safe immutable F/previous-W; no advanced checkpoint without required preceding output. Load exact durable F/W, replay the whole incomplete batch before later work, suppress completed batches, retain pinned F for open cycles. Include normal task stop/rebalance and failure recovery. Existing one-topic fixture is partial evidence only. | Recovery algorithm; P2; H9 |
| C4 | A stale IBM i reader remains alive after its producer is fenced; reader error/reconnect/close races with replacement | Prove it is read-only and cannot publish accepted output, change shared recovery/authority state, release history, authorize maintenance or disrupt replacement correctness. Preserve lifecycle cleanup. If proof fails, retain the current source-reader barrier. Real task/reader integration plus controlled fault injection; no fixture generation token as proof. | P5/P6; H8 |
| C5 | F older than R/W; missing first/middle/boundary receiver; deletion during paging, rollover, partial/cached tail, filtering and API failures | Establish a complete native-identity-qualified interval before dependent publication; reject every required gap without suffix reset or unsafe W advancement. Test oldest-F retention during downtime/replay and capacity failure. Retention settings alone cannot pass; if native detection is insufficient, enforce protection or reject the candidate. | P1/P3; H1-H6/H8 |
| C6 | Same-name replacement, changed native tuple, IPL/rollover, altered-and-restored member/schema/options, incompatible fingerprint; new worker loses all local metadata | Establish identity/history lifetime for the exact release/PTF. Recover required lineage/decoder evidence durably; fail on absent/ambiguous history. Equal catalog reads cannot prove no intervening change. Validate trusted restrictions/enforcement for undetectable transitions; no SQL candidate becomes proof by declaration. | N1-N6; P4/P7; H7/H8; B6 |
| C7 | Initialize with a cycle begun on an unrelated table; lose writer fence or baseline acknowledgement; crash around baseline/initial-offset/handoff | Complete transaction census and consistent snapshot/output baseline, with held protection until safe handoff. No automatic legacy migration or synthetic W=1. Any future alternative to the present writer-fence protocol must prove equivalent starting state. Actual source/snapshot and durable-offset evidence required. | B1-B5/B7 |
| C8 | Existing receipt but missing Kafka offset; missing/corrupt/incompatible offset on ordinary resume; crash during approved first initialization | Current regression: zero new output, unchanged accepted receipt. A future path without receipts must also reject normal resume and distinguish initialization using evidence surviving source-offset loss. No implicit fresh start, newer suffix or automatic snapshot. Restoring/erasing all evidence is not claimed detectable without an independent authority. | Receipt reconciliation; B5/B6; P8 boundary |
| C9 | Unsafe errors/SMTs/filtering, disabled heartbeats, unsupported worker mode/version, non-transactional consumer visibility, changed group/connector identity | Reject or exclude unsupported configuration before protected publication. Pin documented supported framework behavior; do not turn the probe's `UNSUPPORTED`/`requested` setup into a support claim or change capability declarations just to pass validation. | Capture fingerprint; delivery gates; B6 |
| C10 | Real source to truncate-aware sink through separate table/metadata/heartbeat topics, one partition per table, killed workers during clear/reload and after completed output | Final sink equals committed source state after catch-up; no later batch overtakes incomplete replay. Preserve keys, metadata and tombstones; use `read_committed` with transactional output. Document duplicate/partial visibility and reject selective deduplication that reapplies a clear but drops its reload. No cross-topic/source-transaction atomicity claim. | Transaction/live gates; H9; B4 |
| C11 | Attempt retention release or maintenance authorization using a candidate-profile checkpoint, callback or expired lease; present a known restore/reset/replacement | Candidate provides no cleanup/maintenance permission and requires explicit recovery for unsupported continuity changes. If optional stronger capabilities are selected, separately pass their existing pin/receipt/reader/restore tests. Do not claim detection of an indistinguishable old backup from Kafka state alone. | Optional stronger P/N/B obligations below |
| C12 | Review the final mechanism/gate mapping and the exact tested artifacts/configuration before any production wiring | Named accepted scope, supported API/version evidence, per-gate disposition and regressions; any unresolved required guarantee blocks enablement. Existing N/P/H/B remain applicable until replacement is approved and proved. Local historical test counts are not newly earned CI/live evidence. | All release gates |

### Recorded local evidence and open gates

Checked items below mean **the named local subcase has evidence**, not that its
whole C gate or mapped N/P/H/B obligation has passed. The
[runtime design](dcg-runtime-profile-revision.md#recorded-local-kafka-evidence)
and [matrix report](dcg-timeout-profile-validation.md) define the exact scope.

- [x] **C2 publication subset:** the earlier suspended-worker test observed broker
  producer-epoch fencing. The twelve new partition runs observed stale-client
  rejection by local close, no late committed clear/regressed F/W, and legitimate
  new-task output on the original worker. They use no custom owner/receipt gate.
- [x] **C3 recovery subset:** prior delayed-ack/completed-restart, interrupted-batch
  and mixed key-change/tombstone cases passed their real task/dispatcher and actual
  Kafka-offset oracles. The new matrix repeated exact W=6 reload, later publication
  through W=9, immutable offsets, metadata/heartbeats, ordered synthetic projection,
  monotonic committed W and normal stop for twelve partition timings.
- [x] **C8 ordinary-resume subset:** the earlier separate offset-rejection run
  passed missing, malformed and incompatible offset cases with zero new output.
  Its synthetic initial baseline was seeded explicitly by the controller; this
  does not establish production initialization or evidence surviving full restore.
- [x] **C9 observed local profile:** pinned Kafka/Connect 4.3.1 with the exact
  candidate timeouts in the matrix passed the selected partition scenarios. The
  probe still declares `UNSUPPORTED` and uses `requested`; no capability or
  production compatibility claim is changed. Each run used one existing bounded
  administrative-stop retry. Earlier non-partition scenarios were not rerun under
  this profile and are not relabeled as matrix results.
- [x] **Control and evidence preservation:** the original configuration still
  failed its 100-second reassignment wait; fresh broker readers afterward found
  complete config history and unchanged committed output/exact F=W=6. The failed
  report remains failed. Source/artifact hashes and cleanup were verified, and
  Advisor passed the matrix review. This update reruns no runtime tests.

| Gate | Still required; full gate remains open |
| --- | --- |
| C1 | Bounded IBM i 7.5 subtype 3/8 commit, rollback, repeated/nested-savepoint and interleaved traces are captured. The [paired identity model](dcg-file-member-identity.md) separates file/member JIDs; [thread-enabled captures](dcg-native-thread-validation.md) supply actual nonzero thread IDs. The [discrimination experiment](dcg-native-lifecycle-discrimination.md) adds a real missed-write negative case, conservative savepoint consumption and a bounded two-pass lifecycle candidate. Thirteen complete native scenarios pass candidate inspection and component replay; a fresh-coordinator experiment retains rollback-tail context using existing F/W. Default runtime rejection remains unchanged. The [incremental classifier/coordinator](dcg-incremental-lifecycle-validation.md) now passes local record-by-record, RPC-page and fresh-restart cases with automatic dependency retention. Production startup/reader integration, native association/history, receiver expansion and the remaining transaction matrix must pass before C1 closes. This does not repair the [old zero-thread captures](dcg-native-thread-identity.md) or validate native startup or live publication. |
| C2/C3 | Remaining failure/output boundaries, queued-output combinations, failed Kafka commits, actual idle/filtered/rollback scheduling and broader mode/configuration coverage; preserve all existing tests |
| C4 | Real ongoing reader lifecycle and isolation, including native cancellation/reconnect side effects; next local step below, followed by native evidence |
| C5/C6 | Identity-qualified history to oldest F, paging/rollover/deletion races, native identity lifetime and durable historical decoder/lineage evidence; catalog equality is insufficient |
| C7/C8 | Complete source transaction census and safe snapshot/initial-offset handoff; production initialization distinct from ordinary resume even after source-offset loss |
| C9 | Accepted supported runtime/API/version and effective-configuration validation, including exclusion of unsupported modes and identities |
| C10 | Real committed source to truncate-aware sink convergence; the local projection is not a live sink |
| C11/C12 | Explicit retained/alternative/excluded mechanism mapping and accepted scope; optional retention/restore obligations are not waived by local output success |

### Next local implementation checklist (C4)

The [test-only incremental reader lifecycle harness](dcg-runtime-profile-revision.md#next-implementation-step-test-only-reader-lifecycle-harness)
is implemented. See [executed cases, evidence and limitations](dcg-reader-lifecycle-validation.md).
The [cancellation follow-up](dcg-reader-cancellation-validation.md) fixes the
registered in-flight recursion, invalidates completed/failed job handles and
closes temporary cancellation connections. The subsequent
[startup/stop tests](dcg-reader-startup-stop-validation.md) exposed post-close
reconnect/new-call admission. The [terminal-stop fix](dcg-terminal-stop-validation.md)
now closes that local gap. The [task-stop integration](dcg-task-stop-validation.md)
extends the local evidence through the real coordinator, streaming retries and
JDBC cleanup. The complete C4 gate stays **blocked**; native fault/cancellation
behavior remains unverified. No custom dependency or existing source-reader
barrier is removed. Checkmarks below cover only the stated local work:

- [x] Inventory actual retrieval, reconnect, close, server-job logging and
  cancellation effects. Exercise real control flow with injected JT400 transport
  responses/job handles; fail the fixture on any undeclared native action. A copied
  loop or permissive startup proof is insufficient.
- [x] Use independent old/replacement reader instances and durable F/W. Hold an
  old retrieval in flight while Connect replaces its task; return data or errors
  after replacement output is accepted. Preserve real producer rejection and
  independently read committed output/offsets as the publication oracle. Treat the
  RPC loop's mutable position as scan R; it cannot replace the coordinator's
  immutable F/W or become the durable recovery checkpoint.
- [x] Race close, reconnect, delayed read completion and repeated stop. Assert
  that old cleanup/cancellation targets only its own simulated connection/job,
  cannot change replacement reader/cursor/authority state, cannot release history
  or grant maintenance permission, and does not prevent replacement progress.
  Connected/disconnected in-flight cases now pass locally; cancellation failure
  and late completion retain replacement isolation. Native job-lifetime behavior
  remains outside this proof.
- [x] Exercise discovery, call setup, and the registration-to-run handoff with
  bounded deterministic gates, independent replacement progress and saved F/W.
  Twenty-four boundary cases now preserve local checkpoint/output isolation,
  including an exact gate after registration and before transport admission.
  The separate run-entry gate remains after Java invocation.
- [x] Prevent new source-call admission/reconnect after completed terminal stop.
  Stop rejects new admissions, cancels the captured read and drains admitted
  operations before returning; ordinary close remains reconnectable. All 24
  boundaries show no new run after the controlled stop request and no work after
  stop completion. Six additional cases cover task-stop wiring, failed setup,
  and cleanup after an admitted driver-model reconnect/Error. Eight further
  metadata cases verify cleanup of detached captured transports on late data/error.
  Interrupted and
  repeated stop cannot bypass draining. These are local tests; an already-admitted
  operation can proceed while stop is pending, and no native deadline is proved.
- [x] Exercise the real coordinator/streaming/task-stop lifecycle with old RPC
  and RPC metadata calls held during Kafka reassignment. Reject stopped-context
  retries, drain admitted RPC/JDBC retries before task JDBC cleanup, and distinguish
  requested stop from completed drain. Preserve independent committed F/W and
  old/replacement isolation. A native embedded smoke test passed ordinary cleanup;
  it does not establish native fault behavior or bounded shutdown. See the
  [task-stop evidence and limits](dcg-task-stop-validation.md).
- [x] Retain original reproduction, all publication/receipt/missing-offset and
  false-dispatch regressions, finite fault deadlines, artifact provenance and
  owned-resource cleanup. Record legacy reset/skip/error-path limitations as
  blockers, without silently adopting them for buffered recovery.
- [x] Keep transport stubs incapable of contacting IBM i and leave transactional
  streaming wiring/guards unchanged. The terminal-stop follow-up wires ordinary
  task cleanup to RPC stop without enabling D/CG streaming. Record remaining native job-identity/authority/lifetime
  and cancellation semantics separately; local mocks cannot pass them. If local
  isolation fails, retain the source-reader barrier and report the failing case.

### Mapping the existing stronger contract

This is a proposed responsibility split, not an approved waiver. For a future
profile, record each affected gate's retained guarantee, old mechanism, proposed
replacement or excluded optional capability, exact evidence, and decision owner.
The current implementation continues to enforce its existing contract meanwhile.

| Existing gates | Common CDC obligation to retain | Additional capability or mechanism to evaluate separately |
| --- | --- | --- |
| N1-N4 | Native identity correctness, exact bounded reads and release-specific lifetime (C6) | No relaxation from matching names/timestamps or a few observations |
| N5/N6 | Reject observable replacement; establish safe lifecycle/race boundaries (C5/C6) | Independent restore detection and enforced exclusion of undetectable changes need the stronger authority or an explicit trusted administration restriction |
| P1 | Required history reaches durable F throughout the supported recovery window (C5) | Automated durable pin ownership/release is not part of the candidate CDC profile |
| P2/P3 | Checkpoint and required evidence survive restart; missing state/history fails (C3/C5/C8) | Separate receipt/manifest write ordering and provider-outage deletion veto remain required when granting cleanup permission |
| P4/P7 | Fixed validated object/schema/journal history; unsupported changes require recovery (C6/C11) | A universal maintenance-authorizing service is separate; detectable gaps alone cannot replace prevention of undetectable changes |
| P5/P6 | One effective ordered publisher; uncertain takeover must not produce stale output (C2/C4) | Reader isolation before authorized maintenance/retention release remains a stronger obligation; ordinary read-only takeover needs proof before replacement |
| P8 | Never silently reinterpret a known stale/missing/incompatible checkpoint (C8/C11) | Independent detection of old backups and safe garbage collection of their dependencies require the restore/retention authority |
| H1-H7/H9 | Complete bounded history, correct decoding, safe extension and deterministic replay (C1/C3/C5/C6) | Keep these guarantees irrespective of retention backend |
| H8 | Safe inspection-to-reader continuity and failure handling (C4-C6) | The current held protection session can change only after an equivalent boundary is proved |
| B1-B7 | Complete starting transaction state, consistent durable baseline and explicit migration/recovery (C6-C8/C10) | A different baseline mechanism may be considered; idle journals and missing offsets never substitute for evidence |

Optional automated retention-release/maintenance support must retain the complete
P1-P8 protocol, native N/H requirements and relevant B evidence. Independent restore
detection additionally needs a protected authority outside the relevant restore
domain, terminal enrollment state and old-backup fault tests. Existing receipt
regressions, including missing-offset zero-output behavior, remain required in the
present code even if a future design selects neither optional capability.

No source mutation is authorized by this matrix. Read-only edgeqa inspection can
confirm available release/PTF, API and current retention metadata. Destructive,
transactional and lifecycle tests require a separately scoped disposable fixture
and explicit operation/cleanup authorization; existing auctions are not fixtures.


## Operator-managed retention implementation checkpoint (2026-11-07)

The user selected the ordinary CDC contract in
[the completion decision](dcg-completion-decision.md). The protected path and its
existing tests remain intact. Candidate-only progress and evidence are recorded in
[ordinary adapter validation](dcg-operator-retention-validation.md).

| Gate | New evidence | Remaining acceptance requirement |
| --- | --- | --- |
| C1/C3/H9 | Native mixed autocommit/transactional capture, every-entry fresh restart, interrupted real dispatcher output, receiver-spanning transaction replay | Actual native source task to Kafka/sink delivery and recovery |
| C5/C6/H1-H7 | Owned bounded C/D/F/R pages, exact file/member JIDs, native format observations; concrete cut-reader local rejection tests | Combined native cut execution, identity lifetime/source binding, empty selected-range semantics and actual codec acquisition |
| C4/H8 | Existing terminal-stop/cancellation tests preserved; raw failed/pointer response cancels captured job | Complete ordinary metadata/RPC lifecycle admission and drain integration |
| C7/C8/B | Missing/legacy offsets reject before I/O; durable W is retained within F history | Explicit consistent first baseline/census and crash-safe initialization; no implicit first start |
| P1-P8 | No new retention-release or maintenance authorization | Optional protected path remains separate; operator retention is an obligation, not a generated proof |

The initial next native preflight was blocked by expired AWS credentials before
any IBM i connection or fixture creation. After renewal, the native cut/empty-range
and missing-history checks proceeded; see [native cut validation](dcg-native-cut-validation.md).

## Unrelated-transaction progress requirement (2026-11-07)

The user rejected potentially unbounded connector-wide metadata pauses. An open
clear must not prevent publication of other committed transactions. A timeout that
fails the task does not satisfy this requirement. Existing identity, schema,
retained-history and finite-resource failures remain fail-closed.

The [open-clear validation](dcg-open-clear-progress.md) records native reads while
B's TRUNCATE/optimized DELETE remained open, plus local delivery of independent A
commits through the strict decoder, normalizer, buffer and actual dispatcher.
Fresh restarts preserve F before W, suppress completed A output and handle both B
commit and rollback. Standard Debezium table-history serialization preserved the
fixture bindings in a file roundtrip; this is not Kafka history durability proof.

| Gate | Additional evidence | Remaining requirement |
| --- | --- | --- |
| C1/C3/H9 | Actual open native cuts, unrelated committed output before B terminal, every-entry restart and interrupted clear delivery | Actual task/Kafka publication and replacement while B remains open |
| C5/C6 | Metadata visibility matrix and standard history-format feasibility | Qualified durable binding epochs, effective native codecs, history-before-offset ordering and failure tests |
| C7/C8/B | Fixture baseline and serialized F/W only; no implicit restart initialization | Consistent production census/baseline and crash-safe binding-history initialization |

No acceptance gate is closed by replacing unavailable metadata with an unchecked
cache. The existing whole-cut reader and transactional streaming guards are intact.

## Binding-history adapter checkpoint (2026-11-07)

[Binding-history validation](dcg-binding-history-validation.md) adds an immutable
schema epoch through standard `SchemaHistory`, strict effective codec descriptors,
history-before-initial-checkpoint readback, and a separate cut-reader path using
recovered bindings without current table queries. The previous observation-based
constructor and protected startup path remain intact.

| Gate | New evidence | Still open |
| --- | --- | --- |
| C1/C3/H9 | History-backed cut, strict decoding and real dispatcher preserve unrelated open-clear progress and failed-batch replay | Actual task/Connect offsets, output transactions and native source-to-sink convergence |
| C5/C6 | Exact manifest/fingerprint validation, missing/conflicting history rejection, fresh-JVM KafkaSchemaHistory recovery | Native codec acquisition, source association/baseline proof, supported history backend deployment and complete runtime integration |
| C7/C8/B | Failed/lost/partial writes or readback cannot return initial checkpoint; missing offset always rejects resume | Complete production census/baseline and atomic source/output handoff |
| C4/H8 | Existing terminal admission/drain and cancellation tests preserved | History lifecycle admission/cancellation and ordinary task integration |

The corrected fourteen-phase local Kafka run validates metadata persistence and
strict rejection of null, malformed and incomplete records. The qualified backend
preserves standard Kafka history writes but replaces its permissive recovery loop;
the original provider-skipping reproduction is retained. This evidence
does not replace the existing ownership matrix or claim native/Kafka output
publication. No global table pause is accepted as an open-clear runtime policy.

## Native acquisition and explicit initialization (2026-11-07)

See [native initialization](dcg-native-initialization.md) for exact implementation,
evidence and restrictions. Production transactional streaming is still disabled.

| Gate | Newly implemented/checked | Still required |
| --- | --- | --- |
| C5/C6 | Native physical field offsets/lengths, codec/CCSID/key/nullability acquisition, paired file/member identities and immutable history | Supported deployment/PTF profile and full source-task integration |
| C7/B1/B2 | Effective cross-user transaction authority and all-definition journal resource enumeration, independent of captured tables and pending counts | Qualified journal-wide writer/maintenance fence provider; isolated native file locks alone are insufficient |
| B3/B4 | Prepare snapshot before probing stable raw B; acknowledge prepared baseline before history | Actual snapshot output/sink barrier and standard Connect handoff integration |
| B5/B7/C8 | Epoch/baseline receipt in history before offset callback; fresh readback; fault cases for snapshot, history, fence, schema and offset handoff; missing-offset rejection | Real task crash/reassignment/ownership admission and reader continuation across every handoff boundary |

An unimplemented session/provider must refuse initialization. The adapter does not
release fences on success or failure, infer a census from quietness, replace a
missing offset with W=1, or add runtime pauses for open clear transactions.


## Continuous native source to Connect checkpoint (2026-11-07)

See [continuous runtime validation](dcg-continuous-runtime-validation.md) for the
completed 47-assertion edgeqa/local Kafka experiment and retained evidence. The
entry point remains test-only, qualified-session checks and operator retention
remain required, and production transactional streaming is disabled.

| Gate | Additional evidence | Remaining support requirement |
| --- | --- | --- |
| C1/C3/H9 | Autonomous incremental reading, native mixed schemas, both D/CG subtype commit/rollback outcomes, unrelated CDC while a clear stays open, receiver-spanning fresh-task replay | Normal task wiring, sustained-load bounds and truncate-aware sink convergence |
| C2/C4/H8 | Real task/dispatcher, held old truncate producer fenced by Kafka, legitimate reassignment, stop request distinguished from completed RPC/JDBC drain | General supported runtime/deployment profile; no hard native shutdown deadline claimed |
| C5/C6/H1-H7 | Retained F/W and exact R anchors revalidated per interval, verified receiver extension, unchanged immutable history, captured variable-wrapper/padding regressions | Native version/PTF portability, broader schema/DDL support and operator retention qualification |
| C7/C8/B3-B7 | Snapshot acknowledgement before history/initial offset, crash after initial durable checkpoint, reader handoff, missing-offset rejection with zero output | Production journal-wide initialization fence/provider and normal task integration |
| P1-P8 | No automated retention release or maintenance permission added | Optional protected path and its stricter tests remain separate and unchanged |

The successful run and all diagnostic attempts verified native and local cleanup.
No whole-native-transaction atomic sink visibility, universal exactly-once delivery,
independent restore detection or production readiness is inferred from these cases.

## Normal task recovery integration checkpoint (2026-11-07)

[Normal task integration](dcg-normal-task-integration.md) connects already
initialized F/W and binding-history recovery to the actual task, coordinator,
output queue, durable callback/readback barrier and stop/drain path. Admission is
package-private and test-only; normal transactional streaming remains disabled.

| Gate | New local acceptance | Still required |
| --- | --- | --- |
| C1/C3/H9 | Actual task output from recovered native schemas; unrelated progress, conservative offsets and interrupted-batch fresh-task replay | Normal native initialization and supported deployment/load/sink qualification |
| C2/C4/H8 | Stop during history/read/cleanup, replacement progress, callbacks rejected after stop, failed cancellation/cleanup cannot claim drain | Supported native session provider and validation of this normal-task path against an isolated native source |
| C7/C8/B3-B7 | Missing/legacy/mismatched offsets and missing/uninitialized history reject; acknowledged F/W enters the actual coordinator | Normal-task initialization/snapshot acknowledgement and journal-wide writer/maintenance fence/provider |
| P1-P8 | No custom owner/receipt gate added to the candidate; existing stricter tests preserved | Optional retention-release and restore guarantees remain separate |

## Bounded first-start proposal (2026-11-08)

[The bounded snapshot design](dcg-bounded-snapshot-design.md) uses the completed
startup profiles to propose one durably retired pending chunk at a time. Native
snapshot reads and closes still finish before B. A final BASELINE receipt,
initialized history and durable INITIAL F/W remain mandatory; a CHUNK receipt
never permits recovery. No implementation or new validation is claimed here.

| Existing gates | Additional required acceptance | Status |
| --- | --- | --- |
| B3/B4/C7 | S1-S4: complete bounded scan, genuine EOF, unique pending chunk offsets, ordered retirement/readback, final receipt after B | Planned |
| B5-B7/C8 | S5-S10: session-loss and callback races, partial-start rejection, missing-offset restart, native identity/census and handoff checks | Planned |
| C2/C4/H8 | S6/S10-S12: broker fencing, stop versus drain, cursor/validation serialization, native cancellation and cleanup | Planned |
| C1/C3/H9 | S13: ordinary F/W restart and unrelated committed-table progress during an open clear remain unchanged | Regression required |
| B4/C7 and sizing | S12/S14: >9,000-row bounded delivery, driver allocation evidence, whole-window timings within unchanged deadlines | Unverified |

The S1-S14 cases and implementation order are defined in the proposal. Normal
transactional streaming remains disabled; this checklist does not qualify a
general native session provider, enable public admission or change F/CR PR #114.


## Bounded first-start implementation checkpoint (2026-11-08)

The proposal above is now implemented through existing test-only task admission.
[Bounded snapshot validation](dcg-bounded-snapshot-validation.md) records the exact
S1-S14 coverage and limitations, final source/binary provenance, eight isolated
edgeqa runs and verified cleanup. This supersedes the proposal's implementation
status, without declaring every acceptance permutation closed.

| Existing gates | New evidence | Remaining support requirement |
| --- | --- | --- |
| B3/B4/C7 | One bounded pending chunk, exact durable readback/retirement, closed-table EOF before B, final BASELINE then history and INITIAL; 12,000/30,000 native rows | General qualified provider and broader schema/driver qualification |
| B5-B7/C8 | Native revoked/expired/killed/budget-limited attempts leave no initialized F/W; independent broker evidence; replacement rejects partial attempt; missing-offset checks retained | Complete crash/metadata-transition qualification beyond the exercised boundaries |
| C2/C4/H8 | Held-fetch cancellation test and replacement progress, native drain/cleanup; unchanged 29-case Kafka ownership regression | Chunk-specific held-old-producer matrix and native cancellation latency qualification |
| C1/C3/H9 | Ordinary F/W restart without active plan; unrelated native CDC continues while clear is open; truncate precedes reload | Sustained load, deployment support and truncate-aware sink convergence |
| B4/C7 sizing | 30,000 rows in 132,863 ms on final build; 256-row chunk/257-record ledger; fewer native probes; explicit total budgets | No unlimited snapshot or hard pause/heap guarantee; full S14 performance matrix remains open |

The operator first-start window, retention requirements and normal transactional
streaming guard remain. Partial snapshot prefixes can be broker-committed; they
cannot authorize recovery or imply atomic snapshot visibility. F/CR PR #114 is
unchanged.


## Configured native candidate qualification (2026-11-08)

The [configured native profile](dcg-configured-native.md) connects the ordinary
operator-retention implementation to the packaged connector without test-provider
injection. [The validation report](dcg-configured-validation.md) is authoritative
for which gates passed at the final local revision. Historical stronger
retention-release/restore requirements above are not silently waived.

- C1/C3: full native inventory and binding acquisition in a shared single-use
  operator window; bounded snapshot acknowledgement, B, history and INITIAL F/W.
  Capture of every journal file is required; no implied filtered scope.
- C2/C4: terminal JDBC/RPC admission includes metadata handles; late reads cannot
  start work after stop; failed native close cannot claim drain. Broker fencing
  remains the publication authority; completed drain remains separately observed.
- C5/C6: normal configured schemas/keys and conservative output fingerprints,
  dedicated single-partition topics, schema-enabled JSON and explicit runtime
  capability/version checks. No SMT/converter/unsupported type substitution.
- C7/C8: restart from actual Connect F/W and immutable history, missing/partial
  initialization rejection with zero new output; new plans cannot replace a
  missing-offset generation that already has output/history.
- C4/H8/S13: prefix recovery across read budgets, bounded large-batch delivery,
  unrelated-table progress while a clear is open, and whole-batch replay after
  interrupted delivery. R remains separate from durable F/W.
- H9: exact linked receiver acquisition and retirement of obsolete ordering
  metadata below F, with startup anchor identities retained. Only a positively
  observed attachment race is retryable; no raced cut publishes output. The
  configured source yields after a bounded number of attempts and retries with
  unchanged positions. Existing strict exhaustion tests remain strict.
- Release/operations: target schema/journal eligibility, initial writer window,
  retention owner/budget and Kafka publication settings must match the declared
  source profile. Independent event/projection validation establishes the source
  contract; a particular downstream integration is not a connector release gate.
  No deployment, upstream push or F/CR change is part of this qualification.

## Automatic boundary scope qualification (2026-11-09)

The [native lock-scope experiment](dcg-lock-scope-validation.md) adds test-only
WRKJRNA file discovery, independent journal counts, and exact member acquisition,
including an extra file with a differently named member. Count mismatches,
unsupported resources, missing members and changed samples reject. It exercises
unfinished autocommit, schema and journaling races without connecting candidate
observations to startup admission. C7/B1/B2 remain open: stable samples do not prove
an atomic maintenance fence, historical schema origin or complete in-flight
control history. A concurrent journaling-change run exceeded the soft five-second
budget inside a native call; a brief maximum pause is not proved. Preserve the
existing failure guards and disabled automatic migration.
