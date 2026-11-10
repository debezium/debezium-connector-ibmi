# D/CG source identity and protected recovery protocol

Status: design decision and acceptance contract, 2026-11-06. **The complete protocol
is not implemented or enabled.** The native identity candidate collector now has
local fixture coverage; see [validation](dcg-validation.md#native-object-identity-candidate-collection).
This follow-up to the [SQL API assessment](dcg-evidence-apis.md) identifies
an additional native identity API and separates read-only evidence from the
operational protection needed to use it. It changes no checkpoint encoding,
startup acceptance, streaming behavior, or F/CR PR #114.

**Profile scope:** this is the existing stronger protected-startup contract.
The [local runtime profile revision](dcg-runtime-profile-revision.md) proposes a
smaller CDC profile using standard Connect offsets/fencing where sufficient, with
retention-release and independent restore guarantees separate. It does not relax
this implementation's checks. N/P/H/B remain applicable until a reviewed per-gate
replacement has passed the [candidate C cases](dcg-startup-acceptance.md#candidate-cdc-profile-gate-c);
none is passed or waived by this documentation revision.

## Decision and feasibility

Use the documented local object identity tuple from `QSYS2.IFS_OBJECT_STATISTICS`
as the next identity evidence candidate. Combine verified native identities with
an explicitly enrolled source, retained receiver links, a bounded history cut,
and a separately established protection session. Do not derive incarnation from
names, timestamps, directory numbers, a user label, or a hash of those values.

Read-only services can observe native identities, links, entries and current
bindings. They cannot themselves install durable retention, exclude destructive
maintenance, stop writers, or establish an acknowledged downstream baseline.
The initial support profile therefore requires a controlled source environment.
If an environment cannot supply the protections below, transactional startup is
unsupported there. An operator acknowledgement or matching catalog reads are not
implementations of protection.

This is a candidate protocol with explicit release gates, not a claim of complete
IBM i feasibility or production readiness. In particular, native ID behavior on
the deployed release/PTF and an enforceable protection provider remain unverified.
The existing observation adapter must continue to reject both startup paths.

The initial profile is one enrolled local system, system ASP 1, one active local
journal, fixed single-member captured files and fixed decoding policies. Allow
ordinary receiver rollover only after continuity validation. Initially reject
sequence reset, restored/replaced receivers, source restore/clone/failover,
unsupported object lifecycle transitions, and any interruption of the protection
contract. Recover those cases through explicit re-enrollment and a new baseline,
not by editing offsets. The abstract receiver-order implementation's ability to
represent a reset does not establish support for accepting one from a real source.

## Additional API findings

| API / control | Documented fact | Use and limit in this design |
| --- | --- | --- |
| [IFS_OBJECT_STATISTICS][ifs] | `FILE_SYSTEM_IDENTIFIER`, `FILE_IDENTIFIER_NUMBER` and `GENERATION_IDENTIFIER` together identify an object on the system; the service includes QSYS.LIB objects. Columns are nullable and ignored errors can omit rows. | Collect the complete tuple for exactly named `*JRN`, `*JRNRCV`, file and member paths. A local object identity is not a global system identity or proof against a restored system image. |
| [stat64][stat] | Reports file-system/file identity and `st_ino_gen_id`; `st_vfs` identifies a mount instance and can change after IPL or reclaim. | Possible native cross-check, not a second independent proof. Do not use mount ID or timestamps as a durable incarnation. |
| [Qp0lGetAttr][attr] | Provides an opaque 16-byte file ID; unsupported attributes return zero length. | Candidate corroboration only. Do not assume that it encodes the SQL tuple, or infer undocumented bytes. |
| [QjoRtvJrnReceiverInformation][receiver] | Returns receiver attributes/links and takes shared-read locks for the API operation. | Does not document a session-long protection handle or a durable receiver incarnation field. |
| [QJORJIDI][jid] | Maps an object JID to its current object name/type; mappings can disappear after deletion/end-journaling followed by receiver change. | Useful for current object association, not historical format lookup or journal identity. Its list includes `*JRNRCV`, but that alone supplies neither journal incarnation nor a receiver-generation contract. |
| [QDBRTVFD][format] | Provides current record-format definitions and level identifier. | Pin an owned decoder description and hash its relevant fields; current format alone cannot decode arbitrary old history. |
| [QjoRetrieveJournalEntries][entries] | Exposes raw entries, selection/authority requirements and per-call locks; its pointer guidance refers to the deletion exit for retention. | Use owned entries and exact bounds. The call's locks do not establish retention between calls or across downtime. |
| [Delete-receiver exit][exit] and [DLTJRNRCV][delete] | An exit can veto deletion; privileged `*IGNEXITPGM` can bypass it. | Candidate enforcement for durable receiver pins. Does not prevent restore, metadata changes, journal replacement or privileged maintenance. |
| [ALCOBJ][allocate] | Lists supported object types and scoped lock lifetimes; `*JRN` and `*JRNRCV` are absent from the reviewed command list. | Do not prescribe journal/receiver ALCOBJ locks. File locks may help a separately validated writer fence but do not establish a whole-journal cycle census. |
| [SNDJRNE][marker] | Writes a U entry and can force the receiver to auxiliary storage. | Optional controlled-initialization marker candidate. It is a source mutation, not a read-only identity/census proof. |

The initial SQL assessment remains accurate for its implemented query set. This
additional research does not justify filling its missing-proof fields with the
new SQL values alone.

## Identity contract

### Local object evidence

The proposed `NativeObjectEvidence` contains the exact local path, object type,
ASP, and three numeric identifiers from `IFS_OBJECT_STATISTICS`, plus API/service
profile and observation provenance. Preserve exact integers; do not convert via
floating point, truncate, or replace null with zero. Zero is not a wildcard.
Do not reject a component merely for being zero without a documented reason.

Query an exact canonical QSYS.LIB path with parameter binding, no recursion and
error suppression disabled. Require exactly one matching path/type/ASP and all
identity components. Reject remote/symbolic resolution, missing/duplicate rows,
unsupported columns, SQL warnings, truncation and permission failures. Bound
path sizes, result sizes, time and query count. The concrete query and the path
behavior for journals, receivers and members need fixture and IBM i validation;
this document does not claim they have executed successfully.

Canonical, domain-separated tokens may encode the tuple with source enrollment,
ASP and object type. A digest only encodes verified evidence; it does not add
proof. The same name with a different tuple is replacement. Matching tuples
cannot alone rule out a source restore/clone or an in-place content rollback.
Re-read journal and receiver identity around each bounded raw retrieval; discard
the result on mismatch. Protection must prohibit changes between those reads;
matching reads do not protect against a change-and-change-back race.

### Source enrollment and receiver lineage

`systemIdentity` must have an explicit provenance model. An enrollment record
binds a generated epoch to the authenticated source endpoint/partition, native
journal identity, fixed capture policy, baseline and its source boundary. Keep
it in durable storage available to a replacement worker. A hostname, serial
number, TLS endpoint or epoch UUID alone is not a source incarnation.

The epoch is a connector recovery namespace established by the controlled
procedure below, not an IBM-supplied journal-chain UUID. Scope native object
identities to that enrollment. Restore, clone, endpoint reassignment and failover
require revocation before maintenance and new enrollment afterward. The design
trusts the administrator controlling that maintenance boundary; it does not
promise detection of an administrator cloning both the source and all evidence.
If that trust/maintenance boundary is unavailable, fail closed.

Maintain an ordered receiver manifest with native identities, qualified names,
previous/next links, attachment status and verified sequence ranges. Extend it
only after reading the link between the last accepted receiver and the next one.
Bind both endpoints to native identities under protection, check reciprocal
links, and reject forks, gaps, name reuse and reset. Persist any manifest segment
needed by a checkpoint before publishing that checkpoint. Do not recompute a
different lineage from today's receiver directory and silently adopt it.

The source epoch can remain stable across ordinary rollover; the manifest grows
without changing checkpoint identity. Required receiver anchors and links must
remain verifiable after a worker moves. Do not require indefinite retention of
the original baseline receiver solely to retain its epoch: retain its accepted
enrollment/manifest evidence durably, and retain all actual entries still needed
by F and any exact boundary probe.

This is a planned refinement of the current opaque `JournalIdentity` strings.
It requires typed provenance, persistence and validation changes. Do not feed an
enrollment label directly into the current SPI and call it authoritative proof.

## Protection contract

Separate three obligations; none implies the others.

**Durable retention** covers every receiver needed by the oldest acknowledged
replay floor F, any older still-valid durable checkpoint, recovery probes and
active inspectors. It survives connector crash, worker reassignment and provider
unavailability. A candidate implementation is a deny-by-default deletion exit
backed by a durable pin registry. Missing/corrupt state or unavailable registry
denies deletion. Installing it, changing receiver management and granting its
authority are separate operational changes requiring explicit authorization.
The connector must not install them automatically. An existing retention service
is acceptable only if it implements the same tested contract.

Never release a pin based on scan position R, enqueue of a heartbeat, elapsed
time or loss of the connector's lease. Release only after the newer complete
checkpoint and its manifest are durably acknowledged and no inspector or older
accepted checkpoint needs the receiver. A crash between saving evidence and
saving an offset may retain too much; it must never delete too early. Define how
checkpoint rollback/backups are invalidated before garbage collecting their pins.

**Maintenance protection** excludes journal/receiver delete, restore, replace,
rename, sequence reset, journaling-option changes, and captured object/member/
format changes from the start of proof collection through the required replay
window and streaming handoff. Continue it during capture for the supported fixed
schema profile. Require an enforceable administrative access/workflow boundary,
with every ordinary maintenance path passing through the protection provider.
Normal appends and validated rollover may continue. A read-only catalog query,
`DLTRCV(*NO)`, an exit-registration check or a lease heartbeat alone cannot prove
this exclusion. A provider unable to establish exclusion must refuse acquisition.

The provider owns a durable epoch and session generation. Revocation first closes
the connector's retrieval/publication gate and stops or fences its reader; only
after that barrier may maintenance proceed. An expired session, network partition
or unresponsive old worker must not automatically release retention or authorize
maintenance. Require an explicit recovery/barrier procedure; an unsafe override
invalidates the enrollment and forbids automatic resume. Prevent concurrent
owners and stale workers from publishing after a new owner takes over. Checking
a token once before dispatch is not an atomic fencing implementation.

**Initialization writer fence** additionally blocks all producers that can start
or extend a cycle on this journal, including API resources and currently unrelated
tables. Drain existing cycles under that fence and keep it through baseline and
reader handoff. A table-only lock does not prevent an unrelated cycle from later
touching a captured table. A supported provider must identify the covered writers,
how they are excluded, and how loss is detected; a human assertion of quietness
is not sufficient input to `Handoff`.

These are operational requirements, not a new blanket claim that IBM exposes a
single lock or lease implementing them. Provider implementation and contention,
revocation, IPL and outage tests are release gates. An application pause or broad
maintenance window may be necessary for initialization; neither is performed by
this design work. Do not put privileged maintenance commands in the read-only
collector.

## Bounded history and resume

Resume from the acknowledged F/W pair, never from R. Acquire maintenance
protection and durable pins before trusting the catalog or fetching history.
Match enrollment, native journal identity, fingerprint and checkpoint receiver
identities. Validate the manifest path through an explicit upper cut H. H must
be readable in retained storage; a reported last sequence that includes cached
entries is not an accepted boundary. [IBM documents that distinction][cache].

Read each receiver with explicit bounds and no object/member/JID filters that
could hide historical changes. Account for every entry in the required interval,
including internal entries where supported. Use the SQL metadata scan to detect
holes and coordinate coverage; use owned raw entries and release-specific decoders
to classify the relevant lifecycle and transaction semantics. Both views must
describe the same protected interval and identities. Matching counts alone do
not establish raw-history equivalence. Any unavailable entry, warning, unknown
lifecycle/control, unsupported internal-entry coverage or undecodable required
payload prevents `History.complete`.

Bind current file/member native IDs, object JIDs, full decoder format and logical
mapping to the enrollment. Validate changes throughout the required replay window.
The fixed-schema protection must cover the earlier interval since enrollment or
a previously accepted durable binding proof, not merely the new inspection.
A format change followed by a reversal before F is not made safe by comparing
today's format with the baseline. No historical protection/proof means rebaseline.
Unknown D/CG companion transitions remain blocked until their JID/member/format
effects are decoded; this protocol does not silently whitelist F/CH, F/EJ, D/EF,
D/JF or F/JM.

The inspection owns a stable **cut**, not a promise that the source never appends.
Freeze H and verify the prefix through it. Later appends can be consumed only by
extending the validated range under the same protection and receiver admission
rules. Never substitute a later tail during a retry or skip entries between H
and the first live read. Bind the complete history result to the cut, manifest,
native identities, protection generation and pinned decoder policy.

Transfer the still-held protection session to the real reader without a release/
reacquire gap. Serialize ownership with shutdown and revocation. A failure before
transfer closes the unused reader; a failure after transfer terminates it and
preserves the last acknowledged F/W and pins. Resume rebuilds all cycles from F,
suppresses only completed batches through W, and preserves whole-batch replay.
Ordinary restart need not drain open cycles if all these resume proofs hold.
If they do not hold, do not turn resume into implicit initialization.

The current `Verified.recheck()` requires equality of the entire observed tail
and receiver list. That is conservative but can reject harmless appends. A future
bounded-cut adapter must explicitly implement and test prefix stability and
range extension before changing that rule. No recheck is relaxed by this document.

## Controlled initialization and re-enrollment

This is a protocol specification, **not an executable runbook or authorization
to change a source**.

1. Acquire maintenance protection, durable retention and the journal-wide writer
   fence. Establish a fresh pending enrollment and exclusive output ownership.
   For legacy migration, define a new downstream baseline/generation so previously
   emitted uncommitted events cannot survive as valid state.
2. Under the held fence, enumerate all commitment definitions with verified
   cross-user authority and query each journal resource. Reject unavailable,
   partial, ambiguous, prepared/distributed or unresolved state. Wait only within
   a bounded budget for normal completion; do not commit/roll back other jobs.
   Zero pending changes or 100% rollback progress does not prove terminal outcome.
   [The transaction service documents those distinctions][transactions].
3. Establish an exact durable boundary B after the complete empty cycle census.
   If no suitable entry exists, an explicitly authorized, forced U marker is a
   candidate way to produce one. Bind its owned bytes and coordinate to the pending
   enrollment and read it back. Forcing a marker does not prove the census, fence
   or native identity. Do not fake a C/CM record or relax the normalizer to accept
   an arbitrary marker. Boundary-marker handling needs its own implementation
   and tests. Verify a readable durable cut through B and no gaps to handoff.
4. While writers remain fenced, obtain pinned bindings and a consistent baseline.
   Complete baseline delivery and acknowledgement, including any required sink
   completion barrier. A snapshot query returning or a Kafka enqueue is not this
   barrier. Record the baseline identity, source B and capture fingerprint.
5. Persist a complete enrollment/manifest/baseline record before publishing the
   initial checkpoint F=B (exclusive), W=B. Accept that checkpoint only if its
   referenced record is durable and complete. A pending record is never enough.
   A crash at any earlier step keeps capture blocked and the writer fence held
   or explicitly invalidated; it does not resume from a guessed boundary.
6. Construct the reader at the verified continuation after B, transfer maintenance
   protection and pins, durably acknowledge the initial checkpoint, then release
   only the initialization writer fence. Normal append processing begins under
   the retained maintenance and retention contract. If a crash makes fence or
   baseline status uncertain, stop for explicit recovery; never auto-release.

Re-enrollment is a new epoch and baseline decision. It never rewrites an old
checkpoint to make a replacement source look continuous. Whether an existing
downstream baseline can be preserved is a separate migration proof.

## Implementation boundary and next slice

Keep `As400JdbcStartupEvidence` diagnostic until the following providers exist:
native identity evidence; an enforceable protection/pin provider; durable
enrollment/manifest storage; whole-journal census under a held fence; owned raw
history classification; and reader/publication fencing and handoff integration.
Do not replace those providers with config booleans.

The exact-path native object identity collector and fixture tests are implemented
locally as `As400JdbcNativeIdentityEvidence`. It returns **candidate evidence only**
and leaves the existing missing-proof/startup rejection intact. Check positive
identity behavior on a separately authorized disposable IBM i before promoting
it to incarnation proof. In parallel with later integration, choose and validate
a concrete protection provider with the operators and upstream maintainers;
no provider has been selected or installed in this work.

The [protection feasibility proposal](dcg-protection-feasibility.md) compares
enforcement options and maps them to P1-P8. It recommends a conditional disposable
prototype with scoped retain-all protection, enforced maintenance controls and a
verified worker/publication barrier. It selects no deployed backend, passes no
release gate and does not authorize installing source-side components.

The [manual native identity probe](dcg-native-identity-runbook.md) prepares bounded,
sanitized collection using this exact collector. Its offline plan and fixture tests
do not execute IBM i SQL; live collection requires a separately approved disposable
source scope and does not promote candidate observations to proof.

The collector uses an exact starting path and object-type parameter, recursive
traversal disabled and error suppression disabled. The service's documented
`SUBTREE_DIRECTORIES => 'NO'` restricts traversal; it is not an exclusive-object
lookup guarantee for directory-like objects such as *FILE. The adapter accepts
only one exact matching result and never scans a parent library as a fallback.
An absent starting-object row or unexpected additional row fails. The service's
internal work and starting-directory behavior still need release-specific tests;
fixture success is not evidence of those behaviors.

Plan a new checkpoint/proof schema revision when enrollment and typed identities
are integrated; do not reinterpret existing local v2 opaque tokens as this new
protocol. Separate durable R/F/W semantics remain unchanged. Checkpoint references,
manifest-before-offset ordering, pin garbage collection and external baseline
storage require explicit codecs and crash tests.

The [acceptance matrix](dcg-startup-acceptance.md) defines the evidence required to
advance each gate. Native identity alone is not permission to enable streaming.

## Sources and research limits

IBM documentation was reviewed on 2026-11-06. Search-indexed IBM documentation
provided the SQL column/parameter details; several direct pages returned HTTP 403.
The stat64, ALCOBJ and file-ID path references were also readable through the web
tool. Context7 returned an unrelated snippet and its CLI rejected the prescribed
`--research` option, so IBM primary sources were used. No service call, source
lock, journal marker, privilege change, exit installation or live test was run.
Release/PTF availability and native identity lifetime behavior still require
validation; different release documentation is not deployed-release evidence.

[ifs]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-ifs-object-statistics-table-function
[stat]: https://www.ibm.com/docs/en/ssw_ibm_i_74/apis/stat64.htm
[attr]: https://www.ibm.com/docs/ssw_ibm_i_74/apis/qgetattr.htm
[receiver]: https://www.ibm.com/docs/en/i/7.6.0?topic=ssw_ibm_i_76%2Fapis%2FQJORRCVI.html
[jid]: https://www.ibm.com/docs/en/i/7.6.0?topic=ssw_ibm_i_76%2Fapis%2FQJORJIDI.html
[format]: https://www.ibm.com/docs/en/i/7.6.0?topic=q-retrieve-database-file-description-qdbrtvfd
[entries]: https://www.ibm.com/docs/ssw_ibm_i_74/apis/QJORJRNE.htm
[exit]: https://www.ibm.com/docs/en/i/7.4?topic=ssw_ibm_i_74%2Fapis%2FXDLTRCV.htm
[delete]: https://www.ibm.com/docs/en/i/7.5.0?topic=d-delete-journal-receiver
[allocate]: https://www.ibm.com/docs/fr/ssw_ibm_i_74/cl/alcobj.htm
[marker]: https://www.ibm.com/docs/en/ssw_ibm_i_74/cl/sndjrne.htm
[cache]: https://www.ibm.com/docs/en/i/7.6.0?topic=c-change-journal
[transactions]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-journal-info-table-function
