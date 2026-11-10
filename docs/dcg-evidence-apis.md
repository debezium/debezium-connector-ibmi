# D/CG read-only startup evidence: API assessment

Reviewed 2026-11-06 for the staged D/CG work in draft PR #115. No live IBM i
connection, SQL execution, source mutation or Kafka validation was performed.

## Decision

Implement collection of documented observations, but do not manufacture the
stronger proofs required by `As400RecoveryStartup`. The reviewed services expose
useful state and journal history; the assessment did **not** establish a stable,
replacement-resistant identity scheme for system/journal/chain/receiver
incarnations, or a protected inspection spanning all queries. That is an
implementation/design gap, not a claim that every possible IBM i interface
has been exhausted.

The subsequent [source identity and protected recovery protocol](dcg-startup-protocol.md)
identifies `IFS_OBJECT_STATISTICS` as an additional native identity candidate and
defines retention, maintenance and initialization-fence requirements. That service
is not in this collector's query set. It is now queried by the separate
`As400JdbcNativeIdentityEvidence` candidate collector with
[local fixture coverage](dcg-validation.md#native-object-identity-candidate-collection).
The protocol and its [acceptance matrix](dcg-startup-acceptance.md) still provide
no verified startup proof or streaming enablement.

`As400JdbcStartupEvidence` runs only fixed, parameterized SELECT statements.
`As400StartupEvidence.Report` separates observations, findings and missing proof.
The adapter implements the startup `Source` interface, but all methods requiring
verified proof throw `EvidenceUnavailable`, carrying the report. Both resume and
initialization therefore fail before constructing a coordinator. It does not
invent a journal identity from names, timestamps, directory numbers or file JIDs;
does not return `History.complete=true`; and does not assert a writer fence,
durable baseline or complete cycle census.

## What the reviewed APIs establish

| Source | Usable observation | Limit for this design |
| --- | --- | --- |
| `QSYS2.JOURNAL_INFO` | Current qualified journal, ASP, local/remote state, attached receiver and recording options | Does not supply the required stable journal/chain incarnation or hold a source protection lease |
| `QSYS2.JOURNAL_RECEIVER_INFO` | Qualified receiver names, explicit previous/next names, status, sequence range, attach timestamp, pending-transaction indication | Links are name-qualified; timestamps and statuses are observations, not durable receiver incarnation tokens |
| `QSYS2.JOURNALED_OBJECTS` and `SYSPARTITIONSTAT` | Current journal/file association, physical/logical type, image/omission options, current members and SQL/system name mapping | Current scope does not establish historical object JID/record-format continuity |
| `QSYS2.DB_TRANSACTION_INFO` and `DB_TRANSACTION_JOURNAL_INFO` | Caller-visible commitment definitions and their current journal resources/cycles | Visibility is authority-dependent; successive reads are not a protected journal-wide census or writer fence |
| `QSYS2.DISPLAY_JOURNAL` | Entry coordinates, code/type, object metadata and object JID within explicit receiver/sequence bounds | SQL metadata is not an owned RJNE0200 record or a decoded lifecycle-history proof; caching, missing entries and unavailable service options can prevent complete reads |

The [JOURNAL_INFO documentation](https://www.ibm.com/docs/ssw_ibm_i_74/rzajq/rzajqviewjournalinfo.htm)
describes current state, recording options and required authority. The collector
records those facts, checks for local/active state, and conservatively limits
this diagnostic profile to system ASP 1; IASP and other ASP observations cannot
authorize a scan.

The [receiver view documentation](https://www.ibm.com/docs/en/i/7.4.0?topic=services-journal-receiver-info-view)
distinguishes attached/online/saved receivers from freed, partial and empty
receivers and documents explicit links and ranges. The collector follows links
from the requested floor, checks reciprocal names, duplicates/forks, the
attached tail and availability, and refuses to substitute a newer suffix.
Sequence reset is reported as requiring proof rather than inferred to be safe.

The [Retrieve Journal Information API](https://www.ibm.com/docs/en/i/7.6.0?topic=q-retrieve-journal-information-qjoretrievejournalinformation)
states that receiver-directory numbers change as receivers are added/deleted.
They cannot be persisted as incarnation tokens. The
[J/NR and J/PR layouts](https://www.ibm.com/docs/en/i/7.5.0?topic=information-layouts-variable-length-portion-journal-entries)
describe receiver names/libraries; the entry description's word “identifier”
does not establish a replacement-resistant receiver token.

[JOURNALED_OBJECTS](https://www.ibm.com/docs/en/i/7.5.0?topic=services-journaled-objects-view)
describes external journaled objects, including file kind and image settings.
[SYSPARTITIONSTAT](https://www.ibm.com/docs/en/i/7.5.0?topic=views-syspartitionstat)
describes current members/partitions and does not include distributed partitions
on other nodes. Neither query is treated as historical schema evidence.
Missing/duplicate scope, multiple members, after-only images and omitted entries
are reported conservatively; the collector does not construct a row decoder.

[DB_TRANSACTION_INFO](https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-info-view)
can expose a user's own definitions without the broader authority required for
other users.
[DB_TRANSACTION_JOURNAL_INFO](https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-journal-info-table-function)
describes journal resources for one lock space and a nullable current commit
cycle. The collector queries every visible definition, without a captured-file
filter, and records matching journal resources. It does not turn zero visible
rows, zero pending changes, or receiver-level pending flags into a complete empty
census. Holding writers stopped and establishing a consistent downstream baseline
remain separate requirements.

## Bounded journal-history observation

The [DISPLAY_JOURNAL reference](https://www.ibm.com/docs/en/i/7.5.0?topic=services-display-journal-table-function)
documents both explicit sequence endpoints and filtering pitfalls: using an
object selection can exclude older object JIDs, and sequence reset can make an
ending sequence match an earlier receiver. The collector therefore reads each
receiver separately with explicit library/name and numeric bounds, requests all
codes/types and internal entries, and uses zero EOF delay. It does not apply
object, member, job, user, commit-cycle or current-JID filters.

It selects metadata only, including `HEX(JOURNAL_IDENTIFIER)`; no `ENTRY_DATA`,
row values, large object payloads or pointer-backed data are read. An object JID
is kept as an object observation, never reinterpreted as a journal or receiver
incarnation. Rows are ordered within one receiver, checked for duplicates,
regressions and bounds, and compared to the expected sequence cardinality.
A missing endpoint or sequence remains incomplete. D/F/J activity and unknown
controls require further classification; this collector cannot certify their
payload semantics or historical format continuity.

An apparently gap-free result only describes that returned interval. The service
omits journal-cached entries not yet on disk. Removed internal entries or unavailable
receivers cannot be recovered by requesting internal entries. A full read still
does not prove protected retention or rule out replacement during observation.

## Availability and failure behavior

[IBM's 7.5 change history](https://www.ibm.com/docs/en/i/7.5.0?topic=optimization-whats-new-i-75)
lists `JOURNAL_RECEIVER_INFO` in the initial release and transaction-journal
inspection and internal-entry filtering among later updates. Release number alone
does not establish the installed service/PTF level. The collector has no fallback
that drops an unavailable column/parameter, changes to a filtered query or assumes
a successful empty result. SQL errors, missing services/columns, warnings,
timeouts and row/query-limit exhaustion fail collection.

The connection factory must return a new owned connection with bounded connection
establishment. The collector requests and checks JDBC read-only mode, uses only
the fixed SELECT allowlist, applies statement timeouts and aggregate row/query
budgets, and reads a row beyond the budget as a truncation sentinel. JDBC read-only
mode is a driver hint; the fixed query set is what prevents mutation statements.
Result sets, statements and connections close on success and failure. No commands,
DDL, DML, commits, rollbacks, authority changes, receiver management or source
locking are issued.

Catalog reads before and after collection detect visible changes. Equal reads
are **not** called a lease or atomic snapshot. Collector connections are closed
before returning an inspection adapter, which consequently cannot supply the
validator's protected-observation or handoff contract.

## Verification and remaining gates

Hand-authored SQL result fixtures drive the real collector through mocked JDBC
transport. Tests cover parameter binding, explicit per-receiver bounds, sequence
reset, missing/replaced links, unavailable receivers, malformed/partial results,
metadata races, caller-visible transactions, SQL warnings/errors, resource bounds
and cleanup. They also pass the real adapter into both startup paths and require
rejection before a coordinator is created.

These tests do not execute SQL against Db2 or prove installed service support,
source authority, source identity, lifecycle-history interpretation, snapshot
consistency, Kafka ordering or sink convergence. No auction fixture was touched.
The current transactional streaming guard and F/CR PR #114 remain unchanged.

Before enabling buffered streaming, implement and validate the incarnation and
protection providers described in the [follow-up protocol](dcg-startup-protocol.md).
Its native identity lifetime and enforceable protection gates remain open.
Complete the lifecycle/RJNE0200 reader and effective-policy
adapters, protected reader handoff, Kafka acknowledgement tests and authorized
disposable-source validation. Do not weaken the validator to make observational
SQL results pass.
