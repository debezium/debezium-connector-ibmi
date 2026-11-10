# Native evidence gates for an online legacy handoff

## Decision, 2026-11-09

The [subsequent native validation](dcg-native-handoff-followup.md) records the
retry after AWS session renewal, additional IBM documentation and the fresh
fixture's actual collector/replay results. The expired-session record below is
retained as the outcome of this earlier attempt.

Automatic legacy migration remains disabled. The documentation now gives a more
specific basis for terminal **deposition order**, and the test capture/handoff can
carry actual native journal/receiver observations instead of fixture hashes.
Neither improvement establishes complete concurrent transaction discovery or
historical schema coverage for an arbitrary running legacy connector.

The existing [local handoff](dcg-online-handoff-local-validation.md) remains
conditional on complete retained context. Its previous Kafka result is unchanged;
it is not a live test of the native observation extension described here.

## What IBM documents, and what remains unproved

| Gate | Documented basis | Conclusion for this implementation |
| --- | --- | --- |
| Identify a visible journal transaction | [DB_TRANSACTION_JOURNAL_INFO][resources] exposes the current cycle's starting journal sequence, and distinguishes it from pending-change and progress counts. | Fetch the real retained start. Zero pending changes and 100% progress do not establish completion. |
| Commit terminal deposition | IBM's [commit operation][commit] steps place C/CM deposition before marking the transaction ended in its commitment definition. | This is affirmative evidence for local record-change terminal ordering. Continue validating the actual terminal and supported outcome. |
| Rollback terminal deposition | IBM's [rollback operation][rollback] steps place C/RB deposition before the final commitment-definition state change. The [entry sequence][sequence] also identifies start and terminal entries. | This strengthens the full-rollback case. Savepoints, nested cycles, object-level transitions and partial outcomes retain their existing checks. |
| Visibility to retrieval | [QjoRetrieveJournalEntries][retrieve] documents bounded retrieval and native continuation; [journal caching][cache] affects visibility. | Caching remains rejected. Deposition order alone does not explicitly specify the synchronization between SQL current-state enumeration/resource lookup and the API's readable tail in every allowed race. This cross-interface implication is not promoted to a proved guarantee. |
| Quiet transaction present throughout discovery | [DB_TRANSACTION_INFO][definitions] describes commitment definitions and authority. | The reviewed reference does not state that a persistent definition/resource cannot be omitted while other definitions are created, ended or reused. A successful finite experiment cannot supply that missing contract. |
| Object identity | [IFS_OBJECT_STATISTICS][identity] documents the combined filesystem/file/generation identifiers as identifying an object on the system. | Reuse the existing exact-path collector and ordinary-native token derivation. Equality is an observation under the existing retention/maintenance contract, not restore/clone protection. |
| Receiver chain | [JOURNAL_RECEIVER_INFO][receivers] exposes links, ranges, state and recording settings. | Check the exact retained chain and original readable anchors. Reject attachment changes, replacement, unsupported profiles, missing links and range regression. |
| Historical bindings | Native binding acquisition checks current file/member association, JIDs and physical schemas. | Record acquisition bounds and exact serialized definitions. Metadata acquired after required history is insufficient. Even matching current metadata does not prove all earlier schema versions. |

The commit/rollback ordering conclusion is narrower than a blanket claim that
all inventory/API races are resolved. A definition changing state, a resource
lookup returning no row, and the retrieval API exhausting a chosen interval are
different observations. The online proof still needs a documented relationship
between them and completeness for persistent definitions/resources.

Reviewed alternatives do not provide a drop-in complete inventory:

- [QTNRCMTI][current] is scoped to the requesting program's commitment context;
  it does not enumerate arbitrary producer jobs.
- `PENDING_TRANSACTIONS` on a receiver describes that receiver's pending work.
  `NO` on the oldest **available** receiver does not prove that an unavailable
  earlier receiver contains no silent transaction. Nor is it a historical
  transaction inventory at the legacy publication cut. The experiment does not
  adopt it as an empty-boundary certificate.
- Repeated equal or empty SQL observations do not repair an unproved enumeration
  contract. A fixed time delay does not prove terminal visibility.
- A complete replay from an independently established transaction-context origin
  can avoid current-state enumeration. The supported recovery implementation
  already does this with validated F/W and binding history. Ordinary legacy
  offsets do not supply that origin. Retention from a proved journal creation or
  another qualified baseline is a possible restricted route, not a new feature
  established here. Sequence 1, the oldest retained receiver, and a manufactured
  F-1 are not accepted as that proof.

For arbitrary legacy deployments, no supported writer-pause-free replacement for
the missing discovery guarantee was established by this review. The existing
operator-controlled first-start path is not silently substituted: the requested
online approach must preserve ongoing writers and ordinary CDC.

## Test-only implementation

`As400OnlineNativeEvidence` composes the existing
`As400OperatorJournalEvidence` and `As400JdbcNativeIdentityEvidence` collectors.
Before a fixed capture it observes the exact linked receiver chain, then collects
the journal and receiver native tuples. After capture it repeats both observations.
The finite scope is at most 16 receivers plus their journal, with the collectors'
parameterized read-only SQL, five-second query limits, warning/error rejection and
the capture's existing process/page/entry/byte bounds. An attached receiver may
grow beyond the fixed endpoint. Receiver rollover during the observation, changed
identities or links, missing objects and incompatible recording settings reject
the capture; they do not yield usable migration evidence.

The retained report preserves both observations and exact from/through coordinates.
Deserialization rechecks the evidence instead of accepting a saved token string.
The handoff loader derives the same `ordinary-native-v1` tokens as the existing
ordinary reader. It requires the identity window to match the captured history,
and requires every captured receiver to have an observed identity. A partial
native evidence bundle rejects; it cannot silently fall back to fixture tokens.
Older explicitly synthetic bundles retain their separately labeled fixture path.

Binding acquisition additionally saves its actual journal coordinates, native
journal identity and exact serialized schema hash. The exporter retains the
original schema bytes; the Java loader checks their hash and their equality to
the supplied definitions. Acquisition must precede the captured baseline within
the same receiver in this narrow experiment. A later schema observation cannot
certify earlier journal history. The report still says
`historical_schema_proof=false`; these checks establish provenance, not temporal
coverage of an arbitrary live schema history.

The exporter accepts either the prior separate offline audit or a completed native
census report whose independent writer ledger matched, observation windows passed,
unrelated writes progressed and fixture cleanup succeeded. This is transport of
bounded evidence into the conditional handoff, not native admission. The Java
loader validates the native evidence semantics. The explicit
`CONTROLLED_FIXTURE_ASSUMPTION`, `startup_proof=false` and missing native visibility
guarantee remain visible throughout. No normal-task hook, ownership service,
offset migration, public configuration or writer fence is added.

## Validation and live status

The extension's local evidence is under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-native-handoff-gates-20261109/`.
It covers object replacement, missing/duplicate observations, attachment/chain and
profile changes, range regression, out-of-range anchors, attempted promotion to
startup proof, metadata acquired too late, reversed acquisition bounds, changed
schema bytes/hash/journal, partial native bundles and exact interval mismatch.
The fixture's passing path checks that the existing handoff uses the supplied
native-format identity tuples while still requiring the completeness assumption.
Those unit tuples are synthetic and are not labeled as new IBM i observations.

Local `verify -Passembly -DskipITs` passed all 2,545 Java tests, including 20
new native-observation/provenance cases. All 53 Python harness tests passed,
including rejection of partial native evidence during export. Plugin packaging,
formatter validation and import checks passed. These checks do not connect to
IBM i; the previous Kafka fault experiment was not rerun or relabeled as new
native evidence.

The AWS `legacy-mfa` credential preflight returned `ExpiredToken`. No IBM i
connection, fixture creation or native mutation occurred in this attempt. There
is therefore no new native cleanup to claim. Native validation of the added
collector composition and its timing cost remains pending session renewal.

A targeted authorized retry uses a fresh absent `DCNCdddd` library, the existing
`tools/dcg-online-census-probe.py` controller and its verified cleanup. It must
capture identities around native reads, acquire schemas before the baseline,
retain original bytes/observations and keep the unrelated autocommit writer
moving. Silent transactions, terminal races and receiver rotation remain useful
qualification cases. Such a run can validate the collector integration, but
cannot by itself settle the undocumented discovery guarantee. It must retain
`startup_proof=false` even if every controlled schedule passes.

## Explicit stopping condition

Do not enable automatic migration until persistent definition/resource visibility
and the required terminal/API visibility relationship are established for the
supported local, noncached, non-XA profile, or a complete historical origin makes
that inventory unnecessary. Complete retained schema history and the native
reader/publication boundary must be qualified together as well.

The precise external question is whether, with full authority and no query warning
or truncation, a definition/resource that stays relevant for the whole observation
must be returned despite other concurrent changes, and whether disappearance of a
previous cycle guarantees that its terminal is retrievable by the subsequent
fully exhausted bounded journal read. This review sends no IBM support request.

An online preflight must be bounded and must decline without changing the legacy
offset, publishing a migration checkpoint or waiting indefinitely for a dirty
transaction. Ordinary CDC remains the active path after rejection. This extension
does not install that policy into the normal task; automatic migration is still
disabled. F/CR and production Java remain unchanged, and nothing is pushed.

## Documentation access limits

The current Context7 CLI resolved IBM i, but returned no relevant API text; its
documented `--research` retry was rejected as an unknown option. Direct IBM pages
returned HTTP 403 in this environment. IBM's search-indexed primary references
provided the field definitions and commit/rollback steps above. This is a targeted
review of those public interfaces, not a claim that every IBM interface or PTF
implementation was inspected. No inference is presented as IBM confirmation.

[definitions]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-info-view
[resources]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-db-transaction-journal-info-table-function
[commit]: https://www.ibm.com/docs/en/i/7.5.0?topic=work-commit-operation
[rollback]: https://www.ibm.com/docs/en/i/7.5.0?topic=work-rollback-operation
[sequence]: https://www.ibm.com/docs/en/i/7.6.0?topic=objects-sequence-journal-entries-under-commitment-control
[retrieve]: https://www.ibm.com/docs/en/i/7.4.0?topic=q-retrieve-journal-entries-qjoretrievejournalentries
[cache]: https://www.ibm.com/support/pages/basics-journal-cache
[identity]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-ifs-object-statistics-table-function
[receivers]: https://www.ibm.com/docs/en/i/7.5.0?topic=services-journal-receiver-info-view
[current]: https://www.ibm.com/docs/en/i/7.5.0?topic=q-retrieve-commitment-information-qtnrcmti
