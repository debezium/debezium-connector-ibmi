# Offline native journal fixtures

These files are regression inputs replayed from the checkout. They require no
IBM i connection, original host, library or account. Live runners create their
own fixtures on an explicitly supplied disposable system; see
[contributor testing](../../../../../docs/dcg-contributor-testing.md#optional-native-tests).

Recorded native qualification used IBM i 7.5. The original November 7, 2026
transaction captures used database group SF99950 level 11; later files came from
separate isolated runs. These samples do not qualify every release, PTF level,
CCSID or schema. [Validation coverage](../../../../../docs/dcg-validation.md)
records the runtime artifacts, scenario scope and limits. The repository retains
sanitized samples and aggregate results, not the original unsanitized capture
bundles or their local provenance files.

## Fixture inventory

| Files | Regression purpose |
| --- | --- |
| `dcg.json`, `initial.json`, `bindings.json` | Subtype-8 TRUNCATE and subtype-3 optimized DELETE, full rollback, nested/repeated savepoints, interleaved transactions and paired file/member journal IDs. The original simple tables contain integer IDs 1, 2 and 3. |
| `thread-enabled.json`, `thread-enabled-bindings.json` | Separately captured commit, rollback, interleaving and update before/after images with actual nonzero thread IDs. |
| `multiple-clears.json`, `multiple-clears-bindings.json`, `clear-only-savepoint.json`, `clear-only-savepoint-bindings.json` | Repeated clears, multiple affected tables and clear-only savepoint outcomes. |
| `ordinary-cdc.json`, `open-clear-progress.json` | Ordinary row output and unrelated-table progress while a clear remains open, including restart and interrupted output. |
| `noncommit-after-clear.json` | A clearing job inserts with commitment control disabled before COMMIT or ROLLBACK. Reject the mixed-mode change on that table while preserving unrelated-table progress. |
| `lifecycle-discrimination.json`, `lifecycle-discrimination-bindings.json` | Clear outcomes, an actual end/write/start journaling gap, SQL savepoint rollback, a duplicate-key surviving prefix and an atomic compound failure. The unjournaled source row has no journal image and must not be invented. |
| `administrative-entries.json`, `statistics-request.json`, `record-limit.json` | File open/close, save, authority grant, F/CB subtype-C statistics request and R/IL record-limit metadata. Validate identities and complete layouts without treating them as rows or transaction outcomes. |
| `save-while-active.json`, `reorganization.json` | Native save-while-active records and classic RGZPFM/F/RM subtype-E records. Declared-key acceptance remains separate from RRN-key and concurrent-reorganization rejection. |
| `file-deletion.json`, `transactional-file-deletion.json`, `transactional-ddl-rollback.json` | Physical deletion/recreation, commit and rollback, grant/revoke, constraints, triggers and retained historical names/layouts. |
| `new-file.json`, `new-member.json` | New physical-file and member associations with independently checked identities. |
| `logical-file.json`, `access-path.json` | Logical-file/index and access-path records alongside captured physical files. These records cannot establish row bindings. |
| `excluded-lob.json` | Pointer-row metadata for excluded LOB data. No external LOB value is read or qualified by this fixture. |
| `temporal-columns.json`, `temporal-rows.json` | Nullable DATE/TIME and TIMESTAMP(0/3/6/9), leap dates, pre-epoch fractions, extreme years and TIME 24:00:00. Extreme TIMESTAMP(9) values deliberately exceed the epoch-nanosecond converter range. |
| `native-binding-columns-M.json`, `native-binding-keys-M.json` | Captured native layout/key metadata for the mixed-column binding. |
| `automatic-fence-metadata.json`, `automatic-fence-native-scope.json` | Candidate metadata and complete journal/member scope, including a member whose name differs from its file. Current metadata alone is not historical binding proof. |
| `late-membership.json`, `equal-width-alter.json` | Unknown membership and INTEGER → REAL → INTEGER layouts. Unchanged endpoint definitions cannot replace the intervening journal history or authorize decoding with a stale schema. |
| `prospective-native.json`, `prospective-boundary.json`, `short-boundary-markers.json` | Older transaction controls, prospective row replay, active writers and raw A/N/Z marker validation. Fixture-origin assumptions remain explicit. |
| `pending-receiver-boundary.json`, `receiver-control-history.json` | Discovery floors from retained transaction controls, pending flags, complete receiver directories and raw audit intervals. Missing/changed observations cannot authorize progress. |
| `incremental-anchor.json`, `incremental-anchor-padding.json`, `census-comparison.json` | Exact anchors, entry bounds/padding and agreement between raw content and metadata summaries. |

## Preserve the evidence boundaries

The original `dcg.json` and `initial.json` rows have zero thread IDs. Tests retain
that rejection, including at nonzero buffer offsets. Explicit component tests
clone headers and supply synthetic thread ID 1 to exercise paired-ID
normalization and owned values; that substitution is not native thread evidence.
`thread-enabled.json` supplies the separate positive native rows. Swapped file
`*FILE` and member `*QDDS` journal IDs must still reject.

Some component tests select transaction controls and row/clear records to isolate
the transaction buffer. Complete lifecycle tests separately process the scoped
stream. A component projection never authorizes dropping maintenance records
from a production stream. The original SQL inventory also observed internal F/DE
entries omitted by the native retrieval profile.

The row-only projections in `late-membership.json` deliberately omit earlier
maintenance to isolate the final unknown-binding guard. The complete all-code
capture remains separate and rejects unsupported pointer-backed maintenance.
Prospective and boundary bundles remain `startup_proof=false`; a fixture's known
origin, native identity observations or successful offline replay cannot establish
runtime startup authority.

Temporal column metadata and several starting bindings/checkpoints include
explicit test definitions. The statistics-request fixture's unused file ID,
physical decoder and checkpoint are synthetic. Native scope metadata records
observations from a removed disposable fixture; its object tokens are not
identities to reuse on another system. Tests may mutate these inputs to exercise
failure paths, without promoting them into live observations.

The maintained lifecycle, normalizer, coordinator and dispatcher tests check
commit/rollback, every-entry restart, missing or mismatched companions, consumed
savepoints, historical schema acquisition and member restoration. Accepted
administrative metadata cannot publish a row, resolve a transaction, remove a
pending before-image or advance completed output W. Strict-profile rejection
and ordinary-profile acceptance are distinct contracts; preserve both where
exercised. Current schema-change support is described by the
[compatibility matrix](../../../../../docs/dcg-compatibility-comparison.md).

## Sanitization and byte integrity

Sanitization replaces fixed-header job, user, program, system and address fields,
and consistently maps job numbers and file/member journal IDs, including embedded
payload identifiers. Distinct producers and file/member identities remain
distinct. Thread IDs, record numbers, sequence/cycle values, timestamps, flags,
object types, layout offsets, null maps and synthetic application values retain
their recorded relationships.

Standalone entries set the next-entry displacement to zero. Unused trailing
buffer space is removed only after retaining every declared payload, null map
and receiver field. Public checksums describe the sanitized bytes. The deletion,
authority, constraint and trigger captures remove opaque object apply data while
preserving layout and creation-image length. External LOB descriptors/values are
not qualified by pointer-metadata checks.

Keep raw bytes, projected metadata, complete companion records and positive and
negative variants together. Inspect new captures before sharing them; original
journal buffers, job logs and JVM profiles can contain private source data.
IBM's [journal entry layouts](https://www.ibm.com/docs/en/ssw_ibm_i_74/rzaki/rzakivarlength.htm)
describe the object-level prefixes used by these tests.
