# Durable binding history for ordinary D/CG recovery

Status: implemented adapter and local Kafka qualification, 2026-11-07.
Transactional streaming remains disabled. No IBM i connection, production task
enablement, push, or F/CR PR #114 change occurred in this continuation.

## Implemented behavior

`As400BindingDefinition` stores the distinct physical-file and member journal IDs,
native format identity, complete Debezium column/key schema, nullability and explicit
physical codec parameters. It uses standard Debezium table attributes and
`JsonTableChangeSerializer`; there is no Java object serialization or default-CCSID
fallback. The schema, codec type, physical record length and field count must agree.
Constructing a definition is not proof that its native association was observed.
The initialization adapter must still establish that association at a consistent
boundary before calling the history writer.

`As400BindingHistory` wraps the existing `SchemaHistory` API. Each dedicated history
store contains one immutable, complete binding epoch for a fixed capture set.
Explicit initialization writes all tables in one history record, closes that
backend, and verifies it through a fresh backend before returning an initial F/W
checkpoint. Exact retries are idempotent. A write failure, discarded/truncated
record, lost acknowledgement or failed readback cannot return a usable checkpoint.
There is no automatic initialization during resume or overwrite of existing history.

The fingerprint includes the complete canonical schema/codecs, original baseline
and receiver identity, journal identity, and effective capture policy. Recovery
validates every record, including records that a normal position comparator might
otherwise filter out. Missing, partial, foreign, future, noncanonical, altered or
conflicting history rejects. Byte-equivalent semantic duplicates are allowed.
The original fixed epoch can outlive its baseline receiver once the replay floor
moves forward; the ordinary profile forbids journal sequence resets and unsupported
source restore. This is not independent detection of coordinated history/offset
rollback or an old source backup.

The qualified Kafka backend is `As400KafkaBindingHistory`, a subclass retaining
Debezium's standard writer and storage management but supplying a strict recovery
loop. Base `KafkaSchemaHistory` skips null/missing-field records before its listener
and logs some parse failures; listener validation alone cannot enforce this contract.
The qualified loop reads raw bytes, rejects null/invalid UTF-8/JSON/incomplete
records, requires a contiguous prefix starting at offset zero, bounds records/bytes
and polling, and rejects a changing history end offset. Direct use of the
unqualified Kafka provider is rejected by the adapter. This first profile requires
a dedicated single-partition, nontransactional, immutable history topic.

The separate history-backed `As400OperatorCutReader` constructor requires a
successfully recovered manifest. It performs no current table metadata queries,
and retains journal/receiver native identity, receiver-chain/profile, range, F/W
anchor, page/byte limits and re-observation checks. Its returned cut identifies
historical bindings explicitly; the list of current table observations is empty.
Every retrieved entry must still pass the normalizer and native lifecycle checks.
The existing constructor and all its before/after table observation checks remain
intact. A metadata exception never selects the new path automatically.

Supported durable codec descriptors currently cover binary integers, floats,
Boolean, packed/zoned decimals, fixed/varying binary, fixed text with an explicit
CCSID, and single-byte varying text for CCSIDs 37, 500 and 1140. Temporal codecs,
multibyte varying text, XML/LOB/pointers and unknown codec parameters reject.
This is deliberately narrower than all legacy connector conversions. Native
acquisition/qualification of effective descriptors is still an integration gate;
local decoder examples do not prove every IBM i layout or source release.

## Verification

The native open-clear fixture now runs through durable history recovery, the new
cut-reader path, strict decoder, normalizer, lifecycle, buffer and actual dispatcher.
Journal identity observations and raw-page transport in this local test are mocked;
the source entries retain captured native sequence, thread and cycle information.
The table collector is absent from this path, rather than mocked to return success.

- While B's TRUNCATE remains open, A ID 8 is delivered with F=31 and W=42.
  Fresh recovery then emits B truncate and B ID 100 after commit.
- While B's optimized DELETE remains open, A ID 9 is delivered with F=46 and W=56.
  Fresh recovery through rollback emits no B data and completes through W=59.
- Interrupted clear output retains the prior durable W and replays the whole
  incomplete batch without repeating independently completed A output.
- Missing offsets reject before history access, even with an existing manifest.
  Wrong journal/receiver identities, missing anchors, changed row JIDs, changed
  effective policy, and a checkpoint predating the binding epoch reject.
- Codec/schema/key/nullability roundtrips use the standard serializer. Unsupported
  or implicit settings reject. Existing strict, cancellation, row safety, recovery
  and metadata-availability regressions remain in the normal test suite.

`tools/dcg-binding-history-probe.py` runs a dedicated local Kafka 4.3.1 broker and
uses the qualified `KafkaSchemaHistory` subclass. Every phase starts a new JVM.
An independent consumer verifies the complete history record before the experiment
writes its first checkpoint. Later processes discard the fixture's table definitions
and restore them from Kafka history. The original nine phases cover initialization, open
TRUNCATE, commit, open optimized DELETE, rollback, repeat recovery, missing offset,
missing history, and injected conflicting history. The negative cases return no
output and leave the saved checkpoint unchanged. Independent broker reads preserve
the original and conflicting records as evidence.

Advisor found that the original standard-provider loop silently skipped malformed
records before listener validation. The corrected fourteen-phase matrix adds null,
empty-object, source-only, position-only and malformed-JSON records after a valid
manifest in separate owned topics. It preserves the base-provider reproduction
for null/missing fields, then asserts the qualified provider's specific rejection
error. All fourteen corrected phases passed; no output or checkpoint update was
allowed by the negative cases. This resolves the reported finding without a second
Advisor call; the correction was verified locally, not independently re-reviewed.

The history backend is real Kafka. Source transport/native identity observations
are controlled local fixtures, output uses the actual dispatcher with a local
record sink, and the experiment's checkpoints are files. This does **not** qualify
Kafka Connect task integration, durable Kafka source offsets/output transactions,
broker disk-loss/restart durability, or a live source-to-sink path. Existing Kafka
ownership/fencing experiments were preserved and not rerun for this adapter.

The Kafka container was removed and its absence verified. Evidence and exact
runtime hashes are under
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/binding-history/`,
including `kafka-04/report.json`, per-phase records/logs, broker records/logs and
`runtime-sha256.json`. Earlier `kafka-01`/`-02` runs did not cover malformed records
and do not satisfy the corrected gate. The Kafka storage dependency is now included
in the plugin for the qualified backend, but no connector setting or streaming
task selects it yet.

Full Maven verify passed 2,212 tests across 49 classes with no failures, errors or
skips. Assembly packaging, formatter/import validation and diff checks passed.
The final fourteen-phase Kafka repeat is `kafka-04`; its runtime hashes match the
final compiled classes and dependencies. All owned broker containers were removed.
F/CR and original feature worktree hashes/HEADs and the transactional streaming
guard match the start-of-turn snapshot. `audit.json`, `turn.patch`, verification
logs and test reports preserve this continuation's exact scope.

## Remaining integration gates

The next adapter must acquire and validate effective native codec/schema bindings
at an explicit consistent source/output baseline, with a complete transaction
census and supported administration restrictions. It must use this history-before-
checkpoint ordering and never turn missing offsets into automatic initialization.
Then integrate the history-backed reader with task-owned terminal admission/drain,
standard Kafka Connect offsets/fencing and the real output path. Validate restart
and reassignment while an actual native clear remains open, plus failed delivery,
receiver rollover/deletion, schema/identity faults and sink convergence before
removing the streaming guard. Ordinary operator retention and the separate optional
protected-startup/receipt path remain distinct.

Native acquisition and explicit initialization are now implemented in the separate
[native initialization adapter](dcg-native-initialization.md). History record version 2
also fingerprints the enrollment and acknowledged baseline receipt. The qualified
production session, source-task handoff and Connect offset installation remain gates;
the original version-1/local-fixture evidence above retains its original scope.
