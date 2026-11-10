# D/CG upstream submission and first-start decision

Local revision for debezium/dbz#3027 and draft PR #115. F/CR issue #3025 and
PR #114 remain separate. This document does not assert maintainer approval,
publication, deployment, or complete feature parity with other connectors.

## Why initialization differs

An IBM i journal cursor can begin in the middle of an already open commit cycle.
A snapshot alone does not prove which earlier changes a subsequent commit or
rollback controls. A fabricated starting offset can therefore publish a clear
without the surrounding transaction or reconstruct an incorrect reload.

The selected first implementation uses a bounded operator-controlled window:
pause every writer/DDL/maintenance participant on the declared journal, acquire
native file locks, verify complete transaction context and native identities,
scan bounded chunks, acknowledge the baseline, persist/read back binding history,
acknowledge initial F/W, then release the held session to the continuous reader.
Snapshot LAST metadata does not shortcut any of those steps. Ordinary restart
uses existing F/W and binding history and requires no new initialization window.
Unrelated-table commits continue while a transactional clear is open.

The filesystem plan/enrollment is retained as an explicit **first-release
operational prerequisite**, not a new source publisher-ownership service.
Kafka Connect owns producer fencing and durable offsets. All eligible workers
must see the same durable enrollment directory. An immutable plan has bounded
scope/budgets/expiry; atomic durable claim and quarantine prevent a replacement
from silently restarting a partially delivered baseline. Loss of that storage
is not permission to recreate it and resume the same generation.

Replacing enrollment with Kafka is possible future work, but a compacted value
alone does not implement an atomic claim or prove native exclusion. It needs a
separate conditional-claim/fencing design and crash tests. This revision does
not weaken the working failure checks to remove a file dependency cosmetically.

## Recovery and migration runbook

* Durable version-2 F/W plus matching initialized binding history: resume using
  the same capture policy, retained receiver chain and output generation.
* Pending initialization offset, interrupted enrollment, missing offset with
  existing output/history, or uncertain baseline acknowledgement: reject startup.
  Preserve evidence and the failed generation. Establish a new controlled window
  with new empty data/history topics and a new topic prefix; rebuild the baseline.
  Do not delete/reset the old offsets to bypass rejection.
* Legacy offsets: no automatic conversion. Complete the old capture, stop its
  writer, preserve its offsets/output, and perform a separately planned fresh
  initialization generation. Switching consumers to that generation is an
  operator action outside this source connector.
* Missing receiver, identity mismatch, unsupported schema or unknown savepoint:
  stop without advancing the durable output boundary. Restore required retained
  evidence or explicitly initialize a new generation after diagnosis.

## Runtime contract and merge decisions

Public source transaction capability and tested Kafka Connect 4.2.x/4.3.x callback
semantics remain necessary for this first implementation. The source delivery
contract is whole-batch at-least-once; source exactly-once is not advertised.
JSON (schemas on/off) and Avro serialization use ordinary Connect converters.
Record-dropping/rerouting transforms remain rejected: they invalidate the exact
publication acknowledgement contract. Table selection occurs before publication,
while native reading and transaction validation retain the complete inventory.

The version above concerns the **Connect worker**, which loads the plugin. The
broker version is a separate compatibility question. This branch builds against
Debezium 3.8's Kafka 4.3.1 dependency; its primary upstream target follows that
release's [published build/test baseline](https://debezium.io/releases/3.8/release-notes#kafka-compatibility).
Testing additional workers does not establish a requirement to support every
older Kafka release or every worker/broker version combination. The D/CG runtime
check does not apply to the existing CDC/F/CR path when D/CG is disabled.

Engine/Server admission cannot be enabled by removing a version guard: it needs
an equivalent tested durable acknowledgement and replacement-fencing contract.
Single-partition truncate ordering, qualified native initialization and receiver
retention remain requirements. Fixed schemas, single members, one journal/library,
unqualified temporal layouts/precision, LOB/XML and XA limitations remain explicit.
ISO DATE/TIME and TIMESTAMP(0-9) have separate native qualification; this is not
admission of arbitrary layouts or precision above nanoseconds.

Maintainers must agree whether this scope and first-start prerequisite fit the
connector before merging the normal-task integration. That is an upstream design
decision, not something local tests or this document can grant. Unsupported type
admission is a separate evidence gate; retain rejection until native fixtures pass.

## Reviewable submission sequence

Keep the existing working branch intact. Prepare a series of issue-prefixed,
signed-off commits or dependent PRs, each building with its tests:

1. Strict row/control parsing, owned journal pages, read lifecycle and native
   identity candidates, with raw fixtures and fail-closed validation. No configured
   runtime admission.
2. Transaction/lifecycle buffering, paired native bindings and immutable history,
   commit/savepoint/rollback ordering and F/W replay. Include interrupted delivery
   and unrelated-table progress tests.
3. Continuous reader stop/drain/replacement fixes and actual task output/offset
   acknowledgement. Include local Kafka compatibility/fencing tests.
4. Operator first-start, bounded snapshot delivery and configured task admission,
   after the preceding scope decision. Include current user documentation and
   portable optional native qualification tools.

These are dependency boundaries, not independent parallel patches; do not strip
cross-boundary safety tests just to reduce a diff. The historical prototype source
profile and detailed investigation reports should be a separately identified
supporting change, not presented as runtime dependencies. Active user documentation
is `dcg-configured-native.md`; contributor instructions are
`dcg-contributor-testing.md`. Earlier validation/design reports are dated research
and evidence, not additional supported interfaces.

The current upstream compatibility check uses core
`0d2ebf88ceebd7d991b370cb8d028b91d4ac6f91`. Build the same core modules and formatter
checks as cross-maven before the final connector verify/package. The workflow now
adds portable local Kafka contracts rather than requiring private source accounts.
The [local preflight](dcg-upstream-preflight.md) distinguishes the complete active
candidate from the optional historical prototypes and records which submission
checks have actually run. The later [local review series](dcg-review-series.md) commits the first three
dependency boundaries. The fourth has a passing clean build but is held
uncommitted because the manual writer-pause requirement was rejected for the
intended use. The new target is table-specific enablement on an already-streaming
connector without a fresh snapshot, with a validated offset/transaction handoff.
The dependency commits cannot be applied in arbitrary order.

Before publication, supply DCO `Signed-off-by: Michael Gardner <the contributor's
verified email>` on each commit, use the `debezium/dbz#3027` issue prefix and
complete contributor registration in core `COPYRIGHT.txt` and
`jenkins-jobs/scripts/config/Aliases.txt` (`gardnmi,Michael Gardner`). A local
registration patch is prepared separately; it is not registered upstream until
that change is accepted. The review-series commits remain local. No pushes or PR changes are part of
this preparation.

## User-facing naming proposal

`dcg.*` is the currently tested configuration namespace. A clearer user-facing
name such as `transactional.truncate.mode` would describe the capability without
requiring knowledge of IBM i journal codes. This series preserves the tested
names. Any rename must include explicit configuration and capture-fingerprint
compatibility handling; it is a maintainer review proposal, not an additional
accepted configuration key. D/CG remains useful for diagnostics and technical
documentation. The feature stays disabled by default; its first-start pause
requirement applies only to users enabling this transactional capture profile.
