# D/CG local review series — 2026-11-08

The first three dependency commits are prepared and verified locally. The fourth
configured-startup change is held uncommitted: the requirement to manually pause
application writes has been rejected for the intended use. The next requirement
is table-specific enablement on an already-streaming connector without a new
snapshot, using a verified transaction-aware offset handoff. That migration is
not implemented by this series.

This work preserves the earlier tested D/CG candidate for review. It does not publish a
PR update or grant deployment/maintainer approval. It starts at D/CG groundwork
`838359c2811baf96529502cd6c6023573722df29`, which already depends on F/CR
`7f9a68c065d2b52547cd4e6a43e54b4a02bfbb73` (PR #114). The original D/CG, F/CR,
feature and earlier preflight worktrees remain unchanged. Integrating this series
with upstream main must retain the F/CR prerequisite or rebase after it lands.

## Cumulative commit boundaries

1. **Journal parsing and native identity primitives.** Strict row/control
   decoders, owned pages, receiver fields, read admission/cancellation and bounded
   native identity candidates. Configured D/CG startup is unavailable.
2. **Transaction recovery and bindings.** Owned values, paired file/member
   identities, immutable Kafka binding history, transaction/lifecycle buffering,
   commit/rollback/savepoint handling and durable F/W replay. Binding history goes
   with the buffer because it uses that component's shared identity model.
3. **Continuous reader and task integration.** Native schemas, output and offset
   acknowledgement, scan R versus durable F/W, stop/drain/replacement and bounded
   initialization interfaces exercised through controlled test providers.
   Configured D/CG startup explicitly rejects at this boundary.
4. **Configured opt-in initialization.** Native provider, operator first-start
   plan/enrollment, bounded snapshots, reader handoff, normal configured admission,
   user/contributor guidance and local Kafka CI contracts. The final main Java and
   active test sources match the verified working candidate.

These are dependency-ordered commits. Each cumulative tree is clean-built; they
cannot be cherry-picked in arbitrary order. Shared fixture tests enter at the
latest layer they exercise. Assertions are not removed to make a boundary pass.
The signed-off commits use issue prefix `debezium/dbz#3027`.

## Optionality and scope decisions

`dcg.mode=disabled` remains the default. Existing CDC/F/CR users do not acquire
the new first-start requirement. First enabling transactional clear capture
requires an administrator to pause application writes, DDL and maintenance for
the declared journal scope while the baseline and recovery history become
durable. Ordinary restart requires no new window. Unrelated-table commits can
progress while a clear is open. First initialization with continuing writers is
not qualified by this implementation.

Maintainers still need to accept the first-start protocol and the documented
distributed Kafka Connect 4.2.x/4.3.x runtime profile. See the
[support boundary](dcg-configured-native.md) and [submission decisions](dcg-upstream-submission.md).
`transactional.truncate.mode` is proposed as a clearer user-facing name, but is
not an accepted alias in this series. A rename needs configuration and checkpoint
compatibility tests. The current credential-fingerprint compatibility rules and
early startup validation remain intact.

## Validation and artifacts

Each boundary uses JDK 21, Java target 17 and the matching Debezium core
`0d2ebf88ceebd7d991b370cb8d028b91d4ac6f91`:

```sh
./mvnw -B -ntp clean install -Passembly -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
```

The accompanying review bundle records commit hashes, per-boundary unit results,
source/package hashes, exact local Kafka cases and their cleanup. The task-layer
boundary also runs acknowledgement/restart through real Connect with a synthetic
source. Final CI contracts cover acknowledgement/restart and invalid offsets on
paired worker/broker versions 4.2.0 and 4.3.1. These do not qualify mixed-version
clusters or native startup by themselves. Earlier native qualification remains
in its dated reports; no IBM i connection is made for this submission preparation.

The active tree omits eleven historical prototype Java files. They remain intact
in the original worktree and in `optional-prototypes.patch`. The default build,
plugin and local CI contracts do not depend on them. The Maven profile alone
does not provide those omitted sources; apply the optional patch in a disposable
checkout before running the historical suite. Dated investigation documents are
supporting evidence; the current user contract is `dcg-configured-native.md`.

The local bundle includes three `git format-patch` files, their combined patch,
a separate held configured-startup patch, the optional historical patch,
PR/comment drafts, a manifest and the separately
checked contributor-registration patch. Registration of Michael Gardner / gardnmi
in core `COPYRIGHT.txt` and `jenkins-jobs/scripts/config/Aliases.txt` remains
pending upstream acceptance. No push, PR mutation or native source connection is
part of this local work.
