# D/CG code, test and performance review

This review covers the accumulated local `dbz-3025-dcg-followup` implementation
and removes obsolete runtime dependencies without changing its supported
configuration. The [configured runbook](dcg-configured-native.md) and
[source release checklist](dcg-source-release-checklist.md) describe the active
path. Earlier protocol/experiment documents remain historical evidence, not
additional runtime requirements.

## Removed from the shipped connector

Five unused prototype implementations moved from `src/main/java` to
`debezium-connector-ibmi/src/prototype-test/java`:

- `As400CheckpointReceipts`: custom owner/receipt authority, superseded for the
  configured path by standard Connect fencing and durable offsets.
- `As400StartupEvidence`, `As400RecoveryStartup`, `As400JdbcStartupEvidence` and
  `As400VerifiedLifecycleReader`: the earlier protected-startup protocol. They
  remain available to reproduce that protocol's stronger, separate safety tests.

The configured native collectors depended on the old JDBC prototype only for its
nested connection-factory interface. A small `As400JdbcConnectionFactory` now
provides that contract directly. Query bodies, validation, connection ownership
and timeouts are unchanged. No supported recovery check was removed.

The connector JAR now contains 160 class files rather than 205: 46 prototype
class files were removed and one interface added. The measured JAR decreased from
503,924 to 419,500 bytes, approximately 17%. This measures the connector JAR,
not all third-party dependencies in the plugin distribution.

Bytecode comparison found six changed existing classes; their disassembled
instructions match after normalizing the connection-interface name and constant
pool indexes. All other retained main class files match the previous artifact
exactly. Static class dependency traversal from the original connector entry
classes finds no remaining unreachable outer class, and no main class references
the removed prototypes. This is a class-level audit, not a claim that every method
is minimal. Obsolete comments describing configured startup as test-only were
also corrected without changing behavior.

## Test scope

The default build runs **2,251 tests**. A separate, inactive
`dcg-prototype-tests` profile includes **395 historical cases** and runs all
**2,646 tests**. Their test identities exactly match the pre-cleanup suite;
no assertion or regression case was deleted.

| Optional historical class | Cases |
| --- | ---: |
| `As400CheckpointReceiptsTest` | 23 |
| `As400JdbcStartupEvidenceTest` | 67 |
| `As400RecoveryStartupTest` | 80 |
| `As400VerifiedLifecycleReaderTest` | 225 |

The prototype implementations, these four test classes, and the historical
`As400TaskProbeConnector`/`As400ProbeReceiptStore` helpers share the optional
source tree. They are not runtime code. `tools/dcg-task-probe` selects that
profile; the current ownership/native/normal-task harnesses use ordinary test
classes and retain their exclusion assertions for custom publication authority.

Default lifecycle, row decoding, normalization, transaction, restart, missing
offset, first-start, cancellation and native-fixture regressions stay enabled.
The largest parameterized suites exercise distinct journal boundaries and failure
orderings. Reducing their count alone would remove useful coverage. No new
permanent performance-only tests or duplicate test harness were added.

From a checkout with old compiled prototype classes, run a clean build once:

```sh
./mvnw clean install -Passembly -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
./mvnw test -Pdcg-prototype-tests -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
```

The profile has separate `prototype-test-classes` and
`prototype-surefire-reports` output directories. Running the ordinary build after
the profile, without cleaning, again passed exactly 2,251 tests. The profile also
checks formatting/imports for its additional source tree.

The local Kafka acknowledgement/restart scenario passed 14 checks with the
current candidate harness. The historical missing-offset restart scenario passed
four checks, including zero output and an unchanged accepted receipt. Both used
new local brokers, connected to no IBM i system and verified container/network
cleanup. Plugin inspection also passed when assembly and the prototype test
profile were enabled together: none of the 13 packaged dependency JARs contained
the excluded prototype or test-helper classes. The final artifact was rebuilt
with the ordinary profile.

## Performance regression check

The before/after comparison uses the same existing real-task correctness tests,
fixture inputs and assertions. Each version/scenario has two fresh JVMs, eight
warm-up iterations per JVM and 40 measured iterations total, ordered
before/after/after/before. Both use Temurin 21.0.13, a 512 MiB heap, two active
processors and Serial GC. This is a local regression measurement; it excludes
IBM i and broker latency and is not a source throughput or shutdown SLA.

| Existing scenario | Before median | After median | Median change | Before / after p95 |
| --- | ---: | ---: | ---: | ---: |
| 5,000-row reload, interrupted delivery and fresh-task replay | 409.48 ms | 371.58 ms | -9.26% | 1,423.99 / 1,392.89 ms |
| 30,000-row bounded snapshot, durable chunk acknowledgement and reader handoff | 1,433.41 ms | 1,468.05 ms | +2.42% | 1,578.68 / 1,654.79 ms |

No material slowdown was observed in these cases. The differing measurements
should not be presented as an optimization gain: they include task setup,
filesystem fixtures, scheduling, garbage collection and correctness assertions.
The queue/chunk bounds and active buffering, decoding, replay and snapshot
algorithms are unchanged. Existing tests still assert bounded retained records,
durable F/W and replay behavior.

The preceding [source qualification](dcg-source-release-checklist.md) retains
native mixed-column, repeated clear/reload, unrelated-table progress, worker
replacement and interrupted-publication evidence. Its artifact predates this
cleanup. No new live IBM i measurements were made for this review; class and
instruction comparisons establish the narrower relationship to that artifact.

## Reproducible evidence

Local evidence is retained at
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-cleanup-review-20261108/`.
It includes the before source snapshot/JAR, exact cleanup diff, Maven logs and
test identities, dependency and bytecode comparisons, staged harness artifacts,
benchmark runner/raw samples and final package/protected-worktree checks.
The benchmark remains in that evidence directory, outside the repository.

Independent Advisor review passed after inspecting actual target files, packaged
artifacts, test preservation and benchmark evidence, with no actionable issues.
Advisor did not rerun tests or live checks; its performance conclusion remains
limited to the local measurements. The exact result is in `advisor-review.json`.

The historical pending-initialization offset label `test-only-1` is intentionally
unchanged: it is serialized state, not a switch that enables a test provider.
Partial initialization still rejects restart. Renaming it during a packaging
cleanup would change a persisted protocol without improving runtime behavior.

F/CR PR #114 and the original feature worktree remain unchanged. This review
does not commit, push, publish, deploy or access a live IBM i system.
