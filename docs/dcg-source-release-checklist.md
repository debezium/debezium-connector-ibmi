# D/CG source-connector release checklist

The subsequent [code review and cleanup](dcg-code-review.md) removes unsupported
prototype implementations from the distribution and isolates their tests.
The qualification counts and hashes below describe the pre-cleanup artifact;
the cleanup report records the replacement artifact and regression checks.

This checklist covers the configured operator-window source path. The later
[online configuration](dcg-online-configuration.md) adds a distinct default-off
transition from completed legacy streaming offsets; its
[ordinary-plugin native qualification](dcg-public-online-validation.md) is recorded
separately and does not inherit first-start or key-migration claims from this table.
It supersedes historical statements that the normal task/provider is not yet
implemented. It does not change the optional protected path or claim its stronger
retention-release and backup-restore guarantees.

The product boundary is IBM i journal capture through ordinary Kafka Connect
publication. Qualification uses fresh isolated edgeqa fixtures and local Kafka.
Neither a particular downstream sink nor production AWS/EKS access is required.

## Required source behavior and evidence

| Requirement | Implemented behavior and evidence | Current disposition |
| --- | --- | --- |
| C1: native transaction semantics | Complete owned row images, distinct file/member identities, subtype 3/8 commit/full rollback, conservative savepoint handling, interleaved cycles and bounded exhaustion; native lifecycle captures and local every-entry restart tests | Implemented and tested within the configured profile; unknown outcomes/unsupported records reject |
| C2: publication ownership | Standard Connect transactions, offsets and broker fencing; no custom receipt/owner authority in configured publication; local partition matrix and legitimate task reassignment | Qualified Kafka Connect 4.3.1 profile only |
| C3: durable restart | Separate in-memory R and durable F/W; previous W through interrupted output, whole-batch replay, ordered durable acknowledgement/readback | Local interruption/restart coverage, native open-transaction restart and normal-task native committed-prefix replay pass |
| C4: native stop and replacement | Terminal RPC/JDBC admission, active-call cancellation, separate stop request/drain, failure propagation; local lifecycle races and isolated native held-reader/replacement checks | Implemented and tested; no hard native-driver shutdown deadline promised |
| C5: receiver history | Exact receiver identities/reciprocal links and independent F/W anchors; bounded cuts and owned raw pages; missing/partial/cache/filter/error rejection; actual rotations | Operator retains required history; no automatic receiver deletion or suffix reset |
| C6: durable identity/schema | Native acquired paired bindings and physical codecs, immutable Kafka history, capture/output fingerprint, fresh-instance reconstruction | Fixed supported schemas/single-member files; uncoordinated DDL/reorganization and source restore are excluded |
| C7: first initialization | Explicit single-use operator writer/maintenance window; native census/locks, chunked snapshot, durable acknowledgement, B/history/INITIAL then reader handoff | Native baselines beyond 9,000 rows and failure cases pass; partial first start requires a new empty generation |
| C8: offset loss | Missing/partial/malformed/legacy offsets reject; new operator plan cannot initialize over existing output/history | Local and normal packaged native rejection checks pass |
| C9: effective configuration | Explicit admission and runtime capability/version checks; bounded resource settings; fixed topic/converter/error policy and key mode persisted in fingerprint | Only the documented profile is supported; no silent fallback |
| C10: emitted record correctness | Independent `read_committed` consumer checks truncate-before-reload, row keys, metadata/tombstones and native final projection; mixed-column before/after checks | Native primary-key and RRN smoke/large cases, eight repeated workload cycles and native committed-prefix replay pass |
| C11: retention/restore boundary | Candidate grants no deletion/maintenance permission; known identity/history changes reject | Operator-managed retention selected; indistinguishable old-backup restore detection is outside this contract |
| C12: reproducibility/review | Clean CI-equivalent Maven build, exact source/package provenance, independent actual-file review, preserved F/CR/original worktrees and verified cleanup | Final build/provenance/cleanup checks and actual-file Advisor review passed within the documented profile |

"Tested" refers to the linked cases, not exhaustive coverage of every deployment,
IBM i release/PTF or native operation. The configured profile in
[the runbook](dcg-configured-native.md) remains the support boundary. Historical
protected N/P/H/B tests remain intact; their stronger mechanisms are not fabricated
by the ordinary provider.

## Extended workload and native interruption

`tools/dcg-sustained-probe` reuses the packaged normal-task harness and adds eight
1,000-row native clear/reload cycles. It checks unrelated progress while the clear
is open, two worker kills/replacements, a brief local broker outage, repeated
receiver rotation, actual deleted-slot reuse, business-value changes under stable
RRN keys and exact mixed values. Every cycle compares `(RRN, ID)` to independent
native queries and checks durable F/W. Resource samples are observations; a finite
run cannot prove absence of every memory leak or establish an availability SLA.

`tools/dcg-interrupted-publication-probe` adds a 5,000-row native transaction. An
independent `read_committed` consumer signals the host after observing the new
truncate. The host kills only its owned worker, then independently verifies a
committed prefix, incomplete reload and unchanged previous W. A new ordinary task
must replay the complete clear/reload and reach the native final projection. A
run that misses the interruption window fails; it cannot be reported as a pass.

Both tools require a new evidence directory and a fresh `DCNCdddd` fixture name.
They inherit exact-owned native cleanup and local container/network verification.
The test controller and audit classes are outside the worker plugin. Neither tool
changes normal connector admission or uses a test task/provider.

Current evidence directory:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-sustained-qualification-20261108/`.
`native-02` passed **120 assertions** in fresh library **DCNC7802**, including
all eight repeated cycles, both worker replacements and the local broker outage.
The additional workload lasted **591.257 seconds**. Open-marker to finish-marker
intervals were **45.250–58.610 seconds**; these are observed intervals, not a
driver or workload latency guarantee. F advanced to W after each completed cycle.
All five native `(RRN, ID)` projections matched after every cycle; mixed-column
values remained exact and no snapshot was repeated on replacement. Native and
local cleanup passed, including removal of all eleven owned journal receivers.

`native-01` is retained as a failed harness attempt: IBM i rejected an unqualified
RRN reference in fixture DELETE SQL (SQL5001). The correction uses the fixture's
known row value in the mutation and independently verifies the actual native RRN
afterward. Source guards and the reuse assertion were unchanged. The failed
attempt also passed native/local cleanup and is not counted as qualification.

The final sustained harness calls its timer `minimum_cycle_seconds` and its
admission deadline `cycle_start_deadline_seconds`. The staged run used earlier
labels suggesting a native open-duration/hard deadline; the algorithm is unchanged.
`sustained-summary.json` records the actual observed open intervals. A finite
repeated workload is not a multi-hour endurance test or proof of a universal SLA.

`interrupted-02`, fresh **DCNC7804**, passed the committed-prefix experiment. The
host killed the worker after the truncate and **4,989 of 5,000** reload rows had
committed. Durable **F=189 / W=5198** remained unchanged. A fresh normal task
replayed a second truncate and all 5,000 rows, reached the exact native B table
projection and advanced F/W to **5201**. Native CLRPFM on U then produced actual
F/CR, followed by the expected reload through this same configured task. Normal
drain and both missing-offset rejection modes passed. Native/local cleanup passed,
including all three owned receivers. Independent raw F/CR and broker output are
retained with the report.

These checks establish B's partition order and later EXTRA catch-up, not a global
order across topics. The final harness assertion label says "catches up to later
output"; the staged report used "later output follows complete replay" for the
same check. Only that explanatory label changed after staging.

`interrupted-01` is retained as a failed committed-prefix attempt. The worker was
killed after a committed truncate and 4,605 of the 5,000 reload rows. Independent
offset reads showed F=189 / W=5198 unchanged. The replacement reached its offset
store startup but did not join Connect within the harness's original 100-second
deadline, before the source task started. Native and local cleanup passed. This
is not a successful replay result.

The repeat adds an independent, read-only broker transaction/end-offset observer
and a consumer-ready barrier before committing the native transaction. Only this
interruption scenario permits up to 240 seconds for replacement membership; the
original default wait and older failing reproduction remain unchanged. It records
broker evidence and the worker thread stack at 100 seconds if still waiting.
The worker's source transaction timeout remains 180 seconds and the broker's
timeout-abort sweep remains 10 seconds. No offset, ownership or replay assertion
is weakened, and no producer is manually fenced or aborted by the observer.

The successful native repeat observed `CompleteCommit` and equal committed/full
offset-log ends after the kill. Its replacement did not exercise the 100-second
blocked path. `replacement_membership_seconds` in that report measures only the
wait after worker REST readiness, not total JVM startup or recovery duration.
The original native failure has no independent open-transaction snapshot; its
cause must not be described as directly confirmed by this successful repeat.

The separate local-only `tools/dcg-offset-startup-probe.py` starts an ordinary
Connect worker with no configured source connector, writes one uncommitted dummy
offset in a fresh local broker, kills that producer, and starts a new worker. It
requires the compiled test classes and the existing probe dependency classpath.
This isolates Kafka startup from IBM i and source recovery. The independent
observer cannot commit, abort or fence the transaction. It captures both
isolation-level ends, broker transaction state, worker thread stacks and startup
completion. `offset-startup-01` reached the expected ongoing transaction/offset
gap but failed because the image has no `jcmd`; cleanup passed. The diagnostic
now uses the Connect JVM's nonterminating SIGQUIT thread dump instead. This is
also used by the native harness's previously unexercised blocked-start diagnostic.

`offset-startup-02` passed all **four assertions** and local cleanup. At the
100-second sample the worker stack was in `KafkaBasedLog.readToLogEnd` called by
`KafkaOffsetBackingStore.start`; the broker reported `Ongoing`, committed end 0
and full end 1. After the broker's automatic abort it reported `CompleteAbort`
and both ends 2, and the worker joined with no configured source connector. The
membership wait after REST readiness lasted **161.264 seconds**. The final broker
sample was **183.566 seconds** after the transaction's recorded start. These are
different intervals; neither is a universal startup SLA. The original native
failure is consistent with this verified Kafka mechanism but lacks the broker
snapshot required to prove that it had exactly this cause. Its failed result is
preserved rather than counted as a pass.

Kafka 4.3.1's [offset backing store](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/storage/KafkaOffsetBackingStore.java)
starts by catching up its offset log. Its [KafkaBasedLog](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/util/KafkaBasedLog.java)
compares committed consumer progress to the full log end, so an unfinished
transaction can hold startup before group membership and source-task startup.
Copies of these pinned sources are retained under `kafka-source/`.

## Final local verification

The CI-equivalent clean Maven install/assembly passed **2,646 unit tests**, with
formatter/import validation and no failures, errors or skipped tests. Later
test-only audit additions were compiled and exercised by the native/local runs;
`final-harness-validation.log` records their formatter/import checks. No main
Java source changed during this qualification turn. All **205** connector main
classes and main source files match the successful native staged plugin, and
the final distribution ZIP hash matches `interrupted-02` exactly. The package
contains no test provider or audit class. The default remains disabled and the
configured opt-in uses the normal task.

`final-audit.json`, `staged-classes.json`, `package-audit.json`,
`artifact-hashes.json`, `tests-summary.json` and `experiment-summary.json` retain
the checks. F/CR and original feature worktrees are byte-for-byte unchanged,
including their uncommitted files. Final independent Advisor review passed after
inspecting actual target files and retained evidence, with no actionable issues
within the documented profile (`advisor-review.json`). Advisor did not rerun
tests or live checks; broader deployment and endurance guarantees remain outside
that review. No inaccessible-file limitation remains.

## Publication boundary

Source readiness is separate from publishing this accumulated local patch. F/CR
PR #114 and the original feature worktree remain protected. No commit, push, PR
mutation or deployment is part of the current no-push work. Upstream publication
also retains the repository's contributor-name/alias, DCO sign-off, issue-prefixed
commit and CI requirements. Local test success is not upstream maintainer approval.
