# Test-only native initialization with Kafka Connect

Transactional D/CG streaming remains disabled. This harness integrates the existing
`As400NativeInitialization` adapter with the actual `As400ConnectorTask` /
`BaseSourceTask` poll, commit and stop lifecycle. It uses local Kafka for the snapshot,
immutable binding history and source offsets, then hands off to the history-backed raw
reader, strict decoder, normalizer, transaction buffer and dispatcher. It makes no
IBM i connections and does not modify the F/CR branch or the existing stricter probes.

Run `mise exec java@temurin-21 -- tools/dcg-initialization-probe NEW_EVIDENCE_DIRECTORY`.
The destination must not exist. Optional selectors are `ack`, `crash`, `qualification`,
`stop` and `fencing`. The launcher requires a local Unix-socket Docker daemon, uses the
existing digest-pinned Kafka 4.3.1 image, an internal isolated network, dedicated topics,
no host ports, bounded fault gates, and verifies removal of all owned resources. Failed
runs retain independent broker observations as well as worker logs. The probe classes
are test sources, excluded from the connector plugin assembly.

## Qualified scope

The native observations and snapshot rows are controlled fixtures. Captured IBM i
journal bytes exercise the real strict replay path; native metadata acquisition,
transaction census and the journal-wide writer/DDL/maintenance fence are mocked. The
controller's qualified-session token represents that fixture contract only. It is not
an operator option, production fence provider, claim that a quiet journal is safe,
or permission to bypass the production transactional guard.

Qualification must be held for initialization before preparation, throughout snapshot acknowledgement,
history write/readback, initial offset installation and reader handoff. Loss or stop
rejects subsequent initialization work. After admitted handoff, the fixture releases
the source fence; ordinary replay/restart uses cancellation and standard Kafka fencing.
The restart case removes the qualification token to prove it does not require another
journal-wide writer pause. The test retains the session and enrollment on failure;
there is no automatic fence release or re-enrollment. Every native validation in the
initializer remains in place and its earlier failure tests remain unchanged.

Snapshot/output topics must form a dedicated, empty generation (one partition per table and barrier topic), with an
empty history topic, before explicitly initializing a missing offset. Both beginning
and end offsets are checked, including uncommitted/aborted output. The fixture also
consumes a single-use, fsynced enrollment marker. This marker quarantines an attempt
that crashed before it wrote anything; it neither selects a publishing worker nor
stores an authoritative checkpoint. Loss of that marker cannot override existing
broker output/history. Kafka Connect fencing remains the publication authority.

The downstream acknowledgement selected here is **durability in Kafka**, observed
with `read_committed`. No sink/application acknowledgement or restore guarantee is
claimed. Partial snapshots may be committed to this new generation before its final
barrier; a consumer cannot treat the generation as initialized until that barrier.

## Protocol and crash outcomes

The asynchronous initializer lets the real task poll while it waits for output. It
materializes six controlled snapshot rows before the native boundary B=29 is chosen.
Snapshot rows carry test-only pending initialization offsets, never synthetic F/W.
A separate baseline barrier follows all rows in the task queue and source partition.
A/B retain distinct table topics; the existing source envelope does not have a table
field. Assertions preserve each table's order and do not infer a global order by
merging independent topics. The barrier acknowledgement covers all preceding
registered snapshot rows across those topics.

The acknowledgement tracker requires matching, non-null `RecordMetadata` for every
preceding registered record and a subsequent task commit callback. It rejects dropped,
unknown, duplicate, wrong-topic/partition and missing-offset acknowledgements, stops
waiters on cancellation, and has bounded capacity and deadlines. The initializer also
reads its source offset through Connect and requires exact agreement with the barrier.

This is qualified only for the pinned worker with `exactly.once.source.support=enabled`,
`exactly.once.support=requested`, `transaction.boundary=poll`, fail-on-conversion,
`errors.tolerance=none`, and no transformations. Connector configuration alone cannot
establish worker guarantees. In this runtime, `ExactlyOnceWorkerSourceTask` commits the
producer transaction containing records and offsets before `commitRecord` and `commit`.
Generic SourceTask callbacks and raw producer acknowledgements are insufficient.
The inherited connector still advertises EOS **unsupported**: replay of an interrupted
multi-poll source transaction is deliberately at least once.

After the baseline barrier commits, its broker coordinate is stored with the native
binding epoch in standard Kafka schema history, then recovered through a fresh backend.
Only afterwards does an initial checkpoint barrier carry exclusive F=W=29. The reader
is admitted after its commit and a matching Connect offset read, with a recovered
history epoch matching the initialized generation. New initialization still requires
the held qualified session at that handoff. The source task never writes its
own offset topic or seeds F/W through the offset alteration API.

A crash during a partial snapshot, after the baseline acknowledgement, or after history
but before initial F/W leaves no resumable checkpoint. A replacement rejects pending
initialization offsets and emits nothing. Explicit recovery/re-enrollment remains a
separate operator action. A crash after durable F/W, even before reader admission,
resumes from history and Kafka offsets without repeating the snapshot.

The reader's captured journal runs through R=64 while completed output W=59. Open-clear
replay floors and ordinary output remain the existing protocol: A:8 progresses while
B's clear is open, then committed truncate/reload of B, then A:9. Rolled-back B:200 is
absent. A replacement uses durable F/W, not the old reader's scan. Kafka fences a stale
producer held at the first post-handoff send while a replacement completes the replay.

## Acceptance

- Delayed baseline producer acknowledgement: committed snapshot prefix visible, pending
  barrier hidden, no history or F/W installed.
- Delayed initial-checkpoint acknowledgement: history exists, pending baseline offset
  remains authoritative, no reader handoff.
- Successful handoff: exact durable F=W=29 exclusive, initialized history receipt,
  snapshot before native CDC, native source sequences and keys preserved.
- Fresh task restart: no repeated snapshot or completed data; final W=59 and scan R=64.
- Missing offsets, including loss of the local enrollment marker with existing broker
  history/output: zero new output, no automatic initialization.
- Four crash boundaries: partial snapshot, baseline acknowledged, history durable,
  checkpoint durable; quarantine or resume as specified above.
- Unqualified session, session loss and stop while history is durable: no new initial
  offset/reader. Stop request and completed initializer drain are separate evidence.
- Legitimate reassignment and stale sender: replacement reads durable F/W and progresses;
  broker rejects the old producer and output/offsets cannot regress.
- Existing native acquisition/census, identity, journal transition, savepoint, recovery,
  strict receipt, cancellation and Kafka assertions remain unchanged.

## Remaining gates

This removes local-file snapshot/offset substitution from the native-initialization
experiment. It does not implement a general IBM i qualified-session provider, a
production snapshot change source, mixed-column snapshot/output conversion or a sink
barrier. The native handoff and source-side exclusion still need qualification with
real IBM i writers, DDL/maintenance and retention, including open TRUNCATE and optimized
DELETE while unrelated tables progress. Partial initialization recovery is fail closed,
not automatic. No production enablement follows from this local result.

References: [Kafka 4.3.1 exactly-once worker commit implementation](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ExactlyOnceWorkerSourceTask.java),
[SourceTask callback contract](https://github.com/apache/kafka/blob/4.3.1/connect/api/src/main/java/org/apache/kafka/connect/source/SourceTask.java),
[native initialization](dcg-native-initialization.md),
[validated local timeout profile](dcg-timeout-profile-validation.md).

## Completed verification (2026-11-07)

Maven verify passed 2,311 tests across 53 classes, with zero failures/errors/skips.
Checkstyle, formatter/import validation, assembly packaging and diff checks passed.
The final local Kafka matrix passed all 29 assertions, including both delayed
acknowledgements, four crash boundaries, resume without a writer fence, missing-offset
rejection, qualified-session loss/stop, actual scan R=64 versus durable F/W=29, legitimate
reassignment and broker rejection of the stale producer. Final completed output is W=59.

The unchanged ownership acknowledgement/restart matrix passed 14 assertions. The
unchanged stricter receipt missing-offset regression passed all three assertions.
All owned Docker containers and networks were removed and absence was verified.
No IBM i connection or mutation occurred. Production sources, all pre-existing files,
the F/CR worktree and original feature worktree match the start-of-turn snapshot.
The new probe classes are absent from the plugin assembly. Nothing was committed or pushed.

Evidence:
`/home/gardnmi/.bb/thread-storage/thr_vzfz57gebs/dcg-operator-retention/20261107/connect-initialization/`.
`kafka-final/report.json` is the final runtime matrix; `verify-final.log`,
`package-final.log`, `tests.json`, `audit.json` and `turn.patch` identify the exact work.
`strict-regression/report.json` and `ownership-regression/report.json` preserve the
existing behavior. The audit compares staged runtime/source hashes and protected trees.
Earlier attempts are retained: kafka-01 exposed a nameless test barrier schema;
kafka-02/03 exposed a harness assumption about a nonexistent source table field.
Those harness faults were corrected. kafka-04/05 passed intermediate versions; neither
substitutes for the final run with observed scan progress and the final assertions.

Advisor inspected the actual files and saved Kafka evidence and passed with no
actionable defect. It did not independently rerun tests; live-source integration
remains unverified. The exact result is saved in `advisor-review.json`.
