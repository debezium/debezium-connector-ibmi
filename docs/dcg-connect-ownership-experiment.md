# Local Connect ownership and restart experiment

This is a test-only candidate for the [runtime profile revision](dcg-runtime-profile-revision.md).
It evaluates the publication portions of C2/C3/C8/C9. It does not enable buffered
streaming, close those acceptance gates as a whole, or replace current production
startup/receipt checks. F/CR PR #114 is unchanged.

## Dependency being evaluated

The candidate uses the actual `As400ConnectorTask`/`BaseSourceTask` polling,
commit-record, commit and stop lifecycle, `As400BatchDispatcher`, emitter,
transaction monitor, heartbeat implementation and F/W replay coordinator.
Only source startup, schema and journal bytes are synthetic. No JDBC/RPC source
is constructed, and attempting to execute the fixture streaming source throws.

The new `As400ConnectOwnershipProbeConnector` is deliberately separate from
`As400TaskProbeConnector`. The stricter receipt fixture and its tests are preserved.
The candidate artifact excludes `As400ProbeReceiptStore`, `As400CheckpointReceipts`
and the old task fixture classes. There is no owner claim, receipt reconciliation,
custom generation/producer-epoch association or `authorized-worker` file in the
candidate publication path. Diagnostic task IDs distinguish instances; they do
not authorize output. The fixture coordinator's commit callback only records an
observation, never grants retention or publication permission.

The shared test helpers provide local container management, synthetic bytes,
producer fault injection and independent Kafka snapshots. Poll/send/ack gates
schedule the experiment. In the fencing case, the original task already has
permission to drain its queue and its send hook is released: an application gate
cannot account for rejection of its late truncate. A new task assignment on the
original worker is allowed and must publish the remaining work.

## Explicit initialization, strict resume

Every candidate task is resume-only. A missing, malformed or incompatible saved
checkpoint fails before journal preparation or polling. There is no automatic
synthetic W=1 fallback, even on first task startup.

The local controller creates a stopped connector and explicitly seeds a synthetic
F=W=1 boundary using Kafka Connect's public `PATCH /connectors/{name}/offsets`
API. It independently reads the actual offset topic before resuming the connector.
Only the synthetic source partition is accepted by the test connector's
`alterOffsets` override. That override deliberately permits malformed checkpoint
values for failure injection; the resumed task validates them. Neither this
override nor the experiment's CLI belongs in the production plugin.

This setup assumes an already established synthetic baseline. It does not solve
production initialization, consistent snapshot handoff, evidence surviving loss
of all initialization/configuration state, or independent detection of an old
but internally consistent restore. Existing production baseline and missing-offset
requirements remain open and unchanged.

## Runtime and assertions

Use the digest-pinned Kafka/Connect 4.3.1 image from `dcg-connect-probe.py` in a
disposable internal Docker network. The runner requires a local Unix-socket Docker
endpoint, publishes no host ports and accepts no source endpoint or credentials.
One stable group/connector/task identity is used per scenario. This does not fence
independent connectors with other names or deployments in another cluster.

Workers enable `exactly.once.source.support`; the candidate explicitly declares
`ExactlyOnceSupport.UNSUPPORTED`, uses `exactly.once.support=requested` and
`transaction.boundary=poll`. `required` must fail capability validation. These
are tested framework mechanics, not a claim of source exactly-once delivery.
Conservative offsets deliberately allow whole-batch replay after a committed
prefix. Each poll returns one record, splitting the source transaction across
multiple Kafka transactions. Larger polls remain outside this candidate run.

Data, transaction metadata and heartbeats use separate topics, each with one
partition. Assertions check order within each topic and associate metadata by
task/transaction identity; they never sort topics together to invent a global
Kafka order. Real envelope keys/source metadata, key-change delete/create records,
tombstones and heartbeats are retained. The final projection is a synthetic oracle,
not a live sink validation.

| Scenario | Injection and required evidence |
| --- | --- |
| Acknowledgement | Delay the heartbeat's producer acknowledgement; it may appear under `read_uncommitted` but cannot become committed or advance durable W. After release, kill the worker and verify a fresh process loads exact F/W and suppresses completed work. |
| Interrupted batch | Kill after committed clear/partial reload. Reload actual conservative offsets and replay the whole incomplete batch before later table changes; retain original row/source identity. |
| Mixed output | Kill after the key-change delete and tombstone. Replay the full clear/create/delete/tombstone batch, preserve immutable offsets and transaction metadata counts, and converge the synthetic table projection. |
| Suspended producer | Block the old truncate in the real producer, suspend the whole process, let the replacement finish the first batch, release/resume the old process and require its specific send to fail with broker producer-epoch fencing. No late committed clear or regressed offset is permitted. |
| Network partition | Partition the old worker while its send is blocked. Reconnect after takeover and require old-client rejection. This local-close result is kept separate from broker fencing evidence. |
| Legitimate reassignment | After either takeover, remove the replacement worker. Connect assigns a new task on the original worker; it must load the accepted checkpoint and publish the later batch without custom authorization. Distinguish this new task from the rejected old generation. |
| Invalid offsets | After W=9, stop the task and remove the source offset through the public API. Resume on a fresh process and require zero output. Repeat with malformed and incompatible checkpoints; no task may replace rejected state with a fresh baseline. |

## Running and evidence

From this worktree with Docker, Python 3, matching project dependencies and JDK 21:

```sh
mise exec java@temurin-21 -- tools/dcg-ownership-probe /absolute/path/to/new-evidence-directory
```

The destination must not exist. An optional second argument selects `ack`,
`interrupted`, `mixed`, `fencing`, `partition` or `offsets` for a targeted rerun;
the report records the selected scenarios. The launcher builds the test classes
and their project dependencies and resolves the test classpath; it does not run
native integration tests. The runner records source/harness/plugin hashes,
worker/connector configuration, producer faults, task generations, real Kafka
records and offsets, assertions and owned-resource cleanup. Existing resources
are not cleaned. Follow the original probe's labeled-resource procedure after an
uncatchable process/host failure.

When a formerly suspended worker becomes leader again, its configuration-topic
producer can also be fenced. The runner records and retries only the idempotent
administrative stop on Kafka's explicit retryable config-write response or a
rebalance conflict, up to five attempts. A persistent failure fails the run. It
does not retry source publication failures or relax any output/checkpoint oracle.
Normal stop must leave the connector stopped and its task list empty.

## Observed local results (2026-11-06)

The acknowledgement/completed-restart, interrupted-batch, mixed-output and
suspended-producer/reassignment scenarios passed in the final full-run attempt.
The old truncate's specific producer callback reported
`InvalidProducerEpochException`. A new task on the original worker loaded W=6
and published the later batch through W=9 without any custom authority.
The separate invalid-offset run passed all 12 of its assertions, including zero
new output for missing, malformed and incompatible checkpoints. All 732 existing
unit tests, Maven verify, packaging and format/import validation passed. The new
probe is absent from the distributable plugin jars.

**The complete runtime matrix did not pass.** The network-partition scenario
rejected the old producer and preserved the accepted F/W, but killing the
replacement immediately afterward exposed a returning-worker recovery failure.
Kafka reported `Failed to read to end of config topic before performing zombie
fencing`; its new task failed before entering the fixture's `start` method. The
runner then failed its bounded wait for a new task on that worker. This reproduced
in a separate partition-only run on a fresh broker. The subsequent
[configuration-log investigation](dcg-config-catchup-investigation.md) identifies
a delayed shared administration-client log-end lookup exceeding the worker's
synchronization budget. Neither ordinary worker membership nor an observed
producer fence proves that this later startup can complete.

The failing scenario remains intact and still fails the runner. It was not
converted to a pass, skipped, given a longer timeout or recovered through a custom
ownership check. The offset cases were run separately because the full run stops
at the first failure. Output/offset comparisons before this second takeover passed;
those historical failed runs did not contain a final independent broker snapshot
after the reassignment timeout. The new instrumented reproduction closes that
observation gap: fresh broker readers found complete config history and unchanged
committed output/exact W=6 after failure. A late task eventually loaded W=6 during
extended diagnostics, after the original deadline; its polling gate was not
released. The original bounded test remains failed. See the investigation for
separate runtime comparisons and their limitations.
All owned Docker resources from the successful and failed runs were removed.

The first attempt also encountered the explicitly retryable config-writer error
on administrative stop. The bounded stop retry described above was added and
verified in the next full run; publication assertions were unchanged. This is
separate from the partition/reassignment failure, which is not retried
away. Historical failed-run evidence is retained.

The results support further evaluation of standard Connect publication ownership.
They do not authorize removing a production dependency or claiming the candidate
runtime profile has passed its acceptance gates. The original configuration still
fails the partition/reassignment scenario; local comparison results do not close
the complete C2/C3 gates or establish a supported production timeout profile.

## Remaining boundaries

A passing candidate can support replacing the proposed custom publication-owner
authority inside the tested Connect scope. It does not justify deleting the
current receipt implementation or its regressions. Native reader lifetime (C4),
complete retained history and identity/schema continuity (C5/C6), initial baseline
(C7), broader crash/configuration matrices and real source/sink convergence remain
unproved. Automatic retention release, maintenance authorization and independent
restore detection need their separate accepted protocols. All current streaming
and versioned-offset loader guards stay intact.

Read-only edgeqa observations could establish release/PTF and query compatibility
for approved exact objects. They cannot establish deletion races, identity lifetime,
native transaction/rollback semantics or stale-reader effects. This experiment
does not connect to edgeqa or any IBM i system.

References: Kafka 4.3.1 [public REST API and offset management](https://github.com/apache/kafka/blob/4.3.1/docs/kafka-connect/user-guide.md),
[connector validation and transaction boundaries](https://github.com/apache/kafka/blob/4.3.1/docs/kafka-connect/connector-development-guide.md),
and [SourceConnector.alterOffsets](https://github.com/apache/kafka/blob/4.3.1/connect/api/src/main/java/org/apache/kafka/connect/source/SourceConnector.java).
