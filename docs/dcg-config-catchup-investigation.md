# Returning-worker configuration-log investigation

This is a local, test-only investigation of the network-partition case in
[`dcg-ownership-probe`](dcg-connect-ownership-experiment.md), using the pinned
Kafka/Connect 4.3.1 image. Production code, transactional streaming guards, the
stricter receipt tests and F/CR PR #114 are unchanged. No IBM i connection is made.

## Finding

The reproduced startup failure is a runtime recovery timing interaction. The
returning worker cannot finish an administrative log-end lookup within Connect's
configuration synchronization budget. Connect rejects its task before the fixture
starts. The evidence does not show missing configuration records or a source
decoder/replay failure, and does not establish a Kafka defect or the exact cause
of the underlying delayed network recovery.

The original sequence partitions the first worker with its truncate send blocked,
allows a replacement to publish through W=6, reconnects the first worker, checks
that its stale send is rejected and that output/offsets are unchanged, then kills
the replacement. Those checks establish safe stale-send rejection; they do not
establish that every client inside the returning worker has recovered. In
particular, Kafka's group client and its shared administration client are separate.

The original scenario, all its safety assertions and its 100-second reassignment
deadline remain intact. Additional logging and failure evidence do not turn its
failure into a pass. A separate runner makes explicitly named comparisons.

## Kafka path and observed timing

The pinned Kafka sources explain the failure before task startup:

* `DistributedHerder.doFenceZombieSourceTasks()` refreshes the configuration
  snapshot with `worker.sync.timeout.ms`. Failure produces the observed exception
  before source task startup/fencing can finish.
* With `read_committed`, `KafkaBasedLog` obtains the full log end through the
  administration client, then checks the consumer position against that end.
  Already having consumed the configuration records does not eliminate the lookup.
* `ConnectDistributed` creates the shared `TopicAdmin` from `config.originals()`.
  The comparisons therefore use unprefixed worker properties for that client;
  connector `admin.*` overrides would not configure this shared lookup.
* The configuration state tracks the last data record's offset plus one. Its
  reported offset 11 and the consumer/log end 12 are consistent with transaction
  control records; this difference alone is not missing history.

In the instrumented original run, the config consumer had already reached 12
before the partition. At 22:18:16.438 UTC it began another log-end lookup. The
shared administration client later logged a cancelled `LIST_OFFSETS` request
after about 60 seconds, followed by metadata/rebootstrap delays. At
22:21:14.969 it finally returned end offset 12. The worker configuration had a
3-second synchronization budget, 30-second administration request timeout and
60-second administration API timeout. These durations are not a recovery SLA;
the trace also includes client coordination and reconnection time.

During extended diagnostic collection, **after the original test's failed
100-second wait**, a new task eventually started and loaded exact W=6. The test
did not release that late task's polling gate. Thus the original result is a
bounded startup failure, not evidence that the worker can never recover.

## Independent evidence after failure

`As400ConnectBrokerEvidence` runs in a fresh observer container/JVM with no Connect
membership, producer, offset commits, or publication authority. It uses bounded
read-only Admin calls and manually assigned consumers. It records transaction
state, committed/uncommitted end offsets, raw configuration records, source
offsets and all three output topics. Topic creation and auto-commit are disabled.
Snapshots have timestamps and limits; they are not atomic across topics or
isolation levels.

After the original reassignment wait failed:

* Both config end offsets were 12; fresh consumers reached 12 in both isolation
  modes. Data records were at 0, 2, 3, 5, 6, 8 and 10, including the task-count
  fencing record.
* Both inspected transactions were `Empty`: configuration writer producer 0,
  epoch 6; source producer 1, epoch 11. There was no open transaction blocking
  the observed configuration end at that sample time.
* Committed data, transaction metadata and heartbeat output exactly matched the
  snapshot before removal of the replacement. The latest committed checkpoint
  was the exact accepted F=W=6 map. All sampled topics were read to their ends.

The same checks succeeded after the fast-client comparison failed. These are
point-in-time broker observations, not a promise about every future execution.
The runner attempts a worker thread dump, but this image has no `jcmd`; the saved
file records that limitation. Timing conclusions use Kafka TRACE logs and broker
evidence rather than an unavailable thread dump.

## Controlled comparisons

Build with the existing ownership-probe launcher first. Then run the separate
comparison script with a new evidence directory:

```sh
python3 -B tools/dcg-config-catchup-probe.py /absolute/new/evidence aligned
```

Every comparison calls the original partition method and retains its stale-send,
output, F/W, legitimate reassignment, source metadata, projection, monotonic
checkpoint and normal-stop assertions.

| Variant | Deliberate difference | Observation |
| --- | --- | --- |
| Original | None; added TRACE and post-failure observation only | Reproduced the 100-second reassignment failure. Independent broker checks passed. |
| `fast-client` | `request.timeout.ms=5000`, `default.api.timeout.ms=10000` | Still failed startup with the original 3-second sync budget. Independent broker checks passed. |
| `aligned` | Fast-client settings plus `worker.sync.timeout.ms=20000`, `rebalance.timeout.ms=30000` | Initial two runs passed reassignment, exact W=6 reload and later publication through W=9; then failed the harness's 10-second administrative-stop HTTP timeout. |
| `settled` | Original runtime; wait for a fresh config-log-end completion before killing the replacement | Inconclusive: no fresh completion marker arrived before the 240-second precondition deadline. This did not exercise the second fault. |
| `rejoined` | Original runtime; wait for a fresh successful group join before killing the replacement | Corrected run waited 16.688 seconds, then passed reassignment/replay through W=9 within the unchanged deadline; administrative stop failed its original 10-second HTTP deadline. |

For the final `aligned` comparison only, the stop HTTP deadline is 60 seconds,
matching its longer worker synchronization budget. The default probe still uses
10 seconds. This changes only the transport for the idempotent administrative
`PUT /stop`; it retains the existing five-attempt policy for explicit retryable
config-write responses/409 conflicts. It never retries a source failure or
extends the original task/reassignment deadline. The first two partial runs
remain failed in the evidence. Their post-failure comparison to W=6 is naturally
different because legitimate output had already reached W=9; that is not stale
publication or checkpoint regression.

With that comparison-only stop transport correction, two fresh-broker runs
(`aligned-final` and `aligned-final-repeat`) passed all 16 checks, including normal
stop and the absence of expired fault gates. Each needed one existing, explicitly
retryable config-writer stop retry. The combined runtime settings are tested as a
profile: this does not isolate the individual contribution of the synchronization
and rebalance budget changes, or establish that the values suit another system.

The first `rejoined` attempt used a log marker with the wrong wording and is an
invalid sequencing experiment. Its failure is retained. The corrected marker
matches Kafka 4.3.1's `Successfully joined group with generation` message and
must occur after reconnection.

In the corrected `rejoined-final` run, Kafka still logged an initial config-end
startup failure after the second fault, but subsequently started a new task within
the original 100-second test deadline. That task loaded exact W=6 and passed all
publication/replay checks through W=9. The run remains **failed** because normal
stop timed out; it did not use the aligned variant's longer stop transport.
This single sequencing comparison supports a timing-sensitive recovery interaction,
not a guarantee that group rejoin establishes administration-client readiness.
Its post-failure snapshot is compared with the earlier W=6 baseline, so changed
output/checkpoint equality flags reflect the already-verified legitimate W=9
progress, just as in the first two aligned attempts.

## Evidence and limits

The local evidence root is
`$BB_THREAD_STORAGE/dcg-config-catchup` for thread `thr_vzfz57gebs`.
Each run has `report.json`, exact worker properties, command/worker/broker logs,
task-generation events, source/artifact hashes and cleanup results. Failed cases
include `control/failure-broker.json`. Earlier comparison driver versions and the
pre-HTTP-budget Java source are archived so historical hashes remain resolvable.
The final verification inventory records current hashes and protected worktrees.

These comparisons support investigating a coherent Connect/client timeout
profile. They do not establish universal timeout values, production availability,
ordinary network-failure recovery across environments, exactly-once source
delivery, native reader lifetime, initial baseline safety, retained IBM i history,
or source/sink convergence. They do not justify removing existing production
receipt/ownership checks. Transactional streaming stays disabled.

Pinned primary references:
[KafkaBasedLog](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/util/KafkaBasedLog.java),
[DistributedHerder](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/distributed/DistributedHerder.java),
[ConnectDistributed](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/cli/ConnectDistributed.java),
[KafkaConfigBackingStore](https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/storage/KafkaConfigBackingStore.java).
