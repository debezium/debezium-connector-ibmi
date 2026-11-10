# Local Kafka Connect recovery experiment

This manual test runs a real Kafka 4.3.1 broker and distributed Connect workers
against synthetic journal bytes. It exercises the existing
`As400ReplayCoordinator`, source normalizer, transaction buffer and checkpoint
codec. It does **not** connect to IBM i, exercise `As400ConnectorTask`/Debezium dispatch,
validate startup evidence, or enable transactional streaming. F/CR PR #114 is
outside its scope.

The [local runtime profile revision](dcg-runtime-profile-revision.md) uses this
experiment as partial evidence for evaluating standard Connect offsets/fencing.
It separates optional cleanup/restore authority from ordinary recovery. The
stronger contract and remaining-gate discussion below describe the original
experiment context; no current guard or gate is removed by the revision, and the
experiment's runtime configuration is not a production support claim.

## Run and evidence

From this worktree, with Docker, Python 3, Maven dependencies and JDK 21 available:

```sh
mise exec java@temurin-21 -- tools/dcg-connect-probe /absolute/path/to/new-evidence-directory
```

The evidence directory must not exist. The launcher compiles test classes and
resolves the test classpath. The runner stages a **test-only** connector artifact,
then creates a uniquely named, internal Docker network and disposable containers.
No ports are published. The controller accesses worker REST over loopback inside
each container using `docker exec`.
The image is pinned by digest, and the runner requires a local Unix-socket
Docker endpoint. No credentials, IBM i endpoints or arbitrary
connector configurations are accepted. Do not install the staged probe jar as a
production connector.

Each run saves the image digest, plugin hashes, worker properties, command log,
worker/broker logs, raw Kafka record snapshots, actual offset-topic snapshots,
task-generation start/checkpoint observations, fault-injection markers and a
machine-readable `report.json`. Its checks fail on unexpected data, checkpoints,
missing fences or timeouts. The runner removes only its own named containers and
network on normal completion and exceptions, and records cleanup verification.
An uncatchable process/host failure can leave resources bearing its
`dcg.local.probe` label; inspect those names before manual removal. Evidence and
cached Docker images remain locally.

## Fixture and assertions

The fixed input is a transaction start, D/CG subtype 8 clear, two inserts and a
commit at sequence 6, followed by a second transaction inserting another row and
committing at sequence 9. Every fresh task constructs a new coordinator and reads
its checkpoint using Connect's actual offset reader. A synthetic baseline at
sequence 1 is allowed only when the disposable connector has no saved offset;
this is not a production missing-checkpoint policy.

The test adapter prepares a bounded queue, then returns **one record per poll**.
This deliberately splits the source transaction across Kafka transactions.
BEGIN/END and heartbeat values are test markers, not a full Debezium envelope or
transaction-metadata implementation. Output values include source sequence,
task generation and the coordinator-stamped offset so they can be compared with
the broker's actual offset topic. Record timestamps retain the synthetic source
timestamp. Data and offset topics each have one partition.

| Case | Injection | Required evidence |
| --- | --- | --- |
| Capability honesty | Request `exactly.once.support=required` | Framework rejects the probe's explicit `UNSUPPORTED` capability. `requested` runs with worker transactional support enabled without promising source exactly-once delivery. |
| Delayed acknowledgement | Block the real producer interceptor after the broker acknowledges the checkpoint heartbeat, before acknowledgement processing completes | The heartbeat exists under `read_uncommitted` but is absent under `read_committed`; durable F/W remains the previous safe checkpoint. Releasing the hook allows F=W=6 to commit. |
| Completed batch restart | Kill the first worker after F=W=6 is durable; start a new process | New task loads the exact saved checkpoint, suppresses the first batch, and publishes the later transaction in order. |
| Interrupted reload | Kill the first worker after BEGIN, truncate and first insert are committed | Stored F=2 inclusive/W=1 survives. A new process replays the whole first batch before the second. Duplicate truncate/insert records are expected, and the synthetic projection converges. |
| Partitioned worker | Block the old worker inside its producer's send, disconnect its Docker network, let a replacement complete, reconnect and release the send | Connect closes the orphan producer; the stale send is rejected locally. Committed output and W do not regress. This alone does not prove broker fencing. |
| Suspended stale publisher | Block the send and suspend the entire old process before membership expires; let a replacement complete; release the hook and resume the old process | Broker producer-epoch fencing rejects the still-open old producer. No stale truncate appears in committed output after replacement, and W cannot regress. |

The send hook intentionally ignores interruption while blocked, simulating an
uncooperative task. Both hooks have a finite deadline; a timed-out hook is failed
fault injection, not a successful fence. The acknowledgement test delays callback
processing, not a network packet. The network-disconnection case partitions the
entire old worker from Kafka and the other worker. The separate suspension case
prevents the old worker from closing its producer while the replacement starts;
it must observe a broker fencing error, not merely a closed-client error. These are specific failure
windows, not exhaustive coverage of every send/flush/rebalance interleaving.

## What the framework can establish

Kafka's [source-support configuration][worker] and [development guide][development]
distinguish framework transactions from a connector's semantic capability claim.
The probe explicitly returns `ExactlyOnceSupport.UNSUPPORTED`: conservative
F/previous-W offsets deliberately permit whole-batch duplicates after an already
committed output prefix. The worker uses `exactly.once.source.support=enabled`,
the connector requests support with `exactly.once.support=requested`, and
`transaction.boundary=poll` defines the Kafka commit boundary.

Consumers must use `read_committed` to rely on transactional visibility. A Kafka
transaction per poll does not make a whole IBM i transaction atomic, nor does
producer fencing terminate or isolate an IBM i reader. The synthetic projection
is a test oracle, not a deployed downstream sink validation.

In the pinned runtime, [ExactlyOnceWorkerSourceTask][implementation] commits its
producer transaction before invoking task commit-record callbacks. This is useful
framework evidence, but does not turn the existing Debezium `BaseSourceTask`
last-offset bookkeeping into an enrollment/generation-qualified durable receipt.
Reading the actual offset topic proves the checkpoint observed by this local
experiment. A production adapter still needs the exact immutable checkpoint,
source-partition and owner-generation association, failure handling and durable
receipt protocol described in the [protection proposal](dcg-protection-feasibility.md).

## Observed local result (2026-11-06)

The complete local run passed 19 assertions across acknowledgement, restart,
interrupted output, network-partition shutdown and suspended-producer fencing.
The source-specific callback for the resumed stale truncate reported
`InvalidProducerEpochException`; committed output was unchanged after the stale
send. The separate network-partition case reported a closed producer instead,
which establishes that shutdown path but would not by itself establish broker
fencing. Actual offset-topic snapshots and fresh-worker offset reads matched.

The build and 678 existing unit tests passed, as did formatter/import validation
and launcher syntax checks. The probe class was absent from the production jar.
All containers and networks created by the complete run were removed; raw evidence
and logs were retained locally. No IBM i connection or mutation was performed.

The result supports continuing a Kafka 4.3.1 publication-adapter prototype. It does
not justify `ExactlyOnceSupport.SUPPORTED`, whole-source-transaction atomicity or
a production durable-receipt contract. Runtime compatibility with other Kafka
versions, actual Debezium task integration and native protection remain untested.

## Remaining release gates

This test contributes local evidence toward P2/P5/P6; it does not pass those gates
as a whole. Native receiver retention, enforced maintenance exclusion, protected
history, baseline writer census, source-reader fencing, ownership/receipt storage,
anti-rollback behavior, source integration and live IBM i/sink semantics remain
open. No N/H/B/P gate is closed by this test. Transactional clears remain blocked
in the streaming connector.

[worker]: https://kafka.apache.org/43/generated/connect_config.html#exactly.once.source.support
[development]: https://github.com/apache/kafka/blob/4.3.1/docs/kafka-connect/connector-development-guide.md
[implementation]: https://github.com/apache/kafka/blob/4.3.1/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/ExactlyOnceWorkerSourceTask.java

A subsequent [task/receipt prototype](dcg-task-receipt-prototype.md) exercises the
normal task polling, dispatch and commit lifecycle with synthetic source startup.
It retains the production enablement and native protection limitations above.
