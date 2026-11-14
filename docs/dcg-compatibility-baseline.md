# Runtime compatibility

The connector can run in Kafka Connect, Embedded Engine or Debezium Server.
Choose offset, history and acknowledgement settings for the actual runtime;
the optional testing harness is not a deployment dependency.

## Deployment evidence

| Runtime | Offset/history contract | Qualification |
| --- | --- | --- |
| Distributed Connect, ordinary offsets | Worker-managed offsets; matching Kafka binding history | Local task/restart tests and packaged native integration |
| Distributed Connect, source transactions | Connector-managed transaction boundaries and broker fencing; Kafka history | Local Kafka 4.2/4.3 checks and bounded native scenarios |
| Standalone Connect | File offsets, matching durable history, one active task | Local actual-task recovery tests |
| Embedded Engine | Ordered record acknowledgement, file offsets/history, one active instance | Actual Engine tests and native file-history validation |
| Debezium Server | Source properties prefixed with `debezium.source.`; sink acknowledgement must preserve the required order | Native JVM/HTTP profile; other sink/serialization combinations need their own validation |

Debezium Server has packaged the IBM i connector since
[DBZ-9015](https://github.com/debezium/debezium-server/commit/2d10004aa229a1375821008df147a98ebaf1db55).
Packaging availability is separate from validation of a particular deployment.

The operator-controlled first-start profile is narrower: it requires the
source-transaction/Kafka-history configuration documented in
[configured initialization](dcg-configured-native.md). Online activation can
use ordinary source offsets instead.

## Acknowledgement and restart

Online startup chooses the acknowledgement contract from the actual task
context. With ordinary offsets, callbacks identify delivered records but durable
source offsets determine completed output. Snapshot, handoff and recovery
barriers must reach the durable offset store before advancing F/W.

Ordinary delivery is at least once. A crash can repeat output that was delivered
but not durably checkpointed. Engine applications must preserve processing order,
acknowledge only handled records, and prevent independently overlapping instances.
File offsets alone do not provide broker fencing.

Both acknowledgement paths use the same transaction reconstruction, native
identity checks and immutable history. A source transaction context does not
replace those checks. Missing or incompatible offsets/history reject recovery;
disabling transactional capture after handoff cannot reinterpret versioned
offsets as legacy progress.

The [online configuration guide](dcg-online-configuration.md#file-backed-history-with-ordinary-source-offsets)
has file-backed configuration and recovery details.

## Converters, transforms and topics

Standard converters, predicates and transforms run in the selected runtime.
Pure `RegexRouter` chains have predictable destinations; other transforms
correlate acknowledgements through preserved source coordinates and delivery
metadata. Foreign, changed or incomplete acknowledgements cannot advance recovery.

Recovery progress records must survive filtering. Successfully filtering a data
record does not establish that a sink received it, and filtering the record
carrying a checkpoint can prevent that checkpoint from becoming durable.
Binding history is not transformed.

Consumers must understand `op=t`. Some unwrap transforms drop truncate events;
use a truncate-aware transform or preserve the Debezium envelope. Clear/reload
ordering needs one partition per table topic or downstream coordination.
Multiple data partitions remain available for ordinary row capture.

## Operational boundaries

- Retain complete journal history back to the replay floor, including open
  transactions on excluded tables, and retain matching schema/binding history.
- Keep one active task for a source generation. Qualified Kafka fencing and
  application-controlled exclusivity are different mechanisms.
- Memory and disk transaction modes share recovery semantics. Working spill
  files are disposable; durable offsets/history are not.
- Runtime storage, credentials, TLS, converters and sink behavior are deployment
  choices. Validation of the Server HTTP profile does not qualify every sink.
- System/basic ASPs 1–32 are accepted by native identity validation; recorded
  native runs used pool 1. Independent ASPs and remote journals remain unqualified.

See the [feature compatibility matrix](dcg-compatibility-comparison.md) for
row types, members, schema changes and upgrade behavior, and the
[testing guide](dcg-contributor-testing.md) for reproducible runtime checks.
