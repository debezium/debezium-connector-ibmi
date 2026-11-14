# Contributor and agent guidance

This repository contains the Debezium source connector for Db2 for IBM i.
Keep changes focused on connector behavior, maintained tests and contributor
documentation. Machine-specific preferences belong in local configuration.

## Layout

- `debezium-connector-ibmi/`: connector task, snapshots, streaming and recovery.
- `journal-parsing/`: journal retrieval, native records and row decoding.
- `jt400-override-ccsid/`: JT400 encoding support.
- `tools/`: optional local Kafka and isolated IBM i test harnesses.
- `docs/`: configuration, architecture, testing and performance references.

Start with `README.md` and the relevant configuration and test files.
[Contributor testing](docs/dcg-contributor-testing.md) lists prerequisites for
each test layer; optional harness dependencies are not connector build requirements.

## Verification

Use JDK 21 and the checked-in Maven wrapper:

```sh
./mvnw -B -ntp verify -DskipITs
```

Run focused tests while developing, then the affected modules' checks. Add tests
for meaningful behavior and failure cases; avoid asserting private implementation
details or duplicating existing coverage. Changes to Python harnesses should run
their offline tests as described in the testing guide. Documentation changes
should check links, examples and consistency with configuration defaults.

## Correctness

- Keep journal scan position R, replay floor F and durable output position W
  separate. A successful read or callback is not durable acknowledgement.
- Preserve transaction outcomes, savepoints, interrupted delivery, restart and
  replacement guarantees. Persist required history before its checkpoint.
- Validate physical-file/member identity and historical SQL names. Missing
  evidence must not silently change capture selection or routing.
- Preserve existing keys, topics and conversions. A member-only clear cannot
  become a whole-table truncate in shared-topic output.
- Keep resource budgets explicit. Do not discard unfinished work to meet a limit
  or advance past failed decoding/dispatch in fail mode.

## Integration tests and changes

Native tests require a disposable IBM i system, explicit fixture-write opt-in,
and fresh owned libraries. Preserve credential redaction, ownership checks,
bounded operations and cleanup assertions. Do not put credentials, personal
paths, private services or raw source-system identities in the repository.

Record the artifact and workload when reporting integration or performance
results. Distinguish fixture tests from native observations and delivery latency
from throughput. Follow the upstream
[contribution guide](https://github.com/debezium/debezium/blob/main/CONTRIBUTING.md),
use DCO sign-offs, and preserve unrelated changes.

Use standard repository interfaces and document any additional dependencies.
Do not require a particular editor, agent platform, version manager, cloud
account, worktree manager or checkout location.
