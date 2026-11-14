# Repository guidance

## Scope and layout

This is a Debezium source connector for Db2 for IBM i. Preserve ordinary row
capture and standard Debezium configuration/output behavior when changing the
optional transactional truncate path. The F/CR prerequisite is maintained
separately from the complete D/CG proposal; do not update another PR or branch
unless the task authorizes it.

- `debezium-connector-ibmi/`: task, snapshot/streaming lifecycle, transaction
  buffering, native metadata and recovery.
- `journal-parsing/`: journal retrieval, native records and row decoding.
- `jt400-override-ccsid/`: JT400 encoding support.
- `tools/`: optional offline, local Kafka and isolated native validation tools.
- `docs/`: configuration contracts, architecture and validation evidence.

Start with `README.md`, `docs/dcg-online-configuration.md` and the relevant tests.
Historical experiments are evidence, not necessarily current runtime contracts.

## Correctness boundaries

- Keep scan position R, replay floor F and durable completed-output position W
  separate. Reading an entry or returning a batch is not durable acknowledgement.
- Preserve commit, full rollback, savepoint, interrupted-delivery, missing-offset
  and replacement/fencing checks. Persist required history before exposing its
  checkpoint. Never advance past failed decoding or dispatch in fail mode.
- Require the correct physical-file/member identity and historical SQL name;
  do not substitute the current catalog for missing historical proof. Preserve
  ordinary capture selection and reject unsupported or ambiguous evidence.
- Ordinary delivery is at least once. Do not infer exactly-once guarantees from
  one successful test or require Kafka-specific ownership for other runtimes.
- A member-only clear must not become a whole-table truncate in shared-topic
  output. Do not change established keys, topics or temporal behavior implicitly.
- Keep transaction payloads, metadata, test observers and native operations
  bounded. Resource limits must fail explicitly, not silently discard records.

## Build and tests

Use JDK 21 and `./mvnw`. Snapshot core dependencies must match the modules in
`.github/workflows/cross-maven.yml`; record their revision when reproducing a
release artifact.

```sh
./mvnw -B -ntp verify -DskipITs
./mvnw -B -ntp clean install -Passembly -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

For a focused Java change, run its tests with the dependent modules, for example:

```sh
./mvnw -B -ntp -pl debezium-connector-ibmi -am \
  -Dtest=As400TruncateTest -Dsurefire.failIfNoSpecifiedTests=false test
```

Add tests for meaningful behavior, failures and security/recovery boundaries.
Prefer public behavior and controlled transport seams over reflection on private
fields. Consolidate repeated fixtures without hiding assertions. Preserve negative
controls, original failure reproductions and exact output/checkpoint assertions.
Do not count repeated Surefire executions as distinct test cases.

Run checks appropriate to the change; documentation-only changes do not require
repeating the entire suite. See `docs/dcg-contributor-testing.md` for local Kafka
and optional native commands. Never silently replace an original failed run with
an all-pass result from a corrected rerun.

## Native validation and evidence

Native access is optional and requires authorization in the current task or
conversation. Reuse that authorization within its scope; do not ask again for
each bounded fixture. Without it, use local captured fixtures and controlled
transports. Never infer authorization for production or existing application
libraries from permission to use a test system.

Use fresh, absent, explicitly owned libraries and dedicated local runtime
resources. Check server identity and absence first, track exact object ownership,
retain raw evidence privately, and verify cleanup independently. Refuse cleanup
when ownership, locks or unexpected dependencies are unresolved. Never use broad
deletion to bypass those checks.

Keep credentials in an owner-only file outside the checkout, preferably tmpfs;
never print them or put them in command arguments, logs, fixtures or Git. Do not
delete a shared credential file owned by another task. Sanitize native evidence
before publication, including embedded object/job/system names in binary payloads.

Record the exact source commit, plugin SHA-256, dependency/JDK versions, workload,
assertions and cleanup outcome. Check source-to-artifact identity before native
runs. Distinguish local fixture tests, native observations and inference. For
performance, report offered and achieved load, measurement duration, resource
scope and concurrent activity. A short run is not a capacity or leak proof.

## Worktrees and publishing

Use Worktrunk (`wt`) for agent-managed worktrees. Inspect `git worktree list` and
`wt list` first and reuse a matching worktree. Create them only under
`~/Worktrees/<repository>/<branch-slug>`, replacing `/` in the branch with `-`.
Check the effective path if repository configuration overrides the global rule.

Use `wt -C <repo> switch --create <branch> --base <base> --no-cd --format=json`
and use its returned path for later commands. For existing branches use
`wt -C <repo> switch <branch> --no-cd --format=json`. Remove only completed clean
worktrees with `wt remove --foreground --no-delete-branch <branch>`; never delete
a registered worktree directory directly or force removal of unfinished work.

Preserve unrelated changes. Commit/push or update GitHub only when authorized.
Use DCO sign-offs and the repository's `debezium/dbz#<issue> ...` commit convention.
PR descriptions should explain resulting behavior, tested artifact and evidence,
limits and remaining review gates. Preserve draft status unless asked to change it.
Upstream contributor registration in `COPYRIGHT.txt` and
`jenkins-jobs/scripts/config/Aliases.txt` is a separate maintainer/contribution step.
