# Reproducing D/CG tests

The default tests use sanitized raw journal fixtures and controlled transports;
they need JDK 21 and the matching Debezium core, but no IBM i account, AWS,
private tooling, or external Kafka. Run the same core modules listed in
`.github/workflows/cross-maven.yml`, then:

```sh
./mvnw clean install -Passembly -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

`As400SnapshotContractTest` exercises actual task/coordinator snapshot markers,
durability barriers, partial restart rejection and JMX. `As400OutputContractTest`
exercises selected snapshot/CDC output and normal Converter serialization.
`As400BatchDispatcherTest` includes keyless truncate and tombstone conversion.
Avro tests use an in-memory schema registry; no external registry is contacted.

Historical custom-owner/receipt and protected-startup prototypes are not part of
this checkout, build or distribution. Their archived experiments are separate
from the supported runtime and do not establish source exactly-once guarantees.

## Local Kafka

Docker and Python 3 are sufficient after Maven dependencies are installed:

```sh
DCG_KAFKA_VERSION=4.2.0 DCG_PROBE_BOUNDARY=connector tools/dcg-ownership-probe /tmp/dcg-ack-420 ack
DCG_KAFKA_VERSION=4.3.1 DCG_PROBE_BOUNDARY=connector tools/dcg-ownership-probe /tmp/dcg-offsets-431 offsets
```

Directories must not already exist. Versions select digest-pinned images in
`dcg-connect-probe.py`. Public CI runs acknowledgement/restart and missing-offset
cases on both versions with `DCG_PROBE_BOUNDARY=connector`, exercising the actual
task's commit request and production runtime-version/capability admission. The
source is synthetic in these local cases; they do not establish native startup
proof. The default `poll` keeps the original experiment
and failure reproduction unchanged. Additional `interrupted`, `mixed` and `fencing` cases
exercise whole-batch replay, metadata, tombstones and legitimate reassignment.
Every runner retains independent broker output/offset evidence and verifies its
own container/network cleanup. Failures also collect broker evidence.

The original `partition` reproduction intentionally preserves the problematic
timeout profile. It is not a passing CI smoke test. The corrected-profile matrix
uses `dcg-timeout-profile-probe.py`. Retain its timing profile and every original
safety assertion when rerunning it. Do not replace the original reproduction.

## Optional native tests

There is no IBM i equivalent of a local Db2 LUW container. Native tests require
an explicitly supplied disposable IBM i host and IBM i Access ODBC driver,
Python `pyodbc`, Java/JT400 from the build and local Docker. No source access
occurs in the offline or Kafka-only tests.

Create an owner-only (0600) JSON file outside the repository, preferably in
tmpfs, with `host`, `user`, `password`, exact `server` (CURRENT_SERVER), and
`allow_isolated_fixture_writes: true`. Optional `odbc_driver` names the installed
driver; the default is `IBM i Access ODBC Driver`. Set `IBMI_TEST_CONFIG` to its
path. The runner refuses missing credentials, permissive file modes, symlinks,
implicit server selection and unacknowledged fixture writes. This bounded test
profile uses the driver's normal non-TLS connection; use only an isolated test
network. It does not qualify production transport security.

```sh
IBMI_TEST_CONFIG=/dev/shm/ibmi-test.json PYTHON=/path/to/venv/bin/python \
  tools/dcg-packaged-probe /tmp/dcg-native-evidence DCNC9991 smoke
```

Choose a fresh `DCNCdddd` library. The runner checks server identity and library
absence, creates only its fixture, retains the exact owned object inventory,
preserves receiver recording options and tests the packaged connector. Credentials
enter worker tmpfs over stdin, never connector config topics or command arguments.
Reports redact host, user and password. External credential resolution belongs
outside this repository; the runner requires no private credential tool or cloud SDK.

Cleanup refuses unexpected objects, pending transactions, active locks or
unattributed inquiries. Only the exact owned library and receivers are removed.
Read `report.json` and `native/cleanup.json`; a failed test is not evidence that
cleanup succeeded. Retain a failed fixture for inspection if ownership or lock
checks cannot be proven. Remove the credential file after the run.

The portable transport replacement has offline admission/redaction/cleanup
coverage and a fresh packaged native smoke run with 32 passing assertions and
verified cleanup (2026-11-08). This run used externally supplied credentials;
neither an AWS session nor a private credential package is required by the runner.

`tools/dcg-temporal-probe NEW_EVIDENCE_DIRECTORY DCNCdddd` uses the same explicit
configuration and cleanup contract. It adds DATE, TIME and TIMESTAMP(0/3/6/9),
nulls, pre-epoch values, end-of-day TIME, temporal clear/reload commit and rollback,
and replacement while that clear is open. It preserves the packaged smoke
assertions. See [validation scope](dcg-validation.md).

`tools/dcg-member-compatibility-probe.py NEW_EVIDENCE_DIRECTORY DCNCdddd eos`
uses the ordinary packaged connector to exercise legacy rows from two members,
online activation, a newly added member, index creation/deletion, unrelated-table
progress during a clear, replacement and safe rejection of a member-only clear.
The journal also contains a data area, data queue and journaled access paths.
Use `source-offsets` instead of `eos` to exercise ordinary offsets with disk
working storage; the EOS case uses memory. Both retain existing table keys and
topics. Use a distinct absent library and evidence directory for each invocation,
with the same explicit credentials, local Kafka prerequisites and cleanup checks
as the native runner above.

For excluded large-object coexistence, use
`python tools/dcg-excluded-lob-probe.py NEW_DIRECTORY DCNCdddd source-offsets`
(or `eos`). The fixture owns an excluded CLOB table and captured ordinary tables.
It checks legacy-to-native activation, 60 KB/NULL/empty values, mixed commit and
rollback, replacement while a transaction is open, unrelated progress, and
captured truncate/reload. It also places an excluded LOB row in a captured
table's observed ALTER interval and restarts from the resulting schema history.
The DDL acknowledgement override belongs only to the fixture's dedicated job
and is restored before that session closes. It uses the ordinary packaged plugin;
external LOB values are never decoded by the candidate. This does not qualify LOB value
capture. Local `As400ExcludedLobTest` replays sanitized native pointer descriptors
and controls, including interrupted delivery and invalid binding/row metadata;
`RawJournalPageTest` checks handle release on the retrieving job and cancellation
before progress when release fails.

For transactional clears encountered before online activation, use
`python tools/dcg-deferred-clear-probe.py NEW_DIRECTORY DCNCdddd source-offsets metadata`
(or replace `source-offsets` with `eos`). An open excluded-table transaction keeps
admission pending while captured tables commit, roll back and use savepoints.
The runner checks unrelated-table progress, pinned durable legacy offsets across
600 rows, both D/CG subtypes, receiver rotation, worker replacement, consecutive
clears, a following F/CR across restart and eventual handoff without another snapshot. It uses the same explicit
fixture authorization and cleanup as the public probe. Raw native evidence is
private until inspected and sanitized; missing-history and already-published
same-table prefix rejection are also covered by local Java tests.

For unrelated-table progress during unfinished CREATE and DROP, use
`python tools/dcg-schema-refresh-probe.py --open-objects NEW_DIRECTORY DCNCdddd source-offsets`
(or replace `source-offsets` with `eos`). The runner uses the packaged connector
and the same isolated-fixture authorization and cleanup. It holds each DDL
transaction open across unrelated output and worker replacement, then commits
or rolls it back. It checks durable F/W, invisible uncommitted rows, creation
metadata, and rows written before a DROP. Native journal intervals and output
evidence stay in the chosen directory; inspect and sanitize them before sharing.

For mixed schema and transaction outcomes, use
`python tools/dcg-schema-refresh-probe.py --mixed-outcomes NEW_DIRECTORY DCNCdddd source-offsets`
(or `eos metadata`). It pauses the reader to reproduce ALTER followed by a
rolled-back clear and CREATE followed by ALTER before metadata acquisition. It
also commits an ALTER with a later row-only savepoint rollback, exercises
CREATE→DROP, CREATE→clear/reload, adding a new table's key before its first row,
`CHGPF SIZE(*NOMAX)`, and captured/excluded renames with distinct SQL/system names.
It adds/removes a captured CHECK constraint and preserves the already published
key through primary-key constraint removal, replacement and re-addition.
It replaces the worker and checks exact output against the committed writer ledger. The same
fresh-library authorization, retained raw evidence and verified cleanup apply.

For a serial comparison of schema-refresh latency, run
`python tools/dcg-schema-refresh-probe.py --schema-timing NEW_DIRECTORY DCNCdddd source-offsets`
once per packaged build in different fresh libraries. Run without other local
or native workloads where possible. The report separates DDL execution time,
probe insertion-to-observation time and insertion-to-durable-offset time. Two
rounds alternate baseline probes with captured/excluded ALTER and LABEL changes;
every probe must appear exactly once. This includes broker-observer overhead and
does not measure connector-only pause time or saturation throughput.

For captured files moving between libraries on the same journal, run
`python tools/dcg-library-move-probe.py NEW_DIRECTORY DCNCdddd DCNCeeee --distribution PLUGIN.zip`.
Both libraries must be fresh and absent; the second library is a separately
inventoried destination. The runner validates exact object ownership before and
after each `MOVOBJ` and cleans up each fixture using its own inventory. It uses standard
exclusion filters throughout legacy startup and online activation, so tables
created later are not named in a legacy server-side file filter.

The default `--selection both --delivery source-offsets` checks exact topics,
keys, values and ordering for ALTER/row/move and CREATE/row/move catch-up. It
replaces the worker while the moved table has an open truncate, checks unrelated
progress, commit and rollback, moves both files back and restarts again. Use
`--selection source` or `--selection destination` to check capture selection on
each side, and `--delivery eos` for the supported Kafka source-transaction mode.
Each run retains its plugin hash, independent broker ledgers, native journal
interval and cleanup evidence. These bounded scenarios do not qualify
cross-pool moves, save/restore, or clearing one member of a multi-member file.

For a bounded disk-buffer endurance run, use
`python tools/dcg-soak-probe.py NEW_DIRECTORY DCNCdddd --distribution PLUGIN.zip --label candidate --mode transactional --buffer-storage disk --duration 1200 --rows 100 --backlog-rows 5000`.
This retains exact mixed-value and before-image checks, commits and rolls back
5,000-row transactions, and keeps a clear open while unrelated output advances.
By default it rotates three owned receivers during that open clear and performs
three task stop/resume cycles during the timed workload. Each restart checks
durable F/W and unchanged binding history. `--held-clear-rotations 0` and
`--restart-count 1` retain the earlier simpler schedule. The observer has explicit
row, byte and time bounds; a 20-minute pass is not a multi-day soak or a maximum
capacity measurement.

During the soak, offset-topic inspection retains the latest value or tombstone
per key through a fixed broker endpoint. It bounds scanned records, retained
keys/data and elapsed time, so obsolete offset updates do not exhaust the
ordinary event inspector's 1,000-record limit. Data-topic observations still
retain every record and check exact counts; duplicate output is not collapsed.

## Larger bounded profiles

The extended profiles retain the ordinary packaged connector and the same exact
output, rollback, checkpoint and ownership/cleanup assertions. They do not change
production resource limits or the original harness defaults. Use different fresh
libraries and evidence directories for every invocation, with the explicit native
configuration above.

```sh
# 128 journaled files; busy final allocation group, unrelated progress,
# excluded layout, keyless clear, worker replacement and rollback.
python tools/dcg-online-scope-probe.py NEW_SCOPE_DIRECTORY DCNCdddd --files 128

# Independent paced writer; repeat for the original, disabled and enabled ZIPs.
python tools/dcg-sustained-probe.py NEW_RATE_DIRECTORY DCNCeeee \
  --distribution PLUGIN.zip --label candidate --mode transactional \
  --buffer-storage disk --rows 250 --seconds 120 --operations-per-second 1000 \
  --failure-handling warn --worker-defaults

# One hour with no scheduled task restarts in the timed interval.
python tools/dcg-soak-probe.py NEW_HOUR_DIRECTORY DCNCffff \
  --distribution PLUGIN.zip --label candidate --mode transactional \
  --buffer-storage disk --duration 3600 --rows 50 --restart-count 0 \
  --backlog-rows 5000
```

The scope runner defaults to 56 files; `--files 128` expands only the owned test
journal. The sustained runner defaults to 200 operations/second; 500 and 1,000
are higher *requested* rates. Report the achieved submission rate as well as
observed delivery. Higher-rate profiles cap source operations at 120,000 and use
the existing 150,000-record/256 MiB observer; original-rate behavior is unchanged.

The hour profile requires 50 rows per burst and zero scheduled timed restarts,
with at least five seconds per three-burst round. This bounds timed changes to
108,000 while retaining the 145,000 workload and 150,000 observer record limits.
Its observer deadline is 90 minutes and owned worker/observer containers live at
most two hours. The preceding two backlog/restart trials, 5,000-row commit and
rollback, open-clear checks and receiver rotations happen **before** the timed
hour. Verify task continuity during the hour from the worker lifecycle evidence;
zero requested restarts alone does not prove that the task never restarted.

This endurance workload is serial bursts, not an hour at the higher sustained
rate. Keep those results separate. Short rate runs do not establish saturation;
a one-hour pass does not establish multi-day leak freedom or every fault schedule.
