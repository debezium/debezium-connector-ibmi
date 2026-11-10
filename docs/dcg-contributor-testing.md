# Reproducing D/CG tests

The default tests use sanitized raw journal fixtures and controlled transports;
they need JDK 21 and the matching Debezium core, but no IBM i account, AWS,
Auction Edge tooling, or external Kafka. Run the same core modules listed in
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

The active review series omits the historical owner/receipt and protected-startup
Java prototypes. To run them, apply the separate `optional-prototypes.patch` to a
disposable checkout of the series, then use `-Pdcg-prototype-tests`. The profile
alone does not supply the omitted sources. They do not enter the distribution JAR
and do not establish source exactly-once guarantees for the supported runtime.

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
proof. The active experiment also runs with historical prototype Java sources
absent. The default `poll` keeps the original experiment
and failure reproduction unchanged. Additional `interrupted`, `mixed` and `fencing` cases
exercise whole-batch replay, metadata, tombstones and legitimate reassignment.
Every runner retains independent broker output/offset evidence and verifies its
own container/network cleanup. Failures also collect broker evidence.

The original `partition` reproduction intentionally preserves the problematic
timeout profile. It is not a passing CI smoke test. The corrected-profile matrix
uses `dcg-timeout-profile-probe.py`; see the timeout validation report for exact
timings. Do not weaken assertions or replace that original reproduction.

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
outside this repository; the runner imports neither aesql nor an AWS SDK.

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
assertions. See [temporal evidence and limits](dcg-temporal-validation.md).
