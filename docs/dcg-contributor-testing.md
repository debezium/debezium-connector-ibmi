# Building and testing

Choose the test layer relevant to the change. Optional integration tools are not
requirements for building or running the connector.

## Prerequisites

| Work | Requirements |
| --- | --- |
| Connector build and Java unit tests | JDK 21, the checked-in Maven wrapper and access to the required Maven artifacts |
| Python harness tests | Python 3.9 or newer on a POSIX host; standard library only |
| Local Kafka integration | JDK 21, Git, Bash, Python, and a local Docker Engine reachable through a Unix socket |
| Native IBM i integration | An explicitly supplied disposable IBM i system, IBM i Access ODBC driver and `pyodbc`; Kafka profiles also need the local Docker setup |
| Performance resource sampling | Linux host with a local Docker daemon whose worker process is visible in host `/proc`; JDK recording tools |
| Optional `native-identity-probe` | Bash, JDK 21 and GNU `timeout` for the outer process deadline |

No editor, version manager, cloud account or worktree utility is required.
The shell examples use POSIX syntax. Use `mvnw.cmd` for Java builds on Windows.
The optional integration harnesses use POSIX ownership/permissions and Linux
containers; they are not qualified Windows or remote-Docker runners.

## Build and unit tests

Select JDK 21 with `JAVA_HOME` or place its `bin` directory on `PATH`. No
particular vendor or installation directory is required. Check `./mvnw -version`
before building.

```sh
./mvnw -B -ntp verify -DskipITs
./mvnw -B -ntp clean install -Passembly -DskipITs \
  -Dformat.formatter.goal=validate -Dformat.imports.goal=check
```

Java unit tests use captured fixtures and controlled transports. They do not
connect to IBM i or an external Kafka broker. Avro tests use an in-memory schema
registry. `-DskipITs` skips live integration tests, not unit tests.

The sanitized journal samples in each module's `src/test/resources` are test
data, not connection profiles. Tests replay their bytes and metadata from the
checkout; contributors do not need the original server, libraries or accounts.

If the required `3.8.0-SNAPSHOT` core dependencies are unavailable, obtain the
matching [Debezium core](https://github.com/debezium/debezium) checkout and build
the same modules as [Cross Maven CI](../.github/workflows/cross-maven.yml).
From this connector checkout, replace the placeholder with that checkout's path:

```sh
./mvnw -B -ntp -f /path/to/debezium/pom.xml \
  -pl debezium-bom,debezium-connector-common,debezium-util,debezium-config,debezium-embedded,debezium-storage,debezium-assembly-descriptors \
  -am install -DskipTests -DskipITs
```

Record the core revision for reproducible artifacts. A globally installed Maven
is unnecessary. For a focused Java change:

```sh
./mvnw -B -ntp -pl debezium-connector-ibmi -am \
  -Dtest=As400TruncateTest -Dsurefire.failIfNoSpecifiedTests=false test
```

For changes to the Python harnesses:

```sh
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

These tests exercise credential, fixture, allocation/release, observation and
cleanup safety without importing the native ODBC driver.

## Local Kafka

The runners create dedicated containers, networks and evidence directories.
Git records the source revision; Docker must support local bind mounts and a
Unix-socket endpoint. Remote daemons are deliberately refused. The Kafka images
are digest-pinned in `tools/dcg-connect-probe.py`; their internal paths do not
refer to host installations.

```sh
DCG_KAFKA_VERSION=4.2.0 DCG_PROBE_BOUNDARY=connector \
  tools/dcg-ownership-probe /path/to/new-ack-evidence ack
DCG_KAFKA_VERSION=4.3.1 DCG_PROBE_BOUNDARY=connector \
  tools/dcg-ownership-probe /path/to/new-offset-evidence offsets
```

These wrappers build the required test classes and classpath. Set `PYTHON` to
an alternative Python executable, or activate a virtual environment first.
Each output directory must be new. CI runs `ack` and `offsets` on both Kafka
versions. Additional `interrupted`, `mixed` and `fencing` scenarios test replay
and reassignment.

The `partition` scenario retains the original failing timeout reproduction.
`dcg-timeout-profile-probe.py` tests the corrected profile separately; do not
weaken the original failure or its broker safety assertions.

Other maintained local profiles include:

| Runner | Purpose |
| --- | --- |
| `dcg-normal-task-probe` | Actual task/coordinator, ordinary offsets, standalone, routing and restart |
| `dcg-task-stop-probe.py` | Replacement while an old RPC or JDBC operation remains held |
| `dcg-first-start-probe` | Snapshot acknowledgement and crash/reassignment boundaries |

Each runner verifies cleanup of its own resources and retains broker evidence on
failure. These tests use controlled source records and do not qualify native
IBM i startup semantics.

## Optional native tests

IBM i has no equivalent local Db2 LUW container. Use a disposable system and
fresh test libraries. Install the IBM i Access ODBC driver for the host, then
install `pyodbc` in your chosen Python environment:

```sh
python3 -m venv /path/to/test-venv
/path/to/test-venv/bin/python -m pip install pyodbc
```

Provide an owner-only (0600) JSON file outside the checkout with `host`, `user`,
`password`, exact `server` (`CURRENT_SERVER`) and
`allow_isolated_fixture_writes: true`. Optional `odbc_driver` selects the
installed driver; the default is `IBM i Access ODBC Driver`. Set
`IBMI_TEST_CONFIG` to the file. The runner refuses symlinks, permissive modes,
implicit server selection and missing write opt-in.

Native runners also require Python assertions to remain enabled. They reject
`-O`, `-OO` and `PYTHONOPTIMIZE` before reading the connection configuration;
optimization would remove fixture identity and cleanup checks.

Live runners have no built-in host or private credentials. They create their own
schemas, tables, journals and sample rows on the supplied system; they do not
depend on existing application data. Recorded native qualification used IBM i
7.5. A different release, PTF level or system configuration must supply the
required native services, journal settings and authorities; the recorded results
do not qualify every such environment.

```sh
IBMI_TEST_CONFIG=/path/to/private/ibmi-test.json \
  PYTHON=/path/to/test-venv/bin/python \
  tools/dcg-packaged-probe /path/to/new-native-evidence DCNC9991 smoke
```

Use a new absent `DCNCdddd` library for each run. The runner checks server
identity and absence, records object ownership, and verifies cleanup. Never
substitute an application library. The current container harness resolves one
IPv4 address for the test host and uses non-TLS ODBC/JT400 fixture connections;
use an isolated test network. This is a test profile, not a connector transport
requirement or production security qualification.

The local Docker daemon must support the network options used by the native
runner, including `docker network connect --gw-priority`. Credentials enter
worker tmpfs through stdin and are redacted from reports. The Engine runner uses
owner-only temporary files and honors `TMPDIR`; choose a private temporary
directory or tmpfs if required. Cleanup removes only files it created.

Before invoking a `.py` runner directly, build the distribution/test classes and
generate its classpath:

```sh
./mvnw -B -ntp install -Passembly -DskipITs
./mvnw -B -ntp -pl debezium-connector-ibmi dependency:build-classpath \
  -DincludeScope=test -Dmdep.outputFile=target/connect-probe-classpath.txt
```

Use the Python executable with `pyodbc` installed. Native profiles share this
configuration and cleanup contract:

| Runner | Scenario |
| --- | --- |
| `dcg-public-online-probe.py NEW_DIRECTORY DCNCdddd source-offsets` | Public connector, existing-stream handoff, both clear subtypes, commit/rollback and replacement; `eos` selects source transactions |
| `dcg-deferred-clear-probe.py NEW_DIRECTORY DCNCdddd source-offsets metadata` | Clear outcomes before handoff, savepoints, pinned offsets and unrelated progress |
| `dcg-schema-refresh-probe.py --mixed-outcomes NEW_DIRECTORY DCNCdddd source-offsets` | DDL, clear, constraint and rename compositions; `--open-objects` tests unfinished CREATE/DROP |
| `dcg-member-compatibility-probe.py NEW_DIRECTORY DCNCdddd source-offsets` | Multiple members, mixed journal resources, restart and safe partial-clear rejection |
| `dcg-excluded-lob-probe.py NEW_DIRECTORY DCNCdddd source-offsets` | Excluded CLOB values alongside captured ordinary tables |
| `dcg-file-history-probe.py NEW_DIRECTORY DCNCdddd` | Actual Engine with file offsets/history and no Kafka broker |
| `dcg-interrupted-publication-probe NEW_DIRECTORY DCNCdddd` | Native 5,000-row clear/reload interrupted during publication; independent broker evidence and replacement recovery |
| `dcg-library-move-probe.py NEW_DIRECTORY DCNCdddd DCNCeeee --distribution PLUGIN.zip` | Two owned libraries; `--selection both\|source\|destination` and `--delivery source-offsets\|eos` select routing/delivery cases |
| `dcg-online-scope-probe.py NEW_DIRECTORY DCNCdddd --files 128` | Larger journal scope, busy-lock release, keyless clear and replacement |

The interrupted-publication probe performs native transactions and fixture writes.
It requires the `IBMI_TEST_CONFIG` credentials and explicit fixture-write opt-in,
IBM i Access ODBC driver, `pyodbc` and local Docker described above. For a
controlled-source test without IBM i, use the local ownership runner's
`interrupted` scenario instead.

For file moves, both libraries must be fresh and independently inventoried.
For first-start window tests, use the profile in
[configured initialization](dcg-configured-native.md#reproducible-qualification).
See [F/CR testing](fcr-testing.md) for the separate nontransactional clear tests.

## Performance and endurance

Build before measuring; do not run builds or competing test workloads during
timed comparisons. Use a fresh library and evidence directory for every run:

```sh
python3 tools/dcg-sustained-probe.py NEW_RATE_DIRECTORY DCNCdddd \
  --distribution PLUGIN.zip --label candidate --mode transactional \
  --seconds 120 --operations-per-second 500 --rows 250 \
  --failure-handling warn --worker-defaults --buffer-storage disk

python3 tools/dcg-soak-probe.py NEW_HOUR_DIRECTORY DCNCeeee \
  --distribution PLUGIN.zip --label candidate --mode transactional \
  --duration 3600 --rows 50 --restart-count 0 --buffer-storage disk
```

Use the same transactional settings with `--buffer-storage memory` for a
storage comparison. Use `--mode legacy` with the original distribution for
a baseline. The isolated `--journal-additional-delay 0` control is a diagnostic,
not a recommendation to change the legacy default.

The one-hour profile has no scheduled task restart during its timed interval.
Shorter profiles can include three restarts and three receiver rotations.
Observer record, byte and time bounds remain enforced; controller observation
deadlines are not application-lock durations or connector capacity limits.

See [performance methods and recorded results](dcg-performance-comparison.md)
and [validation coverage](dcg-validation.md). Report achieved load, exact output,
artifact identity, resource scope, failures and cleanup. Inspect raw captures
before publication; they can contain source data and system identities.
