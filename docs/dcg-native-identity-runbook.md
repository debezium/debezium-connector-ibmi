# Native identity candidate validation runbook

This is a manual diagnostic for the D/CG follow-up. It does not start a connector,
read application rows or journal payloads, publish events, alter offsets, or create,
clear, lock, delete or change IBM i objects. Its Java entry point is in test sources
and is not shipped in the connector plugin. Maven tests never invoke its live mode.
F/CR PR #114 and the nonzero-commit-cycle streaming rejection remain unchanged.

**No live run of this TLS/HMAC launcher has been performed.** The
[2026-11-07 isolated validation](dcg-multiple-clear-validation.md) separately
invoked the same production collector through the existing edgeqa test connection
profile and obtained eight successful candidate observations. It did not qualify
this launcher, PTF/lifetime behavior or startup proof. Preparing this tool does not authorize a connection.
The example host is deliberately invalid; example objects are names, not provisioned
fixtures. Existing ASI auction schemas must not be used for these tests.

## What this slice can establish

The probe executes the existing `As400JdbcNativeIdentityEvidence` collector without
copying its SQL or relaxing its validation. Each of two samples checks the expected
OS version/release and a specified installed PTF group/level, then observes exactly
one journal, receiver, file and member. Every target uses a fresh owned connection.
This supplies observations for N1 and a small part of N4's new-connection behavior.
It does not close either gate. It performs no raw retrieval, so it does not exercise
the collector's before/read/after N6 behavior.

It can answer whether the actual deployed SQL service accepts the collector's query
and returns exactly the requested objects. In particular, a `*FILE` may behave as a
directory: disabling recursion does not guarantee a starting-object-only result.
Absent or additional rows are rejected. Do not replace an unsuccessful exact lookup
with parent-library enumeration or select the first of multiple rows.

Matching samples are **candidate observations**, not proof of identity lifetime,
source incarnation, retained history, absence of restore/clone, or safe startup.
The profile reads and object reads are separate connections and are not an atomic
snapshot. Equal observations cannot exclude change-and-change-back races. There is
no proof of journal-to-file membership or source binding in this probe.

## Prepare locally

Use this branch's documented JDK/build dependencies (build JDK 21, Java target 17).
The launcher expects Bash, GNU `timeout`, Java and the repository Maven wrapper.
Build dependencies may require ordinary Maven repository access; they do not contact
IBM i. Run from the repository root:

```sh
mise exec java@temurin-21 -- tools/native-identity-probe build
mise exec java@temurin-21 -- tools/native-identity-probe plan tools/native-identity-probe.example.properties
```

The explicit `build` action compiles test sources and builds a test dependency
classpath. It does not run integration tests. Rebuild after source/dependency changes;
the launcher does not silently build or download anything in `plan` or `collect`.
Project module classes precede dependency jars on the classpath. The build assumes
the matching reactor/SNAPSHOT dependencies described by the normal project setup
are installed. An unresolved dependency is a build failure, not a reason to substitute
an arbitrary connector version.

The `plan` action validates the configuration without reading a key, prompting for
a password, constructing a data source or connecting. It prints a sanitized plan
with target kinds, expected profile, query/class hashes and request limits. Exact
host and target paths remain in the private configuration for operator review.
No argument defaults to collection: incomplete or unknown command lines fail.

Copy [the example](../tools/native-identity-probe.example.properties) outside the
repository. Restrict its access and use an absolute filename when calling the
launcher from another directory. Required fields are strict; unknown keys,
duplicates, wildcard paths, unsupported names and a member outside the named file
are rejected. Never place a password or JDBC URL/options in the configuration.
Use a generic `source-label`, such as `source-a`, because that label is included in
the shareable report.

## Define the separately authorized live scope

Before a live run, record an approved disposable system, its host/TLS identity,
read-only profile, expected IBM i release/PTF level and four **existing** exact
QSYS.LIB paths. Document who owns the dedicated library/journal and who may change
or clean them up. Names in the example must be replaced with verified inventory;
do not assume an old DTEST library is still disposable or belongs to this task.

The future authorization must cover the named endpoint/account, the exact SELECTs
below and output destination. Creating a fixture or changing authorities is separate
work. Use an account whose authority is limited to the approved reads; JDBC read-only
mode is an additional restriction, not the administrative security boundary.

TLS is mandatory in the launcher. Configure the JVM's trusted certificates normally;
there is no insecure fallback. Do not enable driver or JVM tracing containing
credentials or source names. Query access is required for:

- `SYSIBMADM.ENV_SYS_INFO`: only `OS_VERSION` and `OS_RELEASE`; no host-name column.
- `QSYS2.GROUP_PTF_INFO`: the exact configured `PTF_GROUP_NAME` and `PTF_GROUP_LEVEL`,
  using parameters, including target release and installation status. IBM documents
  `*USE` authority to `WRKPTFGRP` for this service.
- `QSYS2.IFS_OBJECT_STATISTICS`: only the collector's fixed projection, exact starting
  path and object type, `SUBTREE_DIRECTORIES => 'NO'`, `IGNORE_ERRORS => 'NO'`.

The profile must match the configured one. The PTF check records the requested
installed group/level; it is not a complete PTF inventory, highest-level discovery
or a compatibility certification. If the expected profile is wrong or unavailable,
the probe stops before the sample's object queries. Review the profile discrepancy
with the operator rather than silently changing the expectation to make it pass.

## Comparison key and sanitized evidence

Generate one random 32-byte key locally, in a new private file, and keep it for all
observations intended for comparison. For example, after substituting a private
absolute path, this command fails if that file already exists:

```sh
(umask 077; dd if=/dev/urandom of=/absolute/private/native-identity-comparison.key bs=32 count=1 conv=excl status=none)
```

The harness requires a regular, non-symlink key file with no group/other permissions
and exactly 32 bytes. Do not use a sample or low-entropy key. Do not include the key,
password or private configuration in issues/PRs. The password is requested on an
interactive console; it is never a command-line argument or configuration property.
No environment password or passwordless fallback is implemented.

Output replaces endpoint, user, paths and each identity component with
domain-separated HMAC-SHA-256 tokens. Exact integer decoding happens in the real
collector before tokenization; zero is an ordinary value. The same key and inputs
produce comparable tokens, including values above JavaScript's exact integer range.
A different key produces a different `comparison_key_id`; those reports cannot be
compared. A copied key/equal endpoint token is not authenticated source identity.
Tokens are pseudonymous evidence, not a claim of universal anonymization.

Reports also include the generic source label, UTC observation interval, matched
release/PTF profile, query/collector/harness class hashes, SQLSTATE/vendor codes for
rejection, and explicit unsupported gates. Raw source identifiers and exception
messages/causes/stack traces are omitted. Keep a private mapping from target tokens
to approved paths and record the connector/core revisions, dependency versions and
authority scope separately. Inspect the report before sharing it, especially the
operator-supplied source label. No automated upload is performed.

## Future collection command and interpretation

**Do not execute this step under the current local-only authorization.** Once the
specific read-only live scope is approved, use a new output filename:

```sh
mise exec java@temurin-21 -- tools/native-identity-probe collect /absolute/private/probe.properties /absolute/private/observation-01.json
```

The output is created with owner-only permissions and `CREATE_NEW`; an existing file
is never overwritten. Each invocation is bounded to two samples, ten connections
and twelve SELECTs. Every SELECT requests at most two rows (the second detects
ambiguity). Login timeout is 10 seconds, socket receive timeout 15 seconds and query
timeout 10 seconds; `thread used=false` makes the receive timeout applicable. An
outer process deadline terminates the invocation after 300 seconds, with a five-second
kill grace. JDBC/server timeout behavior is not a guaranteed execution-time bound;
the process deadline covers client stalls. Timeout termination does not prove server
work was cancelled. There are no automatic retries or repeated polling.

Exit codes and report status:

| Outcome | Meaning |
| --- | --- |
| `0`, `OFFLINE_PLAN` | Local configuration accepted; no observations were made. |
| `0`, `OBSERVED_STABLE_CANDIDATES` | All eight object observations accepted and corresponding tuples matched across two fresh-connection samples under the expected profile. `startup_proof` remains false. |
| `1`, `REJECTED` | At least one profile/object query failed or identities differed. Other observations may exist for diagnosis; none authorizes startup. |
| `2` | Configuration, console, file, credential setup or unexpected client failure. A sanitized generic diagnostic is printed. |
| `124`/`137`, signal, absent/empty/partial JSON | Deadline or interruption; incomplete evidence. Never treat a partial report as successful. |

SQL warnings, missing/extra rows, null identifiers, nonlocal or wrong-ASP objects,
symbolic resolution and wrong object types are rejections. A target failure is
recorded independently so one directory-like object does not hide results for the
other requested targets. A failed sample/run can never become an accepted startup
proof. SQLSTATE without a raw driver message may need separately authorized,
privately handled diagnostics; the harness will not broaden its SQL or expose data.

For repeated runs, compare the comparison-key ID, endpoint/authority/path tokens,
release/PTF profile and query/class hashes first. Compare all three identity tokens
for each target only within that documented scope. Preserve differing reports;
never normalize differences away or retry until a desired value appears.

## Lifecycle and protection work remains separate

Use the [full acceptance matrix](dcg-startup-acceptance.md), not a successful probe
exit, as the release criteria:

| Gate | Evidence still required |
| --- | --- |
| N1 | Authorized exact-query results on the deployed release, including file-directory behavior. |
| N2/N3 | Existing negative fixtures plus any explicitly approved source-level authority/service cases. This probe creates no replacements or permission changes. |
| N4 | Documented identity lifetime plus observations across worker moves, ordinary rollover/detach and IPL. Observing normal operations is distinct from authorizing the tool to trigger them. |
| N5 | Separately authorized destructive disposable-source restore/recreate/clone tests and explicit unsupported-maintenance boundaries. |
| N6 | Actual protected raw retrieval and race tests; two matching SQL observations alone are insufficient. |
| P1-P8 | A selected, implemented provider must retain required receivers, survive crashes/outages, fence old workers, and exclude or revoke unsafe maintenance. No provider has been selected or installed. |
| H/B | Complete protected history, controlled enrollment/baseline and reader/publication handoff. |
| Streaming | Real Kafka Connect acknowledgement, commit/rollback/savepoint, crash/restart and source/sink convergence tests. |

For the protection-provider decision, name the owner of durable receiver pins and
their store, how manual/automatic receiver deletion is blocked, who controls
restore/schema/journaling changes, how a stale worker loses publication rights, and
how a partitioned/failed provider denies unsafe operations. A retention interval,
operator checkbox or successful SELECT is not that implementation. Review this
operational contract with source operators and upstream maintainers before wiring
the recovery coordinator to streaming; this runbook sends no messages to them.

Collection creates only local report files. It has no source cleanup action. Any
future fixture-creation plan must separately list every created object, ownership
and cleanup operation. Neither this document nor a successful run authorizes deleting
an existing library, receiver or table.

## References and local verification

- [IBM i ENV_SYS_INFO](https://www.ibm.com/docs/en/i/7.5.0?topic=services-env-sys-info-view): schema `SYSIBMADM`, nullable version/release columns.
- [IBM i GROUP_PTF_INFO](https://www.ibm.com/docs/en/i/7.5.0?topic=services-group-ptf-info-view): group level, target release, installation state and authority.
- [IBM Toolbox socket timeout](https://www.ibm.com/support/pages/ibm-toolbox-java-socket-timeout): login/receive timeout behavior and `thread used=false`.
- [IBM IFS_OBJECT_STATISTICS](https://www.ibm.com/docs/en/i/7.5.0?topic=services-ifs-object-statistics-table-function): identity service and traversal limits.

`As400NativeIdentityProbeTest` uses synthetic JDBC results and the existing native
identity fixture to exercise the real collector through this harness. It checks
offline behavior, read-only/TLS/timeout configuration, strict input validation,
bounded statements and closure, profile rejection, target failures, cross-connection
identity changes and report sanitization. These tests execute no Db2 SQL. The
offline example is also a launcher smoke test, not evidence of source compatibility.
