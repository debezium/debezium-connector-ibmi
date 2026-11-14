# Old versus new connector performance

This compares the pre-truncate upstream connector at
`2e19ad27c934826d96a84202dfbe4509b5d8f666` with the fork at
`dfdaf2b9d1fb135c4642de4c29615b3595c2b9fc`. The comparison has three cases:
upstream, the fork with transactional truncate disabled, and the fork with it
enabled through the public configuration and real legacy-offset handoff.
Earlier startup/cleanup profiles compared revisions of the new implementation;
they did **not** establish old-versus-new performance.

## Method

The ordinary packaged connector runs in local distributed Kafka Connect. Test
audit classes stay outside its plugin path. Both builds use the same installed
Debezium snapshot dependencies and JTOpen 21.0.4; the seven shared third-party
JARs are byte-identical. Additional history-storage dependencies in the fork are
part of the artifact being evaluated, not removed to improve the result.

Each run uses its own JVM, local broker and fresh isolated IBM i test fixture. Runs
are serial. Kafka/Connect uses the pinned `apache/kafka:4.3.1` image recorded by
digest in each report, Java 21.0.11, a 128–512 MiB worker heap, and
`-XX:ActiveProcessorCount=2`. That JVM setting is not a CPU quota or dedicated
hardware reservation. The developer host and remote test server remain shared.

Settings common to all cases include a 64-record batch, 2,048-record queue,
10 ms poll interval, 1,000 ms heartbeat, JSON converters, precise decimals,
one partition per data topic, and Connect source transactions. Legacy mode uses
poll transaction boundaries; the enabled path requires connector boundaries.
The enabled case also includes its required native validation/history machinery.
No sink participates. A separate continuous `read_committed` consumer observes
the records, avoiding the cost of starting a consumer for every measurement.

Every round performs three autocommit bursts of 250 records:

- Integer-key inserts into A.
- Mixed-column inserts into M (decimal, character, binary, bigint and float).
- Updates to those M rows.

Two rounds warm up the task; five subsequent rounds are measured. Each burst
must deliver exactly the expected keys, operations and decoded values. Missing,
duplicate, corrupt or unexpectedly published data fails the run. A final drain
checks the total output count. Dates, LOBs, wide schemas and large committed
transactions are outside this workload.

Timing starts immediately before the client submits SQL and ends when the
independent consumer sees the last committed record. All timestamps use the
same host clock. SQL execution time is reported separately. These are finite
burst completion rates and visibility latencies, **not maximum sustained rows
per second** or per-row commit-to-delivery latency. In legacy mode, the default
5,000 ms additional journal-cache delay substantially affects small bursts.
The comparison leaves that safety-related default unchanged.

The optional backlog workload stops the connector and waits for no assigned
tasks, loads 5,000 rows (half A, half M), verifies no output during loading, then
resumes it. It validates all output and repeats once. The interval is resume
request to last visible record: source loading is excluded, while schema/history
validation and task restart are included. First-visible and first-to-last-visible
times are retained separately. This exposes backlog processing behavior without
claiming to isolate decoder throughput.

The backlog test deliberately retains a strict **zero replay** check. That is
stronger than this connector's advertised delivery contract: its
`exactlyOnceSupport()` returns `UNSUPPORTED`, including in transactional mode.
Connect's Kafka transactions alone do not remove source replay. A strict restart
failure is retained and excluded from the passing performance summary; it must
not be silently deduplicated or called a performance regression. Run the default
burst-only workload separately to compare steady-state performance when this
stronger restart assertion fails.

Worker CPU and resident memory are sampled every 200 ms. Java Flight Recorder
captures socket reads/writes and GC at identical settings for every case.
Resource summaries cover only the measured burst interval, excluding startup,
warm-up and backlog restarts. CPU includes the whole Connect JVM. RSS includes
the heap and native allocations; it is not retained heap or proof of a leak.
Socket counts are transport operations, not one-to-one SQL or RPC counts.
JFR adds overhead, and short runs cannot establish memory stability over days.
The optional `--heap-histogram` diagnostic uses the host JDK's `jcmd` to request
a full GC **after all measurements and task shutdown**. Its retained-heap count
helps distinguish worker/class-loader retention from heap growth during a run;
it does not measure the live task's memory requirement.

## Reproduction

Use a disposable IBM i environment and the explicit owner-only configuration
described in [the contributor testing instructions](dcg-contributor-testing.md). Each run
requires an unused `DCNCdddd` library and explicit fixture-write authorization.
The runner refuses an existing fixture, retains failure evidence, stops owned
workers before native deletion, and verifies schema/library/inquiry absence.
It does not require the author's cloud account, credential tooling or production infrastructure.

Build both distributions with the same JDK/dependency repository. Compile the
current test helpers; they are used only by the independent observer. For each
case, select the assembly explicitly (the baseline must not accidentally use
the current assembly):

```sh
./mvnw -B -ntp test-compile -DskipTests -DskipITs
IBMI_TEST_CONFIG=/absolute/private-fixture.json python3 tools/dcg-performance-probe.py \
  /absolute/new-evidence DCNC9991 \
  --distribution /absolute/plugin.zip --label old --mode legacy \
  --rows 250 --heap-histogram
```

Use `--label new --mode legacy` for the fork without opt-in and
`--label enabled --mode transactional` for the enabled fork. Use fresh evidence
directories and libraries. Repeat with fresh workers in reverse order. Do not
run builds or other benchmarks during timed native workloads.
Add `--backlog-rows 5000` for the separate strict restart experiment, with the
stronger zero-replay expectation described above.

After each successful run, export its recording, then summarize the run
directories:

```sh
jfr print --json --events jdk.SocketRead,jdk.SocketWrite,jdk.GarbageCollection,jdk.GCHeapSummary \
  /absolute/evidence/connect/control/performance.jfr > /absolute/evidence/jfr.json
python3 tools/dcg-performance-summary.py /absolute/old /absolute/new /absolute/enabled \
  --output /absolute/comparison.json
```

The summary rejects incomplete/failed runs, missing cleanup, different images or
burst sizes, missing repetitions and CPU sampling gaps. Keep raw records and
profiles private: they contain test data, host addresses and native job details.
The report's `source_head` identifies the harness checkout; the **selected
distribution SHA-256** identifies the connector actually loaded.

## Results

The repeated comparison found no material throughput or CPU regression in these
bounded ordinary-CDC workloads. With truncate disabled, median burst completion
times were 0.8%–2.3% higher and worker CPU per record was 2.1% higher. The
SQL-return-to-last-visible medians differed by less than 0.3%; source execution
time explains much of the small end-to-end difference. This is an observation,
not a statistical equivalence claim or a production capacity budget.

| Mode | 250 integer inserts | 250 mixed inserts | 250 mixed updates | Worker CPU / record | Peak RSS across runs |
| --- | ---: | ---: | ---: | ---: | ---: |
| Old upstream | 5.56 s | 5.89 s | 5.53 s | 2.88 ms | 413–458 MiB |
| Fork, truncate disabled | 5.62 s | 5.93 s | 5.66 s | 2.94 ms | 448–612 MiB |
| Fork, truncate enabled | 4.22 s | 4.07 s | 4.51 s | 2.24 ms | 443–444 MiB |

Each timing is the median of ten measured bursts across two fresh workers;
each worker first ran six warm-up bursts. CPU is the median of the two whole-worker
CPU/record measurements. RSS lists the two workers' peak range, not a confidence
interval. Six complete runs passed. Two additional strict restart runs failed as
described below and are excluded from this table.

The enabled path completed four 5,000-record restart/backlog trials in
5.88–7.03 seconds (median 6.38 seconds), including restart validation, with no
replay in those specific trials. This does not establish general source
exactly-once delivery. Its initial online-handoff wait was 72.6–75.3 seconds,
excluded from steady-state timing; that is preparation time, **not measured
application write-lock duration**.

Memory parity is not established: one disabled-fork run reached 612 MiB RSS.
The final stopped-worker full-GC histograms were nearly identical (31,889,536
bytes old versus 31,897,032 bytes fork). That is consistent with transient heap
and GC variation, but stopping the task releases its state, so this is not proof
against an active-task leak. Sustained high-volume transactions, long-lived open
transactions, many tables, concurrent tasks and long-duration memory stability
remain outside this bounded comparison.

Eight isolated libraries (DCNC1101–DCNC1108) and all owned Docker resources were
cleaned up, including the failed cases. Retained evidence includes raw consumer
output, worker offset readback, sampled resources, six finalized JFR recordings,
artifact hashes and independent schema/library/inquiry absence checks. The
initial harness gained optional backlog/heap diagnostics and explicit update
before-image checks; timing definitions and worker artifacts stayed the same. Earlier normal
update before-images were checked independently against retained captures.

Local verification passed 2,985 Java tests, 146 Python tests, assembly packaging,
formatter/import validation and whitespace checks. No connector runtime source
changed. These results describe that historical artifact.

Raw evidence was retained privately; rerun the documented tools in a new local evidence directory.
The key files are `comparison.json`, `validation.json`, `restart-replay.json`,
`configuration-equivalence.json`, and each run's `connect/report.json` and
`native/report.json`. Exact tested plugin ZIPs are retained there:

- Old: `da0cbb3ab5228fbe676520e905b88f36158f4adc0a73a856bd18b4583908339a`.
- Fork: `b6eb84aa11f56391e9fc43d8203b39af68a1aa029b787af2409df055b275e2ac`.

The tested fork archive was assembled before the final fork commit, so its
manifest still names `bea38b735afc8a498fe0f8efd604965dbda94fa6`. Final packaging
names `dfdaf2b9d1fb135c4642de4c29615b3595c2b9fc`. Every nested JAR entry was
compared: the only content difference is that manifest's `Scm-Revision` field;
all runtime classes and resources match. Both artifact identities remain explicit
in `validation.json` and the retained tested ZIP, rather than relabelling it.

Independent Advisor review passed with no actionable findings. The review
confirmed that the raw records, artifact provenance, cleanup and bounded claims
support the report; its verdict is retained in `advisor-review.json`.

### Strict restart finding

The old baseline (`old-02`) and the feature-disabled fork (`new-02`) both failed
the retained zero-replay assertion: each produced 15,252 records instead of
15,250. Each restart replayed the immediately preceding row. The repeated update
had the same key, native journal identity and after-image, but `before=null`;
the original update had its full before-image. The repeated insert retained its
values. Both fixtures were cleaned up successfully.

Worker offset readback and source inspection agree: the legacy row's emitted
offset can represent the previously processed journal entry because its reader
advances the mutable position after dispatch. In these captures, published
entry 7062 restarted from processed entry 7061; the next trial repeated the same
pattern at 12062/12061. Both versions share this behavior. This is not a new
throughput regression, and Kafka transaction settings do not fix it.

Both failed runs remain failures. Their roughly 14-second backlog timings are
diagnostic only, excluded from the passing summary. Their JFR recordings were
not finalized when failure cleanup killed the worker, so no JFR-derived resource
results are claimed for them. Separate burst-only repetitions provide the matched
steady-state comparison. All original strict assertions remain in the runner.
Fixing legacy replay/checkpoint semantics is a separate correctness change; this
performance task changes no connector runtime code.

## Review-correction comparison — 2026-11-11

The later review corrections were measured again against the original upstream
artifact, using serial fresh-library runs, local Kafka Connect 4.3.1 and the same
250-row burst workload. Each configuration ran twice, with six warm-up bursts
followed by five measured bursts for each operation in each run. All eight runs
passed the exact record/key/value assertions and verified fixture/resource cleanup.

| Artifact/profile | A inserts | M inserts | M updates | Worker CPU/record | Peak worker RSS |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original upstream | 5.29 s | 5.40 s | 5.42 s | 2.52 ms | 422–441 MiB |
| Corrected fork, truncate disabled | 5.31 s | 5.47 s | 5.46 s | 2.57 ms | 402–475 MiB |
| Corrected fork, enabled before query consolidation | 6.14 s | 6.24 s | 6.30 s | 2.78 ms | 429–439 MiB |
| Corrected fork, enabled after query consolidation | 3.31 s | 3.30 s | 3.33 s | 1.90 ms | 452–455 MiB |

Times are medians of ten measured bursts per operation. CPU is the median of two
whole-worker measurements during the measured burst intervals; RSS is the range
of those workers' peaks. The disabled path was 0.4–1.3% slower than upstream in
these measurements. This small sample does not establish statistical equivalence.

The first enabled comparison exposed an unnecessary extra native journal query.
The correction selects `DELETE_RECEIVER_OPTION` with the existing exact
`JOURNAL_INFO` observation instead. Every task-owned observation still requires
operator-managed retention (`NO`); missing, ambiguous or unsafe settings still
reject. This removes a round trip without caching or weakening the retention check.
The two repeat runs after that correction measured substantially lower burst
latency and worker CPU. Their RSS peaks were slightly above the upstream runs;
the short experiment does not establish memory parity or long-term leak freedom.

Artifact SHA-256 identities:

- Original upstream: `da0cbb3ab5228fbe676520e905b88f36158f4adc0a73a856bd18b4583908339a`.
- Corrected fork before query consolidation (both disabled/enabled measurements):
  `173d5a8cecdc8585d818669fccc80659ab94423eaac339012133a8fb60a0fbcf`.
- Corrected fork after query consolidation (enabled repeat measurements):
  `ec2646b9ad696430aaa9b96dc85ad1e74cb6629df3eae5e03c674ed499675c9e`.

All used the pinned Debezium core revision
`5eea03dd2a5dd25ad7e8db16cc8c59109dac2da8`. The final artifact also restores an
unrelated request-builder condition to its original behavior. The disabled-path
runs were not relabeled as tests of that final artifact. Retained per-run reports
identify the exact distribution, container image, raw consumer records, JFR
recording, resources and cleanup. The earlier slower enabled results remain in
the evidence rather than being discarded.

These are finite-burst latency/resource measurements. They exclude initial
handoff and do not measure saturation throughput, many-table scaling, every
native layout, long transactions or multi-day stability. Historical restart/soak
results above retain their original artifact identities.

Subsequent native qualification found a clear-companion check that accepted only
`OMTJRNE(*OPNCLO)` despite admitting files using `*NONE`. The follow-up artifact
`b02df4334fdc53098e914a7a604b83bbf5fba45330dc38f036df0b564d8230e3`
also accepts the documented `*NONE` payload, retaining the complete-image and
remote-filtering checks. Nested artifact comparison found only
`As400JournalLifecycle.class` changed. The burst matrix was not rerun or relabeled
after this clear-specific correction; its unit and native verification are
recorded separately.

## Fourth-review correction comparison — 2026-11-13

The final fourth-review correction artifact was measured against the original
connector with the sustained runner. Six serial fresh-fixture/fresh-worker runs
used the order original, disabled, enabled, enabled, disabled, original. Each
performed two warm-up rounds, followed by 60 seconds of independently paced
writes. Each delivered all 12,300 expected changes at 201.22–201.27 offered changes
per second, with exact keys, mixed values, update before-images and final output
counts. All six native and local-runtime cleanup checks passed.

| Profile | Run median-delay range | Median p95 delay | CPU ms/change range | Median CPU vs. original | Peak RSS range |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original connector | 5.206–5.226 s | 5.302 s | 0.816–0.839 | Baseline | 421.1–463.3 MiB |
| Candidate, feature disabled | 5.217–5.218 s | 5.294 s | 0.870–0.872 | +5.2% | 441.4–444.8 MiB |
| Candidate, disk buffer | 2.999–3.096 s | 4.102 s | 1.013–1.055 | +25.0% | 439.0–441.3 MiB |

The median of the two run medians was 5.216 s for the original, 5.217 s disabled
and 3.048 s enabled. Feature-off delay differed by 0.03%; enabled delay was 41.6%
lower. CPU increased in both profiles. The p95 column is the median of the two
run-level p95 values, not a percentile computed by pooling both runs. Resource
measurements cover the whole Connect JVM from the first measured SQL submission
through delivery of the final row. They exclude startup and warm-up. Delay is
SQL submission to an independent `read_committed` consumer, not source commit to
delivery. No sink participates.

Both distributions use byte-identical shared driver/core dependencies and the same
pinned Kafka 4.3.1 image. WARN conversion mode and inherited worker converter/task
defaults are identical. History and disk-storage dependencies remain part of the
candidate. No builds or other native experiments ran during the timed workload;
Advisor performed a read-only source/evidence review. The host and remote server
are shared, so the two repeats do not establish statistical equivalence.

Artifact SHA-256 identities:

- Original connector/parser with matched shared dependencies:
  `7926fa3bb7912adc0fcc1948ce5055a382107ef0e40d2d227f93d0cf252481f1`.
- Final fourth-review candidate, both disabled and enabled:
  `6a98bbc07a058e5ead251ca5ecf1c94c395010c0a6c2d919e8be4d8065de10a0`.

Reproduce each run with an unused isolated library and the owner-only connection
configuration from the contributor instructions:

```sh
IBMI_TEST_CONFIG=/absolute/private-fixture.json python3 tools/dcg-sustained-probe.py \
  /absolute/new-evidence DCNC9991 \
  --distribution /absolute/original-plugin.zip --label original --mode legacy \
  --buffer-storage memory --rows 250 --seconds 60 \
  --failure-handling warn --worker-defaults
```

For the disabled case select the candidate ZIP with `--label disabled --mode legacy`.
For the enabled case select it with `--label disk --mode transactional
--buffer-storage disk`. Use a fresh library and evidence directory for every run,
then repeat in reverse order. Keep the same JVM and recording settings. Retain
each `connect/report.json`, `native/report.json`, sampled resources, raw observer
records and finalized JFR recording. The current evidence summary checks artifact
identity, all correctness/cleanup results, runtime image and CPU-sampling continuity
before calculating the table.

These are bounded latency/resource measurements at the offered rate, not maximum
throughput or an equal-CPU claim. The memory-buffer profile, very large catalogs,
long transactions and multi-day memory stability were not remeasured. Earlier
tables and restart results retain their own artifact qualifications.

## Fifth-review schema-change latency — 2026-11-13

The review's schema-change timing observation measures an unrelated row from
insertion to broker observation. It does not measure time spent inside the
connector or establish that all publication stops for that interval. To reproduce
it, `tools/dcg-schema-refresh-probe.py --schema-timing` runs two rounds of ALTER and
LABEL on captured table B and excluded table U, inserting a probe into A after
each statement returns. Baseline probes bracket each round. All 12 probes must
appear exactly once with their expected keys, and their source offsets must become
durable. This is a separate workload from the steady row-change comparison above.

Two fresh fixtures ran serially, first with separate evidence connections and then
with one connection reused within each schema observation. Each identity and
receiver-directory sample still executes fresh queries; no evidence is cached or
validation removed. Both runs passed their exact output and native/local cleanup
checks. The table contains medians of two samples per case:

| Probe follows | Before connection reuse | After connection reuse |
| --- | ---: | ---: |
| Baseline before schema changes | 4.107 s | 4.154 s |
| ALTER on excluded U | 15.761 s | 13.854 s |
| LABEL on excluded U | 15.163 s | 13.009 s |
| ALTER on captured B | 16.381 s | 13.133 s |
| LABEL on captured B | 13.901 s | 12.608 s |
| Baseline after schema changes | 4.082 s | 4.166 s |

Artifact SHA-256 identities:

- Before reuse: `2cf765ec8b22c3185a4cc5029d949fad92d0ac2539ccf86c988de45b4fc645b6`.
- After reuse, source checkpoint `deb8139`:
  `7f00362ce398f6923d1f7b211f1ff97deb0797413e65d5d973310d913aadc46d`.

The revised observation uses one JDBC session instead of eight for its four
receiver/identity samples. Table-definition acquisition retains its own before
and after checks. The later artifact also contains the constraint-key correction;
this timing workload does not change constraints. No other local native experiment
or build ran during either timed workload, but the database and host are shared.
Observer polling and process startup are included in the measurement.

The remaining additional schema-change latency is material: these samples do not
establish parity with the original connector, maximum DDL throughput, or a fixed
pause duration. They also do not rerun or replace the earlier row-throughput/CPU
matrix. Retain both reports, their raw intervals and cleanup results when comparing
future changes; do not infer sustained DDL throughput from these two repetitions.

A separate recorded run of the same workload also passed all 12 probes and
cleanup. Its JFR snapshot covers the first ten probes, including seven schema
refreshes. On the coordinator thread, recorded blocking events attributed about
26.5 seconds to fresh receiver/identity observations, 24.2 seconds to table
definition acquisition, 1.5 seconds to opening the observation connection and
0.6 seconds to raw journal reads, accumulated across those refreshes. This points
to native metadata round trips as the next optimization target. The recording
excludes waits below its thresholds and is not a complete wall-time accounting;
its timing samples include profiling overhead and are not pooled into the clean
comparison above. No validation check was removed to produce these results.

## Fifth-review final-candidate comparison — 2026-11-13

The final candidate at `48978c6`, including the deferred-creation clear correction,
passed six serial native runs with the same sustained workload and forward/reverse
ordering described above. Every run delivered all 12,300 measured changes after
two warm-up rounds: 73,800 measured changes in total, offered at 201.13–201.31
changes per second. Exact keys, mixed values, update before-images and final output
counts passed. All six fixtures, native sessions and local Kafka resources were
cleaned; a separate read-only check confirmed their absence.

| Profile | Run median-delay range | Median p95 delay | CPU ms/change range | Median CPU vs. original | Peak RSS range |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original connector | 5.220–5.222 s | 5.304 s | 0.870–0.906 | Baseline | 442.2–442.7 MiB |
| Candidate, feature disabled | 5.211–5.221 s | 5.299 s | 0.857–0.908 | −0.6% | 448.1–465.8 MiB |
| Candidate, disk buffer | 2.961–3.045 s | 4.060 s | 1.059–1.066 | +19.7% | 434.9–451.0 MiB |

The medians of the two run medians were 5.221 s, 5.216 s and 3.003 s respectively.
Feature-off delay was 0.10% lower and CPU/change 0.6% lower in these samples;
those small differences do not establish an improvement. Enabled delay was 42.5%
lower, with 19.7% more whole-worker CPU/change. The original's additional journal
cache delay remains unchanged. RSS is resident worker memory, not retained heap;
these short runs do not establish memory stability or a leak.

Both builds used the same pinned Kafka image, WARN conversion mode, inherited
worker defaults and byte-identical shared core/driver dependencies. The original
connector/parser remain unchanged at `2e19ad27`; matching the driver override to
the candidate changed only its manifest metadata, not driver classes or resources.
The candidate additionally includes history and disk-buffer dependencies. No
build, other local benchmark or coordinated native review ran during measurements.
The host and server remain shared.

Exact plugin SHA-256 identities:

- Original with matched dependencies:
  `d30e4a8e5a6646a5df009ad4e53b7444dee02dc9486430a023591219fe62e2a8`.
- Candidate, embedded source revision `48978c6`:
  `6e317878fe16b2d7fc0655e399ffec18c2a6996a5a3570f96c0dad173c0c6bdb`.

Use the [sustained reproduction commands](#fourth-review-correction-comparison--2026-11-13)
with these selected artifacts. Retained evidence checks the artifact hashes,
runtime image, exact output, cleanup and resource-sampling continuity before
calculating these results. Each p95 is summarized across run-level measurements,
not calculated from pooled records. Delay remains SQL submission to independent
Kafka observation, not source-commit latency. Two 60-second repetitions per
profile do not establish statistical equivalence, maximum capacity, large-catalog
performance, long-transaction behavior or multi-day stability. The memory profile
was not remeasured. The separate schema-change latency gap above remains open;
steady row traffic does not measure that work.

## Metadata transport comparison — 2026-11-13

A later change, `e799b3b`, batches short native identity observations with the
existing bounded exact-path query and reuses the schema observation's JDBC
connection for table-definition acquisition. Both complete table observations,
column/key reads, warning checks and journal re-observations still run. This
removes repeated connection establishment and queries; it does not cache evidence
or change offsets, schema-history formats or publication rules.

The same `--schema-timing` workload ran four times in fresh isolated libraries,
in this order: preceding build `012b022`, optimized build, optimized build,
preceding build. Each run performed two rounds with captured B and excluded U,
using Kafka source transactions and transaction metadata. All 48 timed probes
appeared exactly once with their expected keys and durable acknowledgements.
Fixture cleanup and separate read-only source/local-resource absence checks
passed after every run. No other local review build or native experiment ran
during the comparison.

The table reports medians of four end-to-end probe measurements per build and
case; ranges include every sample:

| Probe follows | `012b022` median (range) | `e799b3b` median (range) | Median change |
| --- | ---: | ---: | ---: |
| Baseline before schema changes | 4.086 s (3.784–4.215) | 2.926 s (2.841–3.901) | −28.4% |
| ALTER on excluded U | 14.380 s (14.254–14.438) | 11.541 s (11.067–11.759) | −19.7% |
| LABEL on excluded U | 12.672 s (12.512–12.691) | 11.415 s (10.085–11.533) | −9.9% |
| ALTER on captured B | 14.337 s (11.354–14.448) | 11.464 s (10.407–11.679) | −20.0% |
| LABEL on captured B | 12.678 s (12.626–12.881) | 10.275 s (10.009–11.787) | −19.0% |
| Baseline after schema changes | 3.769 s (2.768–4.162) | 4.325 s (2.784–4.412) | +14.7% |

Artifact SHA-256 identities:

- `012b022`: `3ec09b25e7bfd72d562346a8cae8731ab0ff5e45afa80fe9b1a7dcfcbd039b13`.
- `e799b3b`: `8acf4abe559abceebdffe3e619be4b8d8b21478801a85c96bce66cfdfbd7fc7d`.

The manifests differ in exactly the four intended transport classes. All 203 main
source files and 15 packaged JARs are identified; each plugin records its exact
source revision and matches its compiled classes. Local verification for the
optimized build passed 4,766 connector and 256 parser tests, plus assembly and
formatter/import validation. The initial full-suite attempt exposed a fixture
replay helper that accepted only the former single-object identity query; after
adapting it to consume the same independently captured observations in batches,
the full suite passed. No failure or integrity assertion was removed.

These bounded samples support lower latency for the tested schema changes. They
do not establish steady-row throughput, equal CPU cost, saturation capacity, a
fixed pause duration or parity with the original connector. Baseline controls
vary and the after-round baseline median increased; observer polling and process
startup are included, and the host and IBM i are shared. The earlier steady-row
comparison remains pinned to its own builds. The remaining schema-change latency
and the separate correctness review remain open readiness work.

## Shared-rollback candidate comparison — 2026-11-13

The `89ff007` candidate passed six serial native runs after the historical-name,
excluded-lifetime and shared ALTER/DROP commit/rollback corrections. The order was
original, disabled, disk, disk, disabled, original. Each run delivered all 12,300
measured changes after two warm-up rounds: 73,800 changes in total, offered at
201.14–201.28 changes per second. Exact keys, mixed values, update before-images
and output counts passed. All fixtures, sessions and local Kafka resources were
cleaned, and a separate read-only check confirmed their absence.

| Profile | Run median-delay range | Median p95 delay | CPU ms/change range | Median CPU vs. original | Peak RSS range |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original connector | 5.216–5.244 s | 5.461 s | 0.869–0.871 | Baseline | 431.4–468.6 MiB |
| Candidate, feature disabled | 5.213–5.224 s | 5.319 s | 0.876–0.901 | +2.1% | 425.7–457.9 MiB |
| Candidate, disk buffer | 2.164–2.220 s | 3.044 s | 1.058–1.074 | +22.5% | 438.6–440.9 MiB |

Medians of the two run medians were 5.230 s, 5.219 s and 2.192 s respectively.
Feature-off delay was 0.21% lower and CPU/change 2.1% higher. Enabled delay was
58.1% lower, with 22.5% more whole-worker CPU/change. All profiles sustained the
same offered workload; none was driven to saturation. The original journal-cache
delay remains unchanged. RSS is resident worker memory, not retained heap, and
its short-run range does not establish a leak or memory equivalence.

The shared core/driver JARs and pinned Kafka image were identical across profiles.
The original connector and parser remain at `2e19ad27`. Only driver manifest
metadata changed from the preceding matched baseline; driver classes and resources
did not. WARN conversion mode and inherited worker defaults were used in every
run. No coordinated build, test suite or other native experiment overlapped the
measurements. The host and database server remain shared.

Exact plugin SHA-256 identities:

- Original with matched dependencies:
  `3aeacf59343b6b08fe70ab5284f9047eb63c1b1db6fa202dcc7e25270be4d5e1`.
- Candidate, embedded source revision `89ff007`:
  `9fe97cfbf89f4ebd72e5aff801e323693323efd279a76296cc50597d2c6f03dc`.

Use the [sustained reproduction commands](#fourth-review-correction-comparison--2026-11-13)
with these artifacts. The summary verifies artifact identity, runtime image,
correctness, cleanup and resource-sampling continuity before calculating results.
Each p95 is summarized across run-level measurements rather than pooled records.
Delay measures SQL submission to independent Kafka observation, not source commit.
These two 60-second repetitions per profile do not establish maximum capacity,
statistical equivalence, large-catalog performance, long-transaction behavior or
multi-day stability. This matrix exercises ordinary row traffic; it does not
remeasure the memory-buffer profile or the separate schema-change latency cases.


## Library-move candidate comparison — 2026-11-13

Build `04d40bd` passed the same six-run matrix after the member-rename, file-move
and ALTER→move corrections. The runs were serial: original, disabled, disk,
disk, disabled, original. Each used a fresh isolated native fixture and local
Kafka, two warm-up rounds and a 60-second measured workload. All 73,800 changes
passed exact keys, mixed values, update before-images and output counts. Offered
rates were 201.49–201.52 changes per second. Every run's cleanup passed; an
independent read-only check found no remaining fixture libraries, journals,
inquiry messages or owned Docker resources.

| Profile | Run median-delay range | Median p95 delay | CPU ms/change range | Median CPU vs. original | Peak RSS range |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original connector | 5.135–5.140 s | 5.233 s | 0.846–0.872 | Baseline | 431.6–436.1 MiB |
| Candidate, feature disabled | 5.129–5.130 s | 5.201 s | 0.882–0.892 | +3.3% | 464.1–465.4 MiB |
| Candidate, disk buffer | 1.966–1.980 s | 2.713 s | 1.065–1.077 | +24.7% | 447.5–449.0 MiB |

Medians of the two run medians were 5.138 s, 5.130 s and 1.973 s respectively.
Feature-disabled delay differed by −0.16%, with 3.3% more whole-worker CPU per
change. Disk-buffer delay was 61.6% lower, with 24.7% more CPU per change. All
profiles sustained the offered workload. CPU cost is not identical, and this
rate was not a saturation test. RSS is resident worker memory, not Java heap;
the short-run ranges do not establish memory equivalence or leak freedom.

The original connector/parser remain at `2e19ad27`. Shared Debezium and driver
JARs match byte-for-byte across profiles; only manifest metadata changed in the
driver override when preparing the matched baseline. The candidate additionally
packages its history/disk storage. The Kafka image, WARN conversion mode and
inherited worker defaults were the same. No coordinated build, test suite or
other native experiment overlapped the measurements. Independent review of the
correction finished its test execution before timing began; its lightweight
report and Advisor pass did not run builds or tests. The host and database
server remain shared.

Exact plugin SHA-256 identities:

- Original with matched dependencies:
  `1ecaff67dfe7067c0c493b945d2330eebeaf17d4a16fc79b4e7fdc989ec48dfd`.
- Candidate, embedded source revision `04d40bd`:
  `6772a7f39e6c45f9584b24e638a38599cf6862971209c7e32a30f7b936fcb614`.

Use the [sustained reproduction commands](#fourth-review-correction-comparison--2026-11-13)
with these artifacts. The summary verifies distribution hashes, runtime image,
correctness, cleanup and resource-sampling continuity before calculation. Delay
measures SQL submission to independent Kafka observation, not source commit;
p95 values are summarized across runs rather than pooled records. Two short
repetitions do not establish statistical equivalence, large-catalog or
long-transaction behavior, maximum capacity or multi-day stability. These runs
exercise ordinary row traffic; they do not remeasure the memory-buffer profile
or schema-change latency. Earlier results retain their own artifact scope.

## Published-build performance comparison — 2026-11-14

The published `96a395a` artifact passed six serial runs in forward/reverse order:
original, feature disabled, disk buffer, disk buffer, feature disabled, original.
Each used a fresh native fixture, two warm-up rounds and 120 seconds of paced
writes. All 144,000 measured changes (24,000 per run) passed exact key, mixed-value,
update-before-image and final-count checks. Offered rates were 200.75–200.77
changes/second. Each run's native fixture, session and local Kafka cleanup passed.

| Profile | Run median-delay range | Median p95 delay | CPU ms/change range | Median CPU vs. original | Peak worker RSS range |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original connector | 5.123–5.124 s | 5.193 s | 0.729–0.744 | Baseline | 412.1–435.6 MiB |
| Candidate, feature disabled | 5.129–5.131 s | 5.197 s | 0.760–0.766 | +3.6% | 408.9–471.6 MiB |
| Candidate, disk buffer | 1.933–1.935 s | 2.688 s | 0.907–0.914 | +23.6% | 440.7–482.4 MiB |

Medians of the two run medians were 5.124 s, 5.130 s and 1.934 s respectively.
The disabled path's median delay increased 0.12%; enabled delay decreased 62.3%.
All profiles sustained the offered rate. The enabled path costs more CPU per
change; this is not a claim of unchanged resource cost or maximum throughput.
RSS is resident memory for the whole worker JVM, not retained Java heap. The
two-run ranges do not establish memory equivalence or leak freedom.

The original connector/parser remain at `2e19ad27`. Shared Debezium and driver
JARs match the candidate; preparing the baseline changed only driver-override
manifest metadata, not its classes or resources. All runs used the same pinned
Kafka 4.3.1 image, WARN conversion mode and inherited worker defaults. Kafka
source transactions were enabled: the original and disabled paths used poll
boundaries, and the enabled path used connector boundaries. This is a distributed
Connect comparison, not an Embedded Engine or Debezium Server benchmark.
No coordinated build, test suite or other native experiment overlapped timing;
the host and database remain shared.

Exact plugin SHA-256 identities:

- Original with matched dependencies:
  `46f4010f4c0683a43ad6c3602e881b13d125c8d04de940d250b370bc07c1ef6c`.
- Published candidate, embedded source revision `96a395a`:
  `c8a28302ff5b8bebf54d7922c40dba9e937842d9dacfaa01d7b3d1727a96cd80`.

Use the [sustained reproduction commands](#fourth-review-correction-comparison--2026-11-13)
with `--seconds 120` and these artifacts. Delay measures SQL submission to
independent Kafka observation, not source commit time; p95 is the median of the
run-level p95 values. These short repetitions do not establish statistical
equivalence, saturation capacity, large-catalog behavior or multi-day stability.
The matrix does not remeasure the memory-buffer profile or schema-change latency.
