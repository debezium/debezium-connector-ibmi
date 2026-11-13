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
