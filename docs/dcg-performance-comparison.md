# Performance measurement

Compare reader behavior and transaction storage separately. Delivery latency,
throughput, CPU and retained memory are different measurements; no single result
establishes all four. Use the [testing guide](dcg-contributor-testing.md#performance-and-endurance)
for setup and commands.

The recorded comparison uses the original connector/parser at `2e19ad27` and
the transactional runtime at `96a395a`, with byte-identical shared Debezium core
and driver dependencies. Exact artifact identities are recorded below. These
are observations from November 14, 2026, not guarantees for later builds or other
workloads.

## Reader delay is separate from buffer storage

The original/disabled profiles use the legacy journal reader. Its
`journal.additional.delay` defaults to 5,000 ms and is added even when journal
caching is disabled. If caching is enabled, the configured IBM i cache-wait time
is added as well. This conservative delay originates in
[the missing-events correction, PR #72](https://github.com/debezium/debezium-connector-ibmi/pull/72).
IBM documents that receiver attributes can count cached records before journal
retrieval APIs return them ([journal-cache visibility](https://www.ibm.com/support/pages/basics-journal-cache)).

The transactional reader does not apply this fixed delay. Its live scan and
recovery progress are based on retrieved entries and durable output, not a jump
to the receiver's reported end. The two paths also use different Kafka transaction
boundaries. The original-versus-disk timings therefore do **not** show that disk
is faster than memory, nor isolate how much improvement comes from each reader
or acknowledgement difference. Five seconds is not an inherent requirement of
disk or memory storage; changing the legacy default needs separate correctness
validation of delayed visibility. It is unchanged in these comparisons.

A storage comparison must use the same transactional reader, workload, worker,
converter and acknowledgement settings, changing only `truncate.buffer.storage`.
Any such measurements must be reported separately from the original-versus-new
comparison and from the serial-burst endurance workload.

The sustained comparison uses matched **test settings**, not otherwise-default
connector configuration: `max.batch.size=64`, `max.queue.size=2048`,
`poll.interval.ms=10`, `heartbeat.interval.ms=1000`, and worker
`offset.flush.interval.ms=1000`. Each measured table has one Kafka partition.
JSON converters omit schemas; the worker has a 128 MiB initial/512 MiB maximum
heap and `ActiveProcessorCount=2` (a JVM setting, not CPU affinity). All profiles
use the same non-TLS native test configuration. Kafka source transactions
are enabled, with poll boundaries for legacy and connector boundaries for the
transactional reader. Transactional memory/disk comparisons keep those settings
identical and change only `truncate.buffer.storage`.

The independent writer submits 100 operations per SQL statement, alternating
simple inserts, mixed-column inserts and updates. Latency starts immediately
before submitting each statement and ends when a separate `read_committed`
consumer receives each corresponding record. It includes database execution,
transport, capture, serialization, Kafka transaction visibility and consumer
polling. Source/server clocks are not subtracted. The reported rate is the
achieved submission rate; a requested rate that the writer cannot reach is not
evidence of connector capacity. Exact keys, values, before-images, counts and
duplicate checks must pass before a run contributes a result.

CPU covers the whole worker JVM from the first measured submission through final
observed delivery, using 200 ms process samples. RSS is resident worker memory,
not retained heap. The native runs were serial, without coordinated builds or
other test workloads during rate measurements; the host and IBM i remain shared,
so external contention is not ruled out.

## Matched reader and storage controls — 2026-11-14

Ten further serial runs used the same frozen `96a395a` candidate and matched
`2e19ad27` baseline identified below. Each had two warm-up rounds and 120 seconds
of independent paced writes. All **768,300 measured changes** passed exact keys,
mixed values, update before-images, total counts and duplicate checks. Each run
passed native fixture/session and local Kafka cleanup. These are one observation
per profile/rate, not repeated-run confidence intervals.

| Requested changes/s | Profile | Achieved changes/s | Median delay | p95 delay | Worker CPU ms/change | Peak worker RSS |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 500 | Original, 5 s margin | 500.3 | 5.214 s | 5.301 s | 0.323 | 411.7 MiB |
| 500 | Candidate, feature disabled | 500.3 | 5.212 s | 5.298 s | 0.323 | 414.7 MiB |
| 500 | Transactional, disk | 500.4 | 2.310 s | 3.155 s | 0.453 | 453.2 MiB |
| 500 | Transactional, memory | 499.5 | 2.256 s | 3.129 s | 0.377 | 450.0 MiB |
| 500 | Original, zero-delay diagnostic | 500.1 | 0.172 s | 0.252 s | 0.320 | 457.6 MiB |
| 1,000 | Original, 5 s margin | 781.0 | 5.226 s | 5.311 s | 0.220 | 443.4 MiB |
| 1,000 | Candidate, feature disabled | 779.8 | 5.231 s | 5.361 s | 0.240 | 444.5 MiB |
| 1,000 | Transactional, disk | 769.1 | 2.491 s | 3.364 s | 0.368 | 454.1 MiB |
| 1,000 | Transactional, memory | 783.0 | 2.372 s | 3.257 s | 0.285 | 453.5 MiB |
| 1,000 | Original, zero-delay diagnostic | 774.5 | 0.181 s | 0.264 s | 0.230 | 470.3 MiB |

At the matched approximately 500-change/second load, disk buffering on the new
path reduced median delivery delay **55.7%** and p95 delay **40.5%** relative to
the original's default margin. Whole-worker CPU per change increased **40.1%**.
Feature-disabled latency was essentially unchanged in this observation. At the
higher requested rate, writers actually achieved only 769–783 changes/second;
those rows do not establish 1,000-change/second capacity. The disk run's median
was 52.3% lower than the original's, with 67.3% more CPU per change, at slightly
different achieved loads. Feature-disabled CPU/change was 8.9% higher than the
original in that observation; the single runs do not establish CPU equivalence.
The earlier two-repeat 201-change/second matrix remains separate evidence; none
of these short runs is a maximum-throughput benchmark.

**Disk was not faster than memory.** At approximately 500 changes/second, memory
measured 2.256 s median versus disk's 2.310 s, and 0.377 versus 0.453 CPU ms/change.
At the higher achieved rates the medians were 2.372 s and 2.491 s. These modest
latency differences and lower memory-mode CPU cost support offering both storage
choices, not a universal performance ranking. The disk run used the local
container's disk-backed `overlay2` filesystem, without a `/tmp` RAM-disk mount;
H2's cache/write buffering and OS page caching still apply. Small transactions
can therefore exercise the disk-backed store without forcing a physical write
for every record. This is not an uncached disk-I/O benchmark. Disk is the default
to bound pending transaction memory; this workload does not measure very large
transaction capacity.

**The zero-delay legacy controls isolate the wait's cost.** Setting only
`journal.additional.delay=0` reduced legacy median delay from 5.214 to 0.172 s at
approximately 500 changes/second, and from 5.226 to 0.181 s at the higher achieved
rates. That is approximately five seconds in both comparisons. The new
transactional path still has more processing and different Kafka commit
boundaries, so its net benefit over the normal legacy default is smaller than
five seconds. These tests do not isolate each remaining source of latency.

The zero-delay setting was used only in isolated diagnostic runs. Passing their
row/count checks does **not** qualify legacy advancement through temporarily
unavailable cached journal entries, or justify changing its default. Neither
legacy defaults nor production sources were changed for this experiment. The
new reader already avoids the fixed wait and retains progress from retrieved
entries and acknowledged output.

To reproduce the storage comparison, run `dcg-sustained-probe.py` twice with
`--mode transactional`, the same artifact and settings, changing only
`--buffer-storage disk` to `--buffer-storage memory`. Use a fresh fixture and
output directory each time. To reproduce the isolated delay control, use the
same original artifact with `--mode legacy --journal-additional-delay 0`;
omitting that option retains the legacy default. Use `--seconds 120`,
`--operations-per-second 500` or `1000`, `--rows 250`, `--failure-handling warn`
and `--worker-defaults` as in the [performance commands](dcg-contributor-testing.md#performance-and-endurance).
The two requested rates are targets for the independent writer, not connector
capacity claims. Reader, Kafka visibility and storage effects must not be
presented as a single disk-speed gain.

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

Use the [sustained reproduction commands](dcg-contributor-testing.md#performance-and-endurance)
with `--seconds 120` and these artifacts. Delay measures SQL submission to
independent Kafka observation, not source commit time; p95 is the median of the
run-level p95 values. These short repetitions do not establish statistical
equivalence, saturation capacity, large-catalog behavior or multi-day stability.
The matrix does not remeasure the memory-buffer profile or schema-change latency.
