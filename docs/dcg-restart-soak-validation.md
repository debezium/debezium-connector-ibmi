# Legacy restart correction and transactional soak

This follows the [old/fork comparison](dcg-performance-comparison.md), which
retains the original failed restart cases. That comparison found an inherited
legacy offset lag: the emitted row could carry the preceding journal entry's
position. Restart then repeated the final update without its before-image.

## Correction

Ordinary autocommit rows now emit owned record contexts. The final row output
uses the current entry's processed position while the RPC scan cursor still
advances only after successful dispatch. Source metadata describes the current
entry even when the safe restart position is earlier or in another receiver.
Previously queued record offsets do not change as subsequent entries arrive.

An unpaired update before-image pins the serialized restart position to that
entry, inclusively. The scan can continue, and other records can be delivered,
but restart must reread the earliest before-image until **all overlapping update
pairs** complete. For A/UB, B/UB, A/UP, retaining only B/UB is unsafe: restart
would replay A/UP without A/UB. The earliest reconstruction point therefore
remains pinned even after A finishes, until B and any overlapping pairs finish.
A primary
key change's delete/tombstone prefix retains this earlier position; only its
final create can checkpoint the completed update. A plain delete with a
tombstone retains its own inclusive position until successful trailing or idle
heartbeat progress, because failure between delete and tombstone must remain
replayable. The trailing heartbeat cannot regress behind the completed row.

This is not a new exactly-once contract. Crashes before durable offset commit,
pending update pairs, and interrupted multi-record output can still cause replay.
WARN/SKIP intentionally retain their lossy continue-and-advance behavior; FAIL
still stops before RPC advancement. Transaction-buffer and migration emitters
retain their existing checkpoint semantics. F/CR handling and the separate
F/CR PR are preserved.

## Validation method

Local regressions use the real dispatcher and RPC advancement loop with controlled
transport. They cover completed inserts/updates, primary-key changes, receiver
changes, immutable queued offsets, fresh-instance before-image replay, and
failure between delete and tombstone. Existing decode/dispatch, migration,
transaction metadata and stop/recovery assertions remain in place.

Native validation uses the ordinary packaged connector, fresh isolated IBM i test
libraries and local Kafka. The fixed legacy run repeats two 5,000-row
stop/load/resume trials after 21 insert/update bursts. The transactional run
repeats those same checks after real online handoff and then adds:

- A 5,000-row mixed-column transaction committed after its rows remain invisible.
- A 5,000-row transaction rolled back, with unrelated durable progress proving
  the reader has passed the rollback before checking the discarded rows.
- A truncate and 1,000-row reload held open for at least 60 seconds. Unrelated
  table bursts must continue within 15 seconds, the clear must remain invisible,
  and the durable replay floor F must remain behind completed-output position W.
  Commit must deliver one keyless truncate before every reload row and release F.
- Fifteen minutes of serial mixed-column inserts/updates with a real task restart
  halfway through, unchanged binding history and nonregressing durable F/W.

The observer checks exact keys and decoded values, including update before-images.
Final output is drained only after the connector reports STOPPED with no tasks;
late duplicates or rollback leakage fail the strict total-count assertion.
These strict cases provide bounded restart evidence, not general exactly-once
support. All existing production safety guards remain in force.

The worker retains the comparison's pinned Kafka image, 128–512 MiB heap and
two active processors. CPU/RSS sampling and JFR use the same settings. Sustained
traffic is paced by waiting for each burst, so it is not a saturation benchmark.
Live-task RSS and after-GC heap are analyzed separately from the stopped-task
full-GC histogram. A 15-minute run can reveal gross growth or instability but
cannot establish multi-day leak freedom or production capacity.

## Reproduction

Use the explicit private configuration and fixture authorization in
[contributor testing](dcg-contributor-testing.md). No author-specific AWS setup
is required. Compile the test observer and build the assembly before measuring;
do not run builds during the timed workload. Select the same candidate ZIP for
both modes, using fresh directories and unused fixture libraries:

```sh
IBMI_TEST_CONFIG=/absolute/private-fixture.json python3 tools/dcg-performance-probe.py \
  /absolute/legacy-evidence DCNC9991 --distribution /absolute/plugin.zip \
  --label fixed --mode legacy --backlog-rows 5000 --heap-histogram
IBMI_TEST_CONFIG=/absolute/private-fixture.json python3 tools/dcg-soak-probe.py \
  /absolute/soak-evidence DCNC9992 --distribution /absolute/plugin.zip \
  --label fixed-enabled --mode transactional --backlog-rows 5000 \
  --duration 900 --heap-histogram
```

The soak observer has explicit 40-minute, 150,000-record and 256 MiB bounds.
Both runners preserve failure evidence, stop owned workers before deleting the
fixture, close native sessions and verify library/schema/inquiry absence.
Keep raw profiles and captures private: they contain native test and host details.

## Results

The initial legacy and transactional runs passed on 2026-11-10 using the same
pre-review candidate plugin ZIP:
`600996a92b48da52736bb4e07b7f2d031bd80b5e0199889c63757b55e36949dc`.
The source is `feature/transactional-truncate` at
`dfdaf2b9d1fb135c4642de4c29615b3595c2b9fc` plus `pre-advisor-final.patch`.
The manifest's base commit does not identify the uncommitted correction by
itself; the ZIP hash, source hashes and patch identify the tested artifact.

After addressing Advisor's overlapping-pair finding, the final candidate ZIP is
`31fa87562ab01bada0df4e8aeeaa519f58e8b912b838b49c9d3a9a8e0ef93513`,
identified by `final.patch` and `source-files.json`. It passed another complete
native legacy run in fresh DCNC1113: exactly 15,250 records, complete before-images,
no late duplicates, and two 5,000-row resume trials in 15.03 and 14.16 seconds.

- **Legacy:** exactly 15,250 expected records, including both 5,000-row restart
  trials; no replayed final update or missing before-image. Resume-to-drained
  times were 14.68 and 14.50 seconds, including task startup. This repairs the
  retained original 15,252-versus-15,250 failures rather than deduplicating them.
- **Transactional:** exactly 71,401 expected records, including one truncate;
  both initial restart trials passed in 6.69 and 6.53 seconds. The 5,000-row
  transaction drained in 4.01 seconds after commit request. The 5,000 rolled-back
  rows remained absent. The clear stayed open for 62.15 seconds while 12
  unrelated-table bursts progressed; commit delivered the clear followed by all
  1,000 reload rows in 4.44 seconds.
- **Sustained phase:** 904.78 seconds, 65 mixed-workload rounds and 48,850 records,
  including the post-restart progress burst. The halfway task restart preserved
  binding history and durable F/W. All keys, values, before-images, ordering and
  final count assertions passed, including the output drain after task stop.

During the held clear, durable F stayed inclusively at sequence 32176 while W
reached 33284 in the same receiver. After commit, both reached 34385 with an
exclusive F. These are actual Kafka offset readbacks, not inferred scan progress.

The final legacy build's measured 250-record burst medians were 5.45 seconds
for integer inserts, 5.77 for mixed inserts and 5.46 for mixed updates. The
earlier upstream medians were 5.56, 5.89 and 5.53 seconds. Whole-worker CPU was
2.89 ms/record versus the earlier upstream median of 2.88 ms/record. The pre-review
enabled path measured 4.45, 4.27 and 4.37 seconds and 2.20 ms/record. These results
show no material regression in the exercised workload, with the same legacy
journal-cache delay and variable remote SQL time. The new artifact has one run
per mode (five measured bursts per scenario); the earlier baseline has two runs
(ten bursts). This is not a statistical equivalence or maximum-capacity claim.

| Sustained interval | Median worker RSS | Peak worker RSS | Largest after-GC heap |
| --- | ---: | ---: | ---: |
| First 5 minutes | 423.23 MiB | 492.99 MiB | 48.73 MiB |
| Minutes 5–10 | 414.56 MiB | 457.38 MiB | 45.36 MiB |
| Final 5.08 minutes | 414.91 MiB | 419.74 MiB | 44.06 MiB |

Across the sustained phase, observed after-GC heap ranged from 37.39 to
48.73 MiB. Recorded GC pauses totaled 323.50 ms; the largest was 12.28 ms.
There was no sustained memory growth or increasing burst latency in this run.
The same worker JVM hosted the halfway task replacement, so this includes two
roughly 7.5-minute task lifetimes, not a 15-minute uninterrupted task lifetime.
Stopped-worker full-GC histograms retained 32,009,424 bytes in legacy mode and
31,609,024 in transactional mode. Those are separate diagnostics, not the live
heap figures above. Multi-day stability, saturation, many-table workloads and
general exactly-once delivery remain unproven by this experiment.

The long soak preceded the overlapping-pair correction; it was not rerun after
that change. Artifact comparison limits the changed runtime entries to
`As400StreamingChangeEventSource` and its associated generated classes.
Transaction-buffer and recovery classes are byte-identical. The final correction
was checked with the full local suite and repeated native legacy workload. Its
exact two-table interleaving is deterministic local evidence, not a claim that
IBM i test happened to produce that interleaving. Both artifact identities and the
comparison are retained in `review-artifact-delta.json`.

All three fresh fixture libraries, DCNC1111–DCNC1113, were deleted and independently
verified absent along with their schemas and cleanup inquiries. Owned Docker
resources and temporary credential files were removed. No production access,
deployment or push occurred; the separate F/CR worktree/PR was not changed.

Local verification after independent review passed 2,999 Java tests and 146
Python tests, assembly packaging, formatter/import validation and whitespace
checks. Advisor inspected the actual files and found the overlapping-pair
case above. Both new regression variants failed before the correction (restart
at B/UB instead of A/UB), then passed after it. They reload A's published offset
in a fresh instance, reconstruct A's before-image and delete/tombstone/create
key-change sequence, and release the floor only after B completes. This closes
the reported finding; the independent verdict itself remains a concern with its
resolution recorded, rather than being relabeled a second review pass.

Raw evidence was retained privately; rerun the documented tools in a new local evidence directory.
It includes both run reports and raw observer output, durable offsets/history,
native cleanup evidence, JFR recordings/exports, `summary.json`, `analyze.py`,
build/test logs, the tested ZIP and final source/artifact verification.
