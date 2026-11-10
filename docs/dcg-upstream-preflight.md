# D/CG historical local upstream preflight — 2026-11-08

This is the earlier preflight snapshot. The later [review series](dcg-review-series.md)
includes the credential/startup corrections and builds at each commit boundary.
Counts and no-commit statements below describe this earlier preparation.

This is a local qualification record for `dbz-3025-dcg-followup`, based on
`838359c2811baf96529502cd6c6023573722df29` plus the uncommitted implementation.
It does not change the published PR, establish maintainer acceptance, or certify
an arbitrary deployment. F/CR PR #114 remains separate. D/CG stays disabled by
default, and all native initialization/recovery admission checks remain required.
The base already contains F/CR commit `7f9a68c` and D/CG groundwork `838359c`.
The review patch therefore applies on that branch base, not directly on upstream
`main`; upstream submission must retain the PR #114 dependency or rebase after
that prerequisite lands.

## Kafka version policy

The primary upstream target is Kafka Connect **4.3.1**, matching the Debezium 3.8
parent dependency and its [published release baseline](https://debezium.io/releases/3.8/release-notes#kafka-compatibility).
The D/CG candidate admits Apache Connect **4.2.x and 4.3.x**, with actual source
transaction capability required in addition to the version check. These are
Connect worker versions; broker compatibility is a separate question. The local
runs pair each worker with the same broker version. They do not qualify every
mixed-version cluster or every future patch release.

The original 4.3.0/4.3.1 experiment evidence remains in the preceding quality
validation record. This revision adds digest-pinned 4.2 images and checks the real
runtime version, public transaction context, actual connector-defined commit path,
and durable offset callbacks. Source startup is synthetic in these local tests.
They are not a substitute for the separate native initialization qualification.

| Worker and broker | Checks in this revision |
| --- | --- |
| 4.2.0 | Acknowledgement/restart; missing, malformed and incompatible offsets; interrupted batch replay; mixed row/tombstone/metadata output; broker fencing and legitimate reassignment |
| 4.2.2 | Acknowledgement/restart and invalid-offset cases |
| 4.3.1 | Acknowledgement/restart regression and prototype-independent preflight |

Cross-Maven CI selects the oldest admitted minor (4.2.0) and the upstream baseline
(4.3.1), retaining two jobs. It runs the acknowledgement and invalid-offset
contracts on both. Additional matrix cases are available through the same runner.
The original failing network-partition reproduction and stricter historical tests
are preserved. Engine/Server, standalone Connect and untested worker families
remain outside this D/CG runtime contract; ordinary CDC/F/CR admission is unchanged.

Initial attempts to pull the new images failed at the Docker daemon's registry
DNS lookup, before task startup. Those failed reports and their successful cleanup
checks were retained. A client-side transfer verified the pinned OCI index,
linux/amd64 manifest, config, compressed layer digests and uncompressed diff IDs
before loading the images locally. Retried tests use the original pinned image
references. No assertion was relaxed and no Docker/system DNS setting changed.

## First-start helper correction

`dcg-start-window prepare` now defaults to the version-2 chunked plan required by
normal startup: 256 rows / 1,000,000 owned bytes per chunk. Explicit total budgets
may exceed the old list-based 9,000-row ceiling. Historical version-1 plans require
`--legacy-list-plan`; the list-based experiment passes that flag explicitly.
Validation remains active with `python -O`. Offline regressions cover invalid
scope/budgets, partial chunk arguments, overwrite rejection and revocation that
preserves enrollment without claiming completed native drain.

This fixes plan creation; it does not remove the operator-controlled first-start
window or turn a local file into proof that IBM i writers have stopped. Maintainer
acceptance of that operational prerequisite remains a merge decision.

## Independent export and verification

A separate Worktrunk worktree, `dbz-3027-review-preflight`, receives the complete
active candidate patch on the same base. The historical `src/prototype-test`
Java sources are absent. The default build, Python checks and local Connect runner
must work in this checkout without copying those sources or their class files.
The candidate packager no longer hashes or loads custom receipt/owner sources;
the stricter runner retains its existing default and optional test profile.

The local review bundle contains:

* `active-candidate.patch`: complete active implementation, default tests,
  documentation and contributor tools, based on the commit above.
* `optional-prototypes.patch`: historical Java prototypes and stricter tests,
  kept separately; the original working branch retains them unchanged.
* `manifest.json`: base/source/file hashes, patch hashes and qualification paths.
* `pr-description.md` and `maintainer-comment-draft.md`: local publication drafts.
* `contributor-registration.patch`: Michael Gardner / gardnmi additions in core,
  checked against the unmodified core HEAD using a separate temporary Git index.

These patches are additive review artifacts, not the four independently buildable
commits proposed in the submission document. The active patch remains substantial;
maintainers should agree on the runtime/initialization scope and dependency order
before it is presented as a merge-ready series. No commits, pushes or publication
were performed, and the original index/worktree changes were preserved.

Verification uses Debezium core `0d2ebf88ceebd7d991b370cb8d028b91d4ac6f91`, JDK 21,
Java target 17, formatter/import validation and assembly packaging. The clean
active build passes 2,314 unit tests; the original checkout passes 2,709 with the
historical profile; 10 offline Python tests pass. Workflow lint is checked with
Actionlint 1.7.12 and ShellCheck 0.10.0. Local Kafka cases retain independent broker
output/offset evidence and require verified cleanup. See the machine-readable
bundle for exact results and artifact hashes.

No IBM i connection was made for this revision. Earlier isolated native temporal
runs passed 47 checks twice and the portable packaged smoke passed 32, with fixture
cleanup verified. Those qualify the preceding native implementation, not a new
native run on Kafka 4.2. This revision changes runtime admission and local helper/
packaging behavior; it makes no new native performance or production throughput
claim. Remaining supported-scope limitations are in `dcg-configured-native.md`.
